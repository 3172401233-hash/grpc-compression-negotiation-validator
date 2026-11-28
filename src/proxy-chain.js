import { parseMessages } from './message-parser.js';
import { planMessage, negotiateEncoding } from './planner.js';
import { planRetry } from './retries.js';
import { compress, decompress } from './codecs.js';
import { DEFAULT_MAX_MESSAGE_BYTES } from './framing.js';
import { ErrorCode, GrpcProxyError, makeLocation } from './errors.js';

const VALID_TRANSFORMS = new Set(['none', 'force-encoding', 'strip-compression']);

/**
 * 离线规划一条代理链上每条消息的处理方式。
 *
 * 该函数不发起任何网络请求，只基于输入的字节、header 与跳配置，
 * 产出逐跳逐消息的透传/解压/重压/拒绝决策。
 *
 * @param {object} input
 * @param {Buffer} input.body
 * @param {object|Map|Array} input.headers
 * @param {'grpc'|'grpc-web'} [input.mode]
 * @param {object} [input.externalTrailers]
 * @param {Array} input.hops 每跳 { acceptEncoding, transform, transformEncoding,
 *                               maxMessageBytes, maxWireFrameBytes, fallbackProtocols }
 * @param {number} [input.maxMessageBytes] 入站解压后消息上限（独立于各跳输出限制）
 * @param {number} [input.maxWireFrameBytes] 入站线上帧上限（独立于各跳输出限制）
 * @param {object} [input.retry] { idempotent, attempt, maxAttempts, retriableStatus }
 */
export async function planProxyChain({
  body,
  headers,
  mode = 'grpc',
  externalTrailers,
  hops,
  maxMessageBytes,
  maxWireFrameBytes,
  retry = {}
}) {
  if (!Array.isArray(hops) || hops.length === 0) {
    throw new TypeError('hops 必须是非空数组');
  }

  const parsed = await parseMessages({
    body,
    headers,
    mode,
    externalTrailers,
    maxMessageBytes: maxMessageBytes ?? DEFAULT_MAX_MESSAGE_BYTES,
    maxWireFrameBytes
  });

  const hopPlans = [];
  // 链级错误汇总只统计各跳决策错误；解析层错误保留在 parsed.errors，
  // 避免同一根因在 summary 中被重复计数。
  const chainErrors = [];

  // 每一跳维护"进入该跳时"的消息视图，初始为源线上字节与源编码。
  // 离线规划同样在内存里真实转码，保证编码标签与字节始终一致，
  // 下游跳才能按正确算法解压（全程不建立任何网络连接）。
  let currentView = parsed.messages.map((message) => ({
    messageIndex: message.messageIndex,
    payload: message.payload,
    encoding: message.wireEncoding ?? message.headerEncoding,
    compressedFlag: message.compressedFlag,
    sourceError: message.error
  }));

  for (let hopIndex = 0; hopIndex < hops.length; hopIndex += 1) {
    const hop = hops[hopIndex];
    const transform = hop.transform ?? 'none';
    if (!VALID_TRANSFORMS.has(transform)) {
      throw new GrpcProxyError(
        ErrorCode.UNSUPPORTED_TRANSFORM,
        `不支持的代理转换: ${transform}`,
        makeLocation({ hopIndex, phase: 'negotiation' })
      );
    }

    const negotiationLocation = makeLocation({ hopIndex, phase: 'negotiation' });
    const negotiation = negotiateEncoding(hop.acceptEncoding, negotiationLocation);
    const negotiatedEncoding = negotiation.error ? null : negotiation.encoding;

    let forcedEncoding;
    if (transform === 'force-encoding') {
      forcedEncoding = String(hop.transformEncoding ?? '').toLowerCase();
      if (!forcedEncoding) {
        throw new GrpcProxyError(
          ErrorCode.UNSUPPORTED_TRANSFORM,
          'force-encoding 转换必须提供 transformEncoding',
          makeLocation({ hopIndex, phase: 'negotiation' })
        );
      }
    } else if (transform === 'strip-compression') {
      // 剥离压缩的唯一合法目标是 identity，且对端必须显式接受 identity。
      forcedEncoding = 'identity';
    }

    const decisions = currentView.map((entry) => {
      const location = makeLocation({
        hopIndex,
        messageIndex: entry.messageIndex,
        phase: 'payload'
      });

      if (entry.sourceError) {
        return {
          messageIndex: entry.messageIndex,
          action: 'reject',
          sourceEncoding: entry.encoding,
          targetEncoding: null,
          compressedFlag: false,
          inputWireSize: entry.payload ? entry.payload.length : 0,
          outputWireSize: null,
          reencodeCpuCost: 0,
          sizeDelta: 0,
          fallbackProtocol: {
            available: (hop.fallbackProtocols ?? []).length > 0,
            protocol: (hop.fallbackProtocols ?? [])[0] ?? null,
            reason: entry.sourceError.code
          },
          error: {
            ...entry.sourceError,
            location: { ...entry.sourceError.location, hopIndex }
          }
        };
      }

      return planMessage({
        wirePayload: entry.payload,
        sourceEncoding: entry.encoding,
        compressedFlag: entry.compressedFlag,
        acceptEncoding: hop.acceptEncoding,
        targetEncoding: forcedEncoding,
        maxMessageBytes: hop.maxMessageBytes ?? DEFAULT_MAX_MESSAGE_BYTES,
        maxWireFrameBytes:
          hop.maxWireFrameBytes ?? hop.maxMessageBytes ?? DEFAULT_MAX_MESSAGE_BYTES,
        fallbackProtocols: hop.fallbackProtocols,
        location: { ...location, messageIndex: entry.messageIndex }
      });
    });

    for (const decision of decisions) {
      if (decision.error) {
        chainErrors.push(decision.error);
      }
    }

    hopPlans.push({
      hopIndex,
      mode: hop.mode ?? mode,
      acceptEncoding: hop.acceptEncoding,
      transform,
      forcedEncoding: forcedEncoding ?? null,
      negotiatedEncoding,
      negotiationError: negotiation.error
        ? {
            code: negotiation.error.code,
            message: negotiation.error.message,
            location: negotiation.error.location
          }
        : null,
      decisions
    });

    // 为下一跳构造视图：按决策在内存中真实转码；拒绝的消息冻结原字节与错误。
    currentView = currentView.map((entry, index) => {
      const decision = decisions[index];
      if (decision.action === 'reject') {
        return {
          ...entry,
          sourceError: decision.error ?? entry.sourceError
        };
      }

      const decoded =
        entry.encoding === 'identity'
          ? Buffer.from(entry.payload)
          : decompress(
              entry.encoding,
              entry.payload,
              makeLocation({ hopIndex, messageIndex: entry.messageIndex, phase: 'payload' })
            ).output;

      const nextPayload =
        decision.targetEncoding === 'identity'
          ? decoded
          : compress(
              decision.targetEncoding,
              decoded,
              makeLocation({ hopIndex, messageIndex: entry.messageIndex, phase: 'payload' })
            ).output;

      return {
        messageIndex: entry.messageIndex,
        payload: nextPayload,
        encoding: decision.targetEncoding,
        compressedFlag: decision.compressedFlag,
        sourceError: null
      };
    });
  }

  // trailer 终态与重试规划。
  let retryPlan = null;
  if (parsed.trailer && !parsed.trailer.error) {
    retryPlan = planRetry({
      grpcStatus: parsed.trailer.status,
      idempotent: retry.idempotent ?? false,
      attempt: retry.attempt ?? 1,
      maxAttempts: retry.maxAttempts ?? 1,
      sentMessages: parsed.messages.map((message) => ({
        replayable: message.error === null
      })),
      retriableStatus: retry.retriableStatus
    });
    if (retryPlan.error) {
      chainErrors.push({
        code: retryPlan.error.code,
        message: retryPlan.error.message,
        location: retryPlan.error.location
      });
    }
  } else if (parsed.trailer?.error) {
    retryPlan = {
      retry: false,
      reason: 'TRAILER_INVALID',
      error: parsed.trailer.error
    };
  }

  const summary = {
    hopCount: hops.length,
    messageCount: parsed.messages.length,
    rejectedDecisions: chainErrors.length,
    parserErrors: parsed.errors.length,
    totalReencodeCost: hopPlans.reduce(
      (sum, hop) => sum + hop.decisions.reduce((inner, decision) => inner + decision.reencodeCpuCost, 0),
      0
    ),
    ok: chainErrors.length === 0 && parsed.errors.length === 0
  };

  return {
    mode,
    headerEncoding: parsed.headerEncoding,
    parsed,
    hops: hopPlans,
    retry: retryPlan,
    errors: chainErrors,
    summary
  };
}
