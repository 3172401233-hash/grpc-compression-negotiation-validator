import { compress, decompress } from './codecs.js';
import { encodeDataFrame, encodeGrpcWebTrailerFrame } from './framing.js';
import { ErrorCode, GrpcProxyError, makeLocation } from './errors.js';

/**
 * 在内存中执行一条消息的转码决策，产出可直接在线上发送的完整帧字节。
 * 纯本地操作，不建立任何 gRPC / HTTP-2 连接。
 *
 * @param {object} args
 * @param {Buffer} args.wirePayload 源帧载荷
 * @param {string} args.sourceEncoding
 * @param {object} args.decision planMessage 返回的决策
 */
export function executeDecision({ wirePayload, sourceEncoding, decision }) {
  const location = makeLocation({ phase: 'payload' });

  if (decision.action === 'reject') {
    throw new GrpcProxyError(
      decision.error?.code ?? ErrorCode.UNKNOWN_ENCODING,
      decision.error?.message ?? '决策为拒绝，无法执行转码',
      decision.error?.location ?? location
    );
  }

  if (decision.action === 'passthrough') {
    return {
      frame: encodeDataFrame(wirePayload, decision.compressedFlag),
      payload: Buffer.from(wirePayload),
      encoding: decision.targetEncoding
    };
  }

  const decoded =
    sourceEncoding === 'identity'
      ? Buffer.from(wirePayload)
      : decompress(sourceEncoding, wirePayload, location).output;

  if (decision.targetEncoding === 'identity') {
    return {
      frame: encodeDataFrame(decoded, false),
      payload: decoded,
      encoding: 'identity'
    };
  }

  const encoded = compress(decision.targetEncoding, decoded, location);
  return {
    frame: encodeDataFrame(encoded.output, true),
    payload: encoded.output,
    encoding: encoded.encoding
  };
}

/**
 * 对整条代理链做内存模拟：每跳输出新的帧 Buffer 与编码标签，
 * 供测试断言"双代理重压后接收端仍能按边界与编码正确解析"。
 */
export function simulateChain(messages, hopPlans) {
  let view = messages.map((message) => ({
    messageIndex: message.messageIndex,
    payload: message.payload,
    encoding: message.wireEncoding ?? message.headerEncoding
  }));

  const outputs = [];

  for (const hop of hopPlans) {
    const frames = [];
    const nextView = [];
    const viewByIndex = new Map(
      view.map((entry) => [entry.messageIndex, entry])
    );

    for (const decision of hop.decisions) {
      const entry = viewByIndex.get(decision.messageIndex);
      if (!entry) {
        throw new GrpcProxyError(
          decision.error?.code ?? ErrorCode.UNKNOWN_ENCODING,
          `messageIndex=${decision.messageIndex} 缺少上游字节视图，无法模拟`,
          makeLocation({ hopIndex: hop.hopIndex, messageIndex: decision.messageIndex })
        );
      }
      const result = executeDecision({
        wirePayload: entry.payload,
        sourceEncoding: entry.encoding,
        decision
      });
      frames.push(result.frame);
      nextView.push({
        messageIndex: entry.messageIndex,
        payload: result.payload,
        encoding: result.encoding
      });
    }

    outputs.push({
      hopIndex: hop.hopIndex,
      body: Buffer.concat(frames),
      view: nextView
    });
    view = nextView;
  }

  return outputs;
}

/**
 * 按 trailer 映射生成 gRPC-Web trailer 帧，便于端到端模拟。
 */
export function buildTrailerFrame(trailers) {
  const text = Object.entries(trailers)
    .map(([key, value]) => `${key}: ${value}`)
    .join('\r\n');
  return encodeGrpcWebTrailerFrame(text);
}
