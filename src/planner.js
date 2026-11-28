import { compress, decompress, isKnownEncoding, SUPPORTED_ENCODINGS } from './codecs.js';
import { parseAcceptEncoding } from './framing.js';
import { ErrorCode, GrpcProxyError, makeLocation } from './errors.js';

/**
 * 协商一跳的目标编码。
 *
 * 严格规则：
 * - acceptEncoding 为空 => 无共同算法，返回 NO_COMMON_ENCODING（不默认 identity）；
 * - accept 列表中出现未知算法 => 显式 UNKNOWN_ENCODING；
 * - 按 accept 声明顺序选第一个本地支持的算法。
 *
 * @returns {{encoding: string, index: number}|{error: GrpcProxyError}}
 */
export function negotiateEncoding(acceptEncoding, location = {}) {
  const accepted = parseAcceptEncoding(acceptEncoding);
  if (accepted.length === 0) {
    return {
      error: new GrpcProxyError(
        ErrorCode.NO_COMMON_ENCODING,
        'grpc-accept-encoding 为空或缺失，无法协商任何算法（严格模式不默认 identity）',
        makeLocation({ ...location, phase: 'negotiation' })
      )
    };
  }

  for (const token of accepted) {
    if (!isKnownEncoding(token)) {
      return {
        error: new GrpcProxyError(
          ErrorCode.UNKNOWN_ENCODING,
          `对端声明了未知算法 "${token}"，拒绝协商，不回退 identity`,
          makeLocation({ ...location, phase: 'negotiation', detail: `accept-encoding=${token}` })
        )
      };
    }
  }

  const index = accepted.findIndex((token) => SUPPORTED_ENCODINGS.includes(token));
  if (index === -1) {
    return {
      error: new GrpcProxyError(
        ErrorCode.NO_COMMON_ENCODING,
        `无共同算法：对端只接受 ${accepted.join(', ')}，本地支持 ${SUPPORTED_ENCODINGS.join(', ')}`,
        makeLocation({ ...location, phase: 'negotiation' })
      )
    };
  }
  return { encoding: accepted[index], index };
}

/**
 * 决定一条消息在一跳代理上的处理动作。
 *
 * @param {object} args
 * @param {Buffer} args.wirePayload 线上原始字节（已按帧边界切出）
 * @param {string} args.sourceEncoding 该消息线上实际编码（由解析器还原）
 * @param {boolean} args.compressedFlag 源帧 Compressed-Flag
 * @param {string} args.acceptEncoding 下一跳 grpc-accept-encoding
 * @param {string} [args.targetEncoding] 强制目标编码（覆盖协商，需在 accept 内）
 * @param {number} args.maxMessageBytes 解压后上限
 * @param {number} args.maxWireFrameBytes 重压后线上上限
 * @param {object} [args.location]
 * @param {string[]} [args.fallbackProtocols] 该跳声明的可回退协议
 * @returns {object} 决策记录
 */
export function planMessage({
  wirePayload,
  sourceEncoding,
  compressedFlag,
  acceptEncoding,
  targetEncoding,
  maxMessageBytes,
  maxWireFrameBytes,
  location = {},
  fallbackProtocols = []
}) {
  const base = makeLocation({ ...location, phase: 'payload' });
  const accepted = parseAcceptEncoding(acceptEncoding);
  const messageIndex = base.messageIndex;

  const buildReject = (error) => ({
    messageIndex,
    action: 'reject',
    sourceEncoding,
    targetEncoding: null,
    compressedFlag: false,
    inputWireSize: wirePayload.length,
    outputWireSize: null,
    reencodeCpuCost: 0,
    sizeDelta: 0,
    fallbackProtocol: pickFallback(fallbackProtocols, error.code),
    error: { code: error.code, message: error.message, location: error.location }
  });

  // 1. 源编码必须已知，未知立即拒绝。
  if (!isKnownEncoding(sourceEncoding)) {
    return buildReject(
      new GrpcProxyError(
        ErrorCode.UNKNOWN_ENCODING,
        `源消息使用未知算法 ${sourceEncoding}，拒绝透传与重压`,
        makeLocation({ ...base, detail: `source=${sourceEncoding}` })
      )
    );
  }

  // 2. header/标志不一致：identity 却带压缩位。
  if (sourceEncoding === 'identity' && compressedFlag) {
    return buildReject(
      new GrpcProxyError(
        ErrorCode.HEADER_MESSAGE_ENCODING_MISMATCH,
        'Compressed-Flag=1 但源编码为 identity，header 与消息状态矛盾',
        base
      )
    );
  }

  // 3. 协商目标编码。
  let chosen;
  if (targetEncoding !== undefined) {
    const forced = targetEncoding.toLowerCase();
    if (!isKnownEncoding(forced)) {
      return buildReject(
        new GrpcProxyError(
          ErrorCode.UNKNOWN_ENCODING,
          `强制目标编码未知: ${targetEncoding}`,
          makeLocation({ ...base, detail: `forced=${targetEncoding}` })
        )
      );
    }
    if (!accepted.includes(forced)) {
      return buildReject(
        new GrpcProxyError(
          ErrorCode.NO_COMMON_ENCODING,
          `强制目标编码 ${forced} 不在下一跳 accept-encoding(${accepted.join(', ') || '空'}) 内`,
          makeLocation({ ...base, phase: 'negotiation' })
        )
      );
    }
    chosen = forced;
  } else {
    const negotiation = negotiateEncoding(acceptEncoding, base);
    if (negotiation.error) return buildReject(negotiation.error);
    chosen = negotiation.encoding;
  }

  // 4. 透传：编码相同且都带正确的压缩状态，零拷贝零开销。
  if (chosen === sourceEncoding) {
    const passthroughFlag = sourceEncoding === 'identity' ? false : compressedFlag;
    if (sourceEncoding !== 'identity' && !compressedFlag) {
      // flag=0 的帧线上其实是 identity，不应走到 sourceEncoding 非 identity 的分支，
      // 这里做防御性一致性校验。
      return buildReject(
        new GrpcProxyError(
          ErrorCode.HEADER_MESSAGE_ENCODING_MISMATCH,
          `源编码 ${sourceEncoding} 与 Compressed-Flag=0 不一致`,
          base
        )
      );
    }
    return {
      messageIndex,
      action: 'passthrough',
      sourceEncoding,
      targetEncoding: chosen,
      compressedFlag: passthroughFlag,
      inputWireSize: wirePayload.length,
      outputWireSize: wirePayload.length,
      reencodeCpuCost: 0,
      sizeDelta: 0,
      fallbackProtocol: pickFallback(fallbackProtocols, null),
      error: null
    };
  }

  // 5. 需要解压（任何转码都先还原明文，并检查解压后大小）。
  let decoded;
  try {
    decoded =
      sourceEncoding === 'identity'
        ? Buffer.from(wirePayload)
        : decompress(sourceEncoding, wirePayload, base).output;
  } catch (error) {
    return buildReject(error);
  }

  if (decoded.length > maxMessageBytes) {
    return buildReject(
      new GrpcProxyError(
        ErrorCode.MESSAGE_TOO_LARGE,
        `解压后消息 ${decoded.length} 字节超过上限 ${maxMessageBytes}`,
        makeLocation({ ...base, detail: `decoded=${decoded.length} limit=${maxMessageBytes}` })
      )
    );
  }

  // 6. 目标 identity => 只解压；否则重压并检查线上帧上限。
  if (chosen === 'identity') {
    return {
      messageIndex,
      action: 'decompress',
      sourceEncoding,
      targetEncoding: 'identity',
      compressedFlag: false,
      inputWireSize: wirePayload.length,
      outputWireSize: decoded.length,
      reencodeCpuCost: estimateCost(sourceEncoding, 'identity', wirePayload.length, decoded.length),
      sizeDelta: decoded.length - wirePayload.length,
      fallbackProtocol: pickFallback(fallbackProtocols, null),
      error: null
    };
  }

  let encoded;
  try {
    encoded = compress(chosen, decoded, base).output;
  } catch (error) {
    return buildReject(error);
  }

  if (encoded.length > maxWireFrameBytes) {
    return buildReject(
      new GrpcProxyError(
        ErrorCode.MESSAGE_TOO_LARGE,
        `重压为 ${chosen} 后线上帧 ${encoded.length} 字节超过上限 ${maxWireFrameBytes}`,
        makeLocation({
          ...base,
          detail: `compressed=${encoded.length} wireLimit=${maxWireFrameBytes}`
        })
      )
    );
  }

  return {
    messageIndex,
    action: 'recompress',
    sourceEncoding,
    targetEncoding: chosen,
    compressedFlag: true,
    inputWireSize: wirePayload.length,
    outputWireSize: encoded.length,
    reencodeCpuCost: estimateCost(sourceEncoding, chosen, wirePayload.length, encoded.length),
    sizeDelta: encoded.length - wirePayload.length,
    fallbackProtocol: pickFallback(fallbackProtocols, null),
    error: null
  };
}

/**
 * 重编码开销模型（确定性、可测试的相对单位）：
 * - 解压按算法权重 × 输入字节计；
 * - 压缩按算法权重 × 明文字节计；
 * 透传恒为 0。仅用于规划对比，不代表真实 CPU 周期。
 */
const CODEC_WEIGHT = Object.freeze({
  identity: 0,
  gzip: 3,
  deflate: 2
});

function estimateCost(fromEncoding, toEncoding, inputWireSize, outputWireSize) {
  const decompressCost = CODEC_WEIGHT[fromEncoding] * inputWireSize;
  const compressCost = CODEC_WEIGHT[toEncoding] * outputWireSize;
  return decompressCost + compressCost;
}

/**
 * 选择回退协议。只有显式声明的协议可回退；
 * 发生拒绝时返回列表中的第一个协议，绝不静默降级到 identity。
 */
function pickFallback(fallbackProtocols, errorCode) {
  const list = Array.isArray(fallbackProtocols) ? fallbackProtocols : [];
  if (list.length === 0) {
    return { available: false, protocol: null, reason: errorCode ?? 'NO_FALLBACK_DECLARED' };
  }
  return { available: true, protocol: list[0], reason: errorCode ?? 'PLANNED' };
}
