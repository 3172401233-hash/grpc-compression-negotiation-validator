/**
 * 结构化错误码。规划器和解析器只使用这里定义的错误码，
 * 调用方可以稳定地按 code 分支处理，而不是匹配错误文案。
 */
export const ErrorCode = Object.freeze({
  UNKNOWN_ENCODING: 'UNKNOWN_ENCODING',
  HEADER_MESSAGE_ENCODING_MISMATCH: 'HEADER_MESSAGE_ENCODING_MISMATCH',
  PAYLOAD_DECODE_FAILED: 'PAYLOAD_DECODE_FAILED',
  PAYLOAD_ENCODE_FAILED: 'PAYLOAD_ENCODE_FAILED',
  MESSAGE_TOO_LARGE: 'MESSAGE_TOO_LARGE',
  WIRE_FRAME_TOO_LARGE: 'WIRE_FRAME_TOO_LARGE',
  FRAME_TRUNCATED: 'FRAME_TRUNCATED',
  INVALID_FRAME_FLAG: 'INVALID_FRAME_FLAG',
  RESERVED_FRAME_FLAG: 'RESERVED_FRAME_FLAG',
  TRAILER_NOT_TERMINAL: 'TRAILER_NOT_TERMINAL',
  TRAILER_MISSING_STATUS: 'TRAILER_MISSING_STATUS',
  TRAILER_INVALID_STATUS: 'TRAILER_INVALID_STATUS',
  TRAILER_ILLEGAL_IN_GRPC_MODE: 'TRAILER_ILLEGAL_IN_GRPC_MODE',
  TRAILER_IGNORED_COMPRESSION_FLAG: 'TRAILER_IGNORED_COMPRESSION_FLAG',
  RETRY_EXHAUSTED: 'RETRY_EXHAUSTED',
  RETRY_NON_IDEMPOTENT: 'RETRY_NON_IDEMPOTENT',
  RETRY_NOT_REPLAYABLE: 'RETRY_NOT_REPLAYABLE',
  UNSUPPORTED_TRANSFORM: 'UNSUPPORTED_TRANSFORM',
  NO_COMMON_ENCODING: 'NO_COMMON_ENCODING'
});

/**
 * 所有可预期的规划/解析错误都抛这个类型。
 * location 指向字节级或消息级位置，便于定位代理链中的故障点。
 */
export class GrpcProxyError extends Error {
  constructor(code, message, location) {
    super(message);
    this.name = 'GrpcProxyError';
    this.code = code;
    this.location = location ?? {};
  }
}

/**
 * 构造标准化错误位置。
 * @param {object} parts
 * @param {number} [parts.byteOffset] 帧在字节流中的起始偏移
 * @param {number} [parts.frameIndex] 帧序号（含 trailer 帧，0 起）
 * @param {number} [parts.messageIndex] 数据消息序号（不含 trailer）
 * @param {number} [parts.hopIndex] 代理链跳数序号（0 起）
 * @param {string} [parts.phase] headers|frame|payload|trailer|negotiation
 * @param {string} [parts.detail] 人类可读的补充说明
 */
export function makeLocation({
  byteOffset,
  frameIndex,
  messageIndex,
  hopIndex,
  phase,
  detail
} = {}) {
  const location = {};
  if (hopIndex !== undefined) location.hopIndex = hopIndex;
  if (phase !== undefined) location.phase = phase;
  if (byteOffset !== undefined) location.byteOffset = byteOffset;
  if (frameIndex !== undefined) location.frameIndex = frameIndex;
  if (messageIndex !== undefined) location.messageIndex = messageIndex;
  if (detail !== undefined) location.detail = detail;
  return location;
}

/**
 * 把错误位置渲染成稳定的单行字符串，用于测试断言与日志。
 */
export function formatLocation(location = {}) {
  const parts = [];
  if (location.hopIndex !== undefined) parts.push(`hop=${location.hopIndex}`);
  if (location.phase !== undefined) parts.push(`phase=${location.phase}`);
  if (location.frameIndex !== undefined) parts.push(`frame=${location.frameIndex}`);
  if (location.messageIndex !== undefined) parts.push(`msg=${location.messageIndex}`);
  if (location.byteOffset !== undefined) parts.push(`offset=${location.byteOffset}`);
  if (location.detail) parts.push(`detail="${location.detail}"`);
  return parts.length ? parts.join(' ') : 'location=unknown';
}
