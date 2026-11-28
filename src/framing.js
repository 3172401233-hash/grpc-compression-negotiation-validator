import { ErrorCode, GrpcProxyError, makeLocation } from './errors.js';

/**
 * gRPC Length-Prefixed-Message:
 *   1 字节 Compressed-Flag + 4 字节大端 Message-Length + 载荷。
 */
export const FRAME_HEADER_LENGTH = 5;
export const FLAG_COMPRESSED = 0x01;

/**
 * gRPC-Web 文本/二进制帧类型标志的高位。
 * 0x00 数据帧、0x01 压缩数据帧、0x80 trailer 帧。
 */
export const GRPC_WEB_TRAILER_FLAG = 0x80;
export const GRPC_WEB_TRAILER_WITH_COMPRESS_BIT = 0x81;

export const DEFAULT_MAX_MESSAGE_BYTES = 4 * 1024 * 1024;

/**
 * 把多种 header 表示统一成小写键的普通对象。
 * 支持普通对象、Map、以及 [key, value] 数组。
 */
export function normalizeHeaders(input) {
  const result = {};
  if (input == null) return result;
  if (input instanceof Map) {
    for (const [key, value] of input.entries()) {
      result[String(key).toLowerCase()] = String(value);
    }
    return result;
  }
  if (Array.isArray(input)) {
    for (const entry of input) {
      result[String(entry[0]).toLowerCase()] = String(entry[1]);
    }
    return result;
  }
  for (const [key, value] of Object.entries(input)) {
    result[String(key).toLowerCase()] = String(value);
  }
  return result;
}

/**
 * 解析 grpc-accept-encoding 的逗号分隔值，去空格、转小写、去重保序。
 */
export function parseAcceptEncoding(headerValue) {
  if (headerValue === undefined || headerValue === null) return [];
  const seen = new Set();
  const ordered = [];
  for (const raw of String(headerValue).split(',')) {
    const token = raw.trim().toLowerCase();
    if (token && !seen.has(token)) {
      seen.add(token);
      ordered.push(token);
    }
  }
  return ordered;
}

export function isTrailerFlag(flag, mode) {
  return mode === 'grpc-web' && (flag & GRPC_WEB_TRAILER_FLAG) !== 0;
}

/**
 * 校验帧标志位。gRPC 只允许 0x00/0x01；gRPC-Web 允许 0x00/0x01/0x80，
 * trailer 标志上的压缩位被忽略（这是题目强调的语义），其它保留位报错。
 */
export function validateFrameFlag(flag, mode, location) {
  if (mode === 'grpc-web') {
    if (flag === 0x00 || flag === 0x01) return;
    if (flag === GRPC_WEB_TRAILER_FLAG) return;
    // 0x81 = trailer 位 + Compressed-Flag。帧层放行，
    // trailer 的压缩语义错误交给 validateTrailers 显式报告：
    // trailer 永远是明文，Compressed-Flag 不作用于它。
    if (flag === GRPC_WEB_TRAILER_WITH_COMPRESS_BIT) return;
    throw new GrpcProxyError(
      ErrorCode.INVALID_FRAME_FLAG,
      `gRPC-Web 非法帧标志 0x${flag.toString(16).padStart(2, '0')}`,
      makeLocation({ ...location, detail: 'allowed flags: 0x00 0x01 0x80 0x81' })
    );
  }
  if (flag === 0x00 || flag === 0x01) return;
  if ((flag & GRPC_WEB_TRAILER_FLAG) !== 0) {
    throw new GrpcProxyError(
      ErrorCode.TRAILER_ILLEGAL_IN_GRPC_MODE,
      '原生 gRPC 模式下 0x80 trailer 帧非法，trailer 必须走 HTTP/2 HEADERS 帧',
      location
    );
  }
  throw new GrpcProxyError(
    ErrorCode.RESERVED_FRAME_FLAG,
    `gRPC 帧标志保留位被置位: 0x${flag.toString(16).padStart(2, '0')}`,
    makeLocation({ ...location, detail: 'only Compressed-Flag (LSB) is defined' })
  );
}

/**
 * 编码一个数据帧。
 * @param {Buffer} payload 已是对应编码的字节
 * @param {boolean} compressed 是否置 Compressed-Flag
 */
export function encodeDataFrame(payload, compressed) {
  const frame = Buffer.alloc(FRAME_HEADER_LENGTH + payload.length);
  frame[0] = compressed ? FLAG_COMPRESSED : 0x00;
  frame.writeUInt32BE(payload.length, 1);
  payload.copy(frame, FRAME_HEADER_LENGTH);
  return frame;
}

export function encodeGrpcWebTrailerFrame(trailerText) {
  const payload = Buffer.from(trailerText, 'utf8');
  const frame = Buffer.alloc(FRAME_HEADER_LENGTH + payload.length);
  frame[0] = GRPC_WEB_TRAILER_FLAG;
  frame.writeUInt32BE(payload.length, 1);
  payload.copy(frame, FRAME_HEADER_LENGTH);
  return frame;
}

/**
 * 解析 HTTP/2 风格的 trailer 文本（"k: v\r\n"）。
 */
export function parseTrailerText(buffer) {
  const trailers = {};
  const text = buffer.toString('utf8').trim();
  if (!text) return trailers;
  for (const line of text.split(/\r?\n/)) {
    const separator = line.indexOf(':');
    if (separator === -1) continue;
    const key = line.slice(0, separator).trim().toLowerCase();
    const value = line.slice(separator + 1).trim();
    if (key) trailers[key] = value;
  }
  return trailers;
}

/**
 * 校验 trailer。gRPC 规定 grpc-status 必须存在且为 0..99 的整数；
 * gRPC-Web 的 trailer 不携带压缩语义，0x80 上的压缩位永远被忽略。
 */
export function validateTrailers(trailers, { mode, flag, byteOffset, frameIndex, hopIndex }) {
  const baseLocation = makeLocation({
    hopIndex,
    byteOffset,
    frameIndex,
    phase: 'trailer'
  });
  if (mode === 'grpc-web' && flag !== undefined && (flag & FLAG_COMPRESSED) !== 0) {
    return {
      ok: false,
      error: new GrpcProxyError(
        ErrorCode.TRAILER_IGNORED_COMPRESSION_FLAG,
        'trailer 帧 0x80 上的 Compressed-Flag 不控制压缩；收到带压缩位的 trailer 帧',
        makeLocation({ ...baseLocation, detail: 'trailer payload is always plaintext' })
      ),
      trailers
    };
  }
  if (!Object.hasOwn(trailers, 'grpc-status')) {
    return {
      ok: false,
      error: new GrpcProxyError(
        ErrorCode.TRAILER_MISSING_STATUS,
        'trailer 缺少强制字段 grpc-status',
        baseLocation
      ),
      trailers
    };
  }
  const statusText = trailers['grpc-status'];
  const status = Number(statusText);
  if (!Number.isInteger(status) || status < 0 || status > 99) {
    return {
      ok: false,
      error: new GrpcProxyError(
        ErrorCode.TRAILER_INVALID_STATUS,
        `非法 grpc-status: ${statusText}（要求 0..99 的整数）`,
        makeLocation({ ...baseLocation, detail: `grpc-status=${statusText}` })
      ),
      trailers
    };
  }
  return { ok: true, status, trailers };
}
