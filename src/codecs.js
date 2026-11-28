import {
  gzipSync,
  gunzipSync,
  deflateSync,
  inflateSync,
  constants
} from 'node:zlib';
import { ErrorCode, GrpcProxyError, makeLocation } from './errors.js';

/**
 * gRPC 规范内置的三种算法。
 * - identity: 不压缩
 * - gzip:     RFC 1952
 * - deflate:  gRPC 使用裸 DEFLATE（RFC 1951），不是 zlib 容器
 *
 * 注意：没有在注册表中的算法一律视为未知，规划器不会回退到 identity。
 */
export const SUPPORTED_ENCODINGS = Object.freeze(['identity', 'gzip', 'deflate']);

const CODECS = Object.freeze({
  identity: {
    name: 'identity',
    compress: (buffer) => Buffer.from(buffer),
    decompress: (buffer) => Buffer.from(buffer)
  },
  gzip: {
    name: 'gzip',
    compress: (buffer) => gzipSync(buffer),
    decompress: (buffer) => gunzipSync(buffer)
  },
  deflate: {
    name: 'deflate',
    compress: (buffer) =>
      deflateSync(buffer, { level: constants.Z_DEFAULT_COMPRESSION }),
    decompress: (buffer) => inflateSync(buffer, { finishFlush: constants.Z_SYNC_FLUSH })
  }
});

export function isKnownEncoding(encoding) {
  return Object.hasOwn(CODECS, encoding);
}

/**
 * 解析编码标签。空字符串/缺省在这里直接报错，
 * 因为本工具采用严格协商模式，不允许隐式 identity。
 */
export function requireKnownEncoding(encoding, location) {
  if (typeof encoding !== 'string' || encoding.length === 0) {
    throw new GrpcProxyError(
      ErrorCode.UNKNOWN_ENCODING,
      `缺失或非字符串的 message encoding: ${String(encoding)}`,
      makeLocation({ ...location, detail: 'encoding header absent' })
    );
  }
  const normalized = encoding.toLowerCase();
  if (!isKnownEncoding(normalized)) {
    throw new GrpcProxyError(
      ErrorCode.UNKNOWN_ENCODING,
      `未知压缩算法: ${encoding}（支持: ${SUPPORTED_ENCODINGS.join(', ')}），拒绝并停止，不回退 identity`,
      makeLocation({ ...location, detail: `encoding=${encoding}` })
    );
  }
  return normalized;
}

export function getCodec(encoding, location) {
  const normalized = requireKnownEncoding(encoding, location);
  return { encoding: normalized, codec: CODECS[normalized] };
}

/**
 * 同步压缩。失败转成结构化错误，避免 zlib 原始异常穿透规划器。
 */
export function compress(encoding, buffer, location) {
  const { encoding: normalized, codec } = getCodec(encoding, location);
  try {
    return { encoding: normalized, output: codec.compress(buffer) };
  } catch (cause) {
    throw new GrpcProxyError(
      ErrorCode.PAYLOAD_ENCODE_FAILED,
      `重压为 ${normalized} 失败: ${cause.message}`,
      makeLocation({ ...location, detail: `target=${normalized}` })
    );
  }
}

/**
 * 同步解压。identity 原样返回，其它算法走 zlib。
 */
export function decompress(encoding, buffer, location) {
  const { encoding: normalized, codec } = getCodec(encoding, location);
  try {
    return { encoding: normalized, output: codec.decompress(buffer) };
  } catch (cause) {
    throw new GrpcProxyError(
      ErrorCode.PAYLOAD_DECODE_FAILED,
      `使用 ${normalized} 解压载荷失败: ${cause.message}`,
      makeLocation({ ...location, detail: `source=${normalized}` })
    );
  }
}
