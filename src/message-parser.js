import { iterateFrames } from './stream-parser.js';
import { decompress } from './codecs.js';
import {
  normalizeHeaders,
  parseTrailerText,
  validateTrailers,
  DEFAULT_MAX_MESSAGE_BYTES
} from './framing.js';
import { ErrorCode, GrpcProxyError, makeLocation } from './errors.js';

/**
 * 把一个 gRPC/gRPC-Web 字节流解析成结构化消息序列。
 *
 * 这是离线解析：函数只消费内存字节，不建立任何网络连接。
 *
 * @param {object} input
 * @param {Buffer} input.body 线上帧字节
 * @param {object|Map|Array} input.headers HTTP/2 响应头（需含 grpc-encoding）
 * @param {'grpc'|'grpc-web'} [input.mode='grpc']
 * @param {number} [input.maxMessageBytes=4MiB] 解压后单条消息上限
 * @param {number} [input.maxWireFrameBytes] 线上单帧上限（可选）
 * @param {object} [input.externalTrailers] 原生 gRPC 的 HTTP/2 trailer HEADERS
 * @returns {Promise<object>} messages / trailer / errors / 统计信息
 */
export async function parseMessages({
  body,
  headers,
  mode = 'grpc',
  maxMessageBytes = DEFAULT_MAX_MESSAGE_BYTES,
  maxWireFrameBytes,
  externalTrailers
}) {
  const normalizedHeaders = normalizeHeaders(headers);
  const declaredEncoding = normalizedHeaders['grpc-encoding'];

  const headerLocation = makeLocation({ phase: 'headers' });
  let headerEncoding = null;
  if (declaredEncoding === undefined || declaredEncoding === '') {
    headerEncoding = null;
  } else {
    headerEncoding = declaredEncoding.toLowerCase();
  }

  const messages = [];
  const errors = [];
  let trailer = null;
  let dataAfterTrailer = false;
  let frameCount = 0;

  const pushError = (error) => {
    errors.push({
      code: error.code,
      message: error.message,
      location: error.location
    });
  };

  try {
    for await (const frame of iterateFrames(body, { mode, maxWireFrameBytes })) {
      frameCount += 1;
      if (frame.kind === 'trailer') {
        const result = validateTrailers(parseTrailerText(frame.payload), {
          mode,
          flag: frame.flag,
          byteOffset: frame.byteOffset,
          frameIndex: frame.frameIndex
        });
        trailer = {
          transport: 'grpc-web-frame',
          frameIndex: frame.frameIndex,
          byteOffset: frame.byteOffset,
          trailers: result.trailers,
          status: result.ok ? result.status : undefined
        };
        if (!result.ok) {
          trailer.error = {
            code: result.error.code,
            message: result.error.message,
            location: result.error.location
          };
          pushError(result.error);
        }
        continue;
      }

      if (trailer) {
        dataAfterTrailer = true;
        pushError(
          new GrpcProxyError(
            ErrorCode.TRAILER_NOT_TERMINAL,
            'trailer 之后又收到数据帧，违反 gRPC-Web 终态约定',
            makeLocation({
              phase: 'frame',
              frameIndex: frame.frameIndex,
              byteOffset: frame.byteOffset
            })
          )
        );
      }

      const messageIndex = messages.length;
      const frameLocation = makeLocation({
        phase: 'frame',
        frameIndex: frame.frameIndex,
        messageIndex,
        byteOffset: frame.byteOffset
      });

      const record = {
        kind: 'message',
        messageIndex,
        frameIndex: frame.frameIndex,
        byteOffset: frame.byteOffset,
        compressedFlag: frame.compressed,
        headerEncoding,
        wireEncoding: null,
        resolvedEncoding: null,
        wireSize: frame.length,
        decodedSize: null,
        payload: frame.payload,
        decoded: null,
        error: null
      };

      // 严格协商：header 缺 grpc-encoding 时不允许隐式 identity。
      if (headerEncoding === null) {
        record.error = {
          code: ErrorCode.UNKNOWN_ENCODING,
          message: '缺少 grpc-encoding 头，严格模式下不得默认 identity',
          location: makeLocation({ ...headerLocation, messageIndex })
        };
        pushError(new GrpcProxyError(record.error.code, record.error.message, record.error.location));
        messages.push(record);
        continue;
      }

      // Compressed-Flag=1 却声明 identity：头与消息互相矛盾，拒绝。
      if (frame.compressed && headerEncoding === 'identity') {
        record.error = {
          code: ErrorCode.HEADER_MESSAGE_ENCODING_MISMATCH,
          message:
            'Compressed-Flag=1 但 grpc-encoding=identity，header 与消息压缩状态不一致',
          location: makeLocation({ ...frameLocation, detail: 'flag says compressed, header says identity' })
        };
        pushError(new GrpcProxyError(record.error.code, record.error.message, record.error.location));
        messages.push(record);
        continue;
      }

      // flag=0 表示线上即明文；header 声明非 identity 时仅表示"该帧未压缩"，
      // 实际线上编码按 flag 解析为 identity，并记录与 header 的差异。
      record.wireEncoding = frame.compressed ? headerEncoding : 'identity';
      record.resolvedEncoding = record.wireEncoding;

      let decoded;
      try {
        decoded = decompress(record.wireEncoding, frame.payload, frameLocation).output;
      } catch (error) {
        record.error = {
          code: error.code,
          message: error.message,
          location: error.location
        };
        pushError(error);
        messages.push(record);
        continue;
      }

      record.decoded = decoded;
      record.decodedSize = decoded.length;

      if (decoded.length > maxMessageBytes) {
        const error = new GrpcProxyError(
          ErrorCode.MESSAGE_TOO_LARGE,
          `解压后消息 ${decoded.length} 字节超过上限 ${maxMessageBytes}`,
          makeLocation({
            ...frameLocation,
            phase: 'payload',
            detail: `decoded=${decoded.length} limit=${maxMessageBytes}`
          })
        );
        record.error = {
          code: error.code,
          message: error.message,
          location: error.location
        };
        pushError(error);
      }

      messages.push(record);
    }
  } catch (error) {
    // 帧级致命错误（截断、非法标志、线上超限等）：结构化后终止本流解析。
    errors.push({
      code: error.code ?? 'FRAME_FATAL',
      message: error.message,
      location: error.location ?? makeLocation({ phase: 'frame' })
    });
  }

  // 原生 gRPC：trailer 走 HTTP/2 HEADERS，不允许出现在 DATA 帧里（已在帧层拦截）。
  if (mode === 'grpc' && externalTrailers) {
    const normalized = normalizeHeaders(externalTrailers);
    const result = validateTrailers(normalized, { mode });
    trailer = {
      transport: 'http2-headers',
      trailers: result.trailers,
      status: result.ok ? result.status : undefined
    };
    if (!result.ok) {
      trailer.error = {
        code: result.error.code,
        message: result.error.message,
        location: result.error.location
      };
      errors.push(trailer.error);
    }
  }

  return {
    mode,
    headerEncoding,
    messages,
    trailer,
    errors,
    stats: {
      frameCount,
      messageCount: messages.length,
      dataAfterTrailer,
      ok: errors.length === 0
    }
  };
}
