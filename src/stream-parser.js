import { Transform, Readable } from 'node:stream';
import {
  FRAME_HEADER_LENGTH,
  FLAG_COMPRESSED,
  GRPC_WEB_TRAILER_FLAG,
  validateFrameFlag,
  isTrailerFlag
} from './framing.js';
import { ErrorCode, GrpcProxyError, makeLocation } from './errors.js';

/**
 * 帧级流解析器。
 *
 * 输入：任意分片的 gRPC / gRPC-Web 字节流（objectMode 输出）。
 * 输出：逐个帧对象
 *   {
 *     kind: 'data' | 'trailer',
 *     flag, compressed, payload: Buffer,
 *     byteOffset, frameIndex, length
 *   }
 *
 * 解析器只负责边界与标志，不做任何解压：
 * trailer 帧（gRPC-Web 0x80）的 payload 永远是明文，
 * Compressed-Flag 不会作用于它，这一语义在 parseMessages 中进一步校验。
 */
export class FrameStreamParser extends Transform {
  #buffer = Buffer.alloc(0);
  #frameIndex = 0;
  #streamOffset = 0;
  #mode;
  #maxWireFrameBytes;
  #sawTrailer = false;

  constructor({ mode = 'grpc', maxWireFrameBytes } = {}) {
    super({ readableObjectMode: true });
    if (mode !== 'grpc' && mode !== 'grpc-web') {
      throw new TypeError(`mode 必须是 grpc 或 grpc-web，收到: ${mode}`);
    }
    this.#mode = mode;
    this.#maxWireFrameBytes = maxWireFrameBytes;
  }

  get mode() {
    return this.#mode;
  }

  _transform(chunk, _encoding, callback) {
    try {
      this.#buffer = this.#buffer.length
        ? Buffer.concat([this.#buffer, chunk])
        : Buffer.from(chunk);
      this.#drainFrames();
      callback();
    } catch (error) {
      callback(error);
    }
  }

  _flush(callback) {
    if (this.#buffer.length > 0) {
      callback(
        new GrpcProxyError(
          ErrorCode.FRAME_TRUNCATED,
          `流结束时仍有 ${this.#buffer.length} 个字节不足一个完整帧`,
          makeLocation({
            phase: 'frame',
            frameIndex: this.#frameIndex,
            detail: 'dangling frame header/payload'
          })
        )
      );
      return;
    }
    callback();
  }

  #drainFrames() {
    while (this.#buffer.length >= FRAME_HEADER_LENGTH) {
      const byteOffset = this.#streamOffset;
      const flag = this.#buffer[0];
      const length = this.#buffer.readUInt32BE(1);

      validateFrameFlag(
        flag,
        this.#mode,
        makeLocation({ phase: 'frame', frameIndex: this.#frameIndex, byteOffset })
      );

      if (
        this.#maxWireFrameBytes !== undefined &&
        length > this.#maxWireFrameBytes
      ) {
        throw new GrpcProxyError(
          ErrorCode.WIRE_FRAME_TOO_LARGE,
          `线上帧长度 ${length} 超过上限 ${this.#maxWireFrameBytes}`,
          makeLocation({
            phase: 'frame',
            frameIndex: this.#frameIndex,
            byteOffset,
            detail: `declared length=${length}`
          })
        );
      }

      if (this.#buffer.length < FRAME_HEADER_LENGTH + length) {
        // 完整载荷尚未到达，保留缓冲等待后续 chunk。
        return;
      }

      const payload = this.#buffer.subarray(
        FRAME_HEADER_LENGTH,
        FRAME_HEADER_LENGTH + length
      );
      this.#buffer = this.#buffer.subarray(FRAME_HEADER_LENGTH + length);
      this.#streamOffset += FRAME_HEADER_LENGTH + length;

      const kind = isTrailerFlag(flag, this.#mode) ? 'trailer' : 'data';
      if (kind === 'trailer') {
        if (this.#sawTrailer) {
          throw new GrpcProxyError(
            ErrorCode.TRAILER_NOT_TERMINAL,
            'gRPC-Web 流中出现多个 trailer 帧',
            makeLocation({ phase: 'trailer', frameIndex: this.#frameIndex, byteOffset })
          );
        }
        this.#sawTrailer = true;
      } else if (this.#sawTrailer) {
        throw new GrpcProxyError(
          ErrorCode.TRAILER_NOT_TERMINAL,
          'trailer 帧之后不允许再出现数据帧，trailer 必须是流的最后一帧',
          makeLocation({ phase: 'frame', frameIndex: this.#frameIndex, byteOffset })
        );
      }

      this.push({
        kind,
        flag,
        compressed: (flag & FLAG_COMPRESSED) !== 0 && kind === 'data',
        trailerBitSet: (flag & GRPC_WEB_TRAILER_FLAG) !== 0,
        length,
        payload: Buffer.from(payload),
        byteOffset,
        frameIndex: this.#frameIndex
      });
      this.#frameIndex += 1;
    }
  }
}

/**
 * 把 Buffer 或可读流统一成帧异步迭代器。
 * 上游错误会透传为迭代器抛出的 GrpcProxyError / 原始错误。
 */
export async function* iterateFrames(source, options = {}) {
  const parser = new FrameStreamParser(options);
  const readable = Buffer.isBuffer(source) ? Readable.from([source]) : source;
  readable.on('error', (error) => parser.destroy(error));
  const framed = readable.pipe(parser);
  yield* framed;
}
