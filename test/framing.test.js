import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  FrameStreamParser,
  iterateFrames,
  encodeDataFrame,
  encodeGrpcWebTrailerFrame,
  GRPC_WEB_TRAILER_FLAG
} from '../src/index.js';
import { ErrorCode } from '../src/errors.js';

function splitAcrossChunks(buffer, chunkSize) {
  const chunks = [];
  for (let offset = 0; offset < buffer.length; offset += chunkSize) {
    chunks.push(buffer.subarray(offset, offset + chunkSize));
  }
  return chunks;
}

async function collect(iterator) {
  const frames = [];
  for await (const frame of iterator) frames.push(frame);
  return frames;
}

test('跨 chunk 的帧头与载荷被正确还原边界', async () => {
  const stream = Buffer.concat([
    encodeDataFrame(Buffer.from('hello'), false),
    encodeDataFrame(Buffer.from('world!!'), true)
  ]);

  const frames = await collect(
    iterateFrames(Buffer.concat(splitAcrossChunks(stream, 3)), { mode: 'grpc' })
  );

  assert.equal(frames.length, 2);
  assert.deepEqual(frames[0].payload, Buffer.from('hello'));
  assert.equal(frames[0].compressed, false);
  assert.equal(frames[0].byteOffset, 0);
  assert.deepEqual(frames[1].payload, Buffer.from('world!!'));
  assert.equal(frames[1].compressed, true);
  assert.equal(frames[1].byteOffset, 10);
});

test('Transform 流接口逐帧输出对象', async () => {
  const parser = new FrameStreamParser({ mode: 'grpc-web' });
  parser.write(encodeDataFrame(Buffer.from('a'), false));
  parser.write(encodeGrpcWebTrailerFrame(Buffer.from('grpc-status: 0\r\n')));
  parser.end();

  const frames = await collect(parser);
  assert.equal(frames.length, 2);
  assert.equal(frames[0].kind, 'data');
  assert.equal(frames[1].kind, 'trailer');
  assert.equal(frames[1].flag, GRPC_WEB_TRAILER_FLAG);
});

test('截断帧在 flush 时报 FRAME_TRUNCATED', async () => {
  const full = encodeDataFrame(Buffer.from('abcdef'), false);
  await assert.rejects(
    collect(iterateFrames(full.subarray(0, 7), { mode: 'grpc' })),
    (error) => error.code === ErrorCode.FRAME_TRUNCATED
  );
});

test('gRPC 模式拒绝保留标志位与 0x80 trailer 帧', async () => {
  const reserved = Buffer.from([0x02, 0, 0, 0, 1, 0x61]);
  await assert.rejects(
    collect(iterateFrames(reserved, { mode: 'grpc' })),
    (error) => error.code === ErrorCode.RESERVED_FRAME_FLAG
  );

  const webTrailer = encodeGrpcWebTrailerFrame('grpc-status: 0');
  await assert.rejects(
    collect(iterateFrames(webTrailer, { mode: 'grpc' })),
    (error) => error.code === ErrorCode.TRAILER_ILLEGAL_IN_GRPC_MODE
  );
});

test('trailer 帧之后出现数据帧报 TRAILER_NOT_TERMINAL', async () => {
  const stream = Buffer.concat([
    encodeGrpcWebTrailerFrame('grpc-status: 0'),
    encodeDataFrame(Buffer.from('x'), false)
  ]);
  await assert.rejects(
    collect(iterateFrames(stream, { mode: 'grpc-web' })),
    (error) => error.code === ErrorCode.TRAILER_NOT_TERMINAL
  );
});

test('超过线上帧上限立即拒绝', async () => {
  const stream = encodeDataFrame(Buffer.alloc(10), false);
  await assert.rejects(
    collect(iterateFrames(stream, { mode: 'grpc', maxWireFrameBytes: 8 })),
    (error) => error.code === ErrorCode.WIRE_FRAME_TOO_LARGE
  );
});
