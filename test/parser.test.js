import { test } from 'node:test';
import assert from 'node:assert/strict';
import { gzipSync, deflateSync } from 'node:zlib';
import {
  parseMessages,
  encodeDataFrame,
  encodeGrpcWebTrailerFrame
} from '../src/index.js';
import { ErrorCode } from '../src/errors.js';

test('还原每条消息的编码、边界与解压后大小', async () => {
  const plain = Buffer.from('identity-message');
  const gz = gzipSync(Buffer.from('gzip-message'));
  const body = Buffer.concat([
    encodeDataFrame(plain, false),
    encodeDataFrame(gz, true)
  ]);

  const result = await parseMessages({
    body,
    headers: { 'grpc-encoding': 'gzip' },
    mode: 'grpc'
  });

  assert.equal(result.stats.ok, true);
  assert.equal(result.messages.length, 2);
  assert.equal(result.messages[0].wireEncoding, 'identity');
  assert.equal(result.messages[0].resolvedEncoding, 'identity');
  assert.equal(result.messages[0].decodedSize, plain.length);
  assert.equal(result.messages[1].wireEncoding, 'gzip');
  assert.equal(result.messages[1].decoded.toString(), 'gzip-message');
});

test('deflate 帧按 header 声明正确还原', async () => {
  const raw = deflateSync(Buffer.from('deflate-message'));
  const result = await parseMessages({
    body: encodeDataFrame(raw, true),
    headers: { 'grpc-encoding': 'deflate' }
  });
  assert.equal(result.stats.ok, true);
  assert.equal(result.messages[0].decoded.toString(), 'deflate-message');
});

test('header 声明 gzip 但载荷是 deflate 字节时解压失败并定位消息', async () => {
  const raw = deflateSync(Buffer.from('mismatch'));
  const body = encodeDataFrame(raw, true);
  const result = await parseMessages({
    body,
    headers: { 'grpc-encoding': 'gzip' }
  });

  assert.equal(result.messages[0].error.code, ErrorCode.PAYLOAD_DECODE_FAILED);
  assert.equal(result.messages[0].error.location.messageIndex, 0);
  assert.equal(result.stats.ok, false);
});

test('Compressed-Flag=1 与 identity header 矛盾时报 HEADER_MESSAGE_ENCODING_MISMATCH', async () => {
  const body = encodeDataFrame(gzipSync(Buffer.from('x')), true);
  const result = await parseMessages({
    body,
    headers: { 'grpc-encoding': 'identity' }
  });

  assert.equal(
    result.messages[0].error.code,
    ErrorCode.HEADER_MESSAGE_ENCODING_MISMATCH
  );
  assert.equal(result.errors[0].location.messageIndex, 0);
});

test('缺少 grpc-encoding 不默认 identity，报 UNKNOWN_ENCODING', async () => {
  const body = encodeDataFrame(Buffer.from('no-header'), false);
  const result = await parseMessages({ body, headers: {} });

  assert.equal(result.messages[0].error.code, ErrorCode.UNKNOWN_ENCODING);
  assert.equal(result.headerEncoding, null);
});

test('未知算法在解析阶段被拒绝', async () => {
  const body = encodeDataFrame(Buffer.from('z'), true);
  const result = await parseMessages({
    body,
    headers: { 'grpc-encoding': 'br' }
  });

  assert.equal(result.errors[0].code, ErrorCode.UNKNOWN_ENCODING);
  assert.match(result.errors[0].message, /br/);
});

test('解压后超过 maxMessageBytes 报 MESSAGE_TOO_LARGE 且带字节位置', async () => {
  const payload = Buffer.alloc(64, 0x61);
  const body = encodeDataFrame(gzipSync(payload), true);
  const result = await parseMessages({
    body,
    headers: { 'grpc-encoding': 'gzip' },
    maxMessageBytes: 32
  });

  assert.equal(result.messages[0].error.code, ErrorCode.MESSAGE_TOO_LARGE);
  assert.equal(result.messages[0].error.location.byteOffset, 0);
  assert.match(result.messages[0].error.message, /64/);
});

test('gRPC-Web trailer 帧被识别且不受压缩位控制', async () => {
  const trailer = encodeGrpcWebTrailerFrame('grpc-status: 0\r\ngrpc-message: ok');
  const result = await parseMessages({
    body: trailer,
    headers: { 'grpc-encoding': 'identity' },
    mode: 'grpc-web'
  });

  assert.equal(result.trailer.status, 0);
  assert.equal(result.trailer.transport, 'grpc-web-frame');
  assert.equal(result.trailer.trailers['grpc-message'], 'ok');
  assert.equal(result.stats.ok, true);
});

test('trailer 缺少 grpc-status 报 TRAILER_MISSING_STATUS', async () => {
  const trailer = encodeGrpcWebTrailerFrame('grpc-message: no-status');
  const result = await parseMessages({
    body: trailer,
    headers: { 'grpc-encoding': 'identity' },
    mode: 'grpc-web'
  });

  assert.equal(result.trailer.error.code, ErrorCode.TRAILER_MISSING_STATUS);
  assert.equal(result.stats.ok, false);
});

test('trailer 帧上的压缩位（0x81）不控制压缩并被显式拒绝', async () => {
  const payload = Buffer.from('grpc-status: 0');
  const frame = Buffer.alloc(5 + payload.length);
  frame[0] = 0x81; // gRPC-Web trailer 位 + Compressed-Flag，trailer 永远是明文
  frame.writeUInt32BE(payload.length, 1);
  payload.copy(frame, 5);

  const result = await parseMessages({
    body: frame,
    headers: { 'grpc-encoding': 'gzip' },
    mode: 'grpc-web'
  });

  assert.equal(
    result.trailer.error.code,
    ErrorCode.TRAILER_IGNORED_COMPRESSION_FLAG
  );
  assert.equal(result.trailer.trailers['grpc-status'], '0');
});

test('trailer grpc-status 非法值报 TRAILER_INVALID_STATUS', async () => {
  const trailer = encodeGrpcWebTrailerFrame('grpc-status: 200');
  const result = await parseMessages({
    body: trailer,
    headers: { 'grpc-encoding': 'identity' },
    mode: 'grpc-web'
  });

  assert.equal(result.trailer.error.code, ErrorCode.TRAILER_INVALID_STATUS);
});

test('原生 gRPC 的外部 trailer 走 HTTP/2 HEADERS 校验', async () => {
  const body = encodeDataFrame(Buffer.from('ok'), false);
  const result = await parseMessages({
    body,
    headers: { 'grpc-encoding': 'identity' },
    externalTrailers: { 'grpc-status': '14' }
  });

  assert.equal(result.trailer.transport, 'http2-headers');
  assert.equal(result.trailer.status, 14);
});
