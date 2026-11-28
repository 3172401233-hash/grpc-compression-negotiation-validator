import { test } from 'node:test';
import assert from 'node:assert/strict';
import { gzipSync, deflateSync } from 'node:zlib';
import { planMessage, negotiateEncoding, compress } from '../src/index.js';
import { ErrorCode } from '../src/errors.js';

const LIMIT = 1024;

test('编码相同且压缩标志正确时透传，零重编码开销', () => {
  const payload = gzipSync(Buffer.from('passthrough'));
  const decision = planMessage({
    wirePayload: payload,
    sourceEncoding: 'gzip',
    compressedFlag: true,
    acceptEncoding: 'gzip, identity',
    maxMessageBytes: LIMIT,
    maxWireFrameBytes: LIMIT
  });

  assert.equal(decision.action, 'passthrough');
  assert.equal(decision.reencodeCpuCost, 0);
  assert.equal(decision.sizeDelta, 0);
  assert.equal(decision.outputWireSize, payload.length);
});

test('gzip 到 deflate 触发重压并报告开销与大小变化', () => {
  const payload = gzipSync(Buffer.from('recompress-me'.repeat(20)));
  const decision = planMessage({
    wirePayload: payload,
    sourceEncoding: 'gzip',
    compressedFlag: true,
    acceptEncoding: 'deflate',
    maxMessageBytes: LIMIT,
    maxWireFrameBytes: LIMIT
  });

  assert.equal(decision.action, 'recompress');
  assert.equal(decision.targetEncoding, 'deflate');
  assert.equal(decision.compressedFlag, true);
  assert.ok(decision.reencodeCpuCost > 0);
  assert.equal(
    decision.sizeDelta,
    decision.outputWireSize - decision.inputWireSize
  );
});

test('gzip 到 identity 只解压并清除压缩标志', () => {
  const decoded = Buffer.from('decompress-only');
  const payload = gzipSync(decoded);
  const decision = planMessage({
    wirePayload: payload,
    sourceEncoding: 'gzip',
    compressedFlag: true,
    acceptEncoding: 'identity',
    maxMessageBytes: LIMIT,
    maxWireFrameBytes: LIMIT
  });

  assert.equal(decision.action, 'decompress');
  assert.equal(decision.targetEncoding, 'identity');
  assert.equal(decision.compressedFlag, false);
  assert.equal(decision.outputWireSize, decoded.length);
});

test('未知源算法直接拒绝，不回退 identity', () => {
  const decision = planMessage({
    wirePayload: Buffer.from('x'),
    sourceEncoding: 'zstd',
    compressedFlag: true,
    acceptEncoding: 'identity',
    maxMessageBytes: LIMIT,
    maxWireFrameBytes: LIMIT
  });

  assert.equal(decision.action, 'reject');
  assert.equal(decision.error.code, ErrorCode.UNKNOWN_ENCODING);
  assert.equal(decision.targetEncoding, null);
});

test('accept-encoding 为空时协商失败', () => {
  const result = negotiateEncoding('');
  assert.equal(result.error.code, ErrorCode.NO_COMMON_ENCODING);

  const decision = planMessage({
    wirePayload: Buffer.from('x'),
    sourceEncoding: 'identity',
    compressedFlag: false,
    acceptEncoding: '',
    maxMessageBytes: LIMIT,
    maxWireFrameBytes: LIMIT
  });
  assert.equal(decision.action, 'reject');
  assert.equal(decision.error.code, ErrorCode.NO_COMMON_ENCODING);
});

test('accept 中包含未知算法时显式拒绝', () => {
  const result = negotiateEncoding('zstd, gzip');
  assert.equal(result.error.code, ErrorCode.UNKNOWN_ENCODING);
});

test('按 accept-encoding 声明顺序选择第一个共同算法', () => {
  assert.equal(negotiateEncoding('deflate, gzip').encoding, 'deflate');
  assert.equal(negotiateEncoding('gzip, deflate').encoding, 'gzip');
});

test('重压后超过线上帧上限报 MESSAGE_TOO_LARGE', () => {
  // 随机数据压缩比很低，重压后的线上帧仍较大，便于稳定触发上限。
  const decoded = Buffer.from(
    Array.from({ length: 512 }, () => Math.floor(Math.random() * 256))
  );
  const payload = gzipSync(decoded);
  const reencoded = deflateSync(decoded);
  const decision = planMessage({
    wirePayload: payload,
    sourceEncoding: 'gzip',
    compressedFlag: true,
    acceptEncoding: 'deflate',
    maxMessageBytes: LIMIT,
    maxWireFrameBytes: reencoded.length - 1
  });

  assert.equal(decision.action, 'reject');
  assert.equal(decision.error.code, ErrorCode.MESSAGE_TOO_LARGE);
  assert.match(decision.error.message, /重压/);
});

test('解压后超过消息上限报 MESSAGE_TOO_LARGE', () => {
  const payload = gzipSync(Buffer.alloc(128));
  const decision = planMessage({
    wirePayload: payload,
    sourceEncoding: 'gzip',
    compressedFlag: true,
    acceptEncoding: 'identity',
    maxMessageBytes: 64,
    maxWireFrameBytes: LIMIT
  });

  assert.equal(decision.error.code, ErrorCode.MESSAGE_TOO_LARGE);
});

test('拒绝决策返回显式声明的可回退协议，无声明时标记不可回退', () => {
  const withFallback = planMessage({
    wirePayload: Buffer.from('x'),
    sourceEncoding: 'zstd',
    compressedFlag: true,
    acceptEncoding: 'identity',
    maxMessageBytes: LIMIT,
    maxWireFrameBytes: LIMIT,
    fallbackProtocols: ['grpc-web+identity', 'http1.1-close']
  });
  assert.equal(withFallback.fallbackProtocol.available, true);
  assert.equal(withFallback.fallbackProtocol.protocol, 'grpc-web+identity');

  const without = planMessage({
    wirePayload: Buffer.from('x'),
    sourceEncoding: 'zstd',
    compressedFlag: true,
    acceptEncoding: 'identity',
    maxMessageBytes: LIMIT,
    maxWireFrameBytes: LIMIT
  });
  assert.equal(without.fallbackProtocol.available, false);
  assert.equal(without.fallbackProtocol.protocol, null);
});

test('强制编码不在 accept 列表内时拒绝', () => {
  const decision = planMessage({
    wirePayload: Buffer.from('x'),
    sourceEncoding: 'identity',
    compressedFlag: false,
    acceptEncoding: 'identity',
    targetEncoding: 'gzip',
    maxMessageBytes: LIMIT,
    maxWireFrameBytes: LIMIT
  });
  assert.equal(decision.error.code, ErrorCode.NO_COMMON_ENCODING);
});

test('identity 帧携带压缩标志属于 header/消息矛盾', () => {
  const decision = planMessage({
    wirePayload: Buffer.from('x'),
    sourceEncoding: 'identity',
    compressedFlag: true,
    acceptEncoding: 'gzip',
    maxMessageBytes: LIMIT,
    maxWireFrameBytes: LIMIT
  });
  assert.equal(decision.error.code, ErrorCode.HEADER_MESSAGE_ENCODING_MISMATCH);
});

test('重压输出可被对应算法解压回原文', () => {
  const decoded = Buffer.from('roundtrip'.repeat(10));
  const payload = deflateSync(decoded);
  const decision = planMessage({
    wirePayload: payload,
    sourceEncoding: 'deflate',
    compressedFlag: true,
    acceptEncoding: 'gzip',
    maxMessageBytes: LIMIT,
    maxWireFrameBytes: LIMIT
  });
  const reencoded = compress(decision.targetEncoding, decoded).output;
  assert.deepEqual(reencoded.length, decision.outputWireSize);
});
