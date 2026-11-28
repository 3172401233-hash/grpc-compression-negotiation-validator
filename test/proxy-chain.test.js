import { test } from 'node:test';
import assert from 'node:assert/strict';
import { gzipSync, gunzipSync, inflateSync, deflateSync } from 'node:zlib';
import {
  planProxyChain,
  simulateChain,
  encodeDataFrame,
  encodeGrpcWebTrailerFrame,
  parseMessages
} from '../src/index.js';
import { ErrorCode } from '../src/errors.js';

const LIMIT = 4096;

test('双代理：gzip -> deflate -> gzip，每跳决策与最终字节均可还原', async () => {
  const text = Buffer.from('double-proxy-payload'.repeat(30));
  const body = encodeDataFrame(gzipSync(text), true);

  const plan = await planProxyChain({
    body,
    headers: { 'grpc-encoding': 'gzip' },
    hops: [
      { acceptEncoding: 'deflate', maxMessageBytes: LIMIT, maxWireFrameBytes: LIMIT },
      { acceptEncoding: 'gzip, identity', maxMessageBytes: LIMIT, maxWireFrameBytes: LIMIT }
    ]
  });

  assert.equal(plan.summary.ok, true);
  assert.equal(plan.hops[0].decisions[0].action, 'recompress');
  assert.equal(plan.hops[0].decisions[0].targetEncoding, 'deflate');
  assert.equal(plan.hops[1].decisions[0].action, 'recompress');
  assert.equal(plan.hops[1].decisions[0].targetEncoding, 'gzip');

  const outputs = simulateChain(plan.parsed.messages, plan.hops);
  assert.equal(outputs.length, 2);

  // 第一跳输出必须能被 deflate 解压，第二跳输出必须能被 gzip 解压回原文。
  const firstHop = await parseMessages({
    body: outputs[0].body,
    headers: { 'grpc-encoding': 'deflate' }
  });
  assert.equal(firstHop.messages[0].decoded.toString(), text.toString());

  const secondHop = await parseMessages({
    body: outputs[1].body,
    headers: { 'grpc-encoding': 'gzip' }
  });
  assert.equal(secondHop.messages[0].decoded.toString(), text.toString());

  // 直接字节级验证算法确实发生了切换。
  const deflatePayload = outputs[0].view[0].payload;
  assert.deepEqual(inflateSync(deflatePayload), text);
  const gzipPayload = outputs[1].view[0].payload;
  assert.deepEqual(gunzipSync(gzipPayload), text);
});

test('不同 accept-encoding：第二跳只接受 identity 时链路退化为解压', async () => {
  const body = encodeDataFrame(gzipSync(Buffer.from('negotiate')), true);
  const plan = await planProxyChain({
    body,
    headers: { 'grpc-encoding': 'gzip' },
    hops: [
      { acceptEncoding: 'gzip', maxMessageBytes: LIMIT, maxWireFrameBytes: LIMIT },
      { acceptEncoding: 'identity', maxMessageBytes: LIMIT, maxWireFrameBytes: LIMIT }
    ]
  });

  assert.equal(plan.hops[0].decisions[0].action, 'passthrough');
  assert.equal(plan.hops[1].decisions[0].action, 'decompress');
  assert.equal(plan.hops[1].decisions[0].compressedFlag, false);
});

test('某跳 accept-encoding 含未知算法时整跳协商失败并定位 hopIndex', async () => {
  const body = encodeDataFrame(Buffer.from('x'), false);
  const plan = await planProxyChain({
    body,
    headers: { 'grpc-encoding': 'identity' },
    hops: [
      { acceptEncoding: 'identity', maxMessageBytes: LIMIT, maxWireFrameBytes: LIMIT },
      { acceptEncoding: 'zstd', maxMessageBytes: LIMIT, maxWireFrameBytes: LIMIT }
    ]
  });

  assert.equal(plan.hops[1].negotiationError.code, ErrorCode.UNKNOWN_ENCODING);
  assert.equal(plan.hops[1].decisions[0].error.code, ErrorCode.UNKNOWN_ENCODING);
  assert.equal(plan.summary.ok, false);
});

test('超大消息：解压后超限在第一跳即拒绝并给出消息位置', async () => {
  const big = Buffer.alloc(256, 0x63);
  const body = encodeDataFrame(gzipSync(big), true);
  const plan = await planProxyChain({
    body,
    headers: { 'grpc-encoding': 'gzip' },
    hops: [
      { acceptEncoding: 'identity', maxMessageBytes: 64, maxWireFrameBytes: LIMIT },
      { acceptEncoding: 'gzip', maxMessageBytes: 64, maxWireFrameBytes: LIMIT }
    ]
  });

  assert.equal(plan.hops[0].decisions[0].action, 'reject');
  assert.equal(plan.hops[0].decisions[0].error.code, ErrorCode.MESSAGE_TOO_LARGE);
  assert.equal(plan.hops[0].decisions[0].error.location.messageIndex, 0);
  assert.equal(plan.hops[0].decisions[0].error.location.hopIndex, 0);
});

test('重压后超过线上帧上限被拒绝', async () => {
  const decoded = Buffer.from(
    Array.from({ length: 1024 }, () => Math.floor(Math.random() * 256))
  );
  const body = encodeDataFrame(gzipSync(decoded), true);
  const plan = await planProxyChain({
    body,
    headers: { 'grpc-encoding': 'gzip' },
    hops: [
      {
        acceptEncoding: 'deflate',
        maxMessageBytes: LIMIT,
        maxWireFrameBytes: deflateSync(decoded).length - 1
      }
    ]
  });
  assert.equal(plan.hops[0].decisions[0].error.code, ErrorCode.MESSAGE_TOO_LARGE);
});

test('trailer 错误：grpc-web 缺 grpc-status 时标记 trailer 错误且不重试', async () => {
  const body = Buffer.concat([
    encodeDataFrame(Buffer.from('a'), false),
    encodeGrpcWebTrailerFrame('grpc-message: broken')
  ]);
  const plan = await planProxyChain({
    body,
    headers: { 'grpc-encoding': 'identity' },
    mode: 'grpc-web',
    hops: [
      { acceptEncoding: 'identity', maxMessageBytes: LIMIT, maxWireFrameBytes: LIMIT }
    ]
  });

  assert.equal(plan.parsed.trailer.error.code, ErrorCode.TRAILER_MISSING_STATUS);
  assert.equal(plan.retry.reason, 'TRAILER_INVALID');
  assert.equal(plan.summary.ok, false);
});

test('trailer 终态 UNAVAILABLE 且幂等可重试时生成重放条目', async () => {
  const body = Buffer.concat([
    encodeDataFrame(gzipSync(Buffer.from('retry-me')), true),
    encodeGrpcWebTrailerFrame('grpc-status: 14')
  ]);
  const plan = await planProxyChain({
    body,
    headers: { 'grpc-encoding': 'gzip' },
    mode: 'grpc-web',
    hops: [
      { acceptEncoding: 'gzip', maxMessageBytes: LIMIT, maxWireFrameBytes: LIMIT }
    ],
    retry: { idempotent: true, attempt: 1, maxAttempts: 3 }
  });

  assert.equal(plan.retry.retry, true);
  assert.equal(plan.retry.attemptsRemaining, 2);
  assert.equal(plan.retry.replayEntries[0].replayFromOriginalBytes, true);
});

test('非幂等方法的 UNAVAILABLE 不允许重试', async () => {
  const body = encodeGrpcWebTrailerFrame('grpc-status: 14');
  const plan = await planProxyChain({
    body,
    headers: { 'grpc-encoding': 'identity' },
    mode: 'grpc-web',
    hops: [
      { acceptEncoding: 'identity', maxMessageBytes: LIMIT, maxWireFrameBytes: LIMIT }
    ],
    retry: { idempotent: false, attempt: 1, maxAttempts: 3 }
  });
  assert.equal(plan.retry.error.code, ErrorCode.RETRY_NON_IDEMPOTENT);
});

test('strip-compression 转换强制输出 identity 明文帧', async () => {
  const text = Buffer.from('strip-it');
  const body = encodeDataFrame(gzipSync(text), true);
  const plan = await planProxyChain({
    body,
    headers: { 'grpc-encoding': 'gzip' },
    hops: [
      {
        acceptEncoding: 'identity',
        transform: 'strip-compression',
        maxMessageBytes: LIMIT,
        maxWireFrameBytes: LIMIT
      }
    ]
  });

  assert.equal(plan.hops[0].transform, 'strip-compression');
  assert.equal(plan.hops[0].decisions[0].action, 'decompress');
  const outputs = simulateChain(plan.parsed.messages, plan.hops);
  const reparsed = await parseMessages({
    body: outputs[0].body,
    headers: { 'grpc-encoding': 'identity' }
  });
  assert.equal(reparsed.messages[0].decoded.toString(), text.toString());
  assert.equal(reparsed.messages[0].compressedFlag, false);
});

test('不支持的转换类型抛 UNSUPPORTED_TRANSFORM', async () => {
  const body = encodeDataFrame(Buffer.from('x'), false);
  await assert.rejects(
    planProxyChain({
      body,
      headers: { 'grpc-encoding': 'identity' },
      hops: [
        {
          acceptEncoding: 'identity',
          transform: 'tls-bump',
          maxMessageBytes: LIMIT,
          maxWireFrameBytes: LIMIT
        }
      ]
    }),
    (error) => error.code === ErrorCode.UNSUPPORTED_TRANSFORM
  );
});
