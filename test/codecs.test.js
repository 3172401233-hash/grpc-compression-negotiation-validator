import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  compress,
  decompress,
  isKnownEncoding,
  SUPPORTED_ENCODINGS
} from '../src/index.js';
import { ErrorCode } from '../src/errors.js';

test('三种支持算法的压缩/解压往返', () => {
  const input = Buffer.from('codec-roundtrip'.repeat(16));
  for (const encoding of SUPPORTED_ENCODINGS) {
    const { output } = compress(encoding, input);
    const restored = decompress(encoding, output).output;
    assert.deepEqual(restored, input);
  }
});

test('identity 不改变字节', () => {
  const input = Buffer.from([1, 2, 3, 4]);
  assert.deepEqual(compress('identity', input).output, input);
  assert.deepEqual(decompress('identity', input).output, input);
});

test('未知算法报 UNKNOWN_ENCODING 而非回退 identity', () => {
  assert.equal(isKnownEncoding('zstd'), false);
  assert.throws(
    () => compress('zstd', Buffer.from('x')),
    (error) => error.code === ErrorCode.UNKNOWN_ENCODING
  );
});

test('损坏的 gzip 字节报 PAYLOAD_DECODE_FAILED', () => {
  assert.throws(
    () => decompress('gzip', Buffer.from('not-gzip')),
    (error) => error.code === ErrorCode.PAYLOAD_DECODE_FAILED
  );
});
