import { test } from 'node:test';
import assert from 'node:assert/strict';
import { planRetry } from '../src/index.js';
import { ErrorCode } from '../src/errors.js';

const sent = (count, replayable = true) =>
  Array.from({ length: count }, () => ({ replayable, sourceEncoding: 'gzip' }));

test('OK 状态不重试', () => {
  const plan = planRetry({
    grpcStatus: 0,
    idempotent: true,
    attempt: 1,
    maxAttempts: 3,
    sentMessages: sent(1)
  });
  assert.equal(plan.retry, false);
  assert.equal(plan.reason, 'OK');
});

test('可重试状态且预算充足时返回重放计划', () => {
  const plan = planRetry({
    grpcStatus: 14,
    idempotent: true,
    attempt: 1,
    maxAttempts: 3,
    sentMessages: sent(2)
  });
  assert.equal(plan.retry, true);
  assert.equal(plan.attemptsRemaining, 2);
  assert.equal(plan.replayEntries.length, 2);
  assert.equal(plan.replayEntries[1].sourceEncoding, 'gzip');
});

test('达到最大尝试次数报 RETRY_EXHAUSTED', () => {
  const plan = planRetry({
    grpcStatus: 14,
    idempotent: true,
    attempt: 3,
    maxAttempts: 3,
    sentMessages: sent(1)
  });
  assert.equal(plan.error.code, ErrorCode.RETRY_EXHAUSTED);
});

test('不可重放消息阻止重试', () => {
  const plan = planRetry({
    grpcStatus: 14,
    idempotent: true,
    attempt: 1,
    maxAttempts: 3,
    sentMessages: [{ replayable: true }, { replayable: false }]
  });
  assert.equal(plan.error.code, ErrorCode.RETRY_NOT_REPLAYABLE);
});

test('白名单外状态不重试', () => {
  const plan = planRetry({
    grpcStatus: 7,
    idempotent: true,
    attempt: 1,
    maxAttempts: 3,
    sentMessages: sent(1)
  });
  assert.equal(plan.retry, false);
  assert.match(plan.reason, /PERMISSION_DENIED|白名单/);
});
