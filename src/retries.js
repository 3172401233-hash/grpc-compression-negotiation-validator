import { ErrorCode, GrpcProxyError, makeLocation } from './errors.js';

/**
 * gRPC 官方规范中默认可重试的状态码（无 RetryPolicy 定制时的基线）。
 * 14 UNAVAILABLE 与 4 DEADLINE_EXCEEDED 是代理链最常见的瞬时故障。
 */
export const DEFAULT_RETRIABLE_STATUS = Object.freeze([4, 10, 13, 14]);

const STATUS_NAMES = Object.freeze({
  0: 'OK',
  1: 'CANCELLED',
  2: 'UNKNOWN',
  3: 'INVALID_ARGUMENT',
  4: 'DEADLINE_EXCEEDED',
  5: 'NOT_FOUND',
  6: 'ALREADY_EXISTS',
  7: 'PERMISSION_DENIED',
  8: 'RESOURCE_EXHAUSTED',
  9: 'FAILED_PRECONDITION',
  10: 'ABORTED',
  11: 'OUT_OF_RANGE',
  12: 'UNIMPLEMENTED',
  13: 'INTERNAL',
  14: 'UNAVAILABLE',
  15: 'DATA_LOSS',
  16: 'UNAUTHENTICATED'
});

export function statusName(status) {
  return STATUS_NAMES[status] ?? `UNKNOWN_STATUS_${status}`;
}

/**
 * 规划一次交换的重试行为。
 *
 * @param {object} args
 * @param {number} args.grpcStatus trailer 中的 grpc-status
 * @param {boolean} args.idempotent 方法是否幂等
 * @param {number} args.attempt 已尝试次数（1 起）
 * @param {number} args.maxAttempts 最大尝试次数
 * @param {Array<{replayable: boolean}>} args.sentMessages 已发送消息的可重放信息
 * @param {number[]} [args.retriableStatus] 可重试状态码白名单
 */
export function planRetry({
  grpcStatus,
  idempotent,
  attempt,
  maxAttempts,
  sentMessages,
  retriableStatus = DEFAULT_RETRIABLE_STATUS
}) {
  const location = makeLocation({ phase: 'trailer', detail: `grpc-status=${grpcStatus}` });

  if (grpcStatus === 0) {
    return {
      retry: false,
      reason: 'OK',
      status: 0,
      statusName: statusName(0),
      attempt,
      attemptsRemaining: Math.max(0, maxAttempts - attempt),
      replayEntries: []
    };
  }

  const fail = (code, reason) => ({
    retry: false,
    reason,
    status: grpcStatus,
    statusName: statusName(grpcStatus),
    attempt,
    attemptsRemaining: Math.max(0, maxAttempts - attempt),
    replayEntries: [],
    error: new GrpcProxyError(code, reason, location)
  });

  if (!retriableStatus.includes(grpcStatus)) {
    return fail(
      'RETRY_NON_RETRIABLE_STATUS',
      `grpc-status ${grpcStatus} (${statusName(grpcStatus)}) 不在可重试白名单内`
    );
  }

  if (!idempotent) {
    return fail(
      ErrorCode.RETRY_NON_IDEMPOTENT,
      '非幂等方法禁止自动重试，避免重复副作用'
    );
  }

  if (attempt >= maxAttempts) {
    return fail(
      ErrorCode.RETRY_EXHAUSTED,
      `已达最大尝试次数 ${maxAttempts}，重试预算耗尽`
    );
  }

  const nonReplayable = sentMessages.filter((entry) => !entry.replayable);
  if (nonReplayable.length > 0) {
    return fail(
      ErrorCode.RETRY_NOT_REPLAYABLE,
      `存在 ${nonReplayable.length} 条不可重放消息（原始字节未保留），无法安全重试`
    );
  }

  return {
    retry: true,
    reason: `RETRIABLE_${statusName(grpcStatus)}`,
    status: grpcStatus,
    statusName: statusName(grpcStatus),
    attempt,
    attemptsRemaining: maxAttempts - attempt,
    replayEntries: sentMessages.map((entry, index) => ({
      messageIndex: index,
      replayable: true,
      replayFromOriginalBytes: true,
      sourceEncoding: entry.sourceEncoding
    }))
  };
}
