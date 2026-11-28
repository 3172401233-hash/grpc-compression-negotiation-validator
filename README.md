# gRPC 压缩协商与代理链验证器

面向代理链场景的**离线** gRPC 流解析器与策略规划器。它还原每条 Length-Prefixed
消息的编码与边界，并决定每一跳应当透传、解压、重压还是拒绝。

## 不建立真实 gRPC 连接

本工具只处理内存中的字节、header 与跳配置：

- 不打开 socket、不发起 HTTP/2 或 gRPC-Web 连接、不做 TLS；
- 不依赖任何第三方 npm 包，仅使用 Node.js 内置模块（`node:zlib`、`node:stream`、`node:test`）；
- 重压/解压在进程内同步完成，`simulator.js` 产出的是可直接上线发送的帧字节，而非网络流量；
- 运行环境固定为 Node.js `24.16.0`（`package.json` 的 `engines` 字段同步约束）。

## 背景语义

gRPC 帧为 `1 字节标志 + 4 字节大端长度 + 载荷`。代理链中各跳的压缩算法可能不同，
而 **Compressed-Flag 只作用于数据消息，不控制 trailer**：

- 数据帧 `0x00` 表示明文、`0x01` 表示载荷按 `grpc-encoding` 压缩；
- gRPC-Web trailer 帧 `0x80` 的 payload 永远是明文，即使低位被置成 `0x81` 也不会解压，
  解析器会显式报 `TRAILER_IGNORED_COMPRESSION_FLAG`；
- 原生 gRPC 的 trailer 走 HTTP/2 HEADERS，DATA 流里出现 `0x80` 帧直接报
  `TRAILER_ILLEGAL_IN_GRPC_MODE`。

工具采用**严格协商**：缺少 `grpc-encoding`、遇到未知算法或无共同编码时一律显式拒绝，
绝不会静默回退到 `identity`。

## 模块结构

| 文件 | 职责 |
| --- | --- |
| `src/stream-parser.js` | `FrameStreamParser` Transform 流与 `iterateFrames`，处理跨 chunk 帧边界 |
| `src/message-parser.js` | `parseMessages`：还原每消息编码、解压、大小与 trailer 校验 |
| `src/codecs.js` | `identity` / `gzip` / `deflate`（裸 DEFLATE）注册表与同步编解码 |
| `src/planner.js` | `negotiateEncoding` 与 `planMessage`：透传/解压/重压/拒绝决策 |
| `src/proxy-chain.js` | `planProxyChain`：多跳协商、转换规则、trailer 与重试编排 |
| `src/retries.js` | `planRetry`：按 gRPC 状态码、幂等性、可重放性规划重试 |
| `src/simulator.js` | `executeDecision` / `simulateChain`：内存级真实转码 |
| `src/errors.js` | 结构化错误码、`GrpcProxyError` 与字节级错误位置 |

## 快速使用

```js
import { gzipSync } from 'node:zlib';
import {
  parseMessages,
  planProxyChain,
  simulateChain,
  encodeDataFrame
} from './src/index.js';

const body = encodeDataFrame(gzipSync(Buffer.from('hello')), true);

// 1. 解析：还原编码、边界、解压大小和 trailer。
const parsed = await parseMessages({
  body,
  headers: { 'grpc-encoding': 'gzip' }
});

// 2. 双代理规划：第一跳转 deflate，第二跳转回 gzip。
const plan = await planProxyChain({
  body,
  headers: { 'grpc-encoding': 'gzip' },
  hops: [
    { acceptEncoding: 'deflate', maxMessageBytes: 4096, maxWireFrameBytes: 4096 },
    { acceptEncoding: 'gzip, identity', maxMessageBytes: 4096, maxWireFrameBytes: 4096 }
  ],
  retry: { idempotent: true, attempt: 1, maxAttempts: 3 }
});

// 3. 内存模拟每跳输出字节（无网络）。
const outputs = simulateChain(plan.parsed.messages, plan.hops);
```

## 决策与输出字段

每条消息在每一跳都会得到一个决策对象：

- `action`：`passthrough` | `decompress` | `recompress` | `reject`；
- `sourceEncoding` / `targetEncoding` / `compressedFlag`：实际编码与帧标志；
- `inputWireSize` / `outputWireSize` / `sizeDelta`：线上字节与重编码后的尺寸变化；
- `reencodeCpuCost`：确定性的相对开销单位，透传恒为 `0`；
- `fallbackProtocol`：仅当跳配置显式声明回退协议时才可用，否则 `available: false`；
- `error`：拒绝时包含错误码与 `location`（`hopIndex`、`frameIndex`、`messageIndex`、
  `byteOffset`、`phase`）。

## 明确处理的异常

| 错误码 | 触发条件 |
| --- | --- |
| `UNKNOWN_ENCODING` | header、accept-encoding 或消息使用未注册算法；不回退 identity |
| `HEADER_MESSAGE_ENCODING_MISMATCH` | Compressed-Flag 与 `grpc-encoding` 互相矛盾 |
| `PAYLOAD_DECODE_FAILED` / `PAYLOAD_ENCODE_FAILED` | zlib 解压或重压失败 |
| `MESSAGE_TOO_LARGE` | 解压后超消息上限，或重压后超线上帧上限 |
| `WIRE_FRAME_TOO_LARGE` / `FRAME_TRUNCATED` | 入站帧声明超长或流尾部帧不完整 |
| `INVALID_FRAME_FLAG` / `RESERVED_FRAME_FLAG` | 非法或保留标志位 |
| `TRAILER_*` | trailer 非终态、缺/非法 `grpc-status`、非法帧位或压缩位误用 |
| `NO_COMMON_ENCODING` | accept-encoding 为空或与本地无交集 |
| `RETRY_EXHAUSTED` / `RETRY_NON_IDEMPOTENT` / `RETRY_NOT_REPLAYABLE` | 重试预算、幂等或重放约束 |

## 测试

```bash
node --test
```

50 个用例覆盖：跨 chunk 帧边界、双代理 gzip→deflate→gzip 往返、不同
`accept-encoding` 协商、未知算法、解压/重压超限、header 与消息不一致、
gRPC-Web trailer 错误（含 `0x81` 压缩位）、原生 gRPC 外部 trailer 以及重试规划。
