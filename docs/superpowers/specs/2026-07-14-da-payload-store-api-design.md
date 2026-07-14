# DA payload store gRPC 合约设计（`proto/da.proto`）

- **日期**：2026-07-14
- **状态**：已评审，待实施
- **上游设计**：housegate `docs/superpowers/specs/2026-06-30-sentio-arbiter-design.md`（§3.5 数据流、§5.2 锚定、§13 红线、§15 Q6 开放问题）
- **产出**：本仓库新增 `proto/da.proto` —— payload store / DA 层的 gRPC 服务契约（2 个 service、7 个 RPC）

## 背景与目标

Arbiter 设计中 payload 的角色：HouseGate 在 **ingest**（§3.5 step 1）把签名 write-statement 的
payload 字节 spool 进 DA / payload store，随后 `SubmitStatement` 里只携带
`payload_ref`；Verifier 在 **replay**（step 3）按 ref 取回字节，先对 sequenced envelope 的
`payload_hash + payload_length` 校验再执行；step 5 的锚定只发 commitment +
`da_ref` 指针。此前 proto 只有 `StatementEnvelopeV2.payload_ref` 等占位字段，store
本身没有任何 wire 契约——本设计补上这一层。

**目标**：
1. v1 trusted payload store 的完整服务契约（写入 / 读取 / 探测 / 保留生命周期）；
2. 「谁在什么时候可以删」有协议背书（解 §15 Q6 的 retention 半边）；
3. P5+ 换 Celestia / EigenDA / EIP-4844 后端时**仅配置变更、非协议变更**。

**非目标**：proof-of-custody（§15 Q6 另半边，P5+）；block 级 `AnchorRef.da_ref`
方案（§5.2，P5+）；store 的具体实现与部署拓扑。

## 设计过程

四个并行设计代理按不同哲学（极简 / DA-native / 生命周期优先 / 热路径优先）各出完整草案，
每案由对抗校验代理按 7 条冻结约束试图击穿；综合方案以 DA-native 案的语义骨架
（异步状态机 + hold 记账 + release-as-permission）为底，嫁接热路径案的读写形态
（inline put + 帧交错批量 fetch），生命周期收敛为 Pin/Release 两个动词，并把校验
发现的全部 blocking 竞态修复写进合约。`buf lint` / `buf build` 已实测通过，与现有
`package arbiter` 三个文件无消息名 / 枚举值冲突。

## 架构与调用方

payload store 是**被动的数据面服务器**：所有人拨号它，arbiter 设计 spec §11.1 的
"data plane dials the Arbiter" 方向约定在这里不适用，也没有 NotLeader 协议
（服务可以无 leader、水平扩展）。

| 服务 | 调用方 | 路径 |
|---|---|---|
| `PayloadStore` | HouseGate（ingest，SubmitStatement 之前）；Verifier（replay） | 数据面热路径 |
| `PayloadLifecycle` | 仅 Arbiter leader orchestrator | 控制面（retention refcount） |

信任模型（spec §13）：store 只被信任 **availability**，从不被信任 integrity。读路径在
结构上强制这一点——**hash-silent**：任何 fetch/stat 响应都不携带 server 断言的
content hash 或 length，读者只能拿 sequenced envelope 的 `payload_hash +
payload_length` 自己校验，在任何 executor 运行之前完成（pkg/replay verifier
invariant）。写入时的 digest 校验是 ingest 卫生（提前抓住写坏的 spool），不是给读者的
完整性承诺。

本服务解析的是 **per-statement** 的 `StatementEnvelopeV2.payload_ref`。

## 端到端数据流（put → pin → fetch → release）

```
HouseGate  ① PutPayloadInline / PutPayload            [lease]
           ② SubmitStatement(payload_ref) → Arbiter 将其 sequence 进 L3 block N
Arbiter    ③ PinPayloads(REPLAY, scope=N, refs)       [lease + pin]
              —— pin 从 lease 手中接管保留义务，此后 lease 过期无关紧要
Verifier   ④ replay block N 时 FetchPayloads(refs)；对 sequenced envelope 的
              payload_hash + payload_length 校验字节，replay，签名 attest
Arbiter    ⑤ quorum + L2 finality + safe watermark 推过 N 之后：
              先 PinPayloads(AUDIT, scope=N, refs)，再 ReleasePins(REPLAY, N)
              —— 永远先上新锁再解旧锁，被覆盖的 ref 不存在裸奔窗口   [pin(AUDIT)]
Arbiter    ⑥ 审计窗口结束：ReleasePins(AUDIT, N)       [零 hold]
store      ⑦ 零 pin + lease 过期 → MAY collect（从不 MUST；不可删除的后端
              继续照常服务即为合规）
```

失败出口：statement 永远没被 sequenced 时，payload 只受 lease 保护，lease 过期是它
变得可回收的唯一路径。

## 消息与服务参考

**PayloadStore（5 个 RPC）**

- `GetStoreLimits → StoreLimits{max_inline_bytes, max_chunk_bytes, max_batch_refs, max_payload_bytes}`：部署级限制，进程启动时取一次，改限制走部署配置（与 P5+ 后端切换同一机制）。
- `PutPayloadInline(header + payload) → PutPayloadResult`：≤ `max_inline_bytes` 的小 payload 一次往返（主导场景，热路径）。
- `PutPayload(stream PutPayloadFrame) → PutPayloadResult`：大 payload 的 chunked client stream；首帧 header（声明 hash+length），之后按序 chunk，无 offset（严格顺序消除重排歧义）。oneof 字段名是 `header`，刻意避开 `descriptor`（protoc-gen-go 的 `Descriptor()` 冲突）。
- `FetchPayloads(specs) → stream FetchFrame`：批量读，`FetchSpec{payload_ref, PayloadRange{offset, length}}`；每个 ref 的帧序为 `FetchBegin{ref} → FetchData{ref, offset, chunk}* → FetchEnd{ref, code, served_length, message}`；单 ref 内严格有序，不同 ref 之间**可以交错**（慢对象不会队头阻塞整个 block 的拉取）。断流后用 range 只重拉未完成的 spec。
- `StatPayloads(refs) → StatPayloadsResult`：advisory 的批量 presence/state 探测（时点观察，不是预留），受 `max_batch_refs` 约束。

**PayloadLifecycle（2 个 RPC）**

- `PinPayloads(PinKey{holder_id, purpose, scope_key}, refs)`：把 refs **union** 进该 key 的 pin，add-only，**没有 replace 语义**；同 (key, refs) 重放是幂等 no-op。
- `ReleasePins(PinKey, authority_jws)`：按**整 key** 释放整个 pin，永远不按裸 ref 释放；未知/已释放的 key 返回 OK。

`PutPayloadResult` 携带 `payload_ref`（不透明、URI-scheme-friendly、禁止解析）、
`state`（AVAILABLE/PENDING）、`lease_expires_unix_ms`、advisory 的 `deduplicated`。

## 结果码 taxonomy

沿用 AdmissionCode 先例：**每个关注点一个 enum**，绝不共享一个大 enum；零值一律
`_UNSPECIFIED`；应用级结果走 gRPC OK + code，传输失败走 gRPC error；每个带 code 的
结果消息都带人类可读 `message`；所有时间字段 `uint64` unix 毫秒、`_unix_ms` 后缀。

- `PutCode`：`OK / COMMITMENT_MISMATCH / TOO_LARGE / INLINE_LIMIT_EXCEEDED / MALFORMED`
- `FetchCode`（per-ref，在 `FetchEnd` 中）：`OK / NOT_FOUND / PENDING / RELEASED / OFFSET_OUT_OF_RANGE`
- `StatCode`：`OK / NOT_FOUND`（存在时 state 给出 PENDING/AVAILABLE/RELEASED）
- `PinCode`（per-ref）：`OK / NOT_FOUND / RELEASED`（后两者对 replay pin 是 availability incident，升级处理而非重试，§13）
- `ReleaseCode`：`OK / UNAUTHORIZED`

请求级 malformation 与 per-ref 失败严格分层：超 `max_batch_refs`、空 ref、零 specs 等
在发出任何帧之前以 gRPC `INVALID_ARGUMENT` 拒绝；per-ref 失败隔离在 `FetchEnd` /
`PinRefResult` 里，一个 ref 的缺失不掩盖同 block 其余 ref 的可用性。

Put 完整性门（§13 卫生）：store 在 ack OK **之前**把收到的字节对声明的
`(payload_hash "0x"+hex sha256 DigestString profile, payload_length)` 校验；不匹配 →
`PUT_CODE_COMMITMENT_MISMATCH`，什么都不存、不铸 ref。dedupe 快路径**仍必须先 drain
并校验整条流**再 ack OK（否则损坏的重复上传会静默借用合法 ref）。同一 digest 的并发
首次 put 在服务端收敛为一份存储，所有合规 put 拿到 OK + 同一 ref；失败的 put 永不污染
其他 in-flight put。

## 生命周期语义

### lease → pin 交接（修 put→sequenced 竞态）

每次成功 put——**包括 dedupe 命中**——都授予/刷新一个 ingest lease
（`lease_expires_unix_ms`，按 content identity 记账）。交接规则写死在 `PayloadStore`
service 注释里：

1. HouseGate 必须持有未过期 lease，直到 `SubmitStatement` 被 ack（§3.5 step 1–2）；
2. sequencing 时，Arbiter orchestrator 用 pin（purpose `REPLAY`，`scope_key` =
   十进制 `block_seq`）覆盖该 block 的全部 refs，接管保留义务；
3. 如果 statement 永远没被 sequenced，lease 过期是 payload 变得可回收的唯一路径。

关键设计点：**lease 不是 pin，没有 holder 身份**。跨节点的重复 put 只能*延长*保护，
不可能偷走或替换任何东西——这消灭了「重复 put 抢占/替换 hold」一类的竞态。

### pins 是 refcount

内容寻址 dedupe 被允许（store SHOULD 按 `(payload_hash, payload_length)` content
identity 铸稳定 ref），于是保留必须靠显式计数：**GC 仅当某 ref 零 pin 且无未过期
lease 时才被允许**。pin 以 `(holder_id, purpose ∈ {REPLAY, AUDIT, ARCHIVE},
scope_key)` 为 key，add-only；`holder_id` 必须是 cluster-stable 的逻辑身份（如
`"arbiter"`），绝不能用 per-leader 实例 id，failover 后新 leader 重申同一批 pin 而
不是孤儿化前任的。

### release 是许可，不是命令

released 意味着 store **MAY** collect，从不 MUST。不能删除的后端（链上 DA）是一个
「永不回收」的合规 store。合规性落在 **composite store SERVICE**（gateway +
backend）上：物理层自行修剪时（EIP-4844 blob 约 18 天），服务必须在 holds 存续期间
保留自己的副本——被 pin 的 payload 必须保持可取回，这个义务在服务身上，不在链上。
**删除没有 RPC**：不存在任何人可调用的删字节动词。

### 破坏性 RPC 上的 authority slot

`ReleasePinsRequest.authority_jws` 沿用 `PromotionCommand.authority_jws` 先例
（§8.1）：secp256k1 JWS compact form，purpose `"arbiter-payload-release"`，
CanonicalDigest domain `"arbiter-payload-release-v1"`，store 按 address recovery 对
authority allowlist 授权。v1 MAY 以空值跑 channel-trust——字段和校验规则**现在**就写
进协议，P5+ 加固只是配置开关，不是协议变更。

## 兼容性与 P5+ 切换

- `payload_ref` 不透明、URI-scheme-friendly、put 时铸造、永不变化；合约里没有任何链的 namespace/height/quorum 概念。Celestia / EigenDA / EIP-4844 到来时是「新 ref scheme + store 配置」。
- **异步可用性已预埋**：`PayloadState{UNSPECIFIED, PENDING, AVAILABLE, RELEASED}`。异步后端在 dispersal 进行中即返回 OK + ref（state=PENDING）；对 PENDING ref 的 Fetch 得到 per-ref `FETCH_CODE_PENDING`（退避重试）。v1 本地 store 永不发出 PENDING，但客户端**必须**处理它——后端切换才是配置而非协议。
- state 是 **per-content-identity** 的：RELEASED 只是 terminal-as-served；回收后对相同字节重新 put 会重新存储并把**同一个 ref** 带回 AVAILABLE（消除「RELEASED 终态」与内容寻址 dedupe 的状态机矛盾）。
- 本文件不 import arbiter.proto / replay.proto，跨边界只有不透明字符串（`payload_ref`、`payload_hash`），后端切换时 store 合约可整体拆卸。
- `StatementEnvelopeV2.payload_ref/payload_hash/payload_length`、`replay.Statement`、`AnchorRef.da_ref` 全部**不动**。

## 错误处理

分三层：

1. **gRPC error**：传输、鉴权、请求级 malformation（超 `max_batch_refs`、空 ref、零 specs → `INVALID_ARGUMENT`，且发生在任何帧之前）。channel 层重试策略只作用于这一层；put 是 content-idempotent，重试恒安全。
2. **OK + code**：应用级结果。dedupe 命中仍是 `PUT_CODE_OK`（`deduplicated` 仅供 metrics）。
3. **per-ref 隔离**：Fetch/Pin 的单 ref 失败装在 `FetchEnd` / `PinRefResult` 里，不杀整条流/整个批。

几个刻意的语义边界：

- range 的 offset 合法但 length 越过 EOF → **不是错误**，served 到 EOF 并在 `FetchEnd.served_length` 报实际字节数；`offset >= payload length` → `OFFSET_OUT_OF_RANGE`。
- `FETCH_CODE_NOT_FOUND` vs `FETCH_CODE_RELEASED`：从未存过 vs 曾存在、按规则回收——把「availability fault（可升级/可挑战）」和「按策略合法老化」区分开。对一个被 pin 的 block 做 replay 时遇到 NOT_FOUND/RELEASED 是事故升级，不是重试。
- `PUT_CODE_COMMITMENT_MISMATCH` 是调用方 spool bug：大声上报，绝不带同样的字节自动重试。

## 测试与 conformance 备注

- **PENDING 路径防腐**：v1 本地 store 永不发 PENDING，暗路径必然烂掉。要求提供一个 fault-injecting dev store（配置开关），强制对指定比例/指定 ref 发 `PAYLOAD_STATE_PENDING` 与 `FETCH_CODE_PENDING`，verifier 与 orchestrator 的 CI 必须在该 store 上跑通退避重试路径。
- **lease 交接竞态测试**：put →（不 SubmitStatement）→ lease 过期 → GC 允许；put → SubmitStatement acked → pin(REPLAY, block_seq) → lease 过期后 fetch 仍 OK。再加跨节点重复 put：node B 对同 digest 的 put 只能延长 lease，不得影响 node A 的在途流程。
- **dedupe 完整性测试**：先存好某 digest，再以同 header 发一条尾部损坏的流——必须 `COMMITMENT_MISMATCH`，且原 ref 依旧可取（dedupe 快路径必须 drain+verify）。并发首 put 同 digest：全部 OK + 同 ref，只落一份。
- **read-path hash-silent 断言**：conformance 测试对生成的 descriptor 做静态检查，`FetchBegin/FetchData/FetchEnd/PayloadStat` 中不得出现任何 `*hash*` / `payload_length` 字段（`served_length` 是服务量，不是内容断言）——防止后人「顺手」加回 server 断言字段。
- **pin add-only 断言**：`PinPayloads(key, {r1})` 后 `PinPayloads(key, {r2})`，释放前 r1、r2 都必须受保护（union，非 replace）；`ReleasePins` 只认整 key，重复释放返回 OK。
- **边界码测试**：`offset == length` → `OFFSET_OUT_OF_RANGE`；`offset < length, offset+range.length > length` → OK + 截断的 `served_length`；批量超 `max_batch_refs` → 帧前 `INVALID_ARGUMENT`。
- **authority_jws 开关测试**：channel-trust 模式空 JWS 放行；enforcement 模式下空/坏 JWS → `RELEASE_CODE_UNAUTHORIZED` 且 pin 原样保留。
- 与 repo 现有 CI 对齐：`buf lint`（BASIC，除 PACKAGE_DIRECTORY_MATCH）与 `buf build` 已验证通过；`buf breaking` 对新增文件天然通过。

## 附录：`da.proto` 完整草案

```proto
syntax = "proto3";

// da — gRPC contract for the Sentio payload store / DA layer (design §3.5
// steps 1–3): HouseGate spools signed write-statement payload bytes at
// INGEST, before SubmitStatement carries the resulting payload_ref in
// StatementEnvelopeV2; Verifiers fetch the bytes at REPLAY and validate them
// against the sequenced envelope's payload_hash + payload_length BEFORE any
// executor runs (pkg/replay verifier invariant); the Arbiter leader
// orchestrator governs retention through pins.
//
// TRUST MODEL (§13): the store is trusted for AVAILABILITY ONLY, never for
// integrity. Structurally enforced: the read path is HASH-SILENT — no fetch
// or stat response ever asserts a content hash or length, so no caller can
// be tempted to trust the store's word over the envelope's. The put-time
// digest check below is ingest hygiene (catches a corrupting spool before
// sequencing), not an integrity promise to readers.
//
// SCOPE (v1): one trusted composite store SERVICE (gateway + backend);
// caller identity is trusted from the gRPC channel (the RCRecord stance).
// This service resolves the PER-STATEMENT payload_ref carried by
// StatementEnvelopeV2 / replay.Statement. The block-level AnchorRef.da_ref
// scheme (§5.2) is P5+ and out of scope here; proof-of-custody is deferred
// to P5+ (§15 Q6). The §11.1 dial-the-Arbiter direction convention does NOT
// apply: the store is a passive data-plane server — every caller dials it,
// and there is no NotLeader protocol.
//
// BACKEND-SWAP RULE (P5+): payload_ref is an opaque, URI-scheme-friendly
// string callers MUST NOT parse; nothing below assumes synchronous
// availability (see PAYLOAD_STATE_PENDING) or any commitment format.
// Celestia / EigenDA / EIP-4844 blobs arrive as a new ref scheme plus store
// configuration, not a protocol change.
//
// LIFECYCLE MODEL — "who may delete when" has protocol backing:
//   1. Every successful put grants an INGEST LEASE (a TTL on the payload's
//      content identity, refreshed by duplicate puts). The lease bridges the
//      window between spool and sequencing; it has NO holder identity.
//   2. PINS are the refcount. A pin is keyed (holder_id, purpose, scope_key)
//      and is ADD-ONLY: re-pinning unions refs in; there is no replace.
//      ReleasePins releases by exact key — the whole pin, never a raw ref.
//   3. GC is PERMITTED only when a ref has zero pins AND no unexpired lease.
//      Release grants permission to collect, never commands erasure: a
//      backend that cannot delete (chain DA) is a conforming store that
//      never collects. Conversely, if the physical layer prunes on its own
//      (EIP-4844 blobs, ~18 days), the composite SERVICE must retain its own
//      copy while holds exist — a pinned payload MUST stay retrievable, and
//      that obligation sits on the service, not the chain.
//   4. Deletion is NOT an RPC. There is no verb anyone could call to erase
//      bytes; collection is an internal store decision gated by rule 3.
//
// END-TO-END DATA FLOW — one payload's whole life (put → pin → fetch →
// release), actors on the left, retention state on the right:
//
//   HouseGate  ① PutPayloadInline / PutPayload            [lease]
//              ② SubmitStatement(payload_ref) → Arbiter
//                 sequences it into L3 block N
//   Arbiter    ③ PinPayloads(REPLAY, scope=N, refs)       [lease + pin]
//                 — takes over retention from the lease; from here lease
//                 expiry is irrelevant
//   Verifier   ④ FetchPayloads(refs) during block-N replay (§3.5 step 3);
//                 verifies bytes against the SEQUENCED envelope's
//                 payload_hash + payload_length, replays, attests
//   Arbiter    ⑤ after quorum + L2 finality + safe watermark past N:
//                 PinPayloads(AUDIT, scope=N, refs) THEN
//                 ReleasePins(REPLAY, N)                  [pin(AUDIT)]
//                 — always pin-then-release; a covered ref never has a
//                 bare window
//   Arbiter    ⑥ audit window ends: ReleasePins(AUDIT, N) [zero holds]
//   store      ⑦ zero pins + expired lease → MAY collect  (never MUST;
//                 an immutable backend simply keeps serving)
//
// Failure exit: a payload whose statement is never sequenced is protected
// only by its lease; lease expiry is then the ONLY path to collectability.
//
// Result-code convention (the AdmissionCode pattern, one enum PER CONCERN):
// application outcomes return gRPC OK plus a per-RPC code (PutCode /
// FetchCode / StatCode / PinCode / ReleaseCode); gRPC errors are reserved
// for transport, auth, and request-level malformation. Every coded result
// message carries a human-readable `message` detail. All time fields are
// unix epoch milliseconds with an _unix_ms suffix.
//
// Idempotency keys (§10.3 conventions): PutPayloadInline / PutPayload —
// content identity (payload_hash, payload_length); a duplicate put drains
// and verifies the bytes, returns the original payload_ref, and REFRESHES
// the ingest lease — never an error. PinPayloads — per (holder_id, purpose,
// scope_key): refs union in; an identical re-pin is a no-op. ReleasePins —
// the same key; releasing an unknown or already-released key returns OK.
// GetStoreLimits / FetchPayloads / StatPayloads are reads.
package arbiter;
option go_package = "github.com/sentioxyz/arbiter-proto/gen/pb;pb";

// ============================================================
// Store limits
// ============================================================

message GetStoreLimitsRequest {}

// StoreLimits advertises deployment-configured operating limits so callers
// size frames and batches without magic numbers baked into binaries.
// Fetched once at process start; limits change only with deployment config
// (the same mechanism as the P5+ backend swap).
message StoreLimits {
  // Max payload bytes accepted by PutPayloadInline; larger payloads must use
  // the chunked PutPayload stream.
  uint64 max_inline_bytes = 1;
  // Hard cap on one PutPayloadFrame.chunk / FetchData.chunk. Sized to keep
  // frames far below gRPC's 4 MiB default while whole payloads (ClickHouse
  // batch INSERTs) may exceed it freely.
  uint64 max_chunk_bytes = 2;
  // Cap on refs per FetchPayloadsRequest / StatPayloadsRequest /
  // PinPayloadsRequest. Callers split larger blocks across calls; exceeding
  // it is request-level malformation (gRPC INVALID_ARGUMENT).
  uint32 max_batch_refs = 3;
  // Hard cap on a single payload's byte length.
  uint64 max_payload_bytes = 4;
}

// ============================================================
// Payload state
// ============================================================

// PayloadState is the availability lifecycle of one payload_ref. State is
// PER CONTENT IDENTITY (payload_hash, payload_length), not per put: a
// re-put of identical bytes after collection re-stores them and returns the
// SAME ref back in AVAILABLE — RELEASED is terminal only as-served, not
// forever. v1's synchronous local store never emits PENDING; async DA
// backends return OK + ref while dispersal is in flight, and clients MUST
// handle PENDING so the P5+ backend swap stays a configuration change.
enum PayloadState {
  PAYLOAD_STATE_UNSPECIFIED = 0;
  // Accepted, not yet retrievable (dispersal / inclusion in flight).
  PAYLOAD_STATE_PENDING = 1;
  // Retrievable now; the store is obligated to serve it while held.
  PAYLOAD_STATE_AVAILABLE = 2;
  // All pins gone, lease expired, bytes collected. The store no longer
  // promises retrievability under this ref (until an identical re-put).
  PAYLOAD_STATE_RELEASED = 3;
}

// ============================================================
// Put: HouseGate → store (§3.5 step 1, hot write path)
// ============================================================

// PutPayloadHeader declares what the put will carry. The declared pair
// (payload_hash, payload_length) is the put idempotency key AND the content
// identity the store dedupes on: the store SHOULD mint one stable ref per
// content identity. The store verifies the received bytes against the
// declaration BEFORE acking OK — this is the put integrity gate (service
// hygiene, not trust: readers still re-verify from the envelope, §13).
message PutPayloadHeader {
  // "0x" + hex(SHA-256) of the payload bytes — the DigestString profile,
  // byte-identical to StatementEnvelopeV2.payload_hash.
  string payload_hash = 1;
  // Exact payload byte length — StatementEnvelopeV2.payload_length.
  uint64 payload_length = 2;
}

// PutPayloadInlineRequest is the unary fast path for payloads at or under
// StoreLimits.max_inline_bytes — the dominant user-statement case. One
// round trip, zero stream ceremony; safe under channel retry policy because
// puts are content-idempotent.
message PutPayloadInlineRequest {
  PutPayloadHeader header = 1;
  bytes payload = 2;
}

// PutPayloadFrame is one frame of the chunked put stream: exactly one
// header first, then data chunks strictly in payload order (no offsets —
// sequential chunks remove reordering ambiguity). A broken stream is
// retried from zero on a new stream; if the earlier attempt committed, the
// server dedupes at commit and returns the original ref.
// (The oneof field is named "header", never "descriptor" — protoc-gen-go
// reserves Descriptor() on generated types.)
message PutPayloadFrame {
  oneof frame {
    PutPayloadHeader header = 1;
    // Next sequential slice, non-empty, at most max_chunk_bytes.
    bytes chunk = 2;
  }
}

// PutCode is the put outcome taxonomy.
enum PutCode {
  PUT_CODE_UNSPECIFIED = 0;
  PUT_CODE_OK = 1;
  // Received bytes do not match the declared (payload_hash, payload_length)
  // commitment. NOTHING is stored, no ref is minted, and the failure never
  // poisons other in-flight puts of the same digest. A caller-side spool
  // bug: surface loudly, never auto-retry with the same bytes.
  PUT_CODE_COMMITMENT_MISMATCH = 2;
  // Declared payload_length exceeds StoreLimits.max_payload_bytes.
  PUT_CODE_TOO_LARGE = 3;
  // PutPayloadInline with a payload over StoreLimits.max_inline_bytes.
  // Remedy: switch to the chunked PutPayload stream.
  PUT_CODE_INLINE_LIMIT_EXCEEDED = 4;
  // Protocol misuse: missing/duplicate header frame, chunk before header,
  // empty chunk, chunk over max_chunk_bytes, zero declared length.
  PUT_CODE_MALFORMED = 5;
}

// PutPayloadResult closes a put. Every successful put — INCLUDING a dedupe
// hit — refreshes the ingest lease; the dedupe fast path MUST still drain
// and verify the full stream against the declaration before acking OK, so a
// corrupt duplicate can never ride a valid ref. Concurrent first puts of
// one digest converge server-side to one stored copy: all conforming puts
// get OK + the same ref.
message PutPayloadResult {
  PutCode code = 1;
  // Human-readable detail; empty on OK.
  string message = 2;
  // Opaque ref for StatementEnvelopeV2.payload_ref; set only when code ==
  // OK. Callers MUST NOT parse it; equality is the only operation.
  string payload_ref = 3;
  // AVAILABLE from v1's synchronous store; PENDING from async DA backends
  // (the ref is already final — HouseGate may embed and submit immediately;
  // verifiers fetch at replay time, after inclusion).
  PayloadState state = 4;
  // Ingest-lease expiry (store clock). HouseGate MUST hold an unexpired
  // lease until SubmitStatement is acked; see the PayloadStore service
  // comment for the lease→pin handoff rule.
  uint64 lease_expires_unix_ms = 5;
  // Advisory, for metrics: identical content was already committed and
  // payload_ref is the original ref. Still code == OK — dedupe is success.
  bool deduplicated = 6;
}

// ============================================================
// Fetch: Verifier / P5+ challenger → store (§3.5 step 3, hot read path)
// ============================================================

// PayloadRange selects a byte range for resumable fetch: a verifier that
// lost its stream mid-block re-requests only the missing suffixes; P5+
// challengers can sample slices.
message PayloadRange {
  uint64 offset = 1;
  // Byte count; 0 means "to end of payload". A valid offset with a length
  // running past EOF is NOT an error: the store serves to EOF and reports
  // the actual count in FetchEnd.served_length.
  uint64 length = 2;
}

// FetchSpec names one payload (or range of it) to stream.
message FetchSpec {
  string payload_ref = 1;
  // Absent = the whole payload.
  PayloadRange range = 2;
}

// FetchPayloadsRequest asks for 1..max_batch_refs payloads on one server
// stream — a block has many statements, and per-payload RPC fan-out is the
// store's job, not the verifier's. Request-level malformation (over
// max_batch_refs, empty ref, zero specs) fails with gRPC INVALID_ARGUMENT
// before any frame is sent; everything per-ref is reported in FetchEnd.
// Backpressure is HTTP/2 flow control, never re-modeled in messages.
message FetchPayloadsRequest {
  repeated FetchSpec specs = 1;
}

// FetchBegin opens one ref's section. Deliberately hash-silent — carrying
// no server-asserted hash or length: readers MUST verify SHA-256(bytes) ==
// the SEQUENCED envelope's payload_hash and byte count == payload_length
// before any executor runs (§13); the store's word is never an input.
message FetchBegin {
  string payload_ref = 1;
}

// FetchData carries the next slice for one ref. Frames for a given ref are
// strictly ordered (offset is a resume/self-check aid, monotonically
// contiguous per ref); frames for DISTINCT refs MAY interleave so one slow
// backend object never head-of-line blocks the block fetch.
message FetchData {
  string payload_ref = 1;
  // Absolute byte offset of this chunk within the payload.
  uint64 offset = 2;
  // Non-empty, at most max_chunk_bytes.
  bytes chunk = 3;
}

// FetchCode is the per-ref fetch outcome taxonomy.
enum FetchCode {
  FETCH_CODE_UNSPECIFIED = 0;
  FETCH_CODE_OK = 1;
  // Never stored under this ref. During replay of a pinned block this is an
  // availability incident to escalate, not a retry case (§13).
  FETCH_CODE_NOT_FOUND = 2;
  // Stored but not yet retrievable (async backend dispersal in flight).
  // Retry with backoff. v1's local store never emits this; clients MUST
  // handle it anyway (P5+ swap is config, not protocol).
  FETCH_CODE_PENDING = 3;
  // Was stored, then legitimately collected (zero pins, lease expired).
  // Distinguishes "aged out per policy" from an availability fault.
  FETCH_CODE_RELEASED = 4;
  // Requested range offset is at or beyond the stored payload's length.
  FETCH_CODE_OFFSET_OUT_OF_RANGE = 5;
}

// FetchEnd closes one ref's section: OK after all bytes, or a per-ref
// failure (NOT_FOUND / PENDING / RELEASED / OFFSET_OUT_OF_RANGE) sent
// without Begin when nothing was served. Per-ref isolation means one
// missing payload cannot mask the availability of the rest of the block.
message FetchEnd {
  string payload_ref = 1;
  FetchCode code = 2;
  // Bytes actually served for this spec (a range running past EOF serves to
  // EOF and reports the truncated count here, code OK).
  uint64 served_length = 3;
  // Human-readable detail; empty on OK.
  string message = 4;
}

// FetchFrame is the fetch stream element. Per requested ref the server
// sends Begin, Data*, End — exactly one End per spec; the gRPC stream
// closes OK after the last End. A transport error mid-stream is resumed by
// re-requesting unfinished specs with ranges.
message FetchFrame {
  oneof frame {
    FetchBegin begin = 1;
    FetchData data = 2;
    FetchEnd end = 3;
  }
}

// ============================================================
// Stat: any caller → store (advisory probe)
// ============================================================

// StatCode is the per-ref stat outcome taxonomy.
enum StatCode {
  STAT_CODE_UNSPECIFIED = 0;
  STAT_CODE_OK = 1;
  // Never stored under this ref.
  STAT_CODE_NOT_FOUND = 2;
}

// StatPayloadsRequest checks presence/state without moving bytes — verifier
// prefetch planning, orchestrator pre-dispatch gating, ops probes. Bounded
// by max_batch_refs; exceeding it fails with gRPC INVALID_ARGUMENT.
message StatPayloadsRequest {
  repeated string payload_refs = 1;
}

// PayloadStat is one ref's ADVISORY answer — a point-in-time observation,
// not a reservation: state may change between Stat and Fetch. Hash-silent
// like every read.
message PayloadStat {
  StatCode code = 1;
  // Human-readable detail; empty on OK.
  string message = 2;
  // Set only when code == OK: PENDING / AVAILABLE / RELEASED.
  PayloadState state = 3;
}

message StatPayloadsResult {
  // stats[i] corresponds to StatPayloadsRequest.payload_refs[i].
  repeated PayloadStat stats = 1;
}

// ============================================================
// Pins: Arbiter leader orchestrator → store (retention refcount)
// ============================================================

// PinPurpose names WHY bytes are held, gating per-purpose store policy and
// making retention audit-readable.
enum PinPurpose {
  PIN_PURPOSE_UNSPECIFIED = 0;
  // The containing block has not completed quorum replay (§7.1). Placed by
  // the orchestrator at sequencing time, scope_key = decimal block_seq.
  PIN_PURPOSE_REPLAY = 1;
  // Safe watermark passed; audit-retention window running (§8.5).
  PIN_PURPOSE_AUDIT = 2;
  // Operator legal hold / archive; blocks collection until explicitly
  // released, by design.
  PIN_PURPOSE_ARCHIVE = 3;
}

// PinKey is a pin's identity and its idempotency key. One logical pin per
// key; the pin's ref set only ever GROWS until the whole pin is released.
message PinKey {
  // Cluster-stable logical holder (e.g. "arbiter"), never a per-leader
  // instance id — a failed-over leader re-asserts the SAME pins instead of
  // orphaning its predecessor's.
  string holder_id = 1;
  PinPurpose purpose = 2;
  // Pin scope, e.g. the decimal block_seq for REPLAY/AUDIT pins.
  string scope_key = 3;
}

// PinPayloadsRequest unions payload_refs into the pin identified by key —
// ADD-ONLY, no replace semantics exists: re-pinning the same (key, refs) is
// an idempotent no-op, and a retry after partial failure simply re-unions.
// Bounded by max_batch_refs (gRPC INVALID_ARGUMENT if exceeded; empty
// holder_id / UNSPECIFIED purpose likewise).
message PinPayloadsRequest {
  PinKey key = 1;
  repeated string payload_refs = 2;
}

// PinCode is the per-ref pin outcome taxonomy.
enum PinCode {
  PIN_CODE_UNSPECIFIED = 0;
  // The hold is durable. Pinning a PENDING ref is OK — the obligation is
  // recorded; the store must serve it once dispersal completes.
  PIN_CODE_OK = 1;
  // Never stored under this ref. For a block being pinned for replay this
  // is an availability incident to escalate, not to retry (§13).
  PIN_CODE_NOT_FOUND = 2;
  // Already collected; a pin cannot resurrect bytes. Same escalation.
  PIN_CODE_RELEASED = 3;
}

// PinRefResult is one ref's pin outcome.
message PinRefResult {
  string payload_ref = 1;
  PinCode code = 2;
  // Human-readable detail; empty on OK.
  string message = 3;
}

message PinPayloadsResult {
  // results[i] corresponds to PinPayloadsRequest.payload_refs[i].
  repeated PinRefResult results = 1;
  // Human-readable request-level detail; empty when all refs are OK.
  string message = 2;
}

// ReleaseCode is the release outcome taxonomy.
enum ReleaseCode {
  RELEASE_CODE_UNSPECIFIED = 0;
  // The pin is gone (or never existed — release is idempotent).
  RELEASE_CODE_OK = 1;
  // authority_jws verification is enabled and failed (missing, malformed,
  // wrong purpose/domain, or recovered address not on the allowlist).
  RELEASE_CODE_UNAUTHORIZED = 2;
}

// ReleasePinsRequest releases ONE WHOLE PIN by exact key — never a raw ref.
// Release is PERMISSION, not command: after the last pin on a ref goes (and
// its lease has expired) the store MAY collect, MAY retain; an immutable
// backend that never collects is conforming. Releasing an unknown or
// already-released key returns OK.
message ReleasePinsRequest {
  PinKey key = 1;
  // Authority slot on the destructive RPC (the PromotionCommand.authority_jws
  // precedent, §8.1): the Arbiter authority's secp256k1 JWS (compact form)
  // whose payload carries purpose "arbiter-payload-release" and a
  // CanonicalDigest of the key under domain "arbiter-payload-release-v1";
  // the store authorizes by address recovery against its authority
  // allowlist. v1 MAY run channel-trust with this empty — enforcement is
  // store configuration, so P5+ hardening is config, not protocol.
  string authority_jws = 15;
}

message ReleasePinsResult {
  ReleaseCode code = 1;
  // Human-readable detail; empty on OK.
  string message = 2;
}

// ============================================================
// Services. Split by caller (the arbiter.proto convention) so deployments
// apply distinct authn/QoS per plane: PayloadStore is data-plane
// (latency-class ingest, throughput-class fetch); PayloadLifecycle is
// control-plane. Both may be served by one process (v1).
// ============================================================

// PayloadStore is called by HouseGate at ingest (§3.5 step 1, before
// SubmitStatement) and by Verifiers at replay (step 3).
//
// INGEST-LEASE HANDOFF RULE (the put→sequenced bridge): every successful
// put — including a dedupe hit — grants/refreshes an ingest lease on the
// payload's content identity. HouseGate MUST hold an unexpired lease from
// Put until its SubmitStatement is acked; at sequencing time the Arbiter
// orchestrator covers the block's refs with a pin (purpose REPLAY,
// scope_key = decimal block_seq), taking over retention. The lease is NOT a
// pin: it has no holder identity, so a concurrent duplicate put from
// another node can only EXTEND protection — it can never steal, replace, or
// shorten anything.
service PayloadStore {
  // Deployment limits; fetched once at process start. Read-only.
  rpc GetStoreLimits (GetStoreLimitsRequest) returns (StoreLimits) {}
  // Unary fast path for payloads <= max_inline_bytes (the dominant case).
  // Idempotency key: (payload_hash, payload_length).
  rpc PutPayloadInline (PutPayloadInlineRequest) returns (PutPayloadResult) {}
  // Chunked client stream for larger payloads: one header frame, then
  // sequential chunks; commit is atomic after the integrity gate.
  // Idempotency key: (payload_hash, payload_length).
  rpc PutPayload (stream PutPayloadFrame) returns (PutPayloadResult) {}
  // Batch fetch: per-ref Begin/Data*/End sections, ordered per ref,
  // interleaved across refs; per-ref failures isolated in FetchEnd.
  // Hash-silent. Read-only.
  rpc FetchPayloads (FetchPayloadsRequest) returns (stream FetchFrame) {}
  // Advisory batch presence/state probe; bounded by max_batch_refs (gRPC
  // INVALID_ARGUMENT if exceeded). Read-only.
  rpc StatPayloads (StatPayloadsRequest) returns (StatPayloadsResult) {}
}

// PayloadLifecycle is called by the Arbiter leader orchestrator ONLY — pin
// timing is an orchestrator decision outside Apply, like every other side
// effect (§4.1). Pins are the system's retention refcount: GC needs zero
// pins AND an expired lease; there is no Delete RPC anywhere.
service PayloadLifecycle {
  // Idempotency key: (key.holder_id, key.purpose, key.scope_key); refs
  // union in, add-only.
  rpc PinPayloads (PinPayloadsRequest) returns (PinPayloadsResult) {}
  // Idempotency key: the same key; releasing an unknown or already-released
  // key returns OK. Carries the authority slot (see ReleasePinsRequest).
  rpc ReleasePins (ReleasePinsRequest) returns (ReleasePinsResult) {}
}
```
