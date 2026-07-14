# DA payload store gRPC 合约设计（`proto/da.proto`）

- **日期**：2026-07-14
- **状态**：已评审（含对抗校验修订），待实施
- **上游设计**：housegate `docs/superpowers/specs/2026-06-30-sentio-arbiter-design.md`（§3.5 数据流、§5.2 锚定、§14 Q6 开放问题）；base-spec `2026-06-22-storage-integrity-design.md`（§4 trust boundary、§15 Q6）
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
2. 「谁在什么时候可以删」有协议背书；「payload 存活到 quorum replay 完成」从概率性变为**可证明**（custody chain）；
3. P5+ 换 Celestia / EigenDA / EIP-4844 后端时**仅配置变更、非协议变更**。

**非目标**：proof-of-custody 与 retention window（设计 doc §14 Q6，其自身引
base-spec §15 Q6——两份 spec 里都是 open question，本合约刻意不回答，这是本稿的
设计立场而非 spec 规定的推迟）；block 级 `AnchorRef.da_ref` 方案（§5.2，P5+）；
store 的具体实现与部署拓扑。

## 设计过程

四个并行设计代理按不同哲学（极简 / DA-native / 生命周期优先 / 热路径优先）各出完整
草案，每案由对抗校验代理按 7 条冻结约束试图击穿；综合方案以 DA-native 案的语义骨架
（异步状态机 + hold 记账 + release-as-permission）为底，嫁接热路径案的读写形态
（inline put + 帧交错批量 fetch），生命周期收敛为 Pin/Release 两个动词。综合稿再经
一轮 4 轴对抗校验（冻结约束 / 生命周期攻击 / buf 机械 / 仓库惯例），11 项
blocking/fixable 全部修入本稿——最重要的是把 lease→pin 交接升级为完整的
**custody chain**（新增 SEQUENCED pin purpose）、`FetchFrame` 按 `spec_index` 归属、
`authority_jws` 增加 release_seq 防重放、pinned+PENDING 的可取回义务。修订稿复检
再发现 4 项 fixable 残留，同样修入：监护屏障绑定**每一次** ack 发射（含幂等
duplicate-SubmitStatement 回放）、`FETCH_CODE_INTERNAL` 补齐 per-spec 故障隔离、
`PayloadLifecycle` 调用方与 ARCHIVE 的矛盾消解。`buf lint` / `buf build` 已实测
通过，与现有 `package arbiter` 三个文件无消息名 / 枚举值冲突。

## 架构与调用方

payload store 是**被动的数据面服务器**：所有人拨号它，arbiter 设计 spec §11.1 的
"data plane dials the Arbiter" 方向约定在这里不适用，也没有 NotLeader 协议
（服务可以无 leader、水平扩展）。

| 服务 | 调用方 | 路径 |
|---|---|---|
| `PayloadStore` | HouseGate（ingest，SubmitStatement 之前）；Verifier（replay） | 数据面热路径 |
| `PayloadLifecycle` | Arbiter leader orchestrator（SEQUENCED/REPLAY/AUDIT）；operator 管理工具（ARCHIVE legal hold） | 控制面（retention refcount） |

信任模型（设计 spec §3.5 timing："v1 uses a trusted payload store"；base-spec §4
trust boundary）：store 只被信任 **availability**，从不被信任 integrity。读路径在
结构上强制这一点——**hash-silent**：任何 fetch/stat 响应都不携带 server 断言的
content hash 或 length，读者只能拿 sequenced envelope 的 `payload_hash +
payload_length` 自己校验，在任何 executor 运行之前完成（pkg/replay verifier
invariant，见 replay.proto `Statement.payload_ref` 注释）。写入时的 digest 校验是
ingest 卫生（提前抓住写坏的 spool），不是给读者的完整性承诺。

本服务解析的是 **per-statement** 的 `StatementEnvelopeV2.payload_ref`。

## 端到端数据流（put → pin → fetch → release）

```
HouseGate  ① PutPayloadInline / PutPayload             [lease]
           ② SubmitStatement(payload_ref) → Arbiter
Arbiter    ③ statement 被 sequenced；在 ack 之前：
              PinPayloads(SEQUENCED, scope=statement_seq, ref)
              落库后才发 SequencedAck                   [lease + pin]
              —— 从此 lease 过期无关紧要
Arbiter    ④ SealL3Block(N)：PinPayloads(REPLAY, scope=N, refs) 落库后
              才逐个 ReleasePins(SEQUENCED, …)          [pin(REPLAY)]
Verifier   ⑤ replay block N 时 FetchPayloads(refs)；对 sequenced envelope 的
              payload_hash + payload_length 校验字节，replay，签名 attest
Arbiter    ⑥ quorum replay 完成 + 每个已 dispatch 的 verifier 已上报或按
              明示策略超时 + L2 finality + safe watermark 推过 N：
              先 PinPayloads(AUDIT, N) 再 ReleasePins(REPLAY, N)
              —— 永远先上新锁再解旧锁                   [pin(AUDIT)]
Arbiter    ⑦ 审计窗口结束：ReleasePins(AUDIT, N)        [零 hold]
store      ⑧ 零 pin + lease 过期 → MAY collect（从不 MUST；不可删除的后端
              继续照常服务即为合规）
```

失败出口：statement 永远没被 sequenced 时，payload 只受 lease 保护，lease 过期是它
变得可回收的唯一路径。

## 消息与服务参考

**PayloadStore（5 个 RPC）**

- `GetStoreLimits → StoreLimits{max_inline_bytes, max_chunk_bytes, max_batch_refs, max_payload_bytes, ingest_lease_ms}`：部署级限制，进程启动时取一次，改限制走部署配置（与 P5+ 后端切换同一机制）。`ingest_lease_ms` 公布 lease 时长，供 HouseGate 给 put→SubmitStatement 窗口和 re-put 刷新节奏做预算——但 sequenced payload 的保留从不依赖它（见 custody chain）。
- `PutPayloadInline(header + payload) → PutPayloadResult`：≤ `max_inline_bytes` 的小 payload 一次往返（主导场景，热路径）。
- `PutPayload(stream PutPayloadFrame) → PutPayloadResult`：大 payload 的 chunked client stream；首帧 header（声明 hash+length），之后按序 chunk，无 offset（严格顺序消除重排歧义）。oneof 字段名是 `header`，刻意避开 `descriptor`（protoc-gen-go 的 `Descriptor()` 冲突）。
- `FetchPayloads(specs) → stream FetchFrame`：批量读，`FetchSpec{payload_ref, PayloadRange{offset, length}}`；每个 spec 的帧序为 `FetchBegin{ref, spec_index} → FetchData{ref, spec_index, offset, chunk}* → FetchEnd{ref, spec_index, code, served_length, message}`；单 spec 内严格有序，不同 spec 之间**可以交错**（慢对象不会队头阻塞整个 block 的拉取）。**帧按 `spec_index`（specs 数组下标）归属**，不按 ref：同一请求里允许两个 spec 指向同一 ref 的不同 range（断流后重拉 missing suffixes、P5+ challenger 采样 slices），线路上不产生归属歧义，"exactly one End per spec_index" 可精确表达。断流后用 range 只重拉未完成的 spec。
- `StatPayloads(refs) → StatPayloadsResult`：advisory 的批量 presence/state 探测（时点观察，不是预留），受 `max_batch_refs` 约束。

**PayloadLifecycle（2 个 RPC）**

- `PinPayloads(PinKey{holder_id, purpose, scope_key}, refs)`：把 refs **union** 进该 key 的 pin，add-only，**没有 replace 语义**；同 (key, refs) 重放是幂等 no-op。
- `ReleasePins(PinKey, authority_jws)`：按**整 key** 释放整个 pin，永远不按裸 ref 释放；未知/已释放的 key 返回 OK。释放顺序义务在调用方：custody chain 要求后继 hold 先落库。

`PutPayloadResult` 携带 `payload_ref`（不透明、URI-scheme-friendly、禁止解析）、
`state`（AVAILABLE/PENDING）、`lease_expires_unix_ms`、advisory 的 `deduplicated`。

## 结果码 taxonomy

沿用 AdmissionCode 先例：**每个关注点一个 enum**，绝不共享一个大 enum；零值一律
`_UNSPECIFIED`；应用级结果走 gRPC OK + code，传输失败走 gRPC error；每个带 code 的
结果消息都带人类可读 `message`；所有时间戳字段 `uint64` unix 毫秒、`_unix_ms` 后缀。

- `PutCode`：`OK / COMMITMENT_MISMATCH / TOO_LARGE / INLINE_LIMIT_EXCEEDED / MALFORMED`
- `FetchCode`（per-spec，在 `FetchEnd` 中）：`OK / NOT_FOUND / PENDING / RELEASED / OFFSET_OUT_OF_RANGE / INTERNAL`（INTERNAL = store 内部读故障，唯一帧后才可判定的码——可能跟在部分 FetchData 之后，`served_length` 报已出字节、range 续传；它的存在让盘坏/后端对象损坏保持 per-spec 隔离，而不是杀整条流的 gRPC error；pinned ref 上持续 INTERNAL 与 NOT_FOUND 同级事故）
- `StatCode`：`OK / NOT_FOUND`（存在时 state 给出 PENDING/AVAILABLE/RELEASED）
- `PinCode`（per-ref）：`OK / NOT_FOUND / RELEASED`（后两者对 replay pin 是 availability incident，升级处理而非重试——这是本设计的事故分级决定，两份 spec 均无现成条目可引）
- `ReleaseCode`：`OK / UNAUTHORIZED`（含 anti-replay：release_seq 不高于水位线也算 UNAUTHORIZED）

请求级 malformation 与 per-ref/per-spec 失败严格分层：超 `max_batch_refs`、空 ref、
零 specs 等在发出任何帧之前以 gRPC `INVALID_ARGUMENT` 拒绝；per-spec 失败隔离在
`FetchEnd` / `PinRefResult` 里，一个 ref 的缺失不掩盖同 block 其余 ref 的可用性。

Put 完整性门（ingest 卫生，非读者信任）：store 在 ack OK **之前**把收到的字节对声明
的 `(payload_hash "0x"+hex sha256 DigestString profile, payload_length)` 校验；
不匹配 → `PUT_CODE_COMMITMENT_MISMATCH`，什么都不存、不铸 ref。dedupe 快路径
**仍必须先 drain 并校验整条流**再 ack OK（否则损坏的重复上传会静默借用合法 ref）。
同一 digest 的并发首次 put 在服务端收敛为一份存储，所有合规 put 拿到 OK + 同一
ref；失败的 put 永不污染其他 in-flight put。

## 生命周期语义

### custody chain：lease → SEQUENCED pin → REPLAY pin → AUDIT pin

payload "provably survive until quorum replay" 靠一条**监护链**：每次保留责任
转移时，**后继 hold 必须先在 store 落库（durable），前任才允许释放/失效**——
pin-before-release 覆盖所有 purpose 转移，lease 边界上则是 pin-before-ack：

1. **lease（put → ack）**：每次成功 put——**包括 dedupe 命中**——都授予/刷新
   ingest lease（`lease_expires_unix_ms`，按 content identity 记账，时长公布在
   `StoreLimits.ingest_lease_ms`）。HouseGate 必须持有未过期 lease 直到
   `SubmitStatement` 被 ack；刷新原语是 duplicate put（HouseGate 在 ack 前
   保留 spool 字节正是为此），submit 拖过预算就先 re-put；崩溃后从持久 spool
   redrive 未 ack 的 statement（先 re-put 再 re-submit），lease 先于任何 ack
   恢复。
2. **SEQUENCED pin（ack → seal）**：statement 被 sequenced 后，orchestrator
   **先**把 pin（purpose `SEQUENCED`，`scope_key` = 十进制 `statement_seq`）
   落库，**然后才发 SequencedAck**——ack 是终结 HouseGate lease 义务的监护
   屏障，绝不允许跑到 pin 前面。pin 放置仍是 Apply 之外的 orchestrator side
   effect（§4.1），只是 ack 这个 side effect 被排在它后面。屏障绑定**每一次**
   ack 发射，含幂等 duplicate-SubmitStatement 回放（arbiter.proto：重复提交
   返回原结果）：failover 后新 leader 应答 redriven submit 时，必须先幂等
   重申 SEQUENCED pin 落库、再回放原 ack——任何 ack 路径（首发或回放）都
   不能跑到 pin 前面。
3. **REPLAY pin（seal → quorum+verifiers）**：`SealL3Block` 时（block_seq
   只在 seal 时才存在，所以 REPLAY pin 只能在 **block-seal time** 放置）
   orchestrator 用 pin（purpose `REPLAY`，`scope_key` = 十进制 `block_seq`）
   覆盖该 block 全部 refs，落库后才逐个释放所辖 statement 的 SEQUENCED pin。
   REPLAY pin 持有到：block 完成 quorum replay **且**每个已 dispatch 的
   verifier 都已上报或按明示策略超时——慢的/持异议的 verifier 必须还能取到
   支撑其签名 ExecutionReceipt 挑战证据的字节。
4. **AUDIT pin（watermark 之后）**：AUDIT pin 落库之后才释放 REPLAY pin
   （pin-then-release）；audit window 结束后释放 AUDIT pin → 零 holds。

于是从 SequencedAck 起，sequenced payload 永远被至少一个 durable pin 覆盖，
**从不靠残余 lease TTL 过桥**：任意时长的 leader failover 都打不开 GC 窗口，
新 leader 幂等地重申监护链要求存在的所有 pin（sequenced-but-unsealed 的
SEQUENCED pin、尚在 replay hold 内的 REPLAY pin），此时遇到
`PIN_CODE_NOT_FOUND / RELEASED` 即为 store 故障级事故（因为 pin 在对应
ack/release 之前就已 durable，不存在合法竞态）。statement 永远没被 sequenced
的 payload 才走 lease 过期这条唯一的可回收路径。

关键设计点：**lease 不是 pin，没有 holder 身份**。跨节点的重复 put 只能
*延长*保护，不可能偷走或替换任何东西——这消灭了「重复 put 抢占/替换 hold」
一类的竞态。

### pins 是 refcount

内容寻址 dedupe 是合约要求（store **MUST** 按 `(payload_hash, payload_length)`
content identity 铸**唯一稳定 ref**——ref 稳定性是幂等 put、并发收敛、
RELEASED→re-put 恢复三条路径共同的承重墙，降为 SHOULD 会全部击穿），于是保留
必须靠显式计数：**GC 仅当某 ref 零 pin 且无未过期 lease 时才被允许**。pin 以
`(holder_id, purpose ∈ {SEQUENCED, REPLAY, AUDIT, ARCHIVE}, scope_key)` 为
key，add-only；`holder_id` 必须是 cluster-stable 的逻辑身份（如 `"arbiter"`），
绝不能用 per-leader 实例 id，failover 后新 leader 重申同一批 pin 而不是
孤儿化前任的。

### release 是许可，不是命令

released 意味着 store **MAY** collect，从不 MUST。不能删除的后端（链上 DA）是一个
「永不回收」的合规 store。合规性落在 **composite store SERVICE**（gateway +
backend）上：物理层自行修剪时（EIP-4844 blob 约 18 天），服务必须在 holds 存续期间
保留自己的副本——被 pin 的 payload 必须保持可取回，这个义务在服务身上，不在链上。
**删除没有 RPC**：不存在任何人可调用的删字节动词。

**pinned + PENDING 的可取回性**：对 PENDING ref 打 pin 返回 `PIN_CODE_OK` 的
那一刻起，composite service 必须**无视后端 dispersal 状态**、从自己在 put 时
校验并持有的副本直接服务该 ref——`FETCH_CODE_PENDING` 只允许出现在**无 pin
覆盖**的 ref 上。dispersal 永久失败（如 DA quorum failure）是 store 内部的
修复作业，永远不会表现为 pinned ref 的不可用，replay 也就不存在无限退避
自旋的暗路径。

### 破坏性 RPC 上的 authority slot

`ReleasePinsRequest.authority_jws` 沿用 `PromotionCommand.authority_jws` 先例
（§8.1）：secp256k1 JWS compact form，payload 携带 purpose
`"arbiter-payload-release"`、CanonicalDigest domain
`"arbiter-payload-release-v1"` 下的 key 摘要、**以及严格单调的
`release_seq`**；store 按 address recovery 对 authority allowlist 授权，并
按 authority 地址持久化已接受的最高 release_seq 水位线（对齐先例的
§8.3 stale-command 拒绝 + §10.3 幂等锚）。seq 不高于水位线 →
`RELEASE_CODE_UNAUTHORIZED`；唯一例外是逐字节重放上一枚已接受的 JWS——幂等
重试，返回 OK 无副作用。必须有 seq 的原因：PinKey 是稳定坐标（failover 后
重申的是同一 key），没有 seq 则一枚历史 release JWS 对该 key 之后的任何
重申永久有效——对 ARCHIVE legal hold 是致命的。v1 MAY 以空值跑
channel-trust——字段和校验规则**现在**就写进协议，P5+ 加固只是配置开关，
不是协议变更。

## 兼容性与 P5+ 切换

- `payload_ref` 不透明、URI-scheme-friendly、put 时铸造、永不变化；合约里没有任何链的 namespace/height/quorum 概念。Celestia / EigenDA / EIP-4844 到来时是「新 ref scheme + store 配置」。
- **异步可用性已预埋**：`PayloadState{UNSPECIFIED, PENDING, AVAILABLE, RELEASED}`。异步后端在 dispersal 进行中即返回 OK + ref（state=PENDING）；对**无 pin 覆盖**的 PENDING ref 的 Fetch 得到 per-spec `FETCH_CODE_PENDING`（退避重试）；被 pin 的 PENDING ref 必须直接从服务副本服务。v1 本地 store 永不发出 PENDING，但客户端**必须**处理它——后端切换才是配置而非协议。
- state 是 **per-content-identity** 的：RELEASED 只是 terminal-as-served；回收后对相同字节重新 put 会重新存储并把**同一个 ref**（MUST 级稳定）带回 AVAILABLE（消除「RELEASED 终态」与内容寻址 dedupe 的状态机矛盾，也让已 commit statement 的 payload 可由任何持字节者恢复）。
- 本文件不 import arbiter.proto / replay.proto，跨边界只有不透明字符串（`payload_ref`、`payload_hash`），后端切换时 store 合约可整体拆卸。
- `StatementEnvelopeV2.payload_ref/payload_hash/payload_length`、`replay.Statement`、`AnchorRef.da_ref` 全部**不动**。

## 错误处理

分三层：

1. **gRPC error**：传输、鉴权、请求级 malformation（超 `max_batch_refs`、空 ref、零 specs → `INVALID_ARGUMENT`，且发生在任何帧之前）。channel 层重试策略只作用于这一层；put 是 content-idempotent，重试恒安全。
2. **OK + code**：应用级结果。dedupe 命中仍是 `PUT_CODE_OK`（`deduplicated` 仅供 metrics）。
3. **per-spec/per-ref 隔离**：Fetch/Pin 的单项失败装在 `FetchEnd` / `PinRefResult` 里，不杀整条流/整个批。

几个刻意的语义边界：

- range 的 offset 合法但 length 越过 EOF → **不是错误**，served 到 EOF 并在 `FetchEnd.served_length` 报实际字节数；`offset >= payload length` → `OFFSET_OUT_OF_RANGE`。
- `FETCH_CODE_NOT_FOUND` vs `FETCH_CODE_RELEASED`：从未存过 vs 曾存在、按规则回收——把「availability fault（可升级/可挑战）」和「按策略合法老化」区分开。但 custody chain 下，任何本应有 hold 覆盖的 ref（replay 中的 pinned block、已 dispatch 的 verifier 的取证）遇到 NOT_FOUND/RELEASED 都是事故升级，不是重试，也不是「合法老化」——那是协议被打破的保留链，不是良性过期（事故分级是本设计的决定，非 spec 引文）。
- `PUT_CODE_COMMITMENT_MISMATCH` 是调用方 spool bug：大声上报，绝不带同样的字节自动重试。

## 测试与 conformance 备注

- **PENDING 路径防腐**：v1 本地 store 永不发 PENDING，暗路径必然烂掉。要求提供一个 fault-injecting dev store（配置开关），强制对指定比例/指定 ref 发 `PAYLOAD_STATE_PENDING` 与 `FETCH_CODE_PENDING`，verifier 与 orchestrator 的 CI 必须在该 store 上跑通退避重试路径。另测 **pinned+PENDING**：对 PENDING ref 打 pin 后 fetch 必须直接出字节（永不 PENDING），包括注入「dispersal 永久失败」的场景。
- **custody chain 交接测试**（crash walk）：
  - put →（不 SubmitStatement）→ lease 过期 → GC 允许（唯一合法回收路径）；
  - put → sequenced → SEQUENCED pin 落库 → ack → **任意时长** failover/停摆 → lease 早已过期 → fetch 仍 OK（链不依赖时钟）；
  - ack 之前 crash：HouseGate 未收 ack，从持久 spool redrive（re-put 刷新 lease → re-submit）→ 新 leader 先 pin 后 ack；**duplicate 路径同样受屏障约束**：statement 已在 Raft 落定但 pin 未落库时收到 redriven submit，必须先重申 pin 再回放原 ACCEPTED ack（幂等回放不得绕过监护屏障）；
  - **INTERNAL 隔离测试**：批量 fetch 中注入单个 ref 的后端读故障（部分字节后断）→ 该 spec 收 `FETCH_CODE_INTERNAL` + 准确 `served_length`，同流其余 spec 正常完成；range 续传接上缺口；
  - seal：REPLAY pin 落库前释放 SEQUENCED pin 必须被 conformance 测试拒绝；quorum 后 AUDIT pin 落库前释放 REPLAY pin 同理（pin-then-release 断言）；
  - 已 dispatch、未上报的 verifier 在 quorum 完成后仍能 fetch（REPLAY hold 延至全员上报/超时），异议者能取到 ExecutionReceipt 挑战证据的字节；
  - 跨节点重复 put：node B 对同 digest 的 put 只能延长 lease，不得影响 node A 的在途流程。
- **dedupe 完整性测试**：先存好某 digest，再以同 header 发一条尾部损坏的流——必须 `COMMITMENT_MISMATCH`，且原 ref 依旧可取（dedupe 快路径必须 drain+verify）。并发首 put 同 digest：全部 OK + 同 ref，只落一份（ref 稳定性是 MUST，测试断言等号）。RELEASED 后 re-put 相同字节 → 同一 ref 回 AVAILABLE。
- **read-path hash-silent 断言**：conformance 测试对生成的 descriptor 做静态检查，`FetchBegin/FetchData/FetchEnd/PayloadStat` 中不得出现任何 `*hash*` / `payload_length` 字段（`served_length` 是服务量，不是内容断言）——防止后人「顺手」加回 server 断言字段。
- **spec_index 归属测试**：一个请求里两个 spec 指向同一 ref 的不同 range → 两组独立的 Begin/Data/End，各自 `spec_index` 正确、每个 spec_index 恰好一个 End，交错情况下也不歧义。
- **pin add-only 断言**：`PinPayloads(key, {r1})` 后 `PinPayloads(key, {r2})`，释放前 r1、r2 都必须受保护（union，非 replace）；`ReleasePins` 只认整 key，重复释放返回 OK。
- **边界码测试**：`offset == length` → `OFFSET_OUT_OF_RANGE`；`offset < length, offset+range.length > length` → OK + 截断的 `served_length`；批量超 `max_batch_refs` → 帧前 `INVALID_ARGUMENT`。
- **authority_jws 开关与防重放测试**：channel-trust 模式空 JWS 放行；enforcement 模式下空/坏 JWS → `RELEASE_CODE_UNAUTHORIZED` 且 pin 原样保留；release(seq=n) 成功 → 重申同一 PinKey → 重放旧 JWS(seq=n) → `UNAUTHORIZED` 且 pin 保留；逐字节重放上一枚已接受 JWS → 幂等 OK。
- 与 repo 现有 CI 对齐：`buf lint`（BASIC，除 PACKAGE_DIRECTORY_MATCH）与 `buf breaking` 已验证通过。

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
// TRUST MODEL (design §3.5: "v1 uses a trusted payload store"; base-spec §4
// trust boundary): the store is trusted for AVAILABILITY ONLY, never for
// integrity. Structurally enforced: the read path is HASH-SILENT — no fetch
// or stat response ever asserts a content hash or length, so no caller can
// be tempted to trust the store's word over the envelope's. The put-time
// digest check below is ingest hygiene (catches a corrupting spool before
// sequencing), not an integrity promise to readers — readers re-verify
// against the sequenced envelope before any executor runs (the pkg/replay
// verifier invariant).
//
// SCOPE (v1): one trusted composite store SERVICE (gateway + backend);
// caller identity is trusted from the gRPC channel (the RCRecord stance).
// This service resolves the PER-STATEMENT payload_ref carried by
// StatementEnvelopeV2 / replay.Statement. The block-level AnchorRef.da_ref
// scheme (§5.2) is P5+ and out of scope here. Proof-of-custody and the
// retention window are an OPEN QUESTION (§14 Q6); this contract
// deliberately does not answer it — a design stance of this draft, not a
// spec-mandated deferral. The §11.1 dial-the-Arbiter direction convention
// does NOT apply: the store is a passive data-plane server — every caller
// dials it, and there is no NotLeader protocol.
//
// BACKEND-SWAP RULE (P5+): payload_ref is an opaque, URI-scheme-friendly
// string callers MUST NOT parse; nothing below assumes synchronous
// availability (see PAYLOAD_STATE_PENDING) or any commitment format.
// Celestia / EigenDA / EIP-4844 blobs arrive as a new ref scheme plus store
// configuration, not a protocol change.
//
// LIFECYCLE MODEL — "who may delete when" has protocol backing:
//   1. Every successful put grants an INGEST LEASE (a TTL on the payload's
//      content identity, refreshed by duplicate puts; duration advertised
//      as StoreLimits.ingest_lease_ms). The lease bridges the window
//      between spool and sequencing; it has NO holder identity.
//   2. PINS are the refcount. A pin is keyed (holder_id, purpose, scope_key)
//      and is ADD-ONLY: re-pinning unions refs in; there is no replace.
//      ReleasePins releases by exact key — the whole pin, never a raw ref.
//   3. GC is PERMITTED only when a ref has zero pins AND no unexpired lease.
//      Release grants permission to collect, never commands erasure: a
//      backend that cannot delete (chain DA) is a conforming store that
//      never collects. Conversely, if the physical layer prunes on its own
//      (EIP-4844 blobs, ~18 days), the composite SERVICE must retain its own
//      copy while holds exist — a pinned payload MUST stay retrievable
//      (regardless of backend dispersal state; see PAYLOAD_STATE_PENDING),
//      and that obligation sits on the service, not the chain.
//   4. Deletion is NOT an RPC. There is no verb anyone could call to erase
//      bytes; collection is an internal store decision gated by rule 3.
//
// CUSTODY-CHAIN RULE — what makes survival-until-quorum-replay PROVABLE
// rather than probabilistic: whenever retention responsibility transfers,
// the SUCCESSOR hold MUST be durable in the store BEFORE the predecessor is
// released or allowed to lapse. Concretely:
//   lease     → SEQUENCED pin : the orchestrator makes the per-statement
//               SEQUENCED pin durable BEFORE the SubmitStatement ack is
//               sent — the ack is the custody barrier that ends HouseGate's
//               lease duty, so it must never outrun the pin. This binds
//               EVERY ack emission, including the idempotent duplicate-
//               SubmitStatement replay (arbiter.proto: duplicates return
//               the original result): a leader answering a redriven submit
//               after failover re-asserts the SEQUENCED pin durable BEFORE
//               re-emitting the original ack, so no ack path — first send
//               or replay — can outrun the pin;
//   SEQUENCED → REPLAY pin    : at block seal, the block-scoped REPLAY pin
//               is made durable BEFORE the covered statements' SEQUENCED
//               pins are released;
//   REPLAY    → AUDIT pin     : the AUDIT pin is made durable BEFORE the
//               REPLAY pin is released.
// From the SequencedAck onward a sequenced payload is therefore ALWAYS
// covered by at least one durable pin — never by residual lease TTL — so a
// leader failover of any duration cannot open a GC window; the new leader
// idempotently re-asserts the pins the chain says must exist. Only a
// payload whose statement is never sequenced ages out by lease expiry.
//
// END-TO-END DATA FLOW — one payload's whole life (put → pin → fetch →
// release), actors on the left, retention state on the right:
//
//   HouseGate  ① PutPayloadInline / PutPayload            [lease]
//              ② SubmitStatement(payload_ref) → Arbiter
//   Arbiter    ③ statement sequenced; BEFORE the ack:
//                 PinPayloads(SEQUENCED, scope=statement_seq, ref)
//                 then SubmitStatement is acked           [lease + pin]
//                 — from here lease expiry is irrelevant
//   Arbiter    ④ SealL3Block(N): PinPayloads(REPLAY, scope=N, refs) THEN
//                 ReleasePins(SEQUENCED, …) per covered
//                 statement                               [pin(REPLAY)]
//   Verifier   ⑤ FetchPayloads(refs) during block-N replay (§3.5 step 3);
//                 verifies bytes against the SEQUENCED envelope's
//                 payload_hash + payload_length, replays, attests
//   Arbiter    ⑥ after quorum replay + every dispatched verifier has
//                 reported or been timed out per stated policy + L2
//                 finality + safe watermark past N:
//                 PinPayloads(AUDIT, scope=N, refs) THEN
//                 ReleasePins(REPLAY, N)                  [pin(AUDIT)]
//                 — always pin-then-release; a covered ref never has a
//                 bare window
//   Arbiter    ⑦ audit window ends: ReleasePins(AUDIT, N) [zero holds]
//   store      ⑧ zero pins + expired lease → MAY collect  (never MUST;
//                 an immutable backend simply keeps serving)
//
// Failure exit: a payload whose statement is never sequenced is protected
// only by its lease; lease expiry is then the ONLY path to collectability.
//
// Result-code convention (the AdmissionCode pattern, one enum PER CONCERN):
// application outcomes return gRPC OK plus a per-RPC code (PutCode /
// FetchCode / StatCode / PinCode / ReleaseCode); gRPC errors are reserved
// for transport, auth, and request-level malformation. Every coded result
// message carries a human-readable `message` detail. All timestamp fields
// are unix epoch milliseconds with an _unix_ms suffix.
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
  // Duration (milliseconds) of the ingest lease granted or refreshed by
  // every successful put. Sizing input for HouseGate's put→SubmitStatement
  // budget and its re-put refresh cadence when a submit stalls; retention
  // of a SEQUENCED payload never depends on this value (custody-chain
  // rule — pins, not clocks, carry sequenced payloads).
  uint64 ingest_lease_ms = 5;
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
  // Accepted; backend dispersal / inclusion still in flight. Retrievability
  // while PENDING depends on holds: a ref covered by ANY pin MUST be served
  // by the composite service from its own retained copy (the service
  // verified and held the full bytes at put time), even if dispersal is
  // still in flight or ultimately fails — dispersal failure is a
  // store-internal repair job, never a replay blocker. Only an UNPINNED
  // PENDING ref may be temporarily non-retrievable.
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
// identity the store dedupes on: the store MUST mint exactly one stable ref
// per content identity. Ref stability is load-bearing — the idempotent
// re-put contract, concurrent-put convergence, and the RELEASED→re-put
// recovery path all collapse without it — so it is a conformance
// requirement, not a hint. The store verifies the received bytes against
// the declaration BEFORE acking OK — this is the put integrity gate
// (service hygiene, not trust: readers still re-verify from the sequenced
// envelope, the pkg/replay verifier invariant).
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

// FetchPayloadsRequest asks for 1..max_batch_refs specs on one server
// stream — a block has many statements, and per-payload RPC fan-out is the
// store's job, not the verifier's. Two specs MAY name the same payload_ref
// with different ranges (resuming missing suffixes after a broken stream;
// P5+ challengers sampling slices): frames are attributed to specs by
// spec_index, so ref reuse is never ambiguous on the wire. Request-level
// malformation (over max_batch_refs, empty ref, zero specs) fails with
// gRPC INVALID_ARGUMENT before any frame is sent; everything per-spec is
// reported in FetchEnd. Backpressure is HTTP/2 flow control, never
// re-modeled in messages.
message FetchPayloadsRequest {
  repeated FetchSpec specs = 1;
}

// FetchBegin opens one spec's section. Deliberately hash-silent — carrying
// no server-asserted hash or length: readers MUST verify SHA-256(bytes) ==
// the SEQUENCED envelope's payload_hash and byte count == payload_length
// before any executor runs (pkg/replay verifier invariant); the store's
// word is never an input.
message FetchBegin {
  string payload_ref = 1;
  // Index into FetchPayloadsRequest.specs this section serves. spec_index,
  // not payload_ref, is the frame-attribution key: two specs may share a
  // ref, but every frame belongs to exactly one spec.
  uint32 spec_index = 2;
}

// FetchData carries the next slice for one spec. Frames for a given spec
// are strictly ordered (offset is a resume/self-check aid, monotonically
// contiguous per spec); frames for DISTINCT specs MAY interleave so one
// slow backend object never head-of-line blocks the block fetch.
message FetchData {
  string payload_ref = 1;
  // Index into FetchPayloadsRequest.specs (see FetchBegin.spec_index).
  uint32 spec_index = 2;
  // Absolute byte offset of this chunk within the payload.
  uint64 offset = 3;
  // Non-empty, at most max_chunk_bytes.
  bytes chunk = 4;
}

// FetchCode is the per-spec fetch outcome taxonomy.
enum FetchCode {
  FETCH_CODE_UNSPECIFIED = 0;
  FETCH_CODE_OK = 1;
  // Never stored under this ref. During replay of a pinned block this is an
  // availability incident to escalate, not a retry case — the store broke
  // its hold obligation (a design-level incident classification, not a spec
  // citation).
  FETCH_CODE_NOT_FOUND = 2;
  // Stored but not yet retrievable (async backend dispersal in flight) AND
  // not covered by any pin — a pinned ref is always served from the
  // service's retained copy and never returns PENDING. Retry with backoff.
  // v1's local store never emits this; clients MUST handle it anyway (P5+
  // swap is config, not protocol).
  FETCH_CODE_PENDING = 3;
  // Was stored, then legitimately collected (zero pins, lease expired).
  // Distinguishes "aged out per policy" from an availability fault — but
  // under the custody-chain rule a ref that any hold should still cover can
  // never legitimately reach RELEASED: seeing it while replaying a pinned
  // block, or on a dispatched verifier's evidence fetch, is an availability
  // incident to escalate (protocol-broken retention, not benign aging).
  FETCH_CODE_RELEASED = 4;
  // Requested range offset is at or beyond the stored payload's length.
  FETCH_CODE_OFFSET_OUT_OF_RANGE = 5;
  // Store-internal read failure serving THIS spec (backend object
  // unreadable, disk fault) — not decidable before frames, so FetchEnd may
  // arrive after partial FetchData (served_length says how far it got;
  // resume with a range). Other specs on the stream are unaffected: this
  // code exists so a mid-serve fault stays per-spec isolated instead of
  // killing the whole stream as a gRPC error. Transient — retry may
  // succeed; persistent INTERNAL on a pinned ref is an availability
  // incident like NOT_FOUND.
  FETCH_CODE_INTERNAL = 6;
}

// FetchEnd closes one spec's section: OK after all bytes; a pre-frame
// per-spec failure (NOT_FOUND / PENDING / RELEASED / OFFSET_OUT_OF_RANGE)
// sent without Begin when nothing was served; or INTERNAL after a mid-serve
// fault (possibly following partial FetchData). Per-spec isolation means
// one failing payload cannot mask the availability of the rest of the
// block.
message FetchEnd {
  string payload_ref = 1;
  // Index into FetchPayloadsRequest.specs (see FetchBegin.spec_index).
  uint32 spec_index = 2;
  FetchCode code = 3;
  // Bytes actually served for this spec (a range running past EOF serves to
  // EOF and reports the truncated count here, code OK).
  uint64 served_length = 4;
  // Human-readable detail; empty on OK.
  string message = 5;
}

// FetchFrame is the fetch stream element. Per requested spec the server
// sends Begin, Data*, End — exactly one End per spec_index; the gRPC stream
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
  // Set only when code == OK: PENDING / AVAILABLE / RELEASED. A PENDING
  // ref that is pinned is nonetheless fetchable (see PAYLOAD_STATE_PENDING).
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
// making retention audit-readable. Purpose transitions follow the
// custody-chain rule (file header): the successor pin is made durable
// BEFORE the predecessor is released.
enum PinPurpose {
  PIN_PURPOSE_UNSPECIFIED = 0;
  // Block-replay hold. Placed by the orchestrator at BLOCK-SEAL time — a
  // block_seq exists only once SealL3Block runs — with scope_key = decimal
  // block_seq, and made durable BEFORE the covered statements' SEQUENCED
  // pins are released. Held until the containing block has completed quorum
  // replay (§7.1) AND every dispatched verifier has reported or been timed
  // out per stated orchestrator policy — a slow or dissenting verifier must
  // still be able to fetch the bytes behind its signed ExecutionReceipt
  // challenge evidence. Released only AFTER the AUDIT pin is durable.
  PIN_PURPOSE_REPLAY = 1;
  // Safe watermark passed; audit-retention window running (§8.5). Made
  // durable BEFORE the same scope's REPLAY pin is released.
  PIN_PURPOSE_AUDIT = 2;
  // Operator-initiated legal hold / archive, placed through this same
  // PinPayloads entry by operator admin tooling (the one non-orchestrator
  // lifecycle caller); blocks collection until explicitly released, by
  // design.
  PIN_PURPOSE_ARCHIVE = 3;
  // Sequenced-statement bridge hold: covers one statement's payload from
  // BEFORE the SubmitStatement ack until the sealed block's REPLAY pin is
  // durable. scope_key = decimal statement_seq. The orchestrator makes this
  // pin durable and only then sends the ack (pin placement stays an
  // orchestrator side effect outside Apply, §4.1 — the ack side effect is
  // simply ordered after it): the ack ends HouseGate's lease duty, so a
  // durable pin — never residual lease TTL — is what carries retention
  // across the ack→seal gap and any leader failover inside it. The barrier
  // binds duplicate acks too: a redriven SubmitStatement is answered only
  // after this pin is re-asserted durable (custody-chain rule, file
  // header), so the idempotent-replay path cannot outrun the pin either.
  PIN_PURPOSE_SEQUENCED = 4;
}

// PinKey is a pin's identity and its idempotency key. One logical pin per
// key; the pin's ref set only ever GROWS until the whole pin is released.
message PinKey {
  // Cluster-stable logical holder (e.g. "arbiter"), never a per-leader
  // instance id — a failed-over leader re-asserts the SAME pins instead of
  // orphaning its predecessor's.
  string holder_id = 1;
  PinPurpose purpose = 2;
  // Pin scope, e.g. the decimal block_seq for REPLAY/AUDIT pins, the
  // decimal statement_seq for SEQUENCED pins.
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
  // recorded, and from this moment the composite service MUST serve the ref
  // from its own retained copy regardless of backend dispersal state (it
  // verified and held the full bytes at put time); a dispersal that later
  // fails is repaired inside the store, never surfaced as unavailability of
  // a pinned ref.
  PIN_CODE_OK = 1;
  // Never stored under this ref. For a block being pinned for replay this
  // is an availability incident to escalate, not to retry (design-level
  // incident classification).
  PIN_CODE_NOT_FOUND = 2;
  // Already collected; a pin cannot resurrect bytes. Same escalation. Under
  // the custody-chain rule this can only mean a broken hold chain or a
  // never-protected ref — a correctly ordered pin never lands on RELEASED.
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
  // wrong purpose/domain, recovered address not on the allowlist, or a
  // stale release_seq at or below the authority's persisted watermark —
  // the anti-replay check).
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
  // whose payload carries purpose "arbiter-payload-release", a
  // CanonicalDigest of the key under domain "arbiter-payload-release-v1",
  // AND a strictly monotonic release_seq. The store authorizes by address
  // recovery against its authority allowlist and persists, per authority
  // address, the highest accepted release_seq as a watermark (the §8.3 /
  // §10.3 stale-command anchors): a JWS whose release_seq is not above the
  // watermark is rejected UNAUTHORIZED — re-presenting the byte-identical
  // last-accepted JWS is the one idempotent-retry exception and returns OK
  // without effect. The seq is required because PinKey is a STABLE
  // coordinate: a failed-over leader re-asserts the same key, so without it
  // one historic release JWS would stay valid against every future
  // re-assertion of that pin (fatal for ARCHIVE legal holds). v1 MAY run
  // channel-trust with this empty — enforcement is store configuration, so
  // P5+ hardening is config, not protocol.
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
// Put until its SubmitStatement is acked. The refresh primitive is the
// duplicate put — HouseGate keeps the spooled bytes until the ack for
// exactly this reason — budgeted against StoreLimits.ingest_lease_ms: if a
// submit stalls past the budget, HouseGate re-puts before expiry, and after
// a crash it redrives every un-acked statement from its durable spool
// (re-put, then re-submit), restoring the lease before any ack can occur.
// On the other side of the barrier, the orchestrator makes the SEQUENCED
// pin durable BEFORE the ack is sent (custody-chain rule, file header), so
// from the ack onward retention rests on pins, never on the lease clock.
// The lease is NOT a pin: it has no holder identity, so a concurrent
// duplicate put from another node can only EXTEND protection — it can
// never steal, replace, or shorten anything.
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
  // Batch fetch: per-spec Begin/Data*/End sections, ordered per spec,
  // interleaved across specs, attributed by spec_index; per-spec failures
  // isolated in FetchEnd. Hash-silent. Read-only.
  rpc FetchPayloads (FetchPayloadsRequest) returns (stream FetchFrame) {}
  // Advisory batch presence/state probe; bounded by max_batch_refs (gRPC
  // INVALID_ARGUMENT if exceeded). Read-only.
  rpc StatPayloads (StatPayloadsRequest) returns (StatPayloadsResult) {}
}

// PayloadLifecycle is control-plane: called by the Arbiter leader
// orchestrator (SEQUENCED / REPLAY / AUDIT pins) and by operator admin
// tooling (ARCHIVE legal holds), under the same channel-trust / authority
// rules. Pin timing is an orchestrator decision outside Apply, like every
// other side effect (§4.1); the custody-chain rule (file header) constrains
// the ORDER
// of those side effects (successor pin durable before predecessor hold
// released, and before the SequencedAck at the lease boundary), not where
// they execute. Pins are the system's retention refcount: GC needs zero
// pins AND an expired lease; there is no Delete RPC anywhere. After
// failover the new leader idempotently re-asserts every hold the custody
// chain says must exist — SEQUENCED pins for sequenced-but-unsealed
// statements, REPLAY pins for blocks still inside their replay hold —
// before dispatching replay; any PIN_CODE_NOT_FOUND / PIN_CODE_RELEASED on
// that pass is an availability incident, and because the pins were durable
// before the corresponding acks/releases, it indicates a store fault rather
// than an expected race.
service PayloadLifecycle {
  // Idempotency key: (key.holder_id, key.purpose, key.scope_key); refs
  // union in, add-only.
  rpc PinPayloads (PinPayloadsRequest) returns (PinPayloadsResult) {}
  // Idempotency key: the same key; releasing an unknown or already-released
  // key returns OK. Carries the authority slot (see ReleasePinsRequest).
  // Ordering duty sits on the CALLER: under the custody-chain rule the
  // orchestrator releases a hold only after its successor hold is durable.
  rpc ReleasePins (ReleasePinsRequest) returns (ReleasePinsResult) {}
}
```
