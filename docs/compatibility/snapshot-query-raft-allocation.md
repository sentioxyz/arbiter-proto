# Snapshot-query transport integration

This allocation reconciles issue153 head `5d992114012771284fded2ae6719d09f0b039055`
with main `f7d9f070ad4ec3347c0b9c13a326d52eac7823c4`. It supersedes only the
pre-integration Begin18 allocation. Existing commands 1–17 are unchanged.

| RaftCommand.cmd tag | Command / disposition |
| --- | --- |
| 18 | `update_consensus_params` (merged main; unchanged) |
| 19 | `grant_snapshot_query` |
| 20 | `release_snapshot_query` |
| 21 | `submit_snapshot_query` |
| 22 | `abort_snapshot_query` |
| 23 | `activate_query_profile` |
| 24 | `record_snapshot_query_claim` |
| 25 | `record_snapshot_query_attestation` |
| 26 | `publish_executor_profile_transition` |
| 27 | `record_snapshot_artifact_ready` |
| 28 | Separately adopted ArtifactDispositionCmd contract; not implemented or allocated by this merge |
| 29 | Unadopted C3 settlement proposal boundary; not allocated by this merge |
| 30 | `begin_snapshot_query` |

Numbers are message-scoped. Snapshot kind2 and verifier dispatch3 are unchanged.
Main's consensus schema and services retain their types and signatures.
`PromotionAck.safe_partition_parts = 9` retains the complete active safe-partition
inventory, including previously-safe parts; `parts = 6` remains candidate-only.
This integration does not change canonical signing inputs or digest domains.

## Historical compatibility boundary

Begin30 is **wire-incompatible** with issue153's pre-integration Begin18. Tag18
means only UpdateConsensusParams in the integrated descriptor. No alias,
payload-shape guessing, automatic import, replay, or archive rewriting is
provided or authorized. A generic protobuf decoder cannot determine whether
ambiguous historical tag18 bytes were written under either contract. Preserve
original bytes and provenance of known-old-Begin18 or unknown-origin archives;
require a separate version/provenance-aware disposition before using them.

Task-local auditing found callable old Begin18 converters and tests but no
implemented task-owned admission/Apply path. That is not an external-consumer,
archive, or production census: external use remains unknown. The feature remains
default-off; this transport integration authorizes no production activation or
runtime acceptance. Consumer dependency adoption and admission validation are
separate gates.

## Independent descriptor baselines

Both fixtures are immutable; neither was synthesized by deleting candidate
fields. `pre_snapshot_query_descriptor.binpb` retains the original export at
`19d90fc4bd6194a26656dd1a2cf66bb911247d73` and all original canonical JSON fixture
bytes remain unchanged. Its only additional old-message allowance is exactly
`repeated .arbiter.SafePartMapping safe_partition_parts = 9` on PromotionAck.

`main_f7d9f070_descriptor.binpb` was independently exported from a `git archive`
of exact main `f7d9f070ad4ec3347c0b9c13a326d52eac7823c4`, using that archive's
generated Go descriptors and Go module dependencies. The export recursively
collects imports of `arbiter.proto`, `replay.proto`, `raftlog.proto`,
`consensus.proto`, and `da.proto`, sorts files by path, converts them with
`protodesc.ToFileDescriptorProto`, and marshals the FileDescriptorSet with
`proto.MarshalOptions{Deterministic: true}`. No source info is added.

| Fixture | SHA-256 |
| --- | --- |
| pre-snapshot | `bd70cd89d7cb8c306ed32688735614e4358cf005438d685b40d0700bc547a313` |
| exact main | `f03a09fe43042bef4116ccb5c2bb362707cdbd9bf4ab1fe8b7cb67039d6717e9` |

Tests freeze fixture hashes, old fields and RPC signatures against both
baselines, snapshot variants remaining unknown to both old readers, exact
Begin30/Update18 wire keys, and main consensus payload round trips including
repeated addresses and maximum counters. Main's slot18 conformance tests remain
unchanged. Protobuf unknown-field retention is a transport property, not runtime
admission; strict consumer decoding remains a separate responsibility.

## Wave 1a-2b: ConsensusAdmin abort RPCs

Wave 1a-2b adds `ConsensusAdmin.GetSnapshotQueryAbortCandidate` and `ConsensusAdmin.AbortSnapshotQuery` (no new Raft tag; tag 22 remains `abort_snapshot_query`); both are unimplemented until arbiter installs the default-off sequencing dependency.
