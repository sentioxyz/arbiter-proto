# Replicated consensus parameter update protocol

Tracking: [Arbiter #39](https://github.com/sentioxyz/arbiter/issues/39).
The runtime specification lives in Arbiter
`docs/specs/2026-09-17-consensus-parameter-updates.md`.

The command alphabet appends `UpdateConsensusParamsCmd` at Raft oneof field
18. Existing field numbers and messages remain unchanged. Its typed update
binds network ID, genesis snapshot ID, expected epoch, previous full-parameter
digest, the complete target authority set, the target writer limit, and the
expected promotion sequence. `authority_jws` signs the canonical Go update in
arbiter-core using a dedicated versioned purpose and hash domain. Protobuf
bytes are transport only; they are never the signing form.

`ConsensusAdmin.GetProtocolInfo` serves the contacted node's static ID,
protocol version, and local mutation-enable flag. It must not redirect to the
leader or issue a Raft barrier: operators probe every voter before activation.
`GetConsensusParams` is a leader-barrier read returning immutable identity,
bootstrap/current mutable parameters, current epoch, full-parameter digest,
and promotion sequence. `UpdateConsensusParams` proposes the signed command
through the ordinary replicated path. A stale epoch, digest, or promotion
sequence is rejected, including retries of a previously committed update.
Read current parameters after an uncertain result before preparing a new update.

Wire compatibility does not make mixed-version update execution safe. Old
followers cannot apply command 18. Upgrade every voter and verify capability
before enabling mutation RPCs. The protocol change does not authorize cluster
deployment, key rotation, or re-genesis.
