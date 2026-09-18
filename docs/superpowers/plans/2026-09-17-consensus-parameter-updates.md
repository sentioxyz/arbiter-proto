# Consensus parameter update protocol plan

1. Append the signed typed command and administration service without changing
   existing wire field numbers.
2. Regenerate Go using the repository's pinned generator versions.
3. Pin new message fields, RPC signatures, and Raft slot 18 in conformance
   tests; run lint, compatibility, generation drift, build, vet, and tests.
4. Publish a ready PR. After review and CI, the coordinator merges and releases
   arbiter-proto through `cut-release`; arbiter-core then pins that release and
   implements canonical hashing, signing, verification, and wire conversion.

Validation evidence is recorded in the PR; no release or deployment is made
from this implementation branch.
