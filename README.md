# arbiter-proto

Wire contract for the Sentio Arbiter (L3-block sequencer + admission + attestation collector + safe-state publisher). The `.proto` files and the generated Go live here; consumers import `pb "github.com/sentioxyz/arbiter-proto/gen/pb"` — the same pattern as `rewriter-go/gen/pb`.

Design source of truth: housegate `docs/superpowers/specs/2026-06-30-sentio-arbiter-design.md` (§11 services, §4.1 Raft command alphabet).

## Layout

| Path | What |
|---|---|
| `proto/replay.proto` | Wire mirror of housegate `pkg/replay` reused types. Field names are frozen against those types' JSON tags — a conformance test in the `arbiter` repo enforces parity. |
| `proto/arbiter.proto` | Arbiter domain messages + the six gRPC services. |
| `proto/da.proto` | Payload store / DA layer: `PayloadStore` (limits, inline+chunked put with ingest lease, batch interleaved fetch, stat) + `PayloadLifecycle` (add-only pins, release-as-permission). Design: `docs/superpowers/specs/2026-07-14-da-payload-store-api-design.md`. |
| `proto/raftlog.proto` | The replicated-FSM command alphabet (Raft log entry payloads). |
| `gen/pb` | Generated Go (committed). |

## Build & regenerate

```bash
make tools    # install pinned protoc-gen-go / protoc-gen-go-grpc
make proto    # regenerate gen/pb (buf)
make lint     # buf lint
make test     # go build + go vet over generated code
```
