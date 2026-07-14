# DA Payload-Store Proto Landing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the approved `proto/da.proto` contract (PayloadStore + PayloadLifecycle services) in arbiter-proto with regenerated Go and a descriptor-level conformance test, keeping every commit CI-green.

**Architecture:** The contract is already fully designed and reviewed — the authoritative content is the fenced ```proto appendix of `docs/superpowers/specs/2026-07-14-da-payload-store-api-design.md`. This plan lands it verbatim, regenerates `gen/pb`, and adds the spec-required "hash-silent read path" static assertion as a Go test over the generated descriptors.

**Tech Stack:** proto3 / buf v2 (lint BASIC except PACKAGE_DIRECTORY_MATCH; breaking vs `main`), protoc-gen-go v1.36.11 + protoc-gen-go-grpc v1.5.1 (pinned in Makefile, installed via `make tools`), Go (module `github.com/sentioxyz/arbiter-proto`).

## Global Constraints

- `proto/da.proto` content MUST equal the spec appendix verbatim — do not edit the contract while landing it.
- Frozen surface: `StatementEnvelopeV2`, `replay.Statement`, `AnchorRef`, and every existing message/enum/service in `proto/arbiter.proto`, `proto/replay.proto`, `proto/raftlog.proto` are untouched.
- Package/options: `package arbiter;` and `option go_package = "github.com/sentioxyz/arbiter-proto/gen/pb;pb";` (already in the appendix — verify, don't change).
- Every commit on `main` must pass the full CI recipe: `buf lint` && `make tools && make proto && git diff --exit-code -- gen/` && `make test` (this is why proto + generated code + Makefile/README edits land in ONE commit per task).
- Generated code is committed (repo convention); never hand-edit files under `gen/`.

---

### Task 1: Land `proto/da.proto` + regenerated `gen/pb` + README row

**Files:**
- Create: `proto/da.proto` (extracted from spec appendix)
- Create (generated): `gen/pb/da.pb.go`, `gen/pb/da_grpc.pb.go`
- Modify: `README.md` (layout table, after the `proto/arbiter.proto` row)

**Interfaces:**
- Consumes: the fenced ```proto code block in `docs/superpowers/specs/2026-07-14-da-payload-store-api-design.md` (the only ```proto fence in that file).
- Produces: Go types/stubs in package `pb` used by Task 2 — notably messages `FetchBegin`, `FetchData`, `FetchEnd`, `PayloadStat`, `PutPayloadHeader` and services `PayloadStore`, `PayloadLifecycle`.

- [ ] **Step 1: Extract the contract from the spec appendix**

```bash
awk '/^```proto$/{f=1;next}/^```$/{f=0}f' \
  docs/superpowers/specs/2026-07-14-da-payload-store-api-design.md > proto/da.proto
head -3 proto/da.proto && grep -c "^" proto/da.proto
```

Expected: first line is `syntax = "proto3";`; line count ≈ 510. Sanity-check the tail contains `service PayloadLifecycle` and the file ends with `}`.

- [ ] **Step 2: Lint + breaking (the "failing test" gate for a proto change)**

```bash
buf lint
make breaking
```

Expected: both exit 0 (new file is purely additive; existing files untouched). If `buf lint` reports anything, STOP — the spec appendix and the landed file have diverged; reconcile with the spec, do not improvise fixes.

- [ ] **Step 3: Regenerate Go**

```bash
make tools
make proto
git status --short
```

Expected `git status`: exactly three untracked files — `?? proto/da.proto` (from Step 1), `?? gen/pb/da.pb.go`, `?? gen/pb/da_grpc.pb.go`. No modifications to existing `gen/pb/*.go` files (pinned generator versions guarantee this; if existing generated files changed, STOP and check `protoc-gen-go --version` is v1.36.11 and `protoc-gen-go-grpc --version` is v1.5.1).

- [ ] **Step 4: Build + vet over the generated code**

```bash
make test
```

Expected: `go build ./...` and `go vet ./...` both exit 0.

- [ ] **Step 5: Add the README layout row**

In `README.md`, in the `## Layout` table, insert after the `proto/arbiter.proto` row:

```markdown
| `proto/da.proto` | Payload store / DA layer: `PayloadStore` (limits, inline+chunked put with ingest lease, batch interleaved fetch, stat) + `PayloadLifecycle` (add-only pins, release-as-permission). Design: `docs/superpowers/specs/2026-07-14-da-payload-store-api-design.md`. |
```

- [ ] **Step 6: Review the stage and commit atomically**

```bash
git add proto/da.proto gen/pb/da.pb.go gen/pb/da_grpc.pb.go README.md
git status --short   # verify EXACTLY these four paths are staged, nothing else
git commit -m "feat(proto): DA payload-store services (da.proto)

PayloadStore (GetStoreLimits, PutPayloadInline, chunked PutPayload,
batch interleaved FetchPayloads, StatPayloads) and PayloadLifecycle
(add-only PinPayloads, ReleasePins with authority_jws slot). Encodes
the ingest-lease -> pin handoff, hash-silent reads, and PayloadState
PENDING for config-only P5+ DA backend swap. Generated Go included.

Design: docs/superpowers/specs/2026-07-14-da-payload-store-api-design.md

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 7: Verify the commit replays the CI recipe cleanly**

```bash
buf lint && make breaking && make proto && git diff --exit-code -- gen/ && make test && echo CI-RECIPE-GREEN
```

Expected: `CI-RECIPE-GREEN`.

---

### Task 2: Hash-silent read-path conformance test

The spec ("测试与 conformance 备注") requires a descriptor-level static assertion: no read-path message may ever grow a server-asserted content field. The generated descriptors live in THIS repo, so the assertion lives here too.

**Files:**
- Create: `conformance/da_hash_silent_test.go`
- Modify: `Makefile` (add `go test ./...` to the `test` target)

**Interfaces:**
- Consumes: generated package `pb "github.com/sentioxyz/arbiter-proto/gen/pb"` from Task 1 — messages `FetchBegin`, `FetchData`, `FetchEnd`, `PayloadStat` (must be silent) and `PutPayloadHeader` (negative control, carries `payload_hash`/`payload_length`).
- Produces: nothing consumed later; a CI regression tripwire.

- [ ] **Step 1: Write the test (self-validating: includes a teeth-check)**

Create `conformance/da_hash_silent_test.go`:

```go
// Package conformance holds descriptor-level contract assertions for the
// arbiter-proto wire surface (the da.proto design spec's "hash-silent read
// path" rule: the store's word about content is never on the read path).
package conformance

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// contentAssertionFields returns the field names of md that look like a
// server-side content assertion: any *hash* field, or a payload_length
// echo. served_length is deliberately allowed — it reports service volume
// (bytes sent for a spec), not a claim about payload content.
func contentAssertionFields(md protoreflect.MessageDescriptor) []string {
	var hits []string
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		name := string(fields.Get(i).Name())
		if strings.Contains(name, "hash") || name == "payload_length" {
			hits = append(hits, name)
		}
	}
	return hits
}

// TestDaReadPathIsHashSilent pins the spec rule that FetchBegin/FetchData/
// FetchEnd/PayloadStat never assert content hashes or lengths — readers
// verify bytes against the sequenced envelope only (§13). If this fails,
// someone re-added a server assertion to the read path; remove the field,
// do not update the test.
func TestDaReadPathIsHashSilent(t *testing.T) {
	readPath := []protoreflect.MessageDescriptor{
		(&pb.FetchBegin{}).ProtoReflect().Descriptor(),
		(&pb.FetchData{}).ProtoReflect().Descriptor(),
		(&pb.FetchEnd{}).ProtoReflect().Descriptor(),
		(&pb.PayloadStat{}).ProtoReflect().Descriptor(),
	}
	for _, md := range readPath {
		if hits := contentAssertionFields(md); len(hits) > 0 {
			t.Errorf("%s carries content-assertion field(s) %v; the read path must stay hash-silent", md.FullName(), hits)
		}
	}
}

// TestHashSilentDetectorHasTeeth proves the detector actually detects:
// PutPayloadHeader declares payload_hash + payload_length by design, so an
// empty result here means the detector (not the contract) is broken.
func TestHashSilentDetectorHasTeeth(t *testing.T) {
	md := (&pb.PutPayloadHeader{}).ProtoReflect().Descriptor()
	hits := contentAssertionFields(md)
	if len(hits) != 2 {
		t.Fatalf("detector found %v on PutPayloadHeader, want [payload_hash payload_length]; the hash-silent test lost its teeth", hits)
	}
}
```

- [ ] **Step 2: Run the tests, verify both pass (and the teeth-check would catch a dead detector)**

```bash
go test ./conformance/ -v
```

Expected: `PASS` with both `TestDaReadPathIsHashSilent` and `TestHashSilentDetectorHasTeeth` listed as `--- PASS`.

- [ ] **Step 3: Wire `go test` into the repo test target**

In `Makefile`, change the `test` target from:

```makefile
test:
	go build ./...
	go vet ./...
```

to:

```makefile
test:
	go build ./...
	go vet ./...
	go test ./...
```

- [ ] **Step 4: Run the full recipe**

```bash
make test && buf lint && echo GREEN
```

Expected: test output shows `ok  	github.com/sentioxyz/arbiter-proto/conformance`, then `GREEN`.

- [ ] **Step 5: Commit**

```bash
git add conformance/da_hash_silent_test.go Makefile
git commit -m "test: hash-silent read-path conformance for da.proto

Descriptor-level tripwire from the DA design spec: FetchBegin/FetchData/
FetchEnd/PayloadStat must never grow a content hash/length assertion;
PutPayloadHeader doubles as the detector's negative control. make test
now runs go test ./... so CI enforces it.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Self-Review Notes

- **Spec coverage:** the spec's deliverable for THIS repo is the contract file + generated Go + the hash-silent static assertion (Task 1, Task 2). The remaining conformance notes (PENDING fault-injecting dev store, lease-handoff race tests, dedupe drain+verify, pin add-only behavior, authority_jws enforcement toggle) test a store IMPLEMENTATION, which lives in the arbiter repo — out of scope here by the spec's own 非目标 (store 的具体实现).
- **Precondition:** if the in-flight adversarial-verification round produced blocking/fixable amendments, the spec appendix gets amended FIRST (separate commit), then this plan runs against the amended appendix. Step 1's extract-from-spec design makes the plan self-healing on that path.
- **Type consistency:** Task 2 references `FetchBegin/FetchData/FetchEnd/PayloadStat/PutPayloadHeader` exactly as generated from the appendix's message names; `served_length` allowance matches the spec's 结果码 section wording.
