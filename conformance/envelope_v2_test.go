package conformance

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// TestStatementEnvelopeV2HasSignedV2Fields pins the field numbers of the
// v2 additions: an accidental renumber would silently break every stored
// envelope, so the numbers are asserted, not just the names.
func TestStatementEnvelopeV2HasSignedV2Fields(t *testing.T) {
	want := map[string]int32{
		"envelope_version":  11,
		"network_id":        12,
		"keeper_shard_id":   13,
		"payload_format":    14,
		"client_revision":   15,
		"schema_hash":       16,
		"row_id_profile_id": 17,
	}
	fields := (&pb.StatementEnvelopeV2{}).ProtoReflect().Descriptor().Fields()
	for name, number := range want {
		fd := fields.ByName(protoreflectName(name))
		if fd == nil {
			t.Fatalf("StatementEnvelopeV2 is missing field %q", name)
		}
		if int32(fd.Number()) != number {
			t.Fatalf("StatementEnvelopeV2.%s number = %d, want %d", name, fd.Number(), number)
		}
	}
}

func TestReplayStatementCarriesFormatRevisionSchemaHash(t *testing.T) {
	want := map[string]int32{"payload_format": 11, "client_revision": 12, "schema_hash": 13}
	fields := (&pb.Statement{}).ProtoReflect().Descriptor().Fields()
	for name, number := range want {
		fd := fields.ByName(protoreflectName(name))
		if fd == nil {
			t.Fatalf("Statement is missing field %q", name)
		}
		if int32(fd.Number()) != number {
			t.Fatalf("Statement.%s number = %d, want %d", name, fd.Number(), number)
		}
	}
}

func TestSafeStateExposesGetL3Block(t *testing.T) {
	svc := pb.File_arbiter_proto.Services().ByName("SafeState")
	if svc == nil {
		t.Fatal("SafeState service missing")
	}
	m := svc.Methods().ByName("GetL3Block")
	if m == nil {
		t.Fatal("SafeState.GetL3Block missing")
	}
	if got := string(m.Input().FullName()); got != "arbiter.L3BlockRef" {
		t.Fatalf("GetL3Block input = %s, want arbiter.L3BlockRef", got)
	}
	if got := string(m.Output().FullName()); got != "arbiter.L3Block" {
		t.Fatalf("GetL3Block output = %s, want arbiter.L3Block", got)
	}
	// L3Block must carry the sealed envelopes so an auditor can recompute
	// statements_root from the same canonical form the FSM hashed.
	var _ proto.Message = &pb.L3Block{}
	if (&pb.L3Block{}).ProtoReflect().Descriptor().Fields().ByName("statements") == nil {
		t.Fatal("L3Block.statements missing")
	}
}

func protoreflectName(s string) protoreflect.Name { return protoreflect.Name(s) }
