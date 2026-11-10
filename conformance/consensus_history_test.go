package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestConsensusHistoryContract freezes the light-client history read: field
// numbers, kinds, and that each entry is the signed update command itself.
func TestConsensusHistoryContract(t *testing.T) {
	req := (&pb.GetConsensusHistoryRequest{}).ProtoReflect().Descriptor()
	if req.Fields().Len() != 1 {
		t.Fatalf("request field count = %d", req.Fields().Len())
	}
	if f := req.Fields().ByName("from_epoch"); f == nil || f.Number() != 1 || f.Kind() != protoreflect.Uint64Kind {
		t.Fatalf("from_epoch = %v", f)
	}

	d := (&pb.ConsensusHistory{}).ProtoReflect().Descriptor()
	want := []struct {
		name     protoreflect.Name
		kind     protoreflect.Kind
		repeated bool
	}{
		{"network_id", protoreflect.StringKind, false},
		{"genesis_snapshot_id", protoreflect.StringKind, false},
		{"current_epoch", protoreflect.Uint64Kind, false},
		{"bootstrap_authority_addresses", protoreflect.StringKind, true},
		{"updates", protoreflect.MessageKind, true},
	}
	if d.Fields().Len() != len(want) {
		t.Fatalf("field count = %d, want %d", d.Fields().Len(), len(want))
	}
	for i, w := range want {
		f := d.Fields().ByName(w.name)
		if f == nil || f.Number() != protoreflect.FieldNumber(i+1) || f.Kind() != w.kind || f.IsList() != w.repeated {
			t.Fatalf("field %s = %v, want number %d kind %s repeated %v", w.name, f, i+1, w.kind, w.repeated)
		}
	}
	if got := d.Fields().ByName("updates").Message().FullName(); got != "arbiter.UpdateConsensusParamsCmd" {
		t.Fatalf("updates message type = %s", got)
	}

	svc := pb.File_consensus_proto.Services().ByName("ConsensusHistoryService")
	if svc == nil {
		t.Fatal("ConsensusHistoryService missing")
	}
	m := svc.Methods().ByName("GetConsensusHistory")
	if m == nil || m.Input().FullName() != "arbiter.GetConsensusHistoryRequest" || m.Output().FullName() != "arbiter.ConsensusHistory" || m.IsStreamingClient() || m.IsStreamingServer() {
		t.Fatalf("GetConsensusHistory = %v", m)
	}
}
