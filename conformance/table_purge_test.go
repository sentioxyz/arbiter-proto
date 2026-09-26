package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestTablePurgeContract pins the sub-project 4 purge surface: the
// SubmitTablePurged RPC rides PromotionGateway next to AckCleanup and takes
// the replicated RecordTablePurgedCmd as its request (the precedent is
// UpdateConsensusParamsCmd), and GetPurgeNodeSet exposes the node set a purge
// waits on.
func TestTablePurgeContract(t *testing.T) {
	svc := pb.File_arbiter_proto.Services().ByName("PromotionGateway")
	if svc == nil {
		t.Fatal("PromotionGateway service missing")
	}
	m := svc.Methods().ByName("SubmitTablePurged")
	if m == nil || m.Input().FullName() != "arbiter.RecordTablePurgedCmd" || m.Output().FullName() != "arbiter.Ack" || m.IsStreamingClient() || m.IsStreamingServer() {
		t.Fatalf("PromotionGateway.SubmitTablePurged = %v", m)
	}
	if ack := svc.Methods().ByName("AckCleanup"); ack == nil || ack.Output().FullName() != "arbiter.Ack" {
		t.Fatalf("PromotionGateway.AckCleanup = %v", ack)
	}
	cmd := (&pb.RecordTablePurgedCmd{}).ProtoReflect().Descriptor().Fields()
	if cmd.Len() != 2 {
		t.Fatalf("RecordTablePurgedCmd has %d fields, want 2", cmd.Len())
	}
	if f := cmd.ByName("node_id"); f == nil || f.Number() != 1 || f.Kind() != protoreflect.StringKind {
		t.Fatalf("RecordTablePurgedCmd.node_id = %v", f)
	}
	if f := cmd.ByName("incarnation_seq"); f == nil || f.Number() != 2 || f.Kind() != protoreflect.Uint64Kind {
		t.Fatalf("RecordTablePurgedCmd.incarnation_seq = %v", f)
	}
	set := (&pb.PurgeNodeSet{}).ProtoReflect().Descriptor().Fields()
	if f := set.ByName("node_ids"); set.Len() != 1 || f == nil || f.Number() != 1 || f.Kind() != protoreflect.StringKind || !f.IsList() {
		t.Fatalf("PurgeNodeSet fields = %v", set)
	}
}
