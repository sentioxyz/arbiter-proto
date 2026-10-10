package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestClientLanesFieldNumbers pins the client_seq lane additions (housegate
// spec 2026-10-09 §5.2). Each number is a consensus constant: arbiter-core's
// strict Raft command decoder refuses any field it does not know, so a voter
// that disagrees on one of these numbers forks on the first command that
// carries it.
func TestClientLanesFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg      proto.Message
		name     protoreflect.Name
		number   protoreflect.FieldNumber
		kind     protoreflect.Kind
		repeated bool
		msgType  protoreflect.FullName
	}{
		{&pb.StatementID{}, "client_lane", 4, protoreflect.StringKind, false, ""},
		{&pb.ClientLaneParams{}, "max_lanes_per_account", 1, protoreflect.Uint32Kind, false, ""},
		{&pb.ConsensusParamsUpdate{}, "client_lanes", 10, protoreflect.MessageKind, false, "arbiter.ClientLaneParams"},
		{&pb.ConsensusMutableParams{}, "client_lanes", 5, protoreflect.MessageKind, false, "arbiter.ClientLaneParams"},
		{&pb.TableRegistrySnapshot{}, "client_lanes", 6, protoreflect.MessageKind, false, "arbiter.ClientLaneParams"},
		{&pb.ProtocolInfo{}, "features", 4, protoreflect.StringKind, true, ""},
		{&pb.NodeRegistration{}, "features", 5, protoreflect.StringKind, true, ""},
		{&pb.NodeFeatures{}, "entries", 1, protoreflect.MessageKind, true, "arbiter.NodeFeatureEntry"},
		{&pb.NodeFeatures{}, "raft_voter_ids", 2, protoreflect.StringKind, true, ""},
		{&pb.NodeFeatureEntry{}, "node_id", 1, protoreflect.StringKind, false, ""},
		{&pb.NodeFeatureEntry{}, "features", 2, protoreflect.StringKind, true, ""},
		{&pb.NodeFeatureEntry{}, "registered_unix", 3, protoreflect.Int64Kind, false, ""},
		{&pb.GetClientSeqStateRequest{}, "client_account", 1, protoreflect.StringKind, false, ""},
		{&pb.GetClientSeqStateRequest{}, "client_lane", 2, protoreflect.StringKind, false, ""},
		{&pb.ClientSeqRange{}, "start", 1, protoreflect.Uint64Kind, false, ""},
		{&pb.ClientSeqRange{}, "end", 2, protoreflect.Uint64Kind, false, ""},
		{&pb.ClientSeqState{}, "found", 1, protoreflect.BoolKind, false, ""},
		{&pb.ClientSeqState{}, "subject", 2, protoreflect.StringKind, false, ""},
		{&pb.ClientSeqState{}, "hi", 3, protoreflect.Uint64Kind, false, ""},
		{&pb.ClientSeqState{}, "ranges", 4, protoreflect.MessageKind, true, "arbiter.ClientSeqRange"},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name())+"."+string(tc.name), func(t *testing.T) {
			f := d.Fields().ByName(tc.name)
			if f == nil || f.Number() != tc.number || f.Kind() != tc.kind || f.IsList() != tc.repeated {
				t.Fatalf("field = %v, want number %d kind %s repeated %v", f, tc.number, tc.kind, tc.repeated)
			}
			if tc.msgType != "" && f.Message().FullName() != tc.msgType {
				t.Fatalf("message type = %s, want %s", f.Message().FullName(), tc.msgType)
			}
		})
	}
	for msg, want := range map[proto.Message]int{
		&pb.StatementID{}: 4, &pb.NodeRegistration{}: 8, &pb.ClientLaneParams{}: 1,
		&pb.NodeFeatures{}: 2, &pb.NodeFeatureEntry{}: 3, &pb.ProtocolInfo{}: 4,
		&pb.GetClientSeqStateRequest{}: 2, &pb.ClientSeqRange{}: 2, &pb.ClientSeqState{}: 4,
	} {
		if got := msg.ProtoReflect().Descriptor().Fields().Len(); got != want {
			t.Fatalf("%s field count = %d, want %d", msg.ProtoReflect().Descriptor().Name(), got, want)
		}
	}
}

func TestLaneBudgetAdmissionCodeIsNine(t *testing.T) {
	if got := int32(pb.AdmissionCode_ADMISSION_CODE_LANE_BUDGET_EXCEEDED); got != 9 {
		t.Fatalf("ADMISSION_CODE_LANE_BUDGET_EXCEEDED = %d, want 9", got)
	}
	if got := int32(pb.AdmissionCode_ADMISSION_CODE_GAP_BUDGET_EXCEEDED); got != 8 {
		t.Fatalf("ADMISSION_CODE_GAP_BUDGET_EXCEEDED moved to %d", got)
	}
	if n := pb.AdmissionCode(0).Descriptor().Values().Len(); n != 11 {
		t.Fatalf("AdmissionCode has %d values, want 11", n)
	}
}

func TestClientLaneRPCSignatures(t *testing.T) {
	for _, tc := range []struct {
		file          protoreflect.FileDescriptor
		service       protoreflect.Name
		method        protoreflect.Name
		input, output protoreflect.FullName
	}{
		{pb.File_consensus_proto, "ConsensusAdmin", "GetNodeFeatures", "google.protobuf.Empty", "arbiter.NodeFeatures"},
		{pb.File_arbiter_proto, "SafeState", "GetClientSeqState", "arbiter.GetClientSeqStateRequest", "arbiter.ClientSeqState"},
	} {
		svc := tc.file.Services().ByName(tc.service)
		if svc == nil {
			t.Fatalf("service %s missing", tc.service)
		}
		m := svc.Methods().ByName(tc.method)
		if m == nil || m.Input().FullName() != tc.input || m.Output().FullName() != tc.output || m.IsStreamingClient() || m.IsStreamingServer() {
			t.Fatalf("%s.%s = %v, want unary %s -> %s", tc.service, tc.method, m, tc.input, tc.output)
		}
	}
}
