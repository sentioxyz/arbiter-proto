package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestConsensusUpdateContract(t *testing.T) {
	tests := []struct {
		message proto.Message
		names   []protoreflect.Name
		kinds   []protoreflect.Kind
	}{
		{&pb.ConsensusParamsUpdate{},
			[]protoreflect.Name{"network_id", "genesis_snapshot_id", "expected_epoch", "previous_params_digest", "authority_addresses", "max_writers", "expected_promotion_seq", "artifact_disposition_capability", "table_registry", "client_lanes", "si_indexers", "verifiers"},
			[]protoreflect.Kind{protoreflect.StringKind, protoreflect.StringKind, protoreflect.Uint64Kind, protoreflect.StringKind, protoreflect.StringKind, protoreflect.Uint64Kind, protoreflect.Uint64Kind, protoreflect.Uint32Kind, protoreflect.MessageKind, protoreflect.MessageKind, protoreflect.MessageKind, protoreflect.MessageKind}},
		{&pb.UpdateConsensusParamsCmd{},
			[]protoreflect.Name{"update", "authority_jws"},
			[]protoreflect.Kind{protoreflect.MessageKind, protoreflect.StringKind}},
		{&pb.ConsensusMutableParams{},
			[]protoreflect.Name{"authority_addresses", "max_writers", "artifact_disposition_capability", "table_registry", "client_lanes", "si_indexers", "verifiers"},
			[]protoreflect.Kind{protoreflect.StringKind, protoreflect.Uint64Kind, protoreflect.Uint32Kind, protoreflect.MessageKind, protoreflect.MessageKind, protoreflect.MessageKind, protoreflect.MessageKind}},
		{&pb.ProtocolInfo{},
			[]protoreflect.Name{"node_id", "protocol_version", "updates_enabled", "features"},
			[]protoreflect.Kind{protoreflect.StringKind, protoreflect.Uint32Kind, protoreflect.BoolKind, protoreflect.StringKind}},
		{&pb.ConsensusParamsState{},
			[]protoreflect.Name{"protocol_version", "network_id", "genesis_snapshot_id", "bootstrap", "current", "epoch", "params_digest", "promotion_seq"},
			[]protoreflect.Kind{protoreflect.Uint32Kind, protoreflect.StringKind, protoreflect.StringKind, protoreflect.MessageKind, protoreflect.MessageKind, protoreflect.Uint64Kind, protoreflect.StringKind, protoreflect.Uint64Kind}},
	}
	for _, tt := range tests {
		d := tt.message.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			if d.Fields().Len() != len(tt.names) {
				t.Fatalf("field count = %d, want %d", d.Fields().Len(), len(tt.names))
			}
			for i, name := range tt.names {
				f := d.Fields().ByName(name)
				if f == nil || f.Number() != protoreflect.FieldNumber(i+1) || f.Kind() != tt.kinds[i] {
					t.Fatalf("field %s = %v, want number %d kind %s", name, f, i+1, tt.kinds[i])
				}
				if f.IsList() != (name == "authority_addresses" || name == "features" || name == "si_indexers" || name == "verifiers") {
					t.Fatalf("field %s repeated = %v", name, f.IsList())
				}
			}
		})
	}
	update := (&pb.UpdateConsensusParamsCmd{}).ProtoReflect().Descriptor().Fields().ByName("update")
	if update.Message().FullName() != "arbiter.ConsensusParamsUpdate" {
		t.Fatalf("update message type = %s", update.Message().FullName())
	}
	state := (&pb.ConsensusParamsState{}).ProtoReflect().Descriptor()
	for _, name := range []protoreflect.Name{"bootstrap", "current"} {
		if got := state.Fields().ByName(name).Message().FullName(); got != "arbiter.ConsensusMutableParams" {
			t.Fatalf("%s message type = %s", name, got)
		}
	}

	for _, m := range []proto.Message{&pb.ConsensusParamsUpdate{}, &pb.ConsensusMutableParams{}} {
		f := m.ProtoReflect().Descriptor().Fields().ByName("table_registry")
		if got := f.Message().FullName(); got != "arbiter.TableRegistryParams" {
			t.Fatalf("%s.table_registry type = %s", m.ProtoReflect().Descriptor().Name(), got)
		}
		f = m.ProtoReflect().Descriptor().Fields().ByName("client_lanes")
		if got := f.Message().FullName(); got != "arbiter.ClientLaneParams" {
			t.Fatalf("%s.client_lanes type = %s", m.ProtoReflect().Descriptor().Name(), got)
		}
		for name, want := range map[protoreflect.Name]protoreflect.FullName{"si_indexers": "arbiter.SIIndexerEntry", "verifiers": "arbiter.VerifierEntry"} {
			if got := m.ProtoReflect().Descriptor().Fields().ByName(name).Message().FullName(); got != want {
				t.Fatalf("%s.%s type = %s, want %s", m.ProtoReflect().Descriptor().Name(), name, got, want)
			}
		}
	}
}

func TestConsensusUpdateAppendsRaftSlot18(t *testing.T) {
	d := (&pb.RaftCommand{}).ProtoReflect().Descriptor()
	f := d.Fields().ByName("update_consensus_params")
	if f == nil || f.Number() != 18 || f.Message().FullName() != "arbiter.UpdateConsensusParamsCmd" || f.ContainingOneof() == nil || f.ContainingOneof().Name() != "cmd" {
		t.Fatalf("update_consensus_params Raft slot = %v", f)
	}
	if previous := d.Fields().ByName("evict_node"); previous == nil || previous.Number() != 17 {
		t.Fatal("previous Raft command slot changed")
	}
}

func TestConsensusAdminRPCSignatures(t *testing.T) {
	service := pb.File_consensus_proto.Services().ByName("ConsensusAdmin")
	if service == nil || service.Methods().Len() != 8 {
		t.Fatalf("ConsensusAdmin service = %v", service)
	}
	for _, tt := range []struct {
		name          protoreflect.Name
		input, output protoreflect.FullName
	}{
		{"GetProtocolInfo", "google.protobuf.Empty", "arbiter.ProtocolInfo"},
		{"GetConsensusParams", "google.protobuf.Empty", "arbiter.ConsensusParamsState"},
		{"UpdateConsensusParams", "arbiter.UpdateConsensusParamsCmd", "arbiter.Ack"},
		{"GetSnapshotQueryAbortCandidate", "arbiter.GetSnapshotQueryAbortCandidateRequest", "arbiter.SnapshotQueryAbortRecord"},
		{"AbortSnapshotQuery", "arbiter.AbortSnapshotQueryRequest", "arbiter.SnapshotQueryStatus"},
		{"ActivateQueryProfile", "arbiter.ActivateQueryProfileRequest", "arbiter.Ack"},
		{"GetNodeFeatures", "google.protobuf.Empty", "arbiter.NodeFeatures"},
		{"EvictNode", "arbiter.EvictNodeRequest", "arbiter.Ack"},
	} {
		method := service.Methods().ByName(tt.name)
		if method == nil || method.Input().FullName() != tt.input || method.Output().FullName() != tt.output || method.IsStreamingClient() || method.IsStreamingServer() {
			t.Fatalf("RPC %s = %v, want unary %s -> %s", tt.name, method, tt.input, tt.output)
		}
	}
}
