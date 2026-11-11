package conformance

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// raftMembershipFields pins the fields the Raft membership change
// (arbiter#172) appends to ProtocolInfo, after authority_signer_address.
var raftMembershipFields = map[string][]*descriptorpb.FieldDescriptorProto{
	"ProtocolInfo": {
		signedClaimsField("raft_applied_index", "raftAppliedIndex", 7, descriptorpb.FieldDescriptorProto_TYPE_UINT64),
		signedClaimsField("network_id", "networkId", 8, descriptorpb.FieldDescriptorProto_TYPE_STRING),
	},
}

// stripRaftMembershipFields removes the trailing Raft membership fields of a
// message after asserting they are exactly the pinned ones, so the older
// baseline comparisons see the message as it was before.
func stripRaftMembershipFields(t *testing.T, d *descriptorpb.DescriptorProto) {
	t.Helper()
	want, ok := raftMembershipFields[d.GetName()]
	if !ok {
		return
	}
	n := len(d.Field) - len(want)
	if n < 0 {
		t.Fatalf("%s lacks the Raft membership fields: %v", d.GetName(), d)
	}
	for i, w := range want {
		if !proto.Equal(d.Field[n+i], w) {
			t.Fatalf("%s Raft membership addition must be exactly %s = %d: %v", d.GetName(), w.GetName(), w.GetNumber(), d)
		}
	}
	d.Field = d.Field[:n]
}

// raftMembershipMessageFields is the field ledger of the messages the Raft
// membership change adds (the i-th token is field number i+1).
// RaftMembershipChange is a signed body: arbiter-core hashes its fields into
// the authority token's command digest.
var raftMembershipMessageFields = map[string]string{
	"RaftMembershipChange":        "op:RaftMembershipOp server_id:string raft_address:string",
	"ChangeRaftMembershipRequest": "change:RaftMembershipChange authority_jws:string prev_config_index:uint64 allow_below_three:bool",
	"ChangeRaftMembershipReply":   "config_index:uint64 already_applied:bool",
	"RaftServerView":              "id:string address:string voter:bool",
	"RaftConfigurationView":       "servers:[]RaftServerView config_index:uint64 enrolled_voter_ids:[]string state:RaftVoterSetState leader_id:string authority_epoch:uint64",
}

func TestRaftMembershipFieldLedgers(t *testing.T) {
	files := map[string]protoreflect.FileDescriptor{}
	for name := range raftMembershipMessageFields {
		files[name] = pb.File_consensus_proto
	}
	assertMessageFieldLedgers(t, files, raftMembershipMessageFields)
}

// TestRaftMembershipProtocolInfoFields pins raft_applied_index = 7 and
// network_id = 8 on ProtocolInfo and that each is off the wire at its zero
// value, so an older prober reads the same bytes as before.
func TestRaftMembershipProtocolInfoFields(t *testing.T) {
	d := (&pb.ProtocolInfo{}).ProtoReflect().Descriptor()
	for _, tc := range []struct {
		name   protoreflect.Name
		number protoreflect.FieldNumber
		kind   protoreflect.Kind
	}{
		{"raft_applied_index", 7, protoreflect.Uint64Kind},
		{"network_id", 8, protoreflect.StringKind},
	} {
		f := d.Fields().ByName(tc.name)
		if f == nil || f.Number() != tc.number || f.Kind() != tc.kind || f.IsList() || f.HasOptionalKeyword() || f.ContainingOneof() != nil {
			t.Fatalf("%s = %v, want plain %s number %d", tc.name, f, tc.kind, tc.number)
		}
	}
	if got := d.Fields().Len(); got != 8 {
		t.Fatalf("ProtocolInfo field count = %d, want 8", got)
	}
	a, err := proto.Marshal(&pb.ProtocolInfo{NodeId: "a"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := proto.Marshal(&pb.ProtocolInfo{NodeId: "a", RaftAppliedIndex: 0, NetworkId: ""})
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatal("zero Raft membership fields must stay off the wire")
	}
	raw, err := proto.Marshal(&pb.ProtocolInfo{NetworkId: "n"})
	if err != nil {
		t.Fatal(err)
	}
	if number, typ, n := protowire.ConsumeTag(raw); n < 0 || number != 8 || typ != protowire.BytesType {
		t.Fatalf("network_id encoded as tag %d type %d", number, typ)
	}
}

// TestRaftMembershipEnums pins both enums by number and name: arbiter-core
// hashes the op's name into the signed body, and arbiter-admin prints the
// state by name.
func TestRaftMembershipEnums(t *testing.T) {
	for _, tc := range []struct {
		values protoreflect.EnumValueDescriptors
		names  []protoreflect.Name
	}{
		{pb.RaftMembershipOp(0).Descriptor().Values(), []protoreflect.Name{"RAFT_MEMBERSHIP_OP_UNSPECIFIED", "RAFT_MEMBERSHIP_OP_ADD_VOTER", "RAFT_MEMBERSHIP_OP_REMOVE_SERVER"}},
		{pb.RaftVoterSetState(0).Descriptor().Values(), []protoreflect.Name{"RAFT_VOTER_SET_STATE_UNSPECIFIED", "RAFT_VOTER_SET_STATE_INACTIVE", "RAFT_VOTER_SET_STATE_CONSISTENT", "RAFT_VOTER_SET_STATE_TRANSITION", "RAFT_VOTER_SET_STATE_DRIFT"}},
	} {
		if tc.values.Len() != len(tc.names) {
			t.Fatalf("%d values, want %d", tc.values.Len(), len(tc.names))
		}
		for i, name := range tc.names {
			if v := tc.values.ByNumber(protoreflect.EnumNumber(i)); v == nil || v.Name() != name {
				t.Fatalf("value %d = %v, want %s", i, v, name)
			}
		}
	}
}

// TestRaftMembershipConsensusAdminMethods pins the two RPCs as the last
// ConsensusAdmin methods, so every earlier method keeps its position.
func TestRaftMembershipConsensusAdminMethods(t *testing.T) {
	svc := pb.File_consensus_proto.Services().ByName("ConsensusAdmin")
	for _, tc := range []struct {
		index         int
		name          protoreflect.Name
		input, output protoreflect.FullName
		full          string
	}{
		{18, "GetRaftConfiguration", "google.protobuf.Empty", "arbiter.RaftConfigurationView", pb.ConsensusAdmin_GetRaftConfiguration_FullMethodName},
		{19, "ChangeRaftMembership", "arbiter.ChangeRaftMembershipRequest", "arbiter.ChangeRaftMembershipReply", pb.ConsensusAdmin_ChangeRaftMembership_FullMethodName},
	} {
		m := svc.Methods().Get(tc.index)
		if m.Name() != tc.name || m.Input().FullName() != tc.input || m.Output().FullName() != tc.output || m.IsStreamingClient() || m.IsStreamingServer() {
			t.Fatalf("ConsensusAdmin method %d = %v, want unary %s(%s) %s", tc.index, m, tc.name, tc.input, tc.output)
		}
		if want := "/arbiter.ConsensusAdmin/" + string(tc.name); tc.full != want {
			t.Fatalf("full method name = %s, want %s", tc.full, want)
		}
	}
}
