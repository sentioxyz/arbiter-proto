package conformance

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// membershipLeaseFields pins the membership lease appended as the last field
// of both consensus parameter messages, after authority_threshold.
var membershipLeaseFields = map[string]*descriptorpb.FieldDescriptorProto{
	"ConsensusMutableParams": signedClaimsField("membership_lease_l2_blocks", "membershipLeaseL2Blocks", 17, descriptorpb.FieldDescriptorProto_TYPE_UINT64),
	"ConsensusParamsUpdate":  signedClaimsField("membership_lease_l2_blocks", "membershipLeaseL2Blocks", 22, descriptorpb.FieldDescriptorProto_TYPE_UINT64),
}

// stripMembershipLeaseField removes the trailing membership_lease_l2_blocks
// of a consensus parameter message after asserting it is exactly the pinned
// field, so the older baseline comparisons see the message as it was before.
func stripMembershipLeaseField(t *testing.T, d *descriptorpb.DescriptorProto) {
	t.Helper()
	stripPromotionAckDeltaSnapshotField(t, d)
	want, ok := membershipLeaseFields[d.GetName()]
	if !ok {
		return
	}
	if len(d.Field) == 0 || !proto.Equal(d.Field[len(d.Field)-1], want) {
		t.Fatalf("%s membership lease addition must be exactly %s = %d: %v", d.GetName(), want.GetName(), want.GetNumber(), d)
	}
	d.Field = d.Field[:len(d.Field)-1]
}

// membershipLeaseMessageFields is the field ledger of every message the
// membership lease adds (the i-th token is field number i+1). Every number is
// a consensus constant: arbiter-core's strict Raft decoder refuses a field its
// descriptor does not know.
var membershipLeaseMessageFields = map[string]string{
	"NodeLeaseRenewal":  "node_id:string registration_seq:uint64 lease_seq:uint64 signer_jws:string ed25519_signature:string",
	"RenewNodeLeaseCmd": "renewal:NodeLeaseRenewal",
}

func TestMembershipLeaseFieldLedgers(t *testing.T) {
	assertMessageFieldLedgers(t, map[string]protoreflect.FileDescriptor{
		"NodeLeaseRenewal":  pb.File_arbiter_proto,
		"RenewNodeLeaseCmd": pb.File_raftlog_proto,
	}, membershipLeaseMessageFields)
}

// TestMembershipLeaseFieldNumbers pins the lease parameter on both consensus
// messages, Raft tag 44 and the Membership RPC, and that a zero lease is off
// the wire, so every earlier update, params digest and Raft command encodes
// byte for byte as before.
func TestMembershipLeaseFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg    proto.Message
		number protoreflect.FieldNumber
	}{
		{&pb.ConsensusMutableParams{}, 17},
		{&pb.ConsensusParamsUpdate{}, 22},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			f := d.Fields().ByName("membership_lease_l2_blocks")
			if f == nil || f.Number() != tc.number || f.Kind() != protoreflect.Uint64Kind || f.IsList() || f.HasOptionalKeyword() || f.ContainingOneof() != nil {
				t.Fatalf("field = %v, want plain uint64 number %d", f, tc.number)
			}
			// promotion_ack_delta_snapshot, snapshot_query_tracks and
			// custody_frontier are the only fields appended after it.
			if got := d.Fields().Len(); got != int(tc.number)+3 {
				t.Fatalf("field count = %d, want %d (only promotion_ack_delta_snapshot snapshot_query_tracks and custody_frontier follow membership_lease_l2_blocks)", got, tc.number+3)
			}
		})
	}
	for _, pair := range [][2]proto.Message{
		{&pb.ConsensusParamsUpdate{NetworkId: "n"}, &pb.ConsensusParamsUpdate{NetworkId: "n", MembershipLeaseL2Blocks: 0}},
		{&pb.ConsensusMutableParams{MaxWriters: 3}, &pb.ConsensusMutableParams{MaxWriters: 3, MembershipLeaseL2Blocks: 0}},
	} {
		a, err := proto.Marshal(pair[0])
		if err != nil {
			t.Fatal(err)
		}
		b, err := proto.Marshal(pair[1])
		if err != nil {
			t.Fatal(err)
		}
		if string(a) != string(b) {
			t.Fatalf("%T: a zero membership_lease_l2_blocks must stay off the wire", pair[0])
		}
	}
	raw, err := proto.Marshal(&pb.ConsensusParamsUpdate{MembershipLeaseL2Blocks: 600})
	if err != nil {
		t.Fatal(err)
	}
	if number, typ, n := protowire.ConsumeTag(raw); n < 0 || number != 22 || typ != protowire.VarintType {
		t.Fatalf("membership_lease_l2_blocks encoded as tag %d type %d", number, typ)
	}

	raft := (&pb.RaftCommand{}).ProtoReflect().Descriptor()
	if f := raft.Fields().ByNumber(44); f == nil || f.Name() != "renew_node_lease" || f.Message().Name() != "RenewNodeLeaseCmd" || f.ContainingOneof() == nil || f.ContainingOneof().Name() != "cmd" {
		t.Fatalf("RaftCommand tag 44 = %v", f)
	}
	assertSnapshotVariant(t, &pb.RaftCommand{}, "cmd", "renew_node_lease", 44, "RenewNodeLeaseCmd")

	svc := pb.File_arbiter_proto.Services().ByName("Membership")
	if svc == nil || svc.Methods().Len() != 3 {
		t.Fatalf("Membership = %v", svc)
	}
	m := svc.Methods().ByName("RenewNodeLease")
	if m == nil || m.Input().FullName() != "arbiter.NodeLeaseRenewal" || m.Output().FullName() != "arbiter.Ack" || m.IsStreamingClient() || m.IsStreamingServer() {
		t.Fatalf("RenewNodeLease = %v", m)
	}
	if pb.Membership_RenewNodeLease_FullMethodName != "/arbiter.Membership/RenewNodeLease" {
		t.Fatal("Membership.RenewNodeLease method name changed")
	}
}
