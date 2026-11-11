package conformance

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// authorityThresholdFields pins the governance authority threshold appended
// as the last field of both consensus parameter messages.
var authorityThresholdFields = map[string]*descriptorpb.FieldDescriptorProto{
	"ConsensusMutableParams": signedClaimsField("authority_threshold", "authorityThreshold", 16, descriptorpb.FieldDescriptorProto_TYPE_UINT32),
	"ConsensusParamsUpdate":  signedClaimsField("authority_threshold", "authorityThreshold", 21, descriptorpb.FieldDescriptorProto_TYPE_UINT32),
}

// stripAuthorityThresholdField removes the trailing authority_threshold of a
// consensus parameter message after asserting it is exactly the pinned field,
// so the older baseline comparisons see the message as it was before.
func stripAuthorityThresholdField(t *testing.T, d *descriptorpb.DescriptorProto) {
	t.Helper()
	stripMembershipLeaseField(t, d)
	want, ok := authorityThresholdFields[d.GetName()]
	if !ok {
		return
	}
	if len(d.Field) == 0 || !proto.Equal(d.Field[len(d.Field)-1], want) {
		t.Fatalf("%s authority threshold addition must be exactly %s = %d: %v", d.GetName(), want.GetName(), want.GetNumber(), d)
	}
	d.Field = d.Field[:len(d.Field)-1]
}

// TestAuthorityThresholdFieldNumbers pins the threshold's name, number and
// kind on both messages. arbiter-core's strict Raft decoder refuses a field
// its descriptor does not know, so a voter that disagrees on the number forks
// on the first update that sets it. The field is plain proto3 outside any
// oneof, so it is off the wire at zero and every earlier update, params read
// and Raft command encodes byte for byte as before.
func TestAuthorityThresholdFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg    proto.Message
		number protoreflect.FieldNumber
	}{
		{&pb.ConsensusMutableParams{}, 16},
		{&pb.ConsensusParamsUpdate{}, 21},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			f := d.Fields().ByName("authority_threshold")
			if f == nil || f.Number() != tc.number || f.Kind() != protoreflect.Uint32Kind || f.IsList() || f.HasOptionalKeyword() || f.ContainingOneof() != nil {
				t.Fatalf("field = %v, want plain uint32 number %d", f, tc.number)
			}
			// membership_lease_l2_blocks, promotion_ack_delta_snapshot,
			// snapshot_query_tracks, custody_frontier and
			// operations_authority_addresses are the only fields appended after it.
			if got := d.Fields().Len(); got != int(tc.number)+9 {
				t.Fatalf("field count = %d, want %d (only membership_lease_l2_blocks, promotion_ack_delta_snapshot, snapshot_query_tracks, custody_frontier and operations_authority_addresses and history_retain_blocks and the authority change delay fields and account_budget follow authority_threshold)", got, tc.number+9)
			}
		})
	}
	for _, pair := range [][2]proto.Message{
		{&pb.ConsensusParamsUpdate{NetworkId: "n"}, &pb.ConsensusParamsUpdate{NetworkId: "n", AuthorityThreshold: 0}},
		{&pb.ConsensusMutableParams{MaxWriters: 3}, &pb.ConsensusMutableParams{MaxWriters: 3, AuthorityThreshold: 0}},
	} {
		raw, err := proto.Marshal(pair[0])
		if err != nil {
			t.Fatal(err)
		}
		want, err := proto.Marshal(pair[1])
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != string(want) {
			t.Fatalf("%T: a zero authority_threshold must stay off the wire", pair[0])
		}
	}
	raw, err := proto.Marshal(&pb.ConsensusParamsUpdate{AuthorityThreshold: 2})
	if err != nil {
		t.Fatal(err)
	}
	if number, typ, n := protowire.ConsumeTag(raw); n < 0 || number != 21 || typ != 0 {
		t.Fatalf("authority_threshold encoded as tag %d type %d", number, typ)
	}
}
