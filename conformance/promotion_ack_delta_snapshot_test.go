package conformance

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// promotionAckDeltaSnapshotFields pins the bool the promotion acknowledgement
// delta snapshot switch appends as the last field of both consensus parameter
// messages, after membership_lease_l2_blocks.
var promotionAckDeltaSnapshotFields = map[string]*descriptorpb.FieldDescriptorProto{
	"ConsensusMutableParams": signedClaimsField("promotion_ack_delta_snapshot", "promotionAckDeltaSnapshot", 18, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
	"ConsensusParamsUpdate":  signedClaimsField("promotion_ack_delta_snapshot", "promotionAckDeltaSnapshot", 23, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
}

// stripPromotionAckDeltaSnapshotField removes the trailing
// promotion_ack_delta_snapshot of a consensus parameter message after
// asserting it is exactly the pinned field, so the older baseline comparisons
// see the message as it was before.
func stripPromotionAckDeltaSnapshotField(t *testing.T, d *descriptorpb.DescriptorProto) {
	t.Helper()
	stripSnapshotQueryTracksField(t, d)
	stripConsensusIdentityFields(t, d)
	want, ok := promotionAckDeltaSnapshotFields[d.GetName()]
	if !ok {
		return
	}
	if len(d.Field) == 0 || !proto.Equal(d.Field[len(d.Field)-1], want) {
		t.Fatalf("%s promotion ack delta snapshot addition must be exactly %s = %d: %v", d.GetName(), want.GetName(), want.GetNumber(), d)
	}
	d.Field = d.Field[:len(d.Field)-1]
}

// TestPromotionAckDeltaSnapshotFieldNumbers pins the switch's name, number
// and kind. arbiter-core's strict Raft decoder refuses a field its descriptor
// does not know, so a voter that disagrees on the number forks on the first
// update that sets it. The field is plain proto3, outside any oneof, so it is
// off the wire while false and every earlier update and params digest
// encodes byte for byte as before.
func TestPromotionAckDeltaSnapshotFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg    proto.Message
		number protoreflect.FieldNumber
	}{
		{&pb.ConsensusMutableParams{}, 18},
		{&pb.ConsensusParamsUpdate{}, 23},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			f := d.Fields().ByName("promotion_ack_delta_snapshot")
			if f == nil || f.Number() != tc.number || f.Kind() != protoreflect.BoolKind || f.IsList() || f.HasOptionalKeyword() || f.ContainingOneof() != nil {
				t.Fatalf("field = %v, want plain bool number %d", f, tc.number)
			}
			// snapshot_query_tracks, custody_frontier and
			// operations_authority_addresses are the only fields appended after it.
			if got := d.Fields().Len(); got != int(tc.number)+4 {
				t.Fatalf("field count = %d, want %d (only snapshot_query_tracks, custody_frontier and operations_authority_addresses and history_retain_blocks follow promotion_ack_delta_snapshot)", got, tc.number+4)
			}
		})
	}
	for _, pair := range [][2]proto.Message{
		{&pb.ConsensusParamsUpdate{NetworkId: "n", ManifestDeltaSnapshot: true}, &pb.ConsensusParamsUpdate{NetworkId: "n", ManifestDeltaSnapshot: true, PromotionAckDeltaSnapshot: false}},
		{&pb.ConsensusMutableParams{MaxWriters: 3}, &pb.ConsensusMutableParams{MaxWriters: 3, PromotionAckDeltaSnapshot: false}},
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
			t.Fatalf("%T: an unset promotion_ack_delta_snapshot must stay off the wire", pair[0])
		}
	}
	raw, err := proto.Marshal(&pb.ConsensusParamsUpdate{PromotionAckDeltaSnapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	if number, typ, n := protowire.ConsumeTag(raw); n < 0 || number != 23 || typ != protowire.VarintType {
		t.Fatalf("promotion_ack_delta_snapshot encoded as tag %d type %d", number, typ)
	}
}
