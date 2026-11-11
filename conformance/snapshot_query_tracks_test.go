package conformance

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// snapshotQueryTracksFields pins the bool the track-aware snapshot-query
// parameter appends as the last field of both consensus parameter messages,
// after promotion_ack_delta_snapshot.
var snapshotQueryTracksFields = map[string]*descriptorpb.FieldDescriptorProto{
	"ConsensusMutableParams": signedClaimsField("snapshot_query_tracks", "snapshotQueryTracks", 19, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
	"ConsensusParamsUpdate":  signedClaimsField("snapshot_query_tracks", "snapshotQueryTracks", 24, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
}

// stripSnapshotQueryTracksField removes the trailing snapshot_query_tracks of
// a consensus parameter message after asserting it is exactly the pinned
// field, so the older baseline comparisons see the message as it was before.
func stripSnapshotQueryTracksField(t *testing.T, d *descriptorpb.DescriptorProto) {
	t.Helper()
	want, ok := snapshotQueryTracksFields[d.GetName()]
	if !ok {
		return
	}
	if len(d.Field) == 0 || !proto.Equal(d.Field[len(d.Field)-1], want) {
		t.Fatalf("%s snapshot query tracks addition must be exactly %s = %d: %v", d.GetName(), want.GetName(), want.GetNumber(), d)
	}
	d.Field = d.Field[:len(d.Field)-1]
}

// TestSnapshotQueryTracksFieldNumbers pins the parameter's name, number and
// kind. arbiter-core's strict Raft decoder refuses a field its descriptor does
// not know, so a voter that disagrees on the number forks on the first update
// that sets it. The field is plain proto3, outside any oneof, so it is off the
// wire while false and every earlier update and params digest encodes byte
// for byte as before.
func TestSnapshotQueryTracksFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg    proto.Message
		number protoreflect.FieldNumber
	}{
		{&pb.ConsensusMutableParams{}, 19},
		{&pb.ConsensusParamsUpdate{}, 24},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			f := d.Fields().ByName("snapshot_query_tracks")
			if f == nil || f.Number() != tc.number || f.Kind() != protoreflect.BoolKind || f.IsList() || f.HasOptionalKeyword() || f.ContainingOneof() != nil {
				t.Fatalf("field = %v, want plain bool number %d", f, tc.number)
			}
			if got := d.Fields().Len(); got != int(tc.number) {
				t.Fatalf("field count = %d, want %d (snapshot_query_tracks is the last field)", got, tc.number)
			}
		})
	}
	for _, pair := range [][2]proto.Message{
		{&pb.ConsensusParamsUpdate{NetworkId: "n", TracksEnabled: true}, &pb.ConsensusParamsUpdate{NetworkId: "n", TracksEnabled: true, SnapshotQueryTracks: false}},
		{&pb.ConsensusMutableParams{MaxWriters: 3}, &pb.ConsensusMutableParams{MaxWriters: 3, SnapshotQueryTracks: false}},
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
			t.Fatalf("%T: an unset snapshot_query_tracks must stay off the wire", pair[0])
		}
	}
	raw, err := proto.Marshal(&pb.ConsensusParamsUpdate{SnapshotQueryTracks: true})
	if err != nil {
		t.Fatal(err)
	}
	if number, typ, n := protowire.ConsumeTag(raw); n < 0 || number != 24 || typ != protowire.VarintType {
		t.Fatalf("snapshot_query_tracks encoded as tag %d type %d", number, typ)
	}
}

// TestSnapshotQueryControlTargetFieldNumbers pins target_table_id on the
// three control requests. It is a plain string appended after the existing
// fields, so a request without a target (every v1 control on a network
// without tracks) encodes byte for byte as before.
func TestSnapshotQueryControlTargetFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg    proto.Message
		number protoreflect.FieldNumber
	}{
		{&pb.AcquireSnapshotQueryRequest{}, 7},
		{&pb.GetSnapshotQueryReservationRequest{}, 9},
		{&pb.ReleaseSnapshotQueryRequest{}, 9},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			f := d.Fields().ByName("target_table_id")
			if f == nil || f.Number() != tc.number || f.Kind() != protoreflect.StringKind || f.IsList() || f.HasOptionalKeyword() || f.ContainingOneof() != nil {
				t.Fatalf("field = %v, want plain string number %d", f, tc.number)
			}
			if got := d.Fields().Len(); got != int(tc.number) {
				t.Fatalf("field count = %d, want %d (target_table_id is the last field)", got, tc.number)
			}
		})
	}
	a, _ := proto.Marshal(&pb.AcquireSnapshotQueryRequest{NetworkId: "n", ControlJws: "t"})
	b, _ := proto.Marshal(&pb.AcquireSnapshotQueryRequest{NetworkId: "n", ControlJws: "t", TargetTableId: ""})
	if string(a) != string(b) {
		t.Fatal("an empty target_table_id must stay off the wire")
	}
}
