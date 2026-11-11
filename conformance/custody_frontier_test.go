package conformance

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// custodyFrontierFields pins the bool the committed custody frontier appends
// as the last field of both consensus parameter messages, after
// snapshot_query_tracks.
var custodyFrontierFields = map[string]*descriptorpb.FieldDescriptorProto{
	"ConsensusMutableParams": signedClaimsField("custody_frontier", "custodyFrontier", 20, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
	"ConsensusParamsUpdate":  signedClaimsField("custody_frontier", "custodyFrontier", 25, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
}

// stripCustodyFrontierField removes the trailing custody_frontier of a
// consensus parameter message after asserting it is exactly the pinned field,
// so the older baseline comparisons see the message as it was before.
func stripCustodyFrontierField(t *testing.T, d *descriptorpb.DescriptorProto) {
	t.Helper()
	stripOperationsAuthorityField(t, d)
	want, ok := custodyFrontierFields[d.GetName()]
	if !ok {
		return
	}
	if len(d.Field) == 0 || !proto.Equal(d.Field[len(d.Field)-1], want) {
		t.Fatalf("%s custody frontier addition must be exactly %s = %d: %v", d.GetName(), want.GetName(), want.GetNumber(), d)
	}
	d.Field = d.Field[:len(d.Field)-1]
}

// custodyFrontierMessageFields is the field ledger of every message the
// custody frontier adds (the i-th token is field number i+1). Every number is
// a consensus constant: arbiter-core's strict Raft decoder refuses a field its
// descriptor does not know.
var custodyFrontierMessageFields = map[string]string{
	"CustodyMark":              "block_seq:uint64 stage:CustodyStage",
	"RecordCustodyProgressCmd": "marks:[]CustodyMark",
}

func TestCustodyFrontierFieldLedgers(t *testing.T) {
	assertMessageFieldLedgers(t, map[string]protoreflect.FileDescriptor{
		"CustodyMark":              pb.File_raftlog_proto,
		"RecordCustodyProgressCmd": pb.File_raftlog_proto,
	}, custodyFrontierMessageFields)
}

// TestCustodyStageValues pins the stages by number and name: a mark's stage
// is replicated state, so a renumbering would change what every voter
// recorded.
func TestCustodyStageValues(t *testing.T) {
	values := pb.CustodyStage(0).Descriptor().Values()
	if values.Len() != 3 {
		t.Fatalf("CustodyStage has %d values, want 3", values.Len())
	}
	for number, name := range map[protoreflect.EnumNumber]protoreflect.Name{
		0: "CUSTODY_STAGE_UNSPECIFIED",
		1: "CUSTODY_STAGE_REPLAY_PINNED",
		2: "CUSTODY_STAGE_AUDIT_DONE",
	} {
		if v := values.ByNumber(number); v == nil || v.Name() != name {
			t.Fatalf("CustodyStage %d = %v, want %s", number, v, name)
		}
	}
}

// TestCustodyFrontierFieldNumbers pins the parameter on both consensus
// messages and Raft tag 45, and that an unset parameter is off the wire, so
// every earlier update, params digest and Raft command encodes byte for byte
// as before.
func TestCustodyFrontierFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg    proto.Message
		number protoreflect.FieldNumber
	}{
		{&pb.ConsensusMutableParams{}, 20},
		{&pb.ConsensusParamsUpdate{}, 25},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			f := d.Fields().ByName("custody_frontier")
			if f == nil || f.Number() != tc.number || f.Kind() != protoreflect.BoolKind || f.IsList() || f.HasOptionalKeyword() || f.ContainingOneof() != nil {
				t.Fatalf("field = %v, want plain bool number %d", f, tc.number)
			}
			// operations_authority_addresses and history_retain_blocks are the only
			// fields appended after it.
			if got := d.Fields().Len(); got != int(tc.number)+4 {
				t.Fatalf("field count = %d, want %d (only operations_authority_addresses and history_retain_blocks and the authority change delay fields follow custody_frontier)", got, tc.number+4)
			}
		})
	}
	for _, pair := range [][2]proto.Message{
		{&pb.ConsensusParamsUpdate{NetworkId: "n", SnapshotQueryTracks: true}, &pb.ConsensusParamsUpdate{NetworkId: "n", SnapshotQueryTracks: true, CustodyFrontier: false}},
		{&pb.ConsensusMutableParams{MaxWriters: 3}, &pb.ConsensusMutableParams{MaxWriters: 3, CustodyFrontier: false}},
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
			t.Fatalf("%T: an unset custody_frontier must stay off the wire", pair[0])
		}
	}
	raw, err := proto.Marshal(&pb.ConsensusParamsUpdate{CustodyFrontier: true})
	if err != nil {
		t.Fatal(err)
	}
	if number, typ, n := protowire.ConsumeTag(raw); n < 0 || number != 25 || typ != protowire.VarintType {
		t.Fatalf("custody_frontier encoded as tag %d type %d", number, typ)
	}

	raft := (&pb.RaftCommand{}).ProtoReflect().Descriptor()
	if f := raft.Fields().ByNumber(45); f == nil || f.Name() != "record_custody_progress" || f.Message().Name() != "RecordCustodyProgressCmd" || f.ContainingOneof() == nil || f.ContainingOneof().Name() != "cmd" {
		t.Fatalf("RaftCommand tag 45 = %v", f)
	}
	assertSnapshotVariant(t, &pb.RaftCommand{}, "cmd", "record_custody_progress", 45, "RecordCustodyProgressCmd")
}
