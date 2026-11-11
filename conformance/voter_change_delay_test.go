package conformance

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

func voterChangeDelayFieldAt(number int32) []*descriptorpb.FieldDescriptorProto {
	f := signedClaimsField("pending_voter_change", "pendingVoterChange", number, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE)
	f.TypeName = proto.String(".arbiter.PendingVoterChange")
	return []*descriptorpb.FieldDescriptorProto{f}
}

// voterChangeDelayFields pins the field the voter change delay
// (arbiter#223) appends as the last field of both consensus parameter
// messages, after account_budget.
var voterChangeDelayFields = map[string][]*descriptorpb.FieldDescriptorProto{
	"ConsensusMutableParams": voterChangeDelayFieldAt(26),
	"ConsensusParamsUpdate":  voterChangeDelayFieldAt(31),
}

// stripVoterChangeDelayFields removes the trailing pending voter change
// field of a message after asserting it is exactly the pinned one, so the
// older baseline comparisons see the message as it was before.
func stripVoterChangeDelayFields(t *testing.T, d *descriptorpb.DescriptorProto) {
	t.Helper()
	want, ok := voterChangeDelayFields[d.GetName()]
	if !ok {
		return
	}
	n := len(d.Field) - len(want)
	if n < 0 {
		t.Fatalf("%s lacks the pending voter change field: %v", d.GetName(), d)
	}
	for i, w := range want {
		if !proto.Equal(d.Field[n+i], w) {
			t.Fatalf("%s voter change delay addition must be exactly %s = %d: %v", d.GetName(), w.GetName(), w.GetNumber(), d)
		}
	}
	d.Field = d.Field[:n]
}

// voterChangeDelayMessageFields is the field ledger of the message the voter
// change delay adds (the i-th token is field number i+1). Every number is a
// consensus constant: arbiter-core's strict Raft decoder refuses a field its
// descriptor does not know.
var voterChangeDelayMessageFields = map[string]string{
	"PendingVoterChange": "additions:[]VoterEntry anchor_finality_quorum:uint32 not_before_l2_block:uint64",
}

func TestVoterChangeDelayFieldLedgers(t *testing.T) {
	assertMessageFieldLedgers(t, map[string]protoreflect.FileDescriptor{"PendingVoterChange": pb.File_consensus_proto}, voterChangeDelayMessageFields)
}

// TestVoterChangeDelayFieldNumbers pins the field on both consensus messages
// and that it is off the wire while unset, so every earlier update, params
// digest and Raft command encodes byte for byte as before.
func TestVoterChangeDelayFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg    proto.Message
		number protoreflect.FieldNumber
	}{
		{&pb.ConsensusMutableParams{}, 26},
		{&pb.ConsensusParamsUpdate{}, 31},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			f := d.Fields().ByName("pending_voter_change")
			if f == nil || f.Number() != tc.number || f.Kind() != protoreflect.MessageKind || f.IsList() || f.Message().Name() != "PendingVoterChange" || f.ContainingOneof() != nil {
				t.Fatalf("pending_voter_change field = %v, want PendingVoterChange number %d", f, tc.number)
			}
			if got := d.Fields().Len(); got != int(tc.number) {
				t.Fatalf("field count = %d, want %d (pending_voter_change is the last field)", got, tc.number)
			}
		})
	}
	for _, pair := range [][2]proto.Message{
		{&pb.ConsensusParamsUpdate{NetworkId: "n", HistoryRetainBlocks: 5}, &pb.ConsensusParamsUpdate{NetworkId: "n", HistoryRetainBlocks: 5, PendingVoterChange: nil}},
		{&pb.ConsensusMutableParams{MaxWriters: 3}, &pb.ConsensusMutableParams{MaxWriters: 3, PendingVoterChange: nil}},
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
			t.Fatalf("%T: an unset pending voter change must stay off the wire", pair[0])
		}
	}
	raw, err := proto.Marshal(&pb.ConsensusParamsUpdate{PendingVoterChange: &pb.PendingVoterChange{NotBeforeL2Block: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if number, typ, n := protowire.ConsumeTag(raw); n < 0 || number != 31 || typ != protowire.BytesType {
		t.Fatalf("pending_voter_change encoded as tag %d type %d", number, typ)
	}
}
