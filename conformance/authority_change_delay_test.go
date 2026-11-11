package conformance

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

func authorityChangeDelayFieldsAt(delay, pending int32) []*descriptorpb.FieldDescriptorProto {
	p := signedClaimsField("pending_authority_change", "pendingAuthorityChange", pending, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE)
	p.TypeName = proto.String(".arbiter.PendingAuthorityChange")
	return []*descriptorpb.FieldDescriptorProto{
		signedClaimsField("authority_change_delay_l2_blocks", "authorityChangeDelayL2Blocks", delay, descriptorpb.FieldDescriptorProto_TYPE_UINT64), p,
	}
}

// authorityChangeDelayFields pins the two fields the authority change delay
// appends as the last fields of both consensus parameter messages, after
// history_retain_blocks.
var authorityChangeDelayFields = map[string][]*descriptorpb.FieldDescriptorProto{
	"ConsensusMutableParams": authorityChangeDelayFieldsAt(23, 24),
	"ConsensusParamsUpdate":  authorityChangeDelayFieldsAt(28, 29),
}

// stripAuthorityChangeDelayFields removes the trailing authority change delay
// fields of a message after asserting they are exactly the pinned ones, so
// the older baseline comparisons see the message as it was before.
func stripAuthorityChangeDelayFields(t *testing.T, d *descriptorpb.DescriptorProto) {
	t.Helper()
	want, ok := authorityChangeDelayFields[d.GetName()]
	if !ok {
		return
	}
	n := len(d.Field) - len(want)
	if n < 0 {
		t.Fatalf("%s lacks the authority change delay fields: %v", d.GetName(), d)
	}
	for i, w := range want {
		if !proto.Equal(d.Field[n+i], w) {
			t.Fatalf("%s authority change delay addition must be exactly %s = %d: %v", d.GetName(), w.GetName(), w.GetNumber(), d)
		}
	}
	d.Field = d.Field[:n]
}

// authorityChangeDelayMessageFields is the field ledger of the message the
// authority change delay adds (the i-th token is field number i+1). Every
// number is a consensus constant: arbiter-core's strict Raft decoder refuses
// a field its descriptor does not know.
var authorityChangeDelayMessageFields = map[string]string{
	"PendingAuthorityChange": "authority_addresses:[]string authority_threshold:uint32 authority_change_delay_l2_blocks:uint64 not_before_l2_block:uint64",
}

func TestAuthorityChangeDelayFieldLedgers(t *testing.T) {
	assertMessageFieldLedgers(t, map[string]protoreflect.FileDescriptor{"PendingAuthorityChange": pb.File_consensus_proto}, authorityChangeDelayMessageFields)
}

// TestAuthorityChangeDelayFieldNumbers pins both fields on both consensus
// messages and that each is off the wire while unset, so every earlier
// update, params digest and Raft command encodes byte for byte as before.
func TestAuthorityChangeDelayFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg            proto.Message
		delay, pending protoreflect.FieldNumber
	}{
		{&pb.ConsensusMutableParams{}, 23, 24},
		{&pb.ConsensusParamsUpdate{}, 28, 29},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			delay := d.Fields().ByName("authority_change_delay_l2_blocks")
			if delay == nil || delay.Number() != tc.delay || delay.Kind() != protoreflect.Uint64Kind || delay.IsList() || delay.HasOptionalKeyword() || delay.ContainingOneof() != nil {
				t.Fatalf("delay field = %v, want plain uint64 number %d", delay, tc.delay)
			}
			pending := d.Fields().ByName("pending_authority_change")
			if pending == nil || pending.Number() != tc.pending || pending.Kind() != protoreflect.MessageKind || pending.IsList() || pending.Message().Name() != "PendingAuthorityChange" || pending.ContainingOneof() != nil {
				t.Fatalf("pending field = %v, want PendingAuthorityChange number %d", pending, tc.pending)
			}
			if got := d.Fields().Len(); got != int(tc.pending) {
				t.Fatalf("field count = %d, want %d (pending_authority_change is the last field)", got, tc.pending)
			}
		})
	}
	for _, pair := range [][2]proto.Message{
		{&pb.ConsensusParamsUpdate{NetworkId: "n", HistoryRetainBlocks: 5}, &pb.ConsensusParamsUpdate{NetworkId: "n", HistoryRetainBlocks: 5, AuthorityChangeDelayL2Blocks: 0, PendingAuthorityChange: nil}},
		{&pb.ConsensusMutableParams{MaxWriters: 3}, &pb.ConsensusMutableParams{MaxWriters: 3, AuthorityChangeDelayL2Blocks: 0}},
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
			t.Fatalf("%T: unset authority change delay fields must stay off the wire", pair[0])
		}
	}
	raw, err := proto.Marshal(&pb.ConsensusParamsUpdate{AuthorityChangeDelayL2Blocks: 43200})
	if err != nil {
		t.Fatal(err)
	}
	if number, typ, n := protowire.ConsumeTag(raw); n < 0 || number != 28 || typ != protowire.VarintType {
		t.Fatalf("authority_change_delay_l2_blocks encoded as tag %d type %d", number, typ)
	}
	raw, err = proto.Marshal(&pb.ConsensusParamsUpdate{PendingAuthorityChange: &pb.PendingAuthorityChange{NotBeforeL2Block: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if number, typ, n := protowire.ConsumeTag(raw); n < 0 || number != 29 || typ != protowire.BytesType {
		t.Fatalf("pending_authority_change encoded as tag %d type %d", number, typ)
	}
}
