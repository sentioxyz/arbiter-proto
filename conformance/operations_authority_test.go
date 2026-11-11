package conformance

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// operationsAuthorityFields pins the fields the operations authority set
// appends as the last field of both consensus parameter messages (after
// custody_frontier) and of ProtocolInfo (after voter_ed25519_pubkey).
var operationsAuthorityFields = map[string]*descriptorpb.FieldDescriptorProto{
	"ConsensusMutableParams": operationsAuthorityList(21),
	"ConsensusParamsUpdate":  operationsAuthorityList(26),
	"ProtocolInfo":           signedClaimsField("authority_signer_address", "authoritySignerAddress", 6, descriptorpb.FieldDescriptorProto_TYPE_STRING),
}

func operationsAuthorityList(number int32) *descriptorpb.FieldDescriptorProto {
	f := signedClaimsField("operations_authority_addresses", "operationsAuthorityAddresses", number, descriptorpb.FieldDescriptorProto_TYPE_STRING)
	f.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
	return f
}

// stripOperationsAuthorityField removes the trailing operations authority
// field of a message after asserting it is exactly the pinned field, so the
// older baseline comparisons see the message as it was before.
func stripOperationsAuthorityField(t *testing.T, d *descriptorpb.DescriptorProto) {
	t.Helper()
	stripHistoryRetentionFields(t, d)
	want, ok := operationsAuthorityFields[d.GetName()]
	if !ok {
		return
	}
	if len(d.Field) == 0 || !proto.Equal(d.Field[len(d.Field)-1], want) {
		t.Fatalf("%s operations authority addition must be exactly %s = %d: %v", d.GetName(), want.GetName(), want.GetNumber(), d)
	}
	d.Field = d.Field[:len(d.Field)-1]
}

// TestOperationsAuthorityFieldNumbers pins the operations authority set on
// both consensus messages and the voter's signer address on ProtocolInfo, and
// that each is off the wire while empty, so every earlier update, params
// digest, Raft command and protocol read encodes byte for byte as before.
// arbiter-core's strict Raft decoder refuses a field its descriptor does not
// know, so a voter that disagrees on the number forks on the first update
// that sets it.
func TestOperationsAuthorityFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg      proto.Message
		name     protoreflect.Name
		number   protoreflect.FieldNumber
		repeated bool
	}{
		{&pb.ConsensusMutableParams{}, "operations_authority_addresses", 21, true},
		{&pb.ConsensusParamsUpdate{}, "operations_authority_addresses", 26, true},
		{&pb.ProtocolInfo{}, "authority_signer_address", 6, false},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			f := d.Fields().ByName(tc.name)
			if f == nil || f.Number() != tc.number || f.Kind() != protoreflect.StringKind || f.IsList() != tc.repeated || f.HasOptionalKeyword() || f.ContainingOneof() != nil {
				t.Fatalf("field = %v, want plain string number %d repeated %v", f, tc.number, tc.repeated)
			}
			// history_retain_blocks and the read flags of history pruning are
			// the only fields appended after it.
			if got := d.Fields().Len(); got != int(tc.number)+len(historyRetentionFields[string(d.Name())])+len(authorityChangeDelayFields[string(d.Name())]) {
				t.Fatalf("field count = %d, want %d plus the history pruning fields", got, tc.number)
			}
		})
	}
	for _, pair := range [][2]proto.Message{
		{&pb.ConsensusParamsUpdate{NetworkId: "n", CustodyFrontier: true}, &pb.ConsensusParamsUpdate{NetworkId: "n", CustodyFrontier: true, OperationsAuthorityAddresses: []string{}}},
		{&pb.ConsensusMutableParams{MaxWriters: 3}, &pb.ConsensusMutableParams{MaxWriters: 3, OperationsAuthorityAddresses: []string{}}},
		{&pb.ProtocolInfo{NodeId: "a"}, &pb.ProtocolInfo{NodeId: "a", AuthoritySignerAddress: ""}},
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
			t.Fatalf("%T: an empty operations authority field must stay off the wire", pair[0])
		}
	}
	raw, err := proto.Marshal(&pb.ConsensusParamsUpdate{OperationsAuthorityAddresses: []string{"0x01"}})
	if err != nil {
		t.Fatal(err)
	}
	if number, typ, n := protowire.ConsumeTag(raw); n < 0 || number != 26 || typ != protowire.BytesType {
		t.Fatalf("operations_authority_addresses encoded as tag %d type %d", number, typ)
	}
}
