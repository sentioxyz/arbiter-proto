package conformance

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// custodyAuditOnQuorumFields pins the bool custody AUDIT on a latched quorum
// appends as the last field of both consensus parameter messages, after
// pending_voter_change.
var custodyAuditOnQuorumFields = map[string]*descriptorpb.FieldDescriptorProto{
	"ConsensusMutableParams": signedClaimsField("custody_audit_on_quorum", "custodyAuditOnQuorum", 27, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
	"ConsensusParamsUpdate":  signedClaimsField("custody_audit_on_quorum", "custodyAuditOnQuorum", 32, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
}

// stripCustodyAuditOnQuorumField removes the trailing custody_audit_on_quorum
// of a consensus parameter message after asserting it is exactly the pinned
// field, so the older baseline comparisons see the message as it was before.
func stripCustodyAuditOnQuorumField(t *testing.T, d *descriptorpb.DescriptorProto) {
	t.Helper()
	want, ok := custodyAuditOnQuorumFields[d.GetName()]
	if !ok {
		return
	}
	if len(d.Field) == 0 || !proto.Equal(d.Field[len(d.Field)-1], want) {
		t.Fatalf("%s custody audit on quorum addition must be exactly %s = %d: %v", d.GetName(), want.GetName(), want.GetNumber(), d)
	}
	d.Field = d.Field[:len(d.Field)-1]
}

// TestCustodyAuditOnQuorumFieldNumbers pins the parameter on both consensus
// messages and that it is off the wire while unset, so every earlier update,
// params digest and Raft command encodes byte for byte as before.
func TestCustodyAuditOnQuorumFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg    proto.Message
		number protoreflect.FieldNumber
	}{
		{&pb.ConsensusMutableParams{}, 27},
		{&pb.ConsensusParamsUpdate{}, 32},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			f := d.Fields().ByName("custody_audit_on_quorum")
			if f == nil || f.Number() != tc.number || f.Kind() != protoreflect.BoolKind || f.IsList() || f.HasOptionalKeyword() || f.ContainingOneof() != nil {
				t.Fatalf("field = %v, want plain bool number %d", f, tc.number)
			}
			if got := d.Fields().Len(); got != int(tc.number) {
				t.Fatalf("field count = %d, want %d (custody_audit_on_quorum is the last field)", got, tc.number)
			}
		})
	}
	for _, pair := range [][2]proto.Message{
		{&pb.ConsensusParamsUpdate{NetworkId: "n", CustodyFrontier: true}, &pb.ConsensusParamsUpdate{NetworkId: "n", CustodyFrontier: true, CustodyAuditOnQuorum: false}},
		{&pb.ConsensusMutableParams{MaxWriters: 3}, &pb.ConsensusMutableParams{MaxWriters: 3, CustodyAuditOnQuorum: false}},
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
			t.Fatalf("%T: an unset custody_audit_on_quorum must stay off the wire", pair[0])
		}
	}
	raw, err := proto.Marshal(&pb.ConsensusParamsUpdate{CustodyAuditOnQuorum: true})
	if err != nil {
		t.Fatal(err)
	}
	if number, typ, n := protowire.ConsumeTag(raw); n < 0 || number != 32 || typ != protowire.VarintType {
		t.Fatalf("custody_audit_on_quorum encoded as tag %d type %d", number, typ)
	}
}

// custodyAuditOnQuorumCount is how many fields custody AUDIT on a latched
// quorum appends to the named message: one on each consensus parameter
// message, none elsewhere.
func custodyAuditOnQuorumCount(message string) int {
	if _, ok := custodyAuditOnQuorumFields[message]; ok {
		return 1
	}
	return 0
}
