package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// promotionFinalityGateFields pins the bool the promotion finality gate
// appends to each consensus parameter message: ConsensusMutableParams = 10 and
// ConsensusParamsUpdate = 15. Each is its message's last field once
// manifest_delta_snapshot is stripped, so assertSnapshotBaselineDescriptors
// strips it right after that field, before every older exemption.
// Every existing baseline predates it, so the exemption applies
// unconditionally.
var promotionFinalityGateFields = map[string]*descriptorpb.FieldDescriptorProto{
	"ConsensusMutableParams": signedClaimsField("promotion_finality_gate", "promotionFinalityGate", 10, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
	"ConsensusParamsUpdate":  signedClaimsField("promotion_finality_gate", "promotionFinalityGate", 15, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
}

// TestPromotionFinalityGateFieldNumbers pins the gate's name, number and
// kind. arbiter-core's strict Raft decoder refuses a field its descriptor does
// not know, so a voter that disagrees on the number forks on the first update
// that sets it. The field is plain proto3, outside any oneof, so it is off the
// wire while false and every earlier update encodes byte for byte as before.
func TestPromotionFinalityGateFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg    proto.Message
		number protoreflect.FieldNumber
	}{
		{&pb.ConsensusMutableParams{}, 10},
		{&pb.ConsensusParamsUpdate{}, 15},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			f := d.Fields().ByName("promotion_finality_gate")
			if f == nil || f.Number() != tc.number || f.Kind() != protoreflect.BoolKind || f.IsList() || f.HasOptionalKeyword() || f.ContainingOneof() != nil {
				t.Fatalf("field = %v, want plain bool number %d", f, tc.number)
			}
			// manifest_delta_snapshot is the only field appended after it.
			if got := d.Fields().Len(); got != int(tc.number)+1 {
				t.Fatalf("field count = %d, want %d (only manifest_delta_snapshot follows the gate)", got, tc.number+1)
			}
		})
	}
	// Unset stays off the wire: an update without the gate encodes exactly as
	// one built before the field existed.
	raw, err := proto.Marshal(&pb.ConsensusParamsUpdate{NetworkId: "n", TracksEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	want, err := proto.Marshal(&pb.ConsensusParamsUpdate{NetworkId: "n", TracksEnabled: true, PromotionFinalityGate: false})
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != string(want) {
		t.Fatal("an unset promotion_finality_gate must stay off the wire")
	}
}
