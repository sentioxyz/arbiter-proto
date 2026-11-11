package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// tableAdmissionRulesVersionFields pins the uint32 the table admission rules
// version appends to each consensus parameter message: ConsensusMutableParams
// = 12 and ConsensusParamsUpdate = 17. Each is its message's last field, so
// assertSnapshotBaselineDescriptors strips it before every older exemption.
// Every existing baseline predates it, so the exemption applies
// unconditionally.
var tableAdmissionRulesVersionFields = map[string]*descriptorpb.FieldDescriptorProto{
	"ConsensusMutableParams": signedClaimsField("table_admission_rules_version", "tableAdmissionRulesVersion", 12, descriptorpb.FieldDescriptorProto_TYPE_UINT32),
	"ConsensusParamsUpdate":  signedClaimsField("table_admission_rules_version", "tableAdmissionRulesVersion", 17, descriptorpb.FieldDescriptorProto_TYPE_UINT32),
}

// TestTableAdmissionRulesVersionFieldNumbers pins the version's name, number
// and kind on both consensus messages and on the registry read. arbiter-core's
// strict Raft decoder refuses a field its descriptor does not know, so a voter
// that disagrees on the number forks on the first update that sets it. The
// fields are plain proto3, outside any oneof, so they are off the wire while
// zero and every earlier update and snapshot encodes byte for byte as before.
func TestTableAdmissionRulesVersionFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg    proto.Message
		name   protoreflect.Name
		number protoreflect.FieldNumber
	}{
		{&pb.ConsensusMutableParams{}, "table_admission_rules_version", 12},
		{&pb.ConsensusParamsUpdate{}, "table_admission_rules_version", 17},
		{&pb.TableRegistrySnapshot{}, "admission_rules_version", 15},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			f := d.Fields().ByName(tc.name)
			if f == nil || f.Number() != tc.number || f.Kind() != protoreflect.Uint32Kind || f.IsList() || f.HasOptionalKeyword() || f.ContainingOneof() != nil {
				t.Fatalf("field = %v, want plain uint32 number %d", f, tc.number)
			}
			// The three anchor finality quorum fields, authority_threshold,
			// membership_lease_l2_blocks, promotion_ack_delta_snapshot,
			// snapshot_query_tracks and custody_frontier are the only fields
			// appended after it on the consensus messages.
			want := int(tc.number)
			if d.Name() != "TableRegistrySnapshot" {
				want += 8
			}
			if got := d.Fields().Len(); got != want {
				t.Fatalf("field count = %d, want %d (only the anchor finality quorum fields, authority_threshold, membership_lease_l2_blocks, promotion_ack_delta_snapshot, snapshot_query_tracks and custody_frontier follow the version)", got, want)
			}
		})
	}
	// Zero stays off the wire: an update and a registry read without the
	// version encode exactly as ones built before the fields existed.
	for _, pair := range [][2]proto.Message{
		{&pb.ConsensusParamsUpdate{NetworkId: "n"}, &pb.ConsensusParamsUpdate{NetworkId: "n", TableAdmissionRulesVersion: 0}},
		{&pb.TableRegistrySnapshot{Version: 3}, &pb.TableRegistrySnapshot{Version: 3, AdmissionRulesVersion: 0}},
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
			t.Fatalf("%T: a zero table admission rules version must stay off the wire", pair[0])
		}
	}
}
