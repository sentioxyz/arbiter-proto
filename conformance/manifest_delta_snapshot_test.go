package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// manifestDeltaSnapshotFields pins the bool the manifest delta snapshot
// switch appends to each consensus parameter message: ConsensusMutableParams =
// 11 and ConsensusParamsUpdate = 16. Each is its message's last field once
// table_admission_rules_version is stripped, so assertSnapshotBaselineDescriptors
// strips it right after that field, before every older exemption,
// promotion_finality_gate's included.
// Every existing baseline predates it, so the exemption applies
// unconditionally.
var manifestDeltaSnapshotFields = map[string]*descriptorpb.FieldDescriptorProto{
	"ConsensusMutableParams": signedClaimsField("manifest_delta_snapshot", "manifestDeltaSnapshot", 11, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
	"ConsensusParamsUpdate":  signedClaimsField("manifest_delta_snapshot", "manifestDeltaSnapshot", 16, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
}

// TestManifestDeltaSnapshotFieldNumbers pins the switch's name, number and
// kind. arbiter-core's strict Raft decoder refuses a field its descriptor does
// not know, so a voter that disagrees on the number forks on the first update
// that sets it. The field is plain proto3, outside any oneof, so it is off the
// wire while false and every earlier update encodes byte for byte as before.
func TestManifestDeltaSnapshotFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg    proto.Message
		number protoreflect.FieldNumber
	}{
		{&pb.ConsensusMutableParams{}, 11},
		{&pb.ConsensusParamsUpdate{}, 16},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			f := d.Fields().ByName("manifest_delta_snapshot")
			if f == nil || f.Number() != tc.number || f.Kind() != protoreflect.BoolKind || f.IsList() || f.HasOptionalKeyword() || f.ContainingOneof() != nil {
				t.Fatalf("field = %v, want plain bool number %d", f, tc.number)
			}
			// table_admission_rules_version, the three anchor finality quorum
			// fields, authority_threshold, membership_lease_l2_blocks,
			// promotion_ack_delta_snapshot, snapshot_query_tracks, custody_frontier
			// and operations_authority_addresses are the only fields appended after
			// it.
			if got := d.Fields().Len(); got != int(tc.number)+15 {
				t.Fatalf("field count = %d, want %d (only table_admission_rules_version, the anchor finality quorum fields, authority_threshold, membership_lease_l2_blocks, promotion_ack_delta_snapshot, snapshot_query_tracks, custody_frontier and operations_authority_addresses and history_retain_blocks and the authority change delay fields and account_budget and pending_voter_change follow the switch)", got, tc.number+15)
			}
		})
	}
	// Unset stays off the wire: an update without the switch encodes exactly as
	// one built before the field existed.
	raw, err := proto.Marshal(&pb.ConsensusParamsUpdate{NetworkId: "n", Stage3: &pb.Stage3Params{}})
	if err != nil {
		t.Fatal(err)
	}
	want, err := proto.Marshal(&pb.ConsensusParamsUpdate{NetworkId: "n", Stage3: &pb.Stage3Params{}, ManifestDeltaSnapshot: false})
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != string(want) {
		t.Fatal("an unset manifest_delta_snapshot must stay off the wire")
	}
}
