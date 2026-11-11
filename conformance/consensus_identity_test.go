package conformance

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// consensusIdentityFields pins the immutable schema and executor identity
// appended as the last two fields of ConsensusParamsState (the
// GetConsensusParams read). Neither field is Raft-reachable: the state is a
// read-only view, so the strict Raft decoder never sees it.
var consensusIdentityFields = map[string][]*descriptorpb.FieldDescriptorProto{
	"ConsensusParamsState": {
		signedClaimsField("schema_snapshot_id", "schemaSnapshotId", 10, descriptorpb.FieldDescriptorProto_TYPE_STRING),
		signedClaimsField("executor_profile_id", "executorProfileId", 11, descriptorpb.FieldDescriptorProto_TYPE_STRING),
	},
}

// stripConsensusIdentityFields removes the trailing identity fields after
// asserting they are exactly the pinned ones, so the older baseline
// comparisons see the message as it was before. They are the newest addition,
// so every other strip runs after this one.
func stripConsensusIdentityFields(t *testing.T, d *descriptorpb.DescriptorProto) {
	t.Helper()
	want, ok := consensusIdentityFields[d.GetName()]
	if !ok {
		return
	}
	if len(d.Field) < len(want) {
		t.Fatalf("%s lacks its identity fields: %v", d.GetName(), d)
	}
	tail := d.Field[len(d.Field)-len(want):]
	for i := range want {
		if !proto.Equal(tail[i], want[i]) {
			t.Fatalf("%s identity addition must be exactly %s = %d: %v", d.GetName(), want[i].GetName(), want[i].GetNumber(), d)
		}
	}
	d.Field = d.Field[:len(d.Field)-len(want)]
}

// TestConsensusIdentityFieldNumbers pins the names, numbers and kinds, and
// that both fields are plain proto3 strings outside any oneof: off the wire
// when empty, so a read from a server that predates them decodes unchanged.
func TestConsensusIdentityFieldNumbers(t *testing.T) {
	d := (&pb.ConsensusParamsState{}).ProtoReflect().Descriptor()
	for _, tc := range []struct {
		name   protoreflect.Name
		number protoreflect.FieldNumber
	}{
		{"schema_snapshot_id", 10},
		{"executor_profile_id", 11},
	} {
		f := d.Fields().ByName(tc.name)
		if f == nil || f.Number() != tc.number || f.Kind() != protoreflect.StringKind || f.IsList() || f.HasOptionalKeyword() || f.ContainingOneof() != nil {
			t.Fatalf("%s = %v, want plain string field %d", tc.name, f, tc.number)
		}
	}
	empty, err := proto.Marshal(&pb.ConsensusParamsState{NetworkId: "n"})
	if err != nil {
		t.Fatal(err)
	}
	set, err := proto.Marshal(&pb.ConsensusParamsState{NetworkId: "n", SchemaSnapshotId: "", ExecutorProfileId: ""})
	if err != nil || string(empty) != string(set) {
		t.Fatalf("empty identity fields must stay off the wire: %x vs %x err=%v", empty, set, err)
	}
}
