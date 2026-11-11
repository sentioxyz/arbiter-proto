package conformance

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// sharedPartHashesField pins ConsensusParamsState.shared_unpromoted_part_hashes
// = 12, appended after the identity fields: the count arbiter-admin reads
// before it sets part_claim_uniqueness (sentioxyz/arbiter#162). It is a proto3
// optional uint64 so a reader can tell a server that predates it (absent) from
// one that reports 0. The state is a read-only view, not Raft-reachable.
func sharedPartHashesField() *descriptorpb.FieldDescriptorProto {
	f := signedClaimsField("shared_unpromoted_part_hashes", "sharedUnpromotedPartHashes", 12, descriptorpb.FieldDescriptorProto_TYPE_UINT64)
	f.OneofIndex = proto.Int32(0)
	f.Proto3Optional = proto.Bool(true)
	return f
}

// stripSharedPartHashesField removes the trailing shared-hash count and its
// synthetic oneof after asserting they are exactly the pinned ones, so the
// older baseline comparisons see the message as it was before. It is the
// newest addition, so every other strip runs after this one.
func stripSharedPartHashesField(t *testing.T, d *descriptorpb.DescriptorProto) {
	t.Helper()
	if d.GetName() != "ConsensusParamsState" {
		return
	}
	if len(d.Field) == 0 || !proto.Equal(d.Field[len(d.Field)-1], sharedPartHashesField()) ||
		len(d.OneofDecl) != 1 || d.OneofDecl[0].GetName() != "_shared_unpromoted_part_hashes" {
		t.Fatalf("ConsensusParamsState addition must be exactly optional shared_unpromoted_part_hashes = 12: %v", d)
	}
	d.Field = d.Field[:len(d.Field)-1]
	d.OneofDecl = nil
}

// TestSharedPartHashesFieldNumber pins the name, number and kind, and that the
// field has explicit presence: absent stays off the wire, and a reported 0 is
// on it, so arbiter-admin can refuse a leader that does not report the count.
func TestSharedPartHashesFieldNumber(t *testing.T) {
	f := (&pb.ConsensusParamsState{}).ProtoReflect().Descriptor().Fields().ByName("shared_unpromoted_part_hashes")
	if f == nil || f.Number() != 12 || f.Kind() != protoreflect.Uint64Kind || f.IsList() || !f.HasOptionalKeyword() || !f.HasPresence() {
		t.Fatalf("shared_unpromoted_part_hashes = %v, want proto3 optional uint64 field 12", f)
	}
	absent, err := proto.Marshal(&pb.ConsensusParamsState{NetworkId: "n"})
	if err != nil {
		t.Fatal(err)
	}
	zero, err := proto.Marshal(&pb.ConsensusParamsState{NetworkId: "n", SharedUnpromotedPartHashes: proto.Uint64(0)})
	if err != nil || string(absent) == string(zero) {
		t.Fatalf("a reported 0 must be on the wire: %x vs %x err=%v", absent, zero, err)
	}
	var got pb.ConsensusParamsState
	if err := proto.Unmarshal(absent, &got); err != nil || got.SharedUnpromotedPartHashes != nil {
		t.Fatalf("absent count decoded as %v, err=%v", got.SharedUnpromotedPartHashes, err)
	}
}
