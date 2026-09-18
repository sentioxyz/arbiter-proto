package conformance

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestSnapshotQueryDescriptorFixtureIdentities(t *testing.T) {
	// These independently exported baselines must not be refreshed from the candidate.
	for name, hash := range map[string]string{
		"pre_snapshot_query_descriptor.binpb": "bd70cd89d7cb8c306ed32688735614e4358cf005438d685b40d0700bc547a313",
		"main_f7d9f070_descriptor.binpb":      "f03a09fe43042bef4116ccb5c2bb362707cdbd9bf4ab1fe8b7cb67039d6717e9",
	} {
		raw, err := os.ReadFile(filepath.Join(arbiterProtoModuleRoot(t), "conformance/testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(raw)); got != hash {
			t.Fatalf("%s hash = %s, want %s", name, got, hash)
		}
	}
}

func TestSnapshotQueryBegin30IsDistinctFromConsensus18(t *testing.T) {
	// Empty payloads make the transport-key distinction explicit; they are not
	// evidence of successful runtime admission or historical-byte provenance.
	for _, tc := range []struct {
		name    string
		command *pb.RaftCommand
		raw     []byte
		variant protoreflect.Name
	}{
		{"begin", &pb.RaftCommand{Cmd: &pb.RaftCommand_BeginSnapshotQuery{BeginSnapshotQuery: &pb.BeginSnapshotQueryCmd{}}}, []byte{0xf2, 0x01, 0x00}, "begin_snapshot_query"},
		{"consensus", &pb.RaftCommand{Cmd: &pb.RaftCommand_UpdateConsensusParams{UpdateConsensusParams: &pb.UpdateConsensusParamsCmd{}}}, []byte{0x92, 0x01, 0x00}, "update_consensus_params"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := proto.Marshal(tc.command)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(raw, tc.raw) {
				t.Fatalf("wire bytes = %x, want %x", raw, tc.raw)
			}
			out := &pb.RaftCommand{}
			if err := proto.Unmarshal(tc.raw, out); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(tc.command, out) {
				t.Fatalf("wrong variant: %v", out)
			}
			if got := out.ProtoReflect().WhichOneof(out.ProtoReflect().Descriptor().Oneofs().ByName("cmd")); got == nil || got.Name() != tc.variant {
				t.Fatalf("selected %v", got)
			}
		})
	}
	// Main's consensus payload remains readable by the independent main reader,
	// including repeated addresses, maximum counters, and the authority signature.
	in := &pb.RaftCommand{Cmd: &pb.RaftCommand_UpdateConsensusParams{UpdateConsensusParams: &pb.UpdateConsensusParamsCmd{
		Update: &pb.ConsensusParamsUpdate{NetworkId: "network", GenesisSnapshotId: "genesis", ExpectedEpoch: ^uint64(0), PreviousParamsDigest: "digest", AuthorityAddresses: []string{"address-a", "address-b"}, MaxWriters: ^uint64(0), ExpectedPromotionSeq: ^uint64(0)}, AuthorityJws: "authority-jws",
	}}}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	files, err := protodesc.NewFiles(snapshotDescriptorFixture(t, "main_f7d9f070_descriptor.binpb"))
	if err != nil {
		t.Fatal(err)
	}
	desc, err := files.FindDescriptorByName("arbiter.RaftCommand")
	if err != nil {
		t.Fatal(err)
	}
	old := dynamicpb.NewMessage(desc.(protoreflect.MessageDescriptor))
	if err := proto.Unmarshal(raw, old); err != nil {
		t.Fatal(err)
	}
	if fd := old.WhichOneof(old.Descriptor().Oneofs().ByName("cmd")); fd == nil || fd.Number() != 18 {
		t.Fatalf("main reader selected %v", fd)
	}
	round, err := (proto.MarshalOptions{Deterministic: true}).Marshal(old)
	if err != nil || !bytes.Equal(raw, round) {
		t.Fatalf("main consensus payload changed: %v", err)
	}
}
