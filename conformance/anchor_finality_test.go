package conformance

import (
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// anchorFinalityAppendedFields pins the trailing fields the anchor finality
// quorum and the part-claim uniqueness gate append to messages that exist in
// the baselines: part_claim_uniqueness, voters and anchor_finality on both
// consensus parameter messages, voter_ed25519_pubkey on ProtocolInfo and
// finality_attestations on BlockVerification. They are their messages' last
// fields, so assertSnapshotBaselineDescriptors strips them first, before every
// older exemption. Every existing baseline predates them, so the exemption
// applies unconditionally.
var anchorFinalityAppendedFields = map[string][]*descriptorpb.FieldDescriptorProto{
	"ConsensusMutableParams": {
		signedClaimsField("part_claim_uniqueness", "partClaimUniqueness", 13, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
		signedClaimsList("voters", "voters", 14, ".arbiter.VoterEntry"),
		anchorFinalityMessageField("anchor_finality", "anchorFinality", 15, ".arbiter.AnchorFinalityParams"),
	},
	"ConsensusParamsUpdate": {
		signedClaimsField("part_claim_uniqueness", "partClaimUniqueness", 18, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
		signedClaimsList("voters", "voters", 19, ".arbiter.VoterEntry"),
		anchorFinalityMessageField("anchor_finality", "anchorFinality", 20, ".arbiter.AnchorFinalityParams"),
	},
	"ProtocolInfo": {
		signedClaimsField("voter_ed25519_pubkey", "voterEd25519Pubkey", 5, descriptorpb.FieldDescriptorProto_TYPE_BYTES),
	},
	"BlockVerification": {
		signedClaimsList("finality_attestations", "finalityAttestations", 23, ".arbiter.AnchorFinalityAttestation"),
	},
}

func anchorFinalityMessageField(name, jsonName string, number int32, typeName string) *descriptorpb.FieldDescriptorProto {
	f := signedClaimsField(name, jsonName, number, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE)
	f.TypeName = proto.String(typeName)
	return f
}

// stripAnchorFinalityFields removes the anchorFinalityAppendedFields tail of
// got, failing unless it is exactly that tail.
func stripAnchorFinalityFields(t *testing.T, got *descriptorpb.DescriptorProto) {
	t.Helper()
	want, ok := anchorFinalityAppendedFields[got.GetName()]
	if !ok {
		return
	}
	if len(got.Field) < len(want) {
		t.Fatalf("%s lacks its anchor finality fields: %v", got.GetName(), got)
	}
	tail := got.Field[len(got.Field)-len(want):]
	for i := range want {
		if !proto.Equal(tail[i], want[i]) {
			t.Fatalf("%s anchor finality addition must be exactly %s = %d: %v", got.GetName(), want[i].GetName(), want[i].GetNumber(), got)
		}
	}
	got.Field = got.Field[:len(got.Field)-len(want)]
}

// anchorFinalityMessageFields is the field ledger of every message the anchor
// finality quorum adds, in the style of stage3Fields: the i-th token is field
// number i+1, "[]T" is repeated, T is the scalar kind or the message name.
// Every number is a consensus constant: arbiter-core's strict Raft decoder
// refuses a field its descriptor does not know.
var anchorFinalityMessageFields = map[string]string{
	"VoterEntry":                         "voter_id:string ed25519_pubkey:bytes consent_sig:string",
	"AnchorFinalityParams":               "quorum:uint32 finality_mode:string finality_confirmations:uint64 last_mergeable_confirmations:uint64 l2_chain_id:uint64 registry_contract:string",
	"AnchorFinalityAttestation":          "voter_id:string l3_block_seq:uint64 l3_block_hash:string state_root:string l2_block_number:uint64 final:bool last_mergeable:bool signature:string",
	"RecordAnchorFinalityAttestationCmd": "attestation:AnchorFinalityAttestation",
}

func anchorFinalityMessageFiles() map[string]protoreflect.FileDescriptor {
	return map[string]protoreflect.FileDescriptor{
		"VoterEntry":                         pb.File_consensus_proto,
		"AnchorFinalityParams":               pb.File_consensus_proto,
		"AnchorFinalityAttestation":          pb.File_arbiter_proto,
		"RecordAnchorFinalityAttestationCmd": pb.File_raftlog_proto,
	}
}

// TestAnchorFinalityFieldLedgers pins every new message field by field and
// runs the every-field wire round trip of TestStage3FieldLedgers on it.
func TestAnchorFinalityFieldLedgers(t *testing.T) {
	assertMessageFieldLedgers(t, anchorFinalityMessageFiles(), anchorFinalityMessageFields)
}

// assertMessageFieldLedgers pins every field of each ledger message (the i-th
// token is field number i+1) and runs an every-field wire round trip.
func assertMessageFieldLedgers(t *testing.T, files map[string]protoreflect.FileDescriptor, ledger map[string]string) {
	t.Helper()
	if len(files) != len(ledger) {
		t.Fatalf("%d messages have a home file, %d have a ledger", len(files), len(ledger))
	}
	for name, spec := range ledger {
		t.Run(name, func(t *testing.T) {
			md := files[name].Messages().ByName(protoreflect.Name(name))
			if md == nil {
				t.Fatalf("%s is not defined in %s", name, files[name].Path())
			}
			mt, err := protoregistry.GlobalTypes.FindMessageByName(md.FullName())
			if err != nil {
				t.Fatal(err)
			}
			in := mt.New()
			fields := md.Fields()
			want := strings.Fields(spec)
			if fields.Len() != len(want) {
				t.Fatalf("field count = %d, want %d", fields.Len(), len(want))
			}
			for i, item := range want {
				fieldName, typ, _ := strings.Cut(item, ":")
				fd := fields.ByName(protoreflect.Name(fieldName))
				if fd == nil {
					t.Fatalf("missing field %s", fieldName)
				}
				repeated := strings.HasPrefix(typ, "[]")
				typ = strings.TrimPrefix(typ, "[]")
				if fd.Number() != protoreflect.FieldNumber(i+1) || fd.IsList() != repeated || fd.IsMap() || fd.HasOptionalKeyword() || fd.ContainingOneof() != nil {
					t.Fatalf("%s allocation changed: %v", fieldName, fd)
				}
				if got := stage3LedgerType(fd); got != typ {
					t.Fatalf("%s type = %s, want %s", fieldName, got, typ)
				}
				populateSnapshotField(in, fd, uint64(i+1))
				single := mt.New()
				populateSnapshotField(single, fd, uint64(i+1))
				raw, err := proto.Marshal(single.Interface())
				if err != nil {
					t.Fatal(err)
				}
				if number, _, n := protowire.ConsumeTag(raw); n < 0 || number != protowire.Number(i+1) {
					t.Fatalf("%s encoded tag = %d", fieldName, number)
				}
			}
			raw, err := proto.Marshal(in.Interface())
			if err != nil {
				t.Fatal(err)
			}
			out := mt.New()
			if err := proto.Unmarshal(raw, out.Interface()); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(in.Interface(), out.Interface()) {
				t.Fatalf("round trip lost data: %v", out)
			}
		})
	}
}

// TestAnchorFinalityFieldNumbers pins the appended fields on the existing
// messages, the Raft tag and the voter RPC, and that every new field is off
// the wire at its zero value, so every earlier update, params digest, Raft
// command and read encodes byte for byte as before.
func TestAnchorFinalityFieldNumbers(t *testing.T) {
	for _, msg := range []proto.Message{&pb.ConsensusMutableParams{}, &pb.ConsensusParamsUpdate{}, &pb.ProtocolInfo{}, &pb.BlockVerification{}} {
		d := protodescProto(msg)
		stripAuthorityThresholdField(t, d)
		want := anchorFinalityAppendedFields[d.GetName()]
		if len(d.Field) < len(want) {
			t.Fatalf("%s: %d fields", d.GetName(), len(d.Field))
		}
		tail := d.Field[len(d.Field)-len(want):]
		for i := range want {
			if !proto.Equal(tail[i], want[i]) {
				t.Fatalf("%s field %d = %v, want %v (the anchor finality fields are the last ones)", d.GetName(), i, tail[i], want[i])
			}
		}
	}
	raft := (&pb.RaftCommand{}).ProtoReflect().Descriptor()
	if f := raft.Fields().ByNumber(43); f == nil || f.Name() != "record_anchor_finality_attestation" || f.Message().Name() != "RecordAnchorFinalityAttestationCmd" || f.ContainingOneof() == nil || f.ContainingOneof().Name() != "cmd" {
		t.Fatalf("RaftCommand tag 43 = %v", f)
	}
	assertSnapshotVariant(t, &pb.RaftCommand{}, "cmd", "record_anchor_finality_attestation", 43, "RecordAnchorFinalityAttestationCmd")
	svc := pb.File_arbiter_proto.Services().ByName("VoterGateway")
	if svc == nil || svc.Methods().Len() != 1 {
		t.Fatalf("VoterGateway = %v", svc)
	}
	m := svc.Methods().ByName("SubmitAnchorFinalityAttestation")
	if m == nil || m.Input().FullName() != "arbiter.AnchorFinalityAttestation" || m.Output().FullName() != "arbiter.Ack" || m.IsStreamingClient() || m.IsStreamingServer() {
		t.Fatalf("SubmitAnchorFinalityAttestation = %v", m)
	}
	if pb.VoterGateway_SubmitAnchorFinalityAttestation_FullMethodName != "/arbiter.VoterGateway/SubmitAnchorFinalityAttestation" {
		t.Fatal("VoterGateway method name changed")
	}
	var _ pb.VoterGatewayServer = pb.UnimplementedVoterGatewayServer{}
	for _, pair := range [][2]proto.Message{
		{&pb.ConsensusParamsUpdate{NetworkId: "n"}, &pb.ConsensusParamsUpdate{NetworkId: "n", PartClaimUniqueness: false, Voters: []*pb.VoterEntry{}, AnchorFinality: nil}},
		{&pb.ConsensusMutableParams{MaxWriters: 1}, &pb.ConsensusMutableParams{MaxWriters: 1, Voters: []*pb.VoterEntry{}}},
		{&pb.ProtocolInfo{NodeId: "a"}, &pb.ProtocolInfo{NodeId: "a", VoterEd25519Pubkey: []byte{}}},
		{&pb.BlockVerification{ChainHash: "h"}, &pb.BlockVerification{ChainHash: "h", FinalityAttestations: []*pb.AnchorFinalityAttestation{}}},
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
			t.Fatalf("%T: a zero anchor finality field must stay off the wire", pair[0])
		}
	}
	// The attestation is reachable from RaftCommand, the parameters from the
	// consensus update; neither reaches a stage-3-only message.
	reach := reachableTypes(raft)
	for _, name := range []protoreflect.FullName{"arbiter.AnchorFinalityAttestation", "arbiter.VoterEntry", "arbiter.AnchorFinalityParams"} {
		if !reach[name] {
			t.Fatalf("RaftCommand walk does not reach %s", name)
		}
	}
}

func protodescProto(msg proto.Message) *descriptorpb.DescriptorProto {
	return protodesc.ToDescriptorProto(msg.ProtoReflect().Descriptor())
}
