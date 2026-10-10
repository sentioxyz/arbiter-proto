package conformance

import (
	"bytes"
	"context"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestSignedClaimsFieldNumbers pins the stage-1 multi-source SI additions
// (housegate spec 2026-10-10 §6; Plan S1-A contract §1). Each number is a
// consensus constant: arbiter-core's strict Raft command decoder refuses any
// field its descriptor does not know, so a voter that disagrees on one of
// these numbers forks on the first command that carries it.
func TestSignedClaimsFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg      proto.Message
		name     protoreflect.Name
		number   protoreflect.FieldNumber
		kind     protoreflect.Kind
		repeated bool
		optional bool // proto3 `optional`: an explicit 0 stays distinct from absent
		msgType  protoreflect.FullName
	}{
		{&pb.SIIndexerEntry{}, "indexer_id", 1, protoreflect.Uint64Kind, false, false, ""},
		{&pb.SIIndexerEntry{}, "activation_block", 2, protoreflect.Uint64Kind, false, false, ""},
		{&pb.SIIndexerEntry{}, "signer", 3, protoreflect.StringKind, false, false, ""},
		{&pb.SIIndexerEntry{}, "snode_node_id", 4, protoreflect.StringKind, false, false, ""},
		{&pb.SIIndexerEntry{}, "enrollment_jws", 5, protoreflect.StringKind, false, false, ""},
		{&pb.VerifierEntry{}, "node_id", 1, protoreflect.StringKind, false, false, ""},
		{&pb.VerifierEntry{}, "ed25519_pubkey", 2, protoreflect.BytesKind, false, false, ""},
		{&pb.ConsensusParamsUpdate{}, "si_indexers", 11, protoreflect.MessageKind, true, false, "arbiter.SIIndexerEntry"},
		{&pb.ConsensusParamsUpdate{}, "verifiers", 12, protoreflect.MessageKind, true, false, "arbiter.VerifierEntry"},
		{&pb.ConsensusMutableParams{}, "si_indexers", 6, protoreflect.MessageKind, true, false, "arbiter.SIIndexerEntry"},
		{&pb.ConsensusMutableParams{}, "verifiers", 7, protoreflect.MessageKind, true, false, "arbiter.VerifierEntry"},
		{&pb.EvictNodeRequest{}, "node_id", 1, protoreflect.StringKind, false, false, ""},
		{&pb.EvictNodeRequest{}, "expected_registration_seq", 2, protoreflect.Uint64Kind, false, false, ""},
		{&pb.EvictNodeRequest{}, "reason", 3, protoreflect.StringKind, false, false, ""},
		{&pb.EvictNodeRequest{}, "authority_jws", 4, protoreflect.StringKind, false, false, ""},
		{&pb.TableRegistrySnapshot{}, "si_indexers", 7, protoreflect.MessageKind, true, false, "arbiter.SIIndexerEntry"},
		{&pb.TableRegistrySnapshot{}, "seeded_indexers", 8, protoreflect.Uint64Kind, true, false, ""},
		{&pb.TableIncarnation{}, "owner_indexer_id", 18, protoreflect.Uint64Kind, false, true, ""},
		{&pb.AddTableCmd{}, "owner_indexer_id", 8, protoreflect.Uint64Kind, false, true, ""},
		{&pb.SeedLegacyTablesCmd{}, "indexer_id", 3, protoreflect.Uint64Kind, false, true, ""},
		{&pb.RecordTablePurgedCmd{}, "signer_jws", 3, protoreflect.StringKind, false, false, ""},
		{&pb.RecordTablePurgedCmd{}, "ed25519_signature", 4, protoreflect.StringKind, false, false, ""},
		{&pb.NodeRegistration{}, "registration_seq", 6, protoreflect.Uint64Kind, false, false, ""},
		{&pb.NodeRegistration{}, "signer_jws", 7, protoreflect.StringKind, false, false, ""},
		{&pb.NodeRegistration{}, "ed25519_signature", 8, protoreflect.StringKind, false, false, ""},
		{&pb.NodeRef{}, "registration_seq", 2, protoreflect.Uint64Kind, false, false, ""},
		{&pb.NodeRef{}, "signer_jws", 3, protoreflect.StringKind, false, false, ""},
		{&pb.NodeRef{}, "ed25519_signature", 4, protoreflect.StringKind, false, false, ""},
		{&pb.RCRecord{}, "source_jws", 6, protoreflect.StringKind, false, false, ""},
		{&pb.PromotionAck{}, "source_jws", 10, protoreflect.StringKind, false, false, ""},
		{&pb.CleanupAck{}, "source_jws", 5, protoreflect.StringKind, false, false, ""},
		{&pb.RegisterRCCmd{}, "source_jws", 2, protoreflect.StringKind, false, false, ""},
		{&pb.RecordPromotionAckCmd{}, "source_jws", 2, protoreflect.StringKind, false, false, ""},
		{&pb.RecordCleanupAckCmd{}, "source_jws", 2, protoreflect.StringKind, false, false, ""},
		{&pb.RegisterNodeCmd{}, "signer_jws", 2, protoreflect.StringKind, false, false, ""},
		{&pb.RegisterNodeCmd{}, "ed25519_signature", 3, protoreflect.StringKind, false, false, ""},
		{&pb.MarkActiveCmd{}, "registration_seq", 2, protoreflect.Uint64Kind, false, false, ""},
		{&pb.MarkActiveCmd{}, "signer_jws", 3, protoreflect.StringKind, false, false, ""},
		{&pb.MarkActiveCmd{}, "ed25519_signature", 4, protoreflect.StringKind, false, false, ""},
		{&pb.EvictNodeCmd{}, "expected_registration_seq", 3, protoreflect.Uint64Kind, false, false, ""},
		{&pb.EvictNodeCmd{}, "authority_jws", 4, protoreflect.StringKind, false, false, ""},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name())+"."+string(tc.name), func(t *testing.T) {
			f := d.Fields().ByName(tc.name)
			if f == nil || f.Number() != tc.number || f.Kind() != tc.kind || f.IsList() != tc.repeated || f.HasOptionalKeyword() != tc.optional {
				t.Fatalf("field = %v, want number %d kind %s repeated %v optional %v", f, tc.number, tc.kind, tc.repeated, tc.optional)
			}
			if tc.msgType != "" && f.Message().FullName() != tc.msgType {
				t.Fatalf("message type = %s, want %s", f.Message().FullName(), tc.msgType)
			}
		})
	}
	for msg, want := range map[proto.Message]int{
		&pb.SIIndexerEntry{}: 5, &pb.VerifierEntry{}: 2, &pb.EvictNodeRequest{}: 4,
		&pb.ConsensusParamsUpdate{}: 12, &pb.ConsensusMutableParams{}: 7,
		&pb.TableRegistrySnapshot{}: 8, &pb.TableIncarnation{}: 18, &pb.AddTableCmd{}: 8,
		&pb.SeedLegacyTablesCmd{}: 3, &pb.RecordTablePurgedCmd{}: 4,
		&pb.NodeRegistration{}: 8, &pb.NodeRef{}: 4, &pb.RCRecord{}: 6, &pb.PromotionAck{}: 10, &pb.CleanupAck{}: 5,
		&pb.RegisterRCCmd{}: 2, &pb.RecordPromotionAckCmd{}: 2, &pb.RecordCleanupAckCmd{}: 2,
		&pb.RegisterNodeCmd{}: 3, &pb.MarkActiveCmd{}: 4, &pb.EvictNodeCmd{}: 4,
	} {
		if got := msg.ProtoReflect().Descriptor().Fields().Len(); got != want {
			t.Fatalf("%s field count = %d, want %d", msg.ProtoReflect().Descriptor().Name(), got, want)
		}
	}
	// SIIndexerEntry lives in table_registry.proto: consensus.proto imports
	// that file, so defining the entry in consensus.proto would make
	// TableRegistrySnapshot.si_indexers an import cycle.
	for _, tc := range []struct {
		file protoreflect.FileDescriptor
		name protoreflect.Name
	}{
		{pb.File_table_registry_proto, "SIIndexerEntry"},
		{pb.File_consensus_proto, "VerifierEntry"},
		{pb.File_consensus_proto, "EvictNodeRequest"},
	} {
		if tc.file.Messages().ByName(tc.name) == nil {
			t.Fatalf("%s is not defined in %s", tc.name, tc.file.Path())
		}
	}
}

func TestSourceUnavailableAdmissionCodeIsTen(t *testing.T) {
	if got := int32(pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE); got != 10 {
		t.Fatalf("ADMISSION_CODE_SOURCE_UNAVAILABLE = %d, want 10", got)
	}
	if got := int32(pb.AdmissionCode_ADMISSION_CODE_LANE_BUDGET_EXCEEDED); got != 9 {
		t.Fatalf("ADMISSION_CODE_LANE_BUDGET_EXCEEDED moved to %d", got)
	}
	if n := pb.AdmissionCode(0).Descriptor().Values().Len(); n != 11 {
		t.Fatalf("AdmissionCode has %d values, want 11", n)
	}
}

// TestSignedClaimsRPCSignatures pins the authority-signed eviction RPC and the
// request messages that carry the new signature fields. The data-plane RPCs
// keep their request types: the signatures ride inside NodeRegistration,
// NodeRef, RCRecord, PromotionAck and CleanupAck, and SubmitTablePurged keeps
// taking the replicated RecordTablePurgedCmd, whose signer_jws and
// ed25519_signature are therefore the RPC's fields too.
func TestSignedClaimsRPCSignatures(t *testing.T) {
	for _, tc := range []struct {
		file          protoreflect.FileDescriptor
		service       protoreflect.Name
		method        protoreflect.Name
		input, output protoreflect.FullName
	}{
		{pb.File_consensus_proto, "ConsensusAdmin", "EvictNode", "arbiter.EvictNodeRequest", "arbiter.Ack"},
		{pb.File_arbiter_proto, "Membership", "RegisterNode", "arbiter.NodeRegistration", "arbiter.Ack"},
		{pb.File_arbiter_proto, "Membership", "MarkActive", "arbiter.NodeRef", "arbiter.Ack"},
		{pb.File_arbiter_proto, "SourceClaims", "RegisterResultClaim", "arbiter.RCRecord", "arbiter.Ack"},
		{pb.File_arbiter_proto, "PromotionGateway", "AckPromotion", "arbiter.PromotionAck", "arbiter.Ack"},
		{pb.File_arbiter_proto, "PromotionGateway", "AckCleanup", "arbiter.CleanupAck", "arbiter.Ack"},
		{pb.File_arbiter_proto, "PromotionGateway", "SubmitTablePurged", "arbiter.RecordTablePurgedCmd", "arbiter.Ack"},
	} {
		svc := tc.file.Services().ByName(tc.service)
		if svc == nil {
			t.Fatalf("service %s missing", tc.service)
		}
		m := svc.Methods().ByName(tc.method)
		if m == nil || m.Input().FullName() != tc.input || m.Output().FullName() != tc.output || m.IsStreamingClient() || m.IsStreamingServer() {
			t.Fatalf("%s.%s = %v, want unary %s -> %s", tc.service, tc.method, m, tc.input, tc.output)
		}
	}
	// Servers that embed the generated stub answer Unimplemented until the
	// arbiter implements the authority check.
	if _, err := (pb.UnimplementedConsensusAdminServer{}).EvictNode(context.Background(), &pb.EvictNodeRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("UnimplementedConsensusAdminServer.EvictNode = %v, want Unimplemented", err)
	}
}

// TestOptionalIndexerIDsKeepZeroDistinctFromAbsent pins why the three single
// indexer-id fields are proto3 optional: indexer 0 is a valid owner (devnet2's
// founding indexer), so an explicit 0 must survive the wire and differ from
// absent, and an absent id must leave the encoding exactly as a pre-activation
// command encodes it.
func TestOptionalIndexerIDsKeepZeroDistinctFromAbsent(t *testing.T) {
	for _, tc := range []struct {
		name         string
		absent, zero proto.Message
		number       protowire.Number
	}{
		{"AddTableCmd.owner_indexer_id",
			&pb.AddTableCmd{DatabaseId: "db", TableId: "t", SchemaVersion: 1},
			&pb.AddTableCmd{DatabaseId: "db", TableId: "t", SchemaVersion: 1, OwnerIndexerId: proto.Uint64(0)}, 8},
		{"SeedLegacyTablesCmd.indexer_id",
			&pb.SeedLegacyTablesCmd{AtBlock: &pb.L2BlockRef{Number: 5508930}},
			&pb.SeedLegacyTablesCmd{AtBlock: &pb.L2BlockRef{Number: 5508930}, IndexerId: proto.Uint64(0)}, 3},
		{"TableIncarnation.owner_indexer_id",
			&pb.TableIncarnation{Seq: 1, DatabaseId: "db", TableId: "t"},
			&pb.TableIncarnation{Seq: 1, DatabaseId: "db", TableId: "t", OwnerIndexerId: proto.Uint64(0)}, 18},
	} {
		t.Run(tc.name, func(t *testing.T) {
			absent, err := proto.Marshal(tc.absent)
			if err != nil {
				t.Fatal(err)
			}
			zero, err := proto.Marshal(tc.zero)
			if err != nil {
				t.Fatal(err)
			}
			// The new field has the message's highest number, so an explicit 0
			// is the absent encoding plus one tag and a zero varint.
			want := protowire.AppendVarint(protowire.AppendTag(bytes.Clone(absent), tc.number, protowire.VarintType), 0)
			if !bytes.Equal(zero, want) {
				t.Fatalf("explicit 0 encodes as %x, want %x", zero, want)
			}
			fd := tc.zero.ProtoReflect().Descriptor().Fields().ByNumber(tc.number)
			for _, c := range []struct {
				raw     []byte
				present bool
			}{{zero, true}, {absent, false}} {
				back := tc.zero.ProtoReflect().Type().New()
				if err := proto.Unmarshal(c.raw, back.Interface()); err != nil {
					t.Fatal(err)
				}
				if back.Has(fd) != c.present {
					t.Fatalf("presence after decoding %x = %v, want %v", c.raw, back.Has(fd), c.present)
				}
			}
		})
	}
}
