package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestArtifactDispositionRaftTag28(t *testing.T) {
	frozen := map[protoreflect.Name]protoreflect.FieldNumber{
		"submit_statement": 1, "seal_l3_block": 2, "mark_replaying": 3, "register_rc": 4,
		"record_attestation": 5, "record_byte_side_scan": 6, "record_anchor_finality": 7,
		"record_promotion_issued": 8, "record_promotion_ack": 9, "publish_safe_snapshot": 10,
		"schedule_unsafe_cleanup": 11, "record_cleanup_ack": 12, "open_challenge": 13,
		"resolve_challenge": 14, "register_node": 15, "mark_active": 16, "evict_node": 17,
		"update_consensus_params": 18, "grant_snapshot_query": 19, "release_snapshot_query": 20,
		"submit_snapshot_query": 21, "abort_snapshot_query": 22, "activate_query_profile": 23,
		"record_snapshot_query_claim": 24, "record_snapshot_query_attestation": 25,
		"publish_executor_profile_transition": 26, "record_snapshot_artifact_ready": 27,
		"begin_snapshot_query": 30,
	}
	fields := (&pb.RaftCommand{}).ProtoReflect().Descriptor().Fields()
	for name, number := range frozen {
		fd := fields.ByName(name)
		if fd == nil || fd.Number() != number {
			t.Fatalf("frozen Raft tag %s moved", name)
		}
	}
	assertSnapshotVariant(t, &pb.RaftCommand{}, "cmd", "artifact_disposition", 28, "ArtifactDispositionCmd")
	if fields.ByNumber(29) != nil {
		t.Fatal("tag 29 was allocated")
	}
}

func TestArtifactDispositionControlService(t *testing.T) {
	svc := pb.File_artifact_disposition_proto.Services().ByName("ArtifactDispositionControl")
	if svc == nil {
		t.Fatal("ArtifactDispositionControl missing")
	}
	apply := svc.Methods().ByName("ApplyArtifactDisposition")
	get := svc.Methods().ByName("GetArtifactDisposition")
	if apply == nil || string(apply.Input().Name()) != "ApplyArtifactDispositionRequest" || string(apply.Output().Name()) != "ArtifactDispositionReply" {
		t.Fatalf("ApplyArtifactDisposition signature changed: %v", apply)
	}
	if get == nil || string(get.Input().Name()) != "GetArtifactDispositionRequest" || string(get.Output().Name()) != "ArtifactDispositionReply" {
		t.Fatalf("GetArtifactDisposition signature changed: %v", get)
	}
	val := (&pb.ArtifactDispositionValidationV1{}).ProtoReflect().Descriptor().Fields().ByName("capacity_allowance_ordinal")
	if val == nil || val.Number() != 8 {
		t.Fatal("capacity_allowance_ordinal tag 8 moved")
	}
	raw, err := proto.Marshal(&pb.RaftCommand{Cmd: &pb.RaftCommand_ArtifactDisposition{ArtifactDisposition: &pb.ArtifactDispositionCmd{}}})
	if err != nil {
		t.Fatal(err)
	}
	var out pb.RaftCommand
	if err := proto.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if out.GetArtifactDisposition() == nil {
		t.Fatal("tag 28 did not round-trip")
	}
}

func TestArtifactDispositionGrantReservationActionContract(t *testing.T) {
	action := (&pb.ArtifactDispositionActionV1{}).ProtoReflect().Descriptor()
	for name, number := range map[protoreflect.Name]protoreflect.FieldNumber{
		"bind_policy": 1, "register_candidate": 2, "record_ready": 3,
		"publish_candidate": 4, "cancel_candidate": 5, "begin_retirement": 6,
		"finish_retirement": 7, "admit_use": 8, "close_use": 9,
		"open_challenge": 10, "resolve_obligation": 11,
	} {
		field := action.Fields().ByName(name)
		if field == nil || field.Number() != number {
			t.Fatalf("existing disposition action %s moved", name)
		}
	}

	grant := (&pb.ArtifactDispositionGrantReservationV1{}).ProtoReflect().Descriptor()
	fields := grant.Fields()
	for _, want := range []struct {
		name   protoreflect.Name
		number protoreflect.FieldNumber
	}{
		{name: "client_account", number: 1},
		{name: "statement_id", number: 2},
		{name: "control_binding_digest", number: 3},
	} {
		field := fields.ByName(want.name)
		if field == nil || field.Number() != want.number || field.Kind() != protoreflect.StringKind {
			t.Fatalf("grant reservation field %s does not preserve its wire contract", want.name)
		}
	}
	if fields.Len() != 3 {
		t.Fatalf("grant reservation has %d caller fields, want 3", fields.Len())
	}
	for _, forbidden := range []protoreflect.Name{
		"reservation_id", "fencing_generation", "pin", "executor_profile_id", "query_profile_id",
		"activation_id", "block_seq", "assignment", "obligation_seq", "capacity_allowance_ordinal",
		"allocator_ordinal", "barrier_state",
	} {
		if fields.ByName(forbidden) != nil {
			t.Fatalf("server-owned grant decision %s must not be caller supplied", forbidden)
		}
	}

	variant := action.Fields().ByName("grant_reservation")
	if variant == nil || variant.Number() != 12 || variant.Kind() != protoreflect.MessageKind || string(variant.Message().Name()) != "ArtifactDispositionGrantReservationV1" {
		t.Fatalf("grant reservation action descriptor = %v", variant)
	}

	in := &pb.ArtifactDispositionActionV1{Action: &pb.ArtifactDispositionActionV1_GrantReservation{
		GrantReservation: &pb.ArtifactDispositionGrantReservationV1{
			ClientAccount: "0xabc", StatementId: "statement", ControlBindingDigest: "0xdef",
		},
	}}
	raw, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out pb.ArtifactDispositionActionV1
	if err := proto.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(in, &out) || out.GetGrantReservation() == nil {
		t.Fatalf("grant reservation action did not round-trip: %v", out.Action)
	}
}
