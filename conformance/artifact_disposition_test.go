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
