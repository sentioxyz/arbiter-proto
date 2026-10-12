package conformance

import (
	"bytes"
	"context"
	"slices"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestTracksFieldNumbers pins the multi-source SI stage-2 additions (housegate
// spec 2026-10-10 §7; Plan S2-A contract §1). Each number is a consensus
// constant: arbiter-core's strict Raft command decoder refuses any field its
// descriptor does not know, at any depth, so a voter that disagrees on one of
// these numbers forks on the first command that carries it. None of them is
// proto3 optional or inside a oneof: an unset scalar must stay off the wire
// before the tracks activation, and a message field already has presence.
func TestTracksFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg      proto.Message
		name     protoreflect.Name
		number   protoreflect.FieldNumber
		kind     protoreflect.Kind
		repeated bool
		msgType  protoreflect.FullName
	}{
		{&pb.TrackFrontier{}, "indexer_id", 1, protoreflect.Uint64Kind, false, ""},
		{&pb.TrackFrontier{}, "safe_block_seq", 2, protoreflect.Uint64Kind, false, ""},
		{&pb.TrackFrontier{}, "applied_promotion_seq", 3, protoreflect.Uint64Kind, false, ""},
		{&pb.SafeSnapshotManifest{}, "track_frontiers", 11, protoreflect.MessageKind, true, "arbiter.TrackFrontier"},
		{&pb.L3BlockTrack{}, "indexer_id", 1, protoreflect.Uint64Kind, false, ""},
		{&pb.L3BlockHeader{}, "track", 14, protoreflect.MessageKind, false, "arbiter.L3BlockTrack"},
		{&pb.UnsafeCleanup{}, "rejected_block_seq", 5, protoreflect.Uint64Kind, false, ""},
		{&pb.CleanupAck{}, "rejected_block_seq", 6, protoreflect.Uint64Kind, false, ""},
		{&pb.ConsensusParamsUpdate{}, "tracks_enabled", 13, protoreflect.BoolKind, false, ""},
		{&pb.ConsensusMutableParams{}, "tracks_enabled", 8, protoreflect.BoolKind, false, ""},
		{&pb.QuarantineIndexerCmd{}, "indexer_id", 1, protoreflect.Uint64Kind, false, ""},
		{&pb.QuarantineIndexerCmd{}, "expected_generation", 2, protoreflect.Uint64Kind, false, ""},
		{&pb.QuarantineIndexerCmd{}, "reason", 3, protoreflect.StringKind, false, ""},
		{&pb.QuarantineIndexerCmd{}, "authority_jws", 4, protoreflect.StringKind, false, ""},
		{&pb.ReleaseIndexerCmd{}, "indexer_id", 1, protoreflect.Uint64Kind, false, ""},
		{&pb.ReleaseIndexerCmd{}, "expected_generation", 2, protoreflect.Uint64Kind, false, ""},
		{&pb.ReleaseIndexerCmd{}, "acknowledged_rejected_blocks", 3, protoreflect.Uint64Kind, true, ""},
		{&pb.ReleaseIndexerCmd{}, "authority_jws", 4, protoreflect.StringKind, false, ""},
		{&pb.IndexerTrackView{}, "indexer_id", 1, protoreflect.Uint64Kind, false, ""},
		{&pb.IndexerTrackView{}, "initial", 2, protoreflect.MessageKind, false, "arbiter.TrackFrontier"},
		{&pb.IndexerTrackView{}, "quarantine_generation", 3, protoreflect.Uint64Kind, false, ""},
		{&pb.IndexerTrackView{}, "quarantined", 4, protoreflect.BoolKind, false, ""},
		{&pb.IndexerTrackView{}, "quarantine_reason", 5, protoreflect.StringKind, false, ""},
		{&pb.IndexerTrackView{}, "rejected_blocks", 6, protoreflect.Uint64Kind, true, ""},
		{&pb.IndexerTrackView{}, "acknowledged_rejected_blocks", 7, protoreflect.Uint64Kind, true, ""},
		{&pb.IndexerTrackView{}, "prefix_block_seq", 8, protoreflect.Uint64Kind, false, ""},
		{&pb.IndexerTrackView{}, "published", 9, protoreflect.MessageKind, false, "arbiter.TrackFrontier"},
		{&pb.TrackStateView{}, "tracks_enabled", 1, protoreflect.BoolKind, false, ""},
		{&pb.TrackStateView{}, "cutover_block_seq", 2, protoreflect.Uint64Kind, false, ""},
		{&pb.TrackStateView{}, "cutover_promotion_seq", 3, protoreflect.Uint64Kind, false, ""},
		{&pb.TrackStateView{}, "global_safe_block_seq", 4, protoreflect.Uint64Kind, false, ""},
		{&pb.TrackStateView{}, "published_safe_block_seq", 5, protoreflect.Uint64Kind, false, ""},
		{&pb.TrackStateView{}, "indexers", 6, protoreflect.MessageKind, true, "arbiter.IndexerTrackView"},
		{&pb.TableRegistrySnapshot{}, "quarantined_indexers", 9, protoreflect.Uint64Kind, true, ""},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name())+"."+string(tc.name), func(t *testing.T) {
			f := d.Fields().ByName(tc.name)
			if f == nil || f.Number() != tc.number || f.Kind() != tc.kind || f.IsList() != tc.repeated || f.HasOptionalKeyword() || f.ContainingOneof() != nil {
				t.Fatalf("field = %v, want number %d kind %s repeated %v, not optional, outside any oneof", f, tc.number, tc.kind, tc.repeated)
			}
			if tc.msgType != "" && f.Message().FullName() != tc.msgType {
				t.Fatalf("message type = %s, want %s", f.Message().FullName(), tc.msgType)
			}
		})
	}
	for msg, want := range map[proto.Message]int{
		&pb.TrackFrontier{}: 3, &pb.SafeSnapshotManifest{}: 11, &pb.L3BlockTrack{}: 1, &pb.L3BlockHeader{}: 14,
		&pb.UnsafeCleanup{}: 5, &pb.CleanupAck{}: 6, &pb.ConsensusParamsUpdate{}: 13, &pb.ConsensusMutableParams{}: 8,
		&pb.QuarantineIndexerCmd{}: 4, &pb.ReleaseIndexerCmd{}: 4, &pb.IndexerTrackView{}: 9, &pb.TrackStateView{}: 6,
		&pb.TableRegistrySnapshot{}: 9,
	} {
		if got := msg.ProtoReflect().Descriptor().Fields().Len(); got != want {
			t.Fatalf("%s field count = %d, want %d", msg.ProtoReflect().Descriptor().Name(), got, want)
		}
	}
	// TrackFrontier mirrors pkg/replay beside SafeSnapshotManifest, and
	// consensus.proto already imports replay.proto for IndexerTrackView; the
	// commands are RPC requests and Raft commands at once, so they live in
	// consensus.proto, which raftlog.proto imports for tags 36 and 37.
	for _, tc := range []struct {
		file protoreflect.FileDescriptor
		name protoreflect.Name
	}{
		{pb.File_replay_proto, "TrackFrontier"},
		{pb.File_arbiter_proto, "L3BlockTrack"},
		{pb.File_consensus_proto, "QuarantineIndexerCmd"},
		{pb.File_consensus_proto, "ReleaseIndexerCmd"},
		{pb.File_consensus_proto, "IndexerTrackView"},
		{pb.File_consensus_proto, "TrackStateView"},
	} {
		if tc.file.Messages().ByName(tc.name) == nil {
			t.Fatalf("%s is not defined in %s", tc.name, tc.file.Path())
		}
	}
}

// TestIndexerQuarantinedAdmissionCodeIsEleven pins the stage-2 admission code.
// HouseGate maps arbiter codes through their enum name, so the name is pinned
// as well as the number.
func TestIndexerQuarantinedAdmissionCodeIsEleven(t *testing.T) {
	if got := int32(pb.AdmissionCode_ADMISSION_CODE_INDEXER_QUARANTINED); got != 11 {
		t.Fatalf("ADMISSION_CODE_INDEXER_QUARANTINED = %d, want 11", got)
	}
	if got := pb.AdmissionCode_ADMISSION_CODE_INDEXER_QUARANTINED.String(); got != "ADMISSION_CODE_INDEXER_QUARANTINED" {
		t.Fatalf("ADMISSION_CODE_INDEXER_QUARANTINED name = %q", got)
	}
	if got := int32(pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE); got != 10 {
		t.Fatalf("ADMISSION_CODE_SOURCE_UNAVAILABLE moved to %d", got)
	}
	if n := pb.AdmissionCode(0).Descriptor().Values().Len(); n != 12 {
		t.Fatalf("AdmissionCode has %d values, want 12", n)
	}
}

// TestTracksRaftCommandTags pins the two stage-2 Raft commands: the next free
// tags after the dynamic SI registry's 31-35, with tag 29 still unused. A voter
// whose decoder lacks them refuses the command, so neither is proposed before
// the tracks activation.
func TestTracksRaftCommandTags(t *testing.T) {
	cmd := (&pb.RaftCommand{}).ProtoReflect().Descriptor().Oneofs().ByName("cmd").Fields()
	for _, tc := range []struct {
		number protoreflect.FieldNumber
		name   protoreflect.Name
		typ    string
	}{
		{36, "quarantine_indexer", "QuarantineIndexerCmd"},
		{37, "release_indexer", "ReleaseIndexerCmd"},
	} {
		if f := cmd.ByNumber(tc.number); f == nil || f.Name() != tc.name {
			t.Fatalf("RaftCommand tag %d = %v, want %s", tc.number, f, tc.name)
		}
		assertSnapshotVariant(t, &pb.RaftCommand{}, "cmd", string(tc.name), tc.number, tc.typ)
	}
	if f := cmd.ByNumber(29); f != nil {
		t.Fatalf("RaftCommand tag 29 must stay unused, got %s", f.Name())
	}
	if f := cmd.ByNumber(35); f == nil || f.Name() != "record_table_purged" {
		t.Fatalf("RaftCommand tag 35 = %v, want record_table_purged", f)
	}
}

// TestTracksRPCSignatures pins the three ConsensusAdmin RPCs of stage 2 and the
// data-plane and read RPCs whose existing messages carry the new fields.
// Servers that embed the generated stub answer Unimplemented until the arbiter
// implements the tracks gate and the authority checks.
func TestTracksRPCSignatures(t *testing.T) {
	for _, tc := range []struct {
		file          protoreflect.FileDescriptor
		service       protoreflect.Name
		method        protoreflect.Name
		input, output protoreflect.FullName
		serverStream  bool
	}{
		{pb.File_consensus_proto, "ConsensusAdmin", "QuarantineIndexer", "arbiter.QuarantineIndexerCmd", "arbiter.Ack", false},
		{pb.File_consensus_proto, "ConsensusAdmin", "ReleaseIndexer", "arbiter.ReleaseIndexerCmd", "arbiter.Ack", false},
		{pb.File_consensus_proto, "ConsensusAdmin", "GetTrackState", "google.protobuf.Empty", "arbiter.TrackStateView", false},
		{pb.File_arbiter_proto, "PromotionGateway", "AckCleanup", "arbiter.CleanupAck", "arbiter.Ack", false},
		{pb.File_arbiter_proto, "SafeState", "GetL3Block", "arbiter.L3BlockRef", "arbiter.L3Block", false},
		{pb.File_arbiter_proto, "SafeState", "GetManifest", "arbiter.SnapshotRef", "arbiter.SafeSnapshotManifest", false},
		{pb.File_table_registry_proto, "TableRegistry", "WatchTableRegistry", "arbiter.WatchTableRegistryRequest", "arbiter.TableRegistrySnapshot", true},
	} {
		svc := tc.file.Services().ByName(tc.service)
		if svc == nil {
			t.Fatalf("service %s missing", tc.service)
		}
		m := svc.Methods().ByName(tc.method)
		if m == nil || m.Input().FullName() != tc.input || m.Output().FullName() != tc.output || m.IsStreamingClient() || m.IsStreamingServer() != tc.serverStream {
			t.Fatalf("%s.%s = %v, want %s -> %s (server stream %v)", tc.service, tc.method, m, tc.input, tc.output, tc.serverStream)
		}
	}
	if pb.ConsensusAdmin_QuarantineIndexer_FullMethodName != "/arbiter.ConsensusAdmin/QuarantineIndexer" ||
		pb.ConsensusAdmin_ReleaseIndexer_FullMethodName != "/arbiter.ConsensusAdmin/ReleaseIndexer" ||
		pb.ConsensusAdmin_GetTrackState_FullMethodName != "/arbiter.ConsensusAdmin/GetTrackState" {
		t.Fatal("stage-2 ConsensusAdmin full method names moved")
	}
	srv := pb.UnimplementedConsensusAdminServer{}
	ctx := context.Background()
	if _, err := srv.QuarantineIndexer(ctx, &pb.QuarantineIndexerCmd{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("UnimplementedConsensusAdminServer.QuarantineIndexer = %v, want Unimplemented", err)
	}
	if _, err := srv.ReleaseIndexer(ctx, &pb.ReleaseIndexerCmd{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("UnimplementedConsensusAdminServer.ReleaseIndexer = %v, want Unimplemented", err)
	}
	if _, err := srv.GetTrackState(ctx, nil); status.Code(err) != codes.Unimplemented {
		t.Fatalf("UnimplementedConsensusAdminServer.GetTrackState = %v, want Unimplemented", err)
	}
}

// TestTracksFieldsAbsentLeaveTheWireUnchanged is the wire half of the
// strict-decoder rule (Plan S2-A contract §0): every message that gained a
// stage-2 field encodes, with that field unset or zero, without any trace of
// it, so whatever the leader proposes before the tracks activation is byte for
// byte what a v0.10.0 encoder writes. Setting the field only appends its own
// tags after those unchanged bytes, because each new field has its message's
// highest number.
func TestTracksFieldsAbsentLeaveTheWireUnchanged(t *testing.T) {
	for _, tc := range []struct {
		msg    proto.Message
		number protoreflect.FieldNumber
	}{
		{&pb.SafeSnapshotManifest{}, 11},
		{&pb.L3BlockHeader{}, 14},
		{&pb.UnsafeCleanup{}, 5},
		{&pb.CleanupAck{}, 6},
		{&pb.ConsensusParamsUpdate{}, 13},
		{&pb.ConsensusMutableParams{}, 8},
		{&pb.TableRegistrySnapshot{}, 9},
	} {
		m := tc.msg.ProtoReflect()
		t.Run(string(m.Descriptor().Name()), func(t *testing.T) {
			fields := m.Descriptor().Fields()
			fd := fields.ByNumber(tc.number)
			for i := 0; i < fields.Len(); i++ {
				if f := fields.Get(i); f.Number() != tc.number {
					populateSnapshotField(m, f, uint64(i+1))
				}
			}
			legacy := deterministicBytes(t, tc.msg)
			if slices.Contains(wireFieldNumbers(t, legacy), tc.number) {
				t.Fatalf("unset field %d is on the wire: %x", tc.number, legacy)
			}
			if fd.Message() == nil && !fd.IsList() {
				m.Set(fd, fd.Default())
				if got := deterministicBytes(t, tc.msg); !bytes.Equal(got, legacy) {
					t.Fatalf("explicit zero of field %d encodes as %x, want %x", tc.number, got, legacy)
				}
			}
			populateSnapshotField(m, fd, 1)
			full := deterministicBytes(t, tc.msg)
			if !bytes.HasPrefix(full, legacy) {
				t.Fatalf("setting field %d changed the bytes before it: %x, want prefix %x", tc.number, full, legacy)
			}
			tail := wireFieldNumbers(t, full[len(legacy):])
			if len(tail) == 0 {
				t.Fatalf("set field %d is not on the wire", tc.number)
			}
			for _, n := range tail {
				if n != tc.number {
					t.Fatalf("setting field %d appended field %d", tc.number, n)
				}
			}
		})
	}
}

// TestTrackMessagesKeepIndexerZeroDistinctFromAbsent pins why the header's
// track is a message and why a frontier entry serializes zeros: indexer 0 is a
// valid owner (devnet2's founding indexer), so a header of track 0 and a
// frontier of indexer 0 must reach the wire as present, empty messages that
// differ from absent and decode back with their presence.
func TestTrackMessagesKeepIndexerZeroDistinctFromAbsent(t *testing.T) {
	header := &pb.L3BlockHeader{L3BlockSeq: 57, StatementSeqStart: 900, StatementCount: 3}
	absent := deterministicBytes(t, header)
	header.Track = &pb.L3BlockTrack{IndexerId: 0}
	zero := deterministicBytes(t, header)
	// track is the header's highest field, so track 0 is the absent encoding
	// plus one tag and an empty message.
	if want := protowire.AppendBytes(protowire.AppendTag(bytes.Clone(absent), 14, protowire.BytesType), nil); !bytes.Equal(zero, want) {
		t.Fatalf("track 0 encodes as %x, want %x", zero, want)
	}
	var back pb.L3BlockHeader
	if err := proto.Unmarshal(zero, &back); err != nil || back.GetTrack() == nil || back.GetTrack().GetIndexerId() != 0 {
		t.Fatalf("decoded track = %v (err %v), want present indexer 0", back.GetTrack(), err)
	}
	var none pb.L3BlockHeader
	if err := proto.Unmarshal(absent, &none); err != nil || none.GetTrack() != nil {
		t.Fatalf("decoded absent track = %v (err %v), want nil", none.GetTrack(), err)
	}

	manifest := &pb.SafeSnapshotManifest{SnapshotId: "0xsnap", SafeBlockSeq: 57}
	absent = deterministicBytes(t, manifest)
	manifest.TrackFrontiers = []*pb.TrackFrontier{{IndexerId: 0}}
	zero = deterministicBytes(t, manifest)
	if want := protowire.AppendBytes(protowire.AppendTag(bytes.Clone(absent), 11, protowire.BytesType), nil); !bytes.Equal(zero, want) {
		t.Fatalf("a frontier of indexer 0 encodes as %x, want %x", zero, want)
	}
	var manifestBack pb.SafeSnapshotManifest
	if err := proto.Unmarshal(zero, &manifestBack); err != nil || len(manifestBack.GetTrackFrontiers()) != 1 || manifestBack.GetTrackFrontiers()[0].GetIndexerId() != 0 {
		t.Fatalf("decoded frontiers = %v (err %v), want one entry for indexer 0", manifestBack.GetTrackFrontiers(), err)
	}
}

func deterministicBytes(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// wireFieldNumbers lists the top-level field numbers of encoded message bytes
// in wire order.
func wireFieldNumbers(t *testing.T, raw []byte) []protowire.Number {
	t.Helper()
	var out []protowire.Number
	for len(raw) > 0 {
		num, typ, n := protowire.ConsumeTag(raw)
		if n < 0 {
			t.Fatalf("bad tag: %v", protowire.ParseError(n))
		}
		m := protowire.ConsumeFieldValue(num, typ, raw[n:])
		if m < 0 {
			t.Fatalf("bad field %d: %v", num, protowire.ParseError(m))
		}
		out = append(out, num)
		raw = raw[n+m:]
	}
	return out
}
