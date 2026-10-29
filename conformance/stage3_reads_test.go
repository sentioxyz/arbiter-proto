package conformance

import (
	"context"
	"reflect"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// stage3Fields is the frozen field ledger of every message the multi-source
// SI stage-3 P0 work adds (housegate spec 2026-10-14 §15; Plan S3-A contract
// §1), in the style of snapshotQueryFields: the i-th token is field number
// i+1, "[]T" is repeated, and T is the scalar kind, the message name or the
// enum name. The nine arbiter.proto entries are the every-field coverage
// TestSnapshotQueryLedgerCoversEveryNewMessage asks for; the four
// consensus.proto entries are pinned here only.
var stage3Fields = map[string]string{
	"GetPartitionStatesRequest":     "table_id:string partition_id:string",
	"PartitionStateEntry":           "table_id:string partition_id:string base_safe_snapshot_id:string base_partition_root:string publish_seq:uint64 safe_parts:[]PartManifestEntry promoted_unsafe_part_row_lthashes:[]string unacked_promotion_seq:uint64 last_acked_promotion_seq:uint64 inventory_unrefreshed:bool",
	"PartitionStates":               "promotion_seq:uint64 safe_watermark:SafeWatermark partitions:[]PartitionStateEntry",
	"GetPromotionOutcomesRequest":   "table_id:string partition_id:string after_promotion_seq:uint64 limit:uint32",
	"PromotionOutcome":              "promotion_seq:uint64 promote:PromoteSafePartition statement_seqs:[]uint64 block_seq:uint64 track:L3BlockTrack source_node:string authority_jws:string authority_epoch:uint64 acked:bool ack:PromotionAck ack_jws:string ack_epoch:uint64 cleanup_pending:bool",
	"PromotionOutcomes":             "promotion_seq:uint64 outcomes:[]PromotionOutcome next_after_promotion_seq:uint64",
	"StatementCandidates":           "statement_seq:uint64 statement_id:string status:string source_node:string rc_bound:bool source_claim_root:string candidate_parts:[]CandidatePart partition_new_part_sums:[]PartitionLtHashSum unpromoted_part_row_lthashes:[]string",
	"VerifierEvidence":              "replica_id:string attestation:ReplayAttestation byte_side_scan:ByteSideScanMsg evaluated:bool check1_root_match:bool check2_partition_match:bool check3_byte_side_match:bool pass:bool",
	"BlockVerification":             "header:L3BlockHeader chain_hash:string replay_job:ReplayJob statements:[]StatementCandidates scan_parts:[]PartRef source_nodes:[]string verifier_set:[]string evidence:[]VerifierEvidence verdict_evaluated:bool quorum:bool anchor:AnchorRef finality:bool last_mergeable:bool",
	"PrepareConsensusUpdateRequest": "mode:ConsensusHoldMode draft:UpdateConsensusParamsCmd timeout_ms:uint64",
	"ConsensusHoldRef":              "hold_id:string",
	"TrackDrain":                    "indexer_id:uint64 quarantined:bool prefix_block_seq:uint64 frontier_block_seq:uint64 in_flight_blocks:[]uint64",
	"ConsensusHold":                 "hold_id:string mode:ConsensusHoldMode raft_term:uint64 promotion_seq:uint64 expires_unix_ms:uint64 ready:bool drain_refusal:string global_prefix_block_seq:uint64 global_frontier_block_seq:uint64 in_flight_blocks:[]uint64 tracks:[]TrackDrain",
}

// stage3MessageFiles names the file each stage-3 message is defined in: the
// reads beside the SafeState service in arbiter.proto, the hold beside the
// ConsensusAdmin service in consensus.proto.
func stage3MessageFiles() map[string]protoreflect.FileDescriptor {
	files := map[string]protoreflect.FileDescriptor{}
	for _, name := range []string{"GetPartitionStatesRequest", "PartitionStateEntry", "PartitionStates", "GetPromotionOutcomesRequest", "PromotionOutcome", "PromotionOutcomes", "StatementCandidates", "VerifierEvidence", "BlockVerification"} {
		files[name] = pb.File_arbiter_proto
	}
	for _, name := range []string{"PrepareConsensusUpdateRequest", "ConsensusHoldRef", "TrackDrain", "ConsensusHold"} {
		files[name] = pb.File_consensus_proto
	}
	return files
}

// TestStage3FieldLedgers pins every stage-3 message field by field: name,
// number (contiguous from 1, in ledger order), cardinality and type, none
// proto3 optional and none in a oneof, and the file the message lives in.
// It then runs the every-field wire round trip of
// TestSnapshotQueryEveryFieldWireRoundTrip on the generated bindings: every
// field set (two items per list, nested messages filled recursively), each
// field encoded alone under its own tag, and the whole message decoded back
// unchanged.
func TestStage3FieldLedgers(t *testing.T) {
	files := stage3MessageFiles()
	if len(files) != len(stage3Fields) {
		t.Fatalf("%d stage-3 messages have a home file, %d have a ledger", len(files), len(stage3Fields))
	}
	for name, spec := range stage3Fields {
		t.Run(name, func(t *testing.T) {
			file, ok := files[name]
			if !ok {
				t.Fatalf("%s has no home file", name)
			}
			md := file.Messages().ByName(protoreflect.Name(name))
			if md == nil {
				t.Fatalf("%s is not defined in %s", name, file.Path())
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
				fieldName, typ, ok := strings.Cut(item, ":")
				if !ok {
					t.Fatalf("bad ledger token %q", item)
				}
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
				number, _, n := protowire.ConsumeTag(raw)
				if n < 0 || number != protowire.Number(i+1) {
					t.Fatalf("%s encoded tag = %d", fieldName, number)
				}
				restored := mt.New()
				if err := proto.Unmarshal(raw, restored.Interface()); err != nil {
					t.Fatal(err)
				}
				if !proto.Equal(single.Interface(), restored.Interface()) {
					t.Fatalf("%s single-field round trip lost data", fieldName)
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

// stage3LedgerType is the ledger spelling of a field's type: the message or
// enum short name, else the scalar kind.
func stage3LedgerType(fd protoreflect.FieldDescriptor) string {
	switch {
	case fd.Message() != nil:
		return string(fd.Message().Name())
	case fd.Enum() != nil:
		return string(fd.Enum().Name())
	default:
		return fd.Kind().String()
	}
}

// TestStage3ConsensusHoldModeValues pins the hold modes by number and name.
// arbiter-admin's --hold flag and the leader's metrics label map onto these
// names, so a renumbering would change what an operator asked for.
func TestStage3ConsensusHoldModeValues(t *testing.T) {
	values := pb.ConsensusHoldMode(0).Descriptor().Values()
	if values.Len() != 3 {
		t.Fatalf("ConsensusHoldMode has %d values, want 3", values.Len())
	}
	for _, tc := range []struct {
		mode pb.ConsensusHoldMode
		num  protoreflect.EnumNumber
		name string
	}{
		{pb.ConsensusHoldMode_CONSENSUS_HOLD_MODE_UNSPECIFIED, 0, "CONSENSUS_HOLD_MODE_UNSPECIFIED"},
		{pb.ConsensusHoldMode_CONSENSUS_HOLD_MODE_ISSUANCE, 1, "CONSENSUS_HOLD_MODE_ISSUANCE"},
		{pb.ConsensusHoldMode_CONSENSUS_HOLD_MODE_DRAIN, 2, "CONSENSUS_HOLD_MODE_DRAIN"},
	} {
		if tc.mode.Number() != tc.num || tc.mode.String() != tc.name {
			t.Fatalf("%s = %d, want %s = %d", tc.mode, tc.mode.Number(), tc.name, tc.num)
		}
	}
	if pb.File_consensus_proto.Enums().ByName("ConsensusHoldMode") == nil {
		t.Fatal("ConsensusHoldMode is not defined in consensus.proto")
	}
}

// TestStage3RPCSignatures pins the three stage-3 SafeState reads and the
// three ConsensusAdmin hold RPCs: unary, their request and response types,
// their position after the methods that existed at arbiter-proto v0.11.0,
// the generated full method names, and that a server embedding the generated
// stub answers Unimplemented until the arbiter implements them.
func TestStage3RPCSignatures(t *testing.T) {
	for _, tc := range []struct {
		file          protoreflect.FileDescriptor
		service       protoreflect.Name
		index         int
		method        protoreflect.Name
		input, output protoreflect.FullName
		fullName      string
		server        any
	}{
		{pb.File_arbiter_proto, "SafeState", 7, "GetPartitionStates", "arbiter.GetPartitionStatesRequest", "arbiter.PartitionStates", pb.SafeState_GetPartitionStates_FullMethodName, pb.UnimplementedSafeStateServer{}},
		{pb.File_arbiter_proto, "SafeState", 8, "GetPromotionOutcomes", "arbiter.GetPromotionOutcomesRequest", "arbiter.PromotionOutcomes", pb.SafeState_GetPromotionOutcomes_FullMethodName, pb.UnimplementedSafeStateServer{}},
		{pb.File_arbiter_proto, "SafeState", 9, "GetBlockVerification", "arbiter.L3BlockRef", "arbiter.BlockVerification", pb.SafeState_GetBlockVerification_FullMethodName, pb.UnimplementedSafeStateServer{}},
		{pb.File_consensus_proto, "ConsensusAdmin", 11, "PrepareConsensusUpdate", "arbiter.PrepareConsensusUpdateRequest", "arbiter.ConsensusHold", pb.ConsensusAdmin_PrepareConsensusUpdate_FullMethodName, pb.UnimplementedConsensusAdminServer{}},
		{pb.File_consensus_proto, "ConsensusAdmin", 12, "GetConsensusHold", "arbiter.ConsensusHoldRef", "arbiter.ConsensusHold", pb.ConsensusAdmin_GetConsensusHold_FullMethodName, pb.UnimplementedConsensusAdminServer{}},
		{pb.File_consensus_proto, "ConsensusAdmin", 13, "ReleaseConsensusHold", "arbiter.ConsensusHoldRef", "arbiter.Ack", pb.ConsensusAdmin_ReleaseConsensusHold_FullMethodName, pb.UnimplementedConsensusAdminServer{}},
	} {
		t.Run(string(tc.service)+"/"+string(tc.method), func(t *testing.T) {
			svc := tc.file.Services().ByName(tc.service)
			if svc == nil {
				t.Fatalf("service %s missing from %s", tc.service, tc.file.Path())
			}
			m := svc.Methods().ByName(tc.method)
			if m == nil || m.Index() != tc.index || m.Input().FullName() != tc.input || m.Output().FullName() != tc.output || m.IsStreamingClient() || m.IsStreamingServer() {
				t.Fatalf("%s.%s = %v, want unary %s -> %s at index %d", tc.service, tc.method, m, tc.input, tc.output, tc.index)
			}
			if want := "/arbiter." + string(tc.service) + "/" + string(tc.method); tc.fullName != want {
				t.Fatalf("full method name = %q, want %q", tc.fullName, want)
			}
			fn := reflect.ValueOf(tc.server).MethodByName(string(tc.method))
			if !fn.IsValid() {
				t.Fatal("generated stub missing")
			}
			result := fn.Call([]reflect.Value{reflect.ValueOf(context.Background()), reflect.New(fn.Type().In(1).Elem())})
			if !result[0].IsNil() || result[1].IsNil() {
				t.Fatal("new RPC returned success before implementation")
			}
			if code := status.Code(result[1].Interface().(error)); code != codes.Unimplemented {
				t.Fatalf("code = %s, want Unimplemented", code)
			}
		})
	}
	for _, tc := range []struct {
		file    protoreflect.FileDescriptor
		service protoreflect.Name
		want    int
	}{
		{pb.File_arbiter_proto, "SafeState", 10},
		{pb.File_consensus_proto, "ConsensusAdmin", 16},
	} {
		if n := tc.file.Services().ByName(tc.service).Methods().Len(); n != tc.want {
			t.Fatalf("%s has %d methods, want %d", tc.service, n, tc.want)
		}
	}
	// The stage-3 additions are the services' last methods: every method at
	// v0.11.0 keeps its position.
	if m := pb.File_arbiter_proto.Services().ByName("SafeState").Methods().Get(6); m.Name() != "GetClientSeqState" {
		t.Fatalf("SafeState method 6 = %s, want GetClientSeqState", m.Name())
	}
	if m := pb.File_consensus_proto.Services().ByName("ConsensusAdmin").Methods().Get(10); m.Name() != "GetTrackState" {
		t.Fatalf("ConsensusAdmin method 10 = %s, want GetTrackState", m.Name())
	}
}

// TestStage3MessagesAreNotRaftReachable is the no-consensus-change rule of
// Plan S3-A contract §0 at the descriptor level: no message or enum this
// task adds is reachable from RaftCommand at any depth, so arbiter-core's
// strict Raft decoder, which refuses an unknown field at any depth, sees an
// unchanged descriptor and no command can carry stage-3 bytes to a voter of
// arbiter v0.12.0. RaftCommand keeps exactly its 36 tags (1-28 and 30-37).
// The hold request embeds the Raft command type, never the reverse, and
// UpdateConsensusParamsCmd keeps exactly its two fields, so the hold id
// cannot travel inside the replicated update.
func TestStage3MessagesAreNotRaftReachable(t *testing.T) {
	raft := (&pb.RaftCommand{}).ProtoReflect().Descriptor()
	reachable := reachableTypes(raft)
	for name := range stage3Fields {
		if reachable[protoreflect.FullName("arbiter."+name)] {
			t.Errorf("RaftCommand reaches stage-3 message %s", name)
		}
	}
	if reachable["arbiter.ConsensusHoldMode"] {
		t.Error("RaftCommand reaches enum ConsensusHoldMode")
	}
	// The walk has teeth: it reaches every Raft-reachable message a stage-3
	// message embeds, and the containers ConsensusParamsUpdate and
	// SafeSnapshotManifest, so a stage-3 reference from any of them is found.
	for _, name := range []protoreflect.FullName{
		"arbiter.UpdateConsensusParamsCmd", "arbiter.ConsensusParamsUpdate", "arbiter.PromoteSafePartition",
		"arbiter.PromotionAck", "arbiter.ReplayAttestation", "arbiter.ByteSideScanMsg",
		"arbiter.SafeSnapshotManifest", "arbiter.PartManifestEntry", "arbiter.CandidatePart",
		"arbiter.PartitionLtHashSum", "arbiter.AnchorRef", "arbiter.PartRef",
	} {
		if !reachable[name] {
			t.Fatalf("RaftCommand walk does not reach %s", name)
		}
	}
	if hold := reachableTypes((&pb.PrepareConsensusUpdateRequest{}).ProtoReflect().Descriptor()); !hold["arbiter.UpdateConsensusParamsCmd"] || !hold["arbiter.ConsensusHoldMode"] {
		t.Fatal("PrepareConsensusUpdateRequest walk does not reach its draft and mode")
	}
	cmd := raft.Oneofs().ByName("cmd")
	if cmd == nil || raft.Fields().Len() != 36 || cmd.Fields().Len() != 36 {
		t.Fatalf("RaftCommand has %d fields, want its 36 oneof tags", raft.Fields().Len())
	}
	for n := protoreflect.FieldNumber(1); n <= 37; n++ {
		if f := raft.Fields().ByNumber(n); (f == nil) != (n == 29) {
			t.Fatalf("RaftCommand tag %d = %v; want tags 1-28 and 30-37 only", n, f)
		}
	}
	update := (&pb.UpdateConsensusParamsCmd{}).ProtoReflect().Descriptor().Fields()
	if update.Len() != 2 || update.ByNumber(1).Name() != "update" || update.ByNumber(2).Name() != "authority_jws" {
		t.Fatalf("UpdateConsensusParamsCmd fields changed: %v", update)
	}
}

// reachableTypes returns the full names of every message and enum reachable
// from root through its fields, root included, at any depth (map entries
// included).
func reachableTypes(root protoreflect.MessageDescriptor) map[protoreflect.FullName]bool {
	seen := map[protoreflect.FullName]bool{}
	var walk func(protoreflect.MessageDescriptor)
	walk = func(md protoreflect.MessageDescriptor) {
		if seen[md.FullName()] {
			return
		}
		seen[md.FullName()] = true
		fields := md.Fields()
		for i := 0; i < fields.Len(); i++ {
			fd := fields.Get(i)
			if fd.Enum() != nil {
				seen[fd.Enum().FullName()] = true
			}
			if fd.Message() != nil {
				walk(fd.Message())
			}
		}
	}
	walk(root)
	return seen
}
