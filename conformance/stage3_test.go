package conformance

import (
	"bytes"
	"context"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// stage3ConsensusFields is the frozen field ledger of every message the
// multi-source SI stage-3 P5 wire contract adds (housegate spec 2026-10-14
// §6.1-§6.4, §7.1-§7.4, §9.1-§9.2; Plan S3-E1 contract §39), in the style of
// stage3Fields: the i-th token is field number i+1, "[]T" is repeated, and T
// is the scalar kind, the message name or the enum name. Every number is a
// consensus constant once the v21 activation commits. The two arbiter.proto
// entries are the every-field coverage TestSnapshotQueryLedgerCoversEveryNewMessage
// asks for; the others are pinned here only.
var stage3ConsensusFields = map[string]string{
	"Stage3Params":           "group_size:uint32 network_fill_default:bool min_safe_copies:uint32 strict_copies:bool indexer_failure_domains:[]IndexerFailureDomain max_lag_publications:uint32 lag_low:uint32 evidence_timeout_l2_blocks:uint64 max_uncleaned_parts_per_partition:uint32 max_uncleaned_parts_per_table:uint32 exclude_quarantined_hosts:bool purge:bool eviction_voiding:bool owner_aware_selection:bool placement:bool reselection:bool checkpoints:bool narrowed_drain:bool",
	"IndexerFailureDomain":   "indexer_id:uint64 failure_domain:string",
	"CheckpointGroup":        "database_id:string group_generation:uint64 excluded:[]ExcludedTable",
	"ExcludedTable":          "table_id:string reason:CheckpointExclusionReason",
	"MemberCheckpoint":       "node_id:string owner_indexer_id:uint64 checkpoint_seq:uint64 groups:[]CheckpointGroup snapshot_id:string entry:TrackFrontier",
	"MemberCheckpointCmd":    "checkpoint:MemberCheckpoint replica_jws:string",
	"MemberReadyCmd":         "node_id:string owner_indexer_id:uint64 database_id:string generation:uint64 ed25519_signature:string",
	"MemberCheckpointRecord": "checkpoint:MemberCheckpoint replica_jws:string epoch:uint64",
	"GroupMember":            "node_id:string joined_generation:uint64 ready_generation:uint64 member_class:MemberClass checkpoint:MemberCheckpointRecord observed_entry:TrackFrontier lag_advances:uint64 host_indexer_id:uint64",
	"LeavingMember":          "node_id:string removed_generation:uint64 release_entry:TrackFrontier frozen_members:[]string frozen_domains:uint32 released:bool",
	"DatabaseGroup":          "owner_indexer_id:uint64 database_id:string members:[]GroupMember network_fill:bool generation:uint64 past_members:[]string leaving:[]LeavingMember group_key:string placement_salt:string",
	"MemberCheckpointSeq":    "node_id:string owner_indexer_id:uint64 checkpoint_seq:uint64 replica_signer:string",
	"RebalanceGroupCmd":      "owner_indexer_id:uint64 database_id:string expected_generation:uint64 add:[]string remove:[]string network_fill:bool authority_jws:string",
	"ReselectVerifiersCmd":   "l3_block_seq:uint64 expected_set:[]string authority_jws:string",
	"MarkMemberLaggingCmd":   "node_id:string owner_indexer_id:uint64 database_id:string observed_entry:TrackFrontier authority_jws:string",
	"Stage3SwitchCutover":    "switch_name:string epoch:uint64 cutover_block_seq:uint64 cutover_promotion_seq:uint64",
	"Stage3StateView":        "activation_epoch:uint64 activation_block_seq:uint64 activation_promotion_seq:uint64 switches:[]Stage3SwitchCutover",
	"BlockGroupRef":          "database_id:string group_generation:uint64",
	"BlockReselection":       "replaced:[]string replacements:[]string ring_position:uint32 cursor_block:uint64 authority_jws:string authority_epoch:uint64",
}

// stage3ConsensusMessageFiles names the file each stage-3 P5 message is
// defined in: the parameter, the member commands and the registry view of
// groups in table_registry.proto (consensus.proto and arbiter.proto import it,
// and TableRegistrySnapshot carries them), the authority commands and the
// cutover view beside ConsensusAdmin in consensus.proto, and the selection
// record's two messages beside BlockVerification in arbiter.proto.
func stage3ConsensusMessageFiles() map[string]protoreflect.FileDescriptor {
	files := map[string]protoreflect.FileDescriptor{}
	for _, name := range []string{"Stage3Params", "IndexerFailureDomain", "CheckpointGroup", "ExcludedTable", "MemberCheckpoint", "MemberCheckpointCmd", "MemberReadyCmd", "MemberCheckpointRecord", "GroupMember", "LeavingMember", "DatabaseGroup", "MemberCheckpointSeq"} {
		files[name] = pb.File_table_registry_proto
	}
	for _, name := range []string{"RebalanceGroupCmd", "ReselectVerifiersCmd", "MarkMemberLaggingCmd", "Stage3SwitchCutover", "Stage3StateView"} {
		files[name] = pb.File_consensus_proto
	}
	for _, name := range []string{"BlockGroupRef", "BlockReselection"} {
		files[name] = pb.File_arbiter_proto
	}
	return files
}

// stage3AppendedFields pins the singular message field the stage-3 P5 wire
// contract appends to each consensus message that exists in the hash-pinned
// baselines: ConsensusMutableParams.stage3 = 9, ConsensusParamsUpdate.stage3 =
// 14 and ConsensusParamsState.stage3_state = 9. Each is its message's last
// field once promotion_finality_gate is stripped, so
// assertSnapshotBaselineDescriptors strips it right after that field, before
// tracksFields inspects the then-last tracks_enabled. Every existing baseline
// predates them, so the exemption applies unconditionally.
var stage3AppendedFields = map[string]*descriptorpb.FieldDescriptorProto{
	"ConsensusMutableParams": tracksMessage("stage3", "stage3", 9, ".arbiter.Stage3Params"),
	"ConsensusParamsUpdate":  tracksMessage("stage3", "stage3", 14, ".arbiter.Stage3Params"),
	"ConsensusParamsState":   tracksMessage("stage3_state", "stage3State", 9, ".arbiter.Stage3StateView"),
}

// TestStage3ConsensusFieldNumbers pins every field the stage-3 P5 wire
// contract appends to an existing message: name, number, kind, cardinality
// and message type, and each message's new field count. arbiter-core's strict
// Raft decoder refuses any field its descriptor does not know, at any depth,
// so a voter that disagrees on one of these numbers forks on the first
// command that carries it. Only VerifierEntry.host_indexer_id is proto3
// optional (indexer 0 is a valid host), inside its synthetic oneof; every
// other field is plain, outside any oneof, so it is off the wire while unset.
func TestStage3ConsensusFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg      proto.Message
		name     protoreflect.Name
		number   protoreflect.FieldNumber
		kind     protoreflect.Kind
		repeated bool
		optional bool
		msgType  protoreflect.FullName
	}{
		{&pb.VerifierEntry{}, "host_indexer_id", 3, protoreflect.Uint64Kind, false, true, ""},
		{&pb.VerifierEntry{}, "replica_signer", 4, protoreflect.StringKind, false, false, ""},
		{&pb.VerifierEntry{}, "consent_jws", 5, protoreflect.StringKind, false, false, ""},
		{&pb.VerifierEntry{}, "weight", 6, protoreflect.Uint32Kind, false, false, ""},
		{&pb.VerifierEntry{}, "failure_domain", 7, protoreflect.StringKind, false, false, ""},
		{&pb.VerifierEntry{}, "verify", 8, protoreflect.BoolKind, false, false, ""},
		{&pb.VerifierEntry{}, "replicate", 9, protoreflect.BoolKind, false, false, ""},
		{&pb.VerifierEntry{}, "counts", 10, protoreflect.BoolKind, false, false, ""},
		{&pb.VerifierEntry{}, "shared_rmt", 11, protoreflect.BoolKind, false, false, ""},
		{&pb.ConsensusMutableParams{}, "stage3", 9, protoreflect.MessageKind, false, false, "arbiter.Stage3Params"},
		{&pb.ConsensusParamsUpdate{}, "stage3", 14, protoreflect.MessageKind, false, false, "arbiter.Stage3Params"},
		{&pb.ConsensusParamsState{}, "stage3_state", 9, protoreflect.MessageKind, false, false, "arbiter.Stage3StateView"},
		{&pb.TableIncarnation{}, "scoped_keeper_path", 19, protoreflect.BoolKind, false, false, ""},
		{&pb.TableIncarnation{}, "purge_members", 20, protoreflect.StringKind, true, false, ""},
		{&pb.TableRegistrySnapshot{}, "groups", 10, protoreflect.MessageKind, true, false, "arbiter.DatabaseGroup"},
		{&pb.TableRegistrySnapshot{}, "member_checkpoint_seqs", 11, protoreflect.MessageKind, true, false, "arbiter.MemberCheckpointSeq"},
		{&pb.TableRegistrySnapshot{}, "stage3", 12, protoreflect.MessageKind, false, false, "arbiter.Stage3Params"},
		{&pb.L3BlockTrack{}, "group_key", 2, protoreflect.StringKind, false, false, ""},
		{&pb.BlockVerification{}, "eligible_ring", 14, protoreflect.StringKind, true, false, ""},
		{&pb.BlockVerification{}, "ring_position", 15, protoreflect.Uint32Kind, false, false, ""},
		{&pb.BlockVerification{}, "marked_cursor_block", 16, protoreflect.Uint64Kind, false, false, ""},
		{&pb.BlockVerification{}, "group_key", 17, protoreflect.StringKind, false, false, ""},
		{&pb.BlockVerification{}, "group_members", 18, protoreflect.StringKind, true, false, ""},
		{&pb.BlockVerification{}, "group_fill", 19, protoreflect.BoolKind, false, false, ""},
		{&pb.BlockVerification{}, "group_databases", 20, protoreflect.MessageKind, true, false, "arbiter.BlockGroupRef"},
		{&pb.BlockVerification{}, "reselections", 21, protoreflect.MessageKind, true, false, "arbiter.BlockReselection"},
		{&pb.BlockVerification{}, "reselections_at_latch", 22, protoreflect.Uint32Kind, false, false, ""},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name())+"."+string(tc.name), func(t *testing.T) {
			f := d.Fields().ByName(tc.name)
			if f == nil || f.Number() != tc.number || f.Kind() != tc.kind || f.IsList() != tc.repeated || f.IsMap() || f.HasOptionalKeyword() != tc.optional {
				t.Fatalf("field = %v, want number %d kind %s repeated %v optional %v", f, tc.number, tc.kind, tc.repeated, tc.optional)
			}
			if oneof := f.ContainingOneof(); (oneof != nil) != tc.optional || (oneof != nil && !oneof.IsSynthetic()) {
				t.Fatalf("field %s oneof = %v, want only the synthetic oneof of a proto3 optional field", tc.name, oneof)
			}
			if tc.msgType != "" && f.Message().FullName() != tc.msgType {
				t.Fatalf("message type = %s, want %s", f.Message().FullName(), tc.msgType)
			}
		})
	}
	for msg, want := range map[proto.Message]int{
		&pb.VerifierEntry{}: 11, &pb.ConsensusMutableParams{}: 17, &pb.ConsensusParamsUpdate{}: 22, &pb.ConsensusParamsState{}: 9,
		&pb.TableIncarnation{}: 20, &pb.TableRegistrySnapshot{}: 15, &pb.L3BlockTrack{}: 2, &pb.BlockVerification{}: 23,
	} {
		if got := msg.ProtoReflect().Descriptor().Fields().Len(); got != want {
			t.Fatalf("%s field count = %d, want %d", msg.ProtoReflect().Descriptor().Name(), got, want)
		}
	}
	// The generated optional field is a pointer, so 0 and absent stay apart.
	var _ *uint64 = (&pb.VerifierEntry{}).HostIndexerId
	var _ *uint64 = (&pb.GroupMember{}).HostIndexerId
}

// TestStage3ConsensusFieldLedgers pins every stage-3 P5 message field by
// field: name, number (contiguous from 1, in ledger order), cardinality and
// type, none in a map, and the file the message lives in. No field is proto3
// optional or in a oneof, with exactly one exception: GroupMember.host_indexer_id
// (field 8), the served host of a member, which keeps indexer 0 distinct from
// absent and sits only in its synthetic oneof. It then runs the every-field
// wire round trip of TestStage3FieldLedgers on the generated bindings: every
// field set (two items per list, nested messages filled recursively), each
// field encoded alone under its own tag, and the whole message decoded back
// unchanged.
func TestStage3ConsensusFieldLedgers(t *testing.T) {
	files := stage3ConsensusMessageFiles()
	if len(files) != len(stage3ConsensusFields) {
		t.Fatalf("%d stage-3 P5 messages have a home file, %d have a ledger", len(files), len(stage3ConsensusFields))
	}
	names := make([]string, 0, len(stage3ConsensusFields))
	for name := range stage3ConsensusFields {
		if _, ok := stage3Fields[name]; ok {
			t.Fatalf("%s is in both stage-3 ledgers", name)
		}
		names = append(names, name)
	}
	sort.Strings(names)
	optionals := 0
	for _, name := range names {
		spec := stage3ConsensusFields[name]
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
				optional := name == "GroupMember" && fieldName == "host_indexer_id"
				if fd.Number() != protoreflect.FieldNumber(i+1) || fd.IsList() != repeated || fd.IsMap() || fd.HasOptionalKeyword() != optional {
					t.Fatalf("%s allocation changed: %v", fieldName, fd)
				}
				if oneof := fd.ContainingOneof(); (oneof != nil) != optional || (oneof != nil && !oneof.IsSynthetic()) {
					t.Fatalf("%s oneof = %v, want none but a proto3 optional's synthetic one", fieldName, oneof)
				}
				if optional {
					optionals++
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
	if optionals != 1 {
		t.Fatalf("%d proto3 optional fields among the stage-3 P5 messages, want exactly GroupMember.host_indexer_id", optionals)
	}
}

// TestStage3ConsensusEnumValues pins the two stage-3 P5 enums by number and
// name. CheckpointExclusionReason is the closed set of checkpoint exclusions
// (Plan S3-E1 K63): a decoder refuses UNSPECIFIED and any value it does not
// list, so a new reason needs a new release. MemberClass is a member's class
// in one group (spec §9.2), published in the registry view.
func TestStage3ConsensusEnumValues(t *testing.T) {
	for _, tc := range []struct {
		enum   protoreflect.EnumDescriptor
		values []protoreflect.Name
	}{
		{pb.CheckpointExclusionReason(0).Descriptor(), []protoreflect.Name{"CHECKPOINT_EXCLUSION_REASON_UNSPECIFIED", "CHECKPOINT_EXCLUSION_REASON_NOT_INSTALLED", "CHECKPOINT_EXCLUSION_REASON_DIVERGED", "CHECKPOINT_EXCLUSION_REASON_FOREIGN", "CHECKPOINT_EXCLUSION_REASON_DISK_THRESHOLD", "CHECKPOINT_EXCLUSION_REASON_AUDIT_PENDING", "CHECKPOINT_EXCLUSION_REASON_RETIRING"}},
		{pb.MemberClass(0).Descriptor(), []protoreflect.Name{"MEMBER_CLASS_UNSPECIFIED", "MEMBER_CLASS_BOOTSTRAPPING", "MEMBER_CLASS_CAUGHT_UP", "MEMBER_CLASS_BEHIND", "MEMBER_CLASS_LAGGING"}},
	} {
		t.Run(string(tc.enum.Name()), func(t *testing.T) {
			if tc.enum.ParentFile() != pb.File_table_registry_proto {
				t.Fatalf("%s is not defined in table_registry.proto", tc.enum.FullName())
			}
			if tc.enum.Values().Len() != len(tc.values) {
				t.Fatalf("%s has %d values, want %d", tc.enum.FullName(), tc.enum.Values().Len(), len(tc.values))
			}
			for i, name := range tc.values {
				if v := tc.enum.Values().Get(i); v.Number() != protoreflect.EnumNumber(i) || v.Name() != name {
					t.Fatalf("%s value %d = %v, want %s = %d", tc.enum.FullName(), i, v, name, i)
				}
			}
		})
	}
	// The generated constants later plans name, checked by the compiler.
	for _, tc := range []struct {
		got  protoreflect.Enum
		num  protoreflect.EnumNumber
		name string
	}{
		{pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_NOT_INSTALLED, 1, "CHECKPOINT_EXCLUSION_REASON_NOT_INSTALLED"},
		{pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_DIVERGED, 2, "CHECKPOINT_EXCLUSION_REASON_DIVERGED"},
		{pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_FOREIGN, 3, "CHECKPOINT_EXCLUSION_REASON_FOREIGN"},
		{pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_DISK_THRESHOLD, 4, "CHECKPOINT_EXCLUSION_REASON_DISK_THRESHOLD"},
		{pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_AUDIT_PENDING, 5, "CHECKPOINT_EXCLUSION_REASON_AUDIT_PENDING"},
		{pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_RETIRING, 6, "CHECKPOINT_EXCLUSION_REASON_RETIRING"},
		{pb.MemberClass_MEMBER_CLASS_BOOTSTRAPPING, 1, "MEMBER_CLASS_BOOTSTRAPPING"},
		{pb.MemberClass_MEMBER_CLASS_CAUGHT_UP, 2, "MEMBER_CLASS_CAUGHT_UP"},
		{pb.MemberClass_MEMBER_CLASS_BEHIND, 3, "MEMBER_CLASS_BEHIND"},
		{pb.MemberClass_MEMBER_CLASS_LAGGING, 4, "MEMBER_CLASS_LAGGING"},
	} {
		if tc.got.Number() != tc.num || string(tc.got.Descriptor().Values().ByNumber(tc.num).Name()) != tc.name {
			t.Fatalf("%v = %d, want %s = %d", tc.got, tc.got.Number(), tc.name, tc.num)
		}
	}
}

// TestStage3ConsensusAdmissionCodes pins the two stage-3 admission codes by
// number and name: HouseGate maps arbiter codes through their enum name and
// treats an unknown one as an unknown outcome, so a renumbering would send a
// refused statement to status probing (Plan S3-E1 K59).
func TestStage3ConsensusAdmissionCodes(t *testing.T) {
	for _, tc := range []struct {
		code pb.AdmissionCode
		num  int32
		name string
	}{
		{pb.AdmissionCode_ADMISSION_CODE_INDEXER_QUARANTINED, 11, "ADMISSION_CODE_INDEXER_QUARANTINED"},
		{pb.AdmissionCode_ADMISSION_CODE_VERIFIERS_UNAVAILABLE, 12, "ADMISSION_CODE_VERIFIERS_UNAVAILABLE"},
		{pb.AdmissionCode_ADMISSION_CODE_COPIES_BACKPRESSURE, 13, "ADMISSION_CODE_COPIES_BACKPRESSURE"},
	} {
		if int32(tc.code) != tc.num || tc.code.String() != tc.name {
			t.Fatalf("%s = %d, want %s = %d", tc.code, int32(tc.code), tc.name, tc.num)
		}
	}
	if n := pb.AdmissionCode(0).Descriptor().Values().Len(); n != 14 {
		t.Fatalf("AdmissionCode has %d values, want 14", n)
	}
}

// TestStage3ConsensusRaftCommandTags pins the five stage-3 Raft commands as
// tags 38-42 of RaftCommand's cmd oneof, the only oneof members above the
// stage-2 tag 37, with tag 29 still unused. A voter whose decoder lacks them
// refuses the command, so none is proposed before the v21 activation and its
// switch (placement: 38, 40; reselection: 39; checkpoints: 41, 42).
func TestStage3ConsensusRaftCommandTags(t *testing.T) {
	raft := (&pb.RaftCommand{}).ProtoReflect().Descriptor()
	cmd := raft.Oneofs().ByName("cmd")
	if cmd == nil || raft.Fields().Len() != 43 || cmd.Fields().Len() != 43 {
		t.Fatalf("RaftCommand has %d fields, want 43 oneof tags", raft.Fields().Len())
	}
	tags := map[protoreflect.FieldNumber]struct{ name, typ string }{
		38: {"rebalance_group", "RebalanceGroupCmd"},
		39: {"reselect_verifiers", "ReselectVerifiersCmd"},
		40: {"member_ready", "MemberReadyCmd"},
		41: {"member_checkpoint", "MemberCheckpointCmd"},
		42: {"mark_member_lagging", "MarkMemberLaggingCmd"},
	}
	for number := protoreflect.FieldNumber(38); number <= 42; number++ {
		tc := tags[number]
		if f := cmd.Fields().ByNumber(number); f == nil || f.Name() != protoreflect.Name(tc.name) {
			t.Fatalf("RaftCommand tag %d = %v, want %s", number, f, tc.name)
		}
		assertSnapshotVariant(t, &pb.RaftCommand{}, "cmd", tc.name, number, tc.typ)
	}
	for i := 0; i < raft.Fields().Len(); i++ {
		// Tag 43 is the anchor finality attestation (anchor_finality_test.go),
		// tag 44 the membership lease renewal (membership_lease_test.go).
		if f := raft.Fields().Get(i); f.Number() > 37 && f.Number() != 43 && f.Number() != 44 && tags[f.Number()].name == "" {
			t.Fatalf("RaftCommand tag %d (%s) is not a stage-3 command", f.Number(), f.Name())
		}
	}
	if f := raft.Fields().ByNumber(29); f != nil {
		t.Fatalf("RaftCommand tag 29 must stay unused, got %s", f.Name())
	}
	if f := raft.Fields().ByNumber(37); f == nil || f.Name() != "release_indexer" {
		t.Fatalf("RaftCommand tag 37 = %v, want release_indexer", f)
	}
	// The generated oneof wrappers and getters later plans use, checked by the
	// compiler.
	for _, tc := range []struct {
		cmd *pb.RaftCommand
		get func(*pb.RaftCommand) proto.Message
	}{
		{&pb.RaftCommand{Cmd: &pb.RaftCommand_RebalanceGroup{RebalanceGroup: &pb.RebalanceGroupCmd{}}}, func(c *pb.RaftCommand) proto.Message { return c.GetRebalanceGroup() }},
		{&pb.RaftCommand{Cmd: &pb.RaftCommand_ReselectVerifiers{ReselectVerifiers: &pb.ReselectVerifiersCmd{}}}, func(c *pb.RaftCommand) proto.Message { return c.GetReselectVerifiers() }},
		{&pb.RaftCommand{Cmd: &pb.RaftCommand_MemberReady{MemberReady: &pb.MemberReadyCmd{}}}, func(c *pb.RaftCommand) proto.Message { return c.GetMemberReady() }},
		{&pb.RaftCommand{Cmd: &pb.RaftCommand_MemberCheckpoint{MemberCheckpoint: &pb.MemberCheckpointCmd{}}}, func(c *pb.RaftCommand) proto.Message { return c.GetMemberCheckpoint() }},
		{&pb.RaftCommand{Cmd: &pb.RaftCommand_MarkMemberLagging{MarkMemberLagging: &pb.MarkMemberLaggingCmd{}}}, func(c *pb.RaftCommand) proto.Message { return c.GetMarkMemberLagging() }},
	} {
		if !tc.get(tc.cmd).ProtoReflect().IsValid() || tc.get(&pb.RaftCommand{}).ProtoReflect().IsValid() {
			t.Fatalf("getter does not report exactly the command of %v", tc.cmd)
		}
	}
}

// TestStage3ConsensusRPCSignatures pins the four stage-3 P5 RPCs: unary, with
// exactly these request and response types, ConsensusAdmin's RebalanceGroup
// and MarkMemberLagging at indices 16 and 17 after Plan S3-D's GetTrackSettle
// (15), VerifierGateway's SubmitMemberReady and SubmitMemberCheckpoint at 4 and
// 5 after SubmitByteSideScan (3), the generated full method names and client
// and server signatures, and that a server embedding the generated stub
// answers Unimplemented until the arbiter implements them (Plans S3-E3 and
// S3-E4). The request of each is its Raft command. ConsensusAdmin's total is
// pinned by TestConsensusAdminRPCSignatures and TestStage3RPCSignatures.
func TestStage3ConsensusRPCSignatures(t *testing.T) {
	for _, tc := range []struct {
		file          protoreflect.FileDescriptor
		service       protoreflect.Name
		previous      protoreflect.Name
		previousIndex int
		index         int
		method        protoreflect.Name
		input, output protoreflect.FullName
		fullName      string
		server        any
	}{
		{pb.File_consensus_proto, "ConsensusAdmin", "GetTrackSettle", 15, 16, "RebalanceGroup", "arbiter.RebalanceGroupCmd", "arbiter.Ack", pb.ConsensusAdmin_RebalanceGroup_FullMethodName, pb.UnimplementedConsensusAdminServer{}},
		{pb.File_consensus_proto, "ConsensusAdmin", "GetTrackSettle", 15, 17, "MarkMemberLagging", "arbiter.MarkMemberLaggingCmd", "arbiter.Ack", pb.ConsensusAdmin_MarkMemberLagging_FullMethodName, pb.UnimplementedConsensusAdminServer{}},
		{pb.File_arbiter_proto, "VerifierGateway", "SubmitByteSideScan", 3, 4, "SubmitMemberReady", "arbiter.MemberReadyCmd", "arbiter.Ack", pb.VerifierGateway_SubmitMemberReady_FullMethodName, pb.UnimplementedVerifierGatewayServer{}},
		{pb.File_arbiter_proto, "VerifierGateway", "SubmitByteSideScan", 3, 5, "SubmitMemberCheckpoint", "arbiter.MemberCheckpointCmd", "arbiter.Ack", pb.VerifierGateway_SubmitMemberCheckpoint_FullMethodName, pb.UnimplementedVerifierGatewayServer{}},
	} {
		t.Run(string(tc.service)+"/"+string(tc.method), func(t *testing.T) {
			svc := tc.file.Services().ByName(tc.service)
			if svc == nil {
				t.Fatalf("service %s missing from %s", tc.service, tc.file.Path())
			}
			if m := svc.Methods().ByName(tc.previous); m == nil || m.Index() != tc.previousIndex {
				t.Fatalf("%s.%s = %v, want it to keep index %d", tc.service, tc.previous, m, tc.previousIndex)
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
	// The generated client and server signatures, checked by the compiler.
	var (
		_ func(pb.ConsensusAdminClient, context.Context, *pb.RebalanceGroupCmd, ...grpc.CallOption) (*pb.Ack, error)    = pb.ConsensusAdminClient.RebalanceGroup
		_ func(pb.ConsensusAdminClient, context.Context, *pb.MarkMemberLaggingCmd, ...grpc.CallOption) (*pb.Ack, error) = pb.ConsensusAdminClient.MarkMemberLagging
		_ func(pb.VerifierGatewayClient, context.Context, *pb.MemberReadyCmd, ...grpc.CallOption) (*pb.Ack, error)      = pb.VerifierGatewayClient.SubmitMemberReady
		_ func(pb.VerifierGatewayClient, context.Context, *pb.MemberCheckpointCmd, ...grpc.CallOption) (*pb.Ack, error) = pb.VerifierGatewayClient.SubmitMemberCheckpoint
		_ func(pb.ConsensusAdminServer, context.Context, *pb.RebalanceGroupCmd) (*pb.Ack, error)                        = pb.ConsensusAdminServer.RebalanceGroup
		_ func(pb.ConsensusAdminServer, context.Context, *pb.MarkMemberLaggingCmd) (*pb.Ack, error)                     = pb.ConsensusAdminServer.MarkMemberLagging
		_ func(pb.VerifierGatewayServer, context.Context, *pb.MemberReadyCmd) (*pb.Ack, error)                          = pb.VerifierGatewayServer.SubmitMemberReady
		_ func(pb.VerifierGatewayServer, context.Context, *pb.MemberCheckpointCmd) (*pb.Ack, error)                     = pb.VerifierGatewayServer.SubmitMemberCheckpoint
	)
}

// TestStage3ConsensusReadMessagesAreNotRaftReachable is the read-only rule of
// Plan S3-E1 contract §39 at the descriptor level: no RaftCommand field
// reaches the registry view of groups, the cutover view, the BlockVerification
// selection record or any message that carries them, at any depth, so no
// command can carry a served value (GroupMember.host_indexer_id among them)
// into consensus. The parameter, the five commands and what they embed are
// Raft-reachable, which is what makes their numbers consensus constants.
func TestStage3ConsensusReadMessagesAreNotRaftReachable(t *testing.T) {
	reachable := reachableTypes((&pb.RaftCommand{}).ProtoReflect().Descriptor())
	for _, name := range []protoreflect.FullName{
		"arbiter.DatabaseGroup", "arbiter.GroupMember", "arbiter.MemberClass", "arbiter.MemberCheckpointRecord",
		"arbiter.LeavingMember", "arbiter.MemberCheckpointSeq", "arbiter.BlockGroupRef", "arbiter.BlockReselection",
		"arbiter.Stage3SwitchCutover", "arbiter.Stage3StateView", "arbiter.TableIncarnation", "arbiter.TableRegistrySnapshot",
		"arbiter.BlockVerification", "arbiter.ConsensusParamsState",
	} {
		if reachable[name] {
			t.Errorf("RaftCommand reaches read-only %s", name)
		}
	}
	for _, name := range []protoreflect.FullName{
		"arbiter.Stage3Params", "arbiter.IndexerFailureDomain", "arbiter.RebalanceGroupCmd", "arbiter.ReselectVerifiersCmd",
		"arbiter.MarkMemberLaggingCmd", "arbiter.MemberReadyCmd", "arbiter.MemberCheckpointCmd", "arbiter.MemberCheckpoint",
		"arbiter.CheckpointGroup", "arbiter.ExcludedTable", "arbiter.CheckpointExclusionReason", "arbiter.VerifierEntry",
	} {
		if !reachable[name] {
			t.Errorf("RaftCommand walk does not reach %s", name)
		}
	}
	// The walk has teeth: each read response reaches every read-only message
	// it serves.
	for _, tc := range []struct {
		root  proto.Message
		names []protoreflect.FullName
	}{
		{&pb.TableRegistrySnapshot{}, []protoreflect.FullName{"arbiter.DatabaseGroup", "arbiter.GroupMember", "arbiter.MemberClass", "arbiter.MemberCheckpointRecord", "arbiter.LeavingMember", "arbiter.MemberCheckpointSeq", "arbiter.Stage3Params", "arbiter.TableIncarnation"}},
		{&pb.ConsensusParamsState{}, []protoreflect.FullName{"arbiter.Stage3StateView", "arbiter.Stage3SwitchCutover", "arbiter.Stage3Params", "arbiter.VerifierEntry"}},
		{&pb.BlockVerification{}, []protoreflect.FullName{"arbiter.BlockGroupRef", "arbiter.BlockReselection"}},
	} {
		served := reachableTypes(tc.root.ProtoReflect().Descriptor())
		for _, name := range tc.names {
			if !served[name] {
				t.Fatalf("%s walk does not reach %s", tc.root.ProtoReflect().Descriptor().Name(), name)
			}
		}
	}
}

// TestStage3ConsensusFieldsAbsentLeaveTheWireUnchanged is the wire half of
// the activation model (Plan S3-E1 contract §38): every message that gained a
// stage-3 P5 field encodes, with every older field set and the new one unset
// or zero, without any trace of it, so whatever a P5 binary writes before the
// switch that sets a field is byte for byte what a PROTO_TAG_P4 binary writes.
// Setting the field only appends its own tags after those unchanged bytes,
// because the new fields follow every older one. host_indexer_id is proto3
// optional, so its explicit zero is on the wire by design
// (TestStage3ConsensusHostIndexerZeroIsDistinctFromAbsent).
func TestStage3ConsensusFieldsAbsentLeaveTheWireUnchanged(t *testing.T) {
	for _, tc := range []struct {
		msg         func() proto.Message
		first, last protoreflect.FieldNumber
	}{
		{func() proto.Message { return &pb.VerifierEntry{} }, 3, 11},
		{func() proto.Message { return &pb.ConsensusMutableParams{} }, 9, 9},
		{func() proto.Message { return &pb.ConsensusParamsUpdate{} }, 14, 14},
		{func() proto.Message { return &pb.ConsensusParamsState{} }, 9, 9},
		{func() proto.Message { return &pb.TableIncarnation{} }, 19, 20},
		{func() proto.Message { return &pb.TableRegistrySnapshot{} }, 10, 12},
		{func() proto.Message { return &pb.L3BlockTrack{} }, 2, 2},
		{func() proto.Message { return &pb.BlockVerification{} }, 14, 22},
	} {
		for number := tc.first; number <= tc.last; number++ {
			msg := tc.msg()
			m := msg.ProtoReflect()
			fd := m.Descriptor().Fields().ByNumber(number)
			t.Run(string(m.Descriptor().Name())+"."+string(fd.Name()), func(t *testing.T) {
				fields := m.Descriptor().Fields()
				for i := 0; i < fields.Len(); i++ {
					if f := fields.Get(i); f.Number() < tc.first {
						populateSnapshotField(m, f, uint64(i+1))
					}
				}
				legacy := deterministicBytes(t, msg)
				for _, n := range wireFieldNumbers(t, legacy) {
					if n >= tc.first {
						t.Fatalf("stage-3 field %d is on the wire before it is set: %x", n, legacy)
					}
				}
				if fd.Message() == nil && !fd.IsList() && !fd.HasOptionalKeyword() {
					m.Set(fd, fd.Default())
					if got := deterministicBytes(t, msg); !bytes.Equal(got, legacy) {
						t.Fatalf("explicit zero of field %d encodes as %x, want %x", number, got, legacy)
					}
				}
				populateSnapshotField(m, fd, 1)
				full := deterministicBytes(t, msg)
				if !bytes.HasPrefix(full, legacy) {
					t.Fatalf("setting field %d changed the bytes before it: %x, want prefix %x", number, full, legacy)
				}
				tail := wireFieldNumbers(t, full[len(legacy):])
				if len(tail) == 0 || slices.ContainsFunc(tail, func(n protowire.Number) bool { return n != number }) {
					t.Fatalf("setting field %d appended fields %v", number, tail)
				}
			})
		}
	}
}

// TestStage3ConsensusHostIndexerZeroIsDistinctFromAbsent pins why the two
// host_indexer_id fields are proto3 optional: indexer 0 is a valid host
// (devnet2's founding indexer), so a member hosted by indexer 0 must reach the
// wire as an explicit 0 that differs from a network-pool entry, respectively a
// member served without a host, and decode back with its presence.
func TestStage3ConsensusHostIndexerZeroIsDistinctFromAbsent(t *testing.T) {
	zero := uint64(0)
	for _, tc := range []struct {
		name          string
		absent, host0 proto.Message
		number        protowire.Number
		host          func(proto.Message) *uint64
	}{
		{"VerifierEntry", &pb.VerifierEntry{NodeId: "member-a", Ed25519Pubkey: []byte{1, 2, 3}}, &pb.VerifierEntry{NodeId: "member-a", Ed25519Pubkey: []byte{1, 2, 3}, HostIndexerId: &zero}, 3,
			func(m proto.Message) *uint64 { return m.(*pb.VerifierEntry).HostIndexerId }},
		{"GroupMember", &pb.GroupMember{NodeId: "member-a", JoinedGeneration: 1, LagAdvances: 2}, &pb.GroupMember{NodeId: "member-a", JoinedGeneration: 1, LagAdvances: 2, HostIndexerId: &zero}, 8,
			func(m proto.Message) *uint64 { return m.(*pb.GroupMember).HostIndexerId }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			absent := deterministicBytes(t, tc.absent)
			host0 := deterministicBytes(t, tc.host0)
			if want := protowire.AppendVarint(protowire.AppendTag(bytes.Clone(absent), tc.number, protowire.VarintType), 0); !bytes.Equal(host0, want) {
				t.Fatalf("host 0 encodes as %x, want %x", host0, want)
			}
			back := tc.absent.ProtoReflect().New().Interface()
			if err := proto.Unmarshal(host0, back); err != nil {
				t.Fatal(err)
			}
			if got := tc.host(back); got == nil || *got != 0 {
				t.Fatalf("decoded host = %v, want present 0", got)
			}
			none := tc.absent.ProtoReflect().New().Interface()
			if err := proto.Unmarshal(absent, none); err != nil {
				t.Fatal(err)
			}
			if got := tc.host(none); got != nil {
				t.Fatalf("decoded absent host = %d, want nil", *got)
			}
		})
	}
}
