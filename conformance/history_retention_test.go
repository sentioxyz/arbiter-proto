package conformance

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// historyRetentionFields pins the fields history pruning appends as the last
// field of both consensus parameter messages (after
// operations_authority_addresses) and of the three reads that can answer a
// pruned record.
var historyRetentionFields = map[string][]*descriptorpb.FieldDescriptorProto{
	"ConsensusMutableParams": {signedClaimsField("history_retain_blocks", "historyRetainBlocks", 22, descriptorpb.FieldDescriptorProto_TYPE_UINT64)},
	"ConsensusParamsUpdate":  {signedClaimsField("history_retain_blocks", "historyRetainBlocks", 27, descriptorpb.FieldDescriptorProto_TYPE_UINT64)},
	"StatementStatus": {
		signedClaimsField("pruned", "pruned", 6, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
		signedClaimsField("checkpoint_through_block_seq", "checkpointThroughBlockSeq", 7, descriptorpb.FieldDescriptorProto_TYPE_UINT64),
	},
	"L3Block": {
		signedClaimsField("pruned", "pruned", 4, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
		signedClaimsField("checkpoint_through_block_seq", "checkpointThroughBlockSeq", 5, descriptorpb.FieldDescriptorProto_TYPE_UINT64),
	},
	"PromotionOutcome": {signedClaimsField("pruned", "pruned", 14, descriptorpb.FieldDescriptorProto_TYPE_BOOL)},
}

// stripHistoryRetentionFields removes the trailing history pruning fields of
// a message after asserting they are exactly the pinned ones, so the older
// baseline comparisons see the message as it was before.
func stripHistoryRetentionFields(t *testing.T, d *descriptorpb.DescriptorProto) {
	t.Helper()
	want, ok := historyRetentionFields[d.GetName()]
	if !ok {
		return
	}
	n := len(d.Field) - len(want)
	if n < 0 {
		t.Fatalf("%s lacks the history pruning fields: %v", d.GetName(), d)
	}
	for i, w := range want {
		if !proto.Equal(d.Field[n+i], w) {
			t.Fatalf("%s history pruning addition must be exactly %s = %d: %v", d.GetName(), w.GetName(), w.GetNumber(), d)
		}
	}
	d.Field = d.Field[:n]
}

// historyRetentionMessageFields is the field ledger of every message history
// pruning adds (the i-th token is field number i+1). Every number is a
// consensus constant: arbiter-core's strict Raft decoder refuses a field its
// descriptor does not know.
var historyRetentionMessageFields = map[string]string{
	"PruneHistoryCmd":              "through_block_seq:uint64 archive_ref:string archive_sha256:string record_set_digest:string",
	"GetHistoryCheckpointsRequest": "",
	"HistoryCheckpoint":            "through_block_seq:uint64 l3_hash:string archive_ref:string archive_sha256:string record_set_digest:string prev_checkpoint:string statements:uint64 verifications:uint64 manifests:uint64 promotions:uint64 cleanup_audits:uint64",
	"HistoryCheckpoints":           "checkpoints:[]HistoryCheckpoint",
}

func TestHistoryRetentionFieldLedgers(t *testing.T) {
	assertMessageFieldLedgers(t, map[string]protoreflect.FileDescriptor{
		"PruneHistoryCmd":              pb.File_raftlog_proto,
		"GetHistoryCheckpointsRequest": pb.File_arbiter_proto,
		"HistoryCheckpoint":            pb.File_arbiter_proto,
		"HistoryCheckpoints":           pb.File_arbiter_proto,
	}, historyRetentionMessageFields)
}

// TestHistoryRetentionFieldNumbers pins the parameter on both consensus
// messages, Raft tag 46 and the pruned flags on the reads, and that every
// unset field is off the wire, so every earlier update, params digest, Raft
// command and read encodes byte for byte as before.
func TestHistoryRetentionFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg    proto.Message
		number protoreflect.FieldNumber
	}{
		{&pb.ConsensusMutableParams{}, 22},
		{&pb.ConsensusParamsUpdate{}, 27},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			f := d.Fields().ByName("history_retain_blocks")
			if f == nil || f.Number() != tc.number || f.Kind() != protoreflect.Uint64Kind || f.IsList() || f.HasOptionalKeyword() || f.ContainingOneof() != nil {
				t.Fatalf("field = %v, want plain uint64 number %d", f, tc.number)
			}
			if got := d.Fields().Len(); got != int(tc.number) {
				t.Fatalf("field count = %d, want %d (history_retain_blocks is the last field)", got, tc.number)
			}
		})
	}
	for _, pair := range [][2]proto.Message{
		{&pb.ConsensusParamsUpdate{NetworkId: "n", CustodyFrontier: true}, &pb.ConsensusParamsUpdate{NetworkId: "n", CustodyFrontier: true, HistoryRetainBlocks: 0}},
		{&pb.ConsensusMutableParams{MaxWriters: 3}, &pb.ConsensusMutableParams{MaxWriters: 3, HistoryRetainBlocks: 0}},
		{&pb.StatementStatus{Found: true, Status: "Safe"}, &pb.StatementStatus{Found: true, Status: "Safe", Pruned: false, CheckpointThroughBlockSeq: 0}},
		{&pb.L3Block{ChainHash: "h"}, &pb.L3Block{ChainHash: "h", Pruned: false, CheckpointThroughBlockSeq: 0}},
		{&pb.PromotionOutcome{PromotionSeq: 1}, &pb.PromotionOutcome{PromotionSeq: 1, Pruned: false}},
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
			t.Fatalf("%T: an unset history pruning field must stay off the wire", pair[0])
		}
	}
	raw, err := proto.Marshal(&pb.ConsensusParamsUpdate{HistoryRetainBlocks: 64})
	if err != nil {
		t.Fatal(err)
	}
	if number, typ, n := protowire.ConsumeTag(raw); n < 0 || number != 27 || typ != protowire.VarintType {
		t.Fatalf("history_retain_blocks encoded as tag %d type %d", number, typ)
	}
	cmd := (&pb.RaftCommand{}).ProtoReflect().Descriptor().Fields().ByName("prune_history")
	if cmd == nil || cmd.Number() != 46 || cmd.ContainingOneof() == nil || cmd.Message().Name() != "PruneHistoryCmd" {
		t.Fatalf("prune_history = %v, want PruneHistoryCmd at Raft tag 46 in the command oneof", cmd)
	}
}

// TestHistoryCheckpointsRPC pins GetHistoryCheckpoints as the tenth SafeState
// method, after the methods that existed before it.
func TestHistoryCheckpointsRPC(t *testing.T) {
	svc := pb.File_arbiter_proto.Services().ByName("SafeState")
	m := svc.Methods().ByName("GetHistoryCheckpoints")
	if m == nil || m.Index() != 10 || m.Input().FullName() != "arbiter.GetHistoryCheckpointsRequest" || m.Output().FullName() != "arbiter.HistoryCheckpoints" || m.IsStreamingClient() || m.IsStreamingServer() {
		t.Fatalf("SafeState.GetHistoryCheckpoints = %v, want unary at index 10", m)
	}
	if pb.SafeState_GetHistoryCheckpoints_FullMethodName != "/arbiter.SafeState/GetHistoryCheckpoints" {
		t.Fatal("full method name drifted")
	}
}
