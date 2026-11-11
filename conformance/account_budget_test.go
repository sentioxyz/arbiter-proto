package conformance

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

func accountBudgetFieldAt(number int32) []*descriptorpb.FieldDescriptorProto {
	f := signedClaimsField("account_budget", "accountBudget", number, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE)
	f.TypeName = proto.String(".arbiter.AccountBudgetParams")
	return []*descriptorpb.FieldDescriptorProto{f}
}

// accountBudgetFields pins the field the account budget (arbiter#143 layer C)
// appends as the last field of both consensus parameter messages, after
// pending_authority_change.
var accountBudgetFields = map[string][]*descriptorpb.FieldDescriptorProto{
	"ConsensusMutableParams": accountBudgetFieldAt(25),
	"ConsensusParamsUpdate":  accountBudgetFieldAt(30),
}

// stripAccountBudgetFields removes the trailing account budget field of a
// message after asserting it is exactly the pinned one, so the older
// baseline comparisons see the message as it was before.
func stripAccountBudgetFields(t *testing.T, d *descriptorpb.DescriptorProto) {
	t.Helper()
	stripVoterChangeDelayFields(t, d)
	stripRaftMembershipFields(t, d)
	want, ok := accountBudgetFields[d.GetName()]
	if !ok {
		return
	}
	n := len(d.Field) - len(want)
	if n < 0 {
		t.Fatalf("%s lacks the account budget field: %v", d.GetName(), d)
	}
	for i, w := range want {
		if !proto.Equal(d.Field[n+i], w) {
			t.Fatalf("%s account budget addition must be exactly %s = %d: %v", d.GetName(), w.GetName(), w.GetNumber(), d)
		}
	}
	d.Field = d.Field[:n]
}

// accountBudgetMessageFields is the field ledger of the message the account
// budget adds (the i-th token is field number i+1). Every number is a
// consensus constant: arbiter-core's strict Raft decoder refuses a field its
// descriptor does not know.
var accountBudgetMessageFields = map[string]string{
	"AccountBudgetParams": "window_l2_blocks:uint64 max_statements:uint64 max_bytes:uint64 max_statements_total:uint64 max_bytes_total:uint64 enforce:bool",
}

func TestAccountBudgetFieldLedgers(t *testing.T) {
	assertMessageFieldLedgers(t, map[string]protoreflect.FileDescriptor{"AccountBudgetParams": pb.File_consensus_proto}, accountBudgetMessageFields)
}

// TestAccountBudgetFieldNumbers pins the field on both consensus messages
// and that it is off the wire while unset, so every earlier update, params
// digest and Raft command encodes byte for byte as before.
func TestAccountBudgetFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg    proto.Message
		number protoreflect.FieldNumber
	}{
		{&pb.ConsensusMutableParams{}, 25},
		{&pb.ConsensusParamsUpdate{}, 30},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			f := d.Fields().ByName("account_budget")
			if f == nil || f.Number() != tc.number || f.Kind() != protoreflect.MessageKind || f.IsList() || f.Message().Name() != "AccountBudgetParams" || f.ContainingOneof() != nil {
				t.Fatalf("account_budget field = %v, want AccountBudgetParams number %d", f, tc.number)
			}
			if got := d.Fields().Len(); got != int(tc.number)+1 {
				t.Fatalf("field count = %d, want %d (only pending_voter_change follows account_budget)", got, tc.number+1)
			}
		})
	}
	for _, pair := range [][2]proto.Message{
		{&pb.ConsensusParamsUpdate{NetworkId: "n", HistoryRetainBlocks: 5}, &pb.ConsensusParamsUpdate{NetworkId: "n", HistoryRetainBlocks: 5, AccountBudget: nil}},
		{&pb.ConsensusMutableParams{MaxWriters: 3}, &pb.ConsensusMutableParams{MaxWriters: 3, AccountBudget: nil}},
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
			t.Fatalf("%T: an unset account budget must stay off the wire", pair[0])
		}
	}
	raw, err := proto.Marshal(&pb.ConsensusParamsUpdate{AccountBudget: &pb.AccountBudgetParams{WindowL2Blocks: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if number, typ, n := protowire.ConsumeTag(raw); n < 0 || number != 30 || typ != protowire.BytesType {
		t.Fatalf("account_budget encoded as tag %d type %d", number, typ)
	}
}

// TestAccountBudgetAdmissionCode pins ACCOUNT_BUDGET_EXCEEDED by number and
// name: HouseGate maps arbiter codes through their enum name and treats an
// unknown one as an unknown outcome.
func TestAccountBudgetAdmissionCode(t *testing.T) {
	code := pb.AdmissionCode_ADMISSION_CODE_ACCOUNT_BUDGET_EXCEEDED
	if int32(code) != 14 || code.String() != "ADMISSION_CODE_ACCOUNT_BUDGET_EXCEEDED" {
		t.Fatalf("%s = %d, want ADMISSION_CODE_ACCOUNT_BUDGET_EXCEEDED = 14", code, int32(code))
	}
	if n := pb.AdmissionCode(0).Descriptor().Values().Len(); n != 15 {
		t.Fatalf("AdmissionCode has %d values, want 15", n)
	}
}
