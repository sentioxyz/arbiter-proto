package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestTableRegistryContract(t *testing.T) {
	type field struct {
		name     protoreflect.Name
		kind     protoreflect.Kind
		repeated bool
	}
	tests := []struct {
		message proto.Message
		fields  []field
	}{
		{&pb.TableRegistryParams{}, []field{{"chain_id", protoreflect.Uint64Kind, false}, {"databases_contract", protoreflect.StringKind, false}, {"si_indexer_id", protoreflect.Uint64Kind, false}, {"activation_block", protoreflect.Uint64Kind, false}, {"confirmation", protoreflect.StringKind, false}}},
		{&pb.L2BlockRef{}, []field{{"number", protoreflect.Uint64Kind, false}, {"hash", protoreflect.StringKind, false}}},
		{&pb.L2EventRef{}, []field{{"block_number", protoreflect.Uint64Kind, false}, {"block_hash", protoreflect.StringKind, false}, {"log_index", protoreflect.Uint64Kind, false}, {"tx_hash", protoreflect.StringKind, false}}},
		{&pb.LegacyTable{}, []field{{"database_id", protoreflect.StringKind, false}, {"table_id", protoreflect.StringKind, false}, {"created", protoreflect.MessageKind, false}}},
		{&pb.SeedLegacyTablesCmd{}, []field{{"at_block", protoreflect.MessageKind, false}, {"tables", protoreflect.MessageKind, true}}},
		{&pb.AddTableCmd{}, []field{{"database_id", protoreflect.StringKind, false}, {"table_id", protoreflect.StringKind, false}, {"created", protoreflect.MessageKind, false}, {"schema", protoreflect.MessageKind, false}, {"schema_version", protoreflect.Uint32Kind, false}, {"schema_hash", protoreflect.StringKind, false}, {"schema_json", protoreflect.StringKind, false}}},
		{&pb.RetireTablesCmd{}, []field{{"database_id", protoreflect.StringKind, false}, {"table_ids", protoreflect.StringKind, true}, {"deleted", protoreflect.MessageKind, false}, {"reason", protoreflect.EnumKind, false}}},
		{&pb.AdvanceL2CursorCmd{}, []field{{"to", protoreflect.MessageKind, false}}},
		{&pb.RecordTablePurgedCmd{}, []field{{"node_id", protoreflect.StringKind, false}, {"incarnation_seq", protoreflect.Uint64Kind, false}}},
	}
	for _, tt := range tests {
		d := tt.message.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			if d.Fields().Len() != len(tt.fields) {
				t.Fatalf("field count = %d, want %d", d.Fields().Len(), len(tt.fields))
			}
			for i, want := range tt.fields {
				f := d.Fields().ByName(want.name)
				if f == nil || f.Number() != protoreflect.FieldNumber(i+1) || f.Kind() != want.kind || f.IsList() != want.repeated {
					t.Fatalf("field %s = %v, want number %d kind %s repeated %v", want.name, f, i+1, want.kind, want.repeated)
				}
			}
		})
	}
	reasons := pb.TableRetireReason(0).Descriptor().Values()
	if reasons.Len() != 3 || reasons.ByNumber(1).Name() != "TABLE_RETIRE_REASON_TABLE_DELETED" || reasons.ByNumber(2).Name() != "TABLE_RETIRE_REASON_DATABASE_DELETED" {
		t.Fatalf("TableRetireReason values drifted")
	}
	cmd := (&pb.RaftCommand{}).ProtoReflect().Descriptor().Oneofs().ByName("cmd").Fields()
	for number, name := range map[protoreflect.FieldNumber]protoreflect.Name{31: "seed_legacy_tables", 32: "add_table", 33: "retire_tables", 34: "advance_l2_cursor", 35: "record_table_purged"} {
		if f := cmd.ByNumber(number); f == nil || f.Name() != name {
			t.Fatalf("RaftCommand tag %d = %v, want %s", number, f, name)
		}
	}
	if f := cmd.ByNumber(29); f != nil {
		t.Fatalf("RaftCommand tag 29 must stay unused, got %s", f.Name())
	}
}
