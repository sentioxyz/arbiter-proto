package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestTableRegistryReadContract pins the sub-project 2c read surface: the
// TableRegistry service, its snapshot messages (field names equal arbiter
// fsm json tags, which arbiter's server mirror test asserts), the two enums,
// and the L3BlockHeader wire form of the transition header.
func TestTableRegistryReadContract(t *testing.T) {
	type field struct {
		name     protoreflect.Name
		number   protoreflect.FieldNumber
		kind     protoreflect.Kind
		repeated bool
	}
	for _, tc := range []struct {
		message proto.Message
		fields  []field
	}{
		{&pb.TableRegistryCursor{}, []field{{"block_number", 1, protoreflect.Uint64Kind, false}, {"block_hash", 2, protoreflect.StringKind, false}, {"log_index", 3, protoreflect.Uint64Kind, false}, {"block_complete", 4, protoreflect.BoolKind, false}}},
		{&pb.TableIncarnation{}, []field{
			{"seq", 1, protoreflect.Uint64Kind, false}, {"database_id", 2, protoreflect.StringKind, false}, {"table_id", 3, protoreflect.StringKind, false},
			{"origin", 4, protoreflect.EnumKind, false}, {"status", 5, protoreflect.EnumKind, false}, {"created", 6, protoreflect.MessageKind, false},
			{"schema_ref", 7, protoreflect.MessageKind, false}, {"schema_version", 8, protoreflect.Uint32Kind, false}, {"schema_hash", 9, protoreflect.StringKind, false},
			{"schema_json", 10, protoreflect.StringKind, false}, {"refused_reason", 11, protoreflect.StringKind, false}, {"refused_code", 12, protoreflect.StringKind, false},
			{"deleted", 13, protoreflect.MessageKind, false}, {"retire_reason", 14, protoreflect.EnumKind, false}, {"add_block_seq", 15, protoreflect.Uint64Kind, false},
			{"retire_block_seq", 16, protoreflect.Uint64Kind, false}, {"purged_by", 17, protoreflect.StringKind, true},
		}},
		{&pb.TableRegistrySnapshot{}, []field{{"params", 1, protoreflect.MessageKind, false}, {"version", 2, protoreflect.Uint64Kind, false}, {"seeded", 3, protoreflect.BoolKind, false}, {"cursor", 4, protoreflect.MessageKind, false}, {"incarnations", 5, protoreflect.MessageKind, true}}},
		{&pb.WatchTableRegistryRequest{}, []field{{"since_version", 1, protoreflect.Uint64Kind, false}}},
		{&pb.TableSetAdd{}, []field{{"table_id", 1, protoreflect.StringKind, false}, {"schema_hash", 2, protoreflect.StringKind, false}}},
		{&pb.TableSetTransition{}, []field{{"adds", 1, protoreflect.MessageKind, true}, {"retires", 2, protoreflect.StringKind, true}, {"new_schema_root", 3, protoreflect.StringKind, false}}},
	} {
		d := tc.message.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			if d.Fields().Len() != len(tc.fields) {
				t.Fatalf("field count = %d, want %d", d.Fields().Len(), len(tc.fields))
			}
			for _, want := range tc.fields {
				f := d.Fields().ByName(want.name)
				if f == nil || f.Number() != want.number || f.Kind() != want.kind || f.IsList() != want.repeated {
					t.Fatalf("field %s = %v, want number %d kind %s repeated %v", want.name, f, want.number, want.kind, want.repeated)
				}
			}
		})
	}
	inc := (&pb.TableIncarnation{}).ProtoReflect().Descriptor().Fields()
	for name, full := range map[protoreflect.Name]protoreflect.FullName{"created": "arbiter.L2EventRef", "schema_ref": "arbiter.L2EventRef", "deleted": "arbiter.L2EventRef"} {
		if got := inc.ByName(name).Message().FullName(); got != full {
			t.Fatalf("TableIncarnation.%s type = %s, want %s", name, got, full)
		}
	}
	if got := inc.ByName("retire_reason").Enum().FullName(); got != "arbiter.TableRetireReason" {
		t.Fatalf("retire_reason enum = %s", got)
	}
	for enum, names := range map[protoreflect.EnumDescriptor][]protoreflect.Name{
		pb.TableIncarnationStatus(0).Descriptor(): {"TABLE_INCARNATION_STATUS_UNSPECIFIED", "TABLE_INCARNATION_STATUS_LEGACY", "TABLE_INCARNATION_STATUS_PENDING", "TABLE_INCARNATION_STATUS_REFUSED", "TABLE_INCARNATION_STATUS_ACTIVE", "TABLE_INCARNATION_STATUS_RETIRING", "TABLE_INCARNATION_STATUS_PURGING", "TABLE_INCARNATION_STATUS_PURGED"},
		pb.TableIncarnationOrigin(0).Descriptor(): {"TABLE_INCARNATION_ORIGIN_UNSPECIFIED", "TABLE_INCARNATION_ORIGIN_GENESIS", "TABLE_INCARNATION_ORIGIN_LEGACY", "TABLE_INCARNATION_ORIGIN_CHAIN"},
	} {
		if enum.Values().Len() != len(names) {
			t.Fatalf("%s has %d values, want %d", enum.FullName(), enum.Values().Len(), len(names))
		}
		for i, name := range names {
			if v := enum.Values().ByNumber(protoreflect.EnumNumber(i)); v == nil || v.Name() != name {
				t.Fatalf("%s value %d = %v, want %s", enum.FullName(), i, v, name)
			}
		}
	}
	header := (&pb.L3BlockHeader{}).ProtoReflect().Descriptor().Fields()
	if f := header.ByNumber(12); f == nil || f.Name() != "query_statement_root" || f.Kind() != protoreflect.StringKind {
		t.Fatalf("L3BlockHeader field 12 = %v", f)
	}
	if f := header.ByNumber(13); f == nil || f.Name() != "table_set_transition" || f.Message().FullName() != "arbiter.TableSetTransition" {
		t.Fatalf("L3BlockHeader field 13 = %v", f)
	}
	svc := pb.File_table_registry_proto.Services().ByName("TableRegistry")
	if svc == nil || svc.Methods().Len() != 2 {
		t.Fatalf("TableRegistry service = %v", svc)
	}
	for _, want := range []struct {
		name, input, output protoreflect.FullName
		stream              bool
	}{
		{"GetTableRegistry", "google.protobuf.Empty", "arbiter.TableRegistrySnapshot", false},
		{"WatchTableRegistry", "arbiter.WatchTableRegistryRequest", "arbiter.TableRegistrySnapshot", true},
	} {
		m := svc.Methods().ByName(protoreflect.Name(want.name))
		if m == nil || m.Input().FullName() != want.input || m.Output().FullName() != want.output || m.IsStreamingServer() != want.stream || m.IsStreamingClient() {
			t.Fatalf("method %s = %v", want.name, m)
		}
	}
}
