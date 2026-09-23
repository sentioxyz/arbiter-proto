package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestReplayTableSetContract pins the dynamic SI table-set additions to the
// replay job. Field names are frozen against housegate pkg/replay JSON tags
// (arbiter-core conformance/replay_wire_test.go asserts the other half).
func TestReplayTableSetContract(t *testing.T) {
	type field struct {
		name     protoreflect.Name
		number   protoreflect.FieldNumber
		kind     protoreflect.Kind
		repeated bool
	}
	for _, tc := range []struct {
		message proto.Message
		count   int
		fields  []field
	}{
		{&pb.ReplayTableSchema{}, 2, []field{{"table_id", 1, protoreflect.StringKind, false}, {"schema_json", 2, protoreflect.StringKind, false}}},
		{&pb.ReplayTableSetTransition{}, 3, []field{{"adds", 1, protoreflect.MessageKind, true}, {"retires", 2, protoreflect.StringKind, true}, {"new_schema_root", 3, protoreflect.StringKind, false}}},
		{&pb.ReplayJob{}, 9, []field{{"table_set_transition", 8, protoreflect.MessageKind, false}, {"table_schemas", 9, protoreflect.MessageKind, true}}},
	} {
		d := tc.message.ProtoReflect().Descriptor()
		t.Run(string(d.Name()), func(t *testing.T) {
			if d.Fields().Len() != tc.count {
				t.Fatalf("field count = %d, want %d", d.Fields().Len(), tc.count)
			}
			for _, want := range tc.fields {
				f := d.Fields().ByName(want.name)
				if f == nil || f.Number() != want.number || f.Kind() != want.kind || f.IsList() != want.repeated {
					t.Fatalf("field %s = %v, want number %d kind %s repeated %v", want.name, f, want.number, want.kind, want.repeated)
				}
			}
		})
	}
	job := (&pb.ReplayJob{}).ProtoReflect().Descriptor().Fields()
	if got := job.ByName("table_set_transition").Message().FullName(); got != "arbiter.ReplayTableSetTransition" {
		t.Fatalf("table_set_transition type = %s", got)
	}
	if got := job.ByName("table_schemas").Message().FullName(); got != "arbiter.ReplayTableSchema" {
		t.Fatalf("table_schemas type = %s", got)
	}
}
