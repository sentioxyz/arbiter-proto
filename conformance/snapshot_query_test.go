package conformance

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Exercise generated Go bindings (not just dynamic messages) with every field
// nonzero, two items per repeated field, and independent nested values.
func TestSnapshotQueryEveryFieldWireRoundTrip(t *testing.T) {
	for name, spec := range snapshotQueryFields {
		t.Run(name, func(t *testing.T) {
			mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName("arbiter." + name))
			if err != nil {
				t.Fatal(err)
			}
			in := mt.New()
			fields := in.Descriptor().Fields()
			want := strings.Fields(spec)
			if fields.Len() != len(want) {
				t.Fatalf("field count = %d, want %d", fields.Len(), len(want))
			}
			for i, item := range want {
				pair := strings.Split(item, ":")
				fd := fields.ByName(protoreflect.Name(pair[0]))
				repeated := strings.HasPrefix(pair[1], "[]")
				typ := strings.TrimPrefix(pair[1], "[]")
				if fd == nil {
					t.Fatalf("missing field %s", pair[0])
				}
				if fd.Number() != protoreflect.FieldNumber(i+1) || fd.IsList() != repeated {
					t.Fatalf("%s allocation changed: %v", pair[0], fd)
				}
				got := fd.Kind().String()
				if fd.Message() != nil {
					got = string(fd.Message().Name())
				}
				if got != typ {
					t.Fatalf("%s type=%s, want %s", pair[0], got, typ)
				}
				populateSnapshotField(in, fd, uint64(i+1))
				// Isolate each field to verify its actual encoded tag and value. A field
				// accidentally omitted by a binding cannot hide behind another field.
				single := mt.New()
				populateSnapshotField(single, fd, uint64(i+1))
				raw, err := proto.Marshal(single.Interface())
				if err != nil {
					t.Fatal(err)
				}
				number, _, n := protowire.ConsumeTag(raw)
				if n < 0 || number != protowire.Number(i+1) {
					t.Fatalf("%s encoded tag=%d", pair[0], number)
				}
				restored := mt.New()
				if err := proto.Unmarshal(raw, restored.Interface()); err != nil {
					t.Fatal(err)
				}
				if !proto.Equal(single.Interface(), restored.Interface()) {
					t.Fatalf("%s single-field round trip lost data", pair[0])
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

func populateSnapshotField(m protoreflect.Message, fd protoreflect.FieldDescriptor, seed uint64) {
	if fd.IsList() {
		list := m.Mutable(fd).List()
		for i := uint64(0); i < 2; i++ {
			list.Append(snapshotValue(fd, list.NewElement(), seed+i))
		}
		return
	}
	m.Set(fd, snapshotValue(fd, m.NewField(fd), seed))
}

func snapshotValue(fd protoreflect.FieldDescriptor, value protoreflect.Value, seed uint64) protoreflect.Value {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(fmt.Sprintf("%s-%d", fd.Name(), seed))
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte{0, byte(seed), 255})
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true)
	case protoreflect.Uint32Kind:
		return protoreflect.ValueOfUint32(uint32(seed + 54470))
	case protoreflect.Uint64Kind:
		return protoreflect.ValueOfUint64(seed + (1 << 54))
	case protoreflect.EnumKind:
		return protoreflect.ValueOfEnum(fd.Enum().Values().Get(fd.Enum().Values().Len() - 1).Number())
	case protoreflect.MessageKind:
		nested := value.Message()
		fields := nested.Descriptor().Fields()
		for i := 0; i < fields.Len(); i++ {
			populateSnapshotField(nested, fields.Get(i), seed+uint64(i)+1)
		}
		return value
	default:
		panic(fmt.Sprintf("unsupported fixture kind %v", fd.Kind()))
	}
}

func TestSnapshotQueryReservationStatesAndTombstones(t *testing.T) {
	reservation := &pb.SnapshotQueryReservation{ReservationId: "reservation-8", FencingGeneration: 8, ClientAccount: "account", StatementId: "account:2:nonce", ReadSnapshot: &pb.SnapshotPin{NetworkId: "test", KeeperShardId: 3, SnapshotId: "safe-7", SafeBlockSeq: 7, ManifestRoot: "manifest", StateRoot: "state", SchemaSnapshotId: "schema", SchemaRoot: "schema-root"}, ExecutorProfileId: "executor", QueryProfileId: "query", ActivationId: "activation"}
	cases := []struct {
		name   string
		status *pb.SnapshotQueryReservationStatus
	}{
		{"authenticated absent", &pb.SnapshotQueryReservationStatus{Version: 1, RequestId: "absent", ClientAccount: "account", StatementId: "account:2:nonce"}},
		{"draining lost acquire response", &pb.SnapshotQueryReservationStatus{Version: 1, Found: true, State: "draining", RequestId: "request", ClientAccount: "account", StatementId: "account:2:nonce", FencingGeneration: 7}},
		{"granted lost acquire response", &pb.SnapshotQueryReservationStatus{Version: 1, Found: true, State: "granted", RequestId: "request", ClientAccount: "account", StatementId: "account:2:nonce", FencingGeneration: 8, Reservation: reservation}},
		{"consumed", &pb.SnapshotQueryReservationStatus{Version: 1, Found: true, State: "consumed", RequestId: "request", ClientAccount: "account", StatementId: "account:2:nonce", FencingGeneration: 8, Reservation: reservation, BlockSeq: 9}},
		{"released lost release response", &pb.SnapshotQueryReservationStatus{Version: 1, Found: true, State: "released", RequestId: "request", ClientAccount: "account", StatementId: "account:2:nonce", FencingGeneration: 9, Reservation: reservation, TerminalProof: []byte("release-proof")}},
		{"absent request cancellation tombstone", &pb.SnapshotQueryReservationStatus{Version: 1, Found: true, State: "released", RequestId: "late-acquire", ClientAccount: "account", StatementId: "account:2:nonce", FencingGeneration: 1, TerminalProof: []byte("cancellation-proof")}},
		{"terminal consumed tombstone", &pb.SnapshotQueryReservationStatus{Version: 1, Found: true, State: "released", RequestId: "request", ClientAccount: "account", StatementId: "account:2:nonce", FencingGeneration: 9, Reservation: reservation, BlockSeq: 9, TerminalProof: []byte("terminal-authority")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := proto.Marshal(tc.status)
			if err != nil {
				t.Fatal(err)
			}
			out := &pb.SnapshotQueryReservationStatus{}
			if err := proto.Unmarshal(raw, out); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(tc.status, out) {
				t.Fatalf("status changed: %v", out)
			}
			if (tc.status.Reservation == nil) != (out.Reservation == nil) {
				t.Fatal("reservation presence changed")
			}
		})
	}
}

func TestSnapshotQueryPresenceAndEmptyCollections(t *testing.T) {
	// Absent and present-empty submessages remain distinguishable on the wire;
	// proto3 repeated nil/empty equivalence is deliberately NOT canonical JSON.
	for _, in := range []proto.Message{&pb.SnapshotQueryJob{}, &pb.SnapshotQueryJob{SourceClaim: &pb.SnapshotQueryClaim{}}, &pb.SnapshotQueryStatus{}, &pb.SnapshotQueryStatus{Accepted: &pb.SnapshotQuerySubmitResult{}}, &pb.SnapshotQueryReservationStatus{}, &pb.SnapshotQueryReservationStatus{Reservation: &pb.SnapshotQueryReservation{}}} {
		raw, err := proto.Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		out := in.ProtoReflect().Type().New().Interface()
		if err := proto.Unmarshal(raw, out); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(in, out) {
			t.Fatalf("presence lost: %v", in)
		}
	}
	for _, pair := range [][2]proto.Message{
		{&pb.SnapshotReadSet{}, &pb.SnapshotReadSet{Tables: []*pb.SnapshotReadTable{}}},
		{&pb.SnapshotQueryClaim{}, &pb.SnapshotQueryClaim{PartitionDeltas: []*pb.PartitionCommitment{}, PartitionCommitmentsAfter: []*pb.PartitionCommitment{}, CandidateParts: []*pb.SnapshotReadPart{}}},
		{&pb.ReplayJob{}, &pb.ReplayJob{Statements: []*pb.Statement{}}},
	} {
		a, e := proto.Marshal(pair[0])
		if e != nil {
			t.Fatal(e)
		}
		b, e := proto.Marshal(pair[1])
		if e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(a, b) {
			t.Fatal("proto3 nil/empty wire behavior changed")
		}
	}
	if (&pb.SnapshotReadPart{}).ProtoReflect().Descriptor().Fields().ByName("storage_refs") != nil {
		t.Fatal("read part leaked transport hints")
	}
}

func legacySnapshotDescriptors(t *testing.T) *descriptorpb.FileDescriptorSet {
	t.Helper()
	return snapshotDescriptorFixture(t, "pre_snapshot_query_descriptor.binpb")
}

func snapshotDescriptorFixture(t *testing.T, name string) *descriptorpb.FileDescriptorSet {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(arbiterProtoModuleRoot(t), "conformance/testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	set := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(raw, set); err != nil {
		t.Fatal(err)
	}
	return set
}

func TestSnapshotQueryOldDescriptorsUnchanged(t *testing.T) {
	assertSnapshotBaselineDescriptors(t, legacySnapshotDescriptors(t), true)
}

func TestSnapshotQueryMainDescriptorsUnchanged(t *testing.T) {
	assertSnapshotBaselineDescriptors(t, snapshotDescriptorFixture(t, "main_f7d9f070_descriptor.binpb"), false)
}

// artifactDispositionCapabilityFieldNumbers pins the exact field number the
// Wave 1a-3-c1 governance switch (uint32 artifact_disposition_capability)
// occupies on each message it was appended to. Every existing baseline
// predates this addition, so the exemption below applies unconditionally,
// unlike the PromotionAck allowance which is specific to one baseline.
var artifactDispositionCapabilityFieldNumbers = map[string]int32{
	"ConsensusMutableParams": 3,
	"ConsensusParamsUpdate":  8,
}

// tableRegistryFieldNumbers pins the exact field number the dynamic SI table
// registry parameter (TableRegistryParams table_registry) occupies on each
// message it was appended to. It is the last field on both messages — after
// artifact_disposition_capability — so it is stripped before that exemption
// runs below. Every existing baseline predates this addition, so the
// exemption applies unconditionally, same as artifact_disposition_capability.
var tableRegistryFieldNumbers = map[string]int32{
	"ConsensusMutableParams": 4,
	"ConsensusParamsUpdate":  9,
}

func assertSnapshotBaselineDescriptors(t *testing.T, baseline *descriptorpb.FileDescriptorSet, allowPromotionInventory bool) {
	t.Helper()
	for _, old := range baseline.File {
		current, err := protoregistry.GlobalFiles.FindFileByPath(old.GetName())
		if err != nil {
			t.Fatal(err)
		}
		now := protodesc.ToFileDescriptorProto(current)
		for _, m := range old.MessageType {
			var got *descriptorpb.DescriptorProto
			for _, n := range now.MessageType {
				if n.GetName() == m.GetName() {
					got = proto.Clone(n).(*descriptorpb.DescriptorProto)
				}
			}
			if got == nil {
				t.Fatalf("old message removed: %s", m.GetName())
			}
			if m.GetName() == "RaftCommand" || m.GetName() == "VerifierDispatch" {
				if len(got.Field) < len(m.Field) {
					t.Fatalf("fields removed: %s", m.GetName())
				}
				got.Field = got.Field[:len(m.Field)]
			}
			if allowPromotionInventory && m.GetName() == "PromotionAck" {
				want := &descriptorpb.FieldDescriptorProto{
					Name: proto.String("safe_partition_parts"), JsonName: proto.String("safePartitionParts"), Number: proto.Int32(9),
					Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".arbiter.SafePartMapping"),
				}
				if len(got.Field) != len(m.Field)+1 || !proto.Equal(got.Field[len(m.Field)], want) {
					t.Fatalf("PromotionAck addition must be exactly repeated SafePartMapping safe_partition_parts = 9: %v", got)
				}
				got.Field = got.Field[:len(m.Field)]
			}
			if num, ok := tableRegistryFieldNumbers[m.GetName()]; ok {
				if len(got.Field) == 0 {
					t.Fatalf("%s missing fields entirely", m.GetName())
				}
				last := got.Field[len(got.Field)-1]
				want := &descriptorpb.FieldDescriptorProto{
					Name: proto.String("table_registry"), JsonName: proto.String("tableRegistry"), Number: proto.Int32(num),
					Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".arbiter.TableRegistryParams"),
				}
				if last.GetNumber() != num || !proto.Equal(last, want) {
					t.Fatalf("%s addition must be exactly TableRegistryParams table_registry = %d: %v", m.GetName(), num, got)
				}
				got.Field = got.Field[:len(got.Field)-1]
			}
			if num, ok := artifactDispositionCapabilityFieldNumbers[m.GetName()]; ok {
				want := &descriptorpb.FieldDescriptorProto{
					Name: proto.String("artifact_disposition_capability"), JsonName: proto.String("artifactDispositionCapability"), Number: proto.Int32(num),
					Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_UINT32.Enum(),
				}
				if len(got.Field) != len(m.Field)+1 || !proto.Equal(got.Field[len(m.Field)], want) {
					t.Fatalf("%s addition must be exactly uint32 artifact_disposition_capability = %d: %v", m.GetName(), num, got)
				}
				got.Field = got.Field[:len(m.Field)]
			}
			if !proto.Equal(m, got) {
				t.Errorf("old descriptor changed: %s", m.GetName())
			}
		}
		for _, enum := range old.EnumType {
			var got *descriptorpb.EnumDescriptorProto
			for _, n := range now.EnumType {
				if n.GetName() == enum.GetName() {
					got = proto.Clone(n).(*descriptorpb.EnumDescriptorProto)
				}
			}
			if got == nil {
				t.Fatal("old enum removed")
			}
			if len(got.Value) < len(enum.Value) {
				t.Fatalf("enum values removed: %s", enum.GetName())
			}
			got.Value = got.Value[:len(enum.Value)]
			if !proto.Equal(enum, got) {
				t.Errorf("old enum changed: %s", enum.GetName())
			}
		}
		for _, svc := range old.Service {
			var got *descriptorpb.ServiceDescriptorProto
			for _, n := range now.Service {
				if n.GetName() == svc.GetName() {
					got = n
				}
			}
			if got == nil {
				t.Fatal("old service removed")
			}
			for _, method := range svc.Method {
				var found bool
				for _, n := range got.Method {
					if n.GetName() == method.GetName() {
						found = proto.Equal(method, n)
					}
				}
				if !found {
					t.Errorf("old method changed: %s.%s", svc.GetName(), method.GetName())
				}
			}
		}
	}
}

func TestSnapshotQueryUnknownVariantsStayUnknownToOldReaders(t *testing.T) {
	for _, fixture := range []string{"pre_snapshot_query_descriptor.binpb", "main_f7d9f070_descriptor.binpb"} {
		t.Run(fixture, func(t *testing.T) { assertSnapshotUnknownVariants(t, snapshotDescriptorFixture(t, fixture)) })
	}
}

func assertSnapshotUnknownVariants(t *testing.T, baseline *descriptorpb.FileDescriptorSet) {
	t.Helper()
	files, err := protodesc.NewFiles(baseline)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		numbers []protowire.Number
		oneof   protoreflect.Name
	}{{"RaftCommand", []protowire.Number{30, 19, 20, 21, 22, 23, 24, 25, 26, 27}, "cmd"}, {"VerifierDispatch", []protowire.Number{3}, "dispatch"}} {
		desc, err := files.FindDescriptorByName(protoreflect.FullName("arbiter." + tc.name))
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range tc.numbers {
			// A future variant must never become an old, apparently valid command.
			raw := protowire.AppendTag(nil, n, protowire.BytesType)
			raw = protowire.AppendBytes(raw, []byte{10, 1, 'x'})
			out := dynamicpb.NewMessage(desc.(protoreflect.MessageDescriptor))
			if err := proto.Unmarshal(raw, out); err != nil {
				t.Fatal(err)
			}
			if out.WhichOneof(out.Descriptor().Oneofs().ByName(tc.oneof)) != nil {
				t.Fatalf("tag %d selected an old variant", n)
			}
			if !bytes.Equal(raw, out.GetUnknown()) {
				t.Fatal("unknown variant not retained")
			}
			round, err := proto.Marshal(out)
			if err != nil || !bytes.Equal(raw, round) {
				t.Fatalf("unknown variant bytes changed: %v", err)
			}
		}
	}
	oldDesc, err := files.FindDescriptorByName("arbiter.StatementEnvelopeV2")
	if err != nil {
		t.Fatal(err)
	}
	in := &pb.StatementEnvelopeV2{StatementKind: pb.StatementKind_STATEMENT_KIND_SNAPSHOT_QUERY}
	raw, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	old := dynamicpb.NewMessage(oldDesc.(protoreflect.MessageDescriptor))
	if err := proto.Unmarshal(raw, old); err != nil {
		t.Fatal(err)
	}
	fd := old.Descriptor().Fields().ByName("statement_kind")
	got := old.Get(fd).Enum()
	if got != 2 || fd.Enum().Values().ByNumber(got) != nil {
		t.Fatal("new kind became an admitted old enum variant")
	}
	// Runtime admission rejection remains owned by AC/AR; AP only freezes the
	// descriptor boundary that leaves unknown variants outside their old switch.
}

// Baseline descriptor bytes were exported with protodesc at AP commit
// 19d90fc4bd6194a26656dd1a2cf66bb911247d73 before adding any new schema.
// Coverage must grow with the schema, including transport-only wrappers.
func TestSnapshotQueryLedgerCoversEveryNewMessage(t *testing.T) {
	oldNames := map[string]bool{}
	for _, file := range legacySnapshotDescriptors(t).File {
		for _, m := range file.MessageType {
			oldNames[m.GetName()] = true
		}
	}
	for _, file := range []protoreflect.FileDescriptor{pb.File_arbiter_proto, pb.File_replay_proto, pb.File_raftlog_proto} {
		messages := file.Messages()
		for i := 0; i < messages.Len(); i++ {
			name := string(messages.Get(i).Name())
			if !oldNames[name] {
				if _, ok := snapshotQueryFields[name]; !ok {
					t.Errorf("new message %s lacks every-field wire coverage", name)
				}
			}
		}
	}
}

func TestSnapshotQueryLegacyWireRoundTrips(t *testing.T) {
	baseline := legacySnapshotDescriptors(t)
	files, err := protodesc.NewFiles(baseline)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range baseline.File {
		for _, record := range file.MessageType {
			t.Run(record.GetName(), func(t *testing.T) {
				name := protoreflect.FullName("arbiter." + record.GetName())
				desc, err := files.FindDescriptorByName(name)
				if err != nil {
					t.Fatal(err)
				}
				old := dynamicpb.NewMessage(desc.(protoreflect.MessageDescriptor))
				fields := old.Descriptor().Fields()
				for i := 0; i < fields.Len(); i++ {
					populateSnapshotField(old, fields.Get(i), uint64(i+1))
				}
				want, err := (proto.MarshalOptions{Deterministic: true}).Marshal(old)
				if err != nil {
					t.Fatal(err)
				}
				mt, err := protoregistry.GlobalTypes.FindMessageByName(name)
				if err != nil {
					t.Fatal(err)
				}
				current := mt.New()
				if err := proto.Unmarshal(want, current.Interface()); err != nil {
					t.Fatal(err)
				}
				got, err := (proto.MarshalOptions{Deterministic: true}).Marshal(current.Interface())
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(want, got) {
					t.Fatal("legacy wire bytes changed through new generated binding")
				}
			})
		}
	}
}
