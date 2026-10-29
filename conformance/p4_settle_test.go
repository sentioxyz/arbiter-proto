package conformance

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

// settleFields is the frozen field ledger of the three messages of the P4
// track settle (housegate spec 2026-10-14 §14 "Exits from the release
// refusals", §16 P4; Plan S3-D contract §33.2), in the stage3Fields style:
// the i-th token is field number i+1, "[]T" is repeated, and T is the scalar
// kind, the message name or the enum name. All three live in consensus.proto,
// beside the ConsensusAdmin service that carries them.
var settleFields = map[string]string{
	"SettleTrackRequest": "indexer_id:uint64 expected_generation:uint64 promotion_seqs:[]uint64 authority_jws:string timeout_ms:uint64",
	"TrackSettleRef":     "indexer_id:uint64",
	"TrackSettle":        "indexer_id:uint64 quarantined:bool quarantine_generation:uint64 unacked_promotion_seqs:[]uint64 state:TrackSettleState promotion_seqs:[]uint64 raft_term:uint64 expires_unix_ms:uint64 snode_node_id:string snode_settle_capable:bool sends:uint64",
}

// TestSettleFieldNumbers pins every number of the P4 settle surface: each
// field of SettleTrackRequest, TrackSettleRef and TrackSettle (number,
// contiguous from 1 in ledger order, cardinality and type, none proto3
// optional, in a oneof or a map) with the every-field wire round trip of
// TestStage3FieldLedgers; the six TrackSettleState values; and the two
// stream fields, PromotionCommand.settle = 3 (outside the cmd oneof, before
// authority_jws = 15) and SNodeHello.settle = 2. Both stream fields are
// absent on the wire when false, so a stream without a settle is
// byte-identical to the one an older leader or SNode sends, and a reader
// whose descriptor predates them keeps them as unknown fields.
func TestSettleFieldNumbers(t *testing.T) {
	names := make([]string, 0, len(settleFields))
	for name := range settleFields {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		spec := settleFields[name]
		t.Run(name, func(t *testing.T) {
			md := pb.File_consensus_proto.Messages().ByName(protoreflect.Name(name))
			if md == nil {
				t.Fatalf("%s is not defined in consensus.proto", name)
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

	t.Run("TrackSettleState", func(t *testing.T) {
		if pb.File_consensus_proto.Enums().ByName("TrackSettleState") == nil {
			t.Fatal("TrackSettleState is not defined in consensus.proto")
		}
		values := pb.TrackSettleState(0).Descriptor().Values()
		if values.Len() != 6 {
			t.Fatalf("TrackSettleState has %d values, want 6", values.Len())
		}
		for _, tc := range []struct {
			state pb.TrackSettleState
			num   protoreflect.EnumNumber
			name  string
		}{
			{pb.TrackSettleState_TRACK_SETTLE_STATE_UNSPECIFIED, 0, "TRACK_SETTLE_STATE_UNSPECIFIED"},
			{pb.TrackSettleState_TRACK_SETTLE_STATE_ACTIVE, 1, "TRACK_SETTLE_STATE_ACTIVE"},
			{pb.TrackSettleState_TRACK_SETTLE_STATE_SETTLED, 2, "TRACK_SETTLE_STATE_SETTLED"},
			{pb.TrackSettleState_TRACK_SETTLE_STATE_RELEASED, 3, "TRACK_SETTLE_STATE_RELEASED"},
			{pb.TrackSettleState_TRACK_SETTLE_STATE_EXPIRED, 4, "TRACK_SETTLE_STATE_EXPIRED"},
			{pb.TrackSettleState_TRACK_SETTLE_STATE_LEADERSHIP_LOST, 5, "TRACK_SETTLE_STATE_LEADERSHIP_LOST"},
		} {
			if tc.state.Number() != tc.num || tc.state.String() != tc.name || values.Get(int(tc.num)).Name() != protoreflect.Name(tc.name) {
				t.Fatalf("%s = %d, want %s = %d", tc.state, tc.state.Number(), tc.name, tc.num)
			}
		}
	})

	t.Run("StreamFields", func(t *testing.T) {
		for _, tc := range []struct {
			message proto.Message
			names   []protoreflect.Name
			numbers []protoreflect.FieldNumber
			kinds   []protoreflect.Kind
			oneof   []bool
		}{
			{&pb.PromotionCommand{},
				[]protoreflect.Name{"promote", "cleanup", "settle", "authority_jws"},
				[]protoreflect.FieldNumber{1, 2, 3, 15},
				[]protoreflect.Kind{protoreflect.MessageKind, protoreflect.MessageKind, protoreflect.BoolKind, protoreflect.StringKind},
				[]bool{true, true, false, false}},
			{&pb.SNodeHello{},
				[]protoreflect.Name{"node_id", "settle"},
				[]protoreflect.FieldNumber{1, 2},
				[]protoreflect.Kind{protoreflect.StringKind, protoreflect.BoolKind},
				[]bool{false, false}},
		} {
			md := tc.message.ProtoReflect().Descriptor()
			if md.ParentFile() != pb.File_arbiter_proto {
				t.Fatalf("%s is not defined in arbiter.proto", md.Name())
			}
			if md.Fields().Len() != len(tc.names) {
				t.Fatalf("%s has %d fields, want %d", md.Name(), md.Fields().Len(), len(tc.names))
			}
			for i, name := range tc.names {
				fd := md.Fields().Get(i)
				if fd.Name() != name || fd.Number() != tc.numbers[i] || fd.Kind() != tc.kinds[i] || fd.IsList() || fd.IsMap() || fd.HasOptionalKeyword() || (fd.ContainingOneof() != nil) != tc.oneof[i] {
					t.Fatalf("%s field %d = %v, want %s = %d", md.Name(), i, fd, name, tc.numbers[i])
				}
			}
		}
		if cmd := (&pb.PromotionCommand{}).ProtoReflect().Descriptor().Oneofs(); cmd.Len() != 1 || cmd.Get(0).Name() != "cmd" || cmd.Get(0).Fields().Len() != 2 {
			t.Fatalf("PromotionCommand oneofs changed: %v", cmd)
		}
		// The generated getters Plan S3-D's Tasks 50 and 51 call, checked by the
		// compiler.
		var (
			_ func(*pb.PromotionCommand) bool = (*pb.PromotionCommand).GetSettle
			_ func(*pb.SNodeHello) bool       = (*pb.SNodeHello).GetSettle
		)

		promote := &pb.PromotionCommand{Cmd: &pb.PromotionCommand_Promote{Promote: &pb.PromoteSafePartition{PromotionSeq: 7}}, AuthorityJws: "jws"}
		cleanup := &pb.PromotionCommand{Cmd: &pb.PromotionCommand_Cleanup{Cleanup: &pb.UnsafeCleanup{}}, AuthorityJws: "jws"}
		hello := &pb.SNodeHello{NodeId: "snode-3"}
		settled := func(m proto.Message) proto.Message {
			c := proto.Clone(m)
			c.ProtoReflect().Set(c.ProtoReflect().Descriptor().Fields().ByName("settle"), protoreflect.ValueOfBool(true))
			return c
		}
		for _, tc := range []struct {
			name          string
			plain         proto.Message
			without, with []protowire.Number
		}{
			{"PromotionCommand promote", promote, []protowire.Number{1, 15}, []protowire.Number{1, 3, 15}},
			{"PromotionCommand cleanup", cleanup, []protowire.Number{2, 15}, []protowire.Number{2, 3, 15}},
			{"SNodeHello", hello, []protowire.Number{1}, []protowire.Number{1, 2}},
		} {
			plain, err := proto.MarshalOptions{Deterministic: true}.Marshal(tc.plain)
			if err != nil {
				t.Fatal(err)
			}
			if got := settleWireTags(t, plain); !reflect.DeepEqual(got, tc.without) {
				t.Fatalf("%s without settle encodes tags %v, want %v", tc.name, got, tc.without)
			}
			marked, err := proto.MarshalOptions{Deterministic: true}.Marshal(settled(tc.plain))
			if err != nil {
				t.Fatal(err)
			}
			if got := settleWireTags(t, marked); !reflect.DeepEqual(got, tc.with) {
				t.Fatalf("%s with settle encodes tags %v, want %v", tc.name, got, tc.with)
			}
			if len(marked) != len(plain)+2 {
				t.Fatalf("%s: settle adds %d bytes, want the 2 of one varint field", tc.name, len(marked)-len(plain))
			}
		}
		if !settled(promote).(*pb.PromotionCommand).GetSettle() || !settled(hello).(*pb.SNodeHello).GetSettle() || promote.GetSettle() || hello.GetSettle() {
			t.Fatal("GetSettle does not report the settle field")
		}

		// A reader whose descriptor predates the stream fields (the hash-pinned
		// main_f7d9f070 baseline) decodes a settle-marked command and hello
		// with every old field intact and settle kept as an unknown field: it
		// cannot tell a settle from an ordinary promotion, which is why the
		// leader sends settle only to a hello that carried it.
		old, err := protodesc.NewFiles(snapshotDescriptorFixture(t, "main_f7d9f070_descriptor.binpb"))
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			plain   proto.Message
			unknown []byte
		}{
			{promote, protowire.AppendVarint(protowire.AppendTag(nil, 3, protowire.VarintType), 1)},
			{hello, protowire.AppendVarint(protowire.AppendTag(nil, 2, protowire.VarintType), 1)},
		} {
			name := tc.plain.ProtoReflect().Descriptor().FullName()
			desc, err := old.FindDescriptorByName(name)
			if err != nil {
				t.Fatal(err)
			}
			md := desc.(protoreflect.MessageDescriptor)
			if md.Fields().ByName("settle") != nil {
				t.Fatalf("the baseline %s already has settle", name)
			}
			raw, err := proto.Marshal(settled(tc.plain))
			if err != nil {
				t.Fatal(err)
			}
			legacy := dynamicpb.NewMessage(md)
			if err := proto.Unmarshal(raw, legacy); err != nil {
				t.Fatal(err)
			}
			if string(legacy.GetUnknown()) != string(tc.unknown) {
				t.Fatalf("an older reader of %s keeps unknown bytes %x, want %x", name, legacy.GetUnknown(), tc.unknown)
			}
			legacy.SetUnknown(nil)
			oldBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(legacy)
			if err != nil {
				t.Fatal(err)
			}
			plainBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(tc.plain)
			if err != nil {
				t.Fatal(err)
			}
			if string(oldBytes) != string(plainBytes) {
				t.Fatalf("an older reader of %s lost an old field", name)
			}
		}
	})
}

// settleWireTags lists the field numbers of a marshalled message, ascending
// (the Go encoder writes a oneof after the plain fields, so the wire order is
// not the number order).
func settleWireTags(t *testing.T, raw []byte) []protowire.Number {
	t.Helper()
	var tags []protowire.Number
	for len(raw) > 0 {
		number, typ, n := protowire.ConsumeTag(raw)
		if n < 0 {
			t.Fatal(protowire.ParseError(n))
		}
		raw = raw[n:]
		n = protowire.ConsumeFieldValue(number, typ, raw)
		if n < 0 {
			t.Fatal(protowire.ParseError(n))
		}
		raw = raw[n:]
		tags = append(tags, number)
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i] < tags[j] })
	return tags
}

// TestSettleConsensusAdminMethods pins the two settle RPCs: unary, with
// exactly these request and response types, at method indices 14 and 15
// directly after Plan S3-A's ReleaseConsensusHold (13), the generated full
// method names and client and server signatures, and that a server embedding
// the generated stub answers Unimplemented until the arbiter implements them
// (Plan S3-D Task 51). The service's total method count is pinned by
// TestConsensusAdminRPCSignatures and TestStage3RPCSignatures (decision D25),
// not here, so Plan S3-E1 appends RebalanceGroup (16) and MarkMemberLagging
// (17) without editing this file.
func TestSettleConsensusAdminMethods(t *testing.T) {
	svc := pb.File_consensus_proto.Services().ByName("ConsensusAdmin")
	if svc == nil {
		t.Fatal("ConsensusAdmin is not defined in consensus.proto")
	}
	if m := svc.Methods().ByName("ReleaseConsensusHold"); m == nil || m.Index() != 13 {
		t.Fatalf("ReleaseConsensusHold = %v, want method 13", m)
	}
	for _, tc := range []struct {
		index         int
		method        protoreflect.Name
		input, output protoreflect.FullName
		fullName      string
	}{
		{14, "SettleTrack", "arbiter.SettleTrackRequest", "arbiter.TrackSettle", pb.ConsensusAdmin_SettleTrack_FullMethodName},
		{15, "GetTrackSettle", "arbiter.TrackSettleRef", "arbiter.TrackSettle", pb.ConsensusAdmin_GetTrackSettle_FullMethodName},
	} {
		t.Run(string(tc.method), func(t *testing.T) {
			if tc.index >= svc.Methods().Len() {
				t.Fatalf("ConsensusAdmin has %d methods, want at least %d", svc.Methods().Len(), tc.index+1)
			}
			m := svc.Methods().Get(tc.index)
			if m.Name() != tc.method || m.Input().FullName() != tc.input || m.Output().FullName() != tc.output || m.IsStreamingClient() || m.IsStreamingServer() {
				t.Fatalf("ConsensusAdmin method %d = %v, want unary %s(%s) returns (%s)", tc.index, m, tc.method, tc.input, tc.output)
			}
			if want := "/arbiter.ConsensusAdmin/" + string(tc.method); tc.fullName != want {
				t.Fatalf("full method name = %q, want %q", tc.fullName, want)
			}
			fn := reflect.ValueOf(pb.UnimplementedConsensusAdminServer{}).MethodByName(string(tc.method))
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
		_ func(pb.ConsensusAdminClient, context.Context, *pb.SettleTrackRequest, ...grpc.CallOption) (*pb.TrackSettle, error) = pb.ConsensusAdminClient.SettleTrack
		_ func(pb.ConsensusAdminClient, context.Context, *pb.TrackSettleRef, ...grpc.CallOption) (*pb.TrackSettle, error)     = pb.ConsensusAdminClient.GetTrackSettle
		_ func(pb.ConsensusAdminServer, context.Context, *pb.SettleTrackRequest) (*pb.TrackSettle, error)                     = pb.ConsensusAdminServer.SettleTrack
		_ func(pb.ConsensusAdminServer, context.Context, *pb.TrackSettleRef) (*pb.TrackSettle, error)                         = pb.ConsensusAdminServer.GetTrackSettle
	)
}

// TestSettleMessagesAreNotRaftReachable is the activation-model rule of Plan
// S3-D contract §33.1 at the descriptor level: no RaftCommand field reaches
// SettleTrackRequest, TrackSettleRef, TrackSettle, TrackSettleState,
// PromotionCommand or SNodeHello at any depth, so arbiter-core's strict Raft
// decoder sees no settle byte and a settle can never be proposed, replicated
// or snapshotted. The settle messages travel only as the two ConsensusAdmin
// RPCs' requests and responses (no message embeds them, and TrackSettleState
// is a field of TrackSettle only), and the two stream messages only on
// PromotionGateway.SubscribePromotions, the leader-to-SNode stream. No count
// that a later plan changes (RaftCommand's tags, ConsensusAdmin's methods)
// is pinned here.
func TestSettleMessagesAreNotRaftReachable(t *testing.T) {
	raft := (&pb.RaftCommand{}).ProtoReflect().Descriptor()
	reachable := reachableTypes(raft)
	for _, name := range []protoreflect.FullName{"arbiter.SettleTrackRequest", "arbiter.TrackSettleRef", "arbiter.TrackSettle", "arbiter.TrackSettleState", "arbiter.PromotionCommand", "arbiter.SNodeHello"} {
		if reachable[name] {
			t.Errorf("RaftCommand reaches %s", name)
		}
	}
	// The walk has teeth: it reaches the payloads a PromotionCommand carries,
	// their acknowledgements and the quarantine and release commands a settle
	// sits between, so a Raft reference to a settle message from any of them
	// would be found.
	for _, name := range []protoreflect.FullName{"arbiter.PromoteSafePartition", "arbiter.UnsafeCleanup", "arbiter.PromotionAck", "arbiter.CleanupAck", "arbiter.QuarantineIndexerCmd", "arbiter.ReleaseIndexerCmd"} {
		if !reachable[name] {
			t.Fatalf("RaftCommand walk does not reach %s", name)
		}
	}
	if !reachableTypes((&pb.TrackSettle{}).ProtoReflect().Descriptor())["arbiter.TrackSettleState"] {
		t.Fatal("TrackSettle walk does not reach its state enum")
	}

	embedded := map[protoreflect.FullName][]string{}
	used := map[protoreflect.FullName][]string{}
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		var walk func(protoreflect.MessageDescriptors)
		walk = func(messages protoreflect.MessageDescriptors) {
			for i := 0; i < messages.Len(); i++ {
				md := messages.Get(i)
				fields := md.Fields()
				for j := 0; j < fields.Len(); j++ {
					f := fields.Get(j)
					if f.Message() != nil {
						embedded[f.Message().FullName()] = append(embedded[f.Message().FullName()], string(f.FullName()))
					}
					if f.Enum() != nil {
						embedded[f.Enum().FullName()] = append(embedded[f.Enum().FullName()], string(f.FullName()))
					}
				}
				walk(md.Messages())
			}
		}
		walk(fd.Messages())
		services := fd.Services()
		for i := 0; i < services.Len(); i++ {
			methods := services.Get(i).Methods()
			for j := 0; j < methods.Len(); j++ {
				m := methods.Get(j)
				used[m.Input().FullName()] = append(used[m.Input().FullName()], string(m.FullName())+" request")
				used[m.Output().FullName()] = append(used[m.Output().FullName()], string(m.FullName())+" response")
			}
		}
		return true
	})
	for _, tc := range []struct {
		name     protoreflect.FullName
		embedded []string
		used     []string
	}{
		{"arbiter.SettleTrackRequest", nil, []string{"arbiter.ConsensusAdmin.SettleTrack request"}},
		{"arbiter.TrackSettleRef", nil, []string{"arbiter.ConsensusAdmin.GetTrackSettle request"}},
		{"arbiter.TrackSettle", nil, []string{"arbiter.ConsensusAdmin.SettleTrack response", "arbiter.ConsensusAdmin.GetTrackSettle response"}},
		{"arbiter.TrackSettleState", []string{"arbiter.TrackSettle.state"}, nil},
		{"arbiter.SNodeHello", nil, []string{"arbiter.PromotionGateway.SubscribePromotions request"}},
		{"arbiter.PromotionCommand", nil, []string{"arbiter.PromotionGateway.SubscribePromotions response"}},
	} {
		if !reflect.DeepEqual(embedded[tc.name], tc.embedded) {
			t.Errorf("%s is a field type of %v, want %v", tc.name, embedded[tc.name], tc.embedded)
		}
		if !reflect.DeepEqual(used[tc.name], tc.used) {
			t.Errorf("%s is used by %v, want %v", tc.name, used[tc.name], tc.used)
		}
	}
	stream := pb.File_arbiter_proto.Services().ByName("PromotionGateway").Methods().ByName("SubscribePromotions")
	if stream == nil || !stream.IsStreamingServer() || stream.IsStreamingClient() {
		t.Fatalf("SubscribePromotions = %v, want a server stream", stream)
	}
}
