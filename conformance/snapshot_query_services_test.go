package conformance

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestSnapshotQueryRaftAndDispatchAllocations(t *testing.T) {
	if int32(pb.StatementKind_STATEMENT_KIND_SNAPSHOT_QUERY) != 2 {
		t.Fatal("snapshot query kind allocation changed")
	}
	names := []string{"begin_snapshot_query", "grant_snapshot_query", "release_snapshot_query", "submit_snapshot_query", "abort_snapshot_query", "activate_query_profile", "record_snapshot_query_claim", "record_snapshot_query_attestation", "publish_executor_profile_transition", "record_snapshot_artifact_ready"}
	types := []string{"BeginSnapshotQueryCmd", "GrantSnapshotQueryCmd", "ReleaseSnapshotQueryCmd", "SubmitSnapshotQueryCmd", "AbortSnapshotQueryCmd", "ActivateQueryProfileCmd", "RecordSnapshotQueryClaimCmd", "RecordSnapshotQueryAttestationCmd", "PublishExecutorProfileTransitionCmd", "RecordSnapshotArtifactReadyCmd"}
	for i, name := range names {
		t.Run(name, func(t *testing.T) {
			assertSnapshotVariant(t, &pb.RaftCommand{}, "cmd", name, protoreflect.FieldNumber(i+18), types[i])
		})
	}
	assertSnapshotVariant(t, &pb.VerifierDispatch{}, "dispatch", "snapshot_query_job", 3, "SnapshotQueryJob")
}

func assertSnapshotVariant(t *testing.T, in proto.Message, oneof, name string, number protoreflect.FieldNumber, typ string) {
	t.Helper()
	m := in.ProtoReflect()
	fd := m.Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil || fd.Number() != number || fd.ContainingOneof() == nil || string(fd.ContainingOneof().Name()) != oneof || string(fd.Message().Name()) != typ {
		t.Fatalf("variant %s allocation changed: %v", name, fd)
	}
	populateSnapshotField(m, fd, 7)
	raw, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	out := m.Type().New()
	if err := proto.Unmarshal(raw, out.Interface()); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(in, out.Interface()) || out.WhichOneof(out.Descriptor().Oneofs().ByName(protoreflect.Name(oneof))) != fd {
		t.Fatalf("variant %s changed during wire round trip", name)
	}
}

func TestSnapshotQueryServicesRemainUnimplemented(t *testing.T) {
	cases := []struct {
		service, method, input, output string
		server                         any
	}{
		{"ArbiterIngress", "AcquireSnapshotQuery", "AcquireSnapshotQueryRequest", "SnapshotQueryReservation", pb.UnimplementedArbiterIngressServer{}},
		{"ArbiterIngress", "GetSnapshotQueryReservation", "GetSnapshotQueryReservationRequest", "SnapshotQueryReservationStatus", pb.UnimplementedArbiterIngressServer{}},
		{"ArbiterIngress", "ReleaseSnapshotQuery", "ReleaseSnapshotQueryRequest", "SnapshotQueryReservationStatus", pb.UnimplementedArbiterIngressServer{}},
		{"ArbiterIngress", "SubmitSnapshotQuery", "SnapshotQueryEnvelope", "SnapshotQuerySubmitResult", pb.UnimplementedArbiterIngressServer{}},
		{"ArbiterIngress", "GetSnapshotQueryStatus", "GetSnapshotQueryStatusRequest", "SnapshotQueryStatus", pb.UnimplementedArbiterIngressServer{}},
		{"SourceClaims", "RegisterSnapshotQueryClaim", "SnapshotQueryClaim", "Ack", pb.UnimplementedSourceClaimsServer{}},
		{"SourceClaims", "RecordSnapshotArtifactReady", "SnapshotArtifactReadySubmission", "Ack", pb.UnimplementedSourceClaimsServer{}},
		{"VerifierGateway", "SubmitSnapshotQueryAttestation", "SnapshotQueryAttestation", "Ack", pb.UnimplementedVerifierGatewayServer{}},
		{"SafeState", "GetPublishedSnapshot", "GetPublishedSnapshotRequest", "PublishedSnapshot", pb.UnimplementedSafeStateServer{}},
		{"SafeState", "GetQueryPolicy", "GetQueryPolicyRequest", "QueryPolicyStatus", pb.UnimplementedSafeStateServer{}},
	}
	for _, tc := range cases {
		t.Run(tc.service+"/"+tc.method, func(t *testing.T) {
			svc := pb.File_arbiter_proto.Services().ByName(protoreflect.Name(tc.service))
			if svc == nil {
				t.Fatal("service missing")
			}
			method := svc.Methods().ByName(protoreflect.Name(tc.method))
			if method == nil || string(method.Input().Name()) != tc.input || string(method.Output().Name()) != tc.output || method.IsStreamingClient() || method.IsStreamingServer() {
				t.Fatalf("RPC signature changed: %v", method)
			}
			// Embedded old implementations get a safe Unimplemented result until their
			// owner explicitly implements the new lane and its authentication gate.
			fn := reflect.ValueOf(tc.server).MethodByName(tc.method)
			if !fn.IsValid() {
				t.Fatal("generated stub missing")
			}
			result := fn.Call([]reflect.Value{reflect.ValueOf(context.Background()), reflect.New(fn.Type().In(1).Elem())})
			if !result[0].IsNil() || result[1].IsNil() {
				t.Fatal("new RPC returned success before implementation")
			}
			if code := status.Code(result[1].Interface().(error)); code != codes.Unimplemented {
				t.Fatalf("code = %s", code)
			}
		})
	}
}

func TestSnapshotQueryUnknownWireFieldsRetained(t *testing.T) {
	// A later canonical signed-input validator must inspect unknown/present-empty
	// payload fields. AP does not silently discard them or treat them as canonical.
	in := &pb.SnapshotQueryEnvelope{Input: &pb.SnapshotQueryInput{Binding: &pb.SnapshotQueryBinding{EnvelopeVersion: 3, InputKind: "snapshot_query"}, ReadSet: &pb.SnapshotReadSet{}}, InputRoot: "root", UserJws: "original-jws"}
	raw, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	unknown := protowire.AppendTag(nil, 99, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, nil)
	raw = append(raw, unknown...)
	out := &pb.SnapshotQueryEnvelope{}
	if err := proto.Unmarshal(raw, out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.ProtoReflect().GetUnknown(), unknown) {
		t.Fatal("present-empty unknown field disappeared")
	}
	out.ProtoReflect().SetUnknown(nil)
	if !proto.Equal(in, out) {
		t.Fatal("known fields changed with an unknown field present")
	}
}
