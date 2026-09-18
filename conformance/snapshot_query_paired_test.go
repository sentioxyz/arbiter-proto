package conformance

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// fixtureCanonical emits ordered descriptor fields with canonical numeric JSON.
// This is a test-only independent reader, never a replacement canonical API.
func fixtureCanonical(m protoreflect.Message) []byte {
	var b bytes.Buffer
	b.WriteByte('{')
	comma := false
	fs := m.Descriptor().Fields()
	for i := 0; i < fs.Len(); i++ {
		fd := fs.Get(i)
		if fd.Name() == "storage_refs" {
			continue
		} // New receipt projection only.
		if comma {
			b.WriteByte(',')
		}
		comma = true
		k, _ := json.Marshal(string(fd.Name()))
		b.Write(k)
		b.WriteByte(':')
		var emit func(protoreflect.Value)
		emit = func(v protoreflect.Value) {
			if fd.Message() != nil {
				b.Write(fixtureCanonical(v.Message()))
				return
			}
			raw, _ := json.Marshal(v.Interface())
			b.Write(raw)
		}
		v := m.Get(fd)
		if fd.IsList() {
			b.WriteByte('[')
			for j := 0; j < v.List().Len(); j++ {
				if j > 0 {
					b.WriteByte(',')
				}
				emit(v.List().Get(j))
			}
			b.WriteByte(']')
		} else {
			emit(v)
		}
	}
	b.WriteByte('}')
	return b.Bytes()
}
func pairedProto(t *testing.T, raw json.RawMessage, m proto.Message) proto.Message {
	t.Helper()
	if err := protojson.Unmarshal(raw, m); err != nil {
		t.Fatal(err)
	}
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	out := m.ProtoReflect().Type().New().Interface()
	if err = proto.Unmarshal(b, out); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(m, out) {
		t.Fatal("paired transport loss")
	}
	return out
}
func pairedDigest(domain, literal string) string {
	return fmt.Sprintf("0x%x", sha256.Sum256([]byte("housegate-replay-mvp-v0:"+domain+"\x00"+literal)))
}
func TestSnapshotQueryPairedGolden(t *testing.T) {
	raw, err := os.ReadFile("../testdata/snapshot_query_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != "3558d62035a23f4e572d09a59f82bc0ebac4f9cae175ccee127b0034600ed137" {
		t.Fatal("HG golden changed")
	}
	var cases []map[string]json.RawMessage
	if err = json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	str := func(v json.RawMessage) string {
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	for _, c := range cases {
		t.Run(str(c["name"]), func(t *testing.T) {
			// Duplicates/conflicts are retained by protobuf; canonical rejection is HG/AC's job.
			pairedProto(t, c["input"], &pb.SnapshotQueryInput{})
			if len(c["error_contains"]) > 0 {
				return
			}
			for _, v := range []struct {
				bytes, root, domain string
				m                   proto.Message
			}{
				{"canonical_input_json", "input_root", "snapshot-query-input-v1", &pb.SnapshotQueryInput{}},
				{"canonical_read_set_json", "read_set_root", "snapshot-query-read-set-v1", &pb.SnapshotReadSet{}},
				{"canonical_receipt_json", "receipt_hash", "snapshot-query-receipt-v1", &pb.SnapshotQueryReceipt{}},
			} {
				literal := str(c[v.bytes])
				m := pairedProto(t, []byte(literal), v.m)
				actual := fixtureCanonical(m.ProtoReflect())
				if string(actual) != literal {
					t.Fatalf("%s canonical bytes: %s", v.bytes, actual)
				}
				if pairedDigest(v.domain, string(actual)) != str(c[v.root]) {
					t.Fatalf("%s digest", v.root)
				}
			}
			st := pairedProto(t, c["statement"], &pb.SnapshotQueryStatement{}).(*pb.SnapshotQueryStatement)
			projection, _ := json.Marshal(struct {
				Seq  uint64 `json:"statement_seq"`
				Root string `json:"input_root"`
				JWS  string `json:"user_jws"`
			}{st.StatementSeq, st.Envelope.InputRoot, st.Envelope.UserJws})
			if string(projection) != str(c["canonical_statement_root_json"]) || pairedDigest("snapshot-query-statement-root-v1", string(projection)) != str(c["statement_root"]) {
				t.Fatal("statement sequence/JWS identity")
			}
			for _, key := range []string{"reservation_statuses", "statuses"} {
				var statuses []json.RawMessage
				if len(c[key]) == 0 {
					continue
				}
				if err := json.Unmarshal(c[key], &statuses); err != nil {
					t.Fatal(err)
				}
				for _, status := range statuses {
					var m proto.Message = &pb.SnapshotQueryStatus{}
					if key == "reservation_statuses" {
						m = &pb.SnapshotQueryReservationStatus{}
					}
					pairedProto(t, status, m)
				}
			}
			var contracts []struct {
				Name, Domain, Hash string
				Value              json.RawMessage
				CanonicalJSON      string `json:"canonical_json"`
			}
			if len(c["contracts"]) > 0 {
				if err := json.Unmarshal(c["contracts"], &contracts); err != nil {
					t.Fatal(err)
				}
			}
			names := map[string]string{"snapshot-query-profile-v1": "QueryProfileRecord", "snapshot-query-artifact-set-v1": "SnapshotArtifactSet", "snapshot-query-artifact-ready-v1": "SnapshotArtifactReady", "snapshot-query-abort-v1": "SnapshotQueryAbortRecord", "snapshot-query-claim-v1": "SnapshotQueryClaim", "executor-profile-transition-v1": "ExecutorProfileTransition", "snapshot-query-receipt-v1": "SnapshotQueryReceipt"}
			for _, v := range contracts {
				if pairedDigest(v.Domain, v.CanonicalJSON) != v.Hash {
					t.Fatal(v.Name)
				}
				if v.Domain == "snapshot-query-output-v1" {
					continue
				}
				typ, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName("arbiter." + names[v.Domain]))
				if err != nil {
					t.Fatal(err)
				}
				m := pairedProto(t, v.Value, typ.New().Interface())
				if string(fixtureCanonical(m.ProtoReflect())) != v.CanonicalJSON {
					t.Fatalf("contract %s canonical bytes", v.Name)
				}
			}
		})
	}
}

func TestSnapshotQueryPairedIdentityFixture(t *testing.T) {
	raw, err := os.ReadFile("../testdata/snapshot_query_identity_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Account    string
		Input      json.RawMessage
		InputRoot  string `json:"input_root"`
		Identities []struct {
			Iat         int64
			UserJWS     string `json:"user_jws"`
			UserJWSHash string `json:"user_jws_hash"`
		}
	}
	if err = json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	in := pairedProto(t, f.Input, &pb.SnapshotQueryInput{}).(*pb.SnapshotQueryInput)
	if pairedDigest("snapshot-query-input-v1", string(fixtureCanonical(in.ProtoReflect()))) != f.InputRoot {
		t.Fatal("input root")
	}
	if len(f.Identities) != 2 || f.Identities[0].Iat == f.Identities[1].Iat || f.Identities[0].UserJWSHash == f.Identities[1].UserJWSHash {
		t.Fatal("distinct identities")
	}
	// AC verifies the fixture's ES256K signatures. AP stays dependency-light and
	// independently verifies exact compact bytes, hashes and generated transports.
	for _, id := range f.Identities {
		pieces := strings.Split(id.UserJWS, ".")
		if len(pieces) != 3 {
			t.Fatal("compact JWS")
		}
		payload, err := base64.RawURLEncoding.DecodeString(pieces[1])
		if err != nil {
			t.Fatal(err)
		}
		expected := pairedIdentityPayload{"housegate-statement-v3", id.Iat, fixtureCanonical(in.Binding.ProtoReflect()), f.InputRoot}
		if err = checkPairedIdentityPayload(payload, expected); err != nil {
			t.Fatal(err)
		}

		if fmt.Sprintf("0x%x", sha256.Sum256([]byte(id.UserJWS))) != id.UserJWSHash {
			t.Fatal("original JWS digest")
		}
		envelope := &pb.SnapshotQueryEnvelope{Input: in, InputRoot: f.InputRoot, UserJws: id.UserJWS}
		b, err := proto.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		var got pb.SnapshotQueryEnvelope
		if err = proto.Unmarshal(b, &got); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(envelope, &got) {
			t.Fatal("original bytes lost")
		}
	}
	for _, tc := range []struct{ name, hash string }{{"omitted hash", ""}, {"wrong hash", "0xwrong"}, {"lost rejection then lookup", f.Identities[1].UserJWSHash}, {"exact original JWS retry", f.Identities[0].UserJWSHash}} {
		t.Run(tc.name, func(t *testing.T) {
			want := &pb.GetSnapshotQueryStatusRequest{NetworkId: in.Binding.NetworkId, KeeperShardId: in.Binding.KeeperShardId, ClientAccount: f.Account, StatementId: in.Binding.StatementId, ExpectedInputRoot: f.InputRoot, ExpectedUserJwsHash: tc.hash}
			b, err := proto.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			var got pb.GetSnapshotQueryStatusRequest
			if err = proto.Unmarshal(b, &got); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(want, &got) {
				t.Fatal("status identity loss")
			}
		})
	}
}

type pairedIdentityPayload struct {
	Purpose   string          `json:"purpose"`
	Iat       int64           `json:"iat"`
	Binding   json.RawMessage `json:"binding"`
	InputRoot string          `json:"input_root"`
}

func checkPairedIdentityPayload(payload []byte, expected pairedIdentityPayload) error {
	canonical, err := json.Marshal(expected)
	if err != nil {
		return err
	}
	if !bytes.Equal(payload, canonical) {
		return fmt.Errorf("complete four-field canonical payload differs (purpose, iat, binding, input_root)")
	}
	return nil
}
func TestSnapshotQueryPairedIdentityPayloadRejectsIncompleteOrNoncanonical(t *testing.T) {
	raw, err := os.ReadFile("../testdata/snapshot_query_identity_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Input     json.RawMessage
		InputRoot string `json:"input_root"`
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	in := pairedProto(t, fixture.Input, &pb.SnapshotQueryInput{}).(*pb.SnapshotQueryInput)
	root := pairedDigest("snapshot-query-input-v1", string(fixtureCanonical(in.ProtoReflect())))
	if root != fixture.InputRoot {
		t.Fatal("input root")
	}
	expected := pairedIdentityPayload{"housegate-statement-v3", 1789550000, fixtureCanonical(in.Binding.ProtoReflect()), root}
	canonical, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	missing, _ := json.Marshal(struct {
		Purpose string          `json:"purpose"`
		Iat     int64           `json:"iat"`
		Binding json.RawMessage `json:"binding"`
	}{expected.Purpose, expected.Iat, expected.Binding})
	wrong := expected
	wrong.InputRoot = "0xwrong"
	wrongBytes, _ := json.Marshal(wrong)
	reordered, _ := json.Marshal(struct {
		Iat       int64           `json:"iat"`
		Purpose   string          `json:"purpose"`
		Binding   json.RawMessage `json:"binding"`
		InputRoot string          `json:"input_root"`
	}{expected.Iat, expected.Purpose, expected.Binding, expected.InputRoot})
	extra := append(append([]byte{}, canonical[:len(canonical)-1]...), []byte(`,"extra":0}`)...)
	for name, payload := range map[string][]byte{"missing input_root": missing, "wrong input_root": wrongBytes, "reordered fields": reordered, "extra field": extra} {
		t.Run(name, func(t *testing.T) {
			if err := checkPairedIdentityPayload(payload, expected); err == nil {
				t.Fatal("accepted malformed complete payload")
			}
		})
	}
}
