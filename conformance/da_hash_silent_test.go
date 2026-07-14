// Package conformance holds descriptor-level contract assertions for the
// arbiter-proto wire surface (the da.proto design spec's "hash-silent read
// path" rule: the store's word about content is never on the read path).
package conformance

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// contentAssertionFields returns the field names of md that look like a
// server-side content assertion: any *hash* field, or a payload_length
// echo. served_length is deliberately allowed — it reports service volume
// (bytes sent for a spec), not a claim about payload content.
func contentAssertionFields(md protoreflect.MessageDescriptor) []string {
	var hits []string
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		name := string(fields.Get(i).Name())
		if strings.Contains(name, "hash") || name == "payload_length" {
			hits = append(hits, name)
		}
	}
	return hits
}

// TestDaReadPathIsHashSilent pins the spec rule that FetchBegin/FetchData/
// FetchEnd/PayloadStat never assert content hashes or lengths — readers
// verify bytes against the sequenced envelope only (base-spec §4 trust
// boundary). If this fails, someone re-added a server assertion to the
// read path; remove the field, do not update the test.
func TestDaReadPathIsHashSilent(t *testing.T) {
	readPath := []protoreflect.MessageDescriptor{
		(&pb.FetchBegin{}).ProtoReflect().Descriptor(),
		(&pb.FetchData{}).ProtoReflect().Descriptor(),
		(&pb.FetchEnd{}).ProtoReflect().Descriptor(),
		(&pb.PayloadStat{}).ProtoReflect().Descriptor(),
	}
	for _, md := range readPath {
		if hits := contentAssertionFields(md); len(hits) > 0 {
			t.Errorf("%s carries content-assertion field(s) %v; the read path must stay hash-silent", md.FullName(), hits)
		}
	}
}

// TestHashSilentDetectorHasTeeth proves the detector actually detects:
// PutPayloadHeader declares payload_hash + payload_length by design, so an
// empty result here means the detector (not the contract) is broken.
func TestHashSilentDetectorHasTeeth(t *testing.T) {
	md := (&pb.PutPayloadHeader{}).ProtoReflect().Descriptor()
	hits := contentAssertionFields(md)
	if len(hits) != 2 {
		t.Fatalf("detector found %v on PutPayloadHeader, want [payload_hash payload_length]; the hash-silent test lost its teeth", hits)
	}
}
