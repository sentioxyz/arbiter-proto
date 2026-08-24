package conformance

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bufbuild/protocompile/ast"
	"github.com/bufbuild/protocompile/parser"
	"github.com/bufbuild/protocompile/reporter"
	"google.golang.org/protobuf/reflect/protoreflect"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

const arbiterProtoModule = "github.com/sentioxyz/arbiter-proto"

const statementKindBindingComments = `// Bound by user_jws from envelope v2.1 (arbiter-proto v0.6.0) onward: the
// signed payload carries "statement_kind" as this enum's numeric value, so
// an operator cannot re-label an INSERT as another kind once the mutation
// lane ships. v1 still admits INSERT only.`

const statementKindBindingField = statementKindBindingComments + `
StatementKind statement_kind = 2;`

// TestStatementKindIsFieldTwoAndInsertIsOne pins what the envelope-v2.1 user
// JWS binds. The signing payload carries statement_kind as the numeric enum
// value, so the field number, enum type, and enum numbers are consensus
// constants, not implementation details.
func TestStatementKindIsFieldTwoAndInsertIsOne(t *testing.T) {
	fd := (&pb.StatementEnvelopeV2{}).ProtoReflect().Descriptor().Fields().ByName(protoreflectName("statement_kind"))
	if fd == nil {
		t.Fatal("StatementEnvelopeV2 is missing statement_kind")
	}
	if fd.Number() != 2 {
		t.Fatalf("statement_kind number = %d, want 2", fd.Number())
	}
	if fd.Kind() != protoreflect.EnumKind {
		t.Fatalf("statement_kind kind = %s, want enum", fd.Kind())
	}
	wantEnum := pb.StatementKind_STATEMENT_KIND_UNSPECIFIED.Descriptor()
	if fd.Enum() == nil || fd.Enum().FullName() != wantEnum.FullName() {
		t.Fatalf("statement_kind enum type = %v, want %s", fd.Enum(), wantEnum.FullName())
	}
	if got := int32(pb.StatementKind_STATEMENT_KIND_INSERT); got != 1 {
		t.Fatalf("STATEMENT_KIND_INSERT = %d, want 1", got)
	}
	if got := int32(pb.StatementKind_STATEMENT_KIND_UNSPECIFIED); got != 0 {
		t.Fatalf("STATEMENT_KIND_UNSPECIFIED = %d, want 0", got)
	}
}

// TestStatementKindDocumentsTheSignedBinding parses the checked-in contract
// with Buf's protobuf parser. This binds the exact active source comments to
// the exact top-level field without applying Go-comment normalization or
// rejecting valid protobuf-only grammar.
func TestStatementKindDocumentsTheSignedBinding(t *testing.T) {
	protoPath := filepath.Join(arbiterProtoModuleRoot(t), "proto", "arbiter.proto")
	source, err := os.ReadFile(protoPath)
	if err != nil {
		t.Fatalf("read %s: %v", protoPath, err)
	}
	if err := validateStatementKindBinding(protoPath, string(source)); err != nil {
		t.Fatalf("validate %s: %v", protoPath, err)
	}
}

func TestStatementKindParserAcceptsProtoGrammar(t *testing.T) {
	source := `syntax = "proto3";
option (example) = 'valid \x70roto string with \'escape';
/* unrelated block comment with braces { } and // text */
message StatementEnvelopeV2 {
` + statementKindBindingField + `
}`
	if err := validateStatementKindBinding("valid_proto_grammar.proto", source); err != nil {
		t.Fatalf("valid protobuf grammar rejected: %v", err)
	}
}

func TestStatementKindParserAcceptsCRLF(t *testing.T) {
	source := strings.ReplaceAll(`syntax = "proto3";
message StatementEnvelopeV2 {
`+statementKindBindingField+`
}`, "\n", "\r\n")
	if err := validateStatementKindBinding("crlf.proto", source); err != nil {
		t.Fatalf("valid CRLF protobuf source rejected: %v", err)
	}
}

func TestStatementKindParserRejectsMisboundComments(t *testing.T) {
	mutateComment := func(index int, prefix, suffix string) string {
		lines := strings.Split(statementKindBindingComments, "\n")
		lines[index] = prefix + lines[index] + suffix
		return strings.Join(lines, "\n")
	}
	tests := map[string]string{
		"target only inside block comment": `/*
message StatementEnvelopeV2 {
` + statementKindBindingField + `
}
*/`,
		"binding belongs to decoy": `message Decoy {
` + statementKindBindingField + `
}
message StatementEnvelopeV2 {
StatementKind statement_kind = 2;
}`,
		"block comment instead of four line comments": `message StatementEnvelopeV2 {
/* Bound by user_jws from envelope v2.1 onward. */
StatementKind statement_kind = 2;
}`,
		"block comment before binding": `message StatementEnvelopeV2 {
/* NOT bound by user_jws. */
` + statementKindBindingField + `
}`,
		"first comment prefixed by previous field block tail": `message StatementEnvelopeV2 {
StatementID statement_id = 1; /* NOT bound by user_jws.
*/ // Bound by user_jws from envelope v2.1 (arbiter-proto v0.6.0) onward: the
// signed payload carries "statement_kind" as this enum's numeric value, so
// an operator cannot re-label an INSERT as another kind once the mutation
// lane ships. v1 still admits INSERT only.
StatementKind statement_kind = 2;
}`,
		"first comment inline block prefix": `message StatementEnvelopeV2 {
` + mutateComment(0, "/* NOT bound. */ ", "") + `
StatementKind statement_kind = 2;
}`,
		"middle comment multiline block prefix": `message StatementEnvelopeV2 {
` + mutateComment(1, "/* NOT bound.\n*/ ", "") + `
StatementKind statement_kind = 2;
}`,
		"last comment multiline block prefix": `message StatementEnvelopeV2 {
` + mutateComment(3, "/* NOT bound.\n*/ ", "") + `
StatementKind statement_kind = 2;
}`,
		"first comment tail": `message StatementEnvelopeV2 {
` + mutateComment(0, "", " /* NOT bound. */") + `
StatementKind statement_kind = 2;
}`,
		"middle comment tail": `message StatementEnvelopeV2 {
` + mutateComment(1, "", " /* NOT bound. */") + `
StatementKind statement_kind = 2;
}`,
		"last comment tail": `message StatementEnvelopeV2 {
` + mutateComment(3, "", " /* NOT bound. */") + `
StatementKind statement_kind = 2;
}`,
		"directive before binding": `message StatementEnvelopeV2 {
//security:not bound by user_jws.
` + statementKindBindingField + `
}`,
		"trailing negation": `message StatementEnvelopeV2 {
` + statementKindBindingField + ` // NOT bound by user_jws.
}`,
		"multiline block tail reassigned to next field": `message StatementEnvelopeV2 {
` + statementKindBindingField + ` /* NOT bound by user_jws.
*/ string sql = 3;
}`,
		"inline block tail reassigned to next field": `message StatementEnvelopeV2 {
` + statementKindBindingField + ` /* NOT bound by user_jws. */ string sql = 3;
}`,
		"second field on same physical line": `message StatementEnvelopeV2 {
` + statementKindBindingField + ` string sql = 3;
}`,
		"line comment on physical field line": `message StatementEnvelopeV2 {
` + statementKindBindingField + ` // NOT bound by user_jws.
}`,
		"block comment on physical field line": `message StatementEnvelopeV2 {
` + statementKindBindingField + ` /* NOT bound by user_jws. */
}`,
		"interior block comment": `message StatementEnvelopeV2 {
` + statementKindBindingComments + `
StatementKind /* NOT bound by user_jws. */ statement_kind = 2;
}`,
		"blank line inside binding comments": `message StatementEnvelopeV2 {
` + strings.Replace(statementKindBindingComments, "\n// signed payload", "\n\n// signed payload", 1) + `
StatementKind statement_kind = 2;
}`,
		"blank line before field": `message StatementEnvelopeV2 {
` + statementKindBindingComments + `

StatementKind statement_kind = 2;
}`,
		"nested target only": `message Outer {
message StatementEnvelopeV2 {
` + statementKindBindingField + `
}
}`,
	}
	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			if err := validateStatementKindBinding(name+".proto", source); err == nil {
				t.Fatal("invalid statement_kind binding was accepted")
			}
		})
	}
}

func TestStatementKindParserRejectsDuplicateOrWrongField(t *testing.T) {
	tests := map[string]string{
		"duplicate target": `message StatementEnvelopeV2 {
` + statementKindBindingField + `
}
message StatementEnvelopeV2 {
` + statementKindBindingField + `
}`,
		"duplicate field": `message StatementEnvelopeV2 {
` + statementKindBindingField + `
` + statementKindBindingField + `
}`,
		"wrong type": `message StatementEnvelopeV2 {
` + statementKindBindingComments + `
uint32 statement_kind = 2;
}`,
		"wrong tag": `message StatementEnvelopeV2 {
` + statementKindBindingComments + `
StatementKind statement_kind = 3;
}`,
		"field label": `message StatementEnvelopeV2 {
` + statementKindBindingComments + `
optional StatementKind statement_kind = 2;
}`,
		"field options": `message StatementEnvelopeV2 {
` + statementKindBindingComments + `
StatementKind statement_kind = 2 [deprecated = true];
}`,
	}
	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			if err := validateStatementKindBinding(name+".proto", source); err == nil {
				t.Fatal("invalid StatementEnvelopeV2.statement_kind field was accepted")
			}
		})
	}
}

func validateStatementKindBinding(filename, source string) error {
	// Parse and slice one canonical byte sequence. Only normalize standard
	// CRLF pairs; a lone CR remains visible to the protobuf parser.
	source = strings.ReplaceAll(source, "\r\n", "\n")

	handler := reporter.NewHandler(nil)
	file, err := parser.Parse(filename, strings.NewReader(source), handler)
	if err != nil {
		return fmt.Errorf("parse protobuf: %w", err)
	}
	if err := handler.Error(); err != nil {
		return fmt.Errorf("parse protobuf: %w", err)
	}

	var target *ast.MessageNode
	for _, decl := range file.Decls {
		message, ok := decl.(*ast.MessageNode)
		if !ok || message.Name.Val != "StatementEnvelopeV2" {
			continue
		}
		if target != nil {
			return fmt.Errorf("multiple top-level message StatementEnvelopeV2 declarations")
		}
		target = message
	}
	if target == nil {
		return fmt.Errorf("top-level message StatementEnvelopeV2 is missing")
	}

	var field *ast.FieldNode
	for _, decl := range target.Decls {
		candidate, ok := decl.(*ast.FieldNode)
		if !ok || candidate.Name.Val != "statement_kind" {
			continue
		}
		if field != nil {
			return fmt.Errorf("multiple depth-1 StatementEnvelopeV2.statement_kind fields")
		}
		field = candidate
	}
	if field == nil {
		return fmt.Errorf("depth-1 StatementEnvelopeV2.statement_kind field is missing")
	}
	if got := string(field.FldType.AsIdentifier()); got != "StatementKind" {
		return fmt.Errorf("StatementEnvelopeV2.statement_kind type = %q, want StatementKind", got)
	}
	if field.Tag == nil || field.Tag.Val != 2 {
		return fmt.Errorf("StatementEnvelopeV2.statement_kind tag = %v, want 2", field.Tag)
	}
	if field.Label.IsPresent() {
		return fmt.Errorf("StatementEnvelopeV2.statement_kind must not have a field label")
	}
	if field.Options != nil {
		return fmt.Errorf("StatementEnvelopeV2.statement_kind must not have field options")
	}

	info := file.NodeInfo(field)
	if !info.IsValid() {
		return fmt.Errorf("StatementEnvelopeV2.statement_kind source info is missing")
	}
	rawDeclaration := info.RawText()
	if rawDeclaration != "StatementKind statement_kind = 2;" {
		return fmt.Errorf("StatementEnvelopeV2.statement_kind raw declaration = %q, want exact field declaration", rawDeclaration)
	}
	physicalLine, err := physicalSourceLine(source, info.Start().Offset, rawDeclaration)
	if err != nil {
		return fmt.Errorf("StatementEnvelopeV2.statement_kind physical line: %w", err)
	}
	if strings.Trim(physicalLine, " \t") != "StatementKind statement_kind = 2;" {
		return fmt.Errorf("StatementEnvelopeV2.statement_kind physical line = %q, want only the exact field declaration", physicalLine)
	}
	wantComments := strings.Split(statementKindBindingComments, "\n")
	leading := info.LeadingComments()
	if leading.Len() != len(wantComments) {
		return fmt.Errorf("StatementEnvelopeV2.statement_kind leading comments = %d, want %d", leading.Len(), len(wantComments))
	}
	for i, want := range wantComments {
		comment := leading.Index(i)
		rawComment := comment.RawText()
		if rawComment != want {
			return fmt.Errorf("StatementEnvelopeV2.statement_kind leading comment %d = %q, want %q", i+1, rawComment, want)
		}
		physicalLine, err := physicalSourceLine(source, comment.Start().Offset, rawComment)
		if err != nil {
			return fmt.Errorf("StatementEnvelopeV2.statement_kind leading comment %d physical line: %w", i+1, err)
		}
		if strings.Trim(physicalLine, " \t") != want {
			return fmt.Errorf("StatementEnvelopeV2.statement_kind leading comment %d physical line = %q, want only %q", i+1, physicalLine, want)
		}
		if i > 0 {
			previous := leading.Index(i - 1)
			if comment.Start().Line != previous.End().Line+1 {
				return fmt.Errorf("StatementEnvelopeV2.statement_kind leading comments %d and %d are not adjacent", i, i+1)
			}
		}
	}
	lastComment := leading.Index(leading.Len() - 1)
	if info.Start().Line != lastComment.End().Line+1 {
		return fmt.Errorf("StatementEnvelopeV2.statement_kind is not immediately after its binding comments")
	}
	if trailing := info.TrailingComments(); trailing.Len() != 0 {
		return fmt.Errorf("StatementEnvelopeV2.statement_kind trailing comments = %d, want 0", trailing.Len())
	}
	return nil
}

func physicalSourceLine(source string, start int, raw string) (string, error) {
	// RawText's contract includes every interior byte and token. Derive the
	// exclusive source bound from it instead of relying on End().Offset details.
	end := start + len(raw)
	if start < 0 || end < start || end > len(source) {
		return "", fmt.Errorf("source offsets = [%d:%d), source bytes = %d", start, end, len(source))
	}
	if got := source[start:end]; got != raw {
		return "", fmt.Errorf("source slice = %q, parser raw text = %q", got, raw)
	}
	lineStart := strings.LastIndexByte(source[:start], '\n') + 1
	lineEnd := len(source)
	if newline := strings.IndexByte(source[end:], '\n'); newline >= 0 {
		lineEnd = end + newline
	}
	return source[lineStart:lineEnd], nil
}

func arbiterProtoModuleRoot(t *testing.T) string {
	t.Helper()

	start, err := os.Getwd()
	if err != nil {
		t.Fatalf("get test working directory: %v", err)
	}
	for dir := start; ; dir = filepath.Dir(dir) {
		goModPath := filepath.Join(dir, "go.mod")
		goMod, readErr := os.ReadFile(goModPath)
		switch {
		case readErr == nil && goModuleName(string(goMod)) == arbiterProtoModule:
			return dir
		case readErr != nil && !os.IsNotExist(readErr):
			t.Fatalf("read %s while locating module %q from %s: %v", goModPath, arbiterProtoModule, start, readErr)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
	}
	t.Fatalf("walked upward from %s without finding go.mod declaring module %q", start, arbiterProtoModule)
	return ""
}

func goModuleName(goMod string) string {
	for _, line := range strings.Split(goMod, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "module" {
			return strings.Trim(fields[1], `"`)
		}
	}
	return ""
}
