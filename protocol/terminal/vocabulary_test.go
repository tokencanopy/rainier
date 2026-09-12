package terminal_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/protocol/terminal"
)

// TestExecReasonsHoldsEveryReasonTheCodeDeclares closes the last hole in the
// vocabulary check.
//
// Moving the list out of a test's `want` and into ExecReasons made it one
// hand-written list compared against another, so a new Reason* constant added
// to the const block and not to ExecReasons is still invisible — which is
// exactly how no_answer and stdin_overrun drifted out of the design's
// published list in the first place. This reads the DECLARATIONS.
func TestExecReasonsHoldsEveryReasonTheCodeDeclares(t *testing.T) {
	declared := declaredStringConsts(t, "messages.go", "Reason")
	if len(declared) == 0 {
		t.Fatal("no Reason constants found; this guard would pass vacuously")
	}
	published := map[string]bool{}
	for _, r := range terminal.ExecReasons() {
		published[r] = true
	}
	for name, value := range declared {
		if !published[value] {
			t.Errorf("the constant %s = %q is declared and ExecReasons() does not "+
				"carry it. A word a caller can receive and no list names is a word "+
				"nothing checks and no document describes.", name, value)
		}
	}
	if len(published) != len(declared) {
		t.Errorf("ExecReasons() has %d words and the code declares %d; the two must "+
			"be the same set", len(published), len(declared))
	}
}

// declaredStringConsts returns the string constants in one file whose names
// start with prefix, as name -> value.
func declaredStringConsts(t *testing.T, file, prefix string) map[string]string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", file, err)
	}
	out := map[string]string{}
	for _, d := range f.Decls {
		gen, ok := d.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok || len(vs.Names) != len(vs.Values) {
				continue
			}
			for i, name := range vs.Names {
				if !strings.HasPrefix(name.Name, prefix) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				value, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatalf("unquoting %s: %v", name.Name, err)
				}
				out[name.Name] = value
			}
		}
	}
	return out
}
