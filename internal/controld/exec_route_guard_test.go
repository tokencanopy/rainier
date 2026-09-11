// internal/controld/exec_route_guard_test.go
package controld

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// TestTheExecRouteUsesTheExecStream pins which client stream each route wraps
// its socket in, which is a choice no behavioural test in this repository can
// see: the two differ only in a write budget measured in tens of seconds, and
// swapping attachplane.ExecClientStream for ClientStream in handleClientExec
// survived the whole tree.
//
// The budget is the plane's half of "an exec caller that has stopped reading
// backs up the writer every attachment on this session shares" — a person
// watching a screen gets a minute because being disconnected mid-scrollback
// costs them their session, and a script gets twenty seconds because it can
// run the command again. attachplane's own TestExecClientStreamIsOnTheExecBudget
// pins what the two budgets ARE; this pins that this route asks for the right
// one.
func TestTheExecRouteUsesTheExecStream(t *testing.T) {
	for _, tc := range []struct {
		file, fn, want, unwanted string
	}{
		{"exec.go", "handleClientExec", "ExecClientStream", "ClientStream"},
		{"attach.go", "handleClientAttach", "ClientStream", "ExecClientStream"},
	} {
		calls := attachplaneCalls(t, tc.file, tc.fn)
		if !calls[tc.want] {
			t.Errorf("%s does not call attachplane.%s", tc.fn, tc.want)
		}
		if calls[tc.unwanted] {
			t.Errorf("%s calls attachplane.%s; the two write budgets are not "+
				"interchangeable", tc.fn, tc.unwanted)
		}
	}
}

// attachplaneCalls is the set of attachplane selectors one function calls.
func attachplaneCalls(t *testing.T, file, fn string) map[string]bool {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", file, err)
	}
	var decl *ast.FuncDecl
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == fn {
			decl = fd
			break
		}
	}
	if decl == nil {
		t.Fatalf("%s has no %s", file, fn)
	}
	out := map[string]bool{}
	ast.Inspect(decl, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "attachplane" {
			out[sel.Sel.Name] = true
		}
		return true
	})
	return out
}
