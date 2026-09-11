package sandboxexec

import (
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

// TestNothingHereImportsTheSession is the guard for this package's central
// claim, which until now was a sentence in a doc comment and nothing else.
//
// "Nothing in this package imports internal/session" is load-bearing for the
// whole "second kind, not a flag" design: an exec's output must never reach
// the emulator, the event log or another viewer's screen, and the cheapest
// way to guarantee that is for the code to be unable to name the session at
// all. Adding the import AND holding a *session.Session failed no test.
//
// It reads this package's own files rather than `go list -deps`, on purpose:
// a transitive dependency is not the claim. internal/relay is imported here
// and imports internal/session itself, which is fine — the seam is that THIS
// code cannot reach for a session, not that no session exists anywhere below.
func TestNothingHereImportsTheSession(t *testing.T) {
	const forbidden = "github.com/tokencanopy/rainier/internal/session"

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parsing this package: %v", err)
	}
	if len(pkgs) == 0 {
		t.Fatal("no Go files found; this guard would pass vacuously")
	}
	files := 0
	for _, pkg := range pkgs {
		for name, f := range pkg.Files {
			files++
			for _, imp := range f.Imports {
				path, err := strconv.Unquote(imp.Path.Value)
				if err != nil {
					t.Fatalf("%s: unquoting %s: %v", name, imp.Path.Value, err)
				}
				if path == forbidden || strings.HasPrefix(path, forbidden+"/") {
					t.Fatalf("%s imports %s.\n\n"+
						"An exec is spawned BESIDE the session and never inside it. Its "+
						"output must never reach the emulator, the event log or another "+
						"viewer's screen, and this package not being able to name a "+
						"session is what makes that structural rather than careful.",
						name, path)
				}
			}
		}
	}
	if files < 2 {
		t.Fatalf("the guard saw %d file(s); it is meant to read the whole package", files)
	}
}
