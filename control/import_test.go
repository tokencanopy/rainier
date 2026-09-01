package control_test

import (
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
	"github.com/tokencanopy/rainier/protocol/workspace"
)

// TestPublicControlImports is the smoke test for the whole public control
// namespace: an external package (this one — control_test is a separate
// package, exactly as a Rainier Cloud module would be) can build the
// application contract and every public protocol wire shape by importing only
// the four canonical public paths. No internal/ import is needed, and none
// appears here.
func TestPublicControlImports(t *testing.T) {
	_ = control.Scope{
		WorkspaceID: "ws_example",
		Actor:       control.Actor{ID: "act_example", Kind: control.ActorUser},
		Placement:   control.PlacementScope{ProductRegion: "us", HomeCell: "cell-1", Mode: control.ExecutionDedicated},
	}
	_ = control.CreateSession{Name: "investigate", EnvironmentID: "env_example"}
	_ = control.AttachTerminal{SessionID: "sess_example", Since: terminal.SinceAll, Mode: control.AttachmentViewer}
	_ = control.PushWorkspace{SessionID: "sess_example", Path: "src", Body: strings.NewReader("")}
	_ = control.PullWorkspace{SessionID: "sess_example", Path: "src", Body: &strings.Builder{}}
	_ = control.RunnerEvent{WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example", Generation: 1, SessionID: "sess_example", State: control.StateRunning}

	_ = runner.ToRunner{Type: "destroy", Session: "sess_example"}
	_ = terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24}
	_ = workspace.PullRequest{Xfer: "xfer_example", Path: "src", Seq: 0}
}

// isAllowedControlImport reports whether an import path the control package
// uses is standard library or a public protocol package. Anything else —
// internal/, net/http, WebSocket, SQL/pgx, Docker, a GitHub or cloud SDK, a
// billing package, or a provider-named package — is forbidden.
func isAllowedControlImport(path string) bool {
	if !strings.Contains(strings.SplitN(path, "/", 2)[0], ".") {
		return true // standard library
	}
	return strings.HasPrefix(path, "github.com/tokencanopy/rainier/protocol/")
}

// TestControlForbiddenDependencySmoke pins the control package's imports to
// the standard library plus the public protocol packages, by reading its
// source. The shell guard (scripts/check-public-control.sh) enforces the same
// rule outside go test.
func TestControlForbiddenDependencySmoke(t *testing.T) {
	for _, imp := range parseControl(t).imports {
		if !isAllowedControlImport(imp) {
			t.Errorf("control imports %q: only the standard library and public protocol packages are permitted", imp)
		}
	}
}
