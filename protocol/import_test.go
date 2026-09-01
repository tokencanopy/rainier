package protocol_test

import (
	"testing"

	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
	"github.com/tokencanopy/rainier/protocol/workspace"
)

// TestPublicProtocolImports is the smoke test for the whole public protocol
// namespace: an external package (this one — protocol_test is a separate
// package, exactly as a Rainier Cloud module would be) can construct each
// contract's wire shape by importing only the three canonical public paths.
// No internal/ import is needed, and none appears here.
func TestPublicProtocolImports(t *testing.T) {
	_ = runner.ToRunner{Type: "destroy", Session: "sess_example"}
	_ = terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24}
	_ = workspace.PullRequest{Xfer: "xfer_example", Path: "src", Seq: 0}
}
