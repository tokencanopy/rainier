// Package controlapp_test proves a separate module can build and drive the
// whole attachment application behind control.Attachments without importing
// any Rainier internal package or learning socket details: it supplies its
// own authorizer, policy, session repository, runner transport, broker,
// recorder, clock, IDs, and terminal stream, and exercises all four methods
// through the public interface only.
package controlapp_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
	"github.com/tokencanopy/rainier/protocol/workspace"
)

// After all four methods exist, AttachmentService is exactly the public
// control.Attachments contract.
var _ control.Attachments = (*controlapp.AttachmentService)(nil)

func constructAttachments(t *testing.T) control.Attachments {
	t.Helper()
	svc, err := controlapp.NewAttachmentService(externalAttachmentOptions())
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestExternalAttachmentSeam(t *testing.T) {
	app := constructAttachments(t)
	ctx := context.Background()
	scope := control.Scope{
		WorkspaceID: "ws_example",
		Actor:       control.Actor{ID: "act_example", Kind: control.ActorUser},
		Placement:   control.PlacementScope{ProductRegion: "us", HomeCell: "cell-1", Mode: control.ExecutionDedicated},
	}

	if err := app.AttachTerminal(ctx, scope, control.AttachTerminal{
		SessionID: "sess_example", Since: terminal.SinceAll, Mode: control.AttachmentViewer,
	}, &extTerminalStream{}); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ans, err := app.WorkspaceDiff(ctx, scope, control.WorkspaceDiff{SessionID: "sess_example"})
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if b, err := json.Marshal(ans); err != nil || string(b) != `{"repos":[]}` {
		t.Fatalf("diff = %s, %v; want {\\\"repos\\\":[]}", b, err)
	}

	if err := app.PushWorkspace(ctx, scope, control.PushWorkspace{
		SessionID: "sess_example", Path: "dst", Body: strings.NewReader("hello"),
	}); err != nil {
		t.Fatalf("push: %v", err)
	}

	var out strings.Builder
	if err := app.PullWorkspace(ctx, scope, control.PullWorkspace{
		SessionID: "sess_example", Path: "src", Body: &out,
	}); err != nil {
		t.Fatalf("pull: %v", err)
	}
	if out.String() != "x" {
		t.Fatalf("pulled %q, want %q", out.String(), "x")
	}
}

// ---------------------------------------------------------------------------
// external fakes — one per port, no internal/ import
// ---------------------------------------------------------------------------

func externalAttachmentOptions() controlapp.AttachmentOptions {
	return controlapp.AttachmentOptions{
		Authorizer: extAuthorizer{},
		Policy:     extPolicy{},
		Sessions:   extSessions{},
		Transport:  extTransport{},
		Broker:     extBroker{},
		Events:     extEvents{},
		Clock:      extClock(func() time.Time { return time.Unix(0, 0) }),
		IDs:        extIDs{},
	}
}

type extAuthorizer struct{}

func (extAuthorizer) Authorize(context.Context, control.Scope, control.Action, control.Resource) error {
	return nil
}

type extPolicy struct{}

func (extPolicy) AuthorizeAttachment(context.Context, control.Scope, control.Resource, control.AttachmentMode) error {
	return nil
}

type extSessions struct{}

func (extSessions) GetSession(context.Context, control.WorkspaceID, control.SessionID) (control.Session, error) {
	return control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", CreatorID: "act_creator",
		State: control.StateRunning, PoolID: "pool_example", RunnerID: "runner_example",
	}, nil
}
func (extSessions) CreateSession(context.Context, control.WorkspaceID, control.Session) (control.Session, error) {
	return control.Session{}, nil
}
func (extSessions) SessionByIDem(context.Context, control.WorkspaceID, control.ActorID, string) (control.Session, error) {
	return control.Session{}, nil
}
func (extSessions) ListSessions(context.Context, control.WorkspaceID, control.SessionQuery) ([]control.Session, string, error) {
	return nil, "", nil
}
func (extSessions) Transition(context.Context, control.WorkspaceID, control.SessionID, []control.SessionState, control.SessionState, control.TransitionOpts) error {
	return nil
}
func (extSessions) SetSessionSetupHash(context.Context, control.WorkspaceID, control.SessionID, string) error {
	return nil
}
func (extSessions) SetChildExitCode(context.Context, control.WorkspaceID, control.SessionID, int) error {
	return nil
}

type extTransport struct{}

func (extTransport) Connected(control.PoolID, control.RunnerID) bool { return true }

func (extTransport) Dispatch(_ context.Context, _ control.PoolID, _ control.RunnerID, m runner.ToRunner) (runner.FromRunner, error) {
	switch m.RPC.Method {
	case workspace.MethodDiff:
		return extReply(m.RPC.ID, workspace.DiffAnswer{}), nil
	case workspace.MethodPushFiles:
		var c workspace.PushChunk
		_ = json.Unmarshal(m.RPC.Payload, &c)
		return extReply(m.RPC.ID, workspace.PushAck{Seq: c.Seq, Synced: c.Done}), nil
	case workspace.MethodPullFiles:
		var req workspace.PullRequest
		_ = json.Unmarshal(m.RPC.Payload, &req)
		return extReply(m.RPC.ID, workspace.PullChunk{Seq: req.Seq, Data: []byte("x"), Done: true}), nil
	default:
		return runner.FromRunner{}, control.ErrUnsupported
	}
}

func extReply(id uint64, payload any) runner.FromRunner {
	raw, _ := json.Marshal(payload)
	return runner.FromRunner{Type: "session_req", Session: "sess_example",
		RPC: &runner.RPCEnvelope{ID: id, Method: "resp", OK: true, Payload: raw}}
}

type extBroker struct{}

func (extBroker) Attach(context.Context, control.AttachTarget, control.TerminalStream) error {
	return nil
}

type extEvents struct{}

func (extEvents) Record(context.Context, control.Event) error { return nil }

type extClock func() time.Time

func (f extClock) Now() time.Time { return f() }

type extIDs struct{}

func (extIDs) NewSessionID() control.SessionID         { return "sess_example" }
func (extIDs) NewEnvironmentID() control.EnvironmentID { return "env_example" }
func (extIDs) NewEventID() control.EventID             { return "evt_example" }

type extTerminalStream struct{}

func (extTerminalStream) Receive(context.Context) (terminal.ClientMessage, error) {
	return terminal.ClientMessage{}, nil
}
func (extTerminalStream) Send(context.Context, terminal.ServerMessage) error { return nil }
func (extTerminalStream) Close(error) error                                  { return nil }
