package attachplane

import (
	"context"
	"encoding/json"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// runnerConn is the runner half of a splice: one socket, raw frames in and
// out. Its method set is exactly internal/relay.Conn's, so a relay.Conn — the
// type runnerd and sessiond splice the other ends of this hop with — is one
// without any conversion, and a host with its own socket type can supply that
// instead.
//
// It is spelled here rather than imported because this package is public and
// internal/relay is not a leaf: it also holds the sessiond-side hub, so
// importing it for these three methods would compile a pty and a terminal
// emulator into every gateway that ever renders an attach. Nothing in this
// plane reads a relay.Frame — the dial-back socket carries whole terminal
// messages, which is what the frame's own payload is.
type runnerConn interface {
	Read(ctx context.Context) ([]byte, error)
	Write(ctx context.Context, b []byte) error
	Close() error
}

// wsRunnerConn is the dial-back websocket as a runnerConn, matching
// relay.WSConn's framing exactly: one text message per frame.
type wsRunnerConn struct{ c *websocket.Conn }

func (w wsRunnerConn) Read(ctx context.Context) ([]byte, error) {
	_, b, err := w.c.Read(ctx)
	return b, err
}

func (w wsRunnerConn) Write(ctx context.Context, b []byte) error {
	return w.c.Write(ctx, websocket.MessageText, b)
}

func (w wsRunnerConn) Close() error { return w.c.CloseNow() }

// splice pumps one live attach both directions until either side ends, then
// closes both. The two halves are typed differently and deliberately so: the
// client speaks whole terminal messages across control.TerminalStream, while
// the runner's dial-back socket is raw frames — and protocol/terminal is the
// wire format on both, so re-encoding between them is lossless. The plane
// still interprets nothing: a message is decoded to be forwarded and for no
// other reason, and none of it is logged.
func splice(ctx context.Context, client control.TerminalStream, runner runnerConn) {
	done := make(chan struct{}, 2)
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			m, err := client.Receive(ctx)
			if err != nil {
				return
			}
			raw, err := json.Marshal(m)
			if err != nil {
				return
			}
			if runner.Write(ctx, raw) != nil {
				return
			}
		}
	}()
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			raw, err := runner.Read(ctx)
			if err != nil {
				return
			}
			var m terminal.ServerMessage
			if json.Unmarshal(raw, &m) != nil {
				// A frame that is not a server message is the runner half
				// breaking the protocol; ending the attach says so, where
				// dropping it would leave a client missing output it has no
				// way to notice. Nothing about the frame is logged.
				return
			}
			if client.Send(ctx, m) != nil {
				return
			}
		}
	}()
	<-done
	_ = client.Close(errAttachEnded)
	runner.Close()
	<-done // let the second pump exit before returning
}
