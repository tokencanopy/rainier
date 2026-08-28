// internal/relay/wsconn.go
package relay

import (
	"context"

	"github.com/coder/websocket"
)

type wsConn struct{ c *websocket.Conn }

func WSConn(c *websocket.Conn) Conn { return &wsConn{c: c} }

func (w *wsConn) Read(ctx context.Context) ([]byte, error) {
	_, b, err := w.c.Read(ctx)
	return b, err
}
func (w *wsConn) Write(ctx context.Context, b []byte) error {
	return w.c.Write(ctx, websocket.MessageText, b)
}
func (w *wsConn) Close() error { return w.c.CloseNow() }
