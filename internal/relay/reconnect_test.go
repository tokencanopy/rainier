package relay

import (
	"context"
	"encoding/base64"
	"errors"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestGuestHandshakeReadLimitBeforeNewline(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	reader, ok := NetConn(a).(interface {
		ReadLimited(context.Context, int) ([]byte, error)
	})
	if !ok {
		t.Fatal("stream cannot bound an unauthenticated handshake read")
	}
	go func() { _, _ = b.Write([]byte(strings.Repeat("x", 4097))) }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := reader.ReadLimited(ctx, 4096)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversize refusal=%v", err)
	}
}

func TestGuestReconnectFrames(t *testing.T) {
	for _, kind := range []string{KindGuestReconnectChallenge, KindGuestReconnectProof, KindGuestReconnectAccepted, KindGuestReconnectRefused} {
		t.Run(kind, func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			writer, reader := NetConn(a), NetConn(b)
			done := make(chan error, 1)
			go func() {
				if err := WriteGuestReconnectFrame(ctx, writer, kind, []byte(`{"protocol":1}`)); err != nil {
					done <- err
					return
				}
				done <- writer.Write(ctx, []byte("normal-frame"))
			}()
			ev, err := ReadGuestReconnectFrame(ctx, reader)
			if err != nil || ev.Kind != kind {
				t.Fatalf("roundtrip: %v %v", ev, err)
			}
			raw, err := reader.Read(ctx)
			if err != nil || string(raw) != "normal-frame" {
				t.Fatal("lost post-handshake bytes")
			}
			if err = <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestGuestReconnectStrictEnvelope(t *testing.T) {
	goodEvent := `{"kind":"guest_reconnect_proof","payload":{"protocol":1}}`
	frame := func(event string) string {
		return `{"t":4,"a":0,"p":"` + base64.StdEncoding.EncodeToString([]byte(event)) + `"}`
	}
	good := frame(goodEvent)
	cases := []string{`null`, good + ` {}`, strings.Replace(good, `"t":4`, `"t":4,"t":4`, 1), strings.Replace(good, `"a":0`, `"a":1`, 1), strings.Replace(good, `"t":4`, `"t":3`, 1), strings.Replace(good, `"t":4`, `"t":null`, 1), strings.Replace(good, `"t":4`, `"t":4,"unknown":0`, 1), frame(`{"kind":"guest_reconnect_proof","payload":null}`), frame(`{"kind":"guest_reconnect_proof","kind":"guest_reconnect_proof","payload":{}}`), frame(`{"kind":"unknown","payload":{}}`), frame(`{"Kind":"guest_reconnect_proof","payload":{}}`), frame(`{"kind":"guest_reconnect_proof","payload":{},"id":1}`)}
	for i, raw := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			go NetConn(a).Write(ctx, []byte(raw))
			if _, err := ReadGuestReconnectFrame(ctx, NetConn(b)); err == nil {
				t.Fatal("accepted invalid envelope")
			}
		})
	}
}
