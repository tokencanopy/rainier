package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/tokencanopy/rainier/internal/cli"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// A token can expire while an established terminal is disconnected. Recovery
// must use the hosted refresh flow without losing the rendered-output cursor,
// switching workspace, or replaying output already displayed by this process.
func TestAttachReconnectRefreshesHostedTokenPreservingCursor(t *testing.T) {
	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	var attaches, refreshes atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0/auth/refresh":
			refreshes.Add(1)
			var body struct {
				RefreshToken string `json:"refresh_token"`
			}
			if r.Method != http.MethodPost || json.NewDecoder(r.Body).Decode(&body) != nil || body.RefreshToken != "refresh_synthetic" {
				t.Error("refresh must use the saved hosted refresh token")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			fmt.Fprint(w, `{"access_token":"rotated_access_synthetic","refresh_token":"rotated_refresh_synthetic"}`)
		case "/v0/sessions/sess_example/attach":
			attempt := attaches.Add(1)
			if r.Header.Get("Rainier-Workspace") != "ws_example" {
				t.Error("attach lost the original workspace")
			}
			wantCursor := "17"
			if attempt > 1 {
				wantCursor = "23"
			}
			if r.URL.Query().Get("since") != wantCursor {
				t.Errorf("attempt %d cursor = %q, want %q", attempt, r.URL.Query().Get("since"), wantCursor)
			}
			if attempt == 2 {
				if r.Header.Get("Authorization") != "Bearer access_synthetic" {
					t.Error("expired reconnect must present the original access token")
				}
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			wantAuth := "Bearer access_synthetic"
			if attempt > 2 {
				wantAuth = "Bearer rotated_access_synthetic"
			}
			if r.Header.Get("Authorization") != wantAuth || attempt > 3 {
				t.Errorf("attempt %d did not use the expected credential", attempt)
				w.WriteHeader(http.StatusForbidden)
				return
			}
			c, err := websocket.Accept(w, r, nil)
			if err != nil {
				t.Error(err)
				return
			}
			defer c.CloseNow()
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			var first terminal.ClientMessage
			if err := wsjson.Read(ctx, c, &first); err != nil || first.Type != "resize" {
				t.Errorf("attach resize-first handshake failed: %v", err)
				return
			}
			if attempt == 1 {
				if err := wsjson.Write(ctx, c, terminal.ServerMessage{Type: "output", Seq: 23, Data: []byte("before-disconnect\n")}); err != nil {
					t.Error(err)
				}
				c.Close(websocket.StatusPolicyViolation, "attach lease expired; reattach")
				return
			}
			if err := wsjson.Write(ctx, c, terminal.ServerMessage{Type: "output", Seq: 24, Data: []byte("after-reconnect\n")}); err != nil {
				t.Error(err)
			}
			if err := wsjson.Write(ctx, c, terminal.ServerMessage{Type: "exit", ExitCode: 0}); err != nil {
				t.Error(err)
			}
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	cfg := cli.Config{}
	other := cli.Context{Server: "https://other.example.invalid", Token: "other_synthetic", Workspace: "ws_other"}
	cfg.SetContext("other", other)
	cfg.SetContext("example", cli.Context{Server: ts.URL, Token: "access_synthetic", RefreshToken: "refresh_synthetic", Workspace: "ws_example"})
	if err := cli.Save(cfg); err != nil {
		t.Fatal(err)
	}
	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdinR.Close()
	defer stdinW.Close()
	origStdin := os.Stdin
	os.Stdin = stdinR
	t.Cleanup(func() { os.Stdin = origStdin })

	out, err := captureStdout(t, func() error {
		return attachWithRetrySleep(cfg, "sess_example", 17, func(time.Duration) {})
	})
	if err != nil {
		t.Fatalf("established attach failed instead of refreshing and resuming: %v (attach attempts=%d, refreshes=%d, rendered=%q)", err, attaches.Load(), refreshes.Load(), out)
	}
	if attaches.Load() != 3 || refreshes.Load() != 1 {
		t.Fatalf("attach attempts=%d refreshes=%d; want 3 and 1", attaches.Load(), refreshes.Load())
	}
	for _, text := range []string{"before-disconnect\n", "after-reconnect\n"} {
		if strings.Count(out, text) != 1 {
			t.Errorf("terminal must render %q exactly once: %q", text, out)
		}
	}
	saved, err := cli.Load()
	if err != nil {
		t.Fatal(err)
	}
	if saved.Current != "example" || saved.Contexts["other"] != other || saved.Contexts["example"].Workspace != "ws_example" {
		t.Fatal("refresh changed the selected context or workspace grants")
	}
	if saved.Contexts["example"].Token != "rotated_access_synthetic" || saved.Contexts["example"].RefreshToken != "rotated_refresh_synthetic" {
		t.Fatal("rotated credentials were not persisted for subsequent CLI invocations")
	}
}

func TestAttachHostedAuthRecoveryEdges(t *testing.T) {
	for _, tc := range []struct {
		name string
		// Each entry is the only token accepted for one successful interval.
		intervalTokens                                            []string
		storedRotationBeforeAttach, storedRotationAfterDisconnect bool
		alwaysUnauthorized, forbidden, revoked                    bool
		wantRefreshes                                             int32
		wantLoginAgain                                            bool
	}{
		{name: "initial upgrade needs refresh", intervalTokens: []string{"rotated_access_1_synthetic"}, wantRefreshes: 1},
		{name: "preflight already rotated saved pair", intervalTokens: []string{"rotated_access_1_synthetic"}, storedRotationBeforeAttach: true},
		{name: "another CLI rotates while attached", intervalTokens: []string{"access_synthetic", "rotated_access_1_synthetic"}, storedRotationAfterDisconnect: true},
		{name: "expiry across successive intervals", intervalTokens: []string{"access_synthetic", "rotated_access_1_synthetic", "rotated_access_2_synthetic"}, wantRefreshes: 2},
		{name: "refreshed credential is still unauthorized", alwaysUnauthorized: true, wantRefreshes: 1, wantLoginAgain: true},
		{name: "forbidden does not refresh", forbidden: true},
		{name: "refresh revoked after established stream", intervalTokens: []string{"access_synthetic", "rotated_access_1_synthetic"}, revoked: true, wantRefreshes: 1, wantLoginAgain: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
			var attaches, refreshes, intervals atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v0/auth/refresh":
					n := refreshes.Add(1)
					if n > tc.wantRefreshes {
						t.Error("unexpected extra refresh risks replaying a spent token")
						w.WriteHeader(http.StatusForbidden)
						return
					}
					var body struct {
						RefreshToken string `json:"refresh_token"`
					}
					wantRefresh := "refresh_synthetic"
					if n > 1 {
						wantRefresh = fmt.Sprintf("rotated_refresh_%d_synthetic", n-1)
					}
					if r.Method != http.MethodPost || json.NewDecoder(r.Body).Decode(&body) != nil || body.RefreshToken != wantRefresh {
						t.Error("refresh request did not carry the latest unspent token")
						w.WriteHeader(http.StatusForbidden)
						return
					}
					if tc.revoked {
						w.WriteHeader(http.StatusUnauthorized)
						fmt.Fprint(w, `{"error":{"code":"revoked","message":"refresh_synthetic access_synthetic private_server_detail"}}`)
						return
					}
					json.NewEncoder(w).Encode(cli.TokenPair{AccessToken: fmt.Sprintf("rotated_access_%d_synthetic", n), RefreshToken: fmt.Sprintf("rotated_refresh_%d_synthetic", n)})
				case "/v0/sessions/sess_example/attach":
					n := attaches.Add(1)
					if n > 8 {
						t.Error("unbounded auth retry")
						w.WriteHeader(http.StatusForbidden)
						return
					}
					if r.Header.Get("Rainier-Workspace") != "ws_example" {
						t.Error("workspace changed during auth recovery")
					}
					completed := intervals.Load()
					wantCursor := strconv.Itoa(17 + int(completed))
					if r.URL.Query().Get("since") != wantCursor {
						t.Errorf("cursor=%q want %q", r.URL.Query().Get("since"), wantCursor)
					}
					if tc.forbidden {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					if tc.alwaysUnauthorized || int(completed) >= len(tc.intervalTokens) || r.Header.Get("Authorization") != "Bearer "+tc.intervalTokens[completed] {
						w.WriteHeader(http.StatusUnauthorized)
						return
					}
					c, err := websocket.Accept(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer c.CloseNow()
					ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
					defer cancel()
					var first terminal.ClientMessage
					if err := wsjson.Read(ctx, c, &first); err != nil || first.Type != "resize" {
						t.Errorf("resize-first handshake: %v", err)
						return
					}
					interval := intervals.Add(1)
					if err := wsjson.Write(ctx, c, terminal.ServerMessage{Type: "output", Seq: uint64(17 + interval), Data: []byte(fmt.Sprintf("interval-%d\n", interval))}); err != nil {
						t.Error(err)
						return
					}
					if int(interval) < len(tc.intervalTokens) {
						c.Close(websocket.StatusGoingAway, "synthetic disconnect")
						return
					}
					if err := wsjson.Write(ctx, c, terminal.ServerMessage{Type: "exit", ExitCode: 0}); err != nil {
						t.Error(err)
					}
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer ts.Close()
			cfg := cli.Config{}
			cfg.SetContext("example", cli.Context{Server: ts.URL, Token: "access_synthetic", RefreshToken: "refresh_synthetic", Workspace: "ws_example"})
			if err := cli.Save(cfg); err != nil {
				t.Fatal(err)
			}
			rotateStored := func() {
				stored, err := cli.Load()
				if err != nil {
					t.Fatal(err)
				}
				active := stored.Contexts["example"]
				active.Token, active.RefreshToken = "rotated_access_1_synthetic", "rotated_refresh_1_synthetic"
				stored.UpdateContext("example", active)
				if err := cli.Save(stored); err != nil {
					t.Fatal(err)
				}
			}
			if tc.storedRotationBeforeAttach {
				rotateStored()
			}
			stdinR, stdinW, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer stdinR.Close()
			defer stdinW.Close()
			origStdin := os.Stdin
			os.Stdin = stdinR
			t.Cleanup(func() { os.Stdin = origStdin })
			rotated := false
			out, err := captureStdout(t, func() error {
				return attachWithRetrySleep(cfg, "sess_example", 17, func(time.Duration) {
					if tc.storedRotationAfterDisconnect && !rotated {
						rotateStored()
						rotated = true
					}
				})
			})
			if refreshes.Load() != tc.wantRefreshes {
				t.Errorf("refreshes=%d want %d (attaches=%d)", refreshes.Load(), tc.wantRefreshes, attaches.Load())
			}
			if tc.wantLoginAgain {
				if !errors.Is(err, cli.ErrLoginAgain) {
					t.Errorf("error=%v want actionable ErrLoginAgain", err)
				}
			} else if tc.forbidden {
				if err == nil || attaches.Load() != 1 {
					t.Errorf("403 should terminate after one attach: err=%v attempts=%d", err, attaches.Load())
				}
			} else {
				if err != nil {
					t.Errorf("recovery failed: %v", err)
				}
				if int(intervals.Load()) != len(tc.intervalTokens) {
					t.Errorf("completed intervals=%d want %d", intervals.Load(), len(tc.intervalTokens))
				}
			}
			if tc.alwaysUnauthorized && attaches.Load() > 2 {
				t.Errorf("refreshed 401 retried %d times; want bounded single recovery", attaches.Load())
			}
			for i := int32(1); i <= intervals.Load(); i++ {
				if strings.Count(out, fmt.Sprintf("interval-%d\n", i)) != 1 {
					t.Errorf("interval %d output lost or duplicated: %q", i, out)
				}
			}
			combined := out + fmt.Sprint(err)
			for _, secret := range []string{"access_synthetic", "refresh_synthetic", "rotated_access_1_synthetic", "rotated_refresh_1_synthetic", "private_server_detail"} {
				if strings.Contains(combined, secret) {
					t.Errorf("credential or private server detail leaked: %q", combined)
				}
			}
		})
	}
}
