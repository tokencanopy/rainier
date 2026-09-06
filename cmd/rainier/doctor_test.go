package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/cli"
)

const availableRunner = `{"runners":[{"name":"runner_example","connected":true,"capacity_total":2,"capacity_used":0}]}`

func doctorFixture(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/v0/me":
		fmt.Fprint(w, `{"user":{"id":"usr_example","login":"example","role":"member"}}`)
	case "/v0/runners":
		fmt.Fprint(w, availableRunner)
	case "/v0/environments":
		fmt.Fprint(w, `{"environments":[{"id":"env_example","name":"dev","image":"example:test"}]}`)
	case "/v0/agents":
		fmt.Fprint(w, `{"agents":[{"provider":"codex","status":"logged_in"}]}`)
	default:
		http.NotFound(w, r)
	}
}

func TestDoctorReadiness(t *testing.T) {
	cases := []struct {
		name, path, body, want string
		status                 int
		fail                   bool
	}{
		{name: "ready", want: "PASS runners"},
		{name: "none", path: "/v0/runners", body: `{"runners":[]}`, want: "no runners", fail: true},
		{name: "disconnected", path: "/v0/runners", body: `{"runners":[{"connected":false,"capacity_total":2,"capacity_used":0}]}`, want: "no connected runners", fail: true},
		{name: "full", path: "/v0/runners", body: `{"runners":[{"connected":true,"capacity_total":2,"capacity_used":2}]}`, want: "no free capacity", fail: true},
		{name: "unknown capacity", path: "/v0/runners", body: `{"runners":[{"connected":true}]}`, want: "malformed", fail: true},
		{name: "malformed identity", path: "/v0/me", body: `{}`, want: "malformed", fail: true},
		{name: "malformed runners", path: "/v0/runners", body: `{"bad":"shape"}`, want: "malformed", fail: true},
		{name: "no env", path: "/v0/environments", body: `{"environments":[]}`, want: "WARN environments"},
		{name: "no login", path: "/v0/agents", body: `{"agents":[{"provider":"codex","status":"none"}]}`, want: "coding-agent readiness incomplete"},
		{name: "optional unsupported", path: "/v0/agents", status: 404, want: "endpoint unavailable; compatibility not established"},
		{name: "unauthorized", path: "/v0/me", status: 401, want: "authentication rejected (401)", fail: true},
		{name: "forbidden", path: "/v0/me", status: 403, want: "access forbidden (403)", fail: true},
		{name: "required unsupported", path: "/v0/runners", status: 404, want: "compatibility not established", fail: true},
		{name: "limited", path: "/v0/me", status: 429, want: "Retry-After: 12", fail: true},
		{name: "server error", path: "/v0/me", status: 503, want: "server error (503)", fail: true},
		{name: "bad JSON", path: "/v0/me", body: `this contains tok_example`, want: "malformed", fail: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			probes := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				probes++
				if r.Method != "GET" {
					t.Errorf("unexpected mutation: %s", r.Method)
				}
				if r.Header.Get("Authorization") != "Bearer tok_example" {
					t.Error("missing authentication")
				}
				if r.URL.Path == tc.path {
					if tc.status != 0 {
						w.Header().Set("Retry-After", "12")
						w.WriteHeader(tc.status)
						fmt.Fprint(w, "secret response body tok_example\x1b[31m")
						return
					}
					fmt.Fprint(w, tc.body)
					return
				}
				doctorFixture(w, r)
			}))
			defer ts.Close()
			cfg := cli.Config{}
			cfg.SetContext("example\x1b[31m", cli.Context{Server: ts.URL, Token: "tok_example"})
			var out bytes.Buffer
			err := doctorReport(context.Background(), cfg, &out)
			if (err != nil) != tc.fail || !strings.Contains(out.String(), tc.want) {
				t.Fatalf("err=%v want fail=%v\n%s", err, tc.fail, out.String())
			}
			if strings.Contains(out.String(), "tok_example") || strings.Contains(out.String(), "\x1b") || strings.Contains(out.String(), "secret response body") {
				t.Fatalf("unsafe output: %q", out.String())
			}
			if tc.path == "/v0/me" && tc.fail && probes != 1 {
				t.Errorf("dependent probes after auth failure: %d", probes)
			}
		})
	}
}

func TestDoctorDeadlineIncludesRefresh(t *testing.T) {
	for _, refresh := range []bool{false, true} {
		t.Run(fmt.Sprint(refresh), func(t *testing.T) {
			cancelled := make(chan struct{}, 1)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if refresh && r.URL.Path == "/v0/me" {
					w.WriteHeader(401)
					fmt.Fprint(w, `{"error":{"code":"expired"}}`)
					return
				}
				<-r.Context().Done()
				cancelled <- struct{}{}
			}))
			defer ts.Close()
			cfg := cli.Config{}
			active := cli.Context{Server: ts.URL, Token: "tok_example", Workspace: "ws_example"}
			if refresh {
				active.RefreshToken = "refresh_example"
			}
			cfg.SetContext("example", active)
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			var out bytes.Buffer
			start := time.Now()
			if err := doctorReport(ctx, cfg, &out); err == nil || !strings.Contains(out.String(), "timeout") {
				t.Fatalf("err=%v report=%s", err, out.String())
			}
			if time.Since(start) > time.Second {
				t.Fatal("deadline not respected")
			}
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				t.Fatal("request was not cancelled")
			}
		})
	}
}

func TestDoctorCLIConfigAndExit(t *testing.T) {
	bin := buildCLI(t, "")
	config := filepath.Join(t.TempDir(), "config.json")
	out, code := runCLI(t, bin, config, "doctor")
	if code != 1 || !strings.Contains(out, "FAIL config") || !strings.Contains(out, "rainier login") {
		t.Fatalf("%d %s", code, out)
	}
	if err := os.WriteFile(config, []byte(`{"token":"secret_example", broken`), 0600); err != nil {
		t.Fatal(err)
	}
	out, code = runCLI(t, bin, config, "doctor")
	if code != 1 || !strings.Contains(out, "cannot read config") || strings.Contains(out, "secret_example") {
		t.Fatalf("%d %s", code, out)
	}
	ts := httptest.NewServer(http.HandlerFunc(doctorFixture))
	defer ts.Close()
	data, _ := json.Marshal(map[string]string{"server_url": ts.URL, "token": "tok_example"})
	if err := os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	out, code = runCLI(t, bin, config, "doctor")
	if code != 0 || !strings.Contains(out, "basic session readiness passed") {
		t.Fatalf("%d %s", code, out)
	}
	after, _ := os.ReadFile(config)
	if !bytes.Equal(data, after) {
		t.Fatal("doctor changed configuration")
	}
}

func TestDoctorSafeOutputAndTransport(t *testing.T) {
	for _, tc := range []struct {
		name, server string
		active       cli.Context
		want         string
	}{
		{name: "URL credentials", server: "https://user:secret_password@example.invalid/path?token=secret_query", want: "invalid server URL"},
		{name: "workspace required", server: "https://example.invalid", active: cli.Context{RefreshToken: "secret_refresh"}, want: "FAIL workspace"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := cli.Config{}
			active := tc.active
			active.Server = tc.server
			active.Token = "secret_token"
			cfg.SetContext("example", active)
			var out bytes.Buffer
			if err := doctorReport(context.Background(), cfg, &out); err == nil || !strings.Contains(out.String(), tc.want) {
				t.Fatalf("%v %s", err, out.String())
			}
			if strings.Contains(out.String(), "secret_") {
				t.Fatalf("unsafe: %s", out.String())
			}
		})
	}
	ts := httptest.NewTLSServer(http.HandlerFunc(doctorFixture))
	defer ts.Close()
	cfg := cli.Config{}
	cfg.SetContext("example", cli.Context{Server: ts.URL, Token: "tok_example"})
	var out bytes.Buffer
	if err := doctorReport(context.Background(), cfg, &out); err == nil || !strings.Contains(out.String(), "TLS certificate verification failed") {
		t.Fatalf("TLS %v %s", err, out.String())
	}
	if got := readinessError(&net.DNSError{Err: "secret internal detail", Name: "example.invalid"}); !strings.Contains(got, "DNS lookup failed") || strings.Contains(got, "secret") {
		t.Fatalf("DNS %s", got)
	}
}

func TestDoctorRefreshPreservesOtherContexts(t *testing.T) {
	config := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("RAINIER_CONFIG", config)
	refreshes := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v0/auth/refresh" {
			refreshes++
			if r.Method != "POST" {
				t.Error("refresh method")
			}
			var pair map[string]string
			json.NewDecoder(r.Body).Decode(&pair)
			if pair["refresh_token"] != "refresh_example" {
				t.Error("refresh credential missing")
			}
			fmt.Fprint(w, `{"access_token":"rotated_example","refresh_token":"rotated_refresh_example"}`)
			return
		}
		if r.Header.Get("Rainier-Workspace") != "ws_example" {
			t.Error("workspace scope missing")
		}
		if r.Header.Get("Authorization") == "Bearer stale_example" {
			w.WriteHeader(401)
			fmt.Fprint(w, `{"error":{"code":"expired"}}`)
			return
		}
		if r.Header.Get("Authorization") != "Bearer rotated_example" {
			t.Error("rotated credential missing")
		}
		doctorFixture(w, r)
	}))
	defer ts.Close()
	cfg := cli.Config{}
	cfg.SetContext("other", cli.Context{Server: "https://other.example.invalid", Token: "other_example"})
	cfg.SetContext("example", cli.Context{Server: ts.URL, Token: "stale_example", RefreshToken: "refresh_example", Workspace: "ws_example"})
	if err := cli.Save(cfg); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := doctorReport(context.Background(), cfg, &out); err != nil {
		t.Fatalf("%v %s", err, out.String())
	}
	saved, err := cli.Load()
	if err != nil {
		t.Fatal(err)
	}
	if refreshes != 1 || saved.Current != "example" || saved.Contexts["other"] != cfg.Contexts["other"] || saved.Contexts["example"].Token != "rotated_example" {
		t.Fatal("refresh did not preserve config scope")
	}
	if strings.Contains(out.String(), "rotated_") || strings.Contains(out.String(), "refresh_example") {
		t.Fatalf("leaked refresh: %s", out.String())
	}
}

func TestDoctorRedirectAndBodyBound(t *testing.T) {
	for _, redirect := range []bool{true, false} {
		t.Run(fmt.Sprint(redirect), func(t *testing.T) {
			followed := false
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v0/me" {
					followed = true
					doctorFixture(w, r)
					return
				}
				if redirect {
					http.Redirect(w, r, "/should-not-follow", http.StatusFound)
					return
				}
				fmt.Fprintf(w, `{"user":{"id":"usr_example"},"padding":"%s"}`, strings.Repeat("x", (1<<20)+1))
			}))
			defer ts.Close()
			cfg := cli.Config{}
			cfg.SetContext("example", cli.Context{Server: ts.URL, Token: "tok_example"})
			var out bytes.Buffer
			if err := doctorReport(context.Background(), cfg, &out); err == nil {
				t.Fatalf("unexpected success: %s", out.String())
			}
			if followed {
				t.Fatal("followed redirect/dependent request")
			}
		})
	}
}
