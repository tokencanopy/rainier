package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestRefreshUsesStoredRotationEvenWhenItsAccessTokenExpiredDuringSleep(t *testing.T) {
	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["refresh_token"] != "sibling_refresh" {
			t.Error("must exchange sibling's latest refresh token, never original spent token")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		fmt.Fprint(w, `{"access_token":"fresh_access","refresh_token":"fresh_refresh"}`)
	}))
	defer ts.Close()
	cfg := Config{}
	cfg.SetContext("example", Context{Server: ts.URL, Token: "original_access", RefreshToken: "original_refresh"})
	if err := Save(cfg); err != nil {
		t.Fatal(err)
	}
	c := NewClient(cfg)
	cfg.SetContext("example", Context{Server: ts.URL, Token: "sibling_access", RefreshToken: "sibling_refresh", AccessExpiresAt: "2000-01-01T00:00:00Z"})
	if err := Save(cfg); err != nil {
		t.Fatal(err)
	}
	if err := c.RefreshAfterUnauthorized(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requests != 1 || c.Token != "fresh_access" {
		t.Errorf("requests=%d; must return a fresh access credential after sleep", requests)
	}
}

func TestRefreshDoesNotFollowReplacedOrRemovedContext(t *testing.T) {
	for _, replacement := range []string{"removed", "server", "owner"} {
		t.Run(replacement, func(t *testing.T) {
			t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
			requests := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				fmt.Fprint(w, `{"access_token":"new_access","refresh_token":"new_refresh"}`)
			}))
			defer ts.Close()
			cfg := Config{}
			original := Context{Server: ts.URL, OwnerID: "user_original", Token: "original_access", RefreshToken: "original_refresh"}
			cfg.SetContext("example", original)
			if err := Save(cfg); err != nil {
				t.Fatal(err)
			}
			c := NewClient(cfg)
			changed := original
			switch replacement {
			case "removed":
				cfg.RemoveContext("example")
			case "server":
				changed.Server = "https://other.example.invalid"
				cfg.SetContext("example", changed)
			case "owner":
				changed.OwnerID = "user_other"
				cfg.SetContext("example", changed)
			}
			if err := Save(cfg); err != nil {
				t.Fatal(err)
			}
			if err := c.RefreshAfterUnauthorized(context.Background()); !errors.Is(err, ErrLoginAgain) {
				t.Errorf("refresh = %v, want login required after context replacement", err)
			}
			if requests != 0 {
				t.Errorf("sent %d exchanges using a removed/replaced context", requests)
			}
			saved, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			if replacement == "removed" {
				if _, ok := saved.Contexts["example"]; ok {
					t.Error("resurrected removed context")
				}
			} else if saved.Contexts["example"] != changed {
				t.Error("overwrote replacement context")
			}
		})
	}
}
