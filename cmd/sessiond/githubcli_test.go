package main

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestGitHubCLIChildOnly(t *testing.T) {
	for _, args := range [][]string{
		{"pr", "create", "--title", "synthetic change"}, {"api", "user"},
		{"repo", "delete", "example/project", "--yes"},
		{"auth", "token"}, {"extension", "list"},
	} {
		parent := []string{"RAINIER_SESSION=sess_test", "GH_TOKEN=old", "GITHUB_TOKEN=older", "TERM=xterm"}
		before := append([]string(nil), parent...)
		calls := 0
		var output bytes.Buffer
		code := runGitHubCLI(args, parent, func() (string, error) {
			calls++
			return "synthetic-token", nil
		}, func(path string, argv, env []string) error {
			if path != "/usr/local/libexec/rainier/gh" {
				t.Fatal("wrong executable")
			}
			if !reflect.DeepEqual(argv, append([]string{"gh"}, args...)) {
				t.Fatal("argv changed")
			}
			joined := strings.Join(env, "\n")
			if strings.Count(joined, "GH_TOKEN=") != 1 || !strings.Contains(joined, "GH_TOKEN=synthetic-token") {
				t.Fatal("wrong delivery")
			}
			if strings.Contains(joined, "GITHUB_TOKEN=") {
				t.Fatal("alternate token inherited")
			}
			return nil
		}, &output)
		if code != 0 || calls != 1 || output.Len() != 0 {
			t.Fatal("unexpected outcome")
		}
		if !reflect.DeepEqual(parent, before) {
			t.Fatal("parent changed")
		}
	}
}

func TestGitHubCLIDenialNeverExecs(t *testing.T) {
	var output bytes.Buffer
	ran := false
	code := runGitHubCLI([]string{"api", "user"}, []string{"RAINIER_SESSION=sess_test", "GH_TOKEN=old"},
		func() (string, error) { return "", errors.New("synthetic-secret-error") },
		func(string, []string, []string) error { ran = true; return nil }, &output)
	if code == 0 || ran || strings.Contains(output.String(), "synthetic-secret-error") {
		t.Fatal("unsafe denial")
	}
}

func TestGitHubCLIRejectsInvalidTokensWithoutExec(t *testing.T) {
	for _, token := range []string{"", "contains\nnewline", "contains\x7fcontrol", strings.Repeat("x", (16<<10)+1)} {
		t.Run("invalid token", func(t *testing.T) {
			var output bytes.Buffer
			execs := 0
			code := runGitHubCLI([]string{"api", "user"}, []string{"RAINIER_SESSION=sess_test", "GH_TOKEN=inherited"},
				func() (string, error) { return token, nil },
				func(string, []string, []string) error { execs++; return nil }, &output)
			if code != 1 || execs != 0 {
				t.Fatalf("code=%d execs=%d, want refusal without exec", code, execs)
			}
			if output.Len() == 0 || (token != "" && strings.Contains(output.String(), token)) {
				t.Fatal("refusal diagnostic was absent or exposed credential bytes")
			}
		})
	}
}

func TestGitHubCLIOfflineCommandsNeverMint(t *testing.T) {
	for _, args := range [][]string{nil, {"help"}, {"help", "auth"}, {"--help"}, {"-h"}, {"--version"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			mints := 0
			code := runGitHubCLI(args, []string{"RAINIER_SESSION=sess_test"}, func() (string, error) {
				mints++
				return "unexpected", nil
			}, func(path string, argv, env []string) error {
				if path != rainierGitHubCLIPath || !reflect.DeepEqual(argv, append([]string{"gh"}, args...)) {
					t.Fatal("offline argv changed")
				}
				return nil
			}, &bytes.Buffer{})
			if code != 0 || mints != 0 {
				t.Fatalf("code=%d mints=%d, want offline pass-through", code, mints)
			}
		})
	}
}

func TestGitHubCLIUnmanagedPreservesEnvironment(t *testing.T) {
	parent := []string{"GH_TOKEN=inherited", "GITHUB_TOKEN=alternate", "TERM=xterm"}
	before := append([]string(nil), parent...)
	mints := 0
	code := runGitHubCLI([]string{"api", "user"}, parent, func() (string, error) {
		mints++
		return "unexpected", nil
	}, func(_ string, _ []string, env []string) error {
		if !reflect.DeepEqual(env, parent) {
			t.Fatalf("environment = %#v, want unchanged %#v", env, parent)
		}
		return nil
	}, &bytes.Buffer{})
	if code != 0 || mints != 0 || !reflect.DeepEqual(parent, before) {
		t.Fatalf("code=%d mints=%d parent=%#v", code, mints, parent)
	}
}

func TestGitHubCLIExecFailureIsReported(t *testing.T) {
	var output bytes.Buffer
	code := runGitHubCLI([]string{"api", "user"}, nil, func() (string, error) {
		return "unexpected", nil
	}, func(string, []string, []string) error { return errors.New("synthetic exec error") }, &output)
	if code != 1 || !strings.Contains(output.String(), "could not start GitHub CLI") || strings.Contains(output.String(), "synthetic exec error") {
		t.Fatalf("unsafe exec failure: code=%d output=%q", code, output.String())
	}
}
