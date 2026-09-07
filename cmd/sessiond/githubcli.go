package main

import (
	"fmt"
	"io"
	"strings"
	"unicode"
)

const rainierGitHubCLIPath = "/usr/local/libexec/rainier/gh"

// runGitHubCLI obtains a credential only for a managed, credential-bearing gh
// invocation. The token exists only in the replacement process's environment:
// the caller's environment is copied before any entries are removed.
func runGitHubCLI(args, env []string, mint func() (string, error), execProcess func(string, []string, []string) error, stderr io.Writer) int {
	managed := false
	for _, entry := range env {
		if value, ok := strings.CutPrefix(entry, "RAINIER_SESSION="); ok && value != "" {
			managed = true
		}
	}
	offline := len(args) == 0 || args[0] == "help"
	if len(args) == 1 {
		offline = offline || args[0] == "--help" || args[0] == "-h" || args[0] == "--version"
	}
	child := append([]string(nil), env...)
	if managed && !offline {
		token, err := mint()
		invalid := token == "" || len(token) > 16<<10 || strings.IndexFunc(token, func(r rune) bool {
			return r <= 32 || r == 127 || unicode.IsControl(r)
		}) >= 0
		if err != nil || invalid {
			fmt.Fprintln(stderr, "rainier: GitHub credential unavailable; check your connection and workspace sharing with rainier connection ls")
			return 1
		}
		child = make([]string, 0, len(env)+1)
		for _, entry := range env {
			if !strings.HasPrefix(entry, "GH_TOKEN=") && !strings.HasPrefix(entry, "GITHUB_TOKEN=") {
				child = append(child, entry)
			}
		}
		child = append(child, "GH_TOKEN="+token)
	}
	if err := execProcess(rainierGitHubCLIPath, append([]string{"gh"}, args...), child); err != nil {
		fmt.Fprintln(stderr, "rainier: could not start GitHub CLI")
		return 1
	}
	return 0
}
