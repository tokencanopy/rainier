package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

// This file is the git credential helper: the same sessiond binary, re-invoked
// by git as `sessiond git-credential-helper <operation>`. Git spawns it with the
// request on stdin and reads the credential back off stdout.
//
// It is the ONLY path a token takes inside a sandbox, and it is a path with no
// branches: the helper asks sessiond over the unix socket, sessiond asks
// controld over the session RPC, and the answer is printed to the pipe git is
// reading and then forgotten when this process exits. Nothing is cached, logged,
// written to a file, or put in an environment variable — a token that reached
// any of those would outlive the git process that needed it, on a volume that
// outlives the session.
//
// Being a subcommand of sessiond rather than a second binary is what makes that
// affordable: the static binary is already in every image, so there is no new
// artifact to build, ship, or keep in step (design §4.1).

const (
	// credentialHelperSubcommand is the argv[1] main dispatches on. It is half
	// of a contract with the gitconfig sessiond writes (credentialHelperCommand
	// names the same string); TestHelperCommandMatchesTheGitconfig holds them
	// together.
	credentialHelperSubcommand = "git-credential-helper"
	// credentialHelperHost is the only host this helper answers for. For
	// anything else it prints nothing and exits 0, which is how a helper tells
	// git "I have no opinion" and lets the next helper — or the user's own
	// prompt — take over.
	credentialHelperHost = "github.com"
	// credentialUsername is the conventional username for token auth. GitHub
	// accepts any non-empty username when the password is a token; using the
	// documented one keeps the request recognizable in GitHub's own logs.
	credentialUsername = "x-access-token"
	// mintMethod is the session RPC this helper turns into. controld answers it
	// (Task 8) out of the credential vault.
	mintMethod = "mint_git_credential"
	// credentialHelperTimeout bounds the whole round trip: socket, sessiond,
	// runnerd, controld, and back. It is sessiond's own budget for that call
	// (agentSocketCallTimeout) plus the wait it may spend on a connection first
	// (agentSocketConnWait) — deliberately the sum rather than the call bound
	// alone, because a helper that gave up first would show the user a bare
	// local timeout in place of the reason sessiond was about to give it, and
	// the reason is the whole point (it names the action to run).
	credentialHelperTimeout = agentSocketCallTimeout + agentSocketConnWait
)

// runCredentialHelper is `sessiond git-credential-helper <op>`. args is
// everything after the subcommand; the return value is the process exit code.
//
// The contract with git (gitcredentials(7)):
//   - stdin is a block of key=value lines terminated by a blank line or EOF.
//   - for "get", stdout is the credential as more key=value lines. Printing
//     nothing means "I have none" and git moves on.
//   - a non-zero exit with something on stderr is what git shows the user, which
//     is why a refusal prints the server's message THERE and prints nothing at
//     all on stdout: a half-answer would make git retry with garbage instead of
//     showing the reason.
func runCredentialHelper(args []string, sockPath string, timeout time.Duration, stdin io.Reader, stdout, stderr io.Writer) int {
	// Read git's request first, whatever the operation. Git writes the whole
	// block before it waits for anything, and exiting without draining it would
	// hand git an EPIPE in place of the clean "no credential" answer that
	// "store" and "erase" expect.
	req := parseCredentialRequest(stdin)

	op := ""
	if len(args) > 0 {
		op = args[0]
	}
	// Only "get" is answered. "store" and "erase" are git offering to cache and
	// to forget a credential — there is nothing to cache (the token is minted
	// per operation and deliberately never persisted) and nothing to forget.
	// Their request block carries the credential itself, which is why nothing in
	// this function logs, echoes or persists what it just read: for those two
	// operations the map above is a token's last stop before this process exits.
	// (git sends "erase" after a 401, so this is the normal path for a revoked
	// token, not a corner.)
	if op != "get" || req["host"] != credentialHelperHost {
		return 0
	}

	token, err := mintCredential(sockPath, timeout)
	if err != nil {
		// Verbatim: for a refusal this is controld's own sentence, and it names
		// the action the user has to run ("rainier login --refresh github").
		// Rewording it here would break the one link between a failed git
		// command and the fix for it.
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "username=%s\npassword=%s\n", credentialUsername, token)
	return 0
}

// parseCredentialRequest reads git's key=value block. Unknown keys are kept
// rather than rejected — git sends more of them with every version (path,
// wwwauth[], capability[]) and a helper that choked on one it had not been
// taught would break on a git upgrade.
//
// The block ends at the first blank line; anything after it is not this
// request's and is ignored.
func parseCredentialRequest(r io.Reader) map[string]string {
	out := map[string]string{}
	sc := bufio.NewScanner(r)
	// A bounded buffer: this is a pipe from another process, and the request is
	// a handful of short lines.
	sc.Buffer(make([]byte, 0, 4<<10), 64<<10)
	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), "\r")
		if line == "" {
			break
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		out[k] = v
	}
	return out
}

// mintCredential performs the one exchange this helper exists for: dial the
// in-sandbox socket, ask for a credential, read the answer.
//
// The token is returned as a value and never touches anything else — no log
// line, no error message, no retry buffer.
func mintCredential(sockPath string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	c, err := net.DialTimeout("unix", sockPath, time.Until(deadline))
	if err != nil {
		// The user sees this through git, so it says what could not be reached
		// rather than quoting a syscall at them.
		return "", fmt.Errorf("rainier: cannot reach sessiond at %s to mint a GitHub credential: %w", sockPath, err)
	}
	defer c.Close()
	c.SetDeadline(deadline)

	if err := json.NewEncoder(c).Encode(socketRequest{Method: mintMethod, Payload: json.RawMessage(`{}`)}); err != nil {
		return "", fmt.Errorf("rainier: asking sessiond for a GitHub credential: %w", err)
	}
	var resp socketResponse
	if err := json.NewDecoder(c).Decode(&resp); err != nil {
		return "", fmt.Errorf("rainier: no answer from sessiond within %s (%w)", timeout, err)
	}
	if !resp.OK {
		if resp.Error != "" {
			return "", errors.New(resp.Error)
		}
		return "", errors.New("rainier: the GitHub credential was refused without a reason")
	}
	var body struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(resp.Payload, &body); err != nil {
		return "", errors.New("rainier: the GitHub credential came back in a shape this helper cannot read")
	}
	if body.Token == "" {
		return "", errors.New("rainier: the GitHub credential came back empty; run `rainier login --refresh github`")
	}
	return body.Token, nil
}
