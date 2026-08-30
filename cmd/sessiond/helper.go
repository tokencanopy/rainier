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
	// credentialHelperProtocol is the only scheme it answers for. The boot
	// chain writes https:// remotes and nothing else, but an agent — or a
	// .gitmodules in somebody's repository — can name any remote it likes, and
	// git asks the helper for `http://github.com/...` with protocol=http. A
	// helper that ignored this key would hand the user's token to a cleartext
	// request. An ABSENT protocol is refused for the same reason: this helper
	// only ever mints for a request it can see is encrypted.
	credentialHelperProtocol = "https"
	// credentialUsername is the conventional username for token auth. GitHub
	// accepts any non-empty username when the password is a token; using the
	// documented one keeps the request recognizable in GitHub's own logs.
	credentialUsername = "x-access-token"
	// mintMethod is the session RPC this helper turns into. controld answers it
	// (Task 8) out of the credential vault.
	mintMethod = "mint_git_credential"
	// credentialRejectedMethod is the socket call an "erase" turns into. Unlike
	// mintMethod it does not travel upstream as a request: sessiond answers it
	// locally by queueing the `credential_rejected` control event, which is the
	// same event the clone-stage watcher emits and which controld already acts
	// on (internal/controld/runners.go). See reportCredentialRejected.
	credentialRejectedMethod = "report_credential_rejected"
	// credentialHelperTimeout bounds the whole round trip: socket, sessiond,
	// runnerd, controld, and back. It is sessiond's own budget for that call
	// (agentSocketCallTimeout) plus the wait it may spend on a connection first
	// (agentSocketConnWait) — deliberately the sum rather than the call bound
	// alone, because a helper that gave up first would show the user a bare
	// local timeout in place of the reason sessiond was about to give it, and
	// the reason is the whole point (it names the action to run).
	credentialHelperTimeout = agentSocketCallTimeout + agentSocketConnWait
	// credentialRejectedTimeout bounds the erase report instead, and is much
	// shorter on purpose. Git WAITS for the erase helper to exit before it
	// finishes failing, so a long bound here would turn a broken socket into
	// twenty extra seconds on top of a git command that has already failed —
	// and unlike a mint there is nothing to show for the wait: the report is
	// fire-and-forget and its outcome changes nothing the user will see.
	credentialRejectedTimeout = 5 * time.Second
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
	// A request for anything but https://github.com is one this helper has no
	// opinion about: nothing on stdout, exit 0, and git moves on to the next
	// helper or to its own prompt. That is true for EVERY operation, which is
	// why the check is here rather than inside the get arm — an erase for
	// gitlab.com is no more this vault's business than a get for it is.
	if req["host"] != credentialHelperHost || req["protocol"] != credentialHelperProtocol {
		return 0
	}

	switch op {
	case "get":
		token, err := mintCredential(sockPath, timeout)
		if err != nil {
			// Verbatim: for a refusal this is controld's own sentence, and it
			// names the action the user has to run ("rainier login --refresh
			// github"). Rewording it here would break the one link between a
			// failed git command and the fix for it.
			fmt.Fprintln(stderr, err)
			return 1
		}
		fmt.Fprintf(stdout, "username=%s\npassword=%s\n", credentialUsername, token)
		return 0

	case "erase":
		// Git's own signal that the credential it was given did not work: it
		// calls "erase" after an authentication failure, with the rejected
		// credential in the request block. Nothing here is stored or forgotten
		// — the token is minted per operation and deliberately never persisted
		// — but the FACT is worth reporting, because it is the only way the
		// fleet learns that a token was revoked while a session was running.
		//
		// TWO DETECTORS, ONE EVENT, deliberately. The clone stage reads git's
		// OUTPUT and matches its shape (authRejected, gitchain.go): it is the
		// only place that has the output, and it runs before any agent exists.
		// This arm reads git's own PROTOCOL signal instead, which is the only
		// thing available for the agent's git — nothing watches the agent's
		// PTY, and its `git push` is where a mid-session revocation actually
		// shows up. They are different evidence for the same fact, and both
		// land on the same `credential_rejected` control event.
		//
		// Reported fire-and-forget, and a failure to report is not this
		// helper's to surface: git has already failed the user's command with
		// its own message, and turning a bookkeeping call into a second error
		// on top of it would say nothing they can act on. Stdout stays empty
		// and the exit stays 0, exactly as for "store".
		reportCredentialRejected(sockPath, min(timeout, credentialRejectedTimeout))
		return 0

	default:
		// "store" (git offering to cache) and anything a future git invents.
		// Their request block carries the credential itself, which is why
		// nothing in this function logs, echoes or persists what it just read:
		// for those operations the parsed map above is a token's last stop
		// before this process exits.
		return 0
	}
}

// reportCredentialRejected tells sessiond that git rejected the credential it
// was handed, so the vault can flip to needs_refresh and the NEXT operation
// gets a named action instead of another opaque 403.
//
// It is deliberately silent about everything, including its own failures: the
// caller is a helper git is waiting on, git has already failed the user's
// command, and a second message would only compete with the first. It also
// deliberately sends NO payload — whose credential it was is the session's own
// answer upstream, and the erase request this helper just read carries the
// rejected token, which has no business on any wire but the one it arrived on.
func reportCredentialRejected(sockPath string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	c, err := net.DialTimeout("unix", sockPath, time.Until(deadline))
	if err != nil {
		return
	}
	defer c.Close()
	c.SetDeadline(deadline)
	if err := json.NewEncoder(c).Encode(socketRequest{Method: credentialRejectedMethod}); err != nil {
		return
	}
	// The answer is read and dropped. Nothing here acts on it — but the socket
	// protocol is one request and one response per connection, and hanging up
	// before the response would leave sessiond logging a broken pipe for a call
	// that in fact succeeded.
	var resp socketResponse
	_ = json.NewDecoder(c).Decode(&resp)
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
