package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/internal/xfer"
)

// This file is sessiond's boot chain: the staged program that runs in place of
// the agent, the scripts that program runs, and the watcher that reports what
// happened.
//
// The chain generalizes Plan 4's setup wrapper. A session can now ask for up to
// three stages, always in this order:
//
//	setup  — the environment's build (Plan 4). Pre-clone and CACHEABLE, which
//	         is exactly why it runs first and must not depend on repo content.
//	clone  — the repositories controld resolved, cloned onto their session
//	         branches. Generated here from RAINIER_REPOS_B64.
//	init   — the environment's per-boot hook. Post-clone, never cached, so it
//	         may depend on what the clone stage put on disk.
//
// Only after every requested stage succeeds does the chain `exec` the agent.
// A session with none of them is not wrapped at all and boots exactly as it did
// before Plan 5.
//
// SECRET HYGIENE (design §4.3, and the one invariant this whole file exists to
// hold): nothing here ever handles a token. The clone URLs are the plain public
// https ones, the gitconfig names a helper rather than a credential, and the
// only path a token takes in the entire sandbox is the credential helper's
// stdout, straight into the git process that asked for it (see helper.go).
// /workspace is a persistent volume — anything written here outlives the
// session that wrote it.

// The three stage names. They are wire-visible: they travel in
// ControlEvent.Stage and controld composes a session's error text out of them.
const (
	stageSetup = "setup"
	stageClone = "clone"
	stageInit  = "init"
)

// The chain's files, all in the session's own .rainier directory beside the
// setup script Plan 4 put there.
const (
	clonesScriptName = "clones.sh"
	cloneRCName      = "clone.rc"
	initScriptName   = "init.sh"
	initRCName       = "init.rc"
	gitConfigName    = "gitconfig"
)

// workspaceRoot is where the driver mounts the session's volume, and therefore
// where a repo whose Dir is relative (every one controld resolves — Dir is the
// repo's name) is cloned to. It is also the only tree a push or a pull may
// touch, which is why the constant lives in internal/xfer: controld validates a
// transfer path before it ever reaches this process, and a rule only the
// sandbox knows is a rule the sandbox has to be trusted to apply.
const workspaceRoot = xfer.WorkspaceRoot

// credentialHelperCommand is what the workspace gitconfig names as
// credential.helper. It is an absolute path, so git treats the whole string as
// a shell command and appends the operation — which is why the subcommand can
// ride along in it.
//
// The path is a constant because it is a CONTRACT WITH THE SESSION IMAGE:
// /usr/local/bin/sessiond is where the image is required to install it, root
// owned, out of reach of the session user (docs/deploy-gce.md). It is not
// os.Executable() — which would name the same file today, since a path is a
// property of the filesystem and not of the process reading it — because this
// string is written into a file on a PERSISTENT volume and read by a git that
// may run under a later boot, a later image, or a resumed container. Pinning
// the contract is more useful than recording where this particular process
// happened to be started from; an image that installs sessiond elsewhere is
// misconfigured, and gets told so by the first git operation (the runbook's
// criterion-1 notes name that failure).
const credentialHelperCommand = "/usr/local/bin/sessiond " + credentialHelperSubcommand

// cloneTimeoutPerRepo bounds the clone stage, multiplied by the number of
// repositories it has to fetch (design: 600s per repo). Unlike setup and init,
// no one upstream sends a bound for this — the work is sessiond's own, so the
// policy is sessiond's too.
const cloneTimeoutPerRepo = 600 * time.Second

// repoSpec is one repository to clone, decoded from RAINIER_REPOS_B64. The
// JSON tags mirror driver.RepoSpec exactly; the two spellings are the contract
// across the env-var channel and have to stay identical.
type repoSpec struct {
	Owner         string `json:"owner"`
	Name          string `json:"name"`
	BaseBranch    string `json:"base_branch"`
	SessionBranch string `json:"session_branch"`
	Dir           string `json:"dir"`
}

// bootStage is one link of the chain: a script to run, the file its exit code
// lands in, and how long it may take. Name is the wire name of the stage.
type bootStage struct {
	Name       string
	ScriptPath string
	RCPath     string
	Timeout    time.Duration // 0 means no bound
}

// envVar is one variable the chain exports before running anything, so that
// every stage AND the agent that execs at the end see it.
type envVar struct{ Name, Value string }

// bootEnv is the driver's injection, read once at boot. Every field is a raw
// environment value; decoding and validating them is prepareBoot's job.
type bootEnv struct {
	SetupB64     string
	SetupTimeout string
	ReposB64     string
	InitB64      string
	InitTimeout  string

	GitAuthorName  string
	GitAuthorEmail string
}

// bootEnvFromOS reads the variables the driver injects (internal/driver
// docker.go). An ABSENT variable is not an error anywhere in this file: a
// scratch session has no setup, a session with no connectors has no repos, and
// an environment with no hook has no init.
func bootEnvFromOS() bootEnv {
	return bootEnv{
		SetupB64:       os.Getenv("RAINIER_SETUP_B64"),
		SetupTimeout:   os.Getenv("RAINIER_SETUP_TIMEOUT"),
		ReposB64:       os.Getenv("RAINIER_REPOS_B64"),
		InitB64:        os.Getenv("RAINIER_INIT_B64"),
		InitTimeout:    os.Getenv("RAINIER_INIT_TIMEOUT"),
		GitAuthorName:  os.Getenv("RAINIER_GIT_AUTHOR_NAME"),
		GitAuthorEmail: os.Getenv("RAINIER_GIT_AUTHOR_EMAIL"),
	}
}

// any reports whether this environment asks for any stage at all.
func (e bootEnv) any() bool {
	return e.SetupB64 != "" || e.ReposB64 != "" || e.InitB64 != ""
}

// git reports whether git will run in this session — the clone stage, or an
// init hook that is free to use it. It is what gates writing the gitconfig: a
// setup-only session has no identity to configure and no credential to fetch,
// and must boot byte-identically to Plan 4.
func (e bootEnv) git() bool { return e.ReposB64 != "" || e.InitB64 != "" }

// chainStageFmt is one non-final link: run the stage, record its exit code
// where the watcher can read it, and stop the whole chain if it failed.
//
// chainFinalFmt is the last link, which execs the agent on success. It is
// Plan 4's setupWrapperFmt unchanged, and a chain of exactly one setup stage
// composes to exactly Plan 4's program — pinned by TestChainProgram.
//
// Every piece is load-bearing:
//   - `sh <path>` rather than executing the script: the file is written 0755,
//     but the workspace volume's mount options are not this program's to
//     assume, and a stage script is not required to carry a shebang.
//   - `rc=$?` captured immediately, before anything else can overwrite `$?`.
//   - the rc file is written BEFORE the exec, because after a successful exec
//     this shell no longer exists to write anything.
//   - `exec` rather than a plain call: the agent must BE the session's process
//     (pid 1's child, the PTY's owner), not a grandchild behind a shell that
//     would swallow its exit status and its signals.
//   - `"$@"` with a `wrapper` $0: the agent's argv arrives as positional
//     parameters and is passed on byte for byte, so arguments containing
//     spaces, quotes, globs or `$` reach it exactly as controld sent them.
//     Interpolating the argv into this string instead would make every one of
//     those a shell injection.
const (
	chainStageFmt = `sh %s; rc=$?; echo $rc > %s; [ "$rc" -eq 0 ] || exit $rc; `
	chainFinalFmt = `sh %s; rc=$?; echo $rc > %s; [ "$rc" -eq 0 ] && exec "$@"; exit $rc`
)

// chainProgram composes the `sh -c` program for a chain of stages, prefixed by
// the variables every stage and the agent must see exported.
//
// The stage paths are interpolated unquoted, exactly as Plan 4 did: they are
// this program's own constants (or a test's temp dir), never anything that came
// off the wire. Everything that DID come off the wire — repo names, branches,
// an author's name — is quoted at the point it enters a script, not here.
func chainProgram(env []envVar, stages []bootStage) string {
	if len(stages) == 0 {
		return ""
	}
	var b strings.Builder
	for _, v := range env {
		fmt.Fprintf(&b, "export %s=%s; ", v.Name, shQuote(v.Value))
	}
	for i, st := range stages {
		format := chainStageFmt
		if i == len(stages)-1 {
			format = chainFinalFmt
		}
		fmt.Fprintf(&b, format, st.ScriptPath, st.RCPath)
	}
	return b.String()
}

// chainArgv composes the child argv: the chain program, a $0 placeholder, and
// then the real argv verbatim as "$@". With no stages the agent IS the child,
// which is what keeps a session that asked for nothing free of a shell it does
// not need.
func chainArgv(env []envVar, stages []bootStage, argv []string) []string {
	if len(stages) == 0 {
		return argv
	}
	return append([]string{"sh", "-c", chainProgram(env, stages), "wrapper"}, argv...)
}

// setupWrapperArgv composes the Plan 4 wrapper: the chain of exactly one setup
// stage, which is what a session with no repositories and no init hook still
// gets. It is kept as a named entry point because that shape is a contract with
// every already-running sessiond image, and the tests that pin it byte for byte
// (TestSetupWrapperArgv, TestSetupWrapperAgainstRealSh) address it by name.
func setupWrapperArgv(scriptPath, rcPath string, argv []string) []string {
	return chainArgv(nil, []bootStage{{Name: stageSetup, ScriptPath: scriptPath, RCPath: rcPath}}, argv)
}

// prepareBoot lands every stage script this environment asked for, writes the
// workspace gitconfig when git will run, and returns the stages in the order
// the chain runs them together with the variables it exports.
//
// dir is the session's .rainier directory; root is where its volume is mounted
// (both parameters rather than constants so the whole composition is testable
// in a temp directory).
//
// The failure asymmetry between the two script channels is deliberate and comes
// from their contracts:
//   - an undecodable RAINIER_SETUP_B64 is an ERROR, and main treats it as fatal
//     exactly as Plan 4 did. Running the agent in an environment that was never
//     built is the one outcome worse than not starting.
//   - an undecodable RAINIER_REPOS_B64 becomes a clone stage that FAILS LOUDLY
//     (the driver documents this: absent means "nothing to clone", present and
//     unreadable is a failed stage). The user gets a tail naming the variable
//     instead of a container that vanished before it said anything.
func prepareBoot(dir, root string, env bootEnv) ([]bootStage, []envVar, error) {
	if !env.any() {
		return nil, nil, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}

	var vars []envVar
	if env.git() {
		path := dir + "/" + gitConfigName
		if err := os.WriteFile(path, []byte(gitConfig(env.GitAuthorName, env.GitAuthorEmail)), 0o644); err != nil {
			return nil, nil, fmt.Errorf("writing the workspace gitconfig: %w", err)
		}
		// Exported for the WHOLE chain, the agent included: the identity and
		// the credential helper are as much the agent's as the clone stage's,
		// and $HOME is not writable in the sandbox, so ~/.gitconfig is not an
		// option (design §4.3).
		//
		// The three that follow are one rule with three spellings: NOTHING in
		// this sandbox may ask a human for a password. A refused mint (the
		// vault says needs_refresh) makes the credential helper print
		// controld's named action on stderr and exit 1 — and git's response to
		// a helper that produced no credential is to fall through and prompt.
		// The chain runs on the session's PTY (internal/session/proc.go sets
		// Setsid/Setctty), so that prompt finds a real terminal and git BLOCKS:
		// the clone stage would burn its whole 600s-per-repo bound and report
		// "clone timed out" in place of the one sentence that says what to run,
		// and the agent's own `git push` — which nothing bounds at all — would
		// hang forever. With these exported, git dies in milliseconds with
		// "terminal prompts disabled" on the line after the named action, and
		// both land in the 2KB failure tail together.
		//
		//   - GIT_TERMINAL_PROMPT=0 is the prompt itself.
		//   - GIT_ASKPASS="" is git's FIRST choice of prompter, consulted
		//     before core.askPass and SSH_ASKPASS and before the terminal is
		//     considered at all. Empty rather than absent is what matters: git
		//     reads the variable's presence, so an empty value both disables
		//     the askpass helper and shadows a core.askPass a user's own base
		//     image might carry.
		//   - SSH_ASKPASS="" for the same reason on the ssh side, which git
		//     hands off to for an ssh:// remote and which this process does not
		//     otherwise control.
		vars = append(vars,
			envVar{Name: "GIT_CONFIG_GLOBAL", Value: path},
			envVar{Name: "GIT_TERMINAL_PROMPT", Value: "0"},
			envVar{Name: "GIT_ASKPASS", Value: ""},
			envVar{Name: "SSH_ASKPASS", Value: ""},
		)
	}

	var stages []bootStage
	if env.SetupB64 != "" {
		if err := prepareSetup(dir, env.SetupB64); err != nil {
			return nil, nil, err
		}
		stages = append(stages, bootStage{
			Name:       stageSetup,
			ScriptPath: dir + "/" + setupScriptName,
			RCPath:     dir + "/" + setupRCName,
			Timeout:    stageTimeout(env.SetupTimeout),
		})
	}
	if env.ReposB64 != "" {
		repos, err := decodeRepos(env.ReposB64)
		script := clonesScript(root, repos)
		timeout := time.Duration(len(repos)) * cloneTimeoutPerRepo
		if err != nil {
			script = failingScript(fmt.Sprintf("rainier: this session's repository list (RAINIER_REPOS_B64) could not be read: %v", err))
			timeout = cloneTimeoutPerRepo
		}
		if err := writeStageScript(dir, clonesScriptName, cloneRCName, []byte(script)); err != nil {
			return nil, nil, err
		}
		stages = append(stages, bootStage{
			Name:       stageClone,
			ScriptPath: dir + "/" + clonesScriptName,
			RCPath:     dir + "/" + cloneRCName,
			Timeout:    timeout,
		})
	}
	if env.InitB64 != "" {
		script, err := base64.StdEncoding.DecodeString(env.InitB64)
		if err != nil {
			return nil, nil, fmt.Errorf("decoding RAINIER_INIT_B64 (%d bytes): %w", len(env.InitB64), err)
		}
		if err := writeStageScript(dir, initScriptName, initRCName, script); err != nil {
			return nil, nil, err
		}
		stages = append(stages, bootStage{
			Name:       stageInit,
			ScriptPath: dir + "/" + initScriptName,
			RCPath:     dir + "/" + initRCName,
			Timeout:    stageTimeout(env.InitTimeout),
		})
	}
	return stages, vars, nil
}

// writeStageScript lands one stage's script 0755 and clears the rc file a
// previous boot of the same (persistent) workspace volume left behind.
//
// Clearing that file is not housekeeping: the watcher reports the first rc it
// sees, so a stale one would announce the PREVIOUS boot's verdict within a
// second of starting, while this boot's stage was still running.
func writeStageScript(dir, scriptName, rcName string, body []byte) error {
	if err := os.Remove(dir + "/" + rcName); err != nil && !os.IsNotExist(err) {
		return err
	}
	path := dir + "/" + scriptName
	if err := os.WriteFile(path, body, 0o755); err != nil {
		return err
	}
	// WriteFile's mode applies only when it CREATES the file; on a resumed
	// container the script is already there with whatever mode it had.
	return os.Chmod(path, 0o755)
}

// decodeRepos reads RAINIER_REPOS_B64: base64 of the JSON array the driver
// encodes from rwire.Spec.Repos.
func decodeRepos(b64 string) ([]repoSpec, error) {
	blob, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, fmt.Errorf("decoding RAINIER_REPOS_B64 (%d bytes): %w", len(b64), err)
	}
	var repos []repoSpec
	if err := json.Unmarshal(blob, &repos); err != nil {
		return nil, fmt.Errorf("reading the repository list: %w", err)
	}
	return repos, nil
}

// clonesScript generates the clone stage's program: one guarded clone per
// repository, in the order controld resolved them.
//
// The git invocations are the design's verbatim ones (§4.3) and every value
// that came off the wire is shQuoted. That quoting is what makes the
// controller's ruling safe to hold: session names are NOT sanitized into branch
// names, so a git-illegal (or shell-hostile) branch must reach git intact and be
// rejected BY GIT, with git's own error in the failure tail — never quietly
// mangled here, and never executed.
//
// The `[ -d <dir>/.git ]` guard is what makes a resume work. /workspace is a
// persistent volume, so a cold-parked session that starts again finds its repos
// already on disk, and `git clone` into a non-empty directory fails ("already
// exists and is not an empty directory") — without the guard EVERY resume of a
// repo session would fail its clone stage. Skipping is the right answer rather
// than re-cloning: the working tree, its branch and any uncommitted work are the
// session's, and this stage's job is to make sure they exist, not to reset them.
func clonesScript(root string, repos []repoSpec) string {
	var b strings.Builder
	b.WriteString("# generated by sessiond from RAINIER_REPOS_B64 — do not edit.\n")
	for _, r := range repos {
		dir := repoDir(root, r)
		url := "https://github.com/" + r.Owner + "/" + r.Name + ".git"
		slug := r.Owner + "/" + r.Name
		fmt.Fprintf(&b, "if [ -d %s ]; then\n", shQuote(dir+"/.git"))
		fmt.Fprintf(&b, "echo %s\n", shQuote("+ rainier clone: "+dir+" is already cloned; skipping"))
		b.WriteString("else\n")
		fmt.Fprintf(&b, "echo %s\n", shQuote("+ rainier clone: "+slug+" -> "+dir+
			" (branch "+r.SessionBranch+" from "+r.BaseBranch+")"))
		fmt.Fprintf(&b, "git clone --branch %s -- %s %s && git -C %s checkout -b %s\n",
			shQuote(r.BaseBranch), shQuote(url), shQuote(dir), shQuote(dir), shQuote(r.SessionBranch))
		// `A && B` is one AND-OR list, so $? is A's status when A failed and
		// B's when it ran — either way the first thing that went wrong. It is
		// checked explicitly rather than with `set -e`, which POSIX says to
		// ignore for every command of an AND-OR list but the last: a failing
		// clone under `set -e` would fall through to the NEXT repository.
		b.WriteString("rc=$?; [ \"$rc\" -eq 0 ] || exit $rc\n")
		b.WriteString("fi\n")
	}
	return b.String()
}

// repoDir resolves where one repository lands. Dir is controld's to choose (the
// repo's name, deduped) and is relative to the session's workspace; an absolute
// one is honored as given. Falling back to the repo name keeps an empty Dir from
// becoming `git clone <url> ”`.
func repoDir(root string, r repoSpec) string {
	dir := r.Dir
	if dir == "" {
		dir = r.Name
	}
	if strings.HasPrefix(dir, "/") {
		return dir
	}
	return root + "/" + dir
}

// failingScript is a stage that only reports why it cannot run. It is how a
// malformed repo list becomes a failed clone stage with a readable tail instead
// of a boot that dies before anyone can be told.
func failingScript(msg string) string {
	return "echo " + shQuote(msg) + " >&2\nexit 1\n"
}

// shQuote renders s as a single-quoted shell word, which is the only quoting in
// POSIX sh with no escapes inside it at all: every byte between the quotes is
// literal, so the only thing to handle is the quote character itself (close,
// escape one, reopen).
//
// Every value in a generated script goes through this. It is the boundary
// between data that arrived over the wire and a shell that would otherwise run
// it.
func shQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// gitConfig composes the file every git process in the sandbox reads
// (GIT_CONFIG_GLOBAL — $HOME is not writable there, design §4.3).
//
// It names a credential HELPER, never a credential: git runs that helper once
// per operation and the token it prints lives only in that git process. Nothing
// token-shaped is ever written to /workspace, which persists across a cold park
// and would otherwise outlive the session that minted it.
func gitConfig(name, email string) string {
	var b strings.Builder
	b.WriteString("# Written by sessiond at boot. Not a place for secrets: the git\n")
	b.WriteString("# credential is minted per operation by the helper below and never\n")
	b.WriteString("# stored — see `sessiond " + credentialHelperSubcommand + "`.\n")
	b.WriteString("[credential]\n")
	// The empty helper FIRST, which git reads as "reset the helper list": git
	// runs every configured helper in turn, and a session's base image is the
	// user's own — one carrying `credential.helper = store` in /etc/gitconfig
	// would write every token this system mints to a file on a volume that
	// outlives the session. Clearing the list is what makes "the helper's
	// stdout is the only path" true rather than merely intended.
	b.WriteString("\thelper =\n")
	// Unquoted, deliberately: this is a fixed constant of this program, and git
	// reads a credential.helper value beginning with `/` as a shell command.
	b.WriteString("\thelper = " + credentialHelperCommand + "\n")
	if name != "" || email != "" {
		b.WriteString("[user]\n")
		if name != "" {
			b.WriteString("\tname = " + gitConfigValue(name) + "\n")
		}
		if email != "" {
			b.WriteString("\temail = " + gitConfigValue(email) + "\n")
		}
	}
	// push.default = current: a session branch is created locally and has no
	// upstream, so `git push` with git's own default would refuse. "current"
	// pushes it to the same name on the remote, which is the whole point of the
	// branch.
	b.WriteString("[push]\n\tdefault = current\n")
	return b.String()
}

// gitConfigValue renders one value for a git config file. The identity comes
// from a GitHub login and the noreply email built from it, so in practice it is
// alphanumeric — but it is data from a database rendered into a config format,
// and git's own rules make the unquoted form lossy: `#` and `;` start comments,
// and surrounding whitespace is trimmed. Quoting makes the value survive; the
// escapes are the two git recognizes inside quotes, plus the whitespace ones so
// that a value can never break the line it is on.
func gitConfigValue(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`, "\t", `\t`, "\r", "")
	return `"` + r.Replace(s) + `"`
}

// stageTimeout reads a whole-seconds bound out of an env var (the driver injects
// RAINIER_SETUP_TIMEOUT and RAINIER_INIT_TIMEOUT). Anything non-positive or
// unparseable means no bound: sessiond holds no default of its own, because
// controld owns that policy (it sends 900s when an environment declares none,
// design §4.3) and inventing a second one here would mean two components
// disagreeing about when a stage is too slow.
func stageTimeout(v string) time.Duration {
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n <= 0 {
		return 0
	}
	return time.Duration(n) * time.Second
}

// stageTimedOutTail is the diagnostic a timed-out stage carries in place of
// script output, which by definition has no ending to quote.
func stageTimedOutTail(stage string, d time.Duration) string {
	return fmt.Sprintf("%s timed out after %ds", stage, int(d/time.Second))
}

// authRejected reports whether a failure tail is GitHub saying no to the
// credential rather than the repository, the branch or the network being wrong.
//
// This is deliberately a shape match, not a diagnosis. The vault mints
// OPTIMISTICALLY — no GitHub round-trip per mint (design §4.2) — so an observed
// auth failure is the only signal that a token was revoked, and acting on it is
// what turns the NEXT operation's opaque 403 into a named action.
//
// It is one of TWO detectors, and the asymmetry between them is deliberate. The
// clone stage has git's OUTPUT and nothing else — it runs before any agent
// exists, and its verdict is a rc file plus a log tail — so it matches on the
// output's shape, here. The AGENT's git has no watcher at all, but it does talk
// to the credential helper, so that path uses git's own protocol signal instead
// (an "erase" after an authentication failure, see helper.go). Different
// evidence, same conclusion, and both emit the same `credential_rejected`
// control event.
var authRejectedRe = regexp.MustCompile(`(?i)authentication failed|\b401\b|\b403\b`)

// proxyRefusalRe matches a line in which the CONNECT PROXY — not GitHub —
// refused the request. Such a line is DELETED before the auth patterns are
// considered, and that exclusion is a fix for a real corruption, not a
// nicety.
//
// egressd answers a CONNECT it will not tunnel with an HTTP status, and git
// reports it verbatim:
//
//	fatal: unable to access '<url>': CONNECT tunnel failed, response 403
//
// which contains "403" and so matched authRejectedRe outright. A pure
// network-policy failure — a host missing from the session's egress allowlist,
// or (before internal/egress grew its 407 challenge) every git request that
// session ever made — therefore emitted `credential_rejected`, and controld
// flipped the user's GitHub credential to needs_refresh. The user is then told
// to run `rainier login --refresh github`, which cannot fix an allowlist, and
// a credential that was never rejected by anyone is left marked stale in the
// vault. The proxy's own status code says nothing whatsoever about the token
// git was carrying — git had not reached GitHub to present it.
//
// This was accepted at task review as a tolerable false positive costing "one
// refresh". That was wrong on both counts: it fires on a NORMAL failure path
// rather than an exotic one, and its cost is a wrong vault state plus a named
// action that does not work, not a wasted command.
//
// The three wordings are libcurl's, which is the transport git uses: the
// current one, the pre-7.72 one, and the aborted-tunnel one (which carries no
// status code, so it could never have matched anyway — it is here so the set
// reads as "proxy refusals" rather than "the two that happened to bite").
var proxyRefusalRe = regexp.MustCompile(`(?i)CONNECT tunnel failed|HTTP code \d+ from proxy after CONNECT|Proxy CONNECT aborted`)

// authRejected is per-LINE, not whole-tail, which is what keeps the exclusion
// from becoming its own false negative: the clone stage can clone several
// repositories, and one of them failing at the proxy must not suppress
// another's genuine "Authentication failed". Only the proxy's own lines are
// dropped; everything else is still read exactly as before.
func authRejected(tail string) bool {
	for _, line := range strings.Split(tail, "\n") {
		if proxyRefusalRe.MatchString(line) {
			continue
		}
		if authRejectedRe.MatchString(line) {
			return true
		}
	}
	return false
}

// stageFailedEvent composes the control event one failed stage reports.
//
// The setup stage keeps Plan 4's exact wire vocabulary — kind "setup_failed",
// no Stage field — and the new stages use "stage_failed". That split is version
// skew, not taste: sessiond ships INSIDE the session image while runnerd runs on
// the host, so a new sessiond can meet an old runnerd that has never heard of
// "stage_failed" and would drop it. Emitting the legacy name for the one stage
// an old runnerd can produce keeps that pairing working, and it loses nothing
// for the new stages: the clone and init stages only exist when the runnerd's
// own driver injected RAINIER_REPOS_B64/RAINIER_INIT_B64, which an old one never
// does. runnerd accepts both spellings forever (see routeControl).
func stageFailedEvent(stage string, rc int, tail string) relay.ControlEvent {
	if stage == stageSetup {
		return relay.ControlEvent{Kind: "setup_failed", RC: rc, Tail: tail}
	}
	return relay.ControlEvent{Kind: "stage_failed", Stage: stage, RC: rc, Tail: tail}
}

// startStageWatcher runs the watcher on its own goroutine, offering its verdicts
// to the shared control queue. The channel is buffered and never closed: a
// closed one reads as a ready value, which serveConn's select would take for an
// event to deliver.
func startStageWatcher(ctx context.Context, stop func(), stages []bootStage, logPath string, out chan<- []byte) {
	go watchStages(ctx, stop, stages, logPath, stagePollInterval, out)
}

// watchStages follows the chain the wrapper runs: each stage in order, stopping
// at the first failure — the stages after it never ran, so their rc files will
// never appear and waiting for one would hang until its timeout.
//
// CACHE ORCHESTRATION IS UNCHANGED by the new stages, and deliberately so.
// controld snapshots an environment on "setup_done" alone, and this watcher
// still emits that event on setup rc 0 and nowhere else — so a clone or init
// failure can never reach the snapshotting path, and an image is never published
// because of work that happened after the cacheable part was already done.
func watchStages(ctx context.Context, stop func(), stages []bootStage, logPath string, poll time.Duration, out chan<- []byte) {
	for _, st := range stages {
		payloads, cont := watchStage(ctx, stop, st, logPath, poll)
		for _, p := range payloads {
			offerControl(out, p)
		}
		if !cont {
			return
		}
	}
}

// watchStage waits for one stage's verdict and returns the control payloads it
// produces, plus whether the chain continues past it.
//
// It returns nothing at all — and cont false — when ctx ends first, which means
// sessiond is shutting down and there is no one left to tell.
//
// A stage that SUCCEEDS is usually silent. Only setup announces itself
// ("setup_done"), because a finished setup is a fleet-wide fact controld caches
// an image on; a finished clone or init is news to nobody.
func watchStage(ctx context.Context, stop func(), st bootStage, logPath string, poll time.Duration) ([][]byte, bool) {
	rc, ok := awaitStageRC(ctx, stop, st, poll)
	switch {
	case !ok:
		return nil, false
	case rc == 0:
		if st.Name == stageSetup {
			return [][]byte{controlPayload(relay.ControlEvent{Kind: "setup_done"})}, true
		}
		return nil, true
	}

	tail := logTail(logPath, stageTailBytes)
	if rc == -1 {
		tail = stageTimedOutTail(st.Name, st.Timeout)
	}
	var payloads [][]byte
	// Reported BEFORE the failure itself, and only for the clone stage: the
	// clone is the one stage whose git talks to GitHub with a minted token, so
	// it is the only place an auth-shaped error accuses the credential rather
	// than the script. Ordering it first means the vault row is already flipped
	// by the time the user reads why their session failed — `rainier creds`
	// shows needs_refresh in the same breath. controld flips it (Task 8); the
	// event carries no token and names none: controld knows whose credential it
	// minted.
	if st.Name == stageClone && authRejected(tail) {
		payloads = append(payloads, controlPayload(relay.ControlEvent{Kind: "credential_rejected"}))
	}
	return append(payloads, controlPayload(stageFailedEvent(st.Name, rc, tail))), false
}

// awaitStageRC polls for the exit code the wrapper writes for one stage,
// returning it — or -1 when the stage ran past its bound and was killed, or
// ok=false when ctx ended first.
//
// st.Timeout <= 0 means no bound.
func awaitStageRC(ctx context.Context, stop func(), st bootStage, poll time.Duration) (int, bool) {
	var timedOut <-chan time.Time
	if st.Timeout > 0 {
		t := time.NewTimer(st.Timeout)
		defer t.Stop()
		timedOut = t.C
	}
	tick := time.NewTicker(poll)
	defer tick.Stop()
	for {
		// Checked before the first wait, so an rc file that is already there
		// (the stage finished before this goroutine got to it) is noticed at
		// once rather than a poll interval later.
		if rc, ok := readSetupRC(st.RCPath); ok {
			return rc, true
		}
		select {
		case <-tick.C:
		case <-timedOut:
			// SIGTERM the chain: a stage that has run past its bound is not
			// going to be allowed to finish, and leaving it running would leave
			// a container burning a slot on work whose result nobody will
			// accept. rc -1 is the "no exit code exists" marker — the script
			// never got to write one.
			stop()
			return -1, true
		case <-ctx.Done():
			return 0, false
		}
	}
}
