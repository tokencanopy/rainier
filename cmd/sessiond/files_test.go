package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"rainier/internal/xfer"
)

// ---------------------------------------------------------------------------
// the git shim
//
// The diff handler's contract is an exact pair of git invocations (design
// §4.6), so most of these tests want to SEE the argv rather than the result of
// running it. A shell script named `git` at the front of PATH records every
// invocation and answers whatever the test told it to.
//
// The real-git test at the bottom is the other half: a shim can be made to
// agree with any wrong idea of what git does, so one test drives the actual
// binary against real repositories.
// ---------------------------------------------------------------------------

type gitShim struct {
	log string // one line per invocation: the arguments, space-joined
	out string // what `git diff` prints on stdout
}

// installGitShim puts a fake git at the front of PATH for the test's duration.
func installGitShim(t *testing.T) *gitShim {
	t.Helper()
	dir := t.TempDir()
	sh := &gitShim{log: filepath.Join(dir, "invocations"), out: filepath.Join(dir, "stdout")}
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$GIT_SHIM_LOG"
case "$*" in
  *" diff "*)
    [ -f "$GIT_SHIM_OUT" ] && cat "$GIT_SHIM_OUT"
    [ -n "$GIT_SHIM_ERR" ] && printf '%s' "$GIT_SHIM_ERR" >&2
    exit "${GIT_SHIM_DIFF_RC:-0}" ;;
esac
[ -n "$GIT_SHIM_SLEEP" ] && sleep "$GIT_SHIM_SLEEP"
[ -n "$GIT_SHIM_ERR" ] && printf '%s' "$GIT_SHIM_ERR" >&2
exit "${GIT_SHIM_FETCH_RC:-0}"
`
	if err := os.WriteFile(filepath.Join(dir, "git"), []byte(script), 0o755); err != nil {
		t.Fatalf("write git shim: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("GIT_SHIM_LOG", sh.log)
	t.Setenv("GIT_SHIM_OUT", sh.out)
	return sh
}

// answer makes the shim's `git diff` print body.
func (g *gitShim) answer(t *testing.T, body string) {
	t.Helper()
	if err := os.WriteFile(g.out, []byte(body), 0o644); err != nil {
		t.Fatalf("write shim stdout: %v", err)
	}
}

// invocations returns every git command line the shim saw, in order.
func (g *gitShim) invocations(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile(g.log)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read shim log: %v", err)
	}
	return strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
}

func testRepo(dir string) repoSpec {
	return repoSpec{Owner: "acme", Name: "widget", BaseBranch: "main", SessionBranch: "rainier/x", Dir: dir}
}

// runDiff invokes the handler the way the RPC dispatcher does and decodes its
// answer.
func runDiff(t *testing.T, d *differ) xfer.DiffAnswer {
	t.Helper()
	out, err := d.handle(nil)
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	b, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal diff answer: %v", err)
	}
	var ans xfer.DiffAnswer
	if err := json.Unmarshal(b, &ans); err != nil {
		t.Fatalf("decode diff answer: %v; body=%s", err, b)
	}
	return ans
}

// ---------------------------------------------------------------------------
// diff
// ---------------------------------------------------------------------------

// TestDiffComposesTheDesignsGitInvocations pins the two commands and their
// order. They are the design's verbatim ones (§4.6) and a session's whole diff
// surface is whatever they print.
func TestDiffComposesTheDesignsGitInvocations(t *testing.T) {
	shim := installGitShim(t)
	shim.answer(t, " main.go | 2 +-\n 1 file changed\n")

	root := t.TempDir()
	d := newDiffer(root, []repoSpec{testRepo("widget")}, nil, root+"/.rainier/gitconfig")
	ans := runDiff(t, d)

	got := shim.invocations(t)
	want := []string{
		"-C " + root + "/widget fetch -q origin main",
		"-C " + root + "/widget diff --stat origin/main...HEAD",
	}
	if len(got) != len(want) {
		t.Fatalf("git ran %d times %v, want %v", len(got), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("invocation %d = %q, want %q", i, got[i], want[i])
		}
	}

	if len(ans.Repos) != 1 {
		t.Fatalf("repos = %+v, want one", ans.Repos)
	}
	r := ans.Repos[0]
	if r.Repo != "acme/widget" || r.BaseBranch != "main" || r.SessionBranch != "rainier/x" {
		t.Fatalf("repo answer = %+v", r)
	}
	if r.Stat != " main.go | 2 +-\n 1 file changed\n" {
		t.Fatalf("stat = %q, want git's own output verbatim", r.Stat)
	}
}

// TestDiffPassesTheWorkspaceGitconfig: the credential helper and the identity
// live in that file, and sessiond's own process environment does not have it —
// only the boot chain exports it. A diff whose fetch cannot authenticate is a
// diff that fails on every private repository.
func TestDiffPassesTheWorkspaceGitconfig(t *testing.T) {
	installGitShim(t)
	root := t.TempDir()
	d := newDiffer(root, []repoSpec{testRepo("widget")}, nil, "/workspace/.rainier/gitconfig")

	cmd := d.gitCommand(t.Context(), root+"/widget", "status")
	var found string
	for _, kv := range cmd.Env {
		if strings.HasPrefix(kv, "GIT_CONFIG_GLOBAL=") {
			found = kv
		}
	}
	if found != "GIT_CONFIG_GLOBAL=/workspace/.rainier/gitconfig" {
		t.Fatalf("GIT_CONFIG_GLOBAL in the diff's env = %q, want the workspace gitconfig", found)
	}
}

// TestDiffWithNoRepos: a scratch session answers an empty array rather than
// failing — "this session has nothing to diff" is an answer, not an error.
func TestDiffWithNoRepos(t *testing.T) {
	shim := installGitShim(t)
	d := newDiffer(t.TempDir(), nil, nil, "")
	ans := runDiff(t, d)
	if len(ans.Repos) != 0 {
		t.Fatalf("repos = %+v, want empty", ans.Repos)
	}
	if inv := shim.invocations(t); len(inv) != 0 {
		t.Fatalf("git ran %v for a session with no repositories", inv)
	}
	b, err := json.Marshal(ans)
	if err != nil || string(b) != `{"repos":[]}` {
		t.Fatalf("empty answer = %s, %v", b, err)
	}
}

// TestDiffCapsOneRepositorysOutput: a repository with an enormous stat must
// not be able to put an enormous frame on the control channel, and what the
// user sees has to say it was cut.
func TestDiffCapsOneRepositorysOutput(t *testing.T) {
	shim := installGitShim(t)
	shim.answer(t, strings.Repeat("x", 200<<10))

	root := t.TempDir()
	d := newDiffer(root, []repoSpec{testRepo("widget")}, nil, "")
	ans := runDiff(t, d)

	stat := ans.Repos[0].Stat
	if len(stat) > xfer.StatBytes {
		t.Fatalf("stat is %d bytes, want at most %d", len(stat), xfer.StatBytes)
	}
	if !strings.Contains(stat, "truncated") {
		t.Fatalf("a truncated stat must say so; got the last 40 bytes %q", stat[len(stat)-40:])
	}
}

// TestDiffBoundsOneRepositorysTime: git talks to the network here, and an
// unbounded handler would hold a control-channel goroutine (and the user's
// request) for as long as GitHub cared to stall.
func TestDiffBoundsOneRepositorysTime(t *testing.T) {
	installGitShim(t)
	t.Setenv("GIT_SHIM_SLEEP", "5")

	root := t.TempDir()
	d := newDiffer(root, []repoSpec{testRepo("widget")}, nil, "")
	d.timeout = 150 * time.Millisecond
	d.waitDelay = 200 * time.Millisecond

	start := time.Now()
	_, err := d.handle(nil)
	if err == nil {
		t.Fatal("diff of a stalled repository returned nil")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("error = %v, want it to name the timeout", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("diff took %s; the per-repo bound did not fire", elapsed)
	}
}

// TestDiffReportsGitsOwnFailure: when git fails, its stderr is the only thing
// that explains why (a branch that does not exist, a remote that is gone), so
// it travels with the error.
func TestDiffReportsGitsOwnFailure(t *testing.T) {
	installGitShim(t)
	t.Setenv("GIT_SHIM_FETCH_RC", "128")
	t.Setenv("GIT_SHIM_ERR", "fatal: couldn't find remote ref main")

	d := newDiffer(t.TempDir(), []repoSpec{testRepo("widget")}, nil, "")
	_, err := d.handle(nil)
	if err == nil {
		t.Fatal("diff with a failing git returned nil")
	}
	if !strings.Contains(err.Error(), "acme/widget") || !strings.Contains(err.Error(), "couldn't find remote ref") {
		t.Fatalf("error = %v, want it to name the repository and quote git", err)
	}
}

// TestDiffReportsAnUnreadableRepoList: RAINIER_REPOS_B64 that would not decode
// already failed this session's clone stage; the diff says so rather than
// reporting an empty workspace as "no repositories".
func TestDiffReportsAnUnreadableRepoList(t *testing.T) {
	installGitShim(t)
	d := newDiffer(t.TempDir(), nil, fmt.Errorf("reading the repository list: unexpected end of JSON input"), "")
	if _, err := d.handle(nil); err == nil {
		t.Fatal("diff with an undecodable repo list returned nil")
	}
}

// TestDiffAgainstRealGit is the shim's counterweight: real repositories, a real
// commit on a session branch, and git's own --stat for it.
func TestDiffAgainstRealGit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git on PATH; skipping the real-git diff test")
	}
	root := t.TempDir()
	origin := filepath.Join(root, "origin.git")
	work := filepath.Join(root, "widget")

	git := func(dir string, args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(),
			"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.invalid",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.invalid")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s in %s: %v\n%s", strings.Join(args, " "), dir, err, out)
		}
	}

	git(root, "init", "-q", "--bare", "--initial-branch=main", origin)
	git(root, "clone", "-q", origin, work)
	if err := os.WriteFile(filepath.Join(work, "base.txt"), []byte("one\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	git(work, "add", "-A")
	git(work, "commit", "-qm", "base")
	git(work, "push", "-q", "origin", "main")
	git(work, "checkout", "-qb", "rainier/session")
	if err := os.WriteFile(filepath.Join(work, "base.txt"), []byte("one\ntwo\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	git(work, "add", "-A")
	git(work, "commit", "-qm", "session work")

	d := newDiffer(root, []repoSpec{{
		Owner: "acme", Name: "widget", BaseBranch: "main", SessionBranch: "rainier/session", Dir: "widget",
	}}, nil, "")
	ans := runDiff(t, d)
	if len(ans.Repos) != 1 {
		t.Fatalf("repos = %+v", ans.Repos)
	}
	if !strings.Contains(ans.Repos[0].Stat, "base.txt") {
		t.Fatalf("stat = %q, want it to mention the changed file", ans.Repos[0].Stat)
	}
}

// ---------------------------------------------------------------------------
// push / pull
// ---------------------------------------------------------------------------

// newTestTransfers builds a transfer table rooted in a temp workspace, staging
// in another temp directory (production stages in /tmp), with a small cap so
// the size rules can be tested without moving a quarter of a gigabyte.
func newTestTransfers(t *testing.T, max int64) (*fileTransfers, string) {
	t.Helper()
	root := t.TempDir()
	ft := newFileTransfers(root, t.TempDir())
	if max > 0 {
		ft.max = max
	}
	return ft, root
}

// call runs one handler the way the dispatcher does: JSON in, value out.
func callPush(t *testing.T, ft *fileTransfers, c xfer.PushChunk) (xfer.PushAck, error) {
	t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal chunk: %v", err)
	}
	out, err := ft.handlePush(b)
	if err != nil {
		return xfer.PushAck{}, err
	}
	ack, ok := out.(xfer.PushAck)
	if !ok {
		t.Fatalf("push answered %T, want an ack", out)
	}
	return ack, nil
}

func callPull(t *testing.T, ft *fileTransfers, req xfer.PullRequest) (xfer.PullChunk, error) {
	t.Helper()
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	out, err := ft.handlePull(b)
	if err != nil {
		return xfer.PullChunk{}, err
	}
	chunk, ok := out.(xfer.PullChunk)
	if !ok {
		t.Fatalf("pull answered %T, want a chunk", out)
	}
	return chunk, nil
}

// archiveOf tars a directory the way the CLI would.
func archiveOf(t *testing.T, dir string) []byte {
	t.Helper()
	var buf bytes.Buffer
	if _, err := xfer.TarGz(&buf, dir, xfer.MaxBytes); err != nil {
		t.Fatalf("TarGz: %v", err)
	}
	return buf.Bytes()
}

// pushBlob streams blob to dest in chunks of size chunk, returning every ack.
func pushBlob(t *testing.T, ft *fileTransfers, id, dest string, blob []byte, chunk int) []xfer.PushAck {
	t.Helper()
	var acks []xfer.PushAck
	for seq := 0; ; seq++ {
		lo := seq * chunk
		hi := min(lo+chunk, len(blob))
		ack, err := callPush(t, ft, xfer.PushChunk{
			Xfer: id, Path: dest, Seq: seq, Data: blob[lo:hi], Done: hi >= len(blob)})
		if err != nil {
			t.Fatalf("push chunk %d: %v", seq, err)
		}
		acks = append(acks, ack)
		if hi >= len(blob) {
			return acks
		}
	}
}

// TestPushRoundTrip: a directory tarred on one side lands whole on the other.
func TestPushRoundTrip(t *testing.T) {
	ft, root := newTestTransfers(t, 0)
	src := t.TempDir()
	if err := os.MkdirAll(filepath.Join(src, "pkg"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "pkg", "a.txt"), []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	blob := archiveOf(t, src)

	acks := pushBlob(t, ft, "x1", "widget/vendor", blob, 4096)
	if last := acks[len(acks)-1]; !last.Synced {
		t.Fatal("the final ack must report the staging file fsynced")
	}
	for i, ack := range acks {
		if ack.Seq != i {
			t.Fatalf("ack %d echoed seq %d", i, ack.Seq)
		}
	}

	got, err := os.ReadFile(filepath.Join(root, "widget", "vendor", "pkg", "a.txt"))
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if string(got) != "hello\n" {
		t.Fatalf("extracted content = %q", got)
	}
	if n := ft.open(); n != 0 {
		t.Fatalf("%d transfers still open after the last chunk", n)
	}
	if files := stagingFiles(t, ft.tmp); len(files) != 0 {
		t.Fatalf("staging files left behind: %v", files)
	}
}

// TestPushAcksSyncedEveryEighthChunk pins the durability signal: the client is
// entitled to believe everything through a synced ack survived a crash, and
// nothing else in this protocol makes that claim.
func TestPushAcksSyncedEveryEighthChunk(t *testing.T) {
	ft, _ := newTestTransfers(t, 0)
	blob := archiveOf(t, t.TempDir())
	// One byte per chunk, so the chunk count is the test's to choose.
	big := bytes.Repeat([]byte{0}, 20)
	_ = blob

	var synced []int
	for seq := 0; seq < len(big); seq++ {
		ack, err := callPush(t, ft, xfer.PushChunk{Xfer: "x2", Path: "dst", Seq: seq, Data: big[seq : seq+1]})
		if err != nil {
			t.Fatalf("chunk %d: %v", seq, err)
		}
		if ack.Synced {
			synced = append(synced, ack.Seq)
		}
	}
	want := []int{7, 15}
	if len(synced) != len(want) {
		t.Fatalf("synced acks at %v, want %v", synced, want)
	}
	for i := range want {
		if synced[i] != want[i] {
			t.Fatalf("synced acks at %v, want %v", synced, want)
		}
	}
	// Abandoned mid-transfer: the staging file is still open, which is what
	// the sweep below has to clean up.
	if n := ft.open(); n != 1 {
		t.Fatalf("open transfers = %d, want the unfinished one", n)
	}
}

// TestPushRefusesEscapingDestinations is the whole point of validating a path
// inside the sandbox: controld checked it too, but controld is not the process
// about to call open(2).
func TestPushRefusesEscapingDestinations(t *testing.T) {
	for _, dest := range []string{"../escape", "/etc/cron.d", "widget/../../escape", "", "/workspaceother/x"} {
		t.Run(dest, func(t *testing.T) {
			ft, _ := newTestTransfers(t, 0)
			blob := archiveOf(t, t.TempDir())
			_, err := callPush(t, ft, xfer.PushChunk{Xfer: "x3", Path: dest, Seq: 0, Data: blob, Done: true})
			if err == nil {
				t.Fatalf("push to %q was accepted", dest)
			}
			if files := stagingFiles(t, ft.tmp); len(files) != 0 {
				t.Fatalf("a refused push left staging files: %v", files)
			}
		})
	}
}

// hostileArchive builds a gzipped tar by hand: an innocent file followed by a
// symlink to an absolute path.
//
// It cannot go through xfer.TarGz, which refuses that symlink at PACK time —
// which is the point of this helper. The far end is not trusted to have used
// our packer, or any packer, so the extract-side rule has to be provable on
// its own.
func hostileArchive(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	body := []byte("x")
	if err := tw.WriteHeader(&tar.Header{
		Name: "innocent.txt", Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&tar.Header{
		Name: "escape", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd",
	}); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestPushRefusesAHostileArchiveWithoutHalfExtracting: the archive is the far
// end's, and it is validated whole before a single entry lands.
func TestPushRefusesAHostileArchiveWithoutHalfExtracting(t *testing.T) {
	ft, root := newTestTransfers(t, 0)

	blob := hostileArchive(t)

	_, err := callPush(t, ft, xfer.PushChunk{Xfer: "x4", Path: "dst", Seq: 0, Data: blob, Done: true})
	if err == nil {
		t.Fatal("a hostile archive was accepted")
	}
	if _, err := os.Lstat(filepath.Join(root, "dst", "innocent.txt")); err == nil {
		t.Fatal("the refused archive still extracted a file")
	}
	if files := stagingFiles(t, ft.tmp); len(files) != 0 {
		t.Fatalf("a failed push left staging files: %v", files)
	}
	if n := ft.open(); n != 0 {
		t.Fatalf("a failed push left %d transfers open", n)
	}
}

// TestPushReAcksARepeatedChunk: a chunk whose ACK was lost is re-sent by a
// client that has no other way to find out. Answering it again — and appending
// nothing — is what makes one dropped response survivable instead of fatal.
func TestPushReAcksARepeatedChunk(t *testing.T) {
	ft, _ := newTestTransfers(t, 0)
	if _, err := callPush(t, ft, xfer.PushChunk{Xfer: "x5", Path: "dst", Seq: 0, Data: []byte("a")}); err != nil {
		t.Fatalf("chunk 0: %v", err)
	}
	ack, err := callPush(t, ft, xfer.PushChunk{Xfer: "x5", Path: "dst", Seq: 0, Data: []byte("a")})
	if err != nil {
		t.Fatalf("re-sent chunk 0: %v", err)
	}
	if ack.Seq != 0 {
		t.Fatalf("re-ack echoed seq %d", ack.Seq)
	}
	x := ft.pushes["x5"]
	if x == nil {
		t.Fatal("the transfer was dropped by a repeat")
	}
	if x.next != 1 || x.bytes != 1 {
		t.Fatalf("after a repeat the transfer is at seq %d / %d bytes; want 1 / 1 (nothing appended twice)", x.next, x.bytes)
	}
}

// TestPushRefusesOutOfOrderChunks: a gap means bytes are missing, and there is
// no honest way to continue — the transfer dies and takes its staging file.
func TestPushRefusesOutOfOrderChunks(t *testing.T) {
	ft, _ := newTestTransfers(t, 0)
	if _, err := callPush(t, ft, xfer.PushChunk{Xfer: "x5", Path: "dst", Seq: 0, Data: []byte("a")}); err != nil {
		t.Fatalf("chunk 0: %v", err)
	}
	if _, err := callPush(t, ft, xfer.PushChunk{Xfer: "x5", Path: "dst", Seq: 2, Data: []byte("c")}); err == nil {
		t.Fatal("a gap in the sequence was accepted")
	}
	if n := ft.open(); n != 0 {
		t.Fatalf("%d transfers survived a sequence gap", n)
	}
	if files := stagingFiles(t, ft.tmp); len(files) != 0 {
		t.Fatalf("a broken push left staging files: %v", files)
	}
}

// TestPushRefusesAChangedDestination: the destination is fixed by the first
// chunk, or bytes approved against one path get written to another.
func TestPushRefusesAChangedDestination(t *testing.T) {
	ft, _ := newTestTransfers(t, 0)
	if _, err := callPush(t, ft, xfer.PushChunk{Xfer: "x5b", Path: "dst", Seq: 0, Data: []byte("a")}); err != nil {
		t.Fatalf("chunk 0: %v", err)
	}
	if _, err := callPush(t, ft, xfer.PushChunk{Xfer: "x5b", Path: "other", Seq: 1, Data: []byte("b")}); err == nil {
		t.Fatal("a chunk that changed the destination mid-transfer was accepted")
	}
	if n := ft.open(); n != 0 {
		t.Fatalf("%d transfers survived a destination change", n)
	}
}

// TestPushRefusesAnOversizeChunk and TestPushRefusesPastTheTotalCap are the
// sandbox's own half of the bound: the client refuses first and controld
// refuses second, but neither is the process writing to the disk.
func TestPushRefusesAnOversizeChunk(t *testing.T) {
	ft, _ := newTestTransfers(t, 0)
	_, err := callPush(t, ft, xfer.PushChunk{
		Xfer: "x6", Path: "dst", Seq: 0, Data: make([]byte, xfer.ChunkBytes+1)})
	if err == nil {
		t.Fatal("an oversize chunk was accepted")
	}
}

func TestPushRefusesPastTheTotalCap(t *testing.T) {
	ft, _ := newTestTransfers(t, 8<<10)
	blob := make([]byte, 4<<10)
	if _, err := callPush(t, ft, xfer.PushChunk{Xfer: "x7", Path: "dst", Seq: 0, Data: blob}); err != nil {
		t.Fatalf("chunk 0: %v", err)
	}
	if _, err := callPush(t, ft, xfer.PushChunk{Xfer: "x7", Path: "dst", Seq: 1, Data: blob}); err != nil {
		t.Fatalf("chunk 1: %v", err)
	}
	if _, err := callPush(t, ft, xfer.PushChunk{Xfer: "x7", Path: "dst", Seq: 2, Data: blob}); err == nil {
		t.Fatal("a transfer past the cap was accepted")
	}
	if files := stagingFiles(t, ft.tmp); len(files) != 0 {
		t.Fatalf("an over-cap push left staging files: %v", files)
	}
}

// TestPullRoundTrip: sessiond tars a directory in the workspace and serves it
// in chunks; reassembled, it is the directory.
func TestPullRoundTrip(t *testing.T) {
	ft, root := newTestTransfers(t, 0)
	src := filepath.Join(root, "widget", "out")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	body := bytes.Repeat([]byte("payload\n"), 4096) // ~32KB, several chunks at 8KB
	if err := os.WriteFile(filepath.Join(src, "report.txt"), body, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	var got bytes.Buffer
	for seq := 0; ; seq++ {
		chunk, err := callPull(t, ft, xfer.PullRequest{Xfer: "p1", Path: "widget/out", Seq: seq})
		if err != nil {
			t.Fatalf("pull chunk %d: %v", seq, err)
		}
		if chunk.Seq != seq {
			t.Fatalf("chunk echoed seq %d, want %d", chunk.Seq, seq)
		}
		got.Write(chunk.Data)
		if chunk.Done {
			break
		}
		if seq > 1000 {
			t.Fatal("pull never reported done")
		}
	}

	archive := filepath.Join(t.TempDir(), "pulled.tgz")
	if err := os.WriteFile(archive, got.Bytes(), 0o644); err != nil {
		t.Fatalf("write archive: %v", err)
	}
	dest := t.TempDir()
	if err := xfer.UntarGz(archive, dest, xfer.MaxExtractBytes); err != nil {
		t.Fatalf("UntarGz what the pull returned: %v", err)
	}
	back, err := os.ReadFile(filepath.Join(dest, "report.txt"))
	if err != nil {
		t.Fatalf("read pulled file: %v", err)
	}
	if !bytes.Equal(back, body) {
		t.Fatalf("pulled %d bytes, want the original %d", len(back), len(body))
	}
	if n := ft.open(); n != 0 {
		t.Fatalf("%d transfers still open after the last chunk", n)
	}
	if files := stagingFiles(t, ft.tmp); len(files) != 0 {
		t.Fatalf("staging files left behind: %v", files)
	}
}

// TestPullRepeatsAChunk: a chunk whose response was lost can be asked for
// again, because the staging file is served by offset rather than consumed.
func TestPullRepeatsAChunk(t *testing.T) {
	ft, root := newTestTransfers(t, 0)
	dir := filepath.Join(root, "d")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "f"), bytes.Repeat([]byte("z"), 40<<10), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	first, err := callPull(t, ft, xfer.PullRequest{Xfer: "p2", Path: "d", Seq: 0})
	if err != nil {
		t.Fatalf("chunk 0: %v", err)
	}
	again, err := callPull(t, ft, xfer.PullRequest{Xfer: "p2", Path: "d", Seq: 0})
	if err != nil {
		t.Fatalf("chunk 0 again: %v", err)
	}
	if !bytes.Equal(first.Data, again.Data) {
		t.Fatal("re-reading a chunk returned different bytes")
	}
	if _, err := callPull(t, ft, xfer.PullRequest{Xfer: "p2", Path: "d", Seq: 99}); err == nil {
		t.Fatal("a chunk past the end of the archive was served")
	}
}

// TestPullRefusesEscapingPaths and a path that is not there at all.
func TestPullRefusesEscapingPaths(t *testing.T) {
	ft, _ := newTestTransfers(t, 0)
	for _, p := range []string{"../etc", "/etc", "", "d/../../etc"} {
		if _, err := callPull(t, ft, xfer.PullRequest{Xfer: "p3", Path: p, Seq: 0}); err == nil {
			t.Errorf("pull of %q was accepted", p)
		}
	}
	if _, err := callPull(t, ft, xfer.PullRequest{Xfer: "p4", Path: "nope", Seq: 0}); err == nil {
		t.Error("pull of a path that does not exist was accepted")
	}
	if files := stagingFiles(t, ft.tmp); len(files) != 0 {
		t.Fatalf("refused pulls left staging files: %v", files)
	}
}

// TestPullRefusesPastTheCap: a workspace directory bigger than the transfer
// limit is refused rather than half-sent.
func TestPullRefusesPastTheCap(t *testing.T) {
	ft, root := newTestTransfers(t, 4<<10)
	dir := filepath.Join(root, "big")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Incompressible: a repeating pattern would gzip to well under the cap and
	// the test would pass without ever reaching it.
	blob := make([]byte, 512<<10)
	if _, err := rand.Read(blob); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "f"), blob, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := callPull(t, ft, xfer.PullRequest{Xfer: "p5", Path: "big", Seq: 0}); err == nil {
		t.Fatal("a pull past the cap was accepted")
	}
	if files := stagingFiles(t, ft.tmp); len(files) != 0 {
		t.Fatalf("an over-cap pull left staging files: %v", files)
	}
}

// TestTransfersSweepAbandonedStaging: a client that walks away mid-transfer
// leaves an open file in a container that outlives it. Nothing else ever
// collects those, so the table does.
func TestTransfersSweepAbandonedStaging(t *testing.T) {
	ft, _ := newTestTransfers(t, 0)
	if _, err := callPush(t, ft, xfer.PushChunk{Xfer: "old", Path: "dst", Seq: 0, Data: []byte("a")}); err != nil {
		t.Fatalf("chunk: %v", err)
	}
	if files := stagingFiles(t, ft.tmp); len(files) != 1 {
		t.Fatalf("staging files = %v, want the one in flight", files)
	}

	ft.sweep(time.Now().Add(2 * transferIdleTTL))
	if n := ft.open(); n != 0 {
		t.Fatalf("%d transfers survived the sweep", n)
	}
	if files := stagingFiles(t, ft.tmp); len(files) != 0 {
		t.Fatalf("the sweep left staging files: %v", files)
	}
}

// stagingFiles lists what a transfer table has left in its staging directory.
func stagingFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read staging dir: %v", err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}
