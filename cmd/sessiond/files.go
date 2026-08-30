package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"rainier/internal/xfer"
)

// This file is the sandbox end of the workspace-inspection RPCs: the session
// diff, and the bounded push/pull file transfer. controld drives all three
// from the outside (internal/controld/api.go); the shapes on the wire belong
// to internal/xfer, which both ends import so there is one definition of each.
//
// Everything here runs on the RPC dispatcher's per-frame goroutines (rpc.go),
// which means three things it would be easy to forget:
//
//   - Handlers are concurrent with each other and with the terminal traffic
//     multiplexed over the same connection. The transfer table takes a lock;
//     git runs in its own process.
//   - Nothing upstream bounds a handler's runtime, so every one of them bounds
//     itself. The diff carries the design's 30s-per-repository budget.
//   - A panic here would take the whole session down (see invoke's recover),
//     so a bug costs one caller an error and nothing more.
//
// SECRET HYGIENE: the diff runs git, and git inside this sandbox authenticates
// through the credential helper — which means the token reaches the git
// process and nothing else. Nothing in this file reads, logs or stores one.

// registerFileHandlers installs the three methods this file serves.
//
// It reads the session's repository list from the same environment variable
// the clone stage does, and separately from prepareBoot: the two consume the
// same fact for different purposes, and a diff that quietly reported "no
// repositories" because the variable would not decode is worse than one that
// says so (the clone stage has already failed the session by then).
func registerFileHandlers(rpc *rpcDispatcher, env bootEnv) {
	repos, err := sessionRepos(env)
	// The gitconfig the boot chain wrote — the credential helper and the
	// session's identity. Absent for a session with no git at all, in which
	// case there are no repositories to diff either.
	var gitConfig string
	if env.git() {
		gitConfig = setupDir + "/" + gitConfigName
	}
	rpc.RegisterRPCHandler(xfer.MethodDiff, newDiffer(workspaceRoot, repos, err, gitConfig).handle)

	// One table for the whole process: transfers are keyed by the id their
	// client chose, and they have to outlive the single request that started
	// them (that is what makes chunking work at all).
	ft := newFileTransfers(workspaceRoot, prepareTransferStaging(transferStagingDir))
	rpc.RegisterRPCHandler(xfer.MethodPushFiles, ft.handlePush)
	rpc.RegisterRPCHandler(xfer.MethodPullFiles, ft.handlePull)
}

// sessionRepos decodes the repository list, treating an ABSENT variable as no
// repositories (a scratch session) and an unreadable one as an error.
func sessionRepos(env bootEnv) ([]repoSpec, error) {
	if env.ReposB64 == "" {
		return nil, nil
	}
	return decodeRepos(env.ReposB64)
}

// ---------------------------------------------------------------------------
// diff
// ---------------------------------------------------------------------------

const (
	// diffTimeoutPerRepo bounds one repository's fetch-and-diff (design §4.6).
	// It covers BOTH commands together: the pair is one question about one
	// repository, and a fetch that stalls for 30 seconds has already cost the
	// caller their answer whether or not the diff would have been quick.
	diffTimeoutPerRepo = 30 * time.Second
	// diffWaitDelay is the grace a killed git's descendants get to release the
	// output pipes (see differ.waitDelay).
	diffWaitDelay = 2 * time.Second
	// diffStderrCap bounds how much of git's own error text travels with a
	// failure. Enough for the fatal: line and its context.
	diffStderrCap = 4 << 10
	// diffTruncatedMarker is appended, inside the cap, to a stat that was cut.
	// A silently truncated diff is one a user would read as complete.
	diffTruncatedMarker = "\n… (truncated at 64KB)\n"
)

// differ answers the `diff` method: one `--stat` per repository this session
// cloned, in the order controld resolved them.
type differ struct {
	root  string
	repos []repoSpec
	// reposErr is what decoding RAINIER_REPOS_B64 produced, if it failed. The
	// clone stage has already failed this session over it (see gitchain.go);
	// the diff reports it too rather than answering "no repositories", which
	// is what an empty workspace and an unreadable list would otherwise look
	// like from here.
	reposErr error
	// gitConfig is GIT_CONFIG_GLOBAL for the git processes this handler
	// spawns. It matters: the boot chain exports that variable to the AGENT's
	// environment, not to sessiond's own, and without it a fetch has no
	// credential helper to ask and no private repository would ever diff.
	gitConfig string
	timeout   time.Duration
	// waitDelay is how long a killed git's descendants get to release the
	// output pipes before Wait gives up on them. Without it the timeout above
	// would not bound anything: `git fetch` spawns git-remote-https, which
	// INHERITS the pipe, so Wait blocks until the grandchild exits however
	// dead its parent is.
	waitDelay time.Duration
	statCap   int
}

func newDiffer(root string, repos []repoSpec, reposErr error, gitConfig string) *differ {
	return &differ{
		root:      root,
		repos:     repos,
		reposErr:  reposErr,
		gitConfig: gitConfig,
		timeout:   diffTimeoutPerRepo,
		waitDelay: diffWaitDelay,
		statCap:   xfer.StatBytes,
	}
}

// handle serves one `diff` request. The payload is ignored: what to diff is
// the session's own repository list, which controld already resolved and the
// driver already injected — a caller does not get to name a directory here.
func (d *differ) handle([]byte) (any, error) {
	if d.reposErr != nil {
		return nil, fmt.Errorf("this session's repository list could not be read: %w", d.reposErr)
	}
	out := xfer.DiffAnswer{Repos: make([]xfer.RepoDiff, 0, len(d.repos))}
	for _, r := range d.repos {
		stat, err := d.repoStat(r)
		if err != nil {
			return nil, err
		}
		out.Repos = append(out.Repos, xfer.RepoDiff{
			Repo:          r.Owner + "/" + r.Name,
			BaseBranch:    r.BaseBranch,
			SessionBranch: r.SessionBranch,
			Stat:          stat,
		})
	}
	return out, nil
}

// repoStat runs the design's two commands for one repository under a single
// budget, and returns git's own output capped.
func (d *differ) repoStat(r repoSpec) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()

	dir := repoDir(d.root, r)
	slug := r.Owner + "/" + r.Name

	// The fetch is what makes the comparison mean anything: origin/<base> in
	// the session's clone is as old as the clone, so without it the stat would
	// silently describe a base branch that has moved on.
	if _, err := d.run(ctx, dir, "fetch", "-q", "origin", r.BaseBranch); err != nil {
		return "", d.wrap(ctx, slug, "fetching origin/"+r.BaseBranch, err)
	}
	// Three dots: the diff against the MERGE BASE, not against the tip of the
	// base branch — what this session changed, never what the rest of the team
	// changed underneath it.
	out, err := d.run(ctx, dir, "diff", "--stat", "origin/"+r.BaseBranch+"...HEAD")
	if err != nil {
		return "", d.wrap(ctx, slug, "diffing against origin/"+r.BaseBranch, err)
	}
	return capStat(out, d.statCap), nil
}

// run executes one git command, returning its stdout. Both streams are read
// into memory, bounded: stdout by the caller's cap, stderr by diffStderrCap.
func (d *differ) run(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := d.gitCommand(ctx, dir, args...)
	var stdout, stderr strings.Builder
	// The cap is applied to what git may WRITE, not to what it produced: a
	// repository with a pathological diff must not be able to make this
	// process hold hundreds of megabytes. The extra room over statCap is so
	// the truncation is visible to capStat rather than landing exactly on the
	// boundary.
	cmd.Stdout = &cappedWriter{w: &stdout, limit: d.statCap + len(diffTruncatedMarker) + 1}
	cmd.Stderr = &cappedWriter{w: &stderr, limit: diffStderrCap}
	if err := cmd.Run(); err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return "", fmt.Errorf("%w: %s", err, msg)
		}
		return "", err
	}
	return stdout.String(), nil
}

// gitCommand builds one bounded git invocation in dir, with the workspace
// gitconfig in its environment.
func (d *differ) gitCommand(ctx context.Context, dir string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	cmd.WaitDelay = d.waitDelay
	cmd.Env = os.Environ()
	if d.gitConfig != "" {
		cmd.Env = append(cmd.Env, "GIT_CONFIG_GLOBAL="+d.gitConfig)
	}
	// Nothing here is interactive: a prompt would hang until the budget above
	// killed it, and the user would be told "timed out" about a password.
	cmd.Env = append(cmd.Env, "GIT_TERMINAL_PROMPT=0")
	cmd.Stdin = nil
	return cmd
}

// wrap renders one repository's git failure, naming the repository, what was
// being attempted, and git's own words — which are the only part of this a
// user can act on.
func (d *differ) wrap(ctx context.Context, slug, doing string, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("%s: %s timed out after %s", slug, doing, d.timeout)
	}
	return fmt.Errorf("%s: %s: %v", slug, doing, err)
}

// capStat truncates one repository's stat to limit, saying so in the space it
// leaves for the marker.
func capStat(s string, limit int) string {
	if limit <= 0 || len(s) <= limit {
		return s
	}
	keep := limit - len(diffTruncatedMarker)
	if keep < 0 {
		keep = 0
	}
	return strings.ToValidUTF8(s[:keep], "") + diffTruncatedMarker
}

// cappedWriter stops accepting bytes past limit and reports success anyway:
// a child that fills its pipe would otherwise die of EPIPE, and the failure a
// caller sees would be about a broken pipe instead of about the output being
// too long.
type cappedWriter struct {
	w     io.Writer
	n     int
	limit int
}

func (c *cappedWriter) Write(p []byte) (int, error) {
	if room := c.limit - c.n; room > 0 {
		if len(p) < room {
			room = len(p)
		}
		n, err := c.w.Write(p[:room])
		c.n += n
		if err != nil {
			return n, err
		}
	}
	return len(p), nil
}

// ---------------------------------------------------------------------------
// push / pull
// ---------------------------------------------------------------------------

// transferIdleTTL is how long an unfinished transfer's staging file survives
// without a chunk. Nothing else in the system collects those: a client that is
// killed mid-push leaves an open file in a container that deliberately
// outlives every connection it has, so the table sweeps its own.
const transferIdleTTL = 15 * time.Minute

// transferStagingDir is where a push or a pull assembles its archive.
//
// It is on the WORKSPACE VOLUME, not in /tmp, and the difference is host RAM.
// The driver mounts the container's /tmp as a tmpfs with no size option
// (internal/driver/docker.go), so an archive staged there is resident memory —
// up to xfer.MaxBytes (256MiB) per direction per session, on a runner already
// hosting N of them, with no container memory limit above it. On the volume it
// is ordinary disk, bounded by the disk, and `docker commit` excludes volumes
// so nothing staged here can reach a cached environment image either.
//
// It is a subdirectory rather than .rainier itself so that clearing it at boot
// (prepareTransferStaging) cannot touch the boot chain's own files, which live
// beside it and are written by a different part of this process.
const transferStagingDir = setupDir + "/transfers"

// prepareTransferStaging readies that directory and returns it.
//
// The CLEARING is the point, and it is what buys back the one thing /tmp gave
// for free: a tmpfs is empty at every boot, while /workspace survives a crash
// and a cold park, so a transfer killed halfway would otherwise leave its
// staging file on the volume forever (the in-process idle sweep only collects
// entries this boot's table knows about). Nothing in a fresh boot refers to
// anything already in here, so everything already in here is dead.
//
// A failure is logged, not fatal, and the directory is returned anyway: the
// error a caller then gets from os.CreateTemp names this path, which is more
// use than a session that refuses to start over a feature it may never use.
func prepareTransferStaging(dir string) string {
	if err := os.RemoveAll(dir); err != nil {
		log.Printf("clearing the transfer staging directory %s: %v", dir, err)
	}
	// 0700: a staging file is one user's workspace in transit, and everything
	// in the container runs as the session's own user anyway.
	if err := os.MkdirAll(dir, 0o700); err != nil {
		log.Printf("preparing the transfer staging directory %s: %v", dir, err)
	}
	return dir
}

// fileTransfers is this sandbox's transfer state: one entry per push or pull
// in flight, each holding a staging file in transferStagingDir.
//
// The staging file is why a transfer has state at all. A push is assembled
// whole before ANY of it is extracted (a tar is only safe to trust once its
// last entry has been read — see xfer.UntarGz), and a pull is tarred once and
// then served by offset, so the bytes a client reassembles are one consistent
// snapshot rather than a directory read live underneath it.
type fileTransfers struct {
	root string // the session's workspace: the only tree a transfer may touch
	tmp  string // where staging files live (transferStagingDir; "" means os.TempDir)
	max  int64  // the compressed cap, overridden in tests

	mu     sync.Mutex
	pushes map[string]*pushXfer
	pulls  map[string]*pullXfer
}

func newFileTransfers(root, tmp string) *fileTransfers {
	return &fileTransfers{
		root:   root,
		tmp:    tmp,
		max:    xfer.MaxBytes,
		pushes: map[string]*pushXfer{},
		pulls:  map[string]*pullXfer{},
	}
}

// pushXfer is one upload being assembled.
type pushXfer struct {
	path    string // the destination, as the first chunk named it
	dest    string // that destination resolved under the workspace
	staging string
	f       *os.File
	next    int // the sequence number the next chunk must carry
	bytes   int64
	touched time.Time
}

// pullXfer is one download being served: an archive made once, read by offset.
type pullXfer struct {
	path    string
	staging string
	f       *os.File
	size    int64
	touched time.Time
}

// handlePush serves one push_files chunk: append, and on the last one extract.
//
// V0 CRUDENESS, ON PURPOSE (design §4.5). One REST request and one session RPC
// per megabyte, each waiting for its ack, is a slow way to move 200MB and an
// obviously correct one — it needs no new plane, no pairing, no backpressure
// protocol, and it cannot starve the terminal traffic sharing the connection
// because only one chunk is ever in flight. The upgrade path when the cap
// starts to hurt is named and not built: the attach plane already does
// pairing and dial-back for a bidirectional byte stream, and a transfer would
// ride it as one more attachment rather than as a request per megabyte.
func (t *fileTransfers) handlePush(payload []byte) (any, error) {
	var c xfer.PushChunk
	if err := json.Unmarshal(payload, &c); err != nil {
		return nil, fmt.Errorf("reading the push chunk: %w", err)
	}
	if c.Xfer == "" {
		return nil, errors.New("the push chunk names no transfer")
	}
	if len(c.Data) > xfer.ChunkBytes {
		return nil, fmt.Errorf("push chunk %d is %s; the limit is %s",
			c.Seq, xfer.HumanBytes(int64(len(c.Data))), xfer.HumanBytes(xfer.ChunkBytes))
	}
	dest, err := xfer.Resolve(t.root, c.Path)
	if err != nil {
		return nil, err
	}
	t.sweep(time.Now())

	t.mu.Lock()
	defer t.mu.Unlock()

	x := t.pushes[c.Xfer]
	if x == nil {
		if c.Seq != 0 {
			return nil, fmt.Errorf("push chunk %d arrived for a transfer that has not started", c.Seq)
		}
		f, err := os.CreateTemp(t.tmp, "rainier-push-*.tgz")
		if err != nil {
			return nil, fmt.Errorf("staging the push: %w", err)
		}
		x = &pushXfer{path: c.Path, dest: dest, staging: f.Name(), f: f}
		t.pushes[c.Xfer] = x
	}
	x.touched = time.Now()

	if c.Path != x.path {
		// The destination is fixed by the transfer's first chunk. Letting a
		// later one move it would mean the bytes already staged were approved
		// against one path and extracted into another.
		t.abandonPush(c.Xfer, x)
		return nil, fmt.Errorf("push chunk %d changed the destination from %q to %q", c.Seq, x.path, c.Path)
	}
	switch {
	case c.Seq == x.next-1:
		// The chunk just accepted, sent again: the ack was lost, not the data.
		// Re-ack without appending — the alternative is a transfer that can
		// never recover from one dropped response.
		return xfer.PushAck{Seq: c.Seq}, nil
	case c.Seq != x.next:
		t.abandonPush(c.Xfer, x)
		return nil, fmt.Errorf("push chunk %d arrived out of order; expected %d", c.Seq, x.next)
	}
	if x.bytes+int64(len(c.Data)) > t.max {
		t.abandonPush(c.Xfer, x)
		return nil, fmt.Errorf("the push is larger than the %s transfer limit", xfer.HumanBytes(t.max))
	}
	if _, err := x.f.Write(c.Data); err != nil {
		t.abandonPush(c.Xfer, x)
		return nil, fmt.Errorf("staging the push: %w", err)
	}
	x.next++
	x.bytes += int64(len(c.Data))

	// fsync every SyncEvery chunks and on the last one, and say so in the ack:
	// that flag is the only durability claim this protocol makes, so it has to
	// be made after the data is actually on the disk rather than before.
	synced := c.Done || x.next%xfer.SyncEvery == 0
	if synced {
		if err := x.f.Sync(); err != nil {
			t.abandonPush(c.Xfer, x)
			return nil, fmt.Errorf("staging the push: %w", err)
		}
	}
	if !c.Done {
		return xfer.PushAck{Seq: c.Seq, Synced: synced}, nil
	}

	// The last chunk. Everything from here removes the staging file whatever
	// happens: a failed extraction must leave no half-transfer behind for the
	// next one to trip over.
	defer t.abandonPush(c.Xfer, x)
	if err := x.f.Close(); err != nil {
		return nil, fmt.Errorf("staging the push: %w", err)
	}
	x.f = nil
	if err := xfer.UntarGz(x.staging, x.dest, xfer.MaxExtractBytes); err != nil {
		return nil, fmt.Errorf("unpacking into %s: %w", x.path, err)
	}
	return xfer.PushAck{Seq: c.Seq, Synced: true}, nil
}

// handlePull serves one pull_files chunk. Seq 0 makes the archive; every
// sequence number after it is an offset into that file, which is what lets a
// chunk whose response was lost simply be asked for again.
func (t *fileTransfers) handlePull(payload []byte) (any, error) {
	var req xfer.PullRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("reading the pull request: %w", err)
	}
	if req.Xfer == "" {
		return nil, errors.New("the pull request names no transfer")
	}
	if req.Seq < 0 {
		return nil, fmt.Errorf("pull chunk %d is not a sequence number", req.Seq)
	}
	src, err := xfer.Resolve(t.root, req.Path)
	if err != nil {
		return nil, err
	}
	t.sweep(time.Now())

	t.mu.Lock()
	defer t.mu.Unlock()

	x := t.pulls[req.Xfer]
	if x == nil {
		if req.Seq != 0 {
			return nil, fmt.Errorf("pull chunk %d arrived for a transfer that has not started", req.Seq)
		}
		if x, err = t.stagePull(req.Path, src); err != nil {
			return nil, err
		}
		t.pulls[req.Xfer] = x
	}
	if req.Path != x.path {
		t.abandonPull(req.Xfer, x)
		return nil, fmt.Errorf("pull chunk %d changed the source from %q to %q", req.Seq, x.path, req.Path)
	}
	x.touched = time.Now()

	off := int64(req.Seq) * xfer.ChunkBytes
	if off > x.size {
		t.abandonPull(req.Xfer, x)
		return nil, fmt.Errorf("pull chunk %d is past the end of the archive", req.Seq)
	}
	buf := make([]byte, min(int64(xfer.ChunkBytes), x.size-off))
	if _, err := x.f.ReadAt(buf, off); err != nil && err != io.EOF {
		t.abandonPull(req.Xfer, x)
		return nil, fmt.Errorf("reading the staged archive: %w", err)
	}
	done := off+int64(len(buf)) >= x.size
	if done {
		t.abandonPull(req.Xfer, x)
	}
	return xfer.PullChunk{Seq: req.Seq, Data: buf, Done: done}, nil
}

// stagePull tars src into a staging file. Called with the table locked: the
// archive is one transfer's, and a second request for the same transfer has to
// wait for it rather than start a second tar of the same tree.
func (t *fileTransfers) stagePull(path, src string) (*pullXfer, error) {
	if _, err := os.Stat(src); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%s does not exist in this session's workspace", path)
		}
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	f, err := os.CreateTemp(t.tmp, "rainier-pull-*.tgz")
	if err != nil {
		return nil, fmt.Errorf("staging the pull: %w", err)
	}
	x := &pullXfer{path: path, staging: f.Name(), f: f, touched: time.Now()}
	n, err := xfer.TarGz(f, src, t.max)
	if err != nil {
		t.closePull(x)
		return nil, fmt.Errorf("archiving %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		t.closePull(x)
		return nil, fmt.Errorf("staging the pull: %w", err)
	}
	x.size = n
	return x, nil
}

// abandonPush/abandonPull end one transfer: close the staging file, remove it,
// and forget the entry. Every exit path from a failure goes through one of
// them — a staging file that outlives its transfer is a leak in a process that
// outlives everything.
func (t *fileTransfers) abandonPush(id string, x *pushXfer) {
	delete(t.pushes, id)
	if x.f != nil {
		x.f.Close()
	}
	if x.staging != "" {
		os.Remove(x.staging)
	}
}

func (t *fileTransfers) abandonPull(id string, x *pullXfer) {
	delete(t.pulls, id)
	t.closePull(x)
}

func (t *fileTransfers) closePull(x *pullXfer) {
	if x.f != nil {
		x.f.Close()
		x.f = nil
	}
	if x.staging != "" {
		os.Remove(x.staging)
	}
}

// sweep drops every transfer untouched for transferIdleTTL. It runs at the top
// of each request rather than on a timer: transfers only appear when requests
// do, so a table that is not being used has nothing to collect.
func (t *fileTransfers) sweep(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, x := range t.pushes {
		if now.Sub(x.touched) > transferIdleTTL {
			t.abandonPush(id, x)
		}
	}
	for id, x := range t.pulls {
		if now.Sub(x.touched) > transferIdleTTL {
			t.abandonPull(id, x)
		}
	}
}

// open reports how many transfers are in flight. Tests assert every exit path
// takes its entry (and its staging file) with it; production code does not
// read it.
func (t *fileTransfers) open() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pushes) + len(t.pulls)
}
