package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/tokencanopy/rainier/protocol/runner"
)

// This file is sessiond's half of the agent home: the boot-time fetch that
// fills it, the loop that keeps what is in it equal to the control plane's
// sealed copy, and the downward revoke that empties it. A person logs a coding
// agent in once, inside any session; every later session of theirs starts with
// that agent already logged in, because this file put what the agent wrote into
// custody and the next boot fetched it back.
//
// NOTHING HERE KNOWS WHAT A PROVIDER IS. The driver injects a manifest
// (RAINIER_AGENTS_B64) of rows that each say a name, a directory, and the file
// names inside it that are the set; this file makes directories, writes files,
// stats them, reads them and deletes them. Which tool the name belongs to,
// which variable points it at its directory, and which hosts it talks to are
// the control plane's table (controlapp/agents.go) and are never spelled here —
// which is what makes adding a third agent a row rather than a change to the
// sandbox.
//
// SECRET HYGIENE (design §4.2, and the invariant this file is judged by): the
// files' BYTES go exactly two places — the file on the mounted volume, and the
// put's payload on the session RPC. They are never logged, never put in an
// error, never in a note, never in a stage tail, and never in a temp file
// outside the home. What may be said out loud is the provider's name, a FILE
// name, a byte count, a version, and a sentence somebody else wrote.
//
// Ordering: the homes are filled while the chain's earlier stages run, and the
// agents stage (gitchain.go, after the clone and before init) is what makes the
// agent wait for them. The stage's own script never fails over custody — a
// refusal or a timeout is a note and the agent starts and asks the person to
// log in, which is the truthful state. The ONE failure is a runner too old to
// mount the home at all, which nothing inside the sandbox can work around.

const (
	// agentSyncInterval is how often the loop looks for a change. A login is a
	// human event and two seconds is invisible next to one; the cost of the
	// poll is a stat per allowlisted file.
	agentSyncInterval = 2 * time.Second
	// agentSetMaxBytes caps the whole set a session may send. The rows'
	// files are a few hundred bytes; 64 KiB is room for a tool that decides to
	// keep more state beside them, and a bound on what a process inside the
	// sandbox can push through this channel by writing into an allowlisted
	// name.
	agentSetMaxBytes = 64 << 10
	// agentCallTimeout bounds one fetch or put. It is the RPC's own existing
	// call budget — the same one the in-sandbox socket gives a mint — because
	// this is the same hop through the same forwarders.
	agentCallTimeout = agentSocketCallTimeout
	// agentConnWait is how long the boot fetch waits for the relay to come up
	// before it asks anyway. sessiond dials runnerd after the session starts,
	// so the fetch legitimately begins before there is a connection to make it
	// on (the same boot race the credential helper rides out, socket.go), and
	// a Call with no conn fails at once by design. Nothing is lost if the wait
	// expires: the fetch becomes a note and the agent starts.
	agentConnWait = 15 * time.Second
	// agentPutBackoffMin and agentPutBackoffMax bound the retry of a put that
	// did not land. The set on disk is the truth either way — a put that never
	// succeeds costs the person a login in their NEXT session, not this one —
	// so the retry is patient rather than aggressive.
	agentPutBackoffMin = 2 * time.Second
	agentPutBackoffMax = 30 * time.Second
	// agentNoteKind is the control event a boot note travels as: news about one
	// provider's home, carrying a sentence for the person and nothing else. It
	// is an EVENT, like a stage verdict — nobody answers it, and it is never a
	// failure.
	agentNoteKind = "agent_note"
	// agentHomeSubdir is where a provider that ALSO writes under $HOME is
	// pointed, inside its own directory (the A1-false path; see agentHomeVars).
	agentHomeSubdir = "home"
)

// agentMountSentence is what a session says when the runner under it is too old
// to mount agent homes. It is the one refusal in this whole mechanism that
// FAILS the boot instead of noting it: without the mount there is nowhere for a
// login to live, so a session that started anyway would silently lose every
// login the person made in it, and "log in once" would quietly become "log in
// every time". It names the version to upgrade to because that is the only
// action that fixes it.
const agentMountSentence = "this runner does not mount agent homes; upgrade runnerd to v0.0.3"

// agentsMountRoot is where the driver mounts the home volume — the parent of
// every row's directory, and the same path controlapp.HomeMountPath hands down.
// It is spelled again here rather than imported: the sandbox does not depend on
// the control plane's packages, and the value is part of the create's contract
// with the image, not a shared constant. A variable rather than a const so a
// test can point it at a directory it is allowed to make.
var agentsMountRoot = "/rainier/agents"

// agentEntry is one row of RAINIER_AGENTS_B64: a provider's name, its directory
// under the mount, the file names inside it that make up the set, and — for a
// provider that also writes under $HOME — the variable to redirect. The JSON
// tags mirror controlapp's manifest exactly; the two spellings are the contract
// across the env-var channel and have to stay identical.
type agentEntry struct {
	Provider string   `json:"provider"`
	Dir      string   `json:"dir"`
	Files    []string `json:"files"`
	HomeVar  string   `json:"home_var,omitempty"`
}

// agentNote is the boot note's payload: which home, and what to say about it.
// It is deliberately its own shape rather than a relay.ControlEvent field —
// notes are per-provider news and the event vocabulary is per-session — and it
// travels on the same events channel a stage verdict does, so an old runnerd
// that has never heard of it drops one frame and nothing else.
type agentNote struct {
	Kind     string `json:"kind"`
	Provider string `json:"provider"`
	Text     string `json:"text"`
}

// decodeAgents reads the manifest: base64 of the JSON array controlapp encodes.
func decodeAgents(b64 string) ([]agentEntry, error) {
	blob, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, fmt.Errorf("decoding RAINIER_AGENTS_B64 (%d bytes): %w", len(b64), err)
	}
	var entries []agentEntry
	if err := json.Unmarshal(blob, &entries); err != nil {
		return nil, fmt.Errorf("reading the home list: %w", err)
	}
	return entries, nil
}

// agentEntries decodes the manifest and drops every row this sandbox will not
// act on. An ABSENT variable means no homes and nothing in this file runs.
//
// An unreadable one is a LOG LINE, not a failed session — the asymmetry with
// the repository list (gitchain.go fails the clone stage loudly) is the whole
// posture of this mechanism: a session without its repositories is not the
// session that was asked for, while a session without its logins is one where
// the agent asks the person to log in. Custody never fails a boot.
//
// The two checks on a row are not paranoia about controlapp: this manifest is
// what decides where sessiond WRITES files and which files a revoke DELETES, so
// a row naming a directory outside the mount, or a file name with a path in it,
// would make either of those reach into the workspace. It costs four lines to
// make that impossible from here.
func agentEntries(env bootEnv) []agentEntry {
	if env.AgentsB64 == "" {
		return nil
	}
	entries, err := decodeAgents(env.AgentsB64)
	if err != nil {
		log.Printf("the home list: %v; this session runs with none", err)
		return nil
	}
	out := make([]agentEntry, 0, len(entries))
	for _, e := range entries {
		if err := checkAgentEntry(e); err != nil {
			log.Printf("home %q dropped: %v", e.Provider, err)
			continue
		}
		out = append(out, e)
	}
	return out
}

func checkAgentEntry(e agentEntry) error {
	if e.Provider == "" {
		return errors.New("the row names nothing")
	}
	if !underAgentMount(e.Dir) {
		return errors.New("its directory is outside the mount")
	}
	if len(e.Files) == 0 {
		return errors.New("the row lists no files")
	}
	for _, n := range e.Files {
		if n == "" || n != filepath.Base(n) || n == "." || n == ".." {
			return fmt.Errorf("%q is not a bare file name", n)
		}
	}
	return nil
}

// underAgentMount reports whether dir is a directory inside the mounted home
// and not the mount itself.
func underAgentMount(dir string) bool {
	clean := filepath.Clean(dir)
	root := filepath.Clean(agentsMountRoot)
	return clean != root && strings.HasPrefix(clean, root+string(filepath.Separator))
}

// agentMountUsable reports whether the home volume is actually mounted and
// writable by this user. Stat alone would not answer it: an image can perfectly
// well have an empty /rainier/agents directory baked into its read-only rootfs,
// which stats like a mount and refuses the first write, so the check is a file
// this process creates and removes.
//
// It asks about the MOUNT and nothing else. What else lives under it — the
// marker directory the driver's init job leaves there, another provider's home,
// a directory from a session that ran before this one — is none of this
// process's business: sessiond touches the manifest's own directories and never
// enumerates, judges, or removes anything beside them.
func agentMountUsable(root string) error {
	info, err := os.Stat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", root)
	}
	probe, err := os.CreateTemp(root, ".rainier-probe-")
	if err != nil {
		return err
	}
	name := probe.Name()
	probe.Close()
	return os.Remove(name)
}

// agentHomeVars is the A1-false path, wired but unused by today's rows: a
// provider that ALSO writes under $HOME regardless of where its configuration
// directory points gets that variable redirected inside its own home.
//
// These go into the CHAIN's environment (chainArgv's exports), which is the
// agent's and the stages'. They are deliberately not the container's: the
// container's environment is set by the driver for every process in it, and
// moving $HOME for the whole sandbox would change what every unrelated tool in
// the session does.
func agentHomeVars(entries []agentEntry) []envVar {
	var out []envVar
	for _, e := range entries {
		if e.HomeVar == "" {
			continue
		}
		dir := filepath.Join(e.Dir, agentHomeSubdir)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			log.Printf("home %q: %v", e.Provider, err)
			continue
		}
		out = append(out, envVar{Name: e.HomeVar, Value: dir})
	}
	return out
}

// ---------------------------------------------------------------------------
// the stage
// ---------------------------------------------------------------------------

// prepareAgentsStage lands the agents stage's script and returns the stage.
//
// The stage is a WAIT, not the work: the work is an RPC, and an RPC needs the
// relay connection that only exists once sessiond is serving — which is after
// the chain's child has already started. So sessiond fills the homes on a
// goroutine of its own (agentSync.start) while setup and clone run, and this
// stage is what makes the chain hold until it has, so that init and the agent
// find the homes filled rather than filling.
//
// The marker is removed first for the same reason every stage clears its rc
// file: /workspace is a persistent volume, and a marker left by the PREVIOUS
// boot would let this boot's stage pass while this boot's fetch was still in
// flight.
func prepareAgentsStage(dir string, entries []agentEntry) (bootStage, error) {
	done := dir + "/" + agentsDoneName
	if err := os.Remove(done); err != nil && !os.IsNotExist(err) {
		return bootStage{}, err
	}
	script := agentsWaitScript(done, agentStageWaitSeconds)
	if err := agentMountUsable(agentsMountRoot); err != nil {
		log.Printf("the home mount is not usable (%v); this session cannot start", err)
		script = failingScript(agentMountSentence)
	}
	if err := writeStageScript(dir, agentsScriptName, agentsRCName, []byte(script)); err != nil {
		return bootStage{}, err
	}
	return bootStage{
		Name:       stageAgents,
		ScriptPath: dir + "/" + agentsScriptName,
		RCPath:     dir + "/" + agentsRCName,
		// No bound: the script bounds ITSELF (below) and then gets out of the
		// way. A stage timeout would kill the chain, and this stage must never
		// be the reason a session does not start.
	}, nil
}

// agentsWaitScript generates the stage: wait for sessiond's marker, then
// continue — and continue anyway after waitSeconds, because a home that is
// slow, refused, or unreachable is a note and never a reason to hold a session
// shut. It exits 0 on every path for the same reason.
func agentsWaitScript(done string, waitSeconds int) string {
	var b strings.Builder
	b.WriteString("# generated by sessiond — do not edit.\n")
	b.WriteString("# Waits for the homes this session's agents read. sessiond fills them\n")
	b.WriteString("# over the session RPC while the stages before this one run, and marks\n")
	b.WriteString("# the file below when it is done — whatever the outcome was.\n")
	b.WriteString("i=0\n")
	fmt.Fprintf(&b, "while [ ! -f %s ]; do\n", shQuote(done))
	b.WriteString("i=$((i+1))\n")
	fmt.Fprintf(&b, "if [ \"$i\" -gt %d ]; then\n", waitSeconds)
	b.WriteString("echo 'rainier: the agent homes are not ready; starting anyway' >&2\n")
	b.WriteString("break\n")
	b.WriteString("fi\n")
	b.WriteString("sleep 1\n")
	b.WriteString("done\n")
	b.WriteString("exit 0\n")
	return b.String()
}

// ---------------------------------------------------------------------------
// the sync
// ---------------------------------------------------------------------------

// agentSet is one provider's state: what custody last told this session, what
// the files on disk looked like the last time it looked, and what it has
// already sent.
//
// digest is a HASH of the set, not the set: comparing what is on disk against
// what was last put is the whole decision the loop makes, and keeping the bytes
// around to compare them with would be a copy of the credential living for the
// life of the process.
type agentSet struct {
	entry   agentEntry
	version uint64
	stamps  map[string]fileStamp
	digest  [32]byte
	dirty   bool
	backoff time.Duration
	retryAt time.Time
	// noted remembers the conditions already reported, so a symlink or an
	// oversized set is news once rather than every two seconds for the life of
	// the session. An entry is cleared when its condition clears.
	noted map[string]bool
}

// fileStamp is what a poll compares: size and modification time, which is what
// changes when a tool rewrites its own file. The time is unix nanoseconds
// rather than a time.Time so two stamps compare with ==.
type fileStamp struct {
	size int64
	mod  int64
}

// agentSync owns every home in this session: the boot fetch, the loop, the
// downward revoke, and the final put at shutdown.
type agentSync struct {
	rpc     *rpcDispatcher
	events  chan<- []byte
	entries []agentEntry

	interval    time.Duration
	callTimeout time.Duration
	connWait    time.Duration
	backoffMin  time.Duration
	backoffMax  time.Duration
	maxBytes    int64

	mu   sync.Mutex
	sets map[string]*agentSet

	started  atomic.Bool
	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

func newAgentSync(rpc *rpcDispatcher, entries []agentEntry, events chan<- []byte) *agentSync {
	a := &agentSync{
		rpc:         rpc,
		events:      events,
		entries:     entries,
		interval:    agentSyncInterval,
		callTimeout: agentCallTimeout,
		connWait:    agentConnWait,
		backoffMin:  agentPutBackoffMin,
		backoffMax:  agentPutBackoffMax,
		maxBytes:    agentSetMaxBytes,
		sets:        make(map[string]*agentSet, len(entries)),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
	for _, e := range entries {
		a.sets[e.Provider] = &agentSet{entry: e, noted: map[string]bool{}}
	}
	return a
}

// start fills every home and then keeps them equal to custody, on a goroutine
// of its own: the chain's agents stage is what waits for the first half (see
// prepareAgentsStage), and the second half runs for the life of the session.
func (a *agentSync) start(donePath string) {
	a.started.Store(true)
	go func() {
		defer close(a.done)
		a.boot()
		if err := os.WriteFile(donePath, nil, 0o600); err != nil {
			// The stage waits for this file and then gives up on its own
			// bound, so the cost of not writing it is a slow boot, not a stuck
			// one. Worth a line in the log all the same.
			log.Printf("the homes are ready but the marker could not be written: %v", err)
		}
		a.loop()
	}()
}

// close stops the loop and performs one last put per home, bounded by the RPC's
// own timeout. It is the difference between a login completing five seconds
// before the session is torn down and that login surviving.
func (a *agentSync) close() {
	a.stopOnce.Do(func() { close(a.stop) })
	if a.started.Load() {
		// Bounded, because the loop may be inside a call of its own when the
		// signal lands: waiting that out AND then spending a second bound on
		// the final put would be two whole timeouts of shutdown, with the
		// container being torn down around us. A put that overlaps the loop's
		// last one is harmless — they take the same lock, and the second finds
		// the set already where it belongs.
		select {
		case <-a.done:
		case <-time.After(a.callTimeout):
			log.Printf("the homes are still syncing; putting what they hold anyway")
		}
	}
	now := time.Now()
	for _, e := range a.entries {
		set := a.set(e.Provider)
		if set == nil {
			continue
		}
		a.mu.Lock()
		set.dirty = true
		set.retryAt = time.Time{}
		a.mu.Unlock()
		a.putSet(set, now)
	}
}

// boot fetches every home's set from custody and writes it to disk.
func (a *agentSync) boot() {
	// The relay may still be dialing (see agentConnWait). Waiting costs a boot
	// nothing when the conn is already up, and saves a whole session's logins
	// when it is not.
	a.rpc.waitConn(a.connWait)
	for _, e := range a.entries {
		a.fetchOne(e)
	}
}

func (a *agentSync) fetchOne(e agentEntry) {
	set := a.set(e.Provider)
	if set == nil {
		return
	}
	if err := os.MkdirAll(e.Dir, 0o700); err != nil {
		a.note(e.Provider, "this home could not be made ready: "+err.Error())
		return
	}
	// An existing directory keeps the mode it was made with, and this one may
	// have been made by an older sessiond or by the volume's own init.
	if err := os.Chmod(e.Dir, 0o700); err != nil {
		log.Printf("home %q: %v", e.Provider, err)
	}

	raw, err := a.rpc.Call(runner.MethodFetchAgentCredentials,
		map[string]string{"provider": e.Provider}, a.callTimeout)
	if err != nil {
		// A refusal's message is the far end's own sentence and a timeout's is
		// this end's; either way it is what the person needs to read, and
		// neither is a reason not to start the agent.
		a.note(e.Provider, err.Error())
		a.rebaseOnDisk(set)
		return
	}
	var body struct {
		Version uint64            `json:"version"`
		Files   map[string]string `json:"files"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		a.note(e.Provider, "the answer came back in a shape this session cannot read")
		a.rebaseOnDisk(set)
		return
	}

	held := make(map[string][]byte, len(body.Files))
	for _, name := range e.Files {
		encoded, ok := body.Files[name]
		if !ok {
			continue
		}
		blob, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			a.note(e.Provider, fmt.Sprintf("%s came back in a form this session cannot read", name))
			continue
		}
		if err := writeAgentFile(filepath.Join(e.Dir, name), blob); err != nil {
			a.note(e.Provider, fmt.Sprintf("%s could not be written: %v", name, err))
			continue
		}
		held[name] = blob
		log.Printf("home %q: wrote %s (%d bytes) at v%d", e.Provider, name, len(blob), body.Version)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	set.version = body.Version
	// The baseline is what CUSTODY holds, not what is on disk: if the volume
	// still carries a set from a boot whose put never landed, disk and custody
	// differ and the loop's first pass is exactly right to send it up.
	set.digest = agentSetDigest(held)
	set.stamps = statAgentFiles(e)
	set.dirty = false
}

// rebaseOnDisk is what a home whose fetch did not answer starts from: whatever
// is already on the volume.
//
// It matters which way this falls. Treating an unanswered fetch as "custody
// holds nothing" would make the first tick send the volume's existing set
// upward against a version this session never saw; treating the disk as the
// baseline means only a NEW write — a login the person makes in this session —
// is put, which is the one thing that is unambiguously news.
func (a *agentSync) rebaseOnDisk(set *agentSet) {
	a.mu.Lock()
	defer a.mu.Unlock()
	files, ok := a.readSetLocked(set)
	set.stamps = statAgentFiles(set.entry)
	set.dirty = false
	if !ok {
		return
	}
	set.digest = agentSetDigest(files)
}

// loop is the sync: a stat of the allowlisted files every interval, and a put
// whenever what they hold is not what custody last acknowledged.
func (a *agentSync) loop() {
	t := time.NewTicker(a.interval)
	defer t.Stop()
	for {
		select {
		case <-a.stop:
			return
		case <-t.C:
			a.tick(time.Now())
		}
	}
}

func (a *agentSync) tick(now time.Time) {
	for _, e := range a.entries {
		if set := a.set(e.Provider); set != nil {
			a.syncOne(set, now)
		}
	}
}

// flush is one synchronous pass over every home, run the moment the agent
// exits. The last thing an agent wrote is usually the thing worth keeping,
// and a login command that writes its file and exits at once — which is
// exactly what a device-code login does — would otherwise race the
// two-second tick against the CLI removing the session it just logged in
// from. Safe beside the loop: the two take the same lock, and whichever runs
// second finds the set already put.
func (a *agentSync) flush() { a.tick(time.Now()) }

// syncOne is one home's pass: notice a change cheaply, and only then read.
//
// The stat is what keeps this affordable at a two-second interval — a set is
// read (and hashed) only when its size or mtime moved — and it is deliberately
// not the decision: a tool that rewrites its file with identical bytes changes
// the mtime and nothing else, and a put for that would be a credential on the
// wire for no reason.
func (a *agentSync) syncOne(set *agentSet, now time.Time) {
	a.mu.Lock()
	stamps := statAgentFiles(set.entry)
	if !maps.Equal(stamps, set.stamps) {
		set.stamps = stamps
		set.dirty = true
	}
	skip := !set.dirty || now.Before(set.retryAt)
	a.mu.Unlock()
	if skip {
		return
	}
	a.putSet(set, now)
}

// putSet sends one home's set upward, if it is not already there.
//
// The RPC happens OUTSIDE the lock. It is a round trip to the control plane and
// the same lock answers the downward revoke; holding it across the call would
// make a slow control plane the reason a logout took twenty seconds to reach a
// session.
func (a *agentSync) putSet(set *agentSet, now time.Time) {
	a.mu.Lock()
	files, ok := a.readSetLocked(set)
	if !ok {
		// Over the cap, and already noted. Nothing to send and nothing to
		// retry until the files change again.
		set.dirty = false
		a.mu.Unlock()
		return
	}
	digest := agentSetDigest(files)
	if digest == set.digest {
		set.dirty = false
		a.mu.Unlock()
		return
	}
	provider, version := set.entry.Provider, set.version
	a.mu.Unlock()

	encoded := make(map[string]string, len(files))
	total := 0
	for name, blob := range files {
		encoded[name] = base64.StdEncoding.EncodeToString(blob)
		total += len(blob)
	}
	answer, err := a.rpc.Call(runner.MethodPutAgentCredentials, struct {
		Provider string            `json:"provider"`
		Files    map[string]string `json:"files"`
		Version  uint64            `json:"version"`
	}{provider, encoded, version}, a.callTimeout)

	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		set.backoff = nextAgentBackoff(set.backoff, a.backoffMin, a.backoffMax)
		set.retryAt = now.Add(set.backoff)
		log.Printf("home %q: the set did not land (%v); trying again in %s", provider, err, set.backoff)
		return
	}
	var body struct {
		Version uint64 `json:"version"`
	}
	if json.Unmarshal(answer, &body) == nil && body.Version > 0 {
		set.version = body.Version
	}
	set.digest = digest
	set.dirty = false
	set.backoff = 0
	set.retryAt = time.Time{}
	log.Printf("home %q: put %d file(s), %d bytes; custody is now at v%d", provider, len(files), total, set.version)
}

// nextAgentBackoff doubles from min and clamps at max — the same 2s..30s shape
// the relay's redial uses, and clamped on the RESULT so the cap actually holds.
func nextAgentBackoff(d, min, max time.Duration) time.Duration {
	if d <= 0 {
		return min
	}
	d *= 2
	if d > max {
		d = max
	}
	return d
}

// handleRevoke serves the downward revoke: the control plane telling this
// session that a person logged an agent out, or lost the membership that was
// projecting their login into this workspace.
//
// It empties the home and forgets the baseline. Forgetting matters as much as
// deleting: a session that kept the old digest would treat a login made
// afterwards as "the same set we already sent" and never put it, and one that
// kept the old version would offer custody a version that no longer exists.
func (a *agentSync) handleRevoke(payload []byte) (any, error) {
	var in struct {
		Provider string `json:"provider"`
	}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &in); err != nil {
			return nil, errors.New("the request could not be read")
		}
	}
	if in.Provider == "" {
		return nil, errors.New("the request named no provider")
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	set, ok := a.sets[in.Provider]
	if !ok {
		return nil, fmt.Errorf("this session has no home for %q", in.Provider)
	}
	removed := 0
	for _, name := range set.entry.Files {
		if err := os.Remove(filepath.Join(set.entry.Dir, name)); err != nil {
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("%s could not be removed: %v", name, err)
			}
			continue
		}
		removed++
	}
	set.version = 0
	set.digest = agentSetDigest(nil)
	set.stamps = statAgentFiles(set.entry)
	set.dirty = false
	set.backoff = 0
	set.retryAt = time.Time{}
	log.Printf("home %q: %d file(s) removed on request; this session holds nothing for it", in.Provider, removed)
	// The ok:true answer's body. It carries nothing because there is nothing to
	// say: the caller asked for a state, and it is that state now.
	return struct{}{}, nil
}

// baseline reports the version this session believes custody holds. Tests read
// it to prove a fetch, a put and a revoke each moved it; production code does
// not.
func (a *agentSync) baseline(provider string) (uint64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	set, ok := a.sets[provider]
	if !ok {
		return 0, false
	}
	return set.version, true
}

func (a *agentSync) set(provider string) *agentSet {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sets[provider]
}

// note reports one home's news to whoever is watching this session, and says
// the same thing in the log. A note is never a failure: the agent starts, and
// the person is told what state their login is in.
func (a *agentSync) note(provider, text string) {
	log.Printf("home %q: %s", provider, text)
	b, err := json.Marshal(agentNote{Kind: agentNoteKind, Provider: provider, Text: text})
	if err != nil {
		return
	}
	offerControl(a.events, b)
}

// noteOnce reports a condition the loop would otherwise re-discover every
// interval for the life of the session.
func (a *agentSync) noteOnce(set *agentSet, key, text string) {
	if set.noted[key] {
		return
	}
	set.noted[key] = true
	a.note(set.entry.Provider, text)
}

// ---------------------------------------------------------------------------
// the files
// ---------------------------------------------------------------------------

// readSetLocked reads one home's allowlisted files, reporting whether the
// result may be sent.
//
// Two refusals live here, and both are about what the allowlist does and does
// not promise. It says which NAMES are the set; it says nothing about what a
// process inside the sandbox may have put behind one of those names.
//
//   - A SYMLINK is not read. O_NOFOLLOW is the enforcement and the Lstat is
//     what makes it a sentence rather than a syscall error: without it,
//     anything in the session could point an allowlisted name at any file it
//     can read and have this loop deliver it to the control plane.
//   - A set over the cap is not sent AT ALL, rather than truncated. Half a
//     credential written into custody would be a login that fails everywhere
//     later, which is worse than one that never left.
func (a *agentSync) readSetLocked(set *agentSet) (map[string][]byte, bool) {
	e := set.entry
	files := make(map[string][]byte, len(e.Files))
	var total int64
	for _, name := range e.Files {
		path := filepath.Join(e.Dir, name)
		info, err := os.Lstat(path)
		if err != nil {
			continue
		}
		if info.Mode()&os.ModeSymlink != 0 {
			a.noteOnce(set, "link:"+name, fmt.Sprintf("%s is a link and is not part of this home", name))
			continue
		}
		delete(set.noted, "link:"+name)
		blob, err := readNoFollow(path, a.maxBytes+1)
		if err != nil {
			a.noteOnce(set, "read:"+name, fmt.Sprintf("%s could not be read (%v)", name, err))
			continue
		}
		delete(set.noted, "read:"+name)
		total += int64(len(blob))
		files[name] = blob
	}
	if total > a.maxBytes {
		a.noteOnce(set, "size", fmt.Sprintf("this home holds %d bytes, over the %d one session may put; nothing was put", total, a.maxBytes))
		return nil, false
	}
	delete(set.noted, "size")
	return files, true
}

// readNoFollow opens a path without following a final symlink and reads at most
// limit bytes.
func readNoFollow(path string, limit int64) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, limit))
}

// writeAgentFile lands one file 0600 by writing a temp file beside it and
// renaming over it.
//
// The temp file is in the SAME directory — the rename has to be on one
// filesystem, and a credential written to /tmp on the way to the home would be
// a copy nobody removes — and it is removed on every failure path.
func writeAgentFile(path string, blob []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".rainier-tmp-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(blob); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// statAgentFiles is the cheap half of the loop: what the allowlisted files
// looked like, without opening any of them. A file that is not there has no
// entry, so appearing and vanishing are both changes.
func statAgentFiles(e agentEntry) map[string]fileStamp {
	out := make(map[string]fileStamp, len(e.Files))
	for _, name := range e.Files {
		info, err := os.Lstat(filepath.Join(e.Dir, name))
		if err != nil {
			continue
		}
		out[name] = fileStamp{size: info.Size(), mod: info.ModTime().UnixNano()}
	}
	return out
}

// agentSetDigest fingerprints a set so two of them can be compared without
// keeping either. Names are sorted and lengths are hashed alongside the bytes,
// so no two different sets share a digest by rearrangement.
func agentSetDigest(files map[string][]byte) [32]byte {
	names := slices.Sorted(maps.Keys(files))
	h := sha256.New()
	for _, name := range names {
		fmt.Fprintf(h, "%s\x00%d\x00", name, len(files[name]))
		h.Write(files[name])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
