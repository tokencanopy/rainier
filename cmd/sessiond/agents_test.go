package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// The fixture every test in this file writes and reads. It is the plan's
// sanctioned stand-in for a credential and it is never a real one; the tests
// that assert absence (of log lines, of notes) look for exactly these bytes
// and for every three-byte run of them, which is what makes "nothing here
// handles the value" a checked property rather than an intention.
const agentFixture = "credential_example"

// agentFileName is the allowlisted file the fixture lives in. It deliberately
// shares no three-byte run with the fixture, so a log line naming the FILE —
// which sessiond is allowed to do — cannot be mistaken for one leaking the
// file's contents.
const agentFileName = "auth.json"

// ---------------------------------------------------------------------------
// the fake host
// ---------------------------------------------------------------------------

// fakeAgentHost is the far end of the session RPC: it drives a real
// rpcDispatcher through the stubSender the rest of this package's tests use,
// answering the two methods sessiond originates and recording what it saw.
//
// It answers on a goroutine of its own because that is how the real host
// behaves — sessiond's Call blocks until an answer arrives on the control
// channel — and it records the DECODED files so a test can assert on the bytes
// that actually travelled without ever putting them in a failure message.
type fakeAgentHost struct {
	d    *rpcDispatcher
	stub *stubSender

	mu sync.Mutex
	// version is what custody answers a fetch with, and what a successful put
	// increments and returns.
	version uint64
	// files is what a fetch hands down: file name → raw (un-encoded) content.
	files map[string]string
	// fetchErr, when set, makes a fetch a refusal carrying this sentence.
	fetchErr string
	// silent drops every request without answering, which is how a timeout is
	// produced without waiting for one.
	silent bool
	// putRefusals is how many of the next puts are refused before one lands.
	putRefusals int

	fetches []string
	puts    []agentPutRecord

	stop chan struct{}
}

// agentPutRecord is one put as the host saw it.
type agentPutRecord struct {
	Provider string
	Version  uint64
	Files    map[string]string
}

func newFakeAgentHost(t *testing.T) *fakeAgentHost {
	t.Helper()
	h := &fakeAgentHost{
		d:     newRPCDispatcher(),
		stub:  &stubSender{},
		files: map[string]string{},
		stop:  make(chan struct{}),
	}
	h.d.online(h.stub)
	go h.serve()
	t.Cleanup(func() { close(h.stop) })
	return h
}

func (h *fakeAgentHost) serve() {
	seen := 0
	for {
		select {
		case <-h.stop:
			return
		default:
		}
		frames := h.stub.sentStrings()
		for ; seen < len(frames); seen++ {
			h.answer(frames[seen])
		}
		time.Sleep(time.Millisecond)
	}
}

func (h *fakeAgentHost) answer(frame string) {
	var ev relay.ControlEvent
	if json.Unmarshal([]byte(frame), &ev) != nil || !strings.HasPrefix(ev.Kind, "req:") {
		return
	}
	h.mu.Lock()
	silent := h.silent
	h.mu.Unlock()
	if silent {
		return
	}
	switch strings.TrimPrefix(ev.Kind, "req:") {
	case runner.MethodFetchAgentCredentials:
		h.answerFetch(ev)
	case runner.MethodPutAgentCredentials:
		h.answerPut(ev)
	}
}

func (h *fakeAgentHost) answerFetch(ev relay.ControlEvent) {
	var in struct {
		Provider string `json:"provider"`
	}
	json.Unmarshal(ev.Payload, &in)

	h.mu.Lock()
	h.fetches = append(h.fetches, in.Provider)
	refusal, version := h.fetchErr, h.version
	files := map[string]string{}
	for name, body := range h.files {
		files[name] = base64.StdEncoding.EncodeToString([]byte(body))
	}
	h.mu.Unlock()

	if refusal != "" {
		h.reply(ev.ID, false, map[string]string{"error": refusal})
		return
	}
	h.reply(ev.ID, true, map[string]any{"version": version, "files": files})
}

func (h *fakeAgentHost) answerPut(ev relay.ControlEvent) {
	var in struct {
		Provider string            `json:"provider"`
		Files    map[string]string `json:"files"`
		Version  uint64            `json:"version"`
	}
	json.Unmarshal(ev.Payload, &in)

	h.mu.Lock()
	rec := agentPutRecord{Provider: in.Provider, Version: in.Version, Files: map[string]string{}}
	for name, b64 := range in.Files {
		blob, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			rec.Files[name] = "<not base64>"
			continue
		}
		rec.Files[name] = string(blob)
	}
	h.puts = append(h.puts, rec)
	refuse := h.putRefusals > 0
	if refuse {
		h.putRefusals--
	} else {
		h.version++
		h.files = map[string]string{}
		for name, body := range rec.Files {
			h.files[name] = body
		}
	}
	version := h.version
	h.mu.Unlock()

	if refuse {
		h.reply(ev.ID, false, map[string]string{"error": "the set could not be stored"})
		return
	}
	h.reply(ev.ID, true, map[string]any{"version": version})
}

func (h *fakeAgentHost) reply(id uint64, ok bool, body any) {
	raw, err := json.Marshal(body)
	if err != nil {
		return
	}
	frame, err := json.Marshal(relay.ControlEvent{Kind: "resp", ID: id, OK: ok, Payload: raw})
	if err != nil {
		return
	}
	h.d.OnControl(frame)
}

func (h *fakeAgentHost) putCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.puts)
}

func (h *fakeAgentHost) putRecords() []agentPutRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]agentPutRecord(nil), h.puts...)
}

func (h *fakeAgentHost) setFetch(version uint64, files map[string]string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.version = version
	h.files = files
}

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

// agentTestEntry points the mount root at a temp directory for the duration of
// one test and returns the entry a manifest would have carried.
func agentTestEntry(t *testing.T) agentEntry {
	t.Helper()
	root := t.TempDir()
	old := agentsMountRoot
	agentsMountRoot = root
	t.Cleanup(func() { agentsMountRoot = old })
	return agentEntry{Provider: "test", Dir: filepath.Join(root, "test"), Files: []string{agentFileName}}
}

// newTestAgentSync builds a sync whose every bound is small enough for a test
// to wait on.
func newTestAgentSync(h *fakeAgentHost, entries []agentEntry, events chan []byte) *agentSync {
	a := newAgentSync(h.d, entries, events)
	a.interval = 5 * time.Millisecond
	a.callTimeout = 500 * time.Millisecond
	a.connWait = 500 * time.Millisecond
	a.backoffMin = 10 * time.Millisecond
	a.backoffMax = 40 * time.Millisecond
	return a
}

// notes decodes every agent note sitting on an events channel.
func notes(t *testing.T, events chan []byte) []agentNote {
	t.Helper()
	var out []agentNote
	for {
		select {
		case p := <-events:
			var n agentNote
			if err := json.Unmarshal(p, &n); err != nil {
				t.Fatalf("decoding a control payload: %v", err)
			}
			out = append(out, n)
		default:
			return out
		}
	}
}

// writeAgentFixture writes body into the entry's allowlisted file the way an
// agent inside the sandbox would.
func writeAgentFixture(t *testing.T, e agentEntry, name, body string) {
	t.Helper()
	if err := os.MkdirAll(e.Dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(e.Dir, name), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	// The loop notices a change by size and mtime; a test that writes twice
	// within one filesystem timestamp tick would otherwise be invisible to it.
	touchAgentFixture(t, filepath.Join(e.Dir, name))
}

func touchAgentFixture(t *testing.T, path string) {
	t.Helper()
	when := time.Now().Add(time.Duration(-1-agentTouch) * time.Second)
	agentTouch++
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatal(err)
	}
}

// agentTouch keeps each Chtimes distinct so consecutive writes in one test are
// distinct stamps.
var agentTouch int

// ---------------------------------------------------------------------------
// the stage
// ---------------------------------------------------------------------------

// TestAgentsStageWritesFetchedFilesReadOnlyToOwner: what custody holds lands on
// disk 0600 in a 0700 directory, the baseline records the version it came with,
// and nothing is sent back up — a set that was just fetched is by definition
// equal to custody.
func TestAgentsStageWritesFetchedFilesReadOnlyToOwner(t *testing.T) {
	e := agentTestEntry(t)
	h := newFakeAgentHost(t)
	h.setFetch(3, map[string]string{agentFileName: agentFixture})
	events := make(chan []byte, 8)
	a := newTestAgentSync(h, []agentEntry{e}, events)

	a.boot()

	path := filepath.Join(e.Dir, agentFileName)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("the fetched file is not on disk: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("mode = %o, want 600 — a set readable by anyone else is a set that leaked", got)
	}
	dir, err := os.Stat(e.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := dir.Mode().Perm(); got != 0o700 {
		t.Fatalf("directory mode = %o, want 700", got)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != agentFixture {
		t.Fatalf("the file on disk is not what custody sent (%d bytes)", len(body))
	}
	if v, ok := a.baseline("test"); !ok || v != 3 {
		t.Fatalf("baseline version = %d (%v), want the fetched 3", v, ok)
	}
	if got := notes(t, events); len(got) != 0 {
		t.Fatalf("notes = %+v, want none — a fetch that worked is not news", got)
	}

	// And the loop does not put it straight back: the baseline is custody's
	// own bytes, so nothing has changed yet.
	a.tick(time.Now())
	if n := h.putCount(); n != 0 {
		t.Fatalf("%d put(s) after a fetch, want 0", n)
	}
}

// TestAgentsStageWithNoCredentialStartsTheAgentAnyway: version 0 with no files
// is custody's truthful answer for a person who has not logged this agent in.
// It is an answer, not a refusal — the directory is made, nothing is written,
// and no note is raised.
func TestAgentsStageWithNoCredentialStartsTheAgentAnyway(t *testing.T) {
	e := agentTestEntry(t)
	h := newFakeAgentHost(t)
	h.setFetch(0, nil)
	events := make(chan []byte, 8)
	a := newTestAgentSync(h, []agentEntry{e}, events)

	a.boot()

	entries, err := os.ReadDir(e.Dir)
	if err != nil {
		t.Fatalf("the directory was not made: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("%d file(s) in a home custody has nothing for, want 0", len(entries))
	}
	if v, ok := a.baseline("test"); !ok || v != 0 {
		t.Fatalf("baseline version = %d (%v), want 0", v, ok)
	}
	if got := notes(t, events); len(got) != 0 {
		t.Fatalf("notes = %+v, want none — having no login yet is not a problem to report", got)
	}
	a.tick(time.Now())
	if n := h.putCount(); n != 0 {
		t.Fatalf("%d put(s) for an empty home, want 0", n)
	}
}

// TestAgentsStageRefusalIsANoteNotAFailure: a refusal and a timeout both leave
// the session running. The agent starts, asks the person to log in — which is
// the truthful state — and the reason travels as a note on the events channel.
func TestAgentsStageRefusalIsANoteNotAFailure(t *testing.T) {
	const sentence = "you are no longer a member of this workspace"

	t.Run("a refusal", func(t *testing.T) {
		e := agentTestEntry(t)
		h := newFakeAgentHost(t)
		h.mu.Lock()
		h.fetchErr = sentence
		h.mu.Unlock()
		events := make(chan []byte, 8)
		a := newTestAgentSync(h, []agentEntry{e}, events)

		a.boot()

		if _, err := os.Stat(e.Dir); err != nil {
			t.Fatalf("the home was not made ready for an agent that starts anyway: %v", err)
		}
		got := notes(t, events)
		if len(got) != 1 {
			t.Fatalf("notes = %+v, want exactly one", got)
		}
		if got[0].Kind != agentNoteKind || got[0].Provider != "test" || got[0].Text != sentence {
			t.Fatalf("note = %+v, want the refusal's own sentence under %q", got[0], agentNoteKind)
		}
	})

	t.Run("a timeout", func(t *testing.T) {
		e := agentTestEntry(t)
		h := newFakeAgentHost(t)
		h.mu.Lock()
		h.silent = true
		h.mu.Unlock()
		events := make(chan []byte, 8)
		a := newTestAgentSync(h, []agentEntry{e}, events)

		a.boot()

		got := notes(t, events)
		if len(got) != 1 {
			t.Fatalf("notes = %+v, want exactly one", got)
		}
		if got[0].Kind != agentNoteKind || got[0].Text == "" {
			t.Fatalf("note = %+v, want a note saying why", got[0])
		}
	})
}

// TestMissingMountFailsTheStageWithTheSentence is the ONE way this whole
// mechanism is allowed to fail a session: a runner too old to mount the home
// cannot be worked around from inside, and a session that pretended otherwise
// would lose every login the person made in it.
func TestMissingMountFailsTheStageWithTheSentence(t *testing.T) {
	for _, tc := range []struct {
		name string
		root func(t *testing.T) string
	}{
		{"an absent mount", func(t *testing.T) string { return filepath.Join(t.TempDir(), "absent") }},
		{"a mount that is not a directory", func(t *testing.T) string {
			path := filepath.Join(t.TempDir(), "file")
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			return path
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := tc.root(t)
			old := agentsMountRoot
			agentsMountRoot = root
			defer func() { agentsMountRoot = old }()

			dir := t.TempDir()
			env := bootEnv{AgentsB64: encodeAgentsB64(t, []agentEntry{{
				Provider: "test", Dir: filepath.Join(root, "test"), Files: []string{agentFileName},
			}})}
			stages, _, err := prepareBoot(dir, dir, env)
			if err != nil {
				t.Fatalf("prepareBoot: %v", err)
			}
			if len(stages) != 1 || stages[0].Name != stageAgents {
				t.Fatalf("stages = %+v, want the one agents stage", stages)
			}

			out, err := exec.Command("sh", stages[0].ScriptPath).CombinedOutput()
			if err == nil {
				t.Fatal("the stage succeeded; a session whose homes are not mounted must not start")
			}
			if !strings.Contains(string(out), agentMountSentence) {
				t.Fatalf("output = %q, want it to be exactly the named sentence", out)
			}
		})
	}
}

// TestAgentsStageRunsAfterTheCloneAndBeforeInit pins the ordering the design
// asks for: the homes are filled once the repositories are on disk and before
// the environment's per-boot hook, which may itself run the agent.
func TestAgentsStageRunsAfterTheCloneAndBeforeInit(t *testing.T) {
	root := t.TempDir()
	old := agentsMountRoot
	agentsMountRoot = root
	defer func() { agentsMountRoot = old }()

	dir := t.TempDir()
	env := bootEnv{
		SetupB64: base64.StdEncoding.EncodeToString([]byte("echo setup\n")),
		ReposB64: base64.StdEncoding.EncodeToString([]byte(`[{"owner":"o","name":"n","base_branch":"main","session_branch":"s","dir":"n"}]`)),
		InitB64:  base64.StdEncoding.EncodeToString([]byte("echo init\n")),
		AgentsB64: encodeAgentsB64(t, []agentEntry{{
			Provider: "test", Dir: filepath.Join(root, "test"), Files: []string{agentFileName},
		}}),
	}
	stages, _, err := prepareBoot(dir, dir, env)
	if err != nil {
		t.Fatalf("prepareBoot: %v", err)
	}
	var names []string
	for _, st := range stages {
		names = append(names, st.Name)
	}
	want := []string{stageSetup, stageClone, stageAgents, stageInit}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("stages = %v, want %v", names, want)
	}

	// The stage waits for sessiond and then gets out of the way. With the
	// marker already there it must not add a second to every boot.
	if err := os.WriteFile(filepath.Join(dir, agentsDoneName), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if out, err := exec.Command("sh", stages[2].ScriptPath).CombinedOutput(); err != nil {
		t.Fatalf("the agents stage failed with a home that is ready: %v (%s)", err, out)
	}
	if took := time.Since(start); took > 900*time.Millisecond {
		t.Fatalf("the stage took %s with the marker already written", took)
	}
}

// TestAgentsStageWaitsForTheHomesAndNeverFailsOverThem: the stage exists to
// order the chain, not to judge it. It blocks until sessiond says the homes are
// as good as they are going to get, and it exits 0 either way.
func TestAgentsStageWaitsForTheHomesAndNeverFailsOverThem(t *testing.T) {
	dir := t.TempDir()
	done := filepath.Join(dir, agentsDoneName)
	script := filepath.Join(dir, agentsScriptName)
	if err := os.WriteFile(script, []byte(agentsWaitScript(done, 60)), 0o755); err != nil {
		t.Fatal(err)
	}

	out := make(chan error, 1)
	go func() {
		_, err := exec.Command("sh", script).CombinedOutput()
		out <- err
	}()
	select {
	case err := <-out:
		t.Fatalf("the stage ended before the homes were ready (%v)", err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := os.WriteFile(done, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-out:
		if err != nil {
			t.Fatalf("the stage failed once the homes were ready: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stage did not notice the marker")
	}
}

// encodeAgentsB64 renders entries the way controlapp.AgentsEnv does.
func encodeAgentsB64(t *testing.T, entries []agentEntry) string {
	t.Helper()
	raw, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// TestAgentsManifestIsReadLikeTheRepositoryList: an absent variable means no
// homes and nothing runs; an unreadable one is not allowed to fail a session
// over custody, and a row that would write outside the mount is dropped.
func TestAgentsManifestIsReadLikeTheRepositoryList(t *testing.T) {
	root := t.TempDir()
	old := agentsMountRoot
	agentsMountRoot = root
	defer func() { agentsMountRoot = old }()

	if got := agentEntries(bootEnv{}); len(got) != 0 {
		t.Fatalf("entries = %+v with no variable, want none", got)
	}
	if got := agentEntries(bootEnv{AgentsB64: "not base64"}); len(got) != 0 {
		t.Fatalf("entries = %+v for an unreadable manifest, want none", got)
	}
	outside := encodeAgentsB64(t, []agentEntry{
		{Provider: "a", Dir: filepath.Join(root, "a"), Files: []string{agentFileName}},
		{Provider: "b", Dir: "/workspace/b", Files: []string{agentFileName}},
		{Provider: "c", Dir: filepath.Join(root, "c"), Files: []string{"../../escape"}},
		{Provider: "", Dir: filepath.Join(root, "d"), Files: []string{agentFileName}},
	})
	got := agentEntries(bootEnv{AgentsB64: outside})
	if len(got) != 1 || got[0].Provider != "a" {
		t.Fatalf("entries = %+v, want only the row inside the mount with a bare file name", got)
	}
}

// TestHomeVarIsSetForTheAgentOnly covers the A1-false path: a provider that
// also writes under $HOME gets that variable pointed inside its own directory,
// in the chain's environment — never in the container's, which every session on
// the runner would share.
func TestHomeVarIsSetForTheAgentOnly(t *testing.T) {
	root := t.TempDir()
	old := agentsMountRoot
	agentsMountRoot = root
	defer func() { agentsMountRoot = old }()

	dir := t.TempDir()
	e := agentEntry{Provider: "test", Dir: filepath.Join(root, "test"), Files: []string{agentFileName}, HomeVar: "HOME"}
	_, vars, err := prepareBoot(dir, dir, bootEnv{AgentsB64: encodeAgentsB64(t, []agentEntry{e})})
	if err != nil {
		t.Fatalf("prepareBoot: %v", err)
	}
	want := filepath.Join(e.Dir, agentHomeSubdir)
	var found bool
	for _, v := range vars {
		if v.Name == "HOME" {
			found = true
			if v.Value != want {
				t.Fatalf("HOME = %q, want %q", v.Value, want)
			}
		}
	}
	if !found {
		t.Fatalf("vars = %+v, want HOME among them", vars)
	}
	info, err := os.Stat(want)
	if err != nil {
		t.Fatalf("the home directory was not made: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("mode = %o, want 700", got)
	}
	if os.Getenv("HOME") == want {
		t.Fatal("HOME was set in this process; the variable is the agent's, not the container's")
	}
}

// ---------------------------------------------------------------------------
// the sync loop
// ---------------------------------------------------------------------------

// TestSyncPutsExactlyOnceOnChange is the loop's whole contract: a login inside
// the session reaches custody once, and a rewrite of the same bytes — which is
// what a tool touching its own config file produces all day — reaches it not
// at all.
func TestSyncPutsExactlyOnceOnChange(t *testing.T) {
	e := agentTestEntry(t)
	h := newFakeAgentHost(t)
	h.setFetch(0, nil)
	events := make(chan []byte, 8)
	a := newTestAgentSync(h, []agentEntry{e}, events)
	a.boot()

	writeAgentFixture(t, e, agentFileName, agentFixture)
	a.tick(time.Now())

	puts := h.putRecords()
	if len(puts) != 1 {
		t.Fatalf("%d put(s) after one write, want exactly 1", len(puts))
	}
	if puts[0].Provider != "test" || puts[0].Files[agentFileName] != agentFixture {
		t.Fatalf("the put carried %d file(s) for %q, want the written one", len(puts[0].Files), puts[0].Provider)
	}
	if puts[0].Version != 0 {
		t.Fatalf("put version = %d, want the 0 this session last saw", puts[0].Version)
	}
	if v, ok := a.baseline("test"); !ok || v != 1 {
		t.Fatalf("baseline = %d (%v), want the 1 custody answered with", v, ok)
	}

	// The same bytes again, with a new size/mtime stamp: the stat changed, the
	// set did not, and nothing is sent.
	writeAgentFixture(t, e, agentFileName, agentFixture)
	a.tick(time.Now())
	a.tick(time.Now())
	if n := h.putCount(); n != 1 {
		t.Fatalf("%d put(s) after rewriting identical bytes, want the original 1", n)
	}

	// Different bytes are a different set, and go up with the version custody
	// last answered with.
	writeAgentFixture(t, e, agentFileName, agentFixture+"2")
	a.tick(time.Now())
	puts = h.putRecords()
	if len(puts) != 2 {
		t.Fatalf("%d put(s) after a real change, want 2", len(puts))
	}
	if puts[1].Version != 1 || puts[1].Files[agentFileName] != agentFixture+"2" {
		t.Fatalf("the second put = %+v, want the new bytes at v1", puts[1].Version)
	}
}

// TestSyncRetriesAndNeverBlocks: a put that custody refuses is retried on a
// backoff, and the session goes on running while it is. The agent never waits
// on custody — the whole mechanism is allowed to be late, never to be in the
// way.
func TestSyncRetriesAndNeverBlocks(t *testing.T) {
	e := agentTestEntry(t)
	h := newFakeAgentHost(t)
	h.setFetch(0, nil)
	h.mu.Lock()
	h.putRefusals = 2
	h.mu.Unlock()
	events := make(chan []byte, 8)
	a := newTestAgentSync(h, []agentEntry{e}, events)
	a.boot()
	writeAgentFixture(t, e, agentFileName, agentFixture)

	start := time.Now()
	a.tick(start)
	if n := h.putCount(); n != 1 {
		t.Fatalf("%d put(s) after the first tick, want 1", n)
	}
	// The next tick is inside the backoff: nothing is retried yet.
	a.tick(start)
	if n := h.putCount(); n != 1 {
		t.Fatalf("%d put(s) inside the backoff, want the original 1", n)
	}
	// Two more attempts, each after its own (doubling) wait.
	a.tick(start.Add(a.backoffMin))
	if n := h.putCount(); n != 2 {
		t.Fatalf("%d put(s) after the first backoff elapsed, want 2", n)
	}
	a.tick(start.Add(10 * a.backoffMax))
	if n := h.putCount(); n != 3 {
		t.Fatalf("%d put(s) after the second backoff elapsed, want 3", n)
	}
	if v, ok := a.baseline("test"); !ok || v != 1 {
		t.Fatalf("baseline = %d (%v), want the version the put that landed answered with", v, ok)
	}
	// And once it lands, nothing keeps retrying.
	a.tick(start.Add(100 * a.backoffMax))
	if n := h.putCount(); n != 3 {
		t.Fatalf("%d put(s) after one landed, want 3", n)
	}

	// Never blocks: a whole boot-and-sync against a host that answers nothing
	// still finishes, and finishes bounded.
	silentE := agentTestEntry(t)
	silent := newFakeAgentHost(t)
	silent.mu.Lock()
	silent.silent = true
	silent.mu.Unlock()
	quiet := newTestAgentSync(silent, []agentEntry{silentE}, make(chan []byte, 8))
	writeAgentFixture(t, silentE, agentFileName, agentFixture)
	done := make(chan struct{})
	go func() {
		quiet.boot()
		quiet.tick(time.Now())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a host that answers nothing wedged the sync")
	}
}

// TestExitPerformsAFinalPut: the last thing an agent writes is usually the
// thing worth keeping — a login completed seconds before the session is torn
// down — and the 2s loop must not be what decides whether it survives.
func TestExitPerformsAFinalPut(t *testing.T) {
	e := agentTestEntry(t)
	h := newFakeAgentHost(t)
	h.setFetch(0, nil)
	events := make(chan []byte, 8)
	a := newTestAgentSync(h, []agentEntry{e}, events)
	// A loop that would never tick on its own, so the only put that can happen
	// is the one shutdown performs.
	a.interval = time.Hour

	dir := t.TempDir()
	a.start(filepath.Join(dir, agentsDoneName))
	waitFor(t, "the boot fetch to finish", func() bool {
		_, err := os.Stat(filepath.Join(dir, agentsDoneName))
		return err == nil
	})

	writeAgentFixture(t, e, agentFileName, agentFixture)
	a.close()

	puts := h.putRecords()
	if len(puts) != 1 {
		t.Fatalf("%d put(s) at shutdown, want the one final put", len(puts))
	}
	if puts[0].Files[agentFileName] != agentFixture {
		t.Fatalf("the final put carried %d file(s), want what the agent last wrote", len(puts[0].Files))
	}
}

// TestRevokeEmptiesAndResetsTheBaseline: a logout upstream reaches every live
// session. The files go, the baseline goes with them, and a login made
// afterwards in the same session is a new set rather than a re-put of the one
// that was revoked.
func TestRevokeEmptiesAndResetsTheBaseline(t *testing.T) {
	e := agentTestEntry(t)
	h := newFakeAgentHost(t)
	h.setFetch(7, map[string]string{agentFileName: agentFixture})
	events := make(chan []byte, 8)
	a := newTestAgentSync(h, []agentEntry{e}, events)
	a.boot()

	out, err := a.handleRevoke([]byte(`{"provider":"test"}`))
	if err != nil {
		t.Fatalf("the revoke was refused: %v", err)
	}
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "{}" {
		t.Fatalf("answer = %s, want {}", raw)
	}
	if _, err := os.Stat(filepath.Join(e.Dir, agentFileName)); !os.IsNotExist(err) {
		t.Fatalf("the file is still there after a revoke (%v)", err)
	}
	if v, ok := a.baseline("test"); !ok || v != 0 {
		t.Fatalf("baseline = %d (%v), want it reset to 0", v, ok)
	}
	// The empty directory is not news to put.
	a.tick(time.Now())
	if n := h.putCount(); n != 0 {
		t.Fatalf("%d put(s) after a revoke, want 0", n)
	}

	// A later write is a new set, sent with the reset version.
	writeAgentFixture(t, e, agentFileName, agentFixture+"3")
	a.tick(time.Now())
	puts := h.putRecords()
	if len(puts) != 1 {
		t.Fatalf("%d put(s) after a login following a revoke, want 1", len(puts))
	}
	if puts[0].Version != 0 || puts[0].Files[agentFileName] != agentFixture+"3" {
		t.Fatalf("the put = v%d with %d file(s), want the new set at v0", puts[0].Version, len(puts[0].Files))
	}

	// A provider this session has no home for is refused rather than answered.
	if _, err := a.handleRevoke([]byte(`{"provider":"other"}`)); err == nil {
		t.Fatal("a revoke for an unknown provider was answered ok")
	}
}

// TestOversizedAndSymlinkedFilesAreNotSent: the allowlist says which NAMES are
// the set; it does not say what someone inside the sandbox may have put behind
// those names. A symlink is not read (it would be a way to send any file in the
// session up to custody) and a set past the cap is not sent at all.
func TestOversizedAndSymlinkedFilesAreNotSent(t *testing.T) {
	t.Run("a symlink", func(t *testing.T) {
		e := agentTestEntry(t)
		h := newFakeAgentHost(t)
		h.setFetch(0, nil)
		events := make(chan []byte, 8)
		a := newTestAgentSync(h, []agentEntry{e}, events)
		a.boot()

		target := filepath.Join(t.TempDir(), "elsewhere")
		if err := os.WriteFile(target, []byte(agentFixture), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(e.Dir, agentFileName)); err != nil {
			t.Fatal(err)
		}
		a.tick(time.Now())
		a.tick(time.Now())

		if n := h.putCount(); n != 0 {
			t.Fatalf("%d put(s) for a symlinked file, want 0", n)
		}
		got := notes(t, events)
		if len(got) != 1 {
			t.Fatalf("notes = %+v, want exactly one — noted once, not once per tick", got)
		}
		if !strings.Contains(got[0].Text, agentFileName) || strings.Contains(got[0].Text, agentFixture) {
			t.Fatalf("note = %q, want it to name the file and quote nothing", got[0].Text)
		}
	})

	t.Run("a set over the cap", func(t *testing.T) {
		e := agentTestEntry(t)
		h := newFakeAgentHost(t)
		h.setFetch(0, nil)
		events := make(chan []byte, 8)
		a := newTestAgentSync(h, []agentEntry{e}, events)
		a.boot()

		writeAgentFixture(t, e, agentFileName, strings.Repeat("x", agentSetMaxBytes+1))
		a.tick(time.Now())
		a.tick(time.Now())

		if n := h.putCount(); n != 0 {
			t.Fatalf("%d put(s) for a set over the cap, want 0", n)
		}
		got := notes(t, events)
		if len(got) != 1 {
			t.Fatalf("notes = %+v, want exactly one", got)
		}

		// And when it fits again, it goes.
		writeAgentFixture(t, e, agentFileName, agentFixture)
		a.tick(time.Now())
		if n := h.putCount(); n != 1 {
			t.Fatalf("%d put(s) once the set fits again, want 1", n)
		}
	})
}

// TestNoCredentialByteReachesTheLog is the invariant the whole file exists to
// hold. sessiond may log a provider, a file NAME, a size, a version and a
// sentence; the bytes behind that name are never any of those things.
//
// The assertion is deliberately harsher than "the fixture is absent": it looks
// for every three-byte run of the fixture, so a partially quoted value, a
// base64 fragment, or a truncated error carrying part of one still fails.
func TestNoCredentialByteReachesTheLog(t *testing.T) {
	e := agentTestEntry(t)
	h := newFakeAgentHost(t)
	h.setFetch(2, map[string]string{agentFileName: agentFixture})
	events := make(chan []byte, 16)
	a := newTestAgentSync(h, []agentEntry{e}, events)

	var captured strings.Builder
	old := log.Writer()
	log.SetOutput(&captured)
	defer log.SetOutput(old)

	// The whole vocabulary, in one session: a fetch that writes, a change that
	// is put, a symlinked name, a set over the cap, and a revoke.
	a.boot()
	writeAgentFixture(t, e, agentFileName, agentFixture+"-changed")
	a.tick(time.Now())
	writeAgentFixture(t, e, agentFileName, strings.Repeat(agentFixture, 8<<10))
	a.tick(time.Now())
	if err := os.Remove(filepath.Join(e.Dir, agentFileName)); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "elsewhere")
	if err := os.WriteFile(target, []byte(agentFixture), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(e.Dir, agentFileName)); err != nil {
		t.Fatal(err)
	}
	a.tick(time.Now())
	if _, err := a.handleRevoke([]byte(`{"provider":"test"}`)); err != nil {
		t.Fatalf("the revoke was refused: %v", err)
	}

	log.SetOutput(old)
	assertNoFixtureBytes(t, "the log", captured.String())
	for _, n := range notes(t, events) {
		assertNoFixtureBytes(t, "a note", n.Text)
	}
	// The put that DID travel carried the bytes, which is the point of it —
	// this test is about everything that is not the wire.
	if n := h.putCount(); n == 0 {
		t.Fatal("nothing was put; this test would pass vacuously")
	}
}

// assertNoFixtureBytes fails if s contains the fixture, its base64, or any
// three-byte run of either.
func assertNoFixtureBytes(t *testing.T, what, s string) {
	t.Helper()
	b64 := base64.StdEncoding.EncodeToString([]byte(agentFixture))
	for _, secret := range []string{agentFixture, b64} {
		for i := 0; i+3 <= len(secret); i++ {
			run := secret[i : i+3]
			if strings.Contains(s, run) {
				t.Fatalf("%s contains %q, a three-byte run of what a session must never disclose:\n%s",
					what, run, redactForFailure(s, run))
			}
		}
	}
}

// redactForFailure shows WHERE the run was found without reprinting a value: it
// replaces the run itself so a failure message is readable without becoming the
// leak it is reporting.
func redactForFailure(s, run string) string {
	i := strings.Index(s, run)
	start := max(0, i-60)
	end := min(len(s), i+60)
	return fmt.Sprintf("… %s …", strings.ReplaceAll(s[start:end], run, "***"))
}
