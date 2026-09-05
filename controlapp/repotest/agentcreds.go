package repotest

import (
	"bytes"
	"context"
	"testing"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
)

// The executable contract of controlapp.AgentCredentialStore: custody of one
// person's coding-agent logins, which the self-hosted in-memory store, the
// self-hosted Postgres store, and (later) a hosted cell's own store all pass
// unchanged. The port's doc comments say what the behavior is; these cases
// are where it is pinned.
//
// The suite is written against TWO people and TWO providers, because most of
// what this contract has to say is about keys: a store that lets one person's
// set answer another's, or one provider's answer another's, fails here rather
// than in somebody's session.
//
// It holds no credential. The fixture bytes are the literal strings
// credential_example and auth_example, and the people are user_example and
// user_other — the same synthetic vocabulary the rest of this package uses.

// The two people every case is written against. They are exported because a
// store with a foreign key from its credential rows to its user rows (the
// self-hosted Postgres schema has one) must create these rows before the
// suite can write against them, and it can only do that if it knows their
// ids.
const (
	AgentUser  control.ActorID = "user_example"
	AgentOther control.ActorID = "user_other"
)

// The fixture bytes. They are not a credential, they are the WORD for one:
// every test in this plan asserts that these exact strings never reach a log,
// an error, or a response, and a fixture that looked like a real token would
// make that assertion impossible to trust.
var (
	agentFixtureCredential = []byte("credential_example")
	agentFixtureAuth       = []byte("auth_example")
)

// RunAgentCredentialStore drives the contract. open is called once per case
// and must return an EMPTY store each time — a store carrying a set from the
// previous case would pass cases it should fail.
//
// The provider names come from controlapp.AgentProviders() rather than being
// spelled here, which is the plan's rule everywhere below controlapp: a
// provider is a string the table defines and everything else merely carries.
func RunAgentCredentialStore(t *testing.T, open func(t *testing.T) controlapp.AgentCredentialStore) {
	providers := controlapp.AgentProviders()
	if len(providers) < 2 {
		t.Fatalf("the provider table has %d rows; this suite needs two to prove they are kept apart", len(providers))
	}
	first, second := providers[0], providers[1]
	fileOf := func(p controlapp.AgentProvider) string {
		if len(p.Files) == 0 {
			t.Fatalf("provider %q declares no files; there is nothing to store", p.Name)
		}
		return p.Files[0]
	}

	for _, c := range []struct {
		name string
		fn   func(*testing.T, controlapp.AgentCredentialStore, string, string, string, string)
	}{
		{"A1 a set nobody has put is version 0 with no files", caseAgentFetchEmpty},
		{"A2 puts version upward and the last bytes stand", caseAgentPutVersions},
		{"A3 a revoke destroys the set and is idempotent", caseAgentRevoke},
		{"A4 a listing carries versions and no bytes", caseAgentList},
		{"A5 two people never see each other's set", caseAgentUserIsolation},
		{"A6 two providers of one person are separate sets", caseAgentProviderIsolation},
		{"A7 a put of no files is a real version", caseAgentEmptySet},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := open(t)
			c.fn(t, s, first.Name, fileOf(first), second.Name, fileOf(second))
		})
	}
}

// caseAgentFetchEmpty: the boot-time answer for a person who has not logged
// this agent in. It is an ANSWER — version 0, no files, no error — because
// the sandbox starts the agent anyway and lets them log in, and a refusal
// here would turn "not logged in yet" into a failed session.
func caseAgentFetchEmpty(t *testing.T, s controlapp.AgentCredentialStore, provider, file, _, _ string) {
	ctx := context.Background()
	set, err := s.FetchAgentCredentials(ctx, AgentUser, provider)
	if err != nil {
		t.Fatalf("fetch before any put: %v", err)
	}
	if set.Version != 0 || len(set.Files) != 0 {
		t.Fatalf("fetch before any put = version %d with %d files, want version 0 with none", set.Version, len(set.Files))
	}
}

// caseAgentPutVersions: a put returns the version the set now has, versions
// count puts from 1, and a fetch answers with the LAST bytes written. The
// version is what a sandbox and a `rainier agent ls` both read to know
// whether a login landed, so a store that reused one would make a completed
// login indistinguishable from a failed one.
func caseAgentPutVersions(t *testing.T, s controlapp.AgentCredentialStore, provider, file, _, _ string) {
	ctx := context.Background()
	v, err := s.PutAgentCredentials(ctx, AgentUser, provider, map[string][]byte{file: agentFixtureCredential})
	if err != nil {
		t.Fatalf("first put: %v", err)
	}
	if v != 1 {
		t.Fatalf("first put returned version %d, want 1", v)
	}
	wantSet(t, s, AgentUser, provider, 1, map[string][]byte{file: agentFixtureCredential})

	v, err = s.PutAgentCredentials(ctx, AgentUser, provider, map[string][]byte{file: agentFixtureAuth})
	if err != nil {
		t.Fatalf("second put: %v", err)
	}
	if v != 2 {
		t.Fatalf("second put returned version %d, want 2", v)
	}
	// Last-writer-wins, and a whole-set replace: the second put's bytes are
	// the set, not an addition to it.
	wantSet(t, s, AgentUser, provider, 2, map[string][]byte{file: agentFixtureAuth})
}

// caseAgentRevoke: a revoke returns the store to "never logged in", and
// revoking twice is not an error. Idempotence is not a nicety here — a logout
// retried after a timeout must not fail the second time and leave a person
// believing they are still logged in.
func caseAgentRevoke(t *testing.T, s controlapp.AgentCredentialStore, provider, file, _, _ string) {
	ctx := context.Background()
	if _, err := s.PutAgentCredentials(ctx, AgentUser, provider, map[string][]byte{file: agentFixtureCredential}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := s.RevokeAgentCredentials(ctx, AgentUser, provider); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	wantSet(t, s, AgentUser, provider, 0, nil)

	if err := s.RevokeAgentCredentials(ctx, AgentUser, provider); err != nil {
		t.Fatalf("second revoke: %v", err)
	}
	if err := s.RevokeAgentCredentials(ctx, AgentOther, provider); err != nil {
		t.Fatalf("revoke of a set that never existed: %v", err)
	}
}

// caseAgentList: a listing names the providers a person has a set for and the
// version of each. The status type carries no bytes at all, which is the
// point — there is no listing path in this contract on which a credential
// could travel, by construction rather than by discipline.
func caseAgentList(t *testing.T, s controlapp.AgentCredentialStore, first, firstFile, second, secondFile string) {
	ctx := context.Background()
	if got, err := s.ListAgentCredentials(ctx, AgentUser); err != nil || len(got) != 0 {
		t.Fatalf("list before any put = %v, %v; want an empty listing and no error", got, err)
	}
	if _, err := s.PutAgentCredentials(ctx, AgentUser, first, map[string][]byte{firstFile: agentFixtureCredential}); err != nil {
		t.Fatalf("put %s: %v", first, err)
	}
	if _, err := s.PutAgentCredentials(ctx, AgentUser, second, map[string][]byte{secondFile: agentFixtureAuth}); err != nil {
		t.Fatalf("put %s: %v", second, err)
	}
	if _, err := s.PutAgentCredentials(ctx, AgentUser, second, map[string][]byte{secondFile: agentFixtureAuth}); err != nil {
		t.Fatalf("second put %s: %v", second, err)
	}

	got, err := s.ListAgentCredentials(ctx, AgentUser)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	versions := map[string]uint64{}
	for _, st := range got {
		if _, dup := versions[st.Provider]; dup {
			t.Fatalf("provider %q listed twice", st.Provider)
		}
		versions[st.Provider] = st.Version
		if st.UpdatedAt.IsZero() {
			t.Fatalf("provider %q listed with a zero timestamp", st.Provider)
		}
	}
	if len(versions) != 2 || versions[first] != 1 || versions[second] != 2 {
		t.Fatalf("listing = %v, want %s at 1 and %s at 2", versions, first, second)
	}
	if got, err := s.ListAgentCredentials(ctx, AgentOther); err != nil || len(got) != 0 {
		t.Fatalf("another person's listing = %v, %v; want empty and no error", got, err)
	}
}

// caseAgentUserIsolation: one person's set is not another's, at every method.
// The store is keyed by (user, provider) and takes the user as an argument,
// so a store that ignored it would pass every other case in this suite and
// hand every session the same login.
func caseAgentUserIsolation(t *testing.T, s controlapp.AgentCredentialStore, provider, file, _, _ string) {
	ctx := context.Background()
	if _, err := s.PutAgentCredentials(ctx, AgentUser, provider, map[string][]byte{file: agentFixtureCredential}); err != nil {
		t.Fatalf("put for %s: %v", AgentUser, err)
	}
	wantSet(t, s, AgentOther, provider, 0, nil)

	if _, err := s.PutAgentCredentials(ctx, AgentOther, provider, map[string][]byte{file: agentFixtureAuth}); err != nil {
		t.Fatalf("put for %s: %v", AgentOther, err)
	}
	// Each person's version counts THEIR puts: the second person's first put
	// is version 1, not 2.
	wantSet(t, s, AgentOther, provider, 1, map[string][]byte{file: agentFixtureAuth})
	wantSet(t, s, AgentUser, provider, 1, map[string][]byte{file: agentFixtureCredential})

	if err := s.RevokeAgentCredentials(ctx, AgentOther, provider); err != nil {
		t.Fatalf("revoke for %s: %v", AgentOther, err)
	}
	wantSet(t, s, AgentUser, provider, 1, map[string][]byte{file: agentFixtureCredential})
}

// caseAgentProviderIsolation: one person's two agents are two sets. Logging
// out of one must not log them out of the other, and the versions advance
// independently.
func caseAgentProviderIsolation(t *testing.T, s controlapp.AgentCredentialStore, first, firstFile, second, secondFile string) {
	ctx := context.Background()
	if _, err := s.PutAgentCredentials(ctx, AgentUser, first, map[string][]byte{firstFile: agentFixtureCredential}); err != nil {
		t.Fatalf("put %s: %v", first, err)
	}
	wantSet(t, s, AgentUser, second, 0, nil)

	if _, err := s.PutAgentCredentials(ctx, AgentUser, second, map[string][]byte{secondFile: agentFixtureAuth}); err != nil {
		t.Fatalf("put %s: %v", second, err)
	}
	if err := s.RevokeAgentCredentials(ctx, AgentUser, first); err != nil {
		t.Fatalf("revoke %s: %v", first, err)
	}
	wantSet(t, s, AgentUser, first, 0, nil)
	wantSet(t, s, AgentUser, second, 1, map[string][]byte{secondFile: agentFixtureAuth})
}

// caseAgentEmptySet: a put of no files is allowed and yields a NEW version
// holding nothing. It is how "the agent removed its own credential file"
// reaches custody — a real state, and distinct from both "never logged in"
// (version 0) and "still holds what it had", which is why it must advance the
// version rather than being dropped as a no-op.
func caseAgentEmptySet(t *testing.T, s controlapp.AgentCredentialStore, provider, file, _, _ string) {
	ctx := context.Background()
	if _, err := s.PutAgentCredentials(ctx, AgentUser, provider, map[string][]byte{file: agentFixtureCredential}); err != nil {
		t.Fatalf("put: %v", err)
	}
	v, err := s.PutAgentCredentials(ctx, AgentUser, provider, map[string][]byte{})
	if err != nil {
		t.Fatalf("put of an empty set: %v", err)
	}
	if v != 2 {
		t.Fatalf("put of an empty set returned version %d, want 2", v)
	}
	set, err := s.FetchAgentCredentials(ctx, AgentUser, provider)
	if err != nil {
		t.Fatalf("fetch after an empty put: %v", err)
	}
	if set.Version != 2 || len(set.Files) != 0 {
		t.Fatalf("fetch after an empty put = version %d with %d files, want version 2 with none", set.Version, len(set.Files))
	}

	// A nil map is the same statement as an empty one, and stores the same
	// way: callers build these maps by ranging over an allowlist, and a
	// provider whose files are all gone produces nil on some paths and an
	// empty map on others.
	if v, err := s.PutAgentCredentials(ctx, AgentUser, provider, nil); err != nil || v != 3 {
		t.Fatalf("put of a nil set = %d, %v; want version 3 and no error", v, err)
	}
}

// wantSet asserts one fetch: the version, and the files byte for byte. A
// mismatch prints the NAMES and the lengths, never the bytes — this suite's
// fixtures are the words for a credential, and a failure that dumped them
// would teach everyone reading it that dumping a set in a failure is fine.
func wantSet(t *testing.T, s controlapp.AgentCredentialStore, user control.ActorID,
	provider string, wantVersion uint64, wantFiles map[string][]byte) {
	t.Helper()
	set, err := s.FetchAgentCredentials(context.Background(), user, provider)
	if err != nil {
		t.Fatalf("fetch %s/%s: %v", user, provider, err)
	}
	if set.Version != wantVersion {
		t.Fatalf("fetch %s/%s = version %d, want %d", user, provider, set.Version, wantVersion)
	}
	if len(set.Files) != len(wantFiles) {
		t.Fatalf("fetch %s/%s returned %d files, want %d", user, provider, len(set.Files), len(wantFiles))
	}
	for name, want := range wantFiles {
		got, ok := set.Files[name]
		if !ok {
			t.Fatalf("fetch %s/%s is missing file %q", user, provider, name)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("fetch %s/%s file %q is %d bytes, want %d and equal", user, provider, name, len(got), len(want))
		}
	}
}
