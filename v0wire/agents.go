// v0wire/agents.go
package v0wire

import (
	"slices"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
)

// The two statuses an agent row can report. They are the whole vocabulary on
// purpose: custody either holds a credential set for this person and this
// agent or it does not, and every richer thing a client might want to know —
// whether the token still works, when it expires, which account it belongs to
// — is knowledge only the provider has, and knowledge Rainier deliberately
// never asks it for.
const (
	AgentStatusLoggedIn = "logged_in"
	AgentStatusNone     = "none"
)

// AgentView is the client-facing rendering of one coding agent's login: which
// agent, whether this person has logged it in, when custody last saw it move,
// which version it is at, and the workspaces the login reaches.
//
// It has nowhere to put a credential, and that is the durable form of the
// promise: controlapp.AgentCredentialStatus — the only thing RenderAgents
// takes — has no bytes on it either, so no listing path from the store to a
// client passes through a type that could carry one.
//
// There is no "note" field. sessiond emits a boot note when a fetch is
// refused (plan §Task 3), but nothing routes those notes from the runner back
// to the control plane yet, so a note key here would be a promise this
// version cannot keep. The CLI says "the agent wrote no credential" instead,
// which is the truth it can actually establish.
type AgentView struct {
	Provider string `json:"provider"`
	Status   string `json:"status"`
	// Since is when custody last recorded a change to this set, and null for a
	// row that says "none" — there is no credential, so there is no instant
	// one was last written.
	//
	// A POINTER rendered as null rather than an omitted key, exactly like
	// SessionView.ChildExitCode and for the package's own reason (doc.go): no
	// field here is omitempty, because a key that appears only sometimes
	// cannot be told apart by a client from an older server that never had
	// it. The key set of this view is identical on every row.
	Since *time.Time `json:"since"`
	// Version is custody's counter for the set: it goes up by one on every
	// put, and it is what `rainier agent login` compares before and after a
	// login session to tell "the agent wrote a credential" from "the person
	// exited without finishing". Zero for a provider with no set.
	Version uint64 `json:"version"`
	// Workspaces are the workspaces the caller is a member of — where this
	// login is, or would be, in force. It is the same list on every row
	// because it describes the CALLER, not the credential: one login reaches
	// every workspace the person belongs to, which is the whole point of
	// keying custody by (user, provider) rather than by workspace.
	Workspaces []string `json:"workspaces"`
}

// AgentsEnvelope is the JSON shape of GET /v0/agents.
type AgentsEnvelope struct {
	Agents []AgentView `json:"agents"`
}

// RenderAgents renders one person's agent logins for the workspaces they
// belong to. It lists EVERY provider this build knows, in the table's own
// order, because the listing answers "which agents can I log in, and have I"
// — a client that only got back the ones already logged in would have no way
// to discover the others, and `rainier agent ls` would show an empty table to
// exactly the person who most needs to see what is on offer.
//
// statuses is what custody holds; the order and the completeness of the
// answer are the table's. A status naming a provider outside the table is
// dropped rather than rendered: it can only be a set left behind by a build
// that had a row this one does not, and a client cannot act on it.
func RenderAgents(statuses []controlapp.AgentCredentialStatus, workspaces []control.WorkspaceID) AgentsEnvelope {
	held := make(map[string]controlapp.AgentCredentialStatus, len(statuses))
	for _, st := range statuses {
		held[st.Provider] = st
	}
	ws := make([]string, 0, len(workspaces))
	for _, w := range workspaces {
		ws = append(ws, string(w))
	}

	rows := controlapp.AgentProviders()
	out := make([]AgentView, 0, len(rows))
	for _, p := range rows {
		// Each row gets its OWN copy: the views are handed to an encoder that
		// does not care, but a caller that sorts or edits one row's list must
		// not be editing every row's.
		view := AgentView{
			Provider:   p.Name,
			Status:     AgentStatusNone,
			Workspaces: emptyIfNil(slices.Clone(ws)),
		}
		if st, ok := held[p.Name]; ok {
			// Truncated to the second in UTC, the same resolution every other
			// timestamp on this wire is formatted at: a store's sub-second
			// precision is an implementation detail, and two hosts must not
			// render the same instant differently.
			since := st.UpdatedAt.UTC().Truncate(time.Second)
			view.Status = AgentStatusLoggedIn
			view.Since = &since
			view.Version = st.Version
		}
		out = append(out, view)
	}
	return AgentsEnvelope{Agents: out}
}
