package controlapp

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"slices"

	"github.com/tokencanopy/rainier/control"
)

// HomeMountPath is where the agent home lands inside every session: one
// writable volume per (creator, workspace), with a subdirectory per provider
// under it. It is outside /workspace deliberately — a credential set must not
// be something a repository, a diff, a push, or a checkpoint can reach — and
// outside $HOME, which the read-only rootfs owns.
const HomeMountPath = "/rainier/agents"

// agentsEnvVar carries the provider table down to the sandbox as base64 JSON.
// sessiond needs the directories and the file allowlist to do its job, and it
// must not need a provider name to do it: the manifest is the whole of what
// the sandbox knows about providers, which is why the table can grow a row
// without a line changing in sessiond, the driver, or the stores.
const agentsEnvVar = "RAINIER_AGENTS_B64"

// EnableTestAgentProvider adds the synthetic "test" provider to the table. It
// is a package-level switch a host sets ONCE at startup, before it serves
// anything, and only the end-to-end suite's controld sets it: the row's login
// command writes a fixture into its own file so the whole credential path —
// mount, fetch, put, revoke — can be proven on a fleet without a real account
// and without a real credential ever existing. It defaults to off so a
// production control plane cannot be talked into offering it.
var EnableTestAgentProvider bool

// AgentProvider is one coding agent's row: everything Rainier needs to know
// about it and nothing more. The table is data, not code — adding a third
// agent is a row plus the probes that fill it in — and it is the ONLY place
// in this repository where a provider is named. sessiond, the driver, runnerd,
// the stores, and the RPC carry the strings and never spell one.
//
// Each field answers one question the probes had to settle:
//
//   - HomeEnv is the variable the agent already honors to move its
//     configuration directory. Pointing it at the home is the entire
//     integration: the tool is unmodified and does not know it is in Rainier.
//   - HomeVar is the variable to set when the agent ALSO writes under $HOME
//     regardless — empty when everything lands under HomeEnv's directory,
//     which is the case for the rows below. sessiond sets it for the agent
//     process only, never for the container.
//   - Files is the allowlist of credential-bearing file names inside the
//     provider's directory. Bare file names, never paths: this list is what
//     the sync reads, what a revoke deletes, and what a checkpoint excludes,
//     so a row that could name "../../workspace/x" would be a hole in all
//     three at once.
//   - Egress is the hosts the agent's login, token refresh, and inference
//     reach, read off the egress proxy's log during the probes. Apex names
//     are literal: an entry is not a wildcard pattern.
//   - LoginCmd is the command `rainier agent login` runs in a throwaway
//     session so the person completes the tool's own login flow — unmodified,
//     with no credential pasted anywhere.
type AgentProvider struct {
	Name     string
	HomeEnv  string
	HomeVar  string
	Files    []string
	Egress   []string
	LoginCmd []string
}

// AgentProviders returns the provider table, in the order a manifest and an
// egress list present it. Every call builds a fresh slice with fresh inner
// slices: the table is handed to callers that sort, append to, and merge it,
// and none of them may change what the next caller sees.
func AgentProviders() []AgentProvider {
	rows := []AgentProvider{
		{
			Name:     "claude",
			HomeEnv:  "CLAUDE_CONFIG_DIR",
			Files:    []string{".credentials.json"},
			Egress:   []string{"api.anthropic.com", "platform.claude.com", "downloads.claude.ai", "mcp-proxy.anthropic.com"},
			LoginCmd: []string{"claude"},
		},
		{
			Name:    "codex",
			HomeEnv: "CODEX_HOME",
			Files:   []string{"auth.json"},
			// The bare apex, because that is what inference reaches and a
			// "*.chatgpt.com" entry does not match it.
			Egress:   []string{"auth.openai.com", "chatgpt.com"},
			LoginCmd: []string{"codex", "login", "--device-auth"},
		},
	}
	if EnableTestAgentProvider {
		rows = append(rows, AgentProvider{
			Name:    "test",
			HomeEnv: "RAINIER_TEST_AGENT_HOME",
			Files:   []string{"credential.json"},
			// No egress: the synthetic agent's whole login is a local write,
			// which is what makes it safe to run in a suite.
			LoginCmd: []string{"sh", "-c", `printf credential_example > "$RAINIER_TEST_AGENT_HOME/credential.json"`},
		})
	}
	return cloneProviders(rows)
}

// AgentHomeVolume is the name of the volume that holds one person's agent
// homes in one workspace. It is a hash, not a composed identifier, for a
// reason that has nothing to do with length: `docker volume ls` on a runner
// is readable by anyone who can reach the host, and an account id printed
// there is a disclosure the mount does not need to make. Stability is the
// other half — the same pair must resolve to the same volume on every boot,
// forever, or "log in once" quietly becomes "log in once per session".
//
// The NUL between the two ids keeps ("ws_a", "bc") and ("ws_ab", "c") apart;
// without it two people could be handed one credential set.
func AgentHomeVolume(ws control.WorkspaceID, creator control.ActorID) string {
	sum := sha256.Sum256([]byte(string(ws) + "\x00" + string(creator)))
	return "rainier-agents-" + hex.EncodeToString(sum[:])[:16]
}

// agentManifestEntry is one provider's line in RAINIER_AGENTS_B64: where its
// directory is and which files inside it are the credential set. It is the
// sandbox's entire view of the table — no egress, no login command, nothing
// the sandbox has no business acting on.
type agentManifestEntry struct {
	Provider string   `json:"provider"`
	Dir      string   `json:"dir"`
	Files    []string `json:"files"`
	// HomeVar rides along when a row declares one, so a provider that also
	// writes under $HOME becomes a table edit rather than a code change in
	// sessiond. Absent for every row that does not, which is all of them
	// today, so the bytes are exactly the three-key shape the design names.
	HomeVar string `json:"home_var,omitempty"`
}

// AgentsEnv is the environment every session gets so that the agents inside
// it find their homes: each provider's own variable pointed at its
// subdirectory, plus the manifest sessiond reads to know what to fetch, write,
// watch, and delete. It carries no credential — it is a map of paths — which
// is why it can sit in the container environment at all.
//
// Callers merge it into the create's environment BEFORE the resolved launch
// material, so a host that deliberately relocates one provider still wins
// under the documented last-wins rule.
func AgentsEnv(providers []AgentProvider) map[string]string {
	env := make(map[string]string, len(providers)+1)
	entries := make([]agentManifestEntry, 0, len(providers))
	for _, p := range providers {
		dir := HomeMountPath + "/" + p.Name
		env[p.HomeEnv] = dir
		entries = append(entries, agentManifestEntry{
			Provider: p.Name, Dir: dir, Files: slices.Clone(p.Files), HomeVar: p.HomeVar,
		})
	}
	// json.Marshal of a slice of plain structs cannot fail, and a create is
	// not the place to invent an error path that no input can reach: an
	// impossible failure yields an empty manifest, which sessiond reads as
	// "no agents" and boots the session unchanged.
	raw, err := json.Marshal(entries)
	if err != nil {
		raw = []byte("[]")
	}
	env[agentsEnvVar] = base64.StdEncoding.EncodeToString(raw)
	return env
}

func cloneProviders(rows []AgentProvider) []AgentProvider {
	out := make([]AgentProvider, len(rows))
	for i, p := range rows {
		p.Files = slices.Clone(p.Files)
		p.Egress = slices.Clone(p.Egress)
		p.LoginCmd = slices.Clone(p.LoginCmd)
		out[i] = p
	}
	return out
}
