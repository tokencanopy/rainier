package controld

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// launchMaterial is the self-hosted controlapp.LaunchMaterialResolver: given a
// session and the environment as it stands NOW, it answers the sensitive half
// of a create — which repositories to clone, whose identity their commits
// carry, which hosts that clone needs reachable, and the decrypted secret
// environment — without any of it ever being written to a row.
//
// It is deliberately the only place in the self-hosted adapter set that holds
// the secrets key. Every failure it returns is a fixed sentence: a value, a
// token, a repository's content, or a store's own error text must never reach
// the scheduler, a log line, a session's error column, or a test's output. The
// one detail a failure may carry is a secret reference's NAME, because a
// dangling reference is unfixable without it and a name is not a value.
type launchMaterial struct {
	st  Store
	key [32]byte
}

var _ controlapp.LaunchMaterialResolver = launchMaterial{}

// ResolveLaunchMaterial reproduces at dispatch exactly what createSpec used to
// derive inline, in the same order and with the same rules (design §4.3): the
// repositories, then the attribution and git egress a clone implies, then the
// environment's secrets.
func (l launchMaterial) ResolveLaunchMaterial(ctx context.Context, row control.Session, env *control.Environment) (controlapp.LaunchMaterial, error) {
	shimRow, shimEnv := launchShim(row, env)
	refs, err := sessionRepoRefs(shimRow, shimEnv)
	if err != nil {
		// The connector was accepted by the same strict decode on the way in,
		// so this is a store the API cannot read back; its text says nothing a
		// caller can act on and may quote the connector, so it is dropped.
		return controlapp.LaunchMaterial{}, errors.New("could not resolve the repositories this session clones")
	}

	var m controlapp.LaunchMaterial
	if len(refs) > 0 {
		m.Repos = expandRepos(shimRow, refs)
		// Attribution is resolved from the creator's user row at dispatch, not
		// copied onto the session at create: it is derived data, and a login
		// the user has since changed on GitHub is re-read here rather than
		// frozen into every session they ever started.
		u, err := l.st.GetUser(ctx, string(row.CreatorID))
		if err != nil {
			return controlapp.LaunchMaterial{}, errors.New("could not resolve the git identity this session commits as")
		}
		m.GitAuthorName = u.Login
		m.GitAuthorEmail = noreplyEmail(u)
		// The hosts a clone reaches are the resolver's knowledge, not the
		// caller's: the connector said "acme/app", not three CDN names. The
		// scheduler unions them into the session's own allowlist at dispatch,
		// so the row keeps only what the caller or environment declared.
		m.EgressAllow = slices.Clone(gitEgressHosts)
	}

	vars, err := l.secretEnvironment(ctx, env)
	if err != nil {
		return controlapp.LaunchMaterial{}, err
	}
	m.Environment = vars
	return m, nil
}

// secretEnvironment decrypts every secret env references into the environment
// map its sessions' containers get. It fails closed: a reference no stored
// secret answers to fails the whole resolve rather than starting a container
// whose environment promised a credential it does not have.
func (l launchMaterial) secretEnvironment(ctx context.Context, env *control.Environment) (map[string]string, error) {
	if env == nil || len(env.SecretRefs) == 0 {
		return nil, nil
	}
	vars := make(map[string]string, len(env.SecretRefs))
	for _, name := range env.SecretRefs {
		ciphertext, nonce, err := l.st.GetSecret(ctx, name)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				// The name, and only the name: no value was ever loaded.
				return nil, fmt.Errorf("environment references secret %q, which no longer exists", name)
			}
			return nil, errors.New("could not resolve the environment's secrets")
		}
		plaintext, err := Open(l.key, ciphertext, nonce)
		if err != nil {
			return nil, errors.New("could not resolve the environment's secrets")
		}
		vars[name] = string(plaintext)
	}
	return vars, nil
}

// launchShim re-spells the control model in the store's own vocabulary for the
// two moved helpers below, which have always spoken it. It carries only what
// they read — the session's identity, name and repo override, and the
// environment's connectors — and it preserves the one distinction the whole
// clone path turns on: a nil Repos means "inherit the environment's
// connectors", while an explicit empty one means "clone nothing".
func launchShim(row control.Session, env *control.Environment) (Session, *Environment) {
	shimRow := Session{ID: string(row.ID), Name: row.Name}
	if row.Spec.Repos != nil {
		shimRow.Repos = make([]RepoRef, 0, len(row.Spec.Repos))
		for _, r := range row.Spec.Repos {
			shimRow.Repos = append(shimRow.Repos, RepoRef{Repo: r.Repo, BaseBranch: r.BaseBranch})
		}
	}
	if env == nil {
		return shimRow, nil
	}
	shimEnv := &Environment{ID: string(env.ID)}
	if env.Connectors != nil {
		shimEnv.Connectors = make([]Connector, 0, len(env.Connectors))
		for _, c := range env.Connectors {
			shimEnv.Connectors = append(shimEnv.Connectors, Connector{Type: c.Type, Raw: c.Raw})
		}
	}
	return shimRow, shimEnv
}

// ---------------------------------------------------------------------------
// repo and identity resolution, moved verbatim from sched.go/api.go
//
// These are the launch material's own knowledge and have no caller left in the
// session lifecycle once it belongs to controlapp; they live beside their one
// remaining consumer rather than beside a scheduler this package no longer
// has. Their bodies are unchanged.
// ---------------------------------------------------------------------------

// defaultBaseBranch is the branch a github connector clones when it names
// none.
const defaultBaseBranch = "main"

// gitEgressHosts are the hosts a clone, fetch and push actually reach:
// github.com for the git protocol itself, codeload.github.com for the tarball
// endpoints git and the API redirect to, and objects.githubusercontent.com
// for LFS and release assets. Appended to a cloning session's allowlist
// because every one of them is a host the SESSION did not ask for and cannot
// know about — the connector said "acme/app", not "three CDN names".
var gitEgressHosts = []string{"github.com", "codeload.github.com", "objects.githubusercontent.com"}

// sessionRepoRefs returns the repositories row is to clone: its own stored
// override when it has one — including an explicit empty one, which means
// "clone nothing" and must beat any connector — and otherwise the github
// connectors of env, decoded out of the bytes the environment stores.
//
// The connectors are decoded here rather than carried in a parsed form
// because the store keeps a connector as the object its author wrote; this is
// the same strict decode the API validated it with on the way in, so a
// connector that was accepted then is accepted now.
func sessionRepoRefs(row Session, env *Environment) ([]RepoRef, error) {
	if row.Repos != nil {
		return row.Repos, nil
	}
	if env == nil {
		return nil, nil
	}
	var refs []RepoRef
	for i, c := range env.Connectors {
		if c.Type != "github" {
			continue
		}
		gc, err := decodeGitHubConnector(c.Raw)
		if err != nil {
			return nil, fmt.Errorf("connectors[%d]: %w", i, err)
		}
		refs = append(refs, RepoRef{Repo: gc.Repo, BaseBranch: *gc.BaseBranch})
	}
	return refs, nil
}

// noreplyEmail is the GitHub noreply address for u:
// <github_id>+<login>@users.noreply.github.com. GitHub accepts commits from it
// without exposing a private address, and attributes them to that account —
// which is the whole point of attribution here: work done by an agent inside a
// session shows up as the human who asked for it.
func noreplyEmail(u User) string {
	return fmt.Sprintf("%d+%s@users.noreply.github.com", u.GitHubID, u.Login)
}

// expandRepos turns refs into the clone instructions a sandbox executes,
// resolving the two names a repository reference does not carry: the branch
// the session works on, and the directory it lands in.
func expandRepos(row Session, refs []RepoRef) []runner.RepoSpec {
	if len(refs) == 0 {
		return nil
	}
	branch := sessionBranch(row)
	dirs := make(map[string]bool, len(refs))
	out := make([]runner.RepoSpec, 0, len(refs))
	for _, ref := range refs {
		owner, name, _ := strings.Cut(ref.Repo, "/")
		base := ref.BaseBranch
		if base == "" {
			base = defaultBaseBranch
		}
		out = append(out, runner.RepoSpec{
			Owner: owner, Name: name, BaseBranch: base,
			SessionBranch: branch,
			Dir:           uniqueDir(dirs, owner, name),
		})
	}
	return out
}

// sessionBranch is the branch a session's work goes on: rainier/<name>, or
// rainier/<last 12 of the id> for a session with no name. The prefix is what
// makes an agent's branches identifiable (and bulk-deletable) in a repository
// full of human ones, and the id fallback is what keeps two unnamed sessions
// of the same repository from pushing to the same branch.
//
// A session name is the caller's own text and is not sanitized here: a name
// git refuses as a ref fails the clone stage loudly, with the name in the
// error, which is a better answer than silently working on a branch the user
// did not name.
func sessionBranch(row Session) string {
	name := row.Name
	if name == "" {
		name = row.ID
		if len(name) > 12 {
			name = name[len(name)-12:]
		}
	}
	return "rainier/" + name
}

// uniqueDir returns the directory under /workspace a repository lands in,
// recording it in taken so the next one can avoid it. The repository's own
// name is the natural choice and the one an agent will guess; two repositories
// that share a name (acme/app and other/app — a fork alongside its upstream is
// the common case) cannot both have it, so the later one is qualified by
// owner, and the pathological case of the same owner/name twice falls back to
// a counter. Two clones into one directory would fail the second clone
// outright, so uniqueness here is not cosmetic.
func uniqueDir(taken map[string]bool, owner, name string) string {
	for _, candidate := range []string{name, owner + "__" + name} {
		if !taken[candidate] {
			taken[candidate] = true
			return candidate
		}
	}
	for n := 2; ; n++ {
		candidate := fmt.Sprintf("%s__%s-%d", owner, name, n)
		if !taken[candidate] {
			taken[candidate] = true
			return candidate
		}
	}
}

// envID names env in a log line, or "" for a scratch session, so a call site
// can say which environment it was working from without guarding a nil. Its
// one caller is the create handler's repository preflight; it lives here with
// the rest of the repository resolution it belongs to.
func envID(env *Environment) string {
	if env == nil {
		return ""
	}
	return env.ID
}
