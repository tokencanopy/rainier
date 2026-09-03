// Package cli is the rainier CLI's HTTP client and on-disk config — the
// pieces cmd/rainier composes into the actual command surface, and the
// pieces a smoke test can drive against a real controld without going
// through main().
package cli

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"slices"
)

// DefaultContext is the name of the context `rainier login` writes when no
// other is named, and the name a legacy single-server config migrates into.
const DefaultContext = "default"

// Context is one server this CLI can talk to: a self-hosted controld, or a
// hosted edge and the workspace the caller is acting in.
//
// OwnerID is the caller's own user id, learned at login from the identity
// controld returns (the same one GET /v0/me answers with) and cached here so
// every later command has it without a round trip. It is the string a
// session row carries as owner_id, and it is used to break ties when a
// session name resolves to more than one session; see cmd/rainier's
// resolveSessionID. Empty only for a config written before logins carried it
// — one `rainier login` fills it in.
//
// Workspace, RefreshToken and AccessExpiresAt are hosted-only. A hosted
// access token is short-lived: the refresh token buys a new pair, is
// single-use, and rotates with every use (see Client's transparent refresh).
type Context struct {
	Server          string `json:"server_url"`
	Token           string `json:"token"`
	OwnerID         string `json:"owner_id,omitempty"`
	Workspace       string `json:"workspace,omitempty"`
	RefreshToken    string `json:"refresh_token,omitempty"`
	AccessExpiresAt string `json:"access_expires_at,omitempty"`
}

// Hosted reports whether this context came from a hosted edge — the presence
// of a refresh token is what distinguishes the hosted passwordless login
// from a self-hosted GitHub login, whose token neither expires on its own
// nor rotates.
func (c Context) Hosted() bool { return c.RefreshToken != "" }

// Config is what `rainier login` writes and every other command reads: the
// named contexts the CLI knows and which one is current.
//
// ServerURL, Token and OwnerID are not stored: they are the current
// context's, projected for every caller that only ever wants "the server I
// am talking to". Load fills them in; Save folds a change to them back into
// the current context, which is what keeps a pre-contexts caller — including
// `rainier login` itself — working unchanged.
type Config struct {
	Current  string             `json:"current"`
	Contexts map[string]Context `json:"contexts"`

	ServerURL string `json:"-"`
	Token     string `json:"-"`
	OwnerID   string `json:"-"`
}

// ActiveName is the name of the current context — Current, or "default"
// when nothing has named one (the shape a migrated legacy config has).
func (c Config) ActiveName() string {
	if c.Current != "" {
		return c.Current
	}
	return DefaultContext
}

// Active returns the current context, false when there is none (nothing has
// logged in yet, or the current one was removed).
func (c Config) Active() (Context, bool) {
	ctx, ok := c.Contexts[c.ActiveName()]
	return ctx, ok
}

// Names returns every context name, sorted — `rainier context list`'s order.
func (c Config) Names() []string {
	return slices.Sorted(maps.Keys(c.Contexts))
}

// SetContext stores ctx under name and makes it current.
func (c *Config) SetContext(name string, ctx Context) {
	c.UpdateContext(name, ctx)
	c.Current = name
	c.project()
}

// UpdateContext stores ctx under name without changing which context is
// current — how a rotated hosted token pair is written back.
func (c *Config) UpdateContext(name string, ctx Context) {
	if c.Contexts == nil {
		c.Contexts = map[string]Context{}
	}
	c.Contexts[name] = ctx
	c.project()
}

// Use makes name the current context, reporting false (and changing
// nothing) when there is no such context.
func (c *Config) Use(name string) bool {
	if _, ok := c.Contexts[name]; !ok {
		return false
	}
	c.Current = name
	c.project()
	return true
}

// RemoveContext deletes name, reporting false when there was no such
// context. Removing the current one leaves the config with none current:
// the next command says "not logged in" rather than silently acting against
// whichever context happened to sort first.
func (c *Config) RemoveContext(name string) bool {
	if _, ok := c.Contexts[name]; !ok {
		return false
	}
	delete(c.Contexts, name)
	if c.Current == name {
		c.Current = ""
	}
	c.project()
	return true
}

// project refreshes the current context's projection onto the legacy
// accessors, so a caller reading cfg.ServerURL after a context change reads
// the context it just selected.
func (c *Config) project() {
	ctx, _ := c.Active()
	c.ServerURL, c.Token, c.OwnerID = ctx.Server, ctx.Token, ctx.OwnerID
}

// configPath returns the path Load/Save read and write:
// $RAINIER_CONFIG if set (tests use this to avoid touching a real home
// directory), otherwise ~/.config/rainier/config.json.
func configPath() (string, error) {
	if p := os.Getenv("RAINIER_CONFIG"); p != "" {
		return p, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config", "rainier", "config.json"), nil
}

// configFile is the on-disk shape Save writes and the union Load reads: the
// current shape (current + contexts) plus the single-server fields every
// rainier before contexts wrote. Reading both from one struct is what makes
// the migration a read rather than an upgrade step someone has to run.
type configFile struct {
	Current  string             `json:"current"`
	Contexts map[string]Context `json:"contexts"`

	LegacyServerURL string `json:"server_url,omitempty"`
	LegacyToken     string `json:"token,omitempty"`
	LegacyOwnerID   string `json:"owner_id,omitempty"`
}

// Load reads the config file, returning a zero Config (not an error) if it
// doesn't exist yet — "not logged in" is an ordinary state every command
// has to handle, not a failure Load itself should report.
//
// A legacy single-server file is read as the "default" context and made
// current; nothing on disk changes until something calls Save.
func Load() (Config, error) {
	path, err := configPath()
	if err != nil {
		return Config{}, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Config{}, nil
		}
		return Config{}, err
	}
	var f configFile
	if err := json.Unmarshal(data, &f); err != nil {
		return Config{}, err
	}
	c := Config{Current: f.Current, Contexts: f.Contexts}
	if len(c.Contexts) == 0 && (f.LegacyServerURL != "" || f.LegacyToken != "" || f.LegacyOwnerID != "") {
		c.Contexts = map[string]Context{DefaultContext: {
			Server: f.LegacyServerURL, Token: f.LegacyToken, OwnerID: f.LegacyOwnerID,
		}}
		c.Current = DefaultContext
	}
	c.project()
	return c, nil
}

// Save writes c to the config file, creating its directory (0700) if
// necessary. The file itself is 0600: it carries bearer tokens.
//
// A caller that set ServerURL/Token/OwnerID directly (the pre-contexts way,
// which `rainier login` still is) has its values folded into the current
// context — that, and not a second write path, is what keeps those callers
// correct. The legacy fields themselves are never written back: what Save
// leaves on disk is always the current shape.
func Save(c Config) error {
	path, err := configPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(c.normalized(), "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// normalized is the value Save writes: a copy (the caller's map is never
// mutated) with any legacy-accessor change folded into the current context.
func (c Config) normalized() configFile {
	out := configFile{Current: c.Current, Contexts: map[string]Context{}}
	maps.Copy(out.Contexts, c.Contexts)
	if c.ServerURL != "" || c.Token != "" || c.OwnerID != "" {
		name := c.ActiveName()
		cur := out.Contexts[name]
		if c.ServerURL != "" {
			cur.Server = c.ServerURL
		}
		if c.Token != "" {
			cur.Token = c.Token
		}
		if c.OwnerID != "" {
			cur.OwnerID = c.OwnerID
		}
		out.Contexts[name] = cur
		out.Current = name
	}
	return out
}
