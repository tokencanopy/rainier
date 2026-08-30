// Package cli is the rainier CLI's HTTP client and on-disk config — the
// pieces cmd/rainier composes into the actual command surface, and the
// pieces a smoke test can drive against a real controld without going
// through main().
package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Config is what `rainier login` writes and every other command reads: the
// controld server to talk to, and the bearer token to talk to it with.
//
// OwnerID is the caller's own user id, learned at login from the identity
// controld returns (the same one GET /v1/me answers with) and cached here so
// every later command has it without a round trip. It is the string a
// session row carries as owner_id, and it is used to break ties when a
// session name resolves to more than one session; see cmd/rainier's
// resolveSessionID. Empty only for a config written before logins carried it
// — one `rainier login` fills it in.
type Config struct {
	ServerURL string `json:"server_url"`
	Token     string `json:"token"`
	OwnerID   string `json:"owner_id,omitempty"`
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

// Load reads the config file, returning a zero Config (not an error) if it
// doesn't exist yet — "not logged in" is an ordinary state every command
// has to handle, not a failure Load itself should report.
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
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return Config{}, err
	}
	return c, nil
}

// Save writes c to the config file, creating its directory (0700) if
// necessary. The file itself is 0600: it carries a bearer token.
func Save(c Config) error {
	path, err := configPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}
