package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

const guestHandshakeWait = 5 * time.Second

var errGuestReconnect = errors.New("guest reconnect unavailable")

// This identity belongs to one sessiond lifetime. Never serialize it, log it,
// export it through the environment or replace it after ambiguous enrollment.
type guestReconnectIdentity struct {
	session, boot string
	key           ed25519.PrivateKey
	enrolled      bool
	epoch         uint64
}

func (b *bootstrapper) enrollGuest(ctx context.Context, c relay.Conn, cfg runner.BootConfig) (map[string]string, error) {
	b.mu.Lock()
	if b.guest != nil {
		b.mu.Unlock()
		return nil, errGuestReconnect
	}
	// A failed or unsupported opt-in is sticky: later dials cannot downgrade.
	g := &guestReconnectIdentity{session: cfg.SessionID}
	b.guest = g
	b.mu.Unlock()
	if cfg.GuestReconnect != runner.GuestReconnectProtocol || !guestID(cfg.SessionID) {
		return nil, errGuestReconnect
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, errGuestReconnect
	}
	var boot [32]byte
	if _, err = rand.Read(boot[:]); err != nil {
		return nil, errGuestReconnect
	}
	request := runner.GuestReconnectEnrollRequest{Protocol: 1, Token: cfg.BootstrapToken, BootEpoch: base64.RawURLEncoding.EncodeToString(boot[:]), PublicKey: base64.RawURLEncoding.EncodeToString(pub)}
	body, _ := json.Marshal(request)
	if _, err = runner.DecodeGuestReconnectEnrollRequest(body); err != nil {
		return nil, errGuestReconnect
	}
	env, err := guestEnvironmentExchange(ctx, c, runner.MethodEnrollGuestReconnect, body, cfg.SecretNames)
	if err != nil {
		return nil, errGuestReconnect
	}
	b.mu.Lock()
	g.key = key
	g.boot = request.BootEpoch
	g.enrolled = true
	b.attempted = cfg.BootstrapToken
	b.mu.Unlock()
	return env, nil
}

func guestID(s string) bool {
	if len(s) == 0 || len(s) > 256 {
		return false
	}
	for i := range s {
		if s[i] < 0x21 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// reconnectGuest is a preamble only: it never starts setup, init, an agent or a
// relay. The caller may serve its retained session only after this succeeds.
func (b *bootstrapper) reconnectGuest(ctx context.Context, c relay.Conn) error {
	b.mu.Lock()
	g := b.guest
	if g == nil || !g.enrolled || b.reconnecting {
		b.mu.Unlock()
		return errGuestReconnect
	}
	b.reconnecting = true
	session, boot, key, epoch := g.session, g.boot, g.key, g.epoch
	b.mu.Unlock()
	defer func() { b.mu.Lock(); b.reconnecting = false; b.mu.Unlock() }()
	accepted, err := guestProof(ctx, c, session, boot, key, epoch)
	if err != nil {
		return errGuestReconnect
	}
	// Acceptance consumes this epoch even if delivery is subsequently lost.
	b.mu.Lock()
	g.epoch = accepted.Epoch
	b.mu.Unlock()
	delivery, cancel := context.WithTimeout(ctx, time.Duration(accepted.ExpiresInSec)*time.Second)
	defer cancel()
	cfg, err := readBootConfig(delivery, c)
	if err != nil || cfg.SessionID != session || cfg.GuestReconnect != 1 || cfg.BootstrapToken != accepted.Token {
		return errGuestReconnect
	}
	b.mu.Lock()
	repeated := b.attempted == cfg.BootstrapToken
	b.attempted = cfg.BootstrapToken
	b.mu.Unlock()
	if repeated {
		return errGuestReconnect
	}
	body, _ := json.Marshal(struct {
		Protocol int    `json:"protocol"`
		Token    string `json:"token"`
	}{1, cfg.BootstrapToken})
	env, err := guestEnvironmentExchange(delivery, c, runner.MethodFetchSessionSecrets, body, cfg.SecretNames)
	if err != nil || delivery.Err() != nil {
		return errGuestReconnect
	}
	if err = b.refreshConfiguration(cfg, env); err != nil {
		return errGuestReconnect
	}
	return nil
}

func guestProof(ctx context.Context, c relay.Conn, session, boot string, key ed25519.PrivateKey, epoch uint64) (runner.GuestReconnectAcceptResponse, error) {
	var zero runner.GuestReconnectAcceptResponse
	ctx, cancel := context.WithTimeout(ctx, guestHandshakeWait)
	defer cancel()
	ev, err := relay.ReadGuestReconnectFrame(ctx, c)
	if err != nil || ev.Kind != relay.KindGuestReconnectChallenge {
		return zero, errGuestReconnect
	}
	challenge, err := runner.DecodeGuestReconnectChallenge(ev.Payload)
	if err != nil || challenge.SessionID != session || challenge.BootEpoch != boot {
		return zero, errGuestReconnect
	}
	message, err := challenge.SigningMessage()
	if err != nil {
		return zero, errGuestReconnect
	}
	proof := runner.GuestReconnectAcceptRequest{Protocol: 1, AttemptID: challenge.AttemptID, Signature: base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, message))}
	body, _ := json.Marshal(proof)
	if relay.WriteGuestReconnectFrame(ctx, c, relay.KindGuestReconnectProof, body) != nil {
		return zero, errGuestReconnect
	}
	ev, err = relay.ReadGuestReconnectFrame(ctx, c)
	if err != nil || ev.Kind != relay.KindGuestReconnectAccepted {
		return zero, errGuestReconnect
	}
	accepted, err := runner.DecodeGuestReconnectAcceptResponse(ev.Payload)
	if err != nil || accepted.Epoch <= epoch || ctx.Err() != nil {
		return zero, errGuestReconnect
	}
	return accepted, nil
}

// Both enrollment and refresh must redeem authority even for an empty declared
// secret set. These are authorized payloads under the normal transport limit,
// not unauthenticated cryptographic frames. Never expose provider/parser text.
func guestEnvironmentExchange(ctx context.Context, c relay.Conn, method string, body []byte, names []string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, secretsExchangeWait)
	defer cancel()
	payload, _ := json.Marshal(relay.ControlEvent{Kind: "req:" + method, ID: secretsRequestID, Payload: body})
	frame, _ := relay.Encode(relay.Frame{Type: relay.FrameControl, Payload: payload})
	if c.Write(ctx, frame) != nil {
		return nil, errGuestReconnect
	}
	raw, err := c.Read(ctx)
	if err != nil {
		return nil, errGuestReconnect
	}
	f, err := relay.Decode(raw)
	if err != nil || f.Type != relay.FrameControl || f.AttachID != 0 {
		return nil, errGuestReconnect
	}
	var ev relay.ControlEvent
	if json.Unmarshal(f.Payload, &ev) != nil || ev.Kind != "resp" || ev.ID != secretsRequestID || !ev.OK {
		return nil, errGuestReconnect
	}
	env, err := decodeGuestEnvironment(ev.Payload)
	if err != nil || ctx.Err() != nil {
		return nil, errGuestReconnect
	}
	for _, n := range names {
		if _, ok := env[n]; !ok {
			return nil, errGuestReconnect
		}
	}
	return env, nil
}

// Require exactly one non-null environment object, with unique string values.
func decodeGuestEnvironment(raw []byte) (map[string]string, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	tok, err := d.Token()
	if err != nil || tok != json.Delim('{') {
		return nil, errGuestReconnect
	}
	tok, err = d.Token()
	if err != nil || tok != "env" {
		return nil, errGuestReconnect
	}
	tok, err = d.Token()
	if err != nil || tok != json.Delim('{') {
		return nil, errGuestReconnect
	}
	env := map[string]string{}
	for d.More() {
		tok, err = d.Token()
		k, ok := tok.(string)
		if err != nil || !ok {
			return nil, errGuestReconnect
		}
		if _, exists := env[k]; exists {
			return nil, errGuestReconnect
		}
		tok, err = d.Token()
		v, ok := tok.(string)
		if err != nil || !ok {
			return nil, errGuestReconnect
		}
		if !validGuestEnv(k, v) {
			return nil, errGuestReconnect
		}
		env[k] = v
	}
	tok, err = d.Token()
	if err != nil || tok != json.Delim('}') {
		return nil, errGuestReconnect
	}
	tok, err = d.Token()
	if err != nil || tok != json.Delim('}') {
		return nil, errGuestReconnect
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, errGuestReconnect
	}
	return env, nil
}
func validGuestEnv(k, v string) bool {
	return k != "" && !strings.ContainsAny(k, "=\x00") && !strings.ContainsRune(v, '\x00')
}

// Replace only keys previously owned by this boot configuration. Removing an
// empty proxy/script/secret setting must remove its old process value as well.
// Existing children keep their inherited environments; this updates sessiond
// and future children, not an already running coding agent's credentials.
func (b *bootstrapper) refreshConfiguration(cfg runner.BootConfig, secrets map[string]string) error {
	env := map[string]string{}
	if visitBootConfig(cfg, secrets, func(k, v string) error {
		if !validGuestEnv(k, v) {
			return errGuestReconnect
		}
		env[k] = v
		return nil
	}) != nil {
		return errGuestReconnect
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, k := range b.configured {
		if _, ok := env[k]; !ok {
			if os.Unsetenv(k) != nil {
				return errGuestReconnect
			}
		}
	}
	for k, v := range env {
		if os.Setenv(k, v) != nil {
			return errGuestReconnect
		}
	}
	b.configured = namesOf(env)
	b.delivered = namesOf(secrets)
	return nil
}
