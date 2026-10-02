package runnerd

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

func (lease *guestReconnectLease) AuthorizeGuestReconnect(ctx context.Context, id string, prove driver.GuestReconnectProof) (runner.GuestReconnectAcceptResponse, error) {
	if id != lease.row.id {
		return runner.GuestReconnectAcceptResponse{}, errReconnectFenced
	}
	return lease.authorize(ctx, prove)
}

// ReconnectGuest owns an admitted stream through proof, current configuration,
// redemption and readiness. Failure closes it with no relay publication; success
// transfers it to the registry. The caller must bound admission and verify local
// VM/socket ownership before calling. No cached boot token/config is accepted.
func (s *Server) ReconnectGuest(ctx context.Context, id string, conn relay.Conn) error {
	success := false
	defer func() {
		if !success && conn != nil {
			conn.Close()
		}
	}()
	if conn == nil {
		return errReconnectInvalid
	}
	lease, err := s.acquireGuestReconnect(id)
	if err != nil {
		return err
	}
	defer lease.close()
	accepted, err := driver.AuthorizeGuestConnection(ctx, lease, id, conn)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(accepted.ExpiresInSec)*time.Second)
	defer cancel()
	stop := context.AfterFunc(lease.control.ctx, cancel)
	defer stop()
	if err = lease.fence(ctx, accepted.Epoch); err != nil {
		return err
	}
	payload, err := s.reconnectCall(ctx, lease.control, id, runner.MethodGuestReconnectConfiguration, runner.GuestReconnectBeginRequest{Protocol: 1})
	if err != nil || !lease.valid(ctx) {
		return errReconnectUnavailable
	}
	fresh, err := runner.DecodeGuestReconnectConfiguration(payload)
	if err != nil || fresh.SessionID != id || fresh.PlacementGeneration != lease.row.placementGen {
		return errReconnectInvalid
	}
	spec := guestDriverSpec(*fresh.Spec)
	spec.SessionID = id
	spec.ProxyURL = s.proxyURL
	spec.BootstrapToken = accepted.Token
	cfg := driver.GuestBootConfig(spec)
	cfg.GuestReconnect = runner.GuestReconnectProtocol
	body, _ := json.Marshal(accepted)
	if !lease.valid(ctx) || relay.WriteGuestReconnectFrame(ctx, conn, relay.KindGuestReconnectAccepted, body) != nil {
		return errReconnectUnavailable
	}
	writeCtx, writeCancel := context.WithTimeout(ctx, 10*time.Second)
	writeErr := writeGuestControl(writeCtx, conn, relay.ControlEvent{Kind: relay.KindBootConfig, Payload: mustGuestJSON(cfg)})
	writeCancel()
	if writeErr != nil {
		return errReconnectUnavailable
	}
	if err = lease.redeem(ctx, conn, accepted.Token); err != nil {
		return err
	}
	readyCtx, readyCancel := context.WithTimeout(ctx, reconnectBudget)
	defer readyCancel()
	event, err := relay.ReadGuestReconnectFrame(readyCtx, conn)
	if err != nil || event.Kind != relay.KindGuestReconnectReady {
		return errReconnectInvalid
	}
	ready, err := runner.DecodeGuestReconnectReady(event.Payload)
	if err != nil || ready.Epoch != accepted.Epoch || !lease.valid(readyCtx) {
		return errReconnectFenced
	}
	if relay.WriteGuestReconnectFrame(readyCtx, conn, relay.KindGuestReconnectReadyAck, event.Payload) != nil || !lease.valid(readyCtx) {
		return errReconnectUnavailable
	}
	if err = lease.install(ctx, accepted.Epoch, conn); err != nil {
		return err
	}
	success = true
	return nil
}

func (lease *guestReconnectLease) install(ctx context.Context, epoch uint64, conn relay.Conn) error {
	s, id := lease.server, lease.row.id
	// The hub reader waits until publication. No attachment can interleave with
	// configuration, redemption or acknowledgment, and buffered bytes stay on conn.
	gate := make(chan struct{})
	authority := &guestRelayAuthority{}
	reg := s.reg.registration()
	var hub *relay.Hub
	hub = relay.NewHubWithControl(lease.control.ctx, &reconnectReadGate{Conn: conn, ready: gate}, func(payload []byte) {
		go authority.run(func() { s.routeReconnectedControl(lease, hub, reg, payload) })
	})
	if !lease.publish(ctx, epoch, hub, authority) {
		hub.Close()
		return errReconnectFenced
	}
	close(gate)
	go s.monitorSessionHub(id, hub)
	s.sendOwnedGuestMessage(lease, hub, runner.FromRunner{Type: "event", Session: id, State: "running"})
	return nil
}

func mustGuestJSON(v any) json.RawMessage { body, _ := json.Marshal(v); return body }
func writeGuestControl(ctx context.Context, c relay.Conn, event relay.ControlEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return errReconnectInvalid
	}
	raw, err := relay.Encode(relay.Frame{Type: relay.FrameControl, Payload: body})
	if err != nil {
		return errReconnectInvalid
	}
	return c.Write(ctx, raw)
}

type reconnectReadGate struct {
	relay.Conn
	ready <-chan struct{}
}

func (c *reconnectReadGate) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ready:
		return c.Conn.Read(ctx)
	}
}

func (lease *guestReconnectLease) redeem(ctx context.Context, c relay.Conn, token string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	reader, ok := c.(interface {
		ReadLimited(context.Context, int) ([]byte, error)
	})
	if !ok {
		return errReconnectInvalid
	}
	raw, err := reader.ReadLimited(ctx, runner.GuestReconnectPayloadLimit)
	if err != nil {
		return errReconnectUnavailable
	}
	frame, err := relay.Decode(raw)
	if err != nil || frame.Type != relay.FrameControl || frame.AttachID != 0 {
		return errReconnectInvalid
	}
	var event relay.ControlEvent
	if json.Unmarshal(frame.Payload, &event) != nil || event.Kind != "req:"+runner.MethodFetchSessionSecrets || event.ID == 0 || isRunnerOriginated(event.ID) {
		return errReconnectInvalid
	}
	var req struct {
		Protocol uint64 `json:"protocol"`
		Token    string `json:"token"`
	}
	if json.Unmarshal(event.Payload, &req) != nil || req.Protocol != 1 || req.Token != token || !lease.valid(ctx) {
		return errReconnectFenced
	}
	payload, err := lease.server.reconnectCall(ctx, lease.control, lease.row.id, runner.MethodFetchSessionSecrets, req)
	if err != nil || !lease.valid(ctx) {
		return errReconnectUnavailable
	}
	return writeGuestControl(ctx, c, relay.ControlEvent{Kind: "resp", ID: event.ID, OK: true, Payload: payload})
}

func (s *Server) routeReconnectedControl(lease *guestReconnectLease, hub *relay.Hub, reg uint64, payload []byte) {
	var event relay.ControlEvent
	if json.Unmarshal(payload, &event) != nil {
		return
	}
	method, request := strings.CutPrefix(event.Kind, "req:")
	if !request && event.Kind != "resp" {
		// Lifecycle events retain their boot/registration ordering guards.
		s.reg.mu.Lock()
		row, ok := s.reg.items[lease.row.id]
		valid := ok && row.boot == lease.row.boot && row.hub == hub && lease.control.ctx.Err() == nil && s.reconnectControl.Load() == lease.control && lease.control.state.generation.Load() == lease.generation
		s.reg.mu.Unlock()
		if valid {
			s.routeControlWithEvents(lease.row.id, lease.row.boot, reg, payload, func(id, state, detail string) {
				s.sendOwnedGuestMessage(lease, hub, runner.FromRunner{Type: "event", Session: id, State: state, Detail: detail})
			})
		}
		return
	}
	if event.Kind == "resp" {
		method = "resp"
	}
	if method == "" || event.ID == 0 || refuseSandboxOrigin(method, event.ID) != "" {
		return
	}
	msg := runner.FromRunner{Type: "session_req", Session: lease.row.id, RPC: &runner.RPCEnvelope{ID: event.ID, Method: method, OK: event.OK, Payload: event.Payload}}
	s.sendOwnedGuestMessage(lease, hub, msg)
}

func (s *Server) sendOwnedGuestMessage(lease *guestReconnectLease, hub *relay.Hub, msg runner.FromRunner) {
	ctx, cancel := context.WithTimeout(lease.control.ctx, reconnectBudget)
	defer cancel()
	msg.Used, msg.Total, _ = s.drv.Capacity(ctx)
	msg.Active, msg.IdleExited = s.reg.counts()
	// Check ownership at enqueue after all potentially blocking work. Never send
	// an old relay's credentials request on a replacement control connection.
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	row, ok := s.reg.items[lease.row.id]
	if !ok || row.boot != lease.row.boot || row.hub != hub || row.placementGen != lease.row.placementGen || ctx.Err() != nil || s.reconnectControl.Load() != lease.control || lease.control.state.generation.Load() != lease.generation {
		return
	}
	if msg.Type == "event" {
		msg.Generation = lease.generation
		msg.PlacementGeneration = lease.row.placementGen
	}
	select {
	case lease.control.out <- msg:
	default:
	}
}
