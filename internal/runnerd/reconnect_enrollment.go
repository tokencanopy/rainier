package runnerd

import (
	"context"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// Fresh opted-in boots cannot use the legacy environment exchange. Holding
// publication until enrollment commits also prevents ordinary client traffic
// from interleaving with the guest's strict bootstrap response reader.
func (s *Server) enrollGuestConnection(ctx context.Context, id string, conn relay.Conn) error {
	success := false
	defer func() {
		if !success {
			conn.Close()
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	original, ok := s.reg.snapshot(id)
	if !ok {
		return errReconnectFenced
	}
	// The guest may dial while driver.Create is returning its handle. Retain the
	// original boot while waiting; never adopt a replacement entry with this id.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, exists := s.reg.snapshot(id)
		if !exists || current.boot != original.boot {
			return errReconnectFenced
		}
		if current.handle != "" {
			break
		}
		select {
		case <-ctx.Done():
			return errReconnectUnavailable
		case <-ticker.C:
		}
	}
	lease, err := s.acquireGuestReconnect(id)
	if err != nil {
		return err
	}
	defer lease.close()
	if lease.row.boot != original.boot || lease.row.hub != nil || lease.row.guestEpoch != 0 {
		return errReconnectFenced
	}
	stop := context.AfterFunc(lease.control.ctx, cancel)
	defer stop()
	event, err := readGuestBootstrapRequest(ctx, conn, runner.MethodEnrollGuestReconnect)
	if err != nil {
		return err
	}
	request, err := runner.DecodeGuestReconnectEnrollRequest(event.Payload)
	if err != nil || !lease.valid(ctx) {
		return errReconnectInvalid
	}
	payload, err := s.reconnectCall(ctx, lease.control, id, runner.MethodEnrollGuestReconnect, request)
	if err != nil || !lease.valid(ctx) {
		return errReconnectUnavailable
	}
	if writeGuestControl(ctx, conn, relay.ControlEvent{Kind: "resp", ID: event.ID, OK: true, Payload: payload}) != nil {
		return errReconnectUnavailable
	}
	if err = lease.install(ctx, 0, conn); err != nil {
		return err
	}
	success = true
	return nil
}

func readGuestBootstrapRequest(ctx context.Context, conn relay.Conn, method string) (relay.ControlEvent, error) {
	var zero relay.ControlEvent
	reader, ok := conn.(interface {
		ReadLimited(context.Context, int) ([]byte, error)
	})
	if !ok {
		return zero, errReconnectInvalid
	}
	raw, err := reader.ReadLimited(ctx, runner.GuestReconnectPayloadLimit)
	if err != nil {
		return zero, errReconnectUnavailable
	}
	frame, err := relay.Decode(raw)
	if err != nil || frame.Type != relay.FrameControl || frame.AttachID != 0 {
		return zero, errReconnectInvalid
	}
	return decodeGuestBootstrapEvent(frame.Payload, method)
}
