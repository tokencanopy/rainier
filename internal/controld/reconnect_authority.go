package controld

import (
	"context"
	"time"

	"github.com/tokencanopy/rainier/control"
)

// GuestReconnectAuthority is the current, locked host authority for one request.
// Now is sampled after database locks; Epoch distinguishes proof-issued tokens.
type GuestReconnectAuthority struct {
	Session control.Session
	User    User
	Epoch   uint64
	Now     time.Time
}

// GuestReconnectAuthorityStore is optional: volatile stores cannot negotiate
// guest recovery. The callback and all capability mutations commit together.
type GuestReconnectAuthorityStore interface {
	GuestReconnects() control.GuestReconnectStore
	WithGuestReconnectAuthority(context.Context, control.GuestReconnectScope, func(context.Context, GuestReconnectAuthority) error) error
}
