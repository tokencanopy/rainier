package attachplane

import (
	"sync"

	"github.com/tokencanopy/rainier/control"
)

// pendingAttach is one client socket parked between the dial_attach sent to
// its runner and the dial-back that claims it.
//
// done is closed by whoever claims the entry — the dial-back once its splice
// has finished, or the TTL timer when nobody ever came — and is what releases
// the client handler. That handler must not return before then: returning
// runs its deferred close on a socket the splice is still using.
type pendingAttach struct {
	stream control.TerminalStream
	done   chan struct{}
}

// attachTable holds the pairings this replica is waiting on, keyed by
// attach_id. The state is deliberately replica-local (design §6): the
// dial-back's target_url names this exact replica, so no other one can be
// asked to claim an entry, and a replica dying takes only its own live
// attaches with it — clients re-attach, nothing else is lost.
type attachTable struct {
	mu sync.Mutex
	m  map[string]*pendingAttach
}

func newAttachTable() *attachTable { return &attachTable{m: map[string]*pendingAttach{}} }

// park registers pa under id, reporting false if that id is already parked
// rather than overwriting it. An overwrite would orphan the previous client
// on a `done` nobody holds any more — it would hang until its own handler's
// TTL fired, and the TTL would then close a socket the table no longer knows
// about. Ids are 8 random bytes, so a genuine collision is not a thing that
// happens; a duplicate means something is wrong and the caller says so
// loudly rather than papering over it.
func (t *attachTable) park(id string, pa *pendingAttach) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.m[id]; exists {
		return false
	}
	t.m[id] = pa
	return true
}

// claim removes and returns id's entry. The lookup and the removal are one
// locked step, which is what makes ownership of a parked socket exclusive:
// the dial-back and the TTL timer both race for it and exactly one wins, so
// the socket is never closed by one while the other is splicing it, and
// pendingAttach.done is never closed twice.
func (t *attachTable) claim(id string) (*pendingAttach, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	pa, ok := t.m[id]
	if ok {
		delete(t.m, id)
	}
	return pa, ok
}

// has reports whether id is currently parked. It is a hint, not a claim: the
// dial-back uses it to answer an expired attach_id with a plain HTTP 404
// before upgrading. Claiming that early would be a bug — a failed upgrade
// would then leave the client parked on a `done` nobody can ever close — so
// claim, after the upgrade, stays authoritative.
func (t *attachTable) has(id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.m[id]
	return ok
}
