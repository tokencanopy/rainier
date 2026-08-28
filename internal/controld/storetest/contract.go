package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"rainier/internal/controld"
)

func RunContract(t *testing.T, open func(t *testing.T) controld.Store) {
	ctx := context.Background()
	mkUser := func(t *testing.T, st controld.Store) controld.User {
		u, err := st.UpsertUser(ctx, 42, "alice", "admin")
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	mkSess := func(t *testing.T, st controld.Store, owner, name string) controld.Session {
		s, err := st.CreateSession(ctx, controld.Session{
			ID: controld.NewSessionID(), OwnerID: owner, Name: name,
			Image: "img", State: controld.StateQueued})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}

	t.Run("user upsert is stable by github id", func(t *testing.T) {
		st := open(t)
		u1 := mkUser(t, st)
		u2, err := st.UpsertUser(ctx, 42, "alice-renamed", "admin")
		if err != nil {
			t.Fatal(err)
		}
		if u1.ID != u2.ID {
			t.Fatalf("same github id must keep user id: %s vs %s", u1.ID, u2.ID)
		}
		if u2.Login != "alice-renamed" {
			t.Fatalf("login should update")
		}
	})

	t.Run("token round trip and unknown token", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		tok, hash := controld.NewToken()
		if err := st.InsertToken(ctx, u.ID, hash); err != nil {
			t.Fatal(err)
		}
		got, err := st.UserByToken(ctx, controld.HashToken(tok))
		if err != nil || got.ID != u.ID {
			t.Fatalf("lookup: %v %+v", err, got)
		}
		if _, err := st.UserByToken(ctx, controld.HashToken("rnr_bogus")); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})

	t.Run("guarded transition: wrong from-state loses with ErrConflict", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		s := mkSess(t, st, u.ID, "")
		r := "vm1"
		if err := st.Transition(ctx, s.ID, []controld.SessionState{controld.StateQueued}, controld.StateCreating, controld.TransitionOpts{Runner: &r}); err != nil {
			t.Fatal(err)
		}
		err := st.Transition(ctx, s.ID, []controld.SessionState{controld.StateQueued}, controld.StateCanceled, controld.TransitionOpts{})
		if !errors.Is(err, controld.ErrConflict) {
			t.Fatalf("want ErrConflict, got %v", err)
		}
		got, _ := st.GetSession(ctx, s.ID)
		if got.State != controld.StateCreating || got.Runner != "vm1" {
			t.Fatalf("state clobbered: %+v", got)
		}
	})

	t.Run("active name unique per owner; freed by terminal state", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		mkSess(t, st, u.ID, "dev")
		_, err := st.CreateSession(ctx, controld.Session{ID: controld.NewSessionID(), OwnerID: u.ID, Name: "dev", State: controld.StateQueued})
		if !errors.Is(err, controld.ErrConflict) {
			t.Fatalf("want ErrConflict, got %v", err)
		}
		// terminal frees the name
		first, _ := st.SessionByName(ctx, u.ID, "dev")
		if err := st.Transition(ctx, first.ID, controld.NonTerminal, controld.StateCanceled, controld.TransitionOpts{}); err != nil {
			t.Fatal(err)
		}
		if _, err := st.CreateSession(ctx, controld.Session{ID: controld.NewSessionID(), OwnerID: u.ID, Name: "dev", State: controld.StateQueued}); err != nil {
			t.Fatalf("terminal session must free the name: %v", err)
		}
	})

	t.Run("idempotency key replays", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		s1, err := st.CreateSession(ctx, controld.Session{ID: controld.NewSessionID(), OwnerID: u.ID, IdempotencyKey: "k1", State: controld.StateQueued})
		if err != nil {
			t.Fatal(err)
		}
		_, err = st.CreateSession(ctx, controld.Session{ID: controld.NewSessionID(), OwnerID: u.ID, IdempotencyKey: "k1", State: controld.StateQueued})
		if !errors.Is(err, controld.ErrIdemReplay) {
			t.Fatalf("want ErrIdemReplay, got %v", err)
		}
		got, err := st.SessionByIdem(ctx, u.ID, "k1")
		if err != nil || got.ID != s1.ID {
			t.Fatalf("replay lookup: %v %+v", err, got)
		}
	})

	t.Run("list pagination is stable and cursor resumes", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		var ids []string
		for i := 0; i < 5; i++ {
			s := mkSess(t, st, u.ID, "")
			ids = append(ids, s.ID)
			time.Sleep(2 * time.Millisecond) // distinct created_at
		}
		page1, next, err := st.ListSessions(ctx, controld.SessionQuery{Limit: 3})
		if err != nil || len(page1) != 3 || next == "" {
			t.Fatalf("page1: %v n=%d next=%q", err, len(page1), next)
		}
		page2, next2, err := st.ListSessions(ctx, controld.SessionQuery{Limit: 3, Cursor: next})
		if err != nil || len(page2) != 2 || next2 != "" {
			t.Fatalf("page2: %v n=%d", err, len(page2))
		}
		if page1[0].ID != ids[4] {
			t.Fatalf("newest first: got %s want %s", page1[0].ID, ids[4])
		}
		seen := map[string]bool{}
		for _, s := range append(page1, page2...) {
			seen[s.ID] = true
		}
		if len(seen) != 5 {
			t.Fatalf("pages overlap or drop: %v", seen)
		}
	})

	t.Run("terminal sessions hidden unless IncludeTerminal", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		s := mkSess(t, st, u.ID, "")
		st.Transition(ctx, s.ID, controld.NonTerminal, controld.StateDead, controld.TransitionOpts{})
		rows, _, _ := st.ListSessions(ctx, controld.SessionQuery{Limit: 10})
		if len(rows) != 0 {
			t.Fatalf("terminal leaked into default list")
		}
		rows, _, _ = st.ListSessions(ctx, controld.SessionQuery{Limit: 10, IncludeTerminal: true})
		if len(rows) != 1 {
			t.Fatalf("IncludeTerminal missing row")
		}
	})

	t.Run("runners upsert and sessions-on-runner filter", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		if err := st.UpsertRunner(ctx, controld.Runner{Name: "vm1", CapacityUsed: 1, CapacityTotal: 4, Connected: true, LastSeenAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		s := mkSess(t, st, u.ID, "")
		r := "vm1"
		st.Transition(ctx, s.ID, controld.NonTerminal, controld.StateCreating, controld.TransitionOpts{Runner: &r})
		on, err := st.SessionsOnRunner(ctx, "vm1", []controld.SessionState{controld.StateCreating})
		if err != nil || len(on) != 1 || on[0].ID != s.ID {
			t.Fatalf("on-runner: %v %+v", err, on)
		}
		runners, _ := st.ListRunners(ctx)
		if len(runners) != 1 || runners[0].CapacityTotal != 4 {
			t.Fatalf("runners: %+v", runners)
		}
	})

	t.Run("oldest queued ordering", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		a := mkSess(t, st, u.ID, "")
		time.Sleep(2 * time.Millisecond)
		mkSess(t, st, u.ID, "")
		q, err := st.OldestQueued(ctx)
		if err != nil || len(q) != 2 || q[0].ID != a.ID {
			t.Fatalf("fifo order: %v %+v", err, q)
		}
	})
}
