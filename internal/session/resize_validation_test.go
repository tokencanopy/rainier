package session

import "testing"

func TestInvalidTerminalSizesCannotChangeSessionState(t *testing.T) {
	for _, size := range []Size{{-1, 24}, {80, -1}, {0, 24}, {80, 0}, {4097, 24}, {80, 4097}, {4096, 4096}} {
		t.Run("attach", func(t *testing.T) {
			s, _ := newFakeSession(t)
			if _, err := s.Attach(0, size, controllerAt(7)); err == nil {
				t.Fatalf("accepted invalid size %+v", size)
			}
			if len(s.viewers) != 0 || s.controllerGen != 0 {
				t.Fatal("rejected attach changed session authority")
			}
		})
		t.Run("resize", func(t *testing.T) {
			s, fp := newFakeSession(t)
			a, err := s.Attach(0, Size{80, 24}, controllerAt(7))
			if err != nil {
				t.Fatal(err)
			}
			if got := <-fp.resizes; got != (Size{80, 24}) {
				t.Fatalf("initial size: %+v", got)
			}
			before := *s.viewers[a.ID]
			if s.SetSize(a.ID, 7, size) {
				t.Fatalf("accepted invalid size %+v", size)
			}
			if s.viewers[a.ID].size != before.size || s.viewers[a.ID].sizeAt != before.sizeAt || s.size != (Size{80, 24}) {
				t.Fatal("rejected resize changed sizing state")
			}
			assertNoResize(t, fp)
		})
	}
}
