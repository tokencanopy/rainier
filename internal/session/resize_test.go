package session

import "testing"

func TestEffectiveSize(t *testing.T) {
	cases := []struct {
		name string
		in   []Size
		want Size
		ok   bool
	}{
		{"none", nil, Size{}, false},
		{"one", []Size{{120, 40}}, Size{120, 40}, true},
		{"smallest-wins-per-axis", []Size{{120, 30}, {80, 40}}, Size{80, 30}, true},
	}
	for _, c := range cases {
		got, ok := EffectiveSize(c.in)
		if ok != c.ok || got != c.want {
			t.Fatalf("%s: = %+v,%v want %+v,%v", c.name, got, ok, c.want, c.ok)
		}
	}
}
