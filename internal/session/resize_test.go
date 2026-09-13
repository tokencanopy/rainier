package session

import "testing"

// The pty follows the most recent resize from any attachment that may type.
// The tick, not the slice order, is what "most recent" means: applySizeLocked
// walks a map, so a rule that read the last element would be a coin flip.
func TestLatestSize(t *testing.T) {
	cases := []struct {
		name string
		in   []reported
		want Size
		ok   bool
	}{
		{"none", nil, Size{}, false},
		{"one", []reported{{Size{120, 40}, 1}}, Size{120, 40}, true},
		{"the latest wins, whole", []reported{{Size{120, 30}, 1}, {Size{80, 40}, 2}}, Size{80, 40}, true},
		{"and not the smallest", []reported{{Size{80, 40}, 1}, {Size{120, 30}, 2}}, Size{120, 30}, true},
		{"whatever order it is read in", []reported{{Size{120, 30}, 9}, {Size{80, 40}, 2}}, Size{120, 30}, true},
	}
	for _, c := range cases {
		got, ok := latestSize(c.in)
		if ok != c.ok || got != c.want {
			t.Fatalf("%s: = %+v,%v want %+v,%v", c.name, got, ok, c.want, c.ok)
		}
	}
}
