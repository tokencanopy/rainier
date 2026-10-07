package driver

import (
	"strings"
	"testing"
)

func TestGuestProcessIdentityParsing(t *testing.T) {
	// Linux /proc/PID/stat: field 22 is starttime. The comm field can contain
	// spaces and closing parentheses; it must not shift positional parsing.
	stat := "42 (firecracker worker)) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 912345 0 0"
	got, err := guestProcessStart([]byte(stat), 42)
	if err != nil || got != 912345 {
		t.Fatalf("start=%d err=%v", got, err)
	}
	for _, bad := range []string{"", strings.Replace(stat, "42 (", "43 (", 1), strings.Replace(stat, "912345", "0", 1), strings.Replace(stat, "912345", "invalid", 1), "42 (firecracker) S 1"} {
		if _, err := guestProcessStart([]byte(bad), 42); err == nil {
			t.Fatalf("accepted malformed process stat %q", bad)
		}
	}
}
func TestGuestProcessArgumentsRequireExactInstance(t *testing.T) {
	for _, tc := range []struct {
		args string
		ok   bool
	}{
		{"/firecracker\x00--id\x00mvm-1\x00--api-sock\x00/run/firecracker.socket\x00", true},
		{"/firecracker\x00--id\x00mvm-10\x00", false},
		{"/not-firecracker\x00--id\x00mvm-1\x00", false},
		{"/firecracker\x00--log-path\x00mvm-1\x00", false},
		{"/firecracker\x00--id\x00mvm-1\x00--id\x00mvm-2\x00", false},
	} {
		if got := guestProcessArguments([]byte(tc.args), "mvm-1"); got != tc.ok {
			t.Errorf("args=%q got=%v", tc.args, got)
		}
	}
}
