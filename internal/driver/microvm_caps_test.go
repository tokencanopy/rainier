// internal/driver/microvm_caps_test.go
//
// The portable half of the privilege and preparation checks. What needs
// /proc/self/status — the capability mask itself — is in
// microvm_caps_linux_test.go, because off Linux effectiveCapabilities
// answers errors.ErrUnsupported by design and a test of it there would be
// asserting the platform rather than the check.
package driver

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestTheRequirementsListMatchesTheCheck: the text an operator is handed and
// the list the check reads are one list, so a capability cannot be added to
// the check and left out of the instructions.
func TestTheRequirementsListMatchesTheCheck(t *testing.T) {
	doc := MicrovmHostRequirements()
	for _, c := range microvmCapabilities {
		if !strings.Contains(doc, c.Name) {
			t.Errorf("the requirements text does not mention %s", c.Name)
		}
	}
	for _, want := range []string{"/dev/kvm", "cgroup v2", "AmbientCapabilities=", "requires running as root", "euid 0"} {
		if !strings.Contains(doc, want) {
			t.Errorf("the requirements text does not mention %q", want)
		}
	}
}

// writeIPForward points the forwarding check at a fixture holding value.
func writeIPForward(t *testing.T, value string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ip_forward")
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatalf("write the ip_forward fixture: %v", err)
	}
	old := hostIPForwardPath
	hostIPForwardPath = path
	t.Cleanup(func() { hostIPForwardPath = old })
}

// TestForwardingOffOnTheHostIsRefused.
//
// A slot turns forwarding on inside its OWN namespace, which gets a guest's
// packet from the TAP to the veth. The hop from the host end of that veth to
// the egress proxy is the host's namespace, and that switch is machine-wide —
// not something this driver may flip for everything else on the box. So a
// host that has not been prepared is refused at startup and told what to do,
// rather than accepting sessions whose every packet dies one hop out.
func TestForwardingOffOnTheHostIsRefused(t *testing.T) {
	writeIPForward(t, "0\n")
	err := checkHostForwarding()
	if err == nil {
		t.Fatal("a host with net.ipv4.ip_forward=0 was accepted; every guest on it would have a network it cannot send through")
	}
	for _, want := range []string{"ip_forward", "sysctl -w net.ipv4.ip_forward=1", "masquerade"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not mention %q, so an operator is not told how to fix it:\n%s", want, err)
		}
	}
}

// TestForwardingOnIsAccepted, so the check is not one that refuses every
// host.
func TestForwardingOnIsAccepted(t *testing.T) {
	writeIPForward(t, "1\n")
	if err := checkHostForwarding(); err != nil {
		t.Fatalf("a prepared host was refused: %v", err)
	}
}

// TestUnreadableForwardingFailsClosed: a runner that cannot tell whether a
// guest's packets would leave this host must not assume they would.
func TestUnreadableForwardingFailsClosed(t *testing.T) {
	old := hostIPForwardPath
	hostIPForwardPath = filepath.Join(t.TempDir(), "absent")
	t.Cleanup(func() { hostIPForwardPath = old })

	if err := checkHostForwarding(); err == nil {
		t.Fatal("a host whose forwarding state could not be read was accepted")
	}
}

// TestTheHostPreparationTextNamesBothHalves. The SNAT rule is the half this
// runner deliberately does NOT check — it is one rule among whatever else a
// host's ruleset holds — so this text, and the docs that quote it, are the
// only things that carry it to an operator.
func TestTheHostPreparationTextNamesBothHalves(t *testing.T) {
	doc := MicrovmHostPreparation()
	for _, want := range []string{"net.ipv4.ip_forward=1", "masquerade", "postrouting", "--microvm-guest-cidr"} {
		if !strings.Contains(doc, want) {
			t.Errorf("the host preparation text does not mention %q", want)
		}
	}
}
