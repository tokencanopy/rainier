package relay

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// TestColdIsAdditiveOnTheSuspendNotice is the compatibility promise for the
// one field the cold-suspend handshake adds. runnerd runs on the host and
// sessiond ships inside the session image, and a session keeps the sessiond
// it booted with for life — so the suspend notice a sandbox already reads
// must be byte-identical after this, and a sandbox that never learns the
// field must read a plain "suspending".
func TestColdIsAdditiveOnTheSuspendNotice(t *testing.T) {
	warm, err := json.Marshal(ControlEvent{Kind: KindSuspending, ID: 4})
	if err != nil {
		t.Fatal(err)
	}
	if string(warm) != `{"kind":"suspending","id":4}` {
		t.Fatalf("a warm suspend notice = %s", warm)
	}
	if strings.Contains(string(warm), `"cold"`) {
		t.Fatalf("a warm suspend notice leaked a cold key: %s", warm)
	}

	cold, err := json.Marshal(ControlEvent{Kind: KindSuspending, ID: 5, Cold: true})
	if err != nil {
		t.Fatal(err)
	}
	if string(cold) != `{"kind":"suspending","id":5,"cold":true}` {
		t.Fatalf("a cold suspend notice = %s", cold)
	}

	// An older sandbox decodes it as the plain notice it has always answered:
	// unknown JSON keys are dropped, so it quiesces its execs and acks, and
	// the host-side unmount happens without its help.
	var old struct {
		Kind string `json:"kind"`
		ID   uint64 `json:"id,omitempty"`
	}
	if err := json.Unmarshal(cold, &old); err != nil {
		t.Fatalf("an old build could not decode a cold notice at all: %v", err)
	}
	if old.Kind != KindSuspending || old.ID != 5 {
		t.Fatalf("an old build decoded a cold notice as %+v", old)
	}

	var back ControlEvent
	if err := json.Unmarshal(cold, &back); err != nil {
		t.Fatal(err)
	}
	if !back.Cold || back.ID != 5 {
		t.Fatalf("cold round trip mangled: %+v", back)
	}
}

// TestBootConfigKindRidesAnOrdinaryControlFrame pins that the boot
// configuration needs no new frame type and no new envelope: it is a
// FrameControl with AttachID 0 carrying a ControlEvent whose payload relay
// does not read, exactly like every request and response already on this
// channel.
func TestBootConfigKindRidesAnOrdinaryControlFrame(t *testing.T) {
	if KindBootConfig != "boot_config" {
		t.Fatalf("boot config kind = %q", KindBootConfig)
	}
	ev, err := json.Marshal(ControlEvent{Kind: KindBootConfig,
		Payload: json.RawMessage(`{"protocol":1,"session_id":"sess_example"}`)})
	if err != nil {
		t.Fatal(err)
	}
	const wantEvent = `{"kind":"boot_config","payload":{"protocol":1,"session_id":"sess_example"}}`
	if string(ev) != wantEvent {
		t.Fatalf("a boot config event = %s\nwant %s", ev, wantEvent)
	}
	raw, err := Encode(Frame{Type: FrameControl, Payload: ev})
	if err != nil {
		t.Fatal(err)
	}
	var f Frame
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if f.Type != FrameControl || f.AttachID != 0 || !bytes.Equal(f.Payload, ev) {
		t.Fatalf("boot config frame round trip mangled: %+v", f)
	}
}
