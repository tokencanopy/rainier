package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/internal/attachio"
	"github.com/tokencanopy/rainier/internal/cli"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// TestAttachFlagsAskForOwnership pins what each flag actually requests. The
// default is the one that matters: claim when control is free, which is every
// single-device attach and is zero clicks.
func TestAttachFlagsAskForOwnership(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want attachio.Options
	}{
		{"the default claims when control is free", []string{"box1"},
			attachio.Options{Control: true, Mode: terminal.ModeControl}},
		{"--view never claims", []string{"box1", "--view"},
			attachio.Options{Control: true, Mode: terminal.ModeView, NeverClaim: true}},
		{"--take claims once", []string{"--take", "box1"},
			attachio.Options{Control: true, Mode: terminal.ModeControl, Take: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, got, err := attachFlags(tc.args)
			if err != nil {
				t.Fatalf("attachFlags(%q): %v", tc.args, err)
			}
			if got != tc.want {
				t.Fatalf("attachFlags(%q) asked for %+v, want %+v", tc.args, got, tc.want)
			}
		})
	}
}

// TestAttachRefusesOppositeFlags: --view and --take cannot both be honoured,
// and guessing which one somebody meant is worse than saying so.
func TestAttachRefusesOppositeFlags(t *testing.T) {
	_, _, _, _, err := attachFlags([]string{"box1", "--view", "--take"})
	if err == nil {
		t.Fatal("--view --take was accepted")
	}
	if !strings.Contains(err.Error(), "opposite") {
		t.Fatalf("error = %q; it should say why", err)
	}
}

// TestJourney6ReconnectAsksForWhatItActuallyHad is the CLI half of
// conditional reconnect. A controller presents its generation, so it resumes
// only while nobody took it. A viewer stays a viewer rather than quietly
// acquiring control because a network blip happened to free it. And nothing
// re-claims: --take is spent by the attempt that used it.
func TestJourney6ReconnectAsksForWhatItActuallyHad(t *testing.T) {
	for _, tc := range []struct {
		name string
		prev attachio.Options
		out  attachio.Outcome
		want attachio.Options
	}{
		{
			name: "a controller presents its generation",
			prev: attachio.Options{Control: true, Mode: terminal.ModeControl},
			out:  attachio.Outcome{Mode: terminal.ModeControl, Generation: 7},
			want: attachio.Options{Control: true, Mode: terminal.ModeControl, Expected: 7},
		},
		{
			name: "a viewer comes back a viewer",
			prev: attachio.Options{Control: true, Mode: terminal.ModeControl, Expected: 7},
			out:  attachio.Outcome{Mode: terminal.ModeView, Generation: 9},
			want: attachio.Options{Control: true, Mode: terminal.ModeView},
		},
		{
			name: "--take is spent, not renewed",
			prev: attachio.Options{Control: true, Mode: terminal.ModeControl, Take: true},
			out:  attachio.Outcome{Mode: terminal.ModeView, Generation: 2},
			want: attachio.Options{Control: true, Mode: terminal.ModeView},
		},
		{
			name: "nothing negotiated leaves the request alone",
			prev: attachio.Options{Control: true, Mode: terminal.ModeControl},
			out:  attachio.Outcome{},
			want: attachio.Options{Control: true, Mode: terminal.ModeControl},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := reconnectOwnership(tc.prev, tc.out); got != tc.want {
				t.Fatalf("reconnectOwnership = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestInfoControllerLine pins the three answers `info` gives and the one it
// must never give: the API says whether somebody holds control, never who, so
// everybody that is not this device is "another device".
func TestInfoControllerLine(t *testing.T) {
	cfgWith := func(session, generation string) cli.Config {
		return cli.Config{
			Current: "ctx",
			Contexts: map[string]cli.Context{"ctx": {
				Server:                "https://rainier.example.invalid",
				Token:                 "tok_example",
				LastControlSession:    session,
				LastControlGeneration: generation,
			}},
		}
	}
	held := session{ID: "sess_example", Controller: controllerView{Generation: "5", Held: true}}
	free := session{ID: "sess_example", Controller: controllerView{Generation: "5", Held: false}}

	for _, tc := range []struct {
		name string
		cfg  cli.Config
		s    session
		want string
	}{
		{"nobody has it", cfgWith("sess_example", "5"), free, "none"},
		{"this device still holds the generation it was granted", cfgWith("sess_example", "5"), held, "this device"},
		{"somebody advanced past it", cfgWith("sess_example", "4"), held, "another device"},
		{"this device never had this session", cfgWith("sess_other", "5"), held, "another device"},
		{"this device ended as a viewer", cfgWith("sess_example", ""), held, "another device"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := controllerLine(tc.cfg, tc.s); got != tc.want {
				t.Fatalf("controllerLine = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestInfoPrintsTheControllerRow keeps the row in the output `info` actually
// produces, beside the other facts, rather than only in the helper above.
func TestInfoPrintsTheControllerRow(t *testing.T) {
	var buf bytes.Buffer
	printInfo(&buf, cli.Config{}, session{
		ID: "sess_example", Name: "box1", State: "running",
		Controller: controllerView{Generation: "3", Held: true},
	})
	if !strings.Contains(buf.String(), "Controller:   another device") {
		t.Fatalf("info printed:\n%s", buf.String())
	}
}

// TestInfoOnAnOlderServerSaysNobodyHasControl: a server that never rendered
// the object leaves it zero, and "nobody has control" is the truth there —
// nothing was ever conditional.
func TestInfoOnAnOlderServerSaysNobodyHasControl(t *testing.T) {
	if got := controllerLine(cli.Config{}, session{ID: "sess_example"}); got != "none" {
		t.Fatalf("controllerLine against an older server = %q, want none", got)
	}
}
