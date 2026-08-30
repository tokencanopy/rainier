// Command rattach is the dev attach client: it dials sessiond's (or
// runnerd's/controld's) /attach websocket, puts the local terminal in raw
// mode, and pipes stdin/stdout through. Ctrl-] detaches without touching the
// remote session. The loop itself lives in internal/attachio; this file is
// just flag parsing.
package main

import (
	"context"
	"flag"
	"log"

	"rainier/internal/attachio"
)

func main() {
	baseURL := flag.String("url", "ws://127.0.0.1:7070", "sessiond/runnerd base URL (no path)")
	since := flag.Uint64("since", 0, "resume from sequence number; 0 replays the whole event log (omit for the current screen)")
	session := flag.String("session", "", "session id — required when --url points at runnerd's relay; ignored (and unnecessary) when attaching directly to sessiond")
	flag.Parse()

	// Whether --since was typed is the flag's meaning, not its value: an
	// omitted flag asks for the current screen, an explicit --since 0 asks
	// for the whole log. attachio.Cursor owns that mapping for both CLIs.
	given := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "since" {
			given = true
		}
	})

	ctx := context.Background()
	if err := attachio.Run(ctx, attachio.AttachURL(*baseURL, *session), nil, attachio.Cursor(given, *since)); err != nil {
		log.Fatal(err)
	}
}
