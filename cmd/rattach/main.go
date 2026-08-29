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
	since := flag.Uint64("since", 0, "resume from sequence number")
	session := flag.String("session", "", "session id — required when --url points at runnerd's relay; ignored (and unnecessary) when attaching directly to sessiond")
	flag.Parse()

	ctx := context.Background()
	if err := attachio.Run(ctx, attachio.AttachURL(*baseURL, *since, *session), nil, *since); err != nil {
		log.Fatal(err)
	}
}
