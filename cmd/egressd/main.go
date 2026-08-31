// Command egressd is the per-VM HTTP CONNECT proxy that is the only egress
// path for session containers (spec §8). It is default-deny; runnerd pushes
// per-session allowlists at runtime via a small admin endpoint.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/tokencanopy/rainier/internal/egress"
)

func main() {
	listen := flag.String("listen", "0.0.0.0:3128", "proxy listen address")
	// v0: allow rules are pushed at runtime by runnerd via a tiny admin endpoint.
	// For the standalone binary, start permissive-to-nobody (default deny) and
	// rely on runnerd to SetAllow. Expose an admin mux on a second port.
	admin := flag.String("admin", "127.0.0.1:3129", "admin address for allow updates")
	flag.Parse()

	p := egress.New(os.Stdout) // audit to stdout for v0 (container logs)
	http.HandleFunc("/allow", func(w http.ResponseWriter, r *http.Request) {
		s := r.URL.Query().Get("session")
		hosts := r.URL.Query()["host"]
		if s == "" || len(hosts) == 0 {
			http.Error(w, "session required", http.StatusBadRequest)
			return
		}
		p.SetAllow(s, hosts)
		w.WriteHeader(http.StatusNoContent)
	})
	go func() { log.Fatal(http.ListenAndServe(*admin, nil)) }()
	log.Printf("egressd proxy on %s, admin on %s", *listen, *admin)
	log.Fatal(http.ListenAndServe(*listen, p.Handler()))
}
