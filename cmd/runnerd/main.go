// cmd/runnerd/main.go
package main

import (
	"flag"
	"log"
	"net/http"

	"rainier/internal/driver"
	"rainier/internal/runnerd"
)

func main() {
	listen := flag.String("listen", "0.0.0.0:8080", "control + relay listen address")
	dialBase := flag.String("dial-base", "ws://runnerd:8080", "URL sessiond containers dial to register")
	image := flag.String("image", "rainier-session:latest", "default session image")
	network := flag.String("network", "rainier-internal", "internal docker network for sessions")
	egressAdmin := flag.String("egress-admin", "http://egressd:3129", "egressd admin URL")
	slots := flag.Int("slots", 16, "capacity")
	flag.Parse()

	drv := driver.NewDocker(driver.DockerOpts{Image: *image, Network: *network, TotalSlots: *slots})
	s := runnerd.New(drv, *dialBase, *egressAdmin)
	log.Printf("runnerd on %s (dial-base %s)", *listen, *dialBase)
	log.Fatal(http.ListenAndServe(*listen, s.Handler()))
}
