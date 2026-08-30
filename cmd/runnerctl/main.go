// cmd/runnerctl/main.go
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
)

func main() {
	base := flag.String("runnerd", "http://127.0.0.1:8080", "runnerd control URL")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: runnerctl [--runnerd URL] <create|ls|attach|suspend|resume|snapshot|rm> ...")
		os.Exit(2)
	}
	switch args[0] {
	case "create":
		image := ""
		if len(args) > 1 {
			image = args[1]
		}
		body, _ := json.Marshal(map[string]any{"image": image})
		resp, err := http.Post(*base+"/sessions", "application/json", bytes.NewReader(body))
		check(err)
		defer resp.Body.Close()
		io.Copy(os.Stdout, resp.Body)
		fmt.Println()
	case "ls":
		resp, err := http.Get(*base + "/sessions")
		check(err)
		defer resp.Body.Close()
		io.Copy(os.Stdout, resp.Body)
		fmt.Println()
	case "attach":
		id := requireID(args, "attach")
		// rattach's --url is a BASE it appends /attach (and the cursor, and
		// the session) to itself — see attachio.AttachURL and Run; passing
		// it a URL that already has /attach?session=<id> on it would double
		// up the path and mangle the session id into the query string. Pass
		// the bare ws://host:port base and the session id separately instead
		// — rattach folds them into the same URL contract it uses talking to
		// sessiond directly.
		wsBase := strings.Replace(*base, "http", "ws", 1)
		cmd := exec.Command("./bin/rattach", "--url", wsBase, "--session", id)
		cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
		cmd.Run()
	case "suspend":
		post(*base + "/sessions/" + requireID(args, "suspend") + "/suspend")
	case "resume":
		post(*base + "/sessions/" + requireID(args, "resume") + "/resume")
	case "snapshot":
		post(*base + "/sessions/" + requireID(args, "snapshot") + "/snapshot")
	case "rm":
		id := requireID(args, "rm")
		req, _ := http.NewRequest(http.MethodDelete, *base+"/sessions/"+id, nil)
		resp, err := http.DefaultClient.Do(req)
		check(err)
		resp.Body.Close()
		fmt.Println("removed", id)
	default:
		fmt.Fprintln(os.Stderr, "unknown command", args[0])
		os.Exit(2)
	}
}

// requireID pulls the session-id positional argument for a subcommand that
// needs one, exiting with a usage message instead of panicking with an
// index-out-of-range when it's missing.
func requireID(args []string, cmd string) string {
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: runnerctl %s <session-id>\n", cmd)
		os.Exit(2)
	}
	return args[1]
}
func post(url string) {
	resp, err := http.Post(url, "application/json", nil)
	check(err)
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body)
	fmt.Println()
}
func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
