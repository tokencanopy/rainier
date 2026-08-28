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
		id := args[1]
		wsURL := strings.Replace(*base, "http", "ws", 1) + "/attach?session=" + id
		cmd := exec.Command("./bin/rattach", "--url", wsURL)
		cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
		cmd.Run()
	case "suspend":
		post(*base + "/sessions/" + args[1] + "/suspend")
	case "resume":
		post(*base + "/sessions/" + args[1] + "/resume")
	case "snapshot":
		post(*base + "/sessions/" + args[1] + "/snapshot")
	case "rm":
		req, _ := http.NewRequest(http.MethodDelete, *base+"/sessions/"+args[1], nil)
		resp, err := http.DefaultClient.Do(req)
		check(err)
		resp.Body.Close()
		fmt.Println("removed", args[1])
	default:
		fmt.Fprintln(os.Stderr, "unknown command", args[0])
		os.Exit(2)
	}
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
