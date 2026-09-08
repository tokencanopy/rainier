package main

import (
	"os"
	"strings"
)

// defaultServer is the hosted edge a bare `rainier login` signs in to when
// this machine has no context yet. It is EMPTY in source builds and is
// supplied by a release build:
//
//	go build -ldflags '-X main.defaultServer=https://…'
//
// It is a build seam and not a constant on purpose. A hostname compiled into
// the tree is a hostname somebody has to remember to change, and an
// unconfirmed one shipped in a source build would send a first-time user's
// credentials at a server nobody has stood up. A build that names none says
// so and asks for `--cloud URL`, which is the honest answer.
var defaultServer string

// hostedDefaultServer resolves the hosted default: the environment's override
// first, so a developer can point a source build at a staging edge without
// rebuilding it, then the build's own value. The result is trimmed of the
// trailing slash every other URL in this CLI is trimmed of, so the two spellings
// of one server do not become two contexts.
func hostedDefaultServer() string {
	if v := strings.TrimSpace(os.Getenv("RAINIER_SERVER")); v != "" {
		return strings.TrimRight(v, "/")
	}
	return strings.TrimRight(strings.TrimSpace(defaultServer), "/")
}
