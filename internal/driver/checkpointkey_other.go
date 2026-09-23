//go:build !unix

package driver

import "io/fs"

// checkKeyOwner has nothing to check off Unix: file ownership there is not a
// uid, and a microVM host is Linux by construction (ADR-0003 §4.5). The mode
// check in LoadCheckpointKey still applies, and is what the doc comment
// promises on this platform.
func checkKeyOwner(string, fs.FileInfo) error { return nil }
