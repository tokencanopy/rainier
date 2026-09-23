// Package wstream carries a microVM session's workspace out of the guest as
// one byte stream, and presents that stream to the host as the file tree the
// checkpoint writer walks.
//
// It exists because the two ends of a cold suspend see different things. The
// guest has the workspace MOUNTED — it is an ext4 image, and the guest's kernel
// is the only one that may parse it, because a host that loop-mounted a
// tenant's filesystem would be running the host kernel's ext4 driver over
// attacker-controlled metadata (see
// docs/design/2026-09-23-workspace-checkpoint-wiring.md §2). The host has the
// checkpoint library, which takes an fs.FS. So the guest writes the tree and
// the host reads it back as a tree.
//
// # The stream
//
//	"rainier.wsstream.v1\n"
//	index:  repeated uvarint(len) record, terminated by a zero length
//	        record := kind byte ('d' | 'f' | 'l') || name
//	tar:    archive/tar, the same entries in the same order, bodies for files
//
// The index carries the tree's SHAPE and nothing else — a name and a kind.
// Every other attribute (mode, size, modification time, link target) comes from
// the tar header when the walk reaches that entry, so each fact has one source
// and there is nothing for the two sections to disagree about.
//
// The index exists because fs.WalkDir is not streamable: it asks for every one
// of a directory's children before it visits the first of them, and a child's
// subtree sits in the stream between that child and its next sibling. No
// emission order fixes that, and buffering entry CONTENT to paper over it is
// the one thing this design refuses to do — the host would then hold a
// tenant's workspace in memory, or on its disk in plaintext. So the host
// buffers names (bounded by Limits.MaxEntries and Limits.MaxIndexBytes, fail
// closed) and never a file byte: the tar reader is a one-way cursor the walk
// drags forward, and a skipped body costs nothing to discard.
//
// # The order
//
// Entries are emitted in fs.WalkDir order — depth first, lexical within a
// directory — because that is the order the checkpoint writer walks in. The
// host's cursor only ever moves forward, so a stream in any other order fails
// the suspend rather than producing a checkpoint of a tree nobody had.
//
// # Trust
//
// The guest is the untrusted end. Every limit in Limits is applied by the HOST
// as well as by the guest, because a check performed by the hop before you is a
// check you are trusting. The guest applies them too, so that an oversized tree
// fails inside the session with a sentence about the session rather than as a
// refused stream.
//
// # Errors
//
// No error here carries a path, a file name or a symlink target. These travel
// from sessiond through runnerd to a session's error column, where the tenancy
// specification's §15.1 prohibited-logging list applies. A refusal names the
// entry's ORDINAL and its kind, exactly as the checkpoint package's does, which
// is findable with a local walk and is not a path.
package wstream
