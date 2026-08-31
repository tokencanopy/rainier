package cli

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"

	"rainier/internal/xfer"
)

// This file is the client end of the bounded file transfer: `rainier push` and
// `rainier pull`, expressed as two functions over Client so that cmd/rainier's
// subcommands are argument parsing and nothing else — and so the smoke test
// drives exactly the calls the real command makes.
//
// The protocol is deliberately plain (design §4.5): a push is one POST per
// megabyte, each waiting for its ack; a pull is one GET whose body is the
// archive. Everything about it is bounded, and the bounds are checked here
// FIRST — the client is the only hop that knows which local directory a
// transfer is about, so it is the only one that can name it when the answer is
// "that is too big".
//
// The archive rules (what may be in a tar, where an entry may land) belong to
// internal/xfer and are shared with the sandbox. That sharing is the point: a
// pull's archive comes out of somebody's container and is extracted into
// somebody's home directory, so the checks that keep its entries inside the
// destination have to be the same ones, not two implementations that agree
// today.

// Push tars localDir and streams it to remotePath inside session id.
//
// progress, when non-nil, is called after every acked chunk with the bytes
// sent so far and the archive's total size.
func Push(c *Client, sessionID, localDir, remotePath string, progress func(sent, total int64)) error {
	return pushLimited(c, sessionID, localDir, remotePath, progress, xfer.MaxBytes)
}

// pushLimited is Push with the cap as a parameter, so a test can reach it
// without moving a quarter of a gigabyte.
func pushLimited(c *Client, sessionID, localDir, remotePath string, progress func(sent, total int64), limit int64) error {
	if err := xfer.ValidatePath(remotePath); err != nil {
		return fmt.Errorf("destination: %w", err)
	}
	fi, err := os.Stat(localDir)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s is not a directory; push moves a directory tree", localDir)
	}

	archive, size, err := tarToTemp(localDir, limit)
	if err != nil {
		return err
	}
	defer os.Remove(archive.Name())
	defer archive.Close()

	// One id for the whole transfer: it is what lets the sandbox tell this
	// push from any other, and what makes a re-sent chunk recognizable as a
	// repeat rather than as a new transfer.
	id := RandHex(headerIDBytes)
	path := "/v0/sessions/" + sessionID + "/files"
	buf := make([]byte, xfer.ChunkBytes)

	var sent int64
	for seq := 0; ; seq++ {
		n, readErr := io.ReadFull(archive, buf)
		if readErr != nil && readErr != io.ErrUnexpectedEOF && readErr != io.EOF {
			return fmt.Errorf("reading the archive: %w", readErr)
		}
		done := sent+int64(n) >= size
		chunk := xfer.PushChunk{Xfer: id, Path: remotePath, Seq: seq, Data: buf[:n], Done: done}

		var ack xfer.PushAck
		if err := c.Do(http.MethodPost, path, chunk, &ack); err != nil {
			return err
		}
		if ack.Seq != seq {
			// The two ends disagree about where the transfer is. Continuing
			// would assemble the wrong bytes on the far side.
			return fmt.Errorf("the session acked chunk %d for chunk %d; abandoning the transfer", ack.Seq, seq)
		}
		sent += int64(n)
		if progress != nil {
			progress(sent, size)
		}
		if done {
			return nil
		}
	}
}

// tarToTemp writes an archive of dir to a temporary file and rewinds it,
// returning the file and its size.
//
// A file rather than a buffer, and the whole archive before the first chunk:
// the size is what the progress line counts against, the cap is checked while
// writing (so an enormous directory dies before it has produced an enormous
// anything), and a chunk that has to be re-sent can simply be read again.
func tarToTemp(dir string, limit int64) (*os.File, int64, error) {
	f, err := os.CreateTemp("", "rainier-push-*.tgz")
	if err != nil {
		return nil, 0, err
	}
	size, err := xfer.TarGz(f, dir, limit)
	if err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, 0, fmt.Errorf("packing %s: %w", dir, err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, 0, err
	}
	return f, size, nil
}

// Pull downloads remotePath from session id and extracts it into localDir,
// which is created if it does not exist.
//
// progress, when non-nil, is called as bytes arrive; the total is 0 because
// nothing on this side knows it until the stream ends.
func Pull(c *Client, sessionID, remotePath, localDir string, progress func(received, total int64)) error {
	return pullLimited(c, sessionID, remotePath, localDir, progress, xfer.MaxBytes)
}

func pullLimited(c *Client, sessionID, remotePath, localDir string, progress func(received, total int64), limit int64) error {
	if err := xfer.ValidatePath(remotePath); err != nil {
		return fmt.Errorf("source: %w", err)
	}
	path := "/v0/sessions/" + sessionID + "/files?path=" + url.QueryEscape(remotePath)
	body, err := c.Open(http.MethodGet, path)
	if err != nil {
		return err
	}
	defer body.Close()

	// To a temp file first, then extracted. Streaming a tar straight into the
	// destination would mean writing entries before the archive's last one has
	// been seen — and the archive is a session's, which is not a peer this
	// side trusts (xfer.UntarGz validates the whole thing before it writes
	// anything, and it needs a file to do that twice over).
	f, err := os.CreateTemp("", "rainier-pull-*.tgz")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()

	received, err := copyBounded(f, body, limit, progress)
	if err != nil {
		return err
	}
	if received == 0 {
		return fmt.Errorf("%s came back empty", remotePath)
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return xfer.UntarGz(f.Name(), localDir, xfer.MaxExtractBytes)
}

// copyBounded copies src to dst, stopping with an error the moment the total
// would pass limit. It is the client's own half of the transfer cap: controld
// bounds what it will relay and the sandbox bounds what it will send, but a
// client that trusted either of them would still be one compromised session
// away from filling its own disk.
func copyBounded(dst io.Writer, src io.Reader, limit int64, progress func(received, total int64)) (int64, error) {
	buf := make([]byte, xfer.ChunkBytes)
	var total int64
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if total+int64(n) > limit {
				return total, fmt.Errorf("the transfer is larger than the %s limit: %w",
					xfer.HumanBytes(limit), xfer.ErrTooLarge)
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return total, werr
			}
			total += int64(n)
			if progress != nil {
				progress(total, 0)
			}
		}
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
}

// ProgressLine renders one line of transfer progress. It is a function rather
// than a Printf at the call site so the formatting has a test and both
// directions read the same.
//
// A total of zero means "unknown", which is a pull: nothing on the client side
// knows how big the archive is until it ends.
func ProgressLine(verb string, done, total int64) string {
	if total <= 0 {
		return fmt.Sprintf("%s %s", verb, xfer.HumanBytes(done))
	}
	return fmt.Sprintf("%s %s / %s (%d%%)", verb, xfer.HumanBytes(done), xfer.HumanBytes(total), done*100/total)
}
