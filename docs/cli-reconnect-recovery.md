# Hosted terminal reconnect recovery

## Problem and scope

An established terminal used a frozen bearer header on every reconnect. HTTP
requests already knew how to refresh hosted credentials, but WebSocket upgrades
bypassed that path. A scheduled gateway authorization lease renewal therefore
became a permanent 401 after access-token expiry. Restoring termios alone also
left remote mouse-reporting modes active in the local shell after final failure.

The fix is confined to CLI recovery and terminal close classification. No session
restart, gateway deployment, longer credential lifetime, or relaxed authorization
is needed. Existing cloud sessions are not modified.

## Design and invariants

- Keep one authentication client for the attach lifetime. On upgrade401, use the
  existing locked token-reload/rotation path and retry once with the same cursor
  and workspace. Each successful stream starts a fresh recovery interval.
- Adopt a sibling CLI's rotated pair before spending a single-use refresh token.
  If that saved access token also expired while the laptop slept, exchange the
  latest refresh token under the same lock. Bound refresh and lock waiting.
- Pin the original named context's server and owner. Removing/replacing that
  context stops recovery instead of resurrecting it or borrowing another login.
- Login and context/workspace edits use that same config lock and read the latest
  file inside it. Refresh persistence checks the exchanged credential generation;
  file replacement is atomic so readers never see a partial token pair. A new
  login arriving during an exchange wins after the older exchange finishes.
- A second401, refused refresh,403, or ordinary policy close stops recovery.
  The gateway's exact policy-close reason `attach lease expired; reattach` alone
  invites a fresh edge authorization decision. It never authorizes access itself.
- Do not automatically replay a refresh after a transport/persistence failure:
  its single-use token may already have been consumed.
- Cursor advances only after rendered output. Transient recovery keeps emulator
  modes because missed-output replay need not include their original enables.
  Final terminal handoff disables mouse/focus/bracketed-paste modes, restores
  cursor visibility/text attributes, and never clears screen or scrollback.
  Queued TTY input is discarded, but piped input is untouched. Non-TTY output
  receives no added terminal reset sequences. Interrupt/termination signals
  cancel dial, refresh, and retry waits and unwind through this same cleanup;
  connected Ctrl-C remains ordinary remote terminal input.
- Routine recovery writes no local status text into the remote application's
  screen, on either stdout or stderr. The next remote frame may use relative
  cursor movements, so inserting even one line shifts its rendering. The same
  quiet behavior applies throughout retry backoff: recovery emits no local
  diagnostics, and Ctrl-C cancels waiting. Final failure,
  deliberate detach, and remote process exit still report their outcomes.
  The one-shot developer client `rattach` retains its disconnect/resume notice.

Screen-preservation alternatives rejected: stderr normally shares the same TTY;
saving/restoring only the cursor cannot undo text overwritten or scrolled by a
notice; forcing a full repaint changes replay semantics and may duplicate or
lose scrollback. An out-of-band prolonged-outage indicator is a future UI
decision, not a reason to write into the remote application's screen.

Input handling during recovery remains a separate limitation: termios returns
to cooked mode between attempts, so typed input or enabled mouse/focus reports can
echo locally and disturb the screen. Queued TTY input is discarded before the
next stream, but that cannot undo local echo. Keeping input ownership across
retry backoff requires a separate change and PTY tests for typed input and
terminal-generated reports during an outage.

Alternatives rejected: longer token/lease lifetimes only postpone the failure and
weaken revocation bounds; retrying every401 indefinitely masks revoked access;
refreshing via a dummy HTTP resource creates an unrelated endpoint dependency;
resetting terminal modes after each connection breaks cursor-only TUI replay.

## Verification and limits

Regression tests use real loopback WebSocket connections and PTYs for expiry,
successive renewals, saved-token adoption, bounded refusal, exact output replay,
and final mode cleanup. Cursor-sensitive PTY regressions render main and
alternate screens across a disconnect and failed upgrade; they assert both
literal screen contents and the cursor position, with stdout/stderr sharing
one terminal. The built-process test also forbids recovery diagnostics.
Credential tests cover removed/replaced contexts and
cancellation while a sibling holds the config lock. Run `make verify` plus
focused race tests. PTY subprocess tests exercise actual Ctrl-C during both
upgrade and refresh, plus connected Ctrl-C forwarding. A separately gated process test uses the built CLI and real
sessiond with a synthetic hosted-auth proxy; it is not a deployed-edge test.

This does not guarantee VM-loss recovery, unlimited local scrollback, or recovery
from revoked credentials. Actual laptop sleep/wake and deployed lease renewal
must still be qualified using the released CLI. Windows cross-process refresh
locking remains a pre-existing limitation. No release or deployment is part of
this change.
