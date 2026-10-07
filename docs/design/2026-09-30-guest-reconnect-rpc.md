# Guest reconnect session-RPC contract

This slice defines messages and strict decoders in `protocol/runner` for the
[authorization foundation](2026-09-29-guest-reconnect-authorization.md).
It does not register handlers, advertise support, enable reconnect or change the
signed transcript. Existing guests retain their one-connection bootstrap behavior.

The existing session-RPC envelope carries these method names and payloads:

| Method | Origin | Request fields | Successful response |
| --- | --- | --- | --- |
| `enroll_guest_reconnect` | Fresh guest, through its assigned runner | `protocol`, `token`, `boot_epoch`, `public_key` | Existing bootstrap `{"env":{...}}` shape |
| `begin_guest_reconnect` | Runner only | `protocol` | `GuestReconnectChallenge` |
| `accept_guest_reconnect` | Runner only | `protocol`, `attempt_id`, `signature` | `epoch`, `token`, `expires_in_sec` |

`protocol` is exactly 1. Tokens and public keys encode 32 bytes; signatures encode
64 bytes. All use canonical unpadded base64url. Boot and attempt identifiers are
1–256 printable ASCII bytes. Epoch and TTL are positive; TTL is a uint32 count of
seconds. An RPC refusal uses `OK=false` with exactly `{"error":"<code>"}`; the
closed codes are `invalid`, `expired`, `fenced` and `unavailable`. A successful
payload requires `OK=true`; callers must never decode a refusal as success.

Use the exported `DecodeGuestReconnect*` functions at untrusted boundaries.
Unlike plain `json.Unmarshal`, they enforce an inclusive 4096-byte raw payload
limit, including whitespace, and reject duplicate, unknown, missing, case-aliased,
null and trailing values. Failed decoding returns a zero value and a fixed error
without input text. Encoding remains standard `json.Marshal` on the typed structs.
The enrollment environment response retains the existing bootstrap response
contract and its existing size behavior; the 4 KiB cryptographic-message cap must
not truncate an authorized environment. The new strict response decoders cover
challenge, acceptance and fixed refusal payloads, not environment delivery.

The enrollment responder must atomically authorize/spend/pin the key, then resolve
and deliver the environment through the existing secret resolver. A secret
resolution failure does not restore a spent token. The hosted internal cell-api
acknowledgment is not the guest-facing response: the gateway must turn successful
authorization into the environment response only after commit, just as the legacy
exchange resolves secrets after spending. Returning only an acknowledgment to the
guest would break initial boot and is not this contract.

Workload scope is deliberately absent from requests. The transport owns the
original socket's connection generation; the host owns the assigned session and
placement lookup. Hosted membership and policy remain cell-api responsibilities.
Begin/accept must not be exposed as unrestricted guest-originated RPC methods.
A host must compare a decoded challenge with its expected session, placement and
connection incarnation before showing it to a guest. Decoding and signature
verification never replace durable expiry, single-use and current-authority checks.
No challenge or enrollment acknowledgment permits replacing a live relay. A fresh
accepted epoch must fence the old relay before any input or credential RPC.

One shared public protocol is preferable to gateway-local strings and DTOs, which
would allow hosted and self-hosted consumers to drift. Enabling methods before the
consumer/authorization path exists is deliberately deferred. Separate bounded
request/response decoders avoid guessing a message type from attacker input.

Tests pin literal JSON shapes and method names, reject malformed and oversized
messages, enforce canonical cryptographic encodings, preserve zero results on
failure, and execute a challenge/sign/decode/verify example. Fuzzing exercises all
decoders with the same input corpus. This is library execution, not a live guest
or VM qualification result.

Next integration must add both control-plane handlers, capability negotiation,
initial and cold-resume enrollment ordering, bounded guest peer handling,
configuration refresh, and relay takeover. It must retain the five-second
challenge/handshake budget and prove original-socket fencing, replay refusal,
tenant isolation and real PID/PTY/agent continuity before advertising support.

The follow-on [runner host callback](2026-09-30-guest-reconnect-host.md) now supplies
connection-bound begin/proof/accept orchestration without enabling the listener.
