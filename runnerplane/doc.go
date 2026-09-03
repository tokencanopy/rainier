// Package runnerplane is the runner half of a Rainier control plane: the
// WebSocket endpoint runners dial, the connection registry behind it, the
// generation mint and its two fences, capability acceptance, reconciliation,
// event translation, and the dispatch correlation that makes
// control.RunnerTransport work.
//
// It is composed, not configured. A host supplies the Host interface — who a
// connection is (Identify), where its generations come from (NextGeneration),
// the fleet service and repository the plane reports to, the scheduler wake,
// and the two hooks for what the plane deliberately does not decide: the
// events that transition no session (Aside) and the requests a sandbox sends
// upward (SessionRequest). Everything else — the socket, the announce, the
// registry, the fences, the retries — is here, once, for the self-hosted
// controld and a hosted cell alike.
//
// The package imports only the standard library, coder/websocket, and the
// public control and protocol/runner contracts. It has no store, no HTTP
// routing of its own (Handler is mounted by the host), no SQL, and no
// provider.
package runnerplane
