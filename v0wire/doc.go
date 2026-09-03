// Package v0wire is the JSON wire of Rainier's /v0/ HTTP surface: the client-
// facing view of every resource, the request bodies clients send, the
// validators those bodies pass, and the closed mapping from a control sentinel
// to a status code. Its canonical import path is
// github.com/tokencanopy/rainier/v0wire.
//
// It is shapes and rules, not a server. There is no router here and no
// handler: routing, authentication, and the store reads that fill a view in
// stay with each host (ADR-0001 leaves HTTP routing outside the application
// service), so self-hosted controld and a hosted cell can serve the same wire
// from different front doors without either copying the shapes.
//
// The package is neutral in three ways:
//
//   - Storage-neutral: it renders control's domain types and nothing else. A
//     view field that no domain type carries — a session's environment NAME,
//     an environment's snapshot runner — is passed in by the host, which is
//     the one that knows how to look it up.
//   - Identity-neutral: it decodes no credential, names no actor, and carries
//     no authorization. A request body is data until a host has authorized it.
//   - Secret-free: no view type has anywhere to put a secret value, a
//     credential, or a terminal byte, which is the durable way to keep one off
//     the wire. Secrets and credentials views are the host's own.
//
// Two rules hold across every view here, and the golden tests in wire_test.go
// are what keep them true:
//
//   - No field is omitempty. The key set of a view is identical on every row,
//     because a key that appears only sometimes cannot be told apart by a
//     client from an older server that never had it.
//   - A nil slice renders as [] and never as null, so the difference between
//     two stores' nil-vs-empty handling never reaches a client.
package v0wire
