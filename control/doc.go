// Package control defines Rainier's public control-plane application
// contract: the portable domain vocabulary, the caller-facing application
// interfaces, and the narrow host-supplied ports that self-hosted controld
// and Rainier Cloud both consume. Its canonical import path is
// github.com/tokencanopy/rainier/control.
//
// The package is a contract, not a service. It contains no implementation of
// the lifecycle, scheduling, reconciliation, or attach behavior: it freezes
// the seam those behaviors will be extracted behind, and pins it with
// external-package contract tests so the behaviors can be moved out of
// internal/controld by later workers without editing this surface.
//
// The package is neutral in four ways, each a hard boundary:
//
//   - Transport-neutral: it defines no HTTP handler, WebSocket route, SQL
//     statement, or wire frame. Adapters map these sentinels and commands to
//     a concrete transport.
//   - Identity-neutral: it carries no role, email, GitHub account, or
//     membership record. Actors are opaque IDs produced by a host adapter,
//     never decoded from client JSON.
//   - Persistence-neutral: repository ports expose only the semantic
//     reads and transitions the existing application behavior needs. They
//     never expose SQL transactions, rows, JSON blobs, credentials, secrets,
//     user records, or provider resources.
//   - Provider-neutral: no GCP project, AWS account, Azure subscription,
//     machine type, cluster, native zone, or disk ID appears anywhere in the
//     package. Placement is expressed in Rainier execution modes, product
//     regions, portable capabilities, and opaque pool and runner identities.
//
// Terminal and workspace bytes remain the already-public protocol packages
// (github.com/tokencanopy/rainier/protocol/{terminal,runner,workspace}).
// This package references those contracts but never duplicates their message
// structs.
//
// # Compatibility
//
// This surface is pre-v1 and may evolve during /v0/, but there is one
// canonical contract at a time: no V2, Legacy, compatibility-alias, or
// Cloud-fork type exists here. Later workers may consume this package but
// must not edit its exported signatures; a change to the contract is a
// coordinated decision, not a per-lane convenience.
package control
