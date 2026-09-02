// Package controlapp implements the portable application behavior behind the
// frozen public control contract: session lifecycle, environment definition,
// provider-neutral fleet truth and within-pool scheduling, and authorized
// terminal attachment with bounded workspace diff/push/pull orchestration.
// Its canonical import path is github.com/tokencanopy/rainier/controlapp.
//
// The package is a service, not a transport. It depends only on the control
// ports (github.com/tokencanopy/rainier/control) and the public protocol
// packages
// (github.com/tokencanopy/rainier/protocol/{runner,terminal,workspace}).
// Everything else is an adapter the host injects: HTTP and WebSocket
// transport, identity, SQL, Docker, GitHub, cloud providers, and billing all
// live outside this seam and are never imported here, nor is any internal/
// package.
//
// Each service is a deep module over narrow ports. SessionService owns
// session creation, authorization, lifecycle dispatch, and result recording.
// EnvironmentService owns environment definition and the setup-hash identity
// of an environment's build inputs. FleetService owns fleet truth and
// within-pool scheduling, while injected adapters own connections,
// persistence, eligible-pool policy, sensitive launch material, and provider
// execution. AttachmentService resolves and authorizes a session, grants a
// fenced controller generation, and delegates terminal transport to an
// AttachmentBroker.
//
// Terminal bytes stay opaque. This package never parses a socket, logs a
// terminal message, persists terminal bytes, or duplicates the terminal
// protocol; workspace operations share one private session-RPC implementation
// over control.RunnerTransport, which is the single downward request/response
// seam. Terminal and workspace message structs remain the already-public
// protocol packages; this package references those contracts and never
// duplicates them.
//
// Adapter failures are normalized at every port boundary: portError maps any
// error a port returns into the closed control sentinel vocabulary, so an
// adapter's SQL, connection string, or provider text can never leave this
// package through a control interface.
package controlapp
