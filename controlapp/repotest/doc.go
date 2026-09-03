// Package repotest is the executable contract of the three control
// repository ports: control.SessionRepository, control.EnvironmentRepository,
// and control.FleetRepository.
//
// How a host proves its repositories. A host — the self-hosted in-memory
// store, the self-hosted Postgres store, or a hosted cell's own store —
// hands Run a Stores value carrying its three ports over ONE backing store
// plus a way to make a workspace exist there, and Run drives every case of
// the contract against a fresh, empty store. Passing this suite is what
// "implements the ports" means; the ports' doc comments say what the
// behavior is, and these cases are where it is pinned.
//
// The suite is written against two workspaces (Alpha, Beta) and two pools
// (PoolA, PoolB) because most of what the contract has to say is about
// scope: a store that treats either pair as the same thing, or that answers
// a cross-workspace lookup with anything other than "not found", fails here
// rather than in production. Every expected error is compared with errors.Is
// against a control sentinel and nothing else — a store may wrap a sentinel
// with safe context, but the sentinel is the contract.
//
// The package holds no fixture that names a real host, workspace, runner, or
// person: every identifier is a synthetic opaque one (ws_alpha, pool_a,
// sess_example, act_a, runner_a) and every hostname is under a reserved
// test domain.
package repotest
