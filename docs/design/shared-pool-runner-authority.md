# Explicit shared-pool runner registration and reconciliation

A host serving sessions from multiple workspaces cannot reconcile its inventory
under one workspace's identity. `FleetService.RegisterPoolRunner` and
`ReconcilePoolRunner` are explicit application entry points for that host.
Adapters must authenticate pool-wide authority before calling them, and must
pass an empty `WorkspaceID`. Legacy `RegisterRunner` and `ReconcileRunner`
continue to require a workspace. No existing credential is widened by this API.

Reconciliation reads authoritative placements on the named pool and runner and
uses each row's workspace for transitions. A missing creating session returns
to the queue; a missing live session becomes dead. Unknown, unassigned, terminal,
or elsewhere-assigned reports are destroy candidates, never adopted. Since the
wire names sessions without a workspace while storage keys them by workspace,
duplicate IDs in the assigned inventory return `ErrConflict` before session
mutations. Runner capacity/generation registration may already have occurred.

Both entry points retain the existing runner generation and capacity checks.
The protocol and frozen `control.Fleet` interface are unchanged. A hosted
adapter can expose these methods through its own authenticated composition;
self-hosted callers retain the existing path. Credential issuance, pool audit,
tenant resolution for events and RPCs, and Linux hardware qualification belong
to the host integration and are not enabled by adding these methods.
