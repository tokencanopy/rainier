// internal/controld/pgstore/events.go
package pgstore

import (
	"context"

	"github.com/tokencanopy/rainier/control"
)

// Record writes one control.Event to the events table (migration 0009), on
// whatever q(ctx) hands back — so an event recorded inside a Run commits with
// the mutation it describes, and one recorded outside a unit is its own
// write. That is the whole point of the table: an event is a fact exactly
// when the change it reports is.
//
// The columns are the event's fixed fields and nothing else. Nothing here
// takes a terminal byte, a session's content, a secret, a raw error, a price,
// or a provider resource id, because control.Event has none of them to give.
func (s *Store) Record(ctx context.Context, e control.Event) error {
	_, err := s.q(ctx).Exec(ctx, `
		INSERT INTO events (id, workspace_id, actor_id, action, resource_kind, resource_id,
			resource_workspace_id, resource_creator_id, at, placement_generation,
			cpu_time_seconds, memory_byte_seconds, storage_bytes, network_bytes, agent_token_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
		string(e.ID), string(e.WorkspaceID), string(e.ActorID), string(e.Action),
		string(e.Resource.Kind), e.Resource.ID, string(e.Resource.WorkspaceID), string(e.Resource.CreatorID),
		e.At, int64(e.PlacementGeneration),
		e.Usage.CPUTimeSeconds, e.Usage.MemoryByteSeconds, e.Usage.StorageBytes,
		e.Usage.NetworkBytes, e.Usage.AgentTokenCount)
	if err != nil {
		switch code, _ := constraintViolation(err); code {
		case sqlstateUniqueViolation:
			// The primary key: an event id is an identity, and the same one
			// twice is somebody already holding it, not a second fact.
			return control.ErrConflict
		case sqlstateForeignKeyViolation:
			// The workspace is the only foreign key an event row has.
			return control.ErrNotFound
		}
		return unavailable("record event", err)
	}
	return nil
}
