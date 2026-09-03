// v0wire/runners.go
package v0wire

import (
	"time"

	"github.com/tokencanopy/rainier/control"
)

// RunnerView is the client-facing rendering of a Runner: what the fleet
// listing promises about one runner, and nothing about how it was reached. No
// field is omitempty, on the same terms as every other view here.
type RunnerView struct {
	Name          string `json:"name"`
	Connected     bool   `json:"connected"`
	CapacityUsed  int    `json:"capacity_used"`
	CapacityTotal int    `json:"capacity_total"`
	LastSeenAt    string `json:"last_seen_at"`
}

type RunnersEnvelope struct {
	Runners []RunnerView `json:"runners"`
}

// RenderRunner renders r as its client-facing view. connected is the caller's
// answer rather than the row's: whether a runner has a control connection is a
// live fact about one replica, and a host that knows better than the stored
// column says so here.
func RenderRunner(r control.Runner, connected bool) RunnerView {
	return RunnerView{
		Name:          string(r.ID),
		Connected:     connected,
		CapacityUsed:  r.CapacityUsed,
		CapacityTotal: r.CapacityTotal,
		LastSeenAt:    r.LastSeenAt.UTC().Format(time.RFC3339),
	}
}
