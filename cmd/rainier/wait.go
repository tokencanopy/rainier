package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/tokencanopy/rainier/internal/cli"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// safeSessionID admits only an exact id that can be copied into both an API
// path and an unquoted shell argument. Never repair an unsafe id into a new id.
func safeSessionID(id string) bool {
	if !strings.HasPrefix(id, "sess_") || len(id) <= 5 || len(id) > 128 {
		return false
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

// initialAttachGuidance is called only after initial 503 retries expire. A 503
// alone is not evidence of absent runners: establish session state before
// consulting fleet capacity. At most two GETs share a five-second budget.
func initialAttachGuidance(ctx context.Context, cfg cli.Config, id string, since uint64) error {
	if !safeSessionID(id) {
		return fmt.Errorf("attach remained unavailable; no session was removed. Run rainier ls to find its id, then rainier attach <id>. Check rainier doctor")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	c := cli.NewClient(cfg)
	c.HTTP = readinessHTTP()
	var resp sessionEnvelope
	err := readinessGET(ctx, c, "/v0/sessions/"+id, &resp)
	explanation := "attach remained unavailable (503); cause is not established"
	if err != nil {
		explanation += "; " + readinessError(err)
	} else if resp.Session.ID != id || resp.Session.State == "" {
		explanation += "; " + readinessError(errMalformedReadiness)
	} else {
		row := resp.Session
		switch row.State {
		case "queued", "creating":
			explanation = "session is " + row.State + " and not ready to attach"
			if row.QueueReason != "" {
				// Queue text is server-owned prose. Redact known credentials before
				// stripping terminal controls or truncating it; never render row.Error.
				explanation += "; server queue reason: " + diagnosticText(cfg, row.QueueReason, c.Token, c.RefreshToken)
			} else {
				runners, probeErr := fetchReadinessRunners(ctx, c)
				if probeErr != nil {
					explanation += "; runner readiness unknown: " + readinessError(probeErr)
				} else if ready, evidence := runnerReadiness(runners); !ready {
					explanation += "; " + evidence + "; ask your administrator to check runner availability/capacity"
				} else {
					explanation += "; runners report free capacity, but the placement/startup cause is not established"
				}
			}
		}
	}
	command := "rainier attach " + id
	if since == terminal.SinceAll {
		command += " --since 0"
	} else if since != 0 {
		command += fmt.Sprintf(" --since %d", since)
	}
	return fmt.Errorf("%s. No session was removed.\nReattach: %s\nCheck readiness: rainier doctor", explanation, command)
}
