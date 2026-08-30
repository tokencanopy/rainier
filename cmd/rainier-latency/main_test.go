package main

import (
	"math"
	"testing"
	"time"
)

func TestSummarizeReportsCenterSpreadAndInterpolatedTail(t *testing.T) {
	values := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
		100 * time.Millisecond,
	}
	original := append([]time.Duration(nil), values...)

	got := summarize("terminal_rtt", values, 75*time.Millisecond)
	if got.Metric != "terminal_rtt" || got.N != 5 {
		t.Fatalf("summary identity = %+v, want terminal_rtt n=5", got)
	}
	checks := map[string]struct {
		got, want float64
	}{
		"mean": {got.MeanMS, 40},
		"p25":  {got.P25MS, 20},
		"p50":  {got.P50MS, 30},
		"p75":  {got.P75MS, 40},
		"p95":  {got.P95MS, 88}, // R-7: 40 + 0.8*(100-40)
		"min":  {got.MinMS, 10},
		"max":  {got.MaxMS, 100},
	}
	for name, check := range checks {
		if math.Abs(check.got-check.want) > 0.001 {
			t.Errorf("%s = %.3fms, want %.3fms", name, check.got, check.want)
		}
	}
	if got.TargetP95MS != 75 || got.Pass == nil || *got.Pass {
		t.Fatalf("target result = target %.1f pass %v, want 75 false", got.TargetP95MS, got.Pass)
	}

	// summarize must not mutate observations; callers reuse their chronological
	// order when writing raw records before the summaries.
	for i, want := range original {
		if values[i] != want {
			t.Fatalf("values mutated at %d: got %s want %s", i, values[i], want)
		}
	}
}

func TestSummarizeEmptyMetric(t *testing.T) {
	got := summarize("cold_resume_to_usable", nil, 0)
	if got.Metric != "cold_resume_to_usable" || got.N != 0 || got.Pass != nil {
		t.Fatalf("empty summary = %+v, want named n=0 non-passing summary", got)
	}
}
