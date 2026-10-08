//go:build performance

// Package testperf owns summary formatting for opt-in performance fixtures.
// It reports observations; ordinary correctness checks do not assert timings.
package testperf

import (
	"math"
	"sort"
	"testing"
)

// Report retains the first sample separately and never mutates caller samples.
// Metric names carry units (for example admission_ms or bytes_per_op).
func Report(t *testing.T, caseName, metric string, samples []float64) {
	t.Helper()
	if len(samples) == 0 {
		t.Fatal("performance report requires samples")
	}
	sorted := append([]float64(nil), samples...)
	for _, value := range sorted {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			t.Fatalf("invalid performance observation: %v", value)
		}
	}
	sort.Float64s(sorted)
	median := sorted[len(sorted)/2]
	if len(sorted)%2 == 0 {
		median = (sorted[len(sorted)/2-1] + median) / 2
	}
	t.Logf("PERFORMANCE_AUDIT case=%s metric=%s count=%d first=%.6f min=%.6f median=%.6f max=%.6f",
		caseName, metric, len(samples), samples[0], sorted[0], median, sorted[len(sorted)-1])
}
