package executor

import (
	"testing"
	"time"
)

// A batch copy must report live row counts by default: before this, live
// DATA_PLANE_METRICS were opt-in and the Monitoring overview read "Rows 0" for
// the whole copy.
func TestLiveBatchMetricsDue(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		env  string
		last time.Time
		want bool
	}{
		{"default first batch emits", "", time.Time{}, true},
		{"default within interval is throttled", "", now.Add(-2 * time.Second), false},
		{"default after interval emits", "", now.Add(-liveBatchMetricsInterval), true},
		{"true emits every batch", "true", now.Add(-time.Millisecond), true},
		{"false disables", "false", time.Time{}, false},
		{"FALSE disables case-insensitively", " FALSE ", time.Time{}, false},
	}
	for _, tc := range cases {
		if got := liveBatchMetricsDue(tc.env, tc.last, now); got != tc.want {
			t.Errorf("%s: liveBatchMetricsDue(%q) = %v, want %v", tc.name, tc.env, got, tc.want)
		}
	}
}
