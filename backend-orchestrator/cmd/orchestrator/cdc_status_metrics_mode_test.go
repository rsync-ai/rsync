package main

import "testing"

// A batch pipeline must never receive "CDC data plane metrics update" events
// from a CDC status poll.
func TestCDCStatusMetricsApplyToSyncMode(t *testing.T) {
	cases := map[string]bool{
		"batch":     false,
		" BATCH ":   false,
		"cdc":       true,
		"":          true, // legacy row: mode carried by cdc_mode
		"streaming": true,
	}
	for mode, want := range cases {
		if got := cdcStatusMetricsApplyToSyncMode(mode); got != want {
			t.Errorf("cdcStatusMetricsApplyToSyncMode(%q) = %v, want %v", mode, got, want)
		}
	}
}
