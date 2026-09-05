package main

// Coverage for the configurable per-destination-write HTTP timeout knob
// (RSYNC_SINK_HTTP_TIMEOUT_SECONDS). Default is 120s (unchanged); clamped to
// [10s, 600s] so a typo can't set an absurd value.

import (
	"testing"
	"time"
)

func TestDestHTTPTimeout(t *testing.T) {
	cases := []struct {
		set  string
		want time.Duration
	}{
		{"", 120 * time.Second},     // env unset → default unchanged
		{"30", 30 * time.Second},    // operator-tightened
		{"300", 300 * time.Second},  // operator-loosened for a slow dest
		{"5", 10 * time.Second},     // clamp low
		{"0", 10 * time.Second},     // clamp low
		{"9999", 600 * time.Second}, // clamp high
		{"abc", 120 * time.Second},  // unparseable → default
	}
	for _, c := range cases {
		if c.set == "" {
			t.Setenv("RSYNC_SINK_HTTP_TIMEOUT_SECONDS", "")
		} else {
			t.Setenv("RSYNC_SINK_HTTP_TIMEOUT_SECONDS", c.set)
		}
		if got := destHTTPTimeout(); got != c.want {
			t.Errorf("RSYNC_SINK_HTTP_TIMEOUT_SECONDS=%q: got %s want %s", c.set, got, c.want)
		}
	}
}
