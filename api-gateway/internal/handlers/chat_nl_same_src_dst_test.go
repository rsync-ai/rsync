package handlers

import (
	"strings"
	"testing"
)

// TestSameConnectionError locks KI-NLCHAT-SAME-SRC-DST (handoff item D): when the
// resolved source and destination are the SAME connection, the confirmation must
// return a self-replication error; when they differ or either id is unresolved
// (HITL-deferral), it must not fire.
func TestSameConnectionError(t *testing.T) {
	cases := []struct {
		name    string
		src     string
		dst     string
		wantErr bool
	}{
		{"same connection → error", "conn-1", "conn-1", true},
		{"different connections → allow", "conn-1", "conn-2", false},
		{"source unresolved → defer to HITL", "", "conn-2", false},
		{"dest unresolved → defer to HITL", "conn-1", "", false},
		{"both unresolved → defer to HITL", "", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := sameConnectionError(c.src, c.dst, "trace-xyz")
			if c.wantErr {
				if got == nil {
					t.Fatalf("expected a same-connection error, got nil")
				}
				if got.Type != "error" {
					t.Errorf("Type = %q, want \"error\"", got.Type)
				}
				if !strings.Contains(strings.ToLower(got.Message), "same connection") {
					t.Errorf("message should explain the self-replication, got: %q", got.Message)
				}
				if got.TraceID != "trace-xyz" {
					t.Errorf("TraceID = %q, want the passed-in trace", got.TraceID)
				}
			} else if got != nil {
				t.Fatalf("expected nil (no self-replication), got: %+v", got)
			}
		})
	}
}
