package handlers

import (
	"testing"
	"time"
)

// TestRequestedSyncModeForConfirmation pins issue #13: a request that already
// says which sync mode it wants must reach the confirmation card as a
// pre-selection, so the chat does not ask "Choose Sync Mode" again. A request
// that names no mode must still yield "" (the card keeps forcing a pick).
func TestRequestedSyncModeForConfirmation(t *testing.T) {
	cases := []struct {
		name, source, request string
		wantSync, wantCDC     string
	}{
		{"cdc snapshot + streaming", "mongodb",
			"Create a pipeline from MongoDB to GCS using CDC with snapshot + streaming", "cdc", "initial"},
		{"cdc changes only", "mongodb", "stream mongodb to gcs, changes only", "cdc", "streaming_only"},
		{"explicit token", "postgresql", "sync orders sync_mode=cdc cdc_mode=streaming_only", "cdc", "streaming_only"},
		{"batch requested", "mongodb", "one-time batch copy from mongodb to gcs", "batch", ""},
		{"no mode named", "mysql", "sync mysql to postgresql", "", ""},
		{"empty request", "mysql", "", "", ""},
		{"cdc for a source without cdc falls back to a pick", "shopify", "stream shopify to s3 with cdc", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotSync, gotCDC := requestedSyncModeForConfirmation(tc.source, tc.request)
			if gotSync != tc.wantSync || gotCDC != tc.wantCDC {
				t.Fatalf("requestedSyncModeForConfirmation(%q, %q) = (%q, %q), want (%q, %q)",
					tc.source, tc.request, gotSync, gotCDC, tc.wantSync, tc.wantCDC)
			}
		})
	}
}

// TestChatPipelineName pins issue #11: the generated name uses the browser's
// time zone, and anything unusable falls back to UTC.
func TestChatPipelineName(t *testing.T) {
	now := time.Date(2026, 9, 16, 18, 46, 58, 0, time.UTC)
	cases := []struct{ tz, want string }{
		{"Europe/Berlin", "Chat Pipeline 20:46:58"}, // CEST, GMT+2
		{"Asia/Kolkata", "Chat Pipeline 00:16:58"},
		{"", "Chat Pipeline 18:46:58"},
		{"Not/AZone", "Chat Pipeline 18:46:58"},
		{"Local", "Chat Pipeline 18:46:58"},
		{"../../etc/passwd", "Chat Pipeline 18:46:58"},
	}
	for _, tc := range cases {
		if got := chatPipelineName(now, tc.tz); got != tc.want {
			t.Errorf("chatPipelineName(tz=%q) = %q, want %q", tc.tz, got, tc.want)
		}
	}
}
