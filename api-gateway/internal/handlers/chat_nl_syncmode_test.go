package handlers

import "testing"

// TestParseSyncModeOverrides pins the current behavior of parseSyncModeOverrides
// (chat_nl_pipeline.go:180). Previously ZERO tests covered this parser. It
// recognizes explicit UI tokens (sync_mode=/cdc_mode=) and a set of human
// fallbacks, then normalizes anything unrecognized to "".
//
// Note the streaming_only-vs-initial default: any bare CDC request that does not
// mention "streaming only" / "changes only" defaults cdcMode to "initial"
// (chat_nl_pipeline.go:201-207).
func TestParseSyncModeOverrides(t *testing.T) {
	cases := []struct {
		name     string
		message  string
		wantSync string
		wantCDC  string
	}{
		// ── explicit UI tokens ──────────────────────────────────────────────
		// A bare "sync_mode=cdc" carries no cdc_mode, so it defaults to "initial".
		{"explicit sync_mode=cdc defaults cdc to initial", "sync_mode=cdc", "cdc", "initial"},
		{"explicit sync_mode=batch", "sync_mode=batch", "batch", ""},
		{"explicit cdc_mode=streaming_only implies cdc", "cdc_mode=streaming_only", "cdc", "streaming_only"},
		{"explicit cdc_mode=initial implies cdc", "cdc_mode=initial", "cdc", "initial"},

		// ── human fallbacks → cdc ───────────────────────────────────────────
		{"human cdc", "cdc", "cdc", "initial"},
		{"human stream", "stream", "cdc", "initial"},
		{"human real-time", "real-time", "cdc", "initial"},
		{"human realtime", "realtime", "cdc", "initial"},

		// ── human fallbacks → batch ─────────────────────────────────────────
		{"human batch", "batch", "batch", ""},
		{"human one-time", "one-time", "batch", ""},
		{"human one time", "one time", "batch", ""},

		// ── streaming_only vs initial default for a bare cdc request ─────────
		{"cdc streaming only phrase", "cdc streaming only", "cdc", "streaming_only"},
		{"cdc changes only phrase", "cdc changes only", "cdc", "streaming_only"},

		// ── unrecognized / none ─────────────────────────────────────────────
		// sync_mode=turbo is captured then normalized away because it is neither
		// "cdc" nor "batch" (chat_nl_pipeline.go:212-214).
		{"unrecognized mode normalized to empty", "sync_mode=turbo", "", ""},
		{"no override keywords", "sync mysql to s3", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sync, cdc, _ := parseSyncModeOverrides(tc.message)
			if sync != tc.wantSync {
				t.Errorf("syncMode = %q, want %q", sync, tc.wantSync)
			}
			if cdc != tc.wantCDC {
				t.Errorf("cdcMode = %q, want %q", cdc, tc.wantCDC)
			}
		})
	}
}

// TestResolveConfirmationSyncMode pins the confirmation-time precedence
// (resolveConfirmationSyncMode). The most-explicit signal — an override token in
// the confirmation `message`, which the UI mode button posts — MUST win, even when
// pendingIntent.SyncMode already holds a concrete "batch"/"cdc" value.
//
// Regression guard for the Suite-C prod defect (BUG-CDC-1 recurrence): the intent
// classifier maps the verb "sync" → sync_mode="batch", so a request like
// "Sync <table> from <conn> to <conn>" arrived at confirmation with
// pendingSyncMode="batch". The user was then forced to explicitly click "CDC
// (snapshot + changes)" (Start is disabled until a mode is picked for a CDC-capable
// source), which posts "Yes sync_mode=cdc cdc_mode=initial" — yet the pipeline ran
// as batch because the old ordering read the inferred "batch" first and never
// re-parsed the explicit override.
func TestResolveConfirmationSyncMode(t *testing.T) {
	cases := []struct {
		name        string
		message     string
		pendingMode string
		originalReq string
		wantSync    string
		wantCDC     string
	}{
		// ── THE Suite-C regression: explicit CDC click beats inferred "batch" ──
		{
			name:        "explicit CDC click overrides LLM-inferred batch (BUG-CDC-1)",
			message:     "Yes sync_mode=cdc cdc_mode=initial",
			pendingMode: "batch", // intent classifier inferred this from the verb "sync"
			originalReq: "Sync zzuitest_cdc from test-mysql to pg-dest",
			wantSync:    "cdc",
			wantCDC:     "initial",
		},
		{
			name:        "explicit changes-only CDC click beats inferred batch",
			message:     "Yes sync_mode=cdc cdc_mode=streaming_only",
			pendingMode: "batch",
			originalReq: "Sync orders from mysql to postgres",
			wantSync:    "cdc",
			wantCDC:     "streaming_only",
		},
		// User can also flip the other way: explicit batch click beats inferred cdc.
		{
			name:        "explicit batch click overrides inferred cdc",
			message:     "Yes sync_mode=batch",
			pendingMode: "cdc",
			originalReq: "Stream mysql to postgres",
			wantSync:    "batch",
			wantCDC:     "",
		},

		// ── message is silent → fall back to pendingIntent.SyncMode ──
		{
			name:        "synthetic yes falls back to concrete pending batch",
			message:     "yes",
			pendingMode: "batch",
			originalReq: "load mysql to s3",
			wantSync:    "batch",
			wantCDC:     "",
		},
		{
			// When the mode comes from the pending fallback (not via
			// parseSyncModeOverrides), cdc_mode is left empty here — handleConfirmation
			// applies the "initial" default after this function returns.
			name:        "synthetic yes falls back to concrete pending cdc",
			message:     "yes",
			pendingMode: "cdc",
			originalReq: "replicate mysql to postgres",
			wantSync:    "cdc",
			wantCDC:     "",
		},

		// ── message silent AND pending is "auto"/"" → parse the ORIGINAL request ──
		{
			name:        "auto pending re-parses original request for stream keyword",
			message:     "yes",
			pendingMode: "auto",
			originalReq: "stream mysql to postgres",
			wantSync:    "cdc",
			wantCDC:     "initial",
		},
		{
			name:        "empty pending re-parses original request for batch keyword",
			message:     "yes",
			pendingMode: "",
			originalReq: "batch copy mysql to s3",
			wantSync:    "batch",
			wantCDC:     "",
		},
		{
			name:        "nothing specifies a mode → empty (caller falls back to conn default/batch)",
			message:     "yes",
			pendingMode: "auto",
			originalReq: "mysql to postgres",
			wantSync:    "",
			wantCDC:     "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sync, cdc, _ := resolveConfirmationSyncMode(tc.message, tc.pendingMode, tc.originalReq)
			if sync != tc.wantSync {
				t.Errorf("syncMode = %q, want %q", sync, tc.wantSync)
			}
			if cdc != tc.wantCDC {
				t.Errorf("cdcMode = %q, want %q", cdc, tc.wantCDC)
			}
		})
	}
}
