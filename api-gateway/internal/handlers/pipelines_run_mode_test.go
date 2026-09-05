package handlers

import "testing"

func strptr(s string) *string { return &s }

// TestResolveRunModePrecedence pins the four-rung precedence. The third rung —
// the pipeline's stored default — is the one that matters: it is what turns an
// unattended retry into a destructive reload when the caller supplies nothing.
func TestResolveRunModePrecedence(t *testing.T) {
	tests := []struct {
		name           string
		queryOverride  string
		bodyRunMode    string
		defaultRunMode *string
		want           string
	}{
		{"nothing supplied falls back to resume", "", "", nil, "resume"},
		{"empty stored default is not a value", "", "", strptr(""), "resume"},
		{"stored default wins when no override", "", "", strptr("reload"), "reload"},
		{"body override beats the stored default", "", "resume", strptr("reload"), "resume"},
		{"query override beats the body", "reload", "resume", nil, "reload"},
		{"query override beats body AND stored default", "resume", "reload", strptr("reload"), "resume"},
		{"body is consulted only when query is blank", "   ", "reload", nil, "reload"},
		{"overrides are case- and space-insensitive", "  RELOAD  ", "", nil, "reload"},
		{"body override is normalised too", "", "\tResume\n", strptr("reload"), "resume"},
		// An unrecognised override is ignored rather than passed through, so a
		// typo'd ?run_mode=relaod cannot reach the executor as a mode it does
		// not understand — nor can it accidentally mean "reload".
		{"unknown override falls through to the stored default", "relaod", "", strptr("reload"), "reload"},
		{"unknown override falls through to resume", "wipe-everything", "", nil, "resume"},
		{"unknown query override does not shadow a valid body value", "relaod", "reload", nil, "resume"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveRunMode(tt.queryOverride, tt.bodyRunMode, tt.defaultRunMode)
			if got != tt.want {
				t.Errorf("resolveRunMode(%q, %q, %v) = %q, want %q",
					tt.queryOverride, tt.bodyRunMode, safeDeref(tt.defaultRunMode), got, tt.want)
			}
		})
	}
}

// TestResolveRunModeUnattendedHealerPathIsNeverDestructive is the regression
// guard for the fix in RunPipelineInternal.
//
// The healer POSTs {triggered_by, reason} — neither maps onto
// RunPipelineRequest — so before the fix the enqueue ran with a zero-valued
// RunMode and inherited the pipeline's default_run_mode. On a pipeline whose
// owner had chosen "Reload" that meant an unattended full destination wipe
// (DeleteCheckpoints, then delete_prefix / DROP TABLE ... CASCADE per table)
// firing seconds after any transient network blip, up to 3×/24h.
//
// The first case below is the defect: it asserts the old behavior still follows
// from an empty request, which is precisely why the caller must pin the mode.
// The second is the fix.
func TestResolveRunModeUnattendedHealerPathIsNeverDestructive(t *testing.T) {
	reloadPipeline := strptr("reload")

	// What the healer used to send: nothing.
	if got := resolveRunMode("", "", reloadPipeline); got != "reload" {
		t.Fatalf("an empty request on a reload pipeline = %q, want %q — "+
			"if this changed, the pin in RunPipelineInternal may no longer be load-bearing "+
			"and its comment needs revisiting", got, "reload")
	}

	// What RunPipelineInternal sends now: RunPipelineRequest{RunMode: "resume"}.
	if got := resolveRunMode("", "resume", reloadPipeline); got != "resume" {
		t.Errorf("the pinned healer request on a reload pipeline = %q, want resume — "+
			"the unattended retry path is destructive again", got)
	}

	// The pin must hold for every stored default, not just "reload".
	for _, def := range []*string{nil, strptr(""), strptr("resume"), strptr("reload")} {
		if got := resolveRunMode("", "resume", def); got != "resume" {
			t.Errorf("pinned request with default_run_mode=%q = %q, want resume",
				safeDeref(def), got)
		}
	}
}

// The user-facing route still honours an explicit reload — the fix narrowed the
// unattended path only. If this ever fails, the manual "Reload" button is broken.
func TestResolveRunModeExplicitReloadStillWorks(t *testing.T) {
	if got := resolveRunMode("reload", "", nil); got != "reload" {
		t.Errorf("explicit ?run_mode=reload = %q, want reload", got)
	}
	if got := resolveRunMode("", "reload", nil); got != "reload" {
		t.Errorf("explicit body run_mode=reload = %q, want reload", got)
	}
}
