package workflows

import "testing"

// TestNormalizeTerminalProgress guards the fix for a prod defect found on 2026-08-01: a
// finished pipeline run persisted `completed | 88% | 7 of 8` and stayed there forever.
// deriveProgressFromExecutionPlan reports the plan as it stood when the event fired, which
// for the final event is routinely one step short (7/8 rounds to 88).
//
// api-gateway's event projector already normalized this, but that writer is explicitly
// best-effort and loses — StateUpdateActivity is the AUTHORITATIVE writer and its
// ON CONFLICT copies EXCLUDED.progress_* verbatim, overwriting the projector's corrected
// values. Hence the clamp has to live here too. (Reading schema_version on the row tells
// you which writer wrote last: the authoritative one hardcodes 2.)
func TestNormalizeTerminalProgress(t *testing.T) {
	tests := []struct {
		name        string
		status      string
		percent     int
		currentStep int
		totalSteps  int
		wantPercent int
		wantStep    int
	}{
		{
			name:   "the prod repro: completed at 88% on step 7 of 8",
			status: "completed", percent: 88, currentStep: 7, totalSteps: 8,
			wantPercent: 100, wantStep: 8,
		},
		{
			name:   "completed already at 100 stays put",
			status: "completed", percent: 100, currentStep: 8, totalSteps: 8,
			wantPercent: 100, wantStep: 8,
		},
		{
			name:   "completed with the default 7-step plan",
			status: "completed", percent: 71, currentStep: 5, totalSteps: 7,
			wantPercent: 100, wantStep: 7,
		},
		{
			name:   "status comparison is case-insensitive",
			status: "COMPLETED", percent: 88, currentStep: 7, totalSteps: 8,
			wantPercent: 100, wantStep: 8,
		},
		{
			name:   "status comparison ignores surrounding whitespace",
			status: "  Completed  ", percent: 50, currentStep: 4, totalSteps: 8,
			wantPercent: 100, wantStep: 8,
		},
		{
			// A run in flight must keep its real progress or the bar would jump to 100
			// and then back down.
			name:   "running passes through untouched",
			status: "running", percent: 38, currentStep: 3, totalSteps: 8,
			wantPercent: 38, wantStep: 3,
		},
		{
			// Partial progress on a failure IS the diagnostic — it says how far the run
			// got before it died. Never clamp it.
			name:   "failed keeps the partial progress it reached",
			status: "failed", percent: 38, currentStep: 3, totalSteps: 8,
			wantPercent: 38, wantStep: 3,
		},
		{
			name:   "cancelled keeps the partial progress it reached",
			status: "cancelled", percent: 12, currentStep: 1, totalSteps: 8,
			wantPercent: 12, wantStep: 1,
		},
		{
			name:   "needs_input is not terminal",
			status: "needs_input", percent: 25, currentStep: 2, totalSteps: 8,
			wantPercent: 25, wantStep: 2,
		},
		{
			// Unknown total: still finish the bar, but leave the step alone rather than
			// writing a nonsense 0.
			name:   "completed with no known total steps",
			status: "completed", percent: 88, currentStep: 7, totalSteps: 0,
			wantPercent: 100, wantStep: 7,
		},
		{
			name:   "empty status is not terminal",
			status: "", percent: 88, currentStep: 7, totalSteps: 8,
			wantPercent: 88, wantStep: 7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPercent, gotStep := normalizeTerminalProgress(tt.status, tt.percent, tt.currentStep, tt.totalSteps)
			if gotPercent != tt.wantPercent || gotStep != tt.wantStep {
				t.Errorf("normalizeTerminalProgress(%q, %d, %d, %d) = (%d, %d), want (%d, %d)",
					tt.status, tt.percent, tt.currentStep, tt.totalSteps,
					gotPercent, gotStep, tt.wantPercent, tt.wantStep)
			}
		})
	}
}
