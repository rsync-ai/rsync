package handlers

import "testing"

// TestParseScheduleClockBoundaries pins the wall-clock boundary handling of
// parseScheduleClock (chat_nl_pipeline.go:229). The scheduleTimeRe requires the
// literal "at " prefix followed immediately by a digit (chat_nl_pipeline.go:222),
// and 12am/12pm normalization plus the 0-23 / 0-59 range guard live at
// chat_nl_pipeline.go:238-250.
func TestParseScheduleClockBoundaries(t *testing.T) {
	cases := []struct {
		name     string
		lc       string
		wantHour int
		wantMin  int
		wantOK   bool
	}{
		{"12am → hour 0", "at 12am", 0, 0, true},
		{"12pm → hour 12", "at 12pm", 12, 0, true},
		{"2:30 pm → 14:30", "at 2:30 pm", 14, 30, true},
		{"hour out of range 25:00 → not ok", "at 25:00", 0, 0, false},
		{"minute out of range 12:75 → not ok", "at 12:75", 0, 0, false},
		{"no clock phrase → not ok", "sync daily", 0, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, mn, ok := parseScheduleClock(tc.lc)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if h != tc.wantHour || mn != tc.wantMin {
				t.Errorf("hour,min = %d,%d want %d,%d", h, mn, tc.wantHour, tc.wantMin)
			}
		})
	}
}

// TestParseScheduleIntentBoundaries covers cadence boundaries in
// parseScheduleIntent (chat_nl_pipeline.go:259) not exercised by the existing
// suite, and pins several silently-lossy edges as regression baselines.
func TestParseScheduleIntentBoundaries(t *testing.T) {
	cases := []struct {
		name     string
		message  string
		wantType string
		wantCron string
		wantSecs int
		wantOK   bool
	}{
		{"weekly at 9am", "weekly at 9am", "cron", "0 9 * * 0", 0, true},

		{"every 5 minutes → 300s interval", "every 5 minutes", "interval", "", 300, true},

		// FIXED (#7): scheduleTimeRe now matches a bare "at :30", so the hourly
		// cadence honors the minute → "30 * * * *".
		{"hourly at :30 honors the minute", "hourly at :30", "cron", "30 * * * *", 0, true},

		// FIXED (#7): sub-minute cadences are recognized and floored to the 60s
		// scheduler minimum instead of being silently unscheduled.
		{"every 30 seconds → floored to 60s interval", "every 30 seconds", "interval", "", 60, true},

		// FIXED (#7): a pasted 5-field cron literal is now honored.
		{"literal cron string honored", "on cron 0 3 * * 1", "cron", "0 3 * * 1", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			typ, spec, ok := parseScheduleIntent(tc.message)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if typ != tc.wantType {
				t.Errorf("type = %q, want %q", typ, tc.wantType)
			}
			if tc.wantType == "cron" && spec.Cron != tc.wantCron {
				t.Errorf("cron = %q, want %q", spec.Cron, tc.wantCron)
			}
			if tc.wantType == "interval" && spec.EverySeconds != tc.wantSecs {
				t.Errorf("every_seconds = %d, want %d", spec.EverySeconds, tc.wantSecs)
			}
		})
	}
}
