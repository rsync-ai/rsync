package handlers

import "testing"

func TestParseScheduleIntent(t *testing.T) {
	cases := []struct {
		message  string
		wantType string
		wantCron string
		wantSecs int
		wantOK   bool
	}{
		{"sync postgres orders to s3 every day at 2am", "cron", "0 2 * * *", 0, true},
		{"copy mysql to bigquery daily at 14:30", "cron", "30 14 * * *", 0, true},
		{"run this every hour", "cron", "0 * * * *", 0, true},
		{"sync weekly", "cron", "0 0 * * 0", 0, true},
		{"replicate monthly", "cron", "0 0 1 * *", 0, true},
		{"pull every 30 minutes", "interval", "", 1800, true},
		{"poll every 2 hours", "interval", "", 7200, true},
		{"run at 9pm", "cron", "0 21 * * *", 0, true},
		{"sync postgres to s3 in real-time", "", "", 0, false},
		{"mask email and phone", "", "", 0, false},
		// KI-NLCHAT-SCHEDULE-DOW: a named weekday must pin the cron day-of-week
		// field instead of degrading to a daily run.
		{"sync mysql to postgres every monday at 9am", "cron", "0 9 * * 1", 0, true},
		{"replicate every friday", "cron", "0 0 * * 5", 0, true},
		{"backup on weekdays at 6am", "cron", "0 6 * * 1-5", 0, true},
		{"run every weekend", "cron", "0 0 * * 0,6", 0, true},
		{"sync monday and friday at 3pm", "cron", "0 15 * * 1,5", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.message, func(t *testing.T) {
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
