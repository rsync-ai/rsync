package handlers

import "testing"

// The pipelines list gives each row ONE line under the status badge. Two things
// compete for it: the schedule hint ("cron: 0 9 * * 1") and the last run's real
// error message. The frontend renders the hint whenever it is present and only
// falls back to the error when it is absent (PipelinesTable.tsx:297-315).
//
// So an unconditional hint silently hid the failure reason of every scheduled
// pipeline behind its own cron string — a pipeline that died with "zombie:
// execution timed out with no end_time (healer cleanup)" advertised "cron: 0 9 * * 1"
// on the one surface an operator scans to find broken pipelines.

func TestScheduleStatusHintSuppressedOnFailure(t *testing.T) {
	const cronSchedule = `{"status":"active","schedule_type":"cron","schedule_spec":{"cron":"0 9 * * 1"},"timezone":"UTC"}`

	// The regression: a failed run must yield the line to its error message.
	if got := scheduleStatusHint(cronSchedule, "failed"); got != "" {
		t.Errorf("failed pipeline must not show a schedule hint (it hides the error), got %q", got)
	}

	// ...without costing the hint everywhere it is genuinely the most useful thing
	// to say. Every non-failed status still gets it.
	for _, status := range []string{"scheduled", "passed", "running", "idle", "paused", "stopped", "waiting_for_user"} {
		if got := scheduleStatusHint(cronSchedule, status); got != "cron: 0 9 * * 1" {
			t.Errorf("status %q: want %q, got %q", status, "cron: 0 9 * * 1", got)
		}
	}
}

func TestScheduleStatusHintRendering(t *testing.T) {
	cases := []struct {
		name     string
		schedule string
		want     string
	}{
		{
			name:     "cron",
			schedule: `{"status":"active","schedule_type":"cron","schedule_spec":{"cron":"*/5 * * * *"},"timezone":"UTC"}`,
			want:     "cron: */5 * * * *",
		},
		{
			name:     "cron with non-UTC timezone is annotated",
			schedule: `{"status":"active","schedule_type":"cron","schedule_spec":{"cron":"0 9 * * 1"},"timezone":"Asia/Kolkata"}`,
			want:     "cron: 0 9 * * 1 (Asia/Kolkata)",
		},
		{
			name:     "interval in seconds",
			schedule: `{"status":"active","schedule_type":"interval","schedule_spec":{"every_seconds":45},"timezone":"UTC"}`,
			want:     "every 45s",
		},
		{
			name:     "interval rounds up to minutes",
			schedule: `{"status":"active","schedule_type":"interval","schedule_spec":{"every_seconds":900},"timezone":"UTC"}`,
			want:     "every 15m",
		},
		{
			name:     "interval rounds up to hours",
			schedule: `{"status":"active","schedule_type":"interval","schedule_spec":{"every_seconds":7200},"timezone":"UTC"}`,
			want:     "every 2h",
		},
		{
			name:     "interval rounds up to days",
			schedule: `{"status":"active","schedule_type":"interval","schedule_spec":{"every_seconds":172800},"timezone":"UTC"}`,
			want:     "every 2d",
		},
		{
			name:     "paused schedule contributes nothing",
			schedule: `{"status":"paused","schedule_type":"cron","schedule_spec":{"cron":"0 9 * * 1"},"timezone":"UTC"}`,
			want:     "",
		},
		{
			name:     "cron type with no cron string falls back to the generic label",
			schedule: `{"status":"active","schedule_type":"cron","schedule_spec":{},"timezone":"UTC"}`,
			want:     "Scheduled",
		},
		{
			name:     "malformed json contributes nothing rather than erroring",
			schedule: `{not json`,
			want:     "",
		},
		{
			name:     "empty schedule contributes nothing",
			schedule: ``,
			want:     "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// "scheduled" — the status that most wants a hint — isolates rendering
			// from the failure-suppression rule covered above.
			if got := scheduleStatusHint(tc.schedule, "scheduled"); got != tc.want {
				t.Errorf("want %q, got %q", tc.want, got)
			}
		})
	}
}
