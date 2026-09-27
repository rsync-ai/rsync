package transforms

import (
	"errors"
	"testing"
)

func TestStepStatusNamesAnEmptiedBatchInsteadOfCallingItSuccess(t *testing.T) {
	cases := []struct {
		name    string
		in, out int
		err     error
		want    string
	}{
		{"error wins over everything", 10, 0, errors.New("boom"), StepStatusFailed},
		{"error with rows out is still failed", 10, 10, errors.New("boom"), StepStatusFailed},
		{"consumed rows, emitted none", 10, 0, nil, StepStatusEmptyOutput},
		{"consumed rows, emitted some", 10, 4, nil, StepStatusSuccess},
		{"consumed rows, emitted all", 10, 10, nil, StepStatusSuccess},
		// An empty input is not evidence of anything: a step that receives no
		// rows and emits none did nothing wrong, and flagging it would fire on
		// every idle CDC batch.
		{"empty in, empty out", 0, 0, nil, StepStatusSuccess},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := StepStatus(tc.in, tc.out, tc.err); got != tc.want {
				t.Fatalf("StepStatus(%d, %d, %v) = %q, want %q", tc.in, tc.out, tc.err, got, tc.want)
			}
		})
	}
}

// The value the UI and the monitoring aggregate depend on. 'failed' must stay
// exactly 'failed' or transform_monitoring.go's COUNT(*) FILTER stops matching,
// and the new status must not BE 'failed' or it inflates that count.
func TestStepStatusValuesAreTheOnesTheQueriesUse(t *testing.T) {
	if StepStatusFailed != "failed" || StepStatusSuccess != "success" {
		t.Fatalf("transform_monitoring.go filters on these literals: %q %q", StepStatusFailed, StepStatusSuccess)
	}
	if StepStatusEmptyOutput == StepStatusFailed || StepStatusEmptyOutput == StepStatusSuccess {
		t.Fatalf("empty_output must be distinguishable from both, got %q", StepStatusEmptyOutput)
	}
}
