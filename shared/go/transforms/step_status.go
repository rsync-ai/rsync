package transforms

// Status values recorded in transform_execution_logs.status.
const (
	StepStatusSuccess = "success"
	StepStatusFailed  = "failed"
	// StepStatusEmptyOutput marks a step that consumed rows and emitted none.
	//
	// It is deliberately NOT "failed". A filter that legitimately matches nothing
	// is a correct, successful step, and calling it a failure would train people
	// to ignore the field. But it is not "success" either: every silent
	// data-destroying defect in this engine -- a filter on a renamed column, a
	// required column no row carries, a typo in a select_columns list -- landed
	// in the logs as a green row with output_rows = 0, so the run summary the
	// operator actually looks at reported a healthy execution over an empty
	// table. The fixes elsewhere in this package stop those from emptying a
	// batch; this stops the NEXT one from doing it invisibly.
	//
	// The UI's badge mapper falls through to a neutral variant for anything it
	// does not recognise, and the monitoring aggregate counts only 'failed', so
	// this reads as "look at this" without inflating any failure metric.
	StepStatusEmptyOutput = "empty_output"
)

// StepStatus classifies one transform step for transform_execution_logs.
//
// It lives here, not at the two call sites, because the batch executor and the
// CDC sink worker each write that table from their own copy of the same logic,
// and a classification that disagrees between the two paths is worse than
// either one being wrong.
func StepStatus(inputRows, outputRows int, stepErr error) string {
	switch {
	case stepErr != nil:
		return StepStatusFailed
	case inputRows > 0 && outputRows == 0:
		return StepStatusEmptyOutput
	default:
		return StepStatusSuccess
	}
}
