package diagnose

import "testing"

// TestMongoCDCMaskBlock_Escalates locks the RuleBasedDiagnoser rule for the
// executor's MongoDB CDC mask block (KI-MONGO-CDC-MASK-SILENT-NOOP). The refusal
// is a privacy guard: a retry refuses identically and only a human can remove the
// mask or switch to batch, so it must escalate. The messages carry user-named
// columns and a wrapped DB failure, so they deliberately contain auth, transient
// and mask-column needles that must NOT win.
func TestMongoCDCMaskBlock_Escalates(t *testing.T) {
	d := New()
	msgs := []string{
		// Block wording (executor mongoCDCMaskBlockError), columns named like other rules' needles.
		"MongoDB CDC/streaming cannot apply masking yet: 2 enabled mask transform(s) on [email token expired] would land unmasked, " +
			"because each document is written as one packed field. Refusing to run (KI-MONGO-CDC-MASK-SILENT-NOOP). " +
			"Remove the mask or run this pipeline in batch mode",
		// Fail-closed wording on a DB error.
		"MongoDB CDC run refused: could not load the pipeline's transforms, so a mask cannot be ruled out (KI-MONGO-CDC-MASK-SILENT-NOOP)",
		// As the temporal-adapter wraps it, with a transient needle appended.
		"failed: MongoDB CDC run refused: could not read a transform row, so a mask cannot be ruled out " +
			"(KI-MONGO-CDC-MASK-SILENT-NOOP); connection reset by peer (type: V2_ACTIVITY_ERROR, retryable: false)",
	}
	for _, msg := range msgs {
		got := d.Diagnose(Signal{ErrorMessage: msg, ExecutorStatus: "failed"})
		if got.SuggestedAction != ActionEscalate {
			t.Errorf("msg=%q: want %s, got %s (%+v)", msg, ActionEscalate, got.SuggestedAction, got)
		}
		if got.Category != CategoryUserConfig {
			t.Errorf("msg=%q: want category %s, got %s", msg, CategoryUserConfig, got.Category)
		}
		if got.Confidence < 0.85 {
			t.Errorf("msg=%q: confidence %.2f is below the band that marks this rule as certain", msg, got.Confidence)
		}
	}
}
