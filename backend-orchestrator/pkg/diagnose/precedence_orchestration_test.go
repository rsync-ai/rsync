package diagnose

import "testing"

// The rule-based diagnoser was measured against production on 2026-08-01: of the
// 8 healer_decision events ever written, 7 carried the unknown/escalate/0.3
// fallback. Four distinct failure texts account for every failed execution on the
// instance, and only ONE of them matched a rule — and that one matched via
// ExecutorStatus, not via its error text.
//
// The four strings below are VERBATIM from prod (values scrubbed where they were
// customer data). They are the specification for the rules added alongside this
// file: a change that stops any of them from being classified is a regression of
// a measured, real-world gap, not a style change.
const (
	prodWorkflowGone = "This pipeline run is no longer active (workflow not found). Please run the pipeline again."

	prodZombieSwept = "zombie: execution timed out with no end_time (healer cleanup)"

	prodMaskColumnMissing = "Execution failed: activity error (type: ExecutorActivityV2, scheduledEventID: 279, " +
		"startedEventID: 280, identity: 1@f428396c7405@): [DETERMINISTIC:EXECUTION_FAILED] nl-transform planning " +
		"failed: requested mask column(s) [ssn] not found in source schema — refusing to run so PII is never " +
		"silently left unmasked (KI-NLCHAT-MASK-SILENT-NOOP) (type: V2_ACTIVITY_ERROR, retryable: false)"

	prodSilentDrop = "silent_drop_detected: public.customers: read=1703 landed=0; sink error: dest error: " +
		"invalid input syntax for type integer"
)

// TestProdFailureShapes_AllClassified is the headline assertion: none of the four
// observed production failure texts may land on the unknown/0.3 fallback.
func TestProdFailureShapes_AllClassified(t *testing.T) {
	d := New()

	cases := []struct {
		name       string
		sig        Signal
		wantCat    Category
		wantAction Action
		why        string
	}{
		{
			name:       "workflow gone",
			sig:        Signal{ErrorMessage: prodWorkflowGone},
			wantCat:    CategoryOrchestration,
			wantAction: ActionBackoffRetry,
			why:        "the most common prod failure; the run's workflow is gone and only a fresh run can help",
		},
		{
			name:       "zombie swept by the healer itself",
			sig:        Signal{ErrorMessage: prodZombieSwept},
			wantCat:    CategoryOrchestration,
			wantAction: ActionBackoffRetry,
			why:        "the healer must be able to read its own zombie-sweep marker",
		},
		{
			name:       "mask column missing from source schema",
			sig:        Signal{ErrorMessage: prodMaskColumnMissing},
			wantCat:    CategoryUserConfig,
			wantAction: ActionRequestUserConfig,
			why:        "the transform config names a column the source lacks; only a human can say which is wrong",
		},
		{
			name:       "silent drop",
			sig:        Signal{ErrorMessage: prodSilentDrop, ExecutorStatus: "silent_drop_detected", SourceRowCount: 1703, WrittenRows: 0},
			wantCat:    CategoryConnectorBug,
			wantAction: ActionRegenerateConnector,
			why:        "the one shape that already matched — locked so the new rules above it don't steal it",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := d.Diagnose(c.sig)
			if got.Category == CategoryUnknown && got.Confidence == 0.3 {
				t.Fatalf("fell through to the unknown/0.3 fallback: %s\nmessage: %q", c.why, c.sig.ErrorMessage)
			}
			if got.Category != c.wantCat {
				t.Errorf("Category = %q, want %q (%s)", got.Category, c.wantCat, c.why)
			}
			if got.SuggestedAction != c.wantAction {
				t.Errorf("SuggestedAction = %q, want %q (%s)", got.SuggestedAction, c.wantAction, c.why)
			}
		})
	}
}

// TestOrchestrationRules_StayInHITLBand locks the deliberate choice that the
// healer RECOMMENDS a re-run rather than starting one. BackoffRetryExecutor is
// the only non-HITLSafe executor; at >= AutoExecuteBand the Healer would POST
// the api-gateway run endpoint unattended. Nothing has authorised that.
func TestOrchestrationRules_StayInHITLBand(t *testing.T) {
	d := New()

	for _, msg := range []string{prodWorkflowGone, prodZombieSwept} {
		got := d.Diagnose(Signal{ErrorMessage: msg})
		if got.SuggestedAction != ActionBackoffRetry {
			t.Fatalf("precondition: %q did not route to backoff_retry (got %q)", msg, got.SuggestedAction)
		}
		if got.Confidence >= AutoExecuteBand {
			t.Errorf("confidence %.2f >= AutoExecuteBand %.2f for %q — this makes the healer start "+
				"pipeline runs unattended. Raising it is a policy decision, not a tuning tweak.",
				got.Confidence, AutoExecuteBand, msg)
		}
		if got.Confidence < 0.5 {
			t.Errorf("confidence %.2f < the HITL band (0.50) for %q — the recommendation would be "+
				"dropped instead of recorded", got.Confidence, msg)
		}
	}
}

// TestNonRetryable_NeverRetried is the safety net. The executor marks a failure
// non-retryable; no rule below that point may hand it to a retry loop just
// because the text happens to contain a transient-sounding phrase.
func TestNonRetryable_NeverRetried(t *testing.T) {
	d := New()

	cases := []struct {
		name string
		msg  string
	}{
		{"deterministic marker alone", "[DETERMINISTIC:EXECUTION_FAILED] transform planning failed"},
		{"retryable false alone", "activity error (type: V2_ACTIVITY_ERROR, retryable: false)"},
		{
			// The trap this rule exists for: a deterministic failure whose text
			// also contains a needle from the transient-network rule.
			name: "deterministic AND transient-sounding",
			msg:  "[DETERMINISTIC:EXECUTION_FAILED] upstream said context deadline exceeded (retryable: false)",
		},
		{
			name: "deterministic AND a deadlock wording",
			msg:  "non-retryable: deadlock detected while applying the plan",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := d.Diagnose(Signal{ErrorMessage: c.msg})
			if got.SuggestedAction == ActionBackoffRetry {
				t.Errorf("SuggestedAction = backoff_retry for a failure the executor declared "+
					"non-retryable — the retry can never succeed and burns the 3/24h budget.\nmessage: %q", c.msg)
			}
		})
	}
}

// TestSweptZombie_NotTreatedAsTransientNetwork guards the specific word collision
// that motivated the ordering: "execution timed out" (a hung run) vs "connection
// timed out" (an unreachable peer).
func TestSweptZombie_NotTreatedAsTransientNetwork(t *testing.T) {
	d := New()

	got := d.Diagnose(Signal{ErrorMessage: prodZombieSwept})
	if got.Category == CategoryNetwork {
		t.Errorf("a swept zombie was classified as %q — a stalled run is not an unreachable peer, "+
			"and the network rule's 0.85 confidence would make the healer re-run it unattended", got.Category)
	}
}

// TestMaskColumn_NotSchemaDrift locks the mask-column rule ABOVE schema-drift.
// Answering it with regenerate_connector would regenerate a connector that is
// already correct and leave the stale column name in the pipeline's config.
func TestMaskColumn_NotSchemaDrift(t *testing.T) {
	d := New()

	got := d.Diagnose(Signal{ErrorMessage: prodMaskColumnMissing})
	if got.SuggestedAction == ActionRegenerateConnector {
		t.Errorf("mask-column-missing routed to regenerate_connector; the connector is not the "+
			"broken thing here. Got %+v", got)
	}
	if got.Category == CategorySchemaDrift {
		t.Errorf("mask-column-missing classified as schema_drift; the source schema is fine, the "+
			"transform config is stale. Got %+v", got)
	}
}

// TestOrchestrationRules_DoNotOutrankSpecificCauses keeps the new rules from
// swallowing failures that have a real, more specific remedy. Each message below
// deliberately ALSO contains an orchestration needle.
func TestOrchestrationRules_DoNotOutrankSpecificCauses(t *testing.T) {
	d := New()

	cases := []struct {
		name       string
		msg        string
		wantAction Action
		why        string
	}{
		{
			name:       "auth error mentioning the workflow",
			msg:        "workflow not found after HTTP 401 Unauthorized: token expired",
			wantAction: ActionRefreshAuth,
			why:        "auth is fixable; a re-run with a dead token just fails again",
		},
		{
			name:       "CDC provisioning error mentioning the zombie marker",
			msg:        "zombie: execution timed out with no end_time (healer cleanup); publication does not exist",
			wantAction: ActionEscalate,
			why:        "CDC provisioning must escalate — CLAUDE.md's healer classification rule",
		},
		{
			name:       "rate limit mentioning the workflow",
			msg:        "pipeline run is no longer active (workflow not found): HTTP 429 rate limit",
			wantAction: ActionBackoffRetry,
			why:        "same action, but it must come from the rate-limit rule's higher confidence",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := d.Diagnose(Signal{ErrorMessage: c.msg})
			if got.SuggestedAction != c.wantAction {
				t.Errorf("SuggestedAction = %q, want %q (%s)", got.SuggestedAction, c.wantAction, c.why)
			}
			if c.name == "CDC provisioning error mentioning the zombie marker" && got.Category == CategoryOrchestration {
				t.Errorf("a CDC provisioning failure was classified as orchestration — it would be " +
					"recommended for re-run instead of escalated")
			}
		})
	}
}
