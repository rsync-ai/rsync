package healer

import (
	"strings"
	"testing"
)

// The healer's analysis prompt was the one place in this file that sent free-form
// text to an LLM without llmscrub. Everything it sends OUT to the user was already
// scrubbed (:630, :1112, :1222); the DDL it sends IN to the model was raw, which is
// the direction that leaves the platform.
//
// A DDL statement is not purely schema metadata. The identifiers are — the privacy
// contract allows table, column and type names verbatim — but a DEFAULT value or a
// CHECK bound is a customer's data sitting inside the statement. Scrub is exactly
// the right shape for this: it preserves the SQL structure and every identifier and
// rewrites only the quoted literals, so the model still sees the change it is being
// asked to classify.

const ddlWithLiteral = `ALTER TABLE public.users ADD COLUMN ssn text NOT NULL DEFAULT '078-05-1120'`

func scrubTestEvent(ddl string) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		EventType:  "schema_change",
		PipelineID: "pipe-1",
		SchemaChange: SchemaChange{
			ChangeType: "add_column",
			Database:   "app",
			Table:      "users",
			ColumnName: "ssn",
			ColumnType: "text",
			DDL:        ddl,
			RiskLevel:  "low",
		},
		Context: map[string]interface{}{
			"auto_apply_enabled":       false,
			"skip_destructive_enabled": true,
		},
	}
}

func TestBuildAnalysisPromptRedactsDDLLiterals(t *testing.T) {
	prompt := buildAnalysisPrompt(scrubTestEvent(ddlWithLiteral))

	if strings.Contains(prompt, "078-05-1120") {
		t.Errorf("the prompt carries the DEFAULT literal verbatim:\n%s", prompt)
	}
	if !strings.Contains(prompt, "[redacted]") {
		t.Errorf("the prompt shows no redaction marker, so the literal was never scrubbed:\n%s", prompt)
	}
}

func TestBuildAnalysisPromptKeepsTheSchemaMetadataTheModelClassifies(t *testing.T) {
	// The other half of the contract, and the reason this is Scrub rather than
	// dropping the DDL field: over-scrubbing would leave the model classifying a
	// statement it can no longer read. Identifiers are metadata and must survive.
	prompt := buildAnalysisPrompt(scrubTestEvent(ddlWithLiteral))

	for _, want := range []string{"ALTER TABLE", "public.users", "ADD COLUMN", "ssn", "text", "NOT NULL", "DEFAULT"} {
		if !strings.Contains(prompt, want) {
			t.Errorf("the prompt lost %q; the model cannot classify a statement it cannot read:\n%s", want, prompt)
		}
	}
}

func TestBuildAnalysisPromptLeavesALiteralFreeDDLAlone(t *testing.T) {
	const plain = `ALTER TABLE public.users ADD COLUMN nickname text`
	prompt := buildAnalysisPrompt(scrubTestEvent(plain))

	if !strings.Contains(prompt, plain) {
		t.Errorf("a DDL with no literal in it came back altered; want the statement verbatim:\n%s", prompt)
	}
	if strings.Contains(prompt, "[redacted]") {
		t.Errorf("a DDL with no literal in it produced a redaction marker:\n%s", prompt)
	}
}

// TestResolveProposedDDLRefusesAnEchoedRedaction is the second half of the fix, and
// the reason the scrub could not ship on its own. processAnalysis feeds the model's
// suggested_ddl to applyMigration once RSYNC_SCHEMA_DRIFT_AUTOAPPLY is on. Now that
// the model sees "[redacted]" in its input, a model that echoes its input back
// would have that marker EXECUTED against the customer's schema. Scrubbing without
// this guard is worse than not scrubbing.
func TestResolveProposedDDLRefusesAnEchoedRedaction(t *testing.T) {
	const source = ddlWithLiteral

	cases := []struct {
		name         string
		suggested    string
		wantDDL      string
		wantRedacted bool
	}{
		{
			name:      "a real suggestion is used",
			suggested: `ALTER TABLE public.users ADD COLUMN ssn text`,
			wantDDL:   `ALTER TABLE public.users ADD COLUMN ssn text`,
		},
		{
			name:      "no suggestion falls back to the source DDL",
			suggested: "",
			wantDDL:   source,
		},
		{
			name:         "a suggestion echoing the marker falls back to the source DDL",
			suggested:    `ALTER TABLE public.users ADD COLUMN ssn text NOT NULL DEFAULT '[redacted]'`,
			wantDDL:      source,
			wantRedacted: true,
		},
		{
			name:         "any marker counts, not just [redacted]",
			suggested:    `ALTER TABLE public.events ADD COLUMN contact text DEFAULT '[email-redacted]'`,
			wantDDL:      source,
			wantRedacted: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotDDL, gotRedacted := resolveProposedDDL(tc.suggested, source)
			if gotDDL != tc.wantDDL {
				t.Errorf("DDL = %q, want %q", gotDDL, tc.wantDDL)
			}
			if gotRedacted != tc.wantRedacted {
				t.Errorf("redacted flag = %v, want %v", gotRedacted, tc.wantRedacted)
			}
		})
	}
}
