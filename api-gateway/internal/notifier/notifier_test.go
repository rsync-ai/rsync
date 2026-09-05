package notifier

import (
	"encoding/json"
	"strings"
	"testing"

	"api-gateway/internal/slack"
)

// TestSlackPayload_InteractiveDriftButtons pins that an actionable schema-drift
// alert (with interactivity configured) carries real Approve/Reject buttons —
// action_ids matching the inbound receiver, value = pipeline id — plus the
// absolute "View in rsync-ai" link. Every other case keeps the legacy link-only
// attachment.
func TestSlackPayload_InteractiveDriftButtons(t *testing.T) {
	p := notificationPayload{
		Type:       "structured_error_notification",
		PipelineID: "pl-9",
		Message:    "A column was added",
		ActionURL:  "/pipelines/pl-9/schema-changes",
	}
	driftRendered := resolve("SCHEMA_DRIFT_DETECTED", p.Type, notifyTopic, "warning", map[string]string{"pipeline": "orders-sync"})

	t.Run("interactive + actionable emits buttons", func(t *testing.T) {
		n := &Notifier{appBaseURL: "https://app.rsync.ai", interactiveApprovals: true}
		out, _ := json.Marshal(n.slackPayload(p, driftRendered, true))
		s := string(out)
		for _, want := range []string{
			`"blocks"`,
			slack.ActionApproveSchemaChange,
			slack.ActionRejectSchemaChange,
			`"pl-9"`, // button value = pipeline id
			"https://app.rsync.ai/pipelines/pl-9/schema-changes", // absolute view link
		} {
			if !strings.Contains(s, want) {
				t.Errorf("interactive payload missing %q\npayload: %s", want, s)
			}
		}
		if strings.Contains(s, `"attachments"`) {
			t.Errorf("interactive payload should not use the legacy attachment: %s", s)
		}
	})

	t.Run("interactivity off keeps the link attachment", func(t *testing.T) {
		n := &Notifier{appBaseURL: "https://app.rsync.ai", interactiveApprovals: false}
		out, _ := json.Marshal(n.slackPayload(p, driftRendered, true))
		s := string(out)
		if !strings.Contains(s, `"attachments"`) {
			t.Errorf("expected the legacy attachment when interactivity is off: %s", s)
		}
		if strings.Contains(s, slack.ActionApproveSchemaChange) {
			t.Errorf("must not emit approve buttons when interactivity is off: %s", s)
		}
	})

	t.Run("non-actionable notification keeps the link attachment even when interactive", func(t *testing.T) {
		n := &Notifier{appBaseURL: "https://app.rsync.ai", interactiveApprovals: true}
		out, _ := json.Marshal(n.slackPayload(p, driftRendered, false))
		s := string(out)
		if !strings.Contains(s, `"attachments"`) {
			t.Errorf("a non-drift alert must keep the link attachment: %s", s)
		}
		if strings.Contains(s, slack.ActionApproveSchemaChange) {
			t.Errorf("a non-drift alert must not carry approve buttons: %s", s)
		}
	})
}

// TestAbsoluteActionURL pins the delivery-time host-prefixing that makes
// Slack buttons + email links resolve outside the app. The persisted
// action_url is a relative path (e.g. /pipelines/{id}/schema-changes); we
// must prefix APP_BASE_URL only at delivery, never mangle already-absolute
// links, and never emit an empty URL (Slack rejects an empty button URL).
func TestAbsoluteActionURL(t *testing.T) {
	const base = "https://app.rsync.ai"

	cases := []struct {
		name      string
		base      string
		actionURL string
		want      string
	}{
		{
			name:      "relative schema-changes path gets host prefix",
			base:      base,
			actionURL: "/pipelines/abc123/schema-changes",
			want:      "https://app.rsync.ai/pipelines/abc123/schema-changes",
		},
		{
			name:      "relative pipeline path gets host prefix",
			base:      base,
			actionURL: "/pipelines/abc123",
			want:      "https://app.rsync.ai/pipelines/abc123",
		},
		{
			name:      "path without leading slash is still joined with exactly one slash",
			base:      base,
			actionURL: "pipelines/abc123",
			want:      "https://app.rsync.ai/pipelines/abc123",
		},
		{
			name:      "already-absolute https url passes through unchanged",
			base:      base,
			actionURL: "https://elsewhere.example/x",
			want:      "https://elsewhere.example/x",
		},
		{
			name:      "already-absolute http url passes through unchanged",
			base:      base,
			actionURL: "http://localhost:3000/pipelines/abc123",
			want:      "http://localhost:3000/pipelines/abc123",
		},
		{
			name:      "empty action_url falls back to the base (never an empty button URL)",
			base:      base,
			actionURL: "",
			want:      base,
		},
		{
			name:      "whitespace-only action_url falls back to the base",
			base:      base,
			actionURL: "   ",
			want:      base,
		},
		{
			name:      "surrounding whitespace on a relative path is trimmed before joining",
			base:      base,
			actionURL: "  /pipelines/abc123/schema-changes  ",
			want:      "https://app.rsync.ai/pipelines/abc123/schema-changes",
		},
		{
			name:      "localhost dev base joins a relative path",
			base:      "http://localhost:3000",
			actionURL: "/pipelines/xyz/schema-changes",
			want:      "http://localhost:3000/pipelines/xyz/schema-changes",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := absoluteActionURL(tc.base, tc.actionURL); got != tc.want {
				t.Errorf("absoluteActionURL(%q, %q) = %q, want %q", tc.base, tc.actionURL, got, tc.want)
			}
		})
	}
}

// The dedup key must separate two DIFFERENT drifts on the same pipeline.
//
// Every schema drift resolves to the same code (SCHEMA_DRIFT_DETECTED) and the same
// deep link (/pipelines/{id}/schema-changes), so before the subject was folded in, the
// first drift inside the 60-minute window was announced and every later one — a second
// table, a second column, a DROP after an ADD — hashed to the identical key and was
// dropped. The approval row was still filed; the alert telling you to go look was not.
func TestMakeDedupKey_SeparatesDifferentDriftsOnOnePipeline(t *testing.T) {
	const (
		pipeline = "11111111-1111-1111-1111-111111111111"
		code     = "SCHEMA_DRIFT_DETECTED"
		link     = "/pipelines/11111111-1111-1111-1111-111111111111/schema-changes"
	)

	dropColOrders := makeDedupKey(pipeline, code, link, "drop_column:public.orders:legacy_note")
	dropColUsers := makeDedupKey(pipeline, code, link, "drop_column:public.users:legacy_note")
	dropTable := makeDedupKey(pipeline, code, link, "drop_table:public.orders:")
	addCol := makeDedupKey(pipeline, code, link, "add_column:public.orders:total")

	seen := map[string]string{}
	for name, k := range map[string]string{
		"drop_column on orders": dropColOrders,
		"drop_column on users":  dropColUsers,
		"drop_table on orders":  dropTable,
		"add_column on orders":  addCol,
	} {
		if prev, dup := seen[k]; dup {
			t.Errorf("%q and %q collide on one dedup key — the second would be swallowed", name, prev)
		}
		seen[k] = name
	}

	// ...while a RETRY of the same drift still collapses. That is the property dedup
	// exists for, and it is why the subject is built from structured fields rather
	// than from the (possibly LLM-authored, run-to-run varying) message text.
	if makeDedupKey(pipeline, code, link, "drop_column:public.orders:legacy_note") != dropColOrders {
		t.Error("the same subject must produce the same key, or retries stop deduping")
	}
}

// No subject = exactly the old key. Producers that have no sub-identity to report
// (a pipeline failed; there is only one of it) must not be forced to change.
func TestMakeDedupKey_EmptySubjectIsStable(t *testing.T) {
	a := makeDedupKey("pl-1", "PIPELINE_FAILED", "/pipelines/pl-1", "")
	b := makeDedupKey("pl-1", "PIPELINE_FAILED", "/pipelines/pl-1", "")
	if a != b {
		t.Fatal("empty subject must be deterministic")
	}
	if a == makeDedupKey("pl-1", "PIPELINE_FAILED", "/pipelines/pl-1", "x") {
		t.Error("empty and non-empty subjects must not collide")
	}
}

// structuredErrorView is unmarshal-then-remarshalled into metadata.structured_error,
// so a field it does not declare is silently dropped — the trap that has bitten this
// codebase before on the validate DTO. Pin that dedup_subject survives the round trip.
func TestStructuredErrorView_CarriesDedupSubject(t *testing.T) {
	raw := []byte(`{"failure_type":"schema_drift","code":"SCHEMA_DRIFT_DETECTED",
		"severity":"warning","user_message":"A column was dropped",
		"dedup_subject":"drop_column:public.orders:legacy_note"}`)

	var se structuredErrorView
	if err := json.Unmarshal(raw, &se); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if se.DedupSubject != "drop_column:public.orders:legacy_note" {
		t.Fatalf("dedup_subject not parsed: %+v", se)
	}

	out, err := json.Marshal(se)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(out), `"dedup_subject":"drop_column:public.orders:legacy_note"`) {
		t.Errorf("dedup_subject dropped on re-marshal: %s", out)
	}
}
