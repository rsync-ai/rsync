package notifier

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestResolve_NeverLeaksInternalIdentifiers is the regression guard for the bug
// this catalog exists to fix: the header bell rendered "LEGACY_UNCLASSIFIED" and
// "rsync.healer.results" as notification headlines. Whatever the input — a
// curated code, an unmapped code, no code at all, an empty payload on any topic
// — the title must be a human sentence, never a raw code, topic or snake_case
// type, and never an unexpanded {placeholder}.
func TestResolve_NeverLeaksInternalIdentifiers(t *testing.T) {
	cases := []struct {
		name      string
		code      string
		eventType string
		topic     string
		severity  string
	}{
		{"legacy unclassified", "LEGACY_UNCLASSIFIED", "structured_error_notification", notifyTopic, "error"},
		{"unknown error code", "UNKNOWN_ERROR", "structured_error_notification", notifyTopic, "error"},
		{"code not in catalog", "SOME_FUTURE_CODE_NOT_MAPPED", "structured_error_notification", notifyTopic, "error"},
		{"healer results with nothing set", "", "", healerResults, ""},
		{"healer actions with nothing set", "", "", healerActions, ""},
		{"notifications topic with nothing set", "", "", notifyTopic, ""},
		{"unrecognized topic with nothing set", "", "", "rsync.brand.new.topic", "warning"},
		{"carrier type only", "", "structured_error_notification", notifyTopic, "warning"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolve(tc.code, tc.eventType, tc.topic, tc.severity, nil)

			if strings.TrimSpace(got.Title) == "" {
				t.Fatal("title must never be empty")
			}
			if tc.code != "" && strings.Contains(got.Title, tc.code) {
				t.Errorf("title leaked the raw error code: %q", got.Title)
			}
			if strings.Contains(got.Title, "rsync.") {
				t.Errorf("title leaked a Kafka topic name: %q", got.Title)
			}
			if strings.Contains(got.Title, "_") {
				t.Errorf("title leaked a snake_case identifier: %q", got.Title)
			}
			if strings.ContainsAny(got.Title+got.Impact, "{}") {
				t.Errorf("unexpanded placeholder in copy: title=%q impact=%q", got.Title, got.Impact)
			}
			if got.ActionLabel == "" {
				t.Error("action label must never be empty — the button needs a verb")
			}
			if got.Severity != severityInfo && got.Severity != severityWarning && got.Severity != severityCritical {
				t.Errorf("severity %q is outside the {info,warning,critical} vocabulary", got.Severity)
			}
		})
	}
}

// TestSupportReferenceCode guards the one place this package still shows an
// identifier to a customer: the bell's small mono tag, captioned "Quote this
// code when contacting support". A real code earns that space; a placeholder
// does not — it means the classifier gave up, and a user was being shown
// "LEGACY_UNCLASSIFIED" as though it were an answer.
func TestSupportReferenceCode(t *testing.T) {
	for _, code := range []string{"AUTH_TOKEN_EXPIRED", "CDC_TABLE_NOT_FOUND_IN_SOURCE", "SCHEMA_DRIFT_DETECTED"} {
		if got := SupportReferenceCode(code); got != code {
			t.Errorf("SupportReferenceCode(%q) = %q, want it preserved — a real code is quotable", code, got)
		}
	}
	for _, code := range []string{"LEGACY_UNCLASSIFIED", "UNKNOWN_ERROR", "", "  "} {
		if got := SupportReferenceCode(code); got != "" {
			t.Errorf("SupportReferenceCode(%q) = %q, want suppressed — it is not a support reference", code, got)
		}
	}

	// Every placeholder must still resolve to human copy: suppressing the tag
	// must not be mistaken for suppressing the row.
	for code := range placeholderCodes {
		if code == "" {
			continue
		}
		if got := resolve(code, "", notifyTopic, "error", nil); got.Title == "" || strings.Contains(got.Title, code) {
			t.Errorf("placeholder %q resolved to %q — copy must stay human even with no reference code", code, got.Title)
		}
	}
}

// TestResolve_PlaceholdersAndFallbacks pins that copy is personalized when the
// event carries context and degrades to a readable phrase when it doesn't —
// never to a literal brace.
func TestResolve_PlaceholdersAndFallbacks(t *testing.T) {
	t.Run("pipeline name is substituted", func(t *testing.T) {
		got := resolve("LEGACY_UNCLASSIFIED", "", notifyTopic, "error",
			map[string]string{"pipeline": "orders-sync"})
		if !strings.Contains(got.Title, "orders-sync") {
			t.Errorf("expected the pipeline name in the title, got %q", got.Title)
		}
		if got.PipelineName != "orders-sync" {
			t.Errorf("PipelineName = %q, want orders-sync", got.PipelineName)
		}
	})

	t.Run("missing pipeline name degrades to a readable phrase", func(t *testing.T) {
		got := resolve("LEGACY_UNCLASSIFIED", "", notifyTopic, "error", nil)
		if !strings.Contains(got.Title, "Your pipeline") {
			t.Errorf("expected a neutral phrase, got %q", got.Title)
		}
	})

	t.Run("source type is substituted", func(t *testing.T) {
		got := resolve("AUTH_TOKEN_EXPIRED", "", notifyTopic, "error",
			map[string]string{"source": "PostgreSQL"})
		if !strings.Contains(got.Title, "PostgreSQL") {
			t.Errorf("expected the source name in the title, got %q", got.Title)
		}
	})

	t.Run("unmapped code still explains impact by severity", func(t *testing.T) {
		got := resolve("SOME_FUTURE_CODE", "", notifyTopic, "error", nil)
		if got.Impact == "" {
			t.Error("a critical unmapped event should still tell the user what to check")
		}
	})
}

// TestResolve_CatalogSeverityOverride pins that curated copy sets the severity —
// a rate-limit event carries a scary "critical" from the raw classifier but is
// genuinely nothing for the user to act on.
func TestResolve_CatalogSeverityOverride(t *testing.T) {
	got := resolve("RATE_LIMIT_EXCEEDED", "", notifyTopic, "critical", nil)
	if got.Severity != severityInfo {
		t.Errorf("severity = %q, want info (the catalog entry downgrades it)", got.Severity)
	}
	if !strings.Contains(got.Impact, "Nothing to do") {
		t.Errorf("expected a reassuring impact line, got %q", got.Impact)
	}
}

// TestNormalizeSeverity pins the single severity vocabulary. The orchestrator
// emits "error"; before this it fell through the frontend switch and a real
// failure rendered with the same dot color as an informational notice.
func TestNormalizeSeverity(t *testing.T) {
	cases := map[string]string{
		"error":     severityCritical,
		"ERROR":     severityCritical,
		"fatal":     severityCritical,
		"critical":  severityCritical,
		"warning":   severityWarning,
		"warn":      severityWarning,
		"info":      severityInfo,
		"":          severityInfo,
		"nonsense":  severityInfo,
		"  Error  ": severityCritical,
	}
	for in, want := range cases {
		if got := normalizeSeverity(in); got != want {
			t.Errorf("normalizeSeverity(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestCatalogCopyHygiene enforces the house style on every entry, so a future
// entry can't reintroduce the class of bug this file fixes.
func TestCatalogCopyHygiene(t *testing.T) {
	for code, entry := range catalog {
		t.Run(code, func(t *testing.T) {
			if strings.TrimSpace(entry.Title) == "" {
				t.Fatal("entry has no title")
			}
			if strings.Contains(entry.Title, code) {
				t.Errorf("title repeats the raw code: %q", entry.Title)
			}
			// Rendered with no context at all, copy must still be clean.
			got := resolve(code, "", notifyTopic, "info", nil)
			if strings.ContainsAny(got.Title+got.Impact, "{}") {
				t.Errorf("placeholder survived expansion: title=%q impact=%q", got.Title, got.Impact)
			}
			if strings.Contains(got.Title, "_") {
				t.Errorf("title contains a snake_case identifier: %q", got.Title)
			}
			if entry.Severity != "" && normalizeSeverity(entry.Severity) != entry.Severity {
				t.Errorf("severity %q is not one of {info,warning,critical}", entry.Severity)
			}
		})
	}
}

// TestPlaceholderDefaultsReadAsEnglish catches copy that is grammatical only
// when a placeholder gets filled. The defaults carry their own article ("the
// source"), so "Reconnect your {source} account" — which read fine in review —
// rendered "Reconnect your the source account" for every event that arrived
// without a source_db_type. Staging surfaced it on two live entries.
//
// Rendering every entry with NO params is the only way to see the degraded
// form, so this runs the whole catalog rather than the entries known to use a
// placeholder today.
func TestPlaceholderDefaultsReadAsEnglish(t *testing.T) {
	// An article immediately after a possessive or another article. Cheap to
	// state, and it is exactly the class of error that shipped.
	doubled := []string{
		"your the ", "your a ", "your an ",
		"the the ", "a the ", "an the ", "the a ", "the an ",
	}
	for code := range catalog {
		t.Run(code, func(t *testing.T) {
			got := resolve(code, "", notifyTopic, "info", nil)
			for _, text := range []string{got.Title, got.Impact} {
				lower := strings.ToLower(text)
				for _, bad := range doubled {
					if strings.Contains(lower, bad) {
						t.Errorf("copy reads as %q with the placeholder default: %q\n"+
							"place {source}/{table} after a preposition, not a possessive", strings.TrimSpace(bad), text)
					}
				}
			}
		})
	}
}

// TestPlaceholderDefaults_FilledAndUnfilled pins both renderings of the two
// entries the bug was found on, so a future reword cannot regress one while
// fixing the other.
func TestPlaceholderDefaults_FilledAndUnfilled(t *testing.T) {
	cases := []struct {
		name, code string
		params     map[string]string
		want       string
	}{
		{"auth expired, source known", "AUTH_TOKEN_EXPIRED",
			map[string]string{"source": "PostgreSQL"}, "Your connection to PostgreSQL expired"},
		{"auth expired, source unknown", "AUTH_TOKEN_EXPIRED",
			nil, "Your connection to the source expired"},
		{"scope, source known", "AUTH_SCOPE_INSUFFICIENT",
			map[string]string{"source": "Stripe"}, "Your connection to Stripe is missing a permission"},
		{"scope, source unknown", "AUTH_SCOPE_INSUFFICIENT",
			nil, "Your connection to the source is missing a permission"},
		// Sentence-initial placeholder: the default is capitalized, a real
		// value is left as the user's own casing.
		{"rate limit, source unknown", "RATE_LIMIT_EXCEEDED",
			nil, "The source is limiting how fast we can read"},
		{"rate limit, source known", "RATE_LIMIT_EXCEEDED",
			map[string]string{"source": "Shopify"}, "Shopify is limiting how fast we can read"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolve(tc.code, "", notifyTopic, "info", tc.params).Title; got != tc.want {
				t.Errorf("title = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestRenderStored covers the read-time repair of rows persisted before the
// catalog existed — see handlers.repairPreCatalogCopy for the caller.
func TestRenderStored(t *testing.T) {
	// A row whose title was the raw topic, because the event carried no type.
	got := RenderStored("", "", healerResults, "info", "orders-sync")
	if got.Title != "Pipeline update" {
		t.Errorf("topic-only row: title = %q, want %q", got.Title, "Pipeline update")
	}
	// A row whose title was the raw code.
	got = RenderStored("LEGACY_UNCLASSIFIED", "structured_error_notification", notifyTopic, "critical", "orders-sync")
	if got.Title != "orders-sync ran into a problem" {
		t.Errorf("coded row: title = %q", got.Title)
	}
	if got.ActionLabel == "" {
		t.Error("repaired row must carry an action label")
	}
	// With nothing at all to go on it still must not render an identifier.
	got = RenderStored("", "", "", "warning", "")
	if strings.ContainsAny(got.Title, "{}_") || strings.Contains(got.Title, "rsync.") {
		t.Errorf("bare row leaked an identifier: %q", got.Title)
	}
}

// TestNormalizeHealingResult pins the fix for the blank "rsync.healer.results"
// rows: only an applied change is worth telling a user about, and it must
// arrive with real copy. Everything else is suppressed because it either
// duplicates a notification already sent on rsync.notifications or is internal
// telemetry.
func TestNormalizeHealingResult(t *testing.T) {
	t.Run("applied change becomes a real notification", func(t *testing.T) {
		raw, _ := json.Marshal(map[string]string{
			"pipeline_id": "pl-1",
			"change_type": "add_column",
			"table":       "public.categories",
			"status":      "applied",
			"reason":      "Additive change, safe to apply",
		})
		var p notificationPayload
		code, params, keep := normalizeHealingResult(raw, &p)
		if !keep {
			t.Fatal("an applied schema change must reach the user")
		}
		if code != codeSchemaChangeApplied {
			t.Errorf("code = %q, want %q", code, codeSchemaChangeApplied)
		}
		if params["table"] != "public.categories" {
			t.Errorf("table param = %q", params["table"])
		}
		if !strings.Contains(p.Message, "public.categories") || strings.TrimSpace(p.Message) == "" {
			t.Errorf("message must describe the change, got %q", p.Message)
		}
		if p.ActionURL != "/pipelines/pl-1/schema-changes" {
			t.Errorf("action_url = %q, want the schema-changes deep link", p.ActionURL)
		}

		got := resolve(code, p.Type, healerResults, "", params)
		if strings.Contains(got.Title, "rsync.") {
			t.Errorf("title leaked the topic name: %q", got.Title)
		}
		if !strings.Contains(got.Title, "public.categories") {
			t.Errorf("expected the table in the title, got %q", got.Title)
		}
	})

	for _, status := range []string{"pending_approval", "failed", "skipped", ""} {
		t.Run("suppressed: "+status, func(t *testing.T) {
			raw, _ := json.Marshal(map[string]string{
				"pipeline_id": "pl-1",
				"change_type": "add_column",
				"status":      status,
			})
			var p notificationPayload
			if _, _, keep := normalizeHealingResult(raw, &p); keep {
				t.Errorf("status %q must not create a second inbox row", status)
			}
		})
	}

	t.Run("malformed payload is dropped, not rendered", func(t *testing.T) {
		var p notificationPayload
		if _, _, keep := normalizeHealingResult([]byte("not json"), &p); keep {
			t.Error("a malformed healing result must be dropped")
		}
	})
}

// TestPrettySourceType pins that a stored connector type reads like a product
// name in a sentence ("Reconnect your PostgreSQL account").
func TestPrettySourceType(t *testing.T) {
	cases := map[string]string{
		"postgresql":  "PostgreSQL",
		"postgres":    "PostgreSQL",
		"mysql":       "MySQL",
		"sqlserver":   "SQL Server",
		"mongodb":     "MongoDB",
		"oracle":      "Oracle",
		"":            "",
		"some_new_db": "Some_new_db",
	}
	for in, want := range cases {
		if got := prettySourceType(in); got != want {
			t.Errorf("prettySourceType(%q) = %q, want %q", in, got, want)
		}
	}
}
