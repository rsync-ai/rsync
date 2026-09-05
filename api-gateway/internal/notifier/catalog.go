package notifier

import "strings"

// User-facing notification copy catalog.
//
// Every notification the product shows a customer resolves through this file.
// The rule it enforces: a user never sees an internal identifier — not an error
// code (LEGACY_UNCLASSIFIED), not a Kafka topic (rsync.healer.results), not a
// snake_case event type. They see what happened, whether their data is still
// moving, and the one thing to do next.
//
// ── Adding a notification ────────────────────────────────────────────────────
// 1. Emit a StructuredError with a new stable Code from the orchestrator
//    (backend-orchestrator/pkg/diagnose/structured_error.go).
// 2. Add ONE entry to `catalog` below, keyed by that code.
// That is the whole change: the DB row, the header bell, Slack and email all
// read from here.
//
// Forgetting step 2 is safe — `resolve` falls back to a neutral human sentence
// ("<pipeline> ran into a problem") and never leaks the raw code into the
// headline. The code is still persisted as metadata.error_code so support can
// look it up.

// Entry is the plain-language copy for one notification code.
//
// Title is the headline: what happened, in the user's words, never a code.
// Impact answers "is my data still moving, and must I act?" — the single most
// useful line for a non-technical reader, and optional.
// ActionLabel is the verb for the button on the deep link; it defaults to
// "View pipeline" when the event carries an action_url.
// Severity optionally overrides the severity inferred from the event.
//
// Title/Impact support {pipeline}, {source}, {table} and {column} placeholders,
// filled from the event; unfilled ones degrade to a neutral phrase rather than
// rendering literal braces.
type Entry struct {
	Title       string
	Impact      string
	ActionLabel string
	Severity    string
}

// Severity vocabulary used end to end (DB row → bell dot → Slack color).
// The orchestrator's StructuredError emits "error"; normalizeSeverity folds it
// into "critical" so there is exactly one vocabulary downstream.
const (
	severityCritical = "critical"
	severityWarning  = "warning"
	severityInfo     = "info"
)

// codeSchemaChangeApplied is minted by the notifier itself (not the
// orchestrator) when a healing result reports an applied schema change. It has
// no StructuredError counterpart because nothing failed.
const codeSchemaChangeApplied = "SCHEMA_CHANGE_APPLIED"

// catalog maps a stable error code to its user-facing copy. Keys must match the
// Code values in backend-orchestrator/pkg/diagnose/structured_error.go exactly.
var catalog = map[string]Entry{
	// ── Schema drift ────────────────────────────────────────────────────────
	// "Review change" is named verbatim in the drift notification BODY that the
	// orchestrator writes (healer.schemaDriftNotificationText), because that link is
	// the one route to /pipelines/{id}/schema-changes that always exists — there is no
	// Schema Changes tab, and the amber drift badge renders only while something is
	// pending. Renaming this label means updating that copy in the same change.
	"SCHEMA_DRIFT_DETECTED": {
		Title:       "Approval needed: your source schema changed",
		Impact:      "Your existing data keeps syncing. The change won't be applied until you approve it.",
		ActionLabel: "Review change",
		Severity:    severityWarning,
	},
	codeSchemaChangeApplied: {
		Title:       "Schema change applied to {table}",
		Impact:      "Nothing to do — {pipeline} now matches the source.",
		ActionLabel: "View pipeline",
		Severity:    severityInfo,
	},

	// ── Per-table source prerequisites ──────────────────────────────────────
	// The CDC_ prefix on these two codes is historical and does NOT mean the
	// check is CDC-only: assessor.RequiresTablePrimaryKeys() is
	// `IsCDC() || DestinationUsesUpsert()`, so a plain BATCH pipeline into any
	// relational destination runs both checks and can raise either code. The
	// codes are left as-is deliberately — they are stable identifiers, they are
	// persisted on existing notification rows, and each has a published doc URL.
	// Only the user-facing copy below is allowed to describe them.
	"CDC_TABLE_MISSING_PRIMARY_KEY": {
		Title:       "A table needs a primary key before it can sync",
		Impact:      "That table isn't syncing. The rest of {pipeline} is unaffected.",
		ActionLabel: "See how to fix",
		Severity:    severityWarning,
	},
	"CDC_TABLE_NOT_FOUND_IN_SOURCE": {
		Title: "A table has disappeared from {source}",
		// No Severity override: both emitters raise this at error severity
		// (assessor/mysql.go, assessor/postgresql.go — a selected table that is
		// not in the source is SeverityError and blocks the run before any data
		// moves), so pinning it to `warning` here told the user the opposite of
		// what happened. The old Impact went further and promised "the rest of
		// {pipeline} continues", which is exactly what a blocked run does not do.
		Impact:      "{pipeline} can't run until the table is restored or removed from its table selection.",
		ActionLabel: "Review pipeline",
	},

	// ── Source configuration blocking real-time sync ────────────────────────
	"MYSQL_BINLOG_FORMAT_NOT_ROW": {
		Title:       "One MySQL setting is blocking real-time sync",
		Impact:      "Real-time sync can't start until this setting is changed.",
		ActionLabel: "See how to fix",
		Severity:    severityCritical,
	},
	"MYSQL_BINLOG_ROW_IMAGE_NOT_FULL": {
		Title:       "MySQL is only sending partial row data",
		Impact:      "Updates may arrive with missing columns until this setting is changed.",
		ActionLabel: "See how to fix",
		Severity:    severityCritical,
	},
	"POSTGRES_PUBLICATION_DOES_NOT_EXIST": {
		Title:       "Your Postgres replication setup is missing",
		Impact:      "Real-time sync can't start until it's restored.",
		ActionLabel: "See how to fix",
		Severity:    severityCritical,
	},
	"POSTGRES_WAL_LEVEL_NOT_LOGICAL": {
		Title:       "One Postgres setting is blocking real-time sync",
		Impact:      "Real-time sync can't start until this setting is changed.",
		ActionLabel: "See how to fix",
		Severity:    severityCritical,
	},
	"POSTGRES_REPLICATION_SLOT_CONFLICT": {
		Title:       "Something else is already reading changes from this database",
		Impact:      "Real-time sync is paused until the conflict clears.",
		ActionLabel: "See how to fix",
		Severity:    severityCritical,
	},
	"SQLSERVER_CDC_NOT_ENABLED": {
		Title:       "SQL Server needs change tracking turned on",
		Impact:      "Real-time sync can't start until it's enabled.",
		ActionLabel: "See how to fix",
		Severity:    severityCritical,
	},
	"SQLSERVER_CDC_TIER_UNSUPPORTED": {
		Title:       "This SQL Server plan doesn't support real-time sync",
		Impact:      "Real-time sync isn't available on this tier. A scheduled sync still works.",
		ActionLabel: "See options",
		Severity:    severityCritical,
	},
	"SQLSERVER_AGENT_NOT_RUNNING": {
		Title:       "SQL Server Agent isn't running",
		Impact:      "Changes are being recorded but not delivered until the agent starts.",
		ActionLabel: "See how to fix",
		Severity:    severityCritical,
	},
	"SQLSERVER_CAPTURE_INSTANCE_ERROR": {
		Title:       "SQL Server change tracking hit an error",
		Impact:      "Real-time sync is paused until this is resolved.",
		ActionLabel: "See how to fix",
		Severity:    severityCritical,
	},
	"MONGODB_NOT_REPLICA_SET": {
		Title:       "MongoDB needs to run as a replica set",
		Impact:      "Real-time sync can't start until this is changed.",
		ActionLabel: "See how to fix",
		Severity:    severityCritical,
	},
	"MONGODB_RESUME_TOKEN_INVALID": {
		Title:       "MongoDB's change history has moved past where we stopped",
		Impact:      "Real-time sync stopped. A fresh sync is needed to catch up.",
		ActionLabel: "Restart sync",
		Severity:    severityCritical,
	},

	// ── Connection / credentials ────────────────────────────────────────────
	// {source} sits after a preposition in both titles on purpose — see the
	// placeholderDefaults comment for why a possessive ("your {source} account")
	// cannot be worded here.
	"AUTH_TOKEN_EXPIRED": {
		Title:       "Your connection to {source} expired",
		Impact:      "Syncing is paused. It resumes as soon as you reconnect.",
		ActionLabel: "Reconnect",
		Severity:    severityCritical,
	},
	"AUTH_SCOPE_INSUFFICIENT": {
		Title:       "Your connection to {source} is missing a permission",
		Impact:      "Syncing is paused until the connection is re-authorized with access.",
		ActionLabel: "Reconnect",
		Severity:    severityCritical,
	},

	// ── Transient / self-resolving — reassure, don't alarm ───────────────────
	"RATE_LIMIT_EXCEEDED": {
		Title:       "{source} is limiting how fast we can read",
		Impact:      "Still syncing, just slower. Nothing to do.",
		ActionLabel: "View pipeline",
		Severity:    severityInfo,
	},
	"NETWORK_TRANSIENT_FAILURE": {
		Title:       "Temporary connection problem",
		Impact:      "We're retrying automatically. Nothing to do.",
		ActionLabel: "View pipeline",
		Severity:    severityInfo,
	},

	// ── Destination ─────────────────────────────────────────────────────────
	"DESTINATION_CAPACITY_EXCEEDED": {
		Title:       "Your destination is out of space",
		Impact:      "New rows can't be written until space is freed.",
		ActionLabel: "See how to fix",
		Severity:    severityCritical,
	},
	"USER_CONFIG_INVALID": {
		Title:       "A setting in {pipeline} needs fixing",
		Impact:      "Syncing is paused until the setting is corrected.",
		ActionLabel: "Review settings",
		Severity:    severityCritical,
	},

	// ── Our fault — say so plainly and tell them not to chase it ────────────
	"RSYNC_BUG_SILENT_DROP": {
		Title:       "We hit a problem on our side",
		Impact:      "Some rows may be missing. Our team has been alerted — nothing for you to do.",
		ActionLabel: "View pipeline",
		Severity:    severityCritical,
	},
	"RSYNC_BUG_OWNERSHIP_ROW_MISSING": {
		Title:       "We hit a problem on our side",
		Impact:      "Our team has been alerted — nothing for you to do.",
		ActionLabel: "View pipeline",
		Severity:    severityCritical,
	},

	// ── Unclassified. These two are why the bell used to read
	//    "LEGACY_UNCLASSIFIED": both are placeholders meaning "we couldn't
	//    classify this", which is meaningless to a customer. The raw failure
	//    text still lands in the body; the headline stays human.
	"UNKNOWN_ERROR": {
		Title:       "{pipeline} ran into a problem",
		Impact:      "Open the pipeline to see what's affected.",
		ActionLabel: "Open pipeline",
	},
	"LEGACY_UNCLASSIFIED": {
		Title:       "{pipeline} ran into a problem",
		Impact:      "Open the pipeline to see what's affected.",
		ActionLabel: "Open pipeline",
	},
}

// topicDefaults gives every consumed Kafka topic a human headline, so an event
// that carries neither a code nor a type can still never render its topic name.
// Keyed by topic; used only after the code and type lookups miss.
var topicDefaults = map[string]Entry{
	healerActions: {
		Title:       "Automatic recovery in progress",
		Impact:      "We're trying to fix {pipeline} automatically. Nothing to do yet.",
		ActionLabel: "View pipeline",
		Severity:    severityInfo,
	},
	healerResults: {
		Title:       "Pipeline update",
		ActionLabel: "View pipeline",
		Severity:    severityInfo,
	},
}

// placeholderDefaults keep copy readable when the event didn't carry a value —
// "Your connection to {source} expired" degrades to "Your connection to the
// source expired", never to a literal brace.
//
// The defaults carry their own article ("the source", not "source"), so a title
// must place the placeholder where an article reads correctly: after a
// preposition ("disappeared from {source}") or at the start of the sentence,
// where expand capitalizes it ("{source} is limiting…" → "The source is…").
// Never after a possessive — "your {source} account" renders "your the source
// account". TestPlaceholderDefaultsReadAsEnglish guards this for every entry.
var placeholderDefaults = map[string]string{
	"pipeline": "your pipeline",
	"source":   "the source",
	"table":    "the table",
	"column":   "the new column",
}

// defaultActionLabel is used when an entry omits ActionLabel but the event
// carries a deep link.
const defaultActionLabel = "View pipeline"

// Rendered is the resolved user-facing projection of a raw Kafka event —
// everything the DB row, the bell, Slack and email need, with no internal
// identifiers left in it.
type Rendered struct {
	Title       string
	Impact      string
	ActionLabel string
	Severity    string
	// PipelineName is the display name of the owning pipeline (empty when the
	// pipeline has none). Carried here so Slack and email can label the alert
	// with a name instead of a UUID.
	PipelineName string
}

// resolve turns a raw event into user-facing copy.
//
// Lookup ladder, first hit wins:
//  1. catalog[code]                       — curated copy (the normal path)
//  2. topicDefaults[topic]                — human headline per source topic
//  3. humanized event type                — "Schema Change Notification"
//  4. severity-appropriate generic        — "your pipeline needs attention"
//
// Rungs 2-4 exist so that an unmapped new code, a payload with no type, or a
// brand-new topic all still render a sentence a human can read. The raw code is
// never a title at any rung.
func resolve(code, eventType, topic, severity string, params map[string]string) Rendered {
	severity = normalizeSeverity(severity)

	entry, ok := catalog[strings.TrimSpace(code)]
	if !ok {
		entry, ok = topicDefaults[topic]
	}
	if !ok {
		entry = fallbackEntry(eventType, severity)
	}

	if entry.Severity != "" {
		severity = normalizeSeverity(entry.Severity)
	}

	label := entry.ActionLabel
	if label == "" {
		label = defaultActionLabel
	}

	return Rendered{
		Title:        expand(entry.Title, params),
		Impact:       expand(entry.Impact, params),
		ActionLabel:  label,
		Severity:     severity,
		PipelineName: strings.TrimSpace(params["pipeline"]),
	}
}

// fallbackEntry builds copy for an event with no catalog or topic match. It
// prefers a humanized event type ("pipeline_failed" → "Pipeline Failed") and
// otherwise states, by severity, that something needs looking at. The impact
// line is deliberately hedged: for an unrecognized event we genuinely don't
// know what is still flowing, and guessing would be worse than admitting it.
func fallbackEntry(eventType, severity string) Entry {
	title := humanizeType(eventType)
	if title == "" {
		if severity == severityInfo {
			title = "{pipeline} update"
		} else {
			title = "{pipeline} needs attention"
		}
	}

	impact := ""
	switch severity {
	case severityCritical:
		impact = "Your data may not be syncing — open the pipeline to check."
	case severityWarning:
		impact = "Syncing continues for now. Review this when you can."
	}

	return Entry{Title: title, Impact: impact, ActionLabel: defaultActionLabel}
}

// humanizeType turns a snake_case / kebab-case event type into Title Case.
// Returns "" for an empty type and for the structured-error carrier type, which
// describes the envelope rather than the event and would read as gibberish.
func humanizeType(eventType string) string {
	t := strings.TrimSpace(eventType)
	if t == "" || t == "structured_error_notification" {
		return ""
	}
	parts := strings.FieldsFunc(t, func(r rune) bool { return r == '_' || r == '-' })
	for i, w := range parts {
		if len(w) > 0 {
			parts[i] = strings.ToUpper(w[:1]) + w[1:]
		}
	}
	return strings.Join(parts, " ")
}

// expand substitutes {placeholder} tokens, filling anything the event didn't
// supply from placeholderDefaults so no literal brace ever reaches a user.
//
// When a sentence OPENS with a placeholder that fell back to a default, the
// result is capitalized ("your pipeline ran into a problem" → "Your pipeline
// ran into a problem"). A real substituted value is left exactly as the user
// typed it — capitalizing it would rewrite their pipeline name ("orders-sync"
// → "Orders-sync"), which reads as a different pipeline.
func expand(s string, params map[string]string) string {
	if !strings.Contains(s, "{") {
		return s
	}

	capitalize := false
	if strings.HasPrefix(s, "{") {
		if end := strings.Index(s, "}"); end > 1 {
			key := s[1:end]
			_, isKnown := placeholderDefaults[key]
			capitalize = isKnown && strings.TrimSpace(params[key]) == ""
		}
	}

	args := make([]string, 0, len(placeholderDefaults)*2)
	for key, fallback := range placeholderDefaults {
		v := strings.TrimSpace(params[key])
		if v == "" {
			v = fallback
		}
		args = append(args, "{"+key+"}", v)
	}
	out := strings.NewReplacer(args...).Replace(s)

	if capitalize {
		out = capitalizeFirst(out)
	}
	return out
}

// RenderStored re-renders copy for an ALREADY-PERSISTED notification row whose
// metadata predates the copy catalog. Those rows were written with the raw
// error code ("LEGACY_UNCLASSIFIED") or the raw Kafka topic
// ("rsync.healer.results") as their title, which is exactly what the catalog
// exists to prevent — and no deploy fixes them, because they were rendered at
// write time and the catalog only runs on new events.
//
// The old writer persisted metadata.error_code, metadata.raw_type and
// metadata.source_topic on every row, so a stored row carries the same three
// inputs `resolve` takes from a live event. Read-time repair therefore needs no
// backfill migration and mutates nothing: the historical row is left exactly as
// written, and only the projection the bell sees is corrected.
//
// Callers must apply this ONLY to pre-catalog rows — detect them by an empty
// metadata.action_label, which the catalog path always writes non-empty.
func RenderStored(code, eventType, topic, severity, pipelineName string) Rendered {
	return resolve(code, eventType, topic, severity, map[string]string{
		"pipeline": pipelineName,
	})
}

// placeholderCodes are the codes that mean "we could not classify this". They
// are not facts about the failure, so they are not support references either.
var placeholderCodes = map[string]bool{
	"UNKNOWN_ERROR":       true,
	"LEGACY_UNCLASSIFIED": true,
	"":                    true,
}

// SupportReferenceCode returns the code to show the user as a quotable support
// reference, or "" when there is nothing worth quoting.
//
// The bell demotes the error code to a small mono tag on critical rows, captioned
// "Quote this code when contacting support" — the one place this package
// deliberately still shows an identifier, because a real code (AUTH_TOKEN_EXPIRED,
// CDC_SLOT_MISSING) tells support something. The two placeholders do not: they
// mean the classifier gave up, so a user reading "LEGACY_UNCLASSIFIED" under a
// support caption is being handed an internal token dressed up as an answer —
// the header of this file says a user never sees an internal identifier, and this
// was the last route by which one still did.
//
// The code is only suppressed at the presentation edge. It stays on the stored
// row and in metadata.error_code, so `RenderStored` and anything diagnostic still
// sees it.
func SupportReferenceCode(code string) string {
	if placeholderCodes[strings.TrimSpace(code)] {
		return ""
	}
	return code
}

func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// normalizeSeverity folds every producer's vocabulary into the three values the
// UI and Slack understand. The orchestrator's StructuredError emits "error";
// the notifier's own classifySeverity emits "critical". Before this, an "error"
// severity fell through the frontend's switch and rendered as an info-colored
// dot — a real failure that looked routine.
func normalizeSeverity(severity string) string {
	switch strings.ToLower(strings.TrimSpace(severity)) {
	case "critical", "error", "fatal":
		return severityCritical
	case "warning", "warn":
		return severityWarning
	case "info", "":
		return severityInfo
	default:
		return severityInfo
	}
}
