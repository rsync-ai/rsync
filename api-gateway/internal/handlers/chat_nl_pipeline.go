package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"api-gateway/internal/chat"
	"api-gateway/internal/db"
	"api-gateway/internal/telemetry"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rsync-ai/backend-orchestrator/pkg/llmscrub"
	"github.com/rsync-ai/shared/crypto"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/client"
)

// knownCDCSourceTypes is the fail-OPEN allow-list used when a connector's metadata can't
// be read or doesn't declare supports_cdc. #183/#184 removed the root metadata.json copies
// (canonical source is now versions/<current_version>/metadata.json), so a bare-path read
// returns not-found for installed connectors; without this fallback every explicit CDC
// request for a real CDC source was silently downgraded to batch (BUG-CDC-2). Keys are the
// post-NormalizeConnectorName canonical names of the relational sources the orchestrator's
// own mode selector treats as CDC-capable.
var knownCDCSourceTypes = map[string]bool{
	"postgresql": true,
	"mysql":      true,
	"mariadb":    true,
	"mongodb":    true,
	"sqlserver":  true,
	"oracle":     true,
}

func connectorSupportsCDC(connectorType string) bool {
	ct := strings.ToLower(strings.TrimSpace(connectorType))
	if ct == "" {
		return false
	}
	ct = chat.NormalizeConnectorName(ct)

	// Resolve metadata via the connector index so BOTH layouts are honored: the legacy root
	// metadata.json AND versions/<current_version>/metadata.json (the canonical post-#183/#184
	// location). The previous bare os.ReadFile(<dir>/metadata.json) silently missed the
	// versioned layout and fell through to false.
	for _, base := range []string{GetMCPPublicConnectorsPath(), GetMCPInternalConnectorsPath()} {
		if base == "" {
			continue
		}
		resolvedName, err := resolveConnectorDirName(base, ct)
		if err != nil {
			continue
		}
		sc, ok := findScannedConnector(getConnectorIndex(base), resolvedName)
		if !ok || len(sc.Metadata) == 0 {
			continue
		}
		var raw map[string]interface{}
		if json.Unmarshal(sc.Metadata, &raw) == nil {
			if v, ok := raw["supports_cdc"].(bool); ok {
				return v
			}
		}
	}

	// Metadata missing or it didn't declare supports_cdc → fail OPEN for known relational
	// CDC sources rather than silently downgrading a real CDC request to batch (BUG-CDC-2).
	return knownCDCSourceTypes[ct]
}

func connectorSupportsIncrementalBatch(connectorType string) bool {
	ct := strings.ToLower(strings.TrimSpace(connectorType))
	if ct == "" {
		return false
	}
	ct = chat.NormalizeConnectorName(ct)

	// Helper to inspect metadata for incremental parameters on export.
	checkMetadata := func(b []byte) bool {
		if len(b) == 0 {
			return false
		}
		var raw map[string]interface{}
		if json.Unmarshal(b, &raw) != nil {
			return false
		}

		// If connector generator wrote an explicit capability, prefer it.
		if caps, ok := raw["capabilities"].(map[string]interface{}); ok && caps != nil {
			if v, ok := caps["supports_incremental_batch"].(bool); ok {
				return v
			}
		}

		ops, ok := raw["operations"].([]interface{})
		if !ok || len(ops) == 0 {
			return false
		}

		// Executor uses these names when trying to do incremental batch exports.
		incrementalParamNames := map[string]bool{
			"since":             true,
			"updated_since":     true,
			"modified_since":    true,
			"modified_after":    true,
			"cursor":            true,
			"incremental_field": true,
		}

		for _, it := range ops {
			op, ok := it.(map[string]interface{})
			if !ok || op == nil {
				continue
			}
			name := strings.ToLower(strings.TrimSpace(fmt.Sprint(op["name"])))
			typ := strings.ToLower(strings.TrimSpace(fmt.Sprint(op["type"])))
			if name != "export" && typ != "source" {
				continue
			}
			params, _ := op["parameters"].([]interface{})
			for _, pit := range params {
				pm, ok := pit.(map[string]interface{})
				if !ok || pm == nil {
					continue
				}
				pn := strings.ToLower(strings.TrimSpace(fmt.Sprint(pm["name"])))
				if incrementalParamNames[pn] {
					return true
				}
			}
		}
		return false
	}

	// Prefer public connectors
	publicPath := GetMCPPublicConnectorsPath()
	if resolvedName, err := resolveConnectorDirName(publicPath, ct); err == nil {
		metadataPath := filepath.Join(publicPath, resolvedName, "metadata.json")
		if b, err := os.ReadFile(metadataPath); err == nil {
			if checkMetadata(b) {
				return true
			}
		}
	}

	// Also check internal connectors (best-effort).
	internalPath := GetMCPInternalConnectorsPath()
	if internalPath != "" {
		if resolvedName, err := resolveConnectorDirName(internalPath, ct); err == nil {
			metadataPath := filepath.Join(internalPath, resolvedName, "metadata.json")
			if b, err := os.ReadFile(metadataPath); err == nil {
				if checkMetadata(b) {
					return true
				}
			}
		}
	}

	return false
}

func supportedSyncModesForSource(sourceType string) []string {
	// Always support batch; only add CDC when connector declares support.
	if connectorSupportsCDC(sourceType) {
		return []string{"batch", "cdc"}
	}
	return []string{"batch"}
}

func parseSyncModeOverrides(message string) (syncMode string, cdcMode string, cdcInitialLoad string) {
	lc := strings.ToLower(message)

	// Prefer explicit tokens from UI: sync_mode=... cdc_mode=...
	reSync := regexp.MustCompile(`\bsync[_\s-]?mode\s*[:=]\s*([a-z_]+)\b`)
	if m := reSync.FindStringSubmatch(lc); len(m) == 2 {
		syncMode = strings.TrimSpace(m[1])
	}
	reCDC := regexp.MustCompile(`\bcdc[_\s-]?mode\s*[:=]\s*([a-z_]+)\b`)
	if m := reCDC.FindStringSubmatch(lc); len(m) == 2 {
		cdcMode = strings.TrimSpace(m[1])
	}
	reInit := regexp.MustCompile(`\bcdc[_\s-]?initial[_\s-]?load\s*[:=]\s*([a-z_]+)\b`)
	if m := reInit.FindStringSubmatch(lc); len(m) == 2 {
		cdcInitialLoad = strings.TrimSpace(m[1])
	}

	// Light fallback for human-typed confirmations.
	if syncMode == "" {
		if strings.Contains(lc, "cdc") || strings.Contains(lc, "stream") || strings.Contains(lc, "real-time") || strings.Contains(lc, "realtime") {
			syncMode = "cdc"
		} else if strings.Contains(lc, "batch") || strings.Contains(lc, "one-time") || strings.Contains(lc, "one time") {
			syncMode = "batch"
		}
	}
	if cdcMode == "" && syncMode == "cdc" {
		// Default CDC mode.
		cdcMode = "initial"
		if strings.Contains(lc, "streaming_only") || strings.Contains(lc, "streaming only") || strings.Contains(lc, "changes only") {
			cdcMode = "streaming_only"
		}
	}

	// Normalize.
	syncMode = strings.ToLower(strings.TrimSpace(syncMode))
	cdcMode = strings.ToLower(strings.TrimSpace(cdcMode))
	if syncMode != "cdc" && syncMode != "batch" {
		syncMode = ""
	}
	if cdcMode != "initial" && cdcMode != "streaming_only" {
		cdcMode = ""
	}
	cdcInitialLoad = strings.ToLower(strings.TrimSpace(cdcInitialLoad))
	if cdcInitialLoad != "batch" && cdcInitialLoad != "debezium" {
		cdcInitialLoad = ""
	}
	return syncMode, cdcMode, cdcInitialLoad
}

// resolveConfirmationSyncMode picks the pipeline's sync mode at confirmation time
// from the three request-derived signals, MOST-EXPLICIT first:
//
//  1. An explicit override token in the confirmation `message` (the UI sync-mode
//     button posts "sync_mode=cdc cdc_mode=initial"). The confirmation card
//     DISABLES Start until the user picks a mode when the source supports CDC, so
//     for a CDC-capable source this token always reflects the user's deliberate,
//     most-recent choice. It MUST win even over a concrete pendingSyncMode,
//     because that value can be a keyword GUESS from the intent classifier —
//     intent_classification.yaml maps the verb "sync" → sync_mode="batch". The
//     old ordering read pendingSyncMode first and only re-parsed overrides when it
//     was ""/"auto", so a request like "Sync <table> from <conn> to <conn>" was
//     classified batch and the user's explicit "CDC (snapshot + changes)" click
//     was then silently dropped, running the pipeline as batch (the Suite-C prod
//     defect, a recurrence of the BUG-CDC-1 silent-downgrade class).
//  2. pendingSyncMode carried from the NL request ("" and "auto" both mean "no
//     explicit mode expressed — decide downstream").
//  3. An explicit override token in the ORIGINAL NL request — autosend replays the
//     confirmation handler with a synthetic "yes" as `message`, and the fast-path
//     intent parser hardcodes SyncMode="auto", so an explicit "use CDC / stream /
//     sync_mode=cdc" in the first turn would otherwise be lost.
//
// Returns ("", "", "") when no request signal specifies a mode; the caller then
// falls back to the source connection's configured default and finally batch, and
// applies the connector-capability downgrade. Pure + unit-testable so the
// precedence (explicit click beats inferred mode) is pinned by
// chat_nl_syncmode_test.go.
func resolveConfirmationSyncMode(message, pendingSyncMode, originalRequest string) (syncMode, cdcMode, cdcInitialLoad string) {
	syncMode, cdcMode, cdcInitialLoad = parseSyncModeOverrides(message)
	if syncMode == "" {
		if pm := strings.ToLower(strings.TrimSpace(pendingSyncMode)); pm != "" && pm != "auto" {
			syncMode = pm
		}
	}
	if syncMode == "" || syncMode == "auto" {
		syncMode, cdcMode, cdcInitialLoad = parseSyncModeOverrides(originalRequest)
	}
	return syncMode, cdcMode, cdcInitialLoad
}

// scheduleTimeRe captures a wall-clock time ("at 2am", "at 14:00", "at 2:30 pm",
// or a bare minute "at :30" for hourly cadences). The hour group is optional so
// "hourly at :30" no longer silently loses its minute.
var scheduleTimeRe = regexp.MustCompile(`\bat\s+(\d{1,2})?(?::(\d{2}))?\s*(am|pm)?\b`)

// everyNSecondsRe / everyNMinutesRe / everyNHoursRe capture interval cadences.
// Sub-minute cadences are floored to the 60s scheduler minimum rather than
// silently dropped (which left "every 30 seconds" running once as a batch).
var everyNSecondsRe = regexp.MustCompile(`\bevery\s+(\d+)\s*(?:seconds?|secs?)\b`)
var everyNMinutesRe = regexp.MustCompile(`\bevery\s+(\d+)\s*(?:minutes?|mins?)\b`)
var everyNHoursRe = regexp.MustCompile(`\bevery\s+(\d+)\s*(?:hours?|hrs?)\b`)

// cronLiteralRe recognizes a pasted 5-field cron expression ("cron 0 3 * * 1")
// so an explicit schedule is honored instead of dropped.
var cronLiteralRe = regexp.MustCompile(`\bcron\s+((?:[\d*/,\-]+\s+){4}[\d*/,\-]+)`)

// parseScheduleClock extracts an "at HH[:MM] am/pm" time. ok=false when absent.
func parseScheduleClock(lc string) (hour, minute int, ok bool) {
	m := scheduleTimeRe.FindStringSubmatch(lc)
	if m == nil || (m[1] == "" && m[2] == "") {
		// Neither an hour nor a minute followed "at" — not a real clock time.
		return 0, 0, false
	}
	if m[1] != "" {
		hour, _ = strconv.Atoi(m[1])
	}
	if m[2] != "" {
		minute, _ = strconv.Atoi(m[2])
	}
	switch m[3] {
	case "pm":
		if hour < 12 {
			hour += 12
		}
	case "am":
		if hour == 12 {
			hour = 0
		}
	}
	if hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return 0, 0, false
	}
	return hour, minute, true
}

// parseScheduleIntent maps NL cadence phrases to a Temporal schedule spec. It
// recognizes "every N minutes/hours" (interval) and daily/hourly/weekly/monthly
// (+ optional "at HH[:MM] am/pm") cron cadences. Returns ok=false when no
// schedule is expressed. Mirrors parseSyncModeOverrides: re-run on the ORIGINAL
// request at confirmation time so the phrasing survives slot-filling.
// dayOfWeekCron returns the cron day-of-week field (0=Sun … 6=Sat) parsed from a
// weekday mention in the message, or "" when none is present. Handles single days
// ("every monday"), multiple ("monday and friday"), and the "weekday(s)"/"weekend"
// shorthands. Without this, "every monday at 9am" dropped the weekday and scheduled
// a DAILY run (KI-NLCHAT-SCHEDULE-DOW).
func dayOfWeekCron(lc string) string {
	if strings.Contains(lc, "weekday") {
		return "1-5"
	}
	if strings.Contains(lc, "weekend") {
		return "0,6"
	}
	days := []struct{ name, dow string }{
		{"sunday", "0"}, {"monday", "1"}, {"tuesday", "2"}, {"wednesday", "3"},
		{"thursday", "4"}, {"friday", "5"}, {"saturday", "6"},
	}
	var hits []string
	for _, d := range days {
		if strings.Contains(lc, d.name) {
			hits = append(hits, d.dow)
		}
	}
	return strings.Join(hits, ",")
}

func parseScheduleIntent(message string) (scheduleType string, spec ScheduleSpec, ok bool) {
	lc := strings.ToLower(message)

	// An explicit pasted cron expression wins outright.
	if m := cronLiteralRe.FindStringSubmatch(lc); m != nil {
		return "cron", ScheduleSpec{Cron: strings.TrimSpace(m[1]), Timezone: "UTC"}, true
	}

	// Interval cadences take precedence (they carry their own period).
	if m := everyNSecondsRe.FindStringSubmatch(lc); m != nil {
		if n, _ := strconv.Atoi(m[1]); n > 0 {
			secs := n
			if secs < 60 {
				secs = 60 // Temporal/schedule minimum — floor sub-minute cadences.
			}
			return "interval", ScheduleSpec{EverySeconds: secs, Timezone: "UTC"}, true
		}
	}
	if m := everyNMinutesRe.FindStringSubmatch(lc); m != nil {
		if n, _ := strconv.Atoi(m[1]); n > 0 {
			secs := n * 60
			if secs < 60 {
				secs = 60 // Temporal/schedule minimum
			}
			return "interval", ScheduleSpec{EverySeconds: secs, Timezone: "UTC"}, true
		}
	}
	if m := everyNHoursRe.FindStringSubmatch(lc); m != nil {
		if n, _ := strconv.Atoi(m[1]); n > 0 {
			return "interval", ScheduleSpec{EverySeconds: n * 3600, Timezone: "UTC"}, true
		}
	}

	hour, minute, haveTime := parseScheduleClock(lc)
	cron := func(dom, month, dow string) (string, ScheduleSpec, bool) {
		h, mn := 0, 0
		if haveTime {
			h, mn = hour, minute
		}
		return "cron", ScheduleSpec{Cron: fmt.Sprintf("%d %d %s %s %s", mn, h, dom, month, dow), Timezone: "UTC"}, true
	}

	weekdayDOW := dayOfWeekCron(lc)

	switch {
	case strings.Contains(lc, "hourly") || strings.Contains(lc, "every hour"):
		mn := 0
		if haveTime {
			mn = minute
		}
		return "cron", ScheduleSpec{Cron: fmt.Sprintf("%d * * * *", mn), Timezone: "UTC"}, true
	case weekdayDOW != "":
		// A named weekday ("every monday at 9am", "on weekdays") pins the cron DOW
		// field — otherwise it fell through to a plain daily run (KI-NLCHAT-SCHEDULE-DOW).
		return cron("*", "*", weekdayDOW)
	case strings.Contains(lc, "weekly") || strings.Contains(lc, "every week"):
		return cron("*", "*", "0")
	case strings.Contains(lc, "monthly") || strings.Contains(lc, "every month"):
		return cron("1", "*", "*")
	case strings.Contains(lc, "daily") || strings.Contains(lc, "every day") || strings.Contains(lc, "each day"):
		return cron("*", "*", "*")
	case haveTime:
		// A bare "... at 2am" with no cadence word → assume a daily run.
		return cron("*", "*", "*")
	}
	return "", ScheduleSpec{}, false
}

// describeSchedule renders a human-friendly cadence for the chat reply.
func describeSchedule(scheduleType string, spec ScheduleSpec) string {
	if scheduleType == "interval" {
		switch secs := spec.EverySeconds; {
		case secs%3600 == 0:
			return fmt.Sprintf("every %d hour(s)", secs/3600)
		case secs%60 == 0:
			return fmt.Sprintf("every %d minute(s)", secs/60)
		default:
			return fmt.Sprintf("every %d seconds", secs)
		}
	}
	tz := spec.Timezone
	if tz == "" {
		tz = "UTC"
	}
	return fmt.Sprintf("on cron `%s` (%s)", spec.Cron, tz)
}

// nlTransformSpec is the NL-requested transform intent persisted onto
// pipelines.config.nl_transforms. The orchestrator's planNLTransforms gate reads
// it (identical JSON keys) and materializes transform_definitions before data
// moves — so masking / type-conversion apply for BOTH batch and CDC without the
// frontend suggestions dialog. Keep the JSON tags in lockstep with the Go struct
// nlTransformIntent in backend-orchestrator .../executor/nl_transforms_gate.go.
type nlTransformSpec struct {
	MaskColumns []string `json:"mask_columns,omitempty"`
	MaskPII     bool     `json:"mask_pii,omitempty"`
	TypeConvert bool     `json:"type_convert,omitempty"`
}

func (s nlTransformSpec) empty() bool {
	return len(s.MaskColumns) == 0 && !s.MaskPII && !s.TypeConvert
}

// maskVerbRe matches the verbs that signal a masking request in a chat message.
var maskVerbRe = regexp.MustCompile(`\b(?:mask|redact|anonymize|anonymise|obfuscate|pseudonymize|pseudonymise|scrub)\b`)

// nlIdentRe accepts plausible column identifiers (lower-cased); filters out
// numbers and multi-word noise. Hyphens are allowed so a user-typed "user-id"
// is captured as a named column instead of being dropped (which silently
// degraded a targeted mask into blanket generic-PII masking).
var nlIdentRe = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

// nlColumnStopWords are tokens that surround column names in NL but are not
// themselves columns.
var nlColumnStopWords = map[string]bool{
	"the": true, "a": true, "an": true, "and": true, "or": true, "other": true,
	"others": true, "another": true, "column": true, "columns": true, "field": true,
	"fields": true, "data": true, "all": true, "pii": true, "sensitive": true,
	"personal": true, "personally": true, "identifiable": true, "information": true,
	"info": true, "value": true,
	"values": true, "also": true, "please": true, "too": true, "it": true, "its": true,
	"them": true, "their": true, "into": true, "in": true, "on": true, "of": true,
	"for": true, "with": true, "my": true, "this": true, "that": true, "these": true,
	"those": true, "any": true, "some": true, "out": true,
}

// parseMaskingIntent detects a masking request in a chat NL message and extracts
// the explicitly-named columns (e.g. "mask email" → ["email"]) and/or a generic
// PII flag ("mask all PII / sensitive data"). Mirrors parseSyncModeOverrides: it
// is re-run on pendingIntent.OriginalRequest at confirmation time, so the
// original phrasing survives slot-filling. Returns (nil,false) when no masking
// verb is present.
func parseMaskingIntent(message string) (maskColumns []string, maskPII bool) {
	lc := strings.ToLower(message)
	loc := maskVerbRe.FindStringIndex(lc)
	if loc == nil {
		return nil, false
	}
	if strings.Contains(lc, "pii") || strings.Contains(lc, "sensitive") ||
		strings.Contains(lc, "personal data") || strings.Contains(lc, "personally identifiable") {
		maskPII = true
	}
	// Isolate the clause after the mask verb, truncated at the next unrelated
	// instruction so "mask email and convert types" doesn't swallow "convert types".
	clause := lc[loc[1]:]
	cut := len(clause)
	for _, b := range []string{" and convert", " and schedule", " and sync", " and run",
		" convert ", " schedule ", " every ", " then ", " to type", ";", ". "} {
		if i := strings.Index(clause, b); i >= 0 && i < cut {
			cut = i
		}
	}
	clause = clause[:cut]
	maskColumns = extractNLColumnTokens(clause)
	// A bare "mask the data" with no named column and no PII keyword → treat as
	// generic PII masking (safer than silently doing nothing).
	if len(maskColumns) == 0 && !maskPII {
		maskPII = true
	}
	return maskColumns, maskPII
}

// parseTypeConvertIntent detects a request to auto-convert string columns to
// their recommended (numeric/bool) types. The actual per-column recommendation
// is made by the orchestrator's planNLTransforms gate against the real schema.
func parseTypeConvertIntent(message string) bool {
	lc := strings.ToLower(message)
	switch {
	case strings.Contains(lc, "type conversion"),
		strings.Contains(lc, "recommended type"),
		strings.Contains(lc, "recommended data type"),
		strings.Contains(lc, "recommend types"),
		strings.Contains(lc, "recommend data type"):
		return true
	case strings.Contains(lc, "convert") && strings.Contains(lc, "type"):
		return true
	case strings.Contains(lc, "convert") &&
		(strings.Contains(lc, "string column") || strings.Contains(lc, "string columns")):
		return true
	}
	return false
}

// extractNLColumnTokens splits a clause into candidate column identifiers,
// dropping stop-words and non-identifier noise.
func extractNLColumnTokens(clause string) []string {
	clause = strings.NewReplacer(",", " ", "&", " ", "/", " ").Replace(clause)
	var out []string
	seen := map[string]bool{}
	for _, tok := range strings.Fields(clause) {
		tok = strings.Trim(tok, " \t.:;'\"()`")
		if tok == "" || nlColumnStopWords[tok] || !nlIdentRe.MatchString(tok) {
			continue
		}
		if !seen[tok] {
			seen[tok] = true
			out = append(out, tok)
		}
	}
	return out
}

// mergeUniqueColumns concatenates two column lists, preserving order and dropping
// duplicates. Used to union masking columns parsed from the original request and
// the confirmation message so a late "yes but mask email" is not lost.
func mergeUniqueColumns(a, b []string) []string {
	seen := make(map[string]bool, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, list := range [][]string{a, b} {
		for _, c := range list {
			if c == "" || seen[c] {
				continue
			}
			seen[c] = true
			out = append(out, c)
		}
	}
	return out
}

// sameConnectionError implements the KI-NLCHAT-SAME-SRC-DST guard: it returns a
// self-replication error response when the resolved source and destination are the
// SAME connection, or nil when they differ or either id is unresolved (the
// HITL-deferral path). Pure + unit-testable — handleConfirmation calls it so the
// guard's behavior is locked without driving the whole confirmation handler.
func sameConnectionError(sourceConnID, destConnID, traceID string) *ChatMessageResponse {
	if sourceConnID != "" && sourceConnID == destConnID {
		return &ChatMessageResponse{
			Message:   "⚠️ The source and destination resolve to the **same connection**, so this pipeline would replicate data onto itself. Pick a different destination (or create a second connection) and try again.",
			Type:      "error",
			TraceID:   traceID,
			Timestamp: time.Now().Format(time.RFC3339),
		}
	}
	return nil
}

// nlTableStopWords are tokens that sit next to the word "table" in NL but are not
// themselves table names.
var nlTableStopWords = map[string]bool{
	"the": true, "a": true, "an": true, "this": true, "that": true, "these": true,
	"those": true, "each": true, "every": true, "all": true, "any": true, "some": true,
	"one": true, "same": true, "single": true, "new": true, "empty": true, "whole": true,
	"entire": true, "data": true, "source": true, "destination": true, "dest": true,
	"target": true, "from": true, "to": true, "into": true, "in": true, "of": true,
	"and": true, "or": true, "my": true, "your": true, "our": true, "their": true,
	"its": true, "it": true, "other": true, "another": true, "next": true, "first": true,
}

// nlTableBeforeRe captures the identifier immediately before a SINGULAR "table"
// (e.g. "the customers table" → "customers"). Anchored on the word boundary after
// "table" so it never fires on the plural "tables".
var nlTableBeforeRe = regexp.MustCompile(`([a-z_][a-z0-9_.-]*)\s+table\b`)

// nlTableAfterRe captures the identifier right after "table " (e.g. "table customers").
var nlTableAfterRe = regexp.MustCompile("\\btable\\s+[\"'`]?([a-z_][a-z0-9_.-]*)")

// nlPluralTablesRe detects a plural "tables" mention — a signal of multi-table
// intent that we deliberately leave to the table-selection HITL rather than
// half-parsing.
var nlPluralTablesRe = regexp.MustCompile(`\btables\b`)

// parseTableIntent extracts an explicitly-named SINGLE source table from a chat NL
// message so a one-turn create can skip the table-selection HITL (KI-NLCHAT-
// TABLENAME-IGNORED). Conservative by design: it only fires on the singular
// "<name> table" / "table <name>" shape, drops stop-words, and returns nil the
// moment the phrasing looks multi-table ("all tables", "the users and orders
// tables"). A returned name is NOT trusted directly — createAndRunPipeline
// validates it against the source's cached schema and drops anything unresolved or
// ambiguous back to the HITL, so a connector/connection token that slips through
// here never auto-selects a wrong table. Mirrors parseMaskingIntent (re-run on the
// original request at confirmation time). Returns nil when no table is named.
func parseTableIntent(message string) []string {
	lc := strings.ToLower(message)
	// Multi-table intent ("all tables", "…and orders tables") → let the HITL
	// handle it rather than half-parsing a single name.
	if nlPluralTablesRe.MatchString(lc) {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	add := func(tok string) {
		tok = strings.Trim(tok, " \t.:;,'\"()`")
		if tok == "" || nlTableStopWords[tok] || seen[tok] {
			return
		}
		// Reject a lone dot / trailing-dot artifact from the identifier class.
		if strings.Trim(tok, ".") == "" {
			return
		}
		seen[tok] = true
		out = append(out, tok)
	}
	for _, m := range nlTableBeforeRe.FindAllStringSubmatch(lc, -1) {
		add(m[1])
	}
	for _, m := range nlTableAfterRe.FindAllStringSubmatch(lc, -1) {
		add(m[1])
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// shouldPreselectNamedTables is the safety gate for KI-NLCHAT-TABLENAME-IGNORED: a
// parsed table name is only pre-selected (skipping the HITL) when the source
// schema cache resolved it cleanly — the cache was warm (ok), every name matched
// an existing table (no missing), none was ambiguous across schemas, and at least
// one qualified name survived. Any doubt → false → defer to the table-selection
// HITL rather than risk syncing the wrong table.
func shouldPreselectNamedTables(qualified, missing []string, ambiguous map[string][]string, ok bool) bool {
	return ok && len(missing) == 0 && len(ambiguous) == 0 && len(qualified) > 0
}

// SendMessageNLPipeline handles chat messages with NL-driven pipeline flow
// V3: Multi-turn conversation with slot-filling and confirmation gating
// Uses conversation state machine to prevent wrong connector guesses
func (h *ChatHandler) SendMessageNLPipeline(c *gin.Context) {
	var req ChatMessageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		// Return a clean client-facing error rather than gin's raw validation
		// string, which leaks the internal struct field path.
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request: 'message' is required"})
		return
	}

	// Resolve user ID consistently with other handlers (connections/pipelines).
	userIDStr, _ := resolveUserID(c)
	c.Set("user_id", userIDStr)

	// Prefer trace id from middleware/span context
	otelCtx := telemetry.GetOTelContext(c)
	traceID := telemetry.TraceIDFromContext(otelCtx)
	if traceID == "" || traceID == "00000000000000000000000000000000" {
		traceID = telemetry.NormalizeTraceID(c.GetHeader("X-Trace-ID"))
		if traceID == "" || traceID == "00000000000000000000000000000000" {
			traceID = telemetry.NormalizeTraceID(uuid.New().String())
		}
	}

	// Resolve session ID (used as conversation_id). Accept both the
	// top-level `session_id` field (defensive convenience for API callers)
	// and the legacy nested context.session_id form that the frontend
	// uses today. A fresh session is generated only when neither is
	// supplied — without this, multi-turn chat state (intent →
	// awaiting_confirmation → pipeline_started) silently resets on every
	// request and the user can never complete the confirm step.
	sessionID := strings.TrimSpace(req.SessionID)
	if sessionID == "" && req.Context != nil {
		if sid, ok := req.Context["session_id"].(string); ok {
			sessionID = strings.TrimSpace(sid)
		}
	}
	if sessionID == "" {
		sessionID = fmt.Sprintf("session-%d", time.Now().Unix())
	}

	log.WithFields(log.Fields{
		"trace_id":   traceID,
		"session_id": sessionID,
		// Scrub the user message before it ships to the log store — chat text can
		// carry user-typed PII (SSNs, card numbers) that must not land in plaintext.
		"message": llmscrub.Scrub(req.Message),
		"user_id": userIDStr,
	}).Info("📩 Received chat message (NL Pipeline flow)")

	// Load or create conversation context
	ctx := c.Request.Context()
	conv, err := h.getOrCreateConversation(ctx, userIDStr, sessionID)
	if err != nil {
		log.WithError(err).Warn("Failed to load conversation context")
		// Continue with fresh context
		conv = chat.NewConversationContext(userIDStr, sessionID)
	}

	// Update message count
	conv.IncMessageCount()
	conv.SetLastMessage(req.Message)

	// Route based on conversation state
	var response ChatMessageResponse
	switch conv.GetState() {
	case chat.StateAwaitingSource, chat.StateAwaitingDestination:
		response = h.handleSlotFilling(ctx, c, conv, req.Message, traceID, sessionID)
	case chat.StateAwaitingRole:
		response = h.handleRoleClarification(ctx, c, conv, req.Message, traceID, sessionID, userIDStr)
	case chat.StateAwaitingConfirmation:
		response = h.handleConfirmation(ctx, c, conv, req.Message, traceID, sessionID, userIDStr)
	default: // StateIdle
		response = h.handleNewIntent(ctx, c, conv, req.Message, traceID, sessionID, userIDStr)
	}

	// F-24: honor autosend.
	// When the caller (typically a non-interactive API harness) sets
	// {"autosend": true} and the new-intent path produced an unambiguous
	// confirmation response (both source+destination resolved), short-circuit
	// the human "yes" turn by replaying handleConfirmation with a synthetic
	// "yes". This mirrors what the UI's "Start pipeline" button does — it
	// just POSTs "yes" with the same session_id — but keeps the harness's
	// single-shot semantics intact (no need to chain a second request, which
	// would race the session-cache write).
	if req.Autosend && response.Type == "confirmation" && conv.GetState() == chat.StateAwaitingConfirmation {
		pi := conv.GetPendingIntent()
		if pi != nil && pi.SourceType != "" && pi.DestinationType != "" {
			log.WithFields(log.Fields{
				"session_id":       sessionID,
				"source_type":      pi.SourceType,
				"destination_type": pi.DestinationType,
			}).Info("⚡ autosend=true — skipping confirmation step and starting pipeline")
			response = h.handleConfirmation(ctx, c, conv, "yes", traceID, sessionID, userIDStr)
		}
	}

	// Save updated conversation context
	conv.SetLastResponse(response.Message)
	if err := h.saveConversation(ctx, conv); err != nil {
		log.WithError(err).Warn("Failed to save conversation context")
	}

	// Include session_id in response metadata for frontend
	if response.Metadata == nil {
		response.Metadata = make(map[string]interface{})
	}
	response.Metadata["session_id"] = sessionID
	response.Metadata["conversation_state"] = string(conv.GetState())

	c.JSON(http.StatusOK, response)
}

// handleNewIntent processes a new message when conversation is idle
func (h *ChatHandler) handleNewIntent(ctx context.Context, c *gin.Context, conv *chat.ConversationContext, message, traceID, sessionID, userID string) ChatMessageResponse {
	// Fast path: "why did pipeline X fail" / "diagnose execution Y". This
	// runs before the LLM so the common diagnose case costs zero tokens.
	// Composes Phase 3 (Diagnoser) + Phase 5 (intent routing).
	if reply, handled := h.maybeHandleDiagnoseCommand(ctx, message, activeWorkspaceID(c), traceID); handled {
		conv.SetState(chat.StateIdle)
		conv.SetPendingIntent(nil)
		return reply
	}

	// Fast path: explicit "generate <connector>" command. We surface this as
	// the next step from the connector_missing response, so users can launch
	// generation without leaving the chat. The actual generation runs async
	// against tool-generator (1–3 min); we ack immediately.
	if reply, handled := h.maybeHandleGenerateCommand(ctx, message, traceID); handled {
		conv.SetState(chat.StateIdle)
		conv.SetPendingIntent(nil)
		return reply
	}

	// Fast path: if the message clearly looks like "<connector> to <connector>", trust deterministic parsing.
	// This prevents misclassification like "postgresql to mysql" falling into the help flow.
	intent := h.quickParseDataSyncIntent(message)

	// Fast path: exactly one connector named ("sync mysql all data", "load data
	// into postgresql"). Infer its role deterministically so we route to the
	// correct slot-filling question instead of the unreliable LLM. Skip when the
	// message reads as a help/how-to question ("what is mysql") — those belong in
	// the help flow below. When a connector is named but its role is ambiguous
	// (bare "mysql"), ask the user which side it is before continuing.
	if intent == nil && !looksLikeHelpRequest(message) {
		if scIntent, ambiguousConn := h.quickParseSingleConnectorIntent(message); scIntent != nil {
			intent = scIntent
		} else if ambiguousConn != "" {
			pendingIntent := &chat.PendingIntent{
				Action:              "data_sync",
				SyncMode:            "auto",
				OriginalRequest:     message,
				UnresolvedConnector: ambiguousConn,
			}
			conv.SetPendingIntent(pendingIntent)
			conv.SetState(chat.StateAwaitingRole)
			friendly := getFriendlyName(ambiguousConn)
			return ChatMessageResponse{
				Message:   fmt.Sprintf("Got it — is **%s** your data **source** or **destination**?", friendly),
				Type:      "slot_filling",
				TraceID:   traceID,
				Timestamp: time.Now().Format(time.RFC3339),
				Data: map[string]interface{}{
					"missing_slot":         "role",
					"unresolved_connector": ambiguousConn,
				},
				Suggestions: []string{
					fmt.Sprintf("%s is my source", friendly),
					fmt.Sprintf("%s is my destination", friendly),
				},
			}
		}
	}

	if intent == nil {
		// Call LLM for intent classification
		var err error
		intent, err = h.parseIntent(ctx, message)
		if err != nil {
			log.WithError(err).Warn("Failed to parse intent")
			return ChatMessageResponse{
				Message: "I can help you move data between systems. Try something like:\n\n" +
					"• **\"Sync MySQL to BigQuery\"**\n" +
					"• **\"Copy PostgreSQL orders table to S3 every hour\"**\n" +
					"• **\"Stream changes from MySQL to Snowflake\"**\n\n" +
					"What data would you like to move?",
				Type:      "text",
				TraceID:   traceID,
				Timestamp: time.Now().Format(time.RFC3339),
				Suggestions: []string{
					"mysql to s3",
					"postgresql to bigquery",
					"mysql to snowflake",
				},
			}
		}
	}

	intentName := strings.ToLower(strings.TrimSpace(intent.IntentName))

	looksHelpish := looksLikeHelpRequest(message)
	// Respect the prompt contract: some user messages are informational/help requests and should not
	// enter the pipeline-creation slot-filling state machine.
	// Also treat "how/help" questions without both connectors as non-execution help, even if the classifier is noisy.
	if !intent.RequiresExecution || intentName == "general_knowledge" || (looksHelpish && (strings.TrimSpace(intent.SourceType) == "" || strings.TrimSpace(intent.DestinationType) == "")) {
		conv.SetState(chat.StateIdle)
		conv.SetPendingIntent(nil)

		helpMsg, helpSuggestions, helpErr := h.callHelpResponseLLM(ctx, message)
		if helpErr == nil && strings.TrimSpace(helpMsg) != "" {
			return ChatMessageResponse{
				Message:     helpMsg,
				Type:        "text",
				TraceID:     traceID,
				Timestamp:   time.Now().Format(time.RFC3339),
				Suggestions: helpSuggestions,
			}
		}

		return ChatMessageResponse{
			Message:   "To create a pipeline, tell me **your source** and **destination**.\n\nExamples:\n- `mysql to aws-s3`\n- `postgres to bigquery`\n- `replicate mongodb to snowflake` (CDC)\n\nIf you want, tell me what you’re moving (e.g. “users and orders”) and whether it should be **batch** or **CDC**.",
			Type:      "text",
			TraceID:   traceID,
			Timestamp: time.Now().Format(time.RFC3339),
			Suggestions: []string{
				"mysql to aws-s3",
				"postgres to bigquery",
				"mysql users table to snowflake",
				"stream postgres to redshift",
			},
		}
	}

	// For now, only the data_sync flow is implemented in this NL pipeline handler.
	if intentName != "data_sync" {
		conv.SetState(chat.StateIdle)
		conv.SetPendingIntent(nil)

		// If user is asking for guidance, give a dynamic help response rather than a hard-coded limitation.
		if looksHelpish {
			helpMsg, helpSuggestions, helpErr := h.callHelpResponseLLM(ctx, message)
			if helpErr == nil && strings.TrimSpace(helpMsg) != "" {
				return ChatMessageResponse{
					Message:     helpMsg,
					Type:        "text",
					TraceID:     traceID,
					Timestamp:   time.Now().Format(time.RFC3339),
					Suggestions: helpSuggestions,
				}
			}
		}

		return ChatMessageResponse{
			Message:   "I can help you create and run data sync pipelines right now. Tell me the source and destination (e.g. `mysql to s3`).",
			Type:      "text",
			TraceID:   traceID,
			Timestamp: time.Now().Format(time.RFC3339),
			Suggestions: []string{
				"mysql to aws-s3",
				"postgres to bigquery",
			},
		}
	}

	// Validate extracted connector names against stopwords and catalog.
	// We track separately whether the LLM named something that LOOKS like a
	// connector but isn't in our catalog — that lets us guide the user toward
	// generating it instead of silently dropping the request.
	rawSrc := strings.TrimSpace(intent.SourceType)
	rawDst := strings.TrimSpace(intent.DestinationType)
	sourceValid := chat.IsValidConnectorName(intent.SourceType) && h.isKnownConnector(intent.SourceType)
	destValid := chat.IsValidConnectorName(intent.DestinationType) && h.isKnownConnector(intent.DestinationType)

	// Phase 5 — IntentResolver fallback.
	// When the LLM extracted a connector name that isn't recognised, try
	// the keyword/alias index over the full user message before giving up.
	// This fixes cases like "sync shopify to postgres" where the LLM returns
	// "shopify" but the catalog key is "shopify-admin-graphql".
	if !sourceValid && rawSrc != "" {
		resolver := newDiskIntentResolver()
		if m := resolver.ResolveSource(message); m.MatchedConnectorType != "" && h.isKnownConnector(m.MatchedConnectorType) {
			intent.SourceType = m.MatchedConnectorType
			sourceValid = true
			log.WithFields(log.Fields{"raw": rawSrc, "resolved": m.MatchedConnectorType}).
				Debug("IntentResolver: resolved source via alias index")
		}
	}
	if !destValid && rawDst != "" {
		resolver := newDiskIntentResolver()
		if m := resolver.ResolveDestination(message); m.MatchedConnectorType != "" && h.isKnownConnector(m.MatchedConnectorType) {
			intent.DestinationType = m.MatchedConnectorType
			destValid = true
			log.WithFields(log.Fields{"raw": rawDst, "resolved": m.MatchedConnectorType}).
				Debug("IntentResolver: resolved destination via alias index")
		}
	}

	// Connection-name fallback. The user may have named one of their own
	// CONNECTIONS (e.g. "azure-pg-test-dst") rather than a connector type. Adopt
	// that connection's connector_type so we don't mistake a known connection
	// for an unknown connector and offer to "generate" it. The downstream
	// checkConnections name-scan then resolves the concrete connection id.
	if !sourceValid && rawSrc != "" {
		if ct := h.connectorTypeForConnectionName(activeWorkspaceID(c), "source", rawSrc); ct != "" && h.isKnownConnector(ct) {
			intent.SourceType = ct
			sourceValid = true
			log.WithFields(log.Fields{"named_connection": rawSrc, "connector_type": ct}).
				Info("Resolved source connector type from named connection")
		}
	}
	if !destValid && rawDst != "" {
		if ct := h.connectorTypeForConnectionName(activeWorkspaceID(c), "destination", rawDst); ct != "" && h.isKnownConnector(ct) {
			intent.DestinationType = ct
			destValid = true
			log.WithFields(log.Fields{"named_connection": rawDst, "connector_type": ct}).
				Info("Resolved destination connector type from named connection")
		}
	}

	// Re-read raw values after potential resolver override.
	rawSrc = strings.TrimSpace(intent.SourceType)
	rawDst = strings.TrimSpace(intent.DestinationType)

	sourceUnknown := !sourceValid && rawSrc != "" && chat.IsValidConnectorName(intent.SourceType)
	destUnknown := !destValid && rawDst != "" && chat.IsValidConnectorName(intent.DestinationType)

	// "Did you mean?" — try fuzzy-matching unknown connector tokens against the catalog
	// before giving up and offering connector generation.
	if sourceUnknown || destUnknown {
		suggestedSrc := rawSrc
		suggestedDst := rawDst
		srcCorrected := false
		dstCorrected := false

		if sourceUnknown {
			if match, ok := h.fuzzyMatchKnownConnector(rawSrc); ok {
				suggestedSrc = match
				srcCorrected = true
			}
		}
		if destUnknown {
			if match, ok := h.fuzzyMatchKnownConnector(rawDst); ok {
				suggestedDst = match
				dstCorrected = true
			}
		}

		if srcCorrected || dstCorrected {
			// At least one correction found — show a suggestion.
			// If both sides are now known, it's a full correction; otherwise partial.
			bothKnown := (srcCorrected || sourceValid) && (dstCorrected || destValid)
			suggestionChip := fmt.Sprintf("%s to %s", suggestedSrc, suggestedDst)
			msg := fmt.Sprintf("Did you mean **%s to %s**?", suggestedSrc, suggestedDst)
			if !bothKnown {
				msg += "\n\nIf not, pick a supported connector below."
			}
			return ChatMessageResponse{
				Message:   msg,
				Type:      "text",
				TraceID:   traceID,
				Timestamp: time.Now().Format(time.RFC3339),
				Suggestions: append(
					[]string{suggestionChip},
					h.filterKnownSuggestions(h.getSuggestionsForSlot(chat.StateAwaitingSource))...,
				),
			}
		}
	}

	if sourceUnknown || destUnknown {
		// User asked for a connector we don't have. Tell them clearly and
		// offer the generator path instead of pretending we didn't hear them.
		// Preserve the user's original casing where possible: the LLM tends
		// to lowercase the connector name, but the user typed e.g. "QuickBooks"
		// — getFriendlyName falls back to title-case for unknown slugs which
		// reads better than a flat lowercase string.
		missingHuman := []string{}
		missingNames := []string{}
		srcDisplay := preserveCasing(message, rawSrc)
		dstDisplay := preserveCasing(message, rawDst)
		if sourceUnknown {
			missingHuman = append(missingHuman, fmt.Sprintf("`%s` (source)", srcDisplay))
			missingNames = append(missingNames, srcDisplay)
		}
		if destUnknown {
			missingHuman = append(missingHuman, fmt.Sprintf("`%s` (destination)", dstDisplay))
			missingNames = append(missingNames, dstDisplay)
		}
		// Defensive fallback: should never be empty here, but if it somehow
		// is (e.g. an LLM intent parsing edge case), we still render a
		// readable sentence instead of " (source)".
		humanList := strings.Join(missingHuman, " and ")
		generateList := strings.Join(missingNames, " and ")
		if humanList == "" {
			humanList = "the connector you asked for"
		}
		if generateList == "" {
			generateList = strings.TrimSpace(message)
		}
		return ChatMessageResponse{
			Message: fmt.Sprintf(
				"I don't have a connector for %s yet. I can generate one for you — it usually takes 1–3 minutes and runs in the background. Reply **\"generate %s\"** to start, or pick a supported source below.",
				humanList,
				generateList,
			),
			Type:      "connector_missing",
			TraceID:   traceID,
			Timestamp: time.Now().Format(time.RFC3339),
			Data: map[string]interface{}{
				"missing_connectors":  missingHuman,
				"missing_names":       missingNames,
				"source_unknown":      sourceUnknown,
				"destination_unknown": destUnknown,
				"raw_source":          rawSrc,
				"raw_destination":     rawDst,
				"action":              "offer_generation",
			},
			// Prepend a clickable "Generate <name>" suggestion so users can act
			// without retyping. Other suggestions are the supported alternatives.
			Suggestions: append(
				[]string{fmt.Sprintf("generate %s", generateList)},
				h.filterKnownSuggestions(h.getSuggestionsForSlot(chat.StateAwaitingSource))...,
			),
		}
	}

	// Normalize connector names
	if sourceValid {
		intent.SourceType = chat.NormalizeConnectorName(intent.SourceType)
	}
	if destValid {
		intent.DestinationType = chat.NormalizeConnectorName(intent.DestinationType)
	}

	log.WithFields(log.Fields{
		"intent":       intent.IntentName,
		"source":       intent.SourceType,
		"destination":  intent.DestinationType,
		"source_valid": sourceValid,
		"dest_valid":   destValid,
	}).Info("🔍 Intent classification result")

	// Create pending intent
	syncMode := intent.SyncMode
	if syncMode == "" {
		syncMode = "auto"
	}
	pendingIntent := &chat.PendingIntent{
		Action:          intent.IntentName,
		SyncMode:        syncMode,
		Tables:          intent.Tables,
		OriginalRequest: message,
	}

	// Determine what's missing
	if sourceValid && destValid {
		// Both valid - ask for confirmation
		pendingIntent.SourceType = intent.SourceType
		pendingIntent.DestinationType = intent.DestinationType
		conv.SetPendingIntent(pendingIntent)
		conv.SetState(chat.StateAwaitingConfirmation)

		supportedSyncModes := supportedSyncModesForSource(intent.SourceType)
		sourceSupportsCDC := connectorSupportsCDC(intent.SourceType)
		sourceSupportsIncrementalBatch := connectorSupportsIncrementalBatch(intent.SourceType)
		return ChatMessageResponse{
			Message:   fmt.Sprintf("I understood: **%s → %s**. Create and run this pipeline?", getFriendlyName(intent.SourceType), getFriendlyName(intent.DestinationType)),
			Type:      "confirmation",
			TraceID:   traceID,
			Timestamp: time.Now().Format(time.RFC3339),
			Data: map[string]interface{}{
				"source_type":                       intent.SourceType,
				"destination_type":                  intent.DestinationType,
				"supported_sync_modes":              supportedSyncModes,
				"source_supports_cdc":               sourceSupportsCDC,
				"source_supports_incremental_batch": sourceSupportsIncrementalBatch,
				"pending_intent":                    pendingIntent,
			},
		}
	}

	if sourceValid {
		// Only source is valid - ask for destination
		pendingIntent.SourceType = intent.SourceType
		conv.SetPendingIntent(pendingIntent)
		conv.SetState(chat.StateAwaitingDestination)

		return ChatMessageResponse{
			Message:   fmt.Sprintf("Got it! You want to sync from **%s**. Where would you like to send the data?", getFriendlyName(intent.SourceType)),
			Type:      "slot_filling",
			TraceID:   traceID,
			Timestamp: time.Now().Format(time.RFC3339),
			Data: map[string]interface{}{
				"source_type":  intent.SourceType,
				"missing_slot": "destination",
			},
			Suggestions: h.filterKnownSuggestions(h.getSuggestionsForSlot(chat.StateAwaitingDestination)),
		}
	}

	if destValid {
		// Only destination is valid - ask for source
		pendingIntent.DestinationType = intent.DestinationType
		conv.SetPendingIntent(pendingIntent)
		conv.SetState(chat.StateAwaitingSource)

		return ChatMessageResponse{
			Message:   fmt.Sprintf("Got it! You want to sync to **%s**. What's your data source?", getFriendlyName(intent.DestinationType)),
			Type:      "slot_filling",
			TraceID:   traceID,
			Timestamp: time.Now().Format(time.RFC3339),
			Data: map[string]interface{}{
				"destination_type": intent.DestinationType,
				"missing_slot":     "source",
			},
			Suggestions: h.filterKnownSuggestions(h.getSuggestionsForSlot(chat.StateAwaitingSource)),
		}
	}

	// Neither is valid - ask for source first
	conv.SetPendingIntent(pendingIntent)
	conv.SetState(chat.StateAwaitingSource)

	return ChatMessageResponse{
		Message:   "I'd be happy to help you create a data pipeline! What's your data source?",
		Type:      "slot_filling",
		TraceID:   traceID,
		Timestamp: time.Now().Format(time.RFC3339),
		Data: map[string]interface{}{
			"missing_slot": "source",
		},
		Suggestions: h.filterKnownSuggestions(h.getSuggestionsForSlot(chat.StateAwaitingSource)),
	}
}

var (
	reFromToPair = regexp.MustCompile(`(?i)\bfrom\s+([a-z0-9][a-z0-9_-]*)\s+(?:to|->|→)\s*([a-z0-9][a-z0-9_-]*)\b`)
	reToPair     = regexp.MustCompile(`(?i)\b([a-z0-9][a-z0-9_-]*)\s*(?:to|->|→)\s*([a-z0-9][a-z0-9_-]*)\b`)
)

// reToTypo matches common single-word typos of the preposition "to" (e.g. "tod", "tpo").
// Anchored to word boundaries so connector names like "top" are not affected.
var reToTypo = regexp.MustCompile(`\b(tod|tpo|tto|t0)\b`)

func canonicalizeForPairParse(message string) string {
	// Normalize common multi-word connector mentions so regex can catch them.
	// Also collapse repeated whitespace so phrases like "aws  s3" normalize correctly.
	lc := strings.ToLower(strings.TrimSpace(message))
	for strings.Contains(lc, "  ") {
		lc = strings.ReplaceAll(lc, "  ", " ")
	}
	repl := strings.NewReplacer(
		"amazon s3", "amazon-s3",
		"aws s3", "aws-s3",
		"sql server", "sql-server",
	)
	lc = repl.Replace(lc)
	// Fix common "to" preposition typos so the pair regex can still match.
	lc = reToTypo.ReplaceAllString(lc, "to")
	return lc
}

func (h *ChatHandler) quickParseDataSyncIntent(message string) *Intent {
	m := canonicalizeForPairParse(message)

	tryMatch := func(re *regexp.Regexp) (string, string, bool) {
		matches := re.FindStringSubmatch(m)
		if len(matches) != 3 {
			return "", "", false
		}
		src := chat.NormalizeConnectorName(matches[1])
		dst := chat.NormalizeConnectorName(matches[2])
		if src == "" || dst == "" {
			return "", "", false
		}
		if !h.isKnownConnector(src) || !h.isKnownConnector(dst) {
			return "", "", false
		}
		return src, dst, true
	}

	// Prefer explicit "from X to Y"
	if src, dst, ok := tryMatch(reFromToPair); ok {
		return &Intent{
			IntentName:        "data_sync",
			RequiresExecution: true,
			SourceType:        src,
			DestinationType:   dst,
			SyncMode:          "auto",
			Tables:            nil,
		}
	}
	// Fallback to "X to Y"
	if src, dst, ok := tryMatch(reToPair); ok {
		return &Intent{
			IntentName:        "data_sync",
			RequiresExecution: true,
			SourceType:        src,
			DestinationType:   dst,
			SyncMode:          "auto",
			Tables:            nil,
		}
	}
	// Token-stream fallback. The adjacency-based regexes above miss pairs where a
	// CONNECTION NAME sits between the connector type and the preposition, e.g.
	// "postgresql test-pg-src-batch to postgresql test-pg-dst" — reToPair captures
	// "test-pg-src-batch to postgresql" and bails because the connection name isn't
	// a known connector. Without this recovery such a message falls to the single-
	// connector path, which for a SAME-type pair collapses both endpoints to one
	// connector and re-asks for the destination the user already named (OBS-2).
	if src, dst, ok := h.parseConnectorPairFromTokens(message); ok {
		return &Intent{
			IntentName:        "data_sync",
			RequiresExecution: true,
			SourceType:        src,
			DestinationType:   dst,
			SyncMode:          "auto",
			Tables:            nil,
		}
	}
	return nil
}

// knownConnectorToken returns the canonical connector id for a single token when it
// names a connector in the catalog, or "" otherwise.
func (h *ChatHandler) knownConnectorToken(tok string) string {
	if !chat.IsValidConnectorName(tok) {
		return ""
	}
	norm := chat.NormalizeConnectorName(tok)
	if !h.isKnownConnector(norm) {
		return ""
	}
	return norm
}

// parseConnectorPairFromTokens recovers a source→destination connector pair from the
// token stream when a connection NAME sits between the connector type and the
// directional preposition (e.g. "postgresql test-pg-src-batch to postgresql
// test-pg-dst"), which defeats the adjacency-based reToPair/reFromToPair regexes. It
// locates the first destination-direction pivot ("to"/"into"/"onto"/"->"/"→") and
// returns the nearest KNOWN connector strictly before it as the source and the
// nearest KNOWN connector strictly after it as the destination. Returns ok=false
// unless a known connector is found on BOTH sides of the pivot, so single-connector
// phrasings ("load data into postgresql") still defer to the slot-filling path.
func (h *ChatHandler) parseConnectorPairFromTokens(message string) (src, dst string, ok bool) {
	tokens := tokenizeForConnectorScan(message)
	pivot := -1
	for i, t := range tokens {
		if destPreps[t] {
			pivot = i
			break
		}
	}
	if pivot <= 0 || pivot >= len(tokens)-1 {
		return "", "", false
	}
	for i := pivot - 1; i >= 0 && src == ""; i-- {
		src = h.knownConnectorToken(tokens[i])
	}
	for i := pivot + 1; i < len(tokens) && dst == ""; i++ {
		dst = h.knownConnectorToken(tokens[i])
	}
	if src == "" || dst == "" {
		return "", "", false
	}
	return src, dst, true
}

// ── Single-connector intent parsing ──────────────────────────────────────────
//
// When the user names exactly ONE connector ("sync mysql all data", "load data
// into postgresql") there is no "X to Y" pair for quickParseDataSyncIntent to
// catch, so the request used to fall through to the LLM — whose source/destination
// role assignment is unreliable. These helpers deterministically (a) find the lone
// connector and (b) infer whether it is the source or the destination from the
// directional preposition adjacent to it and the governing verb, so the existing
// slot-filling routing can then ask the user for the missing side.

// destinationVerbs / sourceVerbs classify the leading action when no directional
// preposition sits next to the connector. "load/import/push into X" → X is the
// destination; "sync/export/extract from X" → X is the source.
var destinationVerbs = map[string]bool{
	"load": true, "import": true, "insert": true, "write": true, "ingest": true,
	"populate": true, "push": true, "upload": true, "send": true, "sink": true,
	"store": true, "save": true, "put": true,
}

var sourceVerbs = map[string]bool{
	"sync": true, "export": true, "extract": true, "read": true, "pull": true,
	"replicate": true, "migrate": true, "move": true, "get": true, "fetch": true,
	"stream": true, "dump": true, "archive": true, "backup": true, "copy": true,
}

// directional prepositions that, when they immediately precede the connector,
// pin its role deterministically.
var sourcePreps = map[string]bool{"from": true, "off": true}
var destPreps = map[string]bool{"to": true, "into": true, "onto": true, "->": true, "→": true}

// tokenizeForConnectorScan canonicalizes the message (folding "aws s3"→"aws-s3"
// etc.) then splits it into lowercased word tokens with surrounding punctuation
// stripped, so connector detection and role inference share one token stream.
func tokenizeForConnectorScan(message string) []string {
	canon := canonicalizeForPairParse(message)
	raw := strings.FieldsFunc(canon, func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\n' || r == ',' || r == ';' ||
			r == '.' || r == '!' || r == '?' || r == '(' || r == ')' ||
			r == '"' || r == '\''
	})
	out := make([]string, 0, len(raw))
	for _, t := range raw {
		t = strings.TrimSpace(t)
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

// detectSingleKnownConnector scans the message for known connectors. It returns
// the canonical connector id, its token index, the full token stream, and the
// count of DISTINCT known connectors found. Callers use distinctCount to decide:
// 0 → no connector (fall to LLM/help), 1 → single-connector path, ≥2 → a pair
// (handled earlier by quickParseDataSyncIntent / the LLM).
func (h *ChatHandler) detectSingleKnownConnector(message string) (id string, tokenIdx int, tokens []string, distinctCount int) {
	tokens = tokenizeForConnectorScan(message)
	seen := map[string]bool{}
	firstID := ""
	firstIdx := -1
	for i, tok := range tokens {
		if !chat.IsValidConnectorName(tok) {
			continue
		}
		norm := chat.NormalizeConnectorName(tok)
		if !h.isKnownConnector(norm) {
			continue
		}
		if !seen[norm] {
			seen[norm] = true
			if firstID == "" {
				firstID = norm
				firstIdx = i
			}
		}
	}
	return firstID, firstIdx, tokens, len(seen)
}

// inferConnectorRole decides whether the lone connector at tokens[idx] is the
// pipeline's "source" or "destination". Returns "" when the phrasing is
// genuinely ambiguous (e.g. a bare "mysql"), so the caller can ask the user.
//
// Precedence: a directional preposition immediately before the connector wins;
// then a directional marker appearing AFTER it ("mysql to …" → mysql is source);
// then the governing verb anywhere in the message.
func inferConnectorRole(tokens []string, idx int) string {
	if idx < 0 || idx >= len(tokens) {
		return ""
	}
	// 1. Preposition immediately before the connector.
	if idx > 0 {
		prev := tokens[idx-1]
		if sourcePreps[prev] {
			return "source"
		}
		if destPreps[prev] {
			return "destination"
		}
	}
	// 2. A directional marker AFTER the connector implies it sits on the left of
	//    an "X to Y" phrase, i.e. it is the source ("mysql to a file").
	for j := idx + 1; j < len(tokens); j++ {
		if destPreps[tokens[j]] {
			return "source"
		}
		if sourcePreps[tokens[j]] {
			// "<conn> from <something>" — rare; the conn is the destination
			// being loaded from elsewhere. Treat as destination.
			return "destination"
		}
	}
	// 3. Governing verb anywhere in the message.
	sawSource, sawDest := false, false
	for _, tok := range tokens {
		if sourceVerbs[tok] {
			sawSource = true
		}
		if destinationVerbs[tok] {
			sawDest = true
		}
	}
	switch {
	case sawSource && !sawDest:
		return "source"
	case sawDest && !sawSource:
		return "destination"
	default:
		return ""
	}
}

// quickParseSingleConnectorIntent handles messages naming exactly one known
// connector. It returns one of:
//   - (intent, "")        : role inferred → intent has exactly one of Source/Dest set
//   - (nil, connectorID)  : single connector but role ambiguous → caller asks the user
//   - (nil, "")           : zero or ≥2 connectors → caller falls through to other paths
//
// nlConnectorNoiseWords are connector-shaped tokens that are really English noise
// (not a user-named connector). Used so hasUnknownConnectorPeer doesn't treat
// "data"/"table" as an unknown connector.
var nlConnectorNoiseWords = map[string]bool{
	"data": true, "table": true, "tables": true, "file": true, "files": true,
	"csv": true, "json": true, "database": true, "db": true, "warehouse": true,
	"everything": true, "stuff": true, "records": true, "rows": true, "schema": true,
	"dataset": true, "datasets": true, "thing": true, "things": true, "it": true,
	"them": true, "these": true, "those": true, "all": true, "some": true,
}

// hasUnknownConnectorPeer reports whether a connector-shaped token that is NOT in
// our catalog sits on the opposite side of a directional preposition from the one
// known connector at knownIdx. That signals a "known + unknown" pair (e.g.
// "quickbooks to postgres", "postgre to mysql") — the single-connector fast path
// must defer to the LLM/connector_missing/fuzzy path so the unknown side is
// surfaced (generate offer / "did you mean") instead of silently asking for the
// counterpart the user already named.
func (h *ChatHandler) hasUnknownConnectorPeer(tokens []string, knownIdx int) bool {
	for i, tok := range tokens {
		if !sourcePreps[tok] && !destPreps[tok] {
			continue
		}
		peerIdx := -1
		switch {
		case knownIdx < i:
			peerIdx = i + 1 // known is left of the pivot; peer is just right of it
		case knownIdx > i:
			peerIdx = i - 1 // known is right of the pivot; peer is just left of it
		default:
			continue
		}
		if peerIdx < 0 || peerIdx >= len(tokens) || peerIdx == knownIdx {
			continue
		}
		peer := tokens[peerIdx]
		if len(peer) < 3 || nlConnectorNoiseWords[peer] {
			continue
		}
		if chat.IsValidConnectorName(peer) && !h.isKnownConnector(chat.NormalizeConnectorName(peer)) {
			return true
		}
	}
	return false
}

func (h *ChatHandler) quickParseSingleConnectorIntent(message string) (*Intent, string) {
	id, idx, tokens, count := h.detectSingleKnownConnector(message)
	if count != 1 {
		return nil, ""
	}
	role := inferConnectorRole(tokens, idx)
	if role == "" {
		return nil, id
	}
	// "known + unknown" pair → defer so the unknown side is surfaced.
	if h.hasUnknownConnectorPeer(tokens, idx) {
		return nil, ""
	}
	intent := &Intent{
		IntentName:        "data_sync",
		RequiresExecution: true,
		SyncMode:          "auto",
	}
	if role == "source" {
		intent.SourceType = id
	} else {
		intent.DestinationType = id
	}
	return intent, ""
}

// parseRoleReply interprets a user's answer to the "is X your source or
// destination?" question. Returns "source", "destination", or "".
func parseRoleReply(message string) string {
	tokens := tokenizeForConnectorScan(message)
	for _, t := range tokens {
		switch t {
		case "source", "src", "from", "origin", "extract", "read":
			return "source"
		case "destination", "dest", "target", "sink", "into", "to", "load", "write":
			return "destination"
		}
	}
	return ""
}

// editConnectorVerbRe matches verbs that signal editing the pending pipeline at
// confirmation ("change/switch the destination to X").
var editConnectorVerbRe = regexp.MustCompile(`(?i)\b(?:change|switch|edit|modify|replace|use|make|set)\b`)

// editFromWordRe detects that an edit targets the source side ("… from mysql").
var editFromWordRe = regexp.MustCompile(`\bfrom\b`)

// tryEditPendingConnector updates the pending pipeline's source or destination in
// place when the user issues an edit at confirmation ("change the destination to
// bigquery"). Returns true when an edit was applied so the caller re-confirms.
func (h *ChatHandler) tryEditPendingConnector(conv *chat.ConversationContext, pi *chat.PendingIntent, message string) bool {
	if pi == nil || !editConnectorVerbRe.MatchString(message) {
		return false
	}
	id, _, _, count := h.detectSingleKnownConnector(message)
	if count != 1 || id == "" {
		return false
	}
	lc := strings.ToLower(message)
	side := "destination"
	if strings.Contains(lc, "source") || editFromWordRe.MatchString(lc) {
		side = "source"
	}
	if side == "source" {
		pi.SourceType = id
	} else {
		pi.DestinationType = id
	}
	conv.SetPendingIntent(pi)
	conv.SetState(chat.StateAwaitingConfirmation)
	return true
}

// looksLikeHelpRequest reports whether the message reads as an informational /
// how-to question rather than a pipeline-creation command. Kept package-level so
// both the new-intent gate and the single-connector fast-path can consult it.
func looksLikeHelpRequest(s string) bool {
	lc := strings.ToLower(strings.TrimSpace(s))
	if lc == "" {
		return false
	}
	if strings.Contains(lc, "how to") || strings.Contains(lc, "how do") || strings.Contains(lc, "how can") {
		return true
	}
	if strings.Contains(lc, "help") || strings.Contains(lc, "guide") {
		return true
	}
	if strings.HasPrefix(lc, "what is") || strings.Contains(lc, "what is ") || strings.Contains(lc, "explain") {
		return true
	}
	return false
}

// callHelpResponseLLM generates a dynamic help response for non-execution (general knowledge) queries.
// It uses a dedicated prompt so responses can be tailored to the user's message while staying safe.
func (h *ChatHandler) callHelpResponseLLM(ctx context.Context, userMessage string) (string, []string, error) {
	llmServiceURL := os.Getenv("LLM_SERVICE_URL")
	if llmServiceURL == "" {
		llmServiceURL = "http://llm-service:5000"
	}

	requestBody := map[string]interface{}{
		"prompt_name": "chat/help_response",
		"variables": map[string]interface{}{
			"user_message": userMessage,
		},
	}
	jsonBody, _ := json.Marshal(requestBody)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/v1/completion", llmServiceURL), bytes.NewBuffer(jsonBody))
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("failed to call help prompt: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", nil, fmt.Errorf("help prompt returned %d: %s", resp.StatusCode, string(body))
	}

	var llmResponse struct {
		Content string `json:"content"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&llmResponse); err != nil {
		return "", nil, fmt.Errorf("failed to decode help prompt response: %w", err)
	}

	var parsed struct {
		Message     string   `json:"message"`
		Suggestions []string `json:"suggestions"`
	}
	if err := json.Unmarshal([]byte(llmResponse.Content), &parsed); err != nil {
		return "", nil, fmt.Errorf("failed to parse help prompt content: %w", err)
	}

	// Keep suggestions small and safe
	sugs := parsed.Suggestions
	if len(sugs) > 6 {
		sugs = sugs[:6]
	}

	return parsed.Message, sugs, nil
}

// handleSlotFilling processes a message when waiting for source or destination
func (h *ChatHandler) handleSlotFilling(ctx context.Context, c *gin.Context, conv *chat.ConversationContext, message, traceID, sessionID string) ChatMessageResponse {
	state := conv.GetState()

	// Call slot-filling LLM prompt
	slotResult, err := h.callSlotFillingLLM(ctx, conv, message)
	if err != nil {
		log.WithError(err).Warn("Slot-filling LLM call failed")
		// Fallback: try to extract connector name directly
		slotResult = h.extractConnectorFromMessage(message, state)
	}

	log.WithFields(log.Fields{
		"extracted_slot":  slotResult.ExtractedSlot,
		"extracted_value": slotResult.ExtractedValue,
		"confidence":      slotResult.Confidence,
	}).Info("🔧 Slot-filling result")

	pendingIntent := conv.GetPendingIntent()
	if pendingIntent == nil {
		pendingIntent = &chat.PendingIntent{OriginalRequest: message}
	} else if strings.TrimSpace(pendingIntent.OriginalRequest) == "" {
		// Best-effort fallback (normally set in handleNewIntent).
		pendingIntent.OriginalRequest = message
	}

	// Apply extracted value
	if slotResult.ExtractedValue != "" && slotResult.Confidence >= 0.5 {
		normalizedValue := chat.NormalizeConnectorName(slotResult.ExtractedValue)

		// Validate against catalog
		if !h.isKnownConnector(normalizedValue) {
			return ChatMessageResponse{
				Message:     fmt.Sprintf("I don't recognize '%s' as a supported connector. Could you try a different one?", slotResult.ExtractedValue),
				Type:        "slot_filling",
				TraceID:     traceID,
				Timestamp:   time.Now().Format(time.RFC3339),
				Suggestions: h.filterKnownSuggestions(h.getSuggestionsForSlot(state)),
			}
		}

		if slotResult.ExtractedSlot == "source" || state == chat.StateAwaitingSource {
			pendingIntent.SourceType = normalizedValue
		} else if slotResult.ExtractedSlot == "destination" || state == chat.StateAwaitingDestination {
			pendingIntent.DestinationType = normalizedValue
		}
		conv.SetPendingIntent(pendingIntent)
	}

	// Check if all slots are filled
	if pendingIntent.SourceType != "" && pendingIntent.DestinationType != "" {
		// Both filled - move to confirmation
		conv.SetState(chat.StateAwaitingConfirmation)

		supportedSyncModes := supportedSyncModesForSource(pendingIntent.SourceType)
		sourceSupportsCDC := connectorSupportsCDC(pendingIntent.SourceType)
		sourceSupportsIncrementalBatch := connectorSupportsIncrementalBatch(pendingIntent.SourceType)
		return ChatMessageResponse{
			Message:   fmt.Sprintf("Perfect! I understood: **%s → %s**. Create and run this pipeline?", getFriendlyName(pendingIntent.SourceType), getFriendlyName(pendingIntent.DestinationType)),
			Type:      "confirmation",
			TraceID:   traceID,
			Timestamp: time.Now().Format(time.RFC3339),
			Data: map[string]interface{}{
				"source_type":                       pendingIntent.SourceType,
				"destination_type":                  pendingIntent.DestinationType,
				"supported_sync_modes":              supportedSyncModes,
				"source_supports_cdc":               sourceSupportsCDC,
				"source_supports_incremental_batch": sourceSupportsIncrementalBatch,
				"pending_intent":                    pendingIntent,
			},
		}
	}

	// Still missing a slot
	if pendingIntent.SourceType == "" {
		conv.SetState(chat.StateAwaitingSource)
		nextQuestion := slotResult.NextQuestion
		if nextQuestion == "" {
			nextQuestion = "Great — what's your data source? (e.g., MySQL, PostgreSQL, MongoDB)"
		}
		return ChatMessageResponse{
			Message:   nextQuestion,
			Type:      "slot_filling",
			TraceID:   traceID,
			Timestamp: time.Now().Format(time.RFC3339),
			Data: map[string]interface{}{
				"missing_slot":     "source",
				"destination_type": pendingIntent.DestinationType,
			},
			Suggestions: h.filterKnownSuggestions(h.getSuggestionsForSlot(chat.StateAwaitingSource)),
		}
	}

	conv.SetState(chat.StateAwaitingDestination)
	nextQuestion := slotResult.NextQuestion
	if nextQuestion == "" {
		nextQuestion = fmt.Sprintf("Great! Where would you like to sync the %s data to?", getFriendlyName(pendingIntent.SourceType))
	}
	return ChatMessageResponse{
		Message:   nextQuestion,
		Type:      "slot_filling",
		TraceID:   traceID,
		Timestamp: time.Now().Format(time.RFC3339),
		Data: map[string]interface{}{
			"missing_slot": "destination",
			"source_type":  pendingIntent.SourceType,
		},
		Suggestions: h.filterKnownSuggestions(h.getSuggestionsForSlot(chat.StateAwaitingDestination)),
	}
}

// handleRoleClarification processes the user's answer to "is X your source or
// destination?" — the StateAwaitingRole step. It assigns the held connector to
// the chosen side, then hands off to the normal slot-filling routing to collect
// the other side. If the user instead types a fresh "X to Y" request, we reset
// and treat it as a new intent.
func (h *ChatHandler) handleRoleClarification(ctx context.Context, c *gin.Context, conv *chat.ConversationContext, message, traceID, sessionID, userID string) ChatMessageResponse {
	// User pivoted to a full new request ("mysql to s3") — start over with it.
	if ni := h.quickParseDataSyncIntent(message); ni != nil && ni.RequiresExecution {
		conv.Reset()
		return h.handleNewIntent(ctx, c, conv, message, traceID, sessionID, userID)
	}

	pendingIntent := conv.GetPendingIntent()
	conn := ""
	if pendingIntent != nil {
		conn = pendingIntent.UnresolvedConnector
	}
	if pendingIntent == nil || conn == "" {
		// Lost the held connector (e.g. cache miss). Restart cleanly.
		conv.Reset()
		return h.handleNewIntent(ctx, c, conv, message, traceID, sessionID, userID)
	}

	// The user may have answered with the OTHER connector instead of a role word
	// (e.g. asked "is mysql source or dest?" and they reply "postgresql"). If a
	// second distinct connector is named, infer roles from the two and move on.
	if otherID, _, _, count := h.detectSingleKnownConnector(message); count == 1 && otherID != conn {
		// Ambiguous which is which; default the originally-named connector to
		// source and the newly-named one to destination (matches reading order).
		conv.Reset()
		pi := &chat.PendingIntent{
			Action:          "data_sync",
			SyncMode:        "auto",
			SourceType:      conn,
			DestinationType: otherID,
			OriginalRequest: pendingIntent.OriginalRequest,
		}
		conv.SetPendingIntent(pi)
		conv.SetState(chat.StateAwaitingConfirmation)
		supportedSyncModes := supportedSyncModesForSource(conn)
		return ChatMessageResponse{
			Message:   fmt.Sprintf("I understood: **%s → %s**. Create and run this pipeline?", getFriendlyName(conn), getFriendlyName(otherID)),
			Type:      "confirmation",
			TraceID:   traceID,
			Timestamp: time.Now().Format(time.RFC3339),
			Data: map[string]interface{}{
				"source_type":                       conn,
				"destination_type":                  otherID,
				"supported_sync_modes":              supportedSyncModes,
				"source_supports_cdc":               connectorSupportsCDC(conn),
				"source_supports_incremental_batch": connectorSupportsIncrementalBatch(conn),
				"pending_intent":                    pi,
			},
		}
	}

	role := parseRoleReply(message)
	if role == "" {
		// Couldn't tell — re-ask, keeping the held connector.
		friendly := getFriendlyName(conn)
		return ChatMessageResponse{
			Message:   fmt.Sprintf("Just to confirm — is **%s** your data **source** (where data comes from) or **destination** (where it goes)?", friendly),
			Type:      "slot_filling",
			TraceID:   traceID,
			Timestamp: time.Now().Format(time.RFC3339),
			Data: map[string]interface{}{
				"missing_slot":         "role",
				"unresolved_connector": conn,
			},
			Suggestions: []string{
				fmt.Sprintf("%s is my source", friendly),
				fmt.Sprintf("%s is my destination", friendly),
			},
		}
	}

	pendingIntent.UnresolvedConnector = ""
	if role == "source" {
		pendingIntent.SourceType = conn
		conv.SetPendingIntent(pendingIntent)
		conv.SetState(chat.StateAwaitingDestination)
		return ChatMessageResponse{
			Message:   fmt.Sprintf("Got it! You want to sync from **%s**. Where would you like to send the data?", getFriendlyName(conn)),
			Type:      "slot_filling",
			TraceID:   traceID,
			Timestamp: time.Now().Format(time.RFC3339),
			Data: map[string]interface{}{
				"source_type":  conn,
				"missing_slot": "destination",
			},
			Suggestions: h.filterKnownSuggestions(h.getSuggestionsForSlot(chat.StateAwaitingDestination)),
		}
	}

	pendingIntent.DestinationType = conn
	conv.SetPendingIntent(pendingIntent)
	conv.SetState(chat.StateAwaitingSource)
	return ChatMessageResponse{
		Message:   fmt.Sprintf("Got it! You want to sync to **%s**. What's your data source?", getFriendlyName(conn)),
		Type:      "slot_filling",
		TraceID:   traceID,
		Timestamp: time.Now().Format(time.RFC3339),
		Data: map[string]interface{}{
			"destination_type": conn,
			"missing_slot":     "source",
		},
		Suggestions: h.filterKnownSuggestions(h.getSuggestionsForSlot(chat.StateAwaitingSource)),
	}
}

// handleConfirmation processes a message when waiting for yes/no confirmation
func (h *ChatHandler) handleConfirmation(ctx context.Context, c *gin.Context, conv *chat.ConversationContext, message, traceID, sessionID, userID string) ChatMessageResponse {
	// If the user starts a new request while we're awaiting confirmation, do NOT keep
	// looping on the previous pending intent (this is the root cause of "every message shows mysql→postgresql").
	// Instead, reset and treat the message as a fresh intent.
	if ni := h.quickParseDataSyncIntent(message); ni != nil && ni.RequiresExecution {
		conv.Reset()
		return h.handleNewIntent(ctx, c, conv, message, traceID, sessionID, userID)
	}

	confirmation := chat.ParseConfirmation(message)

	switch confirmation {
	case chat.ConfirmationYes:
		// User confirmed - create and run pipeline
		pendingIntent := conv.GetPendingIntent()
		if pendingIntent == nil || pendingIntent.SourceType == "" || pendingIntent.DestinationType == "" {
			// Shouldn't happen, but handle gracefully
			conv.Reset()
			return ChatMessageResponse{
				Message:   "Something went wrong. Let's start over. What would you like to sync?",
				Type:      "text",
				TraceID:   traceID,
				Timestamp: time.Now().Format(time.RFC3339),
			}
		}

		// Resolve the concrete source/destination connections up front. Besides
		// sparing the workflow an error-prone NL re-parse, the source connection's
		// stored sync_mode is needed BELOW so chat resolves the pipeline mode the
		// same way the REST/UI create path does (resolveEffectiveSyncMode). We scan
		// OriginalRequest — which carries any user-named connection — matching the
		// text the later attach step used. Best-effort: HITL fills any gap downstream.
		sourceConnID, destConnID := "", ""
		connScanNL := strings.TrimSpace(pendingIntent.OriginalRequest)
		if connScanNL == "" {
			connScanNL = message
		}
		if scid, dcid, connErr := h.checkConnections(activeWorkspaceID(c), connScanNL, pendingIntent.SourceType, pendingIntent.DestinationType); connErr == nil {
			sourceConnID, destConnID = scid, dcid
		} else {
			log.WithError(connErr).WithFields(log.Fields{
				"source_type":      pendingIntent.SourceType,
				"destination_type": pendingIntent.DestinationType,
			}).Warn("⚠️ Could not resolve active connections by connector type; workflow will request HITL connections if needed")
		}

		// KI-NLCHAT-SAME-SRC-DST: refuse a pipeline whose source and destination
		// resolve to the SAME connection — it would replicate a connection onto
		// itself. Fires only when both ids resolved AND are identical, so the
		// legitimate same-type / different-connection case (two distinct postgres
		// connections → different ids) is unaffected, and the HITL-deferral path
		// (empty ids) is skipped by the != "" guard.
		if resp := sameConnectionError(sourceConnID, destConnID, traceID); resp != nil {
			conv.Reset()
			return *resp
		}

		// Determine sync mode for the pipeline from the request-derived signals,
		// MOST-EXPLICIT first (see resolveConfirmationSyncMode). The confirmation
		// message's explicit override (the UI mode button) wins even over a concrete
		// pendingIntent.SyncMode, because that value can be a keyword GUESS from the
		// intent classifier (verb "sync" → batch) that would otherwise silently
		// override an explicit CDC click and run batch (BUG-CDC-1 recurrence — the
		// Suite-C prod defect). If nothing here specifies a mode, fall back to the
		// source connection default (block below), then batch. The capability guard
		// further below still downgrades to batch if the source genuinely can't CDC.
		desiredSyncMode, desiredCDCMode, desiredCDCInitialLoad := resolveConfirmationSyncMode(
			message, pendingIntent.SyncMode, pendingIntent.OriginalRequest,
		)
		// When the request itself is silent on the mode, inherit the source
		// connection's configured default — mirroring the REST/UI create path
		// (resolveEffectiveSyncMode: explicit request mode > source connection
		// sync_mode > batch). Without this a CDC-configured source synced from chat
		// silently ran as batch, and a "schedule every day" phrase produced a
		// scheduled BATCH pipeline instead of being ignored+warned as CDC (OBS-3).
		// An explicit "batch"/"one-time" or "cdc"/"stream" in the message still wins.
		if desiredSyncMode == "" || desiredSyncMode == "auto" {
			if connSync, connCDC := getSourceConnectionModes(db.GetDB(), sourceConnID); connSync == "cdc" || connSync == "batch" {
				desiredSyncMode = connSync
				if connCDC != "" {
					desiredCDCMode = connCDC
				}
			}
		}
		if desiredSyncMode == "cdc" && desiredCDCMode == "" {
			desiredCDCMode = "initial"
		}

		// Enforce connector capabilities: if source can't CDC, force batch.
		if desiredSyncMode == "cdc" && !connectorSupportsCDC(pendingIntent.SourceType) {
			desiredSyncMode = "batch"
			desiredCDCMode = ""
		}

		// Always lead the NL request with the resolved source→destination pair so the
		// orchestrator's deterministic fast-path (extractExplicitSourceDest) builds a
		// 2-step intent without involving the LLM planner.
		//
		// Why: in a slot-filled conversation the two connectors arrive in separate turns
		// ("load data into postgresql" → "mysql", or "sync from mysql" → "postgresql"),
		// but pendingIntent.OriginalRequest keeps only the FIRST message — which names a
		// single connector. Sending that lone-connector NL to /plan makes the planner emit
		// a <2-step plan, which the orchestrator rejects with [DETERMINISTIC:INTENT_FAILED]
		// ("plan has insufficient steps", intent.go:418). The old `OriginalRequest == ""`
		// fallback never caught this because the string is non-empty, just single-connector.
		//
		// SourceType/DestinationType are both resolved by confirmation time (the UI shows
		// "MySQL → PostgreSQL"), so the canonical pair is always available. We still append
		// the original phrasing — when it adds detail beyond the pair — so downstream can
		// infer tables/transforms.
		canonicalNL := fmt.Sprintf("Sync %s to %s", pendingIntent.SourceType, pendingIntent.DestinationType)
		if desiredSyncMode == "cdc" {
			canonicalNL = fmt.Sprintf("Stream CDC from %s to %s", pendingIntent.SourceType, pendingIntent.DestinationType)
		}
		requestNL := canonicalNL
		if orig := strings.TrimSpace(pendingIntent.OriginalRequest); orig != "" && !strings.EqualFold(orig, canonicalNL) {
			requestNL = canonicalNL + ". " + orig
		}

		// Reset conversation state before creating pipeline
		conv.Reset()

		// Parse schedule intent from the ORIGINAL request — the fast-path/autosend
		// confirmation `message` is a synthetic "yes" and loses it, exactly like
		// sync-mode above. Schedule-only applies to BATCH: create the pipeline and
		// let the first run fire at the scheduled time instead of immediately. CDC
		// streams continuously, so a schedule on a CDC pipeline is ignored + warned.
		// Parse the schedule from the ORIGINAL request first; if none was expressed
		// there, honor one added only at the confirmation turn ("yes, schedule
		// daily") — the OriginalRequest predates that instruction.
		schedType, schedSpec, hasSchedule := parseScheduleIntent(pendingIntent.OriginalRequest)
		if !hasSchedule {
			schedType, schedSpec, hasSchedule = parseScheduleIntent(message)
		}
		scheduleOnly := hasSchedule && desiredSyncMode != "cdc"
		scheduleWarn := ""
		if hasSchedule && desiredSyncMode == "cdc" {
			scheduleWarn = "\n\n⚠️ CDC pipelines stream continuously, so the schedule was ignored — changes replicate in real time."
		}

		// Parse NL transform intent (masking today; type-conversion in a follow-up)
		// from the ORIGINAL request — the fast-path/autosend confirmation `message`
		// is a synthetic "yes" and loses it, exactly like sync-mode above.
		// Parse transform intent (masking / type-conversion) from BOTH the original
		// request AND the confirmation message, then union. A masking instruction
		// added only at the confirmation turn ("yes but mask email") would otherwise
		// be silently dropped — writing the PII the user asked to hide to the
		// destination in plaintext.
		maskCols, maskPII := parseMaskingIntent(pendingIntent.OriginalRequest)
		if mc, mp := parseMaskingIntent(message); len(mc) > 0 || mp {
			maskCols = mergeUniqueColumns(maskCols, mc)
			maskPII = maskPII || mp
		}
		nlSpec := nlTransformSpec{
			MaskColumns: maskCols,
			MaskPII:     maskPII,
			TypeConvert: parseTypeConvertIntent(pendingIntent.OriginalRequest) || parseTypeConvertIntent(message),
		}

		// Parse an explicitly-named source table (KI-NLCHAT-TABLENAME-IGNORED) from
		// the original request ∪ the confirmation message (mirrors masking above) so
		// a one-turn create can skip the table-selection HITL. The name is validated
		// against the source's cached schema inside createAndRunPipeline; anything
		// unresolved/ambiguous is dropped back to the HITL — never auto-selected.
		namedTables := parseTableIntent(pendingIntent.OriginalRequest)
		if mt := parseTableIntent(message); len(mt) > 0 {
			namedTables = mergeUniqueColumns(namedTables, mt)
		}

		pipelineID, workflowID, namespaceNote, err := h.createAndRunPipeline(c,
			requestNL,
			userID, traceID, sourceConnID, destConnID,
			desiredSyncMode, desiredCDCMode, desiredCDCInitialLoad, nlSpec, scheduleOnly, namedTables)
		if err != nil {
			return ChatMessageResponse{
				Message:   fmt.Sprintf("Failed to create pipeline: %v", err),
				Type:      "error",
				TraceID:   traceID,
				Timestamp: time.Now().Format(time.RFC3339),
			}
		}

		// Schedule-only batch pipeline: attach the Temporal schedule now that we
		// have the pipeline id. No immediate run was started (status='scheduled').
		if scheduleOnly {
			if schedErr := attachScheduleForChat(ctx, db.GetDB(), pipelineID, userID, schedType, schedSpec); schedErr != nil {
				log.WithError(schedErr).WithField("pipeline_id", pipelineID).Warn("failed to attach schedule to chat pipeline")
				return ChatMessageResponse{
					Message: fmt.Sprintf("I created the pipeline **%s → %s**, but couldn't set up the schedule: %v. You can add one from the pipeline's settings.",
						getFriendlyName(pendingIntent.SourceType), getFriendlyName(pendingIntent.DestinationType), schedErr),
					Type:      "error",
					TraceID:   traceID,
					Timestamp: time.Now().Format(time.RFC3339),
					Metadata:  map[string]interface{}{"pipeline_id": pipelineID},
				}
			}
			schedMsg := fmt.Sprintf("Pipeline scheduled! **%s → %s** will run %s. No immediate run was started.",
				getFriendlyName(pendingIntent.SourceType), getFriendlyName(pendingIntent.DestinationType),
				describeSchedule(schedType, schedSpec))
			if namespaceNote != "" {
				schedMsg += "\n\n" + namespaceNote
			}
			return ChatMessageResponse{
				Message:   schedMsg,
				Type:      "pipeline_scheduled",
				TraceID:   traceID,
				Timestamp: time.Now().Format(time.RFC3339),
				Metadata: map[string]interface{}{
					"pipeline_id":      pipelineID,
					"source_type":      pendingIntent.SourceType,
					"destination_type": pendingIntent.DestinationType,
					"schedule_type":    schedType,
				},
			}
		}

		startedMsg := fmt.Sprintf("Pipeline started! Syncing **%s → %s**. I'm analyzing your data now.",
			getFriendlyName(pendingIntent.SourceType), getFriendlyName(pendingIntent.DestinationType))
		if namespaceNote != "" {
			startedMsg += "\n\n" + namespaceNote
		}
		startedMsg += scheduleWarn
		return ChatMessageResponse{
			Message: startedMsg,
			Type:    "pipeline_started",
			TraceID: traceID,
			Metadata: map[string]interface{}{
				"pipeline_id":      pipelineID,
				"workflow_id":      workflowID,
				"source_type":      pendingIntent.SourceType,
				"destination_type": pendingIntent.DestinationType,
				"websocket_url":    fmt.Sprintf("/api/v1/pipelines/%s/events/stream", pipelineID),
			},
		}

	case chat.ConfirmationNo:
		// User rejected - reset and start over
		conv.Reset()
		return ChatMessageResponse{
			Message:   "No problem! Let me know when you'd like to create a pipeline, or tell me what you'd like to change.",
			Type:      "text",
			TraceID:   traceID,
			Timestamp: time.Now().Format(time.RFC3339),
		}

	default:
		// Mid-confirmation edit: "change the destination to bigquery" updates the
		// pending pair in place and re-confirms — instead of silently cancelling
		// (the old behavior, which discarded the correction) or rerouting as a brand
		// new intent (which lost the other side the user already gave).
		if pi := conv.GetPendingIntent(); pi != nil && pi.SourceType != "" && pi.DestinationType != "" {
			if h.tryEditPendingConnector(conv, pi, message) {
				return ChatMessageResponse{
					Message:   fmt.Sprintf("Updated: **%s → %s**. Create and run this pipeline?", getFriendlyName(pi.SourceType), getFriendlyName(pi.DestinationType)),
					Type:      "confirmation",
					TraceID:   traceID,
					Timestamp: time.Now().Format(time.RFC3339),
					Data: map[string]interface{}{
						"source_type":                       pi.SourceType,
						"destination_type":                  pi.DestinationType,
						"supported_sync_modes":              supportedSyncModesForSource(pi.SourceType),
						"source_supports_cdc":               connectorSupportsCDC(pi.SourceType),
						"source_supports_incremental_batch": connectorSupportsIncrementalBatch(pi.SourceType),
						"pending_intent":                    pi,
					},
				}
			}
		}

		// If the message is not a yes/no but looks like a help/question OR a new pipeline request,
		// reset and handle it as a new message instead of re-asking about an old pending pipeline.
		//
		// This also catches cases that the fast regex parser missed (multi-word connectors, extra whitespace).
		lc := strings.ToLower(strings.TrimSpace(message))
		looksHelpish := strings.Contains(lc, "how to") ||
			strings.Contains(lc, "how do") ||
			strings.Contains(lc, "how can") ||
			strings.Contains(lc, "help") ||
			strings.Contains(lc, "guide") ||
			strings.HasPrefix(lc, "what is") ||
			strings.Contains(lc, "what is ") ||
			strings.Contains(lc, "explain")
		looksPipelineish := strings.Contains(lc, " to ") ||
			strings.Contains(lc, " from ") ||
			strings.Contains(lc, "->") ||
			strings.Contains(lc, "→") ||
			strings.Contains(lc, "sync ") ||
			strings.Contains(lc, "migrate ") ||
			strings.Contains(lc, "copy ") ||
			strings.Contains(lc, "transfer ") ||
			strings.Contains(lc, "replicate ")
		if looksHelpish || looksPipelineish {
			conv.Reset()
			return h.handleNewIntent(ctx, c, conv, message, traceID, sessionID, userID)
		}

		// Unclear - ask again
		pendingIntent := conv.GetPendingIntent()
		sourceName := "source"
		destName := "destination"
		sourceType := ""
		destType := ""
		if pendingIntent != nil {
			if pendingIntent.SourceType != "" {
				sourceName = getFriendlyName(pendingIntent.SourceType)
				sourceType = pendingIntent.SourceType
			}
			if pendingIntent.DestinationType != "" {
				destName = getFriendlyName(pendingIntent.DestinationType)
				destType = pendingIntent.DestinationType
			}
		}
		supportedSyncModes := supportedSyncModesForSource(sourceType)
		sourceSupportsCDC := connectorSupportsCDC(sourceType)
		sourceSupportsIncrementalBatch := connectorSupportsIncrementalBatch(sourceType)
		return ChatMessageResponse{
			Message:   fmt.Sprintf("I'm not sure if you want to proceed. Create pipeline **%s → %s**? Please reply Yes or No.", sourceName, destName),
			Type:      "confirmation",
			TraceID:   traceID,
			Timestamp: time.Now().Format(time.RFC3339),
			Data: map[string]interface{}{
				"source_type":                       sourceType,
				"destination_type":                  destType,
				"supported_sync_modes":              supportedSyncModes,
				"source_supports_cdc":               sourceSupportsCDC,
				"source_supports_incremental_batch": sourceSupportsIncrementalBatch,
				"pending_intent":                    pendingIntent,
			},
		}
	}
}

// getOrCreateConversation retrieves or creates a conversation context
func (h *ChatHandler) getOrCreateConversation(ctx context.Context, userID, conversationID string) (*chat.ConversationContext, error) {
	if h.conversationCache == nil {
		// No cache - return new context
		return chat.NewConversationContext(userID, conversationID), nil
	}
	return h.conversationCache.GetOrCreate(ctx, userID, conversationID)
}

// saveConversation persists the conversation context
func (h *ChatHandler) saveConversation(ctx context.Context, conv *chat.ConversationContext) error {
	if h.conversationCache == nil {
		return nil // No cache configured
	}
	return h.conversationCache.Set(ctx, conv)
}

// connectorTypeForConnectionName returns the connector_type of an active
// connection the user owns whose name (or alias) matches `name` in the given
// direction ("source"/"destination"), or "" if none. Lets the chat recognise
// that an unknown token is actually one of the user's connections (e.g.
// "azure-pg-test-dst") and adopt its connector type instead of treating it as
// an unknown connector to generate.
func (h *ChatHandler) connectorTypeForConnectionName(wsID, direction, name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	database := db.GetDB()
	if database == nil {
		return ""
	}
	var connectorType string
	err := database.QueryRow(`
		SELECT connector_type FROM connections
		WHERE workspace_id = $1 AND type = $2 AND status = 'active'
		  AND (LOWER(name) = LOWER($3) OR LOWER(COALESCE(alias, '')) = LOWER($3))
		ORDER BY COALESCE(updated_at, created_at) DESC
		LIMIT 1`, wsID, direction, name).Scan(&connectorType)
	if err != nil {
		return ""
	}
	return connectorType
}

// isKnownConnector checks if a connector exists in the catalog
func (h *ChatHandler) isKnownConnector(connectorName string) bool {
	database := db.GetDB()
	if database == nil {
		// If DB not available, accept common connectors
		commonConnectors := map[string]bool{
			"mysql": true, "postgresql": true, "mongodb": true, "oracle": true,
			"sqlserver": true, "sqlite": true, "aws-s3": true, "snowflake": true,
			"bigquery": true, "redshift": true, "kafka": true, "elasticsearch": true,
			"redis": true, "google-cloud-storage": true, "azure-blob-storage": true,
			"minio": true, "mariadb": true, "cassandra": true,
		}
		normalized := chat.NormalizeConnectorName(connectorName)
		return commonConnectors[normalized]
	}

	normalized := chat.NormalizeConnectorName(connectorName)
	var count int
	err := database.QueryRow(`
		SELECT COUNT(*) FROM connector_catalog 
		WHERE name = $1 AND status = 'active'
	`, normalized).Scan(&count)
	if err != nil {
		log.WithError(err).Debug("Failed to check connector catalog")
		return true // Fail open
	}
	return count > 0
}

// listKnownConnectors returns all active connector names from the catalog.
// Falls back to the hardcoded common set when the DB is unavailable.
func (h *ChatHandler) listKnownConnectors() []string {
	database := db.GetDB()
	if database != nil {
		rows, err := database.Query(`SELECT name FROM connector_catalog WHERE status = 'active'`)
		if err == nil {
			defer rows.Close()
			var names []string
			for rows.Next() {
				var n string
				if rows.Scan(&n) == nil {
					names = append(names, n)
				}
			}
			if len(names) > 0 {
				return names
			}
		}
	}
	return []string{
		"mysql", "postgresql", "mongodb", "oracle", "sqlserver", "sqlite",
		"aws-s3", "snowflake", "bigquery", "redshift", "kafka", "elasticsearch",
		"redis", "google-cloud-storage", "azure-blob-storage", "minio", "mariadb",
		"cassandra", "shopify-admin-graphql",
	}
}

// fuzzyMatchKnownConnector returns the closest known connector within edit distance 2
// (distance 1 for short inputs ≤4 chars). Returns "", false if nothing is close enough.
func (h *ChatHandler) fuzzyMatchKnownConnector(input string) (string, bool) {
	if input == "" {
		return "", false
	}
	maxDist := 2
	if len(input) <= 4 {
		maxDist = 1
	}
	// Check alias map first before doing O(n) Levenshtein scan.
	normalized := chat.NormalizeConnectorName(input)
	if normalized != input && h.isKnownConnector(normalized) {
		return normalized, true
	}
	return chat.FuzzyMatchConnector(input, h.listKnownConnectors(), maxDist)
}

// callSlotFillingLLM calls the slot-filling prompt
func (h *ChatHandler) callSlotFillingLLM(ctx context.Context, conv *chat.ConversationContext, message string) (*chat.SlotFillingResult, error) {
	llmServiceURL := os.Getenv("LLM_SERVICE_URL")
	if llmServiceURL == "" {
		llmServiceURL = "http://llm-service:5000"
	}

	pendingSource := ""
	pendingDest := ""
	if pi := conv.GetPendingIntent(); pi != nil {
		pendingSource = pi.SourceType
		pendingDest = pi.DestinationType
	}

	requestBody := map[string]interface{}{
		"prompt_name": "chat/slot_filling",
		"variables": map[string]interface{}{
			"user_message":        message,
			"state":               string(conv.GetState()),
			"pending_source":      pendingSource,
			"pending_destination": pendingDest,
			"last_response":       conv.GetLastResponse(),
		},
	}

	jsonBody, _ := json.Marshal(requestBody)

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/v1/completion", llmServiceURL), bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to call slot-filling LLM: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("slot-filling LLM returned %d: %s", resp.StatusCode, string(body))
	}

	var llmResponse struct {
		Content string `json:"content"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&llmResponse); err != nil {
		return nil, fmt.Errorf("failed to decode LLM response: %w", err)
	}

	var result chat.SlotFillingResult
	if err := json.Unmarshal([]byte(llmResponse.Content), &result); err != nil {
		return nil, fmt.Errorf("failed to parse slot-filling result: %w", err)
	}

	return &result, nil
}

// extractConnectorFromMessage is a fallback when LLM is unavailable
func (h *ChatHandler) extractConnectorFromMessage(message string, state chat.ConversationState) *chat.SlotFillingResult {
	// Simple keyword matching as fallback
	connectors := []string{
		"mysql", "postgresql", "postgres", "mongodb", "oracle", "sqlserver",
		"s3", "aws-s3", "snowflake", "bigquery", "redshift", "kafka",
		"elasticsearch", "redis", "gcs", "minio", "azure-blob",
	}

	message = " " + message + " " // Add spaces for word boundary matching
	for _, conn := range connectors {
		if containsWord(message, conn) {
			slot := "source"
			if state == chat.StateAwaitingDestination {
				slot = "destination"
			}
			return &chat.SlotFillingResult{
				IsAnsweringPrevious: true,
				ExtractedSlot:       slot,
				ExtractedValue:      conn,
				Confidence:          0.7,
			}
		}
	}

	return &chat.SlotFillingResult{
		IsAnsweringPrevious: false,
		Confidence:          0.0,
		NextQuestion:        "I didn't catch that. Could you specify the connector name?",
	}
}

// containsWord checks if a message contains a word (case-insensitive)
func containsWord(message, word string) bool {
	message = " " + message + " "
	word = " " + word + " "
	return len(message) >= len(word) && (message == word ||
		len(message) > len(word) && (message[:len(word)] == word ||
			message[len(message)-len(word):] == word ||
			len(message) > len(word) && containsSubstring(message, word)))
}

// containsSubstring is a simple substring check
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && findSubstring(s, substr) >= 0
}

// findSubstring finds the index of a substring
func findSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// getSuggestionsForSlot returns connector suggestions based on the slot type
func (h *ChatHandler) getSuggestionsForSlot(state chat.ConversationState) []string {
	if state == chat.StateAwaitingSource {
		return []string{"MySQL", "PostgreSQL", "MongoDB", "Oracle", "SQL Server"}
	}
	return []string{"S3", "Snowflake", "BigQuery", "Redshift", "Elasticsearch"}
}

// filterKnownSuggestions removes suggestions that aren't supported by the connector catalog.
// It preserves the original suggestion casing (nice for UI chips) but validates using normalized names.
func (h *ChatHandler) filterKnownSuggestions(suggestions []string) []string {
	out := make([]string, 0, len(suggestions))
	seen := map[string]bool{}
	for _, s := range suggestions {
		ss := strings.TrimSpace(s)
		if ss == "" {
			continue
		}
		key := strings.ToLower(ss)
		if seen[key] {
			continue
		}
		seen[key] = true

		normalized := chat.NormalizeConnectorName(ss)
		if normalized == "" {
			continue
		}
		if !h.isKnownConnector(normalized) {
			continue
		}
		out = append(out, ss)
	}
	return out
}

// Intent represents parsed user intent
type Intent struct {
	SourceType        string
	DestinationType   string
	IntentName        string
	Tables            []string
	SyncMode          string
	RequiresExecution bool
}

// parseIntent calls the Intent agent to parse natural language
func (h *ChatHandler) parseIntent(ctx context.Context, message string) (*Intent, error) {
	// Call LLM service intent agent
	llmServiceURL := os.Getenv("LLM_SERVICE_URL")
	if llmServiceURL == "" {
		llmServiceURL = "http://llm-service:5000"
	}

	// LLM service expects: prompt_name and variables
	requestBody := map[string]interface{}{
		"prompt_name": "chat/intent_classification",
		"variables": map[string]interface{}{
			"user_message": message,
		},
	}

	jsonBody, _ := json.Marshal(requestBody)

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, fmt.Sprintf("%s/v1/completion", llmServiceURL), bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to call intent agent: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("intent agent returned %d: %s", resp.StatusCode, string(body))
	}

	// LLM service returns: {"content": "{...json...}", "model": "...", "usage": {...}}
	var llmResponse struct {
		Content string `json:"content"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&llmResponse); err != nil {
		return nil, fmt.Errorf("failed to decode LLM response: %w", err)
	}

	// Parse the content JSON string. The prompt returns:
	// { "intent": "...", "requires_execution": bool, "parameters": { "source": "...", "destination": "...", ... } }
	var parsed struct {
		Intent            string `json:"intent"`
		RequiresExecution bool   `json:"requires_execution"`
		Parameters        struct {
			Source      string   `json:"source"`
			Destination string   `json:"destination"`
			Tables      []string `json:"tables"`
			SyncMode    string   `json:"sync_mode"`
		} `json:"parameters"`
	}
	if err := json.Unmarshal([]byte(llmResponse.Content), &parsed); err != nil {
		return nil, fmt.Errorf("failed to parse intent content: %w", err)
	}

	return &Intent{
		SourceType:        parsed.Parameters.Source,
		DestinationType:   parsed.Parameters.Destination,
		IntentName:        parsed.Intent,
		Tables:            parsed.Parameters.Tables,
		SyncMode:          parsed.Parameters.SyncMode,
		RequiresExecution: parsed.RequiresExecution,
	}, nil
}

// normalizeConnectorName normalizes connector names to match catalog
func (h *ChatHandler) normalizeConnectorName(name string) string {
	switch name {
	case "s3":
		return "aws-s3"
	case "postgres":
		return "postgresql"
	default:
		return name
	}
}

func connectorKeyForConnResolution(connectorType string) string {
	normalized := strings.ToLower(strings.TrimSpace(connectorType))
	normalized = strings.ReplaceAll(normalized, " ", "")
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, "_", "")
	return normalized
}

func parseExplicitConnectionNames(userRequest string) (sourceName string, destName string) {
	s := strings.TrimSpace(userRequest)
	if s == "" {
		return "", ""
	}

	// Accept a few common formats:
	// - Source connection: <name>
	// - Destination connection: <name>
	// - source_connection=<name>, destination_connection=<name>
	reSrc := regexp.MustCompile(`(?i)\bsource(?:[_\s-]?connection)?\s*[:=]\s*"?([^"\r\n;]+)"?`)
	reDst := regexp.MustCompile(`(?i)\bdestination(?:[_\s-]?connection)?\s*[:=]\s*"?([^"\r\n;]+)"?`)

	if m := reSrc.FindStringSubmatch(s); len(m) == 2 {
		sourceName = strings.TrimSpace(strings.Trim(m[1], `"'`))
	}
	if m := reDst.FindStringSubmatch(s); len(m) == 2 {
		destName = strings.TrimSpace(strings.Trim(m[1], `"'`))
	}

	trim := func(v string) string {
		v = strings.TrimSpace(v)
		v = strings.Trim(v, " .,:;")
		return v
	}
	return trim(sourceName), trim(destName)
}

// requestMentionsName reports whether candidate appears in reqLower as a whole
// token — not as a substring of a longer identifier. Token boundaries are any
// byte outside [a-z0-9_-], or string start/end, so "shopify" matches in
// `... the shopify source ...` but NOT inside `test-shopify`, and the hyphenated
// `azure-pg-test-dst` matches as a unit. reqLower is expected pre-lowercased.
func requestMentionsName(reqLower, candidate string) bool {
	candidate = strings.ToLower(strings.TrimSpace(candidate))
	if len(candidate) < 3 { // too short to be a reliable, non-noisy signal
		return false
	}
	isBoundary := func(b byte) bool {
		return !((b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '_' || b == '-')
	}
	for i := 0; i <= len(reqLower)-len(candidate); {
		j := strings.Index(reqLower[i:], candidate)
		if j < 0 {
			return false
		}
		start := i + j
		end := start + len(candidate)
		leftOK := start == 0 || isBoundary(reqLower[start-1])
		rightOK := end == len(reqLower) || isBoundary(reqLower[end])
		if leftOK && rightOK {
			return true
		}
		i = start + 1
	}
	return false
}

// reGenerateCommand matches user requests to generate a new connector. Examples:
//   - "generate acme-crm"
//   - "generate connector for shopify"
//   - "create connector hubspot"
//   - "build acme_crm connector"
//
// We intentionally accept underscores and dashes; we normalize to kebab-case
// before forwarding to tool-generator (which insists on kebab-case ids).
// The capture class intentionally excludes spaces: a generate command names a
// single kebab/underscore connector id. Allowing spaces let a whole multi-word
// message ("generate acme and sync mysql to postgres") match and hijack a real
// pipeline request into a bogus connector generation.
var reGenerateCommand = regexp.MustCompile(`(?i)^\s*(?:generate|create|build|make)\s+(?:connector\s+(?:for\s+)?|(?:a\s+)?connector\s+|for\s+)?([a-zA-Z][a-zA-Z0-9_\-]{0,40})(?:\s+connector)?\s*$`)

// maybeHandleGenerateCommand inspects the user message for an explicit
// connector-generation command. Returns (reply, true) when handled. The
// generation request is dispatched asynchronously so the chat stays
// responsive — generation takes 1–3 minutes and we don't want to hold the
// HTTP connection that long.
func (h *ChatHandler) maybeHandleGenerateCommand(ctx context.Context, message, traceID string) (ChatMessageResponse, bool) {
	msg := strings.TrimSpace(message)
	m := reGenerateCommand.FindStringSubmatch(msg)
	if len(m) != 2 {
		return ChatMessageResponse{}, false
	}
	rawName := strings.TrimSpace(m[1])
	if rawName == "" {
		return ChatMessageResponse{}, false
	}
	// Refuse if the requested name maps to something we already have — the
	// user likely meant the existing connector. We check both the strict
	// versioned-on-disk path AND the looser "is in our catalog" check used
	// by the rest of the chat handler, so common connectors (mysql,
	// postgresql, etc.) that aren't yet versioned-on-disk still short-circuit.
	apiName := normalizeConnectorName(rawName)
	if mcpConnectorIsVersioned(apiName) || h.isKnownConnector(apiName) {
		return ChatMessageResponse{
			Message:   fmt.Sprintf("`%s` is already available. Try **\"sync %s to <destination>\"** to use it.", apiName, apiName),
			Type:      "text",
			TraceID:   traceID,
			Timestamp: time.Now().Format(time.RFC3339),
		}, true
	}
	if !isKebabCaseConnectorID(apiName) {
		return ChatMessageResponse{
			Message:   fmt.Sprintf("I couldn't normalize `%s` to a valid connector id. Use lowercase kebab-case like `acme-crm` or `hubspot`.", rawName),
			Type:      "error",
			TraceID:   traceID,
			Timestamp: time.Now().Format(time.RFC3339),
		}, true
	}

	// Dispatch to tool-generator asynchronously so chat stays responsive.
	// We log but don't block on the result; the user can retry the original
	// pipeline command (e.g. "sync acme-crm to bigquery") once generation
	// completes — by then the connector will be in our catalog.
	go h.dispatchConnectorGeneration(apiName, traceID)

	return ChatMessageResponse{
		Message: fmt.Sprintf(
			"🤖 Started generating the **%s** connector. This usually takes 1–3 minutes — I'll add it to the catalog when it's ready.\n\n"+
				"Once it's done, retry your original request (e.g. **\"sync %s to bigquery\"**). You can also watch progress at /admin → Connectors.",
			apiName, apiName,
		),
		Type:      "connector_generation_started",
		TraceID:   traceID,
		Timestamp: time.Now().Format(time.RFC3339),
		Data: map[string]interface{}{
			"connector_name": apiName,
			"status":         "generation_started",
			"eta_minutes":    "1-3",
		},
	}, true
}

// dispatchConnectorGeneration POSTs to the tool-generator service in the
// background. Errors are logged but never surface to the caller — the chat
// reply has already been sent. The user retries their original pipeline
// command once they get the "connector ready" signal (or after a few minutes).
func (h *ChatHandler) dispatchConnectorGeneration(apiName, traceID string) {
	toolGenURL := strings.TrimRight(strings.TrimSpace(os.Getenv("TOOL_GENERATOR_URL")), "/")
	if toolGenURL == "" {
		toolGenURL = "http://tool-generator:5010"
	}
	body, _ := json.Marshal(map[string]interface{}{
		"api_name":       apiName,
		"description":    fmt.Sprintf("User-requested connector for %s", apiName),
		"save_artifacts": true,
		"enable_chaos":   true,
	})
	// Use a long timeout — tool-generator can take a couple of minutes.
	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodPost, toolGenURL+"/v1/generate", bytes.NewReader(body))
	if err != nil {
		log.WithError(err).Errorf("chat: failed to build tool-generator request for %s", apiName)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	// SEC-H-03: authenticate this outbound generate call with the shared
	// internal-service secret when configured. tool-generator gates /v1/generate
	// on X-Internal-Secret (env INTERNAL_SERVICE_SECRET; compose sets it on
	// api-gateway in prod). No-op when unset. Mirrors connector_generator.go
	// GenerateConnector and orchestrator server_manager.go — without it the chat
	// "generate <name>" command would 401 upstream once the secret is set.
	if secret := strings.TrimSpace(os.Getenv("INTERNAL_SERVICE_SECRET")); secret != "" {
		req.Header.Set("X-Internal-Secret", secret)
	}
	if traceID != "" {
		req.Header.Set("X-Trace-Id", traceID)
	}
	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		log.WithError(err).Errorf("chat: tool-generator unreachable for %s", apiName)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		log.Errorf("chat: tool-generator returned %d for %s", resp.StatusCode, apiName)
		return
	}
	log.Infof("✅ chat-initiated connector generation completed for %s (status=%d)", apiName, resp.StatusCode)
}

// errAmbiguousConnection signals that multiple active connections match a
// (user, direction, connector_type) tuple AND none of them is unambiguously
// preferable (no database-name hint match). In that case we MUST NOT silently
// auto-pick the most recent one — the caller leaves the connection ID empty
// so the Temporal workflow's connection_validation_activity emits a HITL
// "please pick a connection" prompt downstream.
//
// This guards a regression where the chat handler would auto-pick the latest
// connection (often pointed at an empty database) and the user never got the
// chance to select a different one.
var errAmbiguousConnection = fmt.Errorf("multiple connections match; user selection required")

// checkConnections tries to resolve a concrete source + destination connection for this user.
// It is best-effort: if it cannot confidently pick a connection, callers should fall back to HITL selection.
func (h *ChatHandler) checkConnections(wsID, userRequest, sourceType, destType string) (sourceConnID, destConnID string, err error) {
	database := db.GetDB()
	if database == nil {
		return "", "", fmt.Errorf("database not connected")
	}

	// Normalize connector names
	normalizedSource := h.normalizeConnectorName(sourceType)
	normalizedDest := h.normalizeConnectorName(destType)

	// If user mentions "db.table", use the db/schema as a hint for choosing a matching source connection.
	dbHint := ""
	if strings.TrimSpace(userRequest) != "" {
		re := regexp.MustCompile(`\\b([a-zA-Z0-9_]+)\\.([a-zA-Z0-9_]+)\\b`)
		if m := re.FindStringSubmatch(userRequest); len(m) >= 3 {
			dbHint = m[1]
		}
	}

	// If the user explicitly names connections, honor that first.
	// This prevents the "latest connection wins" behavior when users provide concrete connection names.
	explicitSourceName, explicitDestName := parseExplicitConnectionNames(userRequest)
	resolveByName := func(direction, expectedConnectorType, name string) (string, error) {
		name = strings.TrimSpace(name)
		if name == "" {
			return "", sql.ErrNoRows
		}
		q := `
			SELECT id, connector_type
			FROM connections
			WHERE workspace_id = $1
				AND type = $2
				AND status = 'active'
				AND (
					LOWER(name) = LOWER($3)
					OR LOWER(COALESCE(alias, '')) = LOWER($3)
				)
			ORDER BY COALESCE(updated_at, created_at) DESC
			LIMIT 1
		`
		var id, dbConnectorType string
		if qErr := database.QueryRow(q, wsID, direction, name).Scan(&id, &dbConnectorType); qErr != nil {
			return "", qErr
		}
		if connectorKeyForConnResolution(dbConnectorType) != connectorKeyForConnResolution(expectedConnectorType) {
			return "", fmt.Errorf("connection %q is %s, expected %s", name, dbConnectorType, expectedConnectorType)
		}
		return id, nil
	}

	if explicitSourceName != "" {
		if id, pickErr := resolveByName("source", normalizedSource, explicitSourceName); pickErr == nil {
			sourceConnID = id
		} else {
			log.WithError(pickErr).WithFields(log.Fields{
				"direction":       "source",
				"connection":      explicitSourceName,
				"expected_type":   normalizedSource,
				"fallback_dbHint": dbHint,
			}).Warn("⚠️ Explicit source connection name not resolved; falling back to heuristic selection")
		}
	}
	if explicitDestName != "" {
		if id, pickErr := resolveByName("destination", normalizedDest, explicitDestName); pickErr == nil {
			destConnID = id
		} else {
			log.WithError(pickErr).WithFields(log.Fields{
				"direction":     "destination",
				"connection":    explicitDestName,
				"expected_type": normalizedDest,
			}).Warn("⚠️ Explicit destination connection name not resolved; falling back to heuristic selection")
		}
	}

	// Strongest disambiguation signal: the user literally names one of their
	// own connections in the request (e.g. "...into azure-pg-test-dst..."). A
	// named connection beats connector-type heuristics and resolves the common
	// "multiple connections of the same type" case that would otherwise leave
	// the id empty — which previously forced a brittle downstream text-parse of
	// the raw prompt (grabbing articles like "the"). We accept a direction only
	// when EXACTLY ONE of the user's connections for that direction is named;
	// zero or several stays ambiguous and defers to HITL as before. We match by
	// connection NAME, so the resulting connector type is authoritative from the
	// connection row downstream regardless of how the NL intent guessed it.
	scanNamedConnection := func(direction string) string {
		rows, qErr := database.Query(`
			SELECT id, name, COALESCE(alias, '')
			FROM connections
			WHERE workspace_id = $1 AND type = $2 AND status = 'active'`, wsID, direction)
		if qErr != nil {
			return ""
		}
		defer rows.Close()
		reqLower := strings.ToLower(userRequest)
		matchID := ""
		distinct := 0
		for rows.Next() {
			var id, name, alias string
			if rows.Scan(&id, &name, &alias) != nil {
				continue
			}
			if requestMentionsName(reqLower, name) || (alias != "" && requestMentionsName(reqLower, alias)) {
				if id != matchID {
					distinct++
					matchID = id
				}
			}
		}
		if distinct == 1 {
			return matchID
		}
		return ""
	}

	if sourceConnID == "" {
		if id := scanNamedConnection("source"); id != "" {
			sourceConnID = id
		}
	}
	if destConnID == "" {
		if id := scanNamedConnection("destination"); id != "" {
			destConnID = id
		}
	}

	pick := func(direction, connectorType string, databaseHint string) (string, error) {
		q := `
			SELECT id, connector_type, config, COALESCE(updated_at, created_at) AS ts
			FROM connections
			WHERE workspace_id = $1
				AND type = $2
				AND status = 'active'
			ORDER BY ts DESC
		`
		rows, qErr := database.Query(q, wsID, direction)
		if qErr != nil {
			return "", qErr
		}
		defer rows.Close()

		targetKey := connectorKeyForConnResolution(connectorType)
		matchIDs := make([]string, 0, 4)
		var dbHintMatchID string

		for rows.Next() {
			var id, dbConnectorType, configEncrypted string
			var ts time.Time
			if scanErr := rows.Scan(&id, &dbConnectorType, &configEncrypted, &ts); scanErr != nil {
				continue
			}
			if connectorKeyForConnResolution(dbConnectorType) != targetKey {
				continue
			}
			matchIDs = append(matchIDs, id)

			// If the user named a db/schema in the request, prefer a connection
			// whose decrypted config.database matches it — that's the strongest
			// disambiguation signal we can use without asking the user.
			if databaseHint == "" || dbHintMatchID != "" {
				continue
			}
			cfgJSON, decErr := crypto.DecryptString(configEncrypted)
			if decErr != nil {
				continue
			}
			var cfg map[string]interface{}
			if json.Unmarshal([]byte(cfgJSON), &cfg) != nil {
				continue
			}
			if v, ok := cfg["database"]; ok && strings.EqualFold(strings.TrimSpace(fmt.Sprint(v)), databaseHint) {
				dbHintMatchID = id
			}
		}

		switch {
		case len(matchIDs) == 0:
			return "", sql.ErrNoRows
		case len(matchIDs) == 1:
			return matchIDs[0], nil
		case dbHintMatchID != "":
			// Multiple candidates but exactly one has the right database — safe.
			return dbHintMatchID, nil
		default:
			// Multiple candidates, no clear winner — refuse to pick. Caller
			// leaves the connection_id empty; the workflow's connection
			// validation activity will then emit a HITL prompt asking the
			// user to select.
			log.WithFields(log.Fields{
				"direction":       direction,
				"connector_type":  connectorType,
				"candidate_count": len(matchIDs),
				"db_hint":         databaseHint,
			}).Info("⏸️  Multiple connections match; deferring to HITL user selection")
			return "", errAmbiguousConnection
		}
	}

	if sourceConnID == "" {
		picked, perr := pick("source", normalizedSource, dbHint)
		switch perr {
		case nil:
			sourceConnID = picked
		case errAmbiguousConnection:
			// Leave empty so the workflow asks the user which connection to use.
			sourceConnID = ""
		default:
			return "", "", fmt.Errorf("source connection not found for type %s", sourceType)
		}
	}

	if destConnID == "" {
		// Destination: no database hint (the user request typically doesn't specify).
		picked, perr := pick("destination", normalizedDest, "")
		switch perr {
		case nil:
			destConnID = picked
		case errAmbiguousConnection:
			destConnID = ""
		default:
			return "", "", fmt.Errorf("destination connection not found for type %s", destType)
		}
	}

	return sourceConnID, destConnID, nil
}

// createAndRunPipeline creates a pipeline and starts the Temporal workflow
// V2: Simplified - no pre-parsing, just passes user message to workflow
func (h *ChatHandler) createAndRunPipeline(
	c *gin.Context,
	request, userID, traceID string,
	sourceConnectionID, destinationConnectionID string,
	syncMode string,
	cdcMode string,
	cdcInitialLoad string,
	nlTransforms nlTransformSpec,
	scheduleOnly bool,
	namedTables []string,
) (string, string, string, error) {
	// 4th return value is `namespaceNote`: an empty string when the
	// destination namespace was assigned cleanly, OR a user-facing
	// explanation when auto-suffix kicked in because the bare namespace
	// was already owned by another pipeline. The chat handler appends
	// this note to the "Pipeline started!" message so the user knows
	// their data is landing in `shopify_7150` instead of `shopify`.
	// Design: Option B from .design/destination-namespace.md (transparency,
	// no blocking HITL); Option C will add a real interactive prompt.
	database := db.GetDB()
	if database == nil {
		return "", "", "", fmt.Errorf("database not connected")
	}

	// Plan pipeline-count gate. The chat path both creates AND runs, so this
	// single check covers both the create-limit and the trial-expired (can't
	// run) cases. Scoped to the active WORKSPACE (the billable tenant), not the
	// caller. Surfaced as a chat reply via the error return. See
	// plan_quota.go / migration 060.
	if msg := pipelineCreateBlockedMessage(c.Request.Context(), database, activeWorkspaceID(c)); msg != "" {
		return "", "", "", fmt.Errorf("%s", msg)
	}

	// Plan GB run gate (chat variant, Ship 2 Phase 2). Same per-workspace
	// data-transfer check as the REST run path, surfaced as a chat reply.
	if msg := workspaceGBBlockedMessage(c.Request.Context(), database, activeWorkspaceID(c)); msg != "" {
		return "", "", "", fmt.Errorf("%s", msg)
	}

	// Lifecycle=draft gate: refuse to create a chat pipeline pointing
	// at a draft connector. Mirrors the REST CreatePipeline gate at
	// pipelines.go (see checkConnectionLifecycleDraft). Chat path
	// can't use the HTTP-writing helper because it has its own
	// response-shape contract (returns (id, exec, note, err)), so we
	// surface the message through the error return — the caller
	// renders it as a chat reply.
	if msg := connectionLifecycleDraftError(database, activeWorkspaceID(c), sourceConnectionID, destinationConnectionID); msg != "" {
		return "", "", "", fmt.Errorf("%s", msg)
	}

	// Create pipeline
	createReq := CreatePipelineRequest{
		Name:        fmt.Sprintf("Chat Pipeline %s", time.Now().Format("15:04:05")),
		Description: fmt.Sprintf("Created from chat: %s", request),
		Request:     request,
	}

	// Create pipeline in DB
	pipelineID := uuid.New().String()
	now := time.Now()

	// Workspace scoping: the chat-created pipeline belongs to the caller's ACTIVE
	// workspace (migration 069 made pipelines.workspace_id NOT NULL). Surfaced
	// through the error return (rendered as a chat reply), not an HTTP body.
	workspaceID := activeWorkspaceID(c)
	if workspaceID == "" {
		return "", "", "", fmt.Errorf("no active workspace")
	}

	// Connection-tenancy guard (P2e): checkConnections resolves the source/dest
	// ids scoped to user_id, so a member who belongs to multiple workspaces could
	// resolve a connection outside the active one. Reject any connection that
	// isn't in the active workspace before wiring it into the pipeline (surfaced
	// as a chat reply via the error return).
	if sourceConnectionID != "" && !connectionInWorkspace(database, sourceConnectionID, workspaceID) {
		return "", "", "", fmt.Errorf("the selected source connection is not in your active workspace")
	}
	if destinationConnectionID != "" && !connectionInWorkspace(database, destinationConnectionID, workspaceID) {
		return "", "", "", fmt.Errorf("the selected destination connection is not in your active workspace")
	}

	// Note: created_by must never be NULL to ensure proper event visibility
	// resolveUserID always returns a non-empty string with dev fallback
	_, err := database.Exec(`
		INSERT INTO pipelines (
			id, name, description, natural_language_request, status,
			created_at, updated_at, created_by,
			source_connection_id, destination_connection_id,
			sync_mode, cdc_mode, workspace_id, cdc_initial_load
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	`, pipelineID, createReq.Name, createReq.Description, createReq.Request, "pending", now, now, userID,
		nullString(sourceConnectionID), nullString(destinationConnectionID),
		nullString(syncMode), nullString(cdcMode), workspaceID, nullString(cdcInitialLoad))

	if err != nil {
		return "", "", "", fmt.Errorf("failed to create pipeline: %w", err)
	}

	log.WithFields(log.Fields{
		"pipeline_id": pipelineID,
		"name":        createReq.Name,
	}).Info("✅ Created pipeline from chat")

	// Persist the NL-requested transform intent (masking / type conversion) onto
	// pipelines.config.nl_transforms. The orchestrator's planNLTransforms gate reads
	// it at data-transfer time and materializes transform_definitions for BOTH batch
	// and CDC. FAIL-CLOSED: masking is a privacy control, so a persist failure aborts
	// creation rather than letting the pipeline run and land unmasked PII. Written
	// before the workflow starts (status is still 'pending'), so no run has begun.
	if !nlTransforms.empty() {
		raw, mErr := json.Marshal(nlTransforms)
		if mErr != nil {
			return "", "", "", fmt.Errorf("marshal nl_transforms intent: %w", mErr)
		}
		if _, uErr := database.Exec(`
			UPDATE pipelines
			SET config = jsonb_set(COALESCE(config, '{}'::jsonb), '{nl_transforms}', $2::jsonb, true),
			    updated_at = NOW()
			WHERE id = $1
		`, pipelineID, string(raw)); uErr != nil {
			return "", "", "", fmt.Errorf("persist nl_transforms intent (masking/type-convert): %w", uErr)
		}
		log.WithFields(log.Fields{
			"pipeline_id":  pipelineID,
			"mask_columns": nlTransforms.MaskColumns,
			"mask_pii":     nlTransforms.MaskPII,
			"type_convert": nlTransforms.TypeConvert,
		}).Info("📝 Persisted nl_transforms intent")
	}

	// KI-NLCHAT-TABLENAME-IGNORED: pre-select an explicitly-named source table so a
	// one-turn NL create skips the table-selection HITL — but ONLY when the name
	// resolves UNAMBIGUOUSLY against the source's cached schema. A cold cache, an
	// unknown name, or a name that is ambiguous across schemas is left to the HITL
	// (never auto-select a wrong table). Persisted to config.selected_tables (read
	// by the scheduled-run workflow) and threaded into the immediate workflow input
	// below. Best-effort: a validation/persist miss simply defers to the HITL — this
	// is a convenience, not a safety control, so it never fails pipeline creation.
	var qualifiedTables []string
	if len(namedTables) > 0 && sourceConnectionID != "" {
		qualified, missing, ambiguous, ok := validateAndQualifySelectedTablesFromCache(
			c.Request.Context(), sourceConnectionID, namedTables)
		if shouldPreselectNamedTables(qualified, missing, ambiguous, ok) {
			if raw, mErr := json.Marshal(qualified); mErr == nil {
				if _, uErr := database.Exec(`
					UPDATE pipelines
					SET config = jsonb_set(COALESCE(config, '{}'::jsonb), '{selected_tables}', $2::jsonb, true),
					    updated_at = NOW()
					WHERE id = $1
				`, pipelineID, string(raw)); uErr != nil {
					log.WithError(uErr).WithField("pipeline_id", pipelineID).
						Warn("[chat_nl_pipeline] failed to persist selected_tables (best-effort → HITL)")
				} else {
					qualifiedTables = qualified
					log.WithFields(log.Fields{
						"pipeline_id":     pipelineID,
						"selected_tables": qualifiedTables,
					}).Info("📝 Pre-selected NL-named source table(s)")
				}
			}
		} else {
			log.WithFields(log.Fields{
				"pipeline_id":  pipelineID,
				"requested":    namedTables,
				"missing":      missing,
				"ambiguous_ct": len(ambiguous),
				"cache_warm":   ok,
			}).Info("[chat_nl_pipeline] NL-named table(s) not unambiguously resolved — deferring to table-selection HITL")
		}
	}

	// Round-4 destination-namespace assignment: mirror the CreatePipeline
	// path so chat-created pipelines also get an entry in pipelines.config
	// and _rsync_pipelines ownership rows downstream.
	var namespaceNote string
	if destinationConnectionID != "" {
		var sourceConnectorType string
		if sourceConnectionID != "" {
			_ = database.QueryRow(`SELECT connector_type FROM connections WHERE id = $1 AND workspace_id = $2`, sourceConnectionID, workspaceID).Scan(&sourceConnectorType)
		}
		// Fetch dest connector type first so seedDestinationNamespace can
		// translate generic source-engine defaults to the dest's own default.
		var destConnectorType string
		_ = database.QueryRow(`SELECT connector_type FROM connections WHERE id = $1 AND workspace_id = $2`, destinationConnectionID, workspaceID).Scan(&destConnectorType)
		defaultNamespace := seedDestinationNamespace(sourceConnectorType, destConnectorType)
		destinationNamespace := resolveDestinationNamespace(database, pipelineID, sourceConnectorType, destConnectorType, destinationConnectionID, nil)
		if nsErr := persistDestinationConfig(database, pipelineID, DestinationConfig{
			Namespace:     destinationNamespace,
			NamespaceKind: namespaceKindForConnector(destConnectorType),
			// The destination connector auto-creates a missing schema/database at
			// write time (CREATE … IF NOT EXISTS), so create-if-not-exists is the
			// only sensible default — we just collect the namespace name from the
			// user and provision it for them.
			CreateIfNotExists: true,
		}); nsErr != nil {
			log.WithError(nsErr).Warn("[chat_nl_pipeline] failed to persist destination_config (best-effort)")
		}
		// Option B (transparency): if auto-suffix kicked in, surface a
		// user-facing note so the chat reply explains where data is
		// actually landing. Empty when the bare namespace was free.
		if destinationNamespace != "" && destinationNamespace != defaultNamespace {
			namespaceNote = fmt.Sprintf(
				"⚠️ The `%s` namespace in this destination is already owned by another pipeline — your data will land in `%s` instead. Cancel and recreate the pipeline with a custom destination_namespace if you'd prefer a specific name.",
				defaultNamespace, destinationNamespace,
			)
		}
	}

	// Schedule-only (NL "every day at 2am" on a batch pipeline): create the
	// pipeline in a 'scheduled' state and DO NOT start an immediate run — the
	// caller attaches a Temporal schedule that fires the first run at the
	// scheduled time. CDC pipelines never reach here (streaming is continuous).
	if scheduleOnly {
		if _, err = database.Exec("UPDATE pipelines SET status = 'scheduled', updated_at = NOW() WHERE id = $1", pipelineID); err != nil {
			return "", "", "", fmt.Errorf("failed to mark pipeline scheduled: %w", err)
		}
		log.WithField("pipeline_id", pipelineID).Info("🗓️  Chat pipeline created 'scheduled' (immediate run suppressed)")
		return pipelineID, "", namespaceNote, nil
	}

	// Now start the Temporal workflow
	// Update status to running
	_, err = database.Exec("UPDATE pipelines SET status = 'running', updated_at = NOW() WHERE id = $1", pipelineID)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to update pipeline status: %w", err)
	}

	// Create execution record
	execID := uuid.New().String()
	_, err = database.Exec(`
		INSERT INTO executions (id, pipeline_id, status, start_time)
		VALUES ($1, $2, 'running', NOW())
	`, execID, pipelineID)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to create execution record: %w", err)
	}

	// Start Temporal workflow (lazily (re)dial if the startup connection failed)
	tc := getTemporalClient()
	if tc != nil {
		otelCtx := telemetry.GetOTelContext(c)
		traceHeaders := telemetry.InjectTraceToHeaders(otelCtx)

		workflowOptions := client.StartWorkflowOptions{
			// CRITICAL: workflow ID must be unique per execution (avoid workflow reuse)
			ID:        execID,
			TaskQueue: "pipeline-workflows",
			// Ensure HITL flows have sufficient time for a human to respond.
			WorkflowExecutionTimeout: getPipelineWorkflowTimeout(),
			WorkflowRunTimeout:       getPipelineWorkflowTimeout(),
		}

		// V2: Minimal input - workflow handles everything
		workflowInput := map[string]interface{}{
			"pipeline_id":               pipelineID,
			"execution_id":              execID,
			"message":                   request, // Raw user message - workflow parses intent
			"user_id":                   userID,
			"source_connection_id":      sourceConnectionID,
			"destination_connection_id": destinationConnectionID,
			// Distributed tracing context (so Temporal activities + Kafka can join the same trace).
			"trace_id":    traceID,
			"traceparent": traceHeaders["traceparent"],
			"tracestate":  traceHeaders["tracestate"],
		}

		// KI-NLCHAT-TABLENAME-IGNORED: hand the schema-validated table selection to
		// the workflow so the immediate run pre-selects it (NLPipelineWorkflowV2Input
		// → state.SelectedTables → executor) instead of parking at the table HITL.
		if len(qualifiedTables) > 0 {
			workflowInput["selected_tables"] = qualifiedTables
		}

		// V2 is the ONLY workflow (V1 deleted)
		workflowRun, err := tc.ExecuteWorkflow(
			context.WithoutCancel(otelCtx),
			workflowOptions,
			"NLPipelineWorkflowV2", // Hard-coded - no routing
			workflowInput,
		)

		if err != nil {
			log.WithError(err).Error("❌ Failed to start Temporal workflow")
			database.Exec("UPDATE pipelines SET status = 'failed', updated_at = NOW() WHERE id = $1", pipelineID)
			return "", "", "", fmt.Errorf("failed to start workflow: %w", err)
		}

		workflowID := workflowRun.GetID()
		log.WithFields(log.Fields{
			"pipeline_id": pipelineID,
			"workflow_id": workflowID,
		}).Info("✅ Started Temporal workflow V2 for pipeline")

		return pipelineID, workflowID, namespaceNote, nil
	}

	// Fallback if Temporal client not available
	return pipelineID, execID, namespaceNote, fmt.Errorf("temporal client not configured")
}

// preserveCasing returns the user's original spelling of `slug` if a
// case-insensitive match exists somewhere in `userMessage`. Falls back
// to getFriendlyName(slug) when not found (which title-cases unknown
// slugs and looks up canonical names for known connectors).
//
// Why: LLM intent extraction lowercases connector names. When we tell
// the user "I don't have a connector for X yet", echoing their exact
// spelling ("QuickBooks") reads far better than the lowercased slug.
func preserveCasing(userMessage, slug string) string {
	if slug == "" {
		return slug
	}
	lowerMsg := strings.ToLower(userMessage)
	lowerSlug := strings.ToLower(slug)
	if i := strings.Index(lowerMsg, lowerSlug); i >= 0 {
		// Read the original-cased substring at that offset.
		return userMessage[i : i+len(lowerSlug)]
	}
	return getFriendlyName(slug)
}
