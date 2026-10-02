// Package assessor — Pre-flight Assessment Service (Pillar 1).
//
// Pattern stolen from AWS DMS Premigration Assessment Runs: every CDC
// pipeline should be assessed BEFORE it starts. Assessment checks source
// DB configuration (wal_level, binlog_format, replication grants, primary
// keys) and surfaces structured results to the user via the StructuredError
// envelope with copy-pasteable fixes.
//
// Why a first-class service (vs. inline pre-flight at pipeline start):
//   - Users can run assessments BEFORE clicking Run, fix issues, re-run.
//   - Operators have audit history of which checks passed when.
//   - Pre-flight is enforced, not advisory: the api-gateway's run gate
//     (RunPipeline → evaluateAssessmentGate) runs these checks on every Run
//     and refuses to start on an error finding. There is no opt-in flag.
//
// Adding a new source connector requires exactly one new SourceAssessor
// implementation. The orchestrator's HTTP handler dispatches by source_type;
// the registry below is the single source of truth for which sources are
// covered.
package assessor

import (
	"context"
	"fmt"
	"strings"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// Severity of a single check finding. Mirrors diagnose.Severity so the
// JSON shape on the wire is identical — frontend can render either source
// with the same component.
type Severity = diagnose.Severity

const (
	SeverityInfo    = diagnose.SeverityInfo
	SeverityWarning = diagnose.SeverityWarning
	SeverityError   = diagnose.SeverityError
)

// Check is a single assessment finding. Codes are stable identifiers
// (SCREAMING_SNAKE_CASE) shared with the StructuredError envelope —
// e.g. an assessment "failed" with code MYSQL_BINLOG_FORMAT_NOT_ROW maps
// 1:1 to the user-visible error code at run-time if they ignore the warning.
type Check struct {
	// Code — stable identifier, e.g. POSTGRES_WAL_LEVEL_NOT_LOGICAL.
	// Reuse codes from diagnose/structured_error.go so the user sees the
	// SAME code in pre-flight as in run-time errors.
	Code string `json:"code"`

	// Severity — info | warning | error.
	// 'error' findings block pipeline start (api-gateway run gate).
	Severity Severity `json:"severity"`

	// Passed — true means the check found no issue. When false, see Message
	// + Remediation for what the user should do.
	Passed bool `json:"passed"`

	// Message — short human description ("MySQL binlog_format is 'STATEMENT'").
	Message string `json:"message"`

	// Object — the table/collection a per-object check is about, empty for a
	// server-level check. The gateway's Assessment tab groups one code's checks
	// into a single row and lists these as the affected objects.
	Object string `json:"object,omitempty"`

	// Remediation — populated when Passed=false. Carries the fix.
	Remediation *diagnose.Remediation `json:"remediation,omitempty"`
}

// Result is the output of one Assess() call across all checks for a source.
type Result struct {
	// SourceType — the connector_type assessed (e.g. "postgresql").
	SourceType string `json:"source_type"`

	// Checks — every check the assessor ran (passed AND failed). Storing
	// passed checks too lets the UI render a checklist with green checkmarks
	// next to failures, which is more reassuring than "here's what's wrong".
	Checks []Check `json:"checks"`

	// PassedCount, WarningCount, FailedCount, ErrorCount — summary for
	// the assessments row's columns of the same name. Kept here so the
	// handler doesn't need to count inline.
	PassedCount  int `json:"passed_count"`
	WarningCount int `json:"warning_count"`
	FailedCount  int `json:"failed_count"`
	ErrorCount   int `json:"error_count"` // for "the assessment itself crashed" cases

	// OverallStatus — derived from counts. "passed" | "warning" | "failed" | "error".
	OverallStatus string `json:"overall_status"`
}

// SourceAssessor is the per-source-type assessment interface.
//
// Implementations connect to the source DB, run a fixed set of checks,
// and return a Result. They MUST NOT depend on any pipeline state — the
// only inputs are the connection config and (optionally) the list of
// tables the user has selected.
//
// The interface is intentionally minimal: one method, one input struct,
// one output struct. Adding a new source type = one new file implementing
// this interface + one line in the Registry.
type SourceAssessor interface {
	// SourceType returns the canonical name this assessor handles
	// (e.g. "postgresql", "mysql"). Used by the Registry to dispatch.
	SourceType() string

	// Assess runs all checks and returns the Result. The Input carries
	// the decrypted connection config + the table list. err is non-nil
	// only when assessment itself crashed (couldn't connect, etc.); a
	// check failure is signalled inside Result.Checks, not via err.
	Assess(ctx context.Context, input Input) (*Result, error)
}

// Input is the assessor's read-only context. Kept as a struct so future
// fields (e.g. RunMode, CDCMode) can be added without breaking the interface.
type Input struct {
	// ConnectionConfig — decrypted connection config (host, port, user,
	// password, database, schema, ssl_mode, etc.). The shape matches
	// connections.Manager.Get() output.
	ConnectionConfig map[string]string

	// Tables — optional list of "schema.table" or bare "table" identifiers
	// the user selected. Some checks (PK presence) require this; others
	// (wal_level, binlog_format) don't.
	Tables []string

	// NominatedKeys — optional user-designated key columns for keyless / GIPK
	// tables (PR-D column nomination). Shape: { "<table>": ["col", …] }. When a
	// table appears here, the per-table PK check treats it as keyed (an INFO
	// note, not a keyless WARNING) because the data plane will upsert on these
	// columns. Keys may be bare or schema-qualified; matched against both.
	NominatedKeys map[string][]string

	// PipelineID — optional, when assessment runs in the context of a
	// specific pipeline. Used for log lines / metadata, and to recognise the
	// pipeline's own replication slot / publication on the source (their
	// names start with cdc.PipelineResourcePrefix). Assessors must not read
	// pipeline state from rsync.ai's own database.
	PipelineID string

	// SyncMode — the pipeline's intended replication mode: "cdc", "batch",
	// "snapshot", "" (unknown). DB assessors use this to gate CDC-only
	// server-config checks (binlog_format, wal_level, replication grants):
	// a batch pipeline does NOT need those, so enforcing them would be a
	// false-positive blocker. When empty, assessors assume the strictest
	// (CDC) requirements so we never under-report.
	SyncMode string

	// SourceType — canonical connector type for this connection
	// (e.g. "mysql", "shopify", "stripe"). The generic ConnectorAssessor
	// reads this (falling back to ConnectionConfig["connector_type"]) to
	// know which MCP connector to test. DB assessors ignore it.
	SourceType string

	// Version — connector version pin ("latest" or "vX.Y.Z"). Used by the
	// generic ConnectorAssessor when invoking the MCP connector. DB
	// assessors ignore it.
	Version string

	// DestinationType — canonical connector type of the pipeline's
	// DESTINATION (e.g. "postgresql", "mysql", "aws-s3", "google-sheets").
	// DB source assessors read it to decide whether a missing source primary
	// key actually matters for a BATCH load: a relational-DB destination is
	// written by the kafka-mcp-sink via INSERT … ON CONFLICT (pk) DO UPDATE,
	// which silently drops rows when the source table (and thus the
	// auto-created destination table) has no unique key. File/SaaS
	// destinations don't upsert, so a missing PK is harmless there. Empty
	// when unknown — treated as "does not upsert" so we never over-block a
	// batch load on a guess.
	DestinationType string
}

// IsCDC reports whether the pipeline requires change-data-capture-grade
// source configuration. Empty/unknown sync mode is treated as CDC so we
// never silently skip the strict checks.
func (in Input) IsCDC() bool {
	switch in.SyncMode {
	case "batch", "snapshot", "full", "full_refresh", "full-refresh":
		return false
	default:
		return true
	}
}

// DestinationUsesUpsert reports whether the destination is written through the
// sink's primary-key upsert path (INSERT … ON CONFLICT (pk) DO UPDATE). True
// for relational-database destinations; false for file/SaaS/append-only
// targets and when the destination type is unknown. When true, every selected
// source table needs a primary key even for a BATCH (non-CDC) load — without
// one the auto-created destination table has no unique constraint, the upsert
// matches nothing, and those rows are silently dropped. This is the batch
// counterpart to IsCDC()'s CDC primary-key requirement.
func (in Input) DestinationUsesUpsert() bool {
	switch strings.ToLower(strings.TrimSpace(in.DestinationType)) {
	case "postgresql", "postgres", "mysql", "mariadb", "oracle", "sqlserver", "mssql":
		return true
	default:
		return false
	}
}

// RequiresTablePrimaryKeys reports whether the per-table primary-key check
// should run for this pipeline: always for CDC, and for batch only when the
// destination upserts (relational DB). Centralises the gate so the MySQL and
// PostgreSQL assessors stay in lockstep.
func (in Input) RequiresTablePrimaryKeys() bool {
	return in.IsCDC() || in.DestinationUsesUpsert()
}

// CDCDatabaseDestination returns the normalised database destination
// ("postgresql", "mysql" or "mongodb") when this is a CDC pipeline writing to
// one, and "" otherwise. It picks the wording of the keyless-table finding: a
// keyless source table streams into these destinations (policy 2026-09-27 —
// it used to be blocked), but its UPDATEs and DELETEs cannot be matched to the
// destination row, so the copy drifts and the finding has to say exactly how.
// Aliases fold as the executor's normalizeDBType does (postgres→postgresql,
// mariadb→mysql). Oracle/SQL Server/object-storage destinations keep the
// generic surrogate-key warning.
func (in Input) CDCDatabaseDestination() string {
	if !in.IsCDC() {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(in.DestinationType)) {
	case "postgresql", "postgres":
		return "postgresql"
	case "mysql", "mariadb":
		return "mysql"
	case "mongodb":
		return "mongodb"
	default:
		return ""
	}
}

// nominatedColsFor returns the user-nominated key columns for a table, matching
// both schema-qualified ("schema.table") and bare ("table") shapes, case-
// insensitively. Returns nil when the table has no nomination. Shared by the
// MySQL and PostgreSQL per-table PK checks (PR-D column nomination).
func nominatedColsFor(nominated map[string][]string, schemaOrDB, tableName string) []string {
	if len(nominated) == 0 {
		return nil
	}
	bare := strings.ToLower(strings.TrimSpace(tableName))
	qualified := bare
	if s := strings.TrimSpace(schemaOrDB); s != "" {
		qualified = strings.ToLower(s) + "." + bare
	}
	for k, v := range nominated {
		lk := strings.ToLower(strings.TrimSpace(k))
		if lk == bare || lk == qualified {
			return v
		}
	}
	return nil
}

// keylessCDCDriftCheck is the finding for a keyless source table in a CDC
// pipeline into a database destination (see Input.CDCDatabaseDestination). The
// run is allowed — WARNING, Passed=true — and the message states the exact
// effect: inserts land once, every UPDATE adds a new row/document beside the
// old one, and DELETEs are not applied (the sink dead-letters a keyless delete,
// having nothing to match it on). The ALTER TABLE is in the message itself as
// well as in SQLToRun, so the fix survives any surface that shows only text.
//
// `qualified` is the display name ("schema.table" / "db.table"), `alterSQL` the
// family-correct ALTER TABLE, `destType` the normalised destination, and
// `nominatedCols` the columns the user picked as a key, if any — nomination is
// a batch-load feature (executeBatchDataTransfer's pkByTable) that does not
// reach the CDC sink, and the message says so rather than implying it helps.
func keylessCDCDriftCheck(qualified, alterSQL, destType string, nominatedCols []string) Check {
	unit := "row"
	if destType == "mongodb" {
		unit = "document"
	}
	msg := fmt.Sprintf(
		"%s has no primary key. CDC will copy inserts exactly once, but each UPDATE adds a new %s (the old version stays) and DELETEs are not applied, so the destination drifts from the source. Add a primary key for an exact copy: %s",
		qualified, unit, alterSQL,
	)
	if len(nominatedCols) > 0 {
		msg += fmt.Sprintf(
			" The nominated key column(s) (%s) apply to batch loads only; CDC does not use them.",
			strings.Join(nominatedCols, ", "),
		)
	}
	return Check{
		Code: "CDC_TABLE_MISSING_PRIMARY_KEY", Severity: SeverityWarning, Passed: true,
		Message: msg,
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"No action needed to run — the table streams without a primary key.",
				"For an exact copy (updates in place, deletes applied): add a PRIMARY KEY on the source table.",
			},
			SQLToRun: []string{
				"-- For an exact copy — replace 'id' with the column(s) that identify a row:",
				alterSQL,
			},
			DocURL:           diagnose.ErrorDocURL("cdc-missing-pk"),
			EstimatedMinutes: 10,
		},
	}
}

// Registry maps source_type → SourceAssessor. Populated at startup via
// Register(). The HTTP handler calls Resolve(sourceType) to dispatch.
//
// Concurrency: register all assessors at startup before serving requests;
// Registry is read-only after init. No locking needed.
type Registry struct {
	byType map[string]SourceAssessor

	// fallback — used by Resolve when no source-type-specific assessor is
	// registered. This is how we get UNIVERSAL coverage: DB sources get a
	// dedicated assessor (deep server-config checks); everything else
	// (SaaS/REST/GraphQL/cloud-storage) falls back to the generic
	// ConnectorAssessor, which validates connectivity + auth + required
	// config via the connector's own test_connection/validate_config.
	fallback SourceAssessor
}

func NewRegistry() *Registry {
	return &Registry{byType: make(map[string]SourceAssessor)}
}

// Register adds an assessor to the registry. Panics on duplicate registration
// (a programming error — two assessors claiming the same source type would
// be ambiguous and silent).
func (r *Registry) Register(a SourceAssessor) {
	st := a.SourceType()
	if st == "" {
		panic("assessor: SourceType() returned empty string")
	}
	if _, exists := r.byType[st]; exists {
		panic(fmt.Sprintf("assessor: duplicate registration for source_type=%q", st))
	}
	r.byType[st] = a
}

// SetDefault registers a fallback assessor used by Resolve for any source
// type without a dedicated assessor. Pass the generic ConnectorAssessor here
// to get universal pre-flight coverage. Idempotent; last writer wins.
func (r *Registry) SetDefault(a SourceAssessor) {
	r.fallback = a
}

// Resolve returns the assessor for the given source type. Resolution order:
//  1. a source-type-specific assessor registered via Register();
//  2. otherwise the fallback set via SetDefault() (the generic connector
//     assessor), which covers SaaS/REST/GraphQL/cloud-storage sources.
//
// Returns (nil, false) only when there is neither a specific assessor NOR a
// fallback — in which case callers skip pre-flight (logging a warning).
func (r *Registry) Resolve(sourceType string) (SourceAssessor, bool) {
	if a, ok := r.byType[sourceType]; ok {
		return a, true
	}
	if r.fallback != nil {
		return r.fallback, true
	}
	return nil, false
}

// HasDedicated reports whether a source-type-specific assessor (not the
// fallback) is registered for sourceType. Lets handlers distinguish
// "deep DB checks" from "generic connector checks" for messaging.
func (r *Registry) HasDedicated(sourceType string) bool {
	_, ok := r.byType[sourceType]
	return ok
}

// SupportedTypes returns the list of source types with registered
// assessors. Useful for /v1/health and admin endpoints.
func (r *Registry) SupportedTypes() []string {
	out := make([]string, 0, len(r.byType))
	for k := range r.byType {
		out = append(out, k)
	}
	return out
}

// Summarize fills in the count fields + OverallStatus from a Checks slice.
// Called by implementations before returning their Result.
func Summarize(r *Result) {
	r.PassedCount = 0
	r.WarningCount = 0
	r.FailedCount = 0
	for _, c := range r.Checks {
		switch {
		case c.Passed:
			r.PassedCount++
		case c.Severity == SeverityWarning, c.Severity == SeverityInfo:
			// An info check that did not pass is an advisory (e.g. an unlimited
			// max_slot_wal_keep_size): worth showing, never a reason to block,
			// so it must not reach FailedCount and flip BlocksStart.
			r.WarningCount++
		default:
			r.FailedCount++
		}
	}
	switch {
	case r.ErrorCount > 0:
		r.OverallStatus = "error"
	case r.FailedCount > 0:
		r.OverallStatus = "failed"
	case r.WarningCount > 0:
		r.OverallStatus = "warning"
	default:
		r.OverallStatus = "passed"
	}
}

// withObject records which table/collection a per-object check is about.
func withObject(c Check, object string) Check {
	c.Object = object
	return c
}

// BlocksStart returns true when the Result should prevent pipeline start.
// Only hard failures block — warnings are
// surfaced but don't gate.
func (r *Result) BlocksStart() bool {
	return r.FailedCount > 0 || r.ErrorCount > 0
}
