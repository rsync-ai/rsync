package handlers

// Assessment tab: graded checks, run history and new-issue alerts.
//
// The pre-migration modal reads report.Tables — findings grouped by table. The
// Assessment tab reads report.Checks instead: one row per check, the way the
// AWS DMS premigration assessment lists them, each graded
//
//	critical — blocks the pipeline from starting (exactly the error findings)
//	high     — the pipeline runs, but can lose its place or its data guarantee
//	medium   — the pipeline runs; a setting is likely to cause trouble
//	low      — advisory
//
// Critical is by construction the same set as report.Blocking: only an error
// grades critical, and nothing else can. Every run is kept in
// pipeline_assessment_runs (migration 107); a run that finds a Critical or
// High issue the previous run did not have notifies the pipeline's owner once.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// AssessmentLevel grades how much a check matters.
type AssessmentLevel string

const (
	LevelCritical AssessmentLevel = "critical"
	LevelHigh     AssessmentLevel = "high"
	LevelMedium   AssessmentLevel = "medium"
	LevelLow      AssessmentLevel = "low"
)

// AssessmentCheckResult is the outcome of one check.
type AssessmentCheckResult string

const (
	ResultPassed  AssessmentCheckResult = "passed"
	ResultFailed  AssessmentCheckResult = "failed"  // an error: blocks the start
	ResultWarning AssessmentCheckResult = "warning" // runs, but needs attention
	ResultInfo    AssessmentCheckResult = "info"    // a fact about the pipeline, nothing to fix
)

// Check categories, the tab's first filter.
const (
	CategorySource      = "source"
	CategoryTables      = "tables"
	CategoryDestination = "destination"
)

// assessmentRunTrigger values, as the migration's CHECK constraint lists them.
const (
	AssessmentTriggerManual    = "manual"
	AssessmentTriggerRunGate   = "run_gate"
	AssessmentTriggerScheduled = "scheduled"
)

// assessmentRunsKept is how many runs per pipeline the history holds. At one
// scheduled run every 6 hours that is about 12 days, more with manual runs.
const assessmentRunsKept = 50

// AssessmentRemediation is how to fix a check. SQLToRun is SQL; CommandsToRun
// is anything else pasted into a shell or mongosh.
type AssessmentRemediation struct {
	Steps            []string `json:"steps,omitempty"`
	SQLToRun         []string `json:"sql_to_run,omitempty"`
	CommandsToRun    []string `json:"commands_to_run,omitempty"`
	DocURL           string   `json:"doc_url,omitempty"`
	EstimatedMinutes int      `json:"estimated_minutes,omitempty"`
}

// AssessmentCheckObject is one table or collection a check reported on.
type AssessmentCheckObject struct {
	Name    string `json:"name"`
	Message string `json:"message,omitempty"`
}

// AssessmentCheck is one row of the Assessment tab.
type AssessmentCheck struct {
	Code        string                  `json:"code"`
	Title       string                  `json:"title"`
	Category    string                  `json:"category"`
	Level       AssessmentLevel         `json:"level"`
	Result      AssessmentCheckResult   `json:"result"`
	Message     string                  `json:"message"`
	Objects     []AssessmentCheckObject `json:"objects,omitempty"`
	Remediation *AssessmentRemediation  `json:"remediation,omitempty"`
}

// AssessmentCounts counts failed and warning checks per level, plus passes.
// Info results are not issues and are not counted.
type AssessmentCounts struct {
	Critical int `json:"critical"`
	High     int `json:"high"`
	Medium   int `json:"medium"`
	Low      int `json:"low"`
	Passed   int `json:"passed"`
}

// assessmentCheckSpec is the tab's copy and nominal level for one code. The
// nominal level is what a failure of this check costs; a check reported at a
// lower severity than its nominal level is graded down (see gradeCheck).
type assessmentCheckSpec struct {
	Title string
	Level AssessmentLevel
}

// assessmentCatalog covers every code the orchestrator assessors
// (backend-orchestrator/internal/assessor) and this file's per-table and
// destination checks emit. A code missing here still renders: its title is
// humanized from the code and its level follows the severity it arrived with.
var assessmentCatalog = map[string]assessmentCheckSpec{
	// Connecting to the source.
	"CONNECTOR_CONNECTION":         {"Source connection", LevelCritical},
	"CONNECTOR_CONNECTION_FAILED":  {"Source connection", LevelCritical},
	"CONNECTOR_AUTH_FAILED":        {"Source credentials", LevelCritical},
	"CONNECTOR_CONFIG_INCOMPLETE":  {"Source configuration", LevelCritical},
	"CONNECTOR_TYPE_UNKNOWN":       {"Source connector type", LevelMedium},
	"CONNECTOR_ASSESS_UNAVAILABLE": {"Source check could not run", LevelMedium},
	"POSTGRES_CONNECTION":          {"Source connection", LevelCritical},
	"MYSQL_CONNECTION":             {"Source connection", LevelCritical},
	SourceUnreachableCode:          {"Source schema discovery", LevelCritical},
	FindingEmptyCatalog:            {"Tables to sync", LevelCritical},

	// Reading the selected tables.
	"CONNECTOR_TABLE_READABLE":          {"Table read access", LevelCritical},
	"CONNECTOR_TABLE_READ_FORBIDDEN":    {"Table read access", LevelCritical},
	"CONNECTOR_TABLE_READ_UNVERIFIED":   {"Table read access not confirmed", LevelLow},
	"CDC_TABLE_NOT_FOUND_IN_SOURCE":     {"Table exists in the source", LevelCritical},
	"CDC_TABLE_MISSING_PRIMARY_KEY":     {"Table primary key", LevelCritical},
	"MYSQL_TABLE_PRIMARY_KEY_INVISIBLE": {"Invisible primary key", LevelHigh},
	"POSTGRES_SCHEMA_NOT_VISIBLE":       {"Schema visible to the user", LevelCritical},
	FindingNoPrimaryKey:                 {"Table primary key", LevelHigh},
	FindingJSONCollapse:                 {"Nested columns land as JSON", LevelLow},

	// PostgreSQL change data capture.
	"POSTGRES_WAL_LEVEL_NOT_LOGICAL":            {"wal_level is logical", LevelCritical},
	"POSTGRES_USER_LACKS_REPLICATION":           {"User may replicate", LevelCritical},
	"POSTGRES_REPLICATION_SLOTS_EXHAUSTED":      {"Free replication slot", LevelCritical},
	"POSTGRES_MAX_REPLICATION_SLOTS_LOW":        {"max_replication_slots headroom", LevelHigh},
	"POSTGRES_MAX_WAL_SENDERS_LOW":              {"max_wal_senders headroom", LevelHigh},
	"POSTGRES_MAX_SLOT_WAL_KEEP_SIZE_UNLIMITED": {"WAL a slot may retain", LevelHigh},
	"POSTGRES_WAL_SENDER_TIMEOUT_LOW":           {"wal_sender_timeout", LevelMedium},
	"POSTGRES_PUBLICATION_PRIVILEGE":            {"User may create the publication", LevelMedium},
	"POSTGRES_LOGICAL_DECODING_WORK_MEM_LOW":    {"logical_decoding_work_mem", LevelLow},
	// Tables the pipeline does not copy: the FOR ALL TABLES publication makes
	// their UPDATE/DELETE fail without a primary key. A warning, never a block.
	"POSTGRES_UNSELECTED_TABLE_MISSING_PRIMARY_KEY": {"Primary key on tables outside the pipeline", LevelHigh},

	// MySQL change data capture.
	"MYSQL_LOG_BIN_DISABLED":          {"Binary log enabled", LevelCritical},
	"MYSQL_BINLOG_FORMAT_NOT_ROW":     {"binlog_format is ROW", LevelCritical},
	"MYSQL_BINLOG_ROW_IMAGE_NOT_FULL": {"binlog_row_image is FULL", LevelCritical},
	"MYSQL_USER_LACKS_REPLICATION":    {"User may replicate", LevelCritical},
	"MYSQL_BINLOG_EXPIRE_TOO_SHORT":   {"Binary log retention", LevelHigh},

	// MongoDB change data capture.
	"MONGODB_NOT_REPLICA_SET":            {"Replica set or sharded cluster", LevelCritical},
	"MONGODB_CHANGE_STREAM_UNAUTHORIZED": {"Change stream access", LevelCritical},
	"MONGODB_CHANGE_STREAM_UNVERIFIED":   {"Change stream access not confirmed", LevelLow},
	"MONGODB_OPLOG_WINDOW_SHORT":         {"Oplog window", LevelHigh},

	// Destination.
	FindingSinkNoDDL:                {"Destination creates missing tables", LevelCritical},
	FindingDestNamespaceInvalid:     {"Destination namespace name", LevelCritical},
	FindingDestNamespaceMissing:     {"Destination namespace exists", LevelCritical},
	FindingDestNamespaceNoPrivilege: {"User may create the namespace", LevelCritical},
	FindingDestNamespaceWillCreate:  {"Destination namespace will be created", LevelLow},
	FindingDestNamespaceExists:      {"Destination namespace exists", LevelLow},
	FindingDestNamespaceUnverified:  {"Destination namespace not confirmed", LevelLow},
	FindingDestNamespaceDefault:     {"Destination namespace", LevelLow},
}

// SourceUnreachableCode aliases FindingSourceUnreachable so the catalog reads
// in the order a user meets the checks.
const SourceUnreachableCode = FindingSourceUnreachable

// sourceReadinessTableName is the synthetic table sourceReadinessTable builds
// for the modal. The tab reads the orchestrator's checks directly instead, so
// it skips this table to avoid listing them twice.
const sourceReadinessTableName = "(source readiness)"

var levelRank = map[AssessmentLevel]int{LevelCritical: 0, LevelHigh: 1, LevelMedium: 2, LevelLow: 3}

var resultRank = map[AssessmentCheckResult]int{ResultFailed: 0, ResultWarning: 1, ResultInfo: 2, ResultPassed: 3}

// capHigh keeps anything short of an error out of Critical, so Critical stays
// exactly the set of checks that block the start.
func capHigh(l AssessmentLevel) AssessmentLevel {
	if l == LevelCritical {
		return LevelHigh
	}
	return l
}

func nominalLevel(code string, fallback AssessmentLevel) AssessmentLevel {
	if spec, ok := assessmentCatalog[code]; ok && spec.Level != "" {
		return spec.Level
	}
	return fallback
}

// gradeCheck grades an orchestrator check the way sourceReadinessTable feeds
// the gate: an error blocks whatever its passed flag says, and a warning is
// shown even when passed (a keyless table "passes" — it loads — with a
// warning about duplicates). An info check that did not pass is an advisory:
// the orchestrator never blocks on it, so it is a warning here.
func gradeCheck(code string, sev AssessmentSeverity, passed bool) (AssessmentLevel, AssessmentCheckResult) {
	switch {
	case sev == AssessmentError:
		return LevelCritical, ResultFailed
	case passed && sev != AssessmentWarning:
		return nominalLevel(code, LevelLow), ResultPassed
	case sev == AssessmentInfo:
		return capHigh(nominalLevel(code, LevelLow)), ResultWarning
	default: // warning, or a severity this gateway does not know — fail cautious
		return capHigh(nominalLevel(code, LevelMedium)), ResultWarning
	}
}

// gradeFinding grades a gateway finding. These have no pass state: an info
// finding is a fact about the pipeline (JSON columns, a nominated key), not an
// issue, so it is listed but not counted.
func gradeFinding(code string, sev AssessmentSeverity) (AssessmentLevel, AssessmentCheckResult) {
	switch sev {
	case AssessmentError:
		return LevelCritical, ResultFailed
	case AssessmentInfo:
		return capHigh(nominalLevel(code, LevelLow)), ResultInfo
	default:
		return capHigh(nominalLevel(code, LevelMedium)), ResultWarning
	}
}

func checkTitle(code string) string {
	if spec, ok := assessmentCatalog[code]; ok && spec.Title != "" {
		return spec.Title
	}
	// POSTGRES_WAL_LEVEL_NOT_LOGICAL → "Postgres wal level not logical".
	words := strings.Fields(strings.ToLower(strings.ReplaceAll(code, "_", " ")))
	if len(words) == 0 {
		return "Check"
	}
	words[0] = strings.ToUpper(words[0][:1]) + words[0][1:]
	return strings.Join(words, " ")
}

// checkBuilder groups results into one row per code, result and level, and
// collects the tables each row reported on.
type checkBuilder struct {
	order []string
	rows  map[string]*AssessmentCheck
}

func newCheckBuilder() *checkBuilder {
	return &checkBuilder{rows: map[string]*AssessmentCheck{}}
}

func (b *checkBuilder) add(code, category string, level AssessmentLevel, result AssessmentCheckResult, object, message string, rem *AssessmentRemediation) {
	code = strings.TrimSpace(code)
	if code == "" {
		return
	}
	key := code + "|" + string(result) + "|" + string(level) + "|" + category
	row, ok := b.rows[key]
	if !ok {
		row = &AssessmentCheck{
			Code:     code,
			Title:    checkTitle(code),
			Category: category,
			Level:    level,
			Result:   result,
			Message:  message,
		}
		b.rows[key] = row
		b.order = append(b.order, key)
	} else if row.Message != message {
		// Several tables, several messages: each stays on its object.
		row.Message = ""
	}
	if object != "" {
		row.Objects = append(row.Objects, AssessmentCheckObject{Name: object, Message: message})
	}
	row.Remediation = mergeRemediation(row.Remediation, rem)
}

// mergeRemediation keeps the first check's steps and link, and the union of
// every check's SQL and commands — per-table fixes differ only there.
func mergeRemediation(into, from *AssessmentRemediation) *AssessmentRemediation {
	if from == nil {
		return into
	}
	if into == nil {
		cp := *from
		cp.SQLToRun = append([]string(nil), from.SQLToRun...)
		cp.CommandsToRun = append([]string(nil), from.CommandsToRun...)
		return &cp
	}
	into.SQLToRun = appendUnique(into.SQLToRun, from.SQLToRun...)
	into.CommandsToRun = appendUnique(into.CommandsToRun, from.CommandsToRun...)
	return into
}

func appendUnique(dst []string, add ...string) []string {
	for _, s := range add {
		dup := false
		for _, d := range dst {
			if d == s {
				dup = true
				break
			}
		}
		if !dup {
			dst = append(dst, s)
		}
	}
	return dst
}

func (b *checkBuilder) checks() []AssessmentCheck {
	out := make([]AssessmentCheck, 0, len(b.order))
	for _, k := range b.order {
		row := b.rows[k]
		if row.Message == "" {
			row.Message = fmt.Sprintf("Reported on %d objects — see each one below.", len(row.Objects))
		}
		out = append(out, *row)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if levelRank[out[i].Level] != levelRank[out[j].Level] {
			return levelRank[out[i].Level] < levelRank[out[j].Level]
		}
		if resultRank[out[i].Result] != resultRank[out[j].Result] {
			return resultRank[out[i].Result] < resultRank[out[j].Result]
		}
		return out[i].Code < out[j].Code
	})
	return out
}

// findingCategory places a gateway table in the tab's categories.
func findingCategory(tableName string) string {
	switch tableName {
	case "(source)", "(catalog)":
		return CategorySource
	case "(destination namespace)":
		return CategoryDestination
	default:
		return CategoryTables
	}
}

func qualifiedTableName(t AssessmentTable) string {
	if s := strings.TrimSpace(t.Schema); s != "" {
		return s + "." + t.Name
	}
	return t.Name
}

// attachChecks fills report.Checks and report.Counts from the orchestrator's
// checks (all of them, passes included) and the report's own findings.
func attachChecks(report *AssessmentReport, ra *orchestratorAssessment) {
	if report == nil {
		return
	}
	b := newCheckBuilder()
	if ra != nil {
		for _, c := range ra.Checks {
			sev := AssessmentSeverity(strings.ToLower(strings.TrimSpace(c.Severity)))
			level, result := gradeCheck(c.Code, sev, c.Passed)
			category := CategorySource
			if strings.TrimSpace(c.Object) != "" {
				category = CategoryTables
			}
			b.add(c.Code, category, level, result, strings.TrimSpace(c.Object), c.Message, c.Remediation)
		}
	}
	for _, t := range report.Tables {
		if t.Name == sourceReadinessTableName {
			continue
		}
		category := findingCategory(t.Name)
		object := ""
		if category == CategoryTables {
			object = qualifiedTableName(t)
		}
		for _, f := range t.Findings {
			level, result := gradeFinding(f.Code, f.Severity)
			b.add(f.Code, category, level, result, object, f.Message, nil)
		}
	}
	report.Checks = b.checks()
	report.Counts = countChecks(report.Checks)
}

func countChecks(checks []AssessmentCheck) *AssessmentCounts {
	c := &AssessmentCounts{}
	for _, ch := range checks {
		switch ch.Result {
		case ResultPassed:
			c.Passed++
		case ResultFailed, ResultWarning:
			switch ch.Level {
			case LevelCritical:
				c.Critical++
			case LevelHigh:
				c.High++
			case LevelMedium:
				c.Medium++
			default:
				c.Low++
			}
		}
	}
	return c
}

// alertKeys lists the Critical and High issues in checks, one key per check
// and table, so a keyless table added to a pipeline that already had one is
// still news.
func alertKeys(checks []AssessmentCheck) map[string]AssessmentCheck {
	out := map[string]AssessmentCheck{}
	for _, c := range checks {
		if c.Result != ResultFailed && c.Result != ResultWarning {
			continue
		}
		if c.Level != LevelCritical && c.Level != LevelHigh {
			continue
		}
		if len(c.Objects) == 0 {
			out[c.Code] = c
			continue
		}
		for _, o := range c.Objects {
			out[c.Code+"|"+o.Name] = c
		}
	}
	return out
}

// newAlertIssues returns the Critical/High keys in current that previous did
// not have, sorted. With no previous run every Critical/High issue is new.
func newAlertIssues(previous, current []AssessmentCheck) []string {
	before := alertKeys(previous)
	var fresh []string
	for k := range alertKeys(current) {
		if _, seen := before[k]; !seen {
			fresh = append(fresh, k)
		}
	}
	sort.Strings(fresh)
	return fresh
}

// ── Persistence ─────────────────────────────────────────────────────────────

// assessmentPublisher delivers an in-process notification through the
// notifier's persist → dedup → Slack/email path (notifier.Notifier.Publish).
type assessmentPublisher interface {
	Publish(ctx context.Context, raw []byte) error
}

var (
	assessmentNotifierMu sync.RWMutex
	assessmentNotifier   assessmentPublisher
)

// SetAssessmentNotifier wires the publisher that tells a pipeline's owner
// about a new Critical or High assessment issue. Unset, runs are still
// recorded; nobody is notified.
func SetAssessmentNotifier(p assessmentPublisher) {
	assessmentNotifierMu.Lock()
	defer assessmentNotifierMu.Unlock()
	assessmentNotifier = p
}

func currentAssessmentNotifier() assessmentPublisher {
	assessmentNotifierMu.RLock()
	defer assessmentNotifierMu.RUnlock()
	return assessmentNotifier
}

// recordAssessmentRun stores a run, trims the pipeline's history and, when the
// run found a Critical or High issue the previous run did not have, notifies
// the owner in the background. It returns the new run's id. A failure here is
// logged by the caller and never fails the request that ran the assessment.
func recordAssessmentRun(ctx context.Context, database *sql.DB, pipelineID, trigger, triggeredBy string, report *AssessmentReport) (string, error) {
	if report == nil {
		return "", nil
	}
	if report.Checks == nil {
		attachChecks(report, nil)
	}
	var previous []AssessmentCheck
	var prevRaw []byte
	err := database.QueryRowContext(ctx, `
		SELECT COALESCE(report->'checks', '[]'::jsonb)
		FROM pipeline_assessment_runs
		WHERE pipeline_id = $1
		ORDER BY created_at DESC
		LIMIT 1
	`, pipelineID).Scan(&prevRaw)
	hadPrevious := err == nil
	if err != nil && err != sql.ErrNoRows {
		return "", fmt.Errorf("load previous assessment run: %w", err)
	}
	if hadPrevious {
		if jerr := json.Unmarshal(prevRaw, &previous); jerr != nil {
			log.Warnf("assessment run pipeline=%s: previous run unreadable, treating as none: %v", pipelineID, jerr)
		}
	}

	body, err := json.Marshal(report)
	if err != nil {
		return "", fmt.Errorf("encode assessment report: %w", err)
	}
	counts := report.Counts
	if counts == nil {
		counts = countChecks(report.Checks)
	}
	var by interface{}
	if strings.TrimSpace(triggeredBy) != "" {
		by = triggeredBy
	}
	var runID string
	if err := database.QueryRowContext(ctx, `
		INSERT INTO pipeline_assessment_runs
			(pipeline_id, trigger, triggered_by, blocking,
			 critical_count, high_count, medium_count, low_count, passed_count, report)
		VALUES ($1::uuid, $2, $3::uuid, $4, $5, $6, $7, $8, $9, $10::jsonb)
		RETURNING id::text
	`, pipelineID, trigger, by, report.Blocking,
		counts.Critical, counts.High, counts.Medium, counts.Low, counts.Passed, string(body),
	).Scan(&runID); err != nil {
		return "", fmt.Errorf("insert assessment run: %w", err)
	}

	if _, err := database.ExecContext(ctx, `
		DELETE FROM pipeline_assessment_runs
		WHERE pipeline_id = $1::uuid AND id NOT IN (
			SELECT id FROM pipeline_assessment_runs
			WHERE pipeline_id = $1::uuid
			ORDER BY created_at DESC
			LIMIT $2
		)
	`, pipelineID, assessmentRunsKept); err != nil {
		log.Warnf("assessment run pipeline=%s: trim history: %v", pipelineID, err)
	}

	if fresh := newAlertIssues(previous, report.Checks); len(fresh) > 0 {
		if p := currentAssessmentNotifier(); p != nil {
			raw := assessmentNotificationPayload(pipelineID, report, fresh)
			go func() {
				nctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := p.Publish(nctx, raw); err != nil {
					log.Warnf("assessment run pipeline=%s: notify new issues: %v", pipelineID, err)
				}
			}()
		}
	}
	return runID, nil
}

// assessmentNotificationCode is the notifier catalog key for this alert
// (api-gateway/internal/notifier/catalog.go).
const assessmentNotificationCode = "PRE_MIGRATION_ASSESSMENT_ISSUE"

// assessmentNotificationPayload builds the rsync.notifications-shaped event.
// The body names check titles and tables only — metadata, never row values.
func assessmentNotificationPayload(pipelineID string, report *AssessmentReport, fresh []string) []byte {
	severity := "warning"
	byCheck := map[string][]string{}
	var order []string
	for _, k := range fresh {
		code, object, _ := strings.Cut(k, "|")
		if _, ok := byCheck[code]; !ok {
			order = append(order, code)
		}
		if object != "" {
			byCheck[code] = append(byCheck[code], object)
		} else if byCheck[code] == nil {
			byCheck[code] = []string{}
		}
	}
	levelOf := map[string]AssessmentLevel{}
	for _, c := range report.Checks {
		if c.Result == ResultFailed || c.Result == ResultWarning {
			if prev, ok := levelOf[c.Code]; !ok || levelRank[c.Level] < levelRank[prev] {
				levelOf[c.Code] = c.Level
			}
		}
	}
	parts := make([]string, 0, len(order))
	for _, code := range order {
		level := levelOf[code]
		if level == LevelCritical {
			severity = "critical"
		}
		part := fmt.Sprintf("%s (%s)", checkTitle(code), level)
		if objs := byCheck[code]; len(objs) > 0 {
			shown := objs
			if len(shown) > 3 {
				shown = append(append([]string(nil), shown[:3]...), fmt.Sprintf("%d more", len(objs)-3))
			}
			part += ": " + strings.Join(shown, ", ")
		}
		parts = append(parts, part)
	}
	noun := "issue"
	if len(parts) != 1 {
		noun = "issues"
	}
	message := fmt.Sprintf("The pre-migration assessment found %d new %s: %s.", len(parts), noun, strings.Join(parts, "; "))

	payload := map[string]interface{}{
		"type":        "pre_migration_assessment",
		"pipeline_id": pipelineID,
		"message":     message,
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"action_url":  "/pipelines/" + pipelineID + "?tab=assessment",
		"error": map[string]interface{}{
			"failure_type":   "PRE_MIGRATION_ASSESSMENT",
			"code":           assessmentNotificationCode,
			"severity":       severity,
			"audience":       "user",
			"user_message":   message,
			"source_db_type": report.SourceType,
			"dedup_subject":  strings.Join(fresh, ","),
		},
	}
	raw, _ := json.Marshal(payload)
	return raw
}

// ── History endpoints ───────────────────────────────────────────────────────

// assessmentRunSummary is one row of the tab's history list.
type assessmentRunSummary struct {
	ID          string           `json:"id"`
	Trigger     string           `json:"trigger"`
	TriggeredBy *string          `json:"triggered_by,omitempty"`
	Blocking    bool             `json:"blocking"`
	Counts      AssessmentCounts `json:"counts"`
	CreatedAt   time.Time        `json:"created_at"`
}

// ListPipelineAssessments handles GET /api/v1/pipelines/:id/assessments —
// the pipeline's assessment runs, newest first, without their reports.
func ListPipelineAssessments(c *gin.Context) {
	id, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, id, security.WSViewer); !ok {
		return
	}
	limit := 20
	if v, err := strconv.Atoi(c.Query("limit")); err == nil && v > 0 {
		limit = v
	}
	if limit > assessmentRunsKept {
		limit = assessmentRunsKept
	}
	rows, err := db.GetDB().QueryContext(c.Request.Context(), `
		SELECT id::text, trigger, triggered_by::text, blocking,
		       critical_count, high_count, medium_count, low_count, passed_count, created_at
		FROM pipeline_assessment_runs
		WHERE pipeline_id = $1::uuid
		ORDER BY created_at DESC
		LIMIT $2
	`, id, limit)
	if err != nil {
		log.Errorf("list assessment runs pipeline=%s: %v", id, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load assessment history"})
		return
	}
	defer rows.Close()
	runs := []assessmentRunSummary{}
	for rows.Next() {
		var r assessmentRunSummary
		var by sql.NullString
		if err := rows.Scan(&r.ID, &r.Trigger, &by, &r.Blocking,
			&r.Counts.Critical, &r.Counts.High, &r.Counts.Medium, &r.Counts.Low, &r.Counts.Passed, &r.CreatedAt); err != nil {
			log.Errorf("scan assessment run pipeline=%s: %v", id, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load assessment history"})
			return
		}
		if by.Valid {
			r.TriggeredBy = &by.String
		}
		runs = append(runs, r)
	}
	if err := rows.Err(); err != nil {
		log.Errorf("iterate assessment runs pipeline=%s: %v", id, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load assessment history"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"runs": runs})
}

// GetPipelineAssessment handles GET /api/v1/pipelines/:id/assessments/:run_id
// — one run with its full report. "latest" names the newest run.
func GetPipelineAssessment(c *gin.Context) {
	id, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, id, security.WSViewer); !ok {
		return
	}
	var (
		row *sql.Row
		q   = `
		SELECT id::text, trigger, triggered_by::text, blocking,
		       critical_count, high_count, medium_count, low_count, passed_count, created_at, report
		FROM pipeline_assessment_runs
		WHERE pipeline_id = $1::uuid`
	)
	if strings.TrimSpace(c.Param("run_id")) == "latest" {
		row = db.GetDB().QueryRowContext(c.Request.Context(), q+` ORDER BY created_at DESC LIMIT 1`, id)
	} else {
		runID, ok := requireUUIDParam(c, "run_id", "invalid_assessment_run_id", "Invalid assessment run ID format")
		if !ok {
			return
		}
		row = db.GetDB().QueryRowContext(c.Request.Context(), q+` AND id = $2::uuid`, id, runID)
	}
	var r assessmentRunSummary
	var by sql.NullString
	var report json.RawMessage
	err := row.Scan(&r.ID, &r.Trigger, &by, &r.Blocking,
		&r.Counts.Critical, &r.Counts.High, &r.Counts.Medium, &r.Counts.Low, &r.Counts.Passed, &r.CreatedAt, &report)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Assessment run not found"})
		return
	}
	if err != nil {
		log.Errorf("get assessment run pipeline=%s: %v", id, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load assessment run"})
		return
	}
	if by.Valid {
		r.TriggeredBy = &by.String
	}
	c.JSON(http.StatusOK, gin.H{"run": r, "report": report})
}

// ── Periodic re-check ───────────────────────────────────────────────────────
//
// A source that passed its assessment can drift while the pipeline streams: a
// DBA drops max_replication_slots, a table loses its primary key, an oplog is
// resized. Every running CDC pipeline is re-assessed once per
// ASSESSMENT_RECHECK_INTERVAL (a Go duration, default 6h; "0" turns it off),
// and a new Critical or High issue notifies the owner (recordAssessmentRun).
// A pipeline that already has a run — manual or from its last start — inside
// the interval is skipped, so nothing is assessed more often than that.

const (
	assessmentRecheckDefault = 6 * time.Hour
	// assessmentRecheckFloor stops a typo ("6m") from turning the re-check into
	// a steady load on every customer source.
	assessmentRecheckFloor = 15 * time.Minute
	// assessmentRecheckStartDelay lets the orchestrator and connectors come up
	// after a restart before the first round.
	assessmentRecheckStartDelay = 5 * time.Minute
	// assessmentRecheckBatch bounds one round; the rest wait for the next tick.
	assessmentRecheckBatch = 100
	// assessmentRecheckLockNamespace keeps two gateway replicas from both
	// running a round against the same sources.
	assessmentRecheckLockNamespace = 0x72534153 // "rSAS": rsync assessment scheduler
)

func assessmentRecheckInterval() time.Duration {
	v := strings.TrimSpace(os.Getenv("ASSESSMENT_RECHECK_INTERVAL"))
	if v == "" {
		return assessmentRecheckDefault
	}
	if v == "0" {
		return 0
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		log.Warnf("ASSESSMENT_RECHECK_INTERVAL=%q is not a duration; using %s", v, assessmentRecheckDefault)
		return assessmentRecheckDefault
	}
	if d == 0 {
		return 0
	}
	if d < assessmentRecheckFloor {
		log.Warnf("ASSESSMENT_RECHECK_INTERVAL=%s is below the %s floor; using %s", d, assessmentRecheckFloor, assessmentRecheckFloor)
		return assessmentRecheckFloor
	}
	return d
}

// assessmentRecheckTick checks for due pipelines several times per interval,
// so one assessed by hand an hour before a tick is not left for two intervals.
func assessmentRecheckTick(interval time.Duration) time.Duration {
	t := interval / 6
	if t < 5*time.Minute {
		t = 5 * time.Minute
	}
	if t > time.Hour {
		t = time.Hour
	}
	return t
}

// StartAssessmentScheduler re-assesses running CDC pipelines until ctx ends.
func StartAssessmentScheduler(ctx context.Context, database *sql.DB) {
	interval := assessmentRecheckInterval()
	if interval <= 0 {
		log.Info("assessment re-check disabled (ASSESSMENT_RECHECK_INTERVAL=0)")
		return
	}
	tick := assessmentRecheckTick(interval)
	log.Infof("assessment re-check: running CDC pipelines every %s (checked every %s)", interval, tick)
	go func() {
		timer := time.NewTimer(assessmentRecheckStartDelay)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			recheckRunningCDCPipelines(ctx, database, interval)
			timer.Reset(tick)
		}
	}()
}

type dueAssessment struct {
	pipelineID, workspaceID, ownerID string
}

// dueAssessmentsSQL selects running CDC pipelines with no assessment run in
// the last $1 seconds. pipelineRowIsCDCSQL is the same CDC test the pipeline
// list and detail page use.
var dueAssessmentsSQL = `
	SELECT p.id::text, COALESCE(p.workspace_id::text, ''), COALESCE(p.created_by::text, '')
	FROM pipelines p
	WHERE p.status = 'running'
	  AND ` + pipelineRowIsCDCSQL + `
	  AND NOT EXISTS (
		SELECT 1 FROM pipeline_assessment_runs r
		WHERE r.pipeline_id = p.id
		  AND r.created_at > NOW() - make_interval(secs => $1)
	  )
	ORDER BY p.id
	LIMIT $2`

func recheckRunningCDCPipelines(ctx context.Context, database *sql.DB, interval time.Duration) {
	conn, err := database.Conn(ctx)
	if err != nil {
		log.Warnf("assessment re-check: reserve connection: %v", err)
		return
	}
	defer conn.Close()
	var locked bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, int64(assessmentRecheckLockNamespace)).Scan(&locked); err != nil {
		log.Warnf("assessment re-check: take lock: %v", err)
		return
	}
	if !locked {
		return // another replica is running this round
	}
	defer func() {
		// A session lock survives conn.Close() (the session goes back to the
		// pool), so release it explicitly, on a context that is still live.
		relCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(relCtx, `SELECT pg_advisory_unlock($1)`, int64(assessmentRecheckLockNamespace)); err != nil {
			log.Warnf("assessment re-check: release lock: %v", err)
		}
	}()

	rows, err := conn.QueryContext(ctx, dueAssessmentsSQL, interval.Seconds(), assessmentRecheckBatch)
	if err != nil {
		log.Warnf("assessment re-check: list due pipelines: %v", err)
		return
	}
	var due []dueAssessment
	for rows.Next() {
		var d dueAssessment
		if err := rows.Scan(&d.pipelineID, &d.workspaceID, &d.ownerID); err != nil {
			log.Warnf("assessment re-check: scan due pipeline: %v", err)
			continue
		}
		due = append(due, d)
	}
	rows.Close()

	for _, d := range due {
		if ctx.Err() != nil {
			return
		}
		if d.ownerID == "" {
			continue // buildPipelineAssessment loads the pipeline by its owner
		}
		recheckOne(ctx, database, d)
	}
}

func recheckOne(ctx context.Context, database *sql.DB, d dueAssessment) {
	actx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	report, status, errResp, err := buildPipelineAssessment(actx, database, d.workspaceID, d.pipelineID, d.ownerID)
	switch {
	case err != nil:
		log.Warnf("assessment re-check pipeline=%s: %v", d.pipelineID, err)
		return
	case errResp != nil:
		log.Warnf("assessment re-check pipeline=%s: %d %s", d.pipelineID, status, errResp["error"])
		return
	}
	rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
	defer rcancel()
	if _, err := recordAssessmentRun(rctx, database, d.pipelineID, AssessmentTriggerScheduled, "", report); err != nil {
		log.Warnf("assessment re-check pipeline=%s: record run: %v", d.pipelineID, err)
	}
}
