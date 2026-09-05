package handlers

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"
	"api-gateway/internal/validators"

	"github.com/rsync-ai/shared/pgdriver"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// ============================================================================
// Saved Explorer Queries
// ============================================================================
// Workspace-scoped CRUD over saved_queries (migration 084), replacing the
// per-browser localStorage history the Explorer shipped with.
//
// Authorization model — two independent axes, deliberately NOT merged:
//
//   1. RESOURCE access (does this row belong to you?) — requireResourceRole
//      against the "saved_queries" allowlist entry. One JOIN proves the row
//      exists, sits in the caller's ACTIVE workspace, and the caller is a
//      member. Foreign rows 404, never 403, so the endpoint cannot be used to
//      probe which query IDs exist in other tenants.
//
//   2. SAVING is not EXECUTING. Storing `DROP TABLE x` mutates nothing; it is a
//      member-level act. Running it is owner-level and stays gated where the
//      execution happens (/explorer/query → statement_policy). So these handlers
//      never gate on statement class, and the statement_class column they write
//      is advisory metadata for the UI — every execution path re-derives the
//      class from the CURRENT sql_text. Treating a stored class as an
//      authorization input would make a plain UPDATE a privilege escalation.
//
// The one class check here is a usability guard, not a security one: a
// ClassBlocked statement can never run through the Explorer under any role, so
// accepting it would store a query that silently fails the first time anyone
// clicks Run.

// savedQueryMaxSQLBytes caps stored SQL. Explorer statements are hand-written or
// LLM-generated single statements; anything past this is a paste accident or an
// attempt to bloat the row. Rejecting early keeps the versions table bounded too,
// since every edit appends a copy.
const savedQueryMaxSQLBytes = 256 * 1024

// savedQueryMaxNameLen bounds the display name so list UIs stay legible.
const savedQueryMaxNameLen = 200

// SavedQuery is the API shape of a saved_queries row.
type SavedQuery struct {
	ID             string  `json:"id"`
	WorkspaceID    string  `json:"workspace_id"`
	ConnectionID   string  `json:"connection_id"`
	Name           string  `json:"name"`
	Description    string  `json:"description,omitempty"`
	SQLText        string  `json:"sql_text"`
	NLPrompt       string  `json:"nl_prompt,omitempty"`
	StatementClass string  `json:"statement_class"`
	Visibility     string  `json:"visibility"`
	CreatedBy      string  `json:"created_by"`
	UpdatedBy      string  `json:"updated_by,omitempty"`
	CreatedAt      string  `json:"created_at"`
	UpdatedAt      string  `json:"updated_at"`
	LastRunAt      *string `json:"last_run_at,omitempty"`

	// Model fields (migrations 085, 088). Materialization is "none" for a plain saved
	// query, "table" when a run rebuilds TargetTable from a SELECT, and "statement"
	// when a run executes SQLText as written — that mode names its destination inside
	// the SQL and therefore carries no TargetTable at all.
	Materialization string `json:"materialization"`
	TargetTable     string `json:"target_table,omitempty"`
	LastRunStatus   string `json:"last_run_status,omitempty"`
	LastRunError    string `json:"last_run_error,omitempty"`

	// ConnectorType is the engine behind ConnectionID, carried on the row so a caller
	// rendering a list of queries does not have to fetch every connection to name what
	// each one runs against. "" when the connection has been deleted.
	ConnectorType string `json:"connector_type,omitempty"`

	// SupportsMaterialization is resolved from ConnectorType through the Explorer
	// capability table, never re-derived from the string client-side — a frontend
	// substring allowlist is exactly what that resolver exists to replace. It is what
	// the model dialog reads to disable "table"/"statement" with a reason, instead of
	// letting the user fill in the form and collect the 400 that
	// SetSavedQueryMaterialization would return. "none" stays available on purpose: the
	// server allows it for every engine, so a model created before this gate existed can
	// still be turned off.
	SupportsMaterialization bool `json:"supports_materialization"`

	// ScheduleStatus is the live schedule's status, or "" when there is none. It
	// comes from a LEFT JOIN rather than a per-row lookup so the list stays one
	// query — a schedule badge is not worth an N+1.
	ScheduleStatus string `json:"schedule_status,omitempty"`

	// NextRunAt answers the question a schedule badge raises but cannot answer —
	// "scheduled for WHEN?" — computed from the joined spec on the same row. nil for
	// an unscheduled or paused query, and for a spec too broken to read.
	NextRunAt *time.Time `json:"next_run_at,omitempty"`
}

// applyModelCapability fills the connector-derived fields from the Explorer capability
// table. Shared by the list and single-row reads for the same reason savedQueryNextRun
// is: two call sites resolving the same connector_type by hand is one refactor away from
// answering differently. Mirrors Connection.applyExplorerCapability.
func (q *SavedQuery) applyModelCapability(connectorType string) {
	q.ConnectorType = connectorType
	q.SupportsMaterialization = ResolveExplorerCapability(connectorType).SupportsMaterialization
}

// savedQueryNextRun turns the joined schedule columns into a next fire time. Shared by
// the list and single-row reads so the two cannot answer the same question differently.
// Returns nil for anything but a live active schedule with a readable spec — a paused
// schedule has no next run, and a guess is worse than a blank.
func savedQueryNextRun(status, scheduleType string, specJSON []byte, now time.Time) *time.Time {
	if status != "active" || scheduleType == "" || len(specJSON) == 0 {
		return nil
	}
	var spec ScheduleSpec
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		log.WithError(err).Warn("unreadable schedule spec on saved query")
		return nil
	}
	return nextScheduleRun(scheduleType, spec, now)
}

// SavedQueryVersion is one pre-edit snapshot from saved_query_versions.
type SavedQueryVersion struct {
	Version        int    `json:"version"`
	Name           string `json:"name"`
	SQLText        string `json:"sql_text"`
	StatementClass string `json:"statement_class"`
	ChangedBy      string `json:"changed_by"`
	CreatedAt      string `json:"created_at"`
}

type createSavedQueryRequest struct {
	ConnectionID string `json:"connection_id"`
	Name         string `json:"name"`
	Description  string `json:"description"`
	SQLText      string `json:"sql_text"`
	NLPrompt     string `json:"nl_prompt"`
	Visibility   string `json:"visibility"`
}

// updateSavedQueryRequest uses pointers so PATCH can distinguish "field absent"
// (leave alone) from "field set to empty string" (clear it). A non-pointer struct
// would silently blank every field the client did not resend.
type updateSavedQueryRequest struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
	SQLText     *string `json:"sql_text"`
	Visibility  *string `json:"visibility"`

	// Why the SQL should change. Read only when the edit becomes a proposal (096);
	// on an ungated edit there is no reviewer for it to be addressed to. Not a
	// pointer: absent and empty mean the same thing here — no note was given.
	Note string `json:"note"`

	// ExpectedUpdatedAt is the updated_at the client last saw, echoed back from a
	// GET. Optional, and omitting it is not a loophole: the handler always checks
	// for a write that landed while the request itself was in flight. What this
	// adds is the longer window — an editor left open for ten minutes while a
	// teammate saved — which no server-side check can see, because by the time the
	// request arrives the server's own read already reflects the other write.
	//
	// Sending it turns a silent overwrite into a 409. Not sending it leaves
	// last-write-wins for that window, which is the behaviour every existing client
	// already has, so adding the field breaks none of them.
	ExpectedUpdatedAt *string `json:"expected_updated_at"`
}

// normalizeVisibility validates the visibility enum, defaulting to "workspace".
// Sharing with the workspace is the default because the whole point of moving
// history off localStorage was to make queries visible to teammates; "private"
// is the deliberate opt-out.
func normalizeVisibility(v string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "workspace":
		return "workspace", true
	case "private":
		return "private", true
	default:
		return "", false
	}
}

// isUniqueViolation reports whether err is a Postgres unique-constraint error
// (23505) — here, the case-insensitive per-workspace name index.
func isUniqueViolation(err error) bool {
	return pgdriver.SQLState(err) == "23505"
}

// classifyForStorage derives the advisory statement_class and rejects statements
// no role can ever run via the Explorer. See the package header: this is a
// usability guard, not an authorization decision.
func classifyForStorage(c *gin.Context, sqlText string) (string, bool) {
	class := validators.ClassifyStatementSQL(sqlText)
	if class == validators.ClassBlocked {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "this statement type cannot be run from the Data Explorer, so it cannot be saved",
			"code":  validators.ErrCodeStatementBlocked,
		})
		return "", false
	}
	return string(class), true
}

// ListSavedQueries returns the active workspace's saved queries, newest-updated
// first. Private queries belonging to other members are filtered out in SQL
// rather than after the fact, so they never enter the response buffer.
func ListSavedQueries(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	if _, ok := requireWorkspaceRole(c, security.WSViewer); !ok {
		return
	}
	workspaceID, ok := resolveActiveWorkspace(c)
	if !ok {
		return
	}
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	// Optional narrowing to one connection, for the Explorer's per-connection panel.
	connectionID := strings.TrimSpace(c.Query("connection_id"))

	rows, err := database.Query(`
		SELECT sq.id, sq.workspace_id, sq.connection_id, sq.name, COALESCE(sq.description, ''),
		       sq.sql_text, COALESCE(sq.nl_prompt, ''), sq.statement_class, sq.visibility,
		       sq.created_by, COALESCE(sq.updated_by::text, ''),
		       sq.created_at, sq.updated_at, sq.last_run_at,
		       sq.materialization, COALESCE(sq.target_table, ''),
		       COALESCE(sq.last_run_status, ''), COALESCE(sq.last_run_error, ''),
		       COALESCE(s.status, ''), COALESCE(s.schedule_type, ''), s.schedule_spec,
		       COALESCE(cn.connector_type, '')
		FROM saved_queries sq
		LEFT JOIN saved_query_schedules s
		  ON s.saved_query_id = sq.id AND s.status != 'deleted'
		-- LEFT JOIN so a query outlives its connection: '' resolves to
		-- SupportsMaterialization=false, which is the honest answer for a query whose
		-- engine is gone.
		LEFT JOIN connections cn ON cn.id = sq.connection_id
		WHERE sq.workspace_id = $1
		  AND (sq.visibility = 'workspace' OR sq.created_by = $2)
		  AND ($3 = '' OR sq.connection_id::text = $3)
		ORDER BY sq.updated_at DESC
		LIMIT 500
	`, workspaceID, userID, connectionID)
	if err != nil {
		log.WithError(err).Error("list saved queries")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list saved queries"})
		return
	}
	defer rows.Close()

	now := time.Now()
	out := make([]SavedQuery, 0)
	for rows.Next() {
		var q SavedQuery
		var lastRun sql.NullString
		var scheduleType string
		var specJSON []byte
		var connectorType string
		if err := rows.Scan(&q.ID, &q.WorkspaceID, &q.ConnectionID, &q.Name, &q.Description,
			&q.SQLText, &q.NLPrompt, &q.StatementClass, &q.Visibility,
			&q.CreatedBy, &q.UpdatedBy, &q.CreatedAt, &q.UpdatedAt, &lastRun,
			&q.Materialization, &q.TargetTable, &q.LastRunStatus, &q.LastRunError,
			&q.ScheduleStatus, &scheduleType, &specJSON, &connectorType); err != nil {
			log.WithError(err).Error("scan saved query")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list saved queries"})
			return
		}
		if lastRun.Valid {
			v := lastRun.String
			q.LastRunAt = &v
		}
		q.applyModelCapability(connectorType)
		q.NextRunAt = savedQueryNextRun(q.ScheduleStatus, scheduleType, specJSON, now)
		out = append(out, q)
	}
	if err := rows.Err(); err != nil {
		log.WithError(err).Error("iterate saved queries")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list saved queries"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"saved_queries": out, "count": len(out)})
}

// CreateSavedQuery stores a new query in the active workspace. Requires member:
// viewers get a read-only Explorer and must not be able to plant SQL that an
// admin might later run or schedule.
func CreateSavedQuery(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	if _, ok := requireWorkspaceRole(c, security.WSMember); !ok {
		return
	}
	workspaceID, ok := resolveActiveWorkspace(c)
	if !ok {
		return
	}

	var req createSavedQueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	name := strings.TrimSpace(req.Name)
	sqlText := strings.TrimSpace(req.SQLText)
	connectionID := strings.TrimSpace(req.ConnectionID)

	switch {
	case name == "":
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	case len(name) > savedQueryMaxNameLen:
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is too long"})
		return
	case sqlText == "":
		c.JSON(http.StatusBadRequest, gin.H{"error": "sql_text is required"})
		return
	case len(sqlText) > savedQueryMaxSQLBytes:
		c.JSON(http.StatusBadRequest, gin.H{"error": "sql_text is too large"})
		return
	case connectionID == "":
		c.JSON(http.StatusBadRequest, gin.H{"error": "connection_id is required"})
		return
	}

	visibility, ok := normalizeVisibility(req.Visibility)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "visibility must be 'private' or 'workspace'"})
		return
	}

	// Classification is pure local computation — do it before the connection
	// round-trip so a request that is going to 400 never costs a query.
	statementClass, ok := classifyForStorage(c, sqlText)
	if !ok {
		return
	}

	// The connection is caller-supplied, so prove it belongs to the ACTIVE
	// workspace before storing a row that points at it. Without this a member
	// could bind a saved query to another tenant's connection ID and turn a later
	// execute/schedule into a cross-tenant read. Viewer is the right minimum: this
	// is a reference check, not a use of the connection.
	if _, ok := requireResourceRole(c, "connections", connectionID, security.WSViewer); !ok {
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	var id string
	err := database.QueryRow(`
		INSERT INTO saved_queries
			(workspace_id, connection_id, name, description, sql_text, nl_prompt,
			 statement_class, visibility, created_by, updated_by)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5, NULLIF($6, ''), $7, $8, $9, $9)
		RETURNING id
	`, workspaceID, connectionID, name, strings.TrimSpace(req.Description), sqlText,
		strings.TrimSpace(req.NLPrompt), statementClass, visibility, userID).Scan(&id)
	if isUniqueViolation(err) {
		c.JSON(http.StatusConflict, gin.H{"error": "a saved query with this name already exists in this workspace"})
		return
	}
	if err != nil {
		log.WithError(err).Error("create saved query")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save query"})
		return
	}

	logAudit(c, "saved_query.create", "saved_query", id, map[string]interface{}{
		"name":            name,
		"connection_id":   connectionID,
		"statement_class": statementClass,
		"visibility":      visibility,
	})

	c.JSON(http.StatusCreated, gin.H{
		"id":              id,
		"name":            name,
		"statement_class": statementClass,
		"visibility":      visibility,
	})
}

// loadSavedQuery fetches one row after requireResourceRole has already proven
// workspace membership, and enforces the private-visibility rule. It returns 404
// (never 403) for another member's private query: the caller has no business
// learning that the row exists.
func loadSavedQuery(c *gin.Context, id, userID string) (*SavedQuery, bool) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return nil, false
	}

	var q SavedQuery
	var lastRun sql.NullString
	var scheduleType string
	var specJSON []byte
	var connectorType string
	err := database.QueryRow(`
		SELECT sq.id, sq.workspace_id, sq.connection_id, sq.name, COALESCE(sq.description, ''),
		       sq.sql_text, COALESCE(sq.nl_prompt, ''), sq.statement_class, sq.visibility,
		       sq.created_by, COALESCE(sq.updated_by::text, ''),
		       sq.created_at, sq.updated_at, sq.last_run_at,
		       sq.materialization, COALESCE(sq.target_table, ''),
		       COALESCE(sq.last_run_status, ''), COALESCE(sq.last_run_error, ''),
		       COALESCE(s.status, ''), COALESCE(s.schedule_type, ''), s.schedule_spec,
		       COALESCE(cn.connector_type, '')
		FROM saved_queries sq
		LEFT JOIN saved_query_schedules s
		  ON s.saved_query_id = sq.id AND s.status != 'deleted'
		LEFT JOIN connections cn ON cn.id = sq.connection_id
		WHERE sq.id = $1
	`, id).Scan(&q.ID, &q.WorkspaceID, &q.ConnectionID, &q.Name, &q.Description,
		&q.SQLText, &q.NLPrompt, &q.StatementClass, &q.Visibility,
		&q.CreatedBy, &q.UpdatedBy, &q.CreatedAt, &q.UpdatedAt, &lastRun,
		&q.Materialization, &q.TargetTable, &q.LastRunStatus, &q.LastRunError,
		&q.ScheduleStatus, &scheduleType, &specJSON, &connectorType)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return nil, false
	}
	if err != nil {
		log.WithError(err).Error("load saved query")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load saved query"})
		return nil, false
	}
	if q.Visibility == "private" && q.CreatedBy != userID {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return nil, false
	}
	if lastRun.Valid {
		v := lastRun.String
		q.LastRunAt = &v
	}
	q.applyModelCapability(connectorType)
	q.NextRunAt = savedQueryNextRun(q.ScheduleStatus, scheduleType, specJSON, time.Now())
	return &q, true
}

// GetSavedQuery returns one saved query with its SQL.
func GetSavedQuery(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	id := strings.TrimSpace(c.Param("id"))
	if _, ok := requireResourceRole(c, "saved_queries", id, security.WSViewer); !ok {
		return
	}
	q, ok := loadSavedQuery(c, id, userID)
	if !ok {
		return
	}
	c.JSON(http.StatusOK, q)
}

// rowQueryer is the sliver of *sql.DB and *sql.Tx that the helpers below share, so a
// caller holding a transaction can run them inside it. Callers that hold a row lock
// MUST pass the transaction: a helper that reads on the pool answers about a snapshot
// the caller is not in, which on the approval gate is the difference between a check
// and the appearance of one.
type rowQueryer interface {
	QueryRow(query string, args ...interface{}) *sql.Row
}

// savedQueryIsScheduled reports whether a live schedule exists for this query.
//
// 'deleted' is excluded but 'paused' is NOT. A paused schedule is a schedule someone
// intends to resume, and if an edit could land freely while it is paused then pausing
// becomes the way around the approval gate: pause, rewrite, resume, and the next run
// computes something nobody reviewed. To edit a query's SQL without review, delete its
// schedule — at which point it is an ordinary saved query and nothing downstream
// depends on it.
func savedQueryIsScheduled(q rowQueryer, savedQueryID string) (bool, error) {
	var exists bool
	err := q.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM saved_query_schedules
			WHERE saved_query_id = $1 AND status IN ('active', 'paused')
		)
	`, savedQueryID).Scan(&exists)
	return exists, err
}

// UpdateSavedQuery edits a saved query, snapshotting the pre-edit state into
// saved_query_versions first so an edit is always recoverable.
//
// Who may edit: the creator, or a workspace admin. A plain member editing a
// teammate's shared query would let anyone silently swap the SQL under a name
// that colleagues already trust — the same shape as the post-hoc-edit attack the
// scheduled path has to defend against.
//
// If the query is on a schedule, a change to sql_text does not take effect here. It
// becomes a proposal in saved_query_pending_edits for an admin to approve (096), and
// sql_text keeps its approved value. Only the SQL is gated: name, description and
// visibility apply immediately, because none of them changes what a scheduled run
// computes, and holding a rename behind review would teach people to route around the
// gate rather than respect it.
func UpdateSavedQuery(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	id := strings.TrimSpace(c.Param("id"))
	role, ok := requireResourceRole(c, "saved_queries", id, security.WSMember)
	if !ok {
		return
	}
	existing, ok := loadSavedQuery(c, id, userID)
	if !ok {
		return
	}
	if existing.CreatedBy != userID && !role.Meets(security.WSAdmin) {
		c.JSON(http.StatusForbidden, gin.H{"error": "only the creator or a workspace admin can edit this query"})
		return
	}

	var req updateSavedQueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	name := existing.Name
	if req.Name != nil {
		name = strings.TrimSpace(*req.Name)
		if name == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "name cannot be empty"})
			return
		}
		if len(name) > savedQueryMaxNameLen {
			c.JSON(http.StatusBadRequest, gin.H{"error": "name is too long"})
			return
		}
	}

	// Deliberately left empty when the client did not send sql_text: the default is
	// taken from the LOCKED row below, not from `existing`. Everything this handler
	// decides has to be decided from inside the transaction, and that includes what
	// "the SQL did not change" means.
	var sqlText string
	if req.SQLText != nil {
		sqlText = strings.TrimSpace(*req.SQLText)
		if sqlText == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "sql_text cannot be empty"})
			return
		}
		if len(sqlText) > savedQueryMaxSQLBytes {
			c.JSON(http.StatusBadRequest, gin.H{"error": "sql_text is too large"})
			return
		}
	}

	description := existing.Description
	if req.Description != nil {
		description = strings.TrimSpace(*req.Description)
	}

	visibility := existing.Visibility
	if req.Visibility != nil {
		v, ok := normalizeVisibility(*req.Visibility)
		if !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "visibility must be 'private' or 'workspace'"})
			return
		}
		visibility = v
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	// Snapshot + update in one transaction: a version row without its edit (or an
	// edit without its version row) would make the history lie about what ran.
	//
	// The transaction opens HERE, above the approval gate rather than below it, and
	// that ordering is the point rather than an accident of layout. When the gate ran
	// first it decided from `existing` — a read taken outside any transaction — and
	// then handed the proposal path a second, separate transaction of its own that
	// took no row lock at all. The guard therefore covered the ungated destination
	// and not the gated one, which is the destination that matters more: it is the
	// one whose writes end up under a schedule creator's authority. One transaction,
	// one lock, and both destinations decided inside it.
	tx, err := database.Begin()
	if err != nil {
		log.WithError(err).Error("begin saved query update")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update saved query"})
		return
	}
	defer func() { _ = tx.Rollback() }()

	// Lock the row and re-read it INSIDE the transaction (DX-VersionRace).
	//
	// `existing` was read at the top of this handler, outside any transaction, and
	// every decision above was made from it — including the approval gate. Another
	// writer can commit between that read and this line, and until this lock existed
	// nothing noticed.
	//
	// The reported symptom was the milder half: two concurrent edits both computed
	// MAX(version)+1, got the same number, and the loser hit UNIQUE (saved_query_id,
	// version) and returned 500. The serious half is silent. When a request does not
	// change the SQL — a rename, a description tweak, a visibility flip — the gate
	// above is skipped, and the UPDATE below still writes sqlText, which was
	// defaulted from the SQL read before the other writer committed. So a rename
	// racing an approved SQL change REVERTS that change: no error, no version row
	// saying why, and on a scheduled query it puts un-reviewed SQL back under the
	// schedule creator's authority. The unique constraint never protected against
	// that; it only fires when both writers snapshot, and this path snapshots the
	// stale text quite happily.
	//
	// So the fix is not to make version allocation collision-proof. A sequence or a
	// retry loop would remove the 500 and keep the overwrite — it would convert the
	// one case the constraint accidentally caught into silent data loss too. The fix
	// is to stop deciding from a read taken outside the transaction: lock the row,
	// confirm it still says what we planned against, and refuse if it does not.
	// Every column any decision below reads is in this list, so that no decision has
	// to reach back to `existing` for it.
	var lockedName, lockedDescription, lockedVisibility, lockedSQL, lockedClass string
	var lockedUpdatedAt time.Time
	err = tx.QueryRow(`
		SELECT name, COALESCE(description, ''), visibility, sql_text, statement_class, updated_at
		FROM saved_queries
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&lockedName, &lockedDescription, &lockedVisibility, &lockedSQL, &lockedClass, &lockedUpdatedAt)
	if err == sql.ErrNoRows {
		// Deleted between the two reads. 404 matches loadSavedQuery.
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	if err != nil {
		log.WithError(err).Error("lock saved query for update")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update saved query"})
		return
	}

	// updated_at is the concurrency token. It needs no new column and no API change
	// because a trigger already maintains it on every UPDATE (084), which is exactly
	// the property a version token needs and the reason not to invent one.
	//
	// Compared as the driver's own RFC3339Nano rendering on both sides: existing
	// came from database/sql scanning timestamptz into a string, and that conversion
	// is this same format, so the two are byte-comparable by construction.
	currentUpdatedAt := lockedUpdatedAt.Format(time.RFC3339Nano)
	if currentUpdatedAt != existing.UpdatedAt {
		c.JSON(http.StatusConflict, gin.H{
			"error":              "this query was changed by someone else while your edit was in flight; reload it and re-apply your change",
			"code":               "stale_write",
			"current_updated_at": currentUpdatedAt,
		})
		return
	}
	// The optional client token covers the window the check above cannot see: the
	// time between the user's last GET and this request. Parsed rather than string-
	// compared so a client that re-serialises the timestamp is not punished for it.
	if req.ExpectedUpdatedAt != nil {
		expected, perr := time.Parse(time.RFC3339, strings.TrimSpace(*req.ExpectedUpdatedAt))
		if perr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "expected_updated_at must be an RFC 3339 timestamp"})
			return
		}
		if !lockedUpdatedAt.Equal(expected) {
			c.JSON(http.StatusConflict, gin.H{
				"error":              "this query changed since you loaded it; reload it and re-apply your change",
				"code":               "stale_write",
				"current_updated_at": currentUpdatedAt,
			})
			return
		}
	}

	// Fields the client did not send keep the value the LOCKED row holds. The check
	// above proves that equals what `existing` said, so this changes no outcome
	// today — it is here so that a PATCH of one field can never carry a second
	// field's stale value with it, whatever happens to the check later.
	if req.Name == nil {
		name = lockedName
	}
	if req.Description == nil {
		description = lockedDescription
	}
	if req.Visibility == nil {
		visibility = lockedVisibility
	}
	statementClass := lockedClass
	if req.SQLText == nil {
		sqlText = lockedSQL
	} else {
		// Classified only when the SQL is actually being replaced. Re-classifying SQL
		// that is not changing would let a later tightening of the validator turn an
		// unrelated rename into a 400 on a query that has been saved for months.
		statementClass, ok = classifyForStorage(c, sqlText)
		if !ok {
			return
		}
	}

	// The approval gate, now inside the lock. Checked only when the SQL actually
	// changed, so editing the description of a scheduled query stays a one-step edit.
	//
	// `tx` and not `database`: the probe has to see the same snapshot as the write it
	// authorizes. Reading it on the pool meant the answer could be from before a
	// schedule was created and the write from after it — the gate reporting "not
	// scheduled" about a query that, by the time the UPDATE landed, was.
	if sqlText != lockedSQL {
		scheduled, err := savedQueryIsScheduled(tx, id)
		if err != nil {
			// Fails closed, and closed here means "do not apply the edit" — not "apply
			// it ungated". If we cannot tell whether a schedule depends on this query,
			// writing the new SQL is the one outcome we can never take back.
			//
			// This message is deliberately its own string rather than the generic update
			// failure used below, and it needs to stay that way. The caller learns their
			// edit was not applied and is worth retrying — but more importantly, a test
			// can tell "refused because the probe failed" apart from "fell through to
			// the ungated path and errored later". Both of those return 500, so with a
			// shared body a regression that fails OPEN right here still looks green.
			log.WithError(err).Error("saved query update: could not determine whether the query is scheduled")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "could not verify whether this query is scheduled; no change was made"})
			return
		}
		if scheduled {
			// Same transaction, so the proposal inherits the lock and the staleness
			// check that this handler already passed.
			proposeScheduledSQLEdit(c, tx, id, userID, proposedEdit{
				name:           name,
				description:    description,
				visibility:     visibility,
				sqlText:        sqlText,
				statementClass: statementClass,
				note:           strings.TrimSpace(req.Note),
			}, lockedSQL, lockedClass)
			return
		}
	}

	// Snapshot from the LOCKED row, not from `existing`. The check above proves they
	// are equal, so this is belt and braces — but it is the version that stays true
	// if someone later relaxes that check, and a snapshot that does not describe the
	// row it replaces is worse than no snapshot at all.
	if _, err := tx.Exec(`
		INSERT INTO saved_query_versions
			(saved_query_id, version, name, sql_text, statement_class, changed_by)
		SELECT $1,
		       COALESCE((SELECT MAX(version) FROM saved_query_versions WHERE saved_query_id = $1), 0) + 1,
		       $2, $3, $4, $5
	`, id, lockedName, lockedSQL, lockedClass, userID); err != nil {
		if isUniqueViolation(err) {
			// Unreachable while the FOR UPDATE above serialises writers, and kept
			// anyway: this is the one error whose old handling was a 500 that read
			// like a server fault when it was a conflict. If a future path ever
			// reaches the snapshot without the lock, it should say so honestly.
			c.JSON(http.StatusConflict, gin.H{
				"error": "this query was changed by someone else while your edit was in flight; reload it and re-apply your change",
				"code":  "stale_write",
			})
			return
		}
		log.WithError(err).Error("snapshot saved query version")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update saved query"})
		return
	}

	if _, err := tx.Exec(`
		UPDATE saved_queries
		SET name = $2, description = NULLIF($3, ''), sql_text = $4,
		    statement_class = $5, visibility = $6, updated_by = $7
		WHERE id = $1
	`, id, name, description, sqlText, statementClass, visibility, userID); err != nil {
		if isUniqueViolation(err) {
			c.JSON(http.StatusConflict, gin.H{"error": "a saved query with this name already exists in this workspace"})
			return
		}
		log.WithError(err).Error("update saved query")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update saved query"})
		return
	}

	if err := tx.Commit(); err != nil {
		log.WithError(err).Error("commit saved query update")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update saved query"})
		return
	}

	// Retention (097). After the commit and outside it on purpose: pruning old
	// history must never be able to fail someone's save. Default policy is keep
	// forever, so on an unconfigured workspace this is one indexed lookup.
	pruneSavedQueryVersionsBestEffort(database, id, existing.WorkspaceID)

	logAudit(c, "saved_query.update", "saved_query", id, map[string]interface{}{
		"name":            name,
		"statement_class": statementClass,
		"visibility":      visibility,
		"sql_changed":     sqlText != lockedSQL,
	})

	c.JSON(http.StatusOK, gin.H{
		"id":              id,
		"name":            name,
		"statement_class": statementClass,
		"visibility":      visibility,
	})
}

// DeleteSavedQuery removes a saved query. Versions cascade with it (084).
// Same creator-or-admin rule as edit.
func DeleteSavedQuery(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	id := strings.TrimSpace(c.Param("id"))
	role, ok := requireResourceRole(c, "saved_queries", id, security.WSMember)
	if !ok {
		return
	}
	existing, ok := loadSavedQuery(c, id, userID)
	if !ok {
		return
	}
	if existing.CreatedBy != userID && !role.Meets(security.WSAdmin) {
		c.JSON(http.StatusForbidden, gin.H{"error": "only the creator or a workspace admin can delete this query"})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	// migration 085 declares saved_query_schedules.saved_query_id ... ON DELETE CASCADE,
	// so the DELETE below takes the schedule ROW with it — but the Temporal Schedule
	// behind that row is a separate object in a separate system, and once the saved query
	// is gone nothing in the UI has a handle to it any more. It would keep firing on its
	// cron forever, each tick reaching the internal endpoint, finding no row and
	// reporting "skipped": invisible and permanent. Exactly the orphan
	// deleteTemporalSchedulesBestEffort was written for on the pipeline side.
	//
	// Collect the ids BEFORE the delete, while the rows still exist.
	var temporalScheduleIDs []string
	if rows, qErr := database.Query(
		`SELECT temporal_schedule_id FROM saved_query_schedules WHERE saved_query_id = $1`, id); qErr != nil {
		log.WithError(qErr).WithField("saved_query_id", id).
			Warn("could not list Temporal schedules before deleting saved query; one may be left orphaned")
	} else {
		for rows.Next() {
			var tsID string
			if err := rows.Scan(&tsID); err == nil {
				temporalScheduleIDs = append(temporalScheduleIDs, tsID)
			}
		}
		rows.Close()
	}

	if _, err := database.Exec(`DELETE FROM saved_queries WHERE id = $1`, id); err != nil {
		log.WithError(err).Error("delete saved query")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete saved query"})
		return
	}

	deleteTemporalSchedulesBestEffort(c.Request.Context(), temporalScheduleIDs)

	logAudit(c, "saved_query.delete", "saved_query", id, map[string]interface{}{
		"name": existing.Name,
	})

	c.JSON(http.StatusOK, gin.H{"deleted": true, "id": id})
}

// ListSavedQueryVersions returns the edit history, newest first.
func ListSavedQueryVersions(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	id := strings.TrimSpace(c.Param("id"))
	if _, ok := requireResourceRole(c, "saved_queries", id, security.WSViewer); !ok {
		return
	}
	// Re-check visibility: history is as sensitive as the query itself.
	existing, ok := loadSavedQuery(c, id, userID)
	if !ok {
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	rows, err := database.Query(`
		SELECT version, name, sql_text, statement_class, changed_by, created_at
		FROM saved_query_versions
		WHERE saved_query_id = $1
		ORDER BY version DESC
		LIMIT 100
	`, id)
	if err != nil {
		log.WithError(err).Error("list saved query versions")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list versions"})
		return
	}
	defer rows.Close()

	out := make([]SavedQueryVersion, 0)
	for rows.Next() {
		var v SavedQueryVersion
		if err := rows.Scan(&v.Version, &v.Name, &v.SQLText, &v.StatementClass, &v.ChangedBy, &v.CreatedAt); err != nil {
			log.WithError(err).Error("scan saved query version")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list versions"})
			return
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		log.WithError(err).Error("iterate saved query versions")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list versions"})
		return
	}

	// The open proposal, if any, rides along with the history rather than needing its
	// own round trip: a proposal is the change that has not become history yet, and the
	// panel that shows one shows the other. A read failure here is logged and dropped
	// rather than failing the whole response — history is the thing that was asked for,
	// and losing it because a proposal could not be read would be the worse trade.
	var pending *SavedQueryPendingEdit
	if p, err := loadOpenPendingEdit(database, id, existing.SQLText); err != nil {
		log.WithError(err).Warn("could not read pending edit alongside version history")
	} else {
		pending = p
	}

	c.JSON(http.StatusOK, gin.H{
		"versions": out,
		"count":    len(out),
		// The live text, so the newest snapshot has something to be diffed against
		// without the caller having to supply it from a separate fetch and risk
		// diffing against a copy that has since gone stale.
		"current": gin.H{
			"name":            existing.Name,
			"sql_text":        existing.SQLText,
			"statement_class": existing.StatementClass,
			"updated_at":      existing.UpdatedAt,
		},
		"pending_edit": pending,
	})
}

// ============================================================================
// Approval gate for scheduled queries (migration 096)
// ============================================================================
//
// saved_queries.sql_text is the approved text and nothing here changes that. A
// proposal lives in its own table until an admin approves it, at which point it is
// applied through exactly the same snapshot-then-update transaction as any ordinary
// edit — so an approved change appears in saved_query_versions like every other
// change, and the history has one shape rather than two.

// proposedEdit carries the already-validated fields of an edit that turned out to
// need review. Everything in it has been through the same checks as an ungated edit;
// grouping them keeps the call from growing an unreadable positional argument list.
type proposedEdit struct {
	name           string
	description    string
	visibility     string
	sqlText        string
	statementClass string
	note           string
}

// SavedQueryPendingEdit is one proposed SQL change.
type SavedQueryPendingEdit struct {
	ID             string  `json:"id"`
	SQLText        string  `json:"sql_text"`
	StatementClass string  `json:"statement_class"`
	Note           string  `json:"note,omitempty"`
	Status         string  `json:"status"`
	ProposedBy     string  `json:"proposed_by"`
	ProposedAt     string  `json:"proposed_at"`
	ReviewedBy     *string `json:"reviewed_by,omitempty"`
	ReviewedAt     *string `json:"reviewed_at,omitempty"`
	ReviewNote     string  `json:"review_note,omitempty"`

	// True when sql_text moved after this was proposed, so the reviewer is reading a
	// diff against a base that is no longer live. Derived, never stored: staleness is
	// a fact about the pair, and a stored flag would go out of date the moment the
	// query changed again.
	Stale bool `json:"stale"`
}

// proposeScheduledSQLEdit applies the non-SQL fields and parks the SQL for review.
//
// Both in one transaction. Splitting them would let the rename land while the
// proposal insert fails, leaving someone certain they had saved a change that no
// longer exists anywhere.
//
// The transaction belongs to UpdateSavedQuery and arrives already holding the
// saved_queries row lock, with the staleness check passed. It used to open one of its
// own, which is what made this the unguarded half of the handler: the caller checked
// that nobody had raced the edit, then dropped that guarantee at the door and let this
// function write from values read before any of it. Callers must not pass a
// transaction that has not taken the lock — every guarantee below is inherited, none
// of it is re-established here.
//
// baseSQL and liveClass come from that locked row, not from the caller's pre-
// transaction read, because base_sql_text is what the reviewer will diff against: a
// base recorded from a stale read shows the reviewer a change nobody proposed.
//
// Commits on success, so the caller returns immediately after calling it.
func proposeScheduledSQLEdit(c *gin.Context, tx *sql.Tx, id, userID string, edit proposedEdit, baseSQL, liveClass string) {
	// No version snapshot here, and none is missing: sql_text is not changing, and the
	// versions table records SQL history. The proposal row is itself the record of the
	// pending change, and approval snapshots properly when it applies.
	if _, err := tx.Exec(`
		UPDATE saved_queries
		SET name = $2, description = NULLIF($3, ''), visibility = $4, updated_by = $5
		WHERE id = $1
	`, id, edit.name, edit.description, edit.visibility, userID); err != nil {
		if isUniqueViolation(err) {
			c.JSON(http.StatusConflict, gin.H{"error": "a saved query with this name already exists in this workspace"})
			return
		}
		log.WithError(err).Error("update scheduled saved query metadata")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to propose this change"})
		return
	}

	var pendingID string
	err := tx.QueryRow(`
		INSERT INTO saved_query_pending_edits
			(saved_query_id, sql_text, statement_class, base_sql_text, note, proposed_by)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), $6)
		RETURNING id
	`, id, edit.sqlText, edit.statementClass, baseSQL, edit.note, userID).Scan(&pendingID)
	if err != nil {
		// The partial unique index (096) allows one open proposal per query. A second
		// one is a real answer to give the proposer, not a server error: there is no
		// sensible automatic merge of two SQL rewrites, so the two authors need to
		// talk. 409 rather than 400 — the request was well formed, the state conflicts.
		if isUniqueViolation(err) {
			c.JSON(http.StatusConflict, gin.H{
				"error": "this query already has a change waiting for approval — it must be approved or rejected first",
			})
			return
		}
		log.WithError(err).Error("insert saved query pending edit")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to propose this change"})
		return
	}

	if err := tx.Commit(); err != nil {
		log.WithError(err).Error("commit saved query proposal")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to propose this change"})
		return
	}

	logAudit(c, "saved_query.propose_edit", "saved_query", id, map[string]interface{}{
		"pending_edit_id": pendingID,
		"statement_class": edit.statementClass,
	})

	// 200, not 202: part of this request did apply, and the caller's own view of the
	// query is now out of date either way. The body says plainly what happened to the
	// SQL rather than leaving the status code to imply it.
	c.JSON(http.StatusOK, gin.H{
		"id":              id,
		"name":            edit.name,
		"visibility":      edit.visibility,
		"statement_class": liveClass,
		"pending_approval": gin.H{
			"id":              pendingID,
			"statement_class": edit.statementClass,
			"reason":          "This query is on a schedule, so the SQL change needs a workspace admin's approval before it takes effect. Until then the schedule keeps running the approved SQL.",
		},
	})
}

// loadOpenPendingEdit reads the single open proposal for a query, if there is one.
// Returns (nil, nil) when there is none — a query with nothing awaiting review is the
// normal case, not an error.
func loadOpenPendingEdit(database *sql.DB, savedQueryID, liveSQL string) (*SavedQueryPendingEdit, error) {
	var p SavedQueryPendingEdit
	var note sql.NullString
	var baseSQL string
	err := database.QueryRow(`
		SELECT id, sql_text, statement_class, base_sql_text, note, status, proposed_by, proposed_at
		FROM saved_query_pending_edits
		WHERE saved_query_id = $1 AND status = 'pending'
	`, savedQueryID).Scan(&p.ID, &p.SQLText, &p.StatementClass, &baseSQL, &note, &p.Status, &p.ProposedBy, &p.ProposedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	p.Note = note.String
	p.Stale = baseSQL != liveSQL
	return &p, nil
}

// reviewSavedQueryPendingEdit is the shared body of approve and reject.
//
// Who may review: a workspace admin. Deliberately NOT "an admin who is not the
// proposer". Requiring a second person reads like the stronger control, and in a team
// it is, but a great many workspaces here have exactly one admin — for them it would
// not raise the bar, it would make a scheduled query permanently uneditable, and the
// way out of that is to delete the schedule, which is strictly worse for everyone.
// The teeth of this gate are elsewhere and survive self-approval: a plain member can
// propose but cannot approve, so no member can unilaterally change what a scheduled
// table means, and every approval names its approver in the audit log. Whether to also
// require a second pair of eyes is a workspace policy question, and belongs with the
// other policy settings rather than hard-coded here.
func reviewSavedQueryPendingEdit(c *gin.Context, approve bool) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	id := strings.TrimSpace(c.Param("id"))
	if _, ok := requireResourceRole(c, "saved_queries", id, security.WSAdmin); !ok {
		return
	}
	// Re-check visibility as every other read of this row does: a private query's
	// pending SQL is as sensitive as the query itself.
	existing, ok := loadSavedQuery(c, id, userID)
	if !ok {
		return
	}

	var req struct {
		Note string `json:"note"`
	}
	// A body is optional on both verbs, so a bind failure is only fatal if something
	// was actually sent and was malformed.
	if c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
			return
		}
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	tx, err := database.Begin()
	if err != nil {
		log.WithError(err).Error("begin pending edit review")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to review this change"})
		return
	}
	defer func() { _ = tx.Rollback() }()

	// Lock saved_queries FIRST, then saved_query_pending_edits — and that order is
	// load-bearing rather than incidental.
	//
	// Two paths in this file hold both locks. Proposing an edit on a scheduled query
	// runs inside UpdateSavedQuery's transaction, which locks saved_queries and then
	// inserts into pending_edits. This handler used to take them the other way round,
	// under a comment asserting it was the only path that took both. It was not, and
	// two opposite orders is the textbook deadlock: an approve holding pending_edits
	// and waiting on saved_queries, a propose holding saved_queries and waiting on
	// pending_edits. Postgres resolves that by killing one of them (40P01), which
	// surfaces as a 500 to whichever admin lost the coin toss, on the one path in the
	// explorer whose entire job is to be trustworthy. Both paths now agree:
	// saved_queries, then pending_edits.
	//
	// Snapshot from this locked row rather than from `existing` (DX-VersionRace,
	// second site). `existing` was read before this transaction, so a rename can land
	// in between; approving from it would write a version row labelled with the OLD
	// name — history claiming the query was called something it had already stopped
	// being called.
	//
	// Rejecting does not take this lock at all. It writes the verdict row and nothing
	// else, so there is no saved_queries state for it to protect, and a row lock taken
	// for symmetry is still a lock that a concurrent edit has to queue behind.
	var lockedName, lockedSQL, lockedClass string
	if approve {
		if err := tx.QueryRow(`
			SELECT name, sql_text, statement_class
			FROM saved_queries
			WHERE id = $1
			FOR UPDATE
		`, id).Scan(&lockedName, &lockedSQL, &lockedClass); err != nil {
			if err == sql.ErrNoRows {
				c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
				return
			}
			log.WithError(err).Error("lock saved query for pending edit review")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to review this change"})
			return
		}
	}

	// FOR UPDATE, and the rest of the review runs inside this lock. Two admins
	// clicking Approve at the same moment would otherwise both read a pending row,
	// both write sql_text, and produce two version snapshots for one change.
	var pendingID, proposedSQL, proposedBy string
	err = tx.QueryRow(`
		SELECT id, sql_text, proposed_by
		FROM saved_query_pending_edits
		WHERE saved_query_id = $1 AND status = 'pending'
		FOR UPDATE
	`, id).Scan(&pendingID, &proposedSQL, &proposedBy)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "there is no change waiting for approval on this query"})
		return
	}
	if err != nil {
		log.WithError(err).Error("load pending edit for review")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to review this change"})
		return
	}

	status := "rejected"
	if approve {
		status = "approved"
	}

	if _, err := tx.Exec(`
		UPDATE saved_query_pending_edits
		SET status = $2, reviewed_by = $3, reviewed_at = NOW(), review_note = NULLIF($4, '')
		WHERE id = $1
	`, pendingID, status, userID, strings.TrimSpace(req.Note)); err != nil {
		log.WithError(err).Error("record pending edit verdict")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to review this change"})
		return
	}

	// On a rejection nothing about the query changes, so the class it already had is
	// the class to report. lockedClass is deliberately not read here: the branch that
	// skips the lock must not go on to use what the lock would have produced.
	statementClass := existing.StatementClass
	if approve {
		// Re-classify the text being written rather than trusting the class stored
		// beside the proposal. The proposal has sat in a table that an edit path wrote;
		// the class that ends up on saved_queries must be derived from the SQL that is
		// actually landing, at the moment it lands.
		cls, ok := classifyForStorage(c, proposedSQL)
		if !ok {
			return
		}
		statementClass = cls

		// The same snapshot-then-update as an ordinary edit, so an approved change is
		// indistinguishable from a direct one in saved_query_versions. The history has
		// one shape, and "what did this query say last Tuesday" has one answer.
		if _, err := tx.Exec(`
			INSERT INTO saved_query_versions
				(saved_query_id, version, name, sql_text, statement_class, changed_by)
			SELECT $1,
			       COALESCE((SELECT MAX(version) FROM saved_query_versions WHERE saved_query_id = $1), 0) + 1,
			       $2, $3, $4, $5
		`, id, lockedName, lockedSQL, lockedClass, userID); err != nil {
			if isUniqueViolation(err) {
				// As in UpdateSavedQuery: unreachable under the two locks held here,
				// and reported as a conflict rather than a server fault if a future
				// path ever loses one of them.
				c.JSON(http.StatusConflict, gin.H{
					"error": "this query was changed while the approval was in flight; reload it and review again",
					"code":  "stale_write",
				})
				return
			}
			log.WithError(err).Error("snapshot before applying approved edit")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to review this change"})
			return
		}

		if _, err := tx.Exec(`
			UPDATE saved_queries
			SET sql_text = $2, statement_class = $3, updated_by = $4
			WHERE id = $1
		`, id, proposedSQL, statementClass, userID); err != nil {
			log.WithError(err).Error("apply approved edit")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to review this change"})
			return
		}
	}

	if err := tx.Commit(); err != nil {
		log.WithError(err).Error("commit pending edit review")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to review this change"})
		return
	}

	// Retention (097). Only an approval appends a version; a rejection leaves the
	// history untouched and has nothing to prune.
	if approve {
		pruneSavedQueryVersionsBestEffort(database, id, existing.WorkspaceID)
	}

	logAudit(c, "saved_query.review_edit", "saved_query", id, map[string]interface{}{
		"pending_edit_id": pendingID,
		"verdict":         status,
		"statement_class": statementClass,
		// Recorded because self-approval is allowed by design (see above): the log has
		// to make it visible when it happens, or allowing it would also hide it.
		"self_reviewed": proposedBy == userID,
	})

	c.JSON(http.StatusOK, gin.H{
		"id":              id,
		"pending_edit_id": pendingID,
		"status":          status,
		"applied":         approve,
		"statement_class": statementClass,
	})
}

// ApproveSavedQueryEdit applies the pending SQL change. Admin only.
func ApproveSavedQueryEdit(c *gin.Context) { reviewSavedQueryPendingEdit(c, true) }

// RejectSavedQueryEdit discards the pending SQL change, keeping it as history.
// Admin only.
func RejectSavedQueryEdit(c *gin.Context) { reviewSavedQueryPendingEdit(c, false) }
