package handlers

import (
	"context"
	"database/sql"
	"net/http"
	"strings"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/notifier"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// notificationListLimit caps the header-bell inbox. The bell shows recent
// activity, not an audit log — 50 is plenty and keeps the payload small enough
// to poll. Older rows stay in the DB, queryable by other tooling.
const notificationListLimit = 50

// Notification is one row of a user's pipeline_notifications inbox, projected
// for the header bell. Rows are written by the notifier consumer
// (api-gateway/internal/notifier) from healer/executor Kafka events and are
// scoped to the pipeline owner (user_id).
type Notification struct {
	ID         string `json:"id"`
	PipelineID string `json:"pipeline_id"`
	Type       string `json:"type"`
	Severity   string `json:"severity"` // info | warning | critical
	Title      string `json:"title"`
	Message    string `json:"message"`
	// PipelineName lets the bell say WHICH pipeline the alert is about — with
	// dozens of pipelines, a title alone is unactionable. Joined live from
	// pipelines so a rename is reflected in old notifications.
	PipelineName string `json:"pipeline_name"`
	// Impact answers "is my data still moving, and must I act?" — resolved from
	// the notifier's copy catalog (api-gateway/internal/notifier/catalog.go) and
	// persisted in metadata. Empty when the event type has no impact line.
	Impact string `json:"impact"`
	// ActionLabel is the verb for the deep-link button ("Review change",
	// "Reconnect"). Never empty on the wire — rows that predate the catalog are
	// re-rendered through it by repairPreCatalogCopy before they are returned.
	ActionLabel string `json:"action_label"`
	// ReferenceCode is the stable error code, exposed for support ("quote this
	// code") — deliberately NOT the title. Empty for uncoded events.
	ReferenceCode string `json:"reference_code"`
	// ActionURL is stored RELATIVE (e.g. /pipelines/{id}/schema-changes). The
	// frontend deep-links to it same-origin, so no host prefix is needed here —
	// that prefixing is only for external Slack/email delivery (APP_BASE_URL).
	ActionURL string     `json:"action_url"`
	Read      bool       `json:"read"`
	ReadAt    *time.Time `json:"read_at"`
	CreatedAt time.Time  `json:"created_at"`
}

// audienceFilter hides notifications addressed to rsync engineers rather than
// to the customer. StructuredError carries audience={user|operator|developer};
// a developer-audience event ("RSYNC_BUG_*" internals) is our problem to fix,
// not something a customer can action, so it never reaches their bell. Rows
// without an audience (legacy, or non-structured events) default to user and
// are kept.
//
// Applied identically to the list and the unread count — otherwise the badge
// would count rows the dropdown refuses to show, and the bell could never be
// cleared. ListNotifications inlines the same predicate with its `n.` table
// alias; keep the two in lockstep.
const audienceFilter = `AND COALESCE(metadata->>'audience', 'user') <> 'developer'`

// workspaceScope narrows the bell to the caller's ACTIVE workspace.
//
// pipeline_notifications has no workspace_id of its own; the workspace is a
// property of the pipeline the notification is about (pipeline_id is NOT NULL
// with ON DELETE CASCADE, so the join can never miss). Scoping here rather than
// leaving the inbox user-global matters for more than tidiness: an unscoped bell
// deep-links to /pipelines/{id} for a pipeline in a workspace the user is not
// currently in, and the gateway then 404s that id — the alert was visible but
// unopenable. Scoped, every row in the bell is one the current workspace can act
// on.
//
// Fails closed: with no active workspace the middleware leaves the context
// unset, and callers return an empty inbox rather than every workspace's rows.
func notificationWorkspace(c *gin.Context) (string, bool) {
	ws := activeWorkspaceID(c)
	return ws, ws != ""
}

// countUnreadNotifications returns how many unread notifications the user has in
// the given workspace. Hits the purpose-built partial index
// idx_pipeline_notifications_user_unread, then narrows via the pipeline join.
func countUnreadNotifications(ctx context.Context, database *sql.DB, userID, workspaceID string) (int, error) {
	var count int
	err := database.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM pipeline_notifications n
		JOIN pipelines p ON p.id = n.pipeline_id
		WHERE n.user_id = $1 AND n.read_at IS NULL AND p.workspace_id = $2
		  AND COALESCE(n.metadata->>'audience', 'user') <> 'developer'
	`, userID, workspaceID).Scan(&count)
	return count, err
}

// ListNotifications returns the caller's most recent notifications (read +
// unread) in the active workspace, plus the unread count. Doubly scoped: the
// WHERE user_id clause means a caller only ever sees their own rows, and the
// workspace join means only rows for the workspace they're currently in.
//
// GET /api/v1/notifications
func ListNotifications(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusOK, gin.H{"notifications": []Notification{}, "unread_count": 0})
		return
	}

	workspaceID, hasWorkspace := notificationWorkspace(c)
	if !hasWorkspace {
		c.JSON(http.StatusOK, gin.H{"notifications": []Notification{}, "unread_count": 0})
		return
	}

	// The catalog extras (impact, action_label, error_code) live in the metadata
	// JSONB rather than dedicated columns, so adding notification copy needs no
	// migration. COALESCE against the pipeline name keeps rows renderable even
	// if the join misses.
	rows, err := database.QueryContext(c.Request.Context(), `
		SELECT n.id, n.pipeline_id, n.type, n.severity, n.title, n.message,
		       COALESCE(n.action_url, ''),
		       COALESCE(p.name, n.metadata->>'pipeline_name', ''),
		       COALESCE(n.metadata->>'impact', ''),
		       COALESCE(n.metadata->>'action_label', ''),
		       COALESCE(n.metadata->>'error_code', ''),
		       COALESCE(n.metadata->>'raw_type', ''),
		       COALESCE(n.metadata->>'source_topic', ''),
		       n.read_at, n.created_at
		FROM pipeline_notifications n
		JOIN pipelines p ON p.id = n.pipeline_id
		WHERE n.user_id = $1
		  AND p.workspace_id = $2
		  AND COALESCE(n.metadata->>'audience', 'user') <> 'developer'
		ORDER BY n.created_at DESC
		LIMIT $3
	`, userID, workspaceID, notificationListLimit)
	if err != nil {
		respondError(c, http.StatusInternalServerError, "list_notifications_failed", "Failed to load notifications", err)
		return
	}
	defer rows.Close()

	notifications := make([]Notification, 0)
	for rows.Next() {
		var n Notification
		var rawType, sourceTopic string
		if err := rows.Scan(
			&n.ID, &n.PipelineID, &n.Type, &n.Severity, &n.Title, &n.Message,
			&n.ActionURL, &n.PipelineName, &n.Impact, &n.ActionLabel, &n.ReferenceCode,
			&rawType, &sourceTopic, &n.ReadAt, &n.CreatedAt,
		); err != nil {
			log.WithError(err).Warn("ListNotifications: scan error")
			continue
		}
		repairPreCatalogCopy(&n, rawType, sourceTopic)
		// After the repair, which needs the raw code as an input: a placeholder
		// code is not a support reference, so it must not reach the bell's
		// "quote this code" tag. See notifier.SupportReferenceCode.
		n.ReferenceCode = notifier.SupportReferenceCode(n.ReferenceCode)
		n.Read = n.ReadAt != nil
		notifications = append(notifications, n)
	}

	unread, err := countUnreadNotifications(c.Request.Context(), database, userID, workspaceID)
	if err != nil {
		respondError(c, http.StatusInternalServerError, "list_notifications_failed", "Failed to load notifications", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"notifications": notifications, "unread_count": unread})
}

// repairPreCatalogCopy re-renders rows written before the copy catalog existed.
//
// The catalog runs at WRITE time, so deploying it fixes only events that arrive
// afterwards. Rows already in the table keep the title the old writer gave them:
// the raw error code ("LEGACY_UNCLASSIFIED") or, for an event with no type, the
// raw Kafka topic ("rsync.healer.results") with an empty body — the exact thing
// a customer reported as unreadable. Without this, those rows stay broken in the
// bell forever.
//
// Detection is metadata.action_label being empty: the catalog path always writes
// it non-empty (it falls back to "View pipeline"), so an empty one means the row
// predates the catalog. Nothing is written back — the historical row is left as
// it was persisted and only the API projection is corrected, which keeps this
// migration-free and safe to roll back.
func repairPreCatalogCopy(n *Notification, rawType, sourceTopic string) {
	if strings.TrimSpace(n.ActionLabel) != "" {
		return
	}

	// The old writer stored the event type on the column and mirrored it into
	// metadata.raw_type; prefer whichever is populated.
	eventType := rawType
	if strings.TrimSpace(eventType) == "" {
		eventType = n.Type
	}

	rendered := notifier.RenderStored(n.ReferenceCode, eventType, sourceTopic, n.Severity, n.PipelineName)
	n.Title = rendered.Title
	n.Impact = rendered.Impact
	n.ActionLabel = rendered.ActionLabel
	n.Severity = rendered.Severity
}

// GetUnreadNotificationCount returns just the caller's unread count — the cheap
// endpoint the header bell polls on an interval.
//
// GET /api/v1/notifications/unread-count
func GetUnreadNotificationCount(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusOK, gin.H{"unread_count": 0})
		return
	}

	workspaceID, hasWorkspace := notificationWorkspace(c)
	if !hasWorkspace {
		c.JSON(http.StatusOK, gin.H{"unread_count": 0})
		return
	}

	count, err := countUnreadNotifications(c.Request.Context(), database, userID, workspaceID)
	if err != nil {
		respondError(c, http.StatusInternalServerError, "unread_count_failed", "Failed to load unread count", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"unread_count": count})
}

// markNotificationReadRequest is the body of POST /notifications/mark-read.
type markNotificationReadRequest struct {
	ID string `json:"id"`
}

// MarkNotificationRead marks a single notification read. It is idempotent and
// IDOR-safe: the WHERE clause pins user_id, so a caller can only ever mark
// their own row; COALESCE keeps an existing read_at so re-marking never bumps
// the timestamp; and a row that isn't the caller's matches nothing → 404.
//
// Deliberately NOT workspace-scoped, unlike the list and mark-all-read. The id
// is already pinned to the caller, so there is nothing to leak; adding a
// workspace predicate would only turn a click that races a workspace switch
// into a spurious 404 on a row the user legitimately owns.
//
// POST /api/v1/notifications/mark-read  {"id": "<uuid>"}
func MarkNotificationRead(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	var req markNotificationReadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "A notification id is required", nil)
		return
	}
	id := strings.TrimSpace(req.ID)
	// Validate the UUID up front so a malformed id is a clean 400 rather than a
	// Postgres "invalid input syntax for type uuid" 500.
	if _, err := uuid.Parse(id); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "A valid notification id is required", nil)
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	res, err := database.ExecContext(c.Request.Context(), `
		UPDATE pipeline_notifications
		SET read_at = COALESCE(read_at, NOW())
		WHERE id = $1 AND user_id = $2
	`, id, userID)
	if err != nil {
		respondError(c, http.StatusInternalServerError, "mark_read_failed", "Failed to mark notification read", err)
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "notification not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// MarkAllNotificationsRead marks the caller's unread notifications read within
// the ACTIVE workspace only — it must clear exactly what the bell displays.
// Marking every workspace read from one workspace's dropdown would silently
// dismiss alerts the user never saw.
//
// POST /api/v1/notifications/mark-all-read
func MarkAllNotificationsRead(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	workspaceID, hasWorkspace := notificationWorkspace(c)
	if !hasWorkspace {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "marked": 0})
		return
	}

	res, err := database.ExecContext(c.Request.Context(), `
		UPDATE pipeline_notifications n
		SET read_at = NOW()
		FROM pipelines p
		WHERE p.id = n.pipeline_id
		  AND n.user_id = $1 AND n.read_at IS NULL
		  AND p.workspace_id = $2
	`, userID, workspaceID)
	if err != nil {
		respondError(c, http.StatusInternalServerError, "mark_all_read_failed", "Failed to mark notifications read", err)
		return
	}
	marked, _ := res.RowsAffected()
	c.JSON(http.StatusOK, gin.H{"status": "ok", "marked": marked})
}
