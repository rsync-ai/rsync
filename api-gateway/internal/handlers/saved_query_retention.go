package handlers

import (
	"database/sql"
	"net/http"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// ============================================================================
// Saved-query version retention (DX-VersionRetention, migration 097)
// ============================================================================
// saved_query_versions has always been append-only, and ListSavedQueryVersions
// reads it with LIMIT 100 — so past the hundredth version a workspace is storing
// rows that no UI will ever render. This is the policy that lets an admin bound
// that, and the prune that enforces it.
//
// Two design choices worth stating, because both are the conservative reading:
//
//   1. The default is OFF. retention_days is NULL out of the migration, which
//      means keep forever — byte-for-byte today's behaviour. Nothing is deleted
//      anywhere until a workspace admin opts in. A retention feature that starts
//      deleting on deploy is a data-loss incident with a changelog entry.
//
//   2. The prune is not load-bearing on the edit path. It runs after the edit's
//      transaction has committed, in its own statement, and a failure is logged
//      and swallowed. Housekeeping must never be able to fail a user's save — if
//      the policy query breaks (or 097 has not been applied yet on some
//      environment), editing a saved query still works and the rows simply stay.
//      The next successful edit prunes them.

// Bounds on the policy. These mirror the CHECK constraints in migration 097 —
// the schema is what actually holds, these are what produce a readable 400
// instead of a 500 from a constraint violation. If you change one, change both.
const (
	// savedQueryRetentionMinFloor is the smallest number of versions a workspace
	// may choose to keep. Not 0 and not 1: the entire point of the history is
	// "show me what this said before and put it back", and a policy that can
	// erase the previous version destroys the feature it is configuring.
	savedQueryRetentionMinFloor = 5
	savedQueryRetentionMinCeil  = 1000

	savedQueryRetentionDaysMin = 1
	savedQueryRetentionDaysMax = 3650
)

// savedQueryRetention is the API shape of a workspace's policy. RetentionDays is
// a pointer because NULL is a real, distinct value here — "keep forever" — and
// not an absence to be defaulted away.
type savedQueryRetention struct {
	RetentionDays *int `json:"retention_days"`
	MinVersions   int  `json:"min_versions"`
}

// loadSavedQueryRetention reads one workspace's policy.
func loadSavedQueryRetention(database *sql.DB, workspaceID string) (savedQueryRetention, error) {
	var days sql.NullInt64
	var out savedQueryRetention
	err := database.QueryRow(`
		SELECT saved_query_version_retention_days, saved_query_version_retention_min
		FROM workspaces
		WHERE id = $1
	`, workspaceID).Scan(&days, &out.MinVersions)
	if err != nil {
		return out, err
	}
	if days.Valid {
		v := int(days.Int64)
		out.RetentionDays = &v
	}
	return out, nil
}

// pruneSavedQueryVersions applies the owning workspace's policy to one saved
// query's history and reports how many rows it removed.
//
// The two axes are ANDed, and that is the whole safety argument: a version is
// deleted only if it is BOTH older than retention_days AND outside the newest
// MinVersions. Age alone would wipe the history of a query edited twice a year;
// count alone would keep an unbounded tail of ancient rows on a query edited
// hourly. Neither axis is sufficient, so neither is offered alone.
//
// Called with the saved query's OWN workspace_id, taken from the row rather than
// from the request's active-workspace header. The header is the caller's claim
// about where they are; sq.workspace_id is where the data actually lives, and a
// prune must be governed by the policy of the workspace that owns the history.
func pruneSavedQueryVersions(database *sql.DB, savedQueryID, workspaceID string) (int64, error) {
	policy, err := loadSavedQueryRetention(database, workspaceID)
	if err != nil {
		return 0, err
	}
	if policy.RetentionDays == nil {
		// Keep forever — the default, and the only branch that runs until an
		// admin has explicitly chosen otherwise.
		return 0, nil
	}
	minKeep := policy.MinVersions
	if minKeep < savedQueryRetentionMinFloor {
		// Belt and braces against a row that predates the CHECK constraint or was
		// written by something other than the handler below. Clamping up, never
		// down: the failure direction we accept is keeping too much.
		minKeep = savedQueryRetentionMinFloor
	}

	// MAX(version) - minKeep is NULL when the query has no versions at all, and a
	// NULL comparison deletes nothing — the empty case needs no special handling.
	// Versions are allocated strictly monotonically under the row lock taken in
	// UpdateSavedQuery, so ordering by version is ordering by recency.
	res, err := database.Exec(`
		DELETE FROM saved_query_versions
		WHERE saved_query_id = $1
		  AND created_at < NOW() - make_interval(days => $2::int)
		  AND version <= (
		        SELECT MAX(version) - $3::int
		        FROM saved_query_versions
		        WHERE saved_query_id = $1
		      )
	`, savedQueryID, *policy.RetentionDays, minKeep)
	if err != nil {
		return 0, err
	}
	// RowsAffected has its own error, and dropping it here would turn "the driver
	// could not count" into a confident zero — which reads in the log as "the
	// policy matched nothing", the opposite conclusion. The DELETE has already
	// happened by this point, so this is a reporting failure and not a prune
	// failure: say so, and do not fail the caller over it.
	n, err := res.RowsAffected()
	if err != nil {
		log.WithError(err).WithField("saved_query_id", savedQueryID).
			Warn("pruned saved query versions but could not read the deleted count")
		return 0, nil
	}
	return n, nil
}

// pruneSavedQueryVersionsBestEffort is the edit path's entry point: it never
// returns an error, because no retention failure is worth failing a save over.
// See the header note — this runs after the edit has already committed.
func pruneSavedQueryVersionsBestEffort(database *sql.DB, savedQueryID, workspaceID string) {
	deleted, err := pruneSavedQueryVersions(database, savedQueryID, workspaceID)
	if err != nil {
		log.WithError(err).WithField("saved_query_id", savedQueryID).
			Warn("saved query version prune failed; history was left intact")
		return
	}
	if deleted > 0 {
		log.WithFields(log.Fields{
			"saved_query_id": savedQueryID,
			"deleted":        deleted,
		}).Info("pruned saved query versions per workspace retention policy")
	}
}

// GetSavedQueryVersionRetention returns the active workspace's policy. Readable
// by any member: knowing how long your own edit history survives is not
// privileged information, and hiding it from the people whose work it governs
// would only produce surprise later.
func GetSavedQueryVersionRetention(c *gin.Context) {
	workspaceID, ok := resolveActiveWorkspace(c)
	if !ok {
		return
	}
	if _, ok := requireWorkspaceRole(c, security.WSViewer); !ok {
		return
	}
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}
	policy, err := loadSavedQueryRetention(database, workspaceID)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "workspace not found"})
		return
	}
	if err != nil {
		log.WithError(err).Error("load saved query version retention")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load retention policy"})
		return
	}
	c.JSON(http.StatusOK, policy)
}

// SetSavedQueryVersionRetention replaces the active workspace's policy. Admin
// only — this is the one Explorer setting whose effect is deleting data.
//
// PUT, and strict about it: min_versions is REQUIRED on every call. The
// alternative (treat an omitted field as "leave it alone") means a client that
// meant to change only the age axis can silently widen what the age axis is
// allowed to delete. For a setting that destroys history, having to restate both
// axes is the right amount of friction.
//
// retention_days omitted or null means keep forever. That is the safe direction,
// so it is the one that absence maps to.
func SetSavedQueryVersionRetention(c *gin.Context) {
	workspaceID, ok := resolveActiveWorkspace(c)
	if !ok {
		return
	}
	if _, ok := requireWorkspaceRole(c, security.WSAdmin); !ok {
		return
	}

	var req struct {
		RetentionDays *int `json:"retention_days"`
		MinVersions   *int `json:"min_versions"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	if req.MinVersions == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "min_versions is required"})
		return
	}
	if *req.MinVersions < savedQueryRetentionMinFloor || *req.MinVersions > savedQueryRetentionMinCeil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "min_versions must be between 5 and 1000; keeping fewer than 5 versions would leave nothing to restore from",
		})
		return
	}
	if req.RetentionDays != nil &&
		(*req.RetentionDays < savedQueryRetentionDaysMin || *req.RetentionDays > savedQueryRetentionDaysMax) {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "retention_days must be between 1 and 3650, or null to keep history forever",
		})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	var days sql.NullInt64
	if req.RetentionDays != nil {
		days = sql.NullInt64{Int64: int64(*req.RetentionDays), Valid: true}
	}
	var out savedQueryRetention
	var stored sql.NullInt64
	err := database.QueryRow(`
		UPDATE workspaces
		SET saved_query_version_retention_days = $2,
		    saved_query_version_retention_min  = $3
		WHERE id = $1
		RETURNING saved_query_version_retention_days, saved_query_version_retention_min
	`, workspaceID, days, *req.MinVersions).Scan(&stored, &out.MinVersions)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "workspace not found"})
		return
	}
	if err != nil {
		log.WithError(err).Error("update saved query version retention")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update retention policy"})
		return
	}
	if stored.Valid {
		v := int(stored.Int64)
		out.RetentionDays = &v
	}

	// Audited because it is destructive-by-configuration: the deletions it causes
	// happen later, on an unrelated request, and this record is the only place
	// that connects them back to a decision and a person.
	logAudit(c, "saved_query.retention_update", "workspace", workspaceID, map[string]interface{}{
		"retention_days": req.RetentionDays,
		"min_versions":   out.MinVersions,
	})

	c.JSON(http.StatusOK, out)
}
