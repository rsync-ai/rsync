package handlers

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
)

// cdcSnapshotRequestsLimit caps GET …/cdc/snapshot-requests: the UI shows the latest
// loads, not a history.
const cdcSnapshotRequestsLimit = 10

// CDCSnapshotRequest is one cdc_snapshot_requests row (migration 113) as the UI reads
// it: a Re-snapshot, an Edit tables "load existing rows", or an auto-pickup load, from
// queued until Debezium finished reading the tables. Timestamps are RFC3339; a NULL
// one is omitted.
type CDCSnapshotRequest struct {
	ID              string   `json:"id"`
	Mode            string   `json:"mode"`
	Tables          []string `json:"tables"`
	Source          string   `json:"source"`
	Status          string   `json:"status"`
	Attempts        int      `json:"attempts"`
	CompletedTables []string `json:"completed_tables"`
	LastError       string   `json:"last_error,omitempty"`
	RequestedAt     string   `json:"requested_at"`
	SentAt          string   `json:"sent_at,omitempty"`
	StartedAt       string   `json:"started_at,omitempty"`
	LastProgressAt  string   `json:"last_progress_at,omitempty"`
	CompletedAt     string   `json:"completed_at,omitempty"`
}

// GetPipelineCDCSnapshotRequests lists the pipeline's latest CDC snapshot requests,
// newest first. Read-only, so Viewer is enough, like the other CDC GET routes. A
// pipeline with none answers an empty list, not 404.
//
// GET /api/v1/pipelines/:id/cdc/snapshot-requests
func GetPipelineCDCSnapshotRequests(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	requests, err := listCDCSnapshotRequests(database, pipelineID, cdcSnapshotRequestsLimit)
	if err != nil {
		respondError(c, http.StatusInternalServerError, "snapshot_requests_query_failed",
			"Failed to read the pipeline's snapshot requests", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"requests": requests})
}

func listCDCSnapshotRequests(database *sql.DB, pipelineID string, limit int) ([]CDCSnapshotRequest, error) {
	rows, err := database.Query(`
		SELECT id::text, mode, COALESCE(tables, '[]'::jsonb)::text, source, status, attempts,
		       COALESCE(completed_tables, '[]'::jsonb)::text, last_error,
		       requested_at, sent_at, started_at, last_progress_at, completed_at
		FROM cdc_snapshot_requests
		WHERE pipeline_id = $1::uuid
		ORDER BY requested_at DESC, id
		LIMIT $2
	`, pipelineID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]CDCSnapshotRequest, 0, limit)
	for rows.Next() {
		var r CDCSnapshotRequest
		var tablesText, completedText string
		var lastError sql.NullString
		var requestedAt time.Time
		var sentAt, startedAt, lastProgressAt, completedAt sql.NullTime
		if err := rows.Scan(
			&r.ID, &r.Mode, &tablesText, &r.Source, &r.Status, &r.Attempts,
			&completedText, &lastError,
			&requestedAt, &sentAt, &startedAt, &lastProgressAt, &completedAt,
		); err != nil {
			return nil, err
		}
		r.Tables = jsonStringList(tablesText)
		r.CompletedTables = jsonStringList(completedText)
		r.LastError = strings.TrimSpace(lastError.String)
		r.RequestedAt = requestedAt.UTC().Format(time.RFC3339)
		r.SentAt = rfc3339OrEmpty(sentAt)
		r.StartedAt = rfc3339OrEmpty(startedAt)
		r.LastProgressAt = rfc3339OrEmpty(lastProgressAt)
		r.CompletedAt = rfc3339OrEmpty(completedAt)
		out = append(out, r)
	}
	return out, rows.Err()
}

// jsonStringList decodes a JSONB array of strings, never nil (the JSON is [] not null).
func jsonStringList(text string) []string {
	var arr []string
	_ = json.Unmarshal([]byte(text), &arr)
	return trimmedNonEmpty(arr)
}

func rfc3339OrEmpty(t sql.NullTime) string {
	if !t.Valid {
		return ""
	}
	return t.Time.UTC().Format(time.RFC3339)
}
