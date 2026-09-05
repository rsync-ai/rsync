package handlers

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"time"

	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
)

// Read-only monitoring + versioning views over transform_execution_logs.
//
// transform_execution_logs is WRITTEN only by the orchestrator batch executor and
// the kafka-mcp-sink (api-gateway never writes it). Both endpoints below are pure
// read-side derivations — NO migration, NO new counter, NO writer change:
//   - GetPipelineTransformRollup       (P0'): per-execution GROUP BY summary
//     (in/out row totals, an HONEST rows-dropped figure, freshness, duration).
//   - GetPipelineTransformConfigHistory (P1): config_snapshot revision timeline
//     per transform slot, canonically deduped so only real changes appear.
//
// Both are keyed by :pipeline_id, so tenancy is enforced with the same gate the
// other /transforms/pipeline/:pipeline_id handlers use — requirePipelineWorkspaceRole
// (transform_execution_logs has no workspace_id; the pipeline_id FK is the tenant
// boundary). Reads only require >= viewer.

// TransformExecutionRollup is one row of the per-execution GROUP BY summary.
//
// rows_dropped is HONEST and self-truthing: it sums, per transform step, the rows
// actually removed — GREATEST(input_rows - output_rows, 0). Any transform that does
// not reduce rows (map / rename / type_convert / mask / passthrough) has
// input_rows == output_rows and contributes 0, so there is NO transform-type
// allow-list to keep in lockstep with the engine. This correctly counts every
// row-reducing operation the engine has — filter, null_handle(drop_row) and
// validate — not just filter (the old {filter,aggregate} list both missed the
// first two and named a dead op).
//
// total_input_rows / total_output_rows are NET throughput, not a naive column SUM:
// for each (execution, table) we take the FIRST transform's input and the LAST
// transform's output (by transform_order), then sum across tables. A table with a
// chained transform (2+ steps) is counted once, not once per step.
type TransformExecutionRollup struct {
	ExecutionID      string     `json:"execution_id"`
	TransformCount   int64      `json:"transform_count"`
	TotalInputRows   int64      `json:"total_input_rows"`
	TotalOutputRows  int64      `json:"total_output_rows"`
	RowsDropped      int64      `json:"rows_dropped"`
	TotalDurationMs  int64      `json:"total_duration_ms"`
	FailedCount      int64      `json:"failed_count"`
	HasError         bool       `json:"has_error"`
	FirstActivityAt  *time.Time `json:"first_activity_at,omitempty"`
	LatestActivityAt *time.Time `json:"latest_activity_at,omitempty"`
}

// GetPipelineTransformRollup returns a per-execution rollup of transform stats for
// a pipeline (GROUP BY execution_id over transform_execution_logs), newest first.
//
// Note on CDC: the kafka-mcp-sink writes CDC transform logs with
// execution_id == pipeline_id, so all CDC activity for a pipeline collapses into a
// single ever-accumulating "execution" row here — distinct from batch, which has a
// real per-run execution_id. Callers should treat a rollup row as "a run's worth of
// transform activity", not necessarily one discrete batch run.
func (h *TransformHandler) GetPipelineTransformRollup(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "pipeline_id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	// Tenant-isolation gate (IDOR): prove the pipeline lives in the caller's ACTIVE
	// workspace (>= viewer) before aggregating its per-transform row counts. 404 when
	// the pipeline is not in the active workspace — never reveal its existence.
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	limit := clampListLimit(c.Query("limit"), 50, 200)

	// The gate above already proved pipeline_id ∈ active workspace, so WHERE
	// tel.pipeline_id = $1 is a sufficient tenant boundary (same as
	// GetPipelineTransforms). We query limit+1 rows so we can tell the caller,
	// via "truncated", when there are more executions than shown (symmetric with
	// config-history) rather than silently capping.
	//
	// The per_table CTE collapses each (execution, table) chain to its NET
	// throughput (first transform's input, last transform's output by
	// transform_order) so a chained transform isn't double-counted; rows_dropped
	// sums the actual per-step reductions (see the type doc above).
	rows, err := h.db.Query(`
		WITH per_table AS (
			SELECT
				tel.execution_id,
				tel.table_name,
				(array_agg(tel.input_rows  ORDER BY tel.transform_order ASC,  tel.created_at ASC))[1]  AS first_input,
				(array_agg(tel.output_rows ORDER BY tel.transform_order DESC, tel.created_at DESC))[1] AS last_output,
				COALESCE(SUM(GREATEST(tel.input_rows - tel.output_rows, 0)), 0) AS rows_dropped,
				COUNT(*)                                       AS transform_count,
				COALESCE(SUM(tel.duration_ms), 0)              AS duration_ms,
				COUNT(*) FILTER (WHERE tel.status = 'failed')  AS failed_count,
				MIN(tel.created_at)                            AS first_at,
				MAX(tel.updated_at)                            AS latest_at
			FROM transform_execution_logs tel
			WHERE tel.pipeline_id = $1
			GROUP BY tel.execution_id, tel.table_name
		)
		SELECT
			execution_id::text,
			SUM(transform_count)            AS transform_count,
			COALESCE(SUM(first_input), 0)   AS total_input_rows,
			COALESCE(SUM(last_output), 0)   AS total_output_rows,
			COALESCE(SUM(rows_dropped), 0)  AS rows_dropped,
			COALESCE(SUM(duration_ms), 0)   AS total_duration_ms,
			SUM(failed_count)               AS failed_count,
			MIN(first_at)                   AS first_activity_at,
			MAX(latest_at)                  AS latest_activity_at
		FROM per_table
		GROUP BY execution_id
		ORDER BY latest_activity_at DESC
		LIMIT $2
	`, pipelineID, limit+1)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch transform rollup"})
		return
	}
	defer rows.Close()

	out := make([]TransformExecutionRollup, 0)
	for rows.Next() {
		var r TransformExecutionRollup
		var first, latest sql.NullTime
		if err := rows.Scan(
			&r.ExecutionID, &r.TransformCount, &r.TotalInputRows, &r.TotalOutputRows,
			&r.RowsDropped, &r.TotalDurationMs, &r.FailedCount, &first, &latest,
		); err != nil {
			continue
		}
		r.HasError = r.FailedCount > 0
		if first.Valid {
			t := first.Time
			r.FirstActivityAt = &t
		}
		if latest.Valid {
			t := latest.Time
			r.LatestActivityAt = &t
		}
		out = append(out, r)
	}

	// We fetched limit+1; if the extra row came back there are more executions
	// than we're returning — signal it and trim to the requested page.
	truncated := false
	if len(out) > limit {
		truncated = true
		out = out[:limit]
	}

	c.JSON(http.StatusOK, gin.H{
		"pipeline_id": pipelineID,
		"executions":  out,
		"truncated":   truncated,
	})
}

// ConfigRevision is one entry in a transform slot's config timeline: the first
// execution at which this (canonical) config_snapshot appeared, plus the top-level
// keys that changed relative to the previous revision (empty for the baseline).
type ConfigRevision struct {
	ExecutionID    string                 `json:"execution_id"`
	FirstSeenAt    time.Time              `json:"first_seen_at"`
	ConfigSnapshot map[string]interface{} `json:"config_snapshot"`
	ChangedKeys    []string               `json:"changed_keys"`
}

// TransformConfigSlot is the config history for one durable transform slot,
// identified by (table_name, transform_order) — the transform_execution_logs unique
// key minus execution_id. A single logical transform scoped to N tables yields N
// slots (one per table_name).
type TransformConfigSlot struct {
	TableName      string           `json:"table_name"`
	TransformOrder int              `json:"transform_order"`
	TransformID    *string          `json:"transform_id,omitempty"`
	TransformType  string           `json:"transform_type"`
	RevisionCount  int              `json:"revision_count"`
	Revisions      []ConfigRevision `json:"revisions"`
}

// configHistoryScanCap bounds how many raw log rows the history query scans, so a
// pipeline with a very long run history can't unbound the response. If it is hit,
// the response carries "truncated": true rather than silently dropping older runs.
const configHistoryScanCap = 5000

// deadSnapshotKeys carry zero diff signal and are stripped before comparing/returning:
// NormalizeAndValidate filters out disabled transforms before logging (so `enabled`
// is always true) and blocks requires_full_dataset in execution/CDC modes (so it is
// always absent/false). Keeping them would add noise, never signal.
var deadSnapshotKeys = []string{"enabled", "requires_full_dataset"}

// GetPipelineTransformConfigHistory returns, per transform slot, the timeline of
// config_snapshot revisions across the pipeline's executions — a read-only
// versioning probe. Snapshots are canonically deduped (Go json.Marshal sorts map
// keys, sidestepping jsonb's non-deterministic key order), so only executions where
// the config actually CHANGED produce a new revision.
func (h *TransformHandler) GetPipelineTransformConfigHistory(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "pipeline_id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	// Same tenant gate as the rollup — config_snapshot leaks filter conditions,
	// column lists and PII-masking rules, so it is >= viewer, active-workspace only.
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	rows, err := h.db.Query(`
		SELECT
			tel.execution_id::text,
			tel.table_name,
			tel.transform_order,
			NULLIF(tel.transform_id, '') AS transform_id,
			tel.transform_type,
			tel.config_snapshot,
			tel.created_at
		FROM transform_execution_logs tel
		WHERE tel.pipeline_id = $1
		ORDER BY tel.table_name ASC, tel.transform_order ASC, tel.created_at ASC
		LIMIT $2
	`, pipelineID, configHistoryScanCap+1)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch transform config history"})
		return
	}
	defer rows.Close()

	slots := make([]*TransformConfigSlot, 0)
	byKey := make(map[string]*TransformConfigSlot)
	// lastCanon[key] is the canonical JSON of the slot's most recent revision, used
	// to suppress no-op re-runs where the config didn't change.
	lastCanon := make(map[string]string)
	lastClean := make(map[string]map[string]interface{})

	scanned := 0
	truncated := false
	for rows.Next() {
		if scanned >= configHistoryScanCap {
			truncated = true
			break
		}
		scanned++

		var execID, tableName, transformType string
		var transformOrder int
		var transformID sql.NullString
		var snapJSON []byte
		var createdAt time.Time
		if err := rows.Scan(&execID, &tableName, &transformOrder, &transformID, &transformType, &snapJSON, &createdAt); err != nil {
			continue
		}

		clean := map[string]interface{}{}
		if len(snapJSON) > 0 {
			_ = json.Unmarshal(snapJSON, &clean)
		}
		for _, k := range deadSnapshotKeys {
			delete(clean, k)
		}
		// Treat an empty/`{}` snapshot as a failed/missing capture, not an
		// intentional empty config — skip it so it never masquerades as a revision.
		if len(clean) == 0 {
			continue
		}

		key := tableName + "\x00" + strconv.Itoa(transformOrder)
		slot := byKey[key]
		if slot == nil {
			slot = &TransformConfigSlot{
				TableName:      tableName,
				TransformOrder: transformOrder,
				TransformType:  transformType,
				Revisions:      make([]ConfigRevision, 0, 1),
			}
			if transformID.Valid {
				v := transformID.String
				slot.TransformID = &v
			}
			byKey[key] = slot
			slots = append(slots, slot)
		}

		canon := canonicalJSON(clean)
		if prev, seen := lastCanon[key]; seen && prev == canon {
			continue // config unchanged since the last revision — not a new version
		}

		// Non-nil so the baseline revision serializes as `[]`, not `null`
		// (a nil []string with no omitempty marshals to null, which crashes
		// FE consumers doing changed_keys.length). Stable [] contract.
		changed := []string{}
		if seenClean, ok := lastClean[key]; ok {
			changed = topLevelChangedKeys(seenClean, clean)
		}
		slot.Revisions = append(slot.Revisions, ConfigRevision{
			ExecutionID:    execID,
			FirstSeenAt:    createdAt,
			ConfigSnapshot: clean,
			ChangedKeys:    changed,
		})
		lastCanon[key] = canon
		lastClean[key] = clean
	}

	outSlots := make([]TransformConfigSlot, 0, len(slots))
	for _, s := range slots {
		s.RevisionCount = len(s.Revisions)
		outSlots = append(outSlots, *s)
	}

	c.JSON(http.StatusOK, gin.H{
		"pipeline_id": pipelineID,
		"slots":       outSlots,
		"truncated":   truncated,
	})
}

// clampListLimit parses a ?limit= query value, falling back to def and capping at
// max. Non-positive or unparseable values fall back to def.
func clampListLimit(raw string, def, max int) int {
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

// canonicalJSON returns a stable string encoding of m. encoding/json marshals map
// keys in sorted order (recursively), so two structurally-equal snapshots that
// differ only in jsonb key ordering produce the same string.
func canonicalJSON(m map[string]interface{}) string {
	b, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(b)
}

// topLevelChangedKeys returns the sorted set of top-level keys whose value differs
// between old and cur (including keys present in only one). Values are compared by
// their canonical JSON encoding, so nested key-order differences don't register.
func topLevelChangedKeys(old, cur map[string]interface{}) []string {
	changed := map[string]struct{}{}
	for k, ov := range old {
		cv, ok := cur[k]
		if !ok || !jsonEqual(ov, cv) {
			changed[k] = struct{}{}
		}
	}
	for k := range cur {
		if _, ok := old[k]; !ok {
			changed[k] = struct{}{}
		}
	}
	keys := make([]string, 0, len(changed))
	for k := range changed {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func jsonEqual(a, b interface{}) bool {
	ab, err1 := json.Marshal(a)
	bb, err2 := json.Marshal(b)
	if err1 != nil || err2 != nil {
		return false
	}
	return string(ab) == string(bb)
}
