package workflows

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/activity"
)

// UpdatePipelineStatusActivity updates the pipelines table (status/completed_at/error_message)
// and keeps the executions table in sync when executionID is provided.
//
// This is intentionally best-effort: it should not fail workflow correctness if the DB is unavailable,
// but it keeps the "pipelines" table aligned with the authoritative workflow state stored in pipeline_states.
func UpdatePipelineStatusActivity(ctx context.Context, pipelineID string, executionID string, status string, errorMessage string) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Updating pipeline row status",
		"pipeline_id", pipelineID,
		"execution_id", executionID,
		"status", status,
		"error_message_present", errorMessage != "",
	)

	if activityCtx == nil || activityCtx.DB == nil {
		log.Warn("⚠️  UpdatePipelineStatusActivity: DB not available, skipping pipeline row update")
		return nil
	}

	db, ok := activityCtx.DB.(*sql.DB)
	if !ok || db == nil {
		log.Warn("⚠️  UpdatePipelineStatusActivity: DB has unexpected type, skipping pipeline row update")
		return nil
	}

	// Streaming handoff: close the execution row without touching the pipeline row.
	// Used when a CDC/streaming workflow exits early; the pipeline stays 'running'
	// under CDC sentinel supervision, but the Temporal execution record is done.
	if status == "streaming_active" {
		if executionID != "" {
			if _, err := db.ExecContext(ctx, `
				UPDATE executions
				SET status    = 'completed',
				    end_time  = NOW()
				WHERE id = $1 AND status = 'running'
			`, executionID); err != nil {
				logger.Warn("streaming_active: failed to close execution row (ignored)",
					"execution_id", executionID, "error", err.Error())
			} else {
				logger.Info("streaming_active: execution row closed", "execution_id", executionID)
			}
		}

		// Reconcile pipeline_progress so the streaming state is represented cleanly at
		// its source. The executor's final STAGE_COMPLETED event leaves the row frozen
		// at stage_state='succeeded', a mid-pipeline step count (e.g. 7/8) and a
		// heartbeat that immediately goes stale — which makes a live (or, later, a
		// silently-dead) CDC stream look like a finished batch run. Mirror the
		// terminal-failure reconcile below: a bare, version-bumped UPDATE scoped to
		// this execution. The dependency-aware /runtime endpoint remains the source of
		// truth for whether the stream is actually alive; this only clears the residue
		// so the surfaces that still read pipeline_progress raw (Live panel scalars,
		// list derived_status) don't paint a misleading picture. Best-effort: a failure
		// here must not break the streaming handoff.
		if pipelineID != "" && executionID != "" {
			if _, err := db.ExecContext(ctx, `
				UPDATE pipeline_progress
				SET status                  = 'running',
				    stage_state             = 'running',
				    current_stage           = 'streaming',
				    message                 = 'Streaming pipeline active',
				    stage_summary           = 'Streaming pipeline active',
				    progress_percent        = 100,
				    progress_current_step   = progress_total_steps,
				    stage_last_heartbeat_at = NOW(),
				    version                 = COALESCE(version, 0) + 1,
				    updated_at              = NOW()
				WHERE pipeline_id = $1 AND execution_id = $2
			`, pipelineID, executionID); err != nil {
				logger.Warn("streaming_active: failed to reconcile pipeline_progress (ignored)",
					"pipeline_id", pipelineID, "execution_id", executionID, "error", err.Error())
			} else {
				logger.Info("streaming_active: pipeline_progress reconciled to streaming",
					"pipeline_id", pipelineID, "execution_id", executionID)
			}
		}
		return nil
	}

	// Phase 1 postflight: when the workflow thinks it's completed, verify
	// against destination-truth row stats. The executor's in-process
	// silent-drop check (in CheckForSilentDrop) compares the source-side
	// counters, but the kafka-mcp-sink path only learns the destination
	// landed-row count after the executor returns. So this is the only
	// place that can catch "executor said success, sink wrote 0 rows".
	//
	// If any per-table stats row for this execution shows read_rows>0
	// AND no rows actually landed (inserted_rows + applied_inserts == 0)
	// OR status='degraded', downgrade the workflow status to 'failed'
	// with reason 'silent_drop_detected'. Fail-soft: if the stats query
	// errors or returns nothing, keep the original status — we'd rather
	// miss a drop than spuriously fail a healthy run.
	if status == "completed" && executionID != "" {
		if dropReason, ok := postflightSilentDropCheck(ctx, db, executionID); ok {
			logger.Warn("🚨 Postflight silent-drop guard downgraded completed -> failed",
				"execution_id", executionID,
				"pipeline_id", pipelineID,
				"reason", dropReason,
			)
			status = "failed"
			if errorMessage == "" {
				errorMessage = dropReason
			} else {
				errorMessage = dropReason + " | " + errorMessage
			}
		}
	}

	var errParam any
	if errorMessage != "" {
		errParam = errorMessage
	} else {
		errParam = nil
	}

	// Only set completed_at when terminal.
	isTerminal := status == "completed" || status == "failed" || status == "stopped"
	if isTerminal {
		// F-25: wrap the pipelines + executions terminal updates in a
		// single transaction so consumers of GET /pipelines/:id never
		// observe pipelines.status='completed' while last_execution.status
		// is still 'running'. The sub-select that computes last_execution
		// (api-gateway pipelines.go:1005) reads against the same DB; a
		// transaction commits both writes atomically.
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin tx for terminal status update: %w", err)
		}
		// On any error path below, roll back. Successful path Commits and
		// nils out tx so the deferred Rollback becomes a no-op.
		defer func() {
			if tx != nil {
				_ = tx.Rollback()
			}
		}()

		res, err := tx.ExecContext(ctx, `
			UPDATE pipelines
			SET status = $2,
			    updated_at = NOW(),
			    completed_at = NOW(),
			    error_message = $3
			WHERE id = $1
		`, pipelineID, status, errParam)
		if err != nil {
			return fmt.Errorf("failed to update pipeline terminal status: %w", err)
		}
		pipelineRows, pipelineRowsErr := res.RowsAffected()

		// Keep executions table in sync as well (best-effort within the tx).
		// This prevents the pipelines list from showing "running" due to a stale executions.status.
		if executionID != "" {
			execStatus := status
			if status == "stopped" {
				// The rest of the system uses "cancelled" for execution terminal stops.
				execStatus = "cancelled"
			}
			if _, err := tx.ExecContext(ctx, `
				UPDATE executions
				SET status = $2,
				    end_time = NOW(),
				    error_message = $3
				WHERE id = $1
			`, executionID, execStatus, errParam); err != nil {
				// Stay best-effort: log + roll back so we don't half-commit
				// the pipelines row without the executions row.
				logger.Warn("Failed to update executions terminal status (rolling back tx)",
					"execution_id", executionID,
					"status", execStatus,
					"error", err.Error(),
				)
				return fmt.Errorf("failed to update executions terminal status: %w", err)
			}

			// Reconcile the progress surface on terminal FAILURE. The executor
			// stage emits a "succeeded" / "Pipeline completed successfully"
			// progress event when the source→sink export finishes — but the
			// post-export landed-row reconcile (orchestrator Guard A) can later
			// fail the run, and nothing rewrites pipeline_progress. That left the
			// UI showing stage_state='succeeded' + "completed successfully" for an
			// execution whose executions.status is 'failed'. This is the single
			// authoritative terminal-write site, so it owns clearing that residue.
			// Scoped to THIS execution (single row per pipeline, ON CONFLICT
			// (pipeline_id)) so a healer retry's newer progress is never clobbered;
			// a later healer escalation to 'waiting_for_user' leaves stage_state +
			// message intact (it only sets status + blocking_reason_*).
			if status == "failed" {
				if _, err := tx.ExecContext(ctx, `
					UPDATE pipeline_progress
					SET status        = 'failed',
					    stage_state   = 'failed',
					    message       = COALESCE($2, message),
					    stage_summary = COALESCE($2, stage_summary),
					    version       = COALESCE(version, 0) + 1,
					    updated_at    = NOW()
					WHERE pipeline_id = $1 AND execution_id = $3
				`, pipelineID, errParam, executionID); err != nil {
					logger.Warn("Failed to reconcile pipeline_progress terminal status (rolling back tx)",
						"execution_id", executionID,
						"error", err.Error(),
					)
					return fmt.Errorf("failed to reconcile pipeline_progress terminal status: %w", err)
				}
			}
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit terminal status tx: %w", err)
		}
		tx = nil // disarm the deferred rollback

		if pipelineRowsErr == nil && pipelineRows == 0 {
			logger.Warn("No pipeline row updated (pipeline not found?)", "pipeline_id", pipelineID, "status", status)
		} else if pipelineRowsErr == nil {
			logger.Info("Pipeline row updated", "pipeline_id", pipelineID, "status", status, "rows_affected", pipelineRows)
		}

		return nil
	}

	// Non-terminal update (rare): do not touch completed_at.
	res, err := db.ExecContext(ctx, `
		UPDATE pipelines
		SET status = $2,
		    updated_at = NOW(),
		    error_message = COALESCE($3, error_message)
		WHERE id = $1
	`, pipelineID, status, errParam)
	if err != nil {
		return fmt.Errorf("failed to update pipeline status: %w", err)
	}
	if rows, rerr := res.RowsAffected(); rerr == nil && rows == 0 {
		logger.Warn("No pipeline row updated (pipeline not found?)", "pipeline_id", pipelineID, "status", status)
	} else if rerr == nil {
		logger.Info("Pipeline row updated", "pipeline_id", pipelineID, "status", status, "rows_affected", rows)
	}

	// Best-effort: if a scheduled execution row exists in "pending" state, flip it to "running"
	// once the schedule wrapper marks the pipeline as running.
	if status == "running" && executionID != "" {
		if _, err := db.ExecContext(ctx, `UPDATE executions SET status = 'running' WHERE id = $1 AND status = 'pending'`, executionID); err != nil {
			logger.Warn("Failed to promote scheduled execution pending->running (ignored)",
				"execution_id", executionID,
				"error", err.Error(),
			)
		}
	}

	// Tiny guard to ensure deterministic behavior isn't impacted by time usage here (activity-only).
	_ = time.Now()

	return nil
}

// postflightSilentDropCheck inspects pipeline_run_table_stats for the
// given execution and returns (reason, true) when the destination-truth
// signal indicates a silent drop. Returns ("", false) on no-drop OR on
// any error (fail-soft).
//
// Drop signature: a row with read_rows > 0 AND zero landed rows
// (COALESCE(inserted_rows, applied_inserts, 0) = 0) OR status='degraded'
// with read_rows > 0.
//
// It also catches the inverse blind spot — a selected table with NO stats row at
// all. Every signature above is evaluated per EXISTING row, so a table that was
// selected for the run and then never dispatched (filtered out upstream, or the
// executor died before reaching it) contributed no row and was therefore invisible:
// the loop had nothing to iterate and the run completed clean with that table's data
// missing entirely. See missingSelectedTables for the preconditions that keep this
// from firing on a run whose stats simply aren't being emitted.
func postflightSilentDropCheck(ctx context.Context, db *sql.DB, executionID string) (string, bool) {
	if db == nil || executionID == "" {
		return "", false
	}
	rows, err := db.QueryContext(ctx, `
		SELECT
			COALESCE(qualified_name, table_name) AS tbl,
			COALESCE(table_name, '') AS bare,
			COALESCE(read_rows, 0) AS rr,
			COALESCE(inserted_rows, 0) + COALESCE(applied_inserts, 0) AS landed,
			COALESCE(status, '') AS st
		FROM pipeline_run_table_stats
		WHERE execution_id = $1
	`, executionID)
	if err != nil {
		log.Warnf("postflightSilentDropCheck: query failed (ignored): %v", err)
		return "", false
	}
	defer rows.Close()

	type drop struct {
		table  string
		read   int64
		landed int64
		status string
		// missing marks a table that produced no stats row at all, so read/landed
		// carry no measurement and must not be printed as if they did.
		missing bool
	}
	var drops []drop
	// Every name this execution reported a stats row under, in both the qualified and
	// bare form, lower-cased. missingSelectedTables diffs the selection against this.
	reported := make(map[string]bool)
	for rows.Next() {
		var d drop
		var bare string
		if err := rows.Scan(&d.table, &bare, &d.read, &d.landed, &d.status); err != nil {
			continue
		}
		reported[strings.ToLower(strings.TrimSpace(d.table))] = true
		if bare != "" {
			reported[strings.ToLower(strings.TrimSpace(bare))] = true
		}
		// Silent drop: source had rows, none landed in destination.
		if d.read > 0 && d.landed == 0 {
			drops = append(drops, d)
			continue
		}
		// Degraded with source rows is also a drop signal from the sink.
		if d.read > 0 && strings.EqualFold(strings.TrimSpace(d.status), "degraded") && d.landed < d.read {
			drops = append(drops, d)
			continue
		}
		// Partial drop: status was reported clean but the row count short-
		// changes the source by more than 1%. Catches the case where some
		// batches succeed (emit running stats) and others poison-DLQ
		// (don't emit stats) — sink may not flip the table-level status to
		// degraded if the failures are isolated. Threshold is operator-
		// tunable: a tiny floor of 1% absorbs natural type-coercion drops
		// (NULL filtering on NOT NULL columns, etc.) without flagging
		// healthy runs.
		if d.read > 0 && d.landed > 0 {
			minLanded := d.read - d.read/100 // 99% threshold
			if minLanded > 0 && d.landed < minLanded {
				drops = append(drops, d)
			}
		}
	}

	// A selected table that reported nothing at all is the same failure as read>0
	// landed=0 — the destination is missing that table's data — but it can only be
	// seen by diffing against the selection, not by iterating rows that exist.
	for _, tbl := range missingSelectedTables(ctx, db, executionID, reported) {
		drops = append(drops, drop{table: tbl, missing: true})
	}

	if len(drops) == 0 {
		return "", false
	}
	// Build a compact reason mentioning up to 3 tables.
	parts := make([]string, 0, len(drops))
	for i, d := range drops {
		if i >= 3 {
			parts = append(parts, fmt.Sprintf("(+%d more)", len(drops)-3))
			break
		}
		if d.missing {
			parts = append(parts, fmt.Sprintf("%s: selected but never reported (no stats row)", d.table))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s: read=%d landed=%d", d.table, d.read, d.landed))
	}
	reason := "silent_drop_detected: " + strings.Join(parts, ", ")
	// Surface the sink's REAL destination-write failure reason — recorded on the
	// negative ack's last_error (pipeline_batch_acks, rows_written=0) — so the
	// user-facing error_message carries the actionable cause (e.g. "invalid input
	// syntax for type bigint") instead of just the coarse read=N landed=M counts.
	// Mirrors the orchestrator's ack-ledger Guard A (executor.go). Fail-soft: any
	// query error or absent ack leaves the base reason unchanged.
	if sinkErr := sinkErrorForExecution(ctx, db, executionID); sinkErr != "" {
		reason += "; sink error: " + sinkErr
	}
	return reason, true
}

// missingSelectedTables returns the tables this pipeline was configured to sync that
// produced NO stats row for this execution — the "table vanished from the run entirely"
// drop, which per-row inspection structurally cannot see.
//
// Failing a healthy run is worse than missing a drop, so this fires only when all three
// preconditions hold and returns nil otherwise:
//
//  1. the execution reported at least one stats row — a run with zero rows means the
//     stats path isn't live for it (direct-write destinations, projector lag, an older
//     sink), not that every table was dropped;
//  2. the pipeline has a non-empty config.selected_tables — the same list the run path
//     reads (api-gateway cdc_tables.go:313, pipeline_hitl.go:757), so "no selection
//     recorded" means we have nothing to diff against, not that nothing was expected;
//  3. at least one selected name matched a reported name — proves the two sides use the
//     same naming convention. Without this, a qualified-vs-bare mismatch would make
//     every table look missing and fail every run of that pipeline.
//
// Matching is case-insensitive and accepts either form: a selection of "public.orders"
// matches a stats row reported as "orders", and vice versa.
func missingSelectedTables(ctx context.Context, db *sql.DB, executionID string, reported map[string]bool) []string {
	if db == nil || executionID == "" || len(reported) == 0 {
		return nil
	}

	var raw string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(p.config->'selected_tables', '[]'::jsonb)::text
		FROM executions e
		JOIN pipelines p ON p.id = e.pipeline_id
		WHERE e.id = $1
	`, executionID).Scan(&raw); err != nil {
		if err != sql.ErrNoRows {
			log.Warnf("missingSelectedTables: selection query failed (ignored): %v", err)
		}
		return nil
	}

	var selected []string
	if err := json.Unmarshal([]byte(raw), &selected); err != nil {
		// selected_tables is written as a JSON array of strings by every persistence
		// site; anything else is a shape we don't understand, so we don't judge it.
		log.Warnf("missingSelectedTables: selected_tables is not a string array (ignored): %v", err)
		return nil
	}
	if len(selected) == 0 {
		return nil
	}

	missing, matched := diffSelectedAgainstReported(selected, reported)
	if matched == 0 {
		// Precondition 3: nothing lined up, so this is a naming mismatch rather than a
		// wholesale drop. Log it — a pipeline that trips this permanently has a real
		// convention bug worth finding — but never fail the run on it.
		log.Warnf("missingSelectedTables: no selected table matched any reported stats row for execution %s (%d selected) — treating as a naming mismatch, not a drop",
			executionID, len(selected))
		return nil
	}
	return missing
}

// diffSelectedAgainstReported splits a table selection into the names that produced no
// stats row (missing) and a count of the names that did (matched). Matching is
// case-insensitive and form-insensitive in BOTH directions: a selection of
// "public.orders" matches a row reported bare as "orders", and a bare selection of
// "orders" matches a row reported qualified — the caller seeds `reported` with both
// forms of every row, and this strips the schema off each selected name.
//
// The matched count is the caller's naming-convention check: zero matches across a
// non-empty selection means the two sides disagree about how tables are named, which
// must not be read as "every table was dropped".
func diffSelectedAgainstReported(selected []string, reported map[string]bool) (missing []string, matched int) {
	for _, sel := range selected {
		sel = strings.TrimSpace(sel)
		if sel == "" {
			continue
		}
		key := strings.ToLower(sel)
		bare := key
		if i := strings.LastIndex(key, "."); i >= 0 && i+1 < len(key) {
			bare = key[i+1:]
		}
		if reported[key] || reported[bare] {
			matched++
			continue
		}
		missing = append(missing, sel)
	}
	return missing, matched
}

// sinkErrorForExecution returns the most recent non-empty last_error recorded on
// a negative batch ack (rows_written = 0) for this execution, or "" if none. This
// is the kafka-mcp-sink's real destination-write failure reason, persisted by the
// fail-closed ack ledger. execution_id is a UUID (globally unique), so it alone
// scopes the lookup. Fail-soft: returns "" on any error.
func sinkErrorForExecution(ctx context.Context, db *sql.DB, executionID string) string {
	if db == nil || executionID == "" {
		return ""
	}
	var lastErr sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT MAX(last_error)
		FROM pipeline_batch_acks
		WHERE execution_id = $1 AND rows_written = 0 AND COALESCE(last_error, '') <> ''
	`, executionID).Scan(&lastErr); err != nil {
		log.Warnf("sinkErrorForExecution: query failed (ignored): %v", err)
		return ""
	}
	if !lastErr.Valid {
		return ""
	}
	return strings.TrimSpace(lastErr.String)
}
