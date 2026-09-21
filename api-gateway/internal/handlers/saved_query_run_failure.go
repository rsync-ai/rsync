package handlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"regexp"
	"strings"
	"time"

	"api-gateway/internal/db"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rsync-ai/backend-orchestrator/pkg/llmscrub"
	log "github.com/sirupsen/logrus"
)

// internalModelRunFailureRequest is what RecordModelRunFailureActivity POSTs once a
// workflow's run activity has failed for good.
type internalModelRunFailureRequest struct {
	ScheduleID string `json:"schedule_id"`
	// Trigger is the same door selector the run endpoint takes, so the failure is looked
	// up, and recorded, under the door the run itself went through.
	Trigger string `json:"trigger"`
	// Error is the workflow's own wording of what went wrong. Stored only after
	// modelRunFailureMessage has scrubbed and bounded it.
	Error string `json:"error"`
	// StartedAt is workflow time from before the run activity was scheduled. It is fixed
	// in the workflow's history, so every retry of the recording activity sends the same
	// value, which is what makes it half of the dedupe key.
	StartedAt time.Time `json:"started_at"`

	// Provenance of an event-door run, as the run request carries it. Optional: a failure
	// with none still records, it just cannot say which upstream woke it.
	UpstreamKind  string `json:"upstream_kind,omitempty"`
	UpstreamID    string `json:"upstream_id,omitempty"`
	UpstreamRunID string `json:"upstream_run_id,omitempty"`
	ExecutionID   string `json:"execution_id,omitempty"`
	Depth         int    `json:"depth,omitempty"`
	Coalesced     int    `json:"coalesced,omitempty"`
}

// modelRunFailureErrorMaxRunes bounds the stored error. The text is composed by the
// workflow around an activity error, and an activity error can carry a whole HTTP
// response body; the history panel needs the sentence, not the body.
const modelRunFailureErrorMaxRunes = 1000

// modelRunFailureLockNamespace keys the lock that serializes failure records for one
// schedule. Its own namespace, not modelRunLockNamespace: that one is a session lock held
// for a whole rebuild, and sharing the key space would let a failure record wait on a run
// of a model whose id hashes the same.
const modelRunFailureLockNamespace = 0x72534D46 // "rSMF": rsync saved-query model failure

// modelRunFailureMessage is the text stored for a failure record. Scrubbed before it is
// bounded, so a cut can never leave half a credential behind: the activity error it is
// built from can quote a gateway response or a connection string, and saved_query_runs is
// readable by every member who can open the model.
func modelRunFailureMessage(raw string) string {
	msg := modelRunErrorMessage(raw)
	if msg == "" {
		return "The scheduled run did not report a result, and no reason was given. The schedule will try again at its next run."
	}
	return msg
}

// Driver wording that names where a connection went. pgx opens every connect failure
// with the user and database it tried, then the host:port (and resolved host) of each
// attempt; the net package adds the address again. None of it helps the reader of the
// run history, and the address is the source server's (#51).
var (
	rePgxConnectPreamble = regexp.MustCompile("failed to connect to `[^`]*`:\\s*")
	reAttemptHostPort    = regexp.MustCompile(`\S+:\d+ \([^)]*\):\s*`)
	reDialLookup         = regexp.MustCompile(`dial tcp: lookup \S+?(?: on \S+)?:\s*`)
	reDialAddr           = regexp.MustCompile(`dial tcp \S+:\d+:\s*`)
	reDialError          = regexp.MustCompile(`dial error:\s*`)
)

// modelRunErrorMessage is the text stored and shown for a run that failed while
// executing. The same scrub as a failure record, since both land in saved_query_runs,
// after dropping the connect preamble that names the source host.
func modelRunErrorMessage(raw string) string {
	msg := strings.TrimSpace(raw)
	msg = rePgxConnectPreamble.ReplaceAllString(msg, "could not connect to the database: ")
	msg = reAttemptHostPort.ReplaceAllString(msg, "")
	msg = reDialLookup.ReplaceAllString(msg, "host lookup failed: ")
	msg = reDialAddr.ReplaceAllString(msg, "")
	msg = reDialError.ReplaceAllString(msg, "")
	return llmscrub.ScrubMax(msg, modelRunFailureErrorMaxRunes)
}

// storedRunErrorForDisplay is the scrub again, on the way out. Rows written before the
// write-side scrub still hold the driver's text, source address included, and nothing
// rewrites old rows; the scrub leaves clean text unchanged, so it is safe on every
// row (#51 prod retest: 13 runs from before the deploy still showed the IP).
func storedRunErrorForDisplay(stored string) string {
	if strings.TrimSpace(stored) == "" {
		return ""
	}
	return modelRunErrorMessage(stored)
}

// modelRunFailureDisposition is what recordScheduledModelRunFailure did.
type modelRunFailureDisposition string

const (
	runFailureRecorded     modelRunFailureDisposition = "recorded"
	runFailureDuplicate    modelRunFailureDisposition = "duplicate"
	runFailureModelGone    modelRunFailureDisposition = "model_gone"
	runFailureScheduleGone modelRunFailureDisposition = "schedule_gone"
)

// modelRunFailureRecord is one failure report, already validated by the handler.
type modelRunFailureRecord struct {
	ModelID    string
	ScheduleID string
	EventPath  bool
	StartedAt  time.Time // UTC, microsecond precision
	Message    string    // already scrubbed
	Provenance runProvenance
}

// recordScheduledModelRunFailure writes the history row, and the badge, for a scheduled
// run whose workflow never got a result back from RunSavedQueryModelInternal.
//
// It is not recordModelRunOutcome, for three reasons that are all about arriving late and
// possibly twice. recordModelRunOutcome cannot dedupe, and Temporal delivers an activity
// at least once. It swallows errors, and here a failed write has to reach the activity so
// its retry can try again. And its stamp is unconditional, which is right for a result
// that has just been produced and wrong for a report of a result that never came (see
// stampLastRunFailureIfLatest).
//
// Every read is scoped the way the run path's is: the model resolves its own workspace,
// the schedule is matched on BOTH ids through modelRunScheduleLookup's door (which, on the
// event door, re-checks that no upstream sits outside that workspace), and the badge write
// names the workspace it resolved.
func recordScheduledModelRunFailure(ctx context.Context, database *sql.DB, rec modelRunFailureRecord) (modelRunFailureDisposition, error) {
	tx, err := database.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return "", fmt.Errorf("begin failure record: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op once Commit has succeeded

	var workspaceID string
	err = tx.QueryRowContext(ctx, `SELECT workspace_id::text FROM saved_queries WHERE id = $1`, rec.ModelID).Scan(&workspaceID)
	if errors.Is(err, sql.ErrNoRows) {
		return runFailureModelGone, nil
	}
	if err != nil {
		return "", fmt.Errorf("load model: %w", err)
	}

	query, trigger := modelRunScheduleLookup(rec.EventPath)
	var scheduleID, runAsUserID, status string
	var temporalID sql.NullString
	err = tx.QueryRowContext(ctx, query, rec.ModelID, rec.ScheduleID, scheduleAfterUpstream).
		Scan(&scheduleID, &temporalID, &runAsUserID, &status)
	if errors.Is(err, sql.ErrNoRows) {
		// Deleted, converted to the other door, or never this model's. Nothing is written:
		// a failure filed against a schedule the model does not have would be history for
		// a trigger nobody can see or turn off.
		return runFailureScheduleGone, nil
	}
	if err != nil {
		return "", fmt.Errorf("load schedule: %w", err)
	}
	// A paused schedule is still recorded. Pausing stops the next tick, not a tick that
	// had already fired and failed. That attempt happened, and hiding it would make the
	// pause look like the reason the model is stale.

	// Two deliveries of the same record (an attempt that timed out client-side while its
	// write was still committing, and the retry that followed) would both see no row under
	// READ COMMITTED and both insert. The transaction lock makes the second wait for the
	// first to commit, so its EXISTS sees the row.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1, $2)`,
		int32(modelRunFailureLockNamespace), int32(crc32.ChecksumIEEE([]byte(scheduleID)))); err != nil {
		return "", fmt.Errorf("lock failure record: %w", err)
	}

	var exists bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM saved_query_runs
			WHERE saved_query_id = $1 AND schedule_id = $2::uuid AND started_at = $3
		)
	`, rec.ModelID, scheduleID, rec.StartedAt).Scan(&exists); err != nil {
		return "", fmt.Errorf("check for an existing record: %w", err)
	}
	if exists {
		return runFailureDuplicate, nil
	}

	if err := stampLastRunFailureIfLatest(ctx, tx, rec.ModelID, workspaceID, rec.Message, rec.StartedAt); err != nil {
		return "", fmt.Errorf("stamp model: %w", err)
	}

	// Provenance is only meaningful to a triggered run, as on the run path.
	prov := rec.Provenance
	if trigger != triggerTriggered {
		prov = runProvenance{}
	}
	// started_at is bound, not derived from NOW(): it is the dedupe key, and a retry has
	// to write the value the first delivery would have.
	args := append([]interface{}{rec.ModelID, scheduleID, string(trigger), rec.Message, runAsUserID, rec.StartedAt}, prov.args()...)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO saved_query_runs (
			saved_query_id, schedule_id, trigger_source, status, error,
			ran_as_user_id, started_at, finished_at,
			`+runProvenanceColumns+`
		) VALUES (
			$1, $2::uuid, $3, 'failed', NULLIF($4, ''),
			NULLIF($5, '')::uuid, $6, NOW(),
			`+runProvenanceValues(7)+`
		)
	`, args...); err != nil {
		return "", fmt.Errorf("append run history: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit failure record: %w", err)
	}
	return runFailureRecorded, nil
}

// stampLastRunFailureIfLatest is stampLastRunOutcome for a failure reported after the
// fact, scoped to the model's workspace.
//
// Conditional where the run path's stamp is not. A failure record arrives only after every
// attempt of the workflow's run activity is spent, which can be well over half an hour
// after the attempt began. Anything that stamped the model since then (a manual run, or
// the gateway finishing this very rebuild with only the response lost) describes the
// table more recently than a report that no result came back, so the badge keeps it. The
// history row is written either way.
func stampLastRunFailureIfLatest(ctx context.Context, ex modelExecer, modelID, workspaceID, message string, startedAt time.Time) error {
	_, err := ex.ExecContext(ctx, `
		UPDATE saved_queries
		SET last_run_at = NOW(), last_run_status = 'failed', last_run_error = NULLIF($2, '')
		WHERE id = $1 AND workspace_id = $3::uuid
		  AND (last_run_at IS NULL OR last_run_at <= $4)
	`, modelID, message, workspaceID, startedAt)
	return err
}

// RecordSavedQueryModelRunFailureInternal handles
// POST /api/v1/internal/explorer/models/:id/run-failed.
//
// RunSavedQueryModelInternal records every run it gets to attempt, failed ones included.
// What it cannot record is a run it never answered: the gateway was down or restarting,
// the request timed out, or the gateway itself returned a 500 before running anything.
// Those end in the workflow with the run activity's retries spent and nothing in
// saved_query_runs, so the history panel showed only successes and a broken schedule
// looked like one that had not fired. The workflow reports them here instead.
//
// Status codes are chosen for the activity calling it. 404 with {"status":"not_found"}
// means there is nothing to record against (the model or its schedule is gone), and the
// activity treats it as done. 400 is a malformed call that no retry will fix. 500 is a
// write that failed and should be retried. A duplicate delivery answers 200 and writes
// nothing.
//
// A recorded failure does not write upstream_failed rows for the models below. The run
// path does, because it knows the run failed; this path only knows no answer came back,
// and when the gateway did finish the run and only the response was lost, the models
// below were already woken by it.
func RecordSavedQueryModelRunFailureInternal(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid saved query id"})
		return
	}

	var req internalModelRunFailureRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	if _, err := uuid.Parse(req.ScheduleID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid schedule id"})
		return
	}
	if req.Trigger != "" && req.Trigger != scheduleAfterUpstream {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unknown trigger"})
		return
	}
	if req.StartedAt.IsZero() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "started_at is required"})
		return
	}
	eventPath := req.Trigger == scheduleAfterUpstream

	rec := modelRunFailureRecord{
		ModelID:    id,
		ScheduleID: req.ScheduleID,
		EventPath:  eventPath,
		// started_at is TIMESTAMPTZ, which keeps microseconds. Compared at the precision
		// it is stored at, or a nanosecond-bearing retry would never equal the row it
		// duplicates.
		StartedAt: req.StartedAt.UTC().Truncate(time.Microsecond),
		Message:   modelRunFailureMessage(req.Error),
	}
	if eventPath {
		rec.Provenance = provenanceFor(modelRefreshSource{
			Kind:        req.UpstreamKind,
			ID:          req.UpstreamID,
			RunID:       req.UpstreamRunID,
			ExecutionID: req.ExecutionID,
			Depth:       req.Depth,
		}, req.Coalesced)
	}

	disp, err := recordScheduledModelRunFailure(c.Request.Context(), database, rec)
	if err != nil {
		log.WithError(err).WithField("model_id", id).Error("internal model run failure: could not record")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record the failed run"})
		return
	}

	switch disp {
	case runFailureModelGone:
		c.JSON(http.StatusNotFound, gin.H{"status": "not_found", "reason": "model no longer exists"})
	case runFailureScheduleGone:
		c.JSON(http.StatusNotFound, gin.H{"status": "not_found", "reason": "no such schedule for this model"})
	case runFailureDuplicate:
		c.JSON(http.StatusOK, gin.H{"recorded": false, "duplicate": true})
	default:
		c.JSON(http.StatusOK, gin.H{"recorded": true})
	}
}
