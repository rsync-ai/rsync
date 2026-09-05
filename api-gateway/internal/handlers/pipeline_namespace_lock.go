package handlers

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"api-gateway/internal/db"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// First-run destination-namespace resolution was reachable ONLY through the
// table-selection HITL (KI-NSLOCK-PROBE-UNREACHABLE-WITHOUT-HITL).
//
// Both call sites of lockFirstRunNamespace lived inside ResumeTables, so whether a
// pipeline's namespace was ever probed came down to how its request happened to be
// worded: a prompt that NAMES its tables lets the planner resolve them alone, the
// pipeline never parks, ResumeTables is never called, and the run writes into the
// seeded namespace — merging into tables that were already there, with no probe, no
// lock, and no notification. Prod pipeline 12c3579c did exactly that.
//
// The probe is a data-safety property of the RUN, not of the table-selection UI, so
// it has to hang off something every run passes through. That is the write boundary:
// the moment the executor knows its final table set and before it writes a row. The
// executor calls this over the internal service API because the probe needs to
// decrypt the destination connection and connect out to it, which is api-gateway's
// job, while knowing the final table set is the executor's.

// namespaceLockResult is what the run boundary learns from one lock call.
//
// Relocated/CheckpointsCleared exist so the executor can log, in the run's own
// trace, that its resume state was reset out from under it — a run that moves 0
// rows because it resumed "already complete" is indistinguishable from a healthy
// no-op incremental run unless something says the reset happened.
type namespaceLockResult struct {
	Namespace          string
	Locked             bool
	Relocated          bool
	CheckpointsCleared int64
}

// lockNamespaceForRun resolves + locks a pipeline's destination namespace from the
// run boundary, given the table set the executor is about to write.
//
// Locked=false with a nil error is a deliberate stand-down, not a failure — see the
// guards below.
//
// tables MUST be non-empty. Locking on an empty probe set would freeze the seeded
// namespace having proven nothing about it, and the lock is permanent: the pipeline
// could never afterwards be moved off a namespace it should never have been given.
// A run with no tables writes nothing, so there is nothing to protect yet.
func lockNamespaceForRun(ctx context.Context, database *sql.DB, pipelineID string, tables []string) (namespaceLockResult, error) {
	pipelineID = strings.TrimSpace(pipelineID)
	if database == nil || pipelineID == "" {
		return namespaceLockResult{}, errNamespaceLockBadRequest
	}

	clean := make([]string, 0, len(tables))
	for _, t := range tables {
		if s := strings.TrimSpace(t); s != "" {
			clean = append(clean, s)
		}
	}
	if len(clean) == 0 {
		return namespaceLockResult{}, errNamespaceLockNoTables
	}

	// No user session on this path — the owning workspace comes from the pipeline
	// row, the same way RunPipelineInternal resolves it. The workspace scopes the
	// ownership lookup, so getting it wrong would make every namespace look unowned.
	var workspaceID string
	if err := database.QueryRowContext(ctx,
		`SELECT workspace_id::text FROM pipelines WHERE id = $1::uuid`, pipelineID,
	).Scan(&workspaceID); err != nil {
		// A well-formed id for a pipeline that does not exist is a caller mistake,
		// not a lock failure. RunPipelineInternal answers the identical query against
		// the identical table with 404 (pipelines.go:3309); leaving this one at 500
		// made the same lookup report two different things depending on which
		// internal route asked it.
		if errors.Is(err, sql.ErrNoRows) {
			return namespaceLockResult{}, errNamespaceLockNotFound
		}
		return namespaceLockResult{}, err
	}

	// Cheap idempotency: the overwhelmingly common case is a pipeline that already
	// locked on an earlier run (or in the HITL moments ago). Answer it from the
	// control plane without touching the destination.
	if locked, lockedNS := destinationNamespaceLock(database, pipelineID); locked && strings.TrimSpace(lockedNS) != "" {
		return namespaceLockResult{Namespace: lockedNS, Locked: true}, nil
	}

	// Same gate the silent-caller HITL path uses, and for the same reason: it stands
	// down when source schemas are being MIRRORED, because there is no single
	// namespace to probe then and attaching one would flatten every source schema
	// into it (the PR #549 data loss).
	persisted, schemaMode := pipelineDestinationState(database, pipelineID)
	seeded, probe := serverSideFirstRunNamespace(false, persisted, schemaMode)
	if !probe {
		log.WithFields(log.Fields{"pipeline_id": pipelineID, "schema_mode": schemaMode}).
			Info("namespace lock: run boundary stood down (mirroring, or no seeded mapping)")
		return namespaceLockResult{}, nil
	}

	ns, relocated, cleared := lockFirstRunNamespace(ctx, database, workspaceID, pipelineID, seeded, clean, "run-boundary")
	return namespaceLockResult{
		Namespace:          ns,
		Locked:             true,
		Relocated:          relocated != nil,
		CheckpointsCleared: cleared,
	}, nil
}

var (
	errNamespaceLockBadRequest = &namespaceLockError{"invalid_request"}
	errNamespaceLockNoTables   = &namespaceLockError{"no_tables"}
	errNamespaceLockNotFound   = &namespaceLockError{"pipeline_not_found"}
)

type namespaceLockError struct{ code string }

func (e *namespaceLockError) Error() string { return e.code }

// LockPipelineNamespaceInternal is the internal service endpoint the executor calls
// at the write boundary. Thin wrapper: all of the behaviour is in lockNamespaceForRun
// so the integration test can drive it against a real destination without a router.
//
// Fail-soft is the CALLER's contract, not this handler's — the executor treats any
// non-200 as "proceed unlocked", matching every other degradation in this path
// (a failed probe has never blocked a run). So errors here are reported honestly
// rather than papered over with a 200.
func LockPipelineNamespaceInternal(c *gin.Context) {
	id, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	var req struct {
		SelectedTables []string `json:"selected_tables"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_body", "message": err.Error()})
		return
	}

	res, err := lockNamespaceForRun(c.Request.Context(), database, id, req.SelectedTables)
	if err != nil {
		if err == errNamespaceLockNoTables {
			c.JSON(http.StatusBadRequest, gin.H{"error": "no_tables", "message": "selected_tables must be non-empty"})
			return
		}
		if err == errNamespaceLockBadRequest {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request"})
			return
		}
		if err == errNamespaceLockNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "pipeline_not_found", "message": "Pipeline not found"})
			return
		}
		// The error itself goes to the log, not the body: err here is whatever the
		// driver produced, and this route answers a service caller that has no use
		// for it. Reporting the failure honestly is the status code's job.
		log.WithError(err).WithField("pipeline_id", id).Warn("namespace lock: run-boundary lock failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "namespace_lock_failed", "message": "Failed to lock destination namespace"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"pipeline_id":         id,
		"locked":              res.Locked,
		"namespace":           res.Namespace,
		"relocated":           res.Relocated,
		"checkpoints_cleared": res.CheckpointsCleared,
	})
}
