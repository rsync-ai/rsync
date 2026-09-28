package handlers

// sink_worker_reaper.go — the retry for a sink-worker stop that failed during a pipeline
// Stop or Delete (KI-CDC-SINK-WORKER-NO-REAPER).
//
// stopSinkWorkers reports a stop it could not make and the request moves on: the row is
// deleted or flipped to 'stopped', and the worker keeps consuming and writing. Nothing
// tried again. The sink service's supervisor made it worse: a stop that never reached
// the service set no intentional_stop mark, so an orphan that later crashed was
// respawned.
//
// The reaper is that retry. Every interval it lists the workers the sink service holds
// (list_sinks) and asks the pipelines table about each worker's pipeline. It stops a
// worker only when the database POSITIVELY says its pipeline is gone:
//
//   - no row for the pipeline id (a delete is a hard DELETE), or
//   - status 'stopped', last changed more than a grace period ago.
//
// Everything else keeps the worker: a failed list, a worker with no pipeline UUID, a
// failed query, any other status ('running', 'active', 'paused', 'failed', …). A worker
// must look orphaned on two ticks in a row, and the database is asked again right
// before the stop, so a pipeline started between two looks is never touched. stop_sink
// keeps the consumer group's committed offsets, and nothing here touches a CDC slot,
// publication or connector: a stopped CDC pipeline resumes where it left off.
//
// Dormant by default, like CDC_RECOVERY_ENABLED and CDC_SINK_AUTORESTART_ENABLED:
// SINK_WORKER_REAPER_MODE unset or "off" starts nothing; "dry_run" logs what it would
// stop; "enforce" stops it.

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

type sinkReaperMode int

const (
	sinkReaperOff sinkReaperMode = iota
	sinkReaperDryRun
	sinkReaperEnforce
)

func (m sinkReaperMode) String() string {
	switch m {
	case sinkReaperDryRun:
		return "dry_run"
	case sinkReaperEnforce:
		return "enforce"
	default:
		return "off"
	}
}

// parseSinkReaperMode reads SINK_WORKER_REAPER_MODE. Anything it does not recognise is
// off: a typo must not arm a loop that stops workers.
func parseSinkReaperMode(v string) sinkReaperMode {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "dry_run", "dry-run", "dryrun":
		return sinkReaperDryRun
	case "enforce":
		return sinkReaperEnforce
	default:
		return sinkReaperOff
	}
}

const (
	defaultSinkReaperInterval     = 5 * time.Minute
	defaultSinkReaperStoppedGrace = 10 * time.Minute
	// sinkReaperSightings is how many ticks in a row a worker must look orphaned before
	// it is stopped.
	sinkReaperSightings   = 2
	sinkReaperCallTimeout = 45 * time.Second
)

// sinkWorkerListing is one worker the sink service reported.
type sinkWorkerListing struct {
	consumerGroup string
	pipelineID    string
}

// listSinkWorkers asks the sink service which workers it holds. ok is false on any
// failure, including an older sink image without list_sinks: no listing, no action.
func listSinkWorkers(ctx context.Context, sinks sinkStopExecutor) (workers []sinkWorkerListing, ok bool) {
	if sinks == nil {
		return nil, false
	}
	resp, err := sinks.ExecuteWithContext(ctx, mcp.ExecuteRequest{
		Connector: "kafka-mcp-sink",
		Operation: "list_sinks",
		Config:    map[string]string{},
		Params:    map[string]interface{}{"config": map[string]interface{}{}},
	})
	if err != nil || resp == nil || !resp.Success || resp.Result == nil {
		return nil, false
	}
	raw, isList := resp.Result["workers"].([]interface{})
	if !isList {
		return nil, false
	}
	for _, item := range raw {
		w, isMap := item.(map[string]interface{})
		if !isMap {
			continue
		}
		group, _ := w["consumer_group"].(string)
		pid, _ := w["pipeline_id"].(string)
		group, pid = strings.TrimSpace(group), strings.TrimSpace(pid)
		if group == "" {
			continue
		}
		workers = append(workers, sinkWorkerListing{consumerGroup: group, pipelineID: pid})
	}
	return workers, true
}

// sinkWorkerOwnerGone asks the pipelines table whether the pipeline that owns a worker
// is gone. It returns a reason only on a positive answer; a worker with no pipeline
// UUID, a failed query or any live status returns "".
func sinkWorkerOwnerGone(ctx context.Context, db *sql.DB, pipelineID string, grace time.Duration) string {
	if db == nil {
		return ""
	}
	if _, err := uuid.Parse(pipelineID); err != nil {
		return ""
	}
	var status string
	var settled bool
	err := db.QueryRowContext(ctx,
		`SELECT status, updated_at < NOW() - make_interval(secs => $2) FROM pipelines WHERE id = $1::uuid`,
		pipelineID, grace.Seconds()).Scan(&status, &settled)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "pipeline deleted"
	case err != nil:
		log.WithError(err).WithField("pipeline_id", pipelineID).
			Debug("sink worker reaper: pipeline lookup failed; keeping the worker")
		return ""
	case strings.EqualFold(strings.TrimSpace(status), "stopped") && settled:
		return "pipeline stopped"
	default:
		return ""
	}
}

// sinkReapAction is what one tick decided for one worker, returned for tests and logs.
type sinkReapAction struct {
	ConsumerGroup string
	PipelineID    string
	Reason        string
	Stopped       bool   // enforce mode: stop_sink succeeded
	Failure       string // enforce mode: why the stop failed
}

// SinkWorkerReaper stops kafka-mcp-sink workers whose pipeline is gone.
type SinkWorkerReaper struct {
	db       *sql.DB
	sinks    sinkStopExecutor
	mode     sinkReaperMode
	interval time.Duration
	grace    time.Duration
	// suspects counts consecutive ticks each group has looked orphaned.
	suspects map[string]int
}

func newSinkWorkerReaper(db *sql.DB, sinks sinkStopExecutor, mode sinkReaperMode, interval, grace time.Duration) *SinkWorkerReaper {
	return &SinkWorkerReaper{
		db: db, sinks: sinks, mode: mode, interval: interval, grace: grace,
		suspects: map[string]int{},
	}
}

func envSeconds(name string, def time.Duration) time.Duration {
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name))); err == nil && v > 0 {
		return time.Duration(v) * time.Second
	}
	return def
}

// NewSinkWorkerReaperFromEnv returns nil unless SINK_WORKER_REAPER_MODE arms it and
// there is a database and an MCP manager to use.
func NewSinkWorkerReaperFromEnv(db *sql.DB, mcpManager *mcp.ServerManager) *SinkWorkerReaper {
	mode := parseSinkReaperMode(os.Getenv("SINK_WORKER_REAPER_MODE"))
	sinks := newSinkStopExecutor(mcpManager)
	if mode == sinkReaperOff || db == nil || sinks == nil {
		return nil
	}
	return newSinkWorkerReaper(db, sinks, mode,
		envSeconds("SINK_WORKER_REAPER_INTERVAL_SECONDS", defaultSinkReaperInterval),
		envSeconds("SINK_WORKER_REAPER_STOPPED_GRACE_SECONDS", defaultSinkReaperStoppedGrace))
}

// Mode names the reaper's mode for the startup log.
func (r *SinkWorkerReaper) Mode() string { return r.mode.String() }

// Run ticks until ctx ends.
func (r *SinkWorkerReaper) Run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tickCtx, cancel := context.WithTimeout(ctx, sinkReaperCallTimeout)
			r.Tick(tickCtx)
			cancel()
		}
	}
}

// Tick runs one sweep. A failed listing forgets every suspect, so the two sightings a
// stop needs are always two successful looks in a row.
func (r *SinkWorkerReaper) Tick(ctx context.Context) []sinkReapAction {
	if r == nil || r.mode == sinkReaperOff {
		return nil
	}
	workers, ok := listSinkWorkers(ctx, r.sinks)
	if !ok {
		r.suspects = map[string]int{}
		log.Debug("sink worker reaper: the sink service did not list its workers; nothing done")
		return nil
	}
	next := map[string]int{}
	var actions []sinkReapAction
	for _, w := range workers {
		reason := sinkWorkerOwnerGone(ctx, r.db, w.pipelineID, r.grace)
		if reason == "" {
			continue
		}
		seen := r.suspects[w.consumerGroup] + 1
		next[w.consumerGroup] = seen
		fields := log.Fields{
			"consumer_group": w.consumerGroup, "pipeline_id": w.pipelineID,
			"reason": reason, "sightings": seen, "mode": r.mode.String(),
		}
		if seen < sinkReaperSightings {
			log.WithFields(fields).Info("sink worker reaper: worker looks orphaned; waiting for a second look")
			continue
		}
		action := sinkReapAction{ConsumerGroup: w.consumerGroup, PipelineID: w.pipelineID, Reason: reason}
		if r.mode == sinkReaperDryRun {
			log.WithFields(fields).Warn("sink worker reaper (dry run): would stop this orphaned sink worker")
			actions = append(actions, action)
			continue
		}
		// Ask again right before acting: the pipeline may have been started since.
		if again := sinkWorkerOwnerGone(ctx, r.db, w.pipelineID, r.grace); again == "" {
			delete(next, w.consumerGroup)
			continue
		}
		if failure := stopSinkWorker(ctx, r.sinks, w.consumerGroup); failure != "" {
			action.Failure = failure
			log.WithFields(fields).WithField("failure", failure).
				Warn("sink worker reaper: could not stop the orphaned sink worker; will try again next tick")
		} else {
			action.Stopped = true
			delete(next, w.consumerGroup)
			log.WithFields(fields).Warn("sink worker reaper: stopped an orphaned sink worker")
		}
		actions = append(actions, action)
	}
	r.suspects = next
	return actions
}
