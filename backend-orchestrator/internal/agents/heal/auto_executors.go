package heal

// auto_executors.go — Pillar 4: 3 additional Executor implementations that
// auto-resolve common failure modes the existing 4 executors can't handle.
//
// Today these classes of failure all escalate to the user:
//   - PostgreSQL replication slot/publication conflicts ("slot already exists")
//     — caused by a stale run that didn't clean up. Auto-fixable: drop the
//     stale slot and let BackoffRetry re-provision.
//   - Ownership row missing (OWN-EmptyAfterRun) — destination data landed
//     correctly but the _rsync_pipelines tracking row wasn't inserted.
//     Auto-fixable: re-insert with the known pipeline_id.
//   - Zombie executions — status='running' for >4h with no progress events,
//     usually a Temporal worker crash that left a stranded execution row.
//     Auto-fixable: mark as failed so the healer's main loop can reclassify
//     and retry-or-escalate appropriately.
//
// Each executor:
//   - Respects the 3/24h global retry cap (delegates to BackoffRetryExecutor
//     for the re-run side; the cleanup side is idempotent so re-runs are safe).
//   - Writes a pipeline_run_events row so the UI shows the auto-fix attempt.
//   - Returns nil on success AND on "already cleaned up" — idempotency
//     keeps retries cheap.
//
// Run's return value is not advisory. Heal turns nil into OutcomeAutoExecuted
// with ActionExecuted=true (heal.go), which is what lands on the pipeline
// timeline and in the attempt ledger ApplyMemory later reads. So nil means "the
// fix happened", and the two hook-driven executors below must return an error
// whenever it did not — when no hook is wired, and when the hook itself fails.
// Neither case can crash the healer: Heal captures the error into the result
// and the sweep continues.

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
	log "github.com/sirupsen/logrus"
)

// Action constants for the 3 new auto-heal executors.
//
// These are new values added to the diagnose.Action enum via this package
// rather than diagnose.go because they're only emitted by the healer's
// internal classifier — never by the rule-based diagnoser the executor
// pipeline consumes directly. Keeping them here makes ownership obvious.
const (
	ActionCleanupCDCResources diagnose.Action = "cleanup_cdc_resources"
	ActionRepairOwnershipRow  diagnose.Action = "repair_ownership_row"
	ActionSweepZombie         diagnose.Action = "sweep_zombie_execution"
)

// ── CleanupCDCResourcesExecutor ───────────────────────────────────────────
// Triggered when the diagnoser sees:
//   - "slot already exists"
//   - "publication already exists" with no active subscriber
//   - "replication slot is active" with no active subscriber
//
// Action: drop the stale slot/publication via the cdc package's Cleanup,
// then write a heal event so BackoffRetryExecutor (separate executor on
// the same diagnosis) re-runs the pipeline against the now-clean source.
//
// The healer chains these by emitting the diagnose.Signal back through
// the classifier, which on a freshly-cleaned source produces a fresh
// CategoryUnknown → ActionBackoffRetry (subject to the retry cap).

type CleanupCDCResourcesExecutor struct {
	DB *sql.DB
	// CleanupFn is optional. Production code injects cdc.PostgreSQLManager.CleanupResources
	// via this hook so the heal package doesn't import cdc (would cause an
	// import cycle).
	CleanupFn func(ctx context.Context, pipelineID string) error
}

func (e *CleanupCDCResourcesExecutor) Action() diagnose.Action { return ActionCleanupCDCResources }
func (e *CleanupCDCResourcesExecutor) HITLSafe() bool          { return true }

func (e *CleanupCDCResourcesExecutor) Run(ctx context.Context, sig diagnose.Signal) error {
	if sig.PipelineID == "" {
		return fmt.Errorf("CleanupCDCResourcesExecutor: PipelineID is required")
	}
	if e.CleanupFn == nil {
		// No hook wired, so no cleanup happened. Returning nil here would tell
		// Heal the action succeeded — see the note above Run.
		log.WithFields(log.Fields{
			"pipeline_id": sig.PipelineID,
		}).Warn("healer: CleanupCDCResources — no CleanupFn wired")
		writeHealEvent(ctx, e.DB, sig, "healer_cleanup_cdc_skipped",
			"Healer wanted to clean CDC resources but no cleanup hook was wired")
		return fmt.Errorf("CleanupCDCResourcesExecutor: no CleanupFn wired; " +
			"nothing was cleaned up")
	}

	if err := e.CleanupFn(ctx, sig.PipelineID); err != nil {
		// The heal event stays — it is the operator-facing narration. But the
		// error also has to reach the caller, or the ledger records a fix for a
		// cleanup the hook just told us did not work.
		writeHealEvent(ctx, e.DB, sig, "healer_cleanup_cdc_failed",
			fmt.Sprintf("Healer attempted CDC resource cleanup but failed: %v", err))
		log.WithError(err).WithField("pipeline_id", sig.PipelineID).
			Warn("healer: CleanupCDCResources — cleanup hook failed")
		return fmt.Errorf("CleanupCDCResourcesExecutor: cleanup failed: %w", err)
	}

	writeHealEvent(ctx, e.DB, sig, "healer_cleanup_cdc_resources",
		"Healer dropped stale PostgreSQL replication slot/publication; pipeline can be safely retried")
	log.WithField("pipeline_id", sig.PipelineID).
		Info("healer: CleanupCDCResources — stale resources dropped, pipeline ready for retry")
	return nil
}

// ── RepairOwnershipRowExecutor ────────────────────────────────────────────
// Triggered when the OWN-EmptyAfterRun pattern is detected: pipeline run
// completed successfully (destination tables have data), but the
// _rsync_pipelines ownership row never landed (so reload safety gating
// + cross-pipeline drop guards are blind).
//
// Action: re-insert the ownership row via the destination connector's
// ensure_table MCP call with explicit pipeline_id. This is idempotent
// — the connector's ensure_table uses ON CONFLICT DO NOTHING.

type RepairOwnershipRowExecutor struct {
	DB *sql.DB
	// RepairFn is the production-injected callback that knows how to call
	// the destination connector's ensure_table with pipeline_id.
	// Signature deliberately minimal: (pipelineID) → error. Implementation
	// fetches destination connection config + table list internally.
	RepairFn func(ctx context.Context, pipelineID string) error
}

func (e *RepairOwnershipRowExecutor) Action() diagnose.Action { return ActionRepairOwnershipRow }
func (e *RepairOwnershipRowExecutor) HITLSafe() bool          { return true }

func (e *RepairOwnershipRowExecutor) Run(ctx context.Context, sig diagnose.Signal) error {
	if sig.PipelineID == "" {
		return fmt.Errorf("RepairOwnershipRowExecutor: PipelineID is required")
	}
	if e.RepairFn == nil {
		log.WithField("pipeline_id", sig.PipelineID).
			Warn("healer: RepairOwnershipRow — no RepairFn wired")
		writeHealEvent(ctx, e.DB, sig, "healer_repair_ownership_skipped",
			"Healer wanted to repair ownership row but no repair hook was wired")
		return fmt.Errorf("RepairOwnershipRowExecutor: no RepairFn wired; " +
			"the ownership row was not repaired")
	}

	if err := e.RepairFn(ctx, sig.PipelineID); err != nil {
		writeHealEvent(ctx, e.DB, sig, "healer_repair_ownership_failed",
			fmt.Sprintf("Healer attempted ownership row repair but failed: %v", err))
		log.WithError(err).WithField("pipeline_id", sig.PipelineID).
			Warn("healer: RepairOwnershipRow — repair hook failed")
		return fmt.Errorf("RepairOwnershipRowExecutor: repair failed: %w", err)
	}

	writeHealEvent(ctx, e.DB, sig, "healer_repair_ownership_row",
		"Healer re-inserted the missing _rsync_pipelines ownership row for this pipeline")
	log.WithField("pipeline_id", sig.PipelineID).
		Info("healer: RepairOwnershipRow — ownership repaired")
	return nil
}

// ── ZombieExecutionSweeper ────────────────────────────────────────────────
// Periodic sweeper (called by HealWorker, not triggered by an inbound
// failure signal). Looks for executions stuck at status='running' past
// ZombieAge and marks them failed so the main heal loop can reclassify with
// a real diagnosis.
//
// It does NOT own a zombie statement. It used to — a hand-written near-copy
// of worker.go's sweepZombiesQuery that lacked the pipelines_closed CTE, so
// every row this hourly ticker won before the 60s sweep saw it had its
// execution closed and its pipelines row left at status='running' forever
// (KI-3, reintroduced by the copy that was never taught about the fix). Both
// callers now run runZombieSweep. Do not reintroduce a second statement here.
//
// Default ZombieAge = ZombieTimeout (4h).

type ZombieExecutionSweeper struct {
	DB        *sql.DB
	ZombieAge time.Duration // defaults to ZombieTimeout when zero
}

func (e *ZombieExecutionSweeper) Action() diagnose.Action { return ActionSweepZombie }
func (e *ZombieExecutionSweeper) HITLSafe() bool          { return true }

// Run is technically the Executor interface method but ZombieSweeper is
// driven by a separate ticker (see worker.go RunZombieSweep), not by an
// inbound failure signal. The Run method is implemented so the type
// satisfies Executor for symmetry, but it's a no-op when called from
// the normal heal-on-failure path.
func (e *ZombieExecutionSweeper) Run(ctx context.Context, sig diagnose.Signal) error {
	// When called via the normal heal path, just sweep so any specific
	// pipeline that triggered this is also covered.
	return e.Sweep(ctx)
}

// Sweep scans for zombie executions and marks them failed, reconciling the
// pipeline_progress and (for non-CDC) pipelines rows with them, and writing a
// heal event per reap. Safe to call concurrently with HealWorker's own 60s
// sweep — they run the SAME statement, and its UPDATE guards on
// status='running' so two passes cannot double-reap a row.
func (e *ZombieExecutionSweeper) Sweep(ctx context.Context) error {
	if e.DB == nil {
		return fmt.Errorf("ZombieExecutionSweeper: DB is required")
	}
	return runZombieSweep(ctx, e.DB, e.ZombieAge)
}
