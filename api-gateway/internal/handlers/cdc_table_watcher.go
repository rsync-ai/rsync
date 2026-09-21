package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// CDCTableWatcher keeps a running CDC pipeline's table list current with the
// rule the user selected it by (see cdc_table_rule.go). A pipeline built as
// "replicate this whole database" used to mean "replicate the tables that
// existed the day it was built": every table created afterwards was missing
// with no error anywhere, because nothing was wrong — the include-list simply
// never mentioned it. The watcher re-expands the rule on a schedule and adds
// what appeared.
//
// What it deliberately does NOT do:
//
//   - It never removes a table. A table dropped at the source stays in the
//     include-list (Debezium ignores what is not there) — removing it would
//     silently stop replicating a table that a rename, a failover or a
//     mid-migration moment made invisible for one sweep.
//   - It never touches a pipeline with an exact-name selection. No rule, no
//     auto-add: choosing tables by name IS the instruction not to add more.
//   - It never adds a table the destination cannot accept. The orchestrator
//     owns that policy (a relational destination requires a PRIMARY KEY for
//     upsert/delete); the watcher asks, and records what it was refused, but
//     does not second-guess it. Creating the key is the user's call.
type CDCTableWatcher struct {
	db       *sql.DB
	interval time.Duration
	// maxTables caps one sweep's discovery, mirroring the interactive
	// resolver's ceiling so a pathological source cannot produce an unbounded
	// include-list from a background job nobody is watching.
	maxTables int

	// Seams. Production wires these to the same functions the interactive
	// "edit tables" endpoint uses — a background sweep must take the exact
	// path a human takes, or the two drift and only one of them is tested.
	connectorFor func(pipelineID string) (string, error)
	discover     func(ctx context.Context, connectionID, workspaceID, userID string, maxTables int) ([]TableMetadata, error)
	push         func(ctx context.Context, pipelineID string, tables []string) (int, []byte, error)
	backfill     func(ctx context.Context, pipelineID string, tables []string, mode string) gin.H
	restartSink  func(ctx context.Context, pipelineID string) gin.H
	// persistSelection mirrors the applied list onto the pipeline row;
	// persistSkipped records what a destination refused.
	persistSelection func(pipelineID string, tables []string) error
	persistSkipped   func(pipelineID string, tables []string) error
}

// cdcAutoPickupLockNamespace ("rSAP") scopes the per-pipeline advisory lock, so
// two api-gateway replicas sweeping at the same moment cannot both compute
// "these tables are new" from the same pre-update state and push conflicting
// include-lists.
const cdcAutoPickupLockNamespace = 0x72534150

// defaultCDCTableWatchInterval is a compromise between "a new table starts
// replicating promptly" and "we do not run a full schema discovery against
// every source every minute". Discovery is the expensive half, and it runs once
// per CDC pipeline per tick.
const defaultCDCTableWatchInterval = 5 * time.Minute

// minCDCTableWatchInterval floors the configured interval: below this, a source
// with many pipelines spends more time being discovered than being read.
const minCDCTableWatchInterval = time.Minute

// NewCDCTableWatcher builds the watcher from the environment, or returns nil
// when auto-pickup is disabled. Returning nil (rather than a disabled watcher)
// keeps the "off" state visible at the call site in main.
//
//	CDC_TABLE_AUTOPICKUP_ENABLED      default true  — "false"/"0" turns it off
//	CDC_TABLE_WATCH_INTERVAL_SECONDS  default 300   — floored at 60
func NewCDCTableWatcher(database *sql.DB) *CDCTableWatcher {
	if database == nil {
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(os.Getenv("CDC_TABLE_AUTOPICKUP_ENABLED"))) {
	case "false", "0", "no", "off":
		return nil
	}
	interval := defaultCDCTableWatchInterval
	if raw := strings.TrimSpace(os.Getenv("CDC_TABLE_WATCH_INTERVAL_SECONDS")); raw != "" {
		if secs, err := strconv.Atoi(raw); err == nil && secs > 0 {
			interval = time.Duration(secs) * time.Second
		}
	}
	if interval < minCDCTableWatchInterval {
		interval = minCDCTableWatchInterval
	}
	w := &CDCTableWatcher{
		db:           database,
		interval:     interval,
		maxTables:    selectAllMaxTables,
		connectorFor: findDebeziumConnectorName,
		discover:     discoverConnectionTables,
		push:         pushCDCTableList,
		backfill:     requestCDCBackfill,
		restartSink:  restartCDCSink,
	}
	w.persistSelection = func(pipelineID string, tables []string) error {
		return persistSelectedTables(database, pipelineID, tables)
	}
	w.persistSkipped = func(pipelineID string, tables []string) error {
		return recordAutoPickupSkipped(database, pipelineID, tables)
	}
	return w
}

// Start runs the sweep loop until ctx is cancelled.
func (w *CDCTableWatcher) Start(ctx context.Context) {
	if w == nil {
		return
	}
	go w.loop(ctx)
	log.Infof("cdc auto-pickup: watcher started (interval=%s)", w.interval)
}

func (w *CDCTableWatcher) loop(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	// The first sweep is delayed by one interval on purpose: at cold boot the
	// orchestrator and Kafka Connect are usually still coming up, and a sweep
	// that cannot reach them achieves nothing but a page of warnings.
	for {
		select {
		case <-ctx.Done():
			log.Info("cdc auto-pickup: watcher stopped")
			return
		case <-ticker.C:
			w.RunOnce(ctx)
		}
	}
}

// autoPickupPipeline is one candidate row: a running CDC pipeline that was
// selected by a rule.
type autoPickupPipeline struct {
	ID           string
	WorkspaceID  string
	UserID       string
	ConnectionID string
	Rule         []string
	Selected     []string
}

// RunOnce performs one sweep. Exported for tests and for an operator-triggered
// sweep; it is safe to call concurrently with the loop (the advisory lock makes
// a second sweep of the same pipeline a no-op).
func (w *CDCTableWatcher) RunOnce(ctx context.Context) {
	candidates, err := w.candidates(ctx)
	if err != nil {
		log.WithError(err).Warn("cdc auto-pickup: could not list pipelines")
		return
	}
	for _, p := range candidates {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := w.sweepPipeline(ctx, p); err != nil {
			log.WithError(err).WithField("pipeline_id", p.ID).Warn("cdc auto-pickup: sweep failed")
		}
	}
}

func (w *CDCTableWatcher) candidates(ctx context.Context) ([]autoPickupPipeline, error) {
	// `sync_mode = 'cdc' OR cdc_mode IS NOT NULL` is the repo-wide "is this
	// CDC?" predicate (see the sentinel agents): older rows carry only cdc_mode.
	rows, err := w.db.QueryContext(ctx, `
		SELECT p.id::text,
		       COALESCE(p.workspace_id::text, ''),
		       COALESCE(p.created_by::text, ''),
		       COALESCE(p.source_connection_id::text, ''),
		       COALESCE(p.config->'`+tableSelectionRuleKey+`', '[]'::jsonb)::text,
		       COALESCE(p.config->'selected_tables', '[]'::jsonb)::text
		FROM pipelines p
		WHERE p.status = 'running'
		  AND (p.sync_mode = 'cdc' OR p.cdc_mode IS NOT NULL)
		  AND p.source_connection_id IS NOT NULL
		  AND jsonb_typeof(p.config->'`+tableSelectionRuleKey+`') = 'array'
		  AND jsonb_array_length(p.config->'`+tableSelectionRuleKey+`') > 0
		ORDER BY p.updated_at ASC
		LIMIT 200
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []autoPickupPipeline
	for rows.Next() {
		var p autoPickupPipeline
		var ruleJSON, selectedJSON string
		if err := rows.Scan(&p.ID, &p.WorkspaceID, &p.UserID, &p.ConnectionID, &ruleJSON, &selectedJSON); err != nil {
			return nil, err
		}
		p.Rule = decodeJSONStringArray(ruleJSON)
		p.Selected = decodeJSONStringArray(selectedJSON)
		if len(p.Rule) == 0 || p.WorkspaceID == "" || p.ConnectionID == "" {
			continue
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// sweepPipeline brings one pipeline up to date. It is the whole feature in
// order: hold the lock, check there is a connector to update at all, re-expand
// the rule, diff, apply.
func (w *CDCTableWatcher) sweepPipeline(ctx context.Context, p autoPickupPipeline) error {
	release, acquired, err := acquireAutoPickupLock(ctx, w.db, p.ID)
	if err != nil {
		return err
	}
	if !acquired {
		// Another replica (or a still-running sweep) owns this pipeline.
		return nil
	}
	defer release()

	return w.sweepPipelineLocked(ctx, p)
}

// sweepPipelineLocked is the sweep itself, with the pipeline's advisory lock
// already held.
func (w *CDCTableWatcher) sweepPipelineLocked(ctx context.Context, p autoPickupPipeline) error {
	// A pipeline whose connector does not exist yet has nothing to update:
	// provisioning reads config.selected_tables, so the rule will be applied
	// when CDC is actually provisioned. An unreachable Kafka Connect is a real
	// failure and is reported, but it is not this pipeline's fault, so the
	// sweep simply retries next tick.
	if _, cerr := w.connectorFor(p.ID); cerr != nil {
		return nil //nolint:nilerr // both cases mean "nothing to do this tick"
	}

	// One sweep must not outlive several ticks: discovery alone can take ~90s
	// against a large source.
	sweepCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	desired, _, err := resolveSelectionSentinels(sweepCtx, p.ConnectionID, p.Rule,
		func(dctx context.Context, connectionID string, max int) ([]TableMetadata, error) {
			return w.discover(dctx, connectionID, p.WorkspaceID, p.UserID, max)
		})
	if err != nil {
		return fmt.Errorf("discovery failed: %w", err)
	}

	added := newlySeenTables(p.Selected, desired)
	if len(added) == 0 {
		return nil
	}

	return w.applyNewTables(sweepCtx, p, added)
}

// newlySeenTables returns the entries of `desired` that `selected` does not
// already contain, in discovery order. Matching is case-insensitive: the
// selection may have been typed by a user ("Public.Orders") while discovery
// reports the engine's own casing, and adding a table that is already being
// replicated would restart the connector for nothing, every tick, forever.
func newlySeenTables(selected, desired []string) []string {
	have := make(map[string]struct{}, len(selected))
	for _, s := range selected {
		if v := strings.TrimSpace(s); v != "" {
			have[strings.ToLower(v)] = struct{}{}
		}
	}
	out := []string{}
	for _, d := range desired {
		v := strings.TrimSpace(d)
		if v == "" {
			continue
		}
		k := strings.ToLower(v)
		if _, dup := have[k]; dup {
			continue
		}
		have[k] = struct{}{}
		out = append(out, v)
	}
	return out
}

// applyNewTables pushes selected+added, then snapshots and restarts the sink —
// the same three side-effects, in the same order, as the interactive editor.
func (w *CDCTableWatcher) applyNewTables(ctx context.Context, p autoPickupPipeline, added []string) error {
	union := append(append([]string(nil), p.Selected...), added...)

	status, body, err := w.push(ctx, p.ID, union)
	if err != nil {
		return fmt.Errorf("orchestrator unreachable: %w", err)
	}

	if status == 400 {
		// The orchestrator refused the list because some table has no PRIMARY
		// KEY and this destination needs one. Drop the offending NEW tables and
		// try once more, so one keyless table does not block every other table
		// the rule found.
		missing := cdcMissingPrimaryKeyTables(status, body)
		if len(missing) == 0 {
			return fmt.Errorf("orchestrator rejected the table list (status %d): %s", status, truncateForLog(body))
		}
		keep, skipped := splitByMissingPK(added, missing)
		if len(skipped) != len(missing) {
			// At least one table that is ALREADY being replicated now lacks a
			// PK. Retrying without it would silently stop replicating it, so
			// this needs a human: report and change nothing.
			return fmt.Errorf("a table already in the selection has no primary key (%s); CDC table list left unchanged", strings.Join(missing, ", "))
		}
		if err := w.persistSkipped(p.ID, skipped); err != nil {
			log.WithError(err).WithField("pipeline_id", p.ID).Warn("cdc auto-pickup: could not record skipped tables")
		}
		if len(keep) == 0 {
			log.WithFields(log.Fields{
				"pipeline_id": p.ID,
				"skipped":     len(skipped),
			}).Info("cdc auto-pickup: every new table lacks a primary key; nothing added")
			return nil
		}
		added = keep
		union = append(append([]string(nil), p.Selected...), added...)
		status, body, err = w.push(ctx, p.ID, union)
		if err != nil {
			return fmt.Errorf("orchestrator unreachable on retry: %w", err)
		}
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("orchestrator rejected the table list (status %d): %s", status, truncateForLog(body))
	}

	if err := w.persistSelection(p.ID, union); err != nil {
		// The include-list is already live; losing the mirror would make the
		// next sweep re-add the same tables (harmless but noisy) and a manual
		// edit start from a stale list (not harmless).
		log.WithError(err).WithField("pipeline_id", p.ID).Error("cdc auto-pickup: pushed the table list but failed to persist selected_tables")
	}

	// Newly added tables need their existing rows loaded, not just their future
	// changes streamed — the same "backfill_newly_added" the editor offers.
	bf := w.backfill(ctx, p.ID, added, "incremental")
	sr := w.restartSink(ctx, p.ID)

	log.WithFields(log.Fields{
		"pipeline_id":  p.ID,
		"added":        added,
		"backfill_ok":  bf["success"],
		"sink_restart": sr["success"],
	}).Info("📥 cdc auto-pickup: new source tables added to the running pipeline")
	return nil
}

// splitByMissingPK partitions `added` into the tables to keep and the tables
// the orchestrator named as missing a primary key. Names are compared
// case-insensitively; a name in `missing` that is not in `added` is not
// returned by either side, which is how the caller detects that an
// already-replicated table is the one at fault.
func splitByMissingPK(added, missing []string) (keep, skipped []string) {
	bad := make(map[string]struct{}, len(missing))
	for _, m := range missing {
		if v := strings.TrimSpace(m); v != "" {
			bad[strings.ToLower(v)] = struct{}{}
		}
	}
	for _, t := range added {
		if _, isBad := bad[strings.ToLower(strings.TrimSpace(t))]; isBad {
			skipped = append(skipped, t)
			continue
		}
		keep = append(keep, t)
	}
	return keep, skipped
}

// recordAutoPickupSkipped stores the tables auto-pickup could not add so the
// fact outlives the sweep: the user has to add a primary key before they can be
// replicated, and nobody reads worker logs to find that out.
func recordAutoPickupSkipped(database *sql.DB, pipelineID string, skipped []string) error {
	if database == nil || len(skipped) == 0 {
		return nil
	}
	b, err := json.Marshal(skipped)
	if err != nil {
		return err
	}
	_, err = database.Exec(`
		UPDATE pipelines
		SET config = jsonb_set(COALESCE(config, '{}'::jsonb), '{`+autoPickupSkippedKey+`}', $1::jsonb, true),
		    updated_at = NOW()
		WHERE id = $2::uuid
	`, string(b), pipelineID)
	return err
}

func truncateForLog(body []byte) string {
	s := strings.TrimSpace(string(body))
	if len(s) > 300 {
		return s[:300] + "…"
	}
	return s
}

// acquireAutoPickupLock takes a session-level advisory lock for one pipeline.
// It must be a dedicated *sql.Conn: an advisory lock belongs to the session
// that took it, and a pooled *sql.DB would hand the unlock to some other
// connection.
func acquireAutoPickupLock(ctx context.Context, database *sql.DB, pipelineID string) (release func(), acquired bool, err error) {
	conn, err := database.Conn(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("could not reserve a connection for the auto-pickup lock: %w", err)
	}
	key := int32(crc32.ChecksumIEEE([]byte(pipelineID)))

	var got bool
	if err := conn.QueryRowContext(ctx,
		`SELECT pg_try_advisory_lock($1, $2)`, int32(cdcAutoPickupLockNamespace), key).Scan(&got); err != nil {
		_ = conn.Close()
		return nil, false, fmt.Errorf("could not take the auto-pickup lock: %w", err)
	}
	if !got {
		_ = conn.Close()
		return nil, false, nil
	}
	return func() {
		relCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(relCtx,
			`SELECT pg_advisory_unlock($1, $2)`, int32(cdcAutoPickupLockNamespace), key); err != nil {
			log.WithError(err).WithField("pipeline_id", pipelineID).
				Error("cdc auto-pickup: failed to release the sweep lock; this pipeline is skipped until the connection is recycled")
		}
		_ = conn.Close()
	}, true, nil
}
