package workers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rsync-ai/backend-orchestrator/internal/cdc"
	log "github.com/sirupsen/logrus"
)

// CDCReconciler periodically reconciles the Debezium connectors registered in
// Kafka Connect against the pipelines table, and DELETES connectors that no
// longer have a live pipeline ("orphans"). This is the auto-clean mechanism for
// production.
//
// WHY THIS EXISTS: deleting/stopping a pipeline historically did NOT delete its
// Debezium connector (the cleanup path skipped it). Orphaned connectors keep a
// binlog reader / replication slot open on the source DB forever, eventually
// exhausting the source's replication capacity and stalling NEW pipelines
// (connector shows RUNNING but never snapshots — no topic, no rows). Three
// other cleanup mechanisms existed in the codebase but were all unwired dead
// code. This reconciler is the safety net that guarantees convergence even if a
// teardown call is missed (crash mid-delete, kafka-connect briefly down, etc.).
//
// SAFETY: pipelines are HARD-deleted (DELETE FROM pipelines), so "no pipeline
// row matches this connector" is an unambiguous orphan. The pipeline row is
// always created BEFORE its connector, so a freshly-provisioning pipeline can
// never be misread as an orphan. Connectors whose matching pipeline is in a
// terminal 'stopped' state are also reaped (StopPipeline should have removed
// them). Paused pipelines are intentionally left alone. Only connectors whose
// name matches the canonical "cdc-" prefix are ever touched.
type CDCReconciler struct {
	db         *sql.DB
	httpClient *http.Client
	connectURL string
	tickEvery  time.Duration
	stopCh     chan struct{}
}

// reapableStatuses are pipeline statuses whose connector should be torn down.
// (Absent pipeline row is handled separately and is always reapable.)
var cdcReapableStatuses = map[string]bool{
	"stopped": true,
}

func NewCDCReconciler(db *sql.DB) *CDCReconciler {
	tick := 5 * time.Minute
	if v := strings.TrimSpace(os.Getenv("CDC_RECONCILER_INTERVAL_SECONDS")); v != "" {
		if secs, err := time.ParseDuration(v + "s"); err == nil && secs >= 30*time.Second {
			tick = secs
		}
	}
	connectURL := strings.TrimSpace(os.Getenv("KAFKA_CONNECT_URL"))
	if connectURL == "" {
		connectURL = "http://kafka-connect:8083"
	}
	return &CDCReconciler{
		db:         db,
		httpClient: &http.Client{Timeout: 15 * time.Second},
		connectURL: strings.TrimRight(connectURL, "/"),
		tickEvery:  tick,
		stopCh:     make(chan struct{}),
	}
}

func (r *CDCReconciler) Start(ctx context.Context) {
	if r.db == nil {
		log.Warn("cdc_reconciler: missing db, not starting")
		return
	}
	log.Infof("cdc_reconciler: starting (tick every %s)", r.tickEvery)
	t := time.NewTicker(r.tickEvery)
	defer t.Stop()
	// Delay the first sweep so a just-booted stack isn't reaped mid-provisioning.
	select {
	case <-ctx.Done():
		return
	case <-r.stopCh:
		return
	case <-time.After(60 * time.Second):
	}
	r.sweep(ctx)
	r.reapSlots(ctx)
	r.reapPublications(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-t.C:
			r.sweep(ctx)
			r.reapSlots(ctx)
			r.reapPublications(ctx)
		}
	}
}

// reapSlots drops physical PostgreSQL replication slots whose owning pipeline is
// deleted or stopped — the safety net for the slot lifecycle. It runs every tick
// INDEPENDENTLY of the connector sweep above (which bails when kafka-connect is
// unreachable): a leaked slot retains WAL on the source regardless of connector
// state, so it must be reaped even when Kafka Connect is down. Idempotent; all
// errors logged, never fatal. Also auto-reaps pre-existing debris on first run.
func (r *CDCReconciler) reapSlots(ctx context.Context) {
	dropped, err := cdc.NewPostgreSQLManager(r.db).ReapOrphanedSlots(ctx)
	if err != nil {
		log.WithError(err).Debug("cdc_reconciler: slot reap query failed; skipping")
		return
	}
	if dropped > 0 {
		log.Infof("cdc_reconciler: reaped %d orphaned/stopped PostgreSQL replication slot(s)", dropped)
	}
}

// reapPublications drops physical PostgreSQL publications (debezium_pub_pipe_*)
// whose owning pipeline is deleted or stopped — the publication analogue of
// reapSlots (BUG-3). Runs every tick, independently of the connector sweep and the
// slot reaper. Idempotent (DROP PUBLICATION IF EXISTS); all errors logged, never
// fatal. Also auto-reaps pre-existing orphaned publications on first run.
func (r *CDCReconciler) reapPublications(ctx context.Context) {
	dropped, err := cdc.NewPostgreSQLManager(r.db).ReapOrphanedPublications(ctx)
	if err != nil {
		log.WithError(err).Debug("cdc_reconciler: publication reap query failed; skipping")
		return
	}
	if dropped > 0 {
		log.Infof("cdc_reconciler: reaped %d orphaned/stopped PostgreSQL publication(s)", dropped)
	}
}

func (r *CDCReconciler) Stop() {
	select {
	case <-r.stopCh:
	default:
		close(r.stopCh)
	}
}

// sweep lists connectors, classifies each against the pipelines table, and
// deletes orphans/stale connectors. All errors are logged, never fatal.
func (r *CDCReconciler) sweep(ctx context.Context) {
	names, err := r.listConnectors(ctx)
	if err != nil {
		log.WithError(err).Debug("cdc_reconciler: list connectors failed (kafka-connect down?); skipping sweep")
		return
	}
	if len(names) == 0 {
		return
	}

	// Build id8 -> status map from the pipelines table. If this query fails we
	// MUST NOT delete anything (could nuke live connectors), so bail out.
	live, err := r.pipelineStatusByID8(ctx)
	if err != nil {
		log.WithError(err).Warn("cdc_reconciler: pipelines query failed; skipping sweep (no deletes)")
		return
	}

	reaped := 0
	for _, name := range names {
		ln := strings.ToLower(strings.TrimSpace(name))
		if !strings.HasPrefix(ln, "cdc-") {
			continue // only manage our own CDC connectors
		}
		matched := false
		isLive := false
		for id8, status := range live {
			if id8 == "" {
				continue
			}
			if strings.Contains(ln, id8) {
				matched = true
				if !cdcReapableStatuses[status] {
					isLive = true
				}
			}
		}
		if matched && isLive {
			continue // a live pipeline owns this connector
		}
		// Orphan (no pipeline row) or stale (matching pipeline is 'stopped').
		reason := "orphan: no matching pipeline row"
		if matched {
			reason = "stale: matching pipeline is stopped"
		}
		if err := r.deleteConnector(ctx, name); err != nil {
			log.WithError(err).WithField("connector", name).Warn("cdc_reconciler: delete failed")
			continue
		}
		reaped++
		log.WithFields(log.Fields{"connector": name, "reason": reason}).
			Info("cdc_reconciler: reaped orphaned Debezium connector")
	}
	if reaped > 0 {
		log.Infof("cdc_reconciler: sweep complete, reaped %d connector(s) of %d", reaped, len(names))
	}
}

func (r *CDCReconciler) listConnectors(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.connectURL+"/connectors", nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("kafka connect status %d", resp.StatusCode)
	}
	var names []string
	if err := json.NewDecoder(resp.Body).Decode(&names); err != nil {
		return nil, err
	}
	return names, nil
}

// pipelineStatusByID8 returns a map of the first 8 lowercase hex chars of each
// pipeline id -> its status. Connector names embed this id8 (utils.SafeID8).
func (r *CDCReconciler) pipelineStatusByID8(ctx context.Context) (map[string]string, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT id::text, COALESCE(status, '') FROM pipelines`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var id, status string
		if err := rows.Scan(&id, &status); err != nil {
			return nil, err
		}
		id = strings.ToLower(strings.TrimSpace(id))
		if len(id) >= 8 {
			out[id[:8]] = strings.ToLower(strings.TrimSpace(status))
		}
	}
	return out, rows.Err()
}

func (r *CDCReconciler) deleteConnector(ctx context.Context, name string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, r.connectURL+"/connectors/"+name, nil)
	if err != nil {
		return err
	}
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete status %d", resp.StatusCode)
	}
	return nil
}
