package sentinel

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rsync-ai/backend-orchestrator/internal/cdc"
	"github.com/rsync-ai/backend-orchestrator/internal/connections"
	log "github.com/sirupsen/logrus"
)

// ============================================================================
// WAL watchdog (Part B)
//
// PROBLEM: a PostgreSQL replication slot forces the source server to retain all
// WAL since the slot's restart_lsn. If a slot stops being consumed — the owning
// pipeline was STOPPED (but not deleted) or its Debezium connector stalled — the
// retained WAL grows without bound and eventually fills the source's disk,
// taking down not just CDC but the entire source database. Azure/RDS managed
// Postgres are especially unforgiving here (fixed disk, hard to grow live).
//
// The Part A reconciler reaps slots for stopped/deleted pipelines on a fixed 5m
// cadence regardless of WAL pressure. The watchdog is the WAL-pressure-driven
// safety net layered on top: it measures retained WAL directly and (a) ALERTS in
// two tiers so an operator sees the problem early, and (b) for a stopped/deleted
// pipeline whose slot has crossed the CRITICAL threshold, it auto-drops the slot
// immediately rather than waiting for the next reconciler tick.
//
// SAFETY: the watchdog NEVER drops a slot whose pipeline is 'running' or 'paused'
// — dropping a live/anchored slot would lose the CDC position and break the
// pipeline. For those it only alerts. Auto-drop is reserved for slots that are
// already eligible for reaping (pipeline stopped or deleted), which is exactly
// the policy chosen for this work: "Alert + auto-drop slot" for stopped pipelines.
// ============================================================================

func walDurationFromEnv(name string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	if d, err := time.ParseDuration(v); err == nil && d >= 30*time.Second {
		return d
	}
	return def
}

func walBytesFromEnv(name string, def int64) int64 {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
		return n
	}
	return def
}

func walBoolFromEnv(name string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return def
	}
}

// walWatchdogLoop periodically inspects retained WAL per replication slot.
func (s *CDCSentinel) walWatchdogLoop() {
	// Sanity: critical must be >= warn, else the warn tier would never resolve.
	if s.walCriticalBytes < s.walWarnBytes {
		s.walCriticalBytes = s.walWarnBytes
	}
	log.WithFields(log.Fields{
		"interval":    s.walWatchdogInterval.String(),
		"warn_mb":     s.walWarnBytes / (1024 * 1024),
		"critical_mb": s.walCriticalBytes / (1024 * 1024),
		"auto_act":    s.walAutoAct,
	}).Info("🛡️ CDC WAL watchdog started (retained-WAL monitor + auto-drop of stopped slots)")

	ticker := time.NewTicker(s.walWatchdogInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.checkWALRetention(s.ctx)
		}
	}
}

// walSlotRow is one PostgreSQL replication slot resource joined to its (possibly
// absent) owning pipeline.
type walSlotRow struct {
	slotName      string
	connectionID  string
	pipelineID    sql.NullString
	pipelineState string // '' when the pipeline was deleted (LEFT JOIN miss)
	pipelineName  string
}

// checkWALRetention inspects retained WAL for every tracked PostgreSQL
// replication slot and applies the two-tier alert/action policy.
func (s *CDCSentinel) checkWALRetention(ctx context.Context) {
	defer recoverTick("wal_watchdog")
	rows, err := s.db.QueryContext(ctx, `
		SELECT cr.resource_name, cr.connection_id, cr.pipeline_id,
		       COALESCE(p.status, ''), COALESCE(p.name, '')
		FROM cdc_resources cr
		LEFT JOIN pipelines p ON p.id = cr.pipeline_id
		WHERE cr.resource_type = 'replication_slot'
		  AND cr.database_type = 'postgresql'
		  AND cr.status IN ('active', 'inactive', 'failed', 'orphaned')
	`)
	if err != nil {
		log.WithError(err).Debug("🛡️ WAL watchdog: failed to query slots; skipping")
		return
	}
	defer rows.Close()

	var slots []walSlotRow
	for rows.Next() {
		var r walSlotRow
		if err := rows.Scan(&r.slotName, &r.connectionID, &r.pipelineID, &r.pipelineState, &r.pipelineName); err != nil {
			continue
		}
		slots = append(slots, r)
	}
	if len(slots) == 0 {
		return
	}

	// Cache one source connection per connection_id for this sweep.
	mgr := connections.NewManager(s.db)
	conns := make(map[string]*sql.DB)
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()

	needReap := false           // set true if any reapable slot crossed CRITICAL
	var criticalReaped []string // slot names auto-dropped this sweep (for alert cleanup)
	for _, sl := range slots {
		srcDB := conns[sl.connectionID]
		if srcDB == nil {
			db, derr := s.openSourcePG(ctx, mgr, sl.connectionID)
			if derr != nil {
				log.WithError(derr).WithField("connection_id", sl.connectionID).
					Debug("🛡️ WAL watchdog: cannot open source; skipping its slots")
				conns[sl.connectionID] = nil // remember the failure (nil) so we don't retry every slot
				continue
			}
			conns[sl.connectionID] = db
			srcDB = db
		}

		var retained int64
		var active bool
		qerr := srcDB.QueryRowContext(ctx,
			`SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0), active
			 FROM pg_replication_slots WHERE slot_name = $1`, sl.slotName).Scan(&retained, &active)
		if qerr != nil {
			// No row → the physical slot is already gone (dropped). Nothing to watch.
			s.resolveWALAlert(ctx, sl.slotName)
			continue
		}

		reapable := !sl.pipelineID.Valid || sl.pipelineState == "" || sl.pipelineState == "stopped"
		retainedMB := float64(retained) / (1024 * 1024)

		switch {
		case retained >= s.walCriticalBytes:
			s.emitWALAlert(ctx, sl, retained, active, reapable, IssueSeverityCritical)
			if reapable && s.walAutoAct {
				needReap = true // defer the actual drop to one ReapOrphanedSlots call below
				criticalReaped = append(criticalReaped, sl.slotName)
			}
		case retained >= s.walWarnBytes:
			s.emitWALAlert(ctx, sl, retained, active, reapable, IssueSeverityWarning)
		default:
			// Below warn → healthy, clear any stale alert for this slot.
			s.resolveWALAlert(ctx, sl.slotName)
			log.WithFields(log.Fields{
				"slot":        sl.slotName,
				"retained_mb": fmt.Sprintf("%.1f", retainedMB),
			}).Debug("🛡️ WAL watchdog: slot healthy")
		}
	}

	// One drop pass for all reapable slots that crossed CRITICAL. ReapOrphanedSlots
	// is idempotent and only ever touches stopped/deleted slots (never running/
	// paused), so this can never drop a live slot. Reuses the Part A reaper.
	if needReap && s.walAutoAct {
		dropped, derr := cdc.NewPostgreSQLManager(s.db).ReapOrphanedSlots(ctx)
		if derr != nil {
			log.WithError(derr).Warn("🛡️ WAL watchdog: auto-drop of critical stopped slots failed")
		} else {
			if dropped > 0 {
				log.Warnf("🛡️ WAL watchdog: auto-dropped %d critical orphaned/stopped replication slot(s) under WAL pressure", dropped)
			}
			// Clear the active-issue rows for the slots we just dropped. Once a slot
			// resource flips to 'deleted' it is no longer selected by this loop, so a
			// later sweep would never resolve its alert — do it here.
			for _, slotName := range criticalReaped {
				s.resolveWALAlert(ctx, slotName)
			}
		}
	}
}

// openSourcePG opens a short-lived connection to a source PostgreSQL using the
// stored connection config, with host-aware SSL (managed sources require TLS).
func (s *CDCSentinel) openSourcePG(ctx context.Context, mgr *connections.Manager, connectionID string) (*sql.DB, error) {
	cfg, err := mgr.Get(ctx, connectionID)
	if err != nil {
		return nil, err
	}
	host := cfg["host"]
	port := 5432
	if p := strings.TrimSpace(cfg["port"]); p != "" {
		if n, err2 := fmt.Sscanf(p, "%d", &port); n == 0 || err2 != nil {
			port = 5432
		}
	}
	database := cfg["database"]
	if database == "" {
		database = cfg["db_name"]
	}
	sslmode := cdc.ResolvePostgresSSLMode(
		map[string]interface{}{"sslmode": cfg["sslmode"], "ssl_mode": cfg["ssl_mode"]}, host)

	connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s connect_timeout=5",
		host, port, database, cfg["user"], cfg["password"], sslmode)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

// emitWALAlert persists a WAL-pressure issue (keyed by slot so deleted pipelines
// are still covered) and emits a SENTINEL_ALERT domain event + a user
// notification on rsync.notifications.
func (s *CDCSentinel) emitWALAlert(ctx context.Context, sl walSlotRow, retained int64, active, reapable bool, sev IssueSeverity) {
	retainedMB := float64(retained) / (1024 * 1024)
	pipelineID := ""
	if sl.pipelineID.Valid {
		pipelineID = sl.pipelineID.String
	}

	disposition := "running"
	switch {
	case !sl.pipelineID.Valid || sl.pipelineState == "":
		disposition = "deleted"
	case sl.pipelineState == "stopped":
		disposition = "stopped"
	case sl.pipelineState == "paused":
		disposition = "paused"
	}

	action := "none"
	if sev == IssueSeverityCritical && reapable && s.walAutoAct {
		action = "auto_drop_slot"
	}

	description := fmt.Sprintf(
		"Replication slot %s is retaining %.1f MB of WAL on the source (pipeline %s, consumer active=%t). "+
			"Unbounded WAL retention can exhaust source disk.",
		sl.slotName, retainedMB, disposition, active)
	if action == "auto_drop_slot" {
		description += " Auto-dropping the slot (pipeline is not running)."
	} else if disposition == "running" || disposition == "paused" {
		description += " Pipeline is live — slot will NOT be dropped automatically; investigate the connector."
	}

	metadata := map[string]interface{}{
		"slot_name":      sl.slotName,
		"connection_id":  sl.connectionID,
		"retained_bytes": retained,
		"retained_mb":    retainedMB,
		"slot_active":    active,
		"disposition":    disposition,
		"action":         action,
		"warn_bytes":     s.walWarnBytes,
		"critical_bytes": s.walCriticalBytes,
	}
	metaJSON, _ := json.Marshal(metadata)

	// Persist to sentinel_active_issues (keyed by slot name).
	issueID := fmt.Sprintf("cdc-wal-%s", sl.slotName)
	componentID := pipelineID
	if componentID == "" {
		componentID = sl.slotName
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO sentinel_active_issues (
			id, type, severity, component_id, component_type,
			description, detected_at, occurrence_count, last_occurrence, metadata
		) VALUES ($1, $2, $3, $4, $5, $6, NOW(), 1, NOW(), $7)
		ON CONFLICT (id) DO UPDATE SET
			occurrence_count = sentinel_active_issues.occurrence_count + 1,
			last_occurrence  = NOW(),
			severity         = EXCLUDED.severity,
			description      = EXCLUDED.description,
			metadata         = EXCLUDED.metadata
	`, issueID, string(IssueTypeHighLag), string(sev),
		componentID, string(ComponentTypeCDCPipeline), description, metaJSON); err != nil {
		log.WithError(err).WithField("slot", sl.slotName).Warn("🛡️ WAL watchdog: failed to persist issue")
	}

	if s.kafkaManager == nil {
		return
	}

	status := "warning"
	if sev == IssueSeverityCritical {
		status = "critical"
	}

	// Domain event for the pipeline overview UI (only when we have a pipeline).
	if pipelineID != "" {
		event := map[string]interface{}{
			"schema_version": 2,
			"event_type":     "SENTINEL_ALERT",
			"pipeline_id":    pipelineID,
			"execution_id":   "",
			"trace_id":       pipelineID,
			"timestamp":      time.Now().UTC().Format(time.RFC3339),
			"stage":          "cdc_sentinel",
			"stage_group":    "monitoring",
			"status":         status,
			"message":        description,
			"metadata": map[string]interface{}{
				"source":     "cdc_wal_watchdog",
				"alert_type": "wal_retention",
				"details":    metadata,
			},
		}
		if b, err := json.Marshal(event); err == nil {
			_ = s.kafkaManager.ProduceWithHeaders("pipeline.domain.events", []byte(pipelineID), b,
				map[string]string{"trace_id": pipelineID})
		}
	}

	// User notification (Slack/email fan-out happens off rsync.notifications).
	notification := map[string]interface{}{
		"type":        "cdc_wal_pressure",
		"pipeline_id": pipelineID,
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"action_url":  notificationActionURL(pipelineID),
		"message":     description,
		"severity":    status,
	}
	if b, err := json.Marshal(notification); err == nil {
		_ = s.kafkaManager.Produce("rsync.notifications", []byte(sl.slotName), b)
	}

	log.WithFields(log.Fields{
		"slot":        sl.slotName,
		"pipeline_id": pipelineID,
		"retained_mb": fmt.Sprintf("%.1f", retainedMB),
		"severity":    status,
		"action":      action,
	}).Warn("🛡️ WAL watchdog: retained-WAL alert emitted")
}

// resolveWALAlert clears a WAL-pressure issue once the slot is healthy or gone.
func (s *CDCSentinel) resolveWALAlert(ctx context.Context, slotName string) {
	issueID := fmt.Sprintf("cdc-wal-%s", slotName)
	if _, err := s.db.ExecContext(ctx, `DELETE FROM sentinel_active_issues WHERE id = $1`, issueID); err != nil {
		log.WithError(err).WithField("slot", slotName).Debug("🛡️ WAL watchdog: resolve delete failed (may not exist)")
	}
}

func notificationActionURL(pipelineID string) string {
	if pipelineID == "" {
		return "/pipelines"
	}
	return "/pipelines/" + pipelineID
}
