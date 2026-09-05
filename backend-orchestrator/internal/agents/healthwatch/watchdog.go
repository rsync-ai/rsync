// Package healthwatch — Connector Health Watchdog (Pillar 5).
//
// Pattern stolen from Fivetran's internal connector-ops dashboards. The
// watchdog computes per-(connector_type, version) success rate over a
// rolling 24h window and alerts ops when a version's rate drops more
// than 20% below its predecessor.
//
// Without this signal, regressions only surface when a customer files a
// support ticket. With it, ops are paged within an hour of the regression
// — typically before users notice.
//
// The watchdog runs as a goroutine started by the orchestrator. It:
//   1. Wakes every hour
//   2. Computes a fresh rollup from `executions` → `connector_version_health`
//   3. Flags any (type, version) whose success_rate is >RegressionThreshold
//      below the previous version (with at least MinSampleSize executions)
//   4. Emits a notification via the existing rsync.notifications topic
//      (consumed by api-gateway's notifier service) when a NEW regression
//      is detected (dedup'd by the notifier so flapping versions don't spam)
package healthwatch

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/execrows"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// Watchdog runs the periodic health rollup + anomaly detection.
type Watchdog struct {
	DB *sql.DB
	// KafkaManager is optional; if nil, watchdog skips notification emission
	// (rollup still happens). Used for delivering regression alerts via the
	// rsync.notifications topic.
	KafkaManager *kafka.Manager

	// PollInterval — how often to refresh the rollup. Default 1h.
	PollInterval time.Duration

	// RegressionThreshold — minimum drop (in percentage points) below the
	// previous version's success rate to flag as regressed. Default 0.20
	// (20 percentage points). Tune higher if you see false positives.
	RegressionThreshold float64

	// MinSampleSize — minimum 24h executions required for a (type, version)
	// to be eligible for regression scoring. Default 10. Prevents false
	// positives from new versions with 2-3 runs.
	MinSampleSize int
}

const (
	// notifyTopic is the same topic the healer + sentinel publish to.
	// The api-gateway notifier service consumes from it and delivers via
	// email + Slack.
	notifyTopic = "rsync.notifications"
)

// New returns a Watchdog with sane defaults applied. Caller still needs
// to populate DB and (optionally) KafkaManager.
func New(db *sql.DB, km *kafka.Manager) *Watchdog {
	return &Watchdog{
		DB:                  db,
		KafkaManager:        km,
		PollInterval:        1 * time.Hour,
		RegressionThreshold: 0.20,
		MinSampleSize:       10,
	}
}

// Start runs the rollup loop until ctx is cancelled. Call in a goroutine
// from the orchestrator's main. Runs one rollup immediately on startup so
// the connector_version_health table has data right away.
func (w *Watchdog) Start(ctx context.Context) {
	log.Info("healthwatch: Watchdog started")
	if err := w.Run(ctx); err != nil {
		log.WithError(err).Warn("healthwatch: initial rollup failed")
	}
	t := time.NewTicker(w.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("healthwatch: Watchdog stopping")
			return
		case <-t.C:
			if err := w.Run(ctx); err != nil {
				log.WithError(err).Warn("healthwatch: rollup failed")
			}
		}
	}
}

// Run performs one rollup cycle: compute per-(type, version) success rates,
// flag regressions, upsert into connector_version_health, emit notifications
// for any NEW regressions detected.
//
// Safe to call manually (e.g. from an admin /refresh endpoint) — the
// computation is idempotent on the per-(type, version) row.
func (w *Watchdog) Run(ctx context.Context) error {
	if w.DB == nil {
		return fmt.Errorf("healthwatch: DB is nil")
	}

	rows, err := w.queryRollup(ctx)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		log.Debug("healthwatch: no executions in last 24h, skipping rollup")
		return nil
	}

	// Sort by (connector_type, version) so the regression comparison can
	// look at adjacent rows. Versions are semver-shaped ("1.0.5") so
	// lexicographic sort works for single-digit minor/patch. Production
	// can upgrade to proper semver sorting if needed.
	byType := groupByConnectorType(rows)

	newRegressions := []regression{}
	for connType, versions := range byType {
		// versions is sorted ascending; the LAST entry is the newest.
		for i := 1; i < len(versions); i++ {
			cur := &versions[i]
			prev := versions[i-1]
			cur.PrevVersion = prev.Version
			cur.PrevSuccessRate = prev.SuccessRate

			total := cur.SuccessCount + cur.FailureCount
			drop := prev.SuccessRate - cur.SuccessRate
			if total >= w.MinSampleSize && drop > w.RegressionThreshold {
				cur.Regressed = true
				newReg := regression{
					ConnectorType:     connType,
					Version:           cur.Version,
					PrevVersion:       prev.Version,
					SuccessRate:       cur.SuccessRate,
					PrevSuccessRate:   prev.SuccessRate,
					DropPP:            drop * 100,
					SampleSize:        total,
				}
				newRegressions = append(newRegressions, newReg)
			}
		}
		// Write back the mutated versions slice.
		byType[connType] = versions
	}

	if err := w.upsertRollup(ctx, byType); err != nil {
		return err
	}

	// Emit one notification per new regression. The notifier service
	// dedups by (pipeline_id, code, action_url) so flapping versions
	// don't spam ops.
	for _, r := range newRegressions {
		w.emitRegressionAlert(r)
	}

	log.WithFields(log.Fields{
		"connector_types": len(byType),
		"regressions":     len(newRegressions),
	}).Info("healthwatch: rollup complete")
	return nil
}

// healthRow mirrors one row in the rollup result.
type healthRow struct {
	ConnectorType     string
	Version           string
	SuccessCount      int
	FailureCount      int
	SuccessRate       float64
	PrevVersion       string
	PrevSuccessRate   float64
	Regressed         bool
}

type regression struct {
	ConnectorType   string
	Version         string
	PrevVersion     string
	SuccessRate     float64
	PrevSuccessRate float64
	DropPP          float64 // drop in percentage points
	SampleSize      int
}

// queryRollup computes per-(connector_type, version) success rates over
// the last 24h from `executions`. Returns rows sorted by (type, version asc).
//
// Joins with `connections` to get connector_type + version; tolerates
// missing version (some legacy rows have NULL — we treat them as "unknown"
// and lump them together).
func (w *Watchdog) queryRollup(ctx context.Context) ([]healthRow, error) {
	q := `
		WITH exec_24h AS (
			SELECT
				COALESCE(NULLIF(c.connector_type, ''), 'unknown')                                AS connector_type,
				COALESCE(NULLIF(c.connector_version, ''), 'unknown')                             AS version,
				CASE WHEN e.status IN ('completed', 'success') THEN 1 ELSE 0 END                 AS is_success,
				CASE WHEN e.status IN ('failed', 'error')      THEN 1 ELSE 0 END                 AS is_failure
			FROM executions e
			LEFT JOIN pipelines p ON p.id = e.pipeline_id
			LEFT JOIN connections c ON c.id = p.source_connection_id
			WHERE e.start_time > NOW() - INTERVAL '24 hours'
			  AND e.status IN ('completed', 'success', 'failed', 'error')
			  -- A reaped CDC audit anchor is not a connector failure. See execrows.
			  AND ` + execrows.NotSynthetic + `
		)
		SELECT
			connector_type,
			version,
			SUM(is_success)::int AS success_count,
			SUM(is_failure)::int AS failure_count
		FROM exec_24h
		GROUP BY connector_type, version
		ORDER BY connector_type ASC, version ASC`
	rows, err := w.DB.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("rollup query: %w", err)
	}
	defer rows.Close()
	out := []healthRow{}
	for rows.Next() {
		var r healthRow
		if err := rows.Scan(&r.ConnectorType, &r.Version, &r.SuccessCount, &r.FailureCount); err != nil {
			return nil, fmt.Errorf("rollup scan: %w", err)
		}
		total := r.SuccessCount + r.FailureCount
		if total > 0 {
			r.SuccessRate = float64(r.SuccessCount) / float64(total)
		} else {
			r.SuccessRate = 1.0 // no executions = no data = optimistic default
		}
		out = append(out, r)
	}
	return out, nil
}

func groupByConnectorType(rows []healthRow) map[string][]healthRow {
	out := make(map[string][]healthRow)
	for _, r := range rows {
		out[r.ConnectorType] = append(out[r.ConnectorType], r)
	}
	return out
}

// upsertRollup writes the rollup result into connector_version_health,
// REPLACING the existing row for each (type, version). The unique index
// on (connector_type, version) backs the ON CONFLICT.
func (w *Watchdog) upsertRollup(ctx context.Context, byType map[string][]healthRow) error {
	now := time.Now().UTC()
	for _, versions := range byType {
		for _, r := range versions {
			_, err := w.DB.ExecContext(ctx, `
				INSERT INTO connector_version_health
					(connector_type, version, success_count_24h, failure_count_24h,
					 success_rate, prev_version, prev_success_rate, regressed, computed_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
				ON CONFLICT (connector_type, version) DO UPDATE SET
					success_count_24h = EXCLUDED.success_count_24h,
					failure_count_24h = EXCLUDED.failure_count_24h,
					success_rate      = EXCLUDED.success_rate,
					prev_version      = EXCLUDED.prev_version,
					prev_success_rate = EXCLUDED.prev_success_rate,
					regressed         = EXCLUDED.regressed,
					computed_at       = EXCLUDED.computed_at`,
				r.ConnectorType, r.Version, r.SuccessCount, r.FailureCount,
				r.SuccessRate, nullable(r.PrevVersion), nullableFloat(r.PrevSuccessRate),
				r.Regressed, now,
			)
			if err != nil {
				return fmt.Errorf("upsert (%s,%s): %w", r.ConnectorType, r.Version, err)
			}
		}
	}
	return nil
}

// emitRegressionAlert publishes a StructuredError notification on
// rsync.notifications so the existing notifier service delivers it to
// ops via Slack + email. Uses a synthetic pipeline_id of "system" and
// the new RSYNC_CONNECTOR_VERSION_REGRESSION code so the user sees a
// stable identifier.
func (w *Watchdog) emitRegressionAlert(r regression) {
	if w.KafkaManager == nil {
		log.WithFields(log.Fields{
			"connector_type": r.ConnectorType,
			"version":        r.Version,
			"drop_pp":        r.DropPP,
		}).Warn("healthwatch: regression detected (no KafkaManager wired, alert dropped)")
		return
	}

	se := diagnose.NewStructuredError(
		"RSYNC_CONNECTOR_VERSION_REGRESSION",
		diagnose.FailureTypeSystemError,
		diagnose.AudienceOperator,
		fmt.Sprintf(
			"Connector %s v%s success rate dropped %.1f percentage points below previous v%s (%.0f%% → %.0f%% over last 24h, %d executions). Likely a bad template release.",
			r.ConnectorType, r.Version, r.DropPP, r.PrevVersion,
			r.PrevSuccessRate*100, r.SuccessRate*100, r.SampleSize,
		),
	)
	se.Severity = diagnose.SeverityWarning
	se.Remediation = &diagnose.Remediation{
		Steps: []string{
			fmt.Sprintf("Review recent commits to the %s template / connector code", r.ConnectorType),
			fmt.Sprintf("Inspect failed executions for %s v%s: `SELECT * FROM executions WHERE status='failed' AND connection_id IN (SELECT id FROM connections WHERE connector_type='%s' AND connector_version='%s') ORDER BY start_time DESC LIMIT 20`", r.ConnectorType, r.Version, r.ConnectorType, r.Version),
			"Consider rolling back to the previous version via the connector version manifest",
		},
		DocURL: diagnose.ErrorDocURL("connector-version-regressions"),
	}

	notification := map[string]interface{}{
		"type":        "structured_error_notification",
		"pipeline_id": "system", // synthetic — this is an ops-level alert
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"action_url":  fmt.Sprintf("/admin/health/connector-versions/%s/%s", r.ConnectorType, r.Version),
		"error":       se,
		"message":     se.UserMessage,
	}
	body, err := json.Marshal(notification)
	if err != nil {
		log.WithError(err).Warn("healthwatch: marshal notification failed")
		return
	}
	if err := w.KafkaManager.Produce(notifyTopic, nil, body); err != nil {
		log.WithError(err).Warn("healthwatch: publish notification failed")
		return
	}
	log.WithFields(log.Fields{
		"connector_type": r.ConnectorType,
		"version":        r.Version,
		"drop_pp":        r.DropPP,
	}).Warn("🚨 healthwatch: regression alert published")
}

// SnapshotRow is a minimal projection for the admin /health/connector-versions
// endpoint. Returned by Snapshot().
type SnapshotRow struct {
	ConnectorType   string    `json:"connector_type"`
	Version         string    `json:"version"`
	SuccessCount24h int       `json:"success_count_24h"`
	FailureCount24h int       `json:"failure_count_24h"`
	SuccessRate     float64   `json:"success_rate"`
	PrevVersion     string    `json:"prev_version,omitempty"`
	PrevSuccessRate float64   `json:"prev_success_rate,omitempty"`
	Regressed       bool      `json:"regressed"`
	ComputedAt      time.Time `json:"computed_at"`
}

// Snapshot reads the current connector_version_health rollup for the admin
// dashboard. Safe to call any time — reads the materialized rollup, not
// the full executions table.
func (w *Watchdog) Snapshot(ctx context.Context) ([]SnapshotRow, error) {
	q := `
		SELECT connector_type, version, success_count_24h, failure_count_24h,
		       success_rate, COALESCE(prev_version, ''), COALESCE(prev_success_rate, 0.0),
		       regressed, computed_at
		FROM connector_version_health
		ORDER BY regressed DESC, connector_type ASC, version ASC`
	rows, err := w.DB.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SnapshotRow{}
	for rows.Next() {
		var r SnapshotRow
		if err := rows.Scan(
			&r.ConnectorType, &r.Version, &r.SuccessCount24h, &r.FailureCount24h,
			&r.SuccessRate, &r.PrevVersion, &r.PrevSuccessRate,
			&r.Regressed, &r.ComputedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

func nullable(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

func nullableFloat(f float64) interface{} {
	if f == 0 {
		return nil
	}
	return f
}
