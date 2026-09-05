package retention

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

type PipelineRunRetentionConfig struct {
	Enabled       bool
	RetentionDays int
	BatchSize     int
	Interval      time.Duration
}

func LoadPipelineRunRetentionConfig() PipelineRunRetentionConfig {
	cfg := PipelineRunRetentionConfig{
		Enabled:       os.Getenv("ENABLE_PIPELINE_RUN_RETENTION") == "true",
		RetentionDays: 90,
		BatchSize:     10_000,
		Interval:      24 * time.Hour,
	}

	if v := stringsToInt(os.Getenv("PIPELINE_RUN_RETENTION_DAYS")); v > 0 {
		cfg.RetentionDays = v
	}
	if v := stringsToInt(os.Getenv("PIPELINE_RUN_RETENTION_BATCH_SIZE")); v > 0 {
		cfg.BatchSize = v
	}
	if v := stringsToInt(os.Getenv("PIPELINE_RUN_RETENTION_INTERVAL_HOURS")); v > 0 {
		cfg.Interval = time.Duration(v) * time.Hour
	}

	return cfg
}

func StartPipelineRunRetention(ctx context.Context, database *sql.DB) {
	cfg := LoadPipelineRunRetentionConfig()
	if !cfg.Enabled {
		return
	}
	if database == nil {
		log.Warn("pipeline run retention enabled but DB is nil; skipping")
		return
	}

	go func() {
		// Run once at start (best-effort), then periodically.
		runOnce(ctx, database, cfg)

		t := time.NewTicker(cfg.Interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				runOnce(ctx, database, cfg)
			}
		}
	}()
}

func runOnce(ctx context.Context, database *sql.DB, cfg PipelineRunRetentionConfig) {
	cutoff := time.Now().Add(-time.Duration(cfg.RetentionDays) * 24 * time.Hour)

	// Delete in batches to avoid long locks/transactions.
	deletedEvents := int64(0)
	for {
		n, err := deleteEventsBatch(ctx, database, cutoff, cfg.BatchSize)
		if err != nil {
			log.WithError(err).Warn("pipeline run retention: failed deleting pipeline_run_events batch")
			break
		}
		deletedEvents += n
		if n < int64(cfg.BatchSize) {
			break
		}
	}

	deletedMetrics := int64(0)
	for {
		n, err := deleteMetricsBatch(ctx, database, cutoff, cfg.BatchSize)
		if err != nil {
			log.WithError(err).Warn("pipeline run retention: failed deleting pipeline_run_metrics batch")
			break
		}
		deletedMetrics += n
		if n < int64(cfg.BatchSize) {
			break
		}
	}

	deletedTableStats := int64(0)
	for {
		n, err := deleteTableStatsBatch(ctx, database, cutoff, cfg.BatchSize)
		if err != nil {
			log.WithError(err).Warn("pipeline run retention: failed deleting pipeline_run_table_stats batch")
			break
		}
		deletedTableStats += n
		if n < int64(cfg.BatchSize) {
			break
		}
	}

	log.WithFields(log.Fields{
		"cutoff":              cutoff.Format(time.RFC3339),
		"deleted_events":      deletedEvents,
		"deleted_metrics":     deletedMetrics,
		"deleted_table_stats": deletedTableStats,
	}).Info("pipeline run retention cleanup complete (best-effort)")
}

func deleteEventsBatch(ctx context.Context, database *sql.DB, cutoff time.Time, batchSize int) (int64, error) {
	res, err := database.ExecContext(ctx, `
		WITH del AS (
			SELECT id
			FROM pipeline_run_events
			WHERE created_at < $1
			LIMIT $2
		)
		DELETE FROM pipeline_run_events
		WHERE id IN (SELECT id FROM del)
	`, cutoff, batchSize)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func deleteMetricsBatch(ctx context.Context, database *sql.DB, cutoff time.Time, batchSize int) (int64, error) {
	res, err := database.ExecContext(ctx, `
		WITH del AS (
			SELECT id
			FROM pipeline_run_metrics
			WHERE ts < $1
			LIMIT $2
		)
		DELETE FROM pipeline_run_metrics
		WHERE id IN (SELECT id FROM del)
	`, cutoff, batchSize)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func deleteTableStatsBatch(ctx context.Context, database *sql.DB, cutoff time.Time, batchSize int) (int64, error) {
	res, err := database.ExecContext(ctx, `
		WITH del AS (
			SELECT id
			FROM pipeline_run_table_stats
			WHERE created_at < $1
			LIMIT $2
		)
		DELETE FROM pipeline_run_table_stats
		WHERE id IN (SELECT id FROM del)
	`, cutoff, batchSize)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func stringsToInt(s string) int {
	s = os.ExpandEnv(s)
	if s == "" {
		return 0
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return v
}

