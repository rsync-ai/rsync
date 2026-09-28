package main

import (
	"context"
	"database/sql"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/handlers"
	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// startSinkWorkerReaper starts the orphan sink-worker reaper
// (KI-CDC-SINK-WORKER-NO-REAPER). It retries the stop_sink that a pipeline
// Stop/Delete could not make, only for workers whose pipeline row is gone or
// 'stopped' past a grace period. Dormant unless SINK_WORKER_REAPER_MODE is
// dry_run or enforce. It lives in its own file so main.go line numbers cited
// across the docs do not shift. The goroutine runs for the life of the process.
func startSinkWorkerReaper(db *sql.DB, mcpManager *mcp.ServerManager) {
	reaper := handlers.NewSinkWorkerReaperFromEnv(db, mcpManager)
	if reaper == nil {
		return
	}
	go reaper.Run(context.Background())
	log.WithField("mode", reaper.Mode()).Info("✅ Orphan sink-worker reaper started")
}
