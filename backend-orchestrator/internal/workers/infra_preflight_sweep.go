package workers

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// coreInfraConnectors are the always-on infra MCP services that pipelines depend
// on. This mirrors the mcp_core set in infraPreflightStage.requiredServices
// (minio for batch claim-check staging; kafka-mcp-sink + debezium for CDC).
var coreInfraConnectors = []struct {
	Name    string
	Version string
}{
	{Name: "minio", Version: "v1.0.0"},
	{Name: "kafka-mcp-sink", Version: "v1.0.0"},
	{Name: "debezium", Version: "v1.0.0"},
}

// infraPreflightSweepTimeout bounds how long StartServer polls for each core
// connector to become reachable before the sweep gives up on it for this cycle.
const infraPreflightSweepTimeout = 90 * time.Second

// StartInfraPreflightSweep warms the core infra MCP connectors so the first
// pipeline of the day isn't slowed by a cold container start. It runs one sweep
// immediately in a goroutine (boot is never blocked) and, if intervalSecs > 0,
// repeats on that interval until ctx is cancelled.
//
// This is a best-effort warm-up, NOT a gate: every failure is logged and never
// fatal. The per-pipeline infraPreflightStage remains the authoritative check
// that a connector is up before a pipeline runs. StartServer is idempotent
// (health-checks first, only deploys if down), so repeated sweeps are cheap and
// safe. Safe to call with a nil manager (logs and returns).
func StartInfraPreflightSweep(ctx context.Context, mgr *mcp.ServerManager, intervalSecs int) {
	if mgr == nil {
		log.Warn("infra preflight sweep: nil MCP manager — skipping")
		return
	}
	go func() {
		runInfraPreflightSweep(ctx, mgr)
		if intervalSecs <= 0 {
			return // boot-only sweep
		}
		ticker := time.NewTicker(time.Duration(intervalSecs) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runInfraPreflightSweep(ctx, mgr)
			}
		}
	}()
}

// runInfraPreflightSweep ensures each core infra connector is up via the same
// health-check-then-deploy path the per-pipeline preflight uses.
func runInfraPreflightSweep(ctx context.Context, mgr *mcp.ServerManager) {
	ok, failed := 0, 0
	for _, c := range coreInfraConnectors {
		if ctx.Err() != nil {
			return // shutting down
		}
		if _, err := mgr.StartServer(mcp.ServerConfig{
			Name:              c.Name,
			Version:           c.Version,
			RequireHTTP:       true,
			DeployWaitTimeout: infraPreflightSweepTimeout,
		}); err != nil {
			failed++
			log.Warnf("infra preflight sweep: %s not available: %v", c.Name, err)
			continue
		}
		ok++
	}
	log.Infof("infra preflight sweep: %d/%d core MCP connectors ready (%d failed)",
		ok, len(coreInfraConnectors), failed)
}
