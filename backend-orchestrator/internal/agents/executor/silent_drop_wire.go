package executor

import (
	"context"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// probeSource lets *Agent satisfy SilentDropProber without exposing the
// retry-loop method to the broader package. Tests substitute a fake
// prober rather than spin up a full Agent.
func (a *Agent) probeSource(ctx context.Context, req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error) {
	return a.executeWithRetry(ctx, req)
}
