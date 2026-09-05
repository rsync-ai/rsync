package workers

import (
	"context"
	"time"

	"github.com/rsync-ai/shared/correlation"
	log "github.com/sirupsen/logrus"
)

// startClaimRenewer periodically renews the Redis claim key for a correlation request.
// This prevents long-running tasks from being claimed twice, while still allowing fast recovery
// if the worker crashes (claim TTL is short by default).
//
// Returns a stop() function (idempotent) to end renewal.
func startClaimRenewer(ctx context.Context, client *correlation.Client, correlationID, workerID string) func() {
	if client == nil || correlationID == "" || workerID == "" {
		return func() {}
	}

	stopCh := make(chan struct{})
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-stopCh:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				ok, err := client.RenewClaim(renewCtx, correlationID, workerID)
				cancel()
				if err != nil {
					log.WithError(err).WithFields(log.Fields{
						"correlation_id": correlationID,
						"worker_id":      workerID,
					}).Warn("⚠️  Failed to renew correlation claim")
					continue
				}
				if !ok {
					// Claim no longer belongs to us (or missing) → stop renewing.
					return
				}
			}
		}
	}()

	return func() {
		select {
		case <-stopCh:
			// already closed
		default:
			close(stopCh)
		}
		<-stopped
	}
}


