package main

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

// Bounds for ensurePlatformTopicsWithRetry at startup: five attempts with a doubling
// backoff from one second waits at most 1+2+4+8 = 15 s before startup carries on.
const (
	platformTopicAttempts = 5
	platformTopicBackoff  = time.Second
)

// ensurePlatformTopicsWithRetry runs ensure (kafka.TopologyManager.EnsurePlatformTopics)
// until it succeeds or attempts run out, doubling the wait between tries, and returns
// the last error.
//
// main calls it BEFORE any worker, sentinel or the cdcstats agent starts. The order is
// the point: a sarama consumer-group subscription auto-creates the topic it joins, at
// the broker's defaults, so a consumer that starts first would mint the platform topic
// at the wrong partition count and retention and EnsurePlatformTopics would then leave
// it alone (it never re-sizes an existing topic). The retry covers a broker that
// accepted the metadata connection but is still electing a controller, which rejects
// CreateTopics for a few seconds after a cold start.
//
// It never fails startup: a topic that still cannot be created is logged, and the
// services that use it surface their own produce or consume error.
func ensurePlatformTopicsWithRetry(
	ctx context.Context,
	ensure func(context.Context) error,
	attempts int,
	backoff time.Duration,
	sleep func(time.Duration),
) error {
	if attempts < 1 {
		attempts = 1
	}
	var err error
	wait := backoff
	for i := 1; i <= attempts; i++ {
		if err = ensure(ctx); err == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("ensure platform topics: %w (stopped: %v)", err, ctxErr)
		}
		if i == attempts {
			break
		}
		log.WithError(err).WithFields(log.Fields{
			"attempt": i,
			"of":      attempts,
			"retry":   wait.String(),
		}).Warn("Platform topics not all ensured; retrying")
		sleep(wait)
		wait *= 2
	}
	return fmt.Errorf("ensure platform topics after %d attempts: %w", attempts, err)
}
