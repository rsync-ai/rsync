package executor

import (
	"sync"
	"time"
)

// stagingBreaker stops a batch run from paying the MinIO staging round-trip (two
// attempts and a 500 ms pause) on every batch while MinIO is down. Each batch
// used to try, fail, retry, fail, and only then fall back to Kafka chunks, so a
// MinIO outage slowed the whole run by that cost per batch across every table.
//
// After `threshold` consecutive batches fail staging (each after its retry), the
// breaker opens: batches go straight to the Kafka-chunk fallback. After
// `cooldown` one batch is let through to probe MinIO again; a success closes the
// breaker, a failure re-opens it for another cooldown. Shared by every table
// goroutine of one run.
type stagingBreaker struct {
	mu        sync.Mutex
	threshold int
	cooldown  time.Duration
	failures  int
	openUntil time.Time
	probing   bool
	now       func() time.Time
}

func newStagingBreaker(threshold int, cooldown time.Duration) *stagingBreaker {
	return &stagingBreaker{threshold: threshold, cooldown: cooldown, now: time.Now}
}

// allow reports whether this batch should try MinIO staging. While open it says
// no; once the cooldown ends it lets exactly one batch through as a probe.
func (b *stagingBreaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures < b.threshold {
		return true
	}
	if b.probing || b.now().Before(b.openUntil) {
		return false
	}
	b.probing = true
	return true
}

func (b *stagingBreaker) success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.probing = false
}

// failure records a batch whose staging failed after its retry and reports
// whether that opened (or re-opened) the breaker.
func (b *stagingBreaker) failure() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	b.probing = false
	if b.failures >= b.threshold {
		b.openUntil = b.now().Add(b.cooldown)
		return true
	}
	return false
}

const (
	minioStagingBreakerThreshold = 2
	minioStagingBreakerCooldown  = 60 * time.Second
)
