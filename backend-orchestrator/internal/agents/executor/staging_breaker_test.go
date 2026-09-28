package executor

import (
	"strings"
	"testing"
	"time"
)

func TestStagingBreakerSkipsMinIOAfterRepeatedFailuresThenProbes(t *testing.T) {
	now := time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)
	b := newStagingBreaker(2, time.Minute)
	b.now = func() time.Time { return now }

	if !b.allow() || b.failure() {
		t.Fatal("one failed batch must not open the breaker")
	}
	if !b.allow() || !b.failure() {
		t.Fatal("the second failed batch in a row must open the breaker")
	}
	if b.allow() {
		t.Fatal("while open, batches must skip MinIO staging")
	}
	now = now.Add(time.Minute)
	if !b.allow() {
		t.Fatal("after the cooldown one batch must probe MinIO")
	}
	if b.allow() {
		t.Fatal("only one probe at a time: a concurrent batch must still skip")
	}
	if !b.failure() {
		t.Fatal("a failed probe must re-open the breaker")
	}
	if b.allow() {
		t.Fatal("a failed probe starts a new cooldown")
	}
	now = now.Add(time.Minute)
	if !b.allow() {
		t.Fatal("next probe after the second cooldown")
	}
	b.success()
	if !b.allow() || !b.allow() {
		t.Fatal("a successful probe closes the breaker for every batch")
	}
}

func TestStagingBreakerSuccessResetsTheCount(t *testing.T) {
	b := newStagingBreaker(2, time.Minute)
	b.failure()
	b.success()
	if b.failure() {
		t.Fatal("failures must be consecutive to open the breaker")
	}
}

func TestStagingBreakerIsWiredIntoTheClaimCheckPath(t *testing.T) {
	batch := executeBatchDataTransferBody(t)
	decl := strings.Index(batch, "minioStaging := newStagingBreaker(")
	loop := strings.Index(batch, "using MinIO staging")
	if decl < 0 || loop < 0 || decl > loop {
		t.Fatal("the breaker must be created once per run, before the table loop")
	}
	path := batch[loop:]
	gate := strings.Index(path, "if !minioStaging.allow()")
	stage := strings.Index(path, "a.stageDataToMinIO(")
	if gate < 0 || stage < 0 || gate > stage {
		t.Error("a batch must consult the breaker before staging to MinIO")
	}
	for _, call := range []string{"minioStaging.failure()", "minioStaging.success()"} {
		if !strings.Contains(path, call) {
			t.Errorf("the claim-check path must record %s", call)
		}
	}
}
