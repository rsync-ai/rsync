package main

// Regression coverage for the kafka-mcp-sink fixes:
//   Fix 1 (High):   processCDCEvent must return commit=false on a transient write
//                   failure so the caller's in-place bounded retry is reachable —
//                   not DLQ on attempt 1.
//   Fix 2 (data-loss): a claim-check that reads back empty while the header promised
//                   rows must fail closed (retry → error), not commit an empty batch.
//   Fix 3 (security):  validateClaimCheckURL rejects non-staging / malformed pointers.
// All use scripted RoundTrippers — no live broker/minio needed. failRT is shared
// from aud4_dlq_test.go.

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// --- Fix 1: CDC single-event retry must be reachable on transient failure --------

func TestProcessCDCEventTransientFailureIsRetryable(t *testing.T) {
	ctx := context.Background()
	hw := newHighWaterTracker()
	client := &http.Client{Transport: failRT{}, Timeout: 2 * time.Second}
	cfg := &WorkerConfig{
		PipelineID:           "p-fix1",
		DestinationConnector: "postgresql",
		DestinationConfig:    map[string]interface{}{},
	}
	// DDL resolved+disabled → skip ensure_table, go straight to the (failing) write.
	ddl := &DDLSupport{Enabled: false, resolved: true}
	dlqWriter := &kafka.Writer{} // present (as in prod) but must NOT be used on attempt 1
	msg := kafka.Message{Topic: "t", Partition: 0, Offset: 1, Key: []byte("k1")}
	sm := &SinkMessage{CDCOp: "c", Table: "t", Data: []map[string]interface{}{{"id": 1}}}
	metrics := &Metrics{}
	var ins, upd, del, bytes sync.Map

	commit, err := processCDCEvent(ctx, hw, nil, client, cfg, ddl, nil, dlqWriter, msg, sm, metrics,
		&ins, &upd, &del, &bytes, time.Minute)

	if err == nil {
		t.Fatal("expected a destination write error from failRT, got nil")
	}
	if commit {
		t.Fatal("BUG: processCDCEvent returned commit=true on a transient write failure; " +
			"the caller's bounded-retry loop (guarded on !commit) would be dead code and the " +
			"event would be dead-lettered on attempt 1")
	}
	if isFatal(err) {
		t.Fatalf("a transient destination failure must NOT be fatal (would os.Exit the worker): %v", err)
	}
}

// --- Fix 2: empty claim-check despite a non-zero header row_count fails closed ----

// emptyMinioRT always returns a successful-but-EMPTY claim-check payload (the
// read-after-write / minio-alias split-brain failure mode).
type emptyMinioRT struct{ calls int32 }

func (e *emptyMinioRT) RoundTrip(*http.Request) (*http.Response, error) {
	atomic.AddInt32(&e.calls, 1)
	const body = `{"result":{"success":true,"data":{"data":[]}}}`
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}, nil
}

func TestFetchFromMinIOEmptyDespiteHeaderFailsClosed(t *testing.T) {
	t.Setenv("MINIO_FETCH_MAX_ATTEMPTS", "3")
	rt := &emptyMinioRT{}
	client := &http.Client{Transport: rt}
	// Header promised 5 rows but the object reads back empty → must NOT be a valid
	// empty batch; must retry to the budget and then return an error (caller DLQs).
	rows, err := fetchFromMinIO(context.Background(), client, "s3://pipeline-data/staging/p/t/x.json", 5)
	if err == nil {
		t.Fatalf("BUG: empty read with header row_count=5 returned (%d rows, nil err) — silent drop", len(rows))
	}
	if got := atomic.LoadInt32(&rt.calls); got != 3 {
		t.Fatalf("empty-but-promised read must retry to the budget: expected 3 calls, got %d", got)
	}
}

func TestFetchFromMinIOLegitimateEmptyBatch(t *testing.T) {
	rt := &emptyMinioRT{}
	client := &http.Client{Transport: rt}
	// Header row_count=0 → a legitimately empty staged object: return empty, no retry, no error.
	rows, err := fetchFromMinIO(context.Background(), client, "s3://pipeline-data/staging/p/t/x.json", 0)
	if err != nil {
		t.Fatalf("a legitimately empty batch (row_count=0) must succeed, got: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(rows))
	}
	if got := atomic.LoadInt32(&rt.calls); got != 1 {
		t.Fatalf("a legit empty batch must not retry: expected 1 call, got %d", got)
	}
}

// --- Fix 3: claim_check_url validation -------------------------------------------

func TestValidateClaimCheckURL(t *testing.T) {
	t.Setenv("RSYNC_SINK_STAGING_BUCKET", "")
	t.Setenv("MINIO_BUCKET", "") // force the default allow-list ["pipeline-data"]
	cases := []struct {
		name string
		url  string
		ok   bool
	}{
		{"valid", "s3://pipeline-data/staging/p/t/x.json", true},
		{"wrong_bucket", "s3://evil-bucket/x.json", false},
		{"not_s3", "https://evil/x.json", false},
		{"trailing_slash_no_key", "s3://pipeline-data/", false},
		{"bucket_only", "s3://pipeline-data", false},
		{"empty_bucket", "s3:///x.json", false},
		{"path_traversal", "s3://pipeline-data/../../etc/passwd", false},
		{"control_char", "s3://pipeline-data/x\n.json", false},
	}
	for _, c := range cases {
		err := validateClaimCheckURL(c.url)
		if c.ok && err != nil {
			t.Errorf("%s (%q): expected valid, got %v", c.name, c.url, err)
		}
		if !c.ok && err == nil {
			t.Errorf("%s (%q): expected rejection, got nil", c.name, c.url)
		}
	}
}

func TestExpectedStagingBucketsPrecedence(t *testing.T) {
	t.Setenv("RSYNC_SINK_STAGING_BUCKET", "a, b ,c")
	if got := expectedStagingBuckets(); len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("override precedence: got %v, want [a b c]", got)
	}
	t.Setenv("RSYNC_SINK_STAGING_BUCKET", "")
	t.Setenv("MINIO_BUCKET", "custom-bkt")
	if got := expectedStagingBuckets(); len(got) != 1 || got[0] != "custom-bkt" {
		t.Fatalf("MINIO_BUCKET fallback: got %v, want [custom-bkt]", got)
	}
	t.Setenv("MINIO_BUCKET", "")
	if got := expectedStagingBuckets(); len(got) != 1 || got[0] != "pipeline-data" {
		t.Fatalf("system default: got %v, want [pipeline-data]", got)
	}
}
