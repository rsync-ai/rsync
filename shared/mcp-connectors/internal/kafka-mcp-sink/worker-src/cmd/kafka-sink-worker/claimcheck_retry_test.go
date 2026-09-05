package main

// Unit coverage for the claim-check MinIO fetch retry added to close the
// `minio` DNS-alias split-brain silent drop (a NoSuchKey on the GetObject used
// to dead-letter the whole batch with zero retries). No live broker/minio
// needed — a scripted RoundTripper stands in for the minio MCP connector.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

// scriptedMinioRT returns a failure for the first `failFirst` calls, then a
// valid 2-row claim-check payload. All responses are HTTP 200 — the minio MCP
// connector signals tool errors via {"success":false} in the JSON-RPC result,
// not via HTTP status. Call count lets tests assert retry behavior.
type scriptedMinioRT struct {
	calls     int32
	failFirst int32
	permanent bool // failure is a non-retryable malformed-pointer error
}

func (s *scriptedMinioRT) RoundTrip(*http.Request) (*http.Response, error) {
	n := atomic.AddInt32(&s.calls, 1)
	var body string
	switch {
	case n <= s.failFirst && s.permanent:
		body = `{"result":{"success":false,"error":"claim_check_url must start with s3://"}}`
	case n <= s.failFirst:
		body = `{"result":{"success":false,"error":"An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist."}}`
	default:
		body = `{"result":{"success":true,"data":{"data":[{"id":1},{"id":2}]}}}`
	}
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}, nil
}

func TestFetchFromMinIORetriesThenSucceeds(t *testing.T) {
	rt := &scriptedMinioRT{failFirst: 2} // 2× NoSuchKey, then success
	client := &http.Client{Transport: rt}
	rows, err := fetchFromMinIO(context.Background(), client, "s3://pipeline-data/staging/p/t/x.json", 2)
	if err != nil {
		t.Fatalf("expected success after retries, got error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if got := atomic.LoadInt32(&rt.calls); got != 3 {
		t.Fatalf("expected 3 calls (2 NoSuchKey + 1 success), got %d", got)
	}
}

func TestFetchFromMinIOFailFastOnPermanent(t *testing.T) {
	rt := &scriptedMinioRT{failFirst: 99, permanent: true}
	client := &http.Client{Transport: rt}
	_, err := fetchFromMinIO(context.Background(), client, "not-an-s3-url", 0)
	if err == nil {
		t.Fatal("expected a permanent error, got nil")
	}
	if got := atomic.LoadInt32(&rt.calls); got != 1 {
		t.Fatalf("a deterministic error must not retry: expected 1 call, got %d", got)
	}
}

func TestFetchFromMinIOExhaustsBudget(t *testing.T) {
	t.Setenv("MINIO_FETCH_MAX_ATTEMPTS", "3")
	rt := &scriptedMinioRT{failFirst: 99} // always NoSuchKey
	client := &http.Client{Transport: rt}
	_, err := fetchFromMinIO(context.Background(), client, "s3://b/k.json", 0)
	if err == nil {
		t.Fatal("expected error after exhausting the retry budget")
	}
	if got := atomic.LoadInt32(&rt.calls); got != 3 {
		t.Fatalf("expected 3 attempts (MINIO_FETCH_MAX_ATTEMPTS=3), got %d", got)
	}
}

func TestIsRetryableMinioFetchErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"nosuchkey", errors.New("minio read failed: An error occurred (NoSuchKey) ... does not exist"), true},
		{"network", errors.New(`Post "http://minio-mcp:8000/mcp": dial tcp: connection refused`), true},
		{"missing_result", errors.New("minio missing result"), true},
		{"not_json", errors.New("minio response not json: unexpected EOF"), true},
		{"malformed_url", errors.New("minio read failed: claim_check_url must start with s3://"), false},
		{"bad_s3", errors.New("minio read failed: claim_check_url must be s3://<bucket>/<key>"), false},
		{"missing_param", errors.New("minio read failed: Missing required param: claim_check_url"), false},
		{"unknown_shape", errors.New("minio returned unknown payload shape"), false},
	}
	for _, c := range cases {
		if got := isRetryableMinioFetchErr(c.err); got != c.want {
			t.Errorf("%s: isRetryableMinioFetchErr=%v want %v", c.name, got, c.want)
		}
	}
}

func TestMinioFetchMaxAttempts(t *testing.T) {
	cases := []struct {
		set  string
		want int
	}{
		{"", 5}, // default (env unset)
		{"1", 1},
		{"3", 3},
		{"10", 10},
		{"0", 1},   // clamp low
		{"99", 10}, // clamp high
		{"abc", 5}, // unparseable → default
	}
	for _, c := range cases {
		if c.set == "" {
			os.Unsetenv("MINIO_FETCH_MAX_ATTEMPTS")
		} else {
			t.Setenv("MINIO_FETCH_MAX_ATTEMPTS", c.set)
		}
		if got := minioFetchMaxAttempts(); got != c.want {
			t.Errorf("MINIO_FETCH_MAX_ATTEMPTS=%q: got %d want %d", c.set, got, c.want)
		}
	}
}
