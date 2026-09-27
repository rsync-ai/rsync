package main

// The claim-check fetch and delete must dial the minio connector at the address
// the install gives them. They used a literal `http://minio-mcp:8000/mcp`, a
// Compose-only name: on Helm the Service is <stackPrefix>-minio-v1-0-0-mcp, so
// every claim-checked batch failed with "no such host" and was dead-lettered
// (KI-HELM-SINK-CLAIM-CHECK-HOST, GKE rsync-v016, 2026-09-27).

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

type recordingRT struct {
	mu   sync.Mutex
	urls []string
}

func (r *recordingRT) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.urls = append(r.urls, req.URL.String())
	r.mu.Unlock()
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(`{"result":{"success":true,"data":{"data":[{"id":1}]}}}`)),
		Header:     make(http.Header),
	}, nil
}

func TestMinioMCPURLDefaultsToTheComposeName(t *testing.T) {
	t.Setenv("MINIO_MCP_URL", "")
	if got := minioMCPURL(); got != "http://minio-mcp:8000/mcp" {
		t.Fatalf("minioMCPURL() = %q; Compose installs rely on the minio-mcp default", got)
	}
}

func TestClaimCheckFetchAndDeleteDialMinioMCPURL(t *testing.T) {
	const want = "http://rsync-ai-minio-v1-0-0-mcp:8000/mcp"
	t.Setenv("MINIO_MCP_URL", "  "+want+"  ")
	rt := &recordingRT{}
	client := &http.Client{Transport: rt}

	if _, err := fetchFromMinIOOnce(context.Background(), client, "s3://staging/k"); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	_ = deleteFromMinIO(context.Background(), client, "s3://staging/k")

	if len(rt.urls) != 2 {
		t.Fatalf("expected 2 requests (fetch + delete), got %d: %v", len(rt.urls), rt.urls)
	}
	for _, u := range rt.urls {
		if u != want {
			t.Errorf("request went to %q, want %q from MINIO_MCP_URL", u, want)
		}
	}
}
