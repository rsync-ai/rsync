package main

// Unit cover for the sink's blob (raw-bytes passthrough) lane —
// universal-blob-passthrough plan §3.

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestBlobDestKey(t *testing.T) {
	cases := []struct{ prefix, key, want string }{
		{"archive", "docs/report.pdf", "archive/docs/report.pdf"},
		{"", "docs/report.pdf", "docs/report.pdf"},
		{"/archive/", "/docs/report.pdf", "archive/docs/report.pdf"},
		{"  ", "a.bin", "a.bin"}, // blank prefix → key verbatim
	}
	for _, c := range cases {
		if got := blobDestKey(c.prefix, c.key); got != c.want {
			t.Errorf("blobDestKey(%q,%q)=%q want %q", c.prefix, c.key, got, c.want)
		}
	}
}

func TestParseSinkMessageBlob(t *testing.T) {
	cfg := &WorkerConfig{PipelineID: "p", ExecutionID: "e", Topic: "t", SinkMode: "batch"}
	payload := map[string]interface{}{
		"pipeline_id": "p", "execution_id": "e", "table": "blobs",
		"storage_type": "blob", "is_blob": true,
		"object_key":      "docs/report.pdf",
		"content_type":    "application/pdf",
		"claim_check_url": "s3://pipeline-data/staging/blobs/application/pdf/abc/report.pdf",
		"sha256":          "abc123",
		"size":            float64(1234),
		"staging_config":  map[string]interface{}{"endpoint": "http://minio:9000", "bucket": "pipeline-data"},
	}
	b, _ := json.Marshal(payload)
	sm, err := parseSinkMessage(cfg, kafka.Message{Value: b})
	if err != nil {
		t.Fatalf("parse blob message: %v", err)
	}
	if !sm.IsBlob {
		t.Fatal("expected IsBlob=true")
	}
	if sm.StorageType != "blob" {
		t.Errorf("storage_type=%q want blob", sm.StorageType)
	}
	if sm.ObjectKey != "docs/report.pdf" {
		t.Errorf("object_key=%q", sm.ObjectKey)
	}
	if sm.ContentType != "application/pdf" {
		t.Errorf("content_type=%q", sm.ContentType)
	}
	if sm.ClaimCheckURL == "" {
		t.Error("claim_check_url empty")
	}
	if sm.Sha256 != "abc123" {
		t.Errorf("sha256=%q", sm.Sha256)
	}
	if sm.Size != 1234 {
		t.Errorf("size=%d want 1234", sm.Size)
	}
	if sm.StagingConfig["bucket"] != "pipeline-data" {
		t.Errorf("staging_config not carried: %v", sm.StagingConfig)
	}
}

func TestParseSinkMessageBlobNotRoutedToCDCWhenSinkModeCDC(t *testing.T) {
	// Regression: a blob message must reach the blob branch even when the sink was
	// started in cdc mode — it has no Debezium op/source and must never be DLQ'd as
	// a malformed CDC envelope.
	cfg := &WorkerConfig{Topic: "t", SinkMode: "cdc"}
	payload := map[string]interface{}{
		"pipeline_id": "p", "execution_id": "e", "table": "blobs",
		"storage_type": "blob", "is_blob": true,
		"object_key":      "docs/report.pdf",
		"claim_check_url": "s3://pipeline-data/staging/blobs/x/report.pdf",
	}
	b, _ := json.Marshal(payload)
	sm, err := parseSinkMessage(cfg, kafka.Message{Value: b})
	if err != nil {
		t.Fatalf("blob message under sink_mode=cdc should parse, got: %v", err)
	}
	if !sm.IsBlob {
		t.Fatal("blob message under sink_mode=cdc was not recognized as a blob (mis-routed to CDC)")
	}
	if sm.IsCDC {
		t.Fatal("blob message must not be flagged IsCDC")
	}
}

func TestParseSinkMessageBlobMissingObjectKeyFailsLoud(t *testing.T) {
	cfg := &WorkerConfig{Topic: "t", SinkMode: "batch"}
	payload := map[string]interface{}{
		"pipeline_id": "p", "execution_id": "e", "table": "blobs",
		"storage_type":    "blob",
		"claim_check_url": "s3://pipeline-data/staging/blobs/x",
		// object_key intentionally absent
	}
	b, _ := json.Marshal(payload)
	if _, err := parseSinkMessage(cfg, kafka.Message{Value: b}); err == nil {
		t.Fatal("expected fail-loud error for blob message missing object_key")
	}
}

func TestParseSinkMessageBlobMissingClaimCheckFailsLoud(t *testing.T) {
	cfg := &WorkerConfig{Topic: "t", SinkMode: "batch"}
	payload := map[string]interface{}{
		"pipeline_id": "p", "execution_id": "e", "table": "blobs",
		"storage_type": "blob",
		"object_key":   "docs/report.pdf",
		// claim_check_url intentionally absent
	}
	b, _ := json.Marshal(payload)
	if _, err := parseSinkMessage(cfg, kafka.Message{Value: b}); err == nil {
		t.Fatal("expected fail-loud error for blob message missing claim_check_url")
	}
}

func TestWriteBlobToNonObjectStorageFailsLoud(t *testing.T) {
	// Defense in depth: even if the capability gate were bypassed, the sink must
	// never silently write a blob to a structured destination.
	cfg := &WorkerConfig{DestinationConnector: "postgresql", DestinationConfig: map[string]interface{}{}}
	sm := &SinkMessage{IsBlob: true, ObjectKey: "a.pdf", ClaimCheckURL: "s3://b/k"}
	if _, err := writeBlobToDestination(context.Background(), &http.Client{}, cfg, sm); err == nil {
		t.Fatal("expected fail-loud error for blob → non-object-storage destination")
	}
}
