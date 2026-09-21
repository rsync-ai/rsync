package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

// #16: the sink called gcs_get_cdc_offsets with only {config, pipeline_id}; an object
// store cannot derive offsets without knowing where this pipeline's CDC objects live.
func TestGetCDCOffsetsArgsScopeObjectStoreListing(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID:        "2F1C Pipe",
		DestinationConfig: map[string]interface{}{"bucket": "lake", "path_prefix": "/demo/"},
	}
	args := getCDCOffsetsArgs(cfg, "gcs")
	if args["bucket"] != "lake" {
		t.Errorf("bucket = %v", args["bucket"])
	}
	prefix, _ := args["prefix"].(string)
	if prefix != "demo/"+slugify("2F1C Pipe")+"/" {
		t.Errorf("prefix = %q", prefix)
	}
	// The listing prefix must contain every key the writer produces for this pipeline.
	key := cdcObjectKey("/demo/", cdcPipelineSegment(cfg, nil), "cdc", "users", "dt=2026-09-16", "", 1758000000000, 0, 42, 42, "parquet", "snappy")
	if !strings.HasPrefix(key, prefix) {
		t.Errorf("written key %q is outside the offsets listing prefix %q", key, prefix)
	}
	if args["pipeline_id"] != "2F1C Pipe" {
		t.Errorf("pipeline_id = %v", args["pipeline_id"])
	}

	// No path_prefix → pipeline root at the bucket root, no leading slash.
	cfg.DestinationConfig = map[string]interface{}{"bucket_name": "lake"}
	if got := getCDCOffsetsArgs(cfg, "gcs")["prefix"]; got != slugify("2F1C Pipe")+"/" {
		t.Errorf("root prefix = %v", got)
	}

	// Azure uses container.
	cfg.DestinationConfig = map[string]interface{}{"container": "c1"}
	az := getCDCOffsetsArgs(cfg, "azure-blob")
	if az["container"] != "c1" || az["bucket"] != nil {
		t.Errorf("azure args = %v", az)
	}

	// Control: relational destinations keep the original request shape.
	pg := getCDCOffsetsArgs(&WorkerConfig{PipelineID: "p", DestinationConfig: map[string]interface{}{"bucket": "x"}}, "postgresql")
	if len(pg) != 2 || pg["prefix"] != nil || pg["bucket"] != nil {
		t.Errorf("relational args changed: %v", pg)
	}
	// No pipeline id → no prefix (the connector then returns empty rather than scanning the bucket).
	if p := getCDCOffsetsArgs(&WorkerConfig{DestinationConfig: map[string]interface{}{"bucket": "x"}}, "gcs")["prefix"]; p != nil {
		t.Errorf("prefix without pipeline id = %v", p)
	}
}

func TestCDCObjectMetadataCarriesLastOffset(t *testing.T) {
	md := cdcObjectMetadata("p1", "rsync.cdc.shop.orders", 3, 150, 180)
	want := map[string]string{
		"rsync_pipeline_id": "p1", "rsync_topic": "rsync.cdc.shop.orders",
		"rsync_partition": "3", "rsync_first_offset": "150", "rsync_last_offset": "180",
	}
	for k, v := range want {
		if md[k] != v {
			t.Errorf("%s = %v, want %s", k, md[k], v)
		}
	}
	if cdcObjectMetadata("", "t", 0, 1, 1) != nil || cdcObjectMetadata("p", " ", 0, 1, 1) != nil {
		t.Error("metadata without pipeline id or topic must be omitted (it could never be matched)")
	}
}

// captureImportRT records every destination MCP request body and answers success, so
// flushBatch runs its real success path.
type captureImportRT struct{ bodies []map[string]interface{} }

func (c *captureImportRT) RoundTrip(r *http.Request) (*http.Response, error) {
	raw, _ := io.ReadAll(r.Body)
	var m map[string]interface{}
	_ = json.Unmarshal(raw, &m)
	c.bodies = append(c.bodies, m)
	return &http.Response{StatusCode: 200, Header: http.Header{},
		Body: io.NopCloser(strings.NewReader(`{"result":{"success":true,"rows_inserted":2}}`))}, nil
}

// noKafkaRT fails every Kafka request immediately (the stats emit is best-effort).
type noKafkaRT struct{}

func (noKafkaRT) RoundTrip(context.Context, net.Addr, protocol.Message) (protocol.Message, error) {
	return nil, errors.New("no kafka in unit test")
}

// The object batcher's real flush sends object_metadata stamped with the batch's LAST
// offset, so gcs_get_cdc_offsets can seed the tracker after a restart.
func TestCDCObjectFlushStampsOffsetMetadata(t *testing.T) {
	rt := &captureImportRT{}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{"127.0.0.1:1"}, Topic: "rsync.cdc.shop.orders"})
	defer reader.Close()
	ew := &kafka.Writer{Addr: kafka.TCP("127.0.0.1:1"), Transport: noKafkaRT{}, MaxAttempts: 1}
	cfg := &WorkerConfig{PipelineID: "p1", DestinationConnector: "gcs",
		DestinationConfig: map[string]interface{}{"bucket": "lake", "path_prefix": "demo"}}
	b := &cdcObjectBatcher{
		cfg: cfg, destType: "gcs", destCfg: cfg.DestinationConfig,
		params: cdcBatchingParams{enabled: true}, reader: reader, hw: newHighWaterTracker(),
		httpClient: &http.Client{Transport: rt}, eventsWriter: ew, metrics: &Metrics{},
		cdcInserts: &sync.Map{}, cdcUpdates: &sync.Map{}, cdcDeletes: &sync.Map{}, cdcBytes: &sync.Map{},
		batches: map[string]*cdcObjectBatch{},
	}
	msgs := []kafka.Message{
		{Topic: "rsync.cdc.shop.orders", Partition: 2, Offset: 150},
		{Topic: "rsync.cdc.shop.orders", Partition: 2, Offset: 180},
	}
	sm := &SinkMessage{PipelineID: "p1", Table: "shop.orders", DBOrSchema: "shop", CDCOp: "c"}
	batch := &cdcObjectBatch{topic: msgs[0].Topic, partition: 2, format: "jsonl", compression: "none",
		bucket: "lake", prefix: "demo", timeSeg: "dt=2026-09-16", firstOffset: 150, lastOffset: 180,
		events: []map[string]interface{}{{"op": "I"}, {"op": "I"}}, messages: msgs, sms: []*SinkMessage{sm, sm}}
	b.batches["k"] = batch
	b.flushBatch(context.Background(), "k", batch, "test")

	if len(rt.bodies) == 0 {
		t.Fatal("no destination call captured")
	}
	params, _ := rt.bodies[0]["params"].(map[string]interface{})
	args, _ := params["arguments"].(map[string]interface{})
	md, _ := args["object_metadata"].(map[string]interface{})
	if md["rsync_last_offset"] != "180" || md["rsync_first_offset"] != "150" ||
		md["rsync_partition"] != "2" || md["rsync_topic"] != "rsync.cdc.shop.orders" || md["rsync_pipeline_id"] != "p1" {
		t.Fatalf("object_metadata = %v", md)
	}
	key, _ := args["key"].(string)
	if root := cdcPipelineRootPrefix(cfg.DestinationConfig, cfg); !strings.HasPrefix(key, root) {
		t.Fatalf("flushed key %q not under offsets listing prefix %q", key, root)
	}
	if _, ok := b.batches["k"]; ok {
		t.Fatal("batch not cleared: flush did not take the success path")
	}
}
