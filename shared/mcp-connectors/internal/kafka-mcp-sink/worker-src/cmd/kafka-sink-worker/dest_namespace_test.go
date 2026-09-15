package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/segmentio/kafka-go"
)

// The per-pipeline destination namespace must reach the destination connector on
// BOTH write lanes — the CDC apply path (writeCDCToDestination) and the batch
// path (writeToDestination) — for EVERY destination class that has a namespace
// analog.
//
// This branch used to be silent for document DBs on the false premise that "a
// Mongo database has no schema analog" — it is exactly the analog, the same way a
// ClickHouse database is. The sink was internally inconsistent about it: the batch
// write path and the reload-cleanup drop both forward sm.DBOrSchema
// unconditionally, so a CDC pipeline with a locked destination_namespace wrote
// into the CONNECTION's database while its own drop targeted the namespace. The
// rows land, the counts match, the lag is zero — and they are in the wrong
// database. Nothing downstream can detect that.
//
// addNamespaceParam sets BOTH `namespace` and `db_or_schema` to the same value, so
// a connector honouring either alone is compliant; these tests assert both keys
// because the sink is the side that owes both.

// captureTransport answers every destination MCP call with success and records
// the JSON-RPC request bodies.
type captureTransport struct{ bodies [][]byte }

func (c *captureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		b, _ := io.ReadAll(req.Body)
		req.Body.Close()
		c.bodies = append(c.bodies, b)
	}
	return &http.Response{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader([]byte(`{"success":true,"rows_affected":1}`))),
		Request:    req,
	}, nil
}

// lastArgs returns the "arguments" map of the last captured tools/call, plus the
// tool name.
func (c *captureTransport) lastArgs(t *testing.T) (string, map[string]interface{}) {
	t.Helper()
	if len(c.bodies) == 0 {
		t.Fatal("destination was never called")
	}
	var env struct {
		Params struct {
			Name      string                 `json:"name"`
			Arguments map[string]interface{} `json:"arguments"`
		} `json:"params"`
	}
	if err := json.Unmarshal(c.bodies[len(c.bodies)-1], &env); err != nil {
		t.Fatalf("request body not JSON-RPC: %v (%s)", err, c.bodies[len(c.bodies)-1])
	}
	return env.Params.Name, env.Params.Arguments
}

func nsTestClient() (*http.Client, *captureTransport) {
	ct := &captureTransport{}
	return &http.Client{Transport: ct}, ct
}

func mongoCDCMessage(namespace string) (*WorkerConfig, *SinkMessage, kafka.Message) {
	cfg := &WorkerConfig{
		PipelineID:           "p1",
		DestinationConnector: "mongodb",
		DestinationConfig:    map[string]interface{}{"database": "appdb"},
	}
	sm := &SinkMessage{
		PipelineID: "p1",
		Table:      "public.orders",
		DBOrSchema: namespace,
		RowCount:   1,
		Data:       []map[string]interface{}{{"id": 1, "name": "alice"}},
		PK:         map[string]interface{}{"id": 1},
		KeyFields:  []string{"id"},
		CDCOp:      "u",
	}
	return cfg, sm, kafka.Message{Topic: "t", Partition: 0, Offset: 7}
}

func TestCDCDocumentDBForwardsDestinationNamespace(t *testing.T) {
	for _, op := range []string{"insert", "upsert", "delete"} {
		t.Run(op, func(t *testing.T) {
			client, ct := nsTestClient()
			cfg, sm, msg := mongoCDCMessage("postgres_test")
			if _, _, err := writeCDCToDestination(context.Background(), client, cfg, nil, msg, sm, op); err != nil {
				t.Fatalf("writeCDCToDestination(%s) errored: %v", op, err)
			}
			tool, args := ct.lastArgs(t)
			if args["namespace"] != "postgres_test" || args["db_or_schema"] != "postgres_test" {
				t.Fatalf("tool=%s namespace=%v db_or_schema=%v, want both postgres_test — "+
					"without this the connector writes into the connection's database",
					tool, args["namespace"], args["db_or_schema"])
			}
			// The collection name must stay BARE: mongodb is a single-namespace
			// destination (isSingleNamespaceDest), so the source schema is already
			// stripped — the namespace is the DATABASE, carried in the params above,
			// never a dotted prefix on the collection.
			if args["table"] != "orders" {
				t.Errorf("table=%v, want bare \"orders\"", args["table"])
			}
		})
	}
}

func TestCDCDocumentDBOmitsNamespaceWhenNotReal(t *testing.T) {
	// "" and "default" both mean "no real namespace" (isRealNamespace); forwarding
	// either would retarget the write to a database literally named "default".
	for _, ns := range []string{"", "   ", "default", "DEFAULT"} {
		client, ct := nsTestClient()
		cfg, sm, msg := mongoCDCMessage(ns)
		if _, _, err := writeCDCToDestination(context.Background(), client, cfg, nil, msg, sm, "upsert"); err != nil {
			t.Fatalf("writeCDCToDestination(ns=%q) errored: %v", ns, err)
		}
		_, args := ct.lastArgs(t)
		if _, ok := args["namespace"]; ok {
			t.Errorf("ns=%q: namespace=%v was forwarded, want absent", ns, args["namespace"])
		}
		if _, ok := args["db_or_schema"]; ok {
			t.Errorf("ns=%q: db_or_schema=%v was forwarded, want absent", ns, args["db_or_schema"])
		}
		// the collection name is unaffected either way
		if args["table"] != "orders" {
			t.Errorf("ns=%q: table=%v, want bare orders", ns, args["table"])
		}
	}
}

func TestCDCObjectStorageStillDoesNotForwardNamespace(t *testing.T) {
	// The control. Object stores key by the SOURCE table id and carry db_or_schema
	// as a path segment, not a destination namespace — forwarding it here would be
	// the bug, so this must stay silent. It is what makes the mongodb assertion
	// above a routing claim rather than "the sink forwards to everyone".
	client, ct := nsTestClient()
	cfg := &WorkerConfig{
		PipelineID:           "p1",
		DestinationConnector: "gcs",
		DestinationConfig:    map[string]interface{}{"bucket": "b", "path_prefix": "bronze"},
	}
	sm := &SinkMessage{
		PipelineID: "p1",
		Table:      "public.orders",
		DBOrSchema: "postgres_test",
		RowCount:   1,
		Data:       []map[string]interface{}{{"id": 1}},
		KeyFields:  []string{"id"},
		CDCOp:      "u",
	}
	msg := kafka.Message{Topic: "t", Partition: 0, Offset: 7}
	if _, _, err := writeCDCToDestination(context.Background(), client, cfg, nil, msg, sm, "upsert"); err != nil {
		t.Fatalf("writeCDCToDestination(gcs) errored: %v", err)
	}
	_, args := ct.lastArgs(t)
	if _, ok := args["namespace"]; ok {
		t.Errorf("object storage got namespace=%v, want absent", args["namespace"])
	}
	if args["table"] != "public.orders" {
		t.Errorf("object storage table=%v, want the full source id", args["table"])
	}
}

// ---------------------------------------------------------------------------
// The BATCH lane.
//
// Batch (full/incremental sync) and CDC terminate in the same binary but in
// two separate functions with two separately-built argument maps:
// writeCDCToDestination for CDC, writeToDestination here. The batch lane has
// always forwarded the namespace — that asymmetry is what made the CDC gap
// visible in the first place — so these tests do not fix anything; they pin
// behaviour that was correct only by convention, on the lane a company running
// multi-collection batch syncs actually uses. Without them the batch half of
// this contract is enforced by nothing, and the next edit to this arg map can
// reintroduce exactly the defect the CDC tests above now prevent.

func batchMessage(destType, namespace string, destCfg map[string]interface{}) (*WorkerConfig, *SinkMessage, []map[string]interface{}) {
	cfg := &WorkerConfig{
		PipelineID:           "p1",
		DestinationConnector: destType,
		DestinationConfig:    destCfg,
	}
	sm := &SinkMessage{
		PipelineID: "p1",
		Table:      "public.orders",
		DBOrSchema: namespace,
		RowCount:   1,
		KeyFields:  []string{"id"},
		// IsCDC is false — this is the batch lane.
	}
	return cfg, sm, []map[string]interface{}{{"id": 1, "name": "alice"}}
}

func TestBatchWriteForwardsDestinationNamespace(t *testing.T) {
	client, ct := nsTestClient()
	cfg, sm, rows := batchMessage("mongodb", "postgres_test", map[string]interface{}{"database": "appdb"})
	if _, _, err := writeToDestination(context.Background(), client, cfg, nil, sm, rows, "", ""); err != nil {
		t.Fatalf("writeToDestination errored: %v", err)
	}
	tool, args := ct.lastArgs(t)
	if args["namespace"] != "postgres_test" || args["db_or_schema"] != "postgres_test" {
		t.Fatalf("tool=%s namespace=%v db_or_schema=%v, want both postgres_test — "+
			"a batch sync with a locked destination_namespace would otherwise load "+
			"into the connection's database",
			tool, args["namespace"], args["db_or_schema"])
	}
	// Same bare-table contract as CDC: the namespace travels in the params, never
	// as a dotted prefix on a single-namespace destination.
	if args["table"] != "orders" {
		t.Errorf("table=%v, want bare \"orders\"", args["table"])
	}
	// Batch reruns must be idempotent, so a keyed batch is an upsert, not an insert.
	if tool != "mongodb_upsert_data" {
		t.Errorf("tool=%s, want mongodb_upsert_data for a keyed batch write", tool)
	}
}

func TestBatchWriteOmitsNamespaceWhenNotReal(t *testing.T) {
	for _, ns := range []string{"", "   ", "default", "DEFAULT"} {
		client, ct := nsTestClient()
		cfg, sm, rows := batchMessage("mongodb", ns, map[string]interface{}{"database": "appdb"})
		if _, _, err := writeToDestination(context.Background(), client, cfg, nil, sm, rows, "", ""); err != nil {
			t.Fatalf("writeToDestination(ns=%q) errored: %v", ns, err)
		}
		_, args := ct.lastArgs(t)
		if _, ok := args["namespace"]; ok {
			t.Errorf("ns=%q: namespace=%v was forwarded, want absent", ns, args["namespace"])
		}
		if _, ok := args["db_or_schema"]; ok {
			t.Errorf("ns=%q: db_or_schema=%v was forwarded, want absent", ns, args["db_or_schema"])
		}
	}
}

func TestBatchObjectStorageStillDoesNotForwardNamespace(t *testing.T) {
	// The batch-lane control, mirroring the CDC one above: object storage keys by
	// its own layout and carries db_or_schema as a PATH SEGMENT, so receiving it
	// as a write param would be the bug.
	client, ct := nsTestClient()
	cfg, sm, rows := batchMessage("gcs", "postgres_test",
		map[string]interface{}{"bucket": "b", "path_prefix": "bronze"})
	if _, _, err := writeToDestination(context.Background(), client, cfg, nil, sm, rows, "", ""); err != nil {
		t.Fatalf("writeToDestination(gcs) errored: %v", err)
	}
	_, args := ct.lastArgs(t)
	if _, ok := args["namespace"]; ok {
		t.Errorf("object storage got namespace=%v, want absent", args["namespace"])
	}
	if _, ok := args["db_or_schema"]; ok {
		t.Errorf("object storage got db_or_schema=%v, want absent", args["db_or_schema"])
	}
	if args["table"] != "public.orders" {
		t.Errorf("object storage table=%v, want the full source id", args["table"])
	}
}
