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
// the CDC apply path, for EVERY destination class that has a namespace analog.
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
