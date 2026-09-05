package main

// KI-CDC-TABLE-STATS-SOURCE-SCHEMA: TABLE_STATS used to carry only the SOURCE-derived
// table name. For CDC, parseCDCMessage overwrites sm.Table with
// "<source schema>.<source table>", so a MySQL->Postgres pipeline whose rows landed in
// rsync_verify_cdc reported schema "pipeline_test" — the source database. Anyone reading
// the stats to answer "where did my data go?" was handed the wrong half of the pipeline.
//
// These tests pin the shape of the fix, and — just as importantly — pin the part that
// must NOT change. metadata.table.qualified_name is a cross-producer correlation key:
// the orchestrator's cdcstats agent independently upserts the same
// (pipeline_id, execution_id, qualified_name) row (migration 034's unique index) with the
// captured-side counters, deriving its name from payload.source.schema. Rewriting
// qualified_name here — the KI's own proposed one-line fix — would stop the two producers
// colliding and split every CDC table into two half-filled rows: captured in one, applied
// in the other. So the destination is ADDED alongside, never substituted.

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/segmentio/kafka-go"
)

func statsTable(t *testing.T, ev map[string]interface{}) map[string]interface{} {
	t.Helper()
	meta, ok := ev["metadata"].(map[string]interface{})
	if !ok {
		t.Fatalf("event has no metadata map: %#v", ev)
	}
	table, ok := meta["table"].(map[string]interface{})
	if !ok {
		t.Fatalf("metadata has no table map: %#v", meta)
	}
	return table
}

// The reported destination must be the destination, and the source-side identity must
// survive intact next to it.
func TestCDCTableStatsNamesTheDestinationNotJustTheSource(t *testing.T) {
	sm := &SinkMessage{
		PipelineID:    "p1",
		ExecutionID:   "e1",
		Table:         "pipeline_test.demo_products", // source schema, as parseCDCMessage leaves it
		DestNamespace: "rsync_verify_cdc",            // where the rows actually landed
	}

	table := statsTable(t, buildCDCTableStatsEvent(sm, 10, 5, 2, 1024, 0))

	// Unchanged — and load-bearing. See the file header: cdcstats writes the captured
	// counters into the row keyed by this exact value.
	if got := table["qualified_name"]; got != "pipeline_test.demo_products" {
		t.Errorf("qualified_name = %v, want %q — it is the key cdcstats upserts on, so "+
			"rewriting it splits every CDC table into two half-filled stats rows",
			got, "pipeline_test.demo_products")
	}
	if got := table["schema"]; got != "pipeline_test" {
		t.Errorf("schema = %v, want %q (the source-side name stays as-is)", got, "pipeline_test")
	}
	if got := table["name"]; got != "demo_products" {
		t.Errorf("name = %v, want %q", got, "demo_products")
	}

	// New, and the whole point of the change.
	if got := table["destination_schema"]; got != "rsync_verify_cdc" {
		t.Errorf("destination_schema = %v, want %q — without it the stats name the source "+
			"database for rows that landed somewhere else", got, "rsync_verify_cdc")
	}
	if got := table["destination_qualified_name"]; got != "rsync_verify_cdc.demo_products" {
		t.Errorf("destination_qualified_name = %v, want %q", got, "rsync_verify_cdc.demo_products")
	}
}

// "This sink cannot name a destination" and "the destination namespace is the empty
// string" are different answers, and the projector stores them differently (NULL vs ”).
// Omitting the keys keeps a pre-fix sink, an object-storage destination and an
// unconfigured namespace all reading as "unknown" rather than as a confident "".
func TestTableStatsOmitDestinationWhenTheSinkCannotNameOne(t *testing.T) {
	sm := &SinkMessage{PipelineID: "p1", ExecutionID: "e1", Table: "public.orders"}

	for name, table := range map[string]map[string]interface{}{
		"cdc":   statsTable(t, buildCDCTableStatsEvent(sm, 1, 0, 0, 8, 0)),
		"batch": statsTable(t, buildTableStatsEvent(sm, "batch", "completed", 1, 1, 8)),
	} {
		for _, k := range []string{"destination_schema", "destination_qualified_name"} {
			if v, present := table[k]; present {
				t.Errorf("%s: %s present as %#v with no known destination; it must be absent "+
					"so the projector stores NULL rather than an empty string that reads as an answer",
					name, k, v)
			}
		}
	}
}

// db_or_schema is overloaded by design (executor.go:4095-4118): for a relational
// destination it IS the destination namespace, but for object storage it is a PATH
// SEGMENT in the bronze key layout that the executor deliberately fills with the SOURCE
// schema. Reporting that as "destination_schema" would restore the exact mislabelling
// this change exists to end, under a more confident name.
func TestDestinationNamespaceForStatsRefusesTheObjectStoragePathSegment(t *testing.T) {
	cases := []struct {
		connector  string
		dbOrSchema string
		want       string
	}{
		{"postgresql", "rsync_verify_cdc", "rsync_verify_cdc"},
		{"mysql", "  analytics  ", "analytics"}, // trimmed, still a real namespace
		{"snowflake", "RAW", "RAW"},

		// Object storage: the value is the source schema wearing a path segment's clothes.
		{"aws-s3", "pipeline_test", ""},
		{"s3", "pipeline_test", ""},
		{"minio", "pipeline_test", ""},
		{"gcs", "pipeline_test", ""},
		{"azure-blob", "pipeline_test", ""},

		// "default" is the planner's generic placeholder, not a database name.
		{"postgresql", "default", ""},
		{"postgresql", "DEFAULT", ""},
		{"postgresql", "   ", ""},
		{"postgresql", "", ""},
	}

	for _, c := range cases {
		cfg := &WorkerConfig{DestinationConnector: c.connector}
		if got := destinationNamespaceForStats(cfg, c.dbOrSchema); got != c.want {
			t.Errorf("destinationNamespaceForStats(%q, %q) = %q, want %q",
				c.connector, c.dbOrSchema, got, c.want)
		}
	}

	if got := destinationNamespaceForStats(nil, "rsync_verify_cdc"); got != "" {
		t.Errorf("nil config = %q, want \"\" — with no config there is no destination to name", got)
	}
}

// Both lanes upsert into the same table on the same key, so they must agree on how a
// table is identified. They used to agree by having the same 15 lines copied twice,
// which is agreement that lasts until someone edits one copy.
func TestBatchAndCDCTableStatsAgreeOnTableIdentity(t *testing.T) {
	for _, sm := range []*SinkMessage{
		{PipelineID: "p", ExecutionID: "e", Table: "pipeline_test.demo_products", DestNamespace: "rsync_verify_cdc"},
		{PipelineID: "p", ExecutionID: "e", Table: "orders"},                             // unqualified
		{PipelineID: "p", ExecutionID: "e", Table: "public.public.orders"},               // doubled qualifier
		{PipelineID: "p", ExecutionID: "e", Table: "orders", DestNamespace: "warehouse"}, // bare + destination
	} {
		cdc := statsTable(t, buildCDCTableStatsEvent(sm, 1, 0, 0, 8, 0))
		batch := statsTable(t, buildTableStatsEvent(sm, "batch", "completed", 1, 1, 8))
		if !reflect.DeepEqual(cdc, batch) {
			t.Errorf("table identity diverges for %q: cdc=%#v batch=%#v", sm.Table, cdc, batch)
		}
	}
}

// The builders above are only reachable with a DestNamespace that something set. These
// two prove the live parse paths set it, so a green suite above cannot coexist with a
// field that is empty in production.
func TestParseCDCMessagePopulatesDestNamespace(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID:           "p",
		ExecutionID:          "e",
		Topic:                "dbserver.pipeline_test.demo_products",
		SinkMode:             "cdc",
		DestinationConnector: "postgresql",
		DestinationNamespace: "rsync_verify_cdc",
	}
	envelope := wrapCDCEnvelope(map[string]interface{}{
		"op": "c",
		"source": map[string]interface{}{
			"db":    "pipeline_test",
			"table": "demo_products",
		},
		"before": nil,
		"after":  map[string]interface{}{"id": 1, "name": "widget", "email": "a@b.c"},
		"ts_ms":  int64(1704067200000),
	})
	b, _ := json.Marshal(envelope)

	sm, err := parseSinkMessage(cfg, kafka.Message{Topic: cfg.Topic, Value: b, Offset: 7})
	if err != nil {
		t.Fatalf("parseSinkMessage: %v", err)
	}
	// The source override really did land, so the mislabelling this fixes is reproduced.
	if sm.Table != "pipeline_test.demo_products" {
		t.Fatalf("sm.Table = %q, want %q — without the source override this test proves nothing",
			sm.Table, "pipeline_test.demo_products")
	}
	if sm.DestNamespace != "rsync_verify_cdc" {
		t.Errorf("sm.DestNamespace = %q, want %q — the stats builders can only report a "+
			"destination the parse path gave them", sm.DestNamespace, "rsync_verify_cdc")
	}
}

func TestParseSinkMessagePopulatesDestNamespaceFromBatchPayload(t *testing.T) {
	cfg := &WorkerConfig{PipelineID: "p", ExecutionID: "e", Topic: "t", SinkMode: "batch", DestinationConnector: "postgresql"}
	b, _ := json.Marshal(map[string]interface{}{
		"pipeline_id": "p", "execution_id": "e", "table": "orders",
		"db_or_schema": "rsync_public_20912e3b",
		"rows":         []interface{}{map[string]interface{}{"id": 1}},
	})

	sm, err := parseSinkMessage(cfg, kafka.Message{Value: b})
	if err != nil {
		t.Fatalf("parseSinkMessage: %v", err)
	}
	if sm.DestNamespace != "rsync_public_20912e3b" {
		t.Errorf("sm.DestNamespace = %q, want %q", sm.DestNamespace, "rsync_public_20912e3b")
	}

	// Same payload against an object-storage destination: db_or_schema is a path segment
	// there, and must not be laundered into a destination-schema claim.
	blobCfg := &WorkerConfig{PipelineID: "p", ExecutionID: "e", Topic: "t", SinkMode: "batch", DestinationConnector: "aws-s3"}
	smBlob, err := parseSinkMessage(blobCfg, kafka.Message{Value: b})
	if err != nil {
		t.Fatalf("parseSinkMessage (object storage): %v", err)
	}
	if smBlob.DestNamespace != "" {
		t.Errorf("object-storage sm.DestNamespace = %q, want \"\" — that value is the bronze "+
			"key's source-schema path segment, not a destination namespace", smBlob.DestNamespace)
	}
	if smBlob.DBOrSchema != "rsync_public_20912e3b" {
		t.Errorf("DBOrSchema = %q — the write path's value must be untouched by the stats labelling",
			smBlob.DBOrSchema)
	}
}
