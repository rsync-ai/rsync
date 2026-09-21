package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// A server-level source (a MySQL/MongoDB/ClickHouse connection that names no
// database) captures tables from several databases in one pipeline. Mirror mapping
// sends each to a same-named namespace at the destination: shop.users → <dest>.shop.users,
// crm.users → <dest>.crm.users. Without MirrorSourceNamespace both are normalized to the
// bare "users" (isSingleNamespaceDest) and collide in the connection's one database.

func mirrorPayload(source map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"op":     "c",
		"after":  map[string]interface{}{"id": 1, "name": "a"},
		"source": source,
	}
}

func TestParseCDCMessageMirrorsSourceNamespace(t *testing.T) {
	mysqlSrc := map[string]interface{}{"db": "shop", "table": "users"}
	cases := []struct {
		name   string
		dest   string
		ns     string
		mirror bool
		source map[string]interface{}
		want   string
	}{
		{"mysql source db becomes the namespace", "mysql", "", true, mysqlSrc, "shop"},
		{"mongodb source db (collection, no table)", "mongodb", "", true,
			map[string]interface{}{"db": "crm", "collection": "users"}, "crm"},
		{"schema-shaped source mirrors the schema, not the database", "postgresql", "", true,
			map[string]interface{}{"db": "appdb", "schema": "sales", "table": "orders"}, "sales"},
		{"placeholder namespace still mirrors", "mysql", "default", true, mysqlSrc, "shop"},
		// Pipeline creation seeds a default such as "public"; the orchestrator sets the
		// flag only when the namespace is not one the user chose, so the source wins.
		{"the flag replaces a seeded namespace", "postgresql", "public", true, mysqlSrc, "shop"},
		{"flag off: a typed namespace is used verbatim", "mysql", "analytics", false, mysqlSrc, "analytics"},
		{"flag off: unchanged (no namespace)", "mysql", "", false, mysqlSrc, ""},
		{"flag on but no source namespace: unchanged", "mysql", "", true,
			map[string]interface{}{"table": "users"}, ""},
		// Object storage already keys by the source database when no namespace is set;
		// DBOrSchema is its path segment and must stay empty so cdcObjectPath derives it.
		{"object storage ignores the flag", "gcs", "", true, mysqlSrc, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &WorkerConfig{PipelineID: "p1", SinkMode: "cdc", DestinationConnector: tc.dest,
				DestinationNamespace: tc.ns, MirrorSourceNamespace: tc.mirror}
			sm, err := parseCDCMessage(cfg, kafka.Message{Topic: "srv.x.y", Offset: 1}, mirrorPayload(tc.source), map[string]interface{}{})
			if err != nil {
				t.Fatalf("parseCDCMessage: %v", err)
			}
			if sm.DBOrSchema != tc.want {
				t.Fatalf("DBOrSchema = %q, want %q", sm.DBOrSchema, tc.want)
			}
		})
	}
}

// recordingDBDestination answers like dbDestination and records every write's
// (namespace, table) pair.
type recordingDBDestination struct {
	mu     sync.Mutex
	writes []string
}

func (d *recordingDBDestination) RoundTrip(r *http.Request) (*http.Response, error) {
	raw, _ := io.ReadAll(r.Body)
	var req struct {
		Params struct {
			Arguments map[string]interface{} `json:"arguments"`
		} `json:"params"`
	}
	_ = json.Unmarshal(raw, &req)
	ns, _ := req.Params.Arguments["namespace"].(string)
	table, _ := req.Params.Arguments["table"].(string)
	d.mu.Lock()
	d.writes = append(d.writes, ns+"|"+table)
	d.mu.Unlock()
	r.Body = io.NopCloser(bytes.NewReader(raw))
	return dbDestination{}.RoundTrip(r)
}

func (d *recordingDBDestination) sorted() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := append([]string{}, d.writes...)
	sort.Strings(out)
	return out
}

// deliverTwoDatabases runs shop.users and crm.users through parseCDCMessage and the
// relational/document batcher, then flushes every batch, returning what was written.
func deliverTwoDatabases(t *testing.T, mirror bool, namespace string) (*cdcDBBatcher, []string) {
	t.Helper()
	captureFailClosed(t)
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{"127.0.0.1:1"}, Topic: "srv.shop.users"})
	t.Cleanup(func() { reader.Close() })
	dest := &recordingDBDestination{}
	cfg := &WorkerConfig{PipelineID: "p-mirror", SinkMode: "cdc", DestinationConnector: "mongodb",
		DestinationConfig: map[string]interface{}{}, DestinationNamespace: namespace, MirrorSourceNamespace: mirror}
	b := &cdcDBBatcher{
		cfg:          cfg,
		destType:     "mongodb",
		destCfg:      cfg.DestinationConfig,
		params:       cdcBatchingParams{enabled: true, maxEvents: 100, maxRetries: 0, backoff: time.Millisecond},
		reader:       reader,
		hw:           newHighWaterTracker(),
		httpClient:   &http.Client{Transport: dest},
		ddl:          &DDLSupport{},
		eventsWriter: &kafka.Writer{},
		metrics:      &Metrics{},
		cdcInserts:   &sync.Map{},
		cdcUpdates:   &sync.Map{},
		cdcDeletes:   &sync.Map{},
		cdcBytes:     &sync.Map{},
		batches:      map[string]*cdcDBBatch{},
	}
	ctx := context.Background()
	for i, db := range []string{"shop", "crm"} {
		msg := kafka.Message{Topic: "srv." + db + ".users", Partition: 0, Offset: int64(10 + i)}
		sm, err := parseCDCMessage(cfg, msg, mirrorPayload(map[string]interface{}{"db": db, "table": "users"}), map[string]interface{}{})
		if err != nil {
			t.Fatalf("parseCDCMessage(%s): %v", db, err)
		}
		b.add(ctx, msg, sm)
	}
	if len(b.batches) != 2 {
		t.Fatalf("open batches = %d, want 2 (one per source table)", len(b.batches))
	}
	for key, batch := range b.batches {
		b.flushBatch(ctx, key, batch, "test_flush")
	}
	return b, dest.sorted()
}

func TestCDCDBBatcherMirrorKeepsSourceDatabasesApart(t *testing.T) {
	_, got := deliverTwoDatabases(t, true, "")
	want := []string{"crm|users", "shop|users"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("writes = %v, want %v — each source database must land in its own namespace", got, want)
	}
}

func TestCDCDBBatcherWithoutMirrorCollapsesIntoConnectionDatabase(t *testing.T) {
	// The control: this is the collision the flag exists to prevent. Both tables
	// arrive bare with no namespace, so the connector writes both to config["database"].
	_, got := deliverTwoDatabases(t, false, "")
	want := []string{"|users", "|users"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("writes = %v, want %v", got, want)
	}
}

func TestCDCDBBatcherMirrorReplacesSeededNamespace(t *testing.T) {
	// A Mongo→Mongo pipeline is created with destination_namespace "mongodb"; for a
	// server-level source the orchestrator still mirrors, and the seed must not win.
	_, got := deliverTwoDatabases(t, true, "mongodb")
	want := []string{"crm|users", "shop|users"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("writes = %v, want %v", got, want)
	}
}

func TestCDCDBBatcherTypedNamespaceWithoutMirror(t *testing.T) {
	// A name typed in the destination picker sends everything to that one place
	// (the orchestrator leaves the flag off).
	_, got := deliverTwoDatabases(t, false, "analytics")
	want := []string{"analytics|users", "analytics|users"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("writes = %v, want %v", got, want)
	}
}

func TestCDCDeleteMirrorsSourceNamespace(t *testing.T) {
	// Deletes bypass the batcher (processCDCEvent); they must reach the same namespace
	// the upserts did, or a delete in crm.users would miss the row it should remove.
	dest := &recordingDBDestination{}
	cfg := &WorkerConfig{PipelineID: "p-mirror", SinkMode: "cdc", DestinationConnector: "mongodb",
		DestinationConfig: map[string]interface{}{}, MirrorSourceNamespace: true}
	msg := kafka.Message{Topic: "srv.crm.users", Offset: 3}
	payload := map[string]interface{}{
		"op":     "d",
		"before": map[string]interface{}{"id": 1},
		"source": map[string]interface{}{"db": "crm", "table": "users"},
	}
	sm, err := parseCDCMessage(cfg, msg, payload, map[string]interface{}{})
	if err != nil {
		t.Fatalf("parseCDCMessage: %v", err)
	}
	sm.PK = map[string]interface{}{"id": 1}
	sm.KeyFields = []string{"id"}
	commit, err := processCDCEvent(context.Background(), newHighWaterTracker(), nil, &http.Client{Transport: dest}, cfg,
		&DDLSupport{Enabled: false, resolved: true}, &kafka.Writer{}, nil, msg, sm, &Metrics{},
		&sync.Map{}, &sync.Map{}, &sync.Map{}, &sync.Map{}, time.Minute)
	if err != nil || !commit {
		t.Fatalf("delete not applied (commit=%v): %v", commit, err)
	}
	if got := dest.sorted(); len(got) != 1 || got[0] != "crm|users" {
		t.Fatalf("delete writes = %v, want [crm|users]", got)
	}
}
