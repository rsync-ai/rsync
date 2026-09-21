package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// fakeLoadStore is an in-memory objectLoadStore with the same rules as the Postgres
// one: a bump starts a new generation (numbering restarts at 1), a clean failure leaves
// the generation uncleaned, and no LOAD number is handed out for an uncleaned
// generation. A reservation key always maps to the same number within a generation.
type fakeLoadStore struct {
	mu      sync.Mutex
	gen     map[string]int64
	cleaned map[string]int64
	next    map[string]int64
	res     map[string]string
	cleans  int
	calls   int
}

func newFakeLoadStore() *fakeLoadStore {
	return &fakeLoadStore{gen: map[string]int64{}, cleaned: map[string]int64{}, next: map[string]int64{}, res: map[string]string{}}
}

func (f *fakeLoadStore) ensureClean(ctx context.Context, pipelineID, tableKey string, bump bool, clean func(context.Context) error) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	k := pipelineID + "|" + tableKey
	if _, ok := f.gen[k]; !ok {
		f.gen[k], f.cleaned[k], f.next[k] = 0, -1, 1
	}
	gen := f.gen[k]
	if bump {
		gen++
	}
	if f.cleaned[k] < gen {
		if err := clean(ctx); err != nil {
			return 0, err // the transaction rolls back: no bump, still uncleaned
		}
		f.cleans++
		f.cleaned[k] = gen
	}
	if gen != f.gen[k] {
		f.gen[k], f.next[k] = gen, 1
	}
	return gen, nil
}

func (f *fakeLoadStore) reserveLoadSeq(ctx context.Context, pipelineID, tableKey, reservationKey, executionID string, keyFn func(int64) (string, error)) (int64, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	k := pipelineID + "|" + tableKey
	gen, ok := f.gen[k]
	if !ok {
		return 0, "", fmt.Errorf("layout v2 table %s has no LOAD counter", tableKey)
	}
	if f.cleaned[k] < gen {
		return 0, "", fmt.Errorf("layout v2 table %s generation %d is not cleaned yet", tableKey, gen)
	}
	rk := fmt.Sprintf("%s|%d|%s", k, gen, reservationKey)
	if key, ok := f.res[rk]; ok {
		return 0, key, nil
	}
	key, err := keyFn(f.next[k])
	if err != nil {
		return 0, "", err
	}
	f.res[rk] = key
	f.next[k]++
	return f.next[k] - 1, key, nil
}

// toolRecorder is a fake destination MCP server that records every tool call.
type toolRecorder struct {
	mu    sync.Mutex
	calls []recordedCall
}

type recordedCall struct {
	tool string
	args map[string]interface{}
}

func (r *toolRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	var body struct {
		Params struct {
			Name      string                 `json:"name"`
			Arguments map[string]interface{} `json:"arguments"`
		} `json:"params"`
	}
	raw, _ := io.ReadAll(req.Body)
	_ = json.Unmarshal(raw, &body)
	r.mu.Lock()
	r.calls = append(r.calls, recordedCall{tool: body.Params.Name, args: body.Params.Arguments})
	r.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":1,"result":{"success":true}}`)),
		Request:    req,
	}, nil
}

func (r *toolRecorder) named(suffix string) []recordedCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []recordedCall
	for _, c := range r.calls {
		if strings.HasSuffix(c.tool, suffix) {
			out = append(out, c)
		}
	}
	return out
}

const v2TestPipelineID = "883cae79-cb1b-4541-a2c1-3520f3d3e5b6"

func v2BatchConfig() (*WorkerConfig, *SinkMessage) {
	cfg := &WorkerConfig{
		PipelineID:           v2TestPipelineID,
		DestinationConnector: "gcs",
		DestinationConfig:    map[string]interface{}{"bucket": "b", "path_prefix": "exports", "compression": "gz"},
	}
	sm := &SinkMessage{
		PipelineID:  v2TestPipelineID,
		ExecutionID: "exec-1",
		Table:       "public.users",
		ObjectLayout: &objectLayoutV2Msg{
			PipelinePrefix: "sales", SourceFamily: "postgresql",
			Database: "datingapp", Schema: "public", Table: "users", Dt: "2026-09-18",
		},
	}
	return cfg, sm
}

func TestParseObjectLayoutV2Msg(t *testing.T) {
	if l, err := parseObjectLayoutV2Msg(map[string]interface{}{"table": "x"}); l != nil || err != nil {
		t.Fatalf("no block must mean layout v1: got %+v, %v", l, err)
	}
	l, err := parseObjectLayoutV2Msg(map[string]interface{}{"object_layout": map[string]interface{}{
		"version": float64(2), "pipeline_prefix": " sales ", "source_family": "postgresql",
		"database": "datingapp", "schema": "public", "table": "users", "dt": "2026-09-18",
	}})
	if err != nil || l == nil {
		t.Fatalf("a version 2 block was refused: %v", err)
	}
	want := objectLayoutV2Msg{PipelinePrefix: "sales", SourceFamily: "postgresql", Database: "datingapp", Schema: "public", Table: "users", Dt: "2026-09-18"}
	if *l != want {
		t.Fatalf("parsed %+v, want %+v", *l, want)
	}
	for _, v := range []interface{}{float64(1), float64(3), nil} {
		if _, err := parseObjectLayoutV2Msg(map[string]interface{}{"object_layout": map[string]interface{}{"version": v}}); err == nil {
			t.Errorf("version %v was accepted; a writer must not guess a layout", v)
		}
	}
}

func TestObjectLayoutV2ParquetCodec(t *testing.T) {
	for in, want := range map[string]string{
		"": "none", "none": "none", "gz": "gzip", "GZIP": "gzip", "snappy": "snappy",
		"zstd": "zstd", "bzip2": "snappy", "lzma": "snappy",
	} {
		if got := objectLayoutV2ParquetCodec(in); got != want {
			t.Errorf("codec(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestObjectLayoutV2RenameDtColumns(t *testing.T) {
	rows := []map[string]interface{}{{"id": 1, "dt": "2020-01-01", "DT": "x", "dt_source": "y"}}
	types := map[string]string{"id": "int", "dt": "date", "DT": "text", "dt_source": "text"}
	keys := []string{"id", "dt"}
	objectLayoutV2RenameDtColumns(rows, types, keys)

	// Spellings go in sorted order ("DT" < "dt"); dt_source is taken by a real column.
	wantRow := map[string]interface{}{"id": 1, "dt_source": "y", "dt_source_2": "x", "dt_source_3": "2020-01-01"}
	if fmt.Sprint(rows[0]) != fmt.Sprint(wantRow) {
		t.Fatalf("row = %v, want %v", rows[0], wantRow)
	}
	wantTypes := map[string]string{"id": "int", "dt_source": "text", "dt_source_2": "text", "dt_source_3": "date"}
	if fmt.Sprint(types) != fmt.Sprint(wantTypes) {
		t.Fatalf("types = %v, want %v", types, wantTypes)
	}
	if keys[0] != "id" || keys[1] != "dt_source_3" {
		t.Fatalf("key fields = %v, want [id dt_source_3]", keys)
	}

	plain := []map[string]interface{}{{"id": 1, "created": "x"}}
	objectLayoutV2RenameDtColumns(plain, map[string]string{"id": "int"}, []string{"id"})
	if len(plain[0]) != 2 || plain[0]["created"] != "x" {
		t.Fatalf("a table with no dt column was changed: %v", plain[0])
	}
}

func TestObjectLayoutV2BatcherKeySeparatesLoadFromCDC(t *testing.T) {
	load := objectLayoutV2BatcherKey("cdc.public.users", 3, "public.users", true)
	cdc := objectLayoutV2BatcherKey("cdc.public.users", 3, "public.users", false)
	if load == cdc || !strings.HasSuffix(load, "|load") || !strings.HasSuffix(cdc, "|cdc") {
		t.Fatalf("snapshot and change batches must not share a key: %q / %q", load, cdc)
	}
}

func TestCDCObjectLayoutV2TableNames(t *testing.T) {
	cfg := &WorkerConfig{
		DestinationConfig:    map[string]interface{}{"path_prefix": "exports"},
		DestinationNamespace: "sales", SourceFamily: "postgresql", SourceDatabase: "datingapp",
	}
	fromSource := cdcObjectLayoutV2Table(cfg, &SinkMessage{Table: "x.y", SourceDB: "db1", SourceSchema: "s1", SourceBareTable: "t1"})
	if fromSource.Database != "db1" || fromSource.Schema != "s1" || fromSource.Table != "t1" || fromSource.PipelinePrefix != "sales" || fromSource.ConnPrefix != "exports" {
		t.Fatalf("source block names not used: %+v", fromSource)
	}
	fallback := cdcObjectLayoutV2Table(cfg, &SinkMessage{Table: "public.users"})
	if fallback.Database != "datingapp" || fallback.Table != "users" {
		t.Fatalf("fallbacks not used: %+v", fallback)
	}
}

func TestObjectLayoutV2BatchWriterReservesStableLoadKeys(t *testing.T) {
	ctx := context.Background()
	cfg, sm := v2BatchConfig()
	store := newFakeLoadStore()
	cleaned := objectLayoutV2Cleaned{}
	client, ct := nsTestClient()

	// The first message of a reload: one new generation and one clean, even when the
	// message is retried.
	w := newObjectLayoutV2BatchWriter(store, cleaned, client, cfg, sm, "2026-09-18", true)
	for i := 0; i < 2; i++ {
		if err := w.prepare(ctx); err != nil {
			t.Fatalf("prepare #%d: %v", i+1, err)
		}
	}
	if store.cleans != 1 || store.gen[v2TestPipelineID+"|datingapp/public/users"] != 1 {
		t.Fatalf("a retried reload message must bump once: cleans=%d gen=%v", store.cleans, store.gen)
	}
	var prefixes []string
	for _, b := range ct.bodies {
		var env struct {
			Params struct {
				Name      string                 `json:"name"`
				Arguments map[string]interface{} `json:"arguments"`
			} `json:"params"`
		}
		_ = json.Unmarshal(b, &env)
		if !strings.HasSuffix(env.Params.Name, "delete_prefix") {
			t.Fatalf("unexpected tool %s during prepare", env.Params.Name)
		}
		prefixes = append(prefixes, fmt.Sprint(env.Params.Arguments["prefix"]))
	}
	wantPrefixes := "[exports/sales/datingapp/public/users/ exports/sales/_rsync/datingapp/public/users/]"
	if fmt.Sprint(prefixes) != wantPrefixes {
		t.Fatalf("clean deleted %v, want %s", prefixes, wantPrefixes)
	}

	u0, err := w.unit(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := w.unit(ctx, 0)
	u1, _ := w.unit(ctx, 1)
	const first = "exports/sales/datingapp/public/users/dt=2026-09-18/LOAD00000001.parquet"
	if u0.Key != first || again.Key != first {
		t.Fatalf("unit 0 = %q then %q, want %q both times", u0.Key, again.Key, first)
	}
	if u1.Key != "exports/sales/datingapp/public/users/dt=2026-09-18/LOAD00000002.parquet" {
		t.Fatalf("unit 1 = %q", u1.Key)
	}
	if strings.Contains(u0.Key, v2TestPipelineID) || strings.Contains(u0.Key, slugify(v2TestPipelineID)) {
		t.Fatalf("layout v2 key %q carries the pipeline id", u0.Key)
	}
	if u0.Compression != "gzip" || u0.Metadata["rsync_pipeline_id"] != v2TestPipelineID {
		t.Fatalf("descriptor = %+v", u0)
	}

	// The next message of the same run: no clean (cached), numbering continues.
	sm2 := *sm
	sm2.BatchOffset = 1000
	w2 := newObjectLayoutV2BatchWriter(store, cleaned, client, cfg, &sm2, "2026-09-18", false)
	calls := store.calls
	if err := w2.prepare(ctx); err != nil {
		t.Fatal(err)
	}
	if store.calls != calls {
		t.Fatal("a table already cleaned in this process went back to the store")
	}
	if u, _ := w2.unit(ctx, 0); u == nil || !strings.HasSuffix(u.Key, "/LOAD00000003.parquet") {
		t.Fatalf("next message's first unit = %+v, want LOAD00000003", u)
	}

	// A later reload: new generation, cleaned again, numbering restarts.
	sm3 := *sm
	sm3.ExecutionID = "exec-2"
	w3 := newObjectLayoutV2BatchWriter(store, cleaned, client, cfg, &sm3, "2026-09-19", true)
	if err := w3.prepare(ctx); err != nil {
		t.Fatal(err)
	}
	if store.cleans != 2 {
		t.Fatalf("a reload must clean the folder again: cleans=%d", store.cleans)
	}
	if u, _ := w3.unit(ctx, 0); u == nil || u.Key != "exports/sales/datingapp/public/users/dt=2026-09-19/LOAD00000001.parquet" {
		t.Fatalf("reload's first unit = %+v, want LOAD00000001 under dt=2026-09-19", u)
	}
}

func TestObjectLayoutV2BatchWriterFailsClosed(t *testing.T) {
	ctx := context.Background()
	client, _ := nsTestClient()

	cfg, sm := v2BatchConfig()
	sm.ObjectLayout.PipelinePrefix = "Sales"
	store := newFakeLoadStore()
	w := newObjectLayoutV2BatchWriter(store, objectLayoutV2Cleaned{}, client, cfg, sm, "2026-09-18", false)
	var le *objectLayoutV2Error
	if err := w.prepare(ctx); !errors.As(err, &le) || le.Code != "pipeline_prefix_invalid" {
		t.Fatalf("prepare with an invalid prefix = %v, want pipeline_prefix_invalid", err)
	}
	if store.calls != 0 {
		t.Fatal("an invalid name reached the store")
	}

	cfg, sm = v2BatchConfig()
	w = newObjectLayoutV2BatchWriter(store, objectLayoutV2Cleaned{}, client, cfg, sm, "2026-9-18", false)
	if err := w.prepare(ctx); !errors.As(err, &le) || le.Code != "dt_invalid" {
		t.Fatalf("prepare with a bad dt = %v, want dt_invalid", err)
	}

	// A failed folder clean: nothing is cached and no LOAD number is handed out.
	cleaned := objectLayoutV2Cleaned{}
	w = newObjectLayoutV2BatchWriter(store, cleaned, client, cfg, sm, "2026-09-18", false)
	w.clean = func(context.Context) error { return errors.New("delete_prefix: listing incomplete") }
	if err := w.prepare(ctx); err == nil {
		t.Fatal("prepare succeeded although the folder clean failed")
	}
	if cleaned["datingapp/public/users"] {
		t.Fatal("a failed clean was cached as done")
	}
	if u, err := w.unit(ctx, 0); err == nil {
		t.Fatalf("a LOAD key %q was reserved in an uncleaned folder", u.Key)
	}
}

func TestWriteToDestinationLayoutV2UsesReservedKey(t *testing.T) {
	client, ct := nsTestClient()
	cfg, sm := v2BatchConfig()
	cfg.DestinationConfig["file_format"] = "jsonl"
	rows := []map[string]interface{}{{"id": 1, "name": "alice"}}
	v2 := &objectV2Write{
		Key:         "exports/sales/datingapp/public/users/dt=2026-09-18/LOAD00000007.parquet",
		Compression: "gzip",
		Metadata:    map[string]interface{}{"rsync_pipeline_id": v2TestPipelineID},
	}
	_, key, err := writeToDestination(context.Background(), client, cfg, nil, sm, rows, "", "", v2)
	if err != nil {
		t.Fatalf("writeToDestination: %v", err)
	}
	tool, args := ct.lastArgs(t)
	if !strings.HasSuffix(tool, "import_data") {
		t.Fatalf("tool = %s, want import_data", tool)
	}
	if key != v2.Key || args["key"] != v2.Key {
		t.Fatalf("key = %q / %v, want the reserved %q", key, args["key"], v2.Key)
	}
	if args["format"] != "parquet" || args["file_format"] != "parquet" || args["compression"] != "gzip" {
		t.Fatalf("format=%v file_format=%v compression=%v, want parquet/parquet/gzip (the destination's jsonl must not win)",
			args["format"], args["file_format"], args["compression"])
	}
	md, _ := args["object_metadata"].(map[string]interface{})
	if md["rsync_pipeline_id"] != v2TestPipelineID {
		t.Fatalf("object_metadata = %v", args["object_metadata"])
	}
}

func v2CDCBatcher(t *testing.T, store objectLoadStore) (*cdcObjectBatcher, *toolRecorder) {
	t.Helper()
	rec := &toolRecorder{}
	b, _ := objectBatcherFor(t, map[string]interface{}{"bucket": "b", "path_prefix": "exports", "file_format": "jsonl"}, nil, rec)
	b.cfg.StorageLayoutVersion = 2
	b.cfg.DestinationNamespace = "sales"
	b.cfg.SourceFamily = "postgresql"
	b.cfg.SourceDatabase = "datingapp"
	b.v2 = objectLayoutV2CDCEnabled(b.cfg)
	if !b.v2 {
		t.Fatal("a gcs sink with storage_layout_version=2 must write layout v2")
	}
	b.store = store
	return b, rec
}

func v2CDCMessage(offset int64, snapshot bool, ts int64) (kafka.Message, *SinkMessage) {
	op := "u"
	if snapshot {
		op = "r"
	}
	return kafka.Message{Topic: "cdc.public.users", Partition: 0, Offset: offset},
		&SinkMessage{PipelineID: "p-flush-interval", Table: "public.users", CDCOp: op, IsSnapshot: snapshot, SourceTS: ts,
			SourceSchema: "public", SourceBareTable: "users",
			PK: map[string]interface{}{"id": offset}, After: map[string]interface{}{"id": offset}}
}

func TestCDCBatcherLayoutV2WritesLoadThenCDCFiles(t *testing.T) {
	ctx := t.Context()
	store := newFakeLoadStore()
	b, rec := v2CDCBatcher(t, store)
	ts := time.Date(2026, 9, 18, 10, 11, 12, 345_000_000, time.UTC).UnixMilli()

	for i, snap := range []bool{true, true, false} {
		msg, sm := v2CDCMessage(int64(10+i), snap, ts+int64(i)*1000)
		b.add(ctx, msg, sm)
	}
	// The change flushed the open snapshot batch; flush the change batch too.
	b.flushDue(ctx, time.Now().Add(time.Hour))

	imports := rec.named("import_data")
	if len(imports) != 2 {
		t.Fatalf("want 2 files (one LOAD, one CDC), got %d", len(imports))
	}
	wantKeys := []string{
		"exports/sales/datingapp/public/users/dt=2026-09-18/LOAD00000001.parquet",
		"exports/sales/datingapp/public/users/dt=2026-09-18/20260918-101114345-12.parquet",
	}
	for i, c := range imports {
		if c.args["key"] != wantKeys[i] {
			t.Errorf("file %d key = %v, want %s", i, c.args["key"], wantKeys[i])
		}
		if c.args["format"] != "parquet" {
			t.Errorf("file %d format = %v, want parquet (the destination's jsonl must not win)", i, c.args["format"])
		}
		if md, _ := c.args["object_metadata"].(map[string]interface{}); md["rsync_pipeline_id"] != "p-flush-interval" {
			t.Errorf("file %d object_metadata = %v; get_cdc_offsets filters on it", i, c.args["object_metadata"])
		}
	}
	if n := len(rec.named("delete_prefix")); n != 2 || store.cleans != 1 {
		t.Fatalf("the table folder must be cleaned once before its first file: %d delete calls, %d cleans", n, store.cleans)
	}

	// A restart redelivers the snapshot records: the same LOAD file is rewritten.
	b2, rec2 := v2CDCBatcher(t, store)
	for i := 0; i < 2; i++ {
		msg, sm := v2CDCMessage(int64(10+i), true, ts+int64(i)*1000)
		b2.add(ctx, msg, sm)
	}
	b2.flushDue(ctx, time.Now().Add(time.Hour))
	if got := rec2.named("import_data"); len(got) != 1 || got[0].args["key"] != wantKeys[0] {
		t.Fatalf("redelivered snapshot wrote %v, want the same %s", got, wantKeys[0])
	}
	if store.cleans != 1 {
		t.Fatalf("a restart must not clean an already cleaned folder: cleans=%d", store.cleans)
	}
}

func TestGetCDCOffsetsArgsLayoutV2Prefix(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID: "p", DestinationConnector: "gcs", StorageLayoutVersion: 2, DestinationNamespace: "sales",
		DestinationConfig: map[string]interface{}{"bucket": "x", "path_prefix": "exports"},
	}
	args := getCDCOffsetsArgs(cfg, "gcs")
	if args["prefix"] != "exports/sales/" || args["bucket"] != "x" || args["pipeline_id"] != "p" {
		t.Fatalf("args = %v, want prefix exports/sales/ on bucket x", args)
	}
	cfg.DestinationNamespace = "Sales!"
	if p, ok := getCDCOffsetsArgs(cfg, "gcs")["prefix"]; ok {
		t.Fatalf("an invalid pipeline prefix listed %v; the listing must stay unscoped-empty", p)
	}
	cfg.DestinationNamespace, cfg.DestinationConnector = "sales", "aws-s3"
	if args := getCDCOffsetsArgs(cfg, "aws-s3"); args["prefix"] != "exports/sales/" || args["bucket"] != "x" {
		t.Fatalf("aws-s3 args = %v, want the v2 root exports/sales/ on bucket x", args)
	}
	cfg.DestinationConnector = "azure-blob"
	if args := getCDCOffsetsArgs(cfg, "azure-blob"); args["prefix"] != "exports/sales/" || args["container"] != "x" {
		t.Fatalf("azure-blob args = %v, want the v2 root exports/sales/ in container x", args)
	}
	cfg.DestinationConnector = "minio"
	if p := getCDCOffsetsArgs(cfg, "minio")["prefix"]; p == "exports/sales/" {
		t.Fatal("minio is not a layout v2 store; it must keep the v1 prefix")
	}
}
