package executor

import (
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

const layoutTestPipeline = "11111111-1111-1111-1111-111111111111"
const layoutTestDestConn = "22222222-2222-2222-2222-222222222222"

// layoutTestInput is an eligible PostgreSQL → GCS pipeline with prefix "sales".
func layoutTestInput() ObjectLayoutInput {
	return ObjectLayoutInput{
		PipelineID:   layoutTestPipeline,
		SourceType:   "postgres",
		SourceConfig: map[string]string{"database": "datingapp"},
		DestType:     "gcs",
		Namespace:    "sales",
		DestConnID:   layoutTestDestConn,
	}
}

var (
	layoutReadVersionSQL = regexp.QuoteMeta(`SELECT storage_layout_version FROM pipelines WHERE id = $1::uuid`)
	layoutReadDestSQL    = regexp.QuoteMeta(`SELECT COALESCE(destination_connection_id::text, '') FROM pipelines WHERE id = $1::uuid`)
	layoutLockSQL        = regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext('object_layout_v2:' || $1))`)
	layoutPrefixOwnerSQL = `SELECT EXISTS \(\s*SELECT 1 FROM pipelines`
	layoutUpdateSQL      = regexp.QuoteMeta(`UPDATE pipelines SET storage_layout_version = $2 WHERE id = $1::uuid AND storage_layout_version = $3`)
)

func expectLayoutVersion(mock sqlmock.Sqlmock, v int) {
	mock.ExpectQuery(layoutReadVersionSQL).WithArgs(layoutTestPipeline).
		WillReturnRows(sqlmock.NewRows([]string{"storage_layout_version"}).AddRow(v))
}

// expectLayoutRecord expects one recordObjectLayout transaction that stores want
// over from. prefixTaken nil = no prefix-owner query (a v1 decision).
func expectLayoutRecord(mock sqlmock.Sqlmock, destConn string, prefixTaken *bool, want, from int) {
	mock.ExpectBegin()
	mock.ExpectQuery(layoutReadDestSQL).WithArgs(layoutTestPipeline).
		WillReturnRows(sqlmock.NewRows([]string{"coalesce"}).AddRow(destConn))
	if destConn != "" {
		mock.ExpectExec(layoutLockSQL).WithArgs(destConn).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	if prefixTaken != nil {
		mock.ExpectQuery(layoutPrefixOwnerSQL).WithArgs(layoutTestPipeline, destConn, "sales").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(*prefixTaken))
	}
	mock.ExpectExec(layoutUpdateSQL).WithArgs(layoutTestPipeline, want, from).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
}

func boolp(b bool) *bool { return &b }

func TestObjectLayoutV2SourceFamily(t *testing.T) {
	cases := map[string]string{
		"postgres": "postgresql", "PostgreSQL": "postgresql", "aurora-postgresql": "postgresql",
		"alloydb": "postgresql", "supabase": "postgresql",
		"mongodb": "mongodb", "mongodb_atlas": "mongodb", "atlas": "mongodb",
		"mysql": "mysql", "mariadb": "mysql", "aurora_mysql": "mysql",
		"sqlserver": "sqlserver", "mssql": "sqlserver", "sql-server": "sqlserver",
		"oracle":    "oracle",
		"snowflake": "", "salesforce": "", "gcs": "", "": "",
	}
	for in, want := range cases {
		if got := objectLayoutV2SourceFamily(in); got != want {
			t.Errorf("objectLayoutV2SourceFamily(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestObjectLayoutV2Eligibility(t *testing.T) {
	mod := func(f func(*ObjectLayoutInput)) ObjectLayoutInput {
		in := layoutTestInput()
		f(&in)
		return in
	}
	cases := []struct {
		name   string
		in     ObjectLayoutInput
		reason string
	}{
		{"eligible", layoutTestInput(), ""},
		{"GCS upper case", mod(func(in *ObjectLayoutInput) { in.DestType = " GCS " }), ""},
		{"aws-s3", mod(func(in *ObjectLayoutInput) { in.DestType = "aws-s3" }), ""},
		{"azure-blob with underscores", mod(func(in *ObjectLayoutInput) { in.DestType = "Azure_Blob" }), ""},
		{"minio stays v1", mod(func(in *ObjectLayoutInput) { in.DestType = "minio" }), "destination_not_v2"},
		{"bare s3 is not the aws-s3 connector", mod(func(in *ObjectLayoutInput) { in.DestType = "s3" }), "destination_not_v2"},
		{"google-cloud-storage is not the sink's gcs", mod(func(in *ObjectLayoutInput) { in.DestType = "google-cloud-storage" }), "destination_not_v2"},
		{"saas source", mod(func(in *ObjectLayoutInput) { in.SourceType = "hubspot" }), "source_family_unsupported"},
		{"blank prefix", mod(func(in *ObjectLayoutInput) { in.Namespace = "  " }), "pipeline_prefix_invalid"},
		{"upper-case prefix", mod(func(in *ObjectLayoutInput) { in.Namespace = "Sales" }), "pipeline_prefix_invalid"},
		{"nested prefix", mod(func(in *ObjectLayoutInput) { in.Namespace = "a/b" }), "pipeline_prefix_invalid"},
		{"64-char prefix", mod(func(in *ObjectLayoutInput) { in.Namespace = strings.Repeat("a", 64) }), "pipeline_prefix_invalid"},
		{"no source database", mod(func(in *ObjectLayoutInput) { in.SourceConfig = map[string]string{} }), "source_database_required"},
		{"mongo db_name", mod(func(in *ObjectLayoutInput) {
			in.SourceType, in.SourceConfig = "mongodb", map[string]string{"db_name": "rsync_test"}
		}), ""},
		{"oracle service_name", mod(func(in *ObjectLayoutInput) {
			in.SourceType, in.SourceConfig = "oracle", map[string]string{"service_name": "ORCLPDB1"}
		}), ""},
	}
	for _, c := range cases {
		got, reason := objectLayoutV2Eligibility(c.in)
		if reason != c.reason {
			t.Errorf("%s: reason = %q, want %q", c.name, reason, c.reason)
		}
		if wantV := map[bool]int{true: 2, false: 1}[c.reason == ""]; got.Version != wantV {
			t.Errorf("%s: version = %d, want %d", c.name, got.Version, wantV)
		}
	}
	got, _ := objectLayoutV2Eligibility(layoutTestInput())
	want := ObjectLayout{Version: 2, PipelinePrefix: "sales", SourceFamily: "postgresql", SourceDatabase: "datingapp"}
	if got != want {
		t.Errorf("eligible layout = %+v, want %+v", got, want)
	}
}

// TestResolveObjectLayout drives every mode × stored version through sqlmock. The
// expectations are strict: a case that expects no transaction fails if one runs.
func TestResolveObjectLayout(t *testing.T) {
	v2 := ObjectLayout{Version: 2, PipelinePrefix: "sales", SourceFamily: "postgresql", SourceDatabase: "datingapp"}
	v1 := ObjectLayout{Version: 1}
	mod := func(f func(*ObjectLayoutInput)) ObjectLayoutInput {
		in := layoutTestInput()
		f(&in)
		return in
	}
	ineligible := mod(func(in *ObjectLayoutInput) { in.Namespace = "" })

	cases := []struct {
		name    string
		in      ObjectLayoutInput
		mode    objectLayoutMode
		expect  func(sqlmock.Sqlmock)
		want    ObjectLayout
		wantErr string
	}{
		{"new eligible pipeline decides v2", layoutTestInput(), objectLayoutDecide, func(m sqlmock.Sqlmock) {
			expectLayoutVersion(m, 0)
			expectLayoutRecord(m, layoutTestDestConn, boolp(false), 2, 0)
		}, v2, ""},
		{"new ineligible pipeline decides v1", ineligible, objectLayoutDecide, func(m sqlmock.Sqlmock) {
			expectLayoutVersion(m, 0)
			expectLayoutRecord(m, layoutTestDestConn, nil, 1, 0)
		}, v1, ""},
		{"prefix already written by another v2 pipeline decides v1", layoutTestInput(), objectLayoutDecide, func(m sqlmock.Sqlmock) {
			expectLayoutVersion(m, 0)
			expectLayoutRecord(m, layoutTestDestConn, boolp(true), 1, 0)
		}, v1, ""},
		{"no destination connection decides v1", mod(func(in *ObjectLayoutInput) { in.DestConnID = "" }), objectLayoutDecide, func(m sqlmock.Sqlmock) {
			expectLayoutVersion(m, 0)
			expectLayoutRecord(m, "", nil, 1, 0)
		}, v1, ""},
		{"v1 pipeline stays v1 at sink start", layoutTestInput(), objectLayoutDecide, func(m sqlmock.Sqlmock) {
			expectLayoutVersion(m, 1)
		}, v1, ""},
		{"v1 pipeline moves to v2 on reload", layoutTestInput(), objectLayoutReload, func(m sqlmock.Sqlmock) {
			expectLayoutVersion(m, 1)
			expectLayoutRecord(m, layoutTestDestConn, boolp(false), 2, 1)
		}, v2, ""},
		{"ineligible v1 pipeline stays v1 on reload", ineligible, objectLayoutReload, func(m sqlmock.Sqlmock) {
			expectLayoutVersion(m, 1)
		}, v1, ""},
		{"undecided pipeline is not decided by reload", layoutTestInput(), objectLayoutReload, func(m sqlmock.Sqlmock) {
			expectLayoutVersion(m, 0)
		}, v1, ""},
		{"read never writes", layoutTestInput(), objectLayoutRead, func(m sqlmock.Sqlmock) {
			expectLayoutVersion(m, 0)
		}, v1, ""},
		{"restart fixes an undecided pipeline at v1", layoutTestInput(), objectLayoutRestart, func(m sqlmock.Sqlmock) {
			expectLayoutVersion(m, 0)
			expectLayoutRecord(m, layoutTestDestConn, nil, 1, 0)
		}, v1, ""},
		{"restart keeps v1", layoutTestInput(), objectLayoutRestart, func(m sqlmock.Sqlmock) {
			expectLayoutVersion(m, 1)
		}, v1, ""},
		{"restart keeps v2", layoutTestInput(), objectLayoutRestart, func(m sqlmock.Sqlmock) {
			expectLayoutVersion(m, 2)
		}, v2, ""},
		{"v2 pipeline reads v2", layoutTestInput(), objectLayoutRead, func(m sqlmock.Sqlmock) {
			expectLayoutVersion(m, 2)
		}, v2, ""},
		{"v2 pipeline that lost its prefix fails", ineligible, objectLayoutDecide, func(m sqlmock.Sqlmock) {
			expectLayoutVersion(m, 2)
		}, v1, "no longer eligible (pipeline_prefix_invalid)"},
		{"unknown stored version fails", layoutTestInput(), objectLayoutRead, func(m sqlmock.Sqlmock) {
			expectLayoutVersion(m, 7)
		}, v1, "unknown storage_layout_version 7"},
		{"version read error fails closed", layoutTestInput(), objectLayoutDecide, func(m sqlmock.Sqlmock) {
			m.ExpectQuery(layoutReadVersionSQL).WillReturnError(errors.New("conn reset"))
		}, v1, "read storage_layout_version"},
		{"prefix-owner check error fails closed", layoutTestInput(), objectLayoutDecide, func(m sqlmock.Sqlmock) {
			expectLayoutVersion(m, 0)
			m.ExpectBegin()
			m.ExpectQuery(layoutReadDestSQL).WillReturnRows(sqlmock.NewRows([]string{"coalesce"}).AddRow(layoutTestDestConn))
			m.ExpectExec(layoutLockSQL).WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectQuery(layoutPrefixOwnerSQL).WillReturnError(errors.New("conn reset"))
			m.ExpectRollback()
		}, v1, "check prefix owner"},
		{"a concurrent decision wins the race", layoutTestInput(), objectLayoutDecide, func(m sqlmock.Sqlmock) {
			expectLayoutVersion(m, 0)
			m.ExpectBegin()
			m.ExpectQuery(layoutReadDestSQL).WillReturnRows(sqlmock.NewRows([]string{"coalesce"}).AddRow(layoutTestDestConn))
			m.ExpectExec(layoutLockSQL).WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectQuery(layoutPrefixOwnerSQL).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
			m.ExpectExec(layoutUpdateSQL).WithArgs(layoutTestPipeline, 2, 0).WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectRollback()
			expectLayoutVersion(m, 1) // the other run recorded v1
		}, v1, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()
			c.expect(mock)
			got, err := resolveObjectLayout(context.Background(), db, c.in, c.mode)
			if c.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)) {
				t.Fatalf("error = %v, want one containing %q", err, c.wantErr)
			}
			if got != c.want {
				t.Errorf("layout = %+v, want %+v", got, c.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("sql expectations: %v", err)
			}
		})
	}
}

// TestResolveObjectLayoutLeavesOtherDestinationsAlone: a non-v2 pipeline never
// reads or writes the column (no expectation = any query fails the mock).
func TestResolveObjectLayoutLeavesOtherDestinationsAlone(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	for _, dest := range []string{"minio", "s3", "mongodb", "snowflake", "google-cloud-storage"} {
		in := layoutTestInput()
		in.DestType = dest
		for _, mode := range []objectLayoutMode{objectLayoutDecide, objectLayoutReload, objectLayoutRead, objectLayoutRestart} {
			got, err := resolveObjectLayout(context.Background(), db, in, mode)
			if err != nil || got.Version != 1 {
				t.Errorf("%s mode %d: got %+v, %v; want v1, nil", dest, mode, got, err)
			}
		}
	}
	if got, err := resolveObjectLayout(context.Background(), nil, layoutTestInput(), objectLayoutDecide); err != nil || got.Version != 1 {
		t.Errorf("nil db: got %+v, %v; want v1, nil", got, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sql expectations: %v", err)
	}
	for _, dest := range []string{"gcs", "aws-s3", "azure-blob"} {
		if !ObjectLayoutAppliesTo(dest) {
			t.Errorf("ObjectLayoutAppliesTo(%q) = false, want true", dest)
		}
	}
	if ObjectLayoutAppliesTo("minio") {
		t.Errorf("ObjectLayoutAppliesTo(minio) = true; minio stays v1")
	}
}

// TestObjectLayoutV2DestSupportedMatchesGolden pins the v2 destination list to the
// v2_destinations block the sink and the frontend also read.
func TestObjectLayoutV2DestSupportedMatchesGolden(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "shared", "object_layout_golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v (run from a full repo checkout)", err)
	}
	var golden struct {
		V2Destinations *struct {
			Eligible    []string `json:"eligible"`
			NotEligible []string `json:"not_eligible"`
		} `json:"v2_destinations"`
	}
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	d := golden.V2Destinations
	if d == nil || len(d.Eligible) == 0 || len(d.NotEligible) == 0 {
		t.Fatal("golden has no v2_destinations eligible / not_eligible lists")
	}
	for _, c := range d.Eligible {
		if !objectLayoutV2DestSupported(c) {
			t.Errorf("objectLayoutV2DestSupported(%q) = false, golden says eligible", c)
		}
	}
	for _, c := range d.NotEligible {
		if objectLayoutV2DestSupported(c) {
			t.Errorf("objectLayoutV2DestSupported(%q) = true, golden says not eligible", c)
		}
	}
}

func TestObjectLayoutV2Message(t *testing.T) {
	pg := ObjectLayout{Version: 2, PipelinePrefix: "sales", SourceFamily: "postgresql", SourceDatabase: "datingapp"}
	mongo := ObjectLayout{Version: 2, PipelinePrefix: "sales", SourceFamily: "mongodb", SourceDatabase: "rsync_test"}
	mysql := ObjectLayout{Version: 2, PipelinePrefix: "sales", SourceFamily: "mysql", SourceDatabase: "shop"}
	mssql := ObjectLayout{Version: 2, PipelinePrefix: "sales", SourceFamily: "sqlserver", SourceDatabase: "erp"}
	oracle := ObjectLayout{Version: 2, PipelinePrefix: "sales", SourceFamily: "oracle", SourceDatabase: "ORCLPDB1"}

	cases := []struct {
		name            string
		l               ObjectLayout
		cfg             map[string]string
		table           string
		db, schema, tbl string
	}{
		{"pg schema.table", pg, nil, "public.users", "datingapp", "public", "users"},
		{"pg bare table", pg, nil, "users", "datingapp", "public", "users"},
		{"pg db.schema.table", pg, nil, "datingapp.crm.leads", "datingapp", "crm", "leads"},
		{"mongo db.collection", mongo, nil, "rsync_test.users", "rsync_test", "", "users"},
		{"mongo dotted collection", mongo, nil, "fs.files", "rsync_test", "", "fs.files"},
		{"mysql db.table", mysql, nil, "shop.orders", "shop", "", "orders"},
		{"mysql bare table", mysql, nil, "orders", "shop", "", "orders"},
		{"sqlserver bare table", mssql, nil, "Invoices", "erp", "dbo", "Invoices"},
		{"oracle bare table", oracle, map[string]string{"username": "scott"}, "EMP", "ORCLPDB1", "SCOTT", "EMP"},
	}
	for _, c := range cases {
		msg, err := objectLayoutV2Message(c.l, c.cfg, c.table, "2026-09-18")
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		want := map[string]interface{}{
			"version": 2, "pipeline_prefix": "sales", "source_family": c.l.SourceFamily,
			"database": c.db, "schema": c.schema, "table": c.tbl, "dt": "2026-09-18",
		}
		for k, v := range want {
			if msg[k] != v {
				t.Errorf("%s: %s = %v, want %v", c.name, k, msg[k], v)
			}
		}
		if len(msg) != len(want) {
			t.Errorf("%s: message has %d keys, want %d: %v", c.name, len(msg), len(want), msg)
		}
	}

	if msg, err := objectLayoutV2Message(ObjectLayout{Version: 1}, nil, "public.users", "2026-09-18"); msg != nil || err != nil {
		t.Errorf("v1: got %v, %v; want nil, nil", msg, err)
	}
	for _, bad := range []struct {
		l         ObjectLayout
		table, dt string
		code      string
	}{
		{pg, "public.users", "2026-9-18", "dt_invalid"},
		{pg, "public.users", "", "dt_invalid"},
		{pg, "public.", "2026-09-18", "table_required"},
		{oracle, "EMP", "2026-09-18", "schema_required"}, // no username → no default schema
	} {
		msg, err := objectLayoutV2Message(bad.l, nil, bad.table, bad.dt)
		if msg != nil || err == nil || !strings.HasSuffix(err.Error(), bad.code) {
			t.Errorf("%s %q dt=%q: got %v, %v; want an error ending %q", bad.l.SourceFamily, bad.table, bad.dt, msg, err, bad.code)
		}
	}
}

func TestObjectLayoutSinkConfigFields(t *testing.T) {
	v1 := ObjectLayout{Version: 1}.SinkConfigFields()
	if v1["storage_layout_version"] != 1 || v1["source_family"] != "" || v1["source_database"] != "" || len(v1) != 3 {
		t.Errorf("v1 fields = %v", v1)
	}
	// An undecided value never reaches the sink as 0: the sink reads 0 as v1 too,
	// but only 1 and 2 are sent.
	if f := (ObjectLayout{}).SinkConfigFields(); f["storage_layout_version"] != 1 {
		t.Errorf("zero layout sends storage_layout_version %v, want 1", f["storage_layout_version"])
	}
	v2 := ObjectLayout{Version: 2, PipelinePrefix: "sales", SourceFamily: "mongodb", SourceDatabase: "rsync_test"}.SinkConfigFields()
	if v2["storage_layout_version"] != 2 || v2["source_family"] != "mongodb" || v2["source_database"] != "rsync_test" || len(v2) != 3 {
		t.Errorf("v2 fields = %v", v2)
	}
}

func TestObjectLayoutInputFor(t *testing.T) {
	task := ExecutorTask{
		PipelineID:  layoutTestPipeline,
		Source:      &ConnectorConfig{Type: "mongodb", Config: map[string]string{"database": "rsync_test"}},
		Destination: &ConnectorConfig{Type: "gcs"},
		Params:      map[string]interface{}{"destination_connection_id": "auto"},
		Payload:     map[string]interface{}{"destination_connection_id": " " + layoutTestDestConn + " "},
	}
	in := objectLayoutInputFor(task, "sales")
	if in.PipelineID != layoutTestPipeline || in.SourceType != "mongodb" || in.SourceConfig["database"] != "rsync_test" ||
		in.DestType != "gcs" || in.Namespace != "sales" || in.DestConnID != layoutTestDestConn {
		t.Errorf("objectLayoutInputFor = %+v", in)
	}
	if in := objectLayoutInputFor(ExecutorTask{PipelineID: layoutTestPipeline}, ""); in.SourceType != "" || in.DestType != "" {
		t.Errorf("nil source/destination: %+v", in)
	}
}

// TestObjectLayoutWiring pins the call sites: the sink start decides and sends the
// fields, a batch reload may move v1 → v2 before the sink starts, and every batch
// and EOF message of a v2 table carries object_layout.
func TestObjectLayoutWiring(t *testing.T) {
	fset := token.NewFileSet()
	src, err := os.ReadFile("executor.go")
	if err != nil {
		t.Fatalf("read executor.go: %v", err)
	}
	f, err := parser.ParseFile(fset, "executor.go", src, 0)
	if err != nil {
		t.Fatalf("parse executor.go: %v", err)
	}
	body := func(name string) string {
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == name {
				return string(src[fset.Position(fd.Body.Pos()).Offset:fset.Position(fd.Body.End()).Offset])
			}
		}
		t.Fatalf("%s not found in executor.go; if it moved or was renamed, update this test", name)
		return ""
	}

	sink := body("startKafkaMCPSink")
	decide := strings.Index(sink, "objectLayoutDecide")
	send := strings.Index(sink, "objectLayout.SinkConfigFields()")
	call := strings.Index(sink, "a.executeWithOAuthRetry(ctx, sinkReq)")
	if decide < 0 || send < 0 || call < 0 {
		t.Fatalf("startKafkaMCPSink must resolve the layout (objectLayoutDecide) and send objectLayout.SinkConfigFields() with sinkReq")
	}
	if !(decide < send && send < call) {
		t.Errorf("startKafkaMCPSink must decide the layout, then add its fields to sinkReq, then send sinkReq")
	}

	batch := body("executeBatchDataTransfer")
	reload := strings.Index(batch, "objectLayoutReload")
	sinkStart := strings.Index(batch, "a.startKafkaMCPSink(")
	read := strings.Index(batch, "objectLayoutRead")
	if reload < 0 || sinkStart < 0 || reload > sinkStart {
		t.Errorf("executeBatchDataTransfer must run objectLayoutReload before a.startKafkaMCPSink")
	}
	if read < sinkStart {
		t.Errorf("executeBatchDataTransfer must read the layout (objectLayoutRead) after the sink start decided it")
	}
	if n := strings.Count(batch, `["object_layout"] = objectLayoutMsg`); n != 2 {
		t.Errorf("executeBatchDataTransfer sets object_layout on %d messages, want 2 (claim-check batch and EOF)", n)
	}
	if n := strings.Count(batch, "runMode, objectLayoutMsg)"); n != 2 {
		t.Errorf("executeBatchDataTransfer passes objectLayoutMsg to %d sendChunkedToKafka calls, want 2", n)
	}
	if !strings.Contains(body("sendChunkedToKafka"), `message["object_layout"] = objectLayout`) {
		t.Errorf("sendChunkedToKafka must put object_layout on every inline batch message")
	}
}
