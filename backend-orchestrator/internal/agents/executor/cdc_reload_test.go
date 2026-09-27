package executor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/rsync-ai/backend-orchestrator/internal/cdcsnapshot"
	"github.com/rsync-ai/backend-orchestrator/internal/storage"
)

// A CDC Reload re-reads every captured table through the Re-snapshot queue, for
// every Debezium source; it used to restart the connector with its old offsets,
// so Debezium skipped the snapshot and the run read "completed" with nothing
// re-read.

const reloadPipeline = "44444444-4444-4444-4444-444444444444"

var reloadDefaultModeSQL = regexp.QuoteMeta(`SELECT COALESCE(default_run_mode,'') FROM pipelines`)

func TestTaskRunModePrecedence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		params  map[string]interface{}
		payload map[string]interface{}
		dbMode  *string // nil: no DB query expected
		want    storage.RunMode
	}{
		{"params win", map[string]interface{}{"run_mode": "reload"}, map[string]interface{}{"run_mode": "resume"}, nil, storage.RunModeReload},
		{"payload when params have none", nil, map[string]interface{}{"run_mode": "RELOAD"}, nil, storage.RunModeReload},
		{"blank request mode falls through to the pipeline default", map[string]interface{}{"run_mode": "  "}, nil, reloadStr("reload"), storage.RunModeReload},
		{"pipeline default resume", nil, nil, reloadStr("resume"), storage.RunModeResume},
		{"no mode anywhere is resume", nil, nil, reloadStr(""), storage.RunModeResume},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if tc.dbMode != nil {
				mock.ExpectQuery(reloadDefaultModeSQL).WithArgs(reloadPipeline).
					WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(*tc.dbMode))
			}
			task := ExecutorTask{PipelineID: reloadPipeline, Params: tc.params, Payload: tc.payload}
			if got := taskRunMode(context.Background(), db, task); got != tc.want {
				t.Fatalf("run mode = %q, want %q", got, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTaskRunModeWithoutDBIsResume(t *testing.T) {
	if got := taskRunMode(context.Background(), nil, ExecutorTask{PipelineID: reloadPipeline}); got != storage.RunModeResume {
		t.Fatalf("run mode = %q, want resume", got)
	}
}

func TestCDCReloadNeeded(t *testing.T) {
	reload := func() storage.RunMode { return storage.RunModeReload }
	resume := func() storage.RunMode { return storage.RunModeResume }
	for _, tc := range []struct {
		name        string
		incremental bool
		provider    string
		mode        func() storage.RunMode
		want        bool
	}{
		{"debezium reload", false, "debezium", reload, true},
		{"debezium resume", false, "debezium", resume, false},
		{"incremental snapshot already re-reads", true, "debezium", reload, false},
		{"not a debezium source", false, "hybrid-poll", reload, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := cdcReloadNeeded(tc.incremental, tc.provider, tc.mode); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCDCReloadTables(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cfg   map[string]interface{}
		start []string
		want  []string
	}{
		{"postgres include list, escaped dots", map[string]interface{}{"table.include.list": `public\.orders, public\.customers`}, nil,
			[]string{"public.orders", "public.customers"}},
		{"mongodb collection list", map[string]interface{}{"collection.include.list": "shop.orders,shop.users"}, nil,
			[]string{"shop.orders", "shop.users"}},
		{"config without a list falls back to start_sync's", map[string]interface{}{}, []string{`public\.orders`},
			[]string{"public.orders"}},
		{"pattern names no table, fallback used", map[string]interface{}{"table.include.list": "public\\..*"}, []string{"public.orders"},
			[]string{"public.orders"}},
		{"patterns everywhere is nil", map[string]interface{}{"table.include.list": "public\\..*"}, []string{"public.*"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := cdcReloadTables(tc.cfg, tc.start)
			if strings.Join(got, "|") != strings.Join(tc.want, "|") {
				t.Fatalf("tables = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestHasKafkaSignalChannel(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  map[string]interface{}
		want bool
	}{
		{"kafka channel with topic", map[string]interface{}{"signal.kafka.topic": "rsync.signal.x", "signal.enabled.channels": "source,kafka"}, true},
		{"topic but kafka channel off", map[string]interface{}{"signal.kafka.topic": "rsync.signal.x", "signal.enabled.channels": "source"}, false},
		{"channel but no topic", map[string]interface{}{"signal.enabled.channels": "kafka"}, false},
		{"nothing", map[string]interface{}{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasKafkaSignalChannel(tc.cfg); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// reloadConnect serves one connector config on GET /connectors/<name>/config.
func reloadConnect(t *testing.T, cfg string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/connectors/cdc-44444444/config" {
			_, _ = w.Write([]byte(cfg))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("KAFKA_CONNECT_URL", srv.URL)
}

const reloadSignalCfg = `{"table.include.list":"public\\.orders,public\\.customers",` +
	`"signal.kafka.topic":"rsync.signal.cdc-44444444","signal.enabled.channels":"source,kafka"}`

var (
	reloadLayoutSQL = regexp.QuoteMeta(`SELECT storage_layout_version FROM pipelines`)
	reloadInsertSQL = regexp.QuoteMeta(`INSERT INTO cdc_snapshot_requests`)
)

func reloadRequestRow(cleans bool) *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows(hybridRequestCols).
		AddRow("reload-1", reloadPipeline, "cdc-44444444", "blocking", `["public.orders","public.customers"]`, "resnapshot", "pending",
			0, "[]", "", cleans, now, now, nil, nil, nil, nil, nil)
}

func TestQueueCDCReloadQueuesBlockingResnapshot(t *testing.T) {
	for _, tc := range []struct {
		name     string
		destType string
		layout   interface{} // nil: no layout query expected
		cleans   bool
	}{
		{"gcs layout v2 cleans each folder", "gcs", int64(2), true},
		{"gcs layout v1 keeps the folder", "gcs", int64(1), false},
		{"database destination never queries the layout", "mongodb", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reloadConnect(t, reloadSignalCfg)
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if tc.layout != nil {
				mock.ExpectQuery(reloadLayoutSQL).WithArgs(reloadPipeline).
					WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(tc.layout))
			}
			mock.ExpectQuery(reloadInsertSQL).
				WithArgs(reloadPipeline, "cdc-44444444", "blocking", `["public.orders","public.customers"]`,
					cdcsnapshot.SourceResnapshot, tc.cleans, sqlmock.AnyArg()).
				WillReturnRows(reloadRequestRow(tc.cleans))

			a := &Agent{db: db}
			task := ExecutorTask{PipelineID: reloadPipeline, Destination: &ConnectorConfig{Type: tc.destType}}
			r, err := a.queueCDCReload(context.Background(), task, "cdc-44444444", nil)
			if err != nil {
				t.Fatalf("queueCDCReload: %v", err)
			}
			if r.ID != "reload-1" || r.CleansFolder != tc.cleans {
				t.Fatalf("request = %+v", r)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestQueueCDCReloadRefusesWhatCannotReRead(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cfg     string
		wantErr string
	}{
		{"no kafka signal channel", `{"table.include.list":"public\\.orders"}`, "no Kafka signal channel"},
		{"include list is a pattern", `{"table.include.list":"public\\..*","signal.kafka.topic":"t","signal.enabled.channels":"kafka"}`,
			"names no plain table"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reloadConnect(t, tc.cfg)
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			a := &Agent{db: db}
			_, err = a.queueCDCReload(context.Background(), ExecutorTask{PipelineID: reloadPipeline}, "cdc-44444444", nil)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
			// sqlmock refuses any unexpected statement: nothing was queued.
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestQueueCDCReloadWithoutQueueTable(t *testing.T) {
	reloadConnect(t, reloadSignalCfg)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(reloadInsertSQL).
		WillReturnError(errors.New(`pq: relation "cdc_snapshot_requests" does not exist`))
	a := &Agent{db: db}
	_, err = a.queueCDCReload(context.Background(), ExecutorTask{PipelineID: reloadPipeline}, "cdc-44444444", nil)
	if err == nil || !strings.Contains(err.Error(), "migration 113") {
		t.Fatalf("err = %v, want the migration 113 message", err)
	}
}

func TestQueueCDCReloadConnectUnreachable(t *testing.T) {
	t.Setenv("KAFKA_CONNECT_URL", "http://127.0.0.1:1")
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &Agent{db: db}
	if _, err := a.queueCDCReload(context.Background(), ExecutorTask{PipelineID: reloadPipeline}, "cdc-44444444", nil); err == nil ||
		!strings.Contains(err.Error(), "connector config") {
		t.Fatalf("err = %v, want a config read failure", err)
	}
}

func reloadStr(s string) *string { return &s }
