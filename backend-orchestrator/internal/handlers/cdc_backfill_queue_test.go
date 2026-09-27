package handlers

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

var snapshotRequestCols = []string{"id", "pipeline_id", "connector_name", "mode", "tables", "source", "status",
	"attempts", "completed_tables", "last_error", "cleans_folder",
	"not_before", "requested_at", "sent_at", "last_sent_at", "started_at", "last_progress_at", "completed_at"}

func postQueued(t *testing.T, cfg map[string]interface{}, destType string, layout int, body string, expectInsert func(sqlmock.Sqlmock)) (int, map[string]interface{}, *fakeSignalProducer, sqlmock.Sqlmock) {
	t.Helper()
	signals := &fakeSignalProducer{}
	h, r := backfillHarness(t, cfg, signals, true)
	h.mock.MatchExpectationsInOrder(false)
	h.mock.ExpectQuery("SELECT source_connection_id").
		WithArgs(capabilityTestPipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"source_connection_id"}).AddRow("11111111-1111-1111-1111-111111111111"))
	h.mock.ExpectQuery("SELECT c.connector_type").
		WithArgs(capabilityTestPipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"connector_type"}).AddRow(destType))
	if layout > 0 {
		h.mock.ExpectQuery("SELECT storage_layout_version").
			WithArgs(capabilityTestPipelineID).
			WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(layout))
	}
	expectInsert(h.mock)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/cdc/pipelines/"+capabilityTestPipelineID+"/backfill", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	var out map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w.Code, out, signals, h.mock
}

// Over the Kafka channel the POST queues the request and sends nothing: the
// dispatcher sends it once the connector's task captures the table. A GCS
// layout-v2 destination turns an incremental request into a blocking one that
// empties the table's folder first.
func TestBackfillQueuesObjectStorageRequestAsBlockingAndCleaning(t *testing.T) {
	var before time.Time
	code, body, signals, mock := postQueued(t, cfgPostgresIncremental, "gcs", 2,
		`{"tables":["public.users"],"mode":"incremental","source":"table_edit"}`,
		func(m sqlmock.Sqlmock) {
			before = time.Now()
			m.ExpectQuery(regexp.QuoteMeta("INSERT INTO cdc_snapshot_requests")).
				WithArgs(capabilityTestPipelineID, "cdc-637a0df2", "blocking", `["public.users"]`, "table_edit", true, afterArg{&before, 14 * time.Second}).
				WillReturnRows(sqlmock.NewRows(snapshotRequestCols).AddRow("req-1", capabilityTestPipelineID, "cdc-637a0df2", "blocking",
					`["public.users"]`, "table_edit", "queued", 0, `[]`, "", true, time.Now(), time.Now(), nil, nil, nil, nil, nil))
		})
	if code != http.StatusOK {
		t.Fatalf("status = %d (body %v)", code, body)
	}
	if body["status"] != "queued" || body["request_id"] != "req-1" || body["snapshot_mode"] != "blocking" || body["cleans_folder"] != true {
		t.Fatalf("body = %v", body)
	}
	if msg, _ := body["message"].(string); !strings.Contains(msg, "deleted") {
		t.Fatalf("a cleaning re-snapshot must say the files are deleted: %q", msg)
	}
	if len(signals.sent) != 0 {
		t.Fatalf("the POST sent %d signal(s); the dispatcher sends them", len(signals.sent))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Control: a database destination keeps the requested mode and cleans nothing,
// and a Re-snapshot is due at once (no not_before delay).
func TestBackfillQueuesDatabaseRequestUnchanged(t *testing.T) {
	var before time.Time
	code, body, _, mock := postQueued(t, cfgPostgresIncremental, "bigquery", 0,
		`{"tables":["public.users"],"mode":"incremental"}`,
		func(m sqlmock.Sqlmock) {
			before = time.Now()
			m.ExpectQuery(regexp.QuoteMeta("INSERT INTO cdc_snapshot_requests")).
				WithArgs(capabilityTestPipelineID, "cdc-637a0df2", "incremental", `["public.users"]`, "resnapshot", false, beforeArg{&before, 5 * time.Second}).
				WillReturnRows(sqlmock.NewRows(snapshotRequestCols).AddRow("req-2", capabilityTestPipelineID, "cdc-637a0df2", "incremental",
					`["public.users"]`, "resnapshot", "queued", 0, `[]`, "", false, time.Now(), time.Now(), nil, nil, nil, nil, nil))
		})
	if code != http.StatusOK || body["snapshot_mode"] != "incremental" || body["cleans_folder"] != false {
		t.Fatalf("got %d %v", code, body)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Without the queue table (migration 113 not applied) the POST still works: it
// sends directly, as before the queue existed.
func TestBackfillFallsBackToDirectSendWithoutTheQueue(t *testing.T) {
	code, body, signals, _ := postQueued(t, cfgPostgresIncremental, "bigquery", 0,
		`{"tables":["public.users"]}`,
		func(m sqlmock.Sqlmock) {
			m.ExpectQuery(regexp.QuoteMeta("INSERT INTO cdc_snapshot_requests")).
				WillReturnError(&undefinedTableErr{})
		})
	if code != http.StatusOK || body["status"] != "sent" || len(signals.sent) != 1 {
		t.Fatalf("got %d %v, %d signal(s); want 200 sent with one direct signal", code, body, len(signals.sent))
	}
}

func TestGetCDCBackfillCapabilityObjectStorageIsBlockingOnly(t *testing.T) {
	h, r := capabilityHarness(t, cfgPostgresIncremental)
	h.mock.ExpectQuery("SELECT c.connector_type").
		WithArgs(capabilityTestPipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"connector_type"}).AddRow("gcs"))
	h.mock.ExpectQuery("SELECT storage_layout_version").
		WithArgs(capabilityTestPipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(2))
	code, body := getCapability(t, r)
	if code != http.StatusOK {
		t.Fatalf("status = %d (body %v)", code, body)
	}
	if !reflect.DeepEqual(body["modes"], []interface{}{"blocking"}) || body["default_mode"] != "blocking" ||
		body["object_storage"] != true || body["cleans_folder"] != true || body["destination_type"] != "gcs" {
		t.Fatalf("body = %v", body)
	}
	if err := h.mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestIsObjectStorageDest(t *testing.T) {
	for dest, want := range map[string]bool{
		"gcs": true, "aws-s3": true, "aws_s3": true, "s3": true, "azure-blob": true, "minio": true,
		"postgresql": false, "mongodb": false, "bigquery": false, "": false,
	} {
		if got := isObjectStorageDest(dest); got != want {
			t.Errorf("isObjectStorageDest(%q) = %v, want %v", dest, got, want)
		}
	}
}

// afterArg matches a time at least d after *base (the not_before of a request
// that must wait for the connector to restart).
type afterArg struct {
	base *time.Time
	d    time.Duration
}

func (a afterArg) Match(v driver.Value) bool {
	t, ok := v.(time.Time)
	return ok && !t.Before(a.base.Add(a.d))
}

// beforeArg matches a time within d of *base (a request due at once).
type beforeArg struct {
	base *time.Time
	d    time.Duration
}

func (b beforeArg) Match(v driver.Value) bool {
	t, ok := v.(time.Time)
	return ok && t.Before(b.base.Add(b.d))
}

// undefinedTableErr is what pgx reports for a missing relation (SQLSTATE 42P01).
type undefinedTableErr struct{}

func (*undefinedTableErr) Error() string {
	return `ERROR: relation "cdc_snapshot_requests" does not exist (SQLSTATE 42P01)`
}
