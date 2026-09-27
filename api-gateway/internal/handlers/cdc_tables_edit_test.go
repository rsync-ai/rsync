package handlers

// Edit tables (POST /pipelines/:id/cdc/tables), the sweep's gateway half:
//
//   - BUG #6: when the orchestrator read the connector's live include-list, its
//     added/removed diff replaces the one computed against the SAVED list.
//   - BUG #16/#17: removals are reported (removed_tables, message), and a paused
//     pipeline is told the tables start when it resumes, not that the connector restarts.
//   - A selected_tables save that fails after the push is a warning, not silence.
//   - The backfill carries source "table_edit" and hands back the orchestrator's
//     request_id/status, which the UI polls through /cdc/snapshot-requests.
//   - BUG #5/#9: added-but-not-loaded tables go on config.cdc_streaming_only_tables,
//     and a re-added table with an old stats row gets a warning.
//
// The DB is an ordered sqlmock: best-effort statements the test does not expect get
// an error the handler ignores, so each test names only the statements it asserts.

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

const streamingOnlyUpdate = `UPDATE pipelines\s+SET config = jsonb_set\(COALESCE\(config, '\{\}'::jsonb\), '\{cdc_streaming_only_tables\}'`

type cdcTableEdit struct {
	prev        string // saved selected_tables before the edit
	body        string // the request
	updateAck   string // orchestrator PUT /api/v1/cdc/tables answer
	backfillAck string // orchestrator POST backfill answer; "" answers 500
	persistErr  error  // selected_tables save error
	expect      func(mock sqlmock.Sqlmock)
}

type cdcTableEditResult struct {
	resp         map[string]any
	backfillBody map[string]any
}

func runCDCTableEdit(t *testing.T, tc cdcTableEdit) cdcTableEditResult {
	t.Helper()
	connectorName := "cdc-tenant-" + wsScopePipeline

	connectSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/connectors" {
			_ = json.NewEncoder(w).Encode([]string{connectorName})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer connectSrv.Close()
	t.Setenv("KAFKA_CONNECT_URL", connectSrv.URL)

	var out cdcTableEditResult
	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/cdc/tables":
			_, _ = w.Write([]byte(tc.updateAck))
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/cdc/pipelines/"+wsScopePipeline+"/backfill":
			_ = json.NewDecoder(r.Body).Decode(&out.backfillBody)
			if tc.backfillAck == "" {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"success":false,"error":"signal failed"}`))
				return
			}
			_, _ = w.Write([]byte(tc.backfillAck))
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/cdc/pipelines/"+wsScopePipeline+"/sink/restart":
			_, _ = w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer orch.Close()
	t.Setenv("ORCHESTRATOR_URL", orch.URL)

	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))
	mock.ExpectQuery(`SELECT COALESCE\(config->'selected_tables'`).
		WithArgs(wsScopePipeline).
		WillReturnRows(sqlmock.NewRows([]string{"selected_tables"}).AddRow(tc.prev))
	persist := mock.ExpectExec(`'\{selected_tables\}'`).WithArgs(sqlmock.AnyArg(), wsScopePipeline)
	if tc.persistErr != nil {
		persist.WillReturnError(tc.persistErr)
	} else {
		persist.WillReturnResult(sqlmock.NewResult(0, 1))
	}
	tc.expect(mock)

	r := wsScopeRouterAsRole(http.MethodPost, "/p/:id/cdc/tables", UpdatePipelineCDCTables, "member")
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/p/"+wsScopePipeline+"/cdc/tables", bytes.NewReader([]byte(tc.body)))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out.resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
	return out
}

// expectStreamingOnly pins the one config.cdc_streaming_only_tables UPDATE.
func expectStreamingOnly(mock sqlmock.Sqlmock, add, drop string) {
	mock.ExpectExec(streamingOnlyUpdate).
		WithArgs(add, drop, wsScopePipeline).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

func expectStatsRows(mock sqlmock.Sqlmock, tables string, have ...string) {
	rows := sqlmock.NewRows([]string{"qualified_name"})
	for _, h := range have {
		rows.AddRow(h)
	}
	mock.ExpectQuery(`SELECT DISTINCT qualified_name\s+FROM pipeline_run_table_stats`).
		WithArgs(wsScopePipeline, tables).
		WillReturnRows(rows)
}

func expectPipelineStatus(mock sqlmock.Sqlmock, status string) {
	mock.ExpectQuery(`SELECT COALESCE\(status, ''\) FROM pipelines`).
		WithArgs(wsScopePipeline).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow(status))
}

func respStrings(t *testing.T, resp map[string]any, key string) []string {
	t.Helper()
	raw, ok := resp[key].([]any)
	if !ok {
		t.Fatalf("%s = %#v; want a JSON array", key, resp[key])
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		out = append(out, v.(string))
	}
	return out
}

// BUG #6: the saved list says db.orders is new and nothing was removed; the
// connector's live list (auto-pickup already added db.orders, and still streams
// db.legacy) says otherwise. The live diff wins.
func TestUpdatePipelineCDCTables_LiveListDiffOverridesSavedList(t *testing.T) {
	res := runCDCTableEdit(t, cdcTableEdit{
		prev:      `["db.users"]`,
		body:      `{"tables":["db.users","db.orders"]}`,
		updateAck: `{"success":true,"live_list_read":true,"previous_tables":["db.legacy","db.orders","db.users"],"added":[],"removed":["db.legacy"]}`,
		expect: func(mock sqlmock.Sqlmock) {
			expectStreamingOnly(mock, `[]`, `["db.legacy"]`)
			expectPipelineStatus(mock, "running")
		},
	})
	if got := respStrings(t, res.resp, "new_tables"); len(got) != 0 {
		t.Fatalf("new_tables = %v; want [] from the live diff, not the saved-list diff", got)
	}
	if got := respStrings(t, res.resp, "removed_tables"); !reflect.DeepEqual(got, []string{"db.legacy"}) {
		t.Fatalf("removed_tables = %v; want [db.legacy] from the live diff", got)
	}
	if msg := res.resp["message"]; msg != "CDC tables updated: 1 removed and no longer streamed. The Debezium connector restarts to apply the change." {
		t.Fatalf("message = %q", msg)
	}
	if res.resp["paused"] != false || len(respStrings(t, res.resp, "warnings")) != 0 {
		t.Fatalf("paused/warnings = %v/%v; want false/[]", res.resp["paused"], res.resp["warnings"])
	}
}

// The control: without live_list_read the orchestrator's added/removed are not
// trusted, and the saved-list diff (removals included) is reported.
func TestUpdatePipelineCDCTables_NoLiveListKeepsSavedListDiff(t *testing.T) {
	res := runCDCTableEdit(t, cdcTableEdit{
		prev:      `["db.users","db.legacy"]`,
		body:      `{"tables":["db.users","db.orders"]}`,
		updateAck: `{"success":true,"live_list_read":false,"added":["db.bogus"]}`,
		expect: func(mock sqlmock.Sqlmock) {
			expectStreamingOnly(mock, `["db.orders"]`, `["db.legacy"]`)
			expectStatsRows(mock, `["db.orders"]`)
			expectPipelineStatus(mock, "running")
		},
	})
	if got := respStrings(t, res.resp, "new_tables"); !reflect.DeepEqual(got, []string{"db.orders"}) {
		t.Fatalf("new_tables = %v; want [db.orders]", got)
	}
	if got := respStrings(t, res.resp, "removed_tables"); !reflect.DeepEqual(got, []string{"db.legacy"}) {
		t.Fatalf("removed_tables = %v; want [db.legacy]", got)
	}
	if !strings.HasPrefix(res.resp["message"].(string), "CDC tables updated: 1 added, 1 removed and no longer streamed.") {
		t.Fatalf("message = %q", res.resp["message"])
	}
}

// The list is live on the connector, so a failed save cannot undo the edit — but the
// caller must hear that the saved selection is stale.
func TestUpdatePipelineCDCTables_SaveFailureIsAWarning(t *testing.T) {
	res := runCDCTableEdit(t, cdcTableEdit{
		prev:       `["db.users"]`,
		body:       `{"tables":["db.users"]}`,
		updateAck:  `{"success":true}`,
		persistErr: errors.New("connection reset"),
		expect: func(mock sqlmock.Sqlmock) {
			expectPipelineStatus(mock, "running")
		},
	})
	warnings := respStrings(t, res.resp, "warnings")
	if len(warnings) != 1 || !strings.Contains(warnings[0], "could not be saved") {
		t.Fatalf("warnings = %v; want the save failure", warnings)
	}
	if res.resp["success"] != true {
		t.Fatalf("success = %v; the connector already has the list", res.resp["success"])
	}
}

// BUG #17: a paused pipeline's connector is not restarted, and the added tables wait
// for the resume.
func TestUpdatePipelineCDCTables_PausedPipelineSaysResume(t *testing.T) {
	res := runCDCTableEdit(t, cdcTableEdit{
		prev:      `["db.users"]`,
		body:      `{"tables":["db.users","db.orders"]}`,
		updateAck: `{"success":true}`,
		expect: func(mock sqlmock.Sqlmock) {
			expectStreamingOnly(mock, `["db.orders"]`, `[]`)
			expectStatsRows(mock, `["db.orders"]`)
			expectPipelineStatus(mock, "paused")
		},
	})
	if res.resp["paused"] != true {
		t.Fatalf("paused = %v; want true", res.resp["paused"])
	}
	msg := res.resp["message"].(string)
	if !strings.Contains(msg, "start streaming when it is resumed") || strings.Contains(msg, "connector restarts") {
		t.Fatalf("message = %q; want the resume wording and no restart claim", msg)
	}
}

// The backfill is tagged table_edit and its request_id/status reach the caller; a
// requested-and-accepted load takes the table OFF the streaming-only list.
func TestUpdatePipelineCDCTables_BackfillCarriesSourceAndRequestID(t *testing.T) {
	res := runCDCTableEdit(t, cdcTableEdit{
		prev:        `["db.users"]`,
		body:        `{"tables":["db.users","db.orders"],"backfill_newly_added":true}`,
		updateAck:   `{"success":true}`,
		backfillAck: `{"success":true,"status":"queued","request_id":"a0000000-0000-4000-8000-000000000009","snapshot_mode":"incremental","data_collections":["db.orders"],"signal_channel":"source","message":"queued"}`,
		expect: func(mock sqlmock.Sqlmock) {
			expectStreamingOnly(mock, `[]`, `["db.orders"]`)
			expectPipelineStatus(mock, "running")
		},
	})
	if res.backfillBody["source"] != cdcSnapshotSourceTableEdit {
		t.Fatalf("backfill body source = %#v; want %q", res.backfillBody["source"], cdcSnapshotSourceTableEdit)
	}
	bf, _ := res.resp["backfill"].(map[string]any)
	if bf["success"] != true || bf["request_id"] != "a0000000-0000-4000-8000-000000000009" || bf["status"] != "queued" || bf["mode"] != "incremental" {
		t.Fatalf("backfill = %#v; want success, the request_id, status queued and mode incremental", bf)
	}
	if got := respStrings(t, res.resp, "warnings"); len(got) != 0 {
		t.Fatalf("warnings = %v; a loaded table needs none", got)
	}
}

// A refused backfill loads nothing, so the table stays streaming-only, and a table
// that streamed before its removal is called out.
func TestUpdatePipelineCDCTables_RefusedBackfillIsStreamingOnly(t *testing.T) {
	res := runCDCTableEdit(t, cdcTableEdit{
		prev:      `["db.users"]`,
		body:      `{"tables":["db.users","db.orders"],"backfill_newly_added":true}`,
		updateAck: `{"success":true}`,
		expect: func(mock sqlmock.Sqlmock) {
			expectStreamingOnly(mock, `["db.orders"]`, `[]`)
			expectStatsRows(mock, `["db.orders"]`, "db.orders")
			expectPipelineStatus(mock, "running")
		},
	})
	bf, _ := res.resp["backfill"].(map[string]any)
	if bf["success"] != false {
		t.Fatalf("backfill = %#v; want the 500 reported as a failure", bf)
	}
	warnings := respStrings(t, res.resp, "warnings")
	if len(warnings) != 1 || !strings.HasPrefix(warnings[0], "db.orders was streamed before and removed") {
		t.Fatalf("warnings = %v; want the re-added-without-load warning for db.orders", warnings)
	}
}

// BUG #5 control: a table with no earlier stats row is simply new — no warning.
func TestUpdatePipelineCDCTables_NewTableWithoutHistoryHasNoWarning(t *testing.T) {
	res := runCDCTableEdit(t, cdcTableEdit{
		prev:      `["db.users"]`,
		body:      `{"tables":["db.users","db.orders"]}`,
		updateAck: `{"success":true}`,
		expect: func(mock sqlmock.Sqlmock) {
			expectStreamingOnly(mock, `["db.orders"]`, `[]`)
			expectStatsRows(mock, `["db.orders"]`)
			expectPipelineStatus(mock, "running")
		},
	})
	if got := respStrings(t, res.resp, "warnings"); len(got) != 0 {
		t.Fatalf("warnings = %v; want none for a never-streamed table", got)
	}
}

// BUG #9, re-snapshot half: an ACCEPTED re-snapshot loads the tables' rows, so they
// leave the streaming-only list; a refused one must not touch it.
func TestBackfillPipelineCDCTables_AcceptedLeavesStreamingOnly(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		wantUpdate bool
	}{
		{"accepted", http.StatusOK, true},
		{"refused", http.StatusConflict, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
				WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
				WillReturnRows(gateRoleRows("member"))
			expectStreamingOnly(mock, `[]`, `["public.users"]`)

			orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"success":true,"status":"queued","request_id":"r1"}`))
			}))
			defer orch.Close()
			t.Setenv("ORCHESTRATOR_URL", orch.URL)

			r := wsScopeRouterAsRole(http.MethodPost, "/p/:id/cdc/backfill", BackfillPipelineCDCTables, "member")
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/p/"+wsScopePipeline+"/cdc/backfill",
				strings.NewReader(`{"tables":["public.users"],"mode":"blocking"}`)))
			if w.Code != tc.status {
				t.Fatalf("status = %d, want the orchestrator's %d: %s", w.Code, tc.status, w.Body.String())
			}
			err := mock.ExpectationsWereMet()
			if tc.wantUpdate && err != nil {
				t.Fatalf("the accepted re-snapshot did not clear streaming-only: %v", err)
			}
			if !tc.wantUpdate && err == nil {
				t.Fatal("a refused re-snapshot cleared streaming-only")
			}
		})
	}
}
