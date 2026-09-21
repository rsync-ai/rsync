//go:build integration_pg

// Real-PostgreSQL coverage for executionLiveCDCStreamSQL — UI #35, a live CDC run
// read "Completed" with Re-run buttons on Home, Executions and Execution Details
// while the pipeline page said LIVE.
//
// The sqlmock suites only match the query text. This runs ListExecutions (with and
// without ?status=) and GetExecution against every migration, over the same seeded
// pipelines as pipelines_derived_status_pg_test.go, and asserts that each run's
// status agrees with the list badge of the pipeline it belongs to, that a live run
// has no end time, and that its records come from the pipeline-keyed CDC stats.
// Same server and commands as that file; run with -run 'PG_' to cover both.
package handlers

import (
	"testing"
)

type pgExecListResp struct {
	Executions []struct {
		ID         string  `json:"id"`
		PipelineID string  `json:"pipeline_id"`
		Status     string  `json:"status"`
		LiveStream bool    `json:"live_stream"`
		EndTime    *string `json:"end_time"`
		Metrics    *struct {
			RecordsProcessed int64 `json:"records_processed"`
		} `json:"metrics"`
	} `json:"executions"`
	Total int `json:"total"`
	Stats struct {
		Running int `json:"running"`
		Success int `json:"success"`
	} `json:"stats"`
}

func TestPG_ExecutionLiveStream_AgreesWithPipelineList(t *testing.T) {
	conn := pgDSOpen(t)
	cases := pgDSCases()
	pgDSSeed(t, conn, cases)

	// CDC stats for the live stream are keyed by execution_id = pipeline_id, from a
	// sink too old to send orchestration_execution_id (UI #43).
	live := cases[0]
	pgDSExec(t, conn, `
		INSERT INTO pipeline_run_table_stats
		  (pipeline_id, execution_id, table_name, qualified_name, mode, status, applied_inserts, applied_updates)
		VALUES ($1, $1, 'orders', 'public.orders', 'cdc', 'running', 83000, 230)`, live.id)

	r := pgDSRouter()
	r.GET("/api/v1/executions", ListExecutions)
	r.GET("/api/v1/executions/:id", GetExecution)

	var list pgDSListResp
	pgDSGet(t, r, "/api/v1/pipelines?page=1&per_page=100", &list)
	badge := map[string]string{}
	for _, p := range list.Pipelines {
		badge[p.ID] = p.DerivedStatus
	}

	var execs pgExecListResp
	pgDSGet(t, r, "/api/v1/executions?limit=500", &execs)
	if len(execs.Executions) != len(cases) {
		t.Fatalf("denominator: want %d seeded executions, got %d", len(cases), len(execs.Executions))
	}

	// A run reads running exactly when its pipeline's list badge does; otherwise it
	// keeps the status it closed with (a paused stream's backfill did complete).
	seeded := map[string]string{}
	for _, tc := range cases {
		seeded[tc.id] = *tc.execStatus
	}
	wantRunning := 0
	for _, e := range execs.Executions {
		listSays := badge[e.PipelineID]
		want := seeded[e.PipelineID]
		if listSays == "running" {
			want = "running"
			wantRunning++
		}
		if e.Status != want {
			t.Errorf("pipeline %s: execution status %q, want %q (list says %q)", e.PipelineID, e.Status, want, listSays)
		}
		if e.LiveStream != (want == "running") {
			t.Errorf("pipeline %s: live_stream=%v, list says %q", e.PipelineID, e.LiveStream, listSays)
		}
		if e.LiveStream && e.EndTime != nil {
			t.Errorf("pipeline %s: a live stream reports end_time %s", e.PipelineID, *e.EndTime)
		}
	}
	if wantRunning == 0 {
		t.Fatalf("fixture must contain live streams, or every check above is vacuous")
	}
	if execs.Stats.Running != wantRunning {
		t.Errorf("stats.running = %d, want %d", execs.Stats.Running, wantRunning)
	}

	// The filter matches the reported status, and its COUNT agrees with the page.
	var running pgExecListResp
	pgDSGet(t, r, "/api/v1/executions?limit=500&status=running", &running)
	if len(running.Executions) != wantRunning || running.Total != wantRunning {
		t.Errorf("?status=running: page %d, total %d, want %d", len(running.Executions), running.Total, wantRunning)
	}
	var completed pgExecListResp
	pgDSGet(t, r, "/api/v1/executions?limit=500&status=completed", &completed)
	if len(completed.Executions) == 0 {
		t.Fatalf("?status=completed returned nothing; the batch controls should be there")
	}
	for _, e := range completed.Executions {
		if e.LiveStream || badge[e.PipelineID] == "running" {
			t.Errorf("?status=completed returned the live stream of pipeline %s", e.PipelineID)
		}
	}

	// The single-run read (Execution Details) says the same, with the stream's rows.
	var liveExecID string
	for _, e := range execs.Executions {
		if e.PipelineID == live.id {
			liveExecID = e.ID
		}
	}
	var one struct {
		Status     string `json:"status"`
		LiveStream bool   `json:"live_stream"`
	}
	pgDSGet(t, r, "/api/v1/executions/"+liveExecID, &one)
	if one.Status != "running" || !one.LiveStream {
		t.Errorf("GetExecution: status %q live_stream %v, want running/true", one.Status, one.LiveStream)
	}
	for _, e := range execs.Executions {
		if e.PipelineID != live.id {
			continue
		}
		if e.Metrics == nil || e.Metrics.RecordsProcessed != 83230 {
			t.Errorf("live stream records: got %+v, want 83230 from the pipeline-keyed CDC stats", e.Metrics)
		}
	}
}
