package executor

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

// The run-boundary half of KI-NSLOCK-PROBE-UNREACHABLE-WITHOUT-HITL. The probe's
// decision logic is api-gateway's and tested there; what is tested here is that the
// executor asks at all, asks with the real table set, and then USES the answer.
//
// That last part is the one with teeth. resolveDestinationNamespace reads
// task.Params before it reads the pipelines row, so locking the DB without adopting
// the result would leave the run writing to the namespace it was just moved off of.

func TestNamespaceLockTables(t *testing.T) {
	cases := []struct {
		name string
		task *ExecutorTask
		want []string
	}{{
		name: "params.tables wins — it is what qualifySelectedTablesForSource normalizes into",
		task: &ExecutorTask{
			Params:  map[string]interface{}{"tables": []string{"demo_src.a"}, "selected_tables": []string{"stale"}},
			Payload: map[string]interface{}{"tables": []string{"older"}},
		},
		want: []string{"demo_src.a"},
	}, {
		name: "falls through to selected_tables, then to payload",
		task: &ExecutorTask{
			Params:  map[string]interface{}{"selected_tables": []interface{}{"a", "b"}},
			Payload: map[string]interface{}{"tables": []string{"unused"}},
		},
		want: []string{"a", "b"},
	}, {
		name: "payload only",
		task: &ExecutorTask{Payload: map[string]interface{}{"selected_tables": []interface{}{"p"}}},
		want: []string{"p"},
	}, {
		// A wildcard names no table. Probing one asks the destination about a
		// table that cannot exist and gets back "no collision" — a false
		// all-clear, which is exactly what this path exists to prevent.
		name: "unresolved sentinels are dropped, not probed",
		task: &ExecutorTask{Params: map[string]interface{}{"tables": []interface{}{"*", "demo_src.*", " ", "demo_src.real"}}},
		want: []string{"demo_src.real"},
	}, {
		name: "an all-sentinel list yields nothing, so no lock is taken",
		task: &ExecutorTask{Params: map[string]interface{}{"tables": []string{"*", "public.*"}}},
		want: nil,
	}, {
		name: "no tables anywhere",
		task: &ExecutorTask{Params: map[string]interface{}{"other": 1}},
		want: nil,
	}, {
		name: "nil task",
		task: nil,
		want: nil,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := namespaceLockTables(tc.task); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("namespaceLockTables() = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestEnsureDestinationNamespaceLockedAdoptsResolvedNamespace(t *testing.T) {
	var gotPath string
	var gotBody struct {
		SelectedTables []string `json:"selected_tables"`
	}
	var gotSource string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotSource = r.Header.Get("X-Internal-Source")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"pipeline_id":"p1","locked":true,"namespace":"rsync_public"}`))
	}))
	defer srv.Close()
	t.Setenv("API_GATEWAY_INTERNAL_URL", srv.URL)

	task := &ExecutorTask{
		PipelineID: "12c3579c-52bc-47f2-96ae-10719e4e943c",
		Params:     map[string]interface{}{"tables": []string{"demo_src.demo_customers"}, "destination_namespace": "public"},
		Payload:    map[string]interface{}{"destination_namespace": "public"},
	}
	(&Agent{}).ensureDestinationNamespaceLocked(context.Background(), task)

	if want := "/api/v1/internal/pipelines/12c3579c-52bc-47f2-96ae-10719e4e943c/namespace/lock"; gotPath != want {
		t.Errorf("path = %q, want %q", gotPath, want)
	}
	if gotSource != "executor" {
		t.Errorf("X-Internal-Source = %q, want \"executor\"", gotSource)
	}
	if !reflect.DeepEqual(gotBody.SelectedTables, []string{"demo_src.demo_customers"}) {
		t.Errorf("probed tables = %#v, want [demo_src.demo_customers]", gotBody.SelectedTables)
	}
	// Params is the one that matters: resolveDestinationNamespace reads it first.
	if got := task.Params["destination_namespace"]; got != "rsync_public" {
		t.Errorf("Params[destination_namespace] = %v, want rsync_public — the run would still write to public", got)
	}
	if got := task.Payload["destination_namespace"]; got != "rsync_public" {
		t.Errorf("Payload[destination_namespace] = %v, want rsync_public", got)
	}
}

func TestEnsureDestinationNamespaceLockedFailSoft(t *testing.T) {
	// The executor's contract with the probe is that the probe can never take a run
	// down. Every one of these must leave the run's namespace untouched and return.
	cases := []struct {
		name    string
		handler http.HandlerFunc
	}{{
		name:    "non-200",
		handler: func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusInternalServerError) },
	}, {
		name:    "unauthorized",
		handler: func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusUnauthorized) },
	}, {
		name:    "undecodable body",
		handler: func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`not json`)) },
	}, {
		name: "stand-down: locked=false",
		handler: func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"locked":false,"namespace":""}`))
		},
	}, {
		name: "locked but empty namespace is not adopted",
		handler: func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"locked":true,"namespace":"   "}`))
		},
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			defer srv.Close()
			t.Setenv("API_GATEWAY_INTERNAL_URL", srv.URL)

			task := &ExecutorTask{
				PipelineID: "p1",
				Params:     map[string]interface{}{"tables": []string{"t"}, "destination_namespace": "public"},
			}
			(&Agent{}).ensureDestinationNamespaceLocked(context.Background(), task)
			if got := task.Params["destination_namespace"]; got != "public" {
				t.Errorf("namespace = %v, want public (unchanged)", got)
			}
		})
	}

	t.Run("gateway unreachable", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		url := srv.URL
		srv.Close() // nothing is listening now
		t.Setenv("API_GATEWAY_INTERNAL_URL", url)

		task := &ExecutorTask{
			PipelineID: "p1",
			Params:     map[string]interface{}{"tables": []string{"t"}, "destination_namespace": "public"},
		}
		(&Agent{}).ensureDestinationNamespaceLocked(context.Background(), task)
		if got := task.Params["destination_namespace"]; got != "public" {
			t.Errorf("namespace = %v, want public (unchanged)", got)
		}
	})
}

func TestEnsureDestinationNamespaceLockedSkipsWhenNothingToProbe(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		_, _ = w.Write([]byte(`{"locked":true,"namespace":"rsync_public"}`))
	}))
	defer srv.Close()
	t.Setenv("API_GATEWAY_INTERNAL_URL", srv.URL)

	// No resolved tables — locking here would freeze the seeded namespace having
	// proven nothing about it, and the lock is permanent.
	for _, task := range []*ExecutorTask{
		{PipelineID: "p1", Params: map[string]interface{}{"destination_namespace": "public"}},
		{PipelineID: "p1", Params: map[string]interface{}{"tables": []string{"*"}}},
		{PipelineID: "  ", Params: map[string]interface{}{"tables": []string{"t"}}},
	} {
		(&Agent{}).ensureDestinationNamespaceLocked(context.Background(), task)
	}
	if called {
		t.Error("probed with no resolvable table set — an unfounded lock is permanent")
	}
}
