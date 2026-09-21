package assessor

import (
	"context"
	"errors"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// mongoFake answers test_connection with result and every export probe with success.
func mongoFake(result map[string]interface{}) *fakeExecutor {
	return &fakeExecutor{respond: func(req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error) {
		if req.Operation == "test_connection" {
			out := map[string]interface{}{"success": true}
			for k, v := range result {
				out[k] = v
			}
			return &mcp.ExecuteResponse{Success: true, Result: out}, nil
		}
		return &mcp.ExecuteResponse{Success: true}, nil
	}}
}

func assessMongo(t *testing.T, f *fakeExecutor, in Input) *Result {
	t.Helper()
	in.ConnectionConfig = map[string]string{"host": "mongo.internal"}
	r, err := NewMongoDBAssessor(f).Assess(context.Background(), in)
	if err != nil {
		t.Fatalf("Assess: %v", err)
	}
	return r
}

func findCheck(r *Result, code string) (Check, bool) {
	for _, c := range r.Checks {
		if c.Code == code {
			return c, true
		}
	}
	return Check{}, false
}

var replicaSet = map[string]interface{}{"is_replica_set": true, "is_sharded_cluster": false}

func TestMongoDBAssessor_CDCAsksForReadinessOnTheSelectedCollections(t *testing.T) {
	f := mongoFake(replicaSet)
	assessMongo(t, f, Input{SyncMode: "cdc", Tables: []string{"app.users", "app.orders"}})

	req := f.calls[0]
	if req.Operation != "test_connection" || req.Connector != "mongodb" {
		t.Fatalf("first call = %s/%s; want mongodb/test_connection", req.Connector, req.Operation)
	}
	if req.Params["cdc_readiness"] != true {
		t.Fatalf("cdc_readiness not requested: %v", req.Params)
	}
	got, _ := req.Params["collections"].([]string)
	if len(got) != 2 || got[0] != "app.users" {
		t.Fatalf("collections = %v", req.Params["collections"])
	}
	if len(f.exportCalls()) != 2 {
		t.Fatalf("want one read probe per collection, got %d", len(f.exportCalls()))
	}
}

func TestMongoDBAssessor_BatchSkipsTheCDCChecks(t *testing.T) {
	f := mongoFake(map[string]interface{}{"is_replica_set": false, "is_sharded_cluster": false})
	r := assessMongo(t, f, Input{SyncMode: "batch", Tables: []string{"app.users"}})

	if _, ok := f.calls[0].Params["cdc_readiness"]; ok {
		t.Fatal("a batch pipeline asked for CDC readiness")
	}
	if _, ok := findCheck(r, "MONGODB_NOT_REPLICA_SET"); ok {
		t.Fatal("a standalone blocked a batch pipeline, which needs no change stream")
	}
	if r.BlocksStart() {
		t.Fatalf("batch run blocked: %+v", r.Checks)
	}
	c, ok := findCheck(r, "CONNECTOR_TABLE_READABLE")
	if !ok || c.Object != "app.users" {
		t.Fatalf("read probe missing or not tagged with its collection: %+v", c)
	}
}

func TestMongoDBAssessor_Topology(t *testing.T) {
	cases := []struct {
		name      string
		result    map[string]interface{}
		wantCheck bool
		wantBlock bool
	}{
		{"replica set passes", replicaSet, true, false},
		{"a mongos (no setName) passes", map[string]interface{}{"is_replica_set": false, "is_sharded_cluster": true}, true, false},
		{"a proven standalone blocks", map[string]interface{}{"is_replica_set": false, "is_sharded_cluster": false}, true, true},
		{"an older image without is_sharded_cluster proves nothing", map[string]interface{}{"is_replica_set": false}, false, false},
		{"no topology fields yield no check", map[string]interface{}{}, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := assessMongo(t, mongoFake(tc.result), Input{SyncMode: "cdc"})
			c, ok := findCheck(r, "MONGODB_NOT_REPLICA_SET")
			if ok != tc.wantCheck {
				t.Fatalf("check present=%v; want %v", ok, tc.wantCheck)
			}
			if r.BlocksStart() != tc.wantBlock {
				t.Fatalf("blocks=%v; want %v (%+v)", r.BlocksStart(), tc.wantBlock, c)
			}
			if tc.wantBlock && (c.Remediation == nil || len(c.Remediation.CommandsToRun) == 0) {
				t.Fatal("blocking finding carries no fix")
			}
		})
	}
}

func TestMongoDBAssessor_ChangeStreamAccess(t *testing.T) {
	access := func(fields map[string]interface{}) map[string]interface{} {
		out := map[string]interface{}{"is_replica_set": true, "is_sharded_cluster": false}
		out["change_stream_access"] = fields
		return out
	}
	cases := []struct {
		name      string
		access    map[string]interface{}
		code      string
		wantSev   Severity
		wantPass  bool
		wantBlock bool
	}{
		{"opened passes", map[string]interface{}{"scope": "database", "database": "app", "status": "ok"},
			"MONGODB_CHANGE_STREAM_UNAUTHORIZED", SeverityInfo, true, false},
		{"denied on the one database blocks", map[string]interface{}{"scope": "database", "database": "app", "status": "unauthorized", "error_code": 13.0, "message": "not authorized on app"},
			"MONGODB_CHANGE_STREAM_UNAUTHORIZED", SeverityError, false, true},
		{"denied on the deployment only warns", map[string]interface{}{"scope": "deployment", "status": "unauthorized", "message": "not authorized"},
			"MONGODB_CHANGE_STREAM_UNAUTHORIZED", SeverityWarning, false, false},
		{"a network error is never a verdict", map[string]interface{}{"scope": "database", "database": "app", "status": "error", "message": "timed out"},
			"MONGODB_CHANGE_STREAM_UNVERIFIED", SeverityInfo, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := assessMongo(t, mongoFake(access(tc.access)), Input{SyncMode: "cdc", Tables: []string{"app.users"}})
			c, ok := findCheck(r, tc.code)
			if !ok {
				t.Fatalf("no %s check in %+v", tc.code, r.Checks)
			}
			if c.Severity != tc.wantSev || c.Passed != tc.wantPass {
				t.Fatalf("severity=%q passed=%v; want %q/%v", c.Severity, c.Passed, tc.wantSev, tc.wantPass)
			}
			if r.BlocksStart() != tc.wantBlock {
				t.Fatalf("blocks=%v; want %v", r.BlocksStart(), tc.wantBlock)
			}
			if tc.code == "MONGODB_CHANGE_STREAM_UNAUTHORIZED" && !c.Passed &&
				(c.Remediation == nil || len(c.Remediation.CommandsToRun) == 0) {
				t.Fatal("denied change stream carries no grant command")
			}
		})
	}

	t.Run("a standalone reports once, not again as an unsupported stream", func(t *testing.T) {
		result := map[string]interface{}{
			"is_replica_set": false, "is_sharded_cluster": false,
			"change_stream_access": map[string]interface{}{"scope": "deployment", "status": "unsupported"},
		}
		r := assessMongo(t, mongoFake(result), Input{SyncMode: "cdc"})
		if _, ok := findCheck(r, "MONGODB_CHANGE_STREAM_UNVERIFIED"); ok {
			t.Fatal("standalone reported twice")
		}
	})
}

func TestMongoDBAssessor_OplogWindow(t *testing.T) {
	cases := []struct {
		name      string
		hours     interface{}
		wantCheck bool
		wantPass  bool
	}{
		{"six hours is an advisory", 6.0, true, false},
		{"two days passes", 48.0, true, true},
		{"an unreadable oplog yields no check", nil, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := map[string]interface{}{"is_replica_set": true, "is_sharded_cluster": false}
			if tc.hours != nil {
				result["oplog_window_hours"] = tc.hours
			}
			r := assessMongo(t, mongoFake(result), Input{SyncMode: "cdc"})
			c, ok := findCheck(r, "MONGODB_OPLOG_WINDOW_SHORT")
			if ok != tc.wantCheck {
				t.Fatalf("check present=%v; want %v", ok, tc.wantCheck)
			}
			if !ok {
				return
			}
			if c.Passed != tc.wantPass || c.Severity != SeverityInfo {
				t.Fatalf("severity=%q passed=%v; want info/%v", c.Severity, c.Passed, tc.wantPass)
			}
			if r.BlocksStart() {
				t.Fatal("a short oplog blocked the start; it only limits how long the pipeline may stop")
			}
		})
	}
}

func TestMongoDBAssessor_ConnectionFailures(t *testing.T) {
	t.Run("an auth failure blocks", func(t *testing.T) {
		f := &fakeExecutor{respond: func(mcp.ExecuteRequest) (*mcp.ExecuteResponse, error) {
			return &mcp.ExecuteResponse{Success: false, Error: "Authentication failed."}, nil
		}}
		r := assessMongo(t, f, Input{SyncMode: "cdc", Tables: []string{"app.users"}})
		if _, ok := findCheck(r, "CONNECTOR_AUTH_FAILED"); !ok || !r.BlocksStart() {
			t.Fatalf("want a blocking CONNECTOR_AUTH_FAILED, got %+v", r.Checks)
		}
		if len(f.exportCalls()) != 0 {
			t.Fatal("probed collections on a connection that failed")
		}
	})
	t.Run("an rsync-side transport error only warns", func(t *testing.T) {
		f := &fakeExecutor{respond: func(mcp.ExecuteRequest) (*mcp.ExecuteResponse, error) {
			return nil, errors.New("connector container not running")
		}}
		r := assessMongo(t, f, Input{SyncMode: "cdc"})
		if _, ok := findCheck(r, "CONNECTOR_ASSESS_UNAVAILABLE"); !ok || r.BlocksStart() {
			t.Fatalf("want a non-blocking CONNECTOR_ASSESS_UNAVAILABLE, got %+v", r.Checks)
		}
	})
}

func TestRegistry_MongoDBHasADedicatedAssessor(t *testing.T) {
	reg := NewRegistry()
	reg.Register(NewMongoDBAssessor(nil))
	if !reg.HasDedicated("mongodb") {
		t.Fatal("mongodb is not registered under its connection type")
	}
}
