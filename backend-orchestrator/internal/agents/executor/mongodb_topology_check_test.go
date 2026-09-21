package executor

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
	"github.com/rsync-ai/backend-orchestrator/pkg/llmscrub"
)

// The shapes below are what the mongodb connector's test_connection returns, taken
// from real servers (mongo:7): a standalone mongod, a replica-set member, and a mongos.
// The last two rows are the ones a naive "is_replica_set == false" rule gets wrong.
func TestMongoResultIsStandalone(t *testing.T) {
	cases := []struct {
		name   string
		result map[string]interface{}
		want   bool
	}{
		{"standalone mongod", map[string]interface{}{"success": true, "is_replica_set": false, "is_sharded_cluster": false}, true},
		{"replica-set member", map[string]interface{}{"success": true, "is_replica_set": true, "is_sharded_cluster": false, "replica_set": "rs0"}, false},
		{"mongos router (sharded cluster, no setName)", map[string]interface{}{"success": true, "is_replica_set": false, "is_sharded_cluster": true}, false},
		// A connector image built before is_sharded_cluster existed reports false for
		// every mongos. Blocking on this row would stop every sharded CDC pipeline.
		{"old connector image: is_replica_set only", map[string]interface{}{"success": true, "is_replica_set": false}, false},
		{"topology unknown: both fields omitted", map[string]interface{}{"success": true}, false},
		{"only is_sharded_cluster present", map[string]interface{}{"is_sharded_cluster": false}, false},
		{"non-bool values are not proof", map[string]interface{}{"is_replica_set": "false", "is_sharded_cluster": "false"}, false},
		{"nil result", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mongoResultIsStandalone(tc.result); got != tc.want {
				t.Errorf("mongoResultIsStandalone(%v) = %v, want %v", tc.result, got, tc.want)
			}
		})
	}
}

type recordedTest struct {
	calls         int
	connectorType string
	version       string
	config        map[string]string
	hadDeadline   bool
	deadlineIn    time.Duration
}

func fakeTester(rec *recordedTest, ok bool, msg string, result map[string]interface{}) connectionResultTester {
	return func(ctx context.Context, connectorType, connectorVersion string, config map[string]string) (bool, string, map[string]interface{}) {
		rec.calls++
		rec.connectorType = connectorType
		rec.version = connectorVersion
		rec.config = config
		if dl, has := ctx.Deadline(); has {
			rec.hadDeadline = true
			rec.deadlineIn = time.Until(dl)
		}
		return ok, msg, result
	}
}

var standaloneResult = map[string]interface{}{"success": true, "is_replica_set": false, "is_sharded_cluster": false}

func TestCheckMongoDBCDCSourceBlocksAnExplicitStandalone(t *testing.T) {
	rec := &recordedTest{}
	cfg := map[string]string{"host": "mongo.internal", "port": "27017", "database": "app"}
	msg := checkMongoDBCDCSource(context.Background(), fakeTester(rec, true, "", standaloneResult), "mongodb", "v1.0.0", cfg)

	if msg == "" {
		t.Fatal("a standalone mongod was allowed to start CDC — Debezium would accept the connector and retry forever while the run shows Running")
	}
	if msg != llmscrub.Scrub(mongoStandaloneCDCError) {
		t.Errorf("the run error is not the scrubbed constant:\n got %q", msg)
	}
	for _, want := range []string{"not a replica set", "sharded cluster", "rs.initiate()"} {
		if !strings.Contains(msg, want) {
			t.Errorf("run error is missing %q: %q", want, msg)
		}
	}
	if rec.calls != 1 || rec.connectorType != "mongodb" || rec.version != "v1.0.0" {
		t.Errorf("tester called %d times as (%q, %q), want once as (mongodb, v1.0.0)", rec.calls, rec.connectorType, rec.version)
	}
	if rec.config["host"] != "mongo.internal" || rec.config["database"] != "app" {
		t.Errorf("the check must probe the config Debezium receives, got %v", rec.config)
	}
	if !rec.hadDeadline || rec.deadlineIn > mongoCDCTopologyCheckTimeout {
		t.Errorf("the check must be bounded by %s (deadline set: %v, remaining %s)", mongoCDCTopologyCheckTimeout, rec.hadDeadline, rec.deadlineIn)
	}
}

func TestCheckMongoDBCDCSourceProceedsOnAnythingButProof(t *testing.T) {
	cfg := map[string]string{"host": "mongo.internal"}
	cases := []struct {
		name   string
		ok     bool
		errMsg string
		result map[string]interface{}
	}{
		{"replica set", true, "", map[string]interface{}{"is_replica_set": true, "is_sharded_cluster": false}},
		{"mongos", true, "", map[string]interface{}{"is_replica_set": false, "is_sharded_cluster": true}},
		{"old connector image reporting is_replica_set=false only", true, "", map[string]interface{}{"is_replica_set": false}},
		{"topology unknown", true, "", map[string]interface{}{"success": true}},
		// A failed call can still carry a result map; only a successful call is read.
		{"call failed", false, "No servers match selector Primary()", standaloneResult},
		{"call failed with no result", false, "Connection test failed: context deadline exceeded", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recordedTest{}
			if msg := checkMongoDBCDCSource(context.Background(), fakeTester(rec, tc.ok, tc.errMsg, tc.result), "mongodb", "", cfg); msg != "" {
				t.Errorf("blocked without proof of a standalone: %q", msg)
			}
			if rec.calls != 1 {
				t.Errorf("tester called %d times, want 1", rec.calls)
			}
			if rec.version != "latest" {
				t.Errorf("empty connector version must resolve to latest, got %q", rec.version)
			}
		})
	}
}

// A check that waits on a hung connector must give up and let the run start.
func TestCheckMongoDBCDCSourceFailsOpenWhenTheCallTimesOut(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	hung := func(ctx context.Context, _, _ string, _ map[string]string) (bool, string, map[string]interface{}) {
		<-ctx.Done()
		return false, "Connection test failed: " + ctx.Err().Error(), nil
	}
	done := make(chan string, 1)
	go func() { done <- checkMongoDBCDCSource(ctx, hung, "mongodb", "", map[string]string{"host": "h"}) }()
	select {
	case msg := <-done:
		if msg != "" {
			t.Errorf("a timed-out check blocked the run: %q", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the check did not honour context cancellation")
	}
}

func TestCheckMongoDBCDCSourceOnlyProbesMongoDBWithAServer(t *testing.T) {
	cases := []struct {
		name       string
		sourceType string
		config     map[string]string
		wantCalls  int
	}{
		{"postgresql source is never probed", "postgresql", map[string]string{"host": "h"}, 0},
		{"mysql source is never probed", "mysql", map[string]string{"host": "h"}, 0},
		{"source type is matched case- and space-insensitively", "  MongoDB ", map[string]string{"host": "h"}, 1},
		{"connection_string alone names a server", "mongodb", map[string]string{"connection_string": "mongodb+srv://c.example.net/"}, 1},
		{"mongodb_connection_string alone names a server", "mongodb", map[string]string{"mongodb_connection_string": "mongodb://h/"}, 1},
		{"mongodb_uri alone names a server", "mongodb", map[string]string{"mongodb_uri": "mongodb://h/"}, 1},
		{"uri alone names a server", "mongodb", map[string]string{"uri": "mongodb://h/"}, 1},
		{"empty config is not probed", "mongodb", map[string]string{}, 0},
		{"nil config is not probed", "mongodb", nil, 0},
		{"blank host is not probed", "mongodb", map[string]string{"host": "  ", "database": "app"}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recordedTest{}
			msg := checkMongoDBCDCSource(context.Background(), fakeTester(rec, true, "", standaloneResult), tc.sourceType, "", tc.config)
			if rec.calls != tc.wantCalls {
				t.Errorf("tester called %d times, want %d", rec.calls, tc.wantCalls)
			}
			if (msg != "") != (tc.wantCalls == 1) {
				t.Errorf("message %q does not match whether the probe ran (%d calls)", msg, rec.calls)
			}
		})
	}
	if msg := checkMongoDBCDCSource(context.Background(), nil, "mongodb", "", map[string]string{"host": "h"}); msg != "" {
		t.Errorf("a nil tester must not block: %q", msg)
	}
}

// The run error is bound for the healer's LLM path, so it goes through the scrubber —
// and the scrubber must leave it intact, or the user gets "[redacted]" where the
// remediation was. It must also land in the healer's MongoDB bucket: ActionEscalate
// (not a retry loop) and the MONGODB_NOT_REPLICA_SET structured error with its
// rs.initiate() remediation.
func TestMongoStandaloneCDCErrorIsScrubSafeAndClassified(t *testing.T) {
	if got := llmscrub.Scrub(mongoStandaloneCDCError); got != mongoStandaloneCDCError {
		t.Errorf("llmscrub.Scrub alters the run error:\n got  %q\n want %q", got, mongoStandaloneCDCError)
	}

	signal := diagnose.Signal{
		ErrorMessage:   mongoStandaloneCDCError,
		ExecutorStatus: "failed",
		Stage:          "executor",
		SourceType:     "mongodb",
	}
	d := diagnose.New().Diagnose(signal)
	if d.SuggestedAction != diagnose.ActionEscalate {
		t.Errorf("healer action = %v, want ActionEscalate — a standalone never fixes itself on retry", d.SuggestedAction)
	}
	if se := diagnose.FromDiagnosis(d, signal); se.Code != "MONGODB_NOT_REPLICA_SET" {
		t.Errorf("structured error code = %q, want MONGODB_NOT_REPLICA_SET", se.Code)
	}
}

func TestSourceConnectorVersion(t *testing.T) {
	cases := []struct {
		name string
		src  *ConnectorConfig
		want string
	}{
		{"nil source", nil, "latest"},
		{"nothing pinned", &ConnectorConfig{Type: "mongodb", Config: map[string]string{}}, "latest"},
		{"nil config", &ConnectorConfig{Type: "mongodb"}, "latest"},
		{"task version wins", &ConnectorConfig{Version: " v1.0.0 ", Config: map[string]string{"connector_version": "v9"}}, "v1.0.0"},
		{"connector_version from the connection record", &ConnectorConfig{Config: map[string]string{"connector_version": "v1.0.0", "version": "v9"}}, "v1.0.0"},
		{"version as the last fallback", &ConnectorConfig{Config: map[string]string{"version": "v1.0.0"}}, "v1.0.0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sourceConnectorVersion(tc.src); got != tc.want {
				t.Errorf("sourceConnectorVersion = %q, want %q", got, tc.want)
			}
		})
	}
}
