package executor

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// shrinkCDCStartCheck makes the start check run in milliseconds.
func shrinkCDCStartCheck(t *testing.T, window, settle, poll time.Duration) {
	t.Helper()
	w, s, p := cdcStartCheckWindow, cdcStartCheckSettle, cdcStartCheckPoll
	cdcStartCheckWindow, cdcStartCheckSettle, cdcStartCheckPoll = window, settle, poll
	t.Cleanup(func() { cdcStartCheckWindow, cdcStartCheckSettle, cdcStartCheckPoll = w, s, p })
}

func statusBody(connector string, tasks ...[2]string) string {
	type task struct {
		ID    int    `json:"id"`
		State string `json:"state"`
		Trace string `json:"trace,omitempty"`
	}
	doc := map[string]interface{}{
		"name":      "cdc-abcd1234",
		"connector": map[string]string{"state": connector},
	}
	ts := []task{}
	for i, t := range tasks {
		ts = append(ts, task{ID: i, State: t[0], Trace: t[1]})
	}
	doc["tasks"] = ts
	b, _ := json.Marshal(doc)
	return string(b)
}

// connectStub serves a sequence of status bodies (the last one repeats) and counts
// restart calls. After a restart it switches to afterRestart when that is set.
type connectStub struct {
	mu           sync.Mutex
	bodies       []string
	afterRestart []string
	restarts     int32
	restartPath  string
	statusCode   int
}

func (c *connectStub) handler(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/restart") {
		atomic.AddInt32(&c.restarts, 1)
		c.restartPath = r.URL.RequestURI()
		if c.afterRestart != nil {
			c.bodies, c.afterRestart = c.afterRestart, nil
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if c.statusCode != 0 {
		w.WriteHeader(c.statusCode)
		return
	}
	body := c.bodies[0]
	if len(c.bodies) > 1 {
		c.bodies = c.bodies[1:]
	}
	_, _ = w.Write([]byte(body))
}

func startStub(t *testing.T, c *connectStub) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	t.Cleanup(srv.Close)
	return srv.URL + "/connectors/cdc-abcd1234/status"
}

func TestVerifyCDCConnectorStarted_StableRunningPasses(t *testing.T) {
	shrinkCDCStartCheck(t, 2*time.Second, 30*time.Millisecond, 5*time.Millisecond)
	c := &connectStub{bodies: []string{statusBody("RUNNING", [2]string{"RUNNING", ""})}}
	if got := verifyCDCConnectorStarted(context.Background(), startStub(t, c), false); got != "" {
		t.Fatalf("stable RUNNING must pass, got %q", got)
	}
}

func TestVerifyCDCConnectorStarted_TaskFailedReturnsScrubbedReason(t *testing.T) {
	shrinkCDCStartCheck(t, 2*time.Second, 30*time.Millisecond, 5*time.Millisecond)
	trace := "org.apache.kafka.connect.errors.ConnectException: Error while attempting to connect\n" +
		"\tat io.debezium.connector.mongodb.MongoDbConnectorTask.start(MongoDbConnectorTask.java:120)\n" +
		"Caused by: com.mongodb.MongoSecurityException: Exception authenticating mongodb+srv://appuser:S3cretPass@cluster0.example.net/shop\n" +
		"\tat com.mongodb.internal.connection.SaslAuthenticator.wrapException(SaslAuthenticator.java:231)\n"
	c := &connectStub{bodies: []string{statusBody("RUNNING", [2]string{"FAILED", trace})}}
	got := verifyCDCConnectorStarted(context.Background(), startStub(t, c), false)
	if !strings.HasPrefix(got, "task 0 FAILED: ") {
		t.Fatalf("want a task FAILED reason, got %q", got)
	}
	if !strings.Contains(got, "MongoSecurityException") {
		t.Fatalf("reason must carry the deepest Caused by: line, got %q", got)
	}
	if strings.Contains(got, "S3cretPass") {
		t.Fatalf("reason leaked the connection-string password: %q", got)
	}
	if strings.Contains(got, "SaslAuthenticator.java") {
		t.Fatalf("reason must not carry stack frames: %q", got)
	}
	if n := atomic.LoadInt32(&c.restarts); n != 0 {
		t.Fatalf("a new connector must not be restarted, got %d restarts", n)
	}
}

func TestVerifyCDCConnectorStarted_ConnectorFailed(t *testing.T) {
	shrinkCDCStartCheck(t, 2*time.Second, 30*time.Millisecond, 5*time.Millisecond)
	c := &connectStub{bodies: []string{statusBody("FAILED")}}
	got := verifyCDCConnectorStarted(context.Background(), startStub(t, c), false)
	if got != "connector FAILED: no error detail reported by Kafka Connect" {
		t.Fatalf("got %q", got)
	}
}

func TestVerifyCDCConnectorStarted_RunningThenFailedInsideSettleFails(t *testing.T) {
	shrinkCDCStartCheck(t, 2*time.Second, 200*time.Millisecond, 5*time.Millisecond)
	running := statusBody("RUNNING", [2]string{"RUNNING", ""})
	failed := statusBody("RUNNING", [2]string{"FAILED", "io.debezium.DebeziumException: The replica set name is missing"})
	c := &connectStub{bodies: []string{running, running, running, failed}}
	got := verifyCDCConnectorStarted(context.Background(), startStub(t, c), false)
	if !strings.Contains(got, "replica set name is missing") {
		t.Fatalf("a task that fails inside the settle period must fail the start, got %q", got)
	}
}

func TestVerifyCDCConnectorStarted_UnreadableUntilWindowProceeds(t *testing.T) {
	shrinkCDCStartCheck(t, 60*time.Millisecond, 20*time.Millisecond, 5*time.Millisecond)
	c := &connectStub{statusCode: http.StatusNotFound}
	start := time.Now()
	if got := verifyCDCConnectorStarted(context.Background(), startStub(t, c), false); got != "" {
		t.Fatalf("no verdict inside the window must proceed, got %q", got)
	}
	if time.Since(start) < 60*time.Millisecond {
		t.Fatalf("returned before the window elapsed")
	}

	// Connect not reachable at all behaves the same.
	if got := verifyCDCConnectorStarted(context.Background(), "http://127.0.0.1:1/connectors/x/status", false); got != "" {
		t.Fatalf("unreachable Connect must proceed, got %q", got)
	}
}

func TestVerifyCDCConnectorStarted_UnassignedNeverPassesEarly(t *testing.T) {
	// UNASSIGNED tasks are not a verdict: the check must wait out the window rather
	// than pass after the settle period.
	shrinkCDCStartCheck(t, 80*time.Millisecond, 10*time.Millisecond, 5*time.Millisecond)
	c := &connectStub{bodies: []string{statusBody("RUNNING", [2]string{"UNASSIGNED", ""})}}
	start := time.Now()
	if got := verifyCDCConnectorStarted(context.Background(), startStub(t, c), false); got != "" {
		t.Fatalf("got %q", got)
	}
	if time.Since(start) < 80*time.Millisecond {
		t.Fatalf("UNASSIGNED was treated as a pass before the window elapsed")
	}
}

func TestVerifyCDCConnectorStarted_AlreadyRunningRestartsFailedOnce(t *testing.T) {
	shrinkCDCStartCheck(t, 2*time.Second, 30*time.Millisecond, 5*time.Millisecond)
	failed := statusBody("RUNNING", [2]string{"FAILED", "com.mongodb.MongoCommandException: not authorized on admin to execute command { changeStream }"})
	c := &connectStub{
		bodies:       []string{failed},
		afterRestart: []string{statusBody("RUNNING", [2]string{"RUNNING", ""})},
	}
	if got := verifyCDCConnectorStarted(context.Background(), startStub(t, c), true); got != "" {
		t.Fatalf("a restarted task that holds RUNNING must pass, got %q", got)
	}
	if n := atomic.LoadInt32(&c.restarts); n != 1 {
		t.Fatalf("want exactly 1 restart, got %d", n)
	}
	if !strings.Contains(c.restartPath, "includeTasks=true") || !strings.Contains(c.restartPath, "onlyFailed=true") {
		t.Fatalf("restart must target only failed tasks, got %q", c.restartPath)
	}
}

func TestVerifyCDCConnectorStarted_AlreadyRunningStillFailedAfterRestart(t *testing.T) {
	shrinkCDCStartCheck(t, 2*time.Second, 30*time.Millisecond, 5*time.Millisecond)
	failed := statusBody("RUNNING", [2]string{"FAILED", "com.mongodb.MongoCommandException: not authorized"})
	c := &connectStub{bodies: []string{failed}}
	got := verifyCDCConnectorStarted(context.Background(), startStub(t, c), true)
	if !strings.Contains(got, "not authorized") {
		t.Fatalf("a task still FAILED after the one restart must fail the start, got %q", got)
	}
	if n := atomic.LoadInt32(&c.restarts); n != 1 {
		t.Fatalf("want exactly 1 restart, got %d", n)
	}
}

func TestVerifyCDCConnectorStarted_ContextCancelledProceeds(t *testing.T) {
	shrinkCDCStartCheck(t, 5*time.Second, 5*time.Second, 5*time.Millisecond)
	c := &connectStub{bodies: []string{statusBody("RUNNING", [2]string{"UNASSIGNED", ""})}}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	if got := verifyCDCConnectorStarted(ctx, startStub(t, c), false); got != "" {
		t.Fatalf("got %q", got)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("cancelled context did not stop the check")
	}
}

func TestClassifyConnectStatus(t *testing.T) {
	parse := func(s string) connectStatusDoc {
		var d connectStatusDoc
		if err := json.Unmarshal([]byte(s), &d); err != nil {
			t.Fatal(err)
		}
		return d
	}
	cases := []struct {
		name string
		body string
		want cdcStartState
	}{
		{"running", statusBody("RUNNING", [2]string{"RUNNING", ""}, [2]string{"RUNNING", ""}), cdcStartRunning},
		{"no tasks yet", statusBody("RUNNING"), cdcStartUnknown},
		{"one task unassigned", statusBody("RUNNING", [2]string{"RUNNING", ""}, [2]string{"UNASSIGNED", ""}), cdcStartUnknown},
		{"paused", statusBody("PAUSED", [2]string{"PAUSED", ""}), cdcStartUnknown},
		{"second task failed", statusBody("RUNNING", [2]string{"RUNNING", ""}, [2]string{"FAILED", "x"}), cdcStartFailed},
		{"connector failed", statusBody("FAILED", [2]string{"RUNNING", ""}), cdcStartFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, _ := classifyConnectStatus(parse(tc.body)); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestTraceHeadline(t *testing.T) {
	if got := traceHeadline("  \n\n"); got != "no error detail reported by Kafka Connect" {
		t.Fatalf("empty trace: %q", got)
	}
	if got := traceHeadline("java.lang.IllegalStateException: boom\n\tat a.b.C(C.java:1)"); got != "java.lang.IllegalStateException: boom" {
		t.Fatalf("first line: %q", got)
	}
	long := "x.Y: " + strings.Repeat("a", 2000)
	if got := traceHeadline(long); len([]rune(got)) > cdcStartTraceMaxRunes+16 {
		t.Fatalf("headline not capped: %d runes", len([]rune(got)))
	}
}

func TestStartResultBool(t *testing.T) {
	if startResultBool(nil, "already_running") {
		t.Fatal("nil result")
	}
	if !startResultBool(map[string]interface{}{"already_running": true}, "already_running") {
		t.Fatal("top-level true")
	}
	nested := map[string]interface{}{"result": map[string]interface{}{"already_running": true}}
	if !startResultBool(nested, "already_running") {
		t.Fatal("nested true")
	}
	if startResultBool(map[string]interface{}{"already_running": "true"}, "already_running") {
		t.Fatal("a string is not a bool")
	}
}

func TestKafkaConnectStatusURL(t *testing.T) {
	t.Setenv("KAFKA_CONNECT_URL", "")
	if got := kafkaConnectStatusURL("cdc-abcd1234"); got != "http://kafka-connect:8083/connectors/cdc-abcd1234/status" {
		t.Fatalf("default: %q", got)
	}
	t.Setenv("KAFKA_CONNECT_URL", "http://127.0.0.1:18083/")
	if got := kafkaConnectStatusURL("cdc-abcd1234"); got != "http://127.0.0.1:18083/connectors/cdc-abcd1234/status" {
		t.Fatalf("env: %q", got)
	}
}

// TestCDCStartCheckIsWiredBeforeTheRunIsReportedRunning guards the wiring, not the
// helper: in both CDC start paths the connector check must run after start_sync and
// before the run is handed to the sink / tracked as a running streaming pipeline.
// Without it the helper can be perfectly tested and never called (#19).
func TestCDCStartCheckIsWiredBeforeTheRunIsReportedRunning(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "executor.go", nil, 0)
	if err != nil {
		t.Fatalf("parse executor.go: %v", err)
	}
	cases := []struct {
		fn     string
		before string // the first call the check must precede
	}{
		{"executeStreamingDataTransfer", "startKafkaMCPSink"},
		{"executeStartStreaming", "AddStreamingPipeline"},
	}
	for _, tc := range cases {
		t.Run(tc.fn, func(t *testing.T) {
			var fn *ast.FuncDecl
			for _, d := range file.Decls {
				if f, ok := d.(*ast.FuncDecl); ok && f.Name.Name == tc.fn {
					fn = f
					break
				}
			}
			if fn == nil {
				t.Fatalf("%s not found in executor.go — move this guard with it rather than deleting it", tc.fn)
			}
			var lits []*ast.FuncLit
			ast.Inspect(fn, func(n ast.Node) bool {
				if fl, ok := n.(*ast.FuncLit); ok {
					lits = append(lits, fl)
				}
				return true
			})
			inClosure := func(p token.Pos) bool {
				for _, fl := range lits {
					if p > fl.Body.Lbrace && p < fl.Body.Rbrace {
						return true
					}
				}
				return false
			}
			firstCall := func(name string) token.Pos {
				var pos token.Pos
				ast.Inspect(fn, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok || inClosure(call.Pos()) || pos.IsValid() {
						return true
					}
					switch f := call.Fun.(type) {
					case *ast.Ident:
						if f.Name == name {
							pos = call.Pos()
						}
					case *ast.SelectorExpr:
						if f.Sel.Name == name {
							pos = call.Pos()
						}
					}
					return true
				})
				return pos
			}
			var startSyncPos token.Pos
			ast.Inspect(fn, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING || inClosure(lit.Pos()) || startSyncPos.IsValid() {
					return true
				}
				if v, err := strconv.Unquote(lit.Value); err == nil && v == "start_sync" {
					startSyncPos = lit.Pos()
				}
				return true
			})
			checkPos := firstCall("verifyCDCConnectorStarted")
			beforePos := firstCall(tc.before)
			if !startSyncPos.IsValid() || !beforePos.IsValid() {
				t.Fatalf("start_sync (%v) or %s (%v) not found in %s — re-point this guard",
					startSyncPos.IsValid(), tc.before, beforePos.IsValid(), tc.fn)
			}
			if !checkPos.IsValid() {
				t.Fatalf("%s never calls verifyCDCConnectorStarted: a Debezium task that FAILED on start would be reported as running (#19)", tc.fn)
			}
			if !(startSyncPos < checkPos && checkPos < beforePos) {
				t.Fatalf("in %s the connector check runs at %s; it must sit after start_sync (%s) and before %s (%s)",
					tc.fn, fset.Position(checkPos), fset.Position(startSyncPos), tc.before, fset.Position(beforePos))
			}
		})
	}
}
