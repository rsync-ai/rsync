package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/rsync-ai/backend-temporal-adapter/internal/workflows"
)

// A workflow schedules an activity by name, and an activity the worker never
// registered fails only when that happens — never at startup, never at compile time.
// So this runs a model run whose rebuild never reports a result through exactly the
// registrations the worker gets, against a stand-in gateway, and checks the failure
// record arrives. Nothing here reaches a real gateway.

// The test environment is used as the registry below; this keeps it one.
var _ worker.Registry = (*testsuite.TestWorkflowEnvironment)(nil)

type registrationGateway struct {
	mu          sync.Mutex
	runCalls    int
	recordCalls int
	recordPath  string
	recordBody  map[string]any
}

func TestWorkerRegistrations_AModelRunThatNeverReportedIsRecorded(t *testing.T) {
	g := &registrationGateway{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		g.mu.Lock()
		defer g.mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/run"):
			g.runCalls++
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("upstream unavailable"))
		case strings.HasSuffix(r.URL.Path, "/run-failed"):
			g.recordCalls++
			g.recordPath = r.URL.Path
			_ = json.Unmarshal(raw, &g.recordBody)
			_, _ = w.Write([]byte(`{"recorded":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	t.Setenv("API_GATEWAY_URL", srv.URL)
	t.Setenv("INTERNAL_SERVICE_SECRET", "worker-registration-unit-test-secret")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	env.ExecuteWorkflow(workflows.ScheduledModelRunWorkflow, workflows.ScheduledModelRunInput{
		ScheduleID: "sched-reg-1", SavedQueryID: "model-reg-1",
	})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if werr := env.GetWorkflowError(); werr == nil || !strings.Contains(werr.Error(), "model run failed") {
		t.Fatalf("workflow error = %v, want the run's own failure", werr)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	// Denominator: the run activity was registered, reached the stand-in and spent its budget.
	if g.runCalls != 3 {
		t.Fatalf("run endpoint called %d times, want 3", g.runCalls)
	}
	if g.recordCalls != 1 {
		t.Fatalf("failed-run record endpoint called %d times, want 1 — is RecordModelRunFailureActivity registered on the worker?", g.recordCalls)
	}
	if g.recordPath != "/api/v1/internal/explorer/models/model-reg-1/run-failed" || g.recordBody["schedule_id"] != "sched-reg-1" {
		t.Errorf("record sent to %s with body %v, want model-reg-1 / sched-reg-1", g.recordPath, g.recordBody)
	}
}

// The registrations only count if main applies them to the worker it starts, and
// before it starts it (the SDK refuses registration on a started worker). Ordering is
// the point, so this reads main.go's syntax tree; the controls prove the reader
// rejects each wrong wiring.
func TestMainRegistersOnTheWorkerBeforeStartingIt(t *testing.T) {
	const ok = `package main
func main() {
	w := worker.New(c, q, worker.Options{})
	registerWorkflowsAndActivities(w)
	if err := w.Start(); err != nil {
		log.Fatal(err)
	}
}`
	const call = "registerWorkflowsAndActivities(w)"
	controls := map[string]string{
		"not called":         strings.Replace(ok, call, "", 1),
		"another registry":   strings.Replace(ok, call, "registerWorkflowsAndActivities(other)", 1),
		"after start":        strings.Replace(strings.Replace(ok, call+"\n", "", 1), "\tlog.Fatal(err)\n\t}\n", "\tlog.Fatal(err)\n\t}\n\t"+call+"\n", 1),
		"only on one branch": strings.Replace(ok, call, "if debug { "+call+" }", 1),
		"in a goroutine":     strings.Replace(ok, call, "go "+call, 1),
		"worker not started": strings.Replace(ok, "w.Start()", "w.Run(nil)", 1),
	}
	if problem := workerRegistrationWiringProblem(ok); problem != "" {
		t.Fatalf("reader rejects correct wiring: %s", problem)
	}
	for name, src := range controls {
		if src == ok {
			t.Fatalf("control %q did not change the source", name)
		}
		if workerRegistrationWiringProblem(src) == "" {
			t.Fatalf("reader accepts wrong wiring %q", name)
		}
	}

	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	if problem := workerRegistrationWiringProblem(string(src)); problem != "" {
		t.Fatal(problem)
	}
}

// workerRegistrationWiringProblem says why func main in src does not call
// registerWorkflowsAndActivities(w), as a top-level statement, on the w assigned from
// worker.New, before the top-level statement that calls w.Start(). "" means right.
func workerRegistrationWiringProblem(src string) string {
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", src, 0)
	if err != nil {
		return "parse main.go: " + err.Error()
	}
	var body *ast.BlockStmt
	for _, decl := range file.Decls {
		if fn, isFunc := decl.(*ast.FuncDecl); isFunc && fn.Recv == nil && fn.Name.Name == "main" {
			body = fn.Body
		}
	}
	if body == nil {
		return "main.go has no func main"
	}

	workerVar, registerAt, startAt := "", -1, -1
	for i, stmt := range body.List {
		if assign, isAssign := stmt.(*ast.AssignStmt); isAssign && workerVar == "" && len(assign.Lhs) == 1 && len(assign.Rhs) == 1 {
			if isSelectorCall(assign.Rhs[0], "worker", "New") {
				if id, isIdent := assign.Lhs[0].(*ast.Ident); isIdent {
					workerVar = id.Name
				}
			}
			continue
		}
		if workerVar == "" {
			continue
		}
		if expr, isExpr := stmt.(*ast.ExprStmt); isExpr && registerAt < 0 {
			if call, isCall := expr.X.(*ast.CallExpr); isCall && len(call.Args) == 1 {
				fn, fnIsIdent := call.Fun.(*ast.Ident)
				arg, argIsIdent := call.Args[0].(*ast.Ident)
				if fnIsIdent && fn.Name == "registerWorkflowsAndActivities" && argIsIdent && arg.Name == workerVar {
					registerAt = i
					continue
				}
			}
		}
		if startAt < 0 {
			ast.Inspect(stmt, func(n ast.Node) bool {
				if e, isExpr := n.(ast.Expr); isExpr && isSelectorCall(e, workerVar, "Start") {
					startAt = i
					return false
				}
				return true
			})
		}
	}
	switch {
	case workerVar == "":
		return "main.go: func main never assigns a worker from worker.New"
	case startAt < 0:
		return "main.go: func main never calls " + workerVar + ".Start()"
	case registerAt < 0:
		return "main.go: func main does not call registerWorkflowsAndActivities(" + workerVar + ") as a top-level statement"
	case registerAt > startAt:
		return "main.go: registerWorkflowsAndActivities runs after " + workerVar + ".Start()"
	}
	return ""
}

func isSelectorCall(e ast.Expr, recv, name string) bool {
	call, isCall := e.(*ast.CallExpr)
	if !isCall {
		return false
	}
	sel, isSel := call.Fun.(*ast.SelectorExpr)
	if !isSel || sel.Sel.Name != name {
		return false
	}
	x, isIdent := sel.X.(*ast.Ident)
	return isIdent && x.Name == recv
}
