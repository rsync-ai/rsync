package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"

	"github.com/rsync-ai/backend-orchestrator/internal/config"
)

const orchestratorStartupSecretValue = "unit-test-internal-secret-orchestrator-main"

func startupCheckEnv(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// The problems must reach the log as ERROR lines with the "Startup check: " prefix
// that docs/deployment/env-vars.md tells operators to grep for. A DEBUG line would be
// invisible at the orchestrator's default Info level.
func TestReportStartupSettingProblemsLogsOneErrorLine(t *testing.T) {
	logger, hook := logtest.NewNullLogger()
	if logger.GetLevel() != log.InfoLevel {
		t.Fatalf("test logger must run at the services' default Info level, got %s", logger.GetLevel())
	}

	missing := startupCheckEnv(map[string]string{"JWT_SECRET": orchestratorStartupSecretValue})
	problems := config.StartupSettingProblems(missing)
	if len(problems) != 1 {
		t.Fatalf("fixture must produce exactly one problem, got %d", len(problems))
	}

	reportStartupSettingProblems(logger, missing)
	entries := hook.AllEntries()
	if len(entries) != 1 {
		t.Fatalf("expected one log entry for a missing secret, got %d", len(entries))
	}
	if entries[0].Level != log.ErrorLevel {
		t.Fatalf("expected ERROR level, got %s", entries[0].Level)
	}
	if !strings.HasPrefix(entries[0].Message, "Startup check: INTERNAL_SERVICE_SECRET ") {
		t.Fatalf("log line does not start with the documented prefix and the setting: %q", entries[0].Message)
	}
	if want := "Startup check: " + problems[0]; entries[0].Message != want {
		t.Fatalf("log line is not the full problem text:\n got %q\nwant %q", entries[0].Message, want)
	}
	if strings.Contains(entries[0].Message, orchestratorStartupSecretValue) {
		t.Fatalf("log line leaked a configured value: %q", entries[0].Message)
	}

	// Control: nothing is logged when the secret is set.
	hook.Reset()
	reportStartupSettingProblems(logger, startupCheckEnv(map[string]string{"INTERNAL_SERVICE_SECRET": orchestratorStartupSecretValue}))
	if n := len(hook.AllEntries()); n != 0 {
		t.Fatalf("expected no log entries when the secret is set, got %d", n)
	}
}

// The check only helps if main runs it against the real environment, on the standard
// logger, and before sql.Open: the database calls right after it exit the process
// when Postgres is unreachable, and a check placed after them would never be seen in
// exactly the broken starts it is for. Ordering is the point here, so this reads
// main.go's syntax tree; the controls below prove the reader rejects each wrong wiring.
func TestMainRunsStartupCheckBeforeDatabase(t *testing.T) {
	const dep = "sql.Open"
	const ok = `package main
func main() {
	reportStartupSettingProblems(log.StandardLogger(), os.Getenv)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}
	_ = db
}`
	const call = "reportStartupSettingProblems(log.StandardLogger(), os.Getenv)"
	controls := map[string]string{
		"stubbed environment": strings.Replace(ok, "os.Getenv", `func(string) string { return "set" }`, 1),
		"other logger":        strings.Replace(ok, "log.StandardLogger()", "log.New()", 1),
		"after the database": `package main
func main() {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}
	reportStartupSettingProblems(log.StandardLogger(), os.Getenv)
	_ = db
}`,
		"only on one branch": strings.Replace(ok, call, "if debug { "+call+" }", 1),
		"in a goroutine":     strings.Replace(ok, call, "go "+call, 1),
		"not called":         strings.Replace(ok, call, "", 1),
		"database renamed":   strings.Replace(ok, "sql.Open(", "sqlx.Open(", 1),
	}
	if problem := orchestratorStartupWiringProblem(ok, dep); problem != "" {
		t.Fatalf("reader rejects correct wiring: %s", problem)
	}
	for name, src := range controls {
		if src == ok {
			t.Fatalf("control %q did not change the source", name)
		}
		if orchestratorStartupWiringProblem(src, dep) == "" {
			t.Fatalf("reader accepts wrong wiring %q", name)
		}
	}

	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	if problem := orchestratorStartupWiringProblem(string(src), dep); problem != "" {
		t.Fatal(problem)
	}
}

// orchestratorStartupWiringProblem says why func main in src does not run
// reportStartupSettingProblems(log.StandardLogger(), os.Getenv) as a top-level
// statement before the first top-level statement that calls dep ("pkg.Func").
// It returns "" when the wiring is right.
func orchestratorStartupWiringProblem(src, dep string) string {
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
	checkAt, depAt := -1, -1
	for i, stmt := range body.List {
		if checkAt < 0 && isOrchestratorStartupCheckStatement(stmt) {
			checkAt = i
		}
		if depAt < 0 && orchestratorStatementCalls(stmt, dep) {
			depAt = i
		}
	}
	switch {
	case checkAt < 0:
		return "func main does not call reportStartupSettingProblems(log.StandardLogger(), os.Getenv) as a top-level statement"
	case depAt < 0:
		return "func main does not call " + dep + "; update this test to the service's first dependency call"
	case checkAt > depAt:
		return "func main calls " + dep + " before reportStartupSettingProblems"
	}
	return ""
}

func isOrchestratorStartupCheckStatement(stmt ast.Stmt) bool {
	exprStmt, isExpr := stmt.(*ast.ExprStmt)
	if !isExpr {
		return false
	}
	call, isCall := exprStmt.X.(*ast.CallExpr)
	if !isCall || len(call.Args) != 2 {
		return false
	}
	if name, isIdent := call.Fun.(*ast.Ident); !isIdent || name.Name != "reportStartupSettingProblems" {
		return false
	}
	logger, isCall := call.Args[0].(*ast.CallExpr)
	if !isCall || len(logger.Args) != 0 || orchestratorSelectorName(logger.Fun) != "log.StandardLogger" {
		return false
	}
	return orchestratorSelectorName(call.Args[1]) == "os.Getenv"
}

func orchestratorStatementCalls(stmt ast.Stmt, dep string) bool {
	found := false
	ast.Inspect(stmt, func(n ast.Node) bool {
		if _, isLit := n.(*ast.FuncLit); isLit || found {
			return false
		}
		if call, isCall := n.(*ast.CallExpr); isCall && orchestratorSelectorName(call.Fun) == dep {
			found = true
		}
		return !found
	})
	return found
}

func orchestratorSelectorName(e ast.Expr) string {
	sel, isSel := e.(*ast.SelectorExpr)
	if !isSel {
		return ""
	}
	pkg, isIdent := sel.X.(*ast.Ident)
	if !isIdent {
		return ""
	}
	return pkg.Name + "." + sel.Sel.Name
}
