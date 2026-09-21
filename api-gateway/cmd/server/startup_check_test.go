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
)

const gatewayStartupSecretValue = "unit-test-internal-secret-gateway-startup"

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestStartupSettingProblemsInternalSecret(t *testing.T) {
	const secretValue = gatewayStartupSecretValue

	cases := []struct {
		name     string
		env      map[string]string
		wantName string // empty means no problem expected
	}{
		{name: "missing", env: map[string]string{}, wantName: "INTERNAL_SERVICE_SECRET"},
		{name: "empty", env: map[string]string{"INTERNAL_SERVICE_SECRET": ""}, wantName: "INTERNAL_SERVICE_SECRET"},
		{name: "whitespace only", env: map[string]string{"INTERNAL_SERVICE_SECRET": "  \t"}, wantName: "INTERNAL_SERVICE_SECRET"},
		// Control: a neighbouring setting being present must not satisfy the check.
		{name: "only a different secret set", env: map[string]string{"JWT_SECRET": secretValue, "ENCRYPTION_KEY": secretValue}, wantName: "INTERNAL_SERVICE_SECRET"},
		{name: "present", env: map[string]string{"INTERNAL_SERVICE_SECRET": secretValue}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := startupSettingProblems(envFrom(tc.env))
			if tc.wantName == "" {
				if len(got) != 0 {
					t.Fatalf("expected no problems, got %q", got)
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("expected exactly one problem naming %s, got %d: %q", tc.wantName, len(got), got)
			}
			if !strings.HasPrefix(got[0], tc.wantName+" ") {
				t.Fatalf("problem does not start by naming %s: %q", tc.wantName, got[0])
			}
			// It must say what will not work, and what to do about it.
			for _, text := range []string{
				"scheduled saved-query runs", "503", "401",
				"openssl rand -hex 32",
				"same value to api-gateway, orchestrator, temporal-adapter and frontend",
			} {
				if !strings.Contains(got[0], text) {
					t.Fatalf("problem text does not say %q: %q", text, got[0])
				}
			}
			if strings.Contains(got[0], secretValue) {
				t.Fatalf("problem text leaked a configured value: %q", got[0])
			}
		})
	}
}

// The problems must reach the log as ERROR lines with the "Startup check: " prefix
// that docs/deployment/env-vars.md tells operators to grep for. A DEBUG or INFO line
// would be invisible or lost at the gateway's default level.
func TestReportStartupSettingProblemsLogsOneErrorLine(t *testing.T) {
	logger, hook := logtest.NewNullLogger()
	if logger.GetLevel() != log.InfoLevel {
		t.Fatalf("test logger must run at the services' default Info level, got %s", logger.GetLevel())
	}

	missing := envFrom(map[string]string{"JWT_SECRET": gatewayStartupSecretValue})
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
	if want := "Startup check: " + startupSettingProblems(missing)[0]; entries[0].Message != want {
		t.Fatalf("log line is not the full problem text:\n got %q\nwant %q", entries[0].Message, want)
	}
	if strings.Contains(entries[0].Message, gatewayStartupSecretValue) {
		t.Fatalf("log line leaked a configured value: %q", entries[0].Message)
	}

	// Control: nothing is logged when the secret is set.
	hook.Reset()
	reportStartupSettingProblems(logger, envFrom(map[string]string{"INTERNAL_SERVICE_SECRET": gatewayStartupSecretValue}))
	if n := len(hook.AllEntries()); n != 0 {
		t.Fatalf("expected no log entries when the secret is set, got %d", n)
	}
}

// The check only helps if main runs it against the real environment, on the standard
// logger, and before db.Init: db.Init exits the process when Postgres stays down, and
// a check placed after it would never be seen in exactly the broken starts it is for.
// Ordering is the point here, so this reads main.go's syntax tree; the controls below
// prove the reader rejects each wrong wiring.
func TestMainRunsStartupCheckBeforeDatabase(t *testing.T) {
	const dep = "db.Init"
	const ok = `package main
func main() {
	reportStartupSettingProblems(log.StandardLogger(), os.Getenv)
	if err := db.Init(); err != nil {
		log.Fatal(err)
	}
}`
	controls := map[string]string{
		"stubbed environment": strings.Replace(ok, "os.Getenv", `func(string) string { return "set" }`, 1),
		"other logger":        strings.Replace(ok, "log.StandardLogger()", "log.New()", 1),
		"after the database": `package main
func main() {
	if err := db.Init(); err != nil {
		log.Fatal(err)
	}
	reportStartupSettingProblems(log.StandardLogger(), os.Getenv)
}`,
		"only on one branch": strings.Replace(ok, "reportStartupSettingProblems(log.StandardLogger(), os.Getenv)",
			"if debug { reportStartupSettingProblems(log.StandardLogger(), os.Getenv) }", 1),
		"in a goroutine": strings.Replace(ok, "reportStartupSettingProblems(log.StandardLogger(), os.Getenv)",
			"go reportStartupSettingProblems(log.StandardLogger(), os.Getenv)", 1),
		"not called":       strings.Replace(ok, "reportStartupSettingProblems(log.StandardLogger(), os.Getenv)", "", 1),
		"database renamed": strings.Replace(ok, "db.Init()", "db.Open()", 1),
	}
	if problem := startupCheckWiringProblem(ok, dep); problem != "" {
		t.Fatalf("reader rejects correct wiring: %s", problem)
	}
	for name, src := range controls {
		if src == ok {
			t.Fatalf("control %q did not change the source", name)
		}
		if startupCheckWiringProblem(src, dep) == "" {
			t.Fatalf("reader accepts wrong wiring %q", name)
		}
	}

	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	if problem := startupCheckWiringProblem(string(src), dep); problem != "" {
		t.Fatal(problem)
	}
}

// startupCheckWiringProblem says why func main in src does not run
// reportStartupSettingProblems(log.StandardLogger(), os.Getenv) as a top-level
// statement before the first top-level statement that calls dep ("pkg.Func").
// It returns "" when the wiring is right.
func startupCheckWiringProblem(src, dep string) string {
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
		if checkAt < 0 && isStartupCheckStatement(stmt) {
			checkAt = i
		}
		if depAt < 0 && statementCalls(stmt, dep) {
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

func isStartupCheckStatement(stmt ast.Stmt) bool {
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
	if !isCall || len(logger.Args) != 0 || selectorName(logger.Fun) != "log.StandardLogger" {
		return false
	}
	return selectorName(call.Args[1]) == "os.Getenv"
}

func statementCalls(stmt ast.Stmt, dep string) bool {
	found := false
	ast.Inspect(stmt, func(n ast.Node) bool {
		if _, isLit := n.(*ast.FuncLit); isLit || found {
			return false
		}
		if call, isCall := n.(*ast.CallExpr); isCall && selectorName(call.Fun) == dep {
			found = true
		}
		return !found
	})
	return found
}

func selectorName(e ast.Expr) string {
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
