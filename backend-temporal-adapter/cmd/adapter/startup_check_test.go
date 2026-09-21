package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"

	"github.com/rsync-ai/backend-temporal-adapter/internal/workflows"
)

// Fixtures are deliberately not hex so secret scanners do not mistake them for keys.
const (
	adapterStartupSecretValue = "unit-test-internal-secret-adapter-startup"
	adapterGoodKey            = "unit-test-encryption-key-adapter-startup-check" // 46 characters
	adapterKey32              = "unit-test-encryption-key-32chars"               // 32 characters
	adapterKey31              = "unit-test-encryption-key-31char"                // 31 characters
	adapterKey30              = "unit-test-encryption-key-short"                 // 30 characters
	adapterKey20              = "unit-test-enc-key-20"                           // 20 characters
)

func adapterFixtureLengthsProblem() string {
	for key, want := range map[string]int{adapterGoodKey: 46, adapterKey32: 32, adapterKey31: 31, adapterKey30: 30, adapterKey20: 20} {
		if len(key) != want {
			return "fixture key has the wrong length"
		}
	}
	return ""
}

// What every ENCRYPTION_KEY problem must say will not work. The three self-healing
// activities that decrypt a connection's stored settings are
// FetchConnectionOAuthTokenIDActivity, PlanSchemaDriftRepairActivity and
// ApplySchemaDDLActivity.
var encryptionKeyConsequenceText = []string{"OAuth token", "schema-drift repair", "apply the repair"}

// What the secret problem must tell the operator to do.
var secretRemediationText = []string{
	"openssl rand -hex 32",
	"same value to api-gateway, orchestrator, temporal-adapter and frontend",
}

func TestStartupSettingProblems(t *testing.T) {
	if problem := adapterFixtureLengthsProblem(); problem != "" {
		t.Fatal(problem)
	}
	const secretValue = adapterStartupSecretValue
	keyText := append([]string{"cannot decrypt", "same value"}, encryptionKeyConsequenceText...)
	secretText := append([]string{"Scheduled saved-query runs", "freshness sweep"}, secretRemediationText...)

	cases := []struct {
		name      string
		env       map[string]string
		wantNames []string // settings that must be named, in order; empty means no problem expected
		wantText  []string // what will not work and what to do
		notText   []string // text that would be the wrong diagnosis
	}{
		{
			name:      "secret missing, production with a good key",
			env:       map[string]string{"ENVIRONMENT": "production", "ENCRYPTION_KEY": adapterGoodKey},
			wantNames: []string{"INTERNAL_SERVICE_SECRET"},
			wantText:  secretText,
		},
		{
			name:      "secret whitespace only",
			env:       map[string]string{"ENVIRONMENT": "production", "ENCRYPTION_KEY": adapterGoodKey, "INTERNAL_SERVICE_SECRET": "  "},
			wantNames: []string{"INTERNAL_SERVICE_SECRET"},
			wantText:  secretText,
		},
		{
			name:      "key missing in production",
			env:       map[string]string{"ENVIRONMENT": "production", "INTERNAL_SERVICE_SECRET": secretValue},
			wantNames: []string{"ENCRYPTION_KEY"},
			wantText:  append([]string{"ENCRYPTION_KEY is not set, empty or only spaces", "not ENCRYPTION_KEYS"}, keyText...),
			notText:   []string{"shorter than 32"},
		},
		{
			// The decrypt helper treats an unset ENVIRONMENT as not development.
			name:      "key missing and ENVIRONMENT unset",
			env:       map[string]string{"INTERNAL_SERVICE_SECRET": secretValue},
			wantNames: []string{"ENCRYPTION_KEY"},
			wantText:  keyText,
		},
		{
			// The decrypt helper matches ENVIRONMENT exactly, so this gets no dev key.
			name:      "key missing and ENVIRONMENT is Development",
			env:       map[string]string{"ENVIRONMENT": "Development", "INTERNAL_SERVICE_SECRET": secretValue},
			wantNames: []string{"ENCRYPTION_KEY"},
			wantText:  keyText,
		},
		{
			name:      "key missing and ENVIRONMENT has a leading space",
			env:       map[string]string{"ENVIRONMENT": " development", "INTERNAL_SERVICE_SECRET": secretValue},
			wantNames: []string{"ENCRYPTION_KEY"},
			wantText:  keyText,
		},
		{
			// Only ENCRYPTION_KEYS set: the decrypt helper does not read it.
			name:      "only ENCRYPTION_KEYS in production",
			env:       map[string]string{"ENVIRONMENT": "production", "ENCRYPTION_KEYS": adapterGoodKey, "INTERNAL_SERVICE_SECRET": secretValue},
			wantNames: []string{"ENCRYPTION_KEY"},
			wantText:  append([]string{"not ENCRYPTION_KEYS"}, keyText...),
		},
		{
			// The helper trims, so this is an empty key, not a short one.
			name:      "key only spaces in production",
			env:       map[string]string{"ENVIRONMENT": "production", "ENCRYPTION_KEY": "   ", "INTERNAL_SERVICE_SECRET": secretValue},
			wantNames: []string{"ENCRYPTION_KEY"},
			wantText:  append([]string{"ENCRYPTION_KEY is not set, empty or only spaces"}, keyText...),
			notText:   []string{"shorter than 32"},
		},
		{
			name:      "31-character key in production",
			env:       map[string]string{"ENVIRONMENT": "production", "ENCRYPTION_KEY": adapterKey31, "INTERNAL_SERVICE_SECRET": secretValue},
			wantNames: []string{"ENCRYPTION_KEY"},
			wantText:  append([]string{"ENCRYPTION_KEY is shorter than 32 characters", "at least 32 characters"}, keyText...),
			notText:   []string{"not set, empty"},
		},
		{
			name:      "20-character key in production",
			env:       map[string]string{"ENVIRONMENT": "production", "ENCRYPTION_KEY": adapterKey20, "INTERNAL_SERVICE_SECRET": secretValue},
			wantNames: []string{"ENCRYPTION_KEY"},
			wantText:  append([]string{"ENCRYPTION_KEY is shorter than 32 characters"}, keyText...),
		},
		{
			name:      "short key even in development",
			env:       map[string]string{"ENVIRONMENT": "development", "ENCRYPTION_KEY": adapterKey30, "INTERNAL_SERVICE_SECRET": secretValue},
			wantNames: []string{"ENCRYPTION_KEY"},
			wantText:  append([]string{"ENCRYPTION_KEY is shorter than 32 characters"}, keyText...),
		},
		{
			name:      "both missing in production",
			env:       map[string]string{"ENVIRONMENT": "production"},
			wantNames: []string{"INTERNAL_SERVICE_SECRET", "ENCRYPTION_KEY"},
			wantText:  append(append([]string{}, secretText...), keyText...),
		},
		// Controls: everything present, a key of exactly 32 characters, and
		// development's built-in key when the key is missing or only spaces.
		{name: "all present in production", env: map[string]string{"ENVIRONMENT": "production", "ENCRYPTION_KEY": adapterGoodKey, "INTERNAL_SERVICE_SECRET": secretValue}},
		{name: "32-character key in production", env: map[string]string{"ENVIRONMENT": "production", "ENCRYPTION_KEY": adapterKey32, "INTERNAL_SERVICE_SECRET": secretValue}},
		{name: "key missing in development", env: map[string]string{"ENVIRONMENT": "development", "INTERNAL_SERVICE_SECRET": secretValue}},
		{name: "key missing in dev", env: map[string]string{"ENVIRONMENT": "dev", "INTERNAL_SERVICE_SECRET": secretValue}},
		{name: "key only spaces in development", env: map[string]string{"ENVIRONMENT": "development", "ENCRYPTION_KEY": "   ", "INTERNAL_SERVICE_SECRET": secretValue}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := startupSettingProblems(func(k string) string { return tc.env[k] })
			if len(tc.wantNames) == 0 {
				if len(got) != 0 {
					t.Fatalf("expected no problems, got %q", got)
				}
				return
			}
			if len(got) != len(tc.wantNames) {
				t.Fatalf("expected %d problem(s) naming %v, got %d: %q", len(tc.wantNames), tc.wantNames, len(got), got)
			}
			joined := strings.Join(got, "\n")
			for i, name := range tc.wantNames {
				if !strings.HasPrefix(got[i], name+" ") {
					t.Fatalf("problem %d does not start by naming %s: %q", i, name, got[i])
				}
			}
			for _, text := range tc.wantText {
				if !strings.Contains(joined, text) {
					t.Fatalf("problem text does not say %q: %q", text, joined)
				}
			}
			for _, text := range tc.notText {
				if strings.Contains(joined, text) {
					t.Fatalf("problem text wrongly says %q: %q", text, joined)
				}
			}
			for _, value := range []string{secretValue, adapterGoodKey, adapterKey32, adapterKey31, adapterKey30, adapterKey20} {
				if strings.Contains(joined, value) {
					t.Fatalf("problem text leaked a configured value: %q", joined)
				}
			}
		})
	}
}

// The ENCRYPTION_KEY rule is only right if it flags exactly the environments in which
// the real decrypt path refuses the key. This runs a real self-healing activity
// (ApplySchemaDDLActivity) against a one-row fake database and compares: the helper
// answers "invalid encryption key" when it rejects the key, and "invalid ciphertext"
// when it accepts the key but the stored value is not a ciphertext.
func TestStartupKeyRuleMatchesDecryptHelper(t *testing.T) {
	if problem := adapterFixtureLengthsProblem(); problem != "" {
		t.Fatal(problem)
	}
	db := openFakeConnectionsDB(t)
	workflows.InitActivityContext(nil)
	workflows.SetDB(db)
	t.Cleanup(func() { workflows.SetDB(nil) })

	cases := []struct{ environment, key, keys string }{
		{"production", "", ""},
		{"", "", ""},
		{"Development", "", ""},
		{" development", "", ""},
		{"development", "", ""},
		{"dev", "", ""},
		{"production", "", adapterGoodKey},
		{"production", "   ", ""},
		{"development", "   ", ""},
		{"production", adapterKey20, ""},
		{"production", adapterKey31, ""},
		{"production", adapterKey32, ""},
		{"production", adapterGoodKey, ""},
		{"development", adapterKey30, ""},
		{"development", adapterKey32, ""},
	}
	rejected, accepted := 0, 0
	for i, tc := range cases {
		t.Run(fmt.Sprintf("ENVIRONMENT=%q key length %d keyring length %d", tc.environment, len(tc.key), len(tc.keys)), func(t *testing.T) {
			t.Setenv("ENVIRONMENT", tc.environment)
			t.Setenv("ENCRYPTION_KEY", tc.key)
			t.Setenv("ENCRYPTION_KEYS", tc.keys)

			err := workflows.ApplySchemaDDLActivity(context.Background(), "unit-test-connection", []string{"SELECT 1"})
			if err == nil {
				t.Fatalf("case %d: the activity decrypted a fixture that is not a ciphertext", i)
			}
			var helperRejectsKey bool
			switch err.Error() {
			case "invalid encryption key":
				helperRejectsKey = true
				rejected++
			case "invalid ciphertext":
				accepted++
			default:
				t.Fatalf("case %d: the activity failed before decrypting, so this test proves nothing: %v", i, err)
			}

			flagged := false
			for _, p := range startupSettingProblems(os.Getenv) {
				if strings.HasPrefix(p, "ENCRYPTION_KEY ") {
					flagged = true
				}
			}
			if flagged != helperRejectsKey {
				t.Fatalf("case %d: startup check flags the key=%v, but the decrypt helper rejects it=%v", i, flagged, helperRejectsKey)
			}
		})
	}
	if rejected == 0 || accepted == 0 {
		t.Fatalf("cases must include keys the helper rejects and keys it accepts: rejected=%d accepted=%d", rejected, accepted)
	}
}

// The problems must reach the log as ERROR lines with the "Startup check: " prefix
// that docs/deployment/env-vars.md tells operators to grep for. A DEBUG line would be
// invisible at the adapter's default Info level.
func TestReportStartupSettingProblemsLogsOneErrorLine(t *testing.T) {
	logger, hook := logtest.NewNullLogger()
	if logger.GetLevel() != log.InfoLevel {
		t.Fatalf("test logger must run at the services' default Info level, got %s", logger.GetLevel())
	}

	env := map[string]string{"ENVIRONMENT": "production", "ENCRYPTION_KEYS": adapterGoodKey}
	getenv := func(k string) string { return env[k] }
	problems := startupSettingProblems(getenv)
	if len(problems) != 2 {
		t.Fatalf("fixture must produce two problems, got %d", len(problems))
	}

	reportStartupSettingProblems(logger, getenv)
	entries := hook.AllEntries()
	if len(entries) != len(problems) {
		t.Fatalf("expected %d log entries, got %d", len(problems), len(entries))
	}
	for i, entry := range entries {
		if entry.Level != log.ErrorLevel {
			t.Fatalf("entry %d: expected ERROR level, got %s", i, entry.Level)
		}
		if want := "Startup check: " + problems[i]; entry.Message != want {
			t.Fatalf("entry %d is not the prefixed problem text:\n got %q\nwant %q", i, entry.Message, want)
		}
		if strings.Contains(entry.Message, adapterGoodKey) {
			t.Fatalf("entry %d leaked a configured value", i)
		}
	}
	if !strings.HasPrefix(entries[0].Message, "Startup check: INTERNAL_SERVICE_SECRET ") ||
		!strings.HasPrefix(entries[1].Message, "Startup check: ENCRYPTION_KEY ") {
		t.Fatalf("log lines do not start with the documented prefix and the setting: %q", []string{entries[0].Message, entries[1].Message})
	}

	// Control: nothing is logged when both settings are right.
	hook.Reset()
	env = map[string]string{"ENVIRONMENT": "production", "ENCRYPTION_KEY": adapterGoodKey, "INTERNAL_SERVICE_SECRET": adapterStartupSecretValue}
	reportStartupSettingProblems(logger, getenv)
	if n := len(hook.AllEntries()); n != 0 {
		t.Fatalf("expected no log entries when both settings are right, got %d", n)
	}
}

// The check only helps if main runs it against the real environment, on the standard
// logger, and before db.Init and the Temporal client (which exits the process when it
// cannot connect). Ordering is the point here, so this reads main.go's syntax tree;
// the controls below prove the reader rejects each wrong wiring.
func TestMainRunsStartupCheckBeforeDatabase(t *testing.T) {
	const dep = "db.Init"
	const ok = `package main
func main() {
	reportStartupSettingProblems(log.StandardLogger(), os.Getenv)
	if err := db.Init(); err != nil {
		log.Warn(err)
	}
}`
	const call = "reportStartupSettingProblems(log.StandardLogger(), os.Getenv)"
	controls := map[string]string{
		"stubbed environment": strings.Replace(ok, "os.Getenv", `func(string) string { return "set" }`, 1),
		"other logger":        strings.Replace(ok, "log.StandardLogger()", "log.New()", 1),
		"after the database": `package main
func main() {
	if err := db.Init(); err != nil {
		log.Warn(err)
	}
	reportStartupSettingProblems(log.StandardLogger(), os.Getenv)
}`,
		"only on one branch": strings.Replace(ok, call, "if debug { "+call+" }", 1),
		"in a goroutine":     strings.Replace(ok, call, "go "+call, 1),
		"not called":         strings.Replace(ok, call, "", 1),
		"database renamed":   strings.Replace(ok, "db.Init()", "db.Open()", 1),
	}
	if problem := adapterStartupWiringProblem(ok, dep); problem != "" {
		t.Fatalf("reader rejects correct wiring: %s", problem)
	}
	for name, src := range controls {
		if src == ok {
			t.Fatalf("control %q did not change the source", name)
		}
		if adapterStartupWiringProblem(src, dep) == "" {
			t.Fatalf("reader accepts wrong wiring %q", name)
		}
	}

	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	if problem := adapterStartupWiringProblem(string(src), dep); problem != "" {
		t.Fatal(problem)
	}
}

// adapterStartupWiringProblem says why func main in src does not run
// reportStartupSettingProblems(log.StandardLogger(), os.Getenv) as a top-level
// statement before the first top-level statement that calls dep ("pkg.Func").
// It returns "" when the wiring is right.
func adapterStartupWiringProblem(src, dep string) string {
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
		if checkAt < 0 && isAdapterStartupCheckStatement(stmt) {
			checkAt = i
		}
		if depAt < 0 && adapterStatementCalls(stmt, dep) {
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

func isAdapterStartupCheckStatement(stmt ast.Stmt) bool {
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
	if !isCall || len(logger.Args) != 0 || adapterSelectorName(logger.Fun) != "log.StandardLogger" {
		return false
	}
	return adapterSelectorName(call.Args[1]) == "os.Getenv"
}

func adapterStatementCalls(stmt ast.Stmt, dep string) bool {
	found := false
	ast.Inspect(stmt, func(n ast.Node) bool {
		if _, isLit := n.(*ast.FuncLit); isLit || found {
			return false
		}
		if call, isCall := n.(*ast.CallExpr); isCall && adapterSelectorName(call.Fun) == dep {
			found = true
		}
		return !found
	})
	return found
}

func adapterSelectorName(e ast.Expr) string {
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

// A database/sql driver whose every query returns one connections row whose config
// column is not a ciphertext. It lets a real activity reach decryptString without a
// database.
const fakeConnectionsDriverName = "adapter-startup-check-fake-connections"

var registerFakeConnectionsDriver sync.Once

func openFakeConnectionsDB(t *testing.T) *sql.DB {
	t.Helper()
	registerFakeConnectionsDriver.Do(func() { sql.Register(fakeConnectionsDriverName, fakeConnectionsDriver{}) })
	db, err := sql.Open(fakeConnectionsDriverName, "")
	if err != nil {
		t.Fatalf("open fake database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type fakeConnectionsDriver struct{}

func (fakeConnectionsDriver) Open(string) (driver.Conn, error) { return fakeConnectionsConn{}, nil }

type fakeConnectionsConn struct{}

func (fakeConnectionsConn) Prepare(string) (driver.Stmt, error) { return fakeConnectionsStmt{}, nil }
func (fakeConnectionsConn) Close() error                        { return nil }
func (fakeConnectionsConn) Begin() (driver.Tx, error) {
	return nil, errors.New("fake connections database has no transactions")
}

type fakeConnectionsStmt struct{}

func (fakeConnectionsStmt) Close() error  { return nil }
func (fakeConnectionsStmt) NumInput() int { return -1 }
func (fakeConnectionsStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, errors.New("fake connections database is read-only")
}
func (fakeConnectionsStmt) Query([]driver.Value) (driver.Rows, error) {
	return &fakeConnectionsRows{}, nil
}

type fakeConnectionsRows struct{ done bool }

func (*fakeConnectionsRows) Columns() []string {
	return []string{"type", "connector_type", "connector_version", "config"}
}
func (*fakeConnectionsRows) Close() error { return nil }
func (r *fakeConnectionsRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0], dest[1], dest[2], dest[3] = "destination", "postgresql", "v1.0.0", "not-a-stored-ciphertext"
	return nil
}
