package executor

import (
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// decodeResult builds a discover_schema result the way the MCP client does
// (JSON-decoded, so lists are []interface{} and objects map[string]interface{}).
func decodeResult(t *testing.T, raw string) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("bad fixture: %v", err)
	}
	return m
}

func TestDiscoveryFailure(t *testing.T) {
	cases := []struct {
		name        string
		result      string
		wantFailed  bool
		wantContain []string
		wantAbsent  []string
	}{
		{
			// The exact shape mongodb connector.py returns when the SRV lookup
			// fails: success at the MCP layer, failure only in overall_status.
			name: "mongodb failed with dns error",
			result: `{"success":true,"overall_status":"failed","tables":[],
				"warnings_messages":["The DNS query name does not exist: _mongodb._tcp.cluster0.example.net."]}`,
			wantFailed:  true,
			wantContain: []string{"The DNS query name does not exist"},
		},
		{
			// Database connectors set "failed", then add_warning(severity=error)
			// rewrites it to "partial_success" with zero tables.
			name: "partial_success with no tables and an error warning",
			result: `{"overall_status":"partial_success","tables":[],
				"warnings_objects":[{"category":"connection_error","severity":"error","message":"Schema discovery failed: connection refused"}],
				"warnings_messages":["Schema discovery failed: connection refused"]}`,
			wantFailed:  true,
			wantContain: []string{"connection refused"},
		},
		{
			name: "partial_success that still returned tables is a real partial result",
			result: `{"overall_status":"partial_success","tables":[{"name":"orders"}],
				"warnings_objects":[{"severity":"error","message":"users: permission denied"}]}`,
			wantFailed: false,
		},
		{
			name: "partial_success with no tables and only non-error warnings",
			result: `{"overall_status":"partial_success","tables":[],
				"warnings_objects":[{"severity":"warning","message":"row counts are estimates"}]}`,
			wantFailed: false,
		},
		{
			name:       "success with zero tables is an empty database, not a failure",
			result:     `{"overall_status":"success","tables":[],"warnings_messages":[]}`,
			wantFailed: false,
		},
		{
			name:       "v1 connector without overall_status",
			result:     `{"success":true,"tables":[]}`,
			wantFailed: false,
		},
		{
			name:        "failed with no message falls back to a generic one",
			result:      `{"overall_status":"failed","tables":[]}`,
			wantFailed:  true,
			wantContain: []string{"schema discovery failed"},
		},
		{
			name: "structured error is preferred over plain non-fatal warnings",
			result: `{"overall_status":"failed","tables":[],
				"warnings_objects":[{"severity":"error","message":"authentication failed"}],
				"warnings_messages":["authentication failed","sample truncated"]}`,
			wantFailed:  true,
			wantContain: []string{"authentication failed"},
			wantAbsent:  []string{"sample truncated", "authentication failed; authentication failed"},
		},
		{
			name:        "status is matched case-insensitively",
			result:      `{"overall_status":" FAILED ","error":"Missing 'database' in config"}`,
			wantFailed:  true,
			wantContain: []string{"Missing"},
		},
		{
			// The message can reach an LLM prompt, so credentials in a URI are scrubbed.
			name: "credentials in the connector's message are scrubbed",
			result: `{"overall_status":"failed",
				"warnings_messages":["could not connect to mongodb+srv://reader:s3cret@cluster0.example.net/shop"]}`,
			wantFailed:  true,
			wantContain: []string{"could not connect to mongodb+srv://"},
			wantAbsent:  []string{"s3cret", "reader:"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := discoveryFailure("mongodb", decodeResult(t, tc.result))
			if !tc.wantFailed {
				if err != nil {
					t.Fatalf("expected no failure, got %v", err)
				}
				return
			}
			var failed *DiscoveryFailedError
			if !errors.As(err, &failed) {
				t.Fatalf("expected *DiscoveryFailedError, got %v", err)
			}
			if failed.Connector != "mongodb" {
				t.Errorf("Connector = %q, want mongodb", failed.Connector)
			}
			for _, s := range tc.wantContain {
				if !strings.Contains(failed.Error(), s) {
					t.Errorf("message %q does not contain %q", failed.Error(), s)
				}
			}
			for _, s := range tc.wantAbsent {
				if strings.Contains(failed.Error(), s) {
					t.Errorf("message %q must not contain %q", failed.Error(), s)
				}
			}
		})
	}
}

func TestDiscoveryFailure_MessageIsBounded(t *testing.T) {
	long := strings.Repeat("x", 5000)
	err := discoveryFailure("postgresql", map[string]interface{}{
		"overall_status":    "failed",
		"warnings_messages": []interface{}{long},
	})
	if err == nil {
		t.Fatal("expected a failure")
	}
	if n := len([]rune(err.Error())); n > discoveryFailureMaxLen+1 {
		t.Fatalf("message is %d runes, want at most %d", n, discoveryFailureMaxLen+1)
	}
}

// Both discovery entry points must run the check. The MCP client is backed by
// real connector containers, so the wiring is pinned at the source level: a
// failed discovery skipping this call is exactly the "No tables found" bug.
// DiscoverSchema delegates to discoverSchemaWithTotals, which makes the call.
func TestDiscoverSchemaEntryPointsCheckOverallStatus(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "executor.go", nil, 0)
	if err != nil {
		t.Fatalf("parse executor.go: %v", err)
	}
	want := map[string]bool{"discoverSchemaWithTotals": false, "DiscoverSchemaEnvelope": false}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || fn.Body == nil {
			continue
		}
		if _, tracked := want[fn.Name.Name]; !tracked {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "discoveryFailure" {
					want[fn.Name.Name] = true
				}
			}
			return true
		})
	}
	for name, found := range want {
		if !found {
			t.Errorf("(*Agent).%s does not call discoveryFailure — a connector-reported failure would reach callers as an empty table list", name)
		}
	}
}

func TestDiscoveryFailure_NilResult(t *testing.T) {
	if err := discoveryFailure("mongodb", nil); err != nil {
		t.Fatalf("nil result: got %v", err)
	}
}
