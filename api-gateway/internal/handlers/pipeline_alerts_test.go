package handlers

// GET /api/v1/pipelines/:id/alerts exists to undo a scoping mistake, and these tests
// pin both halves of it.
//
// The pipeline page's lag panel used to read GET /api/v1/monitoring/sentinel/issues,
// which is the ADMIN infrastructure view and carries two gates:
//
//   - FEATURE_MONITORING_INFRA, default false -> 404
//   - a PLATFORM power_user/admin role        -> 403
//
// CDCLagAlertsPanel treats 404 and 403 as "not enabled here" and hides itself, so on
// a default deployment, and for every ordinary workspace member, the Sentinel would
// detect a stalled CDC sink, write cdc-sink-lag-<id>, and the person who owns the
// pipeline would never be told. The product line is: INFRASTRUCTURE monitoring is
// admin (it names topics and containers across every workspace); a PIPELINE's own
// health belongs to whoever owns the pipeline.
//
// So this route must (1) carry no feature flag and no platform-role gate, and
// (2) still be tenant-scoped, through the repo's own IDOR chokepoint, at the Viewer
// floor every other pipeline GET uses. (2) is the part that would be a vulnerability
// if it regressed, so it is asserted against the source rather than trusted.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// alertsHandlerSource parses this package's pipeline_alerts.go and returns the
// GetPipelineAlerts function body as text.
func alertsHandlerSource(t *testing.T) string {
	t.Helper()
	src, err := os.ReadFile("pipeline_alerts.go")
	if err != nil {
		t.Fatalf("read pipeline_alerts.go: %v", err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "pipeline_alerts.go", src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	for _, decl := range file.Decls {
		fn, isFn := decl.(*ast.FuncDecl)
		if !isFn || fn.Name.Name != "GetPipelineAlerts" || fn.Recv != nil {
			continue
		}
		return string(src[fset.Position(fn.Pos()).Offset:fset.Position(fn.End()).Offset])
	}
	t.Fatal("GetPipelineAlerts not found in pipeline_alerts.go")
	return ""
}

// The IDOR gate. Without it, any authenticated user could read any pipeline's
// failure descriptions by id — which is exactly the cross-tenant leak
// sentinelIssueTenantPredicate was written to close on the admin route.
func TestPipelineAlerts_IsTenantScopedAtViewer(t *testing.T) {
	body := alertsHandlerSource(t)

	if !strings.Contains(body, "requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer)") {
		t.Error("GetPipelineAlerts does not gate on requirePipelineWorkspaceRole(..., WSViewer); " +
			"any signed-in user could read another tenant's pipeline alerts")
	}
	// The id must be validated before it reaches a query, and before the ownership
	// lookup, the way every other pipeline route does it.
	if !strings.Contains(body, `requireUUIDParam(c, "id"`) {
		t.Error("GetPipelineAlerts does not validate the pipeline id as a UUID first")
	}
	// Order matters: authorize, then read.
	authAt := strings.Index(body, "requirePipelineWorkspaceRole")
	queryAt := strings.Index(body, "database.Query(")
	if authAt < 0 || queryAt < 0 || authAt > queryAt {
		t.Errorf("the workspace check must precede the query (auth at %d, query at %d)", authAt, queryAt)
	}
}

// The two gates this route exists to NOT have. A later edit that "made it consistent
// with the sibling monitoring routes" would silently re-hide the panel for everybody.
func TestPipelineAlerts_HasNoFeatureFlagOrPlatformRoleGate(t *testing.T) {
	body := alertsHandlerSource(t)

	for _, forbidden := range []struct{ frag, why string }{
		{"MonitoringInfra", "FEATURE_MONITORING_INFRA defaults to false, so this would 404 and the panel would hide itself"},
		{"GetFeatures", "no feature flag may gate a pipeline's own health"},
		{"RolePowerUser", "a platform role is workspace-blind; the pipeline's owner may be an ordinary member"},
		{"AdminRoleMiddleware", "admin-gating this would hide a pipeline's health from the person who owns it"},
	} {
		if strings.Contains(body, forbidden.frag) {
			t.Errorf("GetPipelineAlerts references %q — %s", forbidden.frag, forbidden.why)
		}
	}
}

// Only pipeline-scoped rows may be served here. sentinel_active_issues also holds
// 'agent', 'mcp_connector', 'kafka_consumer' and 'infrastructure' rows, whose
// component ids name topics and containers belonging to every workspace; those stay
// on the admin route. Constraining component_type as well as component_id means a
// future component whose id collided with a pipeline uuid still could not leak.
func TestPipelineAlerts_ServesOnlyPipelineScopedRows(t *testing.T) {
	body := alertsHandlerSource(t)

	if !strings.Contains(body, "component_id = $1") {
		t.Error("the query is not keyed on component_id = the pipeline in the URL")
	}
	if !strings.Contains(body, "component_type IN ('cdc_pipeline', 'batch_pipeline')") {
		t.Error("the query does not constrain component_type; deployment-wide " +
			"infrastructure rows could reach a non-admin caller")
	}
}

// A scan or query error must not answer 200 with a short list. The panel renders an
// empty list as a green all-clear, so a swallowed error would actively tell the
// operator to stop looking — the F-280 failure mode, one layer down.
func TestPipelineAlerts_ReadFailuresAreNotAnEmptyList(t *testing.T) {
	body := alertsHandlerSource(t)

	scanAt := strings.Index(body, "rows.Scan(")
	if scanAt < 0 {
		t.Fatal("no rows.Scan in GetPipelineAlerts")
	}
	// The scan-error arm must return, not `continue`. GetSentinelHealth's own
	// comment makes the same point about its rows.
	afterScan := body[scanAt:]
	arm := afterScan[:min(len(afterScan), 600)]
	if strings.Contains(arm, "continue") {
		t.Error("a row that fails to scan is skipped; a partial list reads as 'fewer problems than there are'")
	}
	if !strings.Contains(arm, "pipeline_alerts_scan_failed") {
		t.Error("a scan error does not surface as an error response")
	}
	if !strings.Contains(body, "rows.Err()") {
		t.Error("rows.Err() is not checked; a truncated result set would read as an all-clear")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
