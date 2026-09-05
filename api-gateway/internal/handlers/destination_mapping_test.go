package handlers

import (
	"errors"
	"strings"
	"testing"
)

func TestExtractDestinationConfigFromConfigJSON(t *testing.T) {
	// Structured destination_config wins.
	dc := extractDestinationConfigFromConfigJSON([]byte(`{"destination_config":{"namespace":"analytics","namespace_kind":"schema","create_if_not_exists":true}}`))
	if dc == nil || dc.Namespace != "analytics" || dc.NamespaceKind != "schema" || !dc.CreateIfNotExists {
		t.Fatalf("structured decode failed: %+v", dc)
	}
	// Legacy destination_namespace string fallback.
	dc = extractDestinationConfigFromConfigJSON([]byte(`{"destination_namespace":"shopify"}`))
	if dc == nil || dc.Namespace != "shopify" || dc.CreateIfNotExists {
		t.Fatalf("legacy fallback failed: %+v", dc)
	}
	// Neither present → nil.
	if got := extractDestinationConfigFromConfigJSON([]byte(`{"selected_tables":["a"]}`)); got != nil {
		t.Fatalf("expected nil for no namespace, got %+v", got)
	}
	// Empty namespace ignored.
	if got := extractDestinationConfigFromConfigJSON([]byte(`{"destination_config":{"namespace":"  "}}`)); got != nil {
		t.Fatalf("expected nil for blank namespace, got %+v", got)
	}
}

func TestDestinationNamespaceFindings(t *testing.T) {
	cases := []struct {
		name              string
		createIfNotExists bool
		exists            bool
		canCreate         bool
		probeErr          error
		wantCode          string
		wantSeverity      AssessmentSeverity
	}{
		{"probe failed → info", false, false, false, errors.New("ping: timeout"), FindingDestNamespaceUnverified, AssessmentInfo},
		{"exists → info", false, true, false, nil, FindingDestNamespaceExists, AssessmentInfo},
		// A missing namespace is never a hard blocker — the destination connector
		// auto-creates it at write time. With confirmed CREATE privilege we say so
		// outright; without it we still only warn (the probe under-reports on
		// managed DBs), and a genuine permission failure surfaces at write time.
		{"missing+priv → warn (will create)", true, false, true, nil, FindingDestNamespaceWillCreate, AssessmentWarning},
		{"missing+no-priv → warn (will attempt)", true, false, false, nil, FindingDestNamespaceNoPrivilege, AssessmentWarning},
		{"missing, create flag ignored → warn", false, false, false, nil, FindingDestNamespaceNoPrivilege, AssessmentWarning},
	}
	for _, c := range cases {
		got := destinationNamespaceFindings("analytics", "schema", c.createIfNotExists, c.exists, c.canCreate, c.probeErr)
		if len(got) != 1 {
			t.Fatalf("%s: expected 1 finding, got %d", c.name, len(got))
		}
		if got[0].Code != c.wantCode || got[0].Severity != c.wantSeverity {
			t.Errorf("%s: got code=%s sev=%s, want code=%s sev=%s", c.name, got[0].Code, got[0].Severity, c.wantCode, c.wantSeverity)
		}
	}
}

// P3-b: when the chosen namespace already exists, the "exists" finding must disclose
// the collision-safe rename (rsync_<ns>) rather than flatly promising the chosen
// namespace — otherwise the label lies about where data actually lands.
func TestDestinationNamespaceExistsDisclosesCollisionRename(t *testing.T) {
	got := destinationNamespaceFindings("public", "schema", false, true /*exists*/, false, nil)
	if len(got) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(got))
	}
	if msg := got[0].Message; !strings.Contains(msg, "rsync_public") {
		t.Errorf("exists-case message must disclose the collision-safe rename; got %q", msg)
	}
	if pfx, _ := got[0].Details["collision_safe_prefix"].(string); pfx != "rsync_public" {
		t.Errorf("expected details.collision_safe_prefix=rsync_public, got %q", pfx)
	}
}

func TestContainsPrivilege(t *testing.T) {
	yes := []string{
		"GRANT CREATE ON *.* TO 'u'@'%'",
		"GRANT SELECT, CREATE, INSERT ON *.* TO 'u'@'%'",
	}
	for _, g := range yes {
		if !containsPrivilege(g, "CREATE") {
			t.Errorf("containsPrivilege(%q, CREATE) = false, want true", g)
		}
	}
	no := []string{
		"GRANT CREATE TEMPORARY TABLES ON *.* TO 'u'@'%'", // not a bare CREATE
		"GRANT CREATE VIEW ON *.* TO 'u'@'%'",
		"GRANT SELECT ON *.* TO 'u'@'%'",
	}
	for _, g := range no {
		if containsPrivilege(g, "CREATE") {
			t.Errorf("containsPrivilege(%q, CREATE) = true, want false", g)
		}
	}
}
