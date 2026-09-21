package executor

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/pkg/namespacefilter"
)

func scopeCfg(mode, patterns string) map[string]string {
	return map[string]string{namespacefilter.ModeKey: mode, namespacefilter.PatternsKey: patterns}
}

func TestConnectionScope(t *testing.T) {
	t.Run("no scope is inactive", func(t *testing.T) {
		_, active, err := connectionScope("postgresql", map[string]string{})
		if err != nil || active {
			t.Fatalf("active=%v err=%v, want inactive", active, err)
		}
	})
	t.Run("postgresql schemas are scoped", func(t *testing.T) {
		_, active, err := connectionScope("postgresql", scopeCfg("include", "sales"))
		if err != nil || !active {
			t.Fatalf("active=%v err=%v, want active", active, err)
		}
	})
	t.Run("server-level mysql is scoped", func(t *testing.T) {
		_, active, err := connectionScope("mysql", scopeCfg("exclude", "tmp_*"))
		if err != nil || !active {
			t.Fatalf("active=%v err=%v, want active", active, err)
		}
	})
	t.Run("a named database pins a database-namespace connection", func(t *testing.T) {
		cfg := scopeCfg("include", "other")
		cfg["database"] = "shop"
		_, active, err := connectionScope("mysql", cfg)
		if err != nil || active {
			t.Fatalf("active=%v err=%v, want the scope ignored", active, err)
		}
	})
	t.Run("an invalid scope fails closed", func(t *testing.T) {
		if _, _, err := connectionScope("postgresql", scopeCfg("only", "x")); err == nil {
			t.Fatal("want an error for an unknown mode, never 'all'")
		}
		if _, _, err := connectionScope("postgresql", scopeCfg("include", "")); err == nil {
			t.Fatal("want an error for include with no patterns")
		}
	})
}

func TestScopeErrorIsDiscoveryFailure(t *testing.T) {
	_, _, err := connectionScope("postgresql", scopeCfg("bogus", ""))
	got := scopeError("postgresql", err)
	var dfe *DiscoveryFailedError
	if !errors.As(got, &dfe) {
		t.Fatalf("scopeError = %T, want *DiscoveryFailedError", got)
	}
	if !strings.Contains(dfe.Message, namespacefilter.ErrInvalid.Error()) {
		t.Fatalf("message %q does not say the filter is invalid", dfe.Message)
	}
}

func TestScopeTablesAndTotals(t *testing.T) {
	f, err := namespacefilter.Parse(scopeCfg("exclude", "tmp_*, audit"))
	if err != nil {
		t.Fatal(err)
	}
	in := []TableMetadata{{Schema: "sales", Name: "orders"}, {Schema: "tmp_x", Name: "t"}, {Schema: "AUDIT", Name: "log"}, {Schema: "crm", Name: "users"}}
	got := scopeTables(in, f)
	var names []string
	for _, tb := range got {
		names = append(names, tb.Schema+"."+tb.Name)
	}
	if want := []string{"sales.orders", "crm.users"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("scopeTables = %v, want %v", names, want)
	}

	// A complete list stays complete; a truncated one stays truncated.
	if tot := applyScopeToTotals(discoveryTotals{Discovered: 4, Available: 4}, 2); tot.truncated() || tot.Discovered != 2 {
		t.Fatalf("complete list: %+v", tot)
	}
	if tot := applyScopeToTotals(discoveryTotals{Discovered: 4, Available: 10}, 2); !tot.truncated() || tot.Available != 8 {
		t.Fatalf("truncated list: %+v", tot)
	}
}

func envelopeTables(schemas ...string) map[string]interface{} {
	tables := make([]interface{}, 0, len(schemas))
	for _, s := range schemas {
		tables = append(tables, map[string]interface{}{"schema": s, "name": "t"})
	}
	return map[string]interface{}{
		"tables":                  tables,
		"total_tables_available":  float64(len(schemas)),
		"total_tables_discovered": float64(len(schemas)),
	}
}

func envelopeSchemas(result map[string]interface{}) []string {
	var out []string
	for _, item := range result["tables"].([]interface{}) {
		out = append(out, item.(map[string]interface{})["schema"].(string))
	}
	return out
}

func TestScopeEnvelopeFiltersPostgresSchemas(t *testing.T) {
	result := envelopeTables("public", "sales", "crm_eu", "hr")
	if err := scopeEnvelope("postgresql", scopeCfg("include", "sales, crm_*"), result); err != nil {
		t.Fatal(err)
	}
	if got, want := envelopeSchemas(result), []string{"sales", "crm_eu"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tables = %v, want %v", got, want)
	}
	if result["total_tables_available"] != float64(2) || result["total_tables_discovered"] != float64(2) {
		t.Fatalf("totals not adjusted: %v / %v", result["total_tables_available"], result["total_tables_discovered"])
	}
	if _, warned := result["warnings_objects"]; warned {
		t.Fatal("a scope that kept tables must not warn")
	}
}

func TestScopeEnvelopeNamedDatabaseUntouched(t *testing.T) {
	cfg := scopeCfg("include", "nothing_matches")
	cfg["database"] = "shop"
	result := envelopeTables("shop", "shop")
	if err := scopeEnvelope("mysql", cfg, result); err != nil {
		t.Fatal(err)
	}
	if got := envelopeSchemas(result); len(got) != 2 {
		t.Fatalf("a named-database connection lost tables to its scope: %v", got)
	}
}

func TestScopeEnvelopeMatchedNothingWarnsOnce(t *testing.T) {
	result := envelopeTables("public", "sales")
	// A connector that already warned (server-level connectors apply the scope
	// themselves) must not get a second copy.
	result["warnings_objects"] = []interface{}{map[string]interface{}{
		"category": scopeCategory, "severity": "warning", "message": namespacefilter.NoMatchWarning}}
	result["warnings_messages"] = []interface{}{namespacefilter.NoMatchWarning}
	if err := scopeEnvelope("postgresql", scopeCfg("include", "nope"), result); err != nil {
		t.Fatal(err)
	}
	if len(result["tables"].([]interface{})) != 0 {
		t.Fatal("want every table dropped")
	}
	if n := len(result["warnings_objects"].([]interface{})); n != 1 {
		t.Fatalf("warnings_objects = %d, want 1", n)
	}

	fresh := envelopeTables("public")
	if err := scopeEnvelope("postgresql", scopeCfg("include", "nope"), fresh); err != nil {
		t.Fatal(err)
	}
	objs, _ := fresh["warnings_objects"].([]interface{})
	msgs, _ := fresh["warnings_messages"].([]interface{})
	if len(objs) != 1 || len(msgs) != 1 || msgs[0] != namespacefilter.NoMatchWarning {
		t.Fatalf("want one scope_empty warning in both shapes, got %v / %v", objs, msgs)
	}
}

func TestScopeEnvelopeInvalidScopeFailsClosed(t *testing.T) {
	result := envelopeTables("public")
	err := scopeEnvelope("postgresql", scopeCfg("everything", ""), result)
	var dfe *DiscoveryFailedError
	if !errors.As(err, &dfe) {
		t.Fatalf("err = %v, want a DiscoveryFailedError", err)
	}
}
