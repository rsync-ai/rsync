package executor

import (
	"errors"

	"github.com/rsync-ai/backend-orchestrator/pkg/namespacefilter"
)

// A connection's Scope (namespace_filter_mode / namespace_filter_patterns, #31)
// narrows which databases or schemas its discovery shows. The server-level
// MySQL/MongoDB/ClickHouse connectors apply it themselves, so they never
// enumerate a database the user left out; every other connector (PostgreSQL
// schemas, SQL Server, …) knows nothing about it. discovery applies it here,
// once, for all of them — on top of a connector that already did, it is a no-op.

// scopeCategory is the warnings_objects category of the "matched nothing"
// warning, the same one the connectors use.
const scopeCategory = "scope_empty"

// connectionScope returns the connection's Scope and whether it narrows
// anything. A connection whose tables live in DATABASES and that names one
// (namespace_model table_namespace "database" + a database in the config) is
// pinned to that database: its scope is ignored, exactly as the connector
// ignores it. A scope that does not parse is an error — never "all".
func connectionScope(connectorType string, config map[string]string) (namespacefilter.Filter, bool, error) {
	if tableNamespaceIsDatabase(connectorType) && configuredDatabase(config) != "" {
		return namespacefilter.Filter{}, false, nil
	}
	f, err := namespacefilter.Parse(config)
	if err != nil {
		return namespacefilter.Filter{}, false, err
	}
	return f, f.Active(), nil
}

// scopeError is the fail-closed discovery error for a Scope that does not
// parse. The filter's messages are fixed text (no names, no patterns).
func scopeError(connectorType string, err error) error {
	msg := err.Error()
	if !errors.Is(err, namespacefilter.ErrInvalid) {
		msg = namespacefilter.ErrInvalid.Error() + ": " + msg
	}
	return &DiscoveryFailedError{Connector: connectorType, Message: msg}
}

// scopeTables keeps the tables whose schema (database or schema name) the
// filter allows. System names are the connectors' business — they already
// leave them out — so none are passed here.
func scopeTables(tables []TableMetadata, f namespacefilter.Filter) []TableMetadata {
	out := make([]TableMetadata, 0, len(tables))
	for _, t := range tables {
		if f.Allowed(t.Schema, nil) {
			out = append(out, t)
		}
	}
	return out
}

// applyScopeToTotals keeps totals honest after dropping `removed` discovered
// tables: they leave both counts, so a list the connector truncated stays
// truncated and a complete one stays complete.
func applyScopeToTotals(t discoveryTotals, removed int) discoveryTotals {
	t.Discovered -= removed
	t.Available -= removed
	if t.Available < t.Discovered {
		t.Available = t.Discovered
	}
	return t
}

// scopeEnvelope applies the connection's Scope to a raw discover_schema result
// in place: out-of-scope tables leave "tables", the counts follow, and a scope
// that leaves nothing adds a warning instead of failing.
func scopeEnvelope(connectorType string, config map[string]string, result map[string]interface{}) error {
	f, active, err := connectionScope(connectorType, config)
	if err != nil {
		return scopeError(connectorType, err)
	}
	if !active || result == nil {
		return nil
	}
	raw, _ := result["tables"].([]interface{})
	kept := make([]interface{}, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		schema, _ := m["schema"].(string)
		if f.Allowed(schema, nil) {
			kept = append(kept, item)
		}
	}
	removed := len(raw) - len(kept)
	result["tables"] = kept
	if removed > 0 {
		if v, ok := result["total_tables_available"].(float64); ok {
			result["total_tables_available"] = maxFloat(v-float64(removed), float64(len(kept)))
		}
		if _, ok := result["total_tables_discovered"]; ok {
			result["total_tables_discovered"] = float64(len(kept))
		}
	}
	if len(kept) == 0 && len(raw) > 0 {
		addScopeWarning(result)
	}
	return nil
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// addScopeWarning adds the "matched nothing" warning once, in both warning
// shapes the connectors emit.
func addScopeWarning(result map[string]interface{}) {
	objs, _ := result["warnings_objects"].([]interface{})
	for _, o := range objs {
		if m, ok := o.(map[string]interface{}); ok && m["category"] == scopeCategory {
			return
		}
	}
	result["warnings_objects"] = append(objs, map[string]interface{}{
		"category": scopeCategory, "severity": "warning", "message": namespacefilter.NoMatchWarning,
	})
	msgs, _ := result["warnings_messages"].([]interface{})
	result["warnings_messages"] = append(msgs, namespacefilter.NoMatchWarning)
}
