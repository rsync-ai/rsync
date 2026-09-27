package handlers

import (
	"strings"
	"testing"
)

// The builder's 9 producer operations, as frontend/src/app/(dashboard)/transforms/page.tsx
// offers them. Every one of these has to survive validateExecutableTransform, or the
// operator gets a 400 for a card the UI told them to click.
func TestValidateExecutableTransform_AcceptsEveryOfferedOperation(t *testing.T) {
	cases := []struct {
		name string
		cfg  map[string]interface{}
	}{
		{"filter", map[string]interface{}{"operation": "filter", "condition": "id > 1"}},
		{"select", map[string]interface{}{"operation": "select", "columns": []interface{}{"id", "email"}}},
		{"exclude", map[string]interface{}{"operation": "exclude", "columns": []interface{}{"ssn"}}},
		{"rename", map[string]interface{}{"operation": "rename", "mappings": map[string]interface{}{"a": "b"}}},
		{"mask", map[string]interface{}{"operation": "mask", "column": "email", "mask_type": "hash"}},
		// hash is the builder's own card; it only reaches the engine because the
		// frontend now maps it to mask_pii with mask_type: hash.
		{"hash-as-mask_pii", map[string]interface{}{"operation": "mask_pii", "column": "email", "mask_type": "hash"}},
		{"type_convert", map[string]interface{}{"operation": "type_convert", "column": "age", "to": "int"}},
		{"null_handle", map[string]interface{}{"operation": "null_handle", "column": "city", "strategy": "default", "default_value": "NA"}},
		{"truncate", map[string]interface{}{"operation": "truncate", "column": "bio", "max_length": 100}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateExecutableTransform(tc.cfg, 0); err != nil {
				t.Fatalf("offered operation %q was rejected: %v", tc.name, err)
			}
		})
	}
}

// The 8 consumer operations the builder used to offer. None has a case in the
// engine's switch, so each one saved cleanly and then failed at run time — on a CDC
// pipeline by fail-closing the whole batch to the DLQ. The save path must refuse them.
func TestValidateExecutableTransform_RejectsOperationsNoEngineCanRun(t *testing.T) {
	unrunnable := []map[string]interface{}{
		{"operation": "aggregate", "group_by": "region", "aggregations": "sum(amount)"},
		{"operation": "join", "lookup_table": "dim", "join_key": "id", "lookup_key": "id"},
		{"operation": "enrich", "api_endpoint": "https://example.test/x"},
		{"operation": "deduplicate", "columns": "id"},
		{"operation": "sort", "columns": "id"},
		{"operation": "limit", "limit": 10},
		{"operation": "sql", "query": "SELECT 1"},
		{"operation": "python_udf", "name": "f", "code": "pass"},
	}

	for _, cfg := range unrunnable {
		op := cfg["operation"].(string)
		t.Run(op, func(t *testing.T) {
			if err := validateExecutableTransform(cfg, 0); err == nil {
				t.Fatalf("operation %q was accepted, but no engine can execute it", op)
			}
		})
	}
}

// A rule saved with enabled=false is inert today, but PUT /transforms/:id flips
// `enabled` without re-reading the config, so accepting a broken rule while disabled
// only defers the DLQ. The validator therefore judges every rule as if it were on.
func TestValidateExecutableTransform_IgnoresTheCallersEnabledFlag(t *testing.T) {
	cfg := map[string]interface{}{"operation": "aggregate", "enabled": false}
	if err := validateExecutableTransform(cfg, 0); err == nil {
		t.Fatal("a disabled unrunnable rule was accepted; enabling it later would reach the sink unchecked")
	}
}

// normalizeOne deletes `operation`, `table` and `requires_full_dataset` from the map it
// is handed, and SavePipelineTransforms marshals that same map straight into
// transform_config. Validating in place would strip `operation` from every stored row,
// so the config the caller owns must come back untouched.
func TestValidateExecutableTransform_DoesNotMutateTheCallersConfig(t *testing.T) {
	cfg := map[string]interface{}{
		"operation":             "filter",
		"condition":             "id > 1",
		"table":                 "users",
		"requires_full_dataset": false,
	}

	if err := validateExecutableTransform(cfg, 0); err != nil {
		t.Fatalf("unexpected rejection: %v", err)
	}

	for _, key := range []string{"operation", "condition", "table", "requires_full_dataset"} {
		if _, ok := cfg[key]; !ok {
			t.Errorf("validation removed %q from the caller's config; it would be lost on save", key)
		}
	}
}

// The rejection message is what the operator sees in place of a silent later failure,
// so it has to name the operation rather than an index alone.
func TestValidateExecutableTransform_ErrorNamesTheOperation(t *testing.T) {
	err := validateExecutableTransform(map[string]interface{}{"operation": "python_udf"}, 3)
	if err == nil {
		t.Fatal("expected a rejection")
	}
	if !strings.Contains(err.Error(), "python_udf") {
		t.Errorf("error does not name the offending operation: %v", err)
	}
}
