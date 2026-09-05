package cdcstats

import "testing"

func TestParseDebeziumChange_UsesSourceFields(t *testing.T) {
	payload := map[string]interface{}{
		"op":    "u",
		"ts_ms": float64(1736400000000),
		"source": map[string]interface{}{
			"schema": "public",
			"table":  "users",
		},
	}

	u, ok := ParseDebeziumChange(payload, "cdc.tenant.db.pipeline.public.users")
	if !ok {
		t.Fatalf("expected ok")
	}
	if u.QualifiedName != "public.users" {
		t.Fatalf("expected qualified_name public.users, got %q", u.QualifiedName)
	}
	if u.Op != "u" {
		t.Fatalf("expected op u, got %q", u.Op)
	}
}

func TestParseDebeziumChange_FallbacksToTopicSuffix(t *testing.T) {
	payload := map[string]interface{}{
		"op": "c",
	}

	u, ok := ParseDebeziumChange(payload, "cdc.tenant.db.pipeline.public.orders")
	if !ok {
		t.Fatalf("expected ok")
	}
	if u.QualifiedName != "public.orders" {
		t.Fatalf("expected qualified_name public.orders, got %q", u.QualifiedName)
	}
}

