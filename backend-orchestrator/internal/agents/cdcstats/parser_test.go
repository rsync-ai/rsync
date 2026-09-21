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


// Debezium runs with JsonConverter schemas.enable=true, so the change event sits
// under "payload". Before the unwrap every such message was dropped.
func TestParseDebeziumChange_UnwrapsSchemaEnvelope(t *testing.T) {
	envelope := map[string]interface{}{
		"schema": map[string]interface{}{"type": "struct"},
		"payload": map[string]interface{}{
			"op":    "c",
			"ts_ms": float64(1736400000000),
			"source": map[string]interface{}{
				"db": "shop",
			},
		},
	}

	u, ok := ParseDebeziumChange(envelope, "cdc-shop.shop.orders")
	if !ok {
		t.Fatalf("expected the schema envelope to parse")
	}
	if u.QualifiedName != "shop.orders" {
		t.Fatalf("expected qualified_name shop.orders, got %q", u.QualifiedName)
	}
	if u.Op != "c" {
		t.Fatalf("expected op c, got %q", u.Op)
	}
	if u.Timestamp.UnixMilli() != 1736400000000 {
		t.Fatalf("expected ts_ms from the inner payload, got %v", u.Timestamp)
	}
}

func TestParseDebeziumChange_EnvelopeWithoutOpIsRejected(t *testing.T) {
	envelope := map[string]interface{}{
		"schema":  map[string]interface{}{"type": "struct"},
		"payload": map[string]interface{}{"after": map[string]interface{}{"id": float64(1)}},
	}
	if _, ok := ParseDebeziumChange(envelope, "cdc-shop.shop.orders"); ok {
		t.Fatalf("expected a payload with no op to be rejected")
	}
}
