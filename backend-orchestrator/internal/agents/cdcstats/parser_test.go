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

func TestParseDebeziumChange_ReadsSnapshotMarker(t *testing.T) {
	for _, c := range []struct {
		snap interface{}
		want string
	}{
		{"last_in_data_collection", "last_in_data_collection"},
		{"incremental", "incremental"},
		{"false", "false"},
		{true, "true"},
		{false, ""},
		{nil, ""},
	} {
		src := map[string]interface{}{"schema": "public", "table": "users"}
		if c.snap != nil {
			src["snapshot"] = c.snap
		}
		u, ok := ParseDebeziumChange(map[string]interface{}{"payload": map[string]interface{}{"op": "r", "source": src}}, "p.public.users")
		if !ok || u.Snapshot != c.want {
			t.Errorf("snapshot %v: got %q (ok=%v), want %q", c.snap, u.Snapshot, ok, c.want)
		}
	}
}

// MongoDB's source block names the collection "collection", not "table", so
// the name comes from the topic: db.collection, the signal's data-collection.
func TestParseDebeziumChange_MongoNamesMatchTheSignal(t *testing.T) {
	payload := map[string]interface{}{"op": "r", "source": map[string]interface{}{
		"connector": "mongodb", "db": "shop", "collection": "orders", "snapshot": "true",
	}}
	u, ok := ParseDebeziumChange(payload, "rsync_cdc_ab12cd34.shop.orders")
	if !ok || u.QualifiedName != "shop.orders" || u.Snapshot != "true" {
		t.Fatalf("got %+v ok=%v", u, ok)
	}
}
