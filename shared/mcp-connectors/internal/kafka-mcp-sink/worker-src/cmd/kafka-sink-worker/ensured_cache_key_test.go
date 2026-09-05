package main

import "testing"

// TestEnsuredCacheKeyCollision is the regression guard for the multi-schema
// same-name data-loss bug: two source schemas holding a same-named table both
// bare-ify to "orders", so before this fix they shared one ddl.Ensured entry and
// the second table's ensure_table (CREATE SCHEMA) was skipped. The key must
// include the namespace so they stay distinct.
func TestEnsuredCacheKeyCollision(t *testing.T) {
	sales := ensuredCacheKey("postgresql", "sales", "orders")
	proc := ensuredCacheKey("postgresql", "procurement", "orders")
	if sales == proc {
		t.Fatalf("cross-schema collision: sales and procurement both keyed %q", sales)
	}
	if sales != "postgresql:sales:orders" {
		t.Errorf("unexpected key %q, want postgresql:sales:orders", sales)
	}
}

func TestEnsuredCacheKeyNamespaceNormalization(t *testing.T) {
	// Empty and the "default" placeholder both mean "no real namespace" and MUST
	// produce the identical key, so historical single-namespace pipelines keep
	// exactly one ensure entry per table (no behavior change).
	empty := ensuredCacheKey("postgresql", "", "orders")
	def := ensuredCacheKey("postgresql", "default", "orders")
	blank := ensuredCacheKey("postgresql", "   ", "orders")
	if empty != def || empty != blank {
		t.Fatalf("namespace normalization mismatch: empty=%q default=%q blank=%q", empty, def, blank)
	}
	if empty != "postgresql::orders" {
		t.Errorf("unexpected key %q, want postgresql::orders", empty)
	}
}

func TestEnsuredCacheKeyCanonicalizesDestType(t *testing.T) {
	// The store site (ensureDestinationTable) and the reload invalidation site must
	// canonicalize the dest type identically, or a mismatch reopens the reload
	// data-loss bug. canonicalConnectorType lowercases and maps "_" -> "-".
	if ensuredCacheKey("Postgres_RDS", "sales", "orders") != ensuredCacheKey("postgres-rds", "sales", "orders") {
		t.Error("dest type must be canonicalized consistently")
	}
}
