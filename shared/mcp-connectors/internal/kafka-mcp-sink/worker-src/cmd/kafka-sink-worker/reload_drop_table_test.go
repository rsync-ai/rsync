package main

import "testing"

func TestReloadCanDropTable(t *testing.T) {
	asked := 0
	ddl := func(v bool) func() bool { return func() bool { asked++; return v } }

	// Document store: no DDL, collections recreate on first write — always drop.
	for _, dest := range []string{"mongodb", "mongo", "mongodb_atlas"} {
		asked = 0
		if !reloadCanDropTable(dest, ddl(false)) {
			t.Errorf("%s: reload must drop the collection", dest)
		}
		if asked != 0 {
			t.Errorf("%s: DDL probe called for a document store", dest)
		}
	}
	// Relational: drop only when ensure_table can recreate it.
	if !reloadCanDropTable("postgresql", ddl(true)) {
		t.Error("postgresql with DDL: want drop")
	}
	if reloadCanDropTable("postgresql", ddl(false)) {
		t.Error("postgresql without DDL: dropping would lose the table")
	}
	if reloadCanDropTable("snowflake", nil) {
		t.Error("nil probe: want no drop")
	}
}
