package cdcstats

import "testing"

func TestAccumulator_CountsOpsAndFlushesDirty(t *testing.T) {
	acc := NewAccumulator("p1")

	acc.Observe(TableUpdate{QualifiedName: "db.users", SchemaName: "db", TableName: "users", Op: "c"})
	acc.Observe(TableUpdate{QualifiedName: "db.users", SchemaName: "db", TableName: "users", Op: "u"})
	acc.Observe(TableUpdate{QualifiedName: "db.users", SchemaName: "db", TableName: "users", Op: "d"})

	updates := acc.FlushDirty()
	if len(updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(updates))
	}
	st := updates[0]
	if st.Inserts != 1 || st.Updates != 1 || st.Deletes != 1 || st.TotalEvents != 3 {
		t.Fatalf("unexpected counts: %+v", st)
	}

	updates2 := acc.FlushDirty()
	if len(updates2) != 0 {
		t.Fatalf("expected 0 updates after flush, got %d", len(updates2))
	}
}

