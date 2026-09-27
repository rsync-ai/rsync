package cdcstats

import (
	"testing"
	"time"
)

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

// A snapshot read is not an insert: a re-snapshot reads every row again, and
// counting it as an insert made Captured Inserts exceed the table (bug #7).
func TestAccumulator_SnapshotReadsAreNotInserts(t *testing.T) {
	acc := NewAccumulator("p1")
	ts := time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)
	acc.Observe(TableUpdate{QualifiedName: "public.users", Op: "r", Snapshot: "first", Timestamp: ts})
	acc.Observe(TableUpdate{QualifiedName: "public.users", Op: "r", Snapshot: "true", Timestamp: ts.Add(time.Second)})
	acc.Observe(TableUpdate{QualifiedName: "public.users", Op: "r", Snapshot: "last_in_data_collection", Timestamp: ts.Add(2 * time.Second)})
	acc.Observe(TableUpdate{QualifiedName: "public.users", Op: "c", Timestamp: ts.Add(3 * time.Second)})

	st := acc.FlushDirty()[0]
	if st.Inserts != 1 || st.Reads != 3 || st.TotalEvents != 4 {
		t.Fatalf("want 1 insert, 3 reads, 4 events: %+v", st)
	}
	ev := BuildCDCTableStatsEvent("p1", "", st)
	ops := ev["metadata"].(map[string]interface{})["ops"].(map[string]interface{})
	if ops["reads"] != int64(3) || ops["inserts"] != int64(1) {
		t.Fatalf("event ops = %v", ops)
	}

	obs := acc.DrainSnapshots()
	if len(obs) != 1 {
		t.Fatalf("want one observation, got %+v", obs)
	}
	o := obs[0]
	if o.Table != "public.users" || o.Rows != 3 || !o.FirstSeen.Equal(ts) || !o.LastSeen.Equal(ts.Add(2*time.Second)) || !o.TableDone || o.AllDone {
		t.Fatalf("observation = %+v", o)
	}
	if again := acc.DrainSnapshots(); again != nil {
		t.Fatalf("drain must empty the window, got %+v", again)
	}

	acc.Observe(TableUpdate{QualifiedName: "public.users", Op: "r", Snapshot: "last", Timestamp: ts.Add(4 * time.Second)})
	if o := acc.DrainSnapshots()[0]; !o.AllDone || !o.TableDone || o.Incremental {
		t.Fatalf(`source.snapshot "last" ends the whole snapshot: %+v`, o)
	}

	// An incremental snapshot marks no table or snapshot end, so the load it is
	// part of has to be told apart: it can only finish by going idle.
	acc.Observe(TableUpdate{QualifiedName: "public.users", Op: "r", Snapshot: "incremental", Timestamp: ts.Add(5 * time.Second)})
	if o := acc.DrainSnapshots()[0]; !o.Incremental || o.TableDone {
		t.Fatalf(`source.snapshot "incremental" must flag the observation: %+v`, o)
	}
}

// A resumed worker continues from the counts the previous one reported (bug #10).
func TestAccumulator_SeedContinuesFromStoredCounts(t *testing.T) {
	acc := NewAccumulator("p1")
	acc.Seed([]TableStats{{QualifiedName: "public.users", Inserts: 10, Updates: 2, Reads: 100, TotalEvents: 112}})
	if got := acc.FlushDirty(); len(got) != 0 {
		t.Fatalf("seeded counts are not news: %+v", got)
	}
	acc.Observe(TableUpdate{QualifiedName: "public.users", Op: "u"})
	st := acc.FlushDirty()[0]
	if st.Inserts != 10 || st.Updates != 3 || st.Reads != 100 || st.TotalEvents != 113 {
		t.Fatalf("counts after seed = %+v", st)
	}
}
