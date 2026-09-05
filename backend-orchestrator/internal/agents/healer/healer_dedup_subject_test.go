package healer

import "testing"

// The notifier deduplicates on (pipeline_id, code, action_url, dedup_subject) inside a
// 60-minute window. The first three are CONSTANT for every schema drift on a pipeline,
// so this subject is the only thing keeping two different drifts from collapsing into
// one alert. Two properties matter and they pull in opposite directions: different
// drifts must differ, and retries of one drift must not.
func TestSchemaDriftDedupSubject_SeparatesDifferentDrifts(t *testing.T) {
	subjects := map[string]SchemaChange{
		"column dropped":       {ChangeType: "drop_column", Table: "public.orders", ColumnName: "legacy_note"},
		"other column dropped": {ChangeType: "drop_column", Table: "public.orders", ColumnName: "legacy_ref"},
		"other table":          {ChangeType: "drop_column", Table: "public.users", ColumnName: "legacy_note"},
		"table dropped":        {ChangeType: "drop_table", Table: "public.orders"},
		"column added":         {ChangeType: "add_column", Table: "public.orders", ColumnName: "total"},
	}

	seen := map[string]string{}
	for name, sc := range subjects {
		got := schemaDriftDedupSubject(sc)
		if got == "" {
			t.Errorf("%s: empty subject would fall back to whole-family dedup", name)
			continue
		}
		if prev, dup := seen[got]; dup {
			t.Errorf("%q and %q share subject %q — the second alert would be swallowed", name, prev, got)
		}
		seen[got] = name
	}
}

// Retries of the SAME drift must produce the same subject, including when the two
// producers describe it slightly differently. Casing and whitespace are normalized;
// the DDL is deliberately not part of the subject, because the batch detector and the
// CDC path render the same drop with different statements and keying on it would file
// the alert twice.
func TestSchemaDriftDedupSubject_CollapsesRetriesOfOneDrift(t *testing.T) {
	base := SchemaChange{
		ChangeType: "drop_column", Table: "public.orders", ColumnName: "legacy_note",
		DDL: "ALTER TABLE rsync_public_a1b2c3d4.orders DROP COLUMN legacy_note",
	}
	variants := []SchemaChange{
		base,
		{ChangeType: "DROP_COLUMN", Table: "Public.Orders", ColumnName: "Legacy_Note", DDL: base.DDL},
		{ChangeType: " drop_column ", Table: " public.orders ", ColumnName: " legacy_note ", DDL: ""},
		{ChangeType: "drop_column", Table: "public.orders", ColumnName: "legacy_note",
			DDL: "ALTER TABLE public.orders DROP COLUMN legacy_note"},
	}

	want := schemaDriftDedupSubject(base)
	for i, v := range variants {
		if got := schemaDriftDedupSubject(v); got != want {
			t.Errorf("variant %d: subject = %q, want %q (same drift must dedupe)", i, got, want)
		}
	}
}

// Nothing identifying at all → empty, which restores the old whole-family key. Making
// up a subject here would split every retry of an unidentifiable event into its own
// notification, which is the failure mode this change exists to avoid inverting into.
func TestSchemaDriftDedupSubject_EmptyWhenNothingIdentifying(t *testing.T) {
	if got := schemaDriftDedupSubject(SchemaChange{}); got != "" {
		t.Errorf("empty change should yield no subject, got %q", got)
	}
	if got := schemaDriftDedupSubject(SchemaChange{DDL: "DROP TABLE x", RiskLevel: "high"}); got != "" {
		t.Errorf("DDL/risk alone are not an identity, got %q", got)
	}
}
