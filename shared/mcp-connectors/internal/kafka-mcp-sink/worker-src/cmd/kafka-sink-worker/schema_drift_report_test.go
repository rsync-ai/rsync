package main

// CDC-A1: batch and CDC answered "who approves a schema change?" oppositely, and
// only the batch answer was visible. The same ALTER TABLE … ADD COLUMN that stops a
// batch pipeline and waits for a human passed through a CDC pipeline with no drift
// row, no badge, no notification and no Schema-changes entry — because the sink
// applied it inside ensureDestinationTable and told nobody. These pin the reporting
// half: WHAT is reported (only genuinely new columns, in a stable order) and WHEN
// (never on the first ensure of a process, or every restart would file drift).

import (
	"context"
	"reflect"
	"testing"
)

func set(cols ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		m[c] = struct{}{}
	}
	return m
}

func TestAddedColumnNames(t *testing.T) {
	tests := []struct {
		name   string
		prev   map[string]struct{}
		merged map[string]struct{}
		want   []string
	}{
		{
			name:   "no change reports nothing",
			prev:   set("id", "name"),
			merged: set("id", "name"),
			want:   []string{},
		},
		{
			name:   "one new column",
			prev:   set("id", "name"),
			merged: set("id", "name", "email"),
			want:   []string{"email"},
		},
		{
			// Sorted, not map order: the DDL string becomes the
			// schema_change_approvals UNIQUE (pipeline_id, ddl) key, so an unstable
			// order would file the same change twice under two different DDLs.
			name:   "multiple new columns come back sorted",
			prev:   set("id"),
			merged: set("id", "zeta", "alpha", "mid"),
			want:   []string{"alpha", "mid", "zeta"},
		},
		{
			// __pk__ entries are ensure-bookkeeping, not columns. Reporting one would
			// emit "ALTER TABLE t ADD COLUMN __pk__id unknown" — DDL for a column that
			// does not exist.
			name:   "pk bookkeeping entries are not columns",
			prev:   set("id"),
			merged: set("id", "__pk__id"),
			want:   []string{},
		},
		{
			name:   "new column alongside new pk bookkeeping",
			prev:   set("id"),
			merged: set("id", "__pk__id", "email"),
			want:   []string{"email"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := addedColumnNames(tt.prev, tt.merged)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("addedColumnNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

// A nil writer is the "reporting disabled" state (batch path, or a worker built
// before the drift writer existed). It must be a no-op, not a nil-pointer panic on
// the CDC apply path — the destination write has already committed by then.
func TestReportAppliedSchemaDrift_NilWriterIsNoop(t *testing.T) {
	reportAppliedSchemaDrift(context.Background(), nil, &WorkerConfig{PipelineID: "p1"},
		"p1", "orders", "public", []string{"email"}, map[string]interface{}{"email": "TEXT"})
}

// An unattributable change cannot be filed: schema_change_approvals.pipeline_id is
// NOT NULL and FK-checked against pipelines(id). Dropping it is the only option, and
// it must not panic or block on a broker to do so. A non-nil writer here would
// attempt a real publish, so the empty-column guard is what keeps this offline.
func TestReportAppliedSchemaDrift_NoColumnsIsNoop(t *testing.T) {
	reportAppliedSchemaDrift(context.Background(), nil, &WorkerConfig{},
		"", "orders", "public", nil, nil)
}
