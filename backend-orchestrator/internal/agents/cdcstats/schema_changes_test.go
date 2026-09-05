package cdcstats

// CDC-D1: a CDC stream whose source table was dropped renders every dependency
// "Healthy" — correctly, because every probe asks "is the process up?" and every
// process is. The signal that the source changed was already in Kafka the whole time:
// Debezium publishes source DDL to the bare topic.prefix topic and nothing consumed it.
//
// The three ALTER/DROP fixtures below are the ACTUAL records read off prod topic
// `cdc-abd8a64d` (the pipeline from the drift matrix), trimmed to the fields this
// parser reads. They are the evidence that the signal exists and the proof that the
// classifier handles the real envelope rather than an idealized one.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// Real record: the type change from Case B′ — the one that left the destination column
// `numeric` forever while the source became VARCHAR(50).
const prodTypeChangeRecord = `{"payload":{
  "source":{"version":"3.1.3.Final","connector":"mysql","name":"cdc-abd8a64d","snapshot":"false","db":"pipeline_test","table":"cdc_drift"},
  "ts_ms":1785488322372,"databaseName":"pipeline_test","schemaName":null,
  "ddl":"ALTER TABLE pipeline_test.cdc_drift MODIFY COLUMN amount VARCHAR(50)",
  "tableChanges":[{"type":"ALTER","id":"\"pipeline_test\".\"cdc_drift\""}]}}`

// Real record: the column drop from Case C′ — destination kept the column, new rows
// arrived with NULL, and nothing anywhere said so.
const prodDropColumnRecord = `{"payload":{
  "source":{"version":"3.1.3.Final","connector":"mysql","name":"cdc-abd8a64d","snapshot":"false","db":"pipeline_test","table":"cdc_drift"},
  "ts_ms":1785489204534,"databaseName":"pipeline_test","schemaName":null,
  "ddl":"ALTER TABLE pipeline_test.cdc_drift DROP COLUMN note",
  "tableChanges":[{"type":"ALTER","id":"\"pipeline_test\".\"cdc_drift\""}]}}`

// Real record: the table drop from Case D′ — the finding itself. Note table:null, which
// is why the classifier keys on tableChanges[].type and not on the table body.
const prodDropTableRecord = `{"payload":{
  "source":{"version":"3.1.3.Final","connector":"mysql","name":"cdc-abd8a64d","snapshot":"false","db":"pipeline_test","table":"cdc_drift"},
  "ts_ms":1785489653074,"databaseName":"pipeline_test","schemaName":null,
  "ddl":"DROP TABLE ` + "`pipeline_test`.`cdc_drift`" + `",
  "tableChanges":[{"type":"DROP","id":"\"pipeline_test\".\"cdc_drift\"","table":null}]}}`

func parseRecord(t *testing.T, raw string) *debeziumSchemaChange {
	t.Helper()
	var m debeziumSchemaChange
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("unmarshal prod record: %v", err)
	}
	return &m
}

func TestClassifySchemaChange_ProdDropTable(t *testing.T) {
	got := classifySchemaChange(parseRecord(t, prodDropTableRecord), "rsync_pipeline_test_a1b2c3d4")
	if len(got) != 1 {
		t.Fatalf("expected 1 change, got %d: %+v", len(got), got)
	}
	if got[0].changeType != "drop_table" {
		t.Errorf("change_type = %q, want drop_table", got[0].changeType)
	}
	// The change's identity stays the SOURCE's name — that is where the drop happened
	// and what the UI labels the row with.
	if got[0].table != "pipeline_test.cdc_drift" {
		t.Errorf("table = %q, want pipeline_test.cdc_drift", got[0].table)
	}
	// The DDL is the opposite: normalized (NOT the source's backticked text, because
	// this string is the schema_change_approvals UNIQUE (pipeline_id, ddl) key) AND
	// qualified with the DESTINATION namespace, because it is an instruction the user
	// runs against the destination. `pipeline_test.cdc_drift` there is a different table.
	if got[0].ddl != "DROP TABLE rsync_pipeline_test_a1b2c3d4.cdc_drift" {
		t.Errorf("ddl = %q, want the DESTINATION-qualified DROP TABLE", got[0].ddl)
	}
}

func TestClassifySchemaChange_ProdDropColumn(t *testing.T) {
	got := classifySchemaChange(parseRecord(t, prodDropColumnRecord), "rsync_pipeline_test_a1b2c3d4")
	if len(got) != 1 {
		t.Fatalf("expected 1 change, got %d: %+v", len(got), got)
	}
	if got[0].changeType != "drop_column" || got[0].column != "note" {
		t.Errorf("got %+v, want drop_column on column note", got[0])
	}
	if got[0].ddl != "ALTER TABLE rsync_pipeline_test_a1b2c3d4.cdc_drift DROP COLUMN note" {
		t.Errorf("ddl = %q, want the DESTINATION-qualified ALTER … DROP COLUMN note", got[0].ddl)
	}
}

// Legacy pipeline: nothing persisted at config->>'destination_namespace'. The connector
// writes into its own default schema, so the bare name is what resolves in the user's
// destination session — and the source qualifier must never be the fallback.
func TestClassifySchemaChange_NoNamespaceFallsBackToBareTable(t *testing.T) {
	for _, ns := range []string{"", "  ", "default"} {
		got := classifySchemaChange(parseRecord(t, prodDropColumnRecord), ns)
		if len(got) != 1 {
			t.Fatalf("ns=%q: expected 1 change, got %d", ns, len(got))
		}
		if got[0].ddl != "ALTER TABLE cdc_drift DROP COLUMN note" {
			t.Errorf("ns=%q: ddl = %q, want the bare-table form", ns, got[0].ddl)
		}
		if got[0].table != "pipeline_test.cdc_drift" {
			t.Errorf("ns=%q: table identity must stay source-qualified, got %q", ns, got[0].table)
		}
	}
}

// The exclusion that keeps the Approve button honest. The only DDL available here is
// MySQL's; the healer applies an approved non-destructive DDL verbatim to the
// DESTINATION, which is Postgres on this pipeline. Filing this would offer the user a
// button whose only outcome is a syntax error and an alarm.
func TestClassifySchemaChange_ProdTypeChangeIsNotReported(t *testing.T) {
	if got := classifySchemaChange(parseRecord(t, prodTypeChangeRecord), ""); len(got) != 0 {
		t.Fatalf("type change must not be reported from the DDL topic, got %+v", got)
	}
}

// The sink reports its own additions with applied=true. Reporting them here too would
// file one addition twice, under two DDL strings the UNIQUE key cannot collapse.
func TestClassifySchemaChange_AddColumnIsNotReported(t *testing.T) {
	raw := `{"payload":{"source":{"snapshot":"false","db":"pipeline_test"},
	  "databaseName":"pipeline_test","ddl":"ALTER TABLE pipeline_test.cdc_drift ADD COLUMN note VARCHAR(50)",
	  "tableChanges":[{"type":"ALTER","id":"\"pipeline_test\".\"cdc_drift\""}]}}`
	if got := classifySchemaChange(parseRecord(t, raw), ""); len(got) != 0 {
		t.Fatalf("add_column must be left to the sink, got %+v", got)
	}
}

// Debezium replays a CREATE (and on some connectors an ALTER) for every captured table
// while snapshotting. Without the snapshot guard, every connector start would file its
// entire catalog as drift.
func TestClassifySchemaChange_SnapshotDDLIsIgnored(t *testing.T) {
	for _, snap := range []string{"true", "first", "last", "incremental"} {
		raw := `{"payload":{"source":{"snapshot":"` + snap + `","db":"pipeline_test"},
		  "databaseName":"pipeline_test","ddl":"DROP TABLE pipeline_test.cdc_drift",
		  "tableChanges":[{"type":"DROP","id":"\"pipeline_test\".\"cdc_drift\""}]}}`
		if got := classifySchemaChange(parseRecord(t, raw), ""); len(got) != 0 {
			t.Errorf("snapshot=%q must be ignored, got %+v", snap, got)
		}
	}
}

// Not every DROP removes user data from the destination. Filing these would invite the
// user to run destructive DDL by hand for a constraint that was never mirrored.
func TestClassifySchemaChange_NonColumnDropsIgnored(t *testing.T) {
	for _, ddl := range []string{
		"ALTER TABLE pipeline_test.cdc_drift DROP FOREIGN KEY fk_owner",
		"ALTER TABLE pipeline_test.cdc_drift DROP INDEX idx_name",
		"ALTER TABLE pipeline_test.cdc_drift DROP PRIMARY KEY",
		"ALTER TABLE pipeline_test.cdc_drift DROP CONSTRAINT chk_amount",
		// MySQL's column shorthand: deliberately skipped rather than guessed at.
		"ALTER TABLE pipeline_test.cdc_drift DROP note",
	} {
		raw := `{"payload":{"source":{"snapshot":"false","db":"pipeline_test"},
		  "databaseName":"pipeline_test","ddl":` + mustJSONString(ddl) + `,
		  "tableChanges":[{"type":"ALTER","id":"\"pipeline_test\".\"cdc_drift\""}]}}`
		if got := classifySchemaChange(parseRecord(t, raw), ""); len(got) != 0 {
			t.Errorf("%q must not be reported, got %+v", ddl, got)
		}
	}
}

func TestClassifySchemaChange_MultipleDropColumnsInOneStatement(t *testing.T) {
	raw := `{"payload":{"source":{"snapshot":"false","db":"pipeline_test"},
	  "databaseName":"pipeline_test","ddl":"ALTER TABLE pipeline_test.cdc_drift DROP COLUMN note, DROP COLUMN amount",
	  "tableChanges":[{"type":"ALTER","id":"\"pipeline_test\".\"cdc_drift\""}]}}`
	got := classifySchemaChange(parseRecord(t, raw), "")
	if len(got) != 2 {
		t.Fatalf("expected 2 drop_column changes, got %d: %+v", len(got), got)
	}
	// Distinct DDL per column, so each files its own approval row rather than one
	// combined statement the user can only accept or reject wholesale.
	if got[0].column != "note" || got[1].column != "amount" {
		t.Errorf("columns = %q,%q; want note,amount", got[0].column, got[1].column)
	}
}

func TestUnquoteIdentifier(t *testing.T) {
	cases := map[string]string{
		`"pipeline_test"."cdc_drift"`: "pipeline_test.cdc_drift",
		"`pipeline_test`.`cdc_drift`": "pipeline_test.cdc_drift",
		`[dbo].[orders]`:              "dbo.orders",
		// SQL Server reports db.schema.table; keep the last two so it reads like the
		// two-part identifiers every other connector produces.
		`"mydb"."dbo"."orders"`: "dbo.orders",
		`orders`:                "orders",
	}
	for in, want := range cases {
		if got := unquoteIdentifier(in); got != want {
			t.Errorf("unquoteIdentifier(%q) = %q, want %q", in, got, want)
		}
	}
}

// applied=false is what routes this down the healer's normal classify → file → notify
// path, the same one a batch pipeline's drop takes. Flipping it would record the change
// as history the destination already has — which for a drop is exactly backwards: the
// destination still has the column.
func TestBuildUnappliedChangeEvent(t *testing.T) {
	c := unappliedChange{changeType: "drop_table", table: "pipeline_test.cdc_drift", ddl: "DROP TABLE pipeline_test.cdc_drift"}
	evt := buildUnappliedChangeEvent("11111111-1111-1111-1111-111111111111", c, "pipeline_test", time.Unix(1785489653, 0))

	if evt.SchemaChange.Applied {
		t.Error("applied must be false: CDC has not made this change to the destination and never will")
	}
	if !evt.ActionNeeded {
		t.Error("action_needed must be true: this is a decision, not a record")
	}
	if evt.EventType != "SCHEMA_CHANGE_DETECTED" {
		t.Errorf("event_type = %q", evt.EventType)
	}
	if evt.Context["destination_unchanged"] != true {
		t.Error("context must state the destination is untouched")
	}
	// The healer deserializes this off the wire; round-trip it rather than trusting
	// the struct literal.
	b, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back map[string]interface{}
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	sc, _ := back["schema_change"].(map[string]interface{})
	if sc["ddl"] != "DROP TABLE pipeline_test.cdc_drift" || sc["change_type"] != "drop_table" {
		t.Errorf("wire form lost fields: %v", sc)
	}
}

func mustJSONString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// ---------------------------------------------------------------------------
// Dropped-selected-table tracking (KI-CDC-DROPPED-SOURCE-TABLE-REPORTS-HEALTHY)
//
// classifySchemaChange files drift for a human to approve. These tests cover the
// second, narrower reading of the same message: whether a table this pipeline was
// told to capture is present at the source right now. That fact is what the
// debezium_task probe reads to stop calling a stream with no source table "healthy",
// and it has to survive the two edges the reporting path never had to care about —
// a table that was never selected, and a table that came back.
// ---------------------------------------------------------------------------

const dropTrackPipelineID = "11111111-1111-1111-1111-111111111111"

// dropRecord builds a schema-change record for one table and one DDL type, with the
// snapshot flag under the caller's control. `snap` is the raw Debezium value:
// "false" for a live change, "true"/"first"/"last"/"incremental" during a snapshot.
func dropRecord(changeType, snap, tableID string) string {
	return `{"payload":{"source":{"snapshot":"` + snap + `","db":"pipeline_test","table":"cdc_drift"},
	  "databaseName":"pipeline_test","ddl":"-- irrelevant to the lifecycle read",
	  "tableChanges":[{"type":"` + changeType + `","id":` + mustJSONString(tableID) + `}]}}`
}

func dropTrackAgent(t *testing.T, selected []string) (*Agent, *pipelineWorker, sqlmock.Sqlmock) {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	w := &pipelineWorker{pipelineID: dropTrackPipelineID}
	w.setSelectedTables(selected)
	return &Agent{db: database}, w, mock
}

func TestTrackSelectedTableDrops(t *testing.T) {
	t.Run("DROP of a selected table opens a row", func(t *testing.T) {
		a, w, mock := dropTrackAgent(t, []string{"cdc_drift"})
		mock.ExpectExec(`INSERT INTO cdc_source_table_drops`).
			WithArgs(dropTrackPipelineID, "cdc_drift").
			WillReturnResult(sqlmock.NewResult(0, 1))

		a.trackSelectedTableDrops(context.Background(), w,
			parseRecord(t, dropRecord("DROP", "false", `"pipeline_test"."cdc_drift"`)))

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("a live DROP of a selected table must be recorded: %v", err)
		}
	})

	t.Run("DROP of a table nobody selected records nothing", func(t *testing.T) {
		// Shared source databases are normal. Degrading a stream because somebody
		// dropped an unrelated table is a false page, and degraded is a signal
		// operators act on.
		a, w, mock := dropTrackAgent(t, []string{"cdc_drift"})

		a.trackSelectedTableDrops(context.Background(), w,
			parseRecord(t, dropRecord("DROP", "false", `"pipeline_test"."some_other_table"`)))

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unexpected DB write for an unselected table: %v", err)
		}
	})

	t.Run("an unresolved selection records nothing", func(t *testing.T) {
		// config->'selected_tables' is empty until the table-selection HITL resolves.
		// With nothing to match against, a drop cannot be attributed to this pipeline.
		a, w, mock := dropTrackAgent(t, nil)

		a.trackSelectedTableDrops(context.Background(), w,
			parseRecord(t, dropRecord("DROP", "false", `"pipeline_test"."cdc_drift"`)))

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unexpected DB write with no selection: %v", err)
		}
	})

	t.Run("snapshot-phase DROP records nothing", func(t *testing.T) {
		// Same guard classifySchemaChange applies: snapshot DDL is Debezium describing
		// the catalog it is about to read, not the source changing.
		for _, snap := range []string{"true", "first", "last", "incremental"} {
			a, w, mock := dropTrackAgent(t, []string{"cdc_drift"})

			a.trackSelectedTableDrops(context.Background(), w,
				parseRecord(t, dropRecord("DROP", snap, `"pipeline_test"."cdc_drift"`)))

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("snapshot=%q must not record a drop: %v", snap, err)
			}
		}
	})

	t.Run("CREATE closes the row", func(t *testing.T) {
		a, w, mock := dropTrackAgent(t, []string{"cdc_drift"})
		mock.ExpectExec(`UPDATE cdc_source_table_drops`).
			WithArgs(dropTrackPipelineID, "cdc_drift").
			WillReturnResult(sqlmock.NewResult(0, 1))

		a.trackSelectedTableDrops(context.Background(), w,
			parseRecord(t, dropRecord("CREATE", "false", `"pipeline_test"."cdc_drift"`)))

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("a CREATE of a selected table must clear its open drop: %v", err)
		}
	})

	t.Run("snapshot-phase CREATE still closes the row", func(t *testing.T) {
		// The deliberate asymmetry with the guard above, and the whole recovery path.
		// The ordinary way to fix a dropped source table is to recreate it and restart
		// the connector so it re-snapshots — and the ONLY CREATE Debezium then emits
		// for that table arrives inside a snapshot. Honouring the guard here would pin
		// the stream degraded forever. A CREATE seen during a snapshot is positive
		// proof the table exists now, whatever phase the connector is in.
		for _, snap := range []string{"true", "first", "last", "incremental"} {
			a, w, mock := dropTrackAgent(t, []string{"cdc_drift"})
			mock.ExpectExec(`UPDATE cdc_source_table_drops`).
				WithArgs(dropTrackPipelineID, "cdc_drift").
				WillReturnResult(sqlmock.NewResult(0, 1))

			a.trackSelectedTableDrops(context.Background(), w,
				parseRecord(t, dropRecord("CREATE", snap, `"pipeline_test"."cdc_drift"`)))

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("snapshot=%q CREATE must still clear an open drop: %v", snap, err)
			}
		}
	})

	t.Run("a DB error is swallowed", func(t *testing.T) {
		// During a rolling deploy the orchestrator runs before migration 099 is
		// applied. This is a reporting side-channel: a failure must cost the signal,
		// never stall the consumer group that is also carrying the drift reports.
		a, w, mock := dropTrackAgent(t, []string{"cdc_drift"})
		mock.ExpectExec(`INSERT INTO cdc_source_table_drops`).
			WithArgs(dropTrackPipelineID, "cdc_drift").
			WillReturnError(errors.New(`pq: relation "cdc_source_table_drops" does not exist`))

		a.trackSelectedTableDrops(context.Background(), w,
			parseRecord(t, dropRecord("DROP", "false", `"pipeline_test"."cdc_drift"`)))

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet expectations: %v", err)
		}
	})
}

// TestMatchSelectedTable is the asymmetry guard. The row the DROP opens and the row
// the CREATE closes must be the SAME row, and the two records need not spell the
// table the same way — Debezium reports it qualified and quoted, while the pipeline's
// selection may hold anything the user picked in the UI. A match that is stricter in
// one direction than the other leaves a recreate keyed differently from its drop and
// pins the stream degraded forever.
//
// The returned name is always the SELECTION's spelling, never the reported one, which
// is what makes both directions land on one key.
func TestMatchSelectedTable(t *testing.T) {
	cases := []struct {
		name     string
		selected []string
		reported string
		want     string
		wantOK   bool
	}{
		{"qualified report against a bare selection", []string{"cdc_drift"}, `"pipeline_test"."cdc_drift"`, "cdc_drift", true},
		{"qualified report against a qualified selection", []string{"pipeline_test.cdc_drift"}, `"pipeline_test"."cdc_drift"`, "pipeline_test.cdc_drift", true},
		{"bare report against a qualified selection", []string{"pipeline_test.cdc_drift"}, "cdc_drift", "pipeline_test.cdc_drift", true},
		{"case-insensitive both ways", []string{`"CDC_Drift"`}, `"pipeline_test"."cdc_drift"`, `"CDC_Drift"`, true},
		{"a different schema's same-named table still matches", []string{"other.cdc_drift"}, `"pipeline_test"."cdc_drift"`, "other.cdc_drift", true},
		{"a genuinely different table does not match", []string{"cdc_drift"}, `"pipeline_test"."orders"`, "", false},
		{"empty selection matches nothing", nil, `"pipeline_test"."cdc_drift"`, "", false},
		{"empty report matches nothing", []string{"cdc_drift"}, "", "", false},
		{"blank entries in the selection are skipped", []string{"", "  ", "cdc_drift"}, "cdc_drift", "cdc_drift", true},
	}
	for _, tc := range cases {
		got, ok := matchSelectedTable(tc.selected, tc.reported)
		if ok != tc.wantOK || got != tc.want {
			t.Errorf("%s: matchSelectedTable(%v, %q) = (%q, %v), want (%q, %v)",
				tc.name, tc.selected, tc.reported, got, ok, tc.want, tc.wantOK)
		}
	}
}

// The prod DROP fixture at the top of this file, read the second way. Same bytes, same
// consumer, one reading files drift for approval and the other answers "is the table
// still there" — which is the fact the health probe never had.
func TestReadTableLifecycle_ProdDropRecord(t *testing.T) {
	life := readTableLifecycle(parseRecord(t, prodDropTableRecord))
	if life.snapshot {
		t.Error("the prod drop is a live change, not a snapshot replay")
	}
	if len(life.dropped) != 1 || life.dropped[0] != "pipeline_test.cdc_drift" {
		t.Errorf("dropped = %v, want [pipeline_test.cdc_drift]", life.dropped)
	}
	if len(life.created) != 0 {
		t.Errorf("created = %v, want empty", life.created)
	}
	// An ALTER is neither: a column drop or a type change leaves the table present,
	// so it must not touch the lifecycle fact in either direction.
	alter := readTableLifecycle(parseRecord(t, prodDropColumnRecord))
	if len(alter.dropped) != 0 || len(alter.created) != 0 {
		t.Errorf("an ALTER must not read as a table lifecycle event: %+v", alter)
	}
}

// setSelectedTables refuses an empty read for the same reason destNamespace does: the
// selection is absent until the table-selection HITL resolves, and a transient empty
// sync tick must not blank a list the DDL consumer is matching against — which would
// silently stop recording drops on a live pipeline.
func TestSetSelectedTablesIgnoresEmptyRefresh(t *testing.T) {
	w := &pipelineWorker{pipelineID: dropTrackPipelineID}
	w.setSelectedTables([]string{"cdc_drift"})
	w.setSelectedTables(nil)
	if got := w.selection(); len(got) != 1 || got[0] != "cdc_drift" {
		t.Errorf("selection = %v, want the earlier non-empty list to survive", got)
	}
	w.setSelectedTables([]string{"orders", "invoices"})
	if got := w.selection(); len(got) != 2 {
		t.Errorf("selection = %v, want the re-selection to be adopted", got)
	}
}

// parseSelectedTables reads the same JSON array every persistence site writes, and
// refuses to guess at anything else — an unparseable selection records no drops,
// which is a missed degrade rather than a false one.
func TestParseSelectedTables(t *testing.T) {
	if got := parseSelectedTables(`["public.orders","cdc_drift"]`); len(got) != 2 || got[0] != "public.orders" {
		t.Errorf("parseSelectedTables = %v, want the two names", got)
	}
	for _, raw := range []string{"", "   ", "[]", "null", "{}", `"orders"`, "not json"} {
		if got := parseSelectedTables(raw); len(got) != 0 {
			t.Errorf("parseSelectedTables(%q) = %v, want empty", raw, got)
		}
	}
}
