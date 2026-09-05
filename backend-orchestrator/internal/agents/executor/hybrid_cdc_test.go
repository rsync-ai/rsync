package executor

import (
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/cdc"
)

func TestBuildDebeziumMySQLOffsetRecord(t *testing.T) {
	t.Run("minimal file+pos", func(t *testing.T) {
		key, val, err := buildDebeziumMySQLOffsetRecord("cdc-abc12345", "cdc-abc12345", cdc.BinlogPosition{File: "mysql-bin.000003", Pos: 7759})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Key must be exactly ["<connector>",{"server":"<prefix>"}] — Debezium matches this verbatim.
		wantKey := `["cdc-abc12345",{"server":"cdc-abc12345"}]`
		if string(key) != wantKey {
			t.Errorf("key = %s, want %s", key, wantKey)
		}
		wantVal := `{"file":"mysql-bin.000003","pos":7759}`
		if string(val) != wantVal {
			t.Errorf("value = %s, want %s", val, wantVal)
		}
	})

	t.Run("with gtid", func(t *testing.T) {
		_, val, err := buildDebeziumMySQLOffsetRecord("cdc-x", "cdc-x", cdc.BinlogPosition{File: "binlog.000001", Pos: 120, GTID: "uuid:1-100"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// pos is numeric (not quoted); gtids present.
		wantVal := `{"file":"binlog.000001","gtids":"uuid:1-100","pos":120}`
		if string(val) != wantVal {
			t.Errorf("value = %s, want %s", val, wantVal)
		}
	})

	t.Run("topic prefix defaults to connector name", func(t *testing.T) {
		key, _, err := buildDebeziumMySQLOffsetRecord("cdc-zz", "", cdc.BinlogPosition{File: "b.1", Pos: 1})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := `["cdc-zz",{"server":"cdc-zz"}]`
		if string(key) != want {
			t.Errorf("key = %s, want %s", key, want)
		}
	})

	t.Run("rejects zero position", func(t *testing.T) {
		if _, _, err := buildDebeziumMySQLOffsetRecord("cdc-x", "cdc-x", cdc.BinlogPosition{}); err == nil {
			t.Error("expected error for zero binlog position, got nil")
		}
	})

	t.Run("rejects empty connector", func(t *testing.T) {
		if _, _, err := buildDebeziumMySQLOffsetRecord("", "p", cdc.BinlogPosition{File: "b.1", Pos: 1}); err == nil {
			t.Error("expected error for empty connector name, got nil")
		}
	})
}

func TestHybridIsPostgresFamily(t *testing.T) {
	pg := []string{"postgresql", "postgres", "PostgreSQL", "aurora_postgresql", "alloydb", "neon", "supabase", "cockroachdb", "cockroach-db"}
	for _, s := range pg {
		if !hybridIsPostgresFamily(s) {
			t.Errorf("hybridIsPostgresFamily(%q) = false, want true", s)
		}
	}
	notPG := []string{"mysql", "mariadb", "mongodb", "oracle", "sqlserver", ""}
	for _, s := range notPG {
		if hybridIsPostgresFamily(s) {
			t.Errorf("hybridIsPostgresFamily(%q) = true, want false", s)
		}
	}
}

func TestHybridNormalizeDBType(t *testing.T) {
	cases := map[string]string{
		"postgres": "postgresql",
		"Postgres": "postgresql",
		"mariadb":  "mysql",
		"MySQL":    "mysql",
		"my-sql":   "my_sql",
	}
	for in, want := range cases {
		if got := hybridNormalizeDBType(in); got != want {
			t.Errorf("hybridNormalizeDBType(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestHybridDestAppendOnly(t *testing.T) {
	mk := func(mode string) ExecutorTask {
		return ExecutorTask{Destination: &ConnectorConfig{Config: map[string]string{"cdc_write_mode": mode}}}
	}
	for _, m := range []string{"append", "append_only", "append-only", "history", "insert", "APPEND"} {
		if !hybridDestAppendOnly(mk(m)) {
			t.Errorf("hybridDestAppendOnly(cdc_write_mode=%q) = false, want true", m)
		}
	}
	for _, m := range []string{"", "upsert", "merge"} {
		if hybridDestAppendOnly(mk(m)) {
			t.Errorf("hybridDestAppendOnly(cdc_write_mode=%q) = true, want false", m)
		}
	}
	// Nil destination/config must not panic and must be false.
	if hybridDestAppendOnly(ExecutorTask{}) {
		t.Error("hybridDestAppendOnly(empty task) = true, want false")
	}
}

func TestHybridTablesFromTask(t *testing.T) {
	t.Run("tables preferred over selected_tables", func(t *testing.T) {
		task := ExecutorTask{Params: map[string]interface{}{
			"tables":          []interface{}{"public.a", "public.b"},
			"selected_tables": []interface{}{"public.c"},
		}}
		got := hybridTablesFromTask(task)
		if len(got) != 2 || got[0] != "public.a" || got[1] != "public.b" {
			t.Errorf("got %v, want [public.a public.b]", got)
		}
	})
	t.Run("falls back to selected_tables", func(t *testing.T) {
		task := ExecutorTask{Params: map[string]interface{}{"selected_tables": []string{"db.t1"}}}
		got := hybridTablesFromTask(task)
		if len(got) != 1 || got[0] != "db.t1" {
			t.Errorf("got %v, want [db.t1]", got)
		}
	})
	t.Run("nil params", func(t *testing.T) {
		if got := hybridTablesFromTask(ExecutorTask{}); len(got) != 0 {
			t.Errorf("got %v, want empty", got)
		}
	})
}
