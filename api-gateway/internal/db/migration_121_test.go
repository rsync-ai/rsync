package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Static guards for migration 121 (drop idx_batch_acks_kafka_lookup). The index is
// the unique_batch_ack_kafka constraint minus table_name; the sink's dedup query and
// its ON CONFLICT target both name the constraint, so the constraint must survive and
// only the plain index may go.

func readMigration121(t *testing.T) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join("..", "..", "migrations", "121_*.sql"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("expected exactly one 121_*.sql migration, found %v (err %v)", matches, err)
	}
	b, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read %s: %v", matches[0], err)
	}
	return normSQL(stripSQLComments(string(b)))
}

func TestMigration121_DropsOnlyTheRedundantIndex(t *testing.T) {
	body := readMigration121(t)
	if !strings.Contains(body, "drop index if exists idx_batch_acks_kafka_lookup") {
		t.Errorf("migration 121 no longer drops idx_batch_acks_kafka_lookup")
	}
	if n := strings.Count(body, "drop "); n != 1 {
		t.Errorf("migration 121 has %d DROP statements, want exactly 1", n)
	}
	// The two that enforce something: removing either turns the sink's idempotent
	// insert into an error.
	for _, keep := range []string{"unique_batch_ack_kafka", "pipeline_batch_acks_pkey"} {
		if strings.Contains(body, keep) {
			t.Errorf("migration 121 touches %s, which enforces the sink's dedup", keep)
		}
	}
}

func TestMigration121_RunsInsideTheRunnersTransaction(t *testing.T) {
	body := readMigration121(t)
	// A literal BEGIN; flips the runner into self-managed mode (migrate.go), where
	// SET LOCAL has no transaction to be local to and the lock_timeout is lost.
	if strings.Contains(body, "begin;") {
		t.Errorf("migration 121 manages its own transaction; the runner must own it")
	}
	if !strings.Contains(body, "set local lock_timeout") {
		t.Errorf("migration 121 lost its lock_timeout; a blocked DROP would queue every sink write")
	}
	if strings.Contains(body, "concurrently") {
		t.Errorf("DROP INDEX CONCURRENTLY cannot run inside the runner's transaction")
	}
}
