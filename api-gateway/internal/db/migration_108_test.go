package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Static guards for migration 108 (layout 0 = undecided for new pipelines).
//
// The orchestrator only moves a pipeline to layout v2 from 0 (never wrote) or on a
// reload, so these pin that the migration changes the default and the CHECK and
// nothing else: an UPDATE here would move live pipelines to a new folder shape.

func readMigration108(t *testing.T) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join("..", "..", "migrations", "108_*.sql"))
	if err != nil {
		t.Fatalf("glob 108: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected exactly one 108_*.sql migration, found %v", matches)
	}
	b, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read %s: %v", matches[0], err)
	}
	return normSQL(stripSQLComments(string(b)))
}

func TestMigration108_NewPipelinesStartUndecided(t *testing.T) {
	body := readMigration108(t)
	if !strings.Contains(body, "alter table pipelines alter column storage_layout_version set default 0") {
		t.Fatalf("migration 108 must set pipelines.storage_layout_version DEFAULT 0")
	}
	if !strings.Contains(body, "check (storage_layout_version in (0, 1, 2))") {
		t.Fatalf("migration 108 must allow exactly 0, 1 and 2")
	}
	drop := strings.Index(body, "drop constraint if exists pipelines_storage_layout_version_check")
	add := strings.Index(body, "add constraint pipelines_storage_layout_version_check")
	if drop < 0 || add < 0 || drop > add {
		t.Fatalf("migration 108 must drop the old CHECK (IF EXISTS) before re-adding it")
	}
}

func TestMigration108_LeavesExistingRowsAlone(t *testing.T) {
	body := readMigration108(t)
	for _, forbidden := range []string{"update ", "delete from", "truncate", "set default 2", "set default 1", "begin;"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("migration 108 must only change the default and the CHECK; found %q", forbidden)
		}
	}
}
