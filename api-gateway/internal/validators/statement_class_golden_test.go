package validators

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestClassifyStatementSQLMatchesSharedGolden pins ClassifyStatementSQL to the
// fixture that frontend/src/lib/explorer/statementClass.ts is also pinned to
// (__tests__/statementClassGolden.test.ts). The UI copy gates the Run button,
// the role message and the destructive-confirm dialog before the request is
// sent, so when the two classifiers disagree the UI refuses statements the
// server would run, or offers ones it will refuse. A behaviour change here
// must update the fixture, which runs the TypeScript side too.
func TestClassifyStatementSQLMatchesSharedGolden(t *testing.T) {
	path := filepath.Join("..", "..", "..", "shared", "explorer_statement_class_golden.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	var golden []struct {
		SQL   string `json:"sql"`
		Class string `json:"class"`
	}
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(golden) == 0 {
		t.Fatal("golden fixture is empty")
	}
	for _, g := range golden {
		if got := ClassifyStatementSQL(g.SQL); string(got) != g.Class {
			t.Errorf("ClassifyStatementSQL(%q) = %q, want %q", g.SQL, got, g.Class)
		}
	}
}
