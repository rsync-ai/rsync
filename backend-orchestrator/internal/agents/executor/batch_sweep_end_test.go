package executor

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSweepEndPositionMarksTheLastFullPageComplete(t *testing.T) {
	now := time.Date(2026, 9, 27, 8, 0, 0, 0, time.UTC)
	last := map[string]interface{}{
		"batch_idx":      float64(4),
		"offset":         float64(0),
		"key_ordinal":    float64(40000),
		"cursor":         "10000",
		"pk_high_water":  "10000",
		"since_cursor":   nil,
		"table_complete": false,
		"execution_id":   "prev-run",
	}
	pos := sweepEndPosition(last, "this-run", now)
	if pos == nil {
		t.Fatal("an empty page after a full one must mark the sweep complete")
	}
	if pos["table_complete"] != true {
		t.Fatalf("table_complete = %v, want true", pos["table_complete"])
	}
	if pos["execution_id"] != "this-run" || pos["updated_at"] != "2026-09-27T08:00:00Z" {
		t.Fatalf("execution_id/updated_at = %v/%v", pos["execution_id"], pos["updated_at"])
	}
	for _, k := range []string{"batch_idx", "offset", "key_ordinal", "cursor", "pk_high_water"} {
		if pos[k] != last[k] {
			t.Errorf("%s = %v, want the saved %v unchanged", k, pos[k], last[k])
		}
	}
	if last["table_complete"] != false {
		t.Error("the saved position must not be mutated in place")
	}
}

func TestSweepEndPositionHasNothingToMark(t *testing.T) {
	now := time.Now()
	if sweepEndPosition(nil, "e", now) != nil {
		t.Error("no checkpoint yet (an empty table) needs no save")
	}
	if sweepEndPosition(map[string]interface{}{"table_complete": true}, "e", now) != nil {
		t.Error("a checkpoint that already says complete needs no save")
	}
}

// The helper is only half the fix: the batch loop must call it on the empty-page
// break and keep lastSavedPosition current, or the exact-multiple table still ends
// on table_complete=false.
func TestSweepEndIsWiredIntoTheBatchLoop(t *testing.T) {
	fset := token.NewFileSet()
	src, err := os.ReadFile("executor.go")
	if err != nil {
		t.Fatalf("read executor.go: %v", err)
	}
	f, err := parser.ParseFile(fset, "executor.go", src, 0)
	if err != nil {
		t.Fatalf("parse executor.go: %v", err)
	}
	var batch string
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "executeBatchDataTransfer" {
			batch = string(src[fset.Position(fd.Body.Pos()).Offset:fset.Position(fd.Body.End()).Offset])
		}
	}
	if batch == "" {
		t.Fatal("executeBatchDataTransfer not found in executor.go; if it moved or was renamed, update this test")
	}

	empty := strings.Index(batch, "returned 0 rows after a full page")
	if empty < 0 {
		t.Fatal("empty-page branch not found; if its log line changed, update this test")
	}
	brk := strings.Index(batch[empty:], "\t\t\t\tbreak\n")
	if brk < 0 || !strings.Contains(batch[empty:empty+brk], "sweepEndPosition(lastSavedPosition") {
		t.Error("the empty-page break must save sweepEndPosition, or an exact-multiple table never records table_complete")
	}
	if !strings.Contains(batch, "lastSavedPosition = checkpointPosition") {
		t.Error("each per-batch checkpoint save must update lastSavedPosition")
	}
	if !strings.Contains(batch, "lastSavedPosition = existingCheckpoint.Position") {
		t.Error("a mid-table resume must seed lastSavedPosition, or an empty first page after resume marks nothing")
	}
}
