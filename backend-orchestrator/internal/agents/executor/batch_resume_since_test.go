package executor

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func TestResumeIncrementalSince(t *testing.T) {
	cases := []struct {
		name      string
		pos       map[string]interface{}
		midTable  bool
		watermark string
		want      string
	}{
		{"a new sweep filters on the last watermark", map[string]interface{}{sinceWatermarkKey: "2026-09-01T00:00:00Z"}, false, "2026-09-20T00:00:00Z", "2026-09-20T00:00:00Z"},
		{"a continuation keeps its sweep's baseline, not the running max", map[string]interface{}{sinceWatermarkKey: "2026-09-01T00:00:00Z"}, true, "2026-09-20T00:00:00Z", "2026-09-01T00:00:00Z"},
		{"a continuation of a full sweep stays unfiltered", map[string]interface{}{sinceWatermarkKey: ""}, true, "2026-09-20T00:00:00Z", ""},
		{"a pre-fix continuation has no baseline and reads the rest unfiltered", map[string]interface{}{}, true, "2026-09-20T00:00:00Z", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := resumeIncrementalSince(c.pos, c.midTable, c.watermark); got != c.want {
				t.Fatalf("since = %q, want %q", got, c.want)
			}
		})
	}
}

func TestSweepBaselineSinceIsWired(t *testing.T) {
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
	if !strings.Contains(batch, "sinceWatermarkKey: incrementalSince,") {
		t.Error("every checkpoint must record the since its sweep started with")
	}
	if !strings.Contains(batch, "incrementalSince = baseline") || !strings.Contains(batch, "resumeIncrementalSince(existingCheckpoint.Position, true,") {
		t.Error("a mid-table resume must filter on its sweep's baseline, not the running watermark")
	}
}

func TestResumeTableRowsSoFarNeverReadsTheRunTotal(t *testing.T) {
	cases := []struct {
		name string
		pos  map[string]interface{}
		want int
	}{
		{"per-table count", map[string]interface{}{"rows_so_far": float64(90000), tableRowsSoFarKey: float64(30000)}, 30000},
		{"legacy checkpoint has only the run total", map[string]interface{}{"rows_so_far": float64(90000)}, 0},
		{"empty", map[string]interface{}{}, 0},
	}
	for _, c := range cases {
		if got := resumeTableRowsSoFar(c.pos); got != c.want {
			t.Errorf("%s: resumeTableRowsSoFar = %d, want %d", c.name, got, c.want)
		}
	}
}

func TestTableRowsSoFarIsSavedAndResumed(t *testing.T) {
	batch := executeBatchDataTransferBody(t)
	if !strings.Contains(batch, "startRowsSoFar = resumeTableRowsSoFar(existingCheckpoint.Position)") {
		t.Error("the runaway backstop must resume from this table's own count")
	}
	if strings.Contains(batch, `existingCheckpoint.Position["rows_so_far"]`) {
		t.Error("rows_so_far is the whole run's total; a table must not resume its backstop from it")
	}
	if !strings.Contains(batch, "tableRowsSoFarKey: startRowsSoFar + dispatchRows + sourceRowCount") {
		t.Error("each checkpoint must save this table's own row count")
	}
}
