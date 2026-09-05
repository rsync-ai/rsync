package handlers

import (
	"encoding/csv"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestWriteDelimited_CSV_QuotingAndEscaping locks in the CSV escaping
// rules: values containing commas, double-quotes, or newlines get
// wrapped in quotes; internal quotes are doubled. Empty values are
// emitted as empty fields (between separators).
func TestWriteDelimited_CSV_QuotingAndEscaping(t *testing.T) {
	gin.SetMode(gin.TestMode)

	result := &ExplorerQueryResponse{
		Columns: []string{"name", "note", "amount"},
		Rows: []map[string]interface{}{
			{"name": "Alice", "note": "ok", "amount": 100},
			{"name": "Bob, Jr.", "note": "needs\nreview", "amount": nil},
			{"name": "Carol", "note": `she said "hi"`, "amount": 50.5},
		},
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	writeDelimited(c, result, ',', "text/csv", "test.csv")

	body := w.Body.String()

	// Round-trip parse: the output must be valid CSV.
	r := csv.NewReader(strings.NewReader(body))
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("output is not valid CSV: %v\nbody=%q", err, body)
	}

	// Expect header + 3 rows.
	if got := len(records); got != 4 {
		t.Fatalf("expected 4 records (header + 3 rows), got %d", got)
	}

	// Header.
	expectedHeader := []string{"name", "note", "amount"}
	for i, h := range expectedHeader {
		if records[0][i] != h {
			t.Errorf("header[%d] = %q, want %q", i, records[0][i], h)
		}
	}

	// Row 1: simple values.
	if records[1][0] != "Alice" || records[1][1] != "ok" || records[1][2] != "100" {
		t.Errorf("row 1 mismatch: %+v", records[1])
	}

	// Row 2: comma in name, newline in note, nil amount → empty.
	if records[2][0] != "Bob, Jr." {
		t.Errorf("row 2 name = %q, want %q", records[2][0], "Bob, Jr.")
	}
	if records[2][1] != "needs\nreview" {
		t.Errorf("row 2 note = %q, want %q", records[2][1], "needs\nreview")
	}
	if records[2][2] != "" {
		t.Errorf("row 2 amount = %q, want empty (was nil)", records[2][2])
	}

	// Row 3: embedded double-quote in note must round-trip cleanly.
	if records[3][1] != `she said "hi"` {
		t.Errorf("row 3 note = %q, want %q", records[3][1], `she said "hi"`)
	}
}

// TestWriteDelimited_TSV_CollapsesNewlinesAndTabs verifies that TSV
// output flattens embedded newlines/tabs to spaces so the row
// structure stays intact even when consumed by naive tab-splitters.
func TestWriteDelimited_TSV_CollapsesNewlinesAndTabs(t *testing.T) {
	gin.SetMode(gin.TestMode)

	result := &ExplorerQueryResponse{
		Columns: []string{"id", "raw"},
		Rows: []map[string]interface{}{
			{"id": 1, "raw": "line1\nline2"},
			{"id": 2, "raw": "col1\tcol2"},
			{"id": 3, "raw": "plain"},
		},
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	writeDelimited(c, result, '\t', "text/tsv", "test.tsv")

	body := w.Body.String()
	lines := strings.Split(strings.TrimRight(body, "\n"), "\n")

	if got := len(lines); got != 4 {
		t.Fatalf("expected 4 lines (header + 3 rows), got %d: %q", got, body)
	}

	for i, line := range lines {
		// Each line must have exactly one tab per column gap = 1 tab
		// for 2 columns. Embedded tabs or newlines would break this.
		if tabs := strings.Count(line, "\t"); tabs != 1 {
			t.Errorf("line %d has %d tabs, want 1: %q", i, tabs, line)
		}
	}

	// Row 1: \n in value must have been replaced with a space.
	if !strings.Contains(lines[1], "line1 line2") {
		t.Errorf("row 1 should have newline replaced with space, got %q", lines[1])
	}
	// Row 2: \t in value must have been replaced with a space.
	if !strings.Contains(lines[2], "col1 col2") {
		t.Errorf("row 2 should have tab replaced with space, got %q", lines[2])
	}
}

// TestWriteDelimited_NoRows confirms the header is still emitted
// (with a trailing newline) when the query returns zero rows.
func TestWriteDelimited_NoRows(t *testing.T) {
	gin.SetMode(gin.TestMode)

	result := &ExplorerQueryResponse{
		Columns: []string{"a", "b"},
		Rows:    []map[string]interface{}{},
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	writeDelimited(c, result, ',', "text/csv", "test.csv")

	body := w.Body.String()
	if body != "a,b\n" {
		t.Errorf("zero-row body = %q, want %q", body, "a,b\n")
	}
}
