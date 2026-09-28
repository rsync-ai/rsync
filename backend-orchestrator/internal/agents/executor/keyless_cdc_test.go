package executor

import (
	"os"
	"strings"
	"testing"
)

// Keyless CDC policy (2026-09-27): a CDC source table with no primary key is not
// refused at start for a database destination. The executor still looks the
// tables up (that lookup is the "table not found" check) but only logs the
// keyless ones.

func TestKeylessCDCTablesWarning(t *testing.T) {
	if got := keylessCDCTablesWarning("postgresql", nil); got != "" {
		t.Fatalf("no keyless tables must produce no warning, got %q", got)
	}
	for dest, unit := range map[string]string{"postgresql": "row", "mysql": "row", "mongodb": "document"} {
		got := keylessCDCTablesWarning(dest, []string{"public.events", "public.audit"})
		for _, want := range []string{
			"public.events, public.audit",
			"each UPDATE adds a new " + unit,
			"DELETEs are dead-lettered",
			"Add a primary key for an exact copy",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("%s: warning lacks %q: %s", dest, want, got)
			}
		}
	}
}

func TestCDCKeyCheckNamespace(t *testing.T) {
	cases := map[string]string{
		"mysql":      "appdb",
		"mongodb":    "appdb",
		"postgresql": "public",
		"sqlserver":  "public",
		"oracle":     "public",
	}
	for source, want := range cases {
		if got := cdcKeyCheckNamespace(source, "appdb", "public"); got != want {
			t.Errorf("cdcKeyCheckNamespace(%q) = %q; want %q", source, got, want)
		}
	}
}

// TestExecutorDoesNotRefuseKeylessCDCTables guards the start path itself, which
// needs a live source to exercise: the refusal it used to return must not come
// back. The historical message stays in pkg/diagnose and workers' classifiers
// because runs recorded before 2026-09-27 still carry it.
func TestExecutorDoesNotRefuseKeylessCDCTables(t *testing.T) {
	src, err := os.ReadFile("executor.go")
	if err != nil {
		t.Fatalf("read executor.go: %v", err)
	}
	for _, refusal := range []string{
		"CDC requires PRIMARY KEY for DB destinations",
		"unsupported CDC source for PK validation",
	} {
		if strings.Contains(string(src), refusal) {
			t.Errorf("executor.go refuses keyless CDC tables again (%q); keyless tables warn, they do not fail the run", refusal)
		}
	}
	if !strings.Contains(string(src), "keylessCDCTablesWarning(normalizedDest, missing)") {
		t.Error("executor.go no longer routes keyless tables through keylessCDCTablesWarning")
	}
}
