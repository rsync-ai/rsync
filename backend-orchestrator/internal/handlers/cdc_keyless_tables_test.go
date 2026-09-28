package handlers

import (
	"os"
	"strings"
	"testing"
)

// Keyless CDC policy (2026-09-27): Edit tables, backfill/re-snapshot and
// recover no longer refuse a keyless table for a database destination. They
// used to answer {"error":"missing_primary_key","tables":[…]}; the pre-flight
// assessor now warns instead, and the start path only logs.
func TestCDCHandlersDoNotRefuseKeylessTables(t *testing.T) {
	for _, file := range []string{"cdc.go", "cdc_recover.go"} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, refusal := range []string{`"missing_primary_key"`, `"cdc_pk_validation_unsupported"`, "ValidateTablesHavePrimaryKeys("} {
			if strings.Contains(string(src), refusal) {
				t.Errorf("%s refuses keyless tables again (%s); keyless CDC tables warn, they are not blocked", file, refusal)
			}
		}
	}
}

func TestNormalizeCDCDestType(t *testing.T) {
	cases := map[string]string{
		"postgres":   "postgresql",
		"postgresql": "postgresql",
		"mariadb":    "mysql",
		"mysql":      "mysql",
		"mongodb":    "mongodb",
		"gcs":        "gcs",
		"":           "",
	}
	for in, want := range cases {
		if got := normalizeCDCDestType(in); got != want {
			t.Errorf("normalizeCDCDestType(%q) = %q; want %q", in, got, want)
		}
	}
}
