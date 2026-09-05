package executor

import (
	"errors"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// Regression guard for O18: in run_mode=reload, a failed drop_table on a RELATIONAL
// destination must abort the run (otherwise the next batch silently appends
// duplicates / writes into a stale-schema table). Non-relational connectors that
// legitimately lack drop_table keep warn-and-continue. classifyReloadDropResult is
// the pure decision the executor's reload block uses.
func TestClassifyReloadDropResult(t *testing.T) {
	ok := &mcp.ExecuteResponse{Success: true, Result: map[string]interface{}{"dropped": true}}
	notSupported := &mcp.ExecuteResponse{Success: false, Error: "drop_table not supported by this connector"}
	otherFail := &mcp.ExecuteResponse{Success: false, Error: "permission denied for table customers"}

	cases := []struct {
		name       string
		destType   string
		resp       *mcp.ExecuteResponse
		err        error
		wantFatal  bool
		wantDetail bool // detail string is non-empty
	}{
		// Relational + any failure mode -> fatal (reload cannot rebuild).
		{"relational_drop_errored", "postgresql", nil, errors.New("conn reset"), true, true},
		{"relational_not_supported", "mysql", notSupported, nil, true, true},
		{"relational_other_failure", "postgres", otherFail, nil, true, true},
		{"relational_nil_resp", "sqlserver", nil, nil, true, false}, // nil resp, no err -> failed, no detail
		// Relational + success -> not fatal, no detail (drop happened).
		{"relational_success", "postgresql", ok, nil, false, false},
		// Non-relational + failure -> warn-and-continue (not fatal) but detail present.
		{"nonrelational_failure", "gsheets", notSupported, nil, false, true},
		{"nonrelational_drop_errored", "notion", nil, errors.New("boom"), false, true},
		// Non-relational + success -> not fatal, no detail.
		{"nonrelational_success", "shopify-admin-graphql", ok, nil, false, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fatal, detail := classifyReloadDropResult(tc.destType, tc.resp, tc.err)
			if fatal != tc.wantFatal {
				t.Fatalf("fatal = %v, want %v (detail=%q)", fatal, tc.wantFatal, detail)
			}
			if (detail != "") != tc.wantDetail {
				t.Fatalf("detail non-empty = %v (%q), want %v", detail != "", detail, tc.wantDetail)
			}
		})
	}
}

// isRelationalDB underpins the fail-loud gate; pin its coverage of the relational
// family and a couple of non-relational connectors so the O18 decision stays correct.
func TestIsRelationalDB_Coverage(t *testing.T) {
	relational := []string{"mysql", "postgresql", "postgres", "oracle", "sqlserver", "mariadb", "sqlite", "MySQL", " postgresql "}
	for _, c := range relational {
		if !isRelationalDB(c) {
			t.Errorf("isRelationalDB(%q) = false, want true", c)
		}
	}
	nonRelational := []string{"minio", "s3", "gsheets", "notion", "shopify-admin-graphql", "github-rest", ""}
	for _, c := range nonRelational {
		if isRelationalDB(c) {
			t.Errorf("isRelationalDB(%q) = true, want false", c)
		}
	}
}
