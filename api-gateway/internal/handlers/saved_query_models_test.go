package handlers

import (
	"strings"
	"testing"

	"api-gateway/internal/security"
	"api-gateway/internal/validators"
)

// A model (migration 085) rebuilds a real table on a schedule with nobody watching,
// so the tests here pin the properties whose failure is silent and expensive:
//
//  1. the target identifier cannot carry SQL — it is interpolated, not bound;
//  2. a rebuild never destroys the live table before the replacement exists;
//  3. a first run cannot take over a table this model did not create;
//  4. the role bar is the existing DDL bar, not a number invented here.

// ---------------------------------------------------------------------------
// 1. Identifier validation
// ---------------------------------------------------------------------------

func TestValidateModelTarget_AcceptsPlainAndQualified(t *testing.T) {
	cases := []struct{ in, wantSchema, wantTable string }{
		{"orders", "", "orders"},
		{"analytics.orders", "analytics", "orders"},
		{"  analytics.orders  ", "analytics", "orders"},
		{"_private_t1", "", "_private_t1"},
	}
	for _, tc := range cases {
		gotSchema, gotTable, err := validateModelTarget(tc.in)
		if err != nil {
			t.Fatalf("validateModelTarget(%q) errored: %v", tc.in, err)
		}
		if gotSchema != tc.wantSchema || gotTable != tc.wantTable {
			t.Fatalf("validateModelTarget(%q) = (%q,%q), want (%q,%q)",
				tc.in, gotSchema, gotTable, tc.wantSchema, tc.wantTable)
		}
	}
}

// The target is interpolated into DDL because identifiers cannot be bound as query
// parameters. This test is the guard rail that makes that safe — every rejection
// below is a string that would otherwise reach the engine as syntax.
func TestValidateModelTarget_RejectsInjectionAndMalformed(t *testing.T) {
	bad := []string{
		``,
		`   `,
		`orders; DROP TABLE users`,
		`orders"; DROP TABLE users; --`,
		"orders`",
		`orders'`,
		`[orders]`,
		`a.b.c`,                 // three parts
		`.orders`,               // empty schema
		`orders.`,               // empty table
		`1orders`,               // leading digit
		`ord ers`,               // space
		`ordersé`,               // non-ascii
		`orders--comment`,       // comment marker
		`orders/*x*/`,           // block comment
		strings.Repeat("a", 53), // over the 52-char ceiling
		`ok.` + strings.Repeat("a", 53),
	}
	for _, in := range bad {
		if _, _, err := validateModelTarget(in); err == nil {
			t.Fatalf("validateModelTarget(%q) accepted a value it must reject", in)
		}
	}
}

// The 52-char ceiling is not cosmetic: it is what keeps a staging name inside
// Postgres' 63-byte identifier limit. Without the headroom, two long target names
// would truncate to the same staging name and one model's rebuild would land in
// another model's scratch table.
func TestModelStagingNamesFitPostgresIdentifierLimit(t *testing.T) {
	const pgIdentLimit = 63
	longest := strings.Repeat("a", 52)
	if _, _, err := validateModelTarget(longest); err != nil {
		t.Fatalf("52 chars should be the accepted maximum, got: %v", err)
	}
	for _, suffix := range []string{modelStagingSuffix, modelRetiredSuffix} {
		if n := len(longest + suffix); n > pgIdentLimit {
			t.Fatalf("longest target + %q = %d bytes, exceeds Postgres' %d-byte limit",
				suffix, n, pgIdentLimit)
		}
	}
}

// ---------------------------------------------------------------------------
// 2. Dialect rendering
// ---------------------------------------------------------------------------

func TestQuoteModelIdent_PerDialect(t *testing.T) {
	cases := []struct{ dialect, want string }{
		{"postgres", `"analytics"."orders"`},
		{"mysql", "`analytics`.`orders`"},
		{"sqlserver", `[analytics].[orders]`},
	}
	for _, tc := range cases {
		if got := quoteModelIdent(tc.dialect, "analytics", "orders"); got != tc.want {
			t.Fatalf("quoteModelIdent(%s) = %s, want %s", tc.dialect, got, tc.want)
		}
	}
	if got := quoteModelIdent("postgres", "", "orders"); got != `"orders"` {
		t.Fatalf("unqualified postgres ident = %s, want %q", got, `"orders"`)
	}
}

// SQL Server has no CREATE TABLE AS SELECT. If this ever silently reverts to the
// generic form, every SQL Server model breaks at the first rebuild.
func TestCreateTableAsSQL_SQLServerUsesSelectInto(t *testing.T) {
	got := createTableAsSQL("sqlserver", "[dbo].[orders]", "SELECT 1 AS n;")
	if !strings.HasPrefix(got, "SELECT * INTO [dbo].[orders] FROM (") {
		t.Fatalf("sqlserver materialization = %q, want a SELECT ... INTO form", got)
	}
	if strings.Contains(got, "CREATE TABLE") {
		t.Fatalf("sqlserver materialization must not use CREATE TABLE AS: %q", got)
	}
	if strings.Contains(got, "1 AS n;") {
		t.Fatalf("trailing semicolon must be stripped before nesting: %q", got)
	}

	for _, d := range []string{"postgres", "mysql"} {
		got := createTableAsSQL(d, "x", "SELECT 1")
		if !strings.HasPrefix(got, "CREATE TABLE x AS ") {
			t.Fatalf("%s materialization = %q, want CREATE TABLE AS", d, got)
		}
	}
}

func TestModelDialect(t *testing.T) {
	cases := map[string]string{
		"postgresql": "postgres",
		"postgres":   "postgres",
		"redshift":   "postgres",
		"mysql":      "mysql",
		"mariadb":    "mysql",
		"sqlserver":  "sqlserver",
		"mssql":      "sqlserver",
		"bigquery":   "",
		"databricks": "",
		"snowflake":  "",
		"salesforce": "",
	}
	for in, want := range cases {
		if got := modelDialect(in); got != want {
			t.Fatalf("modelDialect(%q) = %q, want %q", in, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// 3. The rebuild plan — the safety invariants
// ---------------------------------------------------------------------------

// The whole reason for build-then-swap: an upstream schema change that breaks a
// model's SQL must cost you a failed run, not yesterday's table. If the plan ever
// touches the live target before the replacement is built, drift turns into data
// loss — the exact failure this product exists to catch.
func TestBuildMaterializationPlan_NeverTouchesTargetBeforeTheRebuild(t *testing.T) {
	for _, dialect := range []string{"postgres", "mysql", "sqlserver"} {
		plan := buildMaterializationPlan(dialect, "analytics", "orders", "SELECT 1", true)

		buildIdx := -1
		for i, stmt := range plan {
			if strings.Contains(stmt, "orders"+modelStagingSuffix) &&
				(strings.Contains(stmt, "CREATE TABLE") || strings.Contains(stmt, "INTO")) {
				buildIdx = i
				break
			}
		}
		if buildIdx < 0 {
			t.Fatalf("[%s] plan has no statement that builds the staging table: %v", dialect, plan)
		}

		// Everything before the build may only name rsync's own scratch tables.
		for i := 0; i < buildIdx; i++ {
			if namesLiveTarget(plan[i], "orders") {
				t.Fatalf("[%s] statement %d touches the live target before the rebuild: %q",
					dialect, i, plan[i])
			}
		}
	}
}

// The live table is renamed aside, never dropped outright, and the only DROP aimed
// at real data comes last — after the new table is already in place under the target
// name.
func TestBuildMaterializationPlan_ReplaceRetiresRatherThanDrops(t *testing.T) {
	plan := buildMaterializationPlan("postgres", "", "orders", "SELECT 1", true)

	var retireIdx, swapIdx, cleanupIdx = -1, -1, -1
	for i, stmt := range plan {
		switch {
		case strings.Contains(stmt, `ALTER TABLE "orders" RENAME TO "orders`+modelRetiredSuffix+`"`):
			retireIdx = i
		case strings.Contains(stmt, `ALTER TABLE "orders`+modelStagingSuffix+`" RENAME TO "orders"`):
			swapIdx = i
		case strings.Contains(stmt, `DROP TABLE IF EXISTS "orders`+modelRetiredSuffix+`"`) && i > 0:
			cleanupIdx = i
		}
	}
	if retireIdx < 0 || swapIdx < 0 || cleanupIdx < 0 {
		t.Fatalf("replace plan is missing retire/swap/cleanup: %v", plan)
	}
	if !(retireIdx < swapIdx && swapIdx < cleanupIdx) {
		t.Fatalf("replace plan order must be retire → swap → cleanup, got %d/%d/%d: %v",
			retireIdx, swapIdx, cleanupIdx, plan)
	}

	// No statement anywhere drops the target name itself.
	for _, stmt := range plan {
		if strings.Contains(stmt, "DROP TABLE") && namesLiveTarget(stmt, "orders") {
			t.Fatalf("plan drops the live target directly: %q", stmt)
		}
	}
}

// On a first run nothing has proven ownership, so the plan must contain no statement
// that could displace an existing table of that name. The engine rejects the final
// rename if the name is taken — that refusal is the ownership check, and it only
// works if the plan never drops or renames the target out of the way first.
func TestBuildMaterializationPlan_FirstRunCannotDisplaceAnExistingTable(t *testing.T) {
	for _, dialect := range []string{"postgres", "mysql", "sqlserver"} {
		plan := buildMaterializationPlan(dialect, "", "orders", "SELECT 1", false)
		for _, stmt := range plan {
			if namesLiveTarget(stmt, "orders") &&
				(strings.Contains(stmt, "DROP TABLE") || strings.Contains(stmt, "RENAME TO "+quoteModelIdent(dialect, "", "orders"+modelRetiredSuffix))) {
				t.Fatalf("[%s] first-run plan must not clear the target: %q", dialect, stmt)
			}
		}
		// The last statement must be the swap onto the target name — the step that
		// fails if somebody else owns it.
		last := plan[len(plan)-1]
		if !namesLiveTarget(last, "orders") {
			t.Fatalf("[%s] first-run plan must end by claiming the target, got %q", dialect, last)
		}
	}
}

// namesLiveTarget reports whether a statement refers to the target table itself
// rather than one of rsync's suffixed scratch tables.
func namesLiveTarget(stmt, table string) bool {
	stripped := strings.ReplaceAll(stmt, table+modelStagingSuffix, "")
	stripped = strings.ReplaceAll(stripped, table+modelRetiredSuffix, "")
	return strings.Contains(stripped, table)
}

// ---------------------------------------------------------------------------
// 4. Authorization
// ---------------------------------------------------------------------------

// modelRunMinRole must stay tied to the existing statement-policy table rather than
// drifting into a hand-picked number. A run issues CREATE TABLE and DROP TABLE, so
// the bar is whatever DDL requires.
func TestModelRunMinRoleMatchesDDLPolicy(t *testing.T) {
	ddlMin, blocked := validators.MinRoleForClass(validators.ClassDDL)
	if blocked {
		t.Fatal("ClassDDL is blocked outright; modelRunMinRole is built on a false premise")
	}
	if modelRunMinRole != ddlMin {
		t.Fatalf("modelRunMinRole = %q but ClassDDL requires %q — the model bar has drifted from the policy table",
			modelRunMinRole, ddlMin)
	}
}

// The gate is fail-closed on the way in as well as the way up: an unknown or empty
// role satisfies nothing. That is what makes it safe for resolveWorkspaceRoleForUser
// to express "no membership row" as an empty role — note that it reports a FAILED
// lookup as errRoleUndetermined instead, precisely because both would otherwise land
// here indistinguishably.
func TestModelRunRoleGateFailsClosed(t *testing.T) {
	for _, r := range []security.WorkspaceRole{"", "viewer", "member", "bogus", "Admin "} {
		if r.Meets(modelRunMinRole) {
			t.Fatalf("role %q must not clear the model run bar (%q)", r, modelRunMinRole)
		}
	}
	for _, r := range []security.WorkspaceRole{"admin", "owner"} {
		if !r.Meets(modelRunMinRole) {
			t.Fatalf("role %q should clear the model run bar (%q)", r, modelRunMinRole)
		}
	}
}
