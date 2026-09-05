package handlers

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"api-gateway/internal/security"
	"api-gateway/internal/validators"

	"github.com/DATA-DOG/go-sqlmock"
)

// Regression tests for defects found in the merged saved-query model feature.
//
// Each test here corresponds to a specific way a scheduled rebuild was able to go
// wrong unattended. They are grouped by the property that failed, and every one of
// them fails against the code as it was before the fix — that is the bar for being in
// this file rather than the general suite.

// ---------------------------------------------------------------------------
// The statement gate: classification by leading verb is not authorization
// ---------------------------------------------------------------------------

// classifyForStorage runs on save and only reads the leading verb, so hostile SQL is
// stored happily. authorizeModelRun is therefore the only thing standing between
// stored sql_text and the engine, and it has to reject far more than a bad class.
//
// The plan interpolates sql_text into CREATE TABLE … AS and hands the whole string to
// a parameter-free Exec, which the driver runs under the simple query protocol — every
// semicolon-separated statement executes. A stacked DROP therefore runs with whatever
// authority the run has, which for a schedule is the admin who created it.
func TestModelSQLGate_RejectsStackedAndDataModifyingStatements(t *testing.T) {
	// wantClass records which layer is expected to catch each statement, because the two
	// groups below are caught by different ones and the difference is worth pinning.
	// A stacked payload still classifies on its leading verb — nothing about a class can
	// describe a second statement — so only the single-statement rule stops it. A
	// data-modifying CTE is now classified as the write it performs, so the class gate
	// catches it first and the statement gate is defense in depth.
	hostile := []struct {
		name      string
		sql       string
		wantClass validators.StatementClass
	}{
		{
			// Classifies as a read on "SELECT"; the second statement is the payload.
			// The class gate cannot help here — this is what the single-statement rule is for.
			name:      "stacked DROP behind a read",
			sql:       "SELECT 1 AS x; DROP TABLE customers",
			wantClass: validators.ClassRead,
		},
		{
			name:      "stacked DROP with a trailing comment",
			sql:       "SELECT 1; DROP TABLE customers; --",
			wantClass: validators.ClassRead,
		},
		{
			// Classified on the write inside the CTE, not on the leading WITH.
			name:      "data-modifying CTE: DELETE",
			sql:       "WITH gone AS (DELETE FROM invoices WHERE paid RETURNING *) SELECT * FROM gone",
			wantClass: validators.ClassDMLWrite,
		},
		{
			name:      "data-modifying CTE: UPDATE",
			sql:       "WITH upd AS (UPDATE invoices SET paid = true RETURNING *) SELECT * FROM upd",
			wantClass: validators.ClassDMLWrite,
		},
		{
			name:      "data-modifying CTE: INSERT",
			sql:       "WITH ins AS (INSERT INTO audit VALUES (1) RETURNING *) SELECT * FROM ins",
			wantClass: validators.ClassDMLWrite,
		},
		{
			// The one that actually got through. A CTE prefix made this a read, and the
			// dangerous-keyword list it then fell to named INSERT INTO, UPDATE and
			// DELETE FROM but never MERGE INTO — so it was storable, materializable and
			// schedulable by a viewer. Its bare form was refused the whole time; the
			// prefix was the entire exploit.
			name:      "data-modifying CTE: MERGE",
			sql:       "WITH s AS (SELECT 1 AS id) MERGE INTO invoices t USING s ON t.id = s.id WHEN MATCHED THEN DELETE",
			wantClass: validators.ClassDMLWrite,
		},
	}

	for _, tc := range hostile {
		t.Run(tc.name, func(t *testing.T) {
			if class := validators.ClassifyStatementSQL(tc.sql); class != tc.wantClass {
				t.Fatalf("ClassifyStatementSQL(%q) = %q, want %q", tc.sql, class, tc.wantClass)
			}

			database, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer database.Close()
			// Owner is the highest role there is, so a refusal here means no role can
			// materialize this. Every case consumes this row: authorizeModelRun resolves
			// the role before it decides anything, because the statement gate it feeds is
			// the one check that must run for every class.
			mock.ExpectQuery("SELECT role FROM workspace_members").
				WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow(string(security.WSOwner)))

			// Materialization is left unset on purpose. An unrecognized mode must take the
			// table path — the one that demands a read — so that a row written by some
			// future migration cannot fall into the branch that permits writes.
			m := &savedQueryModel{
				ID:          "33333333-3333-3333-3333-333333333333",
				WorkspaceID: "11111111-1111-1111-1111-111111111111",
				SQLText:     tc.sql,
			}
			_, refusal, aErr := authorizeModelRun(context.Background(), database, m,
				"22222222-2222-2222-2222-222222222222")

			if aErr != nil {
				t.Fatalf("authorization is decidable for %q, not an error: %v", tc.sql, aErr)
			}
			if refusal == "" {
				t.Fatalf("model gate accepted hostile SQL %q", tc.sql)
			}
		})
	}
}

// The gate must not be so broad that it breaks the feature: a model IS a read query,
// and read CTEs are ordinary in analytics SQL.
func TestModelSQLGate_AcceptsLegitimateReads(t *testing.T) {
	ok := []string{
		"SELECT * FROM orders",
		"WITH r AS (SELECT 1 AS n) SELECT * FROM r",
		"WITH monthly AS (SELECT date_trunc('month', created_at) m, sum(total) t FROM orders GROUP BY 1)\nSELECT * FROM monthly ORDER BY m",
		"SELECT c.name, sum(o.total) FROM customers c JOIN orders o ON o.customer_id = c.id GROUP BY 1",
	}
	for _, s := range ok {
		v := validators.ValidateExplorerStatement(s, security.WSAdmin)
		if v == nil || !v.Valid {
			msg := "<nil>"
			if v != nil {
				msg = v.ErrorMessage
			}
			t.Fatalf("statement gate rejected a legitimate read query %q: %s", s, msg)
		}
	}
}

// modelDialects is every value modelDialect can return for a supported engine.
// Redshift is absent on purpose: it arrives as "postgres" (see TestModelDialect), so a
// table driven by literal "redshift" would be exercising a string that never reaches
// these functions.
var modelDialects = []string{"postgres", "mysql", "sqlserver"}

// ---------------------------------------------------------------------------
// The swap: an interrupted rebuild must never leave the target missing
// ---------------------------------------------------------------------------

// The window between "live table renamed aside" and "staging table renamed in" is the
// one moment where the target name does not exist. A crash there used to leave no
// table at all, and the NEXT run's opening DROP of the retired name then destroyed the
// only surviving copy of the data. The window is closed, not narrowed: MySQL by the
// atomic multi-table RENAME, everyone else by transactional DDL.
func TestModelSwap_IsAtomicOnEveryDialect(t *testing.T) {
	// Every value modelDialect can return. Redshift is absent because it arrives here
	// as "postgres" — see TestModelDialect.
	for _, dialect := range modelDialects {
		swap := modelSwapSQL(dialect, "analytics", "orders", true)

		if dialect == "mysql" {
			if len(swap) != 1 {
				t.Fatalf("[%s] the owned swap must be ONE statement (MySQL has no transactional DDL), got %d: %v",
					dialect, len(swap), swap)
			}
			if !strings.HasPrefix(swap[0], "RENAME TABLE ") || strings.Count(swap[0], " TO ") != 2 {
				t.Fatalf("[%s] expected the atomic multi-table RENAME form, got %q", dialect, swap[0])
			}
			continue
		}

		// Everywhere else the two renames are atomic by virtue of the transaction
		// executeModelPlan wraps the plan in.
		if !modelDialectSupportsTransactionalDDL(dialect) {
			t.Fatalf("[%s] has no atomic swap: not MySQL's single-statement form and not transactional", dialect)
		}
		if len(swap) != 2 {
			t.Fatalf("[%s] owned swap should be retire+swap, got %v", dialect, swap)
		}
	}
}

// A first run has not proven ownership, so it must not retire anything — the rename
// onto an occupied name is what refuses to take over somebody else's table.
func TestModelSwap_UnownedNeverRetires(t *testing.T) {
	for _, dialect := range modelDialects {
		for _, stmt := range modelSwapSQL(dialect, "", "orders", false) {
			if strings.Contains(stmt, modelRetiredSuffix) {
				t.Fatalf("[%s] an unowned swap must not name the retired table: %q", dialect, stmt)
			}
		}
	}
}

// Defence in depth for the same failure: even if the atomicity above were ever lost,
// the plan must not clear the retired name until the replacement has been built.
// Ordering the DROP before the CREATE is what made a crashed run unrecoverable.
func TestBuildMaterializationPlan_DropsRetiredOnlyAfterTheBuild(t *testing.T) {
	for _, dialect := range modelDialects {
		plan := buildMaterializationPlan(dialect, "", "orders", "SELECT 1", true)

		buildIdx, firstRetiredDrop := -1, -1
		for i, stmt := range plan {
			isBuild := strings.Contains(stmt, "orders"+modelStagingSuffix) &&
				(strings.Contains(stmt, "CREATE TABLE") || strings.Contains(stmt, "INTO"))
			if isBuild && buildIdx < 0 {
				buildIdx = i
			}
			if strings.Contains(stmt, "DROP TABLE") &&
				strings.Contains(stmt, "orders"+modelRetiredSuffix) && firstRetiredDrop < 0 {
				firstRetiredDrop = i
			}
		}
		if buildIdx < 0 || firstRetiredDrop < 0 {
			t.Fatalf("[%s] plan is missing a build or a retired-table drop: %v", dialect, plan)
		}
		if firstRetiredDrop < buildIdx {
			t.Fatalf("[%s] the retired table is dropped at %d, BEFORE the replacement is built at %d — "+
				"a crashed previous run would lose its only surviving copy: %v",
				dialect, firstRetiredDrop, buildIdx, plan)
		}
	}
}

// ---------------------------------------------------------------------------
// Execution: the run must get its real deadline and unwind cleanly
// ---------------------------------------------------------------------------

// executeDirectWrite caps every statement at 30 seconds and recycles the connection at
// 35 — correct for an interactive Explorer write, and three orders of magnitude below
// the 30 minutes a rebuild is budgeted. Routing model runs through it made the feature
// unable to do the thing it exists for, so the model path must not use it.
func TestModelRunTimeoutIsNotCappedByTheInteractiveWriteDeadline(t *testing.T) {
	if modelRunTimeout <= 35*time.Second {
		t.Fatalf("modelRunTimeout (%s) is not meaningfully longer than the interactive write cap; "+
			"the separate execution path has no purpose", modelRunTimeout)
	}
}

// Only dialects that cannot roll a half-finished rebuild back need the staging table
// swept by hand. Getting this backwards either leaks scratch tables into the user's
// schema or implies a rollback that will never happen.
func TestModelDialectTransactionalDDL(t *testing.T) {
	for _, d := range []string{"postgres", "sqlserver"} {
		if !modelDialectSupportsTransactionalDDL(d) {
			t.Fatalf("%s commits DDL transactionally; treating it as non-transactional forfeits rollback cleanup", d)
		}
	}
	if modelDialectSupportsTransactionalDDL("mysql") {
		t.Fatal("MySQL commits implicitly on every DDL statement; wrapping its plan in a transaction implies a rollback that will not happen")
	}
}

// ---------------------------------------------------------------------------
// Ownership: the row has to catch up with what the warehouse already did
// ---------------------------------------------------------------------------

// The swap has committed by the time this write runs. If it is lost, the next run
// re-attempts the first-run plan and collides with the table this run created —
// the model then fails forever. A single blip must not do that.
func TestRecordTargetOwnership_SurvivesATransientFailure(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()

	mock.ExpectExec("UPDATE saved_queries SET target_owned").
		WillReturnError(errors.New("connection reset by peer"))
	mock.ExpectExec("UPDATE saved_queries SET target_owned").
		WithArgs("33333333-3333-3333-3333-333333333333").
		WillReturnResult(sqlmock.NewResult(0, 1))

	recordTargetOwnership(database, "33333333-3333-3333-3333-333333333333")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the ownership write did not retry past a transient failure: %v", err)
	}
}

// A rebuild is budgeted 30 minutes, so by the time the swap lands the caller has
// frequently hung up. Taking the run's context here would mean the longest, most
// valuable runs are the ones most likely to lose their ownership write — so the
// function deliberately accepts no context at all. That is enforced by its signature:
// this test stops compiling if a caller context is threaded back in, which is the
// point at which someone should re-read why it was removed.
func TestRecordTargetOwnership_TakesNoCallerContext(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()

	mock.ExpectExec("UPDATE saved_queries SET target_owned").
		WithArgs("33333333-3333-3333-3333-333333333333").
		WillReturnResult(sqlmock.NewResult(0, 1))

	recordTargetOwnership(database, "33333333-3333-3333-3333-333333333333")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the ownership write must not depend on the caller still being connected: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Authorization: "denied" and "could not be determined" are different answers
// ---------------------------------------------------------------------------

// Both outcomes deny the run. Only one of them is permanent. When they were the same
// empty role, a one-second database blip auto-paused a production schedule and
// recorded the reason as "the run-as user is no longer a member of this workspace" —
// a false accusation that needed a human to notice and undo.
func TestResolveWorkspaceRole_SeparatesMissingMembershipFromFailedLookup(t *testing.T) {
	const ws, user = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"

	t.Run("no membership row is an authoritative denial", func(t *testing.T) {
		database, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer database.Close()
		mock.ExpectQuery("SELECT role FROM workspace_members").
			WithArgs(ws, user).
			WillReturnError(sql.ErrNoRows)

		role, rErr := resolveWorkspaceRoleForUser(context.Background(), database, ws, user)
		if rErr != nil {
			t.Fatalf("a missing membership row is a definite answer, not a failure: %v", rErr)
		}
		if role != "" {
			t.Fatalf("expected the empty role for a non-member, got %q", role)
		}
	})

	t.Run("a query failure is not a denial", func(t *testing.T) {
		database, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer database.Close()
		mock.ExpectQuery("SELECT role FROM workspace_members").
			WithArgs(ws, user).
			WillReturnError(errors.New("connection reset by peer"))

		role, rErr := resolveWorkspaceRoleForUser(context.Background(), database, ws, user)
		if !errors.Is(rErr, errRoleUndetermined) {
			t.Fatalf("a transient query failure must report errRoleUndetermined, got %v", rErr)
		}
		if role != "" {
			t.Fatal("a failed lookup must still fail closed for this run")
		}
	})

	t.Run("a real role is returned normalized", func(t *testing.T) {
		database, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer database.Close()
		mock.ExpectQuery("SELECT role FROM workspace_members").
			WithArgs(ws, user).
			WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("  Admin "))

		role, rErr := resolveWorkspaceRoleForUser(context.Background(), database, ws, user)
		if rErr != nil {
			t.Fatalf("unexpected error: %v", rErr)
		}
		if role != security.WSAdmin {
			t.Fatalf("expected %q, got %q", security.WSAdmin, role)
		}
	})
}

// The undecided case must reach the caller as an ERROR, never as a refusal string:
// a refusal is what gets written to auto_paused_reason.
func TestAuthorizeModelRun_UndecidedIsAnErrorNotARefusal(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()
	mock.ExpectQuery("SELECT role FROM workspace_members").
		WillReturnError(errors.New("server closed the connection unexpectedly"))

	m := &savedQueryModel{
		ID:          "33333333-3333-3333-3333-333333333333",
		WorkspaceID: "11111111-1111-1111-1111-111111111111",
		SQLText:     "SELECT * FROM orders",
	}
	_, refusal, aErr := authorizeModelRun(context.Background(), database, m,
		"22222222-2222-2222-2222-222222222222")

	if aErr == nil {
		t.Fatal("a failed role lookup must surface as an error so the run stays retryable")
	}
	if refusal != "" {
		t.Fatalf("an undecided authorization must not produce a refusal string "+
			"(it would be recorded as auto_paused_reason and pause the schedule permanently), got %q", refusal)
	}
}

// A genuine demotion is still a refusal, and still pauses. The fix must not have
// turned every denial into a retry.
func TestAuthorizeModelRun_DemotionIsStillARefusal(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()
	mock.ExpectQuery("SELECT role FROM workspace_members").
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))

	m := &savedQueryModel{
		ID:          "33333333-3333-3333-3333-333333333333",
		WorkspaceID: "11111111-1111-1111-1111-111111111111",
		SQLText:     "SELECT * FROM orders",
	}
	_, refusal, aErr := authorizeModelRun(context.Background(), database, m,
		"22222222-2222-2222-2222-222222222222")

	if aErr != nil {
		t.Fatalf("a resolved role is a decided answer, not an error: %v", aErr)
	}
	if refusal == "" {
		t.Fatal("a member below the model-run bar must be refused")
	}
	if !strings.Contains(refusal, string(modelRunMinRole)) {
		t.Fatalf("the refusal should name the role required, got %q", refusal)
	}
}

// ---------------------------------------------------------------------------
// The plan's shape: a table dropped outside rsync must not wedge the model
// ---------------------------------------------------------------------------

// planFor drives the builder against a catalog that answers with the given count, and
// returns the plan it chose.
func planFor(t *testing.T, dialect string, owned bool, targetCount int) []string {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()

	if owned {
		mock.ExpectQuery("information_schema.tables").
			WithArgs("orders", "analytics").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(targetCount))
	}

	plan, err := modelPlanBuilder(dialect, "analytics", "orders", "SELECT 1", owned)(context.Background(), database)
	if err != nil {
		t.Fatalf("[%s] the builder failed: %v", dialect, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("[%s] %v", dialect, err)
	}
	return plan
}

func planRetires(plan []string) bool {
	for _, stmt := range plan {
		if strings.Contains(stmt, modelRetiredSuffix) && !strings.Contains(stmt, "DROP TABLE") {
			return true
		}
	}
	return false
}

// The defect: target_owned is a permanent record, but the table it refers to is not.
// Drop the live table outside rsync — a cleanup script, a hand-run migration, a restore
// from backup — and every subsequent run tried to rename a table that was no longer
// there. It failed identically forever, and no amount of retrying or rescheduling
// helped, because nothing about the run changed the state it was tripping over. The
// only exit was a hand-written UPDATE to a column no operator knows exists.
func TestModelPlan_DoesNotRetireATargetThatIsNoLongerThere(t *testing.T) {
	for _, dialect := range modelDialects {
		plan := planFor(t, dialect, true, 0)

		if planRetires(plan) {
			t.Fatalf("[%s] the target is gone, but the plan still tries to retire it — "+
				"this rename fails on every run and the model never recovers: %v", dialect, plan)
		}
		// Degrading to the first-run shape is only useful if it still rebuilds.
		if !strings.Contains(strings.Join(plan, "\n"), "orders"+modelStagingSuffix) {
			t.Fatalf("[%s] the plan no longer builds the staging table: %v", dialect, plan)
		}
	}
}

// The other half of the same decision: when the table IS there, it still has to be
// moved aside rather than dropped, or the swap loses its only backup.
func TestModelPlan_RetiresTheLiveTableWhenItIsStillThere(t *testing.T) {
	for _, dialect := range modelDialects {
		if plan := planFor(t, dialect, true, 1); !planRetires(plan) {
			t.Fatalf("[%s] an owned run over a live table must retire it, not overwrite it: %v", dialect, plan)
		}
	}
}

// An unowned run has nothing to retire by definition, and must not spend a round trip
// asking. sqlmock fails the test if the builder queries at all.
func TestModelPlan_UnownedNeverAsksTheCatalog(t *testing.T) {
	for _, dialect := range modelDialects {
		if plan := planFor(t, dialect, false, 0); planRetires(plan) {
			t.Fatalf("[%s] an unowned run must not name the retired table: %v", dialect, plan)
		}
	}
}

// Not knowing is not the same as "it is not there". Treating a failed catalog read as
// an absent table would send a live table into the rename that refuses — and on the
// engines with transactional DDL, would do it after the expensive build. Failing the
// run keeps it retryable and leaves the warehouse untouched.
func TestModelPlan_ACatalogFailureIsNotAnAbsentTable(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()
	mock.ExpectQuery("information_schema.tables").
		WillReturnError(errors.New("connection reset by peer"))

	plan, err := modelPlanBuilder("postgres", "analytics", "orders", "SELECT 1", true)(
		context.Background(), database)
	if err == nil {
		t.Fatalf("a catalog read that failed must fail the run, not be read as 'nothing to retire': %v", plan)
	}
	if plan != nil {
		t.Fatalf("no plan should be returned alongside the error: %v", plan)
	}
}

// The check has to resolve an unqualified target the same way the plan's own DDL will —
// through the connection's current schema, not a guess like "public" — and must count
// only base tables. A view standing at the target name is not something rsync left
// behind, and ALTER TABLE … RENAME would happily move a user's view aside.
func TestModelTargetExists_IsSchemaAwareAndTableOnly(t *testing.T) {
	for _, dialect := range modelDialects {
		database, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}

		var captured string
		mock.ExpectQuery("information_schema.tables").
			WithArgs("orders", "").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		exists, err := modelTargetExists(context.Background(), queryRecorder{database, &captured},
			dialect, "", "orders")
		if err != nil {
			t.Fatalf("[%s] %v", dialect, err)
		}
		if !exists {
			t.Fatalf("[%s] a counted row means the table is there", dialect)
		}
		if !strings.Contains(captured, "BASE TABLE") {
			t.Fatalf("[%s] the check must exclude views: %q", dialect, captured)
		}
		if !strings.Contains(captured, "NULLIF") {
			t.Fatalf("[%s] an empty schema must fall back to the connection's current schema, "+
				"which is where the plan's own unqualified DDL lands: %q", dialect, captured)
		}
		database.Close()
	}
}

// queryRecorder captures the SQL on its way past so a test can assert on the text
// itself, which sqlmock's regexp matching cannot express negatively.
type queryRecorder struct {
	inner *sql.DB
	seen  *string
}

func (r queryRecorder) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	*r.seen = query
	return r.inner.QueryRowContext(ctx, query, args...)
}
