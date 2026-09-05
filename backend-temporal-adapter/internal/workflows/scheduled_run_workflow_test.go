package workflows

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

// errConnector is a stdlib-only driver.Connector whose connections always fail,
// so any query issued on the resulting *sql.DB returns a non-nil error. It lets us
// drive CheckActiveRunActivity down its DB-error path without a mock dependency.
type errConnector struct{}

func (errConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("simulated database failure")
}

func (errConnector) Driver() driver.Driver { return errDriver{} }

type errDriver struct{}

func (errDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("simulated database failure")
}

// TestCheckActiveRunActivity_FailsClosedOnDBError is the regression guard for the
// overlap-guard fail-open bug. On a DB error the activity MUST return a non-nil
// error (so the wrapper workflow aborts / Temporal retries) and MUST NOT return
// (false, nil) — which previously let a scheduled run proceed concurrently with an
// in-flight run and risk double-writing the destination.
func TestCheckActiveRunActivity_FailsClosedOnDBError(t *testing.T) {
	prev := activityCtx
	t.Cleanup(func() { activityCtx = prev })

	activityCtx = &ActivityContext{DB: sql.OpenDB(errConnector{})}

	hasActive, err := CheckActiveRunActivity(context.Background(), "pipeline-under-test")
	if err == nil {
		t.Fatalf("expected a non-nil error when the DB check fails (fail-closed); got nil "+
			"(fail-open regression). hasActive=%v", hasActive)
	}
	if hasActive {
		// On the error path the activity returns false; the workflow aborts on the
		// non-nil error, not on this bool. Guard the contract anyway.
		t.Errorf("expected hasActive=false on the error path; got true")
	}
}

// TestCheckActiveRunActivity_ErrorsWhenNoDB verifies the nil-DB guard also fails
// closed rather than silently allowing a run.
func TestCheckActiveRunActivity_ErrorsWhenNoDB(t *testing.T) {
	prev := activityCtx
	t.Cleanup(func() { activityCtx = prev })

	activityCtx = &ActivityContext{DB: nil}

	if _, err := CheckActiveRunActivity(context.Background(), "pipeline-under-test"); err == nil {
		t.Fatalf("expected a non-nil error when the DB is unavailable; got nil")
	}
}
