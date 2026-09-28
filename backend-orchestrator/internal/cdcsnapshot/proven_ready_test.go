package cdcsnapshot

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// The bug class: a request whose connector the caller has ALREADY watched run
// for ReadyStable is held for a second ReadyStable wait (plus the tick phase)
// in the dispatcher. A CDC Reload paid 10-15 s this way on top of the start
// check's own 10 s. The proof may only replace the wait, never the dispatcher's
// own Ready reading.

func expectSend(mock sqlmock.Sqlmock) {
	mock.ExpectExec(regexp.QuoteMeta(`SET status = 'sent'`)).WithArgs("r1", "queued", 0).WillReturnResult(sqlmock.NewResult(0, 1))
}

func TestDispatcher_ProvenStreakSendsOnTheFirstReadyTick(t *testing.T) {
	prod := &fakeProducer{}
	d, mock, now := newTestDispatcher(t, running([]string{`public\.users`}), prod)
	d.Hurry("r1", RunningProof{Since: now.Add(-12 * time.Second), At: now.Add(-2 * time.Second)})

	mock.ExpectQuery(`FROM cdc_snapshot_requests`).WillReturnRows(queuedRow(false))
	expectSend(mock)
	if err := d.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(prod.sent) != 1 {
		t.Fatalf("a Reload whose connector was just watched RUNNING for ReadyStable waited another ReadyStable: sent %d signals on the first Ready tick, want 1", len(prod.sent))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDispatcher_ProofNeverOverridesANotReadyReading(t *testing.T) {
	prod := &fakeProducer{}
	// The running task does not capture the table yet.
	d, mock, now := newTestDispatcher(t, running([]string{`public\.orders`}), prod)
	d.Hurry("r1", RunningProof{Since: now.Add(-12 * time.Second), At: now.Add(-time.Second)})

	mock.ExpectQuery(`FROM cdc_snapshot_requests`).WillReturnRows(queuedRow(false))
	if err := d.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(prod.sent) != 0 {
		t.Fatal("sent while the running task does not capture the table")
	}
	if _, kept := d.proofs["r1"]; kept {
		t.Fatal("a not-Ready reading must drop the proof: the connector may have restarted")
	}

	// Ready now, but the proof is gone: the normal ReadyStable streak applies.
	d.connect.(*fakeConnect).state = running([]string{`public\.users`})
	mock.ExpectQuery(`FROM cdc_snapshot_requests`).WillReturnRows(queuedRow(false))
	if err := d.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(prod.sent) != 0 {
		t.Fatal("sent without a ReadyStable streak after the proof was dropped")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDispatcher_ProofThatDoesNotCoverReadyStableIsIgnored(t *testing.T) {
	cases := map[string]func(now time.Time) (string, RunningProof){
		"streak shorter than ReadyStable": func(now time.Time) (string, RunningProof) {
			return "r1", RunningProof{Since: now.Add(-5 * time.Second), At: now.Add(-time.Second)}
		},
		"streak ended more than ReadyStable ago": func(now time.Time) (string, RunningProof) {
			return "r1", RunningProof{Since: now.Add(-40 * time.Second), At: now.Add(-11 * time.Second)}
		},
		"proof of another request": func(now time.Time) (string, RunningProof) {
			return "r2", RunningProof{Since: now.Add(-12 * time.Second), At: now.Add(-time.Second)}
		},
		"zero proof (start check timed out)": func(time.Time) (string, RunningProof) {
			return "r1", RunningProof{}
		},
	}
	for name, proof := range cases {
		t.Run(name, func(t *testing.T) {
			prod := &fakeProducer{}
			d, mock, now := newTestDispatcher(t, running([]string{`public\.users`}), prod)
			d.Hurry(proof(*now))
			mock.ExpectQuery(`FROM cdc_snapshot_requests`).WillReturnRows(queuedRow(false))
			if err := d.Tick(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(prod.sent) != 0 {
				t.Fatal("sent on the first Ready tick without a proof covering ReadyStable")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Hurry wakes a running dispatcher: the request is looked at now, not on the
// next tick.
func TestDispatcher_HurryRunsATickNow(t *testing.T) {
	d, mock, _ := newTestDispatcher(t, running(nil), &fakeProducer{})
	d.tick = time.Hour
	d.now = time.Now
	listed := make(chan struct{})
	mock.ExpectQuery(`FROM cdc_snapshot_requests`).WillReturnRows(sqlmock.NewRows(requestCols))
	d.Start()
	t.Cleanup(d.Stop)

	go func() {
		for mock.ExpectationsWereMet() != nil {
			time.Sleep(5 * time.Millisecond)
		}
		close(listed)
	}()
	d.Hurry("r1", RunningProof{})
	select {
	case <-listed:
	case <-time.After(2 * time.Second):
		t.Fatal("Hurry did not run a tick; the request would wait for the next interval")
	}
}

func TestDispatcher_HurryAndKickAreSafeOnNil(t *testing.T) {
	var d *Dispatcher
	d.Hurry("r1", RunningProof{})
	d.Kick()
}
