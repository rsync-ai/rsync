package main

// KI-CDC-SINK-INFRA-FAULT-DLQ-COMMITS.
//
// The defect: `cdcDBBatcher.flushBatch` could not tell a poison row from an
// unreachable destination. Both exhausted the ~7s retry budget and both ended at
// sendToDLQ + CommitMessages, so a destination restart lasting longer than the
// budget dead-lettered live CDC rows and committed their offsets — Kafka never
// redelivers them, and the DLQ topic has no reachable consumer. The worker stayed
// up and the pipeline stayed green.
//
// These tests pin the two halves of the fix that can be proven without a broker:
// the classification (which errors change behavior, and which deliberately do not),
// and the branch itself — an unreachable destination must reach the fail-closed
// exit with NOTHING dead-lettered and the batch still held.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestClassifyDestFault_InfraShapesTheDestinationActuallyProduces(t *testing.T) {
	// Every one of these is a destination that never saw the row. Dead-lettering
	// any of them and committing the offset is the silent data loss.
	infra := []string{
		// The MCP connector container is not answering on :8000.
		`Post "http://rsync-ai-postgresql-v1-0-0-mcp:8000/mcp": dial tcp 172.19.0.7:8000: connect: connection refused`,
		`Post "http://postgresql-mcp:8000/mcp": dial tcp: lookup postgresql-mcp on 127.0.0.11:53: no such host`,
		`Post "http://postgresql-mcp:8000/mcp": EOF`,
		`Post "http://postgresql-mcp:8000/mcp": read tcp 10.0.0.2->10.0.0.9:8000: read: connection reset by peer`,
		`Post "http://postgresql-mcp:8000/mcp": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`,
		`destination tool call failed`,
		// The connector answered, relaying a driver error from the database behind it.
		`dest error: connection to server at "pg-dest" (10.1.2.3), port 5432 failed: Connection refused`,
		`dest error: could not translate host name "pg-dest" to address: Name or service not known`,
		`dest error: server closed the connection unexpectedly`,
		`dest error: FATAL: the database system is starting up`,
		`dest error: FATAL: terminating connection due to administrator command`,
		`dest error: FATAL: sorry, too many clients already`,
		`dest error: (2003, "Can't connect to MySQL server on 'mysql-dest' (111)")`,
		`dest error: (2006, 'MySQL server has gone away')`,
		// A proxy or sidecar answered instead of the connector.
		`dest error: 502 Bad Gateway`,
		`dest error: 503 Service Unavailable`,
		// The row is fine; the transaction lost a race. Condemning it to the DLQ
		// would discard a perfectly writable record.
		`dest error: deadlock detected`,
		`dest error: could not serialize access due to concurrent update`,
		`dest error: Lock wait timeout exceeded; try restarting transaction`,
	}
	for _, e := range infra {
		if got := classifyDestFault(errors.New(e)); got != faultInfra {
			t.Errorf("classifyDestFault(%q) = %s, want infra — this error would be dead-lettered and its offset committed", e, got)
		}
	}
}

func TestClassifyDestFault_DNSFailuresOtherThanNXDOMAINAreStillOutages(t *testing.T) {
	// The gap this closes, caught on CI 2026-09-06: the resolver answered
	// SERVFAIL rather than NXDOMAIN, so the text read "server misbehaving"
	// instead of "no such host". classifyDestFault returned unclassified, and a
	// destination that was never reached had its batch condemned to the DLQ with
	// the offsets committed -- the exact silent loss this file exists to prevent.
	//
	// The cases below are not the one wording that was observed. They are every
	// error the Go resolver itself defines (the var block in
	// net/dnsclient_unix.go, plus errNoSuchHost covered above) and the glibc
	// getaddrinfo wording for EAI_AGAIN, so the class is closed against the
	// resolver rather than patched against the failure that happened to be seen.
	dns := []string{
		`Post "http://rsync-ai-postgresql-mcp:8000/mcp": dial tcp: lookup rsync-ai-postgresql-mcp on 127.0.0.53:53: server misbehaving`,
		`Post "http://postgresql-mcp:8000/mcp": dial tcp: lookup postgresql-mcp on 10.96.0.10:53: no answer from DNS server`,
		`Post "http://postgresql-mcp:8000/mcp": dial tcp: lookup postgresql-mcp on 10.96.0.10:53: lame referral`,
		`Post "http://postgresql-mcp:8000/mcp": dial tcp: lookup postgresql-mcp on 10.96.0.10:53: invalid DNS response`,
		`Post "http://postgresql-mcp:8000/mcp": dial tcp: lookup postgresql-mcp on 10.96.0.10:53: cannot unmarshal DNS message`,
		`Post "http://postgresql-mcp:8000/mcp": dial tcp: lookup postgresql-mcp on 10.96.0.10:53: cannot marshal DNS message`,
		`dest error: [Errno -3] Temporary failure in name resolution`,
	}
	for _, e := range dns {
		if got := classifyDestFault(errors.New(e)); got != faultInfra {
			t.Errorf("classifyDestFault(%q) = %s, want infra -- a resolver failing in any wording but NXDOMAIN still means the row never reached a destination that could judge it", e, got)
		}
	}
}

func TestClassifyDestFault_DataFaultsStillReachTheDLQ(t *testing.T) {
	// The DLQ exists for exactly these. Reclassifying any of them as infra would
	// crash-loop the pipeline on a single bad row — the trap poisonError exists to
	// avoid (KI-SINK-KEYLESS-DELETE-CRASHLOOP).
	data := []string{
		`dest error: duplicate key value violates unique constraint "orders_pkey"`,
		`dest error: null value in column "total" violates not-null constraint`,
		`dest error: insert or update on table "orders" violates foreign key constraint "orders_customer_fkey"`,
		`dest error: new row for relation "orders" violates check constraint "orders_total_positive"`,
		`dest error: invalid input syntax for type integer: "n/a"`,
		`dest error: value too long for type character varying(8)`,
		`dest error: date/time field value out of range: "0000-00-00"`,
		`dest error: numeric field overflow`,
		`dest error: psycopg2.errors.UniqueViolation`,
		`dest error: psycopg.errors.StringDataRightTruncation`,
		`dest error: (1062, "Duplicate entry '7' for key 'PRIMARY'")`,
		`dest error: (1406, "Data too long for column 'name' at row 1")`,
		`dest error: (1264, "Out of range value for column 'qty' at row 1")`,
		`dest error: Unknown column 'shipped_at' in 'field list'`,
	}
	for _, e := range data {
		if got := classifyDestFault(errors.New(e)); got != faultData {
			t.Errorf("classifyDestFault(%q) = %s, want data — a poison row must still be dead-lettered, not crash-loop the pipeline", e, got)
		}
	}
}

func TestClassifyDestFault_DataVetoBeatsAnIncidentalNetworkWord(t *testing.T) {
	// The whole reason the data list is consulted first. A constraint violation
	// whose text happens to carry a transport word must stay a poison row: a false
	// infra verdict turns one bad row into an unbounded crash-loop.
	cases := []string{
		`dest error: duplicate key value violates unique constraint "conn_refused_idx"`,
		`dest error: invalid input syntax for type inet: "connection refused"`,
		`dest error: (1406, "Data too long for column 'last_error' at row 1") -- value was 'i/o timeout'`,
		`dest error: null value in column "deadlock detected" violates not-null constraint`,
	}
	for _, e := range cases {
		if got := classifyDestFault(errors.New(e)); got != faultData {
			t.Errorf("classifyDestFault(%q) = %s, want data — the data list must be checked before the infra list", e, got)
		}
	}
}

func TestClassifyDestFault_UnknownKeepsThePreExistingBehavior(t *testing.T) {
	// Deliberately conservative: an error this file cannot place keeps the old
	// DLQ + commit path rather than crash-looping. The counter is what keeps that
	// choice honest — see logUnclassifiedCondemn.
	for _, e := range []error{
		nil,
		errors.New("dest error: something nobody has seen before"),
		errors.New(`dest response not json: invalid character '<' looking for beginning of value`),
	} {
		if got := classifyDestFault(e); got != faultUnclassified {
			t.Errorf("classifyDestFault(%v) = %s, want unclassified", e, got)
		}
		if isDestInfraFault(e) {
			t.Errorf("isDestInfraFault(%v) must be false — only the allowlist changes behavior", e)
		}
	}
}

func TestBareEOF_DoesNotSweepUpArbitraryText(t *testing.T) {
	if !bareEOF("eof") || !bareEOF(`post "http://x:8000/mcp": eof`) {
		t.Error("a transport EOF must be recognised — it is what a connector killed mid-request produces")
	}
	if bareEOF("dest error: column 'eofdate' does not accept nulls") {
		t.Error("substring matching on eof would misclassify row errors as outages")
	}
}

func TestInfraRetryBudget_ClampsLikeTheStallWatchdog(t *testing.T) {
	cases := []struct {
		set  string
		want time.Duration
	}{
		{"", defaultInfraRetrySeconds * time.Second},
		{"600", 600 * time.Second},
		{"0", 0},  // explicit opt-out: fail closed immediately
		{"-5", 0}, // same
		{"abc", defaultInfraRetrySeconds * time.Second},
		// Below RAPID_RESTART_WINDOW_SECONDS every fail-closed exit would count as a
		// rapid restart and trip the supervisor's crash-loop breaker.
		{"5", minInfraRetrySeconds * time.Second},
		{"99999", maxInfraRetrySeconds * time.Second},
	}
	for _, c := range cases {
		t.Setenv("RSYNC_SINK_INFRA_RETRY_SECONDS", c.set)
		if got := infraRetryBudget(); got != c.want {
			t.Errorf("RSYNC_SINK_INFRA_RETRY_SECONDS=%q: got %s want %s", c.set, got, c.want)
		}
	}
}

func TestInfraRetrySchedule_SpendsExactlyTheBudget(t *testing.T) {
	if got := infraRetrySchedule(0); got != nil {
		t.Errorf("a zero budget must produce no sleeps, got %v", got)
	}
	for _, budget := range []time.Duration{30 * time.Second, 300 * time.Second, time.Hour} {
		sched := infraRetrySchedule(budget)
		var total time.Duration
		for _, s := range sched {
			if s <= 0 {
				t.Fatalf("budget %s produced a non-positive sleep %s", budget, s)
			}
			if s > infraRetryMaxSleep {
				t.Errorf("budget %s: sleep %s exceeds the %s cap — long sleeps delay recovery once the destination returns", budget, s, infraRetryMaxSleep)
			}
			total += s
		}
		if total != budget {
			t.Errorf("budget %s: schedule totals %s, want exactly the budget", budget, total)
		}
		if len(sched) < 2 {
			t.Errorf("budget %s produced only %d probe(s); the hold must re-probe repeatedly", budget, len(sched))
		}
	}
	// The first probes must be quick, so a two-second blip is not paid for at the cap.
	if first := infraRetrySchedule(300 * time.Second)[0]; first != infraRetryFirstSleep {
		t.Errorf("first sleep = %s, want %s", first, infraRetryFirstSleep)
	}
}

func TestHoldForInfraFault_StopsEarlyWhenTheDestinationNamesARowFault(t *testing.T) {
	t.Setenv("RSYNC_SINK_INFRA_RETRY_SECONDS", "30")
	calls := 0
	err, landed := holdForInfraFault(context.Background(), "test", "public.orders",
		errors.New("connection refused"), func() error {
			calls++
			return errors.New(`dest error: duplicate key value violates unique constraint "orders_pkey"`)
		})
	if landed {
		t.Fatal("a row fault is not a landed write")
	}
	if calls != 1 {
		t.Errorf("attempts = %d, want 1 — once the destination answers with a row fault the hold must stop, not burn the budget", calls)
	}
	if isDestInfraFault(err) {
		t.Error("the returned error must be the row fault, so the caller falls through to its DLQ path")
	}
}

func TestHoldForInfraFault_ReturnsLandedWhenTheDestinationComesBack(t *testing.T) {
	t.Setenv("RSYNC_SINK_INFRA_RETRY_SECONDS", "30")
	calls := 0
	err, landed := holdForInfraFault(context.Background(), "test", "public.orders",
		errors.New("connection refused"), func() error {
			calls++
			if calls < 2 {
				return errors.New("dial tcp: connect: connection refused")
			}
			return nil
		})
	if !landed || err != nil {
		t.Fatalf("recovery not reported: landed=%v err=%v", landed, err)
	}
}

func TestHoldForInfraFault_CancelledContextIsNotAnOutage(t *testing.T) {
	t.Setenv("RSYNC_SINK_INFRA_RETRY_SECONDS", "3600")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	_, landed := holdForInfraFault(ctx, "test", "public.orders", errors.New("connection refused"), func() error {
		t.Fatal("a cancelled context must not re-attempt the write")
		return nil
	})
	if landed {
		t.Error("a cancelled hold did not land anything")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("shutdown blocked for %s — the hold must abandon its budget on cancel", elapsed)
	}
}

// --- the branch itself ---

// failClosedPanic is the sentinel a swapped-in sinkFailClosed panics with, so a
// test can observe the fail-closed exit that os.Exit makes unobservable.
type failClosedPanic struct{ msg string }

// captureFailClosed swaps sinkFailClosed for the duration of the test.
func captureFailClosed(t *testing.T) *string {
	t.Helper()
	prev := sinkFailClosed
	var got string
	sinkFailClosed = func(format string, args ...interface{}) {
		got = fmt.Sprintf(format, args...)
		panic(failClosedPanic{msg: got})
	}
	t.Cleanup(func() { sinkFailClosed = prev })
	return &got
}

// unreachableDBBatcher builds a batcher whose destination cannot be reached, with
// a DLQ configured — the exact configuration in which the bug lost data, since
// "configuring a DLQ is what converts a safe crash into silent loss".
func unreachableDBBatcher(t *testing.T) (*cdcDBBatcher, string, *cdcDBBatch) {
	t.Helper()
	metrics := &Metrics{}
	b := &cdcDBBatcher{
		cfg:      &WorkerConfig{PipelineID: "p-infra-test"},
		destType: "postgresql",
		destCfg:  map[string]interface{}{"host": "nonexistent.invalid"},
		params:   cdcBatchingParams{maxRetries: 0, backoff: time.Millisecond},
		// Non-nil: the pre-fix code would have parked every message here and
		// committed the offsets. Nothing must reach it.
		dlqWriter:  &kafka.Writer{},
		httpClient: &http.Client{Timeout: 150 * time.Millisecond},
		ddl:        &DDLSupport{},
		metrics:    metrics,
		cdcInserts: &sync.Map{},
		cdcUpdates: &sync.Map{},
		cdcDeletes: &sync.Map{},
		cdcBytes:   &sync.Map{},
		batches:    map[string]*cdcDBBatch{},
	}
	batch := &cdcDBBatch{
		table:       "public.orders",
		targetTable: "orders",
		topic:       "cdc.public.orders",
		partition:   0,
		rows:        []map[string]interface{}{{"id": 1, "total": 10}},
		messages:    []kafka.Message{{Topic: "cdc.public.orders", Partition: 0, Offset: 41}},
		sms:         []*SinkMessage{{PipelineID: "p-infra-test", Table: "public.orders", CDCOp: "c"}},
		keyFields:   []string{"id"},
	}
	key := "cdc.public.orders|0|public.orders"
	b.batches[key] = batch
	return b, key, batch
}

func TestFlushBatch_UnreachableDestinationFailsClosedInsteadOfDLQCommitting(t *testing.T) {
	// Budget 0 keeps the test fast; the hold itself is covered above. What is under
	// test here is the branch: an unreachable destination must not reach
	// sendToDLQ + CommitMessages.
	t.Setenv("RSYNC_SINK_INFRA_RETRY_SECONDS", "0")
	got := captureFailClosed(t)
	b, key, _ := unreachableDBBatcher(t)

	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("flushBatch returned without failing closed — an unreachable destination was condemned to the DLQ and its offsets committed (the P0)")
			}
			if _, ok := r.(failClosedPanic); !ok {
				panic(r)
			}
		}()
		b.flushBatch(context.Background(), key, b.batches[key], "test_flush")
	}()

	if n := atomic.LoadUint64(&b.metrics.dlqRouted); n != 0 {
		t.Errorf("dlqRouted = %d, want 0 — rows the destination never saw must not be dead-lettered", n)
	}
	if _, still := b.batches[key]; !still {
		t.Error("the batch was discarded; failing closed must leave it held for redelivery")
	}
	for _, want := range []string{"failing closed", "offsets NOT committed", "NO rows dead-lettered", "redelivers"} {
		if !strings.Contains(*got, want) {
			t.Errorf("fail-closed log %q does not say %q — an operator reading it must know nothing was lost", *got, want)
		}
	}
	if gUnclassifiedCondemns.Load() != 0 {
		t.Error("an unreachable destination is a classified infra fault, not an unclassified condemnation")
	}
}

func TestFlushBatch_NoDLQStillFailsClosed(t *testing.T) {
	// The pre-existing no-DLQ behavior must be untouched: still a crash, still no
	// commit. This is the control that shows the new gate did not change it.
	t.Setenv("RSYNC_SINK_INFRA_RETRY_SECONDS", "0")
	captureFailClosed(t)
	b, key, _ := unreachableDBBatcher(t)
	b.dlqWriter = nil

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("a flush with no DLQ must fail closed")
		} else if _, ok := r.(failClosedPanic); !ok {
			panic(r)
		}
	}()
	b.flushBatch(context.Background(), key, b.batches[key], "test_flush")
}
