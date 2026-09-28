package main

// KI-CDC-SINK-AUTH-MISCLASSIFIED-AS-INFRA.
//
// Bug class: "a destination that is reachable and refuses the credential is
// classified as something else" — as an outage (libpq prefixes a refused password
// with the infra marker "connection to server at", so it was held for the whole
// 300 s budget) or as unclassified (a 401/403/permission-denied reply, which was
// dead-lettered and its offset committed). These tests pin the classification
// for every destination driver's wording, and the branch in each condemn path: a
// refused credential fails closed AT ONCE through failClosedOnDestAuth — no hold,
// no DLQ, no commit.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// destAuthRefusalsByDriver is one real wording per destination driver. Adding a
// destination whose driver words a refusal differently means adding it here.
var destAuthRefusalsByDriver = map[string]string{
	// The exact last_error captured on the 2026-09-01 gate run.
	"postgresql (libpq, wrapped in the infra marker)": `dest error: Database connection failed: connection to server at "rsync-ci-postgres-e2e" (172.18.0.5), port 5432 failed: FATAL:  password authentication failed for user "e2e_user"`,
	"postgresql pg_hba":        `dest error: connection to server at "pg" (10.0.0.3), port 5432 failed: FATAL:  no pg_hba.conf entry for host "10.0.0.9", user "rsync", database "dw", no encryption`,
	"postgresql grant":         `dest error: permission denied for table orders`,
	"postgresql psycopg class": `dest error: psycopg.errors.InsufficientPrivilege: permission denied for schema analytics`,
	"mysql":                    `dest error: (1045, "Access denied for user 'rsync'@'10.0.0.9' (using password: YES)")`,
	"mysql grant":              `dest error: (1142, "INSERT command denied to user 'rsync'@'%' for table 'orders'")`,
	"sqlserver":                `dest error: ('28000', "[28000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Login failed for user 'rsync'. (18456)")`,
	"oracle":                   `dest error: ORA-01017: invalid username/password; logon denied`,
	"snowflake":                `dest error: 250001 (08001): Failed to connect to DB: xy12345.snowflakecomputing.com:443. Incorrect username or password was specified.`,
	"mongodb":                  `dest error: Authentication failed., full error: {'ok': 0.0, 'errmsg': 'Authentication failed.', 'code': 18, 'codeName': 'AuthenticationFailed'}`,
	"mongodb grant":            `dest error: not authorized on dw to execute command { insert: "orders" }`,
	"clickhouse":               `dest error: Code: 516. DB::Exception: rsync: Authentication failed: password is incorrect, or there is no user with such name. (AUTHENTICATION_FAILED)`,
	"aws-s3 key":               `dest error: An error occurred (InvalidAccessKeyId) when calling the PutObject operation: The AWS Access Key Id you provided does not exist in our records.`,
	"aws-s3 grant":             `dest error: An error occurred (AccessDenied) when calling the PutObject operation: Access Denied`,
	"aws-s3 signature":         `dest error: An error occurred (SignatureDoesNotMatch) when calling the PutObject operation`,
	"gcs":                      `dest error: 403 POST https://storage.googleapis.com/upload/storage/v1/b/bkt/o: rsync@proj.iam.gserviceaccount.com does not have storage.objects.create access to the Google Cloud Storage object.`,
	"bigquery":                 `dest error: 403 Access Denied: Table proj:ds.orders: Permission bigquery.tables.updateData denied on table proj:ds.orders (or it may not exist).`,
	"google oauth":             `dest error: ('invalid_grant: Invalid JWT Signature.', {'error': 'invalid_grant'})`,
	"azure-blob":               `dest error: Server failed to authenticate the request. ErrorCode:AuthenticationFailed`,
	"azure-blob grant":         `dest error: This request is not authorized to perform this operation using this permission. ErrorCode:AuthorizationPermissionMismatch`,
	"http connector 401":       `dest error: HTTP 401 Unauthorized`,
	"http connector 403":       `dest error: request failed with status code 403`,
}

func TestClassifyDestFault_EveryDriversCredentialRefusalIsAuth(t *testing.T) {
	for driver, e := range destAuthRefusalsByDriver {
		if got := classifyDestFault(errors.New(e)); got != faultAuth {
			t.Errorf("%s: classifyDestFault(%q) = %s, want auth — a refused credential must fail closed at once, not be held as an outage or dead-lettered", driver, e, got)
		}
		if isDestInfraFault(errors.New(e)) {
			t.Errorf("%s: isDestInfraFault must be false for a refused credential — it would spend the 300 s infra budget", driver)
		}
	}
}

func TestClassifyDestFault_GenuineOutagesStayInfraNotAuth(t *testing.T) {
	// The auth list is checked before the infra list, so it must not swallow a real
	// outage — including libpq's own "connection to server at" prefix on a refused
	// TCP connection, which is what #777 made fail closed.
	for _, e := range []string{
		`dest error: connection to server at "pg-dest" (10.1.2.3), port 5432 failed: Connection refused`,
		`dest error: connection to server at "pg-dest" (10.1.2.3), port 5432 failed: timeout expired i/o timeout`,
		`Post "http://postgresql-mcp:8000/mcp": dial tcp: lookup postgresql-mcp on 127.0.0.11:53: no such host`,
		`dest error: 503 Service Unavailable`,
		`dest error: FATAL: the database system is starting up`,
	} {
		if got := classifyDestFault(errors.New(e)); got != faultInfra {
			t.Errorf("classifyDestFault(%q) = %s, want infra", e, got)
		}
	}
}

func TestClassifyDestFault_DataVetoStillBeatsAuthWords(t *testing.T) {
	// A poison row whose VALUE carries an auth word stays a poison row.
	e := `dest error: duplicate key value violates unique constraint "orders_pkey" -- note='access denied'`
	if got := classifyDestFault(errors.New(e)); got != faultData {
		t.Errorf("classifyDestFault(%q) = %s, want data", e, got)
	}
}

// --- the branches ---

type authFailClosedPanic struct{ msg string }

// captureAuthFailClosed swaps both halts, so a test sees WHICH one fired. A
// generic sinkFailClosed on a refused credential (the infra budget, or a DLQ
// publish error) is itself the bug.
func captureAuthFailClosed(t *testing.T) (auth *string, generic *string) {
	t.Helper()
	generic = captureFailClosed(t)
	prev := sinkFailClosedAuth
	var got string
	sinkFailClosedAuth = func(format string, args ...interface{}) {
		got = fmt.Sprintf(format, args...)
		panic(authFailClosedPanic{msg: got})
	}
	t.Cleanup(func() { sinkFailClosedAuth = prev })
	return &got, generic
}

// scriptedDest answers each callDestinationTool invocation with the next scripted
// error (the last one repeats). Only write tools consume the script; the capability
// probe gets a plain refusal. An invocation walks destinationHostCandidates only
// past a host that did not answer, so its request count depends on the answer:
// every invocation starts at the first candidate, and that host is what advances
// the script.
type scriptedDest struct {
	errs []string

	mu    sync.Mutex
	first string // host of the first write request = every invocation's first candidate
	calls int
}

func (d *scriptedDest) RoundTrip(r *http.Request) (*http.Response, error) {
	raw, _ := io.ReadAll(r.Body)
	if strings.Contains(string(raw), "_get_capabilities") {
		body, _ := json.Marshal(map[string]interface{}{"success": false, "error": "unknown tool"})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(body)), Header: http.Header{}}, nil
	}
	d.mu.Lock()
	if d.first == "" {
		d.first = r.URL.Hostname()
	}
	if r.URL.Hostname() == d.first {
		d.calls++
	}
	n := d.calls - 1
	d.mu.Unlock()
	if n >= len(d.errs) {
		n = len(d.errs) - 1
	}
	msg := d.errs[n]
	if strings.HasPrefix(msg, "transport:") {
		return nil, errors.New(strings.TrimPrefix(msg, "transport:"))
	}
	body, _ := json.Marshal(map[string]interface{}{"success": false, "error": msg})
	return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(body)), Header: http.Header{}}, nil
}

// runFlushExpectingHalt runs flushBatch and returns which halt fired.
func runFlushExpectingHalt(t *testing.T, b *cdcDBBatcher, key string) (which string) {
	t.Helper()
	defer func() {
		switch r := recover().(type) {
		case nil:
			t.Fatal("flushBatch returned without failing closed — the batch was condemned to the DLQ and committed")
		case authFailClosedPanic:
			which = "auth"
		case failClosedPanic:
			which = "generic"
		default:
			panic(r)
		}
	}()
	b.flushBatch(context.Background(), key, b.batches[key], "test_flush")
	return ""
}

func assertNothingLost(t *testing.T, b *cdcDBBatcher) {
	t.Helper()
	if n := atomic.LoadUint64(&b.metrics.dlqRouted); n != 0 {
		t.Errorf("dlqRouted = %d, want 0 — a row refused for its credential is not poison", n)
	}
	if n := atomic.LoadInt64(&b.metrics.lastCommittedOffset); n != 0 {
		t.Errorf("lastCommittedOffset = %d, want 0 — the offsets must stay uncommitted so Kafka redelivers", n)
	}
}

func TestFlushBatch_RefusedCredentialFailsClosedAtOnceWithoutTheInfraHold(t *testing.T) {
	// The default 300 s budget is left in force on purpose: before the fix the
	// libpq wording was infra and this test would sit in the hold.
	t.Setenv("RSYNC_SINK_INFRA_RETRY_SECONDS", "30")
	auth, generic := captureAuthFailClosed(t)
	b, key, _ := unreachableDBBatcher(t)
	dest := &scriptedDest{errs: []string{destAuthRefusalsByDriver["postgresql (libpq, wrapped in the infra marker)"]}}
	b.httpClient = &http.Client{Transport: dest, Timeout: time.Second}

	start := time.Now()
	if which := runFlushExpectingHalt(t, b, key); which != "auth" {
		t.Fatalf("halt = %s (%q), want the auth halt — a refused password was treated as an outage", which, *generic)
	}
	if el := time.Since(start); el > 1500*time.Millisecond {
		t.Errorf("auth halt took %s — it must not spend the infrastructure-fault hold", el)
	}
	assertNothingLost(t, b)
	for _, want := range []string{"authentication failed", "needs user config", "offsets NOT committed", "NO rows dead-lettered"} {
		if !strings.Contains(*auth, want) {
			t.Errorf("auth halt log %q does not say %q", *auth, want)
		}
	}
}

func TestFlushBatch_CredentialRefusedDuringPerRowIsolationIsNotDeadLettered(t *testing.T) {
	// Whole batch fails on a poison row → per-row isolation → the destination then
	// refuses the credential. Before the fix a "permission denied for table" was
	// unclassified there and went to the DLQ with its offset committed.
	t.Setenv("RSYNC_SINK_INFRA_RETRY_SECONDS", "0")
	_, generic := captureAuthFailClosed(t)
	b, key, batch := unreachableDBBatcher(t)
	batch.rows = append(batch.rows, map[string]interface{}{"id": 2, "total": 20})
	batch.messages = append(batch.messages, batch.messages[0])
	batch.messages[1].Offset = 42
	batch.sms = append(batch.sms, batch.sms[0])
	dest := &scriptedDest{errs: []string{
		`dest error: new row for relation "orders" violates check constraint "orders_total_positive"`,
		`dest error: permission denied for table orders`,
	}}
	b.httpClient = &http.Client{Transport: dest, Timeout: time.Second}

	if which := runFlushExpectingHalt(t, b, key); which != "auth" {
		t.Fatalf("halt = %s (%q), want the auth halt — a refused grant reached the DLQ path", which, *generic)
	}
	assertNothingLost(t, b)
}

func TestFlushBatch_CredentialRefusedWhenAnOutageEndsIsNotDeadLettered(t *testing.T) {
	// Outage → hold → the destination comes back and refuses the (rotated)
	// credential. holdForInfraFault stops early on a non-infra answer; the caller
	// must then take the auth halt, not fall through to per-row isolation / DLQ.
	t.Setenv("RSYNC_SINK_INFRA_RETRY_SECONDS", "30")
	_, generic := captureAuthFailClosed(t)
	b, key, _ := unreachableDBBatcher(t)
	dest := &scriptedDest{errs: []string{
		"transport:dial tcp 10.0.0.7:8000: connect: connection refused",
		`dest error: Authentication failed.`,
	}}
	b.httpClient = &http.Client{Transport: dest, Timeout: time.Second}

	if which := runFlushExpectingHalt(t, b, key); which != "auth" {
		t.Fatalf("halt = %s (%q), want the auth halt after the hold ended on a refused credential", which, *generic)
	}
	assertNothingLost(t, b)
}
