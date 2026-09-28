package main

// Destination-fault classification for the CDC apply path.
//
// KI-CDC-SINK-INFRA-FAULT-DLQ-COMMITS. The DLQ exists for a POISON ROW: a value
// the destination examined and refused (constraint violation, out-of-range
// temporal, over-precision numeric). Parking that row and committing its offset
// is correct — redelivering it would fail identically forever.
//
// An INFRASTRUCTURE FAULT is the opposite situation wearing the same clothes.
// The destination never saw the row: the connector is unreachable, the database
// behind it is restarting, DNS is down, the TCP connection was reset. Before
// this file, `cdcDBBatcher.flushBatch` could not tell the two apart — it
// exhausted `maxRetries` (~7s at the defaults) and condemned `lastErr`
// whatever it was, sending every message to the DLQ and committing the offsets.
// Kafka then never redelivers them and the only copy sits in a DLQ topic with
// no reachable consumer, while the worker stays up and the pipeline stays
// green. A destination restart lasting longer than the retry budget silently
// lost the stream. `flushBatchPerRow` made it worse: during an outage every row
// looks individually poisonous, so per-row isolation converted a whole-batch
// infrastructure error into a whole-batch DLQ.
//
// The sibling `cdcObjectBatcher.flushBatch` already made the opposite call on
// the identical fault — "Fail-closed: do NOT commit offsets when flush fails" —
// so the same worker behaved differently depending only on whether the
// destination was a bucket or a database, and configuring a DLQ is what turned
// a safe crash into silent loss.
//
// The verdict here is deliberately ASYMMETRIC, and it is not a general-purpose
// error taxonomy:
//
//   - Only errors that MATCH the infrastructure allowlist change behavior. They
//     get an extended retry and then fail closed — no DLQ, no commit, exit, and
//     let the supervisor respawn into the uncommitted offset.
//   - Everything else keeps the pre-existing DLQ + commit behavior. Widening
//     fail-closed to unclassified errors would be the safer invariant, but it
//     would also crash-loop the whole pipeline on any un-enumerated poison shape
//     — the trap `poisonError` was introduced to avoid (KI-SINK-KEYLESS-DELETE-
//     CRASHLOOP). Unclassified condemnations are logged as such (see
//     logUnclassifiedCondemn) so the residual tail is observable instead of
//     silent.
//   - The data-fault list is checked FIRST and vetoes an infrastructure verdict.
//     A constraint violation whose text happens to contain "connection" or
//     "timed out" must stay a poison row; a false infrastructure verdict is the
//     crash-loop trap above.
//
// The marker set mirrors `pkg/diagnose`'s transient network/transport rule
// (backend-orchestrator/pkg/diagnose/diagnose.go) — the same taxonomy the CDC
// healer classification rule in CLAUDE.md requires — extended with the shapes
// this worker actually sees: an MCP connector that does not answer on :8000,
// and driver text relayed through `dest error: ...`.

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// destFault is the disposition of a destination write that has exhausted its
// normal retry budget.
type destFault int

const (
	// faultUnclassified — cannot tell. Preserves the pre-existing DLQ + commit
	// behavior; counted and logged so the tail is measurable.
	faultUnclassified destFault = iota
	// faultData — the destination examined the row and refused it. DLQ + commit
	// is correct: this is what the DLQ is for.
	faultData
	// faultInfra — the write never landed for a reason unrelated to the row.
	// Never DLQ, never commit.
	faultInfra
	// faultAuth — the destination is reachable and refused the CREDENTIAL (wrong,
	// rotated or revoked password/key/token, or a missing grant). Never DLQ, never
	// commit — the row is fine — and never hold for the infrastructure budget
	// either: a credential does not start working by waiting. Fail closed at once
	// with exitDestAuthRefused so the supervisor stops the worker and reports it as
	// needing user config. KI-CDC-SINK-AUTH-MISCLASSIFIED-AS-INFRA.
	faultAuth
)

func (f destFault) String() string {
	switch f {
	case faultData:
		return "data"
	case faultInfra:
		return "infra"
	case faultAuth:
		return "auth"
	default:
		return "unclassified"
	}
}

// destDataFaultMarkers are destination replies that name the ROW as the problem.
// Checked before the infrastructure list so they always win.
var destDataFaultMarkers = []string{
	// PostgreSQL integrity + data-exception wordings.
	"violates unique constraint",
	"violates not-null constraint",
	"violates foreign key constraint",
	"violates check constraint",
	"duplicate key value",
	"invalid input syntax",
	"value too long for type",
	"numeric field overflow",
	"date/time field value out of range",
	"integer out of range",
	"invalid byte sequence",
	"cannot be cast to",
	// psycopg error-class names, which connectors surface verbatim.
	"uniqueviolation",
	"notnullviolation",
	"foreignkeyviolation",
	"checkviolation",
	"numericvalueoutofrange",
	"invalidtextrepresentation",
	"stringdatarighttruncation",
	// MySQL wordings.
	"duplicate entry",
	"data too long for column",
	"incorrect string value",
	"incorrect integer value",
	"incorrect datetime value",
	"incorrect decimal value",
	"out of range value for column",
	"column cannot be null",
	"is violated",
	// Column the destination does not have. Additive DDL reconcile already ran;
	// if the column is still missing the row cannot be written as sent.
	"unknown column",
	"undefined column",
	"no such column",
}

// destAuthFaultMarkers are replies from a destination that is REACHABLE and
// refuses the credential or the grant, per destination driver. Checked after the
// data list and BEFORE the infrastructure list, because libpq prefixes every
// connection-phase failure — a wrong password included — with the infrastructure
// marker "connection to server at", which is how a rotated password came to be
// held for the whole 300 s outage budget (KI-CDC-SINK-AUTH-MISCLASSIFIED-AS-INFRA).
//
// A shape listed here fails closed immediately (no DLQ, no commit, no hold).
// Never move one to destDataFaultMarkers: that would dead-letter every row of a
// pipeline whose password merely needs rotating.
var destAuthFaultMarkers = []string{
	// PostgreSQL (libpq / psycopg).
	"password authentication failed",
	"no pg_hba.conf entry",
	"sasl authentication failed",
	"invalidpassword",
	"invalidauthorizationspecification",
	"insufficientprivilege",
	"permission denied for",
	"must be owner of",
	// MySQL / MariaDB.
	"access denied for user",
	"command denied to user",
	// SQL Server.
	"login failed for user",
	// Oracle: invalid username/password, insufficient privileges, account locked.
	"ora-01017",
	"ora-01031",
	"ora-28000",
	// Snowflake.
	"incorrect username or password",
	"jwt token is invalid",
	// MongoDB, ClickHouse and generic driver wordings.
	"authentication failed",
	"not authorized on",
	"not enough privileges",
	"invalid credentials",
	"invalid username or password",
	// Object stores and cloud APIs (S3 / GCS / BigQuery / Azure Blob).
	"invalidaccesskeyid",
	"signaturedoesnotmatch",
	"the aws access key id you provided does not exist",
	"security token included in the request is invalid",
	"expiredtoken",
	"accessdenied",
	"access denied",
	"authorizationfailure",
	"authorizationpermissionmismatch",
	"authenticationfailed",
	"invalid_grant",
	"request had invalid authentication credentials",
	"does not have storage.",
	"does not have bigquery.",
	// HTTP status shapes relayed by a connector or an API client.
	"http 401",
	"401 unauthorized",
	"status 401",
	"status code 401",
	"http 403",
	"403 forbidden",
	"status 403",
	"status code 403",
	"invalid token",
	"token expired",
	"token has expired",
}

// destInfraFaultMarkers are replies that mean the row never reached a
// destination that could judge it. Mirrors pkg/diagnose's transient set.
var destInfraFaultMarkers = []string{
	// Transport — the connector or the database behind it is not answering.
	"connection refused",
	"no such host",
	"name or service not known",
	// DNS answered, but not with an address. NXDOMAIN is the only resolver
	// failure whose wording appears above, and Go reports every OTHER one in a
	// wording of its own -- so a resolver outage read as unclassified, which is
	// DLQ + commit, the silent loss this file exists to prevent. Caught on CI
	// 2026-09-06 by a runner whose resolver answered SERVFAIL: `lookup
	// rsync-ai-postgresql-mcp on 127.0.0.53:53: server misbehaving`. Enumerated
	// from net/dnsclient_unix.go's own error block rather than from that one
	// wording, so the class is closed against the resolver instead of patched
	// against the instance.
	"server misbehaving",
	"no answer from dns server",
	"lame referral",
	"invalid dns response",
	"cannot unmarshal dns message",
	"cannot marshal dns message",
	// getaddrinfo EAI_AGAIN, as glibc words it -- what the cgo resolver produces
	// and what a Python connector relaying its own socket error sends back
	// through `dest error:`. The BSD/macOS wording for the same errno is the
	// bare "try again", too generic to allowlist without sweeping up unrelated
	// text, and its Linux counterpart is what actually runs in the cluster.
	"temporary failure in name resolution",
	"network is unreachable",
	"no route to host",
	"i/o timeout",
	"context deadline exceeded",
	"client.timeout exceeded",
	"timeout awaiting response headers",
	"connection reset",
	"broken pipe",
	"unexpected eof",
	"connection timed out",
	"operation timed out",
	// Peer dropped an established connection mid-stream.
	"server closed the connection unexpectedly",
	"could not translate host name",
	"could not connect to server",
	"connection to server at",
	// The destination database is cycling.
	"the database system is starting up",
	"the database system is shutting down",
	"terminating connection due to administrator command",
	"terminating connection due to unexpected postmaster exit",
	"can't connect to mysql server",
	"lost connection to mysql server",
	"mysql server has gone away",
	// Connection-pool exhaustion — slots free up.
	"too many connections",
	"remaining connection slots",
	"sorry, too many clients already",
	// A proxy or sidecar answered instead of the connector.
	"bad gateway",
	"service unavailable",
	"gateway timeout",
	// Concurrency races that resolve on retry. The ROW is fine, so condemning it
	// to the DLQ would discard a perfectly writable record.
	"deadlock detected",
	"could not serialize access",
	"deadlock found when trying to get lock",
	"lock wait timeout exceeded",
	// The worker's own wording when no candidate host answered at all
	// (callDestinationTool exhausted destinationHostCandidates).
	"destination tool call failed",
}

func matchesAnyMarker(low string, markers []string) bool {
	for _, m := range markers {
		if strings.Contains(low, m) {
			return true
		}
	}
	return false
}

// bareEOF matches a transport EOF that carries no other text — `Post
// "http://host:8000/mcp": EOF`, which is what a connector killed mid-request
// produces. Matched by suffix rather than substring so an arbitrary message
// containing the letters "eof" is not swept up.
func bareEOF(low string) bool {
	return low == "eof" || strings.HasSuffix(low, ": eof") || strings.HasSuffix(low, " eof")
}

// classifyDestFault is the whole decision, as a pure function of the error text
// so it can be unit-tested without a destination, a broker, or a batcher.
func classifyDestFault(err error) destFault {
	if err == nil {
		return faultUnclassified
	}
	low := strings.ToLower(err.Error())
	// Data faults win. See the asymmetry note at the top of the file.
	if matchesAnyMarker(low, destDataFaultMarkers) {
		return faultData
	}
	// Auth before infra: libpq wraps a refused password in the infra marker
	// "connection to server at" (KI-CDC-SINK-AUTH-MISCLASSIFIED-AS-INFRA).
	if matchesAnyMarker(low, destAuthFaultMarkers) {
		return faultAuth
	}
	if matchesAnyMarker(low, destInfraFaultMarkers) || bareEOF(low) {
		return faultInfra
	}
	return faultUnclassified
}

// isDestInfraFault is the guard the condemn paths branch on.
func isDestInfraFault(err error) bool { return classifyDestFault(err) == faultInfra }

// isDestAuthFault is the guard for a refused credential or grant.
func isDestAuthFault(err error) bool { return classifyDestFault(err) == faultAuth }

// exitDestAuthRefused is the worker's exit status when the destination refuses
// its credential: EX_CONFIG from sysexits.h. The supervisor (connector.py
// DEST_AUTH_REFUSED_EXIT_CODE — keep the two equal) reads it as "needs user
// config": it marks the worker crashed with a plain-words last_error instead of
// respawning it into the same refusal until the crash-loop breaker trips with a
// generic "crash-looped" message.
const exitDestAuthRefused = 78

// sinkFailClosedAuth halts the worker like sinkFailClosed — no commit, no DLQ,
// so Kafka redelivers once the credential is fixed — but exits with
// exitDestAuthRefused. A variable so tests can observe the branch.
var sinkFailClosedAuth = func(format string, args ...interface{}) {
	logf("error", format, args...)
	os.Exit(exitDestAuthRefused)
}

// failClosedOnDestAuth is the single wording of the auth halt, so the batch,
// per-row, single-event and object-storage paths cannot drift apart. The text
// carries "authentication failed" so the orchestrator's diagnoser (pkg/diagnose
// auth-scope rule) routes it to ActionRequestUserConfig, never to a retry.
func failClosedOnDestAuth(where, table string, err error) {
	sinkFailClosedAuth("fatal: %s: destination authentication failed or permission denied — the credential or grant was refused, needs user config: failing closed with offsets NOT committed and NO rows dead-lettered, so Kafka redelivers once the destination connection is fixed and the pipeline restarted (table=%s): %v",
		where, table, err)
}

const (
	// A destination restart takes tens of seconds; the pre-existing budget was
	// ~7s, which is why a routine restart lost data. Five minutes covers a
	// rolling restart without holding an outage open indefinitely.
	defaultInfraRetrySeconds = 300
	// Below RAPID_RESTART_WINDOW_SECONDS (30s, connector.py) each fail-closed
	// exit would count as a rapid restart and trip the supervisor's crash-loop
	// breaker, turning a recoverable outage into a dead worker.
	minInfraRetrySeconds = 30
	maxInfraRetrySeconds = 3600
	// Per-sleep ceiling, so the budget is spent on many probes rather than a few
	// long ones and the flush resumes promptly once the destination returns.
	infraRetryMaxSleep = 30 * time.Second
	// First extended sleep. Doubles up to infraRetryMaxSleep.
	infraRetryFirstSleep = 2 * time.Second
)

// infraRetryBudget is how long a CDC destination write keeps retrying an
// infrastructure fault before failing closed. Override with
// RSYNC_SINK_INFRA_RETRY_SECONDS; 0 (or negative) disables the extended retry
// and fails closed immediately, other values are clamped to
// [minInfraRetrySeconds, maxInfraRetrySeconds]. Mirrors stallWatchdogTimeout().
//
// The hold is backpressure, and the stall watchdog must not misread it as a wedge.
// Flushing inline (RSYNC_SINK_FLUSH_LANES <= 1) it blocks the consume loop, so
// lastPoll goes stale and shouldRestartForStall returns false ("blocked in message
// handling, not starved"). With flush lanes the loop keeps polling while a lane
// holds the batch; there the held records were fetched, so waitingFloor does not
// count them as waiting on the broker.
func infraRetryBudget() time.Duration {
	raw := strings.TrimSpace(os.Getenv("RSYNC_SINK_INFRA_RETRY_SECONDS"))
	if raw == "" {
		return defaultInfraRetrySeconds * time.Second
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return defaultInfraRetrySeconds * time.Second
	}
	if n <= 0 {
		return 0
	}
	if n < minInfraRetrySeconds {
		n = minInfraRetrySeconds
	}
	if n > maxInfraRetrySeconds {
		n = maxInfraRetrySeconds
	}
	return time.Duration(n) * time.Second
}

// infraRetrySchedule is the extended backoff, as a pure function of the budget,
// so the pacing is unit-testable. Sleeps double from infraRetryFirstSleep, are
// capped at infraRetryMaxSleep, and the final sleep is trimmed so the schedule
// never overruns the budget.
func infraRetrySchedule(budget time.Duration) []time.Duration {
	if budget <= 0 {
		return nil
	}
	var out []time.Duration
	var spent time.Duration
	sleep := infraRetryFirstSleep
	for spent < budget {
		s := sleep
		if spent+s > budget {
			s = budget - spent
		}
		out = append(out, s)
		spent += s
		if sleep < infraRetryMaxSleep {
			if sleep *= 2; sleep > infraRetryMaxSleep {
				sleep = infraRetryMaxSleep
			}
		}
	}
	return out
}

// gUnclassifiedCondemns counts DLQ condemnations whose error matched neither
// list. A rising count is the signal that a real outage shape is missing from
// destInfraFaultMarkers and is still being committed away.
var gUnclassifiedCondemns atomic.Uint64

// logUnclassifiedCondemn records that a message is about to be dead-lettered on
// an error this file could not classify. It is not an error on its own — it is
// the observability that keeps the conservative default honest.
func logUnclassifiedCondemn(where, table string, err error) {
	gUnclassifiedCondemns.Add(1)
	logf("warning", "unclassified destination fault condemned to DLQ (where=%s table=%s): %v — "+
		"if this is an outage shape it belongs in destInfraFaultMarkers (see KI-CDC-SINK-INFRA-FAULT-DLQ-COMMITS)",
		where, table, err)
}

// sinkFailClosed halts the worker WITHOUT committing offsets and WITHOUT
// dead-lettering, so Kafka redelivers the batch to the respawned worker.
// Indirected through a variable purely so tests can assert that a condemn path
// took this branch — os.Exit is untestable, and the assertion that matters is
// "the DLQ + commit branch was NOT reached".
var sinkFailClosed = func(format string, args ...interface{}) {
	logf("error", format, args...)
	os.Exit(1)
}

// holdForInfraFault spends the extended infrastructure-fault budget re-running
// attempt, and is the single place the hold is implemented so the batch, per-row
// and single-event condemn paths cannot drift apart.
//
// It returns the last error seen and whether the write eventually landed. It
// stops early — without spending the rest of the budget — as soon as the
// destination answers with something that is NOT an infrastructure fault, since
// that is the destination judging the row and the caller's normal DLQ path is
// then the right one. A false return whose error is still an infrastructure
// fault means the caller MUST fail closed: no dead-letter, no offset commit.
//
// The caller is responsible for checking ctx.Err() before failing closed — a
// cancelled context is a graceful shutdown, not an outage, and the correct
// action there is simply to return with the offsets uncommitted.
func holdForInfraFault(ctx context.Context, where, table string, lastErr error, attempt func() error) (error, bool) {
	budget := infraRetryBudget()
	for _, sleep := range infraRetrySchedule(budget) {
		logf("warning", "%s: destination infrastructure fault — holding offsets (nothing dead-lettered, nothing committed), retrying in %s (table=%s, budget=%s): %v",
			where, sleep, table, budget, lastErr)
		if !ctxAwareSleep(ctx, sleep) {
			return lastErr, false
		}
		err := attempt()
		if err == nil {
			logf("info", "%s: destination recovered after an infrastructure fault; batch proceeding normally (table=%s)", where, table)
			return nil, true
		}
		lastErr = err
		if !isDestInfraFault(err) {
			// The destination is answering again and now names a row-level fault.
			// Hand back to the caller's normal per-row / DLQ handling.
			return lastErr, false
		}
	}
	return lastErr, false
}
