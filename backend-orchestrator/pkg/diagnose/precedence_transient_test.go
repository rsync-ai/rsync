package diagnose

import "testing"

// TestTransientTransportErrors_BackoffRetry pins the transient transport
// signals to ActionBackoffRetry. These are genuinely retryable — a bounded
// backoff lets the peer or database recover — and the Healer caps retries at
// 3/24h, so a stuck peer eventually escalates. Contrast with cert errors,
// which are persistent config faults (see TestCertificateErrors_NotAutoRetried).
func TestTransientTransportErrors_BackoffRetry(t *testing.T) {
	d := New()
	cases := []struct {
		msg  string
		desc string
	}{
		{"read tcp 10.0.0.4:5432->10.0.0.9:54120: connection reset by peer", "connection reset by peer"},
		{"write tcp 10.0.0.4:5432: broken pipe", "broken pipe"},
		{"pq: unexpected EOF", "unexpected eof"},
		{"pq: sorry, too many clients already: too many connections open", "too many connections"},
		{"FATAL: remaining connection slots are reserved for non-replication superuser connections", "connection slots exhausted"},
		{"ERROR: deadlock detected (SQLSTATE 40P01)", "postgres deadlock"},
		{"ERROR: could not serialize access due to concurrent update", "postgres serialization failure"},
		{"Error 1213: Deadlock found when trying to get lock; try restarting transaction", "mysql deadlock"},
		{"Error 1205: Lock wait timeout exceeded; try restarting transaction", "mysql lock wait timeout"},
		{"dial tcp 10.0.0.4:5432: connect: connection timed out", "connection timed out"},
		{"server closed the connection unexpectedly", "server closed connection"},
	}
	for _, c := range cases {
		got := d.Diagnose(Signal{ErrorMessage: c.msg})
		if got.Category != CategoryNetwork {
			t.Errorf("desc=%q msg=%q: want network, got %s", c.desc, c.msg, got.Category)
		}
		if got.SuggestedAction != ActionBackoffRetry {
			t.Errorf("desc=%q msg=%q: want backoff_retry, got %s", c.desc, c.msg, got.SuggestedAction)
		}
	}
}

// TestDNSFailuresOtherThanNXDOMAIN_BackoffRetry closes the gap its sibling
// classifier had (destInfraFaultMarkers in the CDC sink worker, which this rule
// is the mirror of): the transient set named only NXDOMAIN's wording, "no such
// host", so a resolver that failed any other way -- SERVFAIL, a truncated or
// unparseable answer, a temporary getaddrinfo failure -- fell through to
// escalate instead of a bounded backoff, sending a human after an outage that
// resolves itself. The cases are every error net/dnsclient_unix.go defines,
// plus the glibc getaddrinfo wording, so the class is closed against the
// resolver rather than against the one wording that was seen in the wild.
func TestDNSFailuresOtherThanNXDOMAIN_BackoffRetry(t *testing.T) {
	d := New()
	cases := []struct {
		msg  string
		desc string
	}{
		{"dial tcp: lookup pg-dest on 127.0.0.53:53: server misbehaving", "SERVFAIL"},
		{"dial tcp: lookup pg-dest on 10.96.0.10:53: no answer from DNS server", "no answer"},
		{"dial tcp: lookup pg-dest on 10.96.0.10:53: lame referral", "lame referral"},
		{"dial tcp: lookup pg-dest on 10.96.0.10:53: invalid DNS response", "invalid response"},
		{"dial tcp: lookup pg-dest on 10.96.0.10:53: cannot unmarshal DNS message", "unparseable answer"},
		{"dial tcp: lookup pg-dest on 10.96.0.10:53: cannot marshal DNS message", "unencodable query"},
		{"could not connect: [Errno -3] Temporary failure in name resolution", "getaddrinfo EAI_AGAIN"},
	}
	for _, c := range cases {
		got := d.Diagnose(Signal{ErrorMessage: c.msg})
		if got.Category != CategoryNetwork {
			t.Errorf("desc=%q msg=%q: want network, got %s", c.desc, c.msg, got.Category)
		}
		if got.SuggestedAction != ActionBackoffRetry {
			t.Errorf("desc=%q msg=%q: want backoff_retry, got %s", c.desc, c.msg, got.SuggestedAction)
		}
	}
}

// TestCertificateErrors_NotAutoRetried is a deliberate-exclusion guard. TLS /
// x509 failures (expired cert, unknown CA, hostname mismatch) are persistent
// configuration faults: a backoff-retry loop can never fix them and would just
// burn the 3/24h cap before escalating. They must therefore NOT classify as
// transient backoff_retry — they fall through to escalate. This test locks that
// choice so a future edit can't quietly add x509/certificate to the transient
// keyword set.
func TestCertificateErrors_NotAutoRetried(t *testing.T) {
	d := New()
	cases := []string{
		"x509: certificate has expired or is not yet valid",
		"tls: failed to verify certificate: x509: certificate signed by unknown authority",
		"x509: certificate is valid for other.host, not db.internal",
	}
	for _, msg := range cases {
		got := d.Diagnose(Signal{ErrorMessage: msg})
		if got.SuggestedAction == ActionBackoffRetry {
			t.Errorf("cert error %q must NOT be backoff_retry (persistent config; retry loops forever), got %s",
				msg, got.SuggestedAction)
		}
	}
}

// TestRulePrecedence_SafetyCriticalOrdering is a golden test that locks the
// first-match ordering of Diagnose(). Dispatch is a sequential if/return chain
// (NOT a highest-confidence sort), so source order alone decides which category
// wins when a message matches more than one rule. Each case below matches
// multiple rules on purpose; the earlier (higher-priority) rule must win. This
// is the safety net for the CLAUDE.md "CDC provisioning -> Escalate, never
// retry" invariant: adding transient keywords must never let a provisioning
// failure be classified as a retryable transport blip.
func TestRulePrecedence_SafetyCriticalOrdering(t *testing.T) {
	d := New()
	cases := []struct {
		msg        string
		wantAction Action
		wantCat    Category // when set, also assert the winning rule's category
		why        string
	}{
		// CDC provisioning keyword + transient keyword in the same message ->
		// provisioning wins (escalate), never backoff_retry.
		{"publication does not exist: pub1; underlying: connection reset by peer",
			ActionEscalate, "", "CDC provisioning beats transient reset"},
		{"failed to connect to postgresql for pk validation: connection timed out",
			ActionEscalate, "", "PK-validation provisioning beats transient timeout"},
		{"wal_level is 'replica', must be 'logical'; deadlock detected downstream",
			ActionEscalate, "", "WAL provisioning beats transient deadlock"},
		// Lost stream position + a "change stream" provisioning needle ->
		// re-snapshot wins over the provisioning escalate block.
		{"resume of change stream was not possible, as the resume point may no longer be in the oplog",
			ActionReSnapshot, "", "resume-token loss beats change-stream provisioning escalate"},
		// Auth error that also mentions a transient word -> auth wins.
		{"request failed: HTTP 401 Unauthorized; connection reset by peer during token refresh",
			ActionRefreshAuth, "", "auth_expired beats transient"},
		// Rate limit that also mentions a transient word -> rate_limit wins. Both
		// rate_limit and the new transient/network rule yield ActionBackoffRetry,
		// so we MUST assert on Category here to prove the message routes through
		// the (earlier) rate_limit rule and not the network rule.
		{"HTTP 429 Too Many Requests; connection reset by peer",
			ActionBackoffRetry, CategoryRateLimit, "rate_limit precedence over transient"},
	}
	for _, c := range cases {
		got := d.Diagnose(Signal{ErrorMessage: c.msg})
		if got.SuggestedAction != c.wantAction {
			t.Errorf("%s: msg=%q want action %s, got %s", c.why, c.msg, c.wantAction, got.SuggestedAction)
		}
		if c.wantCat != "" && got.Category != c.wantCat {
			t.Errorf("%s: msg=%q want category %s, got %s", c.why, c.msg, c.wantCat, got.Category)
		}
	}
}
