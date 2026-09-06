package handlers

import (
	"errors"
	"net/http"
	"testing"
)

// TestParseDBError_EveryResolverFailureIsATemporaryOutage pins the whole DNS
// class to 503 "temporarily unavailable", not the default 500 "contact
// support". The mapper used to name only NXDOMAIN's wording ("no such host"),
// so a resolver answering SERVFAIL -- or a cgo resolver reporting a temporary
// getaddrinfo failure -- told the user their save had permanently failed and
// gave support nothing to act on. Cases are every error the Go resolver defines
// (net/dnsclient_unix.go) plus the two getaddrinfo wordings, so the class is
// closed against the resolver rather than against one observed failure.
func TestParseDBError_EveryResolverFailureIsATemporaryOutage(t *testing.T) {
	cases := []struct {
		desc string
		msg  string
	}{
		{"NXDOMAIN", `dial tcp: lookup db.internal on 10.96.0.10:53: no such host`},
		{"getaddrinfo EAI_NONAME", `dial tcp: lookup db.internal: Name or service not known`},
		{"SERVFAIL", `dial tcp: lookup db.internal on 127.0.0.53:53: server misbehaving`},
		{"no answer", `dial tcp: lookup db.internal on 10.96.0.10:53: no answer from DNS server`},
		{"lame referral", `dial tcp: lookup db.internal on 10.96.0.10:53: lame referral`},
		{"invalid response", `dial tcp: lookup db.internal on 10.96.0.10:53: invalid DNS response`},
		{"unparseable answer", `dial tcp: lookup db.internal on 10.96.0.10:53: cannot unmarshal DNS message`},
		{"unencodable query", `dial tcp: lookup db.internal on 10.96.0.10:53: cannot marshal DNS message`},
		{"getaddrinfo EAI_AGAIN", `dial tcp: lookup db.internal: Temporary failure in name resolution`},
	}
	for _, c := range cases {
		_, code, status := ParseDBError(errors.New(c.msg), "connection", "prod-warehouse")
		if status != http.StatusServiceUnavailable {
			t.Errorf("%s: status = %d, want %d — a resolver outage is retryable, not a permanent save failure",
				c.desc, status, http.StatusServiceUnavailable)
		}
		if code != ErrCodeDatabaseError {
			t.Errorf("%s: code = %q, want %q", c.desc, code, ErrCodeDatabaseError)
		}
	}
}

// TestParseDBError_ARowRejectionIsNotAnOutage is the control. Widening the DNS
// test must not swallow errors the database produced after it examined the row:
// those keep their own mappings, and a duplicate name must still be the 409
// that tells the user to rename rather than to wait.
func TestParseDBError_ARowRejectionIsNotAnOutage(t *testing.T) {
	_, code, status := ParseDBError(
		errors.New(`ERROR: duplicate key value violates unique constraint "connections_name_key"`),
		"connection", "prod-warehouse")
	if status != http.StatusConflict || code != ErrCodeDuplicateName {
		t.Errorf("duplicate name mapped to (%d, %q), want (%d, %q)",
			status, code, http.StatusConflict, ErrCodeDuplicateName)
	}
}
