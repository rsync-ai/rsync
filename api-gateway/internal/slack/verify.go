package slack

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"
)

const (
	// sigVersion is the Slack signature scheme version prefix.
	sigVersion = "v0"

	// maxSkew bounds how far a request timestamp may be from now. Slack
	// recommends 5 minutes; anything older is treated as a replay and rejected.
	maxSkew = 5 * time.Minute
)

// Signature-verification errors. Callers should treat ALL of these as "reject
// the request" — never approve on a verification failure.
var (
	ErrNotConfigured   = errors.New("slack: signing secret not configured")
	ErrMalformedHeader = errors.New("slack: missing or malformed signature headers")
	ErrStaleTimestamp  = errors.New("slack: request timestamp outside the allowed window")
	ErrBadSignature    = errors.New("slack: signature mismatch")
)

// Sign computes the Slack request signature ("v0=<hex hmac-sha256>") over the
// basestring "v0:<timestamp>:<body>". Exposed so tests (and any future outbound
// signing) can produce a valid signature without duplicating the scheme.
func Sign(signingSecret, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(signingSecret))
	// basestring = "v0:" + timestamp + ":" + raw body
	fmt.Fprintf(mac, "%s:%s:%s", sigVersion, timestamp, body)
	return sigVersion + "=" + hex.EncodeToString(mac.Sum(nil))
}

// VerifyRequest validates a Slack request signature per
// https://api.slack.com/authentication/verifying-requests-from-slack.
//
// body MUST be the RAW request body, captured verbatim BEFORE any form parsing —
// re-encoding the form changes the bytes and breaks the HMAC. now is injectable
// for deterministic tests. A nil error means the request is authentically from
// Slack and within the replay window; any non-nil error means reject.
func VerifyRequest(signingSecret, timestamp, signature string, body []byte, now time.Time) error {
	if signingSecret == "" {
		return ErrNotConfigured
	}
	if timestamp == "" || signature == "" {
		return ErrMalformedHeader
	}
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return ErrMalformedHeader
	}
	// Replay guard: reject timestamps too far in the past OR the future.
	skew := now.Sub(time.Unix(ts, 0))
	if skew < 0 {
		skew = -skew
	}
	if skew > maxSkew {
		return ErrStaleTimestamp
	}
	expected := Sign(signingSecret, timestamp, body)
	// Constant-time compare — never a byte-by-byte early return on secrets.
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return ErrBadSignature
	}
	return nil
}
