package slack

import (
	"testing"
	"time"
)

// TestVerifyRequest is the security core of the inbound Slack receiver: a
// request is trusted ONLY if the HMAC over the raw body matches AND the
// timestamp is fresh. Every failure mode must reject.
func TestVerifyRequest(t *testing.T) {
	const secret = "8f742231b10e8888abcd99yyyzzz85a5"
	body := []byte("payload=%7B%22type%22%3A%22block_actions%22%7D")
	now := time.Unix(1_700_000_000, 0)
	ts := "1700000000"
	goodSig := Sign(secret, ts, body)

	t.Run("valid signature within window passes", func(t *testing.T) {
		if err := VerifyRequest(secret, ts, goodSig, body, now); err != nil {
			t.Fatalf("expected valid, got %v", err)
		}
	})

	t.Run("tampered body is rejected", func(t *testing.T) {
		if err := VerifyRequest(secret, ts, goodSig, []byte("payload=evil"), now); err != ErrBadSignature {
			t.Fatalf("expected ErrBadSignature, got %v", err)
		}
	})

	t.Run("wrong signing secret is rejected", func(t *testing.T) {
		if err := VerifyRequest("a-different-secret", ts, goodSig, body, now); err != ErrBadSignature {
			t.Fatalf("expected ErrBadSignature, got %v", err)
		}
	})

	t.Run("empty secret reports not-configured", func(t *testing.T) {
		if err := VerifyRequest("", ts, goodSig, body, now); err != ErrNotConfigured {
			t.Fatalf("expected ErrNotConfigured, got %v", err)
		}
	})

	t.Run("stale timestamp (replay) is rejected", func(t *testing.T) {
		if err := VerifyRequest(secret, ts, goodSig, body, now.Add(10*time.Minute)); err != ErrStaleTimestamp {
			t.Fatalf("expected ErrStaleTimestamp, got %v", err)
		}
	})

	t.Run("far-future timestamp is rejected", func(t *testing.T) {
		if err := VerifyRequest(secret, ts, goodSig, body, now.Add(-10*time.Minute)); err != ErrStaleTimestamp {
			t.Fatalf("expected ErrStaleTimestamp, got %v", err)
		}
	})

	t.Run("missing headers are rejected", func(t *testing.T) {
		if err := VerifyRequest(secret, "", "", body, now); err != ErrMalformedHeader {
			t.Fatalf("expected ErrMalformedHeader, got %v", err)
		}
	})

	t.Run("non-numeric timestamp is rejected", func(t *testing.T) {
		if err := VerifyRequest(secret, "not-a-number", goodSig, body, now); err != ErrMalformedHeader {
			t.Fatalf("expected ErrMalformedHeader, got %v", err)
		}
	})

	t.Run("signature is bound to the timestamp", func(t *testing.T) {
		// A signature minted for a different timestamp must not validate even if
		// that other timestamp is itself fresh — prevents timestamp swapping.
		otherTS := "1700000200"
		if err := VerifyRequest(secret, otherTS, goodSig, body, now.Add(200*time.Second)); err != ErrBadSignature {
			t.Fatalf("expected ErrBadSignature for swapped timestamp, got %v", err)
		}
	})
}
