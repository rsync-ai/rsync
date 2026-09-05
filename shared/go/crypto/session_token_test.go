package crypto

import (
	"regexp"
	"testing"
)

// The shape migration 094 pins with a CHECK constraint. If HashSessionToken ever
// stopped matching this, every INSERT into sessions would fail at the database --
// so the constraint and this regex are deliberately the same expression.
var sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)

func TestHashSessionToken_MatchesTheCheckConstraint(t *testing.T) {
	got := HashSessionToken("3f2504e0-4f89-11d3-9a0c-0305e82c3301")
	if !sha256Hex.MatchString(got) {
		t.Fatalf("hash %q does not match the CHECK constraint in migration 094 (^[0-9a-f]{64}$)", got)
	}
}

// The whole point of the change: what lands in the database must not be usable as
// a credential. A regression that made this an identity function -- or any encoding
// that preserved the input -- would still be 64 characters for some inputs, so
// assert the actual property rather than only the shape.
func TestHashSessionToken_NeverReturnsThePlaintext(t *testing.T) {
	for _, token := range []string{
		"3f2504e0-4f89-11d3-9a0c-0305e82c3301",
		"",
		"short",
	} {
		if got := HashSessionToken(token); got == token {
			t.Fatalf("HashSessionToken(%q) returned the plaintext -- the stored value is a live credential", token)
		}
	}
}

// Lookup is by hash, so the function must be a pure deterministic map. A salt or
// any per-call randomness would compile and pass a shape test while silently
// making every session unfindable -- i.e. signing out every user on deploy.
func TestHashSessionToken_IsDeterministic(t *testing.T) {
	const token = "3f2504e0-4f89-11d3-9a0c-0305e82c3301"
	first := HashSessionToken(token)
	for i := 0; i < 3; i++ {
		if got := HashSessionToken(token); got != first {
			t.Fatalf("not deterministic: call %d gave %q, want %q -- lookup by hash would break", i+2, got, first)
		}
	}
}

// Pinned against an independently computable value (`printf %s ... | shasum -a 256`)
// so this test fails if the algorithm is swapped, not merely if the output shape
// changes. Both properties above hold for the wrong hash function too.
func TestHashSessionToken_IsSHA256(t *testing.T) {
	const (
		token = "3f2504e0-4f89-11d3-9a0c-0305e82c3301"
		want  = "d1bfaf4aff653cb27984b7d978e51a7d406d1572df95d205c254beb18dc134d3"
	)
	if got := HashSessionToken(token); got != want {
		t.Fatalf("HashSessionToken(%q) = %q, want %q", token, got, want)
	}
}

func TestHashSessionToken_DistinctTokensDistinctHashes(t *testing.T) {
	a := HashSessionToken("3f2504e0-4f89-11d3-9a0c-0305e82c3301")
	b := HashSessionToken("3f2504e0-4f89-11d3-9a0c-0305e82c3302")
	if a == b {
		t.Fatalf("two different tokens hashed to the same value %q", a)
	}
}
