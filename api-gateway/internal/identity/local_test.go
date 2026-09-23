package identity

import (
	"context"
	"errors"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

// bcrypt at the gateway's cost factor takes roughly a third of a second, which is
// the point of it but is too slow to pay in every case here. MinCost exercises the
// same comparison.
func hashOf(t *testing.T, password string) string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("hashing the fixture password: %v", err)
	}
	return string(h)
}

func lookupReturning(hash string, err error) PasswordHashLookup {
	return func(context.Context, string) (string, error) { return hash, err }
}

func TestTheCommunityDefaultAcceptsTheRightPassword(t *testing.T) {
	p := NewLocalProvider(lookupReturning(hashOf(t, "correct horse"), nil))

	id, err := p.Authenticate(context.Background(), Attempt{Email: "a@example.com", Secret: "correct horse"})
	if err != nil {
		t.Fatalf("the right password must authenticate, got %v", err)
	}
	if id == nil || id.Email != "a@example.com" {
		t.Fatalf("got identity %+v, want the attempted email", id)
	}
}

func TestTheCommunityDefaultRejectsTheWrongPassword(t *testing.T) {
	p := NewLocalProvider(lookupReturning(hashOf(t, "correct horse"), nil))

	id, err := p.Authenticate(context.Background(), Attempt{Email: "a@example.com", Secret: "wrong horse"})
	if !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("got %v, want ErrInvalidCredentials", err)
	}
	if id != nil {
		t.Fatalf("a rejected attempt must return no identity, got %+v", id)
	}
}

// An empty secret is not a shortcut past the comparison.
func TestTheCommunityDefaultRejectsAnEmptySecret(t *testing.T) {
	p := NewLocalProvider(lookupReturning(hashOf(t, "correct horse"), nil))

	if _, err := p.Authenticate(context.Background(), Attempt{Email: "a@example.com"}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("got %v, want ErrInvalidCredentials", err)
	}
}

// A stored hash that is empty or corrupt is bcrypt's error, not an accepted
// password. This is the case a user row with no password would produce.
func TestTheCommunityDefaultRejectsAnUnusableStoredHash(t *testing.T) {
	for _, hash := range []string{"", "not-a-bcrypt-hash", "$2a$12$"} {
		p := NewLocalProvider(lookupReturning(hash, nil))

		if _, err := p.Authenticate(context.Background(), Attempt{Email: "a@example.com", Secret: "anything"}); !errors.Is(err, ErrInvalidCredentials) {
			t.Fatalf("stored hash %q: got %v, want ErrInvalidCredentials", hash, err)
		}
	}
}

func TestAnAccountThatDoesNotExistIsAWrongCredential(t *testing.T) {
	p := NewLocalProvider(lookupReturning("", ErrInvalidCredentials))

	if _, err := p.Authenticate(context.Background(), Attempt{Email: "nobody@example.com", Secret: "x"}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("got %v, want ErrInvalidCredentials", err)
	}
}

// A database that could not be read has not said the password was wrong. Reporting
// it as a wrong password writes a false reason into the audit log at exactly the
// moment someone is reading one.
func TestALookupFailureIsUnavailableNotARejectedPassword(t *testing.T) {
	p := NewLocalProvider(lookupReturning("", errors.New("connection refused")))

	_, err := p.Authenticate(context.Background(), Attempt{Email: "a@example.com", Secret: "x"})
	if !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("got %v, want ErrProviderUnavailable", err)
	}
	if errors.Is(err, ErrInvalidCredentials) {
		t.Fatal("a lookup failure must not be reported as a rejected credential")
	}
}

// A provider wired with no lookup denies rather than dereferencing nil. Both
// outcomes stop the login; only one of them leaves the gateway running.
func TestAProviderWithNoLookupDenies(t *testing.T) {
	if _, err := NewLocalProvider(nil).Authenticate(context.Background(), Attempt{Email: "a@example.com", Secret: "x"}); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("got %v, want ErrProviderUnavailable", err)
	}
}

func TestTheCommunityDefaultRegistersUnderTheConfiguredName(t *testing.T) {
	if got := NewLocalProvider(nil).Name(); got != LocalProviderName {
		t.Fatalf("the local provider is named %q, want %q -- the constant is what %s must be set to", got, LocalProviderName, ProviderEnvVar)
	}
}
