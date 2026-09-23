package identity

import (
	"context"

	"golang.org/x/crypto/bcrypt"
)

// PasswordHashLookup returns the stored bcrypt hash for an email address.
//
// It returns ErrInvalidCredentials when no such account exists, so that "no user"
// and "wrong password" are one answer to the caller and cannot be told apart by
// timing the two branches at this layer.
//
// It is a function rather than a *sql.DB so that this package holds no SQL and no
// database handle. That is not tidiness: it is what lets the community default be
// tested by the ordinary `go test ./...` that CI runs, instead of behind the
// integration_pg build tag, which CI passes no -tags for and therefore never
// compiles. An extension point whose default is only proven by a test that does
// not run is not proven.
type PasswordHashLookup func(ctx context.Context, email string) (string, error)

// LocalProvider is the community default: a password checked against the bcrypt
// hash stored in the users table.
//
// It is the provider every login uses unless an operator configures another, and
// it stays that way. Nothing in this package treats it as a fallback for a
// provider that failed.
type LocalProvider struct {
	lookup PasswordHashLookup
}

// NewLocalProvider returns the community default provider.
func NewLocalProvider(lookup PasswordHashLookup) *LocalProvider {
	return &LocalProvider{lookup: lookup}
}

// Name implements Provider.
func (p *LocalProvider) Name() string { return LocalProviderName }

// Authenticate implements Provider by comparing the attempt's secret against the
// stored bcrypt hash.
//
// A lookup failure that is not ErrInvalidCredentials is reported as
// ErrProviderUnavailable rather than as a rejected password: a database that
// cannot be read has not said the credential is wrong, and recording it as if it
// had would put a wrong reason in the audit log at exactly the moment someone is
// reading it.
func (p *LocalProvider) Authenticate(ctx context.Context, a Attempt) (*Identity, error) {
	if p.lookup == nil {
		return nil, ErrProviderUnavailable
	}

	hash, err := p.lookup(ctx, a.Email)
	if err != nil {
		if err == ErrInvalidCredentials {
			return nil, ErrInvalidCredentials
		}
		return nil, ErrProviderUnavailable
	}

	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(a.Secret)) != nil {
		return nil, ErrInvalidCredentials
	}

	return &Identity{Email: a.Email}, nil
}
