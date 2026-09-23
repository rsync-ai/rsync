// Package identity is the extension point through which the gateway decides who a
// login attempt belongs to.
//
// It exists so that an identity provider the community build does not ship -- an
// SSO service, a directory, anything -- can answer that question without the
// public tree importing a line of it. The public tree calls this interface and
// nothing else; it never imports an implementation it does not ship.
//
// Three properties make that boundary real rather than decorative:
//
//   - The community default is the only provider registered here, and it is the
//     path every existing login test exercises. An extension point whose default
//     is untested is a second code path pretending to be one.
//   - The contract is generic. It carries an attempt and returns an identity.
//     There is deliberately no method that asks "is this customer entitled to a
//     paid feature?" -- that would be a runtime paid-feature gate living in public
//     code, which CLAUDE.md forbids. Entitlement is decided inside whatever
//     service implements this, never here.
//   - Every error path denies. See Registry.Active and the error values below.
//
// The security contract an out-of-process implementation is held to, decided
// before any of it was written and recorded here so that the first one is built
// against a written contract rather than an invented one: the implementation owns
// the identity dance end to end and the gateway never sees a token from the
// upstream provider; it signs a short-lived assertion, which the gateway verifies
// against a public key pinned in its OWN configuration and never one fetched from
// the asserting service; audience and issuer are checked on every assertion;
// replay is caught by a jti cache with a bounded clock skew; and the gateway never
// trusts an assertion because of where the request came from, since network
// position is not identity. None of that is implemented here. This step is the
// interface and its community default, which is all the gateway needs to stop
// having the password comparison welded into the login handler.
package identity

import (
	"context"
	"errors"
)

// ContractVersion is the version of this extension contract.
//
// Section 3.1 of the split document requires a declared version because under the
// chosen topology the implementation is a separate service, so the two sides are
// deployed independently and can disagree. It is declared at "1" now, while the
// only implementation is in-process and cannot disagree with itself, so that the
// first out-of-process implementation has something to state compatibility
// against rather than inventing versioning at the moment it is hardest to change.
//
// Bump it when a change would make an existing implementation wrong -- a new
// required field, a changed meaning, a removed guarantee. Adding a field that an
// older implementation may leave empty is not a bump.
const ContractVersion = "1"

// LocalProviderName is the community default: a password checked against the
// bcrypt hash stored in the users table.
//
// It is a constant because two places must agree on it -- the provider that
// registers under it and the fallback Active uses when nothing is configured --
// and because a test asserts the configured default resolves to exactly this.
const LocalProviderName = "local"

// ProviderEnvVar selects which registered provider authenticates logins.
//
// Unset means LocalProviderName. That default is the CLOUD behaviour as well as
// the community one: cloud runs no external identity service today, so both
// builds resolve to the same provider and this is not an edition switch. It
// selects an integration point any implementer could fill, which is the whole
// difference between an extension point and a lock.
const ProviderEnvVar = "RSYNC_IDENTITY_PROVIDER"

// Attempt is one login attempt, as the gateway received it.
//
// Secret is the password for the local provider and an assertion or code for a
// provider that ran an identity dance elsewhere. It is named Secret rather than
// Password so that no implementation reads the field name as permission to log
// it: nothing in this package prints an Attempt, and nothing should.
//
// The stored password hash is deliberately NOT a field here. A hash handed to
// every provider is a credential handed to every provider, including a remote
// one that has no business holding it. The local provider reaches its own hash
// through the lookup it was constructed with.
type Attempt struct {
	Email  string
	Secret string
}

// Identity is the answer: which account the attempt belongs to.
//
// It carries an email and nothing else on purpose. Role, status and workspace
// membership are the gateway's to decide from its own tables -- a provider that
// could name a user's role would be a provider that could grant itself one.
type Identity struct {
	Email string
}

// Provider resolves an Attempt to an Identity.
//
// A Provider answers exactly one question: whom does this credential belong to.
// It does not mint a session, does not decide what the user may do, and is not
// consulted again after login.
type Provider interface {
	// Name is the value of ProviderEnvVar that selects this provider. It must be
	// stable: it is configuration, not a display string.
	Name() string

	// Authenticate returns the identity the attempt belongs to, or an error.
	//
	// It must return ErrInvalidCredentials -- and only that -- when the credential
	// is simply wrong, so that the caller can tell a rejected password from a
	// provider that could not answer. Any other error is treated as "could not
	// answer", which denies the login just the same.
	Authenticate(ctx context.Context, a Attempt) (*Identity, error)
}

// The error taxonomy. The caller distinguishes these three cases because they are
// three different incidents -- a wrong password, a misconfigured instance, and a
// provider that is down -- and an operator reading an audit log needs to tell them
// apart. All three DENY: the difference is what gets recorded, never whether a
// session is minted.
var (
	// ErrInvalidCredentials means the provider answered, and the answer was no.
	ErrInvalidCredentials = errors.New("identity: invalid credentials")

	// ErrUnknownProvider means ProviderEnvVar names a provider this binary does
	// not have. Logins fail until it is corrected or unset. See Registry.Active
	// for why that is the safe direction.
	ErrUnknownProvider = errors.New("identity: configured provider is not registered")

	// ErrProviderUnavailable means the provider could not answer -- it was
	// unreachable, timed out, or failed internally. It is NOT a rejected
	// credential, and must never be reported as one.
	ErrProviderUnavailable = errors.New("identity: provider could not answer")
)
