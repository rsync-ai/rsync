// Package tokenauth mints the bearer tokens for the two SASL mechanisms that
// need a credential provider rather than a static username and password:
// AWS_MSK_IAM and OAUTHBEARER.
//
// Both travel on the wire as SASL/OAUTHBEARER, so the two client libraries need
// the same thing from this package — a token and its extensions — and differ
// only in how they carry it. Keeping the token sources here rather than in
// saramaauth and kgoauth is what stops the two from drifting into different
// refresh behavior against the same broker.
//
// It is a separate package from kafkaclient so that importing kafkaclient for
// its Config or its Topic() helper does not pull the AWS SDK into a service
// that authenticates with SCRAM.
package tokenauth

import (
	"context"
	"fmt"
	"sync"
	"time"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// RefreshMargin is how long before expiry a cached token is replaced.
//
// An MSK IAM token lives 15 minutes. Handing one out at 14:59 means the broker
// may see it expired by the time the handshake completes, and the resulting
// failure looks like an intermittent auth problem rather than a clock race.
const RefreshMargin = time.Minute

// SignTimeout bounds one token acquisition. sarama's AccessTokenProvider
// contract asks explicitly for a timeout rather than an indefinite block, so
// that a broker connection can log and retry instead of hanging: an STS or IdP
// endpoint that has gone quiet must not wedge every Kafka connection in the
// process.
const SignTimeout = 15 * time.Second

// Token is one OAUTHBEARER credential.
type Token struct {
	// Value is the token as the broker will see it.
	Value string
	// Extensions are the optional key-value pairs sent alongside it.
	Extensions map[string]string
	// Expires is when the broker will stop accepting Value.
	Expires time.Time
}

// Source produces tokens. Implementations are safe for concurrent use: a
// service opens one connection per broker and each asks for a token at the same
// moment.
type Source interface {
	Token(ctx context.Context) (Token, error)
}

// New returns the token source implied by c, or (nil, nil) when the mechanism
// is not token-based.
//
// A nil Source is the signal that PLAIN, SCRAM or no SASL at all is in play, so
// callers branch on it instead of re-deriving the mechanism.
func New(c kafkaclient.Config) (Source, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	switch {
	case !c.UsesTokenAuth():
		return nil, nil
	case c.SASLMechanism == kafkaclient.MechanismAWSMSKIAM:
		return newMSKSource(c), nil
	case c.SASLMechanism == kafkaclient.MechanismOAuthBearer:
		return newOIDCSource(c), nil
	default:
		// Unreachable: UsesTokenAuth covers exactly these two. Kept so that
		// adding a third mechanism cannot silently produce a nil Source, which
		// callers read as "no authentication needed".
		return nil, fmt.Errorf("tokenauth: unhandled SASL mechanism %q", c.SASLMechanism)
	}
}

// cached wraps a token-minting function with the expiry handling both sources
// need.
//
// The two failure modes it exists to sit between: minting per connection turns
// every reconnect into an STS or IdP round trip, which throttles under a
// reconnect storm; minting once for the process lifetime hands out a token that
// expired 15 minutes in, and the connection dies hours later in a way nobody
// attributes to the token. So: reuse while valid, re-mint before expiry, never
// past it.
type cached struct {
	sign    func(ctx context.Context) (Token, error)
	margin  time.Duration
	timeout time.Duration
	now     func() time.Time // overridden in tests; time.Now in production

	mu  sync.Mutex
	cur Token
}

// Token returns a valid token, minting a new one if the cached one is missing
// or close to expiry.
//
// The lock is held across sign() deliberately. Every broker connection asks at
// once during startup, and without it they would each mint a token — the
// thundering herd against STS that the cache exists to prevent. Holding it means
// exactly one call is in flight and the rest take its result, which is why
// sign() runs under a timeout.
func (s *cached) Token(ctx context.Context) (Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cur.Value != "" && s.now().Before(s.cur.Expires.Add(-s.margin)) {
		return s.cur, nil
	}

	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	t, err := s.sign(ctx)
	if err != nil {
		// The stale token is left in place but not returned: it is already past
		// the refresh margin, so the next call retries rather than serving
		// something the broker is about to reject.
		return Token{}, err
	}
	if t.Value == "" {
		return Token{}, fmt.Errorf("tokenauth: token source returned an empty token")
	}
	s.cur = t
	return t, nil
}

func newCached(sign func(ctx context.Context) (Token, error)) *cached {
	return &cached{sign: sign, margin: RefreshMargin, timeout: SignTimeout, now: time.Now}
}
