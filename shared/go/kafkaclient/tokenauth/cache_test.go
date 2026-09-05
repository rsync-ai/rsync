package tokenauth

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// The failure the cache exists to prevent in the other direction: minting once
// and reusing forever. Reuse is correct only while the token is still valid.
func TestCachedTokenIsReusedWhileValid(t *testing.T) {
	var calls int32
	now := time.Now()
	s := newCached(func(context.Context) (Token, error) {
		n := atomic.AddInt32(&calls, 1)
		return Token{Value: fmt.Sprintf("token-%d", n), Expires: now.Add(15 * time.Minute)}, nil
	})
	s.now = func() time.Time { return now }

	for i := 0; i < 50; i++ {
		got, err := s.Token(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got.Value != "token-1" {
			t.Fatalf("call %d got %q, want the cached token-1", i, got.Value)
		}
	}
	if calls != 1 {
		t.Fatalf("signed %d times, want 1 -- a sign per connection is a request storm against STS on every reconnect", calls)
	}
}

// The failure this branch's brief calls out by name: a token cached for the
// process lifetime dies mid-flight hours later, and nobody attributes it to
// this code.
func TestCachedTokenIsReSignedBeforeItExpires(t *testing.T) {
	var calls int32
	start := time.Now()
	clock := start
	s := newCached(func(context.Context) (Token, error) {
		n := atomic.AddInt32(&calls, 1)
		return Token{Value: fmt.Sprintf("token-%d", n), Expires: clock.Add(15 * time.Minute)}, nil
	})
	s.now = func() time.Time { return clock }

	if got, _ := s.Token(context.Background()); got.Value != "token-1" {
		t.Fatalf("first token = %q", got.Value)
	}

	// Still inside the validity window, minus the margin: reuse.
	clock = start.Add(13 * time.Minute)
	if got, _ := s.Token(context.Background()); got.Value != "token-1" {
		t.Fatalf("at 13m got %q, want the cached token-1", got.Value)
	}

	// Inside the refresh margin: a token that is still technically valid must
	// already be replaced, because the handshake it is about to be used for
	// takes non-zero time.
	clock = start.Add(14*time.Minute + 30*time.Second)
	got, err := s.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != "token-2" {
		t.Fatalf("inside the %v refresh margin got %q, want a freshly signed token-2", RefreshMargin, got.Value)
	}

	// Well past expiry: never serve a dead token.
	clock = start.Add(2 * time.Hour)
	if got, _ := s.Token(context.Background()); got.Value != "token-3" {
		t.Fatalf("two hours in, got %q -- an expired token was served", got.Value)
	}
	if calls != 3 {
		t.Fatalf("signed %d times, want 3", calls)
	}
}

// A single-flight cache is only correct if it is actually correct under the
// concurrency it is built for: a service opens one connection per broker and
// all of them ask at the same instant. Run with -race.
func TestConcurrentCallersMintExactlyOneToken(t *testing.T) {
	var calls int32
	now := time.Now()
	s := newCached(func(context.Context) (Token, error) {
		atomic.AddInt32(&calls, 1)
		// Long enough that a non-single-flight implementation would have every
		// goroutine inside sign() at once.
		time.Sleep(20 * time.Millisecond)
		return Token{Value: "the-one-token", Expires: now.Add(15 * time.Minute)}, nil
	})
	s.now = func() time.Time { return now }

	const n = 32
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := s.Token(context.Background())
			if err != nil {
				errs <- err
				return
			}
			if got.Value != "the-one-token" {
				errs <- fmt.Errorf("got %q", got.Value)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if calls != 1 {
		t.Fatalf("%d concurrent callers produced %d signings, want 1", n, calls)
	}
}

// A signer that fails must not leave a stale token to be served: it is already
// past the refresh margin, so the broker is about to reject it and the error
// would surface as an auth failure instead of the signing error that explains
// it.
func TestSigningFailureIsReturnedRatherThanServingAStaleToken(t *testing.T) {
	start := time.Now()
	clock := start
	fail := false
	s := newCached(func(context.Context) (Token, error) {
		if fail {
			return Token{}, errors.New("no AWS credentials found")
		}
		return Token{Value: "token-1", Expires: clock.Add(15 * time.Minute)}, nil
	})
	s.now = func() time.Time { return clock }

	if _, err := s.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock = start.Add(20 * time.Minute)
	fail = true
	got, err := s.Token(context.Background())
	if err == nil {
		t.Fatalf("expected the signing error, got token %q", got.Value)
	}
	if got.Value != "" {
		t.Errorf("a token was returned alongside the error: %q", got.Value)
	}

	// And it recovers rather than latching: the next successful sign is served.
	fail = false
	if got, err := s.Token(context.Background()); err != nil || got.Value != "token-1" {
		t.Fatalf("after recovery got (%q, %v)", got.Value, err)
	}
}

// An empty token is accepted by neither broker, but it fails there as
// "SASL authentication failed" with no cause. Catching it here names the source.
func TestEmptyTokenIsRejected(t *testing.T) {
	s := newCached(func(context.Context) (Token, error) {
		return Token{Expires: time.Now().Add(time.Hour)}, nil
	})
	_, err := s.Token(context.Background())
	if err == nil {
		t.Fatal("an empty token must be an error")
	}
}

// sarama's AccessTokenProvider contract asks for a bounded wait: an IdP or STS
// endpoint that has gone quiet must not wedge every Kafka connection.
func TestSigningIsBounded(t *testing.T) {
	s := newCached(func(ctx context.Context) (Token, error) {
		<-ctx.Done()
		return Token{}, ctx.Err()
	})
	s.timeout = 50 * time.Millisecond

	done := make(chan error, 1)
	go func() {
		_, err := s.Token(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected the timeout to surface as an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Token() did not respect its own timeout")
	}
}

// A caller's cancelled context must win over the internal timeout rather than
// being replaced by it.
func TestCallerContextIsHonored(t *testing.T) {
	s := newCached(func(ctx context.Context) (Token, error) {
		<-ctx.Done()
		return Token{}, ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Token(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// A nil Source is how the client libraries read "PLAIN, SCRAM, or no SASL at
// all". Returning a non-nil one for those would put an OAUTHBEARER handshake on
// a connection the broker expects to authenticate with a password.
func TestNewReturnsNilForNonTokenMechanisms(t *testing.T) {
	for _, c := range []kafkaclient.Config{
		{Brokers: []string{"b:9092"}, SecurityProtocol: kafkaclient.ProtocolPlaintext},
		{Brokers: []string{"b:9093"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
			SASLMechanism: kafkaclient.MechanismPlain, Username: "u", Password: "p"},
		{Brokers: []string{"b:9093"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
			SASLMechanism: kafkaclient.MechanismSCRAMSHA512, Username: "u", Password: "p"},
	} {
		src, err := New(c)
		if err != nil {
			t.Fatalf("New(%s) = %v", c, err)
		}
		if src != nil {
			t.Errorf("New(%s) returned a token source for a password mechanism", c)
		}
	}
}

func TestNewReturnsASourceForEachTokenMechanism(t *testing.T) {
	for _, c := range []kafkaclient.Config{
		{Brokers: []string{"b:9098"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
			SASLMechanism: kafkaclient.MechanismAWSMSKIAM, AWSRegion: "eu-west-1"},
		{Brokers: []string{"b:9093"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
			SASLMechanism: kafkaclient.MechanismOAuthBearer, OAuthTokenEndpoint: "https://idp.example.com/token",
			OAuthClientID: "id", OAuthClientSecret: "secret"},
	} {
		src, err := New(c)
		if err != nil {
			t.Fatalf("New(%s) = %v", c, err)
		}
		if src == nil {
			t.Fatalf("New(%s) returned no token source", c)
		}
	}
}

// Construction validates, so a config that cannot work is rejected at startup
// rather than at the first handshake -- where it reads as an unexplained SASL
// failure.
func TestNewRejectsAnInvalidConfig(t *testing.T) {
	_, err := New(kafkaclient.Config{
		Brokers: []string{"b:9098"}, SecurityProtocol: kafkaclient.ProtocolSASLPlaintext,
		SASLMechanism: kafkaclient.MechanismAWSMSKIAM, AWSRegion: "eu-west-1",
	})
	if err == nil {
		t.Fatal("MSK IAM without TLS must be rejected when the source is built")
	}
}
