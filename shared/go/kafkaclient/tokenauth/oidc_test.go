package tokenauth

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

type tokenRequest struct {
	form     string
	user     string
	pass     string
	basicOK  bool
	accept   string
	contentT string
}

// oidcServer stands in for the IdP and records what the client actually sent.
func oidcServer(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) (*httptest.Server, *[]tokenRequest) {
	t.Helper()
	var seen []tokenRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		u, p, ok := r.BasicAuth()
		seen = append(seen, tokenRequest{
			form: string(body), user: u, pass: p, basicOK: ok,
			accept: r.Header.Get("Accept"), contentT: r.Header.Get("Content-Type"),
		})
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, &seen
}

func oidcSource(t *testing.T, endpoint string, mut func(*kafkaclient.Config)) Source {
	t.Helper()
	c := kafkaclient.Config{
		Brokers: []string{"b:9093"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
		SASLMechanism: kafkaclient.MechanismOAuthBearer, OAuthTokenEndpoint: endpoint,
		OAuthClientID: "rsync-kafka", OAuthClientSecret: "s3cr3t",
	}
	if mut != nil {
		mut(&c)
	}
	src, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func TestOIDCClientCredentialsGrant(t *testing.T) {
	srv, seen := oidcServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"access_token":"eyJhbGciOi.stub","token_type":"Bearer","expires_in":3600}`)
	})

	src := oidcSource(t, srv.URL, func(c *kafkaclient.Config) { c.OAuthScope = "kafka:write" })
	tok, err := src.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tok.Value != "eyJhbGciOi.stub" {
		t.Errorf("token = %q", tok.Value)
	}
	// Expiry drives the cache, so expires_in has to be read rather than assumed.
	if ttl := time.Until(tok.Expires); ttl < 55*time.Minute || ttl > 60*time.Minute+time.Second {
		t.Errorf("token expires in %v, want ~1h from expires_in=3600", ttl)
	}

	if len(*seen) != 1 {
		t.Fatalf("%d requests, want 1", len(*seen))
	}
	got := (*seen)[0]
	if !strings.Contains(got.form, "grant_type=client_credentials") {
		t.Errorf("form = %q, want the client-credentials grant", got.form)
	}
	if !strings.Contains(got.form, "scope=kafka%3Awrite") {
		t.Errorf("form = %q, want the configured scope", got.form)
	}
	// RFC 6749 §2.3.1: Basic is the mandatory-to-support form. An IdP that
	// accepts only it would otherwise reject every request.
	if !got.basicOK || got.user != "rsync-kafka" || got.pass != "s3cr3t" {
		t.Errorf("basic auth = (%q, %q, ok=%v), want the client id and secret", got.user, got.pass, got.basicOK)
	}
	if got.contentT != "application/x-www-form-urlencoded" {
		t.Errorf("Content-Type = %q", got.contentT)
	}
	// The secret must never appear in the body as well: some IdPs log the form.
	if strings.Contains(got.form, "s3cr3t") {
		t.Error("the client secret was also sent in the request body")
	}
}

func TestOIDCOmitsScopeWhenUnset(t *testing.T) {
	srv, seen := oidcServer(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"access_token":"t","expires_in":600}`)
	})
	if _, err := oidcSource(t, srv.URL, nil).Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	// An empty scope= parameter is not the same as no scope: some providers
	// reject it, and others return a token with no scopes at all.
	if strings.Contains((*seen)[0].form, "scope") {
		t.Errorf("form = %q, want no scope parameter at all", (*seen)[0].form)
	}
}

// A '+' or '/' in a generated secret is common, and an unencoded one changes
// the credential the IdP sees -- which reads as "wrong client secret" with no
// hint that the value in the manifest is correct.
func TestOIDCEncodesCredentialsPerRFC6749(t *testing.T) {
	srv, seen := oidcServer(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"access_token":"t","expires_in":600}`)
	})
	src := oidcSource(t, srv.URL, func(c *kafkaclient.Config) {
		c.OAuthClientSecret = "a+b/c=d e"
	})
	if _, err := src.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := (*seen)[0].pass; got != "a%2Bb%2Fc%3Dd+e" {
		t.Errorf("transmitted secret = %q, want it form-urlencoded first", got)
	}
}

// The response the cache reads: expires_in is only recommended by RFC 6749, and
// guessing long is the dangerous direction -- a token cached past its real
// expiry fails at the broker rather than here.
func TestOIDCMissingExpiresInFallsBackToAShortTTL(t *testing.T) {
	srv, _ := oidcServer(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"access_token":"t","token_type":"Bearer"}`)
	})
	tok, err := oidcSource(t, srv.URL, nil).Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ttl := time.Until(tok.Expires); ttl <= 0 || ttl > fallbackTTL+time.Second {
		t.Errorf("token expires in %v, want at most the %v fallback", ttl, fallbackTTL)
	}
}

func TestOIDCTokenIsCachedAcrossCalls(t *testing.T) {
	var hits int
	srv, _ := oidcServer(t, func(w http.ResponseWriter, r *http.Request) {
		hits++
		fmt.Fprintf(w, `{"access_token":"token-%d","expires_in":3600}`, hits)
	})
	src := oidcSource(t, srv.URL, nil)
	for i := 0; i < 10; i++ {
		tok, err := src.Token(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if tok.Value != "token-1" {
			t.Fatalf("call %d got %q, want the cached token-1", i, tok.Value)
		}
	}
	if hits != 1 {
		t.Fatalf("%d requests to the IdP, want 1 -- one per broker connection is how an IdP starts rate-limiting us", hits)
	}
}

func TestOIDCCarriesConfiguredExtensions(t *testing.T) {
	srv, _ := oidcServer(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"access_token":"t","expires_in":600}`)
	})
	src := oidcSource(t, srv.URL, func(c *kafkaclient.Config) {
		// Confluent Cloud requires exactly these on an OAUTHBEARER connection.
		c.OAuthExtensions = map[string]string{"logicalCluster": "lkc-abc123", "identityPoolId": "pool-1"}
	})
	tok, err := src.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tok.Extensions["logicalCluster"] != "lkc-abc123" || tok.Extensions["identityPoolId"] != "pool-1" {
		t.Fatalf("extensions did not reach the token: %#v", tok.Extensions)
	}
}

// Every one of these surfaces at the broker as an unexplained SASL failure if
// it is not caught and named here.
func TestOIDCErrorsAreDiagnostic(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler func(w http.ResponseWriter, r *http.Request)
		want    []string
	}{
		{"rejected credentials", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"error":"invalid_client","error_description":"client authentication failed"}`)
		}, []string{"invalid_client", "client authentication failed"}},
		{"server error", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "upstream exploded")
		}, []string{"HTTP 500"}},
		{"a login page instead of a token endpoint", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "<html><body>Sign in</body></html>")
		}, []string{"non-JSON"}},
		{"no access_token in the response", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `{"token_type":"Bearer","expires_in":3600}`)
		}, []string{"no access_token"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := oidcServer(t, tc.handler)
			_, err := oidcSource(t, srv.URL, nil).Token(context.Background())
			if err == nil {
				t.Fatal("expected an error")
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error = %q, want it to contain %q", err, w)
				}
			}
			if !strings.Contains(err.Error(), srv.URL) {
				t.Errorf("error = %q, want it to name the endpoint", err)
			}
			// The endpoint's body must not be echoed wholesale: in this
			// codebase an error string is a path into an LLM prompt.
			if strings.Contains(err.Error(), "Sign in") || strings.Contains(err.Error(), "upstream exploded") {
				t.Errorf("the upstream response body was echoed into the error: %q", err)
			}
		})
	}
}

// A token endpoint that answers slowly must not wedge the connection that is
// waiting on it.
func TestOIDCRequestIsBounded(t *testing.T) {
	srv, _ := oidcServer(t, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	src := oidcSource(t, srv.URL, nil)
	c, ok := src.(*cached)
	if !ok {
		t.Fatalf("expected a *cached source, got %T", src)
	}
	c.timeout = 100 * time.Millisecond

	done := make(chan error, 1)
	go func() {
		_, err := src.Token(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a timeout error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the token request was not bounded")
	}
}
