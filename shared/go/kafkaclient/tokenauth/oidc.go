package tokenauth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// fallbackTTL is how long a token is cached when the provider omits
// expires_in. RFC 6749 only recommends the field rather than requiring it, and
// guessing long is the dangerous direction: a token cached past its real expiry
// fails at the broker, not here.
const fallbackTTL = time.Minute

// maxTokenResponse caps what is read from the token endpoint. A misconfigured
// endpoint — a login page, a proxy error, an object store — answers 200 with
// something arbitrarily large, and the alternative to a cap is holding it all
// in memory before finding out.
const maxTokenResponse = 1 << 20

// newOIDCSource exchanges client credentials for an access token.
//
// Credentials go in the Authorization header rather than the form body: RFC 6749
// §2.3.1 makes Basic the mandatory-to-support form and says the values are
// form-urlencoded first, which matters the moment a generated secret contains a
// '+' or a '/'.
func newOIDCSource(c kafkaclient.Config) Source {
	o := &oidcClient{
		endpoint:     c.OAuthTokenEndpoint,
		clientID:     c.OAuthClientID,
		clientSecret: c.OAuthClientSecret,
		scope:        c.OAuthScope,
		extensions:   c.OAuthExtensions,
		http:         &http.Client{Timeout: SignTimeout},
	}
	return newCached(o.fetch)
}

type oidcClient struct {
	endpoint     string
	clientID     string
	clientSecret string
	scope        string
	extensions   map[string]string
	http         *http.Client
}

func (o *oidcClient) fetch(ctx context.Context) (Token, error) {
	form := url.Values{"grant_type": {"client_credentials"}}
	if o.scope != "" {
		form.Set("scope", o.scope)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return Token{}, fmt.Errorf("tokenauth: building the token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(url.QueryEscape(o.clientID), url.QueryEscape(o.clientSecret))

	resp, err := o.http.Do(req)
	if err != nil {
		// The URL can carry no credential — they are in the header — so naming
		// it is safe and is usually the whole diagnosis.
		return Token{}, fmt.Errorf("tokenauth: requesting a token from %s: %w", o.endpoint, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTokenResponse))
	if err != nil {
		return Token{}, fmt.Errorf("tokenauth: reading the token response from %s: %w", o.endpoint, err)
	}

	var parsed struct {
		AccessToken      string `json:"access_token"`
		ExpiresIn        int64  `json:"expires_in"`
		Error            string `json:"error"`
		ErrorDescription string `json:"error_description"`
	}
	// A non-JSON body is only worth reporting as such: echoing it back would
	// put an arbitrary upstream response into a log line, and in this codebase
	// a log line is a path into an LLM prompt.
	jsonErr := json.Unmarshal(body, &parsed)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if jsonErr == nil && parsed.Error != "" {
			return Token{}, fmt.Errorf("tokenauth: %s rejected the client credentials: %s (%s)",
				o.endpoint, parsed.Error, parsed.ErrorDescription)
		}
		return Token{}, fmt.Errorf("tokenauth: %s returned HTTP %d for the client-credentials grant", o.endpoint, resp.StatusCode)
	}
	if jsonErr != nil {
		return Token{}, fmt.Errorf("tokenauth: %s returned a non-JSON token response (%d bytes)", o.endpoint, len(body))
	}
	if parsed.AccessToken == "" {
		return Token{}, fmt.Errorf("tokenauth: %s returned no access_token", o.endpoint)
	}

	ttl := time.Duration(parsed.ExpiresIn) * time.Second
	if parsed.ExpiresIn <= 0 {
		ttl = fallbackTTL
	}
	return Token{
		Value:      parsed.AccessToken,
		Extensions: o.extensions,
		Expires:    time.Now().Add(ttl),
	}, nil
}
