package email

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

// P3.9: SendWorkspaceInvite delivers a team-invite email with the accept link. Like
// every Send method it is a no-op when IsConfigured() is false (self-hosted /
// air-gapped). The Client is constructed directly here (in-package, unexported
// fields) with a stub RoundTripper so the configured path is exercised without a
// real network call.

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSendWorkspaceInvite_NoopWhenUnconfigured(t *testing.T) {
	c := &Client{
		apiKey:   "", // not configured
		fromAddr: "noreply@rsync.ai",
		httpClient: &http.Client{Transport: rtFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("no HTTP request should be made when unconfigured")
			return nil, nil
		})},
	}
	if err := c.SendWorkspaceInvite(context.Background(),
		"invitee@example.com", "Acme", "member", "https://app.rsync.ai/invite/TOK"); err != nil {
		t.Fatalf("unconfigured send should be a no-op nil, got %v", err)
	}
}

func TestSendWorkspaceInvite_ConfiguredSendsInvite(t *testing.T) {
	var gotReq *http.Request
	var gotBody string
	c := &Client{
		apiKey:   "re_test_key",
		fromAddr: "noreply@rsync.ai",
		httpClient: &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
			gotReq = r
			b, _ := io.ReadAll(r.Body)
			gotBody = string(b)
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(strings.NewReader(`{"id":"email_1"}`)),
				Header:     make(http.Header),
			}, nil
		})},
	}

	acceptURL := "https://app.rsync.ai/invite/TOK123"
	if err := c.SendWorkspaceInvite(context.Background(),
		"invitee@example.com", "Acme Analytics", "member", acceptURL); err != nil {
		t.Fatalf("configured send: %v", err)
	}
	if gotReq == nil {
		t.Fatal("expected an HTTP request to be made")
	}
	if gotReq.Method != http.MethodPost || gotReq.URL.String() != resendSendURL {
		t.Fatalf("expected POST %s, got %s %s", resendSendURL, gotReq.Method, gotReq.URL.String())
	}
	if auth := gotReq.Header.Get("Authorization"); auth != "Bearer re_test_key" {
		t.Fatalf("expected bearer auth, got %q", auth)
	}
	// The accept link, workspace name, and recipient must all appear in the payload.
	for _, want := range []string{acceptURL, "Acme Analytics", "invitee@example.com"} {
		if !strings.Contains(gotBody, want) {
			t.Fatalf("invite email payload missing %q; body=%s", want, gotBody)
		}
	}
}

func TestSendUpgradeRequest_NoopWhenUnconfigured(t *testing.T) {
	c := &Client{
		apiKey:   "", // not configured
		fromAddr: "noreply@rsync.ai",
		httpClient: &http.Client{Transport: rtFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("no HTTP request should be made when unconfigured")
			return nil, nil
		})},
	}
	if err := c.SendUpgradeRequest(context.Background(),
		"sales@rsync.ai", "user@example.com", "Ada", "user-123", "trial"); err != nil {
		t.Fatalf("unconfigured send should be a no-op nil, got %v", err)
	}
}

func TestSendUpgradeRequest_NoopWhenNoRecipient(t *testing.T) {
	c := &Client{
		apiKey:   "re_test_key",
		fromAddr: "noreply@rsync.ai",
		httpClient: &http.Client{Transport: rtFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("no HTTP request should be made when the recipient is empty")
			return nil, nil
		})},
	}
	if err := c.SendUpgradeRequest(context.Background(),
		"  ", "user@example.com", "Ada", "user-123", "trial"); err != nil {
		t.Fatalf("empty-recipient send should be a no-op nil, got %v", err)
	}
}

func TestSendUpgradeRequest_ConfiguredNotifiesTeam(t *testing.T) {
	var gotReq *http.Request
	var gotBody string
	c := &Client{
		apiKey:   "re_test_key",
		fromAddr: "noreply@rsync.ai",
		httpClient: &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
			gotReq = r
			b, _ := io.ReadAll(r.Body)
			gotBody = string(b)
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(strings.NewReader(`{"id":"email_1"}`)),
				Header:     make(http.Header),
			}, nil
		})},
	}

	if err := c.SendUpgradeRequest(context.Background(),
		"sales@rsync.ai", "ada@example.com", "Ada Lovelace", "user-abc-123", "trial"); err != nil {
		t.Fatalf("configured send: %v", err)
	}
	if gotReq == nil {
		t.Fatal("expected an HTTP request to be made")
	}
	if gotReq.Method != http.MethodPost || gotReq.URL.String() != resendSendURL {
		t.Fatalf("expected POST %s, got %s %s", resendSendURL, gotReq.Method, gotReq.URL.String())
	}
	// The team recipient plus the requester's details must all appear in the payload
	// so sales can act on it without any manual lookup.
	for _, want := range []string{"sales@rsync.ai", "ada@example.com", "Ada Lovelace", "user-abc-123", "trial"} {
		if !strings.Contains(gotBody, want) {
			t.Fatalf("upgrade-request email payload missing %q; body=%s", want, gotBody)
		}
	}
}

// --- sender configuration -------------------------------------------------
//
// These guard the public-release fix for the hardcoded "noreply@rsync.ai" default.
// The fallback was not a cosmetic branding leak: it made a half-configured install
// (RESEND_API_KEY set, RESEND_FROM_ADDR forgotten) report IsConfigured()==true, so
// signup began gating on email verification while every send was rejected by Resend
// for an unverified sender domain. New accounts were created unverified and the
// unlocking mail never arrived. Restoring the fallback fails TestNew_HasNoDefaultSender.

func TestNew_HasNoDefaultSender(t *testing.T) {
	t.Setenv("RESEND_API_KEY", "re_test_key")
	t.Setenv("RESEND_FROM_ADDR", "")

	c := New()
	if c.fromAddr != "" {
		t.Fatalf("New() invented a sender address %q; there must be no default sender, "+
			"because any default is a domain the operator's Resend account has not verified", c.fromAddr)
	}
	if c.IsConfigured() {
		t.Fatal("IsConfigured() is true with no sender address: signup would gate on a " +
			"verification mail that can never be delivered")
	}
}

func TestIsConfigured_RequiresBothKeyAndFromAddr(t *testing.T) {
	for _, tc := range []struct {
		name, key, from string
		want            bool
	}{
		{"both set", "re_test_key", "noreply@example.com", true},
		{"no key", "", "noreply@example.com", false},
		{"no sender", "re_test_key", "", false},
		{"neither", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{apiKey: tc.key, fromAddr: tc.from}
			if got := c.IsConfigured(); got != tc.want {
				t.Fatalf("IsConfigured() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSend_NoopWhenSenderMissing(t *testing.T) {
	c := &Client{
		apiKey:   "re_test_key", // key present...
		fromAddr: "",            // ...but no verified sender
		httpClient: &http.Client{Transport: rtFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("Send attempted an HTTP call with no sender address; Resend would " +
				"reject it and the caller would log a failure it cannot act on")
			return nil, nil
		})},
	}
	if err := c.Send(context.Background(), "user@example.com", "subj", "<p>body</p>"); err != nil {
		t.Fatalf("Send should be a silent no-op when unconfigured, got %v", err)
	}
}

func TestConfigStatus_NamesTheMissingVariableAndNeverLeaksTheKey(t *testing.T) {
	const secret = "re_super_secret_key"
	for _, tc := range []struct {
		name, key, from string
		mustContain     string
	}{
		{"fully configured", secret, "noreply@example.com", "noreply@example.com"},
		{"nothing set", "", "", "RESEND_API_KEY"},
		{"sender only", "", "noreply@example.com", "RESEND_API_KEY"},
		{"key only", secret, "", "RESEND_FROM_ADDR"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := (&Client{apiKey: tc.key, fromAddr: tc.from}).ConfigStatus()
			if !strings.Contains(got, tc.mustContain) {
				t.Fatalf("ConfigStatus() = %q, want it to name %q so the operator knows what to fix",
					got, tc.mustContain)
			}
			if strings.Contains(got, secret) {
				t.Fatalf("ConfigStatus() leaked the API key into a log line: %q", got)
			}
		})
	}
}

// The env template promised placeholder-safety long before any code implemented it.
// This pins the promise: the literal value shipped in .env.prod.example must behave
// exactly like an unset key, and a real-looking key must never be mistaken for one.
func TestIsConfigured_TreatsTheShippedPlaceholderAsUnset(t *testing.T) {
	for _, tc := range []struct {
		name, key string
		want      bool
	}{
		{"the value shipped in .env.prod.example", "re_xxxxxxxxxxxx", false},
		{"placeholder, upper case", "re_XXXXXXXXXXXX", false},
		{"a real Resend key", "re_A1b2C3d4E5f6G7h8J9k0", true},
		{"a real key that merely contains x", "re_ax1B2xC3d4x", true},
		{"empty", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{apiKey: tc.key, fromAddr: "noreply@example.com"}
			if got := c.IsConfigured(); got != tc.want {
				t.Fatalf("IsConfigured() with key %q = %v, want %v", tc.key, got, tc.want)
			}
		})
	}
}

func TestConfigStatus_CallsOutTheLeftInPlaceholder(t *testing.T) {
	got := (&Client{apiKey: "re_xxxxxxxxxxxx", fromAddr: "noreply@example.com"}).ConfigStatus()
	if !strings.Contains(got, "placeholder") {
		t.Fatalf("ConfigStatus() = %q, want it to say the key is still the placeholder", got)
	}
}
