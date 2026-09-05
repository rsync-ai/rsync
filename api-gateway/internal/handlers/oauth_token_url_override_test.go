package handlers

import "testing"

// These tests pin the per-provider token-URL override behavior added so that a
// <ID>_TOKEN_URL env var (e.g. GITHUB_TOKEN_URL, set by docker-compose.e2e.yml
// to point at a mock OAuth server) actually overrides the providers.json
// token_url on the DYNAMIC registry path. Before this, NewOAuthHandler returned
// after the dynamic path whenever providers.json was present, so the override —
// which only lived in the legacy hardcoded fallback — was dead code.

func TestProviderTokenURLEnvName(t *testing.T) {
	cases := map[string]string{
		"github":   "GITHUB_TOKEN_URL",
		"zoho-crm": "ZOHO_CRM_TOKEN_URL",
		"zoho.crm": "ZOHO_CRM_TOKEN_URL",
		"HubSpot":  "HUBSPOT_TOKEN_URL",
		"  slack ": "SLACK_TOKEN_URL",
	}
	for id, want := range cases {
		if got := providerTokenURLEnvName(id); got != want {
			t.Errorf("providerTokenURLEnvName(%q) = %q, want %q", id, got, want)
		}
	}
}

func TestResolveProviderTokenURL_OverrideWins(t *testing.T) {
	const mock = "http://mock-github:8080/login/oauth/access_token"
	t.Setenv("GITHUB_TOKEN_URL", mock)

	if got := resolveProviderTokenURL("github", "https://github.com/login/oauth/access_token"); got != mock {
		t.Fatalf("override not honored: got %q, want %q", got, mock)
	}
}

func TestResolveProviderTokenURL_FallbackWhenUnset(t *testing.T) {
	// Ensure no stray override leaks in from the environment.
	t.Setenv("GITHUB_TOKEN_URL", "")
	const fallback = "https://github.com/login/oauth/access_token"

	if got := resolveProviderTokenURL("github", fallback); got != fallback {
		t.Fatalf("fallback not used: got %q, want %q", got, fallback)
	}
}

func TestResolveProviderTokenURL_BlankOverrideIgnored(t *testing.T) {
	// A whitespace-only override must be treated as unset, not blank out the URL.
	t.Setenv("GITHUB_TOKEN_URL", "   ")
	const fallback = "https://github.com/login/oauth/access_token"

	if got := resolveProviderTokenURL("github", fallback); got != fallback {
		t.Fatalf("blank override should fall back: got %q, want %q", got, fallback)
	}
}

func TestResolveProviderTokenURL_EmptyIDReturnsFallback(t *testing.T) {
	const fallback = "https://example.com/token"
	if got := resolveProviderTokenURL("", fallback); got != fallback {
		t.Fatalf("empty id: got %q, want %q", got, fallback)
	}
}

func TestResolveProviderTokenURL_IsolatedByProvider(t *testing.T) {
	// GITHUB_TOKEN_URL must not affect a different provider's resolution.
	t.Setenv("GITHUB_TOKEN_URL", "http://mock-github:8080/token")
	const hubspotFallback = "https://api.hubapi.com/oauth/v1/token"

	if got := resolveProviderTokenURL("hubspot", hubspotFallback); got != hubspotFallback {
		t.Fatalf("cross-provider leak: got %q, want %q", got, hubspotFallback)
	}
}
