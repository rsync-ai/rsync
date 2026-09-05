package handlers

import "testing"

// These tests pin the grant_type-honoring behavior of the OAuth token exchange
// body builder. Before this change exchangeToken hardcoded
// grant_type=authorization_code (oauth.go), silently dropping the grant_type
// that PR #281 records in providers.json for generated connectors. This is the
// Go counterpart of llm-service utils/oauth_exchange.py:build_token_exchange_data.

func TestBuildTokenExchangeData_AuthorizationCode(t *testing.T) {
	p := &OAuthProvider{ClientID: "cid", ClientSecret: "secret", Scopes: "read write", GrantType: "authorization_code"}
	data := buildTokenExchangeData(p, "authcode123", "https://host/oauth/callback/acme")

	if got := data.Get("grant_type"); got != "authorization_code" {
		t.Fatalf("grant_type = %q, want authorization_code", got)
	}
	if got := data.Get("code"); got != "authcode123" {
		t.Fatalf("code = %q, want authcode123", got)
	}
	if got := data.Get("redirect_uri"); got != "https://host/oauth/callback/acme" {
		t.Fatalf("redirect_uri = %q, want the callback URL", got)
	}
	if got := data.Get("client_id"); got != "cid" {
		t.Fatalf("client_id = %q, want cid", got)
	}
	if got := data.Get("client_secret"); got != "secret" {
		t.Fatalf("client_secret = %q, want secret", got)
	}
	// authorization_code must NOT send scope in the token-exchange body
	// (scope was negotiated at the authorize step).
	if data.Has("scope") {
		t.Fatalf("scope should be absent for authorization_code, got %q", data.Get("scope"))
	}
}

func TestBuildTokenExchangeData_DefaultsToAuthorizationCode(t *testing.T) {
	// Empty GrantType (curated / legacy providers.json entries that predate
	// PR #281) MUST behave exactly as the old hardcoded path: authorization_code.
	p := &OAuthProvider{ClientID: "cid", ClientSecret: "secret", GrantType: ""}
	data := buildTokenExchangeData(p, "authcode123", "https://host/oauth/callback/acme")

	if got := data.Get("grant_type"); got != "authorization_code" {
		t.Fatalf("grant_type = %q, want authorization_code (legacy default)", got)
	}
	if got := data.Get("code"); got != "authcode123" {
		t.Fatalf("code = %q, want authcode123", got)
	}
	if data.Get("redirect_uri") == "" {
		t.Fatalf("redirect_uri must be set for authorization_code")
	}
}

func TestBuildTokenExchangeData_ClientCredentials(t *testing.T) {
	// 2-legged client_credentials: server-to-server, no user redirect. The body
	// carries only client_id/secret/grant_type (+ scope when present); it must
	// NOT include code or redirect_uri or the provider rejects the request.
	p := &OAuthProvider{ClientID: "cid", ClientSecret: "secret", Scopes: "read:data", GrantType: "client_credentials"}
	data := buildTokenExchangeData(p, "", "https://host/oauth/callback/acme")

	if got := data.Get("grant_type"); got != "client_credentials" {
		t.Fatalf("grant_type = %q, want client_credentials", got)
	}
	if data.Has("code") {
		t.Fatalf("client_credentials must not send code, got %q", data.Get("code"))
	}
	if data.Has("redirect_uri") {
		t.Fatalf("client_credentials must not send redirect_uri, got %q", data.Get("redirect_uri"))
	}
	if got := data.Get("scope"); got != "read:data" {
		t.Fatalf("scope = %q, want read:data", got)
	}
	if got := data.Get("client_id"); got != "cid" {
		t.Fatalf("client_id = %q, want cid", got)
	}
	if got := data.Get("client_secret"); got != "secret" {
		t.Fatalf("client_secret = %q, want secret", got)
	}
}

func TestBuildTokenExchangeData_ClientCredentialsNoScope(t *testing.T) {
	// When the client_credentials provider declares no scopes, omit the scope
	// key entirely rather than sending scope="" (some token endpoints 400 on it).
	p := &OAuthProvider{ClientID: "cid", ClientSecret: "secret", GrantType: "client_credentials"}
	data := buildTokenExchangeData(p, "", "https://host/oauth/callback/acme")

	if data.Has("scope") {
		t.Fatalf("scope should be omitted when empty, got %q", data.Get("scope"))
	}
	if got := data.Get("grant_type"); got != "client_credentials" {
		t.Fatalf("grant_type = %q, want client_credentials", got)
	}
}
