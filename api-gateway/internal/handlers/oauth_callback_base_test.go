package handlers

import "testing"

// A server install sets PUBLIC_URL and nothing else; the provider must then be
// sent back to that host, not to localhost on the operator's own machine.
func TestOAuthCallbackBase(t *testing.T) {
	cases := []struct {
		name, callback, public, want string
	}{
		{"explicit wins", "https://auth.example.com/cb", "https://app.example.com", "https://auth.example.com/cb"},
		{"derived from PUBLIC_URL", "", "https://app.example.com/", "https://app.example.com/oauth/callback"},
		{"blank values fall through", "  ", "  ", "http://localhost:5001/oauth/callback"},
		{"laptop default", "", "", "http://localhost:5001/oauth/callback"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OAUTH_CALLBACK_URL", tc.callback)
			t.Setenv("PUBLIC_URL", tc.public)
			if got := oauthCallbackBase(); got != tc.want {
				t.Fatalf("oauthCallbackBase() = %q, want %q", got, tc.want)
			}
		})
	}
}
