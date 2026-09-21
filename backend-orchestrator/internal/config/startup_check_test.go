package config

import (
	"strings"
	"testing"
)

func TestStartupSettingProblemsInternalSecret(t *testing.T) {
	const secretValue = "unit-test-internal-secret-orchestrator-startup"

	cases := []struct {
		name     string
		env      map[string]string
		wantName string // empty means no problem expected
	}{
		{name: "missing", env: map[string]string{}, wantName: "INTERNAL_SERVICE_SECRET"},
		{name: "empty", env: map[string]string{"INTERNAL_SERVICE_SECRET": ""}, wantName: "INTERNAL_SERVICE_SECRET"},
		{name: "whitespace only", env: map[string]string{"INTERNAL_SERVICE_SECRET": " \n"}, wantName: "INTERNAL_SERVICE_SECRET"},
		// Control: production and neighbouring secrets being present must not satisfy the check.
		{name: "only other settings set", env: map[string]string{"ENVIRONMENT": "production", "JWT_SECRET": secretValue, "ENCRYPTION_KEY": secretValue}, wantName: "INTERNAL_SERVICE_SECRET"},
		{name: "present", env: map[string]string{"INTERNAL_SERVICE_SECRET": secretValue}},
		{name: "present in production", env: map[string]string{"ENVIRONMENT": "production", "INTERNAL_SERVICE_SECRET": secretValue}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := StartupSettingProblems(func(k string) string { return tc.env[k] })
			if tc.wantName == "" {
				if len(got) != 0 {
					t.Fatalf("expected no problems, got %q", got)
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("expected exactly one problem naming %s, got %d: %q", tc.wantName, len(got), got)
			}
			joined := got[0]
			if !strings.HasPrefix(joined, tc.wantName+" ") {
				t.Fatalf("problem does not start by naming %s: %q", tc.wantName, joined)
			}
			// It must say what will not work, and what to do about it.
			for _, text := range []string{
				"namespace locking", "OAuth token refresh",
				"openssl rand -hex 32",
				"same value to api-gateway, orchestrator, temporal-adapter and frontend",
			} {
				if !strings.Contains(joined, text) {
					t.Fatalf("problem text does not say %q: %q", text, joined)
				}
			}
			if strings.Contains(joined, secretValue) {
				t.Fatalf("problem text leaked a configured value: %q", joined)
			}
		})
	}
}
