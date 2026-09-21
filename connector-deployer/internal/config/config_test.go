package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	// Clear the keys we care about so defaults apply.
	for _, k := range []string{
		"PORT", "ENVIRONMENT", "INTERNAL_SERVICE_SECRET", "DEPLOYER_DOCKER_NETWORK",
		"MCP_SHARED_NETWORK", "OAUTH_TOKENS_VOLUME_NAME", "OAUTH_TOKENS_TARGET",
		"TOOLS_DIR", "DOCKER_HOST",
	} {
		t.Setenv(k, "")
	}
	c := Load()
	if c.Port != 5011 {
		t.Errorf("Port = %d, want 5011", c.Port)
	}
	if c.Network != "rsync-ai-mcp" {
		t.Errorf("Network = %q", c.Network)
	}
	if c.ToolsDir != "/app/shared/mcp-connectors" {
		t.Errorf("ToolsDir = %q", c.ToolsDir)
	}
	// An unset ENVIRONMENT must not count as dev: that is the default a container
	// started without ENVIRONMENT gets, and dev is what lets an unauthenticated
	// deploy through when the secret is unset.
	if c.IsDev() {
		t.Errorf("unset ENVIRONMENT must not be dev (Environment=%q)", c.Environment)
	}
	dc := c.DeployerConfig()
	if dc.Network != "rsync-ai-mcp" || dc.OAuthVolumeName != "rsync-ai-oauth-tokens" || dc.OAuthVolumeTarget != "/root/.rsync-ai" {
		t.Errorf("DeployerConfig view wrong: %+v", dc)
	}
}

func TestIsDevIsAnExplicitAllowList(t *testing.T) {
	for env, want := range map[string]bool{
		"development":   true,
		"dev":           true,
		" Development ": true,
		"":              false,
		"production":    false,
		"prod":          false,
		"staging":       false,
		"local":         false,
		"develop":       false,
	} {
		if got := (&Config{Environment: env}).IsDev(); got != want {
			t.Errorf("IsDev(%q) = %v, want %v", env, got, want)
		}
	}
}

func TestProdAndOverrides(t *testing.T) {
	t.Setenv("ENVIRONMENT", "production")
	t.Setenv("PORT", "6000")
	t.Setenv("INTERNAL_SERVICE_SECRET", "  s3cr3t  ")
	t.Setenv("OAUTH_TOKENS_VOLUME_NAME", "")
	c := Load()
	if c.IsDev() {
		t.Error("ENVIRONMENT=production ⇒ not dev")
	}
	if c.Port != 6000 {
		t.Errorf("Port = %d, want 6000", c.Port)
	}
	if c.InternalSecret != "s3cr3t" {
		t.Errorf("secret should be trimmed, got %q", c.InternalSecret)
	}
	// Empty OAUTH_TOKENS_VOLUME_NAME falls back to the default in env(); the empty
	// string is only honored as "disabled" when explicitly handled elsewhere. Assert
	// the default kicked in (env() treats empty as unset).
	if c.OAuthVolumeName != "rsync-ai-oauth-tokens" {
		t.Errorf("OAuthVolumeName = %q", c.OAuthVolumeName)
	}
}
