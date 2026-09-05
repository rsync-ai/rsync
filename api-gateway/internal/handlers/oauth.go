package handlers

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"api-gateway/internal/safehttp"

	"github.com/rsync-ai/shared/crypto"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// Sentinel errors returned by RefreshTokenByID so callers can distinguish
// non-fatal conditions from hard failures.
var (
	// ErrTokenFresh is returned when the token is still valid (>5 min remaining).
	// Callers should treat this as success — no refresh was needed.
	ErrTokenFresh = errors.New("oauth token still fresh, no refresh needed")

	// ErrNoRefreshToken is returned when the token row has no refresh_token.
	// The user must re-authenticate through the OAuth flow to get a new token.
	ErrNoRefreshToken = errors.New("oauth token has no refresh_token; re-authentication required")

	// ErrRefreshTokenExpired is returned when the provider rejects the refresh token
	// (e.g. HTTP 400 invalid_grant, HTTP 401). The user must re-authenticate.
	ErrRefreshTokenExpired = errors.New("oauth refresh token rejected by provider; re-authentication required")
)

// shopLabelRe matches a single valid DNS label (letters, digits, hyphens; no
// dots, slashes, '@', ':' or '#'). It is the allowlist for the user-supplied
// {shop} value substituted into per-shop OAuth provider URLs. Without it,
// shop="169.254.169.254#" or shop="attacker.com#" injects an arbitrary host
// into the token URL — an SSRF and OAuth client-secret exfiltration primitive.
var shopLabelRe = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)

// sanitizeShopSubdomain normalizes and validates a user-supplied shop value,
// returning the bare subdomain label or an error if it is not a single safe
// DNS label.
func sanitizeShopSubdomain(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	s = strings.TrimSuffix(s, ".myshopify.com")
	s = strings.TrimSpace(s)
	if s == "" {
		return "", errors.New("shop subdomain is required")
	}
	if !shopLabelRe.MatchString(s) {
		return "", errors.New("invalid shop subdomain")
	}
	return s, nil
}

// OAuthProvider represents a supported OAuth provider
type OAuthProvider struct {
	Name         string `json:"name"`
	ClientID     string `json:"-"`
	ClientSecret string `json:"-"`
	AuthURL      string `json:"auth_url"`
	TokenURL     string `json:"token_url"`
	Scopes       string `json:"scopes"`
	// GrantType is the OAuth2 flow: "authorization_code" (3-legged, the default
	// when empty) or "client_credentials" (2-legged, server-to-server, no user
	// redirect). Sourced from providers.json (written by the tool generator,
	// PR #281) and honored by buildTokenExchangeData / exchangeToken.
	GrantType string `json:"grant_type,omitempty"`
	Enabled   bool   `json:"enabled"`
}

// OAuthToken represents a stored OAuth token
type OAuthToken struct {
	ID           string    `json:"id"`
	Provider     string    `json:"provider"`
	AccessToken  string    `json:"-"`
	RefreshToken string    `json:"-"`
	TokenType    string    `json:"token_type"`
	ExpiresAt    time.Time `json:"expires_at"`
	Scopes       string    `json:"scopes"`
	AccountID    string    `json:"account_id,omitempty"`
	AccountName  string    `json:"account_name,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// OAuthHandler handles OAuth operations
type OAuthHandler struct {
	db        *sql.DB
	providers map[string]*OAuthProvider
	// registry is the full providers.json entries (enabled + disabled), keyed by provider id
	registry map[string]oauthRegistryEntry
}

// oauthRegistryEntry is the shape of one entry in providers.json
type oauthRegistryEntry struct {
	Name              string `json:"name"`
	AuthURL           string `json:"auth_url"`
	TokenURL          string `json:"token_url"`
	Scopes            string `json:"scopes"`
	ClientIDEnv       string `json:"client_id_env"`
	ClientSecretEnv   string `json:"client_secret_env"`
	RequiresSubdomain bool   `json:"requires_subdomain,omitempty"`
	// TokenNeverExpires marks providers that issue non-expiring access tokens
	// and no refresh_token (e.g. Shopify offline tokens). The tool generator
	// writes this for offline OAuth providers; see exchangeToken for how it is
	// used to avoid stamping a bogus 1-hour expiry.
	TokenNeverExpires bool              `json:"token_never_expires,omitempty"`
	ExtraAuthParams   map[string]string `json:"extra_auth_params,omitempty"`
	Category          string            `json:"category,omitempty"`
	DocsURL           string            `json:"docs_url,omitempty"`
	// GrantType is the OAuth2 flow recorded by the tool generator (PR #281):
	// "authorization_code" (default) or "client_credentials". Empty means
	// authorization_code for backward compatibility with curated entries.
	GrantType string `json:"grant_type,omitempty"`
}

func oauthEnvPrefix(providerName string) string {
	return strings.ToUpper(strings.ReplaceAll(providerName, "-", "_"))
}

func oauthDisabledMessage(providerName string) string {
	envVar := oauthEnvPrefix(providerName)
	return fmt.Sprintf("OAuth provider '%s' is disabled. Configure %s_CLIENT_ID and %s_CLIENT_SECRET to enable", providerName, envVar, envVar)
}

// loadOAuthRegistry reads providers.json from the shared MCP connectors directory.
// Returns a nil map (not an error) when the file is absent — callers fall back to
// the legacy hardcoded list.
func loadOAuthRegistry() map[string]oauthRegistryEntry {
	base := GetMCPConnectorsPath()
	path := base + "/oauth/providers.json"
	data, err := os.ReadFile(path)
	if err != nil {
		log.Infof("oauth: providers.json not found at %s, using built-in list", path)
		return nil
	}
	var reg map[string]oauthRegistryEntry
	if err := json.Unmarshal(data, &reg); err != nil {
		log.Warnf("oauth: failed to parse providers.json: %v — using built-in list", err)
		return nil
	}
	log.Infof("oauth: loaded %d providers from %s", len(reg), path)
	return reg
}

// NewOAuthHandler creates a new OAuth handler.
// It first tries to load provider configuration from providers.json (the single
// source of truth written by the tool generator). Falls back to the legacy
// hardcoded list only when the file is absent.
// providerTokenURLEnvName returns the per-provider token-URL override env var
// name for a registry id: "github" -> "GITHUB_TOKEN_URL", "zoho-crm" ->
// "ZOHO_CRM_TOKEN_URL". Non-alphanumeric separators (-, .) become "_". Pure and
// deterministic so it is unit-testable without touching the environment.
func providerTokenURLEnvName(id string) string {
	up := strings.ToUpper(strings.NewReplacer("-", "_", ".", "_", " ", "_").Replace(strings.TrimSpace(id)))
	return up + "_TOKEN_URL"
}

// resolveProviderTokenURL honors a per-provider <ID>_TOKEN_URL env override
// (e.g. GITHUB_TOKEN_URL) over the providers.json token_url, falling back to the
// registry value when the override is unset/blank. This keeps the documented
// e2e wiring live: docker-compose.e2e.yml points a provider's token endpoint at
// a mock via <ID>_TOKEN_URL, which previously only worked on the legacy
// hardcoded fallback (skipped whenever providers.json is present).
func resolveProviderTokenURL(id, fallback string) string {
	if strings.TrimSpace(id) == "" {
		return fallback
	}
	if override := strings.TrimSpace(os.Getenv(providerTokenURLEnvName(id))); override != "" {
		return override
	}
	return fallback
}

func NewOAuthHandler(db *sql.DB) *OAuthHandler {
	providers := make(map[string]*OAuthProvider)
	registry := loadOAuthRegistry()

	if registry != nil {
		// Dynamic path: activate any provider whose env vars are set
		for id, entry := range registry {
			clientID := os.Getenv(entry.ClientIDEnv)
			if clientID == "" {
				continue
			}
			providers[id] = &OAuthProvider{
				Name:         id,
				ClientID:     clientID,
				ClientSecret: os.Getenv(entry.ClientSecretEnv),
				AuthURL:      entry.AuthURL,
				// Honor a per-provider <ID>_TOKEN_URL env override (e.g.
				// GITHUB_TOKEN_URL) over the providers.json token_url. Without
				// this the override — set by docker-compose.e2e.yml to point the
				// token endpoint at a mock — was DEAD on the dynamic registry
				// path, because this path returns before the legacy hardcoded
				// fallback (the only place the override used to be read) whenever
				// providers.json is present.
				TokenURL:  resolveProviderTokenURL(id, entry.TokenURL),
				Scopes:    entry.Scopes,
				GrantType: entry.GrantType,
				Enabled:   true,
			}
		}
		return &OAuthHandler{db: db, providers: providers, registry: registry}
	}

	// Legacy fallback — hardcoded providers used when providers.json is absent

	// =========================================================================
	// CRM Providers
	// =========================================================================

	// HubSpot
	if clientID := os.Getenv("HUBSPOT_CLIENT_ID"); clientID != "" {
		providers["hubspot"] = &OAuthProvider{
			Name:         "hubspot",
			ClientID:     clientID,
			ClientSecret: os.Getenv("HUBSPOT_CLIENT_SECRET"),
			AuthURL:      "https://app.hubspot.com/oauth/authorize",
			TokenURL:     "https://api.hubapi.com/oauth/v1/token",
			Scopes:       "crm.objects.contacts.read crm.objects.contacts.write crm.objects.companies.read crm.objects.companies.write crm.objects.deals.read crm.objects.deals.write",
			Enabled:      true,
		}
	}

	// Salesforce
	if clientID := os.Getenv("SALESFORCE_CLIENT_ID"); clientID != "" {
		providers["salesforce"] = &OAuthProvider{
			Name:         "salesforce",
			ClientID:     clientID,
			ClientSecret: os.Getenv("SALESFORCE_CLIENT_SECRET"),
			AuthURL:      "https://login.salesforce.com/services/oauth2/authorize",
			TokenURL:     "https://login.salesforce.com/services/oauth2/token",
			Scopes:       "api refresh_token offline_access",
			Enabled:      true,
		}
	}

	// Pipedrive
	if clientID := os.Getenv("PIPEDRIVE_CLIENT_ID"); clientID != "" {
		providers["pipedrive"] = &OAuthProvider{
			Name:         "pipedrive",
			ClientID:     clientID,
			ClientSecret: os.Getenv("PIPEDRIVE_CLIENT_SECRET"),
			AuthURL:      "https://oauth.pipedrive.com/oauth/authorize",
			TokenURL:     "https://oauth.pipedrive.com/oauth/token",
			Scopes:       "",
			Enabled:      true,
		}
	}

	// Zoho CRM
	if clientID := os.Getenv("ZOHO_CLIENT_ID"); clientID != "" {
		providers["zoho-crm"] = &OAuthProvider{
			Name:         "zoho-crm",
			ClientID:     clientID,
			ClientSecret: os.Getenv("ZOHO_CLIENT_SECRET"),
			AuthURL:      "https://accounts.zoho.com/oauth/v2/auth",
			TokenURL:     "https://accounts.zoho.com/oauth/v2/token",
			Scopes:       "ZohoCRM.modules.ALL ZohoCRM.settings.ALL",
			Enabled:      true,
		}
	}

	// =========================================================================
	// Communication Providers
	// =========================================================================

	// Slack
	if clientID := os.Getenv("SLACK_CLIENT_ID"); clientID != "" {
		providers["slack"] = &OAuthProvider{
			Name:         "slack",
			ClientID:     clientID,
			ClientSecret: os.Getenv("SLACK_CLIENT_SECRET"),
			AuthURL:      "https://slack.com/oauth/v2/authorize",
			TokenURL:     "https://slack.com/api/oauth.v2.access",
			Scopes:       "channels:read channels:history users:read chat:write",
			Enabled:      true,
		}
	}

	// =========================================================================
	// Developer Tools
	// =========================================================================

	// GitHub
	if clientID := os.Getenv("GITHUB_CLIENT_ID"); clientID != "" {
		tokenURL := strings.TrimSpace(os.Getenv("GITHUB_TOKEN_URL"))
		if tokenURL == "" {
			tokenURL = "https://github.com/login/oauth/access_token"
		}
		providers["github"] = &OAuthProvider{
			Name:         "github",
			ClientID:     clientID,
			ClientSecret: os.Getenv("GITHUB_CLIENT_SECRET"),
			AuthURL:      "https://github.com/login/oauth/authorize",
			TokenURL:     tokenURL,
			Scopes:       "repo read:user read:org",
			Enabled:      true,
		}
	}

	// Notion
	if clientID := os.Getenv("NOTION_CLIENT_ID"); clientID != "" {
		providers["notion"] = &OAuthProvider{
			Name:         "notion",
			ClientID:     clientID,
			ClientSecret: os.Getenv("NOTION_CLIENT_SECRET"),
			AuthURL:      "https://api.notion.com/v1/oauth/authorize",
			TokenURL:     "https://api.notion.com/v1/oauth/token",
			Scopes:       "",
			Enabled:      true,
		}
	}

	// Jira
	if clientID := os.Getenv("JIRA_CLIENT_ID"); clientID != "" {
		providers["jira"] = &OAuthProvider{
			Name:         "jira",
			ClientID:     clientID,
			ClientSecret: os.Getenv("JIRA_CLIENT_SECRET"),
			AuthURL:      "https://auth.atlassian.com/authorize",
			TokenURL:     "https://auth.atlassian.com/oauth/token",
			Scopes:       "read:jira-user read:jira-work write:jira-work offline_access",
			Enabled:      true,
		}
	}

	// =========================================================================
	// Cloud Storage
	// =========================================================================

	// Google
	if clientID := os.Getenv("GOOGLE_CLIENT_ID"); clientID != "" {
		providers["google"] = &OAuthProvider{
			Name:         "google",
			ClientID:     clientID,
			ClientSecret: os.Getenv("GOOGLE_CLIENT_SECRET"),
			AuthURL:      "https://accounts.google.com/o/oauth2/v2/auth",
			TokenURL:     "https://oauth2.googleapis.com/token",
			Scopes:       "https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/drive.file",
			Enabled:      true,
		}
	}

	// Dropbox
	if clientID := os.Getenv("DROPBOX_CLIENT_ID"); clientID != "" {
		providers["dropbox"] = &OAuthProvider{
			Name:         "dropbox",
			ClientID:     clientID,
			ClientSecret: os.Getenv("DROPBOX_CLIENT_SECRET"),
			AuthURL:      "https://www.dropbox.com/oauth2/authorize",
			TokenURL:     "https://api.dropboxapi.com/oauth2/token",
			Scopes:       "",
			Enabled:      true,
		}
	}

	// =========================================================================
	// Payment Providers
	// =========================================================================

	// Stripe (Connect)
	if clientID := os.Getenv("STRIPE_CLIENT_ID"); clientID != "" {
		providers["stripe"] = &OAuthProvider{
			Name:         "stripe",
			ClientID:     clientID,
			ClientSecret: os.Getenv("STRIPE_SECRET_KEY"),
			AuthURL:      "https://connect.stripe.com/oauth/authorize",
			TokenURL:     "https://connect.stripe.com/oauth/token",
			Scopes:       "read_write",
			Enabled:      true,
		}
	}

	// =========================================================================
	// Marketing/Support Providers
	// =========================================================================

	// Intercom
	if clientID := os.Getenv("INTERCOM_CLIENT_ID"); clientID != "" {
		providers["intercom"] = &OAuthProvider{
			Name:         "intercom",
			ClientID:     clientID,
			ClientSecret: os.Getenv("INTERCOM_CLIENT_SECRET"),
			AuthURL:      "https://app.intercom.com/oauth",
			TokenURL:     "https://api.intercom.io/auth/eagle/token",
			Scopes:       "",
			Enabled:      true,
		}
	}

	// Mailchimp
	if clientID := os.Getenv("MAILCHIMP_CLIENT_ID"); clientID != "" {
		providers["mailchimp"] = &OAuthProvider{
			Name:         "mailchimp",
			ClientID:     clientID,
			ClientSecret: os.Getenv("MAILCHIMP_CLIENT_SECRET"),
			AuthURL:      "https://login.mailchimp.com/oauth2/authorize",
			TokenURL:     "https://login.mailchimp.com/oauth2/token",
			Scopes:       "",
			Enabled:      true,
		}
	}

	return &OAuthHandler{
		db:        db,
		providers: providers,
		registry:  nil, // legacy mode — no registry file
	}
}

// ListProviders returns available OAuth providers.
// When providers.json is loaded (registry != nil), the full list is derived from
// the file — enabled providers show their credentials are set, disabled ones show
// the env var names needed to activate them. New connectors added by the tool
// generator appear automatically without any code change.
func (h *OAuthHandler) ListProviders(c *gin.Context) {
	result := make([]gin.H, 0)

	if h.registry != nil {
		// Dynamic path: build list entirely from providers.json
		for id, entry := range h.registry {
			enabled := false
			if _, ok := h.providers[id]; ok {
				enabled = true
			}
			item := gin.H{
				"name":         id,
				"display_name": entry.Name,
				"category":     entry.Category,
				"enabled":      enabled,
				"scopes":       entry.Scopes,
			}
			if !enabled {
				item["message"] = fmt.Sprintf(
					"Configure %s and %s to enable",
					entry.ClientIDEnv, entry.ClientSecretEnv,
				)
			}
			result = append(result, item)
		}
		c.JSON(http.StatusOK, gin.H{"providers": result, "count": len(result)})
		return
	}

	// Legacy path: hardcoded provider metadata for backward compatibility
	providerMeta := map[string]map[string]string{
		"hubspot":    {"category": "crm", "display_name": "HubSpot"},
		"salesforce": {"category": "crm", "display_name": "Salesforce"},
		"pipedrive":  {"category": "crm", "display_name": "Pipedrive"},
		"zoho-crm":   {"category": "crm", "display_name": "Zoho CRM"},
		"slack":      {"category": "communication", "display_name": "Slack"},
		"github":     {"category": "devtools", "display_name": "GitHub"},
		"notion":     {"category": "productivity", "display_name": "Notion"},
		"jira":       {"category": "project_management", "display_name": "Jira"},
		"google":     {"category": "cloud", "display_name": "Google"},
		"dropbox":    {"category": "storage", "display_name": "Dropbox"},
		"stripe":     {"category": "payments", "display_name": "Stripe"},
		"intercom":   {"category": "support", "display_name": "Intercom"},
		"mailchimp":  {"category": "marketing", "display_name": "Mailchimp"},
	}
	allProviders := []string{
		"hubspot", "salesforce", "pipedrive", "zoho-crm",
		"slack", "github", "notion", "jira",
		"google", "dropbox", "stripe", "intercom", "mailchimp",
	}

	for _, provider := range h.providers {
		meta := providerMeta[provider.Name]
		if meta == nil {
			meta = map[string]string{"category": "other", "display_name": provider.Name}
		}
		result = append(result, gin.H{
			"name":         provider.Name,
			"display_name": meta["display_name"],
			"category":     meta["category"],
			"enabled":      provider.Enabled,
			"scopes":       provider.Scopes,
		})
	}
	for _, name := range allProviders {
		if _, exists := h.providers[name]; !exists {
			meta := providerMeta[name]
			if meta == nil {
				meta = map[string]string{"category": "other", "display_name": name}
			}
			envVar := strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
			result = append(result, gin.H{
				"name":         name,
				"display_name": meta["display_name"],
				"category":     meta["category"],
				"enabled":      false,
				"message":      fmt.Sprintf("Configure %s_CLIENT_ID and %s_CLIENT_SECRET to enable", envVar, envVar),
			})
		}
	}

	c.JSON(http.StatusOK, gin.H{"providers": result, "count": len(result)})
}

// Authorize initiates OAuth flow
func (h *OAuthHandler) Authorize(c *gin.Context) {
	providerName := c.Param("provider")
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	// Resolve the provider: operator-env app first, then the caller's per-user
	// BYO app (oauth_apps) combined with providers.json endpoints. This is what
	// lets a generated/un-provisioned provider be connected without an env edit.
	provider, exists := h.resolveProvider(providerName, userID)
	if !exists {
		// Not configured in this environment (no operator env credentials AND no
		// BYO app). 400 (not 404) so the UI can prompt the user to add an app.
		c.JSON(http.StatusBadRequest, gin.H{
			"error":    "provider_disabled",
			"message":  oauthDisabledMessage(providerName),
			"provider": providerName,
			"enabled":  false,
		})
		return
	}

	// Generate state token for CSRF protection
	state, err := generateState()
	if err != nil {
		respondError(c, http.StatusInternalServerError, "state_generation_failed", "Failed to start OAuth flow", err)
		return
	}

	// Store state. For subdomain providers (e.g. Shopify) also persist the
	// shop so the callback can resolve {shop} in the token URL.
	subdomain := ""
	if strings.Contains(provider.AuthURL, "{shop}") {
		validShop, shopErr := sanitizeShopSubdomain(c.Query("shop"))
		if shopErr != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "shop_required",
				"message": "This provider requires a valid shop subdomain (letters, digits, and hyphens only). Pass ?shop=<your-store> in the authorize URL.",
			})
			return
		}
		subdomain = validShop
	}
	_, err = h.db.Exec(`
		INSERT INTO oauth_states (state, provider, user_id, subdomain, created_at, expires_at)
		VALUES ($1, $2, $3, $4, NOW(), NOW() + INTERVAL '10 minutes')
		ON CONFLICT (state) DO NOTHING
	`, state, providerName, userID, subdomain)
	if err != nil {
		// Ignore error - table might not exist yet
	}

	// Build authorization URL
	callbackURL := os.Getenv("OAUTH_CALLBACK_URL")
	if callbackURL == "" {
		callbackURL = "http://localhost:5001/oauth/callback"
	}

	// Substitute {shop} placeholder for providers that require a per-shop subdomain
	// (e.g. Shopify: https://{shop}.myshopify.com/admin/oauth/authorize).
	// The shop name is passed as a query param: GET /oauth/shopify/authorize?shop=mystore
	// {shop} was validated and normalized above (persisted as subdomain); reuse it.
	baseAuthURL := provider.AuthURL
	if strings.Contains(baseAuthURL, "{shop}") {
		baseAuthURL = strings.ReplaceAll(baseAuthURL, "{shop}", subdomain)
	}

	authURL := fmt.Sprintf("%s?client_id=%s&redirect_uri=%s&scope=%s&state=%s&response_type=code",
		baseAuthURL,
		url.QueryEscape(provider.ClientID),
		url.QueryEscape(callbackURL+"/"+providerName),
		url.QueryEscape(provider.Scopes),
		state,
	)

	// For Google, add access_type for refresh token
	if providerName == "google" {
		authURL += "&access_type=offline&prompt=consent"
	}

	c.JSON(http.StatusOK, gin.H{
		"authorization_url": authURL,
		"state":             state,
		"provider":          providerName,
	})
}

// Callback handles OAuth callback
func (h *OAuthHandler) Callback(c *gin.Context) {
	providerName := c.Param("provider")
	code := c.Query("code")
	state := c.Query("state")
	errorMsg := c.Query("error")

	if errorMsg != "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":       "oauth_error",
			"message":     c.Query("error_description"),
			"oauth_error": errorMsg,
		})
		return
	}

	if code == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "missing_code",
			"message": "Authorization code not provided",
		})
		return
	}

	// Verify state and get user_id + subdomain (for per-shop providers like
	// Shopify). The provider is resolved AFTER this so a per-user BYO app (keyed
	// on user_id) can supply the client credentials for generated providers.
	var userID string
	var subdomain sql.NullString
	err := h.db.QueryRow("SELECT user_id, COALESCE(subdomain,'') FROM oauth_states WHERE state = $1 AND provider = $2 AND expires_at > NOW()", state, providerName).Scan(&userID, &subdomain)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "invalid_state",
			"message": "Invalid or expired state parameter",
		})
		return
	}
	if err != nil {
		respondError(c, http.StatusInternalServerError, "state_check_failed", "Failed to validate OAuth state", err)
		return
	}

	provider, exists := h.resolveProvider(providerName, userID)
	if !exists {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":    "provider_disabled",
			"message":  oauthDisabledMessage(providerName),
			"provider": providerName,
			"enabled":  false,
		})
		return
	}

	// Delete used state
	if _, derr := h.db.Exec("DELETE FROM oauth_states WHERE state = $1", state); derr != nil {
		// Best-effort cleanup; don't fail OAuth callback if deletion fails.
		fmt.Printf("⚠️  Failed to delete oauth state %s: %v\n", state, derr)
	}

	// Exchange code for token (pass shop subdomain for per-shop providers like Shopify)
	token, err := h.exchangeToken(provider, code, providerName, subdomain.String)
	if err != nil {
		respondError(c, http.StatusInternalServerError, "token_exchange_failed", "Failed to exchange OAuth token", err)
		return
	}

	// Store token in database, scoped to the caller from oauth_states.
	tokenID, err := h.storeToken(providerName, userID, token)
	if err != nil {
		respondError(c, http.StatusInternalServerError, "token_storage_failed", "Failed to store OAuth token", err)
		return
	}

	// Get account info
	accountInfo, _ := h.getAccountInfo(providerName, token.AccessToken)

	// Return HTML to notify opener.
	//
	// IMPORTANT: do NOT create a connection record here.
	// The UI creates a connection via the standard /connections API using oauth_token_id,
	// so users can choose name/type/sync_mode and we avoid duplicate connections.
	// Notify the opener over THREE channels so success is delivered even when
	// window.opener is null. Identity providers like Shopify admin set
	// Cross-Origin-Opener-Policy on their authorize page, which severs the
	// opener link for the rest of the popup's lifetime — so an unguarded
	// window.opener.postMessage() throws, window.close() never runs, and the
	// app never hears back. BroadcastChannel + localStorage are same-origin
	// (the app and this callback share an origin in prod/staging behind the
	// proxy) and survive the opener being severed.
	c.Header("Content-Type", "text/html")
	c.String(http.StatusOK, fmt.Sprintf(`<!doctype html>
		<html>
		<body>
			<script>
				(function () {
					var msg = {
						type: 'oauth_success',
						tokenId: '%s',
						token_id: '%s',
						provider: '%s',
						state: '%s',
						account_name: '%s'
					};
					// 1) Same-origin BroadcastChannel — survives COOP severing window.opener.
					try { var bc = new BroadcastChannel('oauth_result'); bc.postMessage(msg); } catch (e) {}
					// 2) localStorage fallback — fires a 'storage' event in the opener tab (same-origin).
					try { localStorage.setItem('oauth_result', JSON.stringify(msg)); } catch (e) {}
					// 3) Classic opener postMessage — works when the opener link is intact.
					try { if (window.opener) { window.opener.postMessage(msg, '*'); } } catch (e) {}
					try { window.close(); } catch (e) {}
				})();
			</script>
			<h1>Connected!</h1>
			<p>You have successfully connected to %s. You can close this window.</p>
		</body>
		</html>
	`, tokenID, tokenID, providerName, state, accountInfo["name"], providerName))
}

// buildTokenExchangeData builds the form-encoded body for the OAuth token
// exchange, honoring the provider's grant type. This is the Go counterpart of
// the Python build_token_exchange_data helper (llm-service
// utils/oauth_exchange.py), kept as a pure function so it is unit-testable
// without performing a network call.
//
//   - authorization_code (the default when GrantType is empty — preserves the
//     legacy behavior for curated providers.json entries): the 3-legged flow
//     sends the authorization code and the redirect_uri it was issued against.
//     scope is NOT sent here; it was negotiated at the authorize step.
//   - client_credentials (2-legged, server-to-server): there is no user redirect,
//     so code and redirect_uri MUST be omitted or the token endpoint rejects the
//     request. scope is sent only when the provider declares one.
func buildTokenExchangeData(provider *OAuthProvider, code, redirectURI string) url.Values {
	grantType := provider.GrantType
	if grantType == "" {
		grantType = "authorization_code"
	}

	data := url.Values{}
	data.Set("grant_type", grantType)
	data.Set("client_id", provider.ClientID)
	data.Set("client_secret", provider.ClientSecret)

	if grantType == "client_credentials" {
		if provider.Scopes != "" {
			data.Set("scope", provider.Scopes)
		}
		return data
	}

	// authorization_code and any other 3-legged grant: include code + redirect.
	if code != "" {
		data.Set("code", code)
	}
	if redirectURI != "" {
		data.Set("redirect_uri", redirectURI)
	}
	return data
}

// exchangeToken exchanges authorization code for access token
func (h *OAuthHandler) exchangeToken(provider *OAuthProvider, code string, providerName string, subdomain string) (*OAuthToken, error) {
	callbackURL := os.Getenv("OAUTH_CALLBACK_URL")
	if callbackURL == "" {
		callbackURL = "http://localhost:5001/oauth/callback"
	}

	// Resolve {shop} in the token URL for per-shop providers (e.g. Shopify).
	// Re-validate the subdomain (defense in depth) so a malformed stored value
	// cannot inject an arbitrary host into the token URL.
	tokenURL := provider.TokenURL
	if strings.Contains(tokenURL, "{shop}") {
		validShop, shopErr := sanitizeShopSubdomain(subdomain)
		if shopErr != nil {
			return nil, fmt.Errorf("invalid shop subdomain for token exchange: %w", shopErr)
		}
		tokenURL = strings.ReplaceAll(tokenURL, "{shop}", validShop)
	}

	data := buildTokenExchangeData(provider, code, callbackURL+"/"+providerName)

	req, err := http.NewRequest("POST", tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	// SSRF-guarded client: refuses internal/link-local targets, pins DNS.
	client := safehttp.NewClient(30 * time.Second)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token exchange failed: %s", string(body))
	}

	var tokenResp map[string]interface{}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		// GitHub returns data as form-encoded
		values, err := url.ParseQuery(string(body))
		if err != nil {
			return nil, fmt.Errorf("failed to parse token response: %s", string(body))
		}
		tokenResp = make(map[string]interface{})
		for k, v := range values {
			if len(v) > 0 {
				tokenResp[k] = v[0]
			}
		}
	}

	accessToken, _ := tokenResp["access_token"].(string)
	refreshToken, _ := tokenResp["refresh_token"].(string)
	tokenType, _ := tokenResp["token_type"].(string)

	// Calculate expiry.
	//
	// Some providers (e.g. Shopify offline tokens) issue access tokens that
	// never expire and carry no refresh_token. For those we must NOT stamp the
	// default 1-hour expiry: the orchestrator's TokenManager treats expires_at
	// as a hard deadline and the background refresher skips tokens with no
	// refresh_token, so a bogus 1h expiry would silently break the connection
	// ~1 hour after connecting with no way to recover. Stamp a far-future
	// expiry so such tokens are treated as live indefinitely. The migration
	// keeps expires_at NOT NULL, so we use a sentinel timestamp rather than NULL.
	_, hasExpiresIn := tokenResp["expires_in"].(float64)
	neverExpires := h.registry[providerName].TokenNeverExpires || (!hasExpiresIn && refreshToken == "")

	var expiresAt time.Time
	if neverExpires {
		expiresAt = time.Now().AddDate(100, 0, 0)
	} else {
		expiresIn := 3600 // Default 1 hour
		if ei, ok := tokenResp["expires_in"].(float64); ok {
			expiresIn = int(ei)
		}
		expiresAt = time.Now().Add(time.Duration(expiresIn) * time.Second)
	}

	return &OAuthToken{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		TokenType:    tokenType,
		ExpiresAt:    expiresAt,
		Scopes:       provider.Scopes,
	}, nil
}

// storeToken stores the OAuth token in database, scoped to the user
// who initiated the OAuth flow (from oauth_states.user_id).
//
// userID MUST be the same userID that was on the oauth_states row
// consumed by the calling /oauth/callback handler. Empty userID is
// refused at the SQL layer (NOT NULL on the column after migration
// 055 backfills, FK references users(id)). Older callers that pass
// empty string will get a constraint-violation error — surface it
// rather than silently writing an orphan row.
func (h *OAuthHandler) storeToken(provider, userID string, token *OAuthToken) (string, error) {
	if strings.TrimSpace(userID) == "" {
		return "", fmt.Errorf("storeToken: userID is required (every oauth token MUST be scoped to its owning user — see security audit T1-1)")
	}
	tokenID := generateTokenID()

	// Encrypt tokens using string helpers
	encryptedAccess, err := crypto.EncryptString(token.AccessToken)
	if err != nil {
		return "", err
	}

	// Store NULL (not empty string) when no refresh_token — an empty string
	// passes the IS NOT NULL check in SQL and misleads callers into thinking
	// a refresh token exists. Shopify offline tokens never include one.
	var encryptedRefreshPtr *string
	if token.RefreshToken != "" {
		encryptedRefresh, err := crypto.EncryptString(token.RefreshToken)
		if err != nil {
			return "", err
		}
		encryptedRefreshPtr = &encryptedRefresh
	}

	// Try to store in database
	_, err = h.db.ExecContext(context.Background(), `
		INSERT INTO oauth_tokens (id, provider, user_id, access_token, refresh_token, token_type, expires_at, scopes, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())
	`, tokenID, provider, userID, encryptedAccess, encryptedRefreshPtr, token.TokenType, token.ExpiresAt, token.Scopes)
	if err != nil {
		// Table might not exist - return ID anyway
		return tokenID, nil
	}

	return tokenID, nil
}

// getAccountInfo gets account information from provider
func (h *OAuthHandler) getAccountInfo(provider string, accessToken string) (map[string]interface{}, error) {
	var userInfoURL string
	var authHeader string

	switch provider {
	case "github":
		base := strings.TrimSpace(os.Getenv("GITHUB_API_BASE_URL"))
		if base != "" {
			userInfoURL = strings.TrimRight(base, "/") + "/user"
		} else {
			userInfoURL = "https://api.github.com/user"
		}
		authHeader = "token " + accessToken
	case "google":
		userInfoURL = "https://www.googleapis.com/oauth2/v2/userinfo"
		authHeader = "Bearer " + accessToken
	case "hubspot":
		userInfoURL = "https://api.hubapi.com/oauth/v1/access-tokens/" + accessToken
		authHeader = ""
	case "salesforce":
		userInfoURL = "https://login.salesforce.com/services/oauth2/userinfo"
		authHeader = "Bearer " + accessToken
	case "slack":
		userInfoURL = "https://slack.com/api/auth.test"
		authHeader = "Bearer " + accessToken
	case "notion":
		userInfoURL = "https://api.notion.com/v1/users/me"
		authHeader = "Bearer " + accessToken
	case "jira":
		userInfoURL = "https://api.atlassian.com/me"
		authHeader = "Bearer " + accessToken
	case "stripe":
		userInfoURL = "https://api.stripe.com/v1/account"
		authHeader = "Bearer " + accessToken
	case "dropbox":
		userInfoURL = "https://api.dropboxapi.com/2/users/get_current_account"
		authHeader = "Bearer " + accessToken
	case "intercom":
		userInfoURL = "https://api.intercom.io/me"
		authHeader = "Bearer " + accessToken
	default:
		return map[string]interface{}{"name": "Unknown"}, nil
	}

	req, err := http.NewRequest("GET", userInfoURL, nil)
	if err != nil {
		return nil, err
	}

	// Dropbox requires POST for user info
	if provider == "dropbox" {
		req.Method = "POST"
	}

	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	req.Header.Set("Accept", "application/json")

	// Notion requires version header
	if provider == "notion" {
		req.Header.Set("Notion-Version", "2022-06-28")
	}

	client := safehttp.NewClient(10 * time.Second)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	// Normalize response
	info := make(map[string]interface{})
	switch provider {
	case "github":
		info["id"] = result["id"]
		info["name"] = result["login"]
		info["email"] = result["email"]
	case "google":
		info["id"] = result["id"]
		info["name"] = result["name"]
		info["email"] = result["email"]
	case "hubspot":
		info["id"] = result["hub_id"]
		info["name"] = result["hub_domain"]
	case "salesforce":
		info["id"] = result["user_id"]
		info["name"] = result["name"]
		info["email"] = result["email"]
	case "slack":
		info["id"] = result["user_id"]
		info["name"] = result["user"]
		info["team"] = result["team"]
	case "notion":
		info["id"] = result["id"]
		info["name"] = result["name"]
	case "jira":
		info["id"] = result["account_id"]
		info["name"] = result["name"]
		info["email"] = result["email"]
	case "stripe":
		info["id"] = result["id"]
		info["name"] = result["business_profile"]
	case "dropbox":
		info["id"] = result["account_id"]
		if name, ok := result["name"].(map[string]interface{}); ok {
			info["name"] = name["display_name"]
		}
		info["email"] = result["email"]
	case "intercom":
		info["id"] = result["id"]
		info["name"] = result["name"]
		info["email"] = result["email"]
	default:
		info = result
	}

	return info, nil
}

// ListTokens returns the OAuth tokens owned by the caller.
//
// T1-1: pre-fix this was tenant-blind — any authed user could list
// every other tenant's OAuth credentials, see provider+account name,
// and call RefreshToken/RevokeToken on those IDs. Scoped by user_id
// post-migration 055.
func (h *OAuthHandler) ListTokens(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	rows, err := h.db.Query(`
		SELECT id, provider, token_type, expires_at, scopes, account_name, created_at, updated_at
		FROM oauth_tokens
		WHERE user_id = $1
		ORDER BY created_at DESC
	`, userID)
	if err != nil {
		// Table might not exist
		c.JSON(http.StatusOK, gin.H{
			"tokens": []gin.H{},
			"count":  0,
		})
		return
	}
	defer rows.Close()

	tokens := make([]gin.H, 0)
	for rows.Next() {
		var t OAuthToken
		var accountName sql.NullString
		if err := rows.Scan(&t.ID, &t.Provider, &t.TokenType, &t.ExpiresAt, &t.Scopes, &accountName, &t.CreatedAt, &t.UpdatedAt); err != nil {
			continue
		}

		expired := time.Now().After(t.ExpiresAt)

		tokens = append(tokens, gin.H{
			"id":           t.ID,
			"provider":     t.Provider,
			"token_type":   t.TokenType,
			"expires_at":   t.ExpiresAt,
			"expired":      expired,
			"scopes":       t.Scopes,
			"account_name": accountName.String,
			"created_at":   t.CreatedAt,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"tokens": tokens,
		"count":  len(tokens),
	})
}

// RefreshTokenByID is a service-internal refresh that does not require a user/Gin context.
// It is safe to call from background goroutines and from enrichConfigWithOAuthToken.
//
// Returns nil when the token was refreshed OR when it is still fresh (idempotent).
// Returns ErrTokenFresh when >5 min remain (caller may treat as success).
// Returns ErrNoRefreshToken when no refresh_token is stored (re-auth required).
// Returns a wrapped error for hard failures (provider call, crypto, DB).
//
// The pg_try_advisory_lock pattern ensures only one concurrent refresh per tokenID
// across all api-gateway instances. If another goroutine/instance holds the lock
// this call returns nil immediately — the other refresh will complete and update the DB.
func (h *OAuthHandler) RefreshTokenByID(ctx context.Context, tokenID string) error {
	lockKey := int64(crc32Hash(tokenID))

	var lockAcquired bool
	if err := h.db.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, lockKey).Scan(&lockAcquired); err != nil {
		return fmt.Errorf("RefreshTokenByID: failed to acquire lock for %s: %w", tokenID, err)
	}
	if !lockAcquired {
		// Another instance is already refreshing this token; it will update the DB.
		log.Debugf("oauth: RefreshTokenByID skipped %s — lock held by another process", tokenID)
		return nil
	}
	defer h.db.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, lockKey)

	// Re-fetch under lock — another goroutine may have refreshed already.
	var provider, encryptedRefresh, encryptedAccess string
	var expiresAt time.Time
	err := h.db.QueryRowContext(ctx, `
		SELECT provider, access_token, refresh_token, expires_at
		FROM oauth_tokens
		WHERE id = $1
	`, tokenID).Scan(&provider, &encryptedAccess, &encryptedRefresh, &expiresAt)
	if err != nil {
		return fmt.Errorf("RefreshTokenByID: token %s not found: %w", tokenID, err)
	}

	// Idempotent: if another goroutine refreshed before we got the lock, bail out.
	if time.Now().Add(5 * time.Minute).Before(expiresAt) {
		return ErrTokenFresh
	}

	if strings.TrimSpace(encryptedRefresh) == "" {
		return ErrNoRefreshToken
	}

	refreshToken, err := crypto.DecryptString(encryptedRefresh)
	if err != nil {
		return fmt.Errorf("RefreshTokenByID: decrypt refresh_token for %s: %w", tokenID, err)
	}

	providerConfig, exists := h.providers[provider]
	if !exists {
		return fmt.Errorf("RefreshTokenByID: provider %q not configured (token %s)", provider, tokenID)
	}

	// Call provider token endpoint.
	data := url.Values{}
	data.Set("client_id", providerConfig.ClientID)
	data.Set("client_secret", providerConfig.ClientSecret)
	data.Set("refresh_token", refreshToken)
	data.Set("grant_type", "refresh_token")

	req, err := http.NewRequestWithContext(ctx, "POST", providerConfig.TokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return fmt.Errorf("RefreshTokenByID: build request for %s: %w", tokenID, err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	httpClient := safehttp.NewClient(30 * time.Second)
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("RefreshTokenByID: provider call for %s: %w", tokenID, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		// HTTP 400 (invalid_grant) or 401 means the refresh token itself is expired/revoked.
		// Return a typed sentinel so the background refresher can stop retrying.
		if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnauthorized {
			return fmt.Errorf("%w: provider=%s status=%d body=%s", ErrRefreshTokenExpired, provider, resp.StatusCode, string(body))
		}
		return fmt.Errorf("RefreshTokenByID: provider returned %d for %s: %s", resp.StatusCode, tokenID, string(body))
	}

	var tokenResp map[string]interface{}
	json.Unmarshal(body, &tokenResp)

	newAccessToken, _ := tokenResp["access_token"].(string)
	if strings.TrimSpace(newAccessToken) == "" {
		return fmt.Errorf("RefreshTokenByID: provider did not return access_token for %s", tokenID)
	}

	newRefreshToken := refreshToken // keep existing unless provider rotates it
	if rt, ok := tokenResp["refresh_token"].(string); ok && rt != "" {
		newRefreshToken = rt
	}

	expiresIn := 3600
	if ei, ok := tokenResp["expires_in"].(float64); ok {
		expiresIn = int(ei)
	}
	newExpiresAt := time.Now().Add(time.Duration(expiresIn) * time.Second)

	encryptedAccessNew, err := crypto.EncryptString(newAccessToken)
	if err != nil {
		return fmt.Errorf("RefreshTokenByID: encrypt access_token for %s: %w", tokenID, err)
	}
	encryptedRefreshNew, err := crypto.EncryptString(newRefreshToken)
	if err != nil {
		return fmt.Errorf("RefreshTokenByID: encrypt refresh_token for %s: %w", tokenID, err)
	}

	if _, err := h.db.ExecContext(ctx, `
		UPDATE oauth_tokens
		SET access_token = $1, refresh_token = $2, expires_at = $3, updated_at = NOW()
		WHERE id = $4
	`, encryptedAccessNew, encryptedRefreshNew, newExpiresAt, tokenID); err != nil {
		return fmt.Errorf("RefreshTokenByID: persist for %s: %w", tokenID, err)
	}

	log.Infof("oauth: refreshed token %s (provider=%s, new_expires_at=%s)", tokenID, provider, newExpiresAt.UTC().Format(time.RFC3339))
	return nil
}

// RefreshToken is the HTTP handler for POST /api/v1/oauth/tokens/:token_id/refresh.
// It enforces per-user ownership (T1-1) then delegates to RefreshTokenByID.
func (h *OAuthHandler) RefreshToken(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	tokenID := c.Param("token_id")

	// Ownership check: verify the token belongs to this user before doing anything.
	// Use the same opaque 404 for "missing" and "other tenant's token" to avoid leaking existence.
	var tokenExists bool
	if err := h.db.QueryRowContext(c.Request.Context(),
		`SELECT EXISTS(SELECT 1 FROM oauth_tokens WHERE id = $1 AND user_id = $2)`,
		tokenID, userID,
	).Scan(&tokenExists); err != nil || !tokenExists {
		c.JSON(http.StatusNotFound, gin.H{
			"error":   "token_not_found",
			"message": "OAuth token not found",
		})
		return
	}

	err := h.RefreshTokenByID(c.Request.Context(), tokenID)
	switch {
	case err == nil, errors.Is(err, ErrTokenFresh):
		// Success — re-read expires_at so the response is accurate.
		var newExpiresAt time.Time
		h.db.QueryRowContext(c.Request.Context(),
			`SELECT expires_at FROM oauth_tokens WHERE id = $1`, tokenID,
		).Scan(&newExpiresAt)
		c.JSON(http.StatusOK, gin.H{
			"success":    true,
			"token_id":   tokenID,
			"expires_at": newExpiresAt,
			"refreshed":  err == nil, // false when ErrTokenFresh (already fresh)
		})
	case errors.Is(err, ErrNoRefreshToken):
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "no_refresh_token",
			"message": "Token does not have a refresh token; re-authenticate via the OAuth flow",
		})
	default:
		log.WithError(err).Warnf("oauth: RefreshToken HTTP handler error for %s", tokenID)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "refresh_failed",
			"message": err.Error(),
		})
	}
}

// RevokeToken revokes/deletes an OAuth token
func (h *OAuthHandler) RevokeToken(c *gin.Context) {
	tokenID := c.Param("token_id")
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	// Fetch token details — MUST be scoped to caller. Pre-fix the
	// caller's user_id was only enforced on the cascading connections
	// update below, while the DELETE FROM oauth_tokens itself was
	// keyed by ID alone — so any tenant could delete any other
	// tenant's OAuth credentials. T1-1.
	var provider, encryptedAccess, encryptedRefresh string
	err := h.db.QueryRow(`
		SELECT provider, access_token, refresh_token
		FROM oauth_tokens
		WHERE id = $1 AND user_id = $2
	`, tokenID, userID).Scan(&provider, &encryptedAccess, &encryptedRefresh)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{
			"error":   "token_not_found",
			"message": "OAuth token not found",
		})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "fetch_failed",
			"message": err.Error(),
		})
		return
	}

	// Best-effort provider revocation (if provider supports it)
	providerConfig, exists := h.providers[provider]
	if exists && encryptedAccess != "" {
		accessToken, decErr := crypto.DecryptString(encryptedAccess)
		if decErr == nil {
			// Attempt revocation at provider (many OAuth providers support revocation)
			// This is best-effort - we still delete from our DB even if this fails
			revocationURL := getRevocationURL(provider, providerConfig)
			if revocationURL != "" {
				revData := url.Values{}
				revData.Set("token", accessToken)
				revData.Set("client_id", providerConfig.ClientID)
				revData.Set("client_secret", providerConfig.ClientSecret)

				revReq, _ := http.NewRequest("POST", revocationURL, strings.NewReader(revData.Encode()))
				if revReq != nil {
					revReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
					revClient := safehttp.NewClient(10 * time.Second)
					revResp, revErr := revClient.Do(revReq)
					if revErr != nil {
						// Log but don't fail - we'll still delete from our DB
						fmt.Printf("⚠️  Provider revocation failed for %s: %v\n", provider, revErr)
					} else {
						revResp.Body.Close()
						if revResp.StatusCode < 200 || revResp.StatusCode >= 300 {
							fmt.Printf("⚠️  Provider revocation returned HTTP %d for %s\n", revResp.StatusCode, provider)
						}
					}
				}
			}
		}
	}

	// Delete token from database — scoped to caller (T1-1).
	result, err := h.db.Exec(`DELETE FROM oauth_tokens WHERE id = $1 AND user_id = $2`, tokenID, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "revoke_failed",
			"message": err.Error(),
		})
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{
			"error":   "token_not_found",
			"message": "OAuth token not found",
		})
		return
	}

	// Update any connections using this token to null out oauth_token_id
	// (Alternative: delete those connections entirely - your choice)
	_, connErr := h.db.Exec(`
		UPDATE connections 
		SET oauth_token_id = NULL, status = 'disconnected', updated_at = NOW()
		WHERE oauth_token_id = $1 AND user_id = $2
	`, tokenID, userID)

	if connErr != nil {
		fmt.Printf("⚠️  Failed to update connections for revoked token %s: %v\n", tokenID, connErr)
	}

	c.JSON(http.StatusOK, gin.H{
		"success":  true,
		"token_id": tokenID,
		"message":  "Token revoked and connections updated",
	})
}

// Helper functions
func generateState() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(bytes), nil
}

func generateTokenID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return fmt.Sprintf("oauth-%x", bytes)
}

// crc32Hash converts a string to a 32-bit hash suitable for Postgres advisory locks
func crc32Hash(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}

// getRevocationURL returns the token revocation URL for a provider (if supported)
func getRevocationURL(provider string, config *OAuthProvider) string {
	// Most OAuth 2.0 providers support token revocation (RFC 7009)
	// but each has a different endpoint
	revocationURLs := map[string]string{
		"github":     "https://api.github.com/applications/{client_id}/token",
		"google":     "https://oauth2.googleapis.com/revoke",
		"salesforce": config.TokenURL + "/revoke", // Relative to token URL
		"slack":      "https://slack.com/api/auth.revoke",
		"hubspot":    "https://api.hubapi.com/oauth/v1/refresh-tokens/{token}",
		// pipedrive, stripe, others: no standard revocation endpoint
	}

	url, exists := revocationURLs[provider]
	if !exists {
		return ""
	}

	// Replace placeholders if needed
	url = strings.ReplaceAll(url, "{client_id}", config.ClientID)
	return url
}
