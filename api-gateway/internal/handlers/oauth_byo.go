package handlers

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/rsync-ai/shared/crypto"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// userOAuthApp holds a user's "bring your own" (BYO) OAuth application
// credentials, as stored in oauth_apps. ClientSecret is decrypted on read.
type userOAuthApp struct {
	ClientID     string
	ClientSecret string
	Scopes       string
}

// oauthCallbackBase returns the external OAuth callback base URL (no provider
// suffix). Mirrors the inline fallback used by Authorize/exchangeToken so all
// three agree on the redirect base.
func oauthCallbackBase() string {
	base := os.Getenv("OAUTH_CALLBACK_URL")
	if base == "" {
		base = "http://localhost:5001/oauth/callback"
	}
	return base
}

// oauthRedirectURI returns the full redirect/callback URL for a provider — the
// exact value the user must register on their own OAuth app at the vendor. The
// vendor rejects the authorize request unless the redirect_uri byte-matches a
// pre-registered value, so surfacing this string is the key BYO UX affordance.
func oauthRedirectURI(providerName string) string {
	return oauthCallbackBase() + "/" + providerName
}

// buildBYOProvider combines a provider's endpoints (from providers.json, via the
// registry) with a user's own client credentials, producing an enabled provider
// the standard Authorize/exchange path can use unchanged. A per-app scope
// override wins over the registry default.
func buildBYOProvider(providerName string, entry oauthRegistryEntry, app *userOAuthApp) *OAuthProvider {
	scopes := entry.Scopes
	if app.Scopes != "" {
		scopes = app.Scopes
	}
	return &OAuthProvider{
		Name:         providerName,
		ClientID:     app.ClientID,
		ClientSecret: app.ClientSecret,
		AuthURL:      entry.AuthURL,
		TokenURL:     entry.TokenURL,
		Scopes:       scopes,
		GrantType:    entry.GrantType,
		Enabled:      true,
	}
}

// loadUserOAuthApp loads a user's BYO OAuth app for a provider, decrypting the
// client_secret. Returns (nil, nil) when the user has no app for the provider.
func (h *OAuthHandler) loadUserOAuthApp(userID, provider string) (*userOAuthApp, error) {
	if h.db == nil || strings.TrimSpace(userID) == "" {
		return nil, nil
	}
	var clientID, secretEnc string
	var scopes sql.NullString
	err := h.db.QueryRow(
		`SELECT client_id, client_secret, scopes FROM oauth_apps WHERE user_id = $1 AND provider = $2`,
		userID, provider,
	).Scan(&clientID, &secretEnc, &scopes)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	secret, err := crypto.DecryptString(secretEnc)
	if err != nil {
		return nil, fmt.Errorf("decrypt client_secret for provider %s: %w", provider, err)
	}
	return &userOAuthApp{ClientID: clientID, ClientSecret: secret, Scopes: scopes.String}, nil
}

// resolveProvider returns the OAuthProvider to use for (provider, user).
// Precedence:
//  1. operator-env provider (h.providers, populated from <PROV>_CLIENT_ID/_SECRET
//     at boot) — the curated path, unchanged.
//  2. per-user BYO app (oauth_apps) combined with the provider's endpoints from
//     providers.json — activates generated/un-provisioned providers with no env
//     edit and no restart.
//
// Returns (nil, false) when neither exists (the caller renders provider_disabled).
func (h *OAuthHandler) resolveProvider(providerName, userID string) (*OAuthProvider, bool) {
	if p, ok := h.providers[providerName]; ok && p.Enabled {
		return p, true
	}
	if h.registry == nil {
		return nil, false
	}
	entry, ok := h.registry[providerName]
	if !ok {
		return nil, false
	}
	app, err := h.loadUserOAuthApp(userID, providerName)
	if err != nil {
		log.Warnf("oauth: failed to load BYO app for user=%s provider=%s: %v", userID, providerName, err)
		return nil, false
	}
	if app == nil {
		return nil, false
	}
	return buildBYOProvider(providerName, entry, app), true
}

// --- HTTP handlers for BYO OAuth apps -----------------------------------------

type saveOAuthAppRequest struct {
	Provider     string `json:"provider"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	Scopes       string `json:"scopes"`
}

// SaveOAuthApp upserts the caller's BYO OAuth app for a provider, encrypting the
// client_secret at rest. POST /api/v1/oauth/apps
func (h *OAuthHandler) SaveOAuthApp(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	var req saveOAuthAppRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request", "message": "Invalid JSON body"})
		return
	}
	req.Provider = strings.TrimSpace(req.Provider)
	req.ClientID = strings.TrimSpace(req.ClientID)
	req.ClientSecret = strings.TrimSpace(req.ClientSecret)
	if req.Provider == "" || req.ClientID == "" || req.ClientSecret == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing_fields", "message": "provider, client_id and client_secret are required"})
		return
	}
	// BYO is only meaningful for a provider whose endpoints we know
	// (providers.json). Refuse unknown providers rather than store dangling creds.
	if h.registry == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "byo_unsupported", "message": "BYO OAuth apps require providers.json (registry mode)"})
		return
	}
	if _, known := h.registry[req.Provider]; !known {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unknown_provider", "message": fmt.Sprintf("Unknown OAuth provider %q", req.Provider), "provider": req.Provider})
		return
	}

	secretEnc, err := crypto.EncryptString(req.ClientSecret)
	if err != nil {
		respondError(c, http.StatusInternalServerError, "encrypt_failed", "Failed to secure client secret", err)
		return
	}

	_, err = h.db.Exec(`
		INSERT INTO oauth_apps (user_id, provider, client_id, client_secret, scopes, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
		ON CONFLICT (user_id, provider) DO UPDATE SET
			client_id = EXCLUDED.client_id,
			client_secret = EXCLUDED.client_secret,
			scopes = EXCLUDED.scopes,
			updated_at = NOW()
	`, userID, req.Provider, req.ClientID, secretEnc, req.Scopes)
	if err != nil {
		respondError(c, http.StatusInternalServerError, "save_failed", "Failed to save OAuth app", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"provider":     req.Provider,
		"client_id":    req.ClientID,
		"redirect_uri": oauthRedirectURI(req.Provider),
		"configured":   true,
	})
}

// GetOAuthApp returns the BYO-app status for a provider for the caller — never
// the secret. Includes the redirect_uri the user must register plus the grant
// type and default scopes so the UI can render the form.
// GET /api/v1/oauth/apps/:provider
func (h *OAuthHandler) GetOAuthApp(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	provider := c.Param("provider")

	resp := gin.H{
		"provider":     provider,
		"redirect_uri": oauthRedirectURI(provider),
		"configured":   false,
		"known":        false,
	}
	if h.registry != nil {
		if entry, known := h.registry[provider]; known {
			resp["known"] = true
			resp["grant_type"] = entry.GrantType
			resp["default_scopes"] = entry.Scopes
			if _, envEnabled := h.providers[provider]; envEnabled {
				// Operator already provisioned a shared app — BYO not required.
				resp["operator_managed"] = true
			}
		}
	}

	app, err := h.loadUserOAuthApp(userID, provider)
	if err != nil {
		// This is a STATUS endpoint. A failure loading the per-user BYO app
		// (missing oauth_apps table, decrypt error, transient DB error) must NOT
		// 500 — that would block the entire connect modal. Degrade to "not
		// configured": the known / operator_managed signals above still stand,
		// and the BYO form re-appears so the user can (re-)enter credentials.
		log.WithError(err).WithField("provider", provider).
			Warn("GetOAuthApp: BYO app load failed; degrading to not-configured")
		c.JSON(http.StatusOK, resp)
		return
	}
	if app != nil {
		resp["configured"] = true
		resp["client_id"] = app.ClientID
		if app.Scopes != "" {
			resp["scopes"] = app.Scopes
		}
	}
	c.JSON(http.StatusOK, resp)
}

// DeleteOAuthApp removes the caller's BYO app for a provider.
// DELETE /api/v1/oauth/apps/:provider
func (h *OAuthHandler) DeleteOAuthApp(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	provider := c.Param("provider")
	if _, err := h.db.Exec(`DELETE FROM oauth_apps WHERE user_id = $1 AND provider = $2`, userID, provider); err != nil {
		respondError(c, http.StatusInternalServerError, "delete_failed", "Failed to delete OAuth app", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"provider": provider, "configured": false})
}
