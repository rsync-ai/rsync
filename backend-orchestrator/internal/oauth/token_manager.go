package oauth

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/rsync-ai/shared/crypto"
)

// OAuthToken represents an OAuth token from the database
type OAuthToken struct {
	ID           string
	Provider     string
	AccessToken  string
	RefreshToken string
	ExpiresAt    time.Time
	UpdatedAt    time.Time
}

// TokenManager handles OAuth token fetching, decryption, and caching
type TokenManager struct {
	db          *sql.DB
	cache       sync.Map // map[tokenID]*cachedToken
	mu          sync.Mutex
	refreshFunc func(ctx context.Context, tokenID string) error // optional; set via SetRefreshFunc
}

// SetRefreshFunc registers a callback that TokenManager calls asynchronously when a cached
// token is within 30 minutes of expiry. The callback should call the api-gateway refresh
// endpoint. Wire it up at startup: tokenManager.SetRefreshFunc(refreshClient.RefreshTokenViaAPIGateway)
func (tm *TokenManager) SetRefreshFunc(f func(ctx context.Context, tokenID string) error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.refreshFunc = f
}

// cachedToken holds a decrypted access token with expiry info
type cachedToken struct {
	AccessToken string
	ExpiresAt   time.Time
	CachedAt    time.Time
}

// NewTokenManager creates a new token manager
func NewTokenManager(db *sql.DB) *TokenManager {
	tm := &TokenManager{
		db: db,
	}
	
	// Start cache cleanup goroutine
	go tm.cleanupExpiredCache()
	
	return tm
}

// GetAccessToken retrieves and decrypts an access token, using cache when possible.
// If a refreshFunc is registered and the token expires within 30 minutes, a background
// refresh is triggered asynchronously (non-blocking) so the next call gets a fresh token.
func (tm *TokenManager) GetAccessToken(tokenID string) (string, error) {
	// Check cache first
	if cached, ok := tm.cache.Load(tokenID); ok {
		ct := cached.(*cachedToken)

		cacheAge := time.Since(ct.CachedAt)
		timeUntilExpiry := time.Until(ct.ExpiresAt)

		// Cache is valid if entry is <5 min old AND token has >5 min remaining.
		// 5-minute buffer (up from 1 min) ensures we evict before the api-gateway's
		// proactive refresh window kicks in, avoiding serving a near-dead token.
		if cacheAge < 5*time.Minute && timeUntilExpiry > 5*time.Minute {
			// Trigger async refresh when token is within 30 min of expiry so the
			// background refresher in api-gateway has a parallel signal from the
			// orchestrator side as well.
			if timeUntilExpiry < 30*time.Minute {
				tm.triggerAsyncRefresh(tokenID)
			}
			return ct.AccessToken, nil
		}

		tm.cache.Delete(tokenID)
	}

	// Fetch from database.
	var encryptedAccess string
	var expiresAt time.Time

	err := tm.db.QueryRow(`
		SELECT access_token, expires_at
		FROM oauth_tokens
		WHERE id = $1
	`, tokenID).Scan(&encryptedAccess, &expiresAt)

	if err == sql.ErrNoRows {
		return "", fmt.Errorf("oauth token not found: %s", tokenID)
	}
	if err != nil {
		return "", fmt.Errorf("failed to fetch oauth token: %w", err)
	}

	timeUntilExpiry := time.Until(expiresAt)

	// Hard expiry: token is already expired or within 1 minute.
	// Return an error so the caller (executor) knows it must trigger a full refresh+retry.
	if timeUntilExpiry < 1*time.Minute {
		return "", fmt.Errorf("oauth token expired or expiring in <1 min (expires at %v); refresh required", expiresAt)
	}

	// Soft near-expiry: trigger an async refresh for next time.
	if timeUntilExpiry < 30*time.Minute {
		tm.triggerAsyncRefresh(tokenID)
	}

	accessToken, err := crypto.DecryptString(encryptedAccess)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt access token: %w", err)
	}

	tm.cache.Store(tokenID, &cachedToken{
		AccessToken: accessToken,
		ExpiresAt:   expiresAt,
		CachedAt:    time.Now(),
	})

	return accessToken, nil
}

// triggerAsyncRefresh fires a non-blocking goroutine to refresh the token via api-gateway.
// Called when a cached token is within 30 minutes of expiry, so by the next pipeline
// execution the fresh token is already in the DB and cache.
func (tm *TokenManager) triggerAsyncRefresh(tokenID string) {
	tm.mu.Lock()
	f := tm.refreshFunc
	tm.mu.Unlock()
	if f == nil {
		return
	}
	go func() {
		if err := f(context.Background(), tokenID); err != nil {
			log.Debugf("TokenManager: async refresh for %s: %v", tokenID, err)
		} else {
			tm.InvalidateCache(tokenID) // next GetAccessToken picks up fresh token from DB
		}
	}()
}

// InvalidateCache removes a token from the cache (called after refresh or revocation)
func (tm *TokenManager) InvalidateCache(tokenID string) {
	tm.cache.Delete(tokenID)
}

// cleanupExpiredCache periodically removes expired entries from cache
func (tm *TokenManager) cleanupExpiredCache() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	for range ticker.C {
		tm.cache.Range(func(key, value interface{}) bool {
			ct := value.(*cachedToken)
			
			// Remove if cache is stale (>5 min) or token expired
			cacheAge := time.Since(ct.CachedAt)
			timeUntilExpiry := time.Until(ct.ExpiresAt)
			
			if cacheAge > 5*time.Minute || timeUntilExpiry < 5*time.Minute {
				tm.cache.Delete(key)
			}
			
			return true
		})
	}
}

