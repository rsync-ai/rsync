package oauth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

// RefreshClient handles OAuth token refresh via API Gateway
type RefreshClient struct {
	apiGatewayURL string
	httpClient    *http.Client
	tokenManager  *TokenManager
}

// NewRefreshClient creates a new OAuth refresh client
func NewRefreshClient(apiGatewayURL string, tokenManager *TokenManager) *RefreshClient {
	return &RefreshClient{
		apiGatewayURL: apiGatewayURL,
		httpClient:    &http.Client{Timeout: 30 * time.Second},
		tokenManager:  tokenManager,
	}
}

// RefreshTokenViaAPIGateway refreshes an OAuth token via the API Gateway internal endpoint.
func (c *RefreshClient) RefreshTokenViaAPIGateway(ctx context.Context, tokenID string) error {
	if tokenID == "" {
		return fmt.Errorf("token_id is empty")
	}

	secret := os.Getenv("INTERNAL_SERVICE_SECRET")

	refreshURL := fmt.Sprintf("%s/api/v1/internal/oauth/tokens/%s/refresh", c.apiGatewayURL, tokenID)

	log.Debugf("🔄 Refreshing OAuth token via API Gateway: %s", tokenID)

	req, err := http.NewRequestWithContext(ctx, "POST", refreshURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create refresh request: %w", err)
	}
	if secret != "" {
		req.Header.Set("X-Internal-Secret", secret)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("refresh request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("refresh failed: HTTP %d - %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("failed to parse refresh response: %w", err)
	}

	if success, ok := result["success"].(bool); !ok || !success {
		return fmt.Errorf("refresh unsuccessful: %v", result)
	}

	// Invalidate local cache so next fetch gets the fresh token
	c.tokenManager.InvalidateCache(tokenID)
	
	log.Debugf("✅ OAuth token refreshed successfully: %s", tokenID)
	
	return nil
}

// RefreshTokenResponse represents the API Gateway refresh response
type RefreshTokenResponse struct {
	Success   bool      `json:"success"`
	TokenID   string    `json:"token_id"`
	ExpiresAt time.Time `json:"expires_at"`
	Message   string    `json:"message,omitempty"`
}

