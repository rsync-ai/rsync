package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// Client is a minimal Slack Web API client. The only call the drift-approval
// receiver needs is users.info (to map a Slack user id to their verified email),
// so this stays deliberately small rather than pulling in a full SDK.
type Client struct {
	botToken   string
	httpClient *http.Client
	baseURL    string // overridable in tests
}

// NewClient builds a client bound to a bot token. The bot token needs the
// users:read.email scope for LookupUserEmail to return an address.
func NewClient(botToken string) *Client {
	return &Client{
		botToken:   botToken,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		baseURL:    "https://slack.com/api",
	}
}

// LookupUserEmail returns the Slack-verified email for a Slack user id via
// users.info. It fails closed: an API error, a missing scope, or an empty email
// all surface as an error / empty string so the caller refuses to approve
// rather than guessing an identity.
func (c *Client) LookupUserEmail(ctx context.Context, slackUserID string) (string, error) {
	if c.botToken == "" {
		return "", fmt.Errorf("slack: bot token not configured")
	}
	if slackUserID == "" {
		return "", fmt.Errorf("slack: empty user id")
	}

	endpoint := c.baseURL + "/users.info?" + url.Values{"user": {slackUserID}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.botToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var out struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
		User  struct {
			Profile struct {
				Email string `json:"email"`
			} `json:"profile"`
		} `json:"user"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if !out.OK {
		return "", fmt.Errorf("slack users.info error: %s", out.Error)
	}
	return out.User.Profile.Email, nil
}
