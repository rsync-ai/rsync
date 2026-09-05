// Package email provides a thin wrapper around the Resend API for transactional email.
// Resend docs: https://resend.com/docs/api-reference/emails/send-email
package email

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

const resendSendURL = "https://api.resend.com/emails"

// Client is a minimal Resend email sender. Zero-value is not usable — use New().
type Client struct {
	apiKey     string
	fromAddr   string
	httpClient *http.Client
}

// New returns a Client configured from environment variables.
//
//	RESEND_API_KEY   — required for sending; if empty, all Send calls are no-ops.
//	RESEND_FROM_ADDR — required for sending; must be an address on a domain that
//	                   the RESEND_API_KEY's own Resend account has verified.
//
// There is deliberately no default sender. A hardcoded fallback on our own domain
// reads as harmless but strands every self-hosted install that sets the API key and
// stops there: IsConfigured() reports true, signup starts gating on verification,
// and every send is rejected because the operator's Resend account has not verified
// our domain. The result is accounts created unverified and the one mail that would
// unlock them never arriving — visible only as an async log line. Requiring both
// values instead degrades that install to the well-trodden no-email path, where
// signups auto-verify and nobody is locked out.
//
// Callers should call IsConfigured() before relying on email delivery.
func New() *Client {
	return &Client{
		apiKey:   strings.TrimSpace(os.Getenv("RESEND_API_KEY")),
		fromAddr: strings.TrimSpace(os.Getenv("RESEND_FROM_ADDR")),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// isPlaceholderKey reports whether key is the placeholder shipped in .env.prod.example
// ("re_xxxxxxxxxxxx") rather than a real Resend key.
//
// The env template has always PROMISED this behaviour — "true ONLY if RESEND_API_KEY is
// set & non-placeholder", "if RESEND_API_KEY is unset OR left as the placeholder below,
// EVERY signup is auto-verified" — but nothing implemented it. So an operator who copied
// .env.prod.example, filled in their sender domain and left the key placeholder got the
// opposite of what the file told them: IsConfigured() true, signup gated on verification,
// and Resend rejecting every send with 401. Every new user was locked out of the install,
// and the only symptom was an async log line.
//
// Real keys are random, so an all-x body is unambiguously the placeholder. Keys that do
// not carry the "re_" prefix are left alone rather than second-guessed.
func isPlaceholderKey(key string) bool {
	body, ok := strings.CutPrefix(key, "re_")
	if !ok || body == "" {
		return false
	}
	return strings.Trim(strings.ToLower(body), "x") == ""
}

// IsConfigured reports whether this Client can actually deliver mail: it needs a real
// API key and a sender address. When false, all Send calls return nil without making
// any HTTP request.
func (c *Client) IsConfigured() bool {
	return c.apiKey != "" && !isPlaceholderKey(c.apiKey) && c.fromAddr != ""
}

// ConfigStatus renders the mail configuration as one startup log line. A
// half-configured install is otherwise completely silent — mail just never sends —
// and the operator has nothing to grep for. It names the variable that is missing,
// and never echoes the key.
func (c *Client) ConfigStatus() string {
	switch {
	case isPlaceholderKey(c.apiKey):
		return "email: Resend disabled — RESEND_API_KEY is still the example placeholder; " +
			"replace it with a real key or signups will keep auto-verifying"
	case c.apiKey != "" && c.fromAddr != "":
		return "email: Resend configured, sending as " + c.fromAddr
	case c.apiKey == "" && c.fromAddr == "":
		return "email: Resend not configured (RESEND_API_KEY and RESEND_FROM_ADDR are unset) — " +
			"signups auto-verify and no mail is sent"
	case c.apiKey == "":
		return "email: Resend disabled — RESEND_FROM_ADDR is set but RESEND_API_KEY is not; " +
			"signups auto-verify and no mail is sent"
	default:
		return "email: Resend disabled — RESEND_API_KEY is set but RESEND_FROM_ADDR is not; " +
			"set it to an address on a domain your own Resend account has verified, " +
			"or signups will keep auto-verifying"
	}
}

type sendRequest struct {
	From    string   `json:"from"`
	To      []string `json:"to"`
	Subject string   `json:"subject"`
	HTML    string   `json:"html"`
}

// Send dispatches a transactional email. It is a no-op when IsConfigured() is false.
func (c *Client) Send(ctx context.Context, to, subject, html string) error {
	if !c.IsConfigured() {
		return nil
	}

	body, err := json.Marshal(sendRequest{
		From:    c.fromAddr,
		To:      []string{to},
		Subject: subject,
		HTML:    html,
	})
	if err != nil {
		return fmt.Errorf("resend: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, resendSendURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("resend: create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("resend: http send: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errBody struct {
			Name    string `json:"name"`
			Message string `json:"message"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errBody)
		return fmt.Errorf("resend: API error %d: %s — %s", resp.StatusCode, errBody.Name, errBody.Message)
	}
	return nil
}

// SendVerification sends the email-verification link to addr.
// verifyURL should be the full URL: http://localhost:3000/verify-email?token=<token>
func (c *Client) SendVerification(ctx context.Context, toEmail, userName, verifyURL string) error {
	name := strings.TrimSpace(userName)
	if name == "" {
		name = "there"
	}

	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>Verify your rsync-ai email</title></head>
<body style="font-family:Arial,sans-serif;background:#f4f4f4;margin:0;padding:0">
  <table width="100%%" cellpadding="0" cellspacing="0" style="background:#f4f4f4;padding:40px 0">
    <tr><td align="center">
      <table width="520" cellpadding="0" cellspacing="0"
             style="background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08)">
        <tr><td style="background:#0f172a;padding:28px 40px">
          <span style="color:#ffffff;font-size:22px;font-weight:700;letter-spacing:-0.5px">rsync<span style="color:#6366f1">-ai</span></span>
        </td></tr>
        <tr><td style="padding:40px">
          <h1 style="margin:0 0 16px;font-size:22px;color:#0f172a">Verify your email address</h1>
          <p style="color:#374151;font-size:15px;line-height:1.6;margin:0 0 24px">
            Hi %s,<br><br>
            Thanks for signing up! Click the button below to verify your email address
            and start using rsync-ai.
          </p>
          <a href="%s"
             style="display:inline-block;background:#6366f1;color:#ffffff;font-size:15px;
                    font-weight:600;text-decoration:none;padding:14px 32px;border-radius:6px">
            Verify email address
          </a>
          <p style="color:#6b7280;font-size:13px;margin:28px 0 0;line-height:1.6">
            This link expires in <strong>24 hours</strong>. If you didn't create an
            rsync-ai account, you can safely ignore this email.
          </p>
          <p style="color:#9ca3af;font-size:12px;margin:16px 0 0">
            Or copy and paste this link into your browser:<br>
            <a href="%s" style="color:#6366f1;word-break:break-all">%s</a>
          </p>
        </td></tr>
        <tr><td style="background:#f9fafb;padding:20px 40px;border-top:1px solid #e5e7eb">
          <p style="color:#9ca3af;font-size:12px;margin:0">
            &copy; 2026 rsync-ai. All rights reserved.
          </p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>`, name, verifyURL, verifyURL, verifyURL)

	return c.Send(ctx, toEmail, "Verify your rsync-ai email address", html)
}

// SendWelcome sends the post-signup welcome email to a new user. It is a no-op
// when IsConfigured() is false. appURL is the product base URL (e.g.
// http://localhost:3000) used for the dashboard call-to-action.
func (c *Client) SendWelcome(ctx context.Context, toEmail, userName, appURL string) error {
	name := strings.TrimSpace(userName)
	if name == "" {
		name = "there"
	}
	if strings.TrimSpace(appURL) == "" {
		appURL = "http://localhost:3000"
	}

	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>Welcome to rsync-ai</title></head>
<body style="font-family:Arial,sans-serif;background:#f4f4f4;margin:0;padding:0">
  <table width="100%%" cellpadding="0" cellspacing="0" style="background:#f4f4f4;padding:40px 0">
    <tr><td align="center">
      <table width="520" cellpadding="0" cellspacing="0"
             style="background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08)">
        <tr><td style="background:#0f172a;padding:28px 40px">
          <span style="color:#ffffff;font-size:22px;font-weight:700;letter-spacing:-0.5px">rsync<span style="color:#6366f1">-ai</span></span>
        </td></tr>
        <tr><td style="padding:40px">
          <h1 style="margin:0 0 16px;font-size:22px;color:#0f172a">Welcome to rsync-ai 🎉</h1>
          <p style="color:#374151;font-size:15px;line-height:1.6;margin:0 0 16px">
            Hi %s,<br><br>
            Your account is ready. You're on the <strong>Free plan</strong> —
            connect a source, pick a destination, and build your first data
            pipeline in minutes.
          </p>
          <p style="color:#374151;font-size:15px;line-height:1.6;margin:0 0 24px">
            The Free plan lets you create up to <strong>2 pipelines</strong>, free
            for <strong>30 days</strong>. Upgrade to Pro any time for unlimited
            pipelines.
          </p>
          <a href="%s"
             style="display:inline-block;background:#6366f1;color:#ffffff;font-size:15px;
                    font-weight:600;text-decoration:none;padding:14px 32px;border-radius:6px">
            Open your dashboard
          </a>
          <p style="color:#6b7280;font-size:13px;margin:28px 0 0;line-height:1.6">
            Questions or feedback? Just reply to this email — we read every message.
          </p>
        </td></tr>
        <tr><td style="background:#f9fafb;padding:20px 40px;border-top:1px solid #e5e7eb">
          <p style="color:#9ca3af;font-size:12px;margin:0">
            &copy; 2026 rsync-ai. All rights reserved.
          </p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>`, name, appURL)

	return c.Send(ctx, toEmail, "Welcome to rsync-ai 🎉", html)
}

// SendNewSignupAdminAlert notifies admins that a new user has signed up. It is
// a no-op when IsConfigured() is false or no admin recipients are supplied.
// Each address receives its own message (Resend's /emails endpoint sends to the
// provided To list; we loop so one bad address can't drop the rest).
func (c *Client) SendNewSignupAdminAlert(ctx context.Context, adminEmails []string, newUserEmail, newUserName string) error {
	if !c.IsConfigured() || len(adminEmails) == 0 {
		return nil
	}
	name := strings.TrimSpace(newUserName)
	if name == "" {
		name = "(no name)"
	}

	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>New rsync-ai signup</title></head>
<body style="font-family:Arial,sans-serif;background:#f4f4f4;margin:0;padding:0">
  <table width="100%%" cellpadding="0" cellspacing="0" style="background:#f4f4f4;padding:40px 0">
    <tr><td align="center">
      <table width="520" cellpadding="0" cellspacing="0"
             style="background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08)">
        <tr><td style="background:#0f172a;padding:28px 40px">
          <span style="color:#ffffff;font-size:22px;font-weight:700;letter-spacing:-0.5px">rsync<span style="color:#6366f1">-ai</span></span>
        </td></tr>
        <tr><td style="padding:40px">
          <h1 style="margin:0 0 16px;font-size:20px;color:#0f172a">New user signup</h1>
          <p style="color:#374151;font-size:15px;line-height:1.6;margin:0 0 8px">
            A new user just registered on rsync-ai:
          </p>
          <table cellpadding="0" cellspacing="0" style="margin:8px 0 0;font-size:14px;color:#374151">
            <tr><td style="padding:4px 12px 4px 0;color:#6b7280">Name</td><td style="padding:4px 0"><strong>%s</strong></td></tr>
            <tr><td style="padding:4px 12px 4px 0;color:#6b7280">Email</td><td style="padding:4px 0"><strong>%s</strong></td></tr>
            <tr><td style="padding:4px 12px 4px 0;color:#6b7280">Plan</td><td style="padding:4px 0">Free Trial (14 days)</td></tr>
          </table>
        </td></tr>
        <tr><td style="background:#f9fafb;padding:20px 40px;border-top:1px solid #e5e7eb">
          <p style="color:#9ca3af;font-size:12px;margin:0">
            Automated notification from rsync-ai.
          </p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>`, name, newUserEmail)

	subject := fmt.Sprintf("New rsync-ai signup: %s", newUserEmail)
	var firstErr error
	for _, addr := range adminEmails {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		if err := c.Send(ctx, addr, subject, html); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// SendWorkspaceInvite delivers a workspace team-invite to toEmail with the accept
// link. It is a no-op when IsConfigured() is false (the inviter still gets the
// accept URL in the API response, which is the fallback delivery channel).
// acceptURL is the full frontend invite-landing link; role is the workspace role
// being offered (admin/member/viewer).
func (c *Client) SendWorkspaceInvite(ctx context.Context, toEmail, workspaceName, role, acceptURL string) error {
	ws := strings.TrimSpace(workspaceName)
	if ws == "" {
		ws = "a team workspace"
	}
	r := strings.TrimSpace(role)
	if r == "" {
		r = "member"
	}

	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>You're invited to join %s on rsync-ai</title></head>
<body style="font-family:Arial,sans-serif;background:#f4f4f4;margin:0;padding:0">
  <table width="100%%" cellpadding="0" cellspacing="0" style="background:#f4f4f4;padding:40px 0">
    <tr><td align="center">
      <table width="520" cellpadding="0" cellspacing="0"
             style="background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08)">
        <tr><td style="background:#0f172a;padding:28px 40px">
          <span style="color:#ffffff;font-size:22px;font-weight:700;letter-spacing:-0.5px">rsync<span style="color:#6366f1">-ai</span></span>
        </td></tr>
        <tr><td style="padding:40px">
          <h1 style="margin:0 0 16px;font-size:22px;color:#0f172a">You've been invited to join %s</h1>
          <p style="color:#374151;font-size:15px;line-height:1.6;margin:0 0 24px">
            You've been invited to collaborate on <strong>%s</strong> in rsync-ai as
            a <strong>%s</strong>. Accept the invitation to share the team's
            connections and data pipelines.
          </p>
          <a href="%s"
             style="display:inline-block;background:#6366f1;color:#ffffff;font-size:15px;
                    font-weight:600;text-decoration:none;padding:14px 32px;border-radius:6px">
            Accept invitation
          </a>
          <p style="color:#6b7280;font-size:13px;margin:28px 0 0;line-height:1.6">
            This invitation expires in <strong>7 days</strong>. If you weren't
            expecting it, you can safely ignore this email.
          </p>
          <p style="color:#9ca3af;font-size:12px;margin:16px 0 0">
            Or copy and paste this link into your browser:<br>
            <a href="%s" style="color:#6366f1;word-break:break-all">%s</a>
          </p>
        </td></tr>
        <tr><td style="background:#f9fafb;padding:20px 40px;border-top:1px solid #e5e7eb">
          <p style="color:#9ca3af;font-size:12px;margin:0">
            &copy; 2026 rsync-ai. All rights reserved.
          </p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>`, ws, ws, ws, r, acceptURL, acceptURL, acceptURL)

	return c.Send(ctx, toEmail, fmt.Sprintf("You're invited to join %s on rsync-ai", ws), html)
}

// SendUpgradeRequest notifies the team (salesEmail) that a user wants to upgrade
// to Pro, so the user never has to compose an email by hand. It is a no-op when
// IsConfigured() is false or salesEmail is empty. The message carries the
// requester's account details (their email is the reply-to contact) so the team
// can reach out directly.
func (c *Client) SendUpgradeRequest(ctx context.Context, salesEmail, userEmail, userName, userID, plan string) error {
	salesEmail = strings.TrimSpace(salesEmail)
	if !c.IsConfigured() || salesEmail == "" {
		return nil
	}
	name := strings.TrimSpace(userName)
	if name == "" {
		name = "(no name)"
	}
	pl := strings.TrimSpace(plan)
	if pl == "" {
		pl = "(unknown)"
	}

	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>Pro upgrade request</title></head>
<body style="font-family:Arial,sans-serif;background:#f4f4f4;margin:0;padding:0">
  <table width="100%%" cellpadding="0" cellspacing="0" style="background:#f4f4f4;padding:40px 0">
    <tr><td align="center">
      <table width="520" cellpadding="0" cellspacing="0"
             style="background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08)">
        <tr><td style="background:#0f172a;padding:28px 40px">
          <span style="color:#ffffff;font-size:22px;font-weight:700;letter-spacing:-0.5px">rsync<span style="color:#6366f1">-ai</span></span>
        </td></tr>
        <tr><td style="padding:40px">
          <h1 style="margin:0 0 16px;font-size:20px;color:#0f172a">Pro upgrade request</h1>
          <p style="color:#374151;font-size:15px;line-height:1.6;margin:0 0 8px">
            A user requested a Pro upgrade from the in-app dialog. Reach out to get them set up:
          </p>
          <table cellpadding="0" cellspacing="0" style="margin:8px 0 0;font-size:14px;color:#374151">
            <tr><td style="padding:4px 12px 4px 0;color:#6b7280">Name</td><td style="padding:4px 0"><strong>%s</strong></td></tr>
            <tr><td style="padding:4px 12px 4px 0;color:#6b7280">Email</td><td style="padding:4px 0"><a href="mailto:%s" style="color:#6366f1"><strong>%s</strong></a></td></tr>
            <tr><td style="padding:4px 12px 4px 0;color:#6b7280">Current plan</td><td style="padding:4px 0"><strong>%s</strong></td></tr>
            <tr><td style="padding:4px 12px 4px 0;color:#6b7280">User ID</td><td style="padding:4px 0"><code>%s</code></td></tr>
          </table>
        </td></tr>
        <tr><td style="background:#f9fafb;padding:20px 40px;border-top:1px solid #e5e7eb">
          <p style="color:#9ca3af;font-size:12px;margin:0">
            Automated notification from rsync-ai.
          </p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>`, name, userEmail, userEmail, pl, userID)

	subject := fmt.Sprintf("Pro upgrade request: %s", userEmail)
	return c.Send(ctx, salesEmail, subject, html)
}
