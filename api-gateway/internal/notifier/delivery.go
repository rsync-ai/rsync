package notifier

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/http"
	"net/mail"
	"net/smtp"
	"net/url"
	"strconv"
	"strings"
	"time"

	"api-gateway/internal/safehttp"
)

// Delivery status values written to pipeline_notifications.delivery_status.
const (
	StatusPending   = "pending"
	StatusDelivered = "delivered"
	StatusFailed    = "failed"
	// StatusSuppressed: no channel is configured at all. Persist-only mode.
	StatusSuppressed = "suppressed"
	// StatusSkipped: channels are configured, but every one that would have
	// carried this alert is muted for its category (or the owner turned email
	// off). Distinct from suppressed and failed on purpose: "you asked not to be
	// told" must never look like "we could not tell you".
	StatusSkipped = "skipped"
)

const (
	slackTimeout = 10 * time.Second
	smtpTimeout  = 30 * time.Second
)

// ErrChannelNotEnabled is returned by SendTestNotification for a channel that
// is not switched on.
var ErrChannelNotEnabled = errors.New("that channel is not enabled")

// smtpRootCAs overrides the system trust store. Tests only.
var smtpRootCAs *x509.CertPool

// deliveryPlan is which channels one alert goes to. status is set, and both
// channels are false, when nothing will be sent.
type deliveryPlan struct {
	slack  bool
	email  bool
	status string
}

// planDelivery applies the admin's Slack mutes and the owner's email choices to
// one alert. Pure, so the whole matrix is unit-tested.
func planDelivery(cfg ChannelConfig, category string, prefs EmailPreferences) deliveryPlan {
	if !cfg.Slack.Enabled && !cfg.Email.Enabled {
		return deliveryPlan{status: StatusSuppressed}
	}
	p := deliveryPlan{
		slack: cfg.Slack.Enabled && !mutedSet(cfg.Slack.Muted)[category],
		email: cfg.Email.Enabled && prefs.Enabled && !mutedSet(prefs.Muted)[category],
	}
	if !p.slack && !p.email {
		p.status = StatusSkipped
	}
	return p
}

// sendSlackWebhook posts one message. The client must be SSRF-guarded
// (safehttp) outside tests.
func sendSlackWebhook(ctx context.Context, client *http.Client, ch SlackChannel, payload map[string]interface{}) error {
	if ch.SecretErr != nil {
		return ch.SecretErr
	}
	if strings.TrimSpace(ch.WebhookURL) == "" {
		return errors.New("Slack is enabled but no webhook URL is configured")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode Slack payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ch.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return errors.New("the Slack webhook URL is not a valid URL")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		// *url.Error embeds the full request URL, and for an incoming webhook the
		// URL is the credential. This error is stored in delivery_error and
		// logged, so keep only the underlying cause.
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return fmt.Errorf("Slack webhook request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return nil
	}
	// Slack answers a bad webhook with a short plain-text reason
	// (invalid_token, no_service, channel_is_archived). That is what an admin
	// needs to see, so keep a bounded amount of it.
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
	if reason := strings.TrimSpace(string(snippet)); reason != "" {
		return fmt.Errorf("Slack webhook returned HTTP %d: %s", resp.StatusCode, reason)
	}
	return fmt.Errorf("Slack webhook returned HTTP %d", resp.StatusCode)
}

type emailMessage struct {
	Subject string
	Body    string
}

// sendSMTP delivers one plain-text message over a single SMTP session, with
// the TLS behavior the channel's mode asks for.
func sendSMTP(ctx context.Context, ch EmailChannel, to string, m emailMessage) error {
	if ch.SecretErr != nil {
		return ch.SecretErr
	}
	host := strings.TrimSpace(ch.Host)
	if host == "" || strings.TrimSpace(ch.From) == "" {
		return errors.New("email is enabled but the SMTP host or from address is missing")
	}
	fromAddr, err := mail.ParseAddress(ch.From)
	if err != nil {
		return fmt.Errorf("the from address %q is not a valid email address", ch.From)
	}
	toAddr, err := mail.ParseAddress(to)
	if err != nil {
		return errors.New("the recipient does not have a valid email address")
	}
	port := ch.Port
	if port == 0 {
		port = defaultSMTPPort
	}
	addr := net.JoinHostPort(host, strconv.Itoa(port))

	deadline := time.Now().Add(smtpTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	dialer := &net.Dialer{Deadline: deadline}
	tlsCfg := &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12, RootCAs: smtpRootCAs}

	var conn net.Conn
	switch ch.TLSMode {
	case TLSModeTLS:
		conn, err = (&tls.Dialer{NetDialer: dialer, Config: tlsCfg}).DialContext(ctx, "tcp", addr)
	case TLSModeStartTLS, TLSModeNone, TLSModeOpportunistic:
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	default:
		return fmt.Errorf("unknown SMTP TLS mode %q", ch.TLSMode)
	}
	if err != nil {
		return fmt.Errorf("could not connect to SMTP server %s: %w", addr, err)
	}
	_ = conn.SetDeadline(deadline)

	c, err := smtp.NewClient(conn, host)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("SMTP server %s did not greet: %w", addr, err)
	}
	defer c.Close()

	if ch.TLSMode == TLSModeStartTLS || ch.TLSMode == TLSModeOpportunistic {
		if ok, _ := c.Extension("STARTTLS"); ok {
			if err := c.StartTLS(tlsCfg); err != nil {
				return fmt.Errorf("SMTP STARTTLS failed: %w", err)
			}
		} else if ch.TLSMode == TLSModeStartTLS {
			return errors.New(`SMTP server does not offer STARTTLS; use TLS mode "tls" for port 465, or "none" only for a relay on a trusted network`)
		}
	}

	if ch.Username != "" && ch.Password != "" {
		if ok, _ := c.Extension("AUTH"); !ok {
			return errors.New("SMTP server does not accept authentication, but a username and password are configured")
		}
		// PlainAuth refuses to send credentials over an unencrypted connection to
		// anything but localhost, which is the behavior we want for mode "none".
		if err := c.Auth(smtp.PlainAuth("", ch.Username, ch.Password, host)); err != nil {
			return fmt.Errorf("SMTP authentication failed: %w", err)
		}
	}

	if err := c.Mail(fromAddr.Address); err != nil {
		return fmt.Errorf("SMTP server rejected the from address: %w", err)
	}
	if err := c.Rcpt(toAddr.Address); err != nil {
		return fmt.Errorf("SMTP server rejected the recipient: %w", err)
	}
	w, err := c.Data()
	if err != nil {
		return fmt.Errorf("SMTP DATA failed: %w", err)
	}
	msg, err := buildEmail(fromAddr, toAddr, m, time.Now())
	if err != nil {
		_ = w.Close()
		return err
	}
	if _, err := w.Write(msg); err != nil {
		_ = w.Close()
		return fmt.Errorf("SMTP DATA failed: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("SMTP server did not accept the message: %w", err)
	}
	// The message is accepted once DATA closes; a failed QUIT does not unsend it.
	_ = c.Quit()
	return nil
}

// buildEmail renders RFC 5322 headers and a quoted-printable UTF-8 body.
func buildEmail(from, to *mail.Address, m emailMessage, now time.Time) ([]byte, error) {
	var b bytes.Buffer
	header := func(k, v string) {
		b.WriteString(k + ": " + v + "\r\n")
	}
	header("From", from.String())
	header("To", to.String())
	header("Subject", mime.QEncoding.Encode("utf-8", singleLine(m.Subject)))
	header("Date", now.Format(time.RFC1123Z))
	header("MIME-Version", "1.0")
	header("Content-Type", "text/plain; charset=utf-8")
	header("Content-Transfer-Encoding", "quoted-printable")
	b.WriteString("\r\n")

	qp := quotedprintable.NewWriter(&b)
	body := strings.ReplaceAll(strings.ReplaceAll(m.Body, "\r\n", "\n"), "\n", "\r\n")
	if _, err := qp.Write([]byte(body)); err != nil {
		return nil, fmt.Errorf("encode email body: %w", err)
	}
	if err := qp.Close(); err != nil {
		return nil, fmt.Errorf("encode email body: %w", err)
	}
	return b.Bytes(), nil
}

// singleLine collapses every run of whitespace, CR and LF included, to one
// space. A header value built from a pipeline name or LLM-authored copy must
// not be able to start a new header.
func singleLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// SendTestNotification sends a test message through one channel using the
// current saved settings (uncached, so a save followed by a test sees the new
// values on any replica). Email goes to toEmail only.
func SendTestNotification(ctx context.Context, db *sql.DB, channel, toEmail string) error {
	cfg, err := LoadChannelConfig(ctx, db)
	if err != nil {
		return err
	}
	settingsURL := appBaseURLFromEnv() + "/admin"
	switch channel {
	case "slack":
		if !cfg.Slack.Enabled {
			return ErrChannelNotEnabled
		}
		return sendSlackWebhook(ctx, safehttp.NewClient(slackTimeout), cfg.Slack, map[string]interface{}{
			"text": "*rsync-ai test notification*\nSlack alerts are working for this rsync-ai instance. " +
				"Pipeline alerts will arrive in this channel. Manage them at " + settingsURL,
		})
	case "email":
		if !cfg.Email.Enabled {
			return ErrChannelNotEnabled
		}
		return sendSMTP(ctx, cfg.Email, toEmail, emailMessage{
			Subject: "[rsync-ai] Test notification",
			Body: "Email alerts are working for this rsync-ai instance.\n\n" +
				"Pipeline owners will receive alerts at their account email address, " +
				"and can choose which kinds they get in their settings.\n\n" +
				"Manage notification channels: " + settingsURL + "\n\n--\nAutomated message from rsync-ai notifier",
		})
	default:
		return fmt.Errorf("unknown channel %q", channel)
	}
}
