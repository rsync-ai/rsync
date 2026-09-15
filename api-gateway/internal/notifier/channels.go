package notifier

// Where alerts go: the instance-wide channel settings an admin saves in the UI
// (notification_channel_settings, migration 102), the env-var fallback that
// predates them, and each user's email category preferences.
//
// The DB row, when it exists, is the whole truth. When it does not, the
// NOTIFIER_SLACK_WEBHOOK_URL / SMTP_* env vars apply exactly as they did before
// the migration, so an env-configured deploy keeps alerting with no action.
//
// Arrays cross the driver as comma-joined strings (array_to_string /
// string_to_array) because this module runs on pgx and has no pq.Array. Category
// ids are [a-z_] and never contain a comma.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rsync-ai/shared/crypto"
	log "github.com/sirupsen/logrus"
)

const (
	SourceDatabase    = "database"
	SourceEnvironment = "environment"

	// TLSModeStartTLS requires STARTTLS: the send fails if the server does not offer it.
	TLSModeStartTLS = "starttls"
	// TLSModeTLS is implicit TLS from the first byte (usually port 465).
	TLSModeTLS = "tls"
	// TLSModeNone is plaintext, for a relay on a trusted network only.
	TLSModeNone = "none"
	// TLSModeOpportunistic exists only for the env-var path: STARTTLS when the
	// server offers it, plaintext when it does not. That is what smtp.SendMail
	// did before migration 102, so an env-configured deploy sends the way it
	// always has. An admin saving channels must pick one of the three above.
	TLSModeOpportunistic = "opportunistic"

	defaultSMTPPort = 587

	// channelCacheTTL bounds how stale the notifier's view of the settings can be.
	// A save invalidates the cache in the replica that served it; every other
	// api-gateway replica picks the change up within this window.
	channelCacheTTL = 30 * time.Second
)

// SlackChannel is the effective Slack configuration, secrets decrypted.
type SlackChannel struct {
	Enabled    bool
	WebhookURL string
	Muted      []string
	// SecretErr is set when the stored webhook could not be decrypted. The
	// channel stays Enabled so every send fails with this error on the
	// notification row. Treating it as "not configured" would turn a rotated
	// ENCRYPTION_KEY into alerts that silently stop.
	SecretErr error
}

// EmailChannel is the effective SMTP configuration, secrets decrypted.
type EmailChannel struct {
	Enabled   bool
	Host      string
	Port      int
	Username  string
	Password  string
	From      string
	TLSMode   string
	SecretErr error
}

// ChannelConfig is what the notifier delivers through.
type ChannelConfig struct {
	Source string
	Slack  SlackChannel
	Email  EmailChannel
}

// StoredChannelSettings is the notification_channel_settings row as stored:
// secrets are still ciphertext. Handlers read and write this shape so a secret
// never has to be decrypted just to show whether one is configured.
type StoredChannelSettings struct {
	SlackEnabled          bool
	SlackWebhookEncrypted string
	SlackMuted            []string

	EmailEnabled          bool
	SMTPHost              string
	SMTPPort              int
	SMTPUsername          string
	SMTPPasswordEncrypted string
	SMTPFrom              string
	SMTPTLSMode           string

	UpdatedAt time.Time
}

// ReadStoredChannelSettings returns the saved row, or nil when no admin has
// saved channels yet.
func ReadStoredChannelSettings(ctx context.Context, db *sql.DB) (*StoredChannelSettings, error) {
	var (
		s     StoredChannelSettings
		muted string
	)
	err := db.QueryRowContext(ctx, `
		SELECT slack_enabled, slack_webhook_encrypted, array_to_string(slack_muted_categories, ','),
		       email_enabled, smtp_host, smtp_port, smtp_username, smtp_password_encrypted,
		       smtp_from, smtp_tls_mode, updated_at
		FROM notification_channel_settings
		WHERE id = 1`,
	).Scan(&s.SlackEnabled, &s.SlackWebhookEncrypted, &muted,
		&s.EmailEnabled, &s.SMTPHost, &s.SMTPPort, &s.SMTPUsername, &s.SMTPPasswordEncrypted,
		&s.SMTPFrom, &s.SMTPTLSMode, &s.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) || isUndefinedTable(err) {
		// A missing table means migration 102 has not run. The migration runner
		// logs and starts anyway on failure, so this must read as "no row" (the
		// pre-migration behavior) rather than fail every alert.
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read notification channel settings: %w", err)
	}
	s.SlackMuted = splitIDList(muted)
	return &s, nil
}

// WriteStoredChannelSettings upserts the single row. updatedBy may be empty.
func WriteStoredChannelSettings(ctx context.Context, db *sql.DB, s StoredChannelSettings, updatedBy string) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO notification_channel_settings
			(id, slack_enabled, slack_webhook_encrypted, slack_muted_categories,
			 email_enabled, smtp_host, smtp_port, smtp_username, smtp_password_encrypted,
			 smtp_from, smtp_tls_mode, updated_by, updated_at)
		VALUES
			(1, $1, $2, string_to_array($3, ','), $4, $5, $6, $7, $8, $9, $10, NULLIF($11, '')::uuid, NOW())
		ON CONFLICT (id) DO UPDATE SET
			slack_enabled           = EXCLUDED.slack_enabled,
			slack_webhook_encrypted = EXCLUDED.slack_webhook_encrypted,
			slack_muted_categories  = EXCLUDED.slack_muted_categories,
			email_enabled           = EXCLUDED.email_enabled,
			smtp_host               = EXCLUDED.smtp_host,
			smtp_port               = EXCLUDED.smtp_port,
			smtp_username           = EXCLUDED.smtp_username,
			smtp_password_encrypted = EXCLUDED.smtp_password_encrypted,
			smtp_from               = EXCLUDED.smtp_from,
			smtp_tls_mode           = EXCLUDED.smtp_tls_mode,
			updated_by              = EXCLUDED.updated_by,
			updated_at              = NOW()`,
		s.SlackEnabled, s.SlackWebhookEncrypted, strings.Join(s.SlackMuted, ","),
		s.EmailEnabled, s.SMTPHost, s.SMTPPort, s.SMTPUsername, s.SMTPPasswordEncrypted,
		s.SMTPFrom, s.SMTPTLSMode, updatedBy)
	if err != nil {
		return fmt.Errorf("write notification channel settings: %w", err)
	}
	return nil
}

// Decrypt turns the stored row into an effective config.
func (s StoredChannelSettings) Decrypt() ChannelConfig {
	cfg := ChannelConfig{
		Source: SourceDatabase,
		Slack:  SlackChannel{Enabled: s.SlackEnabled, Muted: s.SlackMuted},
		Email: EmailChannel{
			Enabled:  s.EmailEnabled,
			Host:     s.SMTPHost,
			Port:     s.SMTPPort,
			Username: s.SMTPUsername,
			From:     s.SMTPFrom,
			TLSMode:  s.SMTPTLSMode,
		},
	}
	if s.SlackWebhookEncrypted != "" {
		if v, err := crypto.DecryptString(s.SlackWebhookEncrypted); err != nil {
			cfg.Slack.SecretErr = fmt.Errorf("the saved Slack webhook could not be decrypted (was ENCRYPTION_KEY changed?): %w", err)
		} else {
			cfg.Slack.WebhookURL = v
		}
	}
	if s.SMTPPasswordEncrypted != "" {
		if v, err := crypto.DecryptString(s.SMTPPasswordEncrypted); err != nil {
			cfg.Email.SecretErr = fmt.Errorf("the saved SMTP password could not be decrypted (was ENCRYPTION_KEY changed?): %w", err)
		} else {
			cfg.Email.Password = v
		}
	}
	return cfg
}

// EnvChannelConfig is the pre-migration-102 configuration, read from env vars.
func EnvChannelConfig() ChannelConfig {
	webhook := strings.TrimSpace(os.Getenv("NOTIFIER_SLACK_WEBHOOK_URL"))
	host := strings.TrimSpace(os.Getenv("SMTP_HOST"))
	from := strings.TrimSpace(os.Getenv("SMTP_FROM"))
	port, err := strconv.Atoi(strings.TrimSpace(os.Getenv("SMTP_PORT")))
	if err != nil || port < 1 || port > 65535 {
		port = defaultSMTPPort
	}
	return ChannelConfig{
		Source: SourceEnvironment,
		Slack:  SlackChannel{Enabled: webhook != "", WebhookURL: webhook},
		Email: EmailChannel{
			Enabled:  host != "" && from != "",
			Host:     host,
			Port:     port,
			Username: strings.TrimSpace(os.Getenv("SMTP_USER")),
			Password: strings.TrimSpace(os.Getenv("SMTP_PASSWORD")),
			From:     from,
			TLSMode:  TLSModeOpportunistic,
		},
	}
}

// LoadChannelConfig reads the effective config, bypassing the cache.
func LoadChannelConfig(ctx context.Context, db *sql.DB) (ChannelConfig, error) {
	if db == nil {
		return EnvChannelConfig(), nil
	}
	stored, err := ReadStoredChannelSettings(ctx, db)
	if err != nil {
		return ChannelConfig{}, err
	}
	if stored == nil {
		return EnvChannelConfig(), nil
	}
	return stored.Decrypt(), nil
}

var channelCache struct {
	sync.Mutex
	cfg      ChannelConfig
	loadedAt time.Time
	valid    bool
}

// CachedChannelConfig is LoadChannelConfig behind a channelCacheTTL cache. If a
// refresh fails, the last good config is served (with a warning) rather than
// failing every alert through a DB blip.
func CachedChannelConfig(ctx context.Context, db *sql.DB) (ChannelConfig, error) {
	channelCache.Lock()
	defer channelCache.Unlock()
	if channelCache.valid && time.Since(channelCache.loadedAt) < channelCacheTTL {
		return channelCache.cfg, nil
	}
	cfg, err := LoadChannelConfig(ctx, db)
	if err != nil {
		if channelCache.valid {
			log.WithError(err).Warn("notifier: could not refresh channel settings; using the last loaded settings")
			return channelCache.cfg, nil
		}
		return ChannelConfig{}, err
	}
	channelCache.cfg = cfg
	channelCache.loadedAt = time.Now()
	channelCache.valid = true
	return cfg, nil
}

// InvalidateChannelCache makes the next CachedChannelConfig re-read the DB.
func InvalidateChannelCache() {
	channelCache.Lock()
	channelCache.valid = false
	channelCache.Unlock()
}

// EmailPreferences is one user's email choices. The zero row is not the
// default; use DefaultEmailPreferences.
type EmailPreferences struct {
	Enabled bool
	Muted   []string
}

// DefaultEmailPreferences applies to a user who never saved preferences.
func DefaultEmailPreferences() EmailPreferences {
	return EmailPreferences{Enabled: true}
}

// ReadEmailPreferences returns the user's saved preferences, or the defaults.
func ReadEmailPreferences(ctx context.Context, db *sql.DB, userID string) (EmailPreferences, error) {
	var (
		p     EmailPreferences
		muted string
	)
	err := db.QueryRowContext(ctx, `
		SELECT email_enabled, array_to_string(email_muted_categories, ',')
		FROM user_notification_preferences
		WHERE user_id = $1::uuid`, userID,
	).Scan(&p.Enabled, &muted)
	if errors.Is(err, sql.ErrNoRows) || isUndefinedTable(err) {
		return DefaultEmailPreferences(), nil
	}
	if err != nil {
		return EmailPreferences{}, fmt.Errorf("read email preferences: %w", err)
	}
	p.Muted = splitIDList(muted)
	return p, nil
}

// WriteEmailPreferences upserts the user's preferences.
func WriteEmailPreferences(ctx context.Context, db *sql.DB, userID string, p EmailPreferences) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO user_notification_preferences (user_id, email_enabled, email_muted_categories, updated_at)
		VALUES ($1::uuid, $2, string_to_array($3, ','), NOW())
		ON CONFLICT (user_id) DO UPDATE SET
			email_enabled          = EXCLUDED.email_enabled,
			email_muted_categories = EXCLUDED.email_muted_categories,
			updated_at             = NOW()`,
		userID, p.Enabled, strings.Join(p.Muted, ","))
	if err != nil {
		return fmt.Errorf("write email preferences: %w", err)
	}
	return nil
}

func splitIDList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// isUndefinedTable reports a Postgres 42P01 (undefined_table).
func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}
