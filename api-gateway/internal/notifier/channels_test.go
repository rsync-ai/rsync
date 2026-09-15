package notifier

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rsync-ai/shared/crypto"
)

const testEncryptionKey = "unit-test-encryption-key-0123456789ab"

var channelColumns = []string{
	"slack_enabled", "slack_webhook_encrypted", "slack_muted_categories",
	"email_enabled", "smtp_host", "smtp_port", "smtp_username", "smtp_password_encrypted",
	"smtp_from", "smtp_tls_mode", "updated_at",
}

func clearNotifierEnv(t *testing.T) {
	for _, k := range []string{"NOTIFIER_SLACK_WEBHOOK_URL", "SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASSWORD", "SMTP_FROM"} {
		t.Setenv(k, "")
	}
}

func TestEnvChannelConfig(t *testing.T) {
	clearNotifierEnv(t)
	cfg := EnvChannelConfig()
	if cfg.Source != SourceEnvironment || cfg.Slack.Enabled || cfg.Email.Enabled {
		t.Fatalf("empty env should configure nothing: %+v", cfg)
	}

	t.Setenv("NOTIFIER_SLACK_WEBHOOK_URL", " https://hooks.slack.com/services/x ")
	t.Setenv("SMTP_HOST", "smtp.example.com")
	t.Setenv("SMTP_PORT", "not-a-port")
	t.Setenv("SMTP_FROM", "alerts@example.com")
	cfg = EnvChannelConfig()
	if !cfg.Slack.Enabled || cfg.Slack.WebhookURL != "https://hooks.slack.com/services/x" {
		t.Errorf("slack = %+v", cfg.Slack)
	}
	if !cfg.Email.Enabled || cfg.Email.Port != defaultSMTPPort || cfg.Email.TLSMode != TLSModeOpportunistic {
		t.Errorf("email = %+v", cfg.Email)
	}

	t.Setenv("SMTP_FROM", "")
	if EnvChannelConfig().Email.Enabled {
		t.Error("email without a from address must not be enabled")
	}
}

func TestLoadChannelConfigFromDatabase(t *testing.T) {
	t.Setenv("ENCRYPTION_KEY", testEncryptionKey)
	clearNotifierEnv(t)
	t.Setenv("NOTIFIER_SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/from-env")

	webhook, err := crypto.EncryptString("https://hooks.slack.com/services/from-db")
	if err != nil {
		t.Fatal(err)
	}
	password, err := crypto.EncryptString("hunter2")
	if err != nil {
		t.Fatal(err)
	}
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnRows(
		sqlmock.NewRows(channelColumns).AddRow(
			true, webhook, "health,data_loss",
			false, "smtp.example.com", 465, "user", password,
			"alerts@example.com", TLSModeTLS, time.Now()))

	cfg, err := LoadChannelConfig(context.Background(), mockDB)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Source != SourceDatabase {
		t.Errorf("source = %s", cfg.Source)
	}
	if cfg.Slack.WebhookURL != "https://hooks.slack.com/services/from-db" {
		t.Errorf("the saved row must override the env var, got webhook %q", cfg.Slack.WebhookURL)
	}
	if !reflect.DeepEqual(cfg.Slack.Muted, []string{"health", "data_loss"}) {
		t.Errorf("muted = %v", cfg.Slack.Muted)
	}
	if cfg.Email.Enabled || cfg.Email.Password != "hunter2" || cfg.Email.Port != 465 || cfg.Email.TLSMode != TLSModeTLS {
		t.Errorf("email = %+v", cfg.Email)
	}
}

// A secret that no longer decrypts (ENCRYPTION_KEY changed) must keep the
// channel enabled so every send fails loudly, not turn into "not configured".
func TestStoredSecretThatWillNotDecryptFailsLoudly(t *testing.T) {
	t.Setenv("ENCRYPTION_KEY", testEncryptionKey)
	cfg := StoredChannelSettings{
		SlackEnabled:          true,
		SlackWebhookEncrypted: "enc.v1:gone:AAAA",
		EmailEnabled:          true,
		SMTPPasswordEncrypted: "enc.v1:gone:AAAA",
	}.Decrypt()
	if !cfg.Slack.Enabled || cfg.Slack.SecretErr == nil {
		t.Errorf("slack = %+v", cfg.Slack)
	}
	if !cfg.Email.Enabled || cfg.Email.SecretErr == nil {
		t.Errorf("email = %+v", cfg.Email)
	}
}

func TestLoadChannelConfigFallsBackToEnv(t *testing.T) {
	clearNotifierEnv(t)
	t.Setenv("SMTP_HOST", "smtp.example.com")
	t.Setenv("SMTP_FROM", "alerts@example.com")

	for name, dbErr := range map[string]error{
		"no row saved":            sql.ErrNoRows,
		"migration 102 never ran": &pgconn.PgError{Code: "42P01"},
	} {
		t.Run(name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer mockDB.Close()
			mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnError(dbErr)
			cfg, err := LoadChannelConfig(context.Background(), mockDB)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Source != SourceEnvironment || !cfg.Email.Enabled {
				t.Errorf("want the env config, got %+v", cfg)
			}
		})
	}

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnError(errors.New("connection reset"))
	if _, err := LoadChannelConfig(context.Background(), mockDB); err == nil {
		t.Error("a real read failure must not be mistaken for 'no row'")
	}
}

func TestCachedChannelConfigServesLastGoodOnRefreshError(t *testing.T) {
	clearNotifierEnv(t)
	t.Setenv("SMTP_HOST", "smtp.example.com")
	t.Setenv("SMTP_FROM", "alerts@example.com")
	InvalidateChannelCache()
	t.Cleanup(InvalidateChannelCache)

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	ctx := context.Background()

	mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnError(sql.ErrNoRows)
	first, err := CachedChannelConfig(ctx, mockDB)
	if err != nil || !first.Email.Enabled {
		t.Fatalf("first load: %+v %v", first, err)
	}
	// Within the TTL: no query at all (sqlmock fails on an unexpected one).
	if _, err := CachedChannelConfig(ctx, mockDB); err != nil {
		t.Fatal(err)
	}

	// Expire it, then fail the refresh.
	channelCache.Lock()
	channelCache.loadedAt = time.Now().Add(-2 * channelCacheTTL)
	channelCache.Unlock()
	mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnError(errors.New("db down"))
	got, err := CachedChannelConfig(ctx, mockDB)
	if err != nil || !got.Email.Enabled {
		t.Errorf("a failed refresh should serve the last good config, got %+v %v", got, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestEmailPreferences(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	ctx := context.Background()
	const user = "11111111-1111-1111-1111-111111111111"

	mock.ExpectQuery(`FROM user_notification_preferences`).WithArgs(user).WillReturnError(sql.ErrNoRows)
	p, err := ReadEmailPreferences(ctx, mockDB, user)
	if err != nil || !p.Enabled || len(p.Muted) != 0 {
		t.Errorf("a user who never saved should get email on, nothing muted: %+v %v", p, err)
	}

	mock.ExpectQuery(`FROM user_notification_preferences`).WithArgs(user).WillReturnRows(
		sqlmock.NewRows([]string{"email_enabled", "muted"}).AddRow(true, "health"))
	p, err = ReadEmailPreferences(ctx, mockDB, user)
	if err != nil || !reflect.DeepEqual(p.Muted, []string{"health"}) {
		t.Errorf("prefs = %+v %v", p, err)
	}

	mock.ExpectExec(`INSERT INTO user_notification_preferences`).
		WithArgs(user, false, "health,run_status").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := WriteEmailPreferences(ctx, mockDB, user, EmailPreferences{Enabled: false, Muted: []string{"health", "run_status"}}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
