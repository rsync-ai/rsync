package handlers

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/rsync-ai/shared/crypto"

	"api-gateway/internal/notifier"
)

const notificationTestKey = "unit-test-encryption-key-0123456789ab"

var notificationChannelColumns = []string{
	"slack_enabled", "slack_webhook_encrypted", "slack_muted_categories",
	"email_enabled", "smtp_host", "smtp_port", "smtp_username", "smtp_password_encrypted",
	"smtp_from", "smtp_tls_mode", "email_muted_categories", "email_extra_recipients", "updated_at",
}

func clearNotificationEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"NOTIFIER_SLACK_WEBHOOK_URL", "SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASSWORD", "SMTP_FROM"} {
		t.Setenv(k, "")
	}
	notifier.InvalidateChannelCache()
	t.Cleanup(notifier.InvalidateChannelCache)
}

// capture records a string argument an Exec was called with.
type capture struct{ into *string }

func (c capture) Match(v driver.Value) bool {
	s, ok := v.(string)
	if ok {
		*c.into = s
	}
	return ok
}

func notificationRouter(userID string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		if userID != "" {
			c.Set("user_id", userID)
			c.Set("admin_user_id", userID)
			c.Set("admin_user_email", "admin@example.com")
		}
		c.Next()
	})
	r.GET("/admin/notifications/channels", AdminGetNotificationChannels)
	r.PUT("/admin/notifications/channels", AdminUpdateNotificationChannels)
	r.POST("/admin/notifications/test", AdminTestNotificationChannel)
	r.GET("/notifications/preferences", GetNotificationPreferences)
	r.PUT("/notifications/preferences", UpdateNotificationPreferences)
	return r
}

func doNotificationJSON(r *gin.Engine, method, path, body string) *httptest.ResponseRecorder {
	resp := httptest.NewRecorder()
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path, bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
	}
	r.ServeHTTP(resp, req)
	return resp
}

func TestAdminGetNotificationChannelsNeverReturnsSecrets(t *testing.T) {
	t.Setenv("ENCRYPTION_KEY", notificationTestKey)
	clearNotificationEnv(t)
	mock := withMockGlobalDB(t)

	webhook, _ := crypto.EncryptString("https://hooks.slack.com/services/T/B/topsecret")
	password, _ := crypto.EncryptString("hunter2")
	mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnRows(
		sqlmock.NewRows(notificationChannelColumns).AddRow(
			true, webhook, "health",
			true, "smtp.example.com", 587, "user", password,
			"alerts@example.com", "starttls", "schema_drift", "oncall@example.com,team@example.com", time.Now()))

	resp := doNotificationJSON(notificationRouter("admin-1"), http.MethodGet, "/admin/notifications/channels", "")
	if resp.Code != http.StatusOK {
		t.Fatalf("status %d: %s", resp.Code, resp.Body.String())
	}
	body := resp.Body.String()
	for _, secret := range []string{"topsecret", "hunter2", webhook, password} {
		if strings.Contains(body, secret) {
			t.Fatalf("response contains a secret (%q): %s", secret, body)
		}
	}
	var view notificationChannelsView
	if err := json.Unmarshal(resp.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if view.Source != "database" || !view.Slack.WebhookConfigured || !view.Email.PasswordConfigured {
		t.Errorf("view = %+v", view)
	}
	if view.Slack.Categories["health"] || !view.Slack.Categories["data_loss"] {
		t.Errorf("categories = %v, want health off and everything else on", view.Slack.Categories)
	}
	if view.Email.Categories["schema_drift"] || !view.Email.Categories["health"] {
		t.Errorf("email categories = %v, want schema_drift off and everything else on", view.Email.Categories)
	}
	if want := []string{"oncall@example.com", "team@example.com"}; strings.Join(view.Email.ExtraRecipients, ",") != strings.Join(want, ",") {
		t.Errorf("extra recipients = %v, want %v", view.Email.ExtraRecipients, want)
	}
}

func TestAdminGetNotificationChannelsReportsEnvFallback(t *testing.T) {
	clearNotificationEnv(t)
	t.Setenv("NOTIFIER_SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/envsecret")
	mock := withMockGlobalDB(t)
	mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnError(sql.ErrNoRows)

	resp := doNotificationJSON(notificationRouter("admin-1"), http.MethodGet, "/admin/notifications/channels", "")
	if resp.Code != http.StatusOK {
		t.Fatalf("status %d: %s", resp.Code, resp.Body.String())
	}
	if strings.Contains(resp.Body.String(), "envsecret") {
		t.Fatalf("response contains the env webhook: %s", resp.Body.String())
	}
	var view notificationChannelsView
	_ = json.Unmarshal(resp.Body.Bytes(), &view)
	if view.Source != "environment" || !view.Slack.Enabled || !view.Slack.WebhookConfigured {
		t.Errorf("view = %+v", view)
	}
	if !strings.Contains(resp.Body.String(), `"extra_recipients":[]`) {
		t.Errorf("extra_recipients should be an empty list, not null: %s", resp.Body.String())
	}
}

func TestAdminUpdateNotificationChannelsValidation(t *testing.T) {
	clearNotificationEnv(t)
	validEmail := `"email":{"enabled":false}`
	cases := map[string]string{
		"missing email section":         `{"slack":{"enabled":false}}`,
		"webhook on another host":       `{"slack":{"enabled":true,"webhook_url":"https://evil.example.com/services/x"},` + validEmail + `}`,
		"webhook over http":             `{"slack":{"enabled":true,"webhook_url":"http://hooks.slack.com/services/x"},` + validEmail + `}`,
		"webhook with userinfo":         `{"slack":{"enabled":true,"webhook_url":"https://a@hooks.slack.com/services/x"},` + validEmail + `}`,
		"webhook on an internal port":   `{"slack":{"enabled":true,"webhook_url":"https://hooks.slack.com:8080/services/x"},` + validEmail + `}`,
		"slack enabled with no webhook": `{"slack":{"enabled":true},` + validEmail + `}`,
		"unknown category":              `{"slack":{"enabled":false,"categories":{"everything":false}},` + validEmail + `}`,
		"email enabled without host":    `{"slack":{"enabled":false},"email":{"enabled":true,"from":"a@example.com"}}`,
		"bad from address":              `{"slack":{"enabled":false},"email":{"enabled":true,"smtp_host":"smtp.example.com","from":"nope"}}`,
		"host with a scheme":            `{"slack":{"enabled":false},"email":{"enabled":true,"smtp_host":"smtp://smtp.example.com","from":"a@example.com"}}`,
		"port out of range":             `{"slack":{"enabled":false},"email":{"enabled":false,"smtp_port":70000}}`,
		"unknown tls mode":              `{"slack":{"enabled":false},"email":{"enabled":false,"tls_mode":"ssl"}}`,
		"password over plaintext":       `{"slack":{"enabled":false},"email":{"enabled":true,"smtp_host":"smtp.example.com","from":"a@example.com","smtp_username":"u","smtp_password":"p","tls_mode":"none"}}`,
		"unknown email category":        `{"slack":{"enabled":false},"email":{"enabled":false,"categories":{"everything":false}}}`,
		"bad extra recipient":           `{"slack":{"enabled":false},"email":{"enabled":false,"extra_recipients":["not-an-address"]}}`,
		"extra recipient with a comma":  `{"slack":{"enabled":false},"email":{"enabled":false,"extra_recipients":["a@example.com, b@example.com"]}}`,
		"extra recipient with a CRLF":   `{"slack":{"enabled":false},"email":{"enabled":false,"extra_recipients":["a@example.com\r\nBcc: x@example.com"]}}`,
	}
	many := make([]string, maxEmailExtraRecipients+1)
	for i := range many {
		many[i] = `"user` + strings.Repeat("x", i) + `@example.com"`
	}
	cases["too many extra recipients"] = `{"slack":{"enabled":false},"email":{"enabled":false,"extra_recipients":[` + strings.Join(many, ",") + `]}}`
	t.Setenv("ENCRYPTION_KEY", notificationTestKey)
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			mock := withMockGlobalDB(t)
			mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnError(sql.ErrNoRows)
			resp := doNotificationJSON(notificationRouter("admin-1"), http.MethodPut, "/admin/notifications/channels", body)
			if resp.Code != http.StatusBadRequest {
				t.Errorf("status %d, want 400: %s", resp.Code, resp.Body.String())
			}
		})
	}
}

// First save on an env-configured instance: the webhook is new (encrypted, not
// stored in the clear), and the SMTP password the admin never re-typed is
// carried over from the env rather than silently dropped.
func TestAdminUpdateNotificationChannelsEncryptsAndInheritsSecrets(t *testing.T) {
	t.Setenv("ENCRYPTION_KEY", notificationTestKey)
	clearNotificationEnv(t)
	t.Setenv("SMTP_PASSWORD", "env-password")
	mock := withMockGlobalDB(t)

	mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnError(sql.ErrNoRows)
	var webhookCipher, muted, passwordCipher, tlsMode, emailMuted, extra string
	mock.ExpectExec(`INSERT INTO notification_channel_settings`).WithArgs(
		true, capture{&webhookCipher}, capture{&muted},
		true, "smtp.example.com", sqlmock.AnyArg(), "user", capture{&passwordCipher},
		"alerts@example.com", capture{&tlsMode}, capture{&emailMuted}, capture{&extra}, "admin-1",
	).WillReturnResult(sqlmock.NewResult(0, 1))

	const hook = "https://hooks.slack.com/services/T000/B000/abcdef"
	body := `{"slack":{"enabled":true,"webhook_url":"` + hook + `","categories":{"health":false,"data_loss":true}},
		"email":{"enabled":true,"smtp_host":" smtp.example.com ","smtp_username":"user","from":"alerts@example.com",
		"categories":{"schema_drift":false,"health":true},
		"extra_recipients":[" On-call <oncall@example.com> ","ONCALL@example.com","","team@example.com"]}}`
	resp := doNotificationJSON(notificationRouter("admin-1"), http.MethodPut, "/admin/notifications/channels", body)
	if resp.Code != http.StatusOK {
		t.Fatalf("status %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	if webhookCipher == "" || strings.Contains(webhookCipher, "abcdef") {
		t.Fatalf("webhook stored in the clear: %q", webhookCipher)
	}
	if got, err := crypto.DecryptString(webhookCipher); err != nil || got != hook {
		t.Errorf("webhook decrypts to %q (%v)", got, err)
	}
	if got, err := crypto.DecryptString(passwordCipher); err != nil || got != "env-password" {
		t.Errorf("password decrypts to %q (%v), want the inherited env password", got, err)
	}
	if muted != "health" || tlsMode != "starttls" {
		t.Errorf("muted=%q tls=%q", muted, tlsMode)
	}
	// Bare addresses, blanks dropped, de-duplicated case-insensitively.
	if emailMuted != "schema_drift" || extra != "oncall@example.com,team@example.com" {
		t.Errorf("email muted=%q extra recipients=%q", emailMuted, extra)
	}
	if strings.Contains(resp.Body.String(), "abcdef") || strings.Contains(resp.Body.String(), "env-password") {
		t.Errorf("response contains a secret: %s", resp.Body.String())
	}
}

func TestAdminTestNotificationChannelRejectsDisabledChannel(t *testing.T) {
	clearNotificationEnv(t)
	mock := withMockGlobalDB(t)
	mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnError(sql.ErrNoRows)

	resp := doNotificationJSON(notificationRouter("admin-1"), http.MethodPost, "/admin/notifications/test", `{"channel":"slack"}`)
	if resp.Code != http.StatusBadRequest || !strings.Contains(resp.Body.String(), "not enabled") {
		t.Errorf("status %d: %s", resp.Code, resp.Body.String())
	}

	resp = doNotificationJSON(notificationRouter("admin-1"), http.MethodPost, "/admin/notifications/test", `{"channel":"sms"}`)
	if resp.Code != http.StatusBadRequest {
		t.Errorf("unknown channel: status %d", resp.Code)
	}
}

func TestNotificationPreferences(t *testing.T) {
	clearNotificationEnv(t)
	const user = "11111111-1111-1111-1111-111111111111"

	if resp := doNotificationJSON(notificationRouter(""), http.MethodGet, "/notifications/preferences", ""); resp.Code != http.StatusUnauthorized {
		t.Errorf("no user: status %d, want 401", resp.Code)
	}

	t.Run("email_enabled is required", func(t *testing.T) {
		withMockGlobalDB(t)
		resp := doNotificationJSON(notificationRouter(user), http.MethodPut, "/notifications/preferences", `{"email_categories":{}}`)
		if resp.Code != http.StatusBadRequest {
			t.Errorf("status %d", resp.Code)
		}
	})

	t.Run("unknown category", func(t *testing.T) {
		mock := withMockGlobalDB(t)
		mock.ExpectQuery(`FROM user_notification_preferences`).WillReturnError(sql.ErrNoRows)
		resp := doNotificationJSON(notificationRouter(user), http.MethodPut, "/notifications/preferences", `{"email_enabled":true,"email_categories":{"nope":false}}`)
		if resp.Code != http.StatusBadRequest {
			t.Errorf("status %d", resp.Code)
		}
	})

	t.Run("save", func(t *testing.T) {
		notifier.InvalidateChannelCache()
		mock := withMockGlobalDB(t)
		mock.ExpectQuery(`FROM user_notification_preferences`).WithArgs(user).WillReturnError(sql.ErrNoRows)
		mock.ExpectExec(`INSERT INTO user_notification_preferences`).
			WithArgs(user, true, "schema_drift").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnError(sql.ErrNoRows)

		resp := doNotificationJSON(notificationRouter(user), http.MethodPut, "/notifications/preferences",
			`{"email_enabled":true,"email_categories":{"schema_drift":false,"data_loss":true}}`)
		if resp.Code != http.StatusOK {
			t.Fatalf("status %d: %s", resp.Code, resp.Body.String())
		}
		var view notificationPreferencesView
		_ = json.Unmarshal(resp.Body.Bytes(), &view)
		if !view.EmailEnabled || view.EmailCategories["schema_drift"] || !view.EmailCategories["other"] {
			t.Errorf("view = %+v", view)
		}
		if !strings.Contains(resp.Body.String(), `"email_blocked_categories":[]`) {
			t.Errorf("email_blocked_categories should be an empty list, not null: %s", resp.Body.String())
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("categories the admin blocked for email", func(t *testing.T) {
		notifier.InvalidateChannelCache()
		mock := withMockGlobalDB(t)
		mock.ExpectQuery(`FROM user_notification_preferences`).WithArgs(user).WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnRows(
			sqlmock.NewRows(notificationChannelColumns).AddRow(
				false, "", "",
				true, "smtp.example.com", 587, "", "",
				"alerts@example.com", "starttls", "schema_drift,health", "", time.Now()))

		resp := doNotificationJSON(notificationRouter(user), http.MethodGet, "/notifications/preferences", "")
		if resp.Code != http.StatusOK {
			t.Fatalf("status %d: %s", resp.Code, resp.Body.String())
		}
		var view notificationPreferencesView
		_ = json.Unmarshal(resp.Body.Bytes(), &view)
		if strings.Join(view.EmailBlockedCategories, ",") != "schema_drift,health" || !view.Channels.Email {
			t.Errorf("view = %+v", view)
		}
	})
}
