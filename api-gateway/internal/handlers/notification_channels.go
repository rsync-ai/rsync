package handlers

// Notification channels (admin) and each user's email alert preferences.
//
// The admin half configures the instance-wide Slack webhook and SMTP relay the
// notifier delivers through (notification_channel_settings, migration 102). The
// user half is the caller's own email switch and muted categories. Delivery
// itself lives in internal/notifier; these handlers only read and write the
// rows and never return a secret, only whether one is configured.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/mail"
	"net/url"
	"strings"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/notifier"
	"api-gateway/internal/safehttp"

	"github.com/gin-gonic/gin"
	"github.com/rsync-ai/shared/crypto"
	log "github.com/sirupsen/logrus"
)

// slackWebhookHost is the only host an incoming webhook is accepted for. The
// webhook URL is admin-supplied and the notifier POSTs to it from inside the
// cluster, so anything looser is a request to wherever the admin points it.
const slackWebhookHost = "hooks.slack.com"

// testNotificationTimeout covers the SMTP send timeout plus the settings read.
const testNotificationTimeout = 45 * time.Second

type slackChannelView struct {
	Enabled           bool            `json:"enabled"`
	WebhookConfigured bool            `json:"webhook_configured"`
	Categories        map[string]bool `json:"categories"`
}

type emailChannelView struct {
	Enabled            bool   `json:"enabled"`
	SMTPHost           string `json:"smtp_host"`
	SMTPPort           int    `json:"smtp_port"`
	SMTPUsername       string `json:"smtp_username"`
	PasswordConfigured bool   `json:"password_configured"`
	From               string `json:"from"`
	TLSMode            string `json:"tls_mode"`
	// Categories: which alerts are emailed at all. An off category reaches
	// no one by email, whatever a user chose on /settings.
	Categories map[string]bool `json:"categories"`
	// ExtraRecipients receive every enabled category, whatever users chose.
	ExtraRecipients []string `json:"extra_recipients"`
}

// maxEmailExtraRecipients caps the admin alert list. Each address is its own
// SMTP session per alert, so the cap bounds how long one alert can take.
const maxEmailExtraRecipients = 20

type notificationChannelsView struct {
	// Source is "database" once an admin has saved channels, "environment"
	// while the SMTP_* / NOTIFIER_SLACK_WEBHOOK_URL env vars still apply.
	Source     string              `json:"source"`
	UpdatedAt  *time.Time          `json:"updated_at"`
	Slack      slackChannelView    `json:"slack"`
	Email      emailChannelView    `json:"email"`
	Categories []notifier.Category `json:"categories"`
}

func buildNotificationChannelsView(stored *notifier.StoredChannelSettings) notificationChannelsView {
	if stored == nil {
		env := notifier.EnvChannelConfig()
		return notificationChannelsView{
			Source: notifier.SourceEnvironment,
			Slack: slackChannelView{
				Enabled:           env.Slack.Enabled,
				WebhookConfigured: env.Slack.WebhookURL != "",
				Categories:        categoryEnabledMap(nil),
			},
			Email: emailChannelView{
				Enabled:            env.Email.Enabled,
				SMTPHost:           env.Email.Host,
				SMTPPort:           env.Email.Port,
				SMTPUsername:       env.Email.Username,
				PasswordConfigured: env.Email.Password != "",
				From:               env.Email.From,
				TLSMode:            env.Email.TLSMode,
				Categories:         categoryEnabledMap(nil),
				ExtraRecipients:    []string{},
			},
			Categories: notifier.Categories(),
		}
	}
	updatedAt := stored.UpdatedAt
	return notificationChannelsView{
		Source:    notifier.SourceDatabase,
		UpdatedAt: &updatedAt,
		Slack: slackChannelView{
			Enabled:           stored.SlackEnabled,
			WebhookConfigured: stored.SlackWebhookEncrypted != "",
			Categories:        categoryEnabledMap(stored.SlackMuted),
		},
		Email: emailChannelView{
			Enabled:            stored.EmailEnabled,
			SMTPHost:           stored.SMTPHost,
			SMTPPort:           stored.SMTPPort,
			SMTPUsername:       stored.SMTPUsername,
			PasswordConfigured: stored.SMTPPasswordEncrypted != "",
			From:               stored.SMTPFrom,
			TLSMode:            stored.SMTPTLSMode,
			Categories:         categoryEnabledMap(stored.EmailMuted),
			ExtraRecipients:    nonNilStrings(stored.EmailExtraRecipients),
		},
		Categories: notifier.Categories(),
	}
}

// categoryEnabledMap turns a stored muted list into {category: enabled} for
// every known category.
func categoryEnabledMap(muted []string) map[string]bool {
	off := make(map[string]bool, len(muted))
	for _, id := range muted {
		off[id] = true
	}
	out := make(map[string]bool)
	for _, cat := range notifier.Categories() {
		out[cat.ID] = !off[cat.ID]
	}
	return out
}

// mutedCategories turns a request's {category: enabled} into the muted list to
// store, in display order. A nil map means "unchanged" and returns current.
func mutedCategories(requested map[string]bool, current []string) ([]string, error) {
	if requested == nil {
		return current, nil
	}
	for id := range requested {
		if !notifier.IsCategory(id) {
			return nil, fmt.Errorf("unknown notification category %q", id)
		}
	}
	muted := []string{}
	for _, cat := range notifier.Categories() {
		if enabled, ok := requested[cat.ID]; ok && !enabled {
			muted = append(muted, cat.ID)
		}
	}
	return muted, nil
}

// nonNilStrings keeps an empty list as [] in JSON rather than null.
func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// normalizeExtraRecipients validates the admin alert list and returns bare,
// de-duplicated addresses in the order given. A nil request means "unchanged"
// and returns current.
func normalizeExtraRecipients(requested, current []string) ([]string, error) {
	if requested == nil {
		return nonNilStrings(current), nil
	}
	out := []string{}
	seen := map[string]bool{}
	for _, raw := range requested {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		// Stored comma-joined across pgx (channels.go), and a line break could
		// start a new SMTP header, so neither may appear in an address.
		if strings.ContainsAny(v, ",\r\n") {
			return nil, fmt.Errorf("extra_recipients: %q must be a single email address", v)
		}
		parsed, err := mail.ParseAddress(v)
		if err != nil {
			return nil, fmt.Errorf("extra_recipients: %q is not a valid email address", v)
		}
		key := strings.ToLower(parsed.Address)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, parsed.Address)
	}
	if len(out) > maxEmailExtraRecipients {
		return nil, fmt.Errorf("extra_recipients: at most %d addresses", maxEmailExtraRecipients)
	}
	return out, nil
}

// AdminGetNotificationChannels handles GET /api/v1/admin/notifications/channels.
func AdminGetNotificationChannels(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}
	stored, err := notifier.ReadStoredChannelSettings(c.Request.Context(), database)
	if err != nil {
		log.WithError(err).Error("admin: read notification channel settings")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load notification channels"})
		return
	}
	c.JSON(http.StatusOK, buildNotificationChannelsView(stored))
}

type updateNotificationChannelsRequest struct {
	Slack *struct {
		Enabled bool `json:"enabled"`
		// WebhookURL: omitted keeps the saved webhook, "" removes it.
		WebhookURL *string         `json:"webhook_url"`
		Categories map[string]bool `json:"categories"`
	} `json:"slack"`
	Email *struct {
		Enabled      bool   `json:"enabled"`
		SMTPHost     string `json:"smtp_host"`
		SMTPPort     int    `json:"smtp_port"`
		SMTPUsername string `json:"smtp_username"`
		// SMTPPassword: omitted keeps the saved password, "" removes it.
		SMTPPassword *string `json:"smtp_password"`
		From         string  `json:"from"`
		TLSMode      string  `json:"tls_mode"`
		// Categories and ExtraRecipients: omitted keeps the saved value.
		Categories      map[string]bool `json:"categories"`
		ExtraRecipients []string        `json:"extra_recipients"`
	} `json:"email"`
}

// validateSlackWebhookURL accepts only a Slack incoming-webhook URL.
func validateSlackWebhookURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return errors.New("webhook_url must be a Slack incoming webhook URL (https://hooks.slack.com/services/...)")
	}
	if u.Scheme != "https" || !strings.EqualFold(u.Hostname(), slackWebhookHost) || u.Port() != "" || u.User != nil {
		return errors.New("webhook_url must be a Slack incoming webhook URL (https://hooks.slack.com/services/...)")
	}
	if err := safehttp.ValidateURL(u); err != nil {
		return fmt.Errorf("webhook_url is not allowed: %v", err)
	}
	return nil
}

// resolveChannelSecret returns the ciphertext to store for one secret field.
//   - set in the request: encrypt the new value ("" clears it)
//   - omitted, row exists: keep the stored ciphertext
//   - omitted, first save: carry the env-configured secret into the row, so
//     saving the form for the first time does not silently drop a working
//     webhook or password the admin never saw
func resolveChannelSecret(requested *string, stored *notifier.StoredChannelSettings, storedCipher, envPlain string) (string, error) {
	if requested != nil {
		v := strings.TrimSpace(*requested)
		if v == "" {
			return "", nil
		}
		return crypto.EncryptString(v)
	}
	if stored != nil {
		return storedCipher, nil
	}
	if envPlain == "" {
		return "", nil
	}
	return crypto.EncryptString(envPlain)
}

func isLoopbackHost(host string) bool {
	h := strings.ToLower(strings.Trim(host, "[]"))
	return h == "localhost" || h == "127.0.0.1" || h == "::1"
}

// AdminUpdateNotificationChannels handles PUT /api/v1/admin/notifications/channels.
// The body replaces both channels; secrets follow resolveChannelSecret.
func AdminUpdateNotificationChannels(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}
	var req updateNotificationChannelsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}
	if req.Slack == nil || req.Email == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Both slack and email must be provided"})
		return
	}
	badRequest := func(msg string) {
		c.JSON(http.StatusBadRequest, gin.H{"error": msg})
	}

	ctx := c.Request.Context()
	stored, err := notifier.ReadStoredChannelSettings(ctx, database)
	if err != nil {
		log.WithError(err).Error("admin: read notification channel settings")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load notification channels"})
		return
	}
	var env notifier.ChannelConfig
	var currentSlackMuted, currentEmailMuted, currentExtraRecipients []string
	storedWebhook, storedPassword := "", ""
	if stored == nil {
		env = notifier.EnvChannelConfig()
	} else {
		currentSlackMuted = stored.SlackMuted
		currentEmailMuted, currentExtraRecipients = stored.EmailMuted, stored.EmailExtraRecipients
		storedWebhook, storedPassword = stored.SlackWebhookEncrypted, stored.SMTPPasswordEncrypted
	}

	// ── Slack ──
	if req.Slack.WebhookURL != nil {
		if v := strings.TrimSpace(*req.Slack.WebhookURL); v != "" {
			if err := validateSlackWebhookURL(v); err != nil {
				badRequest(err.Error())
				return
			}
		}
	}
	slackMuted, err := mutedCategories(req.Slack.Categories, currentSlackMuted)
	if err != nil {
		badRequest(err.Error())
		return
	}

	// ── Email ──
	e := req.Email
	host := strings.TrimSpace(e.SMTPHost)
	from := strings.TrimSpace(e.From)
	username := strings.TrimSpace(e.SMTPUsername)
	if strings.ContainsAny(host, " \t\r\n/") || strings.Contains(host, "://") {
		badRequest("smtp_host must be a host name or IP address, without a scheme or port")
		return
	}
	if strings.ContainsAny(username, "\r\n") {
		badRequest("smtp_username must not contain line breaks")
		return
	}
	port := e.SMTPPort
	if port == 0 {
		port = 587
	}
	if port < 1 || port > 65535 {
		badRequest("smtp_port must be between 1 and 65535")
		return
	}
	tlsMode := strings.ToLower(strings.TrimSpace(e.TLSMode))
	if tlsMode == "" {
		tlsMode = notifier.TLSModeStartTLS
	}
	if tlsMode != notifier.TLSModeStartTLS && tlsMode != notifier.TLSModeTLS && tlsMode != notifier.TLSModeNone {
		badRequest(`tls_mode must be "starttls", "tls" or "none"`)
		return
	}
	if from != "" {
		if _, err := mail.ParseAddress(from); err != nil {
			badRequest("from must be a valid email address")
			return
		}
	}
	emailMuted, err := mutedCategories(e.Categories, currentEmailMuted)
	if err != nil {
		badRequest(err.Error())
		return
	}
	extraRecipients, err := normalizeExtraRecipients(e.ExtraRecipients, currentExtraRecipients)
	if err != nil {
		badRequest(err.Error())
		return
	}

	webhookCipher, err := resolveChannelSecret(req.Slack.WebhookURL, stored, storedWebhook, env.Slack.WebhookURL)
	if err != nil {
		log.WithError(err).Error("admin: encrypt Slack webhook")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to encrypt the Slack webhook"})
		return
	}
	passwordCipher, err := resolveChannelSecret(e.SMTPPassword, stored, storedPassword, env.Email.Password)
	if err != nil {
		log.WithError(err).Error("admin: encrypt SMTP password")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to encrypt the SMTP password"})
		return
	}

	if req.Slack.Enabled && webhookCipher == "" {
		badRequest("Slack is enabled but no webhook_url is set")
		return
	}
	if e.Enabled {
		if host == "" || from == "" {
			badRequest("Email is enabled but smtp_host or from is missing")
			return
		}
		// net/smtp's PlainAuth refuses to send a password in plaintext to
		// anything but localhost, so this setup could never send. Say so now
		// rather than on the first alert.
		if tlsMode == notifier.TLSModeNone && username != "" && passwordCipher != "" && !isLoopbackHost(host) {
			badRequest(`tls_mode "none" cannot authenticate to a remote SMTP server; use "starttls" or "tls"`)
			return
		}
	}

	next := notifier.StoredChannelSettings{
		SlackEnabled:          req.Slack.Enabled,
		SlackWebhookEncrypted: webhookCipher,
		SlackMuted:            slackMuted,
		EmailEnabled:          e.Enabled,
		SMTPHost:              host,
		SMTPPort:              port,
		SMTPUsername:          username,
		SMTPPasswordEncrypted: passwordCipher,
		SMTPFrom:              from,
		SMTPTLSMode:           tlsMode,
		EmailMuted:            emailMuted,
		EmailExtraRecipients:  extraRecipients,
	}
	adminID := c.GetString("admin_user_id")
	if err := notifier.WriteStoredChannelSettings(ctx, database, next, adminID); err != nil {
		log.WithError(err).Error("admin: write notification channel settings")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save notification channels"})
		return
	}
	notifier.InvalidateChannelCache()

	previousSource := notifier.SourceDatabase
	if stored == nil {
		previousSource = notifier.SourceEnvironment
	}
	// Never the secrets: only whether each was changed.
	logAudit(c, "admin_update_notification_channels", "notification_channels", "1", gin.H{
		"previous_source":        previousSource,
		"slack_enabled":          next.SlackEnabled,
		"slack_webhook_changed":  req.Slack.WebhookURL != nil,
		"slack_muted_categories": next.SlackMuted,
		"email_enabled":          next.EmailEnabled,
		"smtp_host":              next.SMTPHost,
		"smtp_port":              next.SMTPPort,
		"smtp_tls_mode":          next.SMTPTLSMode,
		"smtp_password_changed":  e.SMTPPassword != nil,
		"email_muted_categories": next.EmailMuted,
		"email_extra_recipients": next.EmailExtraRecipients,
	})

	next.UpdatedAt = time.Now().UTC()
	c.JSON(http.StatusOK, buildNotificationChannelsView(&next))
}

// AdminTestNotificationChannel handles POST /api/v1/admin/notifications/test.
// It sends through the SAVED settings, so an admin saves before testing. Email
// goes to the calling admin's own address.
func AdminTestNotificationChannel(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}
	var req struct {
		Channel string `json:"channel"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}
	channel := strings.ToLower(strings.TrimSpace(req.Channel))
	if channel != "slack" && channel != "email" {
		c.JSON(http.StatusBadRequest, gin.H{"error": `channel must be "slack" or "email"`})
		return
	}
	to := ""
	if channel == "email" {
		to = c.GetString("admin_user_email")
		if to == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Your account has no email address to send the test to"})
			return
		}
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), testNotificationTimeout)
	defer cancel()
	err := notifier.SendTestNotification(ctx, database, channel, to)

	logAudit(c, "admin_test_notification_channel", "notification_channels", channel, gin.H{
		"channel": channel,
		"success": err == nil,
	})

	switch {
	case errors.Is(err, notifier.ErrChannelNotEnabled):
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("%s notifications are not enabled; save the channel with it enabled, then test", channelLabel(channel))})
	case err != nil:
		// The detail is what the admin needs to fix the setup (an SMTP reply, a
		// Slack "invalid_token"). It never contains the webhook URL or password.
		c.JSON(http.StatusBadGateway, gin.H{"error": "Test notification failed", "detail": err.Error()})
	default:
		resp := gin.H{"success": true, "channel": channel}
		if to != "" {
			resp["sent_to"] = to
		}
		c.JSON(http.StatusOK, resp)
	}
}

func channelLabel(channel string) string {
	if channel == "slack" {
		return "Slack"
	}
	return "Email"
}

type notificationPreferencesView struct {
	EmailEnabled    bool                `json:"email_enabled"`
	EmailCategories map[string]bool     `json:"email_categories"`
	Categories      []notifier.Category `json:"categories"`
	// Channels says which channels the instance has switched on, so the UI can
	// explain why an email preference has no effect.
	Channels struct {
		Email bool `json:"email"`
		Slack bool `json:"slack"`
	} `json:"channels"`
	// EmailBlockedCategories: categories the admin turned off for email. The
	// user's own switch for these has no effect, so the UI shows them locked.
	EmailBlockedCategories []string `json:"email_blocked_categories"`
}

func buildNotificationPreferencesView(ctx context.Context, prefs notifier.EmailPreferences) notificationPreferencesView {
	v := notificationPreferencesView{
		EmailEnabled:           prefs.Enabled,
		EmailCategories:        categoryEnabledMap(prefs.Muted),
		Categories:             notifier.Categories(),
		EmailBlockedCategories: []string{},
	}
	if cfg, err := notifier.CachedChannelConfig(ctx, db.GetDB()); err != nil {
		log.WithError(err).Warn("notification preferences: could not read channel availability")
	} else {
		v.Channels.Email = cfg.Email.Enabled
		v.Channels.Slack = cfg.Slack.Enabled
		v.EmailBlockedCategories = nonNilStrings(cfg.Email.Muted)
	}
	return v
}

// GetNotificationPreferences handles GET /api/v1/notifications/preferences.
func GetNotificationPreferences(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}
	prefs, err := notifier.ReadEmailPreferences(c.Request.Context(), database, userID)
	if err != nil {
		log.WithError(err).WithField("user_id", userID).Error("read notification preferences")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load notification preferences"})
		return
	}
	c.JSON(http.StatusOK, buildNotificationPreferencesView(c.Request.Context(), prefs))
}

// UpdateNotificationPreferences handles PUT /api/v1/notifications/preferences.
func UpdateNotificationPreferences(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}
	var req struct {
		EmailEnabled    *bool           `json:"email_enabled"`
		EmailCategories map[string]bool `json:"email_categories"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}
	if req.EmailEnabled == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email_enabled is required"})
		return
	}

	ctx := c.Request.Context()
	current, err := notifier.ReadEmailPreferences(ctx, database, userID)
	if err != nil {
		log.WithError(err).WithField("user_id", userID).Error("read notification preferences")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load notification preferences"})
		return
	}
	muted, err := mutedCategories(req.EmailCategories, current.Muted)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	next := notifier.EmailPreferences{Enabled: *req.EmailEnabled, Muted: muted}
	if err := notifier.WriteEmailPreferences(ctx, database, userID, next); err != nil {
		log.WithError(err).WithField("user_id", userID).Error("write notification preferences")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save notification preferences"})
		return
	}
	c.JSON(http.StatusOK, buildNotificationPreferencesView(ctx, next))
}
