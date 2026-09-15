package notifier

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"mime/quotedprintable"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rsync-ai/shared/crypto"
)

// deliver wires the saved settings, the category and the send together. These
// run it against a saved row (sqlmock) and a Slack stand-in (httptest).
func TestDeliverAppliesSavedSlackSettings(t *testing.T) {
	t.Setenv("ENCRYPTION_KEY", testEncryptionKey)
	clearNotifierEnv(t)

	var hits atomic.Int32
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(status)
	}))
	defer srv.Close()

	webhook, err := crypto.EncryptString(srv.URL + "/services/T/B/secret")
	if err != nil {
		t.Fatal(err)
	}
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	n := &Notifier{db: mockDB, httpClient: srv.Client(), appBaseURL: "https://app.example.com"}
	p := notificationPayload{Type: "pipeline_run_failed", PipelineID: "p1", Message: "run failed"}
	r := Rendered{Title: "Run failed", Severity: "error"}

	run := func(category string) (string, error) {
		t.Helper()
		InvalidateChannelCache()
		mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnRows(
			sqlmock.NewRows(channelColumns).AddRow(
				true, webhook, CategoryHealth,
				false, "", 587, "", "", "", TLSModeStartTLS, "", "", time.Now()))
		return n.deliver(context.Background(), "11111111-1111-1111-1111-111111111111", p, r, false, category)
	}
	t.Cleanup(InvalidateChannelCache)

	got, err := run(CategoryHealth)
	if got != StatusSkipped || err != nil || hits.Load() != 0 {
		t.Errorf("muted category: status=%q err=%v hits=%d, want skipped with no send", got, err, hits.Load())
	}

	got, err = run(CategoryRunStatus)
	if got != StatusDelivered || err != nil || hits.Load() != 1 {
		t.Errorf("unmuted category: status=%q err=%v hits=%d, want delivered", got, err, hits.Load())
	}

	status = http.StatusNotFound
	got, err = run(CategoryDataLoss)
	if got != StatusFailed || err == nil || !strings.HasPrefix(err.Error(), "slack: ") {
		t.Errorf("failed send: status=%q err=%v, want failed with a slack-prefixed error", got, err)
	}
	if err != nil && strings.Contains(err.Error(), "secret") {
		t.Errorf("delivery error leaks the webhook: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestDeliverWithNoChannelsIsSuppressed(t *testing.T) {
	clearNotifierEnv(t)
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	InvalidateChannelCache()
	t.Cleanup(InvalidateChannelCache)
	mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnRows(sqlmock.NewRows(channelColumns))

	n := &Notifier{db: mockDB, httpClient: http.DefaultClient}
	got, err := n.deliver(context.Background(), "u", notificationPayload{}, Rendered{}, false, CategoryDataLoss)
	if got != StatusSuppressed || err != nil {
		t.Errorf("status=%q err=%v, want suppressed", got, err)
	}
}

// decodedBody returns the plain-text body of one message the fake SMTP server
// accepted. The body is quoted-printable, which soft-wraps long lines.
func decodedBody(t *testing.T, data string) string {
	t.Helper()
	_, body, ok := strings.Cut(strings.ReplaceAll(data, "\r\n", "\n"), "\n\n")
	if !ok {
		t.Fatalf("message has no body: %q", data)
	}
	b, err := io.ReadAll(quotedprintable.NewReader(strings.NewReader(body)))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Email goes to the owner (subject to their own choices) and to every address
// on the admin's alert list (whatever the owner chose), one copy per person,
// and an admin-blocked category goes to nobody.
func TestDeliverEmailsOwnerAndAlertList(t *testing.T) {
	t.Setenv("ENCRYPTION_KEY", testEncryptionKey)
	clearNotifierEnv(t)
	t.Cleanup(InvalidateChannelCache)
	const user = "11111111-1111-1111-1111-111111111111"
	p := notificationPayload{Type: "pipeline_run_failed", PipelineID: "p1", Message: "run failed", ActionURL: "/pipelines/p1"}
	r := Rendered{Title: "Run failed", Severity: "error", PipelineName: "orders"}

	setup := func(t *testing.T, emailMuted, extra string) (*fakeSMTP, sqlmock.Sqlmock, *Notifier) {
		t.Helper()
		InvalidateChannelCache()
		f := startFakeSMTP(t, fakeSMTPOptions{})
		ch := emailChannelFor(t, f, TLSModeNone)
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = mockDB.Close() })
		mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnRows(
			sqlmock.NewRows(channelColumns).AddRow(
				false, "", "",
				true, ch.Host, ch.Port, "", "",
				ch.From, TLSModeNone, emailMuted, extra, time.Now()))
		return f, mock, &Notifier{db: mockDB, httpClient: http.DefaultClient, appBaseURL: "https://app.example.com"}
	}
	rcpts := func(sessions []smtpSession) []string {
		out := []string{}
		for _, s := range sessions {
			out = append(out, s.rcptTo)
		}
		return out
	}

	t.Run("owner and list, owner on the list gets one copy", func(t *testing.T) {
		f, mock, n := setup(t, "", "Owner@Example.com,oncall@example.com")
		mock.ExpectQuery(`FROM user_notification_preferences`).WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery(`SELECT email FROM users`).WillReturnRows(sqlmock.NewRows([]string{"email"}).AddRow("owner@example.com"))

		got, err := n.deliver(context.Background(), user, p, r, false, CategoryRunStatus)
		if got != StatusDelivered || err != nil {
			t.Fatalf("status=%q err=%v", got, err)
		}
		sessions := f.all()
		if want := []string{"RCPT TO:<owner@example.com>", "RCPT TO:<oncall@example.com>"}; !reflect.DeepEqual(rcpts(sessions), want) {
			t.Fatalf("recipients = %v, want %v", rcpts(sessions), want)
		}
		owner, list := decodedBody(t, sessions[0].data), decodedBody(t, sessions[1].data)
		if !strings.Contains(owner, "https://app.example.com/settings") || strings.Contains(owner, "alert list") {
			t.Errorf("owner footer should point at their settings:\n%s", owner)
		}
		if !strings.Contains(list, "alert list") || !strings.Contains(list, "https://app.example.com/admin/notifications") {
			t.Errorf("alert list footer should say why and point at the admin page:\n%s", list)
		}
		if strings.Contains(sessions[1].data, "owner@example.com") {
			t.Errorf("the alert list copy reveals the owner's address:\n%s", sessions[1].data)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("owner opted out, list still emailed", func(t *testing.T) {
		f, mock, n := setup(t, "", "oncall@example.com")
		mock.ExpectQuery(`FROM user_notification_preferences`).WillReturnRows(
			sqlmock.NewRows([]string{"email_enabled", "muted"}).AddRow(false, ""))

		got, err := n.deliver(context.Background(), user, p, r, false, CategoryRunStatus)
		if got != StatusDelivered || err != nil {
			t.Fatalf("status=%q err=%v", got, err)
		}
		if want := []string{"RCPT TO:<oncall@example.com>"}; !reflect.DeepEqual(rcpts(f.all()), want) {
			t.Errorf("recipients = %v, want %v", rcpts(f.all()), want)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("owner copy failed, list copy to the same person still goes", func(t *testing.T) {
		f, mock, n := setup(t, "", "owner@example.com")
		mock.ExpectQuery(`FROM user_notification_preferences`).WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery(`SELECT email FROM users`).WillReturnError(errors.New("connection reset"))

		got, err := n.deliver(context.Background(), user, p, r, false, CategoryRunStatus)
		if got != StatusDelivered || err == nil || !strings.HasPrefix(err.Error(), "email: ") {
			t.Fatalf("status=%q err=%v, want delivered with the owner's failure recorded", got, err)
		}
		if want := []string{"RCPT TO:<owner@example.com>"}; !reflect.DeepEqual(rcpts(f.all()), want) {
			t.Errorf("recipients = %v, want %v", rcpts(f.all()), want)
		}
	})

	t.Run("admin blocked the category", func(t *testing.T) {
		f, mock, n := setup(t, CategoryRunStatus, "oncall@example.com")
		mock.ExpectQuery(`FROM user_notification_preferences`).WillReturnError(sql.ErrNoRows)

		got, err := n.deliver(context.Background(), user, p, r, false, CategoryRunStatus)
		if got != StatusSkipped || err != nil {
			t.Fatalf("status=%q err=%v, want skipped", got, err)
		}
		if len(f.all()) != 0 {
			t.Errorf("an admin-blocked category was emailed to %v", rcpts(f.all()))
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
}
