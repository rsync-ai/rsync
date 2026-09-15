package notifier

import (
	"context"
	"net/http"
	"net/http/httptest"
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
				false, "", 587, "", "", "", TLSModeStartTLS, time.Now()))
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
