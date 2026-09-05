package handlers

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"testing"
	"time"

	"api-gateway/internal/slack"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// Security-focused unit tests for the inbound Slack drift-approval receiver. The
// invariants under test: (1) nothing is trusted without a valid signature;
// (2) an unmapped/unauthorized Slack identity NEVER results in an approval;
// (3) a valid, authorized click runs through the SAME approval SQL the UI uses.

const (
	slackTestSecret   = "test-signing-secret-abc123"
	slackTestPipeline = "44444444-4444-4444-4444-444444444444"
	slackTestChange   = "55555555-5555-5555-5555-555555555555"
	slackTestUserID   = "66666666-6666-6666-6666-666666666666"
	slackTestEmail    = "approver@example.com"
)

// slackTestNow is a fixed clock so signatures validate deterministically.
var slackTestNow = time.Unix(1_700_000_000, 0)

// signedSlackRequest builds a POST carrying a block_actions payload, signed with
// secret over the exact raw body (as Slack does).
func signedSlackRequest(secret, actionID, pipelineID, slackUserID string, now time.Time) *http.Request {
	payloadJSON := `{"type":"block_actions","user":{"id":"` + slackUserID + `"},` +
		`"actions":[{"action_id":"` + actionID + `","value":"` + pipelineID + `"}]}`
	form := url.Values{}
	form.Set("payload", payloadJSON)
	body := []byte(form.Encode())

	ts := strconv.FormatInt(now.Unix(), 10)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/slack/interactions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Slack-Request-Timestamp", ts)
	req.Header.Set("X-Slack-Signature", slack.Sign(secret, ts, body))
	return req
}

func serve(h *SlackInteractionsHandler, req *http.Request) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/slack/interactions", h.HandleInteractions)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func stubEmail(email string) func(context.Context, string) (string, error) {
	return func(context.Context, string) (string, error) { return email, nil }
}

func TestSlackInteractions_HappyApprove(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	kafka := &fakeKafka{}
	h := &SlackInteractionsHandler{
		db: mockDB, kafka: kafka, signingSecret: slackTestSecret,
		resolveEmail: stubEmail(slackTestEmail),
		now:          func() time.Time { return slackTestNow },
	}

	// identity: email -> user
	mock.ExpectQuery(regexp.QuoteMeta("FROM users WHERE lower(email)")).
		WithArgs(slackTestEmail).
		WillReturnRows(sqlmock.NewRows([]string{"id", "email"}).AddRow(slackTestUserID, slackTestEmail))
	// authz: role in the pipeline's workspace
	mock.ExpectQuery(regexp.QuoteMeta("JOIN workspace_members wm")).
		WithArgs(slackTestPipeline, slackTestUserID).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))
	// exactly one pending change
	mock.ExpectQuery(regexp.QuoteMeta("FROM schema_change_approvals")).
		WithArgs(slackTestPipeline).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(slackTestChange))
	// approve core: UPDATE + re-fetch + kafka publish
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_change_approvals")).
		WithArgs(slackTestEmail, slackTestChange, slackTestPipeline).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, pipeline_id, change_type, table_name, ddl FROM schema_change_approvals WHERE id = $1")).
		WithArgs(slackTestChange).
		WillReturnRows(sqlmock.NewRows([]string{"id", "pipeline_id", "change_type", "table_name", "ddl"}).
			AddRow(slackTestChange, slackTestPipeline, "add_column", "orders", "ALTER TABLE orders ADD COLUMN x int"))

	w := serve(h, signedSlackRequest(slackTestSecret, slack.ActionApproveSchemaChange, slackTestPipeline, "U123", slackTestNow))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !contains(w.Body.String(), "approved") {
		t.Errorf("expected an 'approved' message, got %s", w.Body.String())
	}
	if len(kafka.calls) != 1 || kafka.calls[0] != "rsync.healer.approved-changes" {
		t.Errorf("approve must publish to the healer; got %v", kafka.calls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock: %v", err)
	}
}

func TestSlackInteractions_BadSignature_401_NoDB(t *testing.T) {
	mockDB, mock, _ := sqlmock.New()
	defer mockDB.Close()
	h := &SlackInteractionsHandler{
		db: mockDB, kafka: &fakeKafka{}, signingSecret: slackTestSecret,
		resolveEmail: stubEmail(slackTestEmail), now: func() time.Time { return slackTestNow },
	}

	req := signedSlackRequest("WRONG-SECRET", slack.ActionApproveSchemaChange, slackTestPipeline, "U123", slackTestNow)
	w := serve(h, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil { // no queries expected
		t.Errorf("no DB access must occur on a bad signature: %v", err)
	}
}

func TestSlackInteractions_Disabled_NoSecret(t *testing.T) {
	mockDB, mock, _ := sqlmock.New()
	defer mockDB.Close()
	h := &SlackInteractionsHandler{db: mockDB, kafka: &fakeKafka{}, signingSecret: "", now: func() time.Time { return slackTestNow }}

	w := serve(h, signedSlackRequest("anything", slack.ActionApproveSchemaChange, slackTestPipeline, "U123", slackTestNow))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !contains(w.Body.String(), "not configured") {
		t.Errorf("expected a not-configured message, got %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("disabled handler must not touch the DB: %v", err)
	}
}

func TestSlackInteractions_UnmappedIdentity_NoApprove(t *testing.T) {
	mockDB, mock, _ := sqlmock.New()
	defer mockDB.Close()
	h := &SlackInteractionsHandler{
		db: mockDB, kafka: &fakeKafka{}, signingSecret: slackTestSecret,
		resolveEmail: stubEmail("stranger@nowhere.test"), now: func() time.Time { return slackTestNow },
	}

	// email resolves, but no rsync user matches → empty result → no further SQL.
	mock.ExpectQuery(regexp.QuoteMeta("FROM users WHERE lower(email)")).
		WithArgs("stranger@nowhere.test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "email"})) // zero rows

	w := serve(h, signedSlackRequest(slackTestSecret, slack.ActionApproveSchemaChange, slackTestPipeline, "U123", slackTestNow))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !contains(w.Body.String(), "isn't linked") {
		t.Errorf("expected an unlinked-account message, got %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("no approval SQL must run for an unmapped identity: %v", err)
	}
}

func TestSlackInteractions_UnauthorizedRole_NoApprove(t *testing.T) {
	mockDB, mock, _ := sqlmock.New()
	defer mockDB.Close()
	h := &SlackInteractionsHandler{
		db: mockDB, kafka: &fakeKafka{}, signingSecret: slackTestSecret,
		resolveEmail: stubEmail(slackTestEmail), now: func() time.Time { return slackTestNow },
	}

	mock.ExpectQuery(regexp.QuoteMeta("FROM users WHERE lower(email)")).
		WithArgs(slackTestEmail).
		WillReturnRows(sqlmock.NewRows([]string{"id", "email"}).AddRow(slackTestUserID, slackTestEmail))
	// mapped user is only a viewer in the pipeline's workspace → below member.
	mock.ExpectQuery(regexp.QuoteMeta("JOIN workspace_members wm")).
		WithArgs(slackTestPipeline, slackTestUserID).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("viewer"))

	w := serve(h, signedSlackRequest(slackTestSecret, slack.ActionApproveSchemaChange, slackTestPipeline, "U123", slackTestNow))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !contains(w.Body.String(), "permission") {
		t.Errorf("expected a permission-denied message, got %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("no approval SQL must run for an unauthorized role: %v", err)
	}
}

func TestSlackInteractions_NoBotToken_CannotIdentify(t *testing.T) {
	mockDB, mock, _ := sqlmock.New()
	defer mockDB.Close()
	// resolveEmail nil == no bot token configured.
	h := &SlackInteractionsHandler{db: mockDB, kafka: &fakeKafka{}, signingSecret: slackTestSecret, now: func() time.Time { return slackTestNow }}

	w := serve(h, signedSlackRequest(slackTestSecret, slack.ActionApproveSchemaChange, slackTestPipeline, "U123", slackTestNow))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !contains(w.Body.String(), "bot token") {
		t.Errorf("expected a bot-token message, got %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("no DB access without an identity resolver: %v", err)
	}
}

func TestSlackInteractions_NoPendingChange(t *testing.T) {
	mockDB, mock, _ := sqlmock.New()
	defer mockDB.Close()
	h := &SlackInteractionsHandler{
		db: mockDB, kafka: &fakeKafka{}, signingSecret: slackTestSecret,
		resolveEmail: stubEmail(slackTestEmail), now: func() time.Time { return slackTestNow },
	}

	mock.ExpectQuery(regexp.QuoteMeta("FROM users WHERE lower(email)")).
		WithArgs(slackTestEmail).
		WillReturnRows(sqlmock.NewRows([]string{"id", "email"}).AddRow(slackTestUserID, slackTestEmail))
	mock.ExpectQuery(regexp.QuoteMeta("JOIN workspace_members wm")).
		WithArgs(slackTestPipeline, slackTestUserID).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("owner"))
	mock.ExpectQuery(regexp.QuoteMeta("FROM schema_change_approvals")).
		WithArgs(slackTestPipeline).
		WillReturnRows(sqlmock.NewRows([]string{"id"})) // nothing pending

	w := serve(h, signedSlackRequest(slackTestSecret, slack.ActionApproveSchemaChange, slackTestPipeline, "U123", slackTestNow))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !contains(w.Body.String(), "No schema change is pending") {
		t.Errorf("expected a nothing-pending message, got %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock: %v", err)
	}
}

func TestSlackInteractions_HappyReject(t *testing.T) {
	mockDB, mock, _ := sqlmock.New()
	defer mockDB.Close()
	kafka := &fakeKafka{}
	h := &SlackInteractionsHandler{
		db: mockDB, kafka: kafka, signingSecret: slackTestSecret,
		resolveEmail: stubEmail(slackTestEmail), now: func() time.Time { return slackTestNow },
	}

	mock.ExpectQuery(regexp.QuoteMeta("FROM users WHERE lower(email)")).
		WithArgs(slackTestEmail).
		WillReturnRows(sqlmock.NewRows([]string{"id", "email"}).AddRow(slackTestUserID, slackTestEmail))
	mock.ExpectQuery(regexp.QuoteMeta("JOIN workspace_members wm")).
		WithArgs(slackTestPipeline, slackTestUserID).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))
	mock.ExpectQuery(regexp.QuoteMeta("FROM schema_change_approvals")).
		WithArgs(slackTestPipeline).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(slackTestChange))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_change_approvals")).
		WithArgs(slackTestEmail, slackTestChange, slackTestPipeline).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := serve(h, signedSlackRequest(slackTestSecret, slack.ActionRejectSchemaChange, slackTestPipeline, "U123", slackTestNow))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !contains(w.Body.String(), "rejected") {
		t.Errorf("expected a 'rejected' message, got %s", w.Body.String())
	}
	// Reject must NOT publish to the healer — that's approve-only.
	if len(kafka.calls) != 0 {
		t.Errorf("reject must not publish to the healer; got %v", kafka.calls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock: %v", err)
	}
}
