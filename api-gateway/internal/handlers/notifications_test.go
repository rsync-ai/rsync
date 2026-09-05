package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// Unit tests for the per-user notification-inbox handlers backing the header
// bell. The load-bearing contract is double scoping: every query is filtered by
// the caller's user_id (IDOR gate) AND by the active workspace (so the bell never
// deep-links into another workspace's pipeline, which the gateway would 404),
// read-state is read_at IS NULL, and mark-read is idempotent. sqlmock's WithArgs
// enforces both scopes.

const notifTestUser = "11111111-1111-1111-1111-111111111111"
const notifTestID = "22222222-2222-2222-2222-222222222222"
const notifTestWorkspace = "44444444-4444-4444-4444-444444444444"

// withNotificationsDB swaps the global db.DB (which db.GetDB returns) for a
// sqlmock and restores it on teardown.
func withNotificationsDB(t *testing.T, fn func(mock sqlmock.Sqlmock)) {
	t.Helper()
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()
	prev := db.DB
	db.DB = mockDB
	t.Cleanup(func() { db.DB = prev })
	fn(mock)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// newNotifRouter installs a handler behind a shim that stamps user_id and the
// active workspace_id on the gin context (what AuthRequiredMiddleware and
// WorkspaceContextMiddleware do in prod). Pass userID="" to simulate an
// unauthenticated caller so the resolveUserID 401 path is exercised; pass an
// explicit workspaceID of "" to simulate no active workspace and exercise the
// fail-closed empty-inbox path.
func newNotifRouter(method, path string, handler gin.HandlerFunc, userID string, workspaceID ...string) *gin.Engine {
	ws := notifTestWorkspace
	if len(workspaceID) > 0 {
		ws = workspaceID[0]
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	wire := func(c *gin.Context) {
		if userID != "" {
			c.Set("user_id", userID)
		}
		if ws != "" {
			c.Set("workspace_id", ws)
		}
		handler(c)
	}
	r.Handle(method, path, wire)
	return r
}

func TestListNotifications_ReturnsRowsAndUnreadCount(t *testing.T) {
	withNotificationsDB(t, func(mock sqlmock.Sqlmock) {
		now := time.Now()
		readAt := now.Add(-time.Hour)
		mock.ExpectQuery(regexp.QuoteMeta("SELECT n.id, n.pipeline_id, n.type, n.severity, n.title, n.message")).
			WithArgs(notifTestUser, notifTestWorkspace, notificationListLimit).
			WillReturnRows(sqlmock.NewRows([]string{
				"id", "pipeline_id", "type", "severity", "title", "message", "action_url",
				"pipeline_name", "impact", "action_label", "error_code", "raw_type",
				"source_topic", "read_at", "created_at",
			}).
				// unread row (read_at NULL), written by the copy catalog: human
				// title plus the pipeline/impact/action the bell renders.
				AddRow("n1", "p1", "schema_change_notification", "warning",
					"Approval needed: your source schema changed",
					"A new column was added to public.categories",
					"/pipelines/p1/schema-changes", "orders-sync",
					"Your existing data keeps syncing.", "Review change", "SCHEMA_DRIFT_DETECTED",
					"schema_change_notification", "rsync.notifications", nil, now).
				// already-read row that predates the catalog: no action_label in
				// metadata, so repairPreCatalogCopy re-renders it on the way out.
				AddRow("n2", "p1", "pipeline_failed", "critical", "Run failed", "boom",
					"/pipelines/p1", "orders-sync", "", "", "", "pipeline_failed",
					"rsync.notifications", readAt, now.Add(-2*time.Hour)))
		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*)")).
			WithArgs(notifTestUser, notifTestWorkspace).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		r := newNotifRouter("GET", "/api/v1/notifications", ListNotifications, notifTestUser)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/notifications", nil))

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		var resp struct {
			Notifications []Notification `json:"notifications"`
			UnreadCount   int            `json:"unread_count"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(resp.Notifications) != 2 {
			t.Fatalf("expected 2 notifications, got %d", len(resp.Notifications))
		}
		if resp.UnreadCount != 1 {
			t.Errorf("expected unread_count 1, got %d", resp.UnreadCount)
		}
		if resp.Notifications[0].Read {
			t.Error("first row has read_at NULL and must report read=false")
		}
		if !resp.Notifications[1].Read {
			t.Error("second row has read_at set and must report read=true")
		}
		if resp.Notifications[0].ActionURL != "/pipelines/p1/schema-changes" {
			t.Errorf("unexpected action_url: %q", resp.Notifications[0].ActionURL)
		}
		// The copy fields the header bell needs to render an understandable row:
		// which pipeline, what it means, what to do, and the support code — the
		// last of which used to be rendered AS the title.
		first := resp.Notifications[0]
		if first.PipelineName != "orders-sync" {
			t.Errorf("pipeline_name = %q, want orders-sync", first.PipelineName)
		}
		if first.Impact == "" {
			t.Error("impact must be projected so the user learns whether data is still moving")
		}
		if first.ActionLabel != "Review change" {
			t.Errorf("action_label = %q, want \"Review change\"", first.ActionLabel)
		}
		if first.ReferenceCode != "SCHEMA_DRIFT_DETECTED" {
			t.Errorf("reference_code = %q", first.ReferenceCode)
		}
		if strings.Contains(first.Title, first.ReferenceCode) {
			t.Errorf("the error code must not be the headline: %q", first.Title)
		}
		// Pre-catalog row is repaired on the way out rather than shipped bare:
		// it gains a button verb and an impact line it never had persisted.
		if second := resp.Notifications[1]; second.ActionLabel == "" || second.Impact == "" {
			t.Errorf("legacy row should be re-rendered through the catalog, got %+v", second)
		}
	})
}

// TestListNotifications_RepairsPreCatalogRows locks the fix for the rows a
// customer actually reported: notifications written BEFORE the copy catalog
// shipped, whose titles are a raw Kafka topic or a raw error code. Those rows
// were rendered at write time, so no deploy fixes them — only this read-time
// repair does.
func TestListNotifications_RepairsPreCatalogRows(t *testing.T) {
	withNotificationsDB(t, func(mock sqlmock.Sqlmock) {
		now := time.Now()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT n.id, n.pipeline_id, n.type, n.severity, n.title, n.message")).
			WithArgs(notifTestUser, notifTestWorkspace, notificationListLimit).
			WillReturnRows(sqlmock.NewRows([]string{
				"id", "pipeline_id", "type", "severity", "title", "message", "action_url",
				"pipeline_name", "impact", "action_label", "error_code", "raw_type",
				"source_topic", "read_at", "created_at",
			}).
				// The literal row from the bug report: healer.HealingResult
				// carries no type, so the old writer used the TOPIC as the title
				// and left the body empty.
				AddRow("n1", "p1", "", "info", "rsync.healer.results", "",
					"/pipelines/p1", "orders-sync", "", "", "", "",
					"rsync.healer.results", nil, now).
				// The other reported row: the stable code used AS the headline.
				AddRow("n2", "p1", "structured_error_notification", "critical",
					"LEGACY_UNCLASSIFIED", "raw failure text", "/pipelines/p1",
					"orders-sync", "", "", "LEGACY_UNCLASSIFIED",
					"structured_error_notification", "rsync.notifications", nil, now))
		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*)")).
			WithArgs(notifTestUser, notifTestWorkspace).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))

		r := newNotifRouter("GET", "/api/v1/notifications", ListNotifications, notifTestUser)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/notifications", nil))

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		var resp struct {
			Notifications []Notification `json:"notifications"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(resp.Notifications) != 2 {
			t.Fatalf("expected 2 notifications, got %d", len(resp.Notifications))
		}

		topicRow, codeRow := resp.Notifications[0], resp.Notifications[1]
		if topicRow.Title != "Pipeline update" {
			t.Errorf("topic-titled row should resolve via topicDefaults, got %q", topicRow.Title)
		}
		if codeRow.Title != "orders-sync ran into a problem" {
			t.Errorf("code-titled row should resolve via the catalog, got %q", codeRow.Title)
		}
		// The invariant the catalog exists to enforce, now applied to history too.
		for _, n := range resp.Notifications {
			if strings.Contains(n.Title, "rsync.") || strings.Contains(n.Title, "_") {
				t.Errorf("repaired title still leaks an internal identifier: %q", n.Title)
			}
			if n.ActionLabel == "" {
				t.Errorf("repaired row %s has no action label", n.ID)
			}
		}
		// A real code stays available as a support reference; LEGACY_UNCLASSIFIED
		// is not one. It means the classifier gave up, so surfacing it under the
		// bell's "quote this code when contacting support" tag hands the user an
		// internal token dressed up as an answer — the last route by which this
		// package still showed an identifier to a customer. Suppressed at the
		// projection only: metadata.error_code on the stored row is untouched, and
		// repairPreCatalogCopy above still resolved the title from it.
		if codeRow.ReferenceCode != "" {
			t.Errorf("reference_code = %q, want placeholder code suppressed", codeRow.ReferenceCode)
		}
	})
}

// TestRepairPreCatalogCopy_LeavesCatalogRowsAlone guards the detection rule: a
// row the catalog already rendered must pass through byte-identical, or a
// deploy would silently rewrite copy that is already correct.
func TestRepairPreCatalogCopy_LeavesCatalogRowsAlone(t *testing.T) {
	n := Notification{
		Type:          "structured_error_notification",
		Severity:      "warning",
		Title:         "Approval needed: your source schema changed",
		Impact:        "Your existing data keeps syncing.",
		ActionLabel:   "Review change",
		ReferenceCode: "SCHEMA_DRIFT_DETECTED",
		PipelineName:  "orders-sync",
	}
	before := n
	repairPreCatalogCopy(&n, "structured_error_notification", "rsync.notifications")
	if n != before {
		t.Errorf("catalog-rendered row was rewritten:\n before %+v\n after  %+v", before, n)
	}
}

func TestListNotifications_Unauthorized_Returns401(t *testing.T) {
	// No user_id in context → resolveUserID fails closed before any DB access.
	r := newNotifRouter("GET", "/api/v1/notifications", ListNotifications, "")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/notifications", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetUnreadNotificationCount(t *testing.T) {
	withNotificationsDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*)")).
			WithArgs(notifTestUser, notifTestWorkspace).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

		r := newNotifRouter("GET", "/api/v1/notifications/unread-count", GetUnreadNotificationCount, notifTestUser)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/notifications/unread-count", nil))

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		var resp struct {
			UnreadCount int `json:"unread_count"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if resp.UnreadCount != 3 {
			t.Errorf("expected unread_count 3, got %d", resp.UnreadCount)
		}
	})
}

func TestMarkNotificationRead_HappyPath_Returns200(t *testing.T) {
	withNotificationsDB(t, func(mock sqlmock.Sqlmock) {
		// The UPDATE is scoped by BOTH id and user_id — sqlmock WithArgs pins that.
		mock.ExpectExec(regexp.QuoteMeta("UPDATE pipeline_notifications")).
			WithArgs(notifTestID, notifTestUser).
			WillReturnResult(sqlmock.NewResult(0, 1))

		r := newNotifRouter("POST", "/api/v1/notifications/mark-read", MarkNotificationRead, notifTestUser)
		w := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/api/v1/notifications/mark-read", strings.NewReader(`{"id":"`+notifTestID+`"}`))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
	})
}

func TestMarkNotificationRead_NotFoundOrNotOwned_Returns404(t *testing.T) {
	// 0 rows affected ⇒ the id isn't the caller's (or doesn't exist). The
	// user_id clause is what makes another user's row unmatchable — IDOR gate.
	withNotificationsDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectExec(regexp.QuoteMeta("UPDATE pipeline_notifications")).
			WithArgs(notifTestID, notifTestUser).
			WillReturnResult(sqlmock.NewResult(0, 0))

		r := newNotifRouter("POST", "/api/v1/notifications/mark-read", MarkNotificationRead, notifTestUser)
		w := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/api/v1/notifications/mark-read", strings.NewReader(`{"id":"`+notifTestID+`"}`))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
		}
	})
}

func TestMarkNotificationRead_InvalidUUID_Returns400_NoDBCall(t *testing.T) {
	// A malformed id must be a clean 400 BEFORE any DB access — no sqlmock
	// expectations are registered, so a stray query would fail the test.
	withNotificationsDB(t, func(_ sqlmock.Sqlmock) {
		r := newNotifRouter("POST", "/api/v1/notifications/mark-read", MarkNotificationRead, notifTestUser)
		w := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/api/v1/notifications/mark-read", strings.NewReader(`{"id":"not-a-uuid"}`))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
		}
	})
}

func TestMarkNotificationRead_Unauthorized_Returns401(t *testing.T) {
	r := newNotifRouter("POST", "/api/v1/notifications/mark-read", MarkNotificationRead, "")
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/v1/notifications/mark-read", strings.NewReader(`{"id":"`+notifTestID+`"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
}

func TestMarkAllNotificationsRead_Returns200WithCount(t *testing.T) {
	withNotificationsDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectExec(regexp.QuoteMeta("UPDATE pipeline_notifications")).
			WithArgs(notifTestUser, notifTestWorkspace).
			WillReturnResult(sqlmock.NewResult(0, 5))

		r := newNotifRouter("POST", "/api/v1/notifications/mark-all-read", MarkAllNotificationsRead, notifTestUser)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/notifications/mark-all-read", nil))

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		var resp struct {
			Status string `json:"status"`
			Marked int64  `json:"marked"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if resp.Status != "ok" || resp.Marked != 5 {
			t.Errorf("expected {ok,5}, got {%s,%d}", resp.Status, resp.Marked)
		}
	})
}

// TestNotifications_NoActiveWorkspace_FailsClosed pins the fallback: with no
// workspace pinned on the context, the handlers must NOT widen to every
// workspace's rows. No sqlmock expectations are registered, so any query at all
// fails the test — the empty inbox has to come from the guard, not the DB.
func TestNotifications_NoActiveWorkspace_FailsClosed(t *testing.T) {
	cases := []struct {
		name    string
		method  string
		path    string
		handler gin.HandlerFunc
		want    string
	}{
		{"list", "GET", "/api/v1/notifications", ListNotifications, `"unread_count":0`},
		{"unread-count", "GET", "/api/v1/notifications/unread-count", GetUnreadNotificationCount, `"unread_count":0`},
		{"mark-all-read", "POST", "/api/v1/notifications/mark-all-read", MarkAllNotificationsRead, `"marked":0`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withNotificationsDB(t, func(_ sqlmock.Sqlmock) {
				r := newNotifRouter(tc.method, tc.path, tc.handler, notifTestUser, "")
				w := httptest.NewRecorder()
				r.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))

				if w.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
				}
				if !strings.Contains(w.Body.String(), tc.want) {
					t.Errorf("expected body to contain %s, got %s", tc.want, w.Body.String())
				}
			})
		})
	}
}

// TestListNotifications_OtherWorkspaceRowsAreExcluded proves the scope is the
// ACTIVE workspace and not merely "some workspace": the join filters on the
// pinned id, so a user with rows in two workspaces sees only the current one's.
// The bug this prevents is a bell row that deep-links to a pipeline the gateway
// then 404s because it belongs to the workspace the user just left.
func TestListNotifications_OtherWorkspaceRowsAreExcluded(t *testing.T) {
	withNotificationsDB(t, func(mock sqlmock.Sqlmock) {
		// sqlmock returns rows only when the args match, so registering the
		// expectation against notifTestWorkspace IS the assertion that the
		// handler passed the active workspace and not the user's other one.
		mock.ExpectQuery(regexp.QuoteMeta("p.workspace_id = $2")).
			WithArgs(notifTestUser, notifTestWorkspace, notificationListLimit).
			WillReturnRows(sqlmock.NewRows([]string{
				"id", "pipeline_id", "type", "severity", "title", "message", "action_url",
				"pipeline_name", "impact", "action_label", "error_code", "raw_type",
				"source_topic", "read_at", "created_at",
			}).AddRow("n1", "p1", "pipeline_failed", "critical", "Run failed", "boom",
				"/pipelines/p1", "orders-sync", "", "", "", "pipeline_failed",
				"rsync.notifications", nil, time.Now()))
		mock.ExpectQuery(regexp.QuoteMeta("p.workspace_id = $2")).
			WithArgs(notifTestUser, notifTestWorkspace).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		r := newNotifRouter("GET", "/api/v1/notifications", ListNotifications, notifTestUser)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/notifications", nil))

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		var resp struct {
			Notifications []Notification `json:"notifications"`
			UnreadCount   int            `json:"unread_count"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(resp.Notifications) != 1 || resp.UnreadCount != 1 {
			t.Fatalf("expected only the active workspace's row, got %d rows / unread %d",
				len(resp.Notifications), resp.UnreadCount)
		}
	})
}

func TestListNotifications_DBError_Returns500(t *testing.T) {
	withNotificationsDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT n.id, n.pipeline_id, n.type, n.severity, n.title, n.message")).
			WithArgs(notifTestUser, notifTestWorkspace, notificationListLimit).
			WillReturnError(errors.New("connection refused"))

		r := newNotifRouter("GET", "/api/v1/notifications", ListNotifications, notifTestUser)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/notifications", nil))

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("expected 500, got %d: %s", w.Code, w.Body.String())
		}
	})
}
