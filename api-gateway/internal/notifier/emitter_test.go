package notifier

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// The gateway raises a new pre-migration assessment issue through an emitter
// (NewEmitter + Publish) rather than over Kafka. These tests pin that the
// in-process path is the same persist → dedup → deliver path, with the
// catalog's copy, as a consumed event.

const (
	emitterPipeline = "2cb685ed-4cf7-445b-9f77-071794d25423"
	emitterOwner    = "11111111-1111-1111-1111-111111111111"
	emitterSubject  = "NO_PRIMARY_KEY|public.orders"
)

func assessmentIssueEvent(t *testing.T) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]interface{}{
		"type":        "pre_migration_assessment",
		"pipeline_id": emitterPipeline,
		"message":     "The pre-migration assessment found 1 new issue: Table primary key (high): public.orders.",
		"action_url":  "/pipelines/" + emitterPipeline + "?tab=assessment",
		"error": map[string]interface{}{
			"failure_type":   "PRE_MIGRATION_ASSESSMENT",
			"code":           "PRE_MIGRATION_ASSESSMENT_ISSUE",
			"severity":       "critical",
			"audience":       "user",
			"user_message":   "The pre-migration assessment found 1 new issue: Table primary key (high): public.orders.",
			"source_db_type": "postgresql",
			"dedup_subject":  emitterSubject,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestEmitterStopIsSafe(t *testing.T) {
	(*Notifier)(nil).Stop()
	NewEmitter(nil).Stop() // never consumed: no cancel, no done channel to wait on
}

func TestAssessmentIssueCopy(t *testing.T) {
	r := resolve("PRE_MIGRATION_ASSESSMENT_ISSUE", "pre_migration_assessment", resolveNotifierTopics().notify,
		"critical", map[string]string{"source": "PostgreSQL"})
	if r.Title != "The assessment found a new issue in PostgreSQL" {
		t.Errorf("title = %q", r.Title)
	}
	if r.Severity != "critical" || r.ActionLabel != "View assessment" {
		t.Errorf("rendered = %+v", r)
	}
	if got := CategoryFor("PRE_MIGRATION_ASSESSMENT_ISSUE", "pre_migration_assessment"); got != CategorySourceSetup {
		t.Errorf("category = %q; want %q so the source-setup mute applies", got, CategorySourceSetup)
	}
}

type jsonCapture struct{ into *map[string]interface{} }

func (c jsonCapture) Match(v driver.Value) bool {
	var b []byte
	switch s := v.(type) {
	case []byte:
		b = s
	case string:
		b = []byte(s)
	default:
		return false
	}
	return json.Unmarshal(b, c.into) == nil
}

func TestEmitterPublishPersistsAnAssessmentIssue(t *testing.T) {
	clearNotifierEnv(t)
	InvalidateChannelCache()
	t.Cleanup(InvalidateChannelCache)
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()

	actionURL := "/pipelines/" + emitterPipeline + "?tab=assessment"
	dedupKey := makeDedupKey(emitterPipeline, "PRE_MIGRATION_ASSESSMENT_ISSUE", actionURL, emitterSubject)
	var meta map[string]interface{}

	mock.ExpectQuery(`SELECT created_by, COALESCE\(name, ''\) FROM pipelines WHERE id = \$1`).
		WithArgs(emitterPipeline).
		WillReturnRows(sqlmock.NewRows([]string{"created_by", "name"}).AddRow(emitterOwner, "orders-sync"))
	mock.ExpectQuery(`SELECT id::text FROM pipeline_notifications\s+WHERE dedup_key = \$1`).
		WithArgs(dedupKey).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	mock.ExpectExec(`INSERT INTO pipeline_notifications`).
		WithArgs(sqlmock.AnyArg(), emitterPipeline, emitterOwner, "pre_migration_assessment", "critical",
			"The assessment found a new issue in PostgreSQL",
			"The pre-migration assessment found 1 new issue: Table primary key (high): public.orders.",
			actionURL, jsonCapture{&meta}, dedupKey).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnRows(sqlmock.NewRows(channelColumns))
	mock.ExpectExec(`UPDATE pipeline_notifications`).
		WithArgs(StatusSuppressed, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := NewEmitter(mockDB).Publish(context.Background(), assessmentIssueEvent(t)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
	if meta["category"] != CategorySourceSetup || meta["error_code"] != "PRE_MIGRATION_ASSESSMENT_ISSUE" ||
		meta["action_label"] != "View assessment" || meta["pipeline_name"] != "orders-sync" {
		t.Errorf("metadata = %v", meta)
	}
}

// The same new issue published twice inside the dedup window is one alert:
// the gateway's scheduler and a manual run can both see it first.
func TestEmitterPublishDedupsARepeatedIssue(t *testing.T) {
	clearNotifierEnv(t)
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()

	actionURL := "/pipelines/" + emitterPipeline + "?tab=assessment"
	mock.ExpectQuery(`FROM pipelines WHERE id = \$1`).
		WithArgs(emitterPipeline).
		WillReturnRows(sqlmock.NewRows([]string{"created_by", "name"}).AddRow(emitterOwner, "orders-sync"))
	mock.ExpectQuery(`FROM pipeline_notifications\s+WHERE dedup_key = \$1`).
		WithArgs(makeDedupKey(emitterPipeline, "PRE_MIGRATION_ASSESSMENT_ISSUE", actionURL, emitterSubject)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("99999999-9999-9999-9999-999999999999"))

	if err := NewEmitter(mockDB).Publish(context.Background(), assessmentIssueEvent(t)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// No INSERT and no delivery: sqlmock rejects any unexpected statement.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
