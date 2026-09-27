package notifier

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/IBM/sarama"
)

// ─────────────────────────────────────────────────────────────────────────────
// These cover the two ways an alert used to disappear between Kafka and a
// person: the consumer committed the offset of a message it had failed to
// handle, and an alert about the instance rather than a pipeline had nowhere
// to be written at all.
// ─────────────────────────────────────────────────────────────────────────────

const (
	testAdminA   = "aaaaaaaa-0000-0000-0000-000000000001"
	testAdminB   = "bbbbbbbb-0000-0000-0000-000000000002"
	testPipeline = "2cb685ed-4cf7-445b-9f77-071794d25423"
)

// fakeSession records what the notifier commits.
type fakeSession struct {
	ctx    context.Context
	marked []int64
}

func (f *fakeSession) Claims() map[string][]int32               { return nil }
func (f *fakeSession) MemberID() string                         { return "test" }
func (f *fakeSession) GenerationID() int32                      { return 1 }
func (f *fakeSession) MarkOffset(string, int32, int64, string)  {}
func (f *fakeSession) ResetOffset(string, int32, int64, string) {}
func (f *fakeSession) Commit()                                  {}
func (f *fakeSession) Context() context.Context                 { return f.ctx }
func (f *fakeSession) MarkMessage(msg *sarama.ConsumerMessage, _ string) {
	f.marked = append(f.marked, msg.Offset)
}

type fakeClaim struct {
	topic string
	ch    chan *sarama.ConsumerMessage
}

func (f *fakeClaim) Topic() string                            { return f.topic }
func (f *fakeClaim) Partition() int32                         { return 0 }
func (f *fakeClaim) InitialOffset() int64                     { return 0 }
func (f *fakeClaim) HighWaterMarkOffset() int64               { return 1 }
func (f *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return f.ch }

// claimOf delivers exactly one message, then closes, so ConsumeClaim returns.
func claimOf(topic string, value []byte) *fakeClaim {
	ch := make(chan *sarama.ConsumerMessage, 1)
	ch <- &sarama.ConsumerMessage{Topic: topic, Partition: 0, Offset: 42, Value: value}
	close(ch)
	return &fakeClaim{topic: topic, ch: ch}
}

func fastRetries(t *testing.T) {
	t.Helper()
	prev := handleRetryDelay
	handleRetryDelay = time.Millisecond
	t.Cleanup(func() { handleRetryDelay = prev })
}

func pipelineAlert(pipelineID string) []byte {
	return []byte(`{"type":"pipeline_failed","pipeline_id":"` + pipelineID + `","message":"boom","action_url":"/pipelines/x"}`)
}

// instanceAlert is the payload shape healthwatch/watchdog.go actually produces:
// the synthetic pipeline id "system" and a structured error envelope.
func instanceAlert() []byte {
	return []byte(`{"type":"structured_error_notification","pipeline_id":"system",` +
		`"action_url":"/admin/health/connector-versions/postgresql/1.2.0",` +
		`"message":"success rate dropped","error":{"code":"RSYNC_CONNECTOR_VERSION_REGRESSION",` +
		`"severity":"warning","audience":"operator","user_message":"success rate dropped"}}`)
}

// A transient database failure must NOT advance the offset. Before the fix this
// loop logged the error and marked the message anyway, so every alert that
// arrived during a Postgres blip was destroyed — nothing re-reads a Kafka
// message whose offset has moved on.
func TestConsumeClaimDoesNotCommitARetryableFailure(t *testing.T) {
	clearNotifierEnv(t)
	fastRetries(t)
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()

	// Every attempt fails the owner lookup the way a downed database does.
	for i := 0; i < handleAttempts; i++ {
		mock.ExpectQuery(`FROM pipelines WHERE id = \$1`).
			WillReturnError(errors.New("connection refused"))
	}

	n := NewEmitter(mockDB)
	sess := &fakeSession{ctx: context.Background()}
	err = n.ConsumeClaim(sess, claimOf(n.topics.notify, pipelineAlert(testPipeline)))

	if err == nil {
		t.Fatal("want an error so sarama ends the session and the broker redelivers; got nil")
	}
	if len(sess.marked) != 0 {
		t.Fatalf("offset %v was committed for a message that never got handled", sess.marked)
	}
}

// The other half of the trade: a payload that will never parse must be dropped,
// not retried forever, or one malformed message wedges the whole subscription
// and no alert is delivered again.
func TestConsumeClaimCommitsAnUnparseablePayload(t *testing.T) {
	clearNotifierEnv(t)
	fastRetries(t)
	mockDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()

	n := NewEmitter(mockDB)
	sess := &fakeSession{ctx: context.Background()}
	// No DB expectations at all: sqlmock fails the test if this touches one.
	if err := n.ConsumeClaim(sess, claimOf(n.topics.notify, []byte("{not json"))); err != nil {
		t.Fatalf("a poison message must not abort the claim: %v", err)
	}
	if len(sess.marked) != 1 {
		t.Fatalf("want the poison message committed and skipped, marked = %v", sess.marked)
	}
}

// A pipeline that was deleted is permanent for the same reason: its owner is
// never coming back, so retrying to the end of time would stall the topic.
func TestDeletedPipelineIsDroppedNotRetriedForever(t *testing.T) {
	clearNotifierEnv(t)
	fastRetries(t)
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()

	// Exactly ONE lookup: a permanent failure must not be retried.
	mock.ExpectQuery(`FROM pipelines WHERE id = \$1`).WillReturnError(sql.ErrNoRows)

	n := NewEmitter(mockDB)
	sess := &fakeSession{ctx: context.Background()}
	if err := n.ConsumeClaim(sess, claimOf(n.topics.notify, pipelineAlert(testPipeline))); err != nil {
		t.Fatalf("a deleted pipeline must not abort the claim: %v", err)
	}
	if len(sess.marked) != 1 {
		t.Fatalf("want the message committed, marked = %v", sess.marked)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations (a retry would show up here): %v", err)
	}
}

// The retry has to actually recover, not just refuse to commit.
func TestConsumeClaimRetriesUntilTheDatabaseRecovers(t *testing.T) {
	clearNotifierEnv(t)
	InvalidateChannelCache()
	t.Cleanup(InvalidateChannelCache)
	fastRetries(t)
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()

	mock.ExpectQuery(`FROM pipelines WHERE id = \$1`).WillReturnError(errors.New("connection refused"))
	mock.ExpectQuery(`FROM pipelines WHERE id = \$1`).
		WillReturnRows(sqlmock.NewRows([]string{"created_by", "name"}).AddRow(testAdminA, "orders-sync"))
	mock.ExpectQuery(`FROM pipeline_notifications\s+WHERE dedup_key = \$1`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO pipeline_notifications`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnRows(sqlmock.NewRows(channelColumns))
	mock.ExpectExec(`UPDATE pipeline_notifications`).WillReturnResult(sqlmock.NewResult(0, 1))

	n := NewEmitter(mockDB)
	sess := &fakeSession{ctx: context.Background()}
	if err := n.ConsumeClaim(sess, claimOf(n.topics.notify, pipelineAlert(testPipeline))); err != nil {
		t.Fatalf("second attempt should have succeeded: %v", err)
	}
	if len(sess.marked) != 1 {
		t.Fatalf("want the recovered message committed, marked = %v", sess.marked)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// healthwatch's connector-regression alert: pipeline_id "system" is not a uuid,
// so before the fix the owner lookup failed on it every single time and the
// alert was logged and dropped. It must now reach one row per active admin,
// with a NULL pipeline_id.
func TestInstanceAlertFansOutToEveryActiveAdmin(t *testing.T) {
	clearNotifierEnv(t)
	InvalidateChannelCache()
	t.Cleanup(InvalidateChannelCache)
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()

	mock.ExpectQuery(`FROM users\s+WHERE role = 'admin'`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(testAdminA).AddRow(testAdminB))
	mock.ExpectQuery(`FROM pipeline_notifications\s+WHERE dedup_key = \$1`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	mock.ExpectBegin()
	// pipeline_id must be NULL, not the literal "system" and not "".
	mock.ExpectExec(`INSERT INTO pipeline_notifications`).
		WithArgs(sqlmock.AnyArg(), nil, testAdminA, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO pipeline_notifications`).
		WithArgs(sqlmock.AnyArg(), nil, testAdminB, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	// Channel settings are instance-wide and cached (CachedChannelConfig), so a
	// fan-out reads them once no matter how many recipients it has. Each
	// recipient still gets its own delivery_status write, which is what proves
	// the loop ran for both of them.
	mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnRows(sqlmock.NewRows(channelColumns))
	mock.ExpectExec(`UPDATE pipeline_notifications`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE pipeline_notifications`).WillReturnResult(sqlmock.NewResult(0, 1))

	n := NewEmitter(mockDB)
	if err := n.handleMessage(context.Background(), n.topics.notify, instanceAlert()); err != nil {
		t.Fatalf("instance alert: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// An install with no admin cannot receive ops alerts at all. There is no row to
// write (user_id is NOT NULL), so the one thing that must not happen is a
// retry loop that wedges the topic.
func TestInstanceAlertWithNoActiveAdminIsDropped(t *testing.T) {
	clearNotifierEnv(t)
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()

	mock.ExpectQuery(`FROM users\s+WHERE role = 'admin'`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	n := NewEmitter(mockDB)
	if err := n.handleMessage(context.Background(), n.topics.notify, instanceAlert()); err != nil {
		t.Fatalf("want a clean drop, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("nothing else should have been queried: %v", err)
	}
}

// The regression alert is the first coded instance-level event; without copy it
// would render through the fallback and read as if it were about a pipeline.
func TestConnectorRegressionAlertHasOperatorCopy(t *testing.T) {
	r := resolve("RSYNC_CONNECTOR_VERSION_REGRESSION", "structured_error_notification",
		"rsync.notifications", "warning", map[string]string{"pipeline": instanceScopeLabel})
	if strings.Contains(r.Title, "{pipeline}") || r.Title == "" {
		t.Fatalf("unresolved or empty title: %q", r.Title)
	}
	if r.Impact == "" {
		t.Fatal("an alert with no impact line tells an operator nothing")
	}
	if got := CategoryFor("RSYNC_CONNECTOR_VERSION_REGRESSION", ""); got != CategoryHealth {
		t.Fatalf("category = %q, want %q", got, CategoryHealth)
	}
}

// "system" is the literal healthwatch sends; a real uuid must never be caught
// by the same branch, or a pipeline alert would be broadcast to every admin.
func TestInstanceScopeMatchesTheSyntheticIDsOnly(t *testing.T) {
	for _, id := range []string{"system", "System", " system ", "instance", "global"} {
		if !isInstanceScope(id) {
			t.Errorf("isInstanceScope(%q) = false, want true", id)
		}
	}
	for _, id := range []string{testPipeline, "", "systemic", "my-pipeline"} {
		if isInstanceScope(id) {
			t.Errorf("isInstanceScope(%q) = true, want false", id)
		}
	}
}
