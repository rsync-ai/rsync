package sentinel

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rsync-ai/shared/kafkaclient"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

const testConsumerGroupBase = "orchestrator-test"

// stubConsumerLagSource answers the consumer check the way the test dictates. It records
// the group ids it was asked about, so a test can see the check still asks for
// "<base>-<topic>", the name ConsumeWithContext registers.
type stubConsumerLagSource struct {
	closed map[string]bool
	lag    map[string]int64
	asked  []string
}

func (s *stubConsumerLagSource) IsConsumerActive(topic string) bool { return !s.closed[topic] }

func (s *stubConsumerLagSource) GetConsumerGroupLag(groupID string) (map[string]int64, error) {
	s.asked = append(s.asked, groupID)
	topic := strings.TrimPrefix(groupID, testConsumerGroupBase+"-")
	return map[string]int64{topic: s.lag[topic]}, nil
}

// metadataArg matches the metadata column (a JSON object) by the keys it must carry and
// the keys it must not.
type metadataArg struct {
	want   map[string]interface{}
	absent []string
}

func (m metadataArg) Match(v driver.Value) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	var got map[string]interface{}
	if err := json.Unmarshal([]byte(s), &got); err != nil {
		return false
	}
	for k, want := range m.want {
		if got[k] != want {
			return false
		}
	}
	for _, k := range m.absent {
		if _, present := got[k]; present {
			return false
		}
	}
	return true
}

func newConsumerCheckForTest(t *testing.T, src *stubConsumerLagSource) (*HealthMonitor, sqlmock.Sqlmock, func()) {
	t.Helper()
	h, mock, cleanup := newHealthMonitorForTest(t)
	h.ctx = context.Background()
	h.consumerLag = src
	h.consumerGroupBase = testConsumerGroupBase
	return h, mock, cleanup
}

// consumedWireTopics is what the check iterates. A check over zero topics would pass every
// assertion below without writing a row, so an empty list fails here instead.
func consumedWireTopics(t *testing.T) []string {
	t.Helper()
	topics := kafkaclient.Topics(orchestratorConsumedTopics...)
	if len(topics) == 0 {
		t.Fatal("orchestratorConsumedTopics is empty; the consumer check has nothing to write")
	}
	return topics
}

func expectConsumerRow(mock sqlmock.Sqlmock, topic string, status HealthStatus, lag int64, lastErr string, metadata sqlmock.Argument) {
	mock.ExpectExec(`INSERT INTO sentinel_component_health`).
		WithArgs(
			topic,
			ComponentTypeKafkaConsumer,
			status,
			sqlmock.AnyArg(), // last_heartbeat
			sqlmock.AnyArg(), // messages_processed
			sqlmock.AnyArg(), // error_count
			lag,
			lastErr,
			metadata,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

// An active consumer with nothing to catch up on used to produce no write at all: the
// check persisted only a closed group or lag above zero, so a consumer that recovered kept
// its last bad row indefinitely and GET /api/v1/monitoring/sentinel/health served that
// as current. Every consumed topic must now get a healthy row on every tick.
func TestActiveConsumerAtZeroLagIsPersistedHealthy(t *testing.T) {
	topics := consumedWireTopics(t)
	src := &stubConsumerLagSource{lag: map[string]int64{}}
	h, mock, cleanup := newConsumerCheckForTest(t, src)
	defer cleanup()

	var wantGroups []string
	for _, topic := range topics {
		group := testConsumerGroupBase + "-" + topic
		wantGroups = append(wantGroups, group)
		expectConsumerRow(mock, topic, HealthStatusHealthy, 0, "", metadataArg{
			want: map[string]interface{}{"topic": topic, "is_active": true, "consumer_group": group},
		})
	}

	h.checkKafkaConsumerLag()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("a zero-lag active consumer did not get a healthy row: %v", err)
	}
	for _, topic := range topics {
		got := infraHealth(t, h, topic)
		if got.Status != HealthStatusHealthy || got.ConsumerLag != 0 || got.LastError != "" {
			t.Errorf("%s: status=%q lag=%d last_error=%q, want healthy/0/\"\"", topic, got.Status, got.ConsumerLag, got.LastError)
		}
		if got.LastHeartbeat.IsZero() {
			t.Errorf("%s: last_heartbeat is zero; the column is NOT NULL and the check is this component's heartbeat", topic)
		}
	}
	if strings.Join(src.asked, ",") != strings.Join(wantGroups, ",") {
		t.Errorf("lag asked for groups %v, want %v (the \"<base>-<topic>\" names ConsumeWithContext registers)", src.asked, wantGroups)
	}
}

// The user-visible bug, tick by tick: a consumer whose group closed, then came back with a
// backlog of 4500, then drained it. Each tick must rewrite the row, and a recovered row must
// drop both the error text and the issue_type the closed verdict carried.
func TestConsumerRowFollowsTheConsumerBackToHealthy(t *testing.T) {
	topics := consumedWireTopics(t)
	watched := topics[0]
	src := &stubConsumerLagSource{closed: map[string]bool{watched: true}, lag: map[string]int64{}}
	h, mock, cleanup := newConsumerCheckForTest(t, src)
	defer cleanup()

	tick := func(expectWatched func()) {
		for _, topic := range topics {
			if topic == watched {
				expectWatched()
				continue
			}
			expectConsumerRow(mock, topic, HealthStatusHealthy, 0, "", sqlmock.AnyArg())
		}
		h.checkKafkaConsumerLag()
	}
	recovered := metadataArg{want: map[string]interface{}{"is_active": true}, absent: []string{"issue_type"}}

	tick(func() {
		expectConsumerRow(mock, watched, HealthStatusUnhealthy, 0, "Consumer group closed", metadataArg{
			want: map[string]interface{}{"is_active": false, "issue_type": "consumer_group_closed"},
		})
	})

	src.closed = nil
	src.lag[watched] = 4500
	tick(func() { expectConsumerRow(mock, watched, HealthStatusHealthy, 4500, "", recovered) })

	src.lag[watched] = 0
	tick(func() { expectConsumerRow(mock, watched, HealthStatusHealthy, 0, "", recovered) })

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the consumer row did not follow the consumer back to healthy: %v", err)
	}
	if got := infraHealth(t, h, watched); got.Metadata["issue_type"] != nil {
		t.Errorf("recovered consumer still carries issue_type %v in memory", got.Metadata["issue_type"])
	}
}

// The row is rewritten every 30s for every consumed topic, so the Info line must not be.
// RecordHealthChange logs "Component health changed" at Info on every call; the consumer
// check logs it only when a topic's status actually changes.
func TestSteadyConsumerTicksDoNotLogHealthChanges(t *testing.T) {
	topics := consumedWireTopics(t)
	src := &stubConsumerLagSource{lag: map[string]int64{}}
	h, mock, cleanup := newConsumerCheckForTest(t, src)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	hook := &logtest.Hook{}
	savedHooks := log.StandardLogger().ReplaceHooks(make(log.LevelHooks))
	savedLevel := log.GetLevel()
	log.AddHook(hook)
	log.SetLevel(log.InfoLevel)
	defer func() {
		log.SetLevel(savedLevel)
		log.StandardLogger().ReplaceHooks(savedHooks)
	}()

	changes := func() []string {
		var ids []string
		for _, e := range hook.AllEntries() {
			if e.Level <= log.InfoLevel && e.Message == "Component health changed" {
				ids = append(ids, e.Data["component_id"].(string))
			}
		}
		hook.Reset()
		return ids
	}
	tick := func() {
		for range topics {
			mock.ExpectExec(`INSERT INTO sentinel_component_health`).WillReturnResult(sqlmock.NewResult(0, 1))
		}
		h.checkKafkaConsumerLag()
	}

	tick()
	if got := changes(); len(got) != len(topics) {
		t.Fatalf("first tick logged %d health changes, want one per topic (%d): nothing was known before it", len(got), len(topics))
	}

	tick()
	if got := changes(); len(got) != 0 {
		t.Errorf("a tick with no status change logged %d \"Component health changed\" lines at Info: %v", len(got), got)
	}

	src.closed = map[string]bool{topics[0]: true}
	tick()
	if got := changes(); len(got) != 1 || got[0] != topics[0] {
		t.Errorf("closing %s logged health changes for %v, want exactly that topic", topics[0], got)
	}

	tick()
	if got := changes(); len(got) != 0 {
		t.Errorf("a consumer still closed logged %d more health changes: %v", len(got), got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("every tick must still write every row: %v", err)
	}
}
