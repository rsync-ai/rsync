package handlers

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rsync-ai/backend-orchestrator/internal/cdcsnapshot"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

const (
	reapPipeline  = "600b012e-fecf-4810-9bf4-b567abebfe16"
	reapConnector = "cdc-600b012e"
	reapPrefix    = "rsync.cdc-600b012e"
	reapTable     = "public.orders"
	reapTopic     = reapPrefix + "." + reapTable
)

type fakeReapAdmin struct {
	mu      sync.Mutex
	topics  []string
	readers []string
	listErr error
	deleted []string
}

func (f *fakeReapAdmin) ListTopicNamesFresh(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.topics...), f.listErr
}

func (f *fakeReapAdmin) TopicConsumerGroups(context.Context, string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.readers...), nil
}

func (f *fakeReapAdmin) DeleteTopic(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, name)
	return nil
}

func (f *fakeReapAdmin) deletedTopics() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

type fakeReapOffsets struct {
	count  int64
	drains map[string]kafka.ConsumerGroupDrain
}

func (f *fakeReapOffsets) GetConsumerGroupDrain(g string) (kafka.ConsumerGroupDrain, error) {
	d, ok := f.drains[g]
	if !ok {
		return kafka.ConsumerGroupDrain{}, errors.New("group not found")
	}
	return d, nil
}

func (f *fakeReapOffsets) TopicMessageCount(string) (int64, error) { return f.count, nil }

// fakeReapConnect serves configs in turn (the last one repeats), so a test can
// change the connector between the two checks of one poll.
type fakeReapConnect struct {
	mu      sync.Mutex
	configs []map[string]interface{}
	cfgErr  error
	state   cdcsnapshot.ConnectorState
}

func (f *fakeReapConnect) Config(context.Context, string) (map[string]interface{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cfgErr != nil {
		return nil, f.cfgErr
	}
	cfg := f.configs[0]
	if len(f.configs) > 1 {
		f.configs = f.configs[1:]
	}
	return cfg, nil
}

func (f *fakeReapConnect) State(context.Context, string) (cdcsnapshot.ConnectorState, error) {
	return f.state, nil
}

type fakeExcluder struct {
	mu       sync.Mutex
	excluded map[string]bool
	events   []string
}

func (f *fakeExcluder) ExcludeTopic(t string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.excluded == nil {
		f.excluded = map[string]bool{}
	}
	f.excluded[t] = true
	f.events = append(f.events, "exclude "+t)
}

func (f *fakeExcluder) IncludeTopic(t string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.excluded, t)
	f.events = append(f.events, "include "+t)
}

func (f *fakeExcluder) isExcluded(t string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.excluded[t]
}

// connectorWithout is the connector after the edit: public.orders removed.
func connectorWithout() map[string]interface{} {
	return map[string]interface{}{"topic.prefix": reapPrefix, "table.include.list": `public\.users`}
}

func connectorWith() map[string]interface{} {
	return map[string]interface{}{"topic.prefix": reapPrefix, "table.include.list": `public\.users,public\.orders`}
}

// restartedTask is a connector whose task already runs the new list.
func restartedTask() cdcsnapshot.ConnectorState {
	return cdcsnapshot.ConnectorState{Found: true, State: "RUNNING", TaskIncludes: [][]string{{`public\.users`}}}
}

type reapHarness struct {
	admin   *fakeReapAdmin
	offsets *fakeReapOffsets
	connect *fakeReapConnect
	stats   *fakeExcluder
	r       *RemovedTopicReaper
}

// newReapHarness is the state in which the topic may go: the connector and its
// task no longer capture the table, nothing reads the topic, and the sink's
// stream group has read it to the end while the group it left behind still
// shows the lag it had when it stopped.
func newReapHarness(t *testing.T, deadline time.Duration) *reapHarness {
	t.Helper()
	h := &reapHarness{
		admin: &fakeReapAdmin{topics: []string{reapPrefix + ".public.users", reapTopic}},
		offsets: &fakeReapOffsets{count: 1200, drains: map[string]kafka.ConsumerGroupDrain{
			"sink-600b012e-batch":  {LagByTopic: map[string]int64{reapTopic: 900}},
			"sink-600b012e-stream": {LagByTopic: map[string]int64{reapTopic: 0}},
		}},
		connect: &fakeReapConnect{configs: []map[string]interface{}{connectorWithout()}, state: restartedTask()},
		stats:   &fakeExcluder{},
	}
	h.r = newReapedHarnessReaper(h, deadline)
	t.Cleanup(h.r.Stop)
	return h
}

func newReapedHarnessReaper(h *reapHarness, deadline time.Duration) *RemovedTopicReaper {
	return newRemovedTopicReaper(h.admin, h.offsets, h.connect, h.stats,
		func(context.Context, string) []string {
			return []string{"sink-600b012e-batch", "sink-600b012e-stream"}
		}, 2*time.Millisecond, deadline)
}

func (h *reapHarness) step() (reapVerdict, string, bool) {
	excluded := false
	v, why := h.r.step(reapPipeline, reapConnector, reapTable, reapTopic, &excluded)
	return v, why, excluded
}

func TestReaperDeletesAnAppliedTopic(t *testing.T) {
	h := newReapHarness(t, time.Minute)
	v, why, excluded := h.step()
	if v != reapDone {
		t.Fatalf("verdict = %v (%s), want done", v, why)
	}
	if got := h.admin.deletedTopics(); !reflect.DeepEqual(got, []string{reapTopic}) {
		t.Fatalf("deleted = %v, want [%s]", got, reapTopic)
	}
	if !excluded || !h.stats.isExcluded(reapTopic) {
		t.Fatal("the stats consumer must drop the topic before it is deleted")
	}
}

// An empty topic has nothing to lose, whatever the groups say.
func TestReaperDeletesAnEmptyTopicWithoutSinkOffsets(t *testing.T) {
	h := newReapHarness(t, time.Minute)
	h.offsets.count = 0
	h.offsets.drains = nil
	if v, why, _ := h.step(); v != reapDone {
		t.Fatalf("verdict = %v (%s), want done", v, why)
	}
}

func TestReaperWaits(t *testing.T) {
	cases := map[string]struct {
		mutate func(*reapHarness)
		reason string
	}{
		"a task still runs the old include list": {func(h *reapHarness) {
			h.connect.state.TaskIncludes = [][]string{{`public\.users`}, {`public\.users`, `public\.orders`}}
		}, "task still captures"},
		"a task runs with no include list": {func(h *reapHarness) {
			h.connect.state.TaskIncludes = [][]string{nil}
		}, "task still captures"},
		"the connector has no task configs yet": {func(h *reapHarness) {
			h.connect.state.TaskIncludes = nil
		}, "no task configs"},
		"a consumer group still reads the topic": {func(h *reapHarness) {
			h.admin.readers = []string{"sink-600b012e-stream"}
		}, "still read the topic"},
		"every sink group is behind": {func(h *reapHarness) {
			h.offsets.drains["sink-600b012e-stream"] = kafka.ConsumerGroupDrain{LagByTopic: map[string]int64{reapTopic: 3}}
		}, "has not applied"},
		"no sink group has read a topic that holds messages": {func(h *reapHarness) {
			h.offsets.drains = map[string]kafka.ConsumerGroupDrain{}
		}, "no sink group has read"},
		"listing topics fails": {func(h *reapHarness) {
			h.admin.listErr = errors.New("broker down")
		}, "listing topics failed"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := newReapHarness(t, time.Minute)
			tc.mutate(h)
			v, why, _ := h.step()
			if v != reapWait || !strings.Contains(why, tc.reason) {
				t.Fatalf("verdict = %v (%q), want wait (%q)", v, why, tc.reason)
			}
			if got := h.admin.deletedTopics(); len(got) != 0 {
				t.Fatalf("deleted %v while it had to wait", got)
			}
		})
	}
}

func TestReaperKeeps(t *testing.T) {
	cases := map[string]struct {
		mutate func(*reapHarness)
		reason string
	}{
		"the table was added back": {func(h *reapHarness) {
			h.connect.configs = []map[string]interface{}{connectorWith()}
		}, "added back"},
		// Added back between the first look and the delete.
		"the table was added back during the poll": {func(h *reapHarness) {
			h.connect.configs = []map[string]interface{}{connectorWithout(), connectorWith()}
		}, "added back"},
		"the pipeline was deleted": {func(h *reapHarness) {
			h.connect.cfgErr = errors.New(`connector "cdc-600b012e" not found`)
		}, "no longer exists"},
		"the topic is already gone": {func(h *reapHarness) {
			h.admin.topics = []string{reapPrefix + ".public.users"}
		}, "does not exist"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := newReapHarness(t, time.Minute)
			tc.mutate(h)
			v, why, _ := h.step()
			if v != reapKeep || !strings.Contains(why, tc.reason) {
				t.Fatalf("verdict = %v (%q), want keep (%q)", v, why, tc.reason)
			}
			if got := h.admin.deletedTopics(); len(got) != 0 {
				t.Fatalf("deleted %v; the topic had to stay", got)
			}
		})
	}
}

// waitForJobs waits until the reaper has no job left.
func waitForJobs(t *testing.T, r *RemovedTopicReaper) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		n := len(r.jobs)
		r.mu.Unlock()
		if n == 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("reaper jobs did not finish")
}

func TestReaperScheduleDeletesAndKeepsStatsOff(t *testing.T) {
	h := newReapHarness(t, time.Minute)
	h.r.Schedule(reapPipeline, reapConnector, connectorWithout(), []string{reapTable})
	waitForJobs(t, h.r)
	if got := h.admin.deletedTopics(); !reflect.DeepEqual(got, []string{reapTopic}) {
		t.Fatalf("deleted = %v, want [%s]", got, reapTopic)
	}
	// A broker can list a deleted topic for a while; the stats consumer must not
	// subscribe to it again (that would auto-create it).
	if !h.stats.isExcluded(reapTopic) {
		t.Fatal("the stats consumer took the deleted topic back")
	}
	// Adding the table back lifts it.
	h.r.Readd(connectorWith(), []string{reapTable})
	if h.stats.isExcluded(reapTopic) {
		t.Fatal("Readd did not give the stats consumer its topic back")
	}
}

// A job still waiting at the deadline keeps the topic and hands it back to the
// stats consumer.
func TestReaperDeadlineKeepsTheTopic(t *testing.T) {
	h := newReapHarness(t, 20*time.Millisecond)
	h.admin.readers = []string{"someone"}
	h.r.Schedule(reapPipeline, reapConnector, connectorWithout(), []string{reapTable})
	waitForJobs(t, h.r)
	if got := h.admin.deletedTopics(); len(got) != 0 {
		t.Fatalf("deleted %v past the deadline", got)
	}
	if h.stats.isExcluded(reapTopic) {
		t.Fatal("a kept topic must go back to the stats consumer")
	}
}

func TestReaperScheduleSkipsWhatItCannotName(t *testing.T) {
	h := newReapHarness(t, time.Minute)
	// A pattern entry does not name one topic.
	h.r.Schedule(reapPipeline, reapConnector, connectorWithout(), []string{`public\.orders_.*`, "public.orders_[0-9]+"})
	// Without topic.prefix the topic name would be a guess.
	h.r.Schedule(reapPipeline, reapConnector, map[string]interface{}{}, []string{reapTable})
	waitForJobs(t, h.r)
	if got := h.admin.deletedTopics(); len(got) != 0 {
		t.Fatalf("deleted %v", got)
	}
	var nilReaper *RemovedTopicReaper
	nilReaper.Schedule(reapPipeline, reapConnector, connectorWithout(), []string{reapTable})
	nilReaper.Readd(connectorWith(), []string{reapTable})
	nilReaper.Stop()
}

func TestDiffTableLists(t *testing.T) {
	cases := []struct {
		name                 string
		previous, written    []string
		wantAdded, wantRemov []string
	}{
		{"plain edit", []string{`public\.users`, `public\.orders`}, []string{"public.users", "public.items"},
			[]string{"public.items"}, []string{"public.orders"}},
		// The connector held no include list: it captured every table, so nothing
		// written is new; nothing can be named as removed either.
		{"no previous list", nil, []string{"public.users"}, []string{}, []string{}},
		// A pattern still covers the new name.
		{"pattern keeps covering", []string{`public\.orders_.*`}, []string{"public.orders_2026", `public\.orders_.*`},
			[]string{}, []string{}},
		{"case-insensitive like Debezium", []string{`Public\.Users`}, []string{"public.users"}, []string{}, []string{}},
		{"mongo qualified", []string{"shop.users", "shop.orders"}, []string{"shop.users"}, []string{}, []string{"shop.orders"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			added, removed := diffTableLists(tc.previous, tc.written)
			if !reflect.DeepEqual(added, tc.wantAdded) || !reflect.DeepEqual(removed, tc.wantRemov) {
				t.Fatalf("added %v removed %v, want %v %v", added, removed, tc.wantAdded, tc.wantRemov)
			}
		})
	}
}

func TestTableNamesAsRequested(t *testing.T) {
	mongo := map[string]interface{}{
		"connector.class":       "io.debezium.connector.mongodb.MongoDbConnector",
		"database.include.list": "shop",
	}
	if got := tableNamesAsRequested(mongo, []string{"users"}, []string{"shop.orders"}); !reflect.DeepEqual(got, []string{"orders"}) {
		t.Fatalf("bare request: %v", got)
	}
	if got := tableNamesAsRequested(mongo, []string{"shop.users"}, []string{"shop.orders"}); !reflect.DeepEqual(got, []string{"shop.orders"}) {
		t.Fatalf("qualified request: %v", got)
	}
	pg := map[string]interface{}{"connector.class": "io.debezium.connector.postgresql.PostgresConnector"}
	if got := tableNamesAsRequested(pg, []string{"users"}, []string{"public.orders"}); !reflect.DeepEqual(got, []string{"public.orders"}) {
		t.Fatalf("postgres: %v", got)
	}
}
