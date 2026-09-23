package executor

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

// The streaming CDC sink pre-creates the Debezium topics it subscribes to. That
// pre-create runs AFTER start_sync, so it used to race Debezium: whichever side created
// a topic first set its partition count, and the other side left it alone. Debezium's
// side was the BROKER's auto-create (1 partition), so asking for anything else here
// produced a count that depended on who won the race.
//
// The race is closed upstream now: executeStreamingDataTransfer resolves the shape from
// the live broker list and hands it to Connect through topic.creation.*, so Connect
// creates the topic itself — through the AdminClient, before it produces — and broker
// auto-create never gets a turn. This pre-create is the backstop, and it asks for the
// SAME number, which it reads off task.Params rather than deriving a second time.
//
// This file pins three things:
//   - the pre-create asks for the resolved count, falling back to 1 when nothing
//     resolved it (batch pipelines, the blob lane, an older caller);
//   - the provider-topic backstop runs BEFORE any pre-create, because a pre-create of a
//     wrongly derived name makes that name real and hides the bug; and
//   - an existing topic with more partitions is reported, not resized.

// sinkTopicEvents is the single ordered record both the fake manager and the log hook
// write to, so a test can ask which happened first.
type sinkTopicEvents struct {
	mu     sync.Mutex
	events []string
}

func (e *sinkTopicEvents) add(s string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, s)
}

func (e *sinkTopicEvents) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.events...)
}

// fakeSinkTopicManager stands in for *kafka.Manager.
type fakeSinkTopicManager struct {
	ev *sinkTopicEvents

	// existing maps a topic to the partition count GetTopicMetadata reports.
	existing map[string]int
	// ensureErr and metaErr make one topic's call fail.
	ensureErr map[string]error
	metaErr   map[string]error
	// metaNil makes GetTopicMetadata return (nil, nil) for a topic. The real manager
	// never does that, but the interface allows it and the check must not panic.
	metaNil map[string]bool

	created   []string         // topics passed to EnsureTopicExists, in order
	requested map[string]int32 // partitions requested per topic
	metaRead  []string         // topics passed to GetTopicMetadata, in order
}

func newFakeSinkTopicManager(ev *sinkTopicEvents) *fakeSinkTopicManager {
	return &fakeSinkTopicManager{
		ev:        ev,
		existing:  map[string]int{},
		ensureErr: map[string]error{},
		metaErr:   map[string]error{},
		metaNil:   map[string]bool{},
		requested: map[string]int32{},
	}
}

func (f *fakeSinkTopicManager) EnsureTopicExists(topic string, partitions int32) error {
	f.ev.add("ensure:" + topic)
	f.created = append(f.created, topic)
	f.requested[topic] = partitions
	if err := f.ensureErr[topic]; err != nil {
		return err
	}
	if _, ok := f.existing[topic]; !ok {
		f.existing[topic] = int(partitions)
	}
	return nil
}

func (f *fakeSinkTopicManager) GetTopicMetadata(topic string) (*kafka.TopicMetadata, error) {
	f.ev.add("meta:" + topic)
	f.metaRead = append(f.metaRead, topic)
	if err := f.metaErr[topic]; err != nil {
		return nil, err
	}
	if f.metaNil[topic] {
		return nil, nil
	}
	n, ok := f.existing[topic]
	if !ok {
		return nil, fmt.Errorf("unknown topic %s", topic)
	}
	return &kafka.TopicMetadata{Name: topic, NumPartitions: n, ReplicationFactor: 1}, nil
}

// sinkTopicLogHook records Warn and Error entries into the shared event list.
type sinkTopicLogHook struct {
	ev      *sinkTopicEvents
	mu      sync.Mutex
	entries []*log.Entry
}

func (h *sinkTopicLogHook) Levels() []log.Level {
	return []log.Level{log.ErrorLevel, log.WarnLevel}
}

func (h *sinkTopicLogHook) Fire(e *log.Entry) error {
	kind := "log:" + e.Level.String()
	if _, ok := e.Data["provider_topic"]; ok && e.Level == log.ErrorLevel {
		kind = "backstop"
	}
	h.ev.add(kind)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = append(h.entries, e)
	return nil
}

func (h *sinkTopicLogHook) warnings() []*log.Entry {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []*log.Entry
	for _, e := range h.entries {
		if e.Level == log.WarnLevel {
			out = append(out, e)
		}
	}
	return out
}

// captureSinkTopicLogs installs the hook on the standard logger for one test.
func captureSinkTopicLogs(t *testing.T, ev *sinkTopicEvents) *sinkTopicLogHook {
	t.Helper()
	h := &sinkTopicLogHook{ev: ev}
	std := log.StandardLogger()
	prev := std.ReplaceHooks(make(log.LevelHooks))
	std.AddHook(h)
	t.Cleanup(func() { std.ReplaceHooks(prev) })
	return h
}

func indexOf(events []string, want string) int {
	for i, e := range events {
		if e == want {
			return i
		}
	}
	return -1
}

func firstWithPrefix(events []string, prefix string) int {
	for i, e := range events {
		if strings.HasPrefix(e, prefix) {
			return i
		}
	}
	return -1
}

const partitionsLiveTopic = "rsync.cdc-ec6d3a3b.shop.customers"

func mongoSinkInputs(tables ...string) sinkTopicInputs {
	return sinkTopicInputs{
		kafkaTopic: partitionsLiveTopic,
		syncMode:   "cdc",
		tables:     tables,
		sourceType: "mongodb",
		pipelineID: "p1",
	}
}

// TestCDCSinkTopicsArePreCreatedWithTheResolvedPartitionCount: every CDC data topic
// rsync pre-creates asks for the count the caller resolved, on both the per-table path
// and the single-topic path — and for 1 when nothing resolved one.
func TestCDCSinkTopicsArePreCreatedWithTheResolvedPartitionCount(t *testing.T) {
	withTopicPrefix(t, nil)

	t.Run("unresolved count falls back to one partition", func(t *testing.T) {
		ev := &sinkTopicEvents{}
		km := newFakeSinkTopicManager(ev)
		got := prepareSinkTopics(km, mongoSinkInputs("shop.customers", "shop.orders", "shop.products"))

		subscribed, ok := got.([]string)
		if !ok {
			t.Fatalf("topics = %#v, want a []string of per-table topics", got)
		}
		if len(km.created) != 3 {
			t.Fatalf("pre-created %d topics %v, want 3", len(km.created), km.created)
		}
		for _, topic := range km.created {
			if p := km.requested[topic]; p != 1 {
				t.Errorf("topic %q pre-created with %d partitions, want 1 (nothing resolved a count, "+
					"so this must stay what every caller got before the count was derived)", topic, p)
			}
		}
		if strings.Join(km.created, ",") != strings.Join(subscribed, ",") {
			t.Errorf("pre-created %v but the sink subscribes to %v", km.created, subscribed)
		}
	})

	t.Run("single provider topic when no tables are selected", func(t *testing.T) {
		ev := &sinkTopicEvents{}
		km := newFakeSinkTopicManager(ev)
		in := mongoSinkInputs()
		got := prepareSinkTopics(km, in)

		if s, ok := got.(string); !ok || s != partitionsLiveTopic {
			t.Fatalf("topics = %#v, want the provider topic %q", got, partitionsLiveTopic)
		}
		if len(km.created) != 1 || km.requested[partitionsLiveTopic] != 1 {
			t.Fatalf("created %v with %v, want %q with 1 partition", km.created, km.requested, partitionsLiveTopic)
		}
	})

	// The load-balancing fix, seen from the backstop: a three-broker cluster resolved 3
	// upstream, and the backstop has to ask for the same 3. Asking for 1 here would not
	// shrink Connect's topic — Kafka cannot reduce a partition count — but on any topic
	// this side wins the race to create, the cluster silently goes back to one leader.
	t.Run("resolved count reaches every pre-created topic", func(t *testing.T) {
		ev := &sinkTopicEvents{}
		km := newFakeSinkTopicManager(ev)
		in := mongoSinkInputs("shop.customers", "shop.orders")
		in.partitions = 3
		got := prepareSinkTopics(km, in)

		if len(km.created) != 2 {
			t.Fatalf("pre-created %d topics %v, want 2", len(km.created), km.created)
		}
		for _, topic := range km.created {
			if p := km.requested[topic]; p != 3 {
				t.Errorf("topic %q pre-created with %d partitions, want the resolved 3", topic, p)
			}
		}
		assertTopicList(t, got, km.created)
	})

	t.Run("resolved count reaches the single provider topic too", func(t *testing.T) {
		ev := &sinkTopicEvents{}
		km := newFakeSinkTopicManager(ev)
		in := mongoSinkInputs()
		in.partitions = 3
		prepareSinkTopics(km, in)

		if km.requested[partitionsLiveTopic] != 3 {
			t.Fatalf("created %v with %v, want %q with the resolved 3", km.created, km.requested, partitionsLiveTopic)
		}
	})

	// 0 is "not resolved", but a negative or absurd value must not reach EnsureTopicExists:
	// Kafka rejects partitions<1 outright, which would fail the pre-create of every topic.
	t.Run("a nonsense count falls back rather than reaching the broker", func(t *testing.T) {
		for _, bad := range []int32{0, -1} {
			ev := &sinkTopicEvents{}
			km := newFakeSinkTopicManager(ev)
			in := mongoSinkInputs()
			in.partitions = bad
			prepareSinkTopics(km, in)
			if p := km.requested[partitionsLiveTopic]; p != 1 {
				t.Errorf("partitions=%d → pre-created with %d, want the fallback 1", bad, p)
			}
		}
	})
}

// TestSinkTopicBackstopRunsBeforePreCreate: when the per-table derivation misses the
// topic the provider reported, the backstop must add it BEFORE any topic is created.
// The control case (a correct derivation) shows the backstop probe does not fire on its
// own, while pre-creates still happen, so a missing event there is meaningful.
func TestSinkTopicBackstopRunsBeforePreCreate(t *testing.T) {
	withTopicPrefix(t, nil)

	t.Run("derivation misses the live topic", func(t *testing.T) {
		ev := &sinkTopicEvents{}
		captureSinkTopicLogs(t, ev)
		km := newFakeSinkTopicManager(ev)

		// These tables do not rebuild to the live topic, so the backstop must fire.
		got := prepareSinkTopics(km, mongoSinkInputs("inventory.items", "inventory.stock"))

		events := ev.snapshot()
		backstop := indexOf(events, "backstop")
		firstEnsure := firstWithPrefix(events, "ensure:")
		if backstop < 0 {
			t.Fatalf("backstop never fired; events %v", events)
		}
		if firstEnsure < 0 {
			t.Fatalf("nothing was pre-created; events %v", events)
		}
		if backstop > firstEnsure {
			t.Fatalf("a topic was pre-created before the backstop ran (events %v). The pre-create "+
				"must use the repaired list, or it makes wrongly derived topics real", events)
		}

		subscribed, ok := got.([]string)
		if !ok || len(subscribed) != 3 {
			t.Fatalf("topics = %#v, want the live topic plus 2 derived topics", got)
		}
		if strings.Join(km.created, ",") != strings.Join(subscribed, ",") {
			t.Fatalf("pre-created %v but the sink subscribes to %v", km.created, subscribed)
		}
		if km.created[0] != partitionsLiveTopic || km.requested[partitionsLiveTopic] != 1 {
			t.Fatalf("live topic not pre-created first with 1 partition: created %v requested %v",
				km.created, km.requested)
		}
	})

	t.Run("control: correct derivation", func(t *testing.T) {
		ev := &sinkTopicEvents{}
		captureSinkTopicLogs(t, ev)
		km := newFakeSinkTopicManager(ev)

		prepareSinkTopics(km, mongoSinkInputs("shop.customers", "shop.orders"))

		events := ev.snapshot()
		if i := indexOf(events, "backstop"); i >= 0 {
			t.Fatalf("backstop fired for a correct derivation; events %v", events)
		}
		if n := len(km.created); n != 2 {
			t.Fatalf("pre-created %d topics, want 2; events %v", n, events)
		}
	})
}

// TestExistingWideCDCTopicIsReportedNotResized: a topic that already has more than one
// partition gets a warning naming it and its count; nothing asks for a different count,
// one-partition topics stay quiet, and a topic that could not be created is never
// looked up (a metadata read can auto-create it on a broker with auto-create on).
func TestExistingWideCDCTopicIsReportedNotResized(t *testing.T) {
	withTopicPrefix(t, nil)

	const (
		orders   = "rsync.cdc-ec6d3a3b.shop.orders"
		products = "rsync.cdc-ec6d3a3b.shop.products"
		refunds  = "rsync.cdc-ec6d3a3b.shop.refunds"
		returns  = "rsync.cdc-ec6d3a3b.shop.returns"
	)
	ev := &sinkTopicEvents{}
	hook := captureSinkTopicLogs(t, ev)
	km := newFakeSinkTopicManager(ev)
	km.existing[partitionsLiveTopic] = 3 // created earlier with the old count
	km.existing[orders] = 1
	km.metaErr[products] = errors.New("metadata unavailable")
	km.ensureErr[refunds] = errors.New("not authorized to create topics")
	km.metaNil[returns] = true

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("partition check panicked on empty metadata: %v", r)
			}
		}()
		prepareSinkTopics(km, mongoSinkInputs("shop.customers", "shop.orders", "shop.products", "shop.refunds", "shop.returns"))
	}()

	if len(km.created) != 5 {
		t.Fatalf("pre-create attempted for %v, want 5 topics", km.created)
	}
	for topic, p := range km.requested {
		if p != 1 {
			t.Errorf("topic %q requested with %d partitions, want 1", topic, p)
		}
	}
	for _, topic := range km.metaRead {
		if topic == refunds {
			t.Errorf("partition count read for %q after its pre-create failed", refunds)
		}
	}
	if len(km.metaRead) != 4 {
		t.Errorf("partition count read for %v, want the 4 topics that exist", km.metaRead)
	}

	var wide, failed []*log.Entry
	for _, e := range hook.warnings() {
		switch {
		case e.Data["partitions"] != nil:
			wide = append(wide, e)
		case e.Data["topic"] == refunds:
			failed = append(failed, e)
		default:
			t.Errorf("unexpected warning for %v: %s", e.Data["topic"], e.Message)
		}
	}
	if len(failed) != 1 {
		t.Errorf("got %d pre-create failure warnings, want 1 for %q", len(failed), refunds)
	}
	if len(wide) != 1 {
		t.Fatalf("got %d partition warnings, want exactly 1 (for %q, not the 1-partition %q)",
			len(wide), partitionsLiveTopic, orders)
	}
	w := wide[0]
	if w.Data["topic"] != partitionsLiveTopic || w.Data["partitions"] != 3 {
		t.Errorf("warning fields %v, want topic %q partitions 3", w.Data, partitionsLiveTopic)
	}
	// Operators find this warning by pipeline, so the field must be there.
	if w.Data["pipeline_id"] != "p1" {
		t.Errorf("warning fields %v, want pipeline_id %q", w.Data, "p1")
	}
	if !strings.Contains(w.Message, partitionsLiveTopic) || !strings.Contains(w.Message, "3 partitions") {
		t.Errorf("warning text must name the topic and its count, got %q", w.Message)
	}
}

// TestBatchBackfillSinkTopicIsNotPreCreated: the hybrid batch topic already exists
// from its bootstrap marker and must not be touched. The control flips only the
// backfill flag and shows the fake does record calls for the same input.
func TestBatchBackfillSinkTopicIsNotPreCreated(t *testing.T) {
	withTopicPrefix(t, nil)
	in := sinkTopicInputs{
		kafkaTopic:    "rsync.pipeline.ec6d3a3b.data",
		syncMode:      "cdc",
		tables:        []string{"shop.customers"},
		sourceType:    "mongodb",
		pipelineID:    "p1",
		batchBackfill: true,
	}

	ev := &sinkTopicEvents{}
	km := newFakeSinkTopicManager(ev)
	got := prepareSinkTopics(km, in)
	if s, ok := got.(string); !ok || s != in.kafkaTopic {
		t.Fatalf("topics = %#v, want the batch topic unchanged", got)
	}
	if events := ev.snapshot(); len(events) != 0 {
		t.Fatalf("batch-backfill sink touched Kafka: %v", events)
	}

	control := in
	control.batchBackfill = false
	ckm := newFakeSinkTopicManager(&sinkTopicEvents{})
	prepareSinkTopics(ckm, control)
	if len(ckm.created) == 0 {
		t.Fatal("control: the same input without the backfill flag created nothing, so the " +
			"empty result above proves nothing")
	}

	if got := prepareSinkTopics(nil, mongoSinkInputs("shop.customers")); got == nil {
		t.Fatal("nil manager: topics must still be returned")
	}
}

// TestSinkTopicCreatorForNilManagerIsNilInterface: an agent without a Kafka manager
// must skip the pre-create. A nil *kafka.Manager wrapped in the interface is not nil,
// so prepareSinkTopics would call it and panic. The control shows a real manager is
// passed through.
func TestSinkTopicCreatorForNilManagerIsNilInterface(t *testing.T) {
	if got := sinkTopicCreatorFor(nil); got != nil {
		t.Fatalf("sinkTopicCreatorFor(nil) = %#v, want a nil interface", got)
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("prepareSinkTopics panicked with no Kafka manager: %v", r)
			}
		}()
		prepareSinkTopics(sinkTopicCreatorFor(nil), mongoSinkInputs("shop.customers"))
	}()

	m := &kafka.Manager{}
	if got := sinkTopicCreatorFor(m); got == nil {
		t.Fatal("control: a real manager was dropped")
	}
}

func assertTopicList(t *testing.T, got interface{}, want []string) {
	t.Helper()
	list, ok := got.([]string)
	if !ok {
		t.Fatalf("topics = %#v, want the list %v", got, want)
	}
	if strings.Join(list, ",") != strings.Join(want, ",") {
		t.Fatalf("topics = %v, want %v", list, want)
	}
}

// TestSinkTopicsForUnifiedTopicMode: a unified topic is set by itself when a selected
// table is named dim_*, dimension_*, lookup_*, lkp_* or ref_*. Those tables must
// subscribe to the unified topic (created with 1 partition), and the backstop must stay
// quiet even though the provider reported the dimension table's own topic. The control
// removes only the unified topic and gets a different list, so the input is used.
func TestSinkTopicsForUnifiedTopicMode(t *testing.T) {
	withTopicPrefix(t, nil)

	const (
		live    = "rsync.cdc-ec6d3a3b.shop.dim_region"
		unified = "rsync.cdc-ec6d3a3b.dimensions"
		orders  = "rsync.cdc-ec6d3a3b.shop.orders"
	)
	tables := []string{"shop.dim_region", "shop.orders"}

	t.Run("unified topic set", func(t *testing.T) {
		ev := &sinkTopicEvents{}
		captureSinkTopicLogs(t, ev)
		km := newFakeSinkTopicManager(ev)
		task := ExecutorTask{
			PipelineID: "p1",
			Source:     &ConnectorConfig{Type: "mongodb"},
			Params:     map[string]interface{}{"cdc_unified_topic": " " + unified + " "},
		}

		got := prepareSinkTopics(km, sinkTopicInputsFor(task, live, "cdc", tables, false))

		assertTopicList(t, got, []string{unified, orders})
		if events := ev.snapshot(); indexOf(events, "backstop") >= 0 {
			t.Errorf("backstop fired in unified-topic mode; events %v", events)
		}
		if strings.Join(km.created, ",") != unified+","+orders {
			t.Errorf("pre-created %v, want %v", km.created, []string{unified, orders})
		}
		if p := km.requested[unified]; p != 1 {
			t.Errorf("unified topic pre-created with %d partitions, want 1", p)
		}
	})

	t.Run("control: no unified topic", func(t *testing.T) {
		ev := &sinkTopicEvents{}
		captureSinkTopicLogs(t, ev)
		km := newFakeSinkTopicManager(ev)
		task := ExecutorTask{PipelineID: "p1", Source: &ConnectorConfig{Type: "mongodb"}}

		got := prepareSinkTopics(km, sinkTopicInputsFor(task, live, "cdc", tables, false))

		assertTopicList(t, got, []string{live, orders})
		if events := ev.snapshot(); indexOf(events, "backstop") >= 0 {
			t.Errorf("backstop fired for a correct derivation; events %v", events)
		}
	})
}

// TestSinkTopicsForSQLServerKeepTheDatabaseSegment: SQL Server topics carry the
// database as an extra segment ("{prefix}.{db}.{schema}.{table}"), so a "dbo.orders"
// selection must become "...salesdb.dbo.orders". The control uses the same topic and
// tables with another source type and gets a different list, so the source type is
// what produced the result.
func TestSinkTopicsForSQLServerKeepTheDatabaseSegment(t *testing.T) {
	withTopicPrefix(t, nil)

	const live = "rsync.cdc-ec6d3a3b.salesdb.dbo.customers"
	tables := []string{"dbo.customers", "dbo.orders"}
	want := []string{live, "rsync.cdc-ec6d3a3b.salesdb.dbo.orders"}

	ev := &sinkTopicEvents{}
	captureSinkTopicLogs(t, ev)
	km := newFakeSinkTopicManager(ev)
	task := ExecutorTask{PipelineID: "p1", Source: &ConnectorConfig{Type: "sqlserver"}}

	got := prepareSinkTopics(km, sinkTopicInputsFor(task, live, "cdc", tables, false))

	assertTopicList(t, got, want)
	if events := ev.snapshot(); indexOf(events, "backstop") >= 0 {
		t.Errorf("backstop fired for SQL Server topics; events %v", events)
	}
	if strings.Join(km.created, ",") != strings.Join(want, ",") {
		t.Errorf("pre-created %v, want %v", km.created, want)
	}
	for _, topic := range km.created {
		if p := km.requested[topic]; p != 1 {
			t.Errorf("topic %q pre-created with %d partitions, want 1", topic, p)
		}
	}

	ctask := ExecutorTask{PipelineID: "p1", Source: &ConnectorConfig{Type: "postgresql"}}
	cgot := prepareSinkTopics(newFakeSinkTopicManager(&sinkTopicEvents{}),
		sinkTopicInputsFor(ctask, live, "cdc", tables, false))
	if list, ok := cgot.([]string); !ok || strings.Join(list, ",") == strings.Join(want, ",") {
		t.Fatalf("control: a postgresql source gave %#v, the same as SQL Server, so the source type "+
			"did not decide the result above", cgot)
	}
}

// TestNonCDCSyncModeKeepsTheProviderTopic: the per-table rebuild applies only to
// sync_mode "cdc". With any other mode (or none resolved) the sink keeps the single
// topic it was given, still created with 1 partition. The control is the same input with
// "cdc", which does rebuild.
func TestNonCDCSyncModeKeepsTheProviderTopic(t *testing.T) {
	withTopicPrefix(t, nil)
	tables := []string{"shop.customers", "shop.orders"}

	for _, mode := range []string{"", "batch"} {
		t.Run("sync_mode="+mode, func(t *testing.T) {
			km := newFakeSinkTopicManager(&sinkTopicEvents{})
			in := mongoSinkInputs(tables...)
			in.syncMode = mode

			got := prepareSinkTopics(km, in)

			if s, ok := got.(string); !ok || s != partitionsLiveTopic {
				t.Fatalf("topics = %#v, want only the provider topic %q", got, partitionsLiveTopic)
			}
			if strings.Join(km.created, ",") != partitionsLiveTopic || km.requested[partitionsLiveTopic] != 1 {
				t.Fatalf("pre-created %v with %v, want %q with 1 partition", km.created, km.requested, partitionsLiveTopic)
			}
		})
	}

	got := prepareSinkTopics(newFakeSinkTopicManager(&sinkTopicEvents{}), mongoSinkInputs(tables...))
	assertTopicList(t, got, []string{partitionsLiveTopic, "rsync.cdc-ec6d3a3b.shop.orders"})
}

// TestProviderTopicWithoutTableSegmentIsNotRebuilt: when the provider reports no topic,
// the CDC stream topic falls back to the connector name alone ("rsync.cdc-<id>"), which
// has no database or table segment. Nothing can be rebuilt from it, so the sink keeps
// it and no ".db.table" topic is ever created. The control gives the full topic and
// gets one topic per table.
func TestProviderTopicWithoutTableSegmentIsNotRebuilt(t *testing.T) {
	withTopicPrefix(t, nil)
	const bare = "rsync.cdc-ec6d3a3b"

	km := newFakeSinkTopicManager(&sinkTopicEvents{})
	in := mongoSinkInputs("shop.customers", "shop.orders")
	in.kafkaTopic = bare

	got := prepareSinkTopics(km, in)

	if s, ok := got.(string); !ok || s != bare {
		t.Fatalf("topics = %#v, want only %q", got, bare)
	}
	if strings.Join(km.created, ",") != bare {
		t.Fatalf("pre-created %v, want only %q (no topic without a prefix)", km.created, bare)
	}

	cgot := prepareSinkTopics(newFakeSinkTopicManager(&sinkTopicEvents{}), mongoSinkInputs("shop.customers", "shop.orders"))
	assertTopicList(t, cgot, []string{partitionsLiveTopic, "rsync.cdc-ec6d3a3b.shop.orders"})
}

// TestBlankSinkTopicNamesAreNeverCreated: a blank name must never reach the broker, and
// a table list that is all blank must not replace the provider topic with an empty list
// (the sink would subscribe to nothing). The first case is the control: the fake does
// record the one real name in the same call.
func TestBlankSinkTopicNamesAreNeverCreated(t *testing.T) {
	withTopicPrefix(t, nil)

	t.Run("blank names in the list", func(t *testing.T) {
		km := newFakeSinkTopicManager(&sinkTopicEvents{})
		preCreateCDCSinkTopics(km, []string{"", "   ", partitionsLiveTopic}, "p1", 0)
		if strings.Join(km.created, ",") != partitionsLiveTopic {
			t.Fatalf("pre-created %q, want only %q", km.created, partitionsLiveTopic)
		}
	})

	t.Run("blank provider topic", func(t *testing.T) {
		ev := &sinkTopicEvents{}
		km := newFakeSinkTopicManager(ev)
		in := mongoSinkInputs("shop.customers")
		in.kafkaTopic = "  "

		prepareSinkTopics(km, in)

		if events := ev.snapshot(); len(events) != 0 {
			t.Fatalf("a blank topic reached Kafka: %q", events)
		}
	})

	t.Run("all table names blank keep the provider topic", func(t *testing.T) {
		km := newFakeSinkTopicManager(&sinkTopicEvents{})

		got := prepareSinkTopics(km, mongoSinkInputs("", "  "))

		if s, ok := got.(string); !ok || s != partitionsLiveTopic {
			t.Fatalf("topics = %#v, want the provider topic %q, not an empty list", got, partitionsLiveTopic)
		}
		if strings.Join(km.created, ",") != partitionsLiveTopic {
			t.Fatalf("pre-created %v, want %q", km.created, partitionsLiveTopic)
		}
	})
}

// TestSinkTopicInputsForReadsTheTask: the values startKafkaMCPSink already resolved
// pass through unchanged, and the unified topic, source type and pipeline id come from
// the task. Two calls with different values keep a constant from passing.
func TestSinkTopicInputsForReadsTheTask(t *testing.T) {
	tables := []string{"shop.customers", "shop.orders"}
	task := ExecutorTask{
		PipelineID: "p-42",
		Source:     &ConnectorConfig{Type: "sqlserver"},
		Params:     map[string]interface{}{"cdc_unified_topic": "  rsync.cdc-ec6d3a3b.dimensions \n"},
	}

	got := sinkTopicInputsFor(task, partitionsLiveTopic, "cdc", tables, true)
	want := sinkTopicInputs{
		kafkaTopic:    partitionsLiveTopic,
		syncMode:      "cdc",
		tables:        tables,
		unifiedTopic:  "rsync.cdc-ec6d3a3b.dimensions",
		sourceType:    "sqlserver",
		pipelineID:    "p-42",
		batchBackfill: true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sinkTopicInputsFor = %+v, want %+v", got, want)
	}

	other := ExecutorTask{
		PipelineID: "p-7",
		Source:     &ConnectorConfig{Type: "mongodb"},
		Params:     map[string]interface{}{"cdc_unified_topic": "rsync.cdc-0badf00d.dimensions"},
	}
	got = sinkTopicInputsFor(other, "rsync.pipeline.0badf00d.data", "batch", []string{"a.b"}, false)
	want = sinkTopicInputs{
		kafkaTopic:    "rsync.pipeline.0badf00d.data",
		syncMode:      "batch",
		tables:        []string{"a.b"},
		unifiedTopic:  "rsync.cdc-0badf00d.dimensions",
		sourceType:    "mongodb",
		pipelineID:    "p-7",
		batchBackfill: false,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sinkTopicInputsFor = %+v, want %+v", got, want)
	}

	t.Run("no params and no source", func(t *testing.T) {
		got := sinkTopicInputsFor(ExecutorTask{PipelineID: "p-42"}, partitionsLiveTopic, "cdc", tables, false)
		if got.unifiedTopic != "" || got.sourceType != "" {
			t.Fatalf("got unified %q source %q, want both empty", got.unifiedTopic, got.sourceType)
		}
	})

	t.Run("unified topic that is not a string", func(t *testing.T) {
		task := ExecutorTask{Params: map[string]interface{}{"cdc_unified_topic": 7}}
		if got := sinkTopicInputsFor(task, partitionsLiveTopic, "cdc", tables, false); got.unifiedTopic != "" {
			t.Fatalf("unified topic = %q, want empty", got.unifiedTopic)
		}
	})

	// The count travels from executeStreamingDataTransfer to the backstop on the task,
	// rather than being derived twice. A key that stops being read is silent: the
	// backstop just goes back to asking for 1 and only the topics it wins the race for
	// are affected, so nothing reports it.
	t.Run("the resolved partition count is read off the task", func(t *testing.T) {
		task := ExecutorTask{Params: map[string]interface{}{"cdc_topic_partitions": 3}}
		if got := sinkTopicInputsFor(task, partitionsLiveTopic, "cdc", tables, false); got.partitions != 3 {
			t.Fatalf("partitions = %d, want 3 (task.Params[\"cdc_topic_partitions\"])", got.partitions)
		}
	})

	t.Run("an absent or unusable count leaves the fallback in place", func(t *testing.T) {
		for _, v := range []interface{}{nil, 0, -1, "3", int32(3), 3.0} {
			params := map[string]interface{}{}
			if v != nil {
				params["cdc_topic_partitions"] = v
			}
			task := ExecutorTask{Params: params}
			if got := sinkTopicInputsFor(task, partitionsLiveTopic, "cdc", tables, false); got.partitions != 0 {
				t.Errorf("cdc_topic_partitions=%#v → partitions %d, want 0 (read as the fallback)", v, got.partitions)
			}
		}
	})
}

// sinkTopicWiringProblems checks the call in startKafkaMCPSink that picks the sink's
// topics, and returns what is wrong with it (nil when nothing is).
func sinkTopicWiringProblems(file *ast.File) []string {
	var fn *ast.FuncDecl
	for _, d := range file.Decls {
		if f, ok := d.(*ast.FuncDecl); ok && f.Name.Name == "startKafkaMCPSink" {
			fn = f
		}
	}
	if fn == nil || fn.Body == nil {
		return []string{"startKafkaMCPSink not found in executor.go; if it moved or was renamed, update this test"}
	}

	var problems []string
	var prepares []*ast.CallExpr
	var topicsValues []string // values given to the sink's "topics" config key
	direct, topicsParamAssigns := 0, 0
	assignedFromPrepare := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			for _, lhs := range x.Lhs {
				if id, ok := lhs.(*ast.Ident); ok && id.Name == "topicsParam" {
					topicsParamAssigns++
					if len(x.Lhs) == 1 && len(x.Rhs) == 1 {
						if c, ok := x.Rhs[0].(*ast.CallExpr); ok && exprText(c.Fun) == "prepareSinkTopics" {
							assignedFromPrepare = true
						}
					}
				}
			}
		case *ast.KeyValueExpr:
			if k, ok := x.Key.(*ast.BasicLit); ok && k.Kind == token.STRING && k.Value == `"topics"` {
				topicsValues = append(topicsValues, exprText(x.Value))
			}
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch f := call.Fun.(type) {
		case *ast.SelectorExpr:
			if f.Sel.Name == "EnsureTopicExists" || f.Sel.Name == "EnsureTopicExistsWithConfig" {
				direct++
			}
		case *ast.Ident:
			if f.Name == "prepareSinkTopics" {
				prepares = append(prepares, call)
			}
		}
		return true
	})
	if direct != 0 {
		problems = append(problems, fmt.Sprintf("startKafkaMCPSink calls EnsureTopicExists directly %d time(s); "+
			"pre-create through prepareSinkTopics so the backstop runs first and the count stays 1", direct))
	}
	// The sink must subscribe to exactly what prepareSinkTopics returned and pre-created.
	if !assignedFromPrepare || topicsParamAssigns != 1 {
		problems = append(problems, fmt.Sprintf("topicsParam is assigned %d time(s) (from prepareSinkTopics: %v), "+
			"want once, from prepareSinkTopics", topicsParamAssigns, assignedFromPrepare))
	}
	if len(topicsValues) != 1 || topicsValues[0] != "topicsParam" {
		problems = append(problems, fmt.Sprintf("the sink's \"topics\" config is %v, want [topicsParam]", topicsValues))
	}
	if len(prepares) != 1 {
		return append(problems, fmt.Sprintf("startKafkaMCPSink calls prepareSinkTopics %d time(s), want 1", len(prepares)))
	}
	call := prepares[0]
	if len(call.Args) != 2 {
		return append(problems, fmt.Sprintf("prepareSinkTopics has %d arguments, want 2", len(call.Args)))
	}

	recv := ""
	if fn.Recv != nil && len(fn.Recv.List) == 1 && len(fn.Recv.List[0].Names) == 1 {
		recv = fn.Recv.List[0].Names[0].Name
	}
	if got, want := exprText(call.Args[0]), "sinkTopicCreatorFor("+recv+".kafkaManager)"; got != want {
		problems = append(problems, fmt.Sprintf("manager argument is %s, want %s (anything else skips the pre-create)", got, want))
	}

	// Each value must be the one startKafkaMCPSink resolved: a wrong one here makes the
	// sink subscribe to too few topics, or none, while the pipeline shows running.
	inner, ok := call.Args[1].(*ast.CallExpr)
	if !ok {
		return append(problems, fmt.Sprintf("inputs argument is %s, want a sinkTopicInputsFor call", exprText(call.Args[1])))
	}
	want := "sinkTopicInputsFor(task, kafkaTopic, syncMode, tablesList, isBatchBackfillTopic)"
	if got := exprText(inner); got != want {
		problems = append(problems, fmt.Sprintf("inputs argument is %s, want %s", got, want))
	}
	return problems
}

// TestStartKafkaMCPSinkPreCreatesOnlyThroughPrepareSinkTopics: startKafkaMCPSink cannot
// run in a unit test (it resolves and starts the destination connector container before
// it picks topics), so the one call that picks the sink's topics is checked from the
// source: it must reach topic creation only through prepareSinkTopics, with the real
// manager and the values the function resolved. The controls apply each wrong wiring to
// a copy of executor.go and show the check rejects it, so a clean result on the real
// file means something.
func TestStartKafkaMCPSinkPreCreatesOnlyThroughPrepareSinkTopics(t *testing.T) {
	src, err := os.ReadFile("executor.go")
	if err != nil {
		t.Fatalf("read executor.go: %v", err)
	}
	check := func(source string) []string {
		t.Helper()
		file, err := parser.ParseFile(token.NewFileSet(), "executor.go", source, 0)
		if err != nil {
			t.Fatalf("parse executor.go: %v", err)
		}
		return sinkTopicWiringProblems(file)
	}

	if problems := check(string(src)); len(problems) != 0 {
		t.Fatalf("startKafkaMCPSink topic wiring:\n  %s", strings.Join(problems, "\n  "))
	}

	const (
		inputs  = "sinkTopicInputsFor(task, kafkaTopic, syncMode, tablesList, isBatchBackfillTopic)"
		manager = "prepareSinkTopics(sinkTopicCreatorFor(a.kafkaManager),"
		assign  = "\ttopicsParam := prepareSinkTopics("
	)
	controls := []struct{ name, old, new string }{
		{"no tables", inputs, "sinkTopicInputsFor(task, kafkaTopic, syncMode, nil, isBatchBackfillTopic)"},
		{"no provider topic", inputs, `sinkTopicInputsFor(task, "", syncMode, tablesList, isBatchBackfillTopic)`},
		{"sync mode forced", inputs, `sinkTopicInputsFor(task, kafkaTopic, "cdc", tablesList, isBatchBackfillTopic)`},
		{"sink mode instead of sync mode", inputs, "sinkTopicInputsFor(task, kafkaTopic, sinkMode, tablesList, isBatchBackfillTopic)"},
		{"backfill forced off", inputs, "sinkTopicInputsFor(task, kafkaTopic, syncMode, tablesList, false)"},
		{"empty task", inputs, "sinkTopicInputsFor(ExecutorTask{}, kafkaTopic, syncMode, tablesList, isBatchBackfillTopic)"},
		{"arguments swapped", inputs, "sinkTopicInputsFor(task, syncMode, kafkaTopic, tablesList, isBatchBackfillTopic)"},
		{"nil manager", manager, "prepareSinkTopics(sinkTopicCreatorFor(nil),"},
		{"direct create added", assign, "\t_ = a.kafkaManager.EnsureTopicExists(kafkaTopic, 3)\n" + assign},
		{"sink given the provider topic", "\"topics\":                  topicsParam,", "\"topics\":                  kafkaTopic,"},
		{"result replaced after the call", "\n\tsinkReq := mcp.ExecuteRequest{", "\n\ttopicsParam = kafkaTopic\n\tsinkReq := mcp.ExecuteRequest{"},
	}
	for _, c := range controls {
		t.Run("control: "+c.name, func(t *testing.T) {
			if n := strings.Count(string(src), c.old); n != 1 {
				t.Fatalf("expected %q exactly once in executor.go, found %d; update this test", c.old, n)
			}
			if problems := check(strings.Replace(string(src), c.old, c.new, 1)); len(problems) == 0 {
				t.Fatalf("the wiring check accepted a startKafkaMCPSink with %s", c.name)
			}
		})
	}
}
