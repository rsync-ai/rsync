package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// The orchestrator never finished a graceful shutdown: `docker stop -t 30` ended
// in SIGKILL (exit 137) every time. Manager.Close closed its nine consumer groups
// one after another, and because they all shared one client, each group's close
// queued behind the others' requests on the same broker connection. That took
// 33 s, past the grace period. Meanwhile every closed group's consume loop
// retried Consume every 5 s and logged "tried to use a consumer group that was
// closed" until the process died.
//
// These tests pin both halves: the groups close together, bounded by a deadline,
// and a loop whose group is closed or whose context is cancelled returns. Each
// group now also has a client of its own; manager_consumer_client_test.go pins
// that.

// fakeGroup is a sarama.ConsumerGroup whose Close takes closeDelay (or blocks
// until release is closed) and whose Consume answers from consume.
type fakeGroup struct {
	closeDelay time.Duration
	release    chan struct{}
	closed     atomic.Bool
	consumes   atomic.Int32
	consume    func(ctx context.Context, call int32) error
}

func (g *fakeGroup) Consume(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
	return g.consume(ctx, g.consumes.Add(1))
}

func (g *fakeGroup) Close() error {
	if g.release != nil {
		<-g.release
	}
	time.Sleep(g.closeDelay)
	g.closed.Store(true)
	return nil
}

func (g *fakeGroup) Errors() <-chan error                 { return nil }
func (g *fakeGroup) Pause(partitions map[string][]int32)  {}
func (g *fakeGroup) Resume(partitions map[string][]int32) {}
func (g *fakeGroup) PauseAll()                            {}
func (g *fakeGroup) ResumeAll()                           {}

// recordingProducer and recordingClient stand in for what Close shuts after the
// groups. The embedded interfaces are nil, so calling anything but Close panics.
type recordingProducer struct {
	sarama.SyncProducer
	closed atomic.Bool
}

func (p *recordingProducer) Close() error { p.closed.Store(true); return nil }

type recordingClient struct {
	sarama.Client
	closed atomic.Bool
}

func (c *recordingClient) Close() error { c.closed.Store(true); return nil }

func slowGroups(n int, delay time.Duration) map[string]sarama.ConsumerGroup {
	groups := make(map[string]sarama.ConsumerGroup, n)
	for i := 0; i < n; i++ {
		groups[fmt.Sprintf("rsync.healer.schema-changes.w%d", i)] = &fakeGroup{closeDelay: delay}
	}
	return groups
}

func TestCloseConsumerGroupsClosesThemTogether(t *testing.T) {
	const n, delay = 9, 200 * time.Millisecond
	groups := slowGroups(n, delay)

	start := time.Now()
	closeConsumerGroups(groups, 10*time.Second)
	elapsed := time.Since(start)

	for topic, g := range groups {
		if !g.(*fakeGroup).closed.Load() {
			t.Errorf("group for %s was not closed", topic)
		}
	}
	// One after another this is n*delay = 1.8 s; together it is about one delay.
	if sequential := n * delay; elapsed >= sequential/2 {
		t.Errorf("closing %d groups took %s, want well under the sequential %s", n, elapsed, sequential)
	}
}

func TestCloseConsumerGroupsGivesUpAtTheDeadline(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	groups := map[string]sarama.ConsumerGroup{
		"stuck": &fakeGroup{release: release},
		"quick": &fakeGroup{},
	}

	start := time.Now()
	closeConsumerGroups(groups, 100*time.Millisecond)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("closeConsumerGroups waited %s for a group that never leaves; the deadline was 100ms", elapsed)
	}
	if !groups["quick"].(*fakeGroup).closed.Load() {
		t.Error("a group that could close was not closed")
	}
}

func TestManagerCloseClosesGroupsTogetherThenProducerAndClient(t *testing.T) {
	const n, delay = 9, 200 * time.Millisecond
	producer, client := &recordingProducer{}, &recordingClient{}
	m := newTestManager(producer)
	m.client = client
	m.consumers = slowGroups(n, delay)
	m.consumeStops = map[string]context.CancelFunc{}
	var ctxs []context.Context
	for topic := range m.consumers {
		ctx, cancel := context.WithCancel(context.Background())
		m.consumeStops[topic] = cancel
		ctxs = append(ctxs, ctx)
	}

	start := time.Now()
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	elapsed := time.Since(start)

	if sequential := n * delay; elapsed >= sequential/2 {
		t.Errorf("Close took %s, want well under the sequential %s", elapsed, sequential)
	}
	for i, ctx := range ctxs {
		if ctx.Err() == nil {
			t.Errorf("consume loop %d was not cancelled", i)
		}
	}
	if len(m.consumeStops) != 0 {
		t.Errorf("%d consume stops left after Close", len(m.consumeStops))
	}
	if !producer.closed.Load() || !client.closed.Load() {
		t.Errorf("producer closed=%v client closed=%v, want both", producer.closed.Load(), client.closed.Load())
	}
}

func TestManagerCloseStillClosesTheClientWhenAGroupHangs(t *testing.T) {
	old := consumerCloseTimeout
	consumerCloseTimeout = 100 * time.Millisecond
	defer func() { consumerCloseTimeout = old }()

	release := make(chan struct{})
	defer close(release)
	producer, client := &recordingProducer{}, &recordingClient{}
	m := newTestManager(producer)
	m.client = client
	m.consumers["stuck"] = &fakeGroup{release: release}

	start := time.Now()
	_ = m.Close()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Close waited %s for a group that never leaves", elapsed)
	}
	if !producer.closed.Load() || !client.closed.Load() {
		t.Errorf("producer closed=%v client closed=%v, want both", producer.closed.Load(), client.closed.Load())
	}
}

// runLoop runs consumeLoop and reports whether it returned within limit.
func runLoop(t *testing.T, ctx context.Context, g *fakeGroup, limit time.Duration) bool {
	t.Helper()
	done := make(chan struct{})
	go func() {
		consumeLoop(ctx, "rsync.healer.schema-changes", g, nil)
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(limit):
		return false
	}
}

func TestConsumeLoopReturnsOnceItsGroupIsClosed(t *testing.T) {
	old := consumeRetryDelay
	consumeRetryDelay = time.Millisecond
	defer func() { consumeRetryDelay = old }()

	g := &fakeGroup{consume: func(context.Context, int32) error { return sarama.ErrClosedConsumerGroup }}
	if !runLoop(t, context.Background(), g, 2*time.Second) {
		t.Fatalf("consumeLoop kept retrying a closed group (%d Consume calls)", g.consumes.Load())
	}
	if calls := g.consumes.Load(); calls != 1 {
		t.Errorf("Consume called %d times, want 1: a closed group never opens again", calls)
	}
}

func TestConsumeLoopReturnsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// Like sarama, Consume blocks for the whole session and returns nil when
	// the context ends it.
	g := &fakeGroup{consume: func(ctx context.Context, _ int32) error { <-ctx.Done(); return nil }}
	time.AfterFunc(50*time.Millisecond, cancel)
	if !runLoop(t, ctx, g, 2*time.Second) {
		t.Fatal("consumeLoop did not return after its context was cancelled")
	}
}

func TestConsumeLoopReturnsWhenCancelledDuringTheRetryWait(t *testing.T) {
	old := consumeRetryDelay
	consumeRetryDelay = time.Hour
	defer func() { consumeRetryDelay = old }()

	ctx, cancel := context.WithCancel(context.Background())
	g := &fakeGroup{consume: func(context.Context, int32) error { return errors.New("kafka: broker not available") }}
	time.AfterFunc(50*time.Millisecond, cancel)
	if !runLoop(t, ctx, g, 2*time.Second) {
		t.Fatal("consumeLoop slept out its retry delay instead of returning on cancel")
	}
}

// Any other error is still retried: a broker that is briefly unreachable must
// not stop the orchestrator consuming that topic for the rest of its life.
func TestConsumeLoopRetriesOtherErrors(t *testing.T) {
	old := consumeRetryDelay
	consumeRetryDelay = time.Millisecond
	defer func() { consumeRetryDelay = old }()

	g := &fakeGroup{consume: func(_ context.Context, call int32) error {
		if call < 3 {
			return errors.New("kafka: broker not available")
		}
		return sarama.ErrClosedConsumerGroup
	}}
	if !runLoop(t, context.Background(), g, 2*time.Second) {
		t.Fatal("consumeLoop did not return")
	}
	if calls := g.consumes.Load(); calls != 3 {
		t.Errorf("Consume called %d times, want 3: two retried failures, then the closed group", calls)
	}
}
