package kafka

import (
	"context"
	"go/ast"
	"go/token"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// Each consumer group has a client of its own (startConsumerLocked). With all
// nine groups and the producer on one client, every request to a single broker
// queued on one connection, and a Produce blocked for 4.5 s at p50 behind the
// groups' idle Fetches. manager_delivery_probe_test.go measures that against a
// real broker; these tests pin the wiring without one.

// liveGroup is a sarama.ConsumerGroup whose Consume reports the handler it was
// given and then blocks, like a real session, until its context is cancelled or
// the group is closed.
type liveGroup struct {
	closed    chan struct{}
	closeOnce sync.Once
	joined    chan sarama.ConsumerGroupHandler
}

func newLiveGroup() *liveGroup {
	return &liveGroup{closed: make(chan struct{}), joined: make(chan sarama.ConsumerGroupHandler, 1)}
}

func (g *liveGroup) Consume(ctx context.Context, _ []string, h sarama.ConsumerGroupHandler) error {
	select {
	case g.joined <- h:
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.closed:
		return sarama.ErrClosedConsumerGroup
	}
}

func (g *liveGroup) Close() error {
	g.closeOnce.Do(func() { close(g.closed) })
	return nil
}

func (g *liveGroup) isClosed() bool {
	select {
	case <-g.closed:
		return true
	default:
		return false
	}
}

func (g *liveGroup) Errors() <-chan error                 { return nil }
func (g *liveGroup) Pause(partitions map[string][]int32)  {}
func (g *liveGroup) Resume(partitions map[string][]int32) {}
func (g *liveGroup) PauseAll()                            {}
func (g *liveGroup) ResumeAll()                           {}

// groupFactory stands in for NewManager's newConsumerGroup: it records the group
// IDs the manager asks for and hands out a fresh liveGroup for each.
type groupFactory struct {
	mu     sync.Mutex
	ids    []string
	groups []*liveGroup
}

func (f *groupFactory) build(groupID string) (sarama.ConsumerGroup, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	g := newLiveGroup()
	f.ids = append(f.ids, groupID)
	f.groups = append(f.groups, g)
	return g, nil
}

func managerWithFactory(f *groupFactory) *Manager {
	m := newTestManager(&recordingProducer{})
	m.newConsumerGroup = f.build
	return m
}

// joinedHandler waits for the consume loop to call Consume on g and returns the
// handler it passed.
func joinedHandler(t *testing.T, g *liveGroup) sarama.ConsumerGroupHandler {
	t.Helper()
	select {
	case h := <-g.joined:
		return h
	case <-time.After(2 * time.Second):
		t.Fatal("the consume loop never called Consume on the group")
		return nil
	}
}

func noopHandler(context.Context, *sarama.ConsumerMessage) error { return nil }

func TestEachTopicJoinsItsOwnGroup(t *testing.T) {
	f := &groupFactory{}
	m := managerWithFactory(f)
	topics := []string{"healer.schema-changes", "healer.approved-changes"}
	for _, topic := range topics {
		if err := m.ConsumeWithContext(topic, noopHandler); err != nil {
			t.Fatalf("ConsumeWithContext(%s): %v", topic, err)
		}
		defer m.StopConsuming(kafkaclient.Topic(topic))
	}

	if len(f.ids) != len(topics) {
		t.Fatalf("built %d consumer groups for %d topics", len(f.ids), len(topics))
	}
	for i, topic := range topics {
		if want := "orchestrator-" + kafkaclient.Topic(topic); f.ids[i] != want {
			t.Errorf("topic %s joined group %q, want %q", topic, f.ids[i], want)
		}
		joinedHandler(t, f.groups[i])
	}
}

func TestRestartConsumerGroupRejoinsTheTopicsOwnGroup(t *testing.T) {
	f := &groupFactory{}
	m := managerWithFactory(f)
	topic := kafkaclient.Topic("healer.schema-changes")
	var handled atomic.Int32
	handler := func(context.Context, *sarama.ConsumerMessage) error {
		handled.Add(1)
		return nil
	}
	if err := m.ConsumeWithContext(topic, handler); err != nil {
		t.Fatalf("ConsumeWithContext: %v", err)
	}
	old := f.groups[0]
	joinedHandler(t, old)

	if err := m.RestartConsumerGroup(topic); err != nil {
		t.Fatalf("RestartConsumerGroup: %v", err)
	}
	defer m.StopConsuming(topic)

	if !old.isClosed() {
		t.Error("the old group was not closed")
	}
	if len(f.ids) != 2 {
		t.Fatalf("built %d consumer groups in all, want the original and one replacement", len(f.ids))
	}
	if f.ids[1] != f.ids[0] {
		t.Errorf("the restart joined group %q; the topic's group is %q", f.ids[1], f.ids[0])
	}
	m.mu.RLock()
	current := m.consumers[topic]
	m.mu.RUnlock()
	if current != sarama.ConsumerGroup(f.groups[1]) {
		t.Error("the manager does not hold the replacement group")
	}

	// A consume loop is running on the new group, with the topic's own handler.
	h, ok := joinedHandler(t, f.groups[1]).(*ConsumerGroupHandler)
	if !ok {
		t.Fatal("the new group was consumed with an unexpected handler type")
	}
	if err := h.handlerWithCtx(context.Background(), &sarama.ConsumerMessage{}); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if handled.Load() != 1 {
		t.Error("the restarted consumer runs a different handler from the one the topic was consumed with")
	}
}

func TestRestartConsumerGroupWithNoHandlerLeavesTheGroupRunning(t *testing.T) {
	f := &groupFactory{}
	m := managerWithFactory(f)
	running := newLiveGroup()
	m.consumers["rsync.orphan"] = running

	if err := m.RestartConsumerGroup("rsync.orphan"); err == nil {
		t.Fatal("RestartConsumerGroup succeeded with no handler to restart with")
	}
	if running.isClosed() {
		t.Error("the running group was closed by a restart that could not finish")
	}
	if len(f.ids) != 0 {
		t.Errorf("built %d consumer groups for a restart that could not finish", len(f.ids))
	}
}

// NewConsumerGroupFromClient is how the groups came to share one client. The
// manager's own path is covered above through the factory; this catches the
// constructor being brought back anywhere in the service.
func TestNoConsumerGroupIsBuiltOnASharedClient(t *testing.T) {
	root := serviceRoot(t)
	var shared []string
	owned := 0
	walkServiceSource(t, root, func(_ string, fset *token.FileSet, file *ast.File) {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "sarama" {
				return true
			}
			switch sel.Sel.Name {
			case "NewConsumerGroupFromClient":
				shared = append(shared, relPos(root, fset, call.Pos()))
			case "NewConsumerGroup":
				owned++
			}
			return true
		})
	})
	// NewManager's factory calls sarama.NewConsumerGroup, so a matcher that finds
	// none is broken, and the check below would pass without looking.
	if owned == 0 {
		t.Fatal("found no sarama.NewConsumerGroup call anywhere in the service; the matcher is broken")
	}
	for _, pos := range shared {
		t.Errorf("%s: sarama.NewConsumerGroupFromClient builds a group on a client other code "+
			"also uses; give the group its own client with sarama.NewConsumerGroup", pos)
	}
}
