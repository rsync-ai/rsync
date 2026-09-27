//go:build kafka_probe

// A measurement, not a guard: how long a message takes to reach its handler,
// and how long Produce blocks, while the manager holds as many idle consumer
// groups as the orchestrator does. CI never runs it. It needs a real broker:
//
//	KAFKA_PROBE_BROKERS=127.0.0.1:9092 go test -tags kafka_probe \
//	    -run TestDeliveryLatencyWithIdleGroups -count=1 -v ./internal/kafka/
//
// It creates its own uniquely named topics and consumer groups and deletes
// exactly those afterwards, so it is safe on a broker other things use.
package kafka

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/rsync-ai/shared/kafkaclient"
)

func TestDeliveryLatencyWithIdleGroups(t *testing.T) {
	brokers := os.Getenv("KAFKA_PROBE_BROKERS")
	if brokers == "" {
		t.Skip("set KAFKA_PROBE_BROKERS to run against a real broker")
	}
	const idleGroups = 9 // the orchestrator's agent command topics
	// Produce for a fixed window rather than a fixed count: on the shared
	// client one Produce took seconds, so a count had no useful bound.
	const produceFor = 30 * time.Second
	const spacing = 50 * time.Millisecond

	run := fmt.Sprintf("probe-%d", time.Now().UnixNano())
	groupBase := "orchestrator-" + run
	var topics []string
	for i := 0; i < idleGroups; i++ {
		topics = append(topics, kafkaclient.Topic(fmt.Sprintf("%s.idle-%d", run, i)))
	}
	hot := kafkaclient.Topic(run + ".hot")
	topics = append(topics, hot)

	m, err := NewManager(Config{Brokers: brokers, GroupID: groupBase})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	admin, err := sarama.NewClusterAdmin([]string{brokers}, sarama.NewConfig())
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	defer admin.Close()
	var groups []string
	for _, topic := range topics {
		groups = append(groups, fmt.Sprintf("%s-%s", groupBase, topic))
	}
	closed := false
	defer func() {
		if !closed { // a group with members cannot be deleted
			_ = m.Close()
		}
		for _, topic := range topics {
			if err := admin.DeleteTopic(topic); err != nil {
				t.Logf("cleanup: delete topic %s: %v", topic, err)
			}
		}
		for _, g := range groups {
			// A group that never committed an offset is already gone once its
			// members leave.
			if err := admin.DeleteConsumerGroup(g); err != nil && !errors.Is(err, sarama.ErrGroupIDNotFound) {
				t.Logf("cleanup: delete group %s: %v", g, err)
			}
		}
	}()

	for _, topic := range topics {
		if err := m.EnsureTopicExists(topic, 3); err != nil {
			t.Fatalf("EnsureTopicExists %s: %v", topic, err)
		}
	}

	var mu sync.Mutex
	var delivered []time.Duration
	for _, topic := range topics[:idleGroups] {
		if err := m.Consume(topic, func(*sarama.ConsumerMessage) error { return nil }); err != nil {
			t.Fatalf("Consume %s: %v", topic, err)
		}
	}
	if err := m.Consume(hot, func(msg *sarama.ConsumerMessage) error {
		sent, err := strconv.ParseInt(string(msg.Value), 10, 64)
		if err != nil {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		delivered = append(delivered, time.Since(time.Unix(0, sent)))
		return nil
	}); err != nil {
		t.Fatalf("Consume %s: %v", hot, err)
	}

	// Measure only once every group holds its partitions; a join in progress
	// would count rebalance time as delivery time.
	deadline := time.Now().Add(90 * time.Second)
	for {
		described, err := admin.DescribeConsumerGroups(groups)
		stable := 0
		if err == nil {
			for _, d := range described {
				if d.State == "Stable" && len(d.Members) > 0 {
					stable++
				}
			}
		}
		if stable == len(groups) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d groups stable after 90s (err=%v)", stable, len(groups), err)
		}
		time.Sleep(time.Second)
	}
	time.Sleep(5 * time.Second)

	var produced []time.Duration
	window := time.Now()
	for i := 0; time.Since(window) < produceFor; i++ {
		start := time.Now()
		if err := m.Produce(hot, []byte(strconv.Itoa(i)), []byte(strconv.FormatInt(start.UnixNano(), 10))); err != nil {
			t.Fatalf("Produce: %v", err)
		}
		produced = append(produced, time.Since(start))
		time.Sleep(spacing)
	}
	for wait := time.Now(); ; time.Sleep(100 * time.Millisecond) {
		mu.Lock()
		got := len(delivered)
		mu.Unlock()
		if got == len(produced) {
			break
		}
		if time.Since(wait) > 30*time.Second {
			t.Fatalf("only %d of %d messages delivered", got, len(produced))
		}
	}

	closeStart := time.Now()
	if err := m.Close(); err != nil {
		t.Logf("Close: %v", err)
	}
	closeTook := time.Since(closeStart)
	closed = true

	mu.Lock()
	defer mu.Unlock()
	t.Logf("groups=%d produced=%d in %s (spacing %s)", len(groups), len(produced), produceFor, spacing)
	t.Logf("delivery (produce call -> handler): %s", percentiles(delivered))
	t.Logf("produce  (SendMessage blocking):    %s", percentiles(produced))
	t.Logf("Manager.Close took %s", closeTook.Round(time.Millisecond))
}

func percentiles(d []time.Duration) string {
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	at := func(p float64) time.Duration { return s[int(p*float64(len(s)-1))].Round(time.Millisecond) }
	return fmt.Sprintf("p50=%s p90=%s p99=%s max=%s", at(0.50), at(0.90), at(0.99), s[len(s)-1].Round(time.Millisecond))
}
