package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestStallWatchdogTimeoutClamps(t *testing.T) {
	cases := []struct {
		name string
		env  string
		set  bool
		want time.Duration
	}{
		{name: "unset uses the default", want: defaultStallWatchdogSeconds * time.Second},
		{name: "zero disables the watchdog", env: "0", set: true, want: 0},
		{name: "negative disables the watchdog", env: "-5", set: true, want: 0},
		{name: "below the floor clamps up so restarts are not rapid", env: "10", set: true, want: minStallWatchdogSeconds * time.Second},
		{name: "in range is honoured", env: "120", set: true, want: 120 * time.Second},
		{name: "above the ceiling clamps down", env: "99999", set: true, want: maxStallWatchdogSeconds * time.Second},
		{name: "garbage falls back to the default", env: "soon", set: true, want: defaultStallWatchdogSeconds * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv("RSYNC_SINK_STALL_WATCHDOG_SECONDS", tc.env)
			} else {
				os.Unsetenv("RSYNC_SINK_STALL_WATCHDOG_SECONDS")
			}
			if got := stallWatchdogTimeout(); got != tc.want {
				t.Fatalf("stallWatchdogTimeout()=%v, want %v", got, tc.want)
			}
		})
	}
}

// The restart decision is the safety-critical part: a false positive kills a healthy
// worker. Each case below flips exactly one of the three facts away from the wedge.
func TestShouldRestartForStall(t *testing.T) {
	stall := 90 * time.Second
	now := time.Unix(1_760_000_000, 0)
	wedged := stallSnapshot{
		now:         now,
		lastPoll:    now.Add(-1 * time.Second),  // loop alive
		lastMessage: now.Add(-10 * time.Minute), // fetched nothing
		dataWaiting: true,                       // records available
	}

	cases := []struct {
		name  string
		snap  stallSnapshot
		stall time.Duration
		want  bool
	}{
		{name: "wedged: polling, no messages, data waiting", snap: wedged, stall: stall, want: true},
		{
			name:  "healthy: still fetching messages",
			snap:  func() stallSnapshot { s := wedged; s.lastMessage = now.Add(-5 * time.Second); return s }(),
			stall: stall, want: false,
		},
		{
			name:  "idle stream: quiet, but nothing waiting on the broker",
			snap:  func() stallSnapshot { s := wedged; s.dataWaiting = false; return s }(),
			stall: stall, want: false,
		},
		{
			name:  "backpressure: loop blocked in a slow destination write, not starved",
			snap:  func() stallSnapshot { s := wedged; s.lastPoll = now.Add(-5 * time.Minute); return s }(),
			stall: stall, want: false,
		},
		{
			name:  "exactly at the stall boundary does not fire",
			snap:  func() stallSnapshot { s := wedged; s.lastMessage = now.Add(-stall + time.Millisecond); return s }(),
			stall: stall, want: false,
		},
		{name: "disabled watchdog never fires", snap: wedged, stall: 0, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldRestartForStall(tc.snap, tc.stall); got != tc.want {
				t.Fatalf("shouldRestartForStall()=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestConsumerActivitySeedsBothClocksSoTheFirstWindowIsAGracePeriod(t *testing.T) {
	start := time.Now()
	act := newConsumerActivity(start)
	if got := act.lastMessage(); got.Sub(start).Abs() > time.Millisecond {
		t.Fatalf("lastMessage seeded at %v, want %v", got, start)
	}
	if got := act.lastPoll(); got.Sub(start).Abs() > time.Millisecond {
		t.Fatalf("lastPoll seeded at %v, want %v", got, start)
	}

	// A worker that just started must not look wedged.
	if shouldRestartForStall(stallSnapshot{now: start, lastPoll: act.lastPoll(), lastMessage: act.lastMessage(), dataWaiting: true}, 90*time.Second) {
		t.Fatal("a freshly started worker must not be diagnosed as stalled")
	}

	act.pollTick()
	act.messageTick()
	if !act.lastPoll().After(start.Add(-time.Millisecond)) || !act.lastMessage().After(start.Add(-time.Millisecond)) {
		t.Fatal("ticks did not advance the clocks")
	}
}

// unconsumedRecords is the half of the decision that talks to Kafka, so it needs a
// real broker. Two-sided on purpose: a probe that answers the same way for a topic
// with waiting records and one without proves nothing.
//
//	SINK_LIVE_KAFKA_BROKER=localhost:9092 go test -run LiveKafka ./...
func TestUnconsumedRecordsAgainstLiveKafka(t *testing.T) {
	broker := os.Getenv("SINK_LIVE_KAFKA_BROKER")
	if broker == "" {
		t.Skip("set SINK_LIVE_KAFKA_BROKER to run this against a live broker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stamp := time.Now().UnixNano()
	empty := fmt.Sprintf("stallwatch-empty-%d", stamp)
	full := fmt.Sprintf("stallwatch-full-%d", stamp)

	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("dial %s: %v", broker, err)
	}
	defer conn.Close()
	for _, topic := range []string{empty, full} {
		if err := conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
			t.Fatalf("create topic %s: %v", topic, err)
		}
	}

	w := &kafka.Writer{Addr: kafka.TCP(broker), Topic: full, RequiredAcks: kafka.RequireAll, AllowAutoTopicCreation: true}
	defer w.Close()
	var writeErr error
	for attempt := 0; attempt < 20; attempt++ {
		writeErr = w.WriteMessages(ctx, kafka.Message{Value: []byte("row")})
		if writeErr == nil {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if writeErr != nil {
		t.Fatalf("produce to %s: %v", full, writeErr)
	}

	group := fmt.Sprintf("stallwatch-group-%d", stamp) // never joined, so never committed

	waiting, detail, err := unconsumedRecords(ctx, broker, group, []string{full}, true, map[string]int64{})
	if err != nil {
		t.Fatalf("unconsumedRecords(full): %v", err)
	}
	if !waiting {
		t.Fatalf("topic with a produced record and an uncommitted group must report records waiting")
	}
	t.Logf("waiting detail: %s", detail)

	waiting, _, err = unconsumedRecords(ctx, broker, group, []string{empty}, true, map[string]int64{})
	if err != nil {
		t.Fatalf("unconsumedRecords(empty): %v", err)
	}
	if waiting {
		t.Fatal("an empty topic must not report records waiting")
	}
}

// The wedge is probabilistic (~1 join in 5), so this test cannot assert that one
// happens. It asserts the property that must hold either way: for every trial, the
// shipped decision agrees with what the reader actually did. A wedged reader must be
// diagnosed as stalled; a reader that consumed and committed must not be. Run it
// enough times and both branches get exercised — the trial log says which.
//
//	SINK_LIVE_KAFKA_BROKER=localhost:9092 go test -count=1 -v -run LiveReaderOutcome ./...
func TestStallDecisionMatchesLiveReaderOutcome(t *testing.T) {
	broker := os.Getenv("SINK_LIVE_KAFKA_BROKER")
	if broker == "" {
		t.Skip("set SINK_LIVE_KAFKA_BROKER to run this against a live broker")
	}

	const (
		trials = 6
		// Comfortably past PartitionWatchInterval (5s), so a reader that kafka-go's
		// partition watcher is going to rescue has already been rescued by the time we
		// judge it. Production's floor is 30s, further past it still.
		stall    = 15 * time.Second
		fetchFor = 20 * time.Second
	)
	stamp := time.Now().UnixNano()

	admin, err := kafka.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("dial %s: %v", broker, err)
	}
	defer admin.Close()

	wedged, healthy := 0, 0
	for i := 0; i < trials; i++ {
		topic := fmt.Sprintf("stallwatch-outcome-%d-%d", stamp, i)
		group := fmt.Sprintf("stallwatch-outcome-grp-%d-%d", stamp, i)

		// Alternate the two shapes so both branches of the decision get exercised:
		// even trials join a topic that already exists (the healthy control), odd
		// trials join before the topic exists — Debezium's per-table topics are lazy,
		// which is exactly how the sink meets an empty assignment in the wild.
		preCreate := i%2 == 0
		if preCreate {
			if err := admin.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
				t.Fatalf("trial %d: create topic: %v", i, err)
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)

		// Same knobs the worker uses, so the join behaves the same way.
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:                []string{broker},
			GroupID:                group,
			GroupTopics:            []string{topic},
			MinBytes:               1,
			MaxBytes:               16 * 1024 * 1024,
			QueueCapacity:          2000,
			MaxWait:                500 * time.Millisecond,
			WatchPartitionChanges:  true,
			PartitionWatchInterval: 5 * time.Second,
			CommitInterval:         0,
			StartOffset:            kafka.FirstOffset,
		})

		if !preCreate {
			time.Sleep(2 * time.Second) // let the member join while the topic is still absent
		}

		w := &kafka.Writer{Addr: kafka.TCP(broker), Topic: topic, RequiredAcks: kafka.RequireAll, AllowAutoTopicCreation: true}
		var writeErr error
		for attempt := 0; attempt < 20; attempt++ {
			if writeErr = w.WriteMessages(ctx, kafka.Message{Value: []byte("row")}); writeErr == nil {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		w.Close()
		if writeErr != nil {
			reader.Close()
			cancel()
			t.Fatalf("trial %d: produce never succeeded: %v", i, writeErr)
		}

		act := newConsumerActivity(time.Now())
		consumed := false
		for deadline := time.Now().Add(fetchFor); time.Now().Before(deadline); {
			act.pollTick()
			fetchCtx, cancelFetch := context.WithTimeout(ctx, 1*time.Second)
			msg, ferr := reader.FetchMessage(fetchCtx)
			cancelFetch()
			if ferr != nil {
				continue
			}
			act.messageTick()
			// The worker commits only after the destination ack; mirror that here so
			// the broker-side view matches a healthy sink that has done its work.
			if cerr := reader.CommitMessages(ctx, msg); cerr != nil {
				t.Fatalf("trial %d: commit: %v", i, cerr)
			}
			consumed = true
			break
		}

		waiting, detail, uerr := unconsumedRecords(ctx, broker, group, []string{topic}, true, map[string]int64{})
		if uerr != nil {
			reader.Close()
			cancel()
			t.Fatalf("trial %d: unconsumedRecords: %v", i, uerr)
		}
		restart := shouldRestartForStall(stallSnapshot{
			now:         time.Now(),
			lastPoll:    act.lastPoll(),
			lastMessage: act.lastMessage(),
			dataWaiting: waiting,
		}, stall)

		reader.Close()
		cancel()

		if consumed {
			healthy++
			if waiting {
				t.Errorf("trial %d: reader consumed and committed, but the broker still reports records waiting (%s)", i, detail)
			}
			if restart {
				t.Errorf("trial %d: healthy reader diagnosed as stalled — this would kill a working sink", i)
			}
			t.Logf("trial %d: HEALTHY  pre_created=%v waiting=%v restart=%v", i, preCreate, waiting, restart)
			continue
		}

		wedged++
		if !waiting {
			t.Errorf("trial %d: reader consumed nothing yet the broker reports no records waiting", i)
		}
		if !restart {
			t.Errorf("trial %d: WEDGED reader NOT diagnosed as stalled — the watchdog would miss it", i)
		}
		t.Logf("trial %d: WEDGED   pre_created=%v waiting=%v restart=%v detail=%s", i, preCreate, waiting, restart, detail)
	}

	t.Logf("trials=%d healthy=%d wedged=%d", trials, healthy, wedged)
}

// The wedge itself is a race, so the test above cannot be relied on to produce one.
// This test manufactures the *state* the watchdog has to catch — a live group member
// holding an empty partition assignment while records wait — deterministically, by
// putting three members on a one-partition topic and then watching only the member
// that may well have been assigned nothing. It is still self-labeling: whichever
// branch a trial lands in, the shipped decision must agree with reality.
//
//	SINK_LIVE_KAFKA_BROKER=localhost:9092 go test -count=1 -v -run EmptyAssignment ./...
func TestStallDecisionDetectsLiveEmptyAssignmentMember(t *testing.T) {
	broker := os.Getenv("SINK_LIVE_KAFKA_BROKER")
	if broker == "" {
		t.Skip("set SINK_LIVE_KAFKA_BROKER to run this against a live broker")
	}

	const (
		// kafka-go's range assignor hands partition 0 to the lowest-sorted member, so
		// the last-created member is reliably the empty-assigned one; a handful of
		// trials is enough and keeps the runtime sane. The healthy branch is covered by
		// TestStallDecisionMatchesLiveReaderOutcome.
		trials  = 3
		members = 3
		// Past PartitionWatchInterval and past a rebalance, so "consumed nothing" is a
		// settled fact rather than a slow start.
		stall    = 15 * time.Second
		fetchFor = 20 * time.Second
	)
	stamp := time.Now().UnixNano()

	admin, err := kafka.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("dial %s: %v", broker, err)
	}
	defer admin.Close()

	empties, assigned := 0, 0
	for i := 0; i < trials; i++ {
		topic := fmt.Sprintf("stallwatch-empty-assign-%d-%d", stamp, i)
		group := fmt.Sprintf("stallwatch-empty-assign-grp-%d-%d", stamp, i)
		if err := admin.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
			t.Fatalf("trial %d: create topic: %v", i, err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)

		newMember := func() *kafka.Reader {
			return kafka.NewReader(kafka.ReaderConfig{
				Brokers:                []string{broker},
				GroupID:                group,
				GroupTopics:            []string{topic},
				MinBytes:               1,
				MaxBytes:               16 * 1024 * 1024,
				QueueCapacity:          2000,
				MaxWait:                500 * time.Millisecond,
				WatchPartitionChanges:  true,
				PartitionWatchInterval: 5 * time.Second,
				CommitInterval:         0,
				StartOffset:            kafka.FirstOffset,
			})
		}

		readers := make([]*kafka.Reader, 0, members)
		for m := 0; m < members; m++ {
			r := newMember()
			readers = append(readers, r)
			// Force the join while the topic is still empty. The fetch is expected to
			// time out; joining is the point, not reading.
			joinCtx, cancelJoin := context.WithTimeout(ctx, 3*time.Second)
			_, _ = r.FetchMessage(joinCtx)
			cancelJoin()
		}
		// Only the last member is watched. The others hold membership and never commit,
		// so if one of them owns the partition the record stays unconsumed — which is
		// precisely the wedge signature: alive, polling, nothing arriving, data waiting.
		watched := readers[len(readers)-1]

		w := &kafka.Writer{Addr: kafka.TCP(broker), Topic: topic, RequiredAcks: kafka.RequireAll, AllowAutoTopicCreation: true}
		var writeErr error
		for attempt := 0; attempt < 20; attempt++ {
			if writeErr = w.WriteMessages(ctx, kafka.Message{Value: []byte("row")}); writeErr == nil {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		w.Close()
		if writeErr != nil {
			for _, r := range readers {
				r.Close()
			}
			cancel()
			t.Fatalf("trial %d: produce never succeeded: %v", i, writeErr)
		}

		act := newConsumerActivity(time.Now())
		consumed := false
		for deadline := time.Now().Add(fetchFor); time.Now().Before(deadline); {
			act.pollTick()
			fetchCtx, cancelFetch := context.WithTimeout(ctx, 1*time.Second)
			msg, ferr := watched.FetchMessage(fetchCtx)
			cancelFetch()
			if ferr != nil {
				continue
			}
			act.messageTick()
			if cerr := watched.CommitMessages(ctx, msg); cerr != nil {
				t.Fatalf("trial %d: commit: %v", i, cerr)
			}
			consumed = true
			break
		}

		waiting, detail, uerr := unconsumedRecords(ctx, broker, group, []string{topic}, true, map[string]int64{})
		restart := shouldRestartForStall(stallSnapshot{
			now:         time.Now(),
			lastPoll:    act.lastPoll(),
			lastMessage: act.lastMessage(),
			dataWaiting: waiting,
		}, stall)

		for _, r := range readers {
			r.Close()
		}
		cancel()

		if uerr != nil {
			t.Fatalf("trial %d: unconsumedRecords: %v", i, uerr)
		}

		if consumed {
			assigned++
			if restart {
				t.Errorf("trial %d: the watched member consumed and committed, yet was diagnosed as stalled", i)
			}
			t.Logf("trial %d: ASSIGNED       waiting=%v restart=%v", i, waiting, restart)
			continue
		}

		empties++
		if !waiting {
			t.Errorf("trial %d: nothing was consumed, yet the broker reports no records waiting", i)
		}
		if !restart {
			t.Errorf("trial %d: EMPTY-ASSIGNMENT member NOT diagnosed as stalled — the watchdog would miss the wedge", i)
		}
		t.Logf("trial %d: EMPTY-ASSIGN   waiting=%v restart=%v detail=%s", i, waiting, restart, detail)
	}

	t.Logf("trials=%d assigned=%d empty-assignment=%d", trials, assigned, empties)
	if empties == 0 {
		t.Skip("no trial landed on an empty assignment; the detection branch was not exercised")
	}
}
