package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// Bug class: "records this process already holds are counted as waiting on the
// broker". The stall watchdog restarts a worker whose loop is polling, has fetched
// nothing for a stall window, and whose group has uncommitted records. A flush lane
// holding a batch through a destination outage (holdForInfraFault, up to 300 s)
// satisfies all three while the consume loop keeps polling: the held rows are
// uncommitted, so the watchdog killed a healthy worker ~70 s into the outage
// (nightly test_chaos_dest_down_dlq_healthy: "end=3 floor=1 committed=1"), and on
// prod each kill is a respawn toward the supervisor's crash-loop breaker. The same
// class covers any buffer that outlives a stall window (the object batcher's flush
// interval, until now guarded only by widening the window).

func TestWaitingFloorCountsFetchedRecordsAsInHand(t *testing.T) {
	cases := []struct {
		name         string
		pos          partitionPosition
		fromEarliest bool
		waiting      bool
	}{
		{name: "lane holds the fetched rows through an outage (the nightly shape)",
			pos: partitionPosition{first: 0, end: 3, committed: 1, fetchedNext: 3}, fromEarliest: true, waiting: false},
		{name: "nothing fetched past the commit: waiting",
			pos: partitionPosition{first: 0, end: 3, committed: 1}, fromEarliest: true, waiting: true},
		{name: "new records past what was fetched: waiting",
			pos: partitionPosition{first: 0, end: 5, committed: 1, fetchedNext: 3}, fromEarliest: true, waiting: true},
		{name: "never committed, earliest reader, everything fetched",
			pos: partitionPosition{first: 0, end: 4, committed: -1, fetchedNext: 4}, fromEarliest: true, waiting: false},
		{name: "never committed, earliest reader, nothing fetched: the empty-assignment wedge",
			pos: partitionPosition{first: 0, end: 4, committed: -1}, fromEarliest: true, waiting: true},
		{name: "latest reader, only pre-existing data",
			pos: partitionPosition{first: 0, end: 3, committed: -1, baseline: 3}, fromEarliest: false, waiting: false},
		{name: "latest reader, new data it never fetched: waiting",
			pos: partitionPosition{first: 0, end: 4, committed: -1, baseline: 3}, fromEarliest: false, waiting: true},
		{name: "latest reader, new data fetched and held",
			pos: partitionPosition{first: 0, end: 4, committed: -1, baseline: 3, fetchedNext: 4}, fromEarliest: false, waiting: false},
		{name: "a fetch mark below the commit never lowers the floor",
			pos: partitionPosition{first: 0, end: 5, committed: 5, fetchedNext: 2}, fromEarliest: true, waiting: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			floor := waitingFloor(tc.pos, tc.fromEarliest)
			if got := tc.pos.end > floor; got != tc.waiting {
				t.Fatalf("waiting = %v (end=%d floor=%d), want %v", got, tc.pos.end, floor, tc.waiting)
			}
		})
	}
}

// The whole decision, as watchConsumerStall makes it, for a loop that keeps polling
// while a lane holds offsets 1-2: no restart. The control differs in one fact — the
// fetch was on another partition — and must restart, so this is not a probe that
// answers "no" to everything.
func TestStallDecision_LaneHoldingFetchedRowsIsNotAWedge(t *testing.T) {
	const topic = "cdc.inventory.orders"
	stall := 60 * time.Second
	now := time.Now()

	decide := func(fetchedPartition int) (restart bool, floor int64) {
		act := newConsumerActivity(now.Add(-2 * time.Minute))
		act.messageTick(kafka.Message{Topic: topic, Partition: fetchedPartition, Offset: 1})
		act.messageTick(kafka.Message{Topic: topic, Partition: fetchedPartition, Offset: 2})
		pos := partitionPosition{first: 0, end: 3, committed: 1, fetchedNext: act.fetchedNext(topic, 0)}
		floor = waitingFloor(pos, true)
		return shouldRestartForStall(stallSnapshot{
			now:         now,
			lastPoll:    now,                        // the lane is holding, the loop still polls
			lastMessage: now.Add(-90 * time.Second), // nothing new for longer than the window
			dataWaiting: pos.end > floor,
		}, stall), floor
	}

	if restart, floor := decide(0); restart {
		t.Fatalf("a worker holding its fetched rows in a flush lane was diagnosed as stalled (floor=%d) — this kills a healthy sink mid-outage", floor)
	}
	if restart, floor := decide(1); !restart {
		t.Fatalf("control: partition 0 had records nobody fetched (floor=%d), yet no restart — the probe cannot fail", floor)
	}
}

func TestConsumerActivityFetchedNext(t *testing.T) {
	act := newConsumerActivity(time.Now())
	if got := act.fetchedNext("a", 0); got != 0 {
		t.Fatalf("untouched partition: fetchedNext = %d, want 0", got)
	}
	act.messageTick(kafka.Message{Topic: "a", Partition: 0, Offset: 9})
	act.messageTick(kafka.Message{Topic: "a", Partition: 0, Offset: 4}) // a rebalance redelivers from the commit
	if got := act.fetchedNext("a", 0); got != 10 {
		t.Errorf("after a rewind: fetchedNext = %d, want 10 — the mark only moves forward", got)
	}
	act.messageTick(kafka.Message{Topic: "a", Partition: 1, Offset: 2})
	act.messageTick(kafka.Message{Topic: "b", Partition: 0, Offset: 5})
	for _, c := range []struct {
		topic     string
		partition int
		want      int64
	}{{"a", 0, 10}, {"a", 1, 3}, {"b", 0, 6}, {"b", 1, 0}} {
		if got := act.fetchedNext(c.topic, c.partition); got != c.want {
			t.Errorf("fetchedNext(%s,%d) = %d, want %d — marks are per topic and partition", c.topic, c.partition, got, c.want)
		}
	}
	var none *consumerActivity
	if got := none.fetchedNext("a", 0); got != 0 {
		t.Errorf("nil activity: fetchedNext = %d, want 0", got)
	}
}

// Live, two-sided: the group committed offset 0 of three records, and the reader
// fetched 1-2 without committing them (what a lane holding a batch looks like to the
// broker). With this process's fetch marks, nothing is waiting; without them — the
// old watchdog — the same broker state reads as waiting records.
//
//	SINK_LIVE_KAFKA_BROKER=localhost:9092 go test -count=1 -v -run LiveHeldRecords ./...
func TestUnconsumedRecords_LiveHeldRecordsAreNotWaiting(t *testing.T) {
	broker := liveKafkaBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	stamp := time.Now().UnixNano()
	topic := fmt.Sprintf("stallwatch-held-%d", stamp)
	group := fmt.Sprintf("stallwatch-held-group-%d", stamp)
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("dial %s: %v", broker, err)
	}
	defer conn.Close()
	if err := conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	w := &kafka.Writer{Addr: kafka.TCP(broker), Topic: topic, RequiredAcks: kafka.RequireAll}
	var werr error
	for attempt := 0; attempt < 20; attempt++ {
		if werr = w.WriteMessages(ctx, kafka.Message{Value: []byte("r0")}, kafka.Message{Value: []byte("r1")}, kafka.Message{Value: []byte("r2")}); werr == nil {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	w.Close()
	if werr != nil {
		t.Fatalf("produce: %v", werr)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, GroupID: group, Topic: topic,
		StartOffset: kafka.FirstOffset, MaxWait: 500 * time.Millisecond})
	defer reader.Close()
	act := newConsumerActivity(time.Now())
	for i := 0; i < 3; i++ {
		msg, ferr := reader.FetchMessage(ctx)
		if ferr != nil {
			t.Fatalf("fetch %d: %v", i, ferr)
		}
		act.messageTick(msg)
		if i == 0 {
			if cerr := reader.CommitMessages(ctx, msg); cerr != nil {
				t.Fatalf("commit: %v", cerr)
			}
		}
	}

	waiting, detail, err := unconsumedRecords(ctx, broker, group, []string{topic}, true, map[string]int64{}, act)
	if err != nil {
		t.Fatalf("unconsumedRecords: %v", err)
	}
	if waiting {
		t.Errorf("records the reader fetched and holds read as waiting (%s) — the watchdog would kill it", detail)
	}
	waiting, detail, err = unconsumedRecords(ctx, broker, group, []string{topic}, true, map[string]int64{}, nil)
	if err != nil {
		t.Fatalf("unconsumedRecords(control): %v", err)
	}
	if !waiting {
		t.Fatal("control: without fetch marks the uncommitted records must read as waiting — the probe cannot see the broker")
	}
	t.Logf("control detail (no fetch marks): %s", detail)
}
