package main

// Regression coverage for KI-CDC-1 round 2: the sink must resolve its subscribed
// topics BEFORE joining the consumer group, because kafka-go fixes a generation's
// partition assignment at join time and its partition watcher only reacts to a
// *change* in partition count — a topic that blinks into existence between the
// assignment and the watcher's first read is invisible forever, leaving the member
// consuming nothing while the producer writes into it. See waitForTopicPartitions.
//
// No live broker needed; the live-kafka case is env-gated per aud4_dlq_test.go.

import (
	"net"
	"os"
	"testing"
	"time"
)

func TestTopicResolveTimeoutClamps(t *testing.T) {
	const env = "RSYNC_SINK_TOPIC_WAIT_SECONDS"
	prev, had := os.LookupEnv(env)
	t.Cleanup(func() {
		if had {
			os.Setenv(env, prev)
		} else {
			os.Unsetenv(env)
		}
	})

	cases := []struct {
		name string
		set  string // "" => unset
		want time.Duration
	}{
		{"default when unset", "", 30 * time.Second},
		{"explicit override", "5", 5 * time.Second},
		{"zero disables the wait", "0", 0},
		{"negative clamps to zero", "-10", 0},
		{"above ceiling clamps to 300s", "9000", 300 * time.Second},
		{"garbage falls back to default", "not-a-number", 30 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set == "" {
				os.Unsetenv(env)
			} else {
				os.Setenv(env, tc.set)
			}
			if got := topicResolveTimeout(); got != tc.want {
				t.Fatalf("topicResolveTimeout() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestWaitForTopicPartitionsReturnsImmediatelyWhenThereIsNothingToWaitFor(t *testing.T) {
	// An unroutable broker guarantees every dial fails, so anything other than an
	// immediate return would burn the full timeout.
	for _, tc := range []struct {
		name    string
		topics  []string
		timeout time.Duration
	}{
		{"no topics", nil, 30 * time.Second},
		{"only blank topics", []string{"", "   "}, 30 * time.Second},
		{"wait disabled", []string{"some-topic"}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			waitForTopicPartitions("127.0.0.1:1", tc.topics, tc.timeout)
			if elapsed := time.Since(start); elapsed > 2*time.Second {
				t.Fatalf("returned after %v, expected an immediate return", elapsed)
			}
		})
	}
}

func TestWaitForTopicPartitionsGivesUpAtTheDeadline(t *testing.T) {
	// A closed port: dials fail fast with ECONNREFUSED, so the topic never resolves
	// and the deadline is the only thing that can end the loop. This is the guard
	// against a pre-join wait that could hang the worker forever.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()

	const timeout = time.Second
	start := time.Now()
	waitForTopicPartitions(addr, []string{"topic-that-cannot-exist"}, timeout)
	elapsed := time.Since(start)

	if elapsed < 900*time.Millisecond {
		t.Fatalf("returned after %v, expected it to wait ~%v before giving up", elapsed, timeout)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("returned after %v, expected it to give up at the %v deadline", elapsed, timeout)
	}
}

func TestWaitForTopicPartitionsResolvesAgainstLiveKafka(t *testing.T) {
	broker := os.Getenv("SINK_LIVE_KAFKA_BROKER")
	if broker == "" {
		t.Skip("set SINK_LIVE_KAFKA_BROKER (e.g. localhost:9092) to run against a live kafka")
	}
	// The whole point of the fix: a topic that does not exist yet must be resolvable
	// before we join, well inside the timeout (auto-create makes the first probe
	// create it; without auto-create this test is expected to be run against a
	// broker where the topic is created by hand first).
	topic := "sink-topic-wait-probe-" + time.Now().UTC().Format("20060102150405")
	start := time.Now()
	waitForTopicPartitions(broker, []string{topic}, 20*time.Second)
	if elapsed := time.Since(start); elapsed > 15*time.Second {
		t.Fatalf("topic %q did not resolve in %v — the pre-join wait is not working", topic, elapsed)
	}
}
