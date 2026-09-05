package handlers

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// A bootstrap list is a list of entry points into ONE cluster. Probing only
// brokers[0] reports "kafka: down" for a fully healthy cluster whenever its
// first entry is the one being restarted — an operator then chases an outage
// that is not happening.
func TestProbeKafkaBrokersUpWhenAnyBrokerAnswers(t *testing.T) {
	var tried []string
	got := probeKafkaBrokers(
		[]string{"b1:9092", "b2:9092", "b3:9092"},
		func(ctx context.Context, addr string) error {
			tried = append(tried, addr)
			if addr == "b2:9092" {
				return nil
			}
			return errors.New("connection refused")
		},
	)

	if got.Status != "up" {
		t.Fatalf("status = %q (error %q), want %q: b2 answered", got.Status, got.Error, "up")
	}
	if len(tried) != 2 || tried[0] != "b1:9092" || tried[1] != "b2:9092" {
		t.Errorf("tried %q, want the list in order stopping at the first success", tried)
	}
}

func TestProbeKafkaBrokersDownOnlyWhenAllFail(t *testing.T) {
	var tried []string
	got := probeKafkaBrokers(
		[]string{"b1:9092", "b2:9092"},
		func(ctx context.Context, addr string) error {
			tried = append(tried, addr)
			return errors.New("connection refused")
		},
	)

	if got.Status != "down" {
		t.Fatalf("status = %q, want %q", got.Status, "down")
	}
	if len(tried) != 2 {
		t.Errorf("tried %q, want every broker attempted", tried)
	}
	// The reported error names which address failed; with several brokers a
	// bare "connection refused" does not say which one.
	if !strings.Contains(got.Error, "b2:9092") {
		t.Errorf("error %q does not name the broker it came from", got.Error)
	}
}

// Each attempt gets its own deadline: one entry that hangs until timeout must
// not consume the whole budget and take the untried brokers down with it.
func TestProbeKafkaBrokersGivesEachBrokerItsOwnDeadline(t *testing.T) {
	got := probeKafkaBrokers(
		[]string{"b1:9092", "b2:9092"},
		func(ctx context.Context, addr string) error {
			if _, ok := ctx.Deadline(); !ok {
				t.Errorf("probe of %s got a context with no deadline", addr)
			}
			if err := ctx.Err(); err != nil {
				t.Errorf("probe of %s got an already-expired context: %v", addr, err)
			}
			if addr == "b1:9092" {
				return errors.New("i/o timeout")
			}
			return nil
		},
	)
	if got.Status != "up" {
		t.Fatalf("status = %q, want %q", got.Status, "up")
	}
}

func TestProbeKafkaBrokersNoBrokers(t *testing.T) {
	got := probeKafkaBrokers(nil, func(context.Context, string) error {
		t.Fatal("probe called with no brokers configured")
		return nil
	})
	if got.Status != "down" || got.Error == "" {
		t.Fatalf("got %+v, want a down status with an explanation", got)
	}
}

// "b1:9092, b2:9092" is two brokers, not one broker plus an address with a
// leading space that never resolves.
func TestKafkaProbeBrokersTrimsAndDropsBlanks(t *testing.T) {
	clearKafkaEnvForTest(t)
	t.Setenv("KAFKA_BROKERS", "b1:9092, b2:9092,,")

	got := kafkaProbeBrokers()
	want := []string{"b1:9092", "b2:9092"}
	if len(got) != len(want) {
		t.Fatalf("brokers = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("brokers = %q, want %q", got, want)
		}
	}
}

func TestKafkaProbeBrokersFallsBackWhenUnset(t *testing.T) {
	clearKafkaEnvForTest(t)

	got := kafkaProbeBrokers()
	if len(got) != 1 || got[0] != "localhost:9092" {
		t.Fatalf("brokers = %q, want the built-in default", got)
	}
}
