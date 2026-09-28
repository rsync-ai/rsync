package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/IBM/sarama"
	"github.com/segmentio/kafka-go"
)

// What sarama logs when a broker answers metadata with an empty broker list --
// KRaft leaves fenced brokers out -- which is the case the matrix's 04-sasl-scram
// row kept failing on as a bare "run out of available brokers".
const emptyBrokers = "client/metadata receiving empty brokers from the metadata response when requesting the broker #1 at kafka:9092"

// What sarama logs for a broker KError it then DROPS: nothing of it survives in
// the returned error, which is bare ErrOutOfBrokers exactly as above.
const droppedKError = "client/metadata got error from broker 1 while fetching metadata: kafka server: The broker does not support the requested SASL mechanism"

func TestTransientRetriesOnlyClusterStates(t *testing.T) {
	bare := sarama.Wrap(sarama.ErrOutOfBrokers)
	tlsFail := errors.New("tls: failed to verify certificate: x509: certificate signed by unknown authority")
	cases := []struct {
		name string
		err  error
		log  string
		want bool
	}{
		// Transient: the cluster has not caught up yet.
		{"kafka-go leader not known (the \":0\" dial)", fmt.Errorf("dial leader: %w", errLeaderUnknown), "", true},
		{"sarama not leader", fmt.Errorf("produce: %w", sarama.ErrNotLeaderForPartition), "", true},
		{"sarama leader not available", fmt.Errorf("produce: %w", sarama.ErrLeaderNotAvailable), "", true},
		{"sarama unknown topic", fmt.Errorf("consume: %w", sarama.ErrUnknownTopicOrPartition), "", true},
		{"sarama out of brokers, cause leader not available", sarama.Wrap(sarama.ErrOutOfBrokers, sarama.ErrLeaderNotAvailable), "", true},
		{"kafka-go not leader", fmt.Errorf("write: %w", kafka.NotLeaderForPartition), "", true},
		{"kafka-go leader not available", fmt.Errorf("lookup partition: %w", kafka.LeaderNotAvailable), "", true},
		{"kafka-go unknown topic", fmt.Errorf("read offset: %w", kafka.UnknownTopicOrPartition), "", true},
		{"kafka-go write errors, not leader", fmt.Errorf("write: %w", kafka.WriteErrors{kafka.NotLeaderForPartition}), "", true},
		{"bare out of brokers after an empty broker list", fmt.Errorf("producer: %w", bare), emptyBrokers, true},
		{"bare out of brokers after leaderless partitions", bare, "client/metadata found some partitions to be leaderless", true},

		// Never transient: a rejection, or something the harness's own timeout retry owns.
		{"sarama SASL auth failed", fmt.Errorf("new client: %w", sarama.ErrSASLAuthenticationFailed), emptyBrokers, false},
		{"sarama topic authorization", sarama.ErrTopicAuthorizationFailed, "", false},
		{"kafka-go SASL auth failed", fmt.Errorf("dial leader: %w", kafka.SASLAuthenticationFailed), "", false},
		{"kafka-go write errors, SASL", kafka.WriteErrors{kafka.SASLAuthenticationFailed}, "", false},
		{"bare out of brokers from a dropped KError", bare, droppedKError, false},
		{"bare out of brokers, nothing logged", bare, "", false},
		{"out of brokers with a TLS cause", sarama.Wrap(sarama.ErrOutOfBrokers, tlsFail), emptyBrokers, false},
		{"raw dial error", errors.New("dial tcp :0: connect: connection refused"), "", false},
		{"deadline (run.py retries timeouts itself)", fmt.Errorf("read: %w", context.DeadlineExceeded), "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := transient(c.err, c.log); got != c.want {
				t.Errorf("transient(%q, log=%q) = %v, want %v", c.err, c.log, got, c.want)
			}
		})
	}
}

// The tap must see what sarama logs, or every bare ErrOutOfBrokers reads as a
// rejection and the fix is silently off.
func TestLogTapSeesSaramaLogger(t *testing.T) {
	old := sarama.Logger
	t.Cleanup(func() { sarama.Logger = old })
	tap := &logTap{}
	sarama.Logger = log.New(tap, "", 0)
	sarama.Logger.Printf("client/metadata receiving empty brokers from the metadata response when requesting the broker #%d at %s", 1, "kafka:9092")
	if !transient(sarama.Wrap(sarama.ErrOutOfBrokers), tap.String()) {
		t.Fatalf("tap captured %q, which transient() does not accept", tap.String())
	}
	if (*logTap)(nil).String() != "" {
		t.Fatal("a nil tap (the kafka-go path) must read as empty")
	}
}

// saramaReady is matched against sarama's log text, which is not API: an
// upgrade that rewords a line would turn the retry off with nothing red. Pin
// each line to the sarama source this module actually builds against.
func TestSaramaStillLogsTheReadyLines(t *testing.T) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/IBM/sarama").Output()
	if err != nil {
		t.Fatalf("go list sarama: %v", err)
	}
	src, err := os.ReadFile(filepath.Join(strings.TrimSpace(string(out)), "client.go"))
	if err != nil {
		t.Fatalf("read sarama client.go: %v", err)
	}
	for _, line := range saramaReady {
		if !strings.Contains(string(src), line) {
			t.Errorf("sarama's client.go no longer logs %q; update saramaReady", line)
		}
	}
}
