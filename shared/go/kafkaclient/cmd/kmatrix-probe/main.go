// Command kmatrix-probe is the Go runtime of the Kafka security matrix
// (deploy/helm/rsync-ai/test/kind/kafka-matrix). It is a test tool: nothing
// ships it and no service imports it.
//
// Config comes ONLY from kafkaclient.FromEnvForService -- the call every rsync
// Go service makes -- and the clients are built only through saramaauth and
// kgoauth. Nothing security-related is set by hand here, so a pass or a failure
// is a statement about rsync's code, not about this probe.
//
//	kmatrix-probe sarama   -> saramaauth.NewClient, SyncProducer, partition consumer
//	kmatrix-probe kafkago  -> kgoauth.Dialer (DialLeader + Reader) and kgoauth.Transport (Writer)
//
// It writes PROBE_ID to PROBE_TOPIC (default "kmatrix") and reads it back.
// Output: exactly one line starting with RESULT; exit 0 on PASS, 1 on FAIL.
//
// A round trip that fails on a transient cluster state -- see transient() -- is
// re-run until retryBudget has passed. Nothing else is: an auth or TLS rejection
// ends the probe on its first attempt, so retrying cannot turn one into a PASS.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/rsync-ai/shared/kafkaclient"
	"github.com/rsync-ai/shared/kafkaclient/kgoauth"
	"github.com/rsync-ai/shared/kafkaclient/saramaauth"
	"github.com/segmentio/kafka-go"
)

func main() {
	if len(os.Args) != 2 || (os.Args[1] != "sarama" && os.Args[1] != "kafkago") {
		fmt.Fprintln(os.Stderr, "usage: kmatrix-probe sarama|kafkago")
		os.Exit(2)
	}
	mode, id := os.Args[1], os.Getenv("PROBE_ID")
	topic := os.Getenv("PROBE_TOPIC")
	if topic == "" {
		topic = "kmatrix"
	}
	c, err := kafkaclient.FromEnvForService("kmatrix-probe", "")
	if err == nil {
		err = c.Validate()
	}
	if err != nil {
		out(mode, "CONFIG_REJECTED", err)
	}
	start, backoff := time.Now(), time.Second
	for attempt := 1; ; attempt++ {
		var tap *logTap
		if mode == "sarama" {
			tap = &logTap{}
			sarama.Logger = log.New(tap, "", 0)
			err = viaSarama(c, topic, id)
		} else {
			err = viaKafkaGo(c, topic, id)
		}
		if err == nil || !transient(err, tap.String()) || time.Since(start)+backoff > retryBudget {
			out(mode, "", err)
		}
		fmt.Fprintf(os.Stderr, "attempt %d: transient %v; retrying in %s\n", attempt, err, backoff)
		time.Sleep(backoff)
		backoff = min(backoff*2, 5*time.Second)
	}
}

// retryBudget bounds when a NEW attempt may start. The last one can then run
// ~30 s more (sarama: 10 s metadata + 15 s consume; kafka-go: its 20 s ctx),
// which keeps the whole probe under run.py's CELL_TIMEOUT of 150 s.
const retryBudget = 75 * time.Second

// errLeaderUnknown is what viaKafkaGo returns instead of dialing ":0". kafka-go
// maps a partition's leader through the metadata broker list and ignores the
// partition's error code (conn.go readTopicMetadatav1), so a leader that is
// fenced -- KRaft leaves fenced brokers out of that list -- or not elected yet
// comes back as a zero Broker, and DialLeader then dials ":0".
var errLeaderUnknown = errors.New("partition leader not known yet")

// saramaReady are sarama log lines emitted only after a broker ANSWERED a
// metadata request (client.go tryRefreshMetadata), so the connection had
// already authenticated. Seeing one proves a bare ErrOutOfBrokers came from the
// cluster not being ready, not from a rejection.
var saramaReady = []string{
	"receiving empty brokers from the metadata response",
	"found some partitions to be leaderless",
}

// transient reports whether err is a cluster state that a retry can outlive:
// a partition with no leader yet, a stale leader, a topic not yet propagated,
// or a metadata response listing no brokers. saramaLog is what sarama logged
// during the attempt ("" for kafka-go).
//
// ErrOutOfBrokers needs the log because the error cannot say which case it is.
// sarama drops non-SASL broker KErrors without recording them -- an
// unsupported SASL mechanism among them (the kerror branch of
// tryRefreshMetadata) -- so a bare ErrOutOfBrokers is also what some
// REJECTIONS look like. One with a cause attached (a TLS or dial error) is
// never transient.
func transient(err error, saramaLog string) bool {
	if errors.Is(err, errLeaderUnknown) ||
		errors.Is(err, sarama.ErrLeaderNotAvailable) ||
		errors.Is(err, sarama.ErrNotLeaderForPartition) ||
		errors.Is(err, sarama.ErrUnknownTopicOrPartition) {
		return true
	}
	var ke kafka.Error
	if errors.As(err, &ke) {
		return ke == kafka.LeaderNotAvailable || ke == kafka.NotLeaderForPartition ||
			ke == kafka.UnknownTopicOrPartition
	}
	// kafka.WriteErrors is a []error with no Unwrap, so errors.As stops at it.
	var we kafka.WriteErrors
	if errors.As(err, &we) {
		for _, e := range we {
			if e != nil && transient(e, saramaLog) {
				return true
			}
		}
		return false
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		if e.Error() == sarama.ErrOutOfBrokers.Error() {
			for _, line := range saramaReady {
				if strings.Contains(saramaLog, line) {
					return true
				}
			}
			return false
		}
	}
	return false
}

// logTap collects sarama's log for transient(). sarama logs from its own
// goroutines, hence the lock.
type logTap struct {
	mu sync.Mutex
	b  strings.Builder
}

func (t *logTap) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.b.Write(p)
}

func (t *logTap) String() string {
	if t == nil {
		return ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.b.String()
}

func out(mode, stage string, err error) {
	if err == nil {
		fmt.Printf("RESULT PASS %s round-trip ok\n", mode)
		os.Exit(0)
	}
	msg := strings.ReplaceAll(err.Error(), "\n", " ")
	if stage != "" {
		msg = stage + ": " + msg
	}
	fmt.Printf("RESULT FAIL %s %s\n", mode, msg)
	os.Exit(1)
}

func viaSarama(c kafkaclient.Config, topic, id string) error {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_3_0_0
	cfg.Producer.Return.Successes = true
	cfg.Net.DialTimeout = 5 * time.Second
	cfg.Metadata.Retry.Max = 0
	cfg.Metadata.Timeout = 10 * time.Second
	client, err := saramaauth.NewClient(c, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	p, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return fmt.Errorf("producer: %w", err)
	}
	part, off, err := p.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(id)})
	if err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	cons, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return fmt.Errorf("consumer: %w", err)
	}
	pc, err := cons.ConsumePartition(topic, part, off)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	defer pc.Close()
	select {
	case m := <-pc.Messages():
		if string(m.Value) != id {
			return fmt.Errorf("read back %q, wrote %q", m.Value, id)
		}
		return nil
	case e := <-pc.Errors():
		return fmt.Errorf("consume: %w", e)
	case <-time.After(15 * time.Second):
		return fmt.Errorf("consume: timed out waiting for own message")
	}
}

func viaKafkaGo(c kafkaclient.Config, topic, id string) error {
	dialer, err := kgoauth.Dialer(c)
	if err != nil {
		return fmt.Errorf("dialer: %w", err)
	}
	transport, err := kgoauth.Transport(c)
	if err != nil {
		return fmt.Errorf("transport: %w", err)
	}
	dialer.Timeout = 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	p, err := dialer.LookupPartition(ctx, "tcp", c.Brokers[0], topic, 0)
	if err != nil {
		return fmt.Errorf("lookup partition: %w", err)
	}
	if p.Leader.Host == "" {
		return fmt.Errorf("dial leader: %w", errLeaderUnknown)
	}
	conn, err := dialer.DialPartition(ctx, "tcp", c.Brokers[0], p)
	if err != nil {
		return fmt.Errorf("dial leader: %w", err)
	}
	start, err := conn.ReadLastOffset()
	conn.Close()
	if err != nil {
		return fmt.Errorf("read offset: %w", err)
	}

	w := &kafka.Writer{Addr: kgoauth.Addr(c), Topic: topic, Transport: transport,
		RequiredAcks: kafka.RequireAll, MaxAttempts: 1}
	if err := w.WriteMessages(ctx, kafka.Message{Value: []byte(id)}); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	w.Close()

	r := kafka.NewReader(kafka.ReaderConfig{Brokers: c.Brokers, Topic: topic, Partition: 0,
		Dialer: dialer, MaxBytes: 1 << 20, MaxWait: time.Second})
	defer r.Close()
	if err := r.SetOffset(start); err != nil {
		return fmt.Errorf("seek: %w", err)
	}
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if string(m.Value) == id {
			return nil
		}
	}
}
