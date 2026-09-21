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
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
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
	if mode == "sarama" {
		out(mode, "", viaSarama(c, topic, id))
	}
	out(mode, "", viaKafkaGo(c, topic, id))
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

	conn, err := dialer.DialLeader(ctx, "tcp", c.Brokers[0], topic, 0)
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
