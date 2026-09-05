package main

import (
	"fmt"
	"net"
	"sync"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	"github.com/rsync-ai/shared/kafkaclient/kgoauth"
	"github.com/segmentio/kafka-go"
)

// Kafka security + broker plumbing for the sink worker.
//
// Two separate defects motivated this file, and both were reachable from every
// client constructed below:
//
//  1. Collapse. cfg.KafkaBootstrapServers is ONE string. A customer running a
//     three-broker cluster sets it to "b1:9093,b2:9093,b3:9093", and the worker
//     did []string{cfg.KafkaBootstrapServers} / kafka.TCP(cfg.KafkaBootstrapServers),
//     which treats the whole CSV as a single hostname. That host does not
//     resolve, so the sink cannot reach a cluster that is perfectly healthy —
//     and the failure looks like a broker outage, not a config bug.
//
//  2. Plaintext. Every client dialed with kafka-go's defaults: no TLS, no SASL.
//     Against a cluster that requires either, the worker cannot connect at all.
//
// The helpers here fix both at once. They accept the same `string` the old call
// sites passed, so a single address still works unchanged and the existing tests
// keep compiling — but a CSV is now split rather than collapsed, and the dial
// carries whatever TLS/SASL the environment configured.
//
// The zero value of kafkaSecurity is PLAINTEXT with no credentials, which is a
// strict no-op: a test that never calls initKafkaSecurity talks to its local
// broker exactly as before.
// kafkaServiceName is the identity this worker presents to the broker; it
// becomes the default client.id unless KAFKA_CLIENT_ID overrides it.
//
// The sink is the process that actually moves customer rows, so it is the one
// most likely to be throttled on a shared cluster — and until it named itself,
// a customer looking at their broker's quota metrics saw only the anonymous
// default shared by every client on the cluster. Reaches the wire through
// kgoauth.Transport/Dialer, which is a different applier from the sarama one
// the Go services use, so this is not covered by their tests.
const kafkaServiceName = "kafka-sink"

var (
	kafkaSecurity kafkaclient.Config

	transportOnce sync.Once
	sharedTransp  *kafka.Transport
	transportErr  error
)

// initKafkaSecurity reads the SASL/TLS profile from the environment and pins the
// brokers to what the pipeline config asked for. Security comes from env; the
// address comes from the caller — adopting this changes only HOW the worker
// connects, never WHERE.
//
// Invalid security config is fatal. Starting anyway would mean silently dialing
// plaintext at a cluster the operator asked us to authenticate to, and the
// resulting connection error names the broker rather than the misconfiguration.
func initKafkaSecurity(brokers string) error {
	c, err := kafkaclient.FromEnvForService(kafkaServiceName, brokers)
	if err != nil {
		return err
	}
	c = c.WithBrokers(brokers)
	if err := c.Validate(); err != nil {
		return err
	}
	kafkaSecurity = c
	for _, w := range c.Warnings() {
		logf("warn", "kafka config: %s", w)
	}
	logf("info", "kafka client config: %s", c)
	return nil
}

// securityFor returns the shared profile pointed at addr, which may be a single
// broker or a CSV list. Used by the helpers below so a caller that carries its
// own address (the DLQ override, the stall watchdog) still gets the same
// credentials.
func securityFor(addr string) kafkaclient.Config {
	return kafkaSecurity.WithBrokers(addr)
}

// brokerAddr splits addr into individual broker addresses. This is the fix for
// defect 1: kafka.TCP is variadic, so passing a CSV as one argument produced a
// single unresolvable host.
func brokerAddr(addr string) net.Addr {
	return kgoauth.Addr(securityFor(addr))
}

// kafkaDialer returns the secured dialer for addr.
func kafkaDialer(addr string) (*kafka.Dialer, error) {
	return kgoauth.Dialer(securityFor(addr))
}

// kafkaTransport returns the process-wide secured transport. Writers and Clients
// share one so connection pools are shared, matching kafka-go's own guidance.
func kafkaTransport() *kafka.Transport {
	transportOnce.Do(func() {
		sharedTransp, transportErr = kgoauth.Transport(kafkaSecurity)
	})
	if transportErr != nil {
		// Unreachable in practice: initKafkaSecurity already validated and main
		// exits on error. Returning nil keeps kafka-go on its default transport
		// rather than panicking a live sink.
		logf("error", "kafka transport: %v", transportErr)
		return nil
	}
	return sharedTransp
}

// dialBroker replaces kafka.Dial for metadata connections. It tries each broker
// in turn: a CSV that used to become one bogus hostname now fails over across
// the real cluster members, and the dial carries TLS/SASL.
func dialBroker(network, addr string) (*kafka.Conn, error) {
	sec := securityFor(addr)
	if len(sec.Brokers) == 0 {
		return nil, fmt.Errorf("no kafka brokers configured")
	}
	d, err := kafkaDialer(addr)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, b := range sec.Brokers {
		conn, err := d.Dial(network, b)
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, lastErr
}
