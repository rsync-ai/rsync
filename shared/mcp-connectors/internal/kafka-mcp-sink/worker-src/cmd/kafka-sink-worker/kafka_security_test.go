package main

import (
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/segmentio/kafka-go"
)

// freePort returns a port nothing is listening on, so a dial to it is refused.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}

// livePort returns an address that accepts TCP connections for the test's life.
func livePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = c.Close() })
		}
	}()
	return l.Addr().String()
}

func withSecurity(t *testing.T, brokers string) {
	t.Helper()
	prev := kafkaSecurity
	t.Cleanup(func() { kafkaSecurity = prev })
	if err := initKafkaSecurity(brokers); err != nil {
		t.Fatalf("initKafkaSecurity(%q): %v", brokers, err)
	}
}

// TestDialBrokerFailsOverAcrossACSVList is the two-sided proof that the collapse
// bug is fixed. The list names a dead broker first and a live one second.
//
// The control half matters as much as the fix half: kafka.Dial — exactly what
// this worker called before — is handed the same string and MUST fail, because
// it treats "host1:port,host2:port" as one hostname. If the control ever starts
// passing, the test has stopped discriminating and the fix half proves nothing.
func TestDialBrokerFailsOverAcrossACSVList(t *testing.T) {
	dead := freePort(t)
	live := livePort(t)
	csv := dead + "," + live

	withSecurity(t, csv)

	conn, err := dialBroker("tcp", csv)
	if err != nil {
		t.Fatalf("dialBroker(%q) failed over to no broker: %v", csv, err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Control: the pre-fix call on the same input.
	if c, cerr := kafka.Dial("tcp", csv); cerr == nil {
		_ = c.Close()
		t.Fatalf("control: kafka.Dial(%q) unexpectedly succeeded — the test no longer distinguishes collapsed from split", csv)
	}
}

// TestBrokerAddrKeepsEveryBroker guards the writer/client Addr path, which used
// kafka.TCP(csv) and so addressed one bogus host.
func TestBrokerAddrKeepsEveryBroker(t *testing.T) {
	withSecurity(t, "b1:9093,b2:9093,b3:9093")

	got := brokerAddr("b1:9093,b2:9093,b3:9093")
	if got == nil {
		t.Fatal("brokerAddr returned nil")
	}
	// kafka-go models a multi-broker address as a list and a single broker as a
	// scalar; both stringify the same way, so compare the concrete types instead.
	single := kafka.TCP("b1:9093")
	if fmt.Sprintf("%T", got) == fmt.Sprintf("%T", single) {
		t.Fatalf("brokerAddr collapsed 3 brokers into a single address of type %T", got)
	}
}

// TestInitKafkaSecurityFailsClosed: half a SASL config must stop the worker, not
// silently downgrade it to plaintext against a cluster that requires auth.
func TestInitKafkaSecurityFailsClosed(t *testing.T) {
	prev := kafkaSecurity
	t.Cleanup(func() { kafkaSecurity = prev })

	t.Setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
	t.Setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
	t.Setenv("KAFKA_SASL_USERNAME", "svc-rsync")
	// password deliberately absent

	err := initKafkaSecurity("b1:9093")
	if err == nil {
		t.Fatal("initKafkaSecurity accepted SASL_SSL with no password — the worker would dial unauthenticated")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "password") {
		t.Fatalf("error should name the missing credential, got: %v", err)
	}
}

// TestUnsetSecurityIsAStrictNoOp: an existing plaintext deployment must be
// byte-identical after adopting this. No TLS, no SASL, brokers unchanged.
func TestUnsetSecurityIsAStrictNoOp(t *testing.T) {
	t.Setenv("KAFKA_SECURITY_PROTOCOL", "")
	t.Setenv("KAFKA_SASL_MECHANISM", "")
	t.Setenv("KAFKA_SASL_USERNAME", "")
	t.Setenv("KAFKA_SASL_PASSWORD", "")
	withSecurity(t, "kafka:29092")

	if kafkaSecurity.UsesTLS() {
		t.Error("unset config turned on TLS")
	}
	if kafkaSecurity.UsesSASL() {
		t.Error("unset config turned on SASL")
	}
	if len(kafkaSecurity.Brokers) != 1 || kafkaSecurity.Brokers[0] != "kafka:29092" {
		t.Errorf("brokers changed: %v", kafkaSecurity.Brokers)
	}
	d, err := kafkaDialer("kafka:29092")
	if err != nil {
		t.Fatalf("dialer: %v", err)
	}
	if d.TLS != nil {
		t.Error("plaintext dialer carries a TLS config")
	}
	if d.SASLMechanism != nil {
		t.Error("plaintext dialer carries a SASL mechanism")
	}
}

// TestInitKafkaSecurityRevalidatesTheCallersBrokers pins the second Validate in
// initKafkaSecurity. FromEnv validates what the ENVIRONMENT held; the caller's
// brokers are applied afterwards with WithBrokers, so a bad pipeline-config
// address would otherwise sail past every check and surface later as an
// unresolvable host at dial time.
//
// The environment here is deliberately valid, so FromEnv passes and only the
// post-WithBrokers check can catch this.
func TestInitKafkaSecurityRevalidatesTheCallersBrokers(t *testing.T) {
	prev := kafkaSecurity
	t.Cleanup(func() { kafkaSecurity = prev })

	t.Setenv("KAFKA_BROKERS", "good:9092")
	t.Setenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")

	if err := initKafkaSecurity("bad broker:9092"); err == nil {
		t.Fatal("initKafkaSecurity accepted a broker address containing whitespace from the pipeline config")
	}
}
