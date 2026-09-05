package kgoauth

import (
	"strings"
	"testing"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

func TestPlaintextYieldsAPlainDialer(t *testing.T) {
	c := kafkaclient.Config{Brokers: []string{"kafka:29092"}, SecurityProtocol: kafkaclient.ProtocolPlaintext}
	d, err := Dialer(c)
	if err != nil {
		t.Fatal(err)
	}
	if d.TLS != nil {
		t.Error("PLAINTEXT dialer must not carry a TLS config")
	}
	if d.SASLMechanism != nil {
		t.Error("PLAINTEXT dialer must not carry a SASL mechanism")
	}
	tr, err := Transport(c)
	if err != nil {
		t.Fatal(err)
	}
	if tr.TLS != nil || tr.SASL != nil {
		t.Error("PLAINTEXT transport must not carry TLS or SASL")
	}
}

func TestMechanismNames(t *testing.T) {
	for _, tc := range []struct{ mech, want string }{
		{kafkaclient.MechanismPlain, "PLAIN"},
		{kafkaclient.MechanismSCRAMSHA256, "SCRAM-SHA-256"},
		{kafkaclient.MechanismSCRAMSHA512, "SCRAM-SHA-512"},
	} {
		c := kafkaclient.Config{
			Brokers: []string{"b:9093"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
			SASLMechanism: tc.mech, Username: "svc", Password: "pw",
		}
		m, err := Mechanism(c)
		if err != nil {
			t.Fatalf("%s: %v", tc.mech, err)
		}
		if m == nil {
			t.Fatalf("%s: mechanism is nil", tc.mech)
		}
		// kafka-go sends Name() on the wire; a mismatch is an unsupported-
		// mechanism error from the broker, so assert the exact string.
		if m.Name() != tc.want {
			t.Errorf("%s: Name() = %q, want %q", tc.mech, m.Name(), tc.want)
		}
	}
}

func TestSASLSSLCarriesBothLayers(t *testing.T) {
	c := kafkaclient.Config{
		Brokers: []string{"b1:9093", "b2:9093"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
		SASLMechanism: kafkaclient.MechanismSCRAMSHA512, Username: "svc", Password: "pw",
	}
	d, err := Dialer(c)
	if err != nil {
		t.Fatal(err)
	}
	if d.TLS == nil || d.SASLMechanism == nil {
		t.Error("SASL_SSL dialer must carry both TLS and a SASL mechanism")
	}
	tr, err := Transport(c)
	if err != nil {
		t.Fatal(err)
	}
	if tr.TLS == nil || tr.SASL == nil {
		t.Error("SASL_SSL transport must carry both TLS and a SASL mechanism")
	}

	// Addr must spread every broker, otherwise a Writer loses failover.
	addr := Addr(c)
	if !strings.Contains(addr.String(), "b1:9093") || !strings.Contains(addr.String(), "b2:9093") {
		t.Errorf("Addr() = %q, want both brokers", addr.String())
	}
}

func TestInvalidConfigIsRejectedNotIgnored(t *testing.T) {
	c := kafkaclient.Config{
		Brokers: []string{"b:9093"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
		SASLMechanism: kafkaclient.MechanismPlain, // no credentials
	}
	if _, err := Dialer(c); err == nil {
		t.Error("Dialer must reject SASL without credentials rather than dialing anonymously")
	}
	if _, err := Transport(c); err == nil {
		t.Error("Transport must reject SASL without credentials rather than dialing anonymously")
	}
}
