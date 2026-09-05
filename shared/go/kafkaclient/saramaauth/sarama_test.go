package saramaauth

import (
	"strings"
	"testing"

	"github.com/IBM/sarama"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// TestApplyPlaintextIsANoOp is the safety property that lets every existing
// call site adopt Apply in one commit: with no security env set, the resulting
// sarama.Config must be indistinguishable from today's.
func TestApplyPlaintextIsANoOp(t *testing.T) {
	cfg := sarama.NewConfig()
	c := kafkaclient.Config{Brokers: []string{"kafka:29092"}, SecurityProtocol: kafkaclient.ProtocolPlaintext}
	if err := Apply(cfg, c); err != nil {
		t.Fatal(err)
	}
	if cfg.Net.SASL.Enable {
		t.Error("PLAINTEXT must not enable SASL")
	}
	if cfg.Net.TLS.Enable {
		t.Error("PLAINTEXT must not enable TLS")
	}
}

func TestApplySASLPlain(t *testing.T) {
	cfg := sarama.NewConfig()
	c := kafkaclient.Config{
		Brokers: []string{"b:9092"}, SecurityProtocol: kafkaclient.ProtocolSASLPlaintext,
		SASLMechanism: kafkaclient.MechanismPlain, Username: "svc", Password: "pw",
	}
	if err := Apply(cfg, c); err != nil {
		t.Fatal(err)
	}
	if !cfg.Net.SASL.Enable {
		t.Fatal("SASL_PLAINTEXT must enable SASL")
	}
	if cfg.Net.TLS.Enable {
		t.Error("SASL_PLAINTEXT is authenticated but unencrypted; TLS must stay off")
	}
	if cfg.Net.SASL.Mechanism != sarama.SASLTypePlaintext {
		t.Errorf("mechanism = %q, want %q", cfg.Net.SASL.Mechanism, sarama.SASLTypePlaintext)
	}
	if cfg.Net.SASL.User != "svc" || cfg.Net.SASL.Password != "pw" {
		t.Error("credentials were not carried onto the sarama config")
	}
}

// TestSCRAMGeneratorProducesAWorkingClient is the test that matters most here.
// sarama ships no SCRAM implementation: setting Mechanism to SCRAM-SHA-512
// without a SCRAMClientGeneratorFunc compiles, passes a nil check, and then
// panics on the first handshake against a real broker. Asserting the func is
// non-nil would not catch a broken adapter, so this drives an actual
// conversation step and inspects the client-first message.
func TestSCRAMGeneratorProducesAWorkingClient(t *testing.T) {
	for _, tc := range []struct {
		mech string
		want sarama.SASLMechanism
	}{
		{kafkaclient.MechanismSCRAMSHA256, sarama.SASLTypeSCRAMSHA256},
		{kafkaclient.MechanismSCRAMSHA512, sarama.SASLTypeSCRAMSHA512},
	} {
		t.Run(tc.mech, func(t *testing.T) {
			cfg := sarama.NewConfig()
			c := kafkaclient.Config{
				Brokers: []string{"b:9093"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
				SASLMechanism: tc.mech, Username: "svc-rsync", Password: "pw",
			}
			if err := Apply(cfg, c); err != nil {
				t.Fatal(err)
			}
			if cfg.Net.SASL.Mechanism != tc.want {
				t.Errorf("mechanism = %q, want %q", cfg.Net.SASL.Mechanism, tc.want)
			}
			if !cfg.Net.TLS.Enable || cfg.Net.TLS.Config == nil {
				t.Error("SASL_SSL must also enable TLS")
			}
			if cfg.Net.SASL.SCRAMClientGeneratorFunc == nil {
				t.Fatal("SCRAMClientGeneratorFunc is nil; sarama would panic at handshake time")
			}

			client := cfg.Net.SASL.SCRAMClientGeneratorFunc()
			if err := client.Begin("svc-rsync", "pw", ""); err != nil {
				t.Fatalf("Begin: %v", err)
			}
			first, err := client.Step("")
			if err != nil {
				t.Fatalf("Step: %v", err)
			}
			// RFC 5802 client-first-message: gs2 header, then n=<user>,r=<nonce>.
			if !strings.Contains(first, "n=svc-rsync") || !strings.Contains(first, ",r=") {
				t.Errorf("client-first-message %q does not look like SCRAM", first)
			}
			if client.Done() {
				t.Error("conversation reported Done after only the first step")
			}
		})
	}
}

func TestApplyRejectsInvalidConfig(t *testing.T) {
	cfg := sarama.NewConfig()
	// SASL requested, credentials missing — must not fall through to anonymous.
	c := kafkaclient.Config{
		Brokers: []string{"b:9093"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
		SASLMechanism: kafkaclient.MechanismPlain,
	}
	if err := Apply(cfg, c); err == nil {
		t.Fatal("Apply must reject SASL without credentials")
	}
	if cfg.Net.SASL.Enable {
		t.Error("a rejected config must not leave SASL half-enabled")
	}
}
