// Package saramaauth stamps a kafkaclient.Config onto a *sarama.Config.
//
// It is a separate package from kafkaclient so that a service using only
// segmentio/kafka-go never compiles sarama into its binary.
package saramaauth

import (
	"fmt"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	"github.com/rsync-ai/shared/kafkaclient/tokenauth"
)

// Apply configures authentication and transport security on cfg.
//
// It is a no-op for PLAINTEXT, so every existing call site can adopt it
// unconditionally and keep its current behavior until the deployment actually
// sets KAFKA_SECURITY_PROTOCOL.
func Apply(cfg *sarama.Config, c kafkaclient.Config) error {
	if cfg == nil {
		return fmt.Errorf("saramaauth: nil *sarama.Config")
	}
	if err := c.Validate(); err != nil {
		return err
	}

	// An empty ClientID leaves sarama's own default in place rather than
	// blanking it: a Config built as a struct literal has no client.id to give,
	// and "sarama" is still better than "".
	if c.ClientID != "" {
		cfg.ClientID = c.ClientID
	}

	if c.UsesTLS() {
		tlsCfg, err := c.TLSConfig()
		if err != nil {
			return err
		}
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = tlsCfg
	}

	if !c.UsesSASL() {
		return nil
	}

	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Handshake = true
	cfg.Net.SASL.User = c.Username
	cfg.Net.SASL.Password = c.Password

	switch c.SASLMechanism {
	case kafkaclient.MechanismPlain:
		cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	case kafkaclient.MechanismSCRAMSHA256:
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &scramClient{hashGen: scram.SHA256}
		}
	case kafkaclient.MechanismSCRAMSHA512:
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &scramClient{hashGen: scram.SHA512}
		}
	case kafkaclient.MechanismAWSMSKIAM, kafkaclient.MechanismOAuthBearer:
		// Both are SASL/OAUTHBEARER on the wire; the token source is what
		// differs, and tokenauth has already decided which one from the config.
		src, err := tokenauth.New(c)
		if err != nil {
			return err
		}
		cfg.Net.SASL.Mechanism = sarama.SASLTypeOAuth
		cfg.Net.SASL.TokenProvider = tokenProvider{src: src}
		// The token is the credential; a username and password left over from a
		// previous mechanism would otherwise be sent along with it.
		cfg.Net.SASL.User = ""
		cfg.Net.SASL.Password = ""
	default:
		// Unreachable: Validate already rejected anything else. Kept so a new
		// mechanism constant cannot silently fall through to no authentication.
		return fmt.Errorf("saramaauth: unhandled SASL mechanism %q", c.SASLMechanism)
	}
	return nil
}

// NewClient is the one-call replacement for sarama.NewClient at a call site
// that currently builds its own broker slice. It exists to make the
// multi-broker collapse bug — []string{"b1:9093,b2:9093"} — unrepresentable:
// the brokers come from c.Brokers, which is always already split.
func NewClient(c kafkaclient.Config, cfg *sarama.Config) (sarama.Client, error) {
	if err := Apply(cfg, c); err != nil {
		return nil, err
	}
	return sarama.NewClient(c.Brokers, cfg)
}
