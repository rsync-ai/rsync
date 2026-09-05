// Package kgoauth translates a kafkaclient.Config into the segmentio/kafka-go
// shapes: a *kafka.Dialer for Readers and a *kafka.Transport for Writers and
// Clients.
//
// It is a separate package from kafkaclient so that a service using only
// IBM/sarama never compiles kafka-go into its binary.
package kgoauth

import (
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	"github.com/rsync-ai/shared/kafkaclient/tokenauth"
)

// DefaultDialTimeout matches the 10s that the existing call sites use.
const DefaultDialTimeout = 10 * time.Second

// Mechanism returns the kafka-go SASL mechanism, or nil for a non-SASL config.
//
// Unlike sarama, kafka-go ships its own SCRAM implementation, so there is no
// adapter to write here.
func Mechanism(c kafkaclient.Config) (sasl.Mechanism, error) {
	if !c.UsesSASL() {
		return nil, nil
	}
	switch c.SASLMechanism {
	case kafkaclient.MechanismPlain:
		return plain.Mechanism{Username: c.Username, Password: c.Password}, nil
	case kafkaclient.MechanismSCRAMSHA256:
		return scram.Mechanism(scram.SHA256, c.Username, c.Password)
	case kafkaclient.MechanismSCRAMSHA512:
		return scram.Mechanism(scram.SHA512, c.Username, c.Password)
	case kafkaclient.MechanismAWSMSKIAM, kafkaclient.MechanismOAuthBearer:
		// kafka-go ships no OAUTHBEARER mechanism, so oauthbearer.go supplies
		// one; tokenauth decides whether the token comes from the AWS signer or
		// an OIDC endpoint.
		src, err := tokenauth.New(c)
		if err != nil {
			return nil, err
		}
		return oauthBearer{src: src}, nil
	default:
		return nil, fmt.Errorf("kgoauth: unhandled SASL mechanism %q", c.SASLMechanism)
	}
}

// Dialer builds the *kafka.Dialer a kafka.Reader (or a raw kafka.Dial) needs.
// It returns a usable dialer even for PLAINTEXT so call sites do not have to
// branch on whether security is configured.
func Dialer(c kafkaclient.Config) (*kafka.Dialer, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	tlsCfg, err := c.TLSConfig()
	if err != nil {
		return nil, err
	}
	m, err := Mechanism(c)
	if err != nil {
		return nil, err
	}
	return &kafka.Dialer{
		ClientID:      c.ClientID,
		Timeout:       DefaultDialTimeout,
		DualStack:     true,
		TLS:           tlsCfg, // nil ⇒ plaintext, which is kafka-go's own default
		SASLMechanism: m,
	}, nil
}

// Transport builds the *kafka.Transport a kafka.Writer or kafka.Client needs.
func Transport(c kafkaclient.Config) (*kafka.Transport, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	tlsCfg, err := c.TLSConfig()
	if err != nil {
		return nil, err
	}
	m, err := Mechanism(c)
	if err != nil {
		return nil, err
	}
	return &kafka.Transport{
		ClientID:    c.ClientID,
		DialTimeout: DefaultDialTimeout,
		TLS:         tlsCfg,
		SASL:        m,
	}, nil
}

// Addr is the net.Addr covering every broker in the config. A Writer built
// with kafka.TCP(one-address) against a multi-broker cluster loses its
// failover, so call sites should spread the whole list through this.
func Addr(c kafkaclient.Config) net.Addr {
	return kafka.TCP(c.Brokers...)
}
