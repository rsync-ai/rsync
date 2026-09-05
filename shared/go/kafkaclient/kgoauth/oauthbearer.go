package kgoauth

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/segmentio/kafka-go/sasl"

	"github.com/rsync-ai/shared/kafkaclient/tokenauth"
)

// MechanismNameOAuthBearer is the name sent in the SASL handshake. Both
// AWS_MSK_IAM and OIDC authenticate as OAUTHBEARER: a broker offered the
// literal string "AWS_MSK_IAM" answers with UnsupportedSaslMechanism.
const MechanismNameOAuthBearer = "OAUTHBEARER"

// oauthBearer implements SASL/OAUTHBEARER (RFC 7628) for kafka-go, which —
// unlike sarama — ships PLAIN and SCRAM only and leaves this one to the caller.
type oauthBearer struct {
	src tokenauth.Source
}

func (m oauthBearer) Name() string { return MechanismNameOAuthBearer }

// Start mints a token and returns it as the initial client response.
//
// The token is fetched per handshake rather than per Mechanism so that a
// long-lived Reader reconnecting hours later presents a current token; the
// source's cache is what keeps that from becoming a request per connection.
func (m oauthBearer) Start(ctx context.Context) (sasl.StateMachine, []byte, error) {
	t, err := m.src.Token(ctx)
	if err != nil {
		return nil, nil, err
	}
	return oauthBearerSession{}, clientInitialResponse(t.Value, t.Extensions), nil
}

type oauthBearerSession struct{}

// Next completes the exchange. The broker answers an accepted token with an
// empty response; anything else is a failure message.
func (oauthBearerSession) Next(_ context.Context, challenge []byte) (bool, []byte, error) {
	if len(challenge) == 0 {
		return true, nil, nil
	}
	// RFC 7628 has the client acknowledge a failure with a lone kvsep and wait
	// for the server to close the connection. Doing that here would surface as
	// a bare EOF; returning the server's own reason instead is the difference
	// between "SASL authentication failed" and a message naming the expired
	// token or the missing scope.
	return false, nil, fmt.Errorf("kgoauth: broker rejected the OAUTHBEARER token: %s", bytes.TrimSpace(challenge))
}

// clientInitialResponse builds the RFC 7628 client-first message:
//
//	"n,," kvsep "auth=Bearer " token kvsep *(key "=" value kvsep) kvsep
//
// where kvsep is 0x01. It is byte-for-byte what sarama's own
// buildClientFirstMessage produces, so a cluster that accepts one client
// library's handshake accepts the other's.
//
// Extensions are emitted in sorted order. Map iteration order would make the
// bytes differ between runs, which is exactly the kind of thing that turns a
// reproducible handshake failure into an intermittent one.
func clientInitialResponse(token string, extensions map[string]string) []byte {
	var b bytes.Buffer
	b.WriteString("n,,\x01auth=Bearer ")
	b.WriteString(token)
	b.WriteByte(0x01)

	keys := make([]string, 0, len(extensions))
	for k := range extensions {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(extensions[k])
		b.WriteByte(0x01)
	}

	b.WriteByte(0x01)
	return b.Bytes()
}
