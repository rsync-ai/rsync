package kafkaclient

import (
	"fmt"
	"strings"
)

// saslExtAuthKey is the one extension name a client may not send: it is how the
// token itself travels in the OAUTHBEARER initial response, so a second copy
// makes the message unparseable at the broker.
const saslExtAuthKey = "auth"

// saslExtSeparator is the byte the SASL/OAUTHBEARER wire format uses to
// separate key-value pairs (RFC 7628 calls it kvsep). A value containing it
// would terminate the pair early and silently change what the broker reads.
const saslExtSeparator = "\x01"

// ParseSASLExtensions reads the KAFKA_SASL_OAUTHBEARER_EXTENSIONS form:
// comma-separated key=value pairs, e.g. "logicalCluster=lkc-abc123,identityPoolId=pool-1".
//
// Confluent Cloud's OAuth support is the reason this is configurable at all —
// it routes a connection to the right logical cluster by extension, and a
// connection missing one is rejected with a message that does not say which.
//
// An empty input yields a nil map, not an error: extensions are optional.
func ParseSASLExtensions(raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	out := make(map[string]string)
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, fmt.Errorf("%q is not key=value", pair)
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, validateSASLExtensions(out)
}

// validateSASLExtensions rejects extensions that would corrupt the initial
// client response rather than merely being ignored by the broker. It runs from
// Validate as well as from ParseSASLExtensions, so a Config assembled in code
// gets the same guarantee as one read from the environment.
func validateSASLExtensions(ext map[string]string) error {
	for k, v := range ext {
		switch {
		case k == "":
			return fmt.Errorf("kafkaclient: SASL extension with an empty name")
		case strings.EqualFold(k, saslExtAuthKey):
			return fmt.Errorf("kafkaclient: SASL extension %q is reserved for the token itself", k)
		case strings.Contains(k, "="):
			return fmt.Errorf("kafkaclient: SASL extension name %q contains '='", k)
		case strings.Contains(k, saslExtSeparator) || strings.Contains(v, saslExtSeparator):
			return fmt.Errorf("kafkaclient: SASL extension %q contains the reserved separator byte 0x01", k)
		}
	}
	return nil
}
