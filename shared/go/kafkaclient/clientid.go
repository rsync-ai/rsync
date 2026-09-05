package kafkaclient

import "strings"

// ClientIDNamespace is the prefix every rsync-ai client.id carries.
//
// It is the same idea as DefaultTopicPrefix and for the same reason: on a
// customer's shared cluster, everything this product puts on the wire should
// name the product. A broker operator reading a throttle metric or a request
// log sees rsync-orchestrator, not the client library's anonymous default.
const ClientIDNamespace = "rsync"

// DefaultClientID derives the client.id for a service.
//
// client.id was set nowhere in this platform, in any language, which on a
// managed cluster (MSK, Confluent Cloud) has a specific cost: broker-side quotas
// and throttle metrics key off client.id, so with the default value every
// connection from every rsync service was indistinguishable both from each other
// and from any other tenant's default client. When the cluster throttled us
// neither side could tell which service caused it, and the customer could not
// scope a quota to this product even if they wanted to.
//
// An empty service name yields the bare namespace rather than a trailing
// separator, so a caller that has no name to give still stops being anonymous.
func DefaultClientID(service string) string {
	s := sanitizeClientIDChars(service)
	if s == "" {
		return ClientIDNamespace
	}
	return ClientIDNamespace + "-" + s
}

// sanitizeClientIDChars reduces s to the character set Kafka accepts in a
// client.id ([a-zA-Z0-9._-]).
//
// Brokers older than 1.0.0 reject anything else outright, and sarama refuses to
// build a config for them; on newer brokers a stray character survives into
// metrics names and quota rules instead, where it is far harder to notice. The
// value can arrive from KAFKA_CLIENT_ID or from a service name assembled in
// code, so neither source is trusted to be clean.
func sanitizeClientIDChars(s string) string {
	s = strings.TrimSpace(s)
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '.', c == '_', c == '-':
			b.WriteByte(c)
		}
	}
	return b.String()
}
