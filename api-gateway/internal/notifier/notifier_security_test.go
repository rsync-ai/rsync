package notifier

import (
	"context"
	"strings"
	"testing"
)

// The notifier must read the deployment's Kafka security settings like every
// other client in this service.
//
// Pre-fix it built a bare sarama.NewConfig(), so on a customer's SASL cluster
// it connected anonymously and every Slack and email alert stopped — the
// failure silences the alerting that would otherwise report it.
//
// The probe is a SASL configuration the consumer cannot possibly use (a
// mechanism with no credentials): wired, it is rejected by name before any
// connection is attempted; unwired, the environment is invisible and the only
// error that can come back is a transport failure from dialing the broker.
// db is nil because the security check must happen before any query.
func TestStartAppliesKafkaSecurity(t *testing.T) {
	for _, k := range []string{
		"KAFKA_BROKERS", "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_CLIENT_ID",
		"KAFKA_SECURITY_PROTOCOL", "KAFKA_SASL_MECHANISM",
		"KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD",
		"KAFKA_SSL_CA_LOCATION", "KAFKA_SSL_CERT_LOCATION", "KAFKA_SSL_KEY_LOCATION",
	} {
		t.Setenv(k, "")
	}
	t.Setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
	t.Setenv("KAFKA_SASL_MECHANISM", "PLAIN")

	// 127.0.0.1:1 refuses immediately, so an unwired consumer fails fast with a
	// dial error rather than hanging on a metadata retry loop.
	n, err := Start(context.Background(), nil, []string{"127.0.0.1:1"})
	if err == nil {
		t.Fatal("notifier started with a SASL mechanism and no credentials")
	}
	if n != nil {
		t.Error("a Notifier was returned alongside the error")
	}
	if !strings.Contains(err.Error(), "KAFKA_SASL_USERNAME") {
		t.Fatalf("error %q does not name the missing SASL credential: the notifier is not reading the "+
			"deployment's Kafka security config and would connect unauthenticated", err)
	}
}
