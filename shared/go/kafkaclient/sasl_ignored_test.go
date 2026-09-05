package kafkaclient

import (
	"strings"
	"testing"
)

// The failure this file guards is not a crash. An operator sets a Kafka
// username and password, omits KAFKA_SECURITY_PROTOCOL because they did not
// know it existed, and gets a client that connects as an anonymous user. The
// broker either has no ACLs and accepts it -- so the credentials are never
// checked and nobody finds out -- or it has ACLs and rejects it with an error
// that names the broker, sending the operator to look at the network.
//
// Config.Validate deliberately does not fail here (see
// TestWarningsFlagIgnoredCredentials: an upgrade must not break a deployment
// that has stray variables set), and Warnings() only reaches an operator whose
// service remembered to log it -- five of this platform's eight callers do not.
// So the package says it itself, once, through the logger a service can
// redirect but not silence.

func TestCredentialsUnderPlaintextWarnLoudly(t *testing.T) {
	clearKafkaEnv(t)
	warnings := captureWarnings(t)
	t.Setenv(EnvBootstrapServers, "b1:9092,b2:9092")
	t.Setenv(EnvSASLUsername, "svc-rsync")
	t.Setenv(EnvSASLPassword, "hunter2")
	// KAFKA_SECURITY_PROTOCOL deliberately unset: this is the whole bug.

	c, err := FromEnv("kafka:29092")
	if err != nil {
		t.Fatalf("this must stay non-fatal: %v", err)
	}
	if c.UsesSASL() {
		t.Fatal("precondition broken: an unset protocol should not resolve to SASL")
	}

	got := strings.Join(warnings(), "\n")
	if got == "" {
		t.Fatal("credentials that cannot be used produced no warning at all -- " +
			"this is the silent path the fix exists to close")
	}
	// It must name the variable to change, the variables that are wasted, and
	// the consequence. Naming only the consequence leaves the operator guessing.
	for _, want := range []string{EnvSecurityProtocol, EnvSASLUsername, EnvSASLPassword, "anonymous", "IGNORED"} {
		if !strings.Contains(got, want) {
			t.Errorf("warning is missing %q: %q", want, got)
		}
	}
	// "unset" and "unencrypted" are the two facts the operator does not have:
	// that a default was chosen for them, and what it cost.
	if !strings.Contains(got, "unset") {
		t.Errorf("an unset %s must be reported as unset, not as a choice: %q", EnvSecurityProtocol, got)
	}
	// The claim about THIS connection, not the remediation sentence -- which
	// names SASL_PLAINTEXT and so contains the word either way.
	if !strings.Contains(got, "anonymous and unencrypted") {
		t.Errorf("PLAINTEXT must be reported as unencrypted: %q", got)
	}
	// The password must never appear, in this or any other diagnostic.
	if strings.Contains(got, "hunter2") {
		t.Errorf("the warning leaks the password: %q", got)
	}
}

// A mechanism on its own was invisible before: Warnings() only looked at the
// username and password, so KAFKA_SASL_MECHANISM=SCRAM-SHA-512 with no protocol
// produced nothing anywhere.
func TestMechanismAloneUnderPlaintextWarns(t *testing.T) {
	clearKafkaEnv(t)
	warnings := captureWarnings(t)
	t.Setenv(EnvBootstrapServers, "b:9092")
	t.Setenv(EnvSASLMechanism, MechanismSCRAMSHA512)

	c, err := FromEnv("kafka:29092")
	if err != nil {
		t.Fatalf("this must stay non-fatal: %v", err)
	}
	got := strings.Join(warnings(), "\n")
	if !strings.Contains(got, EnvSASLMechanism) {
		t.Errorf("a mechanism set under a non-SASL protocol must be reported: %q", got)
	}
	if w := strings.Join(c.Warnings(), " | "); !strings.Contains(w, EnvSASLMechanism) {
		t.Errorf("Warnings() must agree with the emitted warning, got %v", c.Warnings())
	}
}

// Same for an OAuth endpoint, which no earlier check looked at.
func TestOAuthEndpointAloneUnderPlaintextWarns(t *testing.T) {
	clearKafkaEnv(t)
	warnings := captureWarnings(t)
	t.Setenv(EnvBootstrapServers, "b:9092")
	t.Setenv(EnvOAuthTokenEndpoint, "https://issuer.example/oauth2/token")

	if _, err := FromEnv("kafka:29092"); err != nil {
		t.Fatalf("this must stay non-fatal: %v", err)
	}
	if got := strings.Join(warnings(), "\n"); !strings.Contains(got, EnvOAuthTokenEndpoint) {
		t.Errorf("an OAuth token endpoint set under a non-SASL protocol must be reported: %q", got)
	}
}

// Positive control: a correctly configured SASL client must stay quiet, or the
// warning becomes scroll and stops being read.
func TestConfiguredSASLDoesNotWarn(t *testing.T) {
	clearKafkaEnv(t)
	warnings := captureWarnings(t)
	t.Setenv(EnvBootstrapServers, "b:9093")
	t.Setenv(EnvSecurityProtocol, ProtocolSASLSSL)
	t.Setenv(EnvSASLMechanism, MechanismSCRAMSHA512)
	t.Setenv(EnvSASLUsername, "svc")
	t.Setenv(EnvSASLPassword, "pw")

	c, err := FromEnv("kafka:29092")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(warnings(), "\n"); strings.Contains(got, "IGNORED") {
		t.Errorf("a correctly configured SASL client must not be warned at: %q", got)
	}
	if n := c.ignoredSASLSettings(); n != nil {
		t.Errorf("ignoredSASLSettings() = %v, want nil when the protocol speaks SASL", n)
	}
}

// The other way to make the warning worthless: fire it on a plaintext in-VPC
// broker. AWS_REGION is set for unrelated reasons on every EC2 host and CI
// runner with AWS credentials, and Config reads it as a region fallback, so it
// must not count as "the operator asked for SASL".
func TestPlaintextBrokerOnAWSDoesNotWarn(t *testing.T) {
	clearKafkaEnv(t)
	warnings := captureWarnings(t)
	t.Setenv(EnvBootstrapServers, "b:9092")
	t.Setenv(EnvAWSRegionFallback, "us-east-1")

	c, err := FromEnv("kafka:29092")
	if err != nil {
		t.Fatal(err)
	}
	if c.AWSRegion != "us-east-1" {
		t.Fatalf("precondition broken: region should still be read, got %q", c.AWSRegion)
	}
	if got := strings.Join(warnings(), "\n"); strings.Contains(got, "IGNORED") {
		t.Errorf("AWS_REGION alone must not look like a request to authenticate: %q", got)
	}
}

// The OAuth client id and secret fall back to the SASL username and password.
// Listing them unconditionally would report a plain username-and-password slip
// as an OAuth one too, sending the operator to configure an issuer they do not
// have.
func TestIgnoredSettingsDoNotDoubleNameOAuthAliases(t *testing.T) {
	c := Config{
		Brokers: []string{"b:9092"}, SecurityProtocol: ProtocolPlaintext,
		Username: "svc", Password: "pw",
		OAuthClientID: "svc", OAuthClientSecret: "pw", // exactly what firstEnv's fallback produces
	}
	got := strings.Join(c.ignoredSASLSettings(), ",")
	want := strings.Join([]string{EnvSASLUsername, EnvSASLPassword}, ",")
	if got != want {
		t.Errorf("ignoredSASLSettings() = %q, want %q", got, want)
	}

	// But a client id and secret of their own are a separate configuration and
	// must be named.
	c.OAuthClientID, c.OAuthClientSecret = "client-abc", "secret-xyz"
	got = strings.Join(c.ignoredSASLSettings(), ",")
	for _, want := range []string{EnvOAuthClientID, EnvOAuthClientSecret} {
		if !strings.Contains(got, want) {
			t.Errorf("ignoredSASLSettings() = %q, want it to name %s", got, want)
		}
	}
}

// SSL without SASL is the quieter half of the same mistake: the connection is
// encrypted, so the "unencrypted" half of the message must not be claimed, but
// the credentials are still discarded.
func TestCredentialsUnderPlainTLSWarnWithoutClaimingCleartext(t *testing.T) {
	clearKafkaEnv(t)
	warnings := captureWarnings(t)
	t.Setenv(EnvBootstrapServers, "b:9093")
	t.Setenv(EnvSecurityProtocol, ProtocolSSL)
	t.Setenv(EnvSASLUsername, "svc")
	t.Setenv(EnvSASLPassword, "pw")

	if _, err := FromEnv("kafka:29092"); err != nil {
		t.Fatal(err)
	}
	got := strings.Join(warnings(), "\n")
	if !strings.Contains(got, "IGNORED") {
		t.Errorf("credentials under SSL are still discarded and must be reported: %q", got)
	}
	if strings.Contains(got, "anonymous and unencrypted") {
		t.Errorf("SSL is encrypted; the warning must not say otherwise: %q", got)
	}
	if strings.Contains(got, "unset") {
		t.Errorf("%s was set explicitly and must not be reported as unset: %q", EnvSecurityProtocol, got)
	}
}
