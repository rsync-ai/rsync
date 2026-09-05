package kafkaclient

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestParseBrokersDoesNotCollapse is the regression test for the bug this
// package was written to make unrepresentable: five call sites wrapped a
// comma-separated bootstrap string as []string{s}, turning a customer's
// three-broker cluster into one hostname that does not resolve.
func TestParseBrokersDoesNotCollapse(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"kafka:29092", []string{"kafka:29092"}},
		{"b1:9093,b2:9093,b3:9093", []string{"b1:9093", "b2:9093", "b3:9093"}},
		{" b1:9093 , b2:9093 ", []string{"b1:9093", "b2:9093"}}, // trailing whitespace is common in .env files
		{"b1:9093,,b2:9093", []string{"b1:9093", "b2:9093"}},    // a doubled comma must not become a blank broker
		{"b1:9093,", []string{"b1:9093"}},                       // trailing comma likewise
		{"", nil},
		{"   ", nil},
	}
	for _, tc := range cases {
		got := ParseBrokers(tc.in)
		if len(got) != len(tc.want) {
			t.Fatalf("ParseBrokers(%q) = %v (len %d), want %v (len %d)", tc.in, got, len(got), tc.want, len(tc.want))
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("ParseBrokers(%q)[%d] = %q, want %q", tc.in, i, got[i], tc.want[i])
			}
		}
	}
}

func TestFromEnvDefaultsToPlaintextAndChangesNothing(t *testing.T) {
	clearKafkaEnv(t)
	c, err := FromEnv("kafka:29092")
	if err != nil {
		t.Fatalf("FromEnv with a bare environment must succeed, got %v", err)
	}
	// The whole adoption story depends on this: an existing deployment that
	// sets none of the new vars must behave exactly as it does today.
	if c.SecurityProtocol != ProtocolPlaintext {
		t.Errorf("SecurityProtocol = %q, want %q", c.SecurityProtocol, ProtocolPlaintext)
	}
	if c.UsesTLS() || c.UsesSASL() {
		t.Errorf("bare environment must not enable TLS or SASL, got %s", c)
	}
	if !c.UsingDefaultBrokers {
		t.Error("UsingDefaultBrokers should be true when no broker env var is set")
	}
	if len(c.Brokers) != 1 || c.Brokers[0] != "kafka:29092" {
		t.Errorf("Brokers = %v, want [kafka:29092]", c.Brokers)
	}
}

func TestFromEnvBrokersPrecedence(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv(EnvBootstrapServers, "bootstrap:9092")
	c, err := FromEnv("unused:1")
	if err != nil {
		t.Fatal(err)
	}
	if c.Brokers[0] != "bootstrap:9092" {
		t.Errorf("KAFKA_BOOTSTRAP_SERVERS should be honored, got %v", c.Brokers)
	}
	if c.UsingDefaultBrokers {
		t.Error("UsingDefaultBrokers must be false once an env var supplied the brokers")
	}

	// KAFKA_BROKERS wins — the precedence the temporal-adapter already had.
	t.Setenv(EnvBrokers, "explicit:9092")
	c, err = FromEnv("unused:1")
	if err != nil {
		t.Fatal(err)
	}
	if c.Brokers[0] != "explicit:9092" {
		t.Errorf("KAFKA_BROKERS must win over KAFKA_BOOTSTRAP_SERVERS, got %v", c.Brokers)
	}
}

func TestFromEnvSASLSSL(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv(EnvBootstrapServers, "b1:9093,b2:9093")
	t.Setenv(EnvSecurityProtocol, "sasl_ssl") // lowercase: operators write it both ways
	t.Setenv(EnvSASLMechanism, "scram-sha-512")
	t.Setenv(EnvSASLUsername, "svc-rsync")
	t.Setenv(EnvSASLPassword, "hunter2")

	c, err := FromEnv("kafka:29092")
	if err != nil {
		t.Fatalf("valid SASL_SSL config rejected: %v", err)
	}
	if !c.UsesTLS() || !c.UsesSASL() {
		t.Errorf("SASL_SSL must enable both TLS and SASL, got %s", c)
	}
	if c.SASLMechanism != MechanismSCRAMSHA512 {
		t.Errorf("mechanism = %q, want %q (case must be normalized)", c.SASLMechanism, MechanismSCRAMSHA512)
	}
	if len(c.Brokers) != 2 {
		t.Errorf("Brokers = %v, want 2 entries", c.Brokers)
	}
}

// TestSASLWithoutCredentialsIsAnError is the fail-loudly guarantee: a broker
// that requires SASL must not be dialed anonymously because an env var was
// forgotten. Silently degrading here would look like a network problem.
func TestSASLWithoutCredentialsIsAnError(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv(EnvBootstrapServers, "b1:9093")
	t.Setenv(EnvSecurityProtocol, ProtocolSASLSSL)
	t.Setenv(EnvSASLMechanism, MechanismPlain)
	// username/password deliberately unset
	if _, err := FromEnv("kafka:29092"); err == nil {
		t.Fatal("SASL_SSL without username/password must be an error, got nil")
	}
}

func TestValidateRejects(t *testing.T) {
	cases := []struct {
		name string
		c    Config
		want string
	}{
		{"no brokers", Config{SecurityProtocol: ProtocolPlaintext}, "no brokers"},
		{"bad protocol", Config{Brokers: []string{"b:9092"}, SecurityProtocol: "TLS"}, "unsupported"},
		{"bad mechanism", Config{Brokers: []string{"b:9092"}, SecurityProtocol: ProtocolSASLSSL,
			SASLMechanism: "GSSAPI", Username: "u", Password: "p"}, "unsupported"},
		// AWS_MSK_IAM is implemented now, so the rejection that used to name it
		// as unimplemented has become a rejection of the thing it cannot work
		// without: a region to sign against.
		{"msk iam without a region", Config{Brokers: []string{"b:9092"}, SecurityProtocol: ProtocolSASLSSL,
			SASLMechanism: MechanismAWSMSKIAM}, "AWS region"},
		{"half a keypair", Config{Brokers: []string{"b:9092"}, SecurityProtocol: ProtocolSSL,
			ClientCertFile: "/tmp/c.pem"}, "must be set together"},
		{"space in broker", Config{Brokers: []string{"b1:9092 b2:9092"}, SecurityProtocol: ProtocolPlaintext}, "whitespace"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.c.Validate()
			if err == nil {
				t.Fatalf("Validate() = nil, want an error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Validate() = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}

// TestStringRedactsPassword guards a credential-leak path. Broker credentials
// reaching a log line is also a path into an LLM prompt in this codebase, and
// the repo rule is that credentials never do.
func TestStringRedactsPassword(t *testing.T) {
	c := Config{
		Brokers: []string{"b:9093"}, SecurityProtocol: ProtocolSASLSSL,
		SASLMechanism: MechanismPlain, Username: "svc", Password: "s3cr3t-do-not-log",
	}
	for _, rendered := range []string{c.String(), sprintfV(c), sprintfPlusV(c)} {
		if strings.Contains(rendered, "s3cr3t-do-not-log") {
			t.Fatalf("password leaked into %q", rendered)
		}
		if !strings.Contains(rendered, "REDACTED") {
			t.Errorf("expected REDACTED marker in %q", rendered)
		}
	}
}

func TestWarningsFlagIgnoredCredentials(t *testing.T) {
	c := Config{Brokers: []string{"b:9092"}, SecurityProtocol: ProtocolPlaintext,
		Username: "svc", Password: "pw"}
	w := c.Warnings()
	if len(w) == 0 || !strings.Contains(strings.Join(w, " "), "ignored") {
		t.Errorf("credentials under PLAINTEXT should warn that they are ignored, got %v", w)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("that situation must stay non-fatal so an upgrade cannot break a running deployment: %v", err)
	}
}

func TestTLSConfig(t *testing.T) {
	plain := Config{Brokers: []string{"b:9092"}, SecurityProtocol: ProtocolPlaintext}
	if tc, err := plain.TLSConfig(); err != nil || tc != nil {
		t.Errorf("PLAINTEXT must yield (nil, nil), got (%v, %v)", tc, err)
	}

	// No CA file is legitimate: managed clusters use publicly-trusted certs.
	noCA := Config{Brokers: []string{"b:9093"}, SecurityProtocol: ProtocolSSL}
	tc, err := noCA.TLSConfig()
	if err != nil || tc == nil {
		t.Fatalf("SSL without a CA file must succeed and use system roots, got (%v, %v)", tc, err)
	}
	if tc.RootCAs != nil {
		t.Error("RootCAs should stay nil so the system pool is used")
	}

	// A CA path that is not a certificate must name the problem.
	dir := t.TempDir()
	bad := filepath.Join(dir, "not-a-cert.pem")
	if err := os.WriteFile(bad, []byte("this is not PEM\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := Config{Brokers: []string{"b:9093"}, SecurityProtocol: ProtocolSSL, CACertFile: bad}
	if _, err := c.TLSConfig(); err == nil || !strings.Contains(err.Error(), "no PEM certificates") {
		t.Errorf("a non-PEM CA file must be reported clearly, got %v", err)
	}

	missing := Config{Brokers: []string{"b:9093"}, SecurityProtocol: ProtocolSSL,
		CACertFile: filepath.Join(dir, "absent.pem")}
	if _, err := missing.TLSConfig(); err == nil {
		t.Error("a missing CA file must be an error, not a silent fallback to system roots")
	}
}

// clearKafkaEnv unsets every variable this package reads.
//
// AWS_REGION in particular is set on most developer machines and on every CI
// runner with AWS credentials, so a test asserting "no region configured" would
// otherwise pass or fail depending on whose machine it ran on.
func clearKafkaEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		EnvBrokers, EnvBootstrapServers, EnvSecurityProtocol, EnvSASLMechanism,
		EnvSASLUsername, EnvSASLPassword, EnvSSLCALocation, EnvSSLCertLocation,
		EnvSSLKeyLocation, EnvSSLSkipVerify, EnvClientID, EnvAWSRegion,
		EnvOAuthTokenEndpoint, EnvOAuthClientID, EnvOAuthClientSecret,
		EnvOAuthScope, EnvOAuthExtensions,
		EnvSSLSkipVerifyAlias, EnvTLSCAAlias, EnvTLSCertAlias, EnvTLSKeyAlias,
		EnvAWSRegionFallback, EnvAWSRegionFallbackLegacy,
	} {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}
	resetWarnOnce()
}

// TestWithBrokersKeepsSecurityAndSplits covers the adoption path used by every
// service that already owns a bootstrap-string field: the field must win for
// addressing, the environment must still win for security, and the string must
// be split rather than collapsed.
func TestWithBrokersKeepsSecurityAndSplits(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv(EnvSecurityProtocol, ProtocolSASLSSL)
	t.Setenv(EnvSASLMechanism, MechanismSCRAMSHA512)
	t.Setenv(EnvSASLUsername, "svc")
	t.Setenv(EnvSASLPassword, "pw")
	t.Setenv(EnvBootstrapServers, "from-env:9092")

	c, err := FromEnv("unused:1")
	if err != nil {
		t.Fatal(err)
	}
	c = c.WithBrokers("field1:9093,field2:9093")

	if len(c.Brokers) != 2 || c.Brokers[0] != "field1:9093" || c.Brokers[1] != "field2:9093" {
		t.Errorf("the caller's field must win for addressing and be split, got %v", c.Brokers)
	}
	if !c.UsesSASL() || c.SASLMechanism != MechanismSCRAMSHA512 || c.Username != "svc" {
		t.Errorf("security settings must survive WithBrokers, got %s", c)
	}
	if c.UsingDefaultBrokers {
		t.Error("an explicit broker list is not the built-in default")
	}
	if err := c.Validate(); err != nil {
		t.Errorf("result must be valid: %v", err)
	}
}

func TestWithBrokerListCleansEntries(t *testing.T) {
	clearKafkaEnv(t)
	c, err := FromEnv("kafka:29092")
	if err != nil {
		t.Fatal(err)
	}
	c = c.WithBrokerList([]string{" b1:9093", "", "b2:9093 ", "   "})
	if len(c.Brokers) != 2 || c.Brokers[0] != "b1:9093" || c.Brokers[1] != "b2:9093" {
		t.Errorf("Brokers = %#v, want [b1:9093 b2:9093]", c.Brokers)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("cleaned list must validate: %v", err)
	}
}
