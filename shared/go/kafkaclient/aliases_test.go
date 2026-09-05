package kafkaclient

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

// captureWarnings redirects this package's own warnings for the duration of a
// test and restores the previous logger afterwards. The once-map is reset on
// both ends so neither this test nor the next one silently observes nothing
// because an earlier test consumed the same key.
func captureWarnings(t *testing.T) func() []string {
	t.Helper()
	var mu sync.Mutex
	var lines []string

	warnMu.Lock()
	prev := warnLogf
	warnMu.Unlock()

	resetWarnOnce()
	SetWarnLogger(func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, fmt.Sprintf(format, args...))
	})
	t.Cleanup(func() {
		warnMu.Lock()
		warnLogf = prev
		warnMu.Unlock()
		resetWarnOnce()
	})
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), lines...)
	}
}

// The Go code shipped KAFKA_SSL_INSECURE_SKIP_VERIFY; the Python code and
// INVENTORY.md:1017 say KAFKA_SSL_SKIP_VERIFY. An operator following the docs
// set a variable Go never read, so the setting appeared to be ignored -- in the
// direction where "ignored" happens to be the safe one, which is why it went
// unnoticed for so long.
func TestBothSkipVerifySpellingsAreHonored(t *testing.T) {
	for _, name := range []string{EnvSSLSkipVerify, EnvSSLSkipVerifyAlias} {
		t.Run(name, func(t *testing.T) {
			clearKafkaEnv(t)
			_ = captureWarnings(t)
			t.Setenv(EnvBootstrapServers, "b:9093")
			t.Setenv(EnvSecurityProtocol, ProtocolSSL)
			t.Setenv(name, "true")

			c, err := FromEnv("kafka:29092")
			if err != nil {
				t.Fatal(err)
			}
			if !c.InsecureSkipVerify {
				t.Fatalf("%s=true was ignored", name)
			}
			if c.InsecureSkipVerifySource != name {
				t.Errorf("InsecureSkipVerifySource = %q, want %q -- a warning that names the wrong variable sends the operator looking in the wrong place",
					c.InsecureSkipVerifySource, name)
			}
			tlsCfg, err := c.TLSConfig()
			if err != nil {
				t.Fatal(err)
			}
			if !tlsCfg.InsecureSkipVerify {
				t.Fatal("the setting did not reach the *tls.Config, which is the only place it does anything")
			}
		})
	}
}

// Impossible to enable quietly: the package says it itself rather than relying
// on the caller to log Warnings().
func TestSkipVerifyWarnsLoudlyAndNamesTheVariable(t *testing.T) {
	clearKafkaEnv(t)
	warnings := captureWarnings(t)
	t.Setenv(EnvBootstrapServers, "b:9093")
	t.Setenv(EnvSecurityProtocol, ProtocolSSL)
	t.Setenv(EnvSSLSkipVerifyAlias, "1")

	c, err := FromEnv("kafka:29092")
	if err != nil {
		t.Fatal(err)
	}

	got := strings.Join(warnings(), "\n")
	if got == "" {
		t.Fatal("enabling skip-verify produced no warning at all")
	}
	if !strings.Contains(got, EnvSSLSkipVerifyAlias) {
		t.Errorf("the warning does not name the variable that is set: %q", got)
	}
	// The risk has to be stated, not just the fact.
	for _, want := range []string{"NOT verified", "impersonate"} {
		if !strings.Contains(got, want) {
			t.Errorf("the warning does not name the risk (%q missing): %q", want, got)
		}
	}
	// And the caller-facing list still carries it, naming the same variable.
	if w := strings.Join(c.Warnings(), " | "); !strings.Contains(w, EnvSSLSkipVerifyAlias) {
		t.Errorf("Warnings() = %v, want it to name %s", c.Warnings(), EnvSSLSkipVerifyAlias)
	}
}

// Impossible to enable by accident: only an explicitly truthy value counts.
func TestSkipVerifyIsOffUnlessExplicitlyTruthy(t *testing.T) {
	for _, v := range []string{"", "false", "0", "no", "off", "FALSE", "maybe", " "} {
		clearKafkaEnv(t)
		t.Setenv(EnvBootstrapServers, "b:9093")
		t.Setenv(EnvSecurityProtocol, ProtocolSSL)
		t.Setenv(EnvSSLSkipVerify, v)
		t.Setenv(EnvSSLSkipVerifyAlias, v)

		c, err := FromEnv("kafka:29092")
		if err != nil {
			t.Fatal(err)
		}
		if c.InsecureSkipVerify {
			t.Errorf("%s=%q enabled skip-verify", EnvSSLSkipVerify, v)
		}
		if c.InsecureSkipVerifySource != "" {
			t.Errorf("%s=%q recorded a source %q with the setting off", EnvSSLSkipVerify, v, c.InsecureSkipVerifySource)
		}
	}
}

// The escape hatch is warned and default-off, but it must not be removed: a
// local broker with a self-signed certificate is a real development setup, and
// taking the knob away sends people to a worse workaround.
func TestSkipVerifyRemainsAvailable(t *testing.T) {
	c := Config{Brokers: []string{"b:9093"}, SecurityProtocol: ProtocolSSL, InsecureSkipVerify: true}
	tlsCfg, err := c.TLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !tlsCfg.InsecureSkipVerify {
		t.Fatal("the escape hatch must keep working")
	}
}

// A silently-ignored TLS path is the worst failure in this file: the connection
// still comes up (against a broker with a publicly-trusted certificate) or
// fails with a certificate error that names no variable, and in the mTLS case
// the client simply presents no certificate.
func TestTLSPathAliasesAreHonored(t *testing.T) {
	// A client certificate without its key is already rejected, so the two mTLS
	// cases carry the counterpart under its shipped name.
	for _, tc := range []struct {
		name, canon, alias string
		alsoSet            map[string]string
		field              func(Config) string
	}{
		{"CA bundle", EnvSSLCALocation, EnvTLSCAAlias, nil,
			func(c Config) string { return c.CACertFile }},
		{"client cert", EnvSSLCertLocation, EnvTLSCertAlias, map[string]string{EnvSSLKeyLocation: "/certs/client.key"},
			func(c Config) string { return c.ClientCertFile }},
		{"client key", EnvSSLKeyLocation, EnvTLSKeyAlias, map[string]string{EnvSSLCertLocation: "/certs/client.crt"},
			func(c Config) string { return c.ClientKeyFile }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearKafkaEnv(t)
			t.Setenv(EnvBootstrapServers, "b:9093")
			t.Setenv(EnvSecurityProtocol, ProtocolSSL)
			for k, v := range tc.alsoSet {
				t.Setenv(k, v)
			}

			t.Setenv(tc.alias, "/certs/from-alias.pem")
			c, err := FromEnv("kafka:29092")
			if err != nil {
				t.Fatal(err)
			}
			if got := tc.field(c); got != "/certs/from-alias.pem" {
				t.Fatalf("%s was ignored: field = %q", tc.alias, got)
			}

			// The shipped name wins where both are set, so adopting the alias
			// cannot change an existing deployment's behavior.
			t.Setenv(tc.canon, "/certs/canonical.pem")
			c, err = FromEnv("kafka:29092")
			if err != nil {
				t.Fatal(err)
			}
			if got := tc.field(c); got != "/certs/canonical.pem" {
				t.Errorf("%s = %q, want the canonical %s to win", tc.name, got, tc.canon)
			}
		})
	}
}

// An alias that resolved but never reached crypto/tls would pass the test above
// and still ignore the certificate. Go all the way to the loaded pool.
func TestAliasedCAPathReachesTheTLSConfig(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv(EnvBootstrapServers, "b:9093")
	t.Setenv(EnvSecurityProtocol, ProtocolSSL)
	t.Setenv(EnvTLSCAAlias, "/no/such/ca.pem")

	c, err := FromEnv("kafka:29092")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.TLSConfig()
	if err == nil {
		t.Fatal("a CA path that does not exist must fail loudly; silently falling back to the system pool is how a customer's private CA gets ignored")
	}
	if !strings.Contains(err.Error(), "/no/such/ca.pem") {
		t.Errorf("the error must name the path it could not read, got %q", err)
	}
}

// The error names both spellings. An operator who set KAFKA_TLS_CA and is told
// to check KAFKA_SSL_CA_LOCATION goes looking for a variable they never set,
// while the certificate they are actually debugging stays unloaded.
func TestTLSErrorsNameBothSpellings(t *testing.T) {
	c := Config{
		Brokers: []string{"b:9093"}, SecurityProtocol: ProtocolSSL,
		CACertFile: "/no/such/ca.pem",
	}
	_, err := c.TLSConfig()
	if err == nil {
		t.Fatal("expected an error for an unreadable CA bundle")
	}
	for _, name := range []string{EnvSSLCALocation, EnvTLSCAAlias} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error = %q, want it to name %s", err, name)
		}
	}

	c = Config{
		Brokers: []string{"b:9093"}, SecurityProtocol: ProtocolSSL,
		ClientCertFile: "/no/such/client.crt", ClientKeyFile: "/no/such/client.key",
	}
	_, err = c.TLSConfig()
	if err == nil {
		t.Fatal("expected an error for an unreadable client keypair")
	}
	for _, name := range []string{EnvSSLCertLocation, EnvTLSCertAlias, EnvSSLKeyLocation, EnvTLSKeyAlias} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error = %q, want it to name %s", err, name)
		}
	}
}
