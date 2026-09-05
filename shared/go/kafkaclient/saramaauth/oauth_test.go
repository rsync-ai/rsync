package saramaauth

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/IBM/sarama"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

func mskConfig() kafkaclient.Config {
	return kafkaclient.Config{
		Brokers: []string{"b-1.msk.eu-west-1.amazonaws.com:9098"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
		SASLMechanism: kafkaclient.MechanismAWSMSKIAM, AWSRegion: "eu-west-1",
	}
}

func staticAWSCreds(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
}

// The mechanism sent on the wire is OAUTHBEARER for both token mechanisms. A
// broker offered the literal string "AWS_MSK_IAM" answers with
// UnsupportedSaslMechanism, so the constant that reaches sarama matters.
func TestApplyMSKIAMUsesOAuthBearer(t *testing.T) {
	cfg := sarama.NewConfig()
	if err := Apply(cfg, mskConfig()); err != nil {
		t.Fatal(err)
	}
	if !cfg.Net.SASL.Enable {
		t.Fatal("MSK IAM must enable SASL")
	}
	if cfg.Net.SASL.Mechanism != sarama.SASLTypeOAuth {
		t.Errorf("mechanism = %q, want %q", cfg.Net.SASL.Mechanism, sarama.SASLTypeOAuth)
	}
	// Setting SASLTypeOAuth without a provider compiles and then panics on the
	// first handshake, which is exactly the class of bug the SCRAM test above
	// exists for.
	if cfg.Net.SASL.TokenProvider == nil {
		t.Fatal("SASLTypeOAuth with a nil TokenProvider panics on the first handshake")
	}
	// TLS is mandatory for MSK IAM and Validate enforces it, but the config
	// still has to carry it.
	if !cfg.Net.TLS.Enable {
		t.Error("SASL_SSL must enable TLS")
	}
	// A username left over from a previous mechanism would be sent alongside
	// the token.
	if cfg.Net.SASL.User != "" || cfg.Net.SASL.Password != "" {
		t.Errorf("token auth must clear the static credentials, got user=%q", cfg.Net.SASL.User)
	}
	// sarama validates its own config on NewClient; a config it rejects fails
	// at connect time with an error that names none of this.
	if err := cfg.Validate(); err != nil {
		t.Fatalf("sarama rejected the config we built: %v", err)
	}
}

// Asserting the provider is non-nil does not prove it produces a token, which
// is the only thing the broker cares about.
func TestTokenProviderReturnsASignedToken(t *testing.T) {
	staticAWSCreds(t)
	cfg := sarama.NewConfig()
	if err := Apply(cfg, mskConfig()); err != nil {
		t.Fatal(err)
	}

	tok, err := cfg.Net.SASL.TokenProvider.Token()
	if err != nil {
		t.Fatalf("the provider could not mint a token: %v", err)
	}
	if tok == nil || tok.Token == "" {
		t.Fatal("the provider returned an empty token")
	}
	raw, err := base64.RawURLEncoding.DecodeString(tok.Token)
	if err != nil {
		t.Fatalf("the token is not the base64url the broker decodes: %v", err)
	}
	if !strings.Contains(string(raw), "kafka-cluster%3AConnect") && !strings.Contains(string(raw), "kafka-cluster:Connect") {
		t.Errorf("token is not a kafka-cluster:Connect presigned URL: %s", raw)
	}
}

// sarama calls Token() once per broker connection, so a provider that signed
// on every call would turn a reconnect storm into a signing storm against STS.
//
// The other half of this — re-signing before the ~15-minute token expires,
// which is what keeps a connection opened hours later from dying — is driven
// through an injected clock in tokenauth's own tests rather than by sleeping
// fourteen minutes here.
func TestTokenProviderReusesATokenAcrossConnections(t *testing.T) {
	staticAWSCreds(t)
	cfg := sarama.NewConfig()
	if err := Apply(cfg, mskConfig()); err != nil {
		t.Fatal(err)
	}
	p := cfg.Net.SASL.TokenProvider

	first, err := p.Token()
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.Token()
	if err != nil {
		t.Fatal(err)
	}
	if first.Token != second.Token {
		t.Error("a token still well inside its validity window was re-signed on every call")
	}
}

func TestApplyOAuthBearerUsesTheOIDCEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"access_token":"oidc-token","expires_in":3600}`))
	}))
	defer srv.Close()

	cfg := sarama.NewConfig()
	c := kafkaclient.Config{
		Brokers: []string{"b:9093"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
		SASLMechanism: kafkaclient.MechanismOAuthBearer, OAuthTokenEndpoint: srv.URL,
		OAuthClientID: "id", OAuthClientSecret: "secret",
		OAuthExtensions: map[string]string{"logicalCluster": "lkc-abc123"},
	}
	if err := Apply(cfg, c); err != nil {
		t.Fatal(err)
	}
	if cfg.Net.SASL.Mechanism != sarama.SASLTypeOAuth {
		t.Fatalf("mechanism = %q, want %q", cfg.Net.SASL.Mechanism, sarama.SASLTypeOAuth)
	}
	tok, err := cfg.Net.SASL.TokenProvider.Token()
	if err != nil {
		t.Fatal(err)
	}
	if tok.Token != "oidc-token" {
		t.Errorf("token = %q, want the one the endpoint issued", tok.Token)
	}
	// Confluent Cloud rejects the handshake without these, and sarama sends
	// exactly what the provider hands it.
	if tok.Extensions["logicalCluster"] != "lkc-abc123" {
		t.Errorf("extensions = %#v, want logicalCluster to reach sarama", tok.Extensions)
	}
}

// client.id is what a managed cluster attributes throttling and quotas to.
// Unset, every rsync connection was the library's anonymous default.
func TestApplySetsClientID(t *testing.T) {
	cfg := sarama.NewConfig()
	c := kafkaclient.Config{
		Brokers: []string{"b:9092"}, SecurityProtocol: kafkaclient.ProtocolPlaintext,
		ClientID: "rsync-orchestrator",
	}
	if err := Apply(cfg, c); err != nil {
		t.Fatal(err)
	}
	if cfg.ClientID != "rsync-orchestrator" {
		t.Errorf("ClientID = %q, want rsync-orchestrator", cfg.ClientID)
	}
	// sarama rejects a client.id outside [A-Za-z0-9._-] in Validate, so a bad
	// one fails here rather than at connect time.
	if err := cfg.Validate(); err != nil {
		t.Fatalf("sarama rejected the client id: %v", err)
	}
}

// A Config built as a struct literal has no client.id to give. Blanking
// sarama's own default would be a regression for every existing call site.
func TestApplyLeavesSaramaDefaultClientIDWhenUnset(t *testing.T) {
	cfg := sarama.NewConfig()
	before := cfg.ClientID
	c := kafkaclient.Config{Brokers: []string{"b:9092"}, SecurityProtocol: kafkaclient.ProtocolPlaintext}
	if err := Apply(cfg, c); err != nil {
		t.Fatal(err)
	}
	if cfg.ClientID != before {
		t.Errorf("ClientID = %q, want sarama's own default %q left in place", cfg.ClientID, before)
	}
	if cfg.ClientID == "" {
		t.Error("an empty client.id is rejected by sarama's Validate")
	}
}

// Validate runs before anything is stamped onto the sarama config, so a
// misconfigured deployment fails at startup rather than at the first handshake.
func TestApplyRejectsMSKIAMWithoutTLS(t *testing.T) {
	cfg := sarama.NewConfig()
	c := mskConfig()
	c.SecurityProtocol = kafkaclient.ProtocolSASLPlaintext
	err := Apply(cfg, c)
	if err == nil {
		t.Fatal("MSK IAM over SASL_PLAINTEXT must be rejected")
	}
	if cfg.Net.SASL.Enable {
		t.Error("a rejected config must leave the sarama config untouched")
	}
}
