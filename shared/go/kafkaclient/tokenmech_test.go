package kafkaclient

import (
	"strings"
	"testing"
)

// The blocker this whole branch exists for: an MSK cluster in IAM auth mode —
// the AWS default and recommendation — was unreachable, because the only way to
// configure it was rejected by name.
func TestMSKIAMIsAccepted(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv(EnvBootstrapServers, "b-1.msk.eu-west-1.amazonaws.com:9098,b-2.msk.eu-west-1.amazonaws.com:9098")
	t.Setenv(EnvSecurityProtocol, ProtocolSASLSSL)
	t.Setenv(EnvSASLMechanism, "aws_msk_iam") // lowercase: operators write it both ways
	t.Setenv(EnvAWSRegion, "eu-west-1")

	c, err := FromEnv("kafka:29092")
	if err != nil {
		t.Fatalf("AWS_MSK_IAM must be a supported mechanism now: %v", err)
	}
	if c.SASLMechanism != MechanismAWSMSKIAM {
		t.Errorf("mechanism = %q, want %q", c.SASLMechanism, MechanismAWSMSKIAM)
	}
	if !c.UsesTokenAuth() {
		t.Error("AWS_MSK_IAM must report as token auth so the client libraries take the OAUTHBEARER path")
	}
	if c.AWSRegion != "eu-west-1" {
		t.Errorf("AWSRegion = %q, want eu-west-1", c.AWSRegion)
	}
	// No username or password is set, and that is the entire point: an IRSA or
	// pod-identity role supplies the credential. Requiring them would make the
	// mechanism unusable in the deployment it exists for.
	if c.Username != "" || c.Password != "" {
		t.Error("MSK IAM must not need a static username/password")
	}
}

// MSK IAM sends a presigned URL that is a bearer credential for the cluster.
// Allowing SASL_PLAINTEXT would put it on the wire in the clear and the
// connection would still come up, so the operator would never learn.
func TestMSKIAMRequiresTLS(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv(EnvBootstrapServers, "b:9098")
	t.Setenv(EnvSecurityProtocol, ProtocolSASLPlaintext)
	t.Setenv(EnvSASLMechanism, MechanismAWSMSKIAM)
	t.Setenv(EnvAWSRegion, "us-east-1")

	_, err := FromEnv("kafka:29092")
	if err == nil {
		t.Fatal("MSK IAM over SASL_PLAINTEXT must be an error, not a silent downgrade")
	}
	if !strings.Contains(err.Error(), ProtocolSASLSSL) {
		t.Errorf("the error must name the protocol the operator has to set, got %q", err)
	}
}

// The region reaches the signer, so an unset one produces a token signed for
// the wrong cluster or no token at all. Naming all three accepted variables in
// the error is what keeps this a 30-second fix.
func TestMSKIAMRegionFallbackChain(t *testing.T) {
	for _, tc := range []struct {
		name, env, want string
	}{
		{"kafka-specific wins", EnvAWSRegion, "eu-central-1"},
		{"AWS_REGION is honored", EnvAWSRegionFallback, "us-west-2"},
		{"AWS_DEFAULT_REGION is honored", EnvAWSRegionFallbackLegacy, "ap-south-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearKafkaEnv(t)
			t.Setenv(EnvBootstrapServers, "b:9098")
			t.Setenv(EnvSecurityProtocol, ProtocolSASLSSL)
			t.Setenv(EnvSASLMechanism, MechanismAWSMSKIAM)
			t.Setenv(tc.env, tc.want)

			c, err := FromEnv("kafka:29092")
			if err != nil {
				t.Fatalf("%s=%s should be enough to configure MSK IAM: %v", tc.env, tc.want, err)
			}
			if c.AWSRegion != tc.want {
				t.Errorf("AWSRegion = %q, want %q", c.AWSRegion, tc.want)
			}
		})
	}

	// Precedence: the Kafka-specific name must win, so a pod whose AWS_REGION
	// points at its own region can still reach an MSK cluster in another.
	clearKafkaEnv(t)
	t.Setenv(EnvBootstrapServers, "b:9098")
	t.Setenv(EnvSecurityProtocol, ProtocolSASLSSL)
	t.Setenv(EnvSASLMechanism, MechanismAWSMSKIAM)
	t.Setenv(EnvAWSRegionFallback, "us-east-1")
	t.Setenv(EnvAWSRegionFallbackLegacy, "us-east-2")
	t.Setenv(EnvAWSRegion, "eu-west-1")
	c, err := FromEnv("kafka:29092")
	if err != nil {
		t.Fatal(err)
	}
	if c.AWSRegion != "eu-west-1" {
		t.Errorf("AWSRegion = %q, want the Kafka-specific eu-west-1 to win", c.AWSRegion)
	}
}

func TestMSKIAMWithoutRegionIsAnError(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv(EnvBootstrapServers, "b:9098")
	t.Setenv(EnvSecurityProtocol, ProtocolSASLSSL)
	t.Setenv(EnvSASLMechanism, MechanismAWSMSKIAM)

	_, err := FromEnv("kafka:29092")
	if err == nil {
		t.Fatal("MSK IAM without a region must be rejected before the first connection")
	}
	for _, name := range []string{EnvAWSRegion, EnvAWSRegionFallback, EnvAWSRegionFallbackLegacy} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("the error should name %s as a way to fix it, got %q", name, err)
		}
	}
}

func TestOAuthBearerIsAccepted(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv(EnvBootstrapServers, "b:9093")
	t.Setenv(EnvSecurityProtocol, ProtocolSASLSSL)
	t.Setenv(EnvSASLMechanism, "oauthbearer")
	t.Setenv(EnvOAuthTokenEndpoint, "https://idp.example.com/oauth2/token")
	t.Setenv(EnvOAuthClientID, "rsync-kafka")
	t.Setenv(EnvOAuthClientSecret, "sh-do-not-log")
	t.Setenv(EnvOAuthScope, "kafka:write")

	c, err := FromEnv("kafka:29092")
	if err != nil {
		t.Fatalf("OAUTHBEARER must be a supported mechanism now: %v", err)
	}
	if c.SASLMechanism != MechanismOAuthBearer || !c.UsesTokenAuth() {
		t.Errorf("OAUTHBEARER must report as token auth, got %s", c)
	}
	if c.OAuthClientID != "rsync-kafka" || c.OAuthClientSecret != "sh-do-not-log" || c.OAuthScope != "kafka:write" {
		t.Errorf("OIDC settings were not carried onto the config: %s", c)
	}
}

// One obvious path: an operator who already set KAFKA_SASL_USERNAME/PASSWORD
// for PLAIN and switches the mechanism should not have to discover a second
// pair of variables spelling the same thing.
func TestOAuthBearerFallsBackToSASLCredentials(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv(EnvBootstrapServers, "b:9093")
	t.Setenv(EnvSecurityProtocol, ProtocolSASLSSL)
	t.Setenv(EnvSASLMechanism, MechanismOAuthBearer)
	t.Setenv(EnvOAuthTokenEndpoint, "https://idp.example.com/oauth2/token")
	t.Setenv(EnvSASLUsername, "client-from-sasl-vars")
	t.Setenv(EnvSASLPassword, "secret-from-sasl-vars")

	c, err := FromEnv("kafka:29092")
	if err != nil {
		t.Fatalf("the SASL username/password must serve as the client id/secret: %v", err)
	}
	if c.OAuthClientID != "client-from-sasl-vars" || c.OAuthClientSecret != "secret-from-sasl-vars" {
		t.Errorf("fallback did not apply: clientID=%q", c.OAuthClientID)
	}

	// The dedicated names still win where both are set.
	t.Setenv(EnvOAuthClientID, "dedicated")
	c, err = FromEnv("kafka:29092")
	if err != nil {
		t.Fatal(err)
	}
	if c.OAuthClientID != "dedicated" {
		t.Errorf("OAuthClientID = %q, want the dedicated variable to win", c.OAuthClientID)
	}
}

// Each of these surfaces at handshake time as an opaque SASL failure that names
// no variable, so they are worth catching at construction.
func TestOAuthBearerRejectsIncompleteConfig(t *testing.T) {
	base := func() Config {
		return Config{
			Brokers: []string{"b:9093"}, SecurityProtocol: ProtocolSASLSSL,
			SASLMechanism:      MechanismOAuthBearer,
			OAuthTokenEndpoint: "https://idp.example.com/oauth2/token",
			OAuthClientID:      "id", OAuthClientSecret: "secret",
		}
	}
	for _, tc := range []struct {
		name string
		mut  func(*Config)
		want string
	}{
		{"no endpoint", func(c *Config) { c.OAuthTokenEndpoint = "" }, EnvOAuthTokenEndpoint},
		{"endpoint is not a URL", func(c *Config) { c.OAuthTokenEndpoint = "idp.example.com/token" }, "absolute URL"},
		{"no client id", func(c *Config) { c.OAuthClientID = "" }, "client id and secret"},
		{"no client secret", func(c *Config) { c.OAuthClientSecret = "" }, "client id and secret"},
		{"reserved extension", func(c *Config) { c.OAuthExtensions = map[string]string{"auth": "x"} }, "reserved"},
		{"extension carries the separator", func(c *Config) {
			c.OAuthExtensions = map[string]string{"cluster": "lkc\x01evil"}
		}, "separator"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := base()
			tc.mut(&c)
			err := c.Validate()
			if err == nil {
				t.Fatalf("Validate() = nil, want an error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Validate() = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}

// A bearer token on an unencrypted connection is replayable by anyone on the
// path. Unlike MSK IAM this is not fatal — a private network with a
// non-TLS-terminating broker is a real deployment — but it must be said.
func TestOAuthBearerOverPlaintextWarns(t *testing.T) {
	c := Config{
		Brokers: []string{"b:9092"}, SecurityProtocol: ProtocolSASLPlaintext,
		SASLMechanism:      MechanismOAuthBearer,
		OAuthTokenEndpoint: "http://idp.internal/token",
		OAuthClientID:      "id", OAuthClientSecret: "secret",
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("OAUTHBEARER over SASL_PLAINTEXT must stay legal: %v", err)
	}
	w := strings.Join(c.Warnings(), " | ")
	if !strings.Contains(w, "replay") {
		t.Errorf("expected a warning about the token being capturable, got %v", c.Warnings())
	}
	if !strings.Contains(w, "clear") {
		t.Errorf("expected a warning about the http token endpoint, got %v", c.Warnings())
	}
}

// The same credential-leak guarantee the password already had. A client secret
// in a log line is a client secret in an LLM prompt in this codebase.
func TestStringRedactsOAuthClientSecret(t *testing.T) {
	c := Config{
		Brokers: []string{"b:9093"}, SecurityProtocol: ProtocolSASLSSL,
		SASLMechanism:      MechanismOAuthBearer,
		OAuthTokenEndpoint: "https://idp.example.com/oauth2/token",
		OAuthClientID:      "rsync-kafka", OAuthClientSecret: "oauth-s3cr3t-do-not-log",
	}
	for _, rendered := range []string{c.String(), sprintfV(c), sprintfPlusV(c)} {
		if strings.Contains(rendered, "oauth-s3cr3t-do-not-log") {
			t.Fatalf("client secret leaked into %q", rendered)
		}
		if !strings.Contains(rendered, "REDACTED") {
			t.Errorf("expected REDACTED marker in %q", rendered)
		}
	}
}

func TestParseSASLExtensions(t *testing.T) {
	got, err := ParseSASLExtensions(" logicalCluster=lkc-abc123 , identityPoolId=pool-1 ")
	if err != nil {
		t.Fatal(err)
	}
	if got["logicalCluster"] != "lkc-abc123" || got["identityPoolId"] != "pool-1" {
		t.Errorf("parsed = %#v", got)
	}

	if got, err := ParseSASLExtensions("   "); err != nil || got != nil {
		t.Errorf("an empty value must yield (nil, nil), got (%#v, %v)", got, err)
	}
	if _, err := ParseSASLExtensions("justakey"); err == nil {
		t.Error("a pair without '=' must be an error rather than an ignored setting")
	}
	if _, err := ParseSASLExtensions("auth=Bearer x"); err == nil {
		t.Error("the reserved 'auth' extension must be rejected; the broker cannot parse two of them")
	}
}

func TestPlainAndSCRAMStillRequireCredentials(t *testing.T) {
	// The token mechanisms had to be carved out of the credential check, so
	// prove the carve-out did not widen: PLAIN and SCRAM must still fail loudly
	// rather than dialing anonymously.
	for _, mech := range []string{MechanismPlain, MechanismSCRAMSHA256, MechanismSCRAMSHA512} {
		c := Config{Brokers: []string{"b:9093"}, SecurityProtocol: ProtocolSASLSSL, SASLMechanism: mech}
		if err := c.Validate(); err == nil {
			t.Errorf("%s without credentials must be an error", mech)
		}
	}
}
