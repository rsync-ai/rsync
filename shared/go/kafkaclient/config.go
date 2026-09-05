// Package kafkaclient is the single source of truth for how an rsync-ai
// service reaches Kafka.
//
// It exists because the platform grew up against the broker it ships: one
// address, no authentication, plaintext. Every one of those assumptions breaks
// at once when the deployment points at a customer-managed cluster (MSK,
// Confluent Cloud, Aiven, or a self-run cluster), and the two Go client
// libraries in this repo express the fix in completely different shapes. The
// configuration is decided here; the saramaauth and kgoauth subpackages only
// translate it.
//
// Two failure modes motivate the details:
//
//   - A managed cluster hands you a MULTI-broker bootstrap string. Code that
//     wraps that string as []string{s} turns "b1:9093,b2:9093" into one bogus
//     hostname. ParseBrokers is the fix, and it is the reason this package
//     exists at all rather than a per-service tweak.
//   - A default that silently points at the bundled broker turns a missing
//     env var into a connection to the wrong cluster instead of an error.
//     Callers therefore pass their default explicitly and can check
//     UsingDefaultBrokers to decide whether that is acceptable.
//
// This package deliberately has no Kafka dependency: importing it must never
// pull a client library into a service that does not already use one.
package kafkaclient

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

// Security protocols, spelled the way Kafka spells them.
const (
	ProtocolPlaintext     = "PLAINTEXT"
	ProtocolSSL           = "SSL"
	ProtocolSASLPlaintext = "SASL_PLAINTEXT"
	ProtocolSASLSSL       = "SASL_SSL"
)

// SASL mechanisms this package can configure.
//
// PLAIN and SCRAM carry a static username/password. The other two carry a
// bearer token minted per connection instead, which is why they need a
// credential provider rather than two env vars:
//
//   - AWS_MSK_IAM signs the token from the ambient AWS credential chain. That
//     is the whole point of it on EKS: an IRSA or pod-identity role satisfies
//     an MSK cluster in IAM auth mode — the AWS default — with no static secret
//     stored anywhere. On the wire it is SASL/OAUTHBEARER; only the token
//     source is AWS-specific.
//   - OAUTHBEARER fetches the token from an OIDC provider by client-credentials
//     grant.
//
// Both are implemented by the tokenauth subpackage and applied by saramaauth
// and kgoauth.
const (
	MechanismPlain       = "PLAIN"
	MechanismSCRAMSHA256 = "SCRAM-SHA-256"
	MechanismSCRAMSHA512 = "SCRAM-SHA-512"
	MechanismAWSMSKIAM   = "AWS_MSK_IAM"
	MechanismOAuthBearer = "OAUTHBEARER"
)

// Env var names. KAFKA_SECURITY_PROTOCOL is reused rather than renamed: it was
// already being read into a struct field in the orchestrator's consumer config
// and then never applied to a client. This package is what finally gives that
// variable an effect.
const (
	EnvBrokers          = "KAFKA_BROKERS"
	EnvBootstrapServers = "KAFKA_BOOTSTRAP_SERVERS"
	EnvSecurityProtocol = "KAFKA_SECURITY_PROTOCOL"
	EnvSASLMechanism    = "KAFKA_SASL_MECHANISM"
	EnvSASLUsername     = "KAFKA_SASL_USERNAME"
	EnvSASLPassword     = "KAFKA_SASL_PASSWORD"
	EnvSSLCALocation    = "KAFKA_SSL_CA_LOCATION"
	EnvSSLCertLocation  = "KAFKA_SSL_CERT_LOCATION"
	EnvSSLKeyLocation   = "KAFKA_SSL_KEY_LOCATION"
	EnvSSLSkipVerify    = "KAFKA_SSL_INSECURE_SKIP_VERIFY"
	EnvClientID         = "KAFKA_CLIENT_ID"
	EnvAWSRegion        = "KAFKA_AWS_REGION"
)

// OIDC client-credentials settings for KAFKA_SASL_MECHANISM=OAUTHBEARER.
//
// EnvSASLUsername/EnvSASLPassword are accepted as the client id and secret when
// the dedicated names are unset, so an operator configuring any SASL mechanism
// has one pair of variables to reach for rather than two spellings of the same
// idea.
const (
	EnvOAuthTokenEndpoint = "KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT"
	EnvOAuthClientID      = "KAFKA_SASL_OAUTHBEARER_CLIENT_ID"
	EnvOAuthClientSecret  = "KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET"
	EnvOAuthScope         = "KAFKA_SASL_OAUTHBEARER_SCOPE"
	EnvOAuthExtensions    = "KAFKA_SASL_OAUTHBEARER_EXTENSIONS"
)

// Accepted spellings that are not the canonical one.
//
// The TLS aliases exist because the platform shipped two names for one setting:
// the Go services read KAFKA_SSL_INSECURE_SKIP_VERIFY while the Python services
// read KAFKA_SSL_SKIP_VERIFY, which is also the name the documentation gives.
// An operator following the doc therefore turned verification off on half the
// platform and left it on for the other half, with no error on either side —
// the exact shape of failure that is impossible to diagnose from a log. Both
// names are honored here so the two halves cannot disagree again. Likewise
// KAFKA_TLS_CA/CERT/KEY: a CA bundle that is silently not loaded produces a
// handshake failure that names no file.
//
// The AWS region falls back to the SDK's own variables so a pod that already
// has AWS_REGION set — which every EKS pod running the AWS SDK does — needs no
// Kafka-specific duplicate of it.
const (
	EnvSSLSkipVerifyAlias = "KAFKA_SSL_SKIP_VERIFY"
	EnvTLSCAAlias         = "KAFKA_TLS_CA"
	EnvTLSCertAlias       = "KAFKA_TLS_CERT"
	EnvTLSKeyAlias        = "KAFKA_TLS_KEY"

	EnvAWSRegionFallback       = "AWS_REGION"
	EnvAWSRegionFallbackLegacy = "AWS_DEFAULT_REGION"
)

// Config describes one Kafka endpoint and how to authenticate to it.
type Config struct {
	// Brokers is always a list, even for a single address.
	Brokers []string

	SecurityProtocol string // one of the Protocol* constants; empty means PLAINTEXT
	SASLMechanism    string // one of the Mechanism* constants
	Username         string
	Password         string

	// ClientID is the client.id every connection announces itself with. It is
	// not cosmetic on a managed cluster: broker throttling, quota assignment
	// and the request logs all key off it, so an empty one means neither side
	// can tell which rsync service is responsible for load, and a per-client
	// quota cannot be scoped to this product at all.
	ClientID string

	// AWSRegion is the MSK cluster's region, used to sign AWS_MSK_IAM tokens.
	AWSRegion string

	// OIDC client-credentials settings for OAUTHBEARER.
	OAuthTokenEndpoint string
	OAuthClientID      string
	OAuthClientSecret  string
	OAuthScope         string
	OAuthExtensions    map[string]string

	CACertFile         string // PEM bundle used to verify the broker
	ClientCertFile     string // mTLS client certificate
	ClientKeyFile      string // mTLS client key
	InsecureSkipVerify bool

	// InsecureSkipVerifySource names the environment variable that turned
	// InsecureSkipVerify on. With two accepted spellings, a warning that does
	// not say which one is set sends the operator looking in the wrong place.
	InsecureSkipVerifySource string

	// UsingDefaultBrokers reports that no broker env var was set and the
	// caller-supplied default was used. Callers deploying against a customer
	// cluster should treat this as a misconfiguration.
	UsingDefaultBrokers bool

	// clientIDFromEnv records that KAFKA_CLIENT_ID was set explicitly, so
	// WithServiceName cannot overwrite an operator's deliberate choice.
	clientIDFromEnv bool
}

// ParseBrokers splits a bootstrap string into the list every Kafka client
// actually wants. Empty entries are dropped so a trailing comma cannot produce
// a blank broker address that fails at dial time with a confusing error.
func ParseBrokers(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// FromEnv builds a Config from the environment.
//
// defaultBrokers is required rather than assumed: services historically
// defaulted to different addresses ("kafka:29092" vs "localhost:9092"), and
// silently picking one here would move a deployment onto the wrong cluster.
//
// KAFKA_BROKERS wins over KAFKA_BOOTSTRAP_SERVERS, preserving the precedence
// the temporal-adapter already documented for back-compat.
func FromEnv(defaultBrokers string) (Config, error) {
	return FromEnvForService("", defaultBrokers)
}

// FromEnvForService is FromEnv for a caller that knows what it is called.
//
// service names the process ("orchestrator", "api-gateway", "kafka-sink") and
// becomes the default client.id, so the customer's broker logs and quota
// metrics can attribute load to a specific rsync service rather than to an
// anonymous default client. KAFKA_CLIENT_ID still overrides it, for a
// deployment that wants one identity for the whole platform.
func FromEnvForService(service, defaultBrokers string) (Config, error) {
	raw := strings.TrimSpace(os.Getenv(EnvBrokers))
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv(EnvBootstrapServers))
	}
	usingDefault := raw == ""
	if usingDefault {
		raw = defaultBrokers
	}

	skip, skipVar := truthyEnv(EnvSSLSkipVerify, EnvSSLSkipVerifyAlias)
	clientID, clientIDSet := lookupEnv(EnvClientID)

	c := Config{
		Brokers:                  ParseBrokers(raw),
		SecurityProtocol:         strings.ToUpper(strings.TrimSpace(os.Getenv(EnvSecurityProtocol))),
		SASLMechanism:            strings.ToUpper(strings.TrimSpace(os.Getenv(EnvSASLMechanism))),
		Username:                 os.Getenv(EnvSASLUsername),
		Password:                 os.Getenv(EnvSASLPassword),
		ClientID:                 sanitizeClientIDChars(clientID),
		AWSRegion:                firstEnv(EnvAWSRegion, EnvAWSRegionFallback, EnvAWSRegionFallbackLegacy),
		OAuthTokenEndpoint:       firstEnv(EnvOAuthTokenEndpoint),
		OAuthClientID:            firstEnv(EnvOAuthClientID, EnvSASLUsername),
		OAuthClientSecret:        firstEnv(EnvOAuthClientSecret, EnvSASLPassword),
		OAuthScope:               firstEnv(EnvOAuthScope),
		CACertFile:               firstEnv(EnvSSLCALocation, EnvTLSCAAlias),
		ClientCertFile:           firstEnv(EnvSSLCertLocation, EnvTLSCertAlias),
		ClientKeyFile:            firstEnv(EnvSSLKeyLocation, EnvTLSKeyAlias),
		InsecureSkipVerify:       skip,
		InsecureSkipVerifySource: skipVar,
		UsingDefaultBrokers:      usingDefault,
		clientIDFromEnv:          clientIDSet,
	}
	if c.ClientID == "" {
		c.ClientID = DefaultClientID(service)
		c.clientIDFromEnv = false
	}
	protocolWasSet := c.SecurityProtocol != ""
	if c.SecurityProtocol == "" {
		c.SecurityProtocol = ProtocolPlaintext
	}
	// A mechanism is meaningless without a SASL protocol, but a SASL protocol
	// without a mechanism is a common operator slip — default it to PLAIN only
	// when SASL was explicitly requested, so the error surface stays small.
	if c.UsesSASL() && c.SASLMechanism == "" {
		c.SASLMechanism = MechanismPlain
	}

	ext, err := ParseSASLExtensions(os.Getenv(EnvOAuthExtensions))
	if err != nil {
		return Config{}, fmt.Errorf("kafkaclient: %s: %w", EnvOAuthExtensions, err)
	}
	c.OAuthExtensions = ext

	// Warnings() only reaches the operator if the caller remembers to log it,
	// and turning off certificate verification is the one setting that must not
	// depend on that. Say it here too, unconditionally, so the choice cannot be
	// made quietly.
	if c.InsecureSkipVerify {
		warnOnce(c.InsecureSkipVerifySource,
			"kafkaclient: %s is enabled — Kafka broker TLS certificates are NOT verified. "+
				"Any host that can intercept the connection can impersonate the broker and read every record, "+
				"including the credentials this client presents. Unset it outside of local testing.",
			c.InsecureSkipVerifySource)
	}

	// Credentials that will never be used are the same class of problem, and
	// are reported here for the same reason. An operator who set a username and
	// a password believes this client authenticates; if the protocol says
	// otherwise it does not, and nothing on the wire says so. Warnings() also
	// reports it, but four of the seven callers in this platform never log
	// Warnings() -- so on those paths the only signal was silence.
	//
	// The most common way to reach this is to set the credentials and forget
	// KAFKA_SECURITY_PROTOCOL entirely, which defaults to PLAINTEXT a few lines
	// above. That is why the message distinguishes "unset" from an explicit
	// choice: the operator who never set it is not making a decision, and needs
	// to be told a decision was made for them.
	if ignored := c.ignoredSASLSettings(); len(ignored) > 0 {
		protocol := fmt.Sprintf("%s=%s", EnvSecurityProtocol, c.SecurityProtocol)
		if !protocolWasSet {
			protocol = fmt.Sprintf("%s is unset, defaulting to %s", EnvSecurityProtocol, ProtocolPlaintext)
		}
		exposure := ""
		if !c.UsesTLS() {
			exposure = " and unencrypted"
		}
		warnOnce("sasl-ignored",
			"kafkaclient: %s, which does not authenticate, but SASL settings are configured (%s) -- "+
				"they are IGNORED and this connection is anonymous%s. A broker that enforces ACLs will "+
				"reject it with an error naming the broker rather than this setting; a broker that does "+
				"not will accept it, and the credentials will never have been checked. "+
				"Set %s=%s, or %s if the broker's SASL listener is unencrypted.",
			protocol, strings.Join(ignored, ", "), exposure,
			EnvSecurityProtocol, ProtocolSASLSSL, ProtocolSASLPlaintext)
	}
	return c, c.Validate()
}

func isTruthy(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// lookupEnv is os.LookupEnv with the trimming every reader here wants. An
// empty-but-set variable reports as unset: in a compose file or a Kubernetes
// manifest that is how an operator spells "leave this alone".
func lookupEnv(name string) (string, bool) {
	v := strings.TrimSpace(os.Getenv(name))
	return v, v != ""
}

// firstEnv returns the value of the first of names that is set and non-empty.
// It is what makes an alias an alias rather than a second setting.
func firstEnv(names ...string) string {
	for _, n := range names {
		if v, ok := lookupEnv(n); ok {
			return v
		}
	}
	return ""
}

// truthyEnv reports whether any of names is set to a truthy value, and which
// one it was.
func truthyEnv(names ...string) (bool, string) {
	for _, n := range names {
		if isTruthy(os.Getenv(n)) {
			return true, n
		}
	}
	return false, ""
}

// UsesTLS reports whether the connection must be wrapped in TLS.
func (c Config) UsesTLS() bool {
	return c.SecurityProtocol == ProtocolSSL || c.SecurityProtocol == ProtocolSASLSSL
}

// UsesSASL reports whether the connection must authenticate.
func (c Config) UsesSASL() bool {
	return c.SecurityProtocol == ProtocolSASLPlaintext || c.SecurityProtocol == ProtocolSASLSSL
}

// ignoredSASLSettings names the environment variables that were set in order to
// authenticate this client but cannot, because the resolved security protocol
// does not speak SASL. Empty when the protocol does, so a caller can treat a
// non-empty result as "the operator supplied credentials we are about to
// discard".
//
// KAFKA_AWS_REGION is deliberately not in this list. It falls back to
// AWS_REGION, which is set for unrelated reasons in every AWS environment, so
// counting it would warn about every plaintext in-VPC broker on EC2.
func (c Config) ignoredSASLSettings() []string {
	if c.UsesSASL() {
		return nil
	}
	var named []string
	if c.SASLMechanism != "" {
		named = append(named, EnvSASLMechanism)
	}
	if c.Username != "" {
		named = append(named, EnvSASLUsername)
	}
	if c.Password != "" {
		named = append(named, EnvSASLPassword)
	}
	if c.OAuthTokenEndpoint != "" {
		named = append(named, EnvOAuthTokenEndpoint)
	}
	// The OAuth client id and secret fall back to the SASL username and
	// password, so name them only when they carry their own value -- otherwise
	// a plain username-and-password slip would be reported as an OAuth one too.
	if c.OAuthClientID != "" && c.OAuthClientID != c.Username {
		named = append(named, EnvOAuthClientID)
	}
	if c.OAuthClientSecret != "" && c.OAuthClientSecret != c.Password {
		named = append(named, EnvOAuthClientSecret)
	}
	return named
}

// UsesTokenAuth reports whether authentication needs a bearer token minted per
// connection rather than a static username and password. Both such mechanisms
// speak SASL/OAUTHBEARER on the wire; they differ only in where the token comes
// from, which is why the client libraries branch on this rather than on the
// mechanism name.
func (c Config) UsesTokenAuth() bool {
	if !c.UsesSASL() {
		return false
	}
	return c.SASLMechanism == MechanismAWSMSKIAM || c.SASLMechanism == MechanismOAuthBearer
}

// Validate rejects configurations that would fail at connect time or, worse,
// succeed without the protection the operator asked for.
func (c Config) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("kafkaclient: no brokers configured (set %s)", EnvBootstrapServers)
	}
	for _, b := range c.Brokers {
		if strings.ContainsAny(b, " \t") {
			return fmt.Errorf("kafkaclient: broker %q contains whitespace; separate addresses with commas", b)
		}
	}

	switch c.SecurityProtocol {
	case ProtocolPlaintext, ProtocolSSL, ProtocolSASLPlaintext, ProtocolSASLSSL:
	default:
		return fmt.Errorf("kafkaclient: unsupported %s=%q (want one of %s, %s, %s, %s)",
			EnvSecurityProtocol, c.SecurityProtocol,
			ProtocolPlaintext, ProtocolSSL, ProtocolSASLPlaintext, ProtocolSASLSSL)
	}

	if c.UsesSASL() {
		switch c.SASLMechanism {
		case MechanismPlain, MechanismSCRAMSHA256, MechanismSCRAMSHA512:
			if c.Username == "" || c.Password == "" {
				return fmt.Errorf("kafkaclient: %s requires both %s and %s",
					c.SecurityProtocol, EnvSASLUsername, EnvSASLPassword)
			}
		case MechanismAWSMSKIAM:
			if err := c.validateMSKIAM(); err != nil {
				return err
			}
		case MechanismOAuthBearer:
			if err := c.validateOAuthBearer(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("kafkaclient: unsupported %s=%q (want %s, %s, %s, %s or %s)",
				EnvSASLMechanism, c.SASLMechanism,
				MechanismPlain, MechanismSCRAMSHA256, MechanismSCRAMSHA512,
				MechanismAWSMSKIAM, MechanismOAuthBearer)
		}
	}

	// A half-configured client certificate authenticates as nobody and fails
	// with a TLS handshake error that names neither file.
	if (c.ClientCertFile == "") != (c.ClientKeyFile == "") {
		return fmt.Errorf("kafkaclient: %s and %s must be set together",
			EnvSSLCertLocation, EnvSSLKeyLocation)
	}
	return nil
}

// validateMSKIAM enforces the two things an MSK IAM connection cannot work
// without, and one it must not be allowed to work without.
func (c Config) validateMSKIAM() error {
	// The MSK IAM token is a presigned URL: whoever holds it can connect as
	// this identity until it expires. On SASL_PLAINTEXT it crosses the network
	// in the clear, so AWS offers the mechanism only over TLS. Accepting the
	// downgrade here would hand an eavesdropper a working credential, and the
	// connection would come up — which is why this is an error rather than a
	// warning.
	if c.SecurityProtocol != ProtocolSASLSSL {
		return fmt.Errorf("kafkaclient: %s=%s requires %s=%s, got %s: the IAM auth token is a bearer credential and must not travel unencrypted",
			EnvSASLMechanism, MechanismAWSMSKIAM, EnvSecurityProtocol, ProtocolSASLSSL, c.SecurityProtocol)
	}
	if c.AWSRegion == "" {
		return fmt.Errorf("kafkaclient: %s=%s needs the cluster's AWS region (set %s, %s or %s)",
			EnvSASLMechanism, MechanismAWSMSKIAM, EnvAWSRegion, EnvAWSRegionFallback, EnvAWSRegionFallbackLegacy)
	}
	return nil
}

// validateOAuthBearer checks the OIDC settings before the first connection
// rather than at handshake time, where the failure arrives as an opaque SASL
// authentication error with no mention of which variable is missing.
func (c Config) validateOAuthBearer() error {
	if c.OAuthTokenEndpoint == "" {
		return fmt.Errorf("kafkaclient: %s=%s requires %s",
			EnvSASLMechanism, MechanismOAuthBearer, EnvOAuthTokenEndpoint)
	}
	u, err := url.Parse(c.OAuthTokenEndpoint)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("kafkaclient: %s=%q is not an absolute URL (want https://issuer/oauth2/token)",
			EnvOAuthTokenEndpoint, c.OAuthTokenEndpoint)
	}
	if c.OAuthClientID == "" || c.OAuthClientSecret == "" {
		return fmt.Errorf("kafkaclient: %s=%s requires a client id and secret (set %s and %s, or reuse %s and %s)",
			EnvSASLMechanism, MechanismOAuthBearer,
			EnvOAuthClientID, EnvOAuthClientSecret, EnvSASLUsername, EnvSASLPassword)
	}
	return validateSASLExtensions(c.OAuthExtensions)
}

// Warnings lists configurations that work but are probably not what the
// operator intended. Callers should log these; none of them is fatal.
func (c Config) Warnings() []string {
	var w []string
	if c.InsecureSkipVerify {
		w = append(w, fmt.Sprintf("%s is enabled: broker certificates are NOT verified, which defeats TLS against an active attacker",
			orDefault(c.InsecureSkipVerifySource, EnvSSLSkipVerify)))
	}
	if c.UsesTokenAuth() && !c.UsesTLS() {
		w = append(w, fmt.Sprintf("%s=%s sends a bearer token but %s=%s is unencrypted, so anyone on the path can capture and replay it",
			EnvSASLMechanism, c.SASLMechanism, EnvSecurityProtocol, c.SecurityProtocol))
	}
	if c.SASLMechanism == MechanismOAuthBearer && c.OAuthTokenEndpoint != "" &&
		!strings.HasPrefix(strings.ToLower(c.OAuthTokenEndpoint), "https://") {
		w = append(w, fmt.Sprintf("%s is not https, so the client secret is sent in the clear", EnvOAuthTokenEndpoint))
	}
	if ignored := c.ignoredSASLSettings(); len(ignored) > 0 {
		w = append(w, fmt.Sprintf("%s are set but %s=%s does not use SASL, so the credentials are ignored",
			strings.Join(ignored, "/"), EnvSecurityProtocol, c.SecurityProtocol))
	}
	if !c.UsesTLS() && (c.CACertFile != "" || c.ClientCertFile != "") {
		w = append(w, fmt.Sprintf("TLS material is configured but %s=%s is not a TLS protocol, so it is ignored",
			EnvSecurityProtocol, c.SecurityProtocol))
	}
	if c.UsingDefaultBrokers {
		w = append(w, fmt.Sprintf("neither %s nor %s was set; falling back to the built-in default %v",
			EnvBrokers, EnvBootstrapServers, c.Brokers))
	}
	return w
}

// String renders the config for logs with the password removed. It is a
// Stringer specifically so that an accidental %v or %+v on a Config cannot
// leak a broker credential into a log line — which, in this codebase, is also
// a path into an LLM prompt.
func (c Config) String() string {
	return fmt.Sprintf("kafka{brokers=%v protocol=%s mechanism=%s clientID=%q user=%q password=%s ca=%q clientCert=%q skipVerify=%t region=%s oauthEndpoint=%q oauthClientID=%q oauthSecret=%s}",
		c.Brokers, c.SecurityProtocol, orNone(c.SASLMechanism), c.ClientID, c.Username, redacted(c.Password),
		c.CACertFile, c.ClientCertFile, c.InsecureSkipVerify, orNone(c.AWSRegion),
		c.OAuthTokenEndpoint, c.OAuthClientID, redacted(c.OAuthClientSecret))
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

func orDefault(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

func redacted(secret string) string {
	if secret == "" {
		return "unset"
	}
	return "REDACTED"
}

// WithBrokers overrides the broker list while keeping the security settings.
//
// Call sites that already plumb a bootstrap string through their own config
// struct use this so that adopting kafkaclient changes only HOW a service
// connects, never WHERE: whatever precedence that struct field already had
// stays authoritative, and only the SASL/TLS settings come from the
// environment.
func (c Config) WithBrokers(s string) Config {
	c.Brokers = ParseBrokers(s)
	c.UsingDefaultBrokers = false
	return c
}

// WithBrokerList is WithBrokers for a caller that already holds a split list.
// Entries are trimmed and blanks dropped, so a list built by a naive
// strings.Split cannot smuggle in an empty or space-padded address.
func (c Config) WithBrokerList(brokers []string) Config {
	out := make([]string, 0, len(brokers))
	for _, b := range brokers {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	c.Brokers = out
	c.UsingDefaultBrokers = false
	return c
}

// WithServiceName stamps the caller's client.id onto a Config it did not build
// itself — the shape a service has when it holds a Config from FromEnv, or one
// threaded through its own settings struct.
//
// An explicit KAFKA_CLIENT_ID wins, so a deployment that wants one identity for
// the whole platform sets the variable once and no service can override it from
// code. An empty service name is ignored rather than clearing the id: losing
// the identity is the failure this exists to prevent.
func (c Config) WithServiceName(service string) Config {
	if c.clientIDFromEnv {
		return c
	}
	if id := DefaultClientID(service); id != DefaultClientID("") {
		c.ClientID = id
	}
	return c
}
