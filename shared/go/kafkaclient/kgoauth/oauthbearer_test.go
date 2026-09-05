package kgoauth

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

func oidcConfig(endpoint string) kafkaclient.Config {
	return kafkaclient.Config{
		Brokers: []string{"b:9093"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
		SASLMechanism: kafkaclient.MechanismOAuthBearer, OAuthTokenEndpoint: endpoint,
		OAuthClientID: "id", OAuthClientSecret: "secret",
	}
}

func idpServing(t *testing.T, token string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"access_token":"` + token + `","expires_in":3600}`))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
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

// kafka-go sends Name() verbatim in SaslHandshake. "AWS_MSK_IAM" is not a
// mechanism any broker advertises -- MSK included -- so both token mechanisms
// must announce themselves as OAUTHBEARER.
func TestTokenMechanismsAnnounceOAuthBearer(t *testing.T) {
	staticAWSCreds(t)
	for _, tc := range []struct {
		name string
		c    kafkaclient.Config
	}{
		{"AWS_MSK_IAM", kafkaclient.Config{
			Brokers: []string{"b:9098"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
			SASLMechanism: kafkaclient.MechanismAWSMSKIAM, AWSRegion: "eu-west-1"}},
		{"OAUTHBEARER", oidcConfig(idpServing(t, "t"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := Mechanism(tc.c)
			if err != nil {
				t.Fatal(err)
			}
			if m == nil {
				t.Fatal("no mechanism returned")
			}
			if m.Name() != "OAUTHBEARER" {
				t.Errorf("Name() = %q, want OAUTHBEARER", m.Name())
			}
		})
	}
}

// kafka-go ships no OAUTHBEARER mechanism, so the RFC 7628 client-first message
// is hand-written here. Its exact bytes are what the broker parses; a stray or
// missing 0x01 fails the handshake with an error that names nothing.
func TestClientInitialResponseIsRFC7628(t *testing.T) {
	got := clientInitialResponse("the-token", nil)
	want := []byte("n,,\x01auth=Bearer the-token\x01\x01")
	if !bytes.Equal(got, want) {
		t.Fatalf("initial response = %q, want %q", got, want)
	}
}

func TestClientInitialResponseCarriesExtensions(t *testing.T) {
	got := clientInitialResponse("tok", map[string]string{"identityPoolId": "pool-1", "logicalCluster": "lkc-abc"})
	// Sorted: map iteration order would make the bytes differ between runs,
	// which turns a reproducible handshake failure into an intermittent one.
	want := []byte("n,,\x01auth=Bearer tok\x01identityPoolId=pool-1\x01logicalCluster=lkc-abc\x01\x01")
	if !bytes.Equal(got, want) {
		t.Fatalf("initial response = %q, want %q", got, want)
	}
	for i := 0; i < 20; i++ {
		if !bytes.Equal(clientInitialResponse("tok", map[string]string{"identityPoolId": "pool-1", "logicalCluster": "lkc-abc"}), want) {
			t.Fatal("the initial response is not deterministic across runs")
		}
	}
}

// The property that makes a cluster accepting one client library accept the
// other: this message must be byte-identical to what sarama builds. Both
// libraries talk to the same brokers in this platform, and a difference would
// surface as "the sink authenticates but the orchestrator does not".
func TestClientInitialResponseMatchesSaramaLayout(t *testing.T) {
	got := string(clientInitialResponse("tok", map[string]string{"a": "1"}))
	// sarama's buildClientFirstMessage: "n,," + kvsep + "auth=Bearer " + token
	// + kvsep + each "k=v" + kvsep + kvsep.
	const kvsep = "\x01"
	want := "n,," + kvsep + "auth=Bearer " + "tok" + kvsep + "a=1" + kvsep + kvsep
	if got != want {
		t.Fatalf("initial response = %q, want sarama's layout %q", got, want)
	}
}

// Start is where the token is fetched, so a Reader reconnecting hours later
// presents a current one rather than the token minted when the process booted.
func TestStartMintsTheTokenAndSendsIt(t *testing.T) {
	m, err := Mechanism(oidcConfig(idpServing(t, "issued-token")))
	if err != nil {
		t.Fatal(err)
	}
	sess, resp, err := m.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if sess == nil {
		t.Fatal("Start returned no state machine")
	}
	if !strings.Contains(string(resp), "auth=Bearer issued-token") {
		t.Fatalf("initial response = %q, want the issued token", resp)
	}
}

// A token source that cannot mint must fail the handshake with its own error
// rather than sending an empty credential and letting the broker answer with an
// unexplained authentication failure.
func TestStartSurfacesTheTokenError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"invalid_client"}`))
	}))
	defer srv.Close()

	m, err := Mechanism(oidcConfig(srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Start(context.Background()); err == nil {
		t.Fatal("a failing token source must fail the handshake")
	} else if !strings.Contains(err.Error(), "invalid_client") {
		t.Errorf("error = %q, want the IdP's reason", err)
	}
}

func TestNextCompletesOnAnEmptyChallenge(t *testing.T) {
	done, resp, err := oauthBearerSession{}.Next(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Error("an empty broker response means the token was accepted; the exchange is over")
	}
	if resp != nil {
		t.Errorf("nothing more should be sent, got %q", resp)
	}
}

// The broker's rejection carries the reason (expired token, missing scope). RFC
// 7628 has the client acknowledge and wait for a close, which surfaces here as
// a bare EOF -- so the reason is returned instead.
func TestNextReportsTheBrokerReason(t *testing.T) {
	_, _, err := oauthBearerSession{}.Next(context.Background(),
		[]byte(`{"status":"invalid_token"} `))
	if err == nil {
		t.Fatal("a non-empty challenge is a rejection and must be an error")
	}
	if !strings.Contains(err.Error(), "invalid_token") {
		t.Errorf("error = %q, want the broker's own reason", err)
	}
}

// The end-to-end shape for EKS: an MSK IAM config produces a mechanism whose
// initial response carries a presigned kafka-cluster:Connect URL.
func TestMSKIAMHandshakeCarriesAPresignedURL(t *testing.T) {
	staticAWSCreds(t)
	m, err := Mechanism(kafkaclient.Config{
		Brokers: []string{"b:9098"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
		SASLMechanism: kafkaclient.MechanismAWSMSKIAM, AWSRegion: "eu-west-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, resp, err := m.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSuffix(strings.TrimPrefix(string(resp), "n,,\x01auth=Bearer "), "\x01\x01")
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		t.Fatalf("the handshake does not carry a base64url token: %v", err)
	}
	if !strings.Contains(string(raw), "kafka-cluster%3AConnect") && !strings.Contains(string(raw), "kafka-cluster:Connect") {
		t.Errorf("the token is not a kafka-cluster:Connect presigned URL: %s", raw)
	}
}

// client.id is what a managed cluster attributes throttling and quotas to, and
// kafka-go has a separate field on each of the two shapes -- so setting it on
// one and not the other leaves half the connections anonymous.
func TestClientIDReachesBothDialerAndTransport(t *testing.T) {
	c := kafkaclient.Config{
		Brokers: []string{"b:9092"}, SecurityProtocol: kafkaclient.ProtocolPlaintext,
		ClientID: "rsync-kafka-sink",
	}
	d, err := Dialer(c)
	if err != nil {
		t.Fatal(err)
	}
	if d.ClientID != "rsync-kafka-sink" {
		t.Errorf("Dialer.ClientID = %q, want rsync-kafka-sink", d.ClientID)
	}
	tr, err := Transport(c)
	if err != nil {
		t.Fatal(err)
	}
	if tr.ClientID != "rsync-kafka-sink" {
		t.Errorf("Transport.ClientID = %q, want rsync-kafka-sink", tr.ClientID)
	}
}

// Token mechanisms must reach the Dialer and the Transport too: a Reader that
// authenticates while its Writer does not is the confusing half-working state.
func TestTokenMechanismReachesDialerAndTransport(t *testing.T) {
	c := oidcConfig(idpServing(t, "t"))
	d, err := Dialer(c)
	if err != nil {
		t.Fatal(err)
	}
	if d.SASLMechanism == nil || d.SASLMechanism.Name() != "OAUTHBEARER" {
		t.Errorf("Dialer carries %v, want an OAUTHBEARER mechanism", d.SASLMechanism)
	}
	if d.TLS == nil {
		t.Error("SASL_SSL must give the dialer a TLS config")
	}
	tr, err := Transport(c)
	if err != nil {
		t.Fatal(err)
	}
	if tr.SASL == nil || tr.SASL.Name() != "OAUTHBEARER" {
		t.Errorf("Transport carries %v, want an OAUTHBEARER mechanism", tr.SASL)
	}
}

func TestDialerRejectsMSKIAMWithoutTLS(t *testing.T) {
	c := kafkaclient.Config{
		Brokers: []string{"b:9098"}, SecurityProtocol: kafkaclient.ProtocolSASLPlaintext,
		SASLMechanism: kafkaclient.MechanismAWSMSKIAM, AWSRegion: "eu-west-1",
	}
	if _, err := Dialer(c); err == nil {
		t.Error("MSK IAM over SASL_PLAINTEXT must be rejected")
	}
	if _, err := Transport(c); err == nil {
		t.Error("MSK IAM over SASL_PLAINTEXT must be rejected")
	}
}
