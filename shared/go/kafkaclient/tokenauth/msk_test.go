package tokenauth

import (
	"context"
	"encoding/base64"
	"net/url"
	"strings"
	"testing"
	"time"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// staticAWSCreds puts credentials in the environment so the SDK's default chain
// resolves them locally. Signing is then pure computation with no call to STS
// or IMDS, which is what makes this testable without an AWS account -- and the
// same chain is what finds the IRSA role on EKS.
func staticAWSCreds(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
	t.Setenv("AWS_SESSION_TOKEN", "")
	// Nothing may reach the container credential endpoint or IMDS during a
	// unit test; an unset one here would let the chain wander off the machine.
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
}

// The end-to-end shape of the mechanism this branch adds: config in, a real
// MSK IAM token out. A token that is merely non-empty proves nothing, so this
// decodes it and checks it is the presigned kafka-cluster:Connect URL the
// broker expects.
func TestMSKTokenIsAPresignedConnectURL(t *testing.T) {
	staticAWSCreds(t)
	src, err := New(kafkaclient.Config{
		Brokers: []string{"b-1.msk.eu-west-1.amazonaws.com:9098"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
		SASLMechanism: kafkaclient.MechanismAWSMSKIAM, AWSRegion: "eu-west-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	tok, err := src.Token(context.Background())
	if err != nil {
		t.Fatalf("signing an MSK IAM token: %v", err)
	}
	if tok.Value == "" {
		t.Fatal("empty token")
	}

	// The signer emits base64url without padding, which is what the broker
	// decodes; standard base64 would fail there and nowhere else.
	raw, err := base64.RawURLEncoding.DecodeString(tok.Value)
	if err != nil {
		t.Fatalf("the token is not raw base64url, so the broker cannot decode it: %v", err)
	}
	u, err := url.Parse(string(raw))
	if err != nil {
		t.Fatalf("the decoded token is not a URL: %v", err)
	}
	if !strings.Contains(u.Host, "kafka.eu-west-1.amazonaws.com") {
		t.Errorf("token host = %q, want the eu-west-1 kafka endpoint -- a token signed for the wrong region is rejected by the broker", u.Host)
	}
	q := u.Query()
	if q.Get("Action") != "kafka-cluster:Connect" {
		t.Errorf("Action = %q, want kafka-cluster:Connect", q.Get("Action"))
	}
	if !strings.Contains(q.Get("X-Amz-Credential"), "eu-west-1") {
		t.Errorf("X-Amz-Credential = %q, want it scoped to eu-west-1", q.Get("X-Amz-Credential"))
	}
	if q.Get("X-Amz-Signature") == "" {
		t.Error("the token carries no signature")
	}

	// Expiry has to be the signer's own, not a guess: the cache refreshes
	// against it, and a wrong one means either a needless re-sign per call or a
	// dead token handed to a broker.
	ttl := time.Until(tok.Expires)
	if ttl <= 0 || ttl > 20*time.Minute {
		t.Errorf("token expires in %v, want roughly the signer's 15 minutes", ttl)
	}
	if ttl < 10*time.Minute {
		t.Errorf("token expires in %v, which is shorter than the signer's documented validity", ttl)
	}
}

// The region is what the token is signed against, so it must be the configured
// one rather than whatever the ambient AWS_REGION happens to be -- a pod in
// us-east-1 talking to an MSK cluster in eu-west-1 is the normal case this gets
// wrong.
func TestMSKTokenIsSignedForTheConfiguredRegion(t *testing.T) {
	staticAWSCreds(t)
	t.Setenv("AWS_REGION", "us-east-1")

	src, err := New(kafkaclient.Config{
		Brokers: []string{"b:9098"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
		SASLMechanism: kafkaclient.MechanismAWSMSKIAM, AWSRegion: "ap-southeast-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	tok, err := src.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(tok.Value)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "ap-southeast-2") {
		t.Fatalf("the token was not signed for the configured region: %s", raw)
	}
	if strings.Contains(string(raw), "us-east-1") {
		t.Fatal("the ambient AWS_REGION leaked into the signature")
	}
}

// MSK IAM sends no username or password, which is the entire reason it works on
// EKS with no stored secret.
func TestMSKNeedsNoStaticCredentials(t *testing.T) {
	staticAWSCreds(t)
	c := kafkaclient.Config{
		Brokers: []string{"b:9098"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
		SASLMechanism: kafkaclient.MechanismAWSMSKIAM, AWSRegion: "eu-west-1",
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("a Config with no username/password must be valid for MSK IAM: %v", err)
	}
	src, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// A signing failure has to name the mechanism and the region: the SDK's own
// error ("failed to refresh cached credentials") appears identically for a
// dozen unrelated AWS problems.
func TestMSKSigningErrorNamesTheRegion(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	src, err := New(kafkaclient.Config{
		Brokers: []string{"b:9098"}, SecurityProtocol: kafkaclient.ProtocolSASLSSL,
		SASLMechanism: kafkaclient.MechanismAWSMSKIAM, AWSRegion: "eu-west-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = src.Token(context.Background())
	if err == nil {
		t.Skip("the host has ambient AWS credentials, so signing succeeded; the error path cannot be exercised here")
	}
	if !strings.Contains(err.Error(), "eu-west-1") || !strings.Contains(err.Error(), "MSK IAM") {
		t.Errorf("error = %q, want it to name the mechanism and the region", err)
	}
}
