package heal

import (
	"strings"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// The whole value of a signature is that two occurrences of ONE fault collapse to the
// same string. Each pair here is the same operational problem wearing different
// per-incident noise; if any pair diverges, recall silently stops working — the healer
// looks up a key it has never stored and concludes it has no history.
func TestNormalizeErrorForSignatureCollapsesIncidentNoise(t *testing.T) {
	pairs := []struct {
		name string
		a, b string
	}{
		{
			name: "different hosts, same refusal",
			a:    "dial tcp 10.0.3.7:5432: connect: connection refused",
			b:    "dial tcp 10.0.9.211:5432: connect: connection refused",
		},
		{
			name: "different execution ids",
			a:    "execution 3f1a5c8e-0b2d-4a77-9c31-1de4f0a9b6c2 aborted mid-batch",
			b:    "execution 91bb0d4f-77aa-4e10-8f02-cc5512ab3390 aborted mid-batch",
		},
		{
			name: "different timestamps",
			a:    "replication slot inactive since 2026-07-30T11:04:22Z",
			b:    "replication slot inactive since 2026-07-31T02:57:09Z",
		},
		{
			name: "different row counts",
			a:    "wrote 0 of 148213 rows to destination",
			b:    "wrote 0 of 7 rows to destination",
		},
		{
			name: "different urls under the same host shape",
			a:    "GET https://api.stripe.com/v1/charges?starting_after=ch_3Pab failed",
			b:    "GET https://api.stripe.com/v1/charges?starting_after=ch_9Zzz failed",
		},
		{
			name: "different WAL offsets",
			a:    "restart_lsn 0x16B3F28 is ahead of confirmed_flush",
			b:    "restart_lsn 0x2A9C104 is ahead of confirmed_flush",
		},
		{
			name: "case and padding differences",
			a:    "  Connection Refused  ",
			b:    "connection refused",
		},
	}

	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) {
			ga, gb := NormalizeErrorForSignature(p.a), NormalizeErrorForSignature(p.b)
			if ga != gb {
				t.Errorf("same fault normalized differently:\n  %q -> %q\n  %q -> %q", p.a, ga, p.b, gb)
			}
			if ga == "" {
				t.Errorf("normalized to empty string, which would merge unrelated faults")
			}
		})
	}
}

// The inverse risk is worse than over-collapsing: if two genuinely different faults
// share a signature, a remedy that healed one gets recalled for the other.
func TestNormalizeErrorForSignatureKeepsDistinctFaultsDistinct(t *testing.T) {
	pairs := [][2]string{
		{"connection refused", "connection reset by peer"},
		{"permission denied for table orders", "permission denied for table customers"},
		{"replication slot does not exist", "publication does not exist"},
	}
	for _, p := range pairs {
		a, b := NormalizeErrorForSignature(p[0]), NormalizeErrorForSignature(p[1])
		if a == b {
			t.Errorf("distinct faults collapsed to the same text: %q and %q both -> %q", p[0], p[1], a)
		}
	}
}

func TestNormalizeErrorForSignatureBounds(t *testing.T) {
	if got := NormalizeErrorForSignature(""); got != "" {
		t.Errorf("empty input = %q, want empty", got)
	}
	if got := NormalizeErrorForSignature("   \t "); got != "" {
		t.Errorf("whitespace-only input = %q, want empty", got)
	}
	long := "failed to apply batch: " + strings.Repeat("stack frame at some/very/long/path.go; ", 40)
	if got := NormalizeErrorForSignature(long); len(got) > maxSignatureErrorLen {
		t.Errorf("length = %d, want <= %d", len(got), maxSignatureErrorLen)
	}
}

// The status prefix is carried as its own signature field; leaving it in the error
// fragment would double-count it and make the key noisier for no gain.
func TestNormalizeErrorForSignatureDropsStatusPrefix(t *testing.T) {
	got := NormalizeErrorForSignature("silent_drop_detected: orders: read=100 landed=0")
	if strings.Contains(got, "silent_drop_detected") {
		t.Errorf("status prefix survived normalization: %q", got)
	}
}

func TestFailureSignatureSeparatesByEndpoint(t *testing.T) {
	dx := diagnose.Diagnosis{Category: diagnose.CategoryNetwork, SuggestedAction: diagnose.ActionBackoffRetry}
	pg := FailureSignature(diagnose.Signal{
		ErrorMessage: "connection refused", ExecutorStatus: "failed",
		SourceType: "postgresql", DestinationType: "snowflake",
	}, dx)
	s3 := FailureSignature(diagnose.Signal{
		ErrorMessage: "connection refused", ExecutorStatus: "failed",
		SourceType: "aws-s3", DestinationType: "snowflake",
	}, dx)
	// Same category, same error text — but "the Postgres source is unreachable" and
	// "S3 is unreachable" are different operational problems with different remedies.
	if pg == s3 {
		t.Fatalf("different source types produced one signature: %q", pg)
	}
}

// Recall exists to discover that the rules' suggested action is NOT the action that
// works. Baking the action into the key makes that discovery impossible: the winning
// action would be filed under a key the rules never generate.
func TestFailureSignatureIgnoresSuggestedAction(t *testing.T) {
	sig := diagnose.Signal{
		ErrorMessage: "connection refused", ExecutorStatus: "failed",
		SourceType: "postgresql", DestinationType: "snowflake",
	}
	retry := FailureSignature(sig, diagnose.Diagnosis{
		Category: diagnose.CategoryNetwork, SuggestedAction: diagnose.ActionBackoffRetry,
	})
	escalate := FailureSignature(sig, diagnose.Diagnosis{
		Category: diagnose.CategoryNetwork, SuggestedAction: diagnose.ActionEscalate,
	})
	if retry != escalate {
		t.Fatalf("suggested action leaked into the signature:\n  %q\n  %q", retry, escalate)
	}
}

func TestFailureSignatureIsStableAndReadable(t *testing.T) {
	got := FailureSignature(diagnose.Signal{
		ErrorMessage:   "dial tcp 10.0.3.7:5432: connect: connection refused",
		ExecutorStatus: "failed", SourceType: "PostgreSQL", DestinationType: "Snowflake",
	}, diagnose.Diagnosis{Category: diagnose.CategoryNetwork})

	if n := strings.Count(got, "|"); n != 3 {
		t.Errorf("signature = %q, want exactly 3 field separators, got %d", got, n)
	}
	if !strings.HasPrefix(got, string(diagnose.CategoryNetwork)+"|failed|postgresql->snowflake|") {
		t.Errorf("signature = %q, want lower-cased category|status|src->dst prefix", got)
	}
	// Readability is a requirement, not a nicety: an operator reading a heal_attempts
	// row must be able to see why two incidents were grouped without decoding a hash.
	if strings.Contains(got, "10.0.3.7") {
		t.Errorf("raw address survived into the signature: %q", got)
	}
}

// A Signal with nothing populated still has to produce a usable key rather than a
// bare "|||" that every unrelated empty failure would collide on.
func TestFailureSignatureEmptySignalUsesFallbacks(t *testing.T) {
	got := FailureSignature(diagnose.Signal{}, diagnose.Diagnosis{})
	if got != "unknown|unknown|?->?|" {
		t.Fatalf("empty signal = %q, want %q", got, "unknown|unknown|?->?|")
	}
}
