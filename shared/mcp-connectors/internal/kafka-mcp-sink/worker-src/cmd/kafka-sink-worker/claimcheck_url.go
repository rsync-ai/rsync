package main

import (
	"fmt"
	"os"
	"strings"
)

// expectedStagingBuckets returns the allow-list of object-store buckets a
// claim_check_url may reference. Precedence:
//  1. RSYNC_SINK_STAGING_BUCKET — comma-separated sink-specific override.
//  2. MINIO_BUCKET — the stack-wide staging bucket the orchestrator producer and
//     minio-mcp use (docker-compose default ${MINIO_BUCKET:-pipeline-data}).
//  3. "pipeline-data" — the system default when neither env is set.
//
// Operators who override the staging bucket for minio-mcp must propagate the same
// MINIO_BUCKET (or set RSYNC_SINK_STAGING_BUCKET) to the sink, or legitimate
// claim-check messages will be rejected fail-closed.
func expectedStagingBuckets() []string {
	if v := strings.TrimSpace(os.Getenv("RSYNC_SINK_STAGING_BUCKET")); v != "" {
		var out []string
		for _, b := range strings.Split(v, ",") {
			if b = strings.TrimSpace(b); b != "" {
				out = append(out, b)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	if v := strings.TrimSpace(os.Getenv("MINIO_BUCKET")); v != "" {
		return []string{v}
	}
	return []string{"pipeline-data"}
}

// validateClaimCheckURL fail-closes a claim_check_url that crosses the Kafka trust
// boundary before the sink fetches or DELETEs it. The pointer must be of the form
// s3://<bucket>/<key> where bucket is on the staging allow-list and key carries no
// path-traversal segment or control character. Without this, any party able to
// publish to the sink's input topic could drive minio-mcp (which holds the staging
// credentials) to read or delete arbitrary objects in any reachable bucket — a
// confused-deputy primitive that includes a destructive delete.
//
// The error strings deliberately mirror the deterministic-failure substrings in
// isRetryableMinioFetchErr ("must start with s3://", "must be s3://") so that, even
// if a rejected pointer ever reached the fetch path, it would fail fast rather than
// burn the retry budget.
func validateClaimCheckURL(raw string) error {
	const scheme = "s3://"
	if !strings.HasPrefix(raw, scheme) {
		return fmt.Errorf("claim_check_url must start with s3://")
	}
	rest := raw[len(scheme):]
	for _, r := range rest {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("claim_check_url contains a control character")
		}
	}
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 || slash >= len(rest)-1 {
		return fmt.Errorf("claim_check_url must be s3://<bucket>/<key>")
	}
	bucket := rest[:slash]
	key := rest[slash+1:]
	if strings.TrimSpace(bucket) == "" || strings.TrimSpace(key) == "" {
		return fmt.Errorf("claim_check_url must be s3://<bucket>/<key>")
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == ".." {
			return fmt.Errorf("claim_check_url key contains a path-traversal segment")
		}
	}
	allowed := expectedStagingBuckets()
	for _, b := range allowed {
		if bucket == b {
			return nil
		}
	}
	return fmt.Errorf("claim_check_url bucket %q is not in the staging allow-list %v", bucket, allowed)
}
