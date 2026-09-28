package executor

// Unit cover for the executor's blob (raw-bytes passthrough) lane helpers —
// universal-blob-passthrough plan §3.

import (
	"os"
	"testing"
)

func TestBlobTablesToExport(t *testing.T) {
	// Explicit selection (interface slice, as it arrives from JSON) is honored.
	got := blobTablesToExport(map[string]interface{}{"tables": []interface{}{"a/", "b/"}})
	if len(got) != 2 || got[0] != "a/" || got[1] != "b/" {
		t.Fatalf("tables: got %v", got)
	}
	// selected_tables is the fallback selection key.
	got = blobTablesToExport(map[string]interface{}{"selected_tables": []string{"c/"}})
	if len(got) != 1 || got[0] != "c/" {
		t.Fatalf("selected_tables: got %v", got)
	}
	// No selection → one whole-bucket pass (empty table), never zero passes
	// (which would silently copy nothing).
	got = blobTablesToExport(map[string]interface{}{})
	if len(got) != 1 || got[0] != "" {
		t.Fatalf("default: got %v", got)
	}
}

func TestBuildStagingConfigDefaults(t *testing.T) {
	for _, k := range []string{"MINIO_ENDPOINT_URL", "MINIO_ACCESS_KEY_ID", "MINIO_SECRET_ACCESS_KEY", "MINIO_REGION", "MINIO_BUCKET", "MINIO_PREFIX"} {
		os.Unsetenv(k)
	}
	sc := (&Agent{}).buildStagingConfig()
	want := map[string]string{
		"endpoint":   "http://rsync-ai-minio:9000",
		"access_key": "minioadmin",
		"secret_key": "minioadmin",
		"region":     "us-east-1",
		"bucket":     "pipeline-data",
		"prefix":     "staging/blobs", // MINIO_PREFIX default "staging" + "/blobs"
	}
	for k, v := range want {
		if sc[k] != v {
			t.Errorf("staging_config[%q]=%v want %q", k, sc[k], v)
		}
	}
}

func TestBuildStagingConfigEnvOverride(t *testing.T) {
	os.Setenv("MINIO_ENDPOINT_URL", "http://custom-minio:9000")
	os.Setenv("MINIO_PREFIX", "myprefix")
	os.Setenv("MINIO_BUCKET", "mybucket")
	defer func() {
		os.Unsetenv("MINIO_ENDPOINT_URL")
		os.Unsetenv("MINIO_PREFIX")
		os.Unsetenv("MINIO_BUCKET")
	}()
	sc := (&Agent{}).buildStagingConfig()
	if sc["endpoint"] != "http://custom-minio:9000" {
		t.Errorf("endpoint=%v", sc["endpoint"])
	}
	if sc["bucket"] != "mybucket" {
		t.Errorf("bucket=%v", sc["bucket"])
	}
	if sc["prefix"] != "myprefix/blobs" {
		t.Errorf("prefix=%v want myprefix/blobs", sc["prefix"])
	}
}

// On IRSA / GKE Workload Identity / AKS the chart emits NO credential env by
// design: the SDK is meant to read a projected token. The blob lane used to
// substitute "minioadmin" there, and because the staging config is passed to the
// connectors and on to boto3 explicitly, that did not fall back -- it overrode
// the token and produced an auth failure naming a key nobody had configured.
//
// The discriminator is the endpoint: minioadmin is only ever right against the
// bundled MinIO. (KI-CHART-ORCHESTRATOR-LOSES-OBJECTSTORAGE-ENV.)
//
// This assertion is only worth anything because the OTHER half of the contract
// now holds. Emitting empty credentials is correct exclusively if
// base_connector.py _get_staging_client omits the boto3 credential kwargs when
// both are empty; it used to reject them instead, which made this test green
// while the behavior it describes failed in production. The Python half is
// pinned by shared/mcp-connectors/tests/test_staging_client_workload_identity.py
// -- if that suite is deleted, this one is asserting a fiction again.
func TestBuildStagingConfigLeavesCredentialsEmptyOffTheBundledMinIO(t *testing.T) {
	for _, k := range []string{"MINIO_ACCESS_KEY_ID", "MINIO_SECRET_ACCESS_KEY", "MINIO_REGION", "MINIO_BUCKET", "MINIO_PREFIX"} {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}
	t.Setenv("MINIO_ENDPOINT_URL", "https://s3.us-east-2.amazonaws.com")

	sc := (&Agent{}).buildStagingConfig()
	if sc["endpoint"] != "https://s3.us-east-2.amazonaws.com" {
		t.Fatalf("endpoint=%v -- the rest of this test asserts nothing if the endpoint did not take", sc["endpoint"])
	}
	for _, k := range []string{"access_key", "secret_key"} {
		if sc[k] != "" {
			t.Errorf("staging_config[%q]=%v, want empty.\n"+
				"A non-empty key here reaches boto3 as an explicit credential and "+
				"silences the workload-identity chain.", k, sc[k])
		}
	}
}

// The bundled MinIO keeps its well-known defaults: they are that deployment's
// real credentials, so the fix above must not take them away.
func TestBuildStagingConfigKeepsBundledMinIODefaults(t *testing.T) {
	for _, k := range []string{"MINIO_ENDPOINT_URL", "MINIO_ACCESS_KEY_ID", "MINIO_SECRET_ACCESS_KEY"} {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}
	sc := (&Agent{}).buildStagingConfig()
	if sc["endpoint"] != bundledMinIOEndpoint {
		t.Fatalf("endpoint=%v want %v", sc["endpoint"], bundledMinIOEndpoint)
	}
	if sc["access_key"] != "minioadmin" || sc["secret_key"] != "minioadmin" {
		t.Errorf("bundled MinIO lost its defaults: access_key=%v secret_key=%v", sc["access_key"], sc["secret_key"])
	}
}

// Explicit credentials win everywhere, including off the bundled endpoint.
func TestBuildStagingConfigHonoursExplicitCredentials(t *testing.T) {
	t.Setenv("MINIO_ENDPOINT_URL", "https://s3.us-east-2.amazonaws.com")
	t.Setenv("MINIO_ACCESS_KEY_ID", "AKIAEXAMPLE")
	t.Setenv("MINIO_SECRET_ACCESS_KEY", "PLACEHOLDER")
	sc := (&Agent{}).buildStagingConfig()
	if sc["access_key"] != "AKIAEXAMPLE" || sc["secret_key"] != "PLACEHOLDER" {
		t.Errorf("explicit credentials dropped: access_key=%v secret_key=%v", sc["access_key"], sc["secret_key"])
	}
}

// The dedicated staging store (KI-BLOB-LANE-STAGING-UNREACHABLE-FROM-CONNECTORS)
// wins over the claim-check MinIO, and carries only its OWN credentials: the
// claim-check MinIO's keys must not ride along to the connectors, and the
// minioadmin default must not be substituted for an empty pair.
func TestBuildStagingConfigPrefersTheDedicatedStore(t *testing.T) {
	t.Setenv("MINIO_ENDPOINT_URL", "http://minio:9000")
	t.Setenv("MINIO_ACCESS_KEY_ID", "claim-check-key")
	t.Setenv("MINIO_SECRET_ACCESS_KEY", "claim-check-secret")
	t.Setenv("MINIO_BUCKET", "pipeline-data")
	t.Setenv("MINIO_PREFIX", "staging")
	t.Setenv("BLOB_STAGING_ENDPOINT_URL", "http://blob-staging:9000")
	t.Setenv("BLOB_STAGING_ACCESS_KEY_ID", "")
	t.Setenv("BLOB_STAGING_SECRET_ACCESS_KEY", "")

	sc := (&Agent{}).buildStagingConfig()
	if sc["endpoint"] != "http://blob-staging:9000" {
		t.Fatalf("endpoint=%v, want the dedicated store", sc["endpoint"])
	}
	if sc["access_key"] != "" || sc["secret_key"] != "" {
		t.Fatalf("credentials=%v/%v, want empty: never the claim-check keys, never minioadmin", sc["access_key"], sc["secret_key"])
	}
	// Same bucket and prefix as before: the sink's allow-list and the store's
	// expiry rule are both keyed on them.
	if sc["bucket"] != "pipeline-data" || sc["prefix"] != "staging/blobs" {
		t.Fatalf("bucket/prefix=%v/%v", sc["bucket"], sc["prefix"])
	}

	t.Setenv("BLOB_STAGING_ACCESS_KEY_ID", "staging-key")
	t.Setenv("BLOB_STAGING_SECRET_ACCESS_KEY", "staging-secret")
	sc = (&Agent{}).buildStagingConfig()
	if sc["access_key"] != "staging-key" || sc["secret_key"] != "staging-secret" {
		t.Fatalf("credentials=%v/%v, want the staging store's own", sc["access_key"], sc["secret_key"])
	}
}

// Unset, the lane keeps its old source: the chart sets no BLOB_STAGING_* and
// relies on MINIO_ENDPOINT_URL.
func TestBuildStagingConfigFallsBackWithoutADedicatedStore(t *testing.T) {
	t.Setenv("BLOB_STAGING_ENDPOINT_URL", "")
	t.Setenv("MINIO_ENDPOINT_URL", "http://release-minio:9000")
	sc := (&Agent{}).buildStagingConfig()
	if sc["endpoint"] != "http://release-minio:9000" {
		t.Fatalf("endpoint=%v, want MINIO_ENDPOINT_URL", sc["endpoint"])
	}
}
