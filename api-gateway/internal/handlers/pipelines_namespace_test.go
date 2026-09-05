package handlers

import "testing"

// aws-s3 is the real connector id (metadata.json connector_type: "aws-s3"). The
// kind/default maps historically listed only "s3", so an aws-s3 destination fell
// through to the "schema" default: the first-run table-selection HITL then
// seeded namespace_kind="schema" and labeled the field a required "Schema name",
// which blocked submitting an S3 pipeline. These pin aws-s3 to the same
// object-storage (path) behavior as the other cloud-storage connectors.
func TestNamespaceKindForConnector_AwsS3IsPath(t *testing.T) {
	cases := map[string]string{
		"aws-s3":         "path",
		"s3":             "path",
		"gcs":            "path",
		"azure-blob":     "path",
		"minio":          "path",
		"object-storage": "path",
		"postgresql":     "schema",
		"mysql":          "database",
		"bigquery":       "dataset",
	}
	for connType, want := range cases {
		if got := namespaceKindForConnector(connType); got != want {
			t.Errorf("namespaceKindForConnector(%q) = %q, want %q", connType, got, want)
		}
	}
}

func TestDestDefaultSchemaName_AwsS3HasNoDefault(t *testing.T) {
	// Path-style destinations have no schema concept → empty default so the HITL
	// field starts blank (and, for a multi-schema source, auto-preserve mirrors
	// each source schema at the destination).
	for _, connType := range []string{"aws-s3", "s3", "gcs", "azure-blob", "minio", "object-storage"} {
		if got := destDefaultSchemaName(connType); got != "" {
			t.Errorf("destDefaultSchemaName(%q) = %q, want empty", connType, got)
		}
	}
}
