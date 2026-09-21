package handlers

import "testing"

// aws-s3 is the real connector id (metadata.json connector_type: "aws-s3"). The
// kind/default maps historically listed only "s3", so an aws-s3 destination fell
// through to the "schema" default: the first-run table-selection HITL then
// seeded namespace_kind="schema" and labeled the field a required "Schema name",
// which blocked submitting an S3 pipeline. These pin aws-s3 to the same
// object-storage (path) behavior as the other cloud-storage connectors. The
// answers come from each connector's namespace_model block; every name is pinned
// in shared/namespace_model_golden.json (backend-orchestrator/pkg/namespacemodel).
func TestNamespaceKindForConnector_AwsS3IsPath(t *testing.T) {
	cases := map[string]string{
		"aws-s3":     "path",
		"s3":         "path",
		"gcs":        "path",
		"azure-blob": "path",
		"minio":      "path",
		"postgresql": "schema",
		"mysql":      "database",
		"bigquery":   "dataset",
		"mongodb":    "database",
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
	for _, connType := range []string{"aws-s3", "s3", "gcs", "azure-blob", "minio"} {
		if got := destDefaultSchemaName(connType); got != "" {
			t.Errorf("destDefaultSchemaName(%q) = %q, want empty", connType, got)
		}
	}
}

// #13: an object-storage namespace is the <db_or_schema> folder of every object
// key, and left empty the writers fill it from the source database. A seeded
// source-type name ("sqlserver", "mongodb") put every database into one folder
// named after the connector type. Every source must seed empty for a path
// destination, while relational destinations keep their seeds.
func TestSeedDestinationNamespace_ObjectStorageSeedsEmpty(t *testing.T) {
	for _, dest := range []string{"aws-s3", "s3", "gcs", "azure-blob", "minio"} {
		for _, src := range []string{"mongodb", "sqlserver", "snowflake", "postgresql", "mysql", "shopify", "oracle"} {
			if got := seedDestinationNamespace(src, dest); got != "" {
				t.Errorf("seedDestinationNamespace(%q, %q) = %q, want empty", src, dest, got)
			}
		}
	}
	relational := []struct{ src, dest, want string }{
		{"sqlserver", "postgresql", "sqlserver"},
		{"mysql", "postgresql", "public"},
		{"postgresql", "mysql", "default"},
		{"mongodb", "postgresql", "public"},
	}
	for _, c := range relational {
		if got := seedDestinationNamespace(c.src, c.dest); got != c.want {
			t.Errorf("seedDestinationNamespace(%q, %q) = %q, want %q", c.src, c.dest, got, c.want)
		}
	}
}
