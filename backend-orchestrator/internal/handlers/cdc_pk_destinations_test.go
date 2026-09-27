package handlers

import (
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/assessor"
)

// The keyless-table block has three gates that must agree on which
// destinations it covers: this handler's (Edit tables + backfill), the
// executor's hard-block at pipeline start, and the pre-flight assessor. A
// MongoDB destination used to pass the handler and the executor while its CDC
// sink guessed a key per row — rows sharing the guess replaced each other.
func TestCDCDestinationRequiresPrimaryKeys(t *testing.T) {
	cases := map[string]bool{
		"postgresql": true,
		"postgres":   true,
		"mysql":      true,
		"mariadb":    true,
		"mongodb":    true,
		" MongoDB ":  true,
		"gcs":        false,
		"aws-s3":     false,
		"azure-blob": false,
		"minio":      false,
		"oracle":     false,
		"sqlserver":  false,
		"":           false,
	}
	for dest, want := range cases {
		if got := cdcDestinationRequiresPrimaryKeys(dest); got != want {
			t.Errorf("cdcDestinationRequiresPrimaryKeys(%q) = %v; want %v", dest, got, want)
		}
	}
}

func TestCDCDestinationRequiresPrimaryKeys_LockstepWithAssessor(t *testing.T) {
	for _, dest := range []string{
		"postgresql", "postgres", "mysql", "mariadb", "mongodb",
		"gcs", "aws-s3", "azure-blob", "minio", "oracle", "sqlserver", "snowflake", "bigquery", "",
	} {
		handler := cdcDestinationRequiresPrimaryKeys(dest)
		pre := assessor.Input{SyncMode: "cdc", DestinationType: dest}.CDCBlocksWithoutPrimaryKey()
		if handler != pre {
			t.Errorf("destination %q: handler gate = %v, assessor gate = %v — the two must block the same destinations", dest, handler, pre)
		}
	}
}
