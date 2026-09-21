package handlers

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// A chat request names a connector however the user or the intent parser spelled
// it, and the connection row stores the connector's id. When the two keys differ
// the lookup finds no connection and the pipeline is created with neither side
// set, which the pipeline list renders as "— → —". These tests derive the
// spellings from the connectors' own metadata so a newly declared alias cannot
// silently reopen that gap.

type declaredConnector struct {
	dir     string
	id      string
	name    string
	aliases []string
}

// publicConnectorsOnDisk reads versions/<current_version>/metadata.json for every
// user-facing connector. Internal connectors are never a connection's type.
func publicConnectorsOnDisk(t *testing.T) []declaredConnector {
	t.Helper()
	root := filepath.Join("..", "..", "..", "shared", "mcp-connectors", "public")
	if fi, err := os.Stat(root); err != nil || !fi.IsDir() {
		// Fatal, not Skip: a skipped guard reads exactly like a passing one.
		t.Fatalf("connector tree not found at %s (err=%v)", root, err)
	}
	var out []declaredConnector
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() != "latest.json" {
			return err
		}
		dir := filepath.Dir(path)
		if filepath.Base(filepath.Dir(dir)) == "versions" {
			return nil
		}
		var manifest struct {
			CurrentVersion string `json:"current_version"`
		}
		b, err := os.ReadFile(path)
		if err != nil || json.Unmarshal(b, &manifest) != nil || manifest.CurrentVersion == "" {
			return nil
		}
		mb, err := os.ReadFile(filepath.Join(dir, "versions", manifest.CurrentVersion, "metadata.json"))
		if err != nil {
			return nil
		}
		var meta struct {
			ID      string   `json:"id"`
			Name    string   `json:"name"`
			Aliases []string `json:"aliases"`
		}
		if json.Unmarshal(mb, &meta) != nil || meta.ID == "" {
			return nil
		}
		out = append(out, declaredConnector{dir: filepath.Base(dir), id: meta.ID, name: meta.Name, aliases: meta.Aliases})
		return nil
	})
	if err != nil {
		t.Fatalf("walk connector tree: %v", err)
	}
	return out
}

func TestConnectorKeyFoldsEveryDeclaredAlias(t *testing.T) {
	connectors := publicConnectorsOnDisk(t)
	if len(connectors) < 10 {
		t.Fatalf("found %d connectors; the walk is not seeing the tree", len(connectors))
	}
	checked := 0
	for _, c := range connectors {
		want := connectorKeyForConnResolution(c.id)
		for _, spelling := range append([]string{c.dir, c.name}, c.aliases...) {
			if spelling == "" {
				continue
			}
			checked++
			if got := connectorKeyForConnResolution(spelling); got != want {
				t.Errorf("%s declares %q, which keys to %q, but its id keys to %q; add it to connectorIdentityAliases",
					c.id, spelling, got, want)
			}
		}
	}
	if checked < 30 {
		t.Fatalf("checked only %d spellings; expected the declared aliases to be read", checked)
	}
}

func TestConnectorKeyMatchesStorageSpellingsButNotOtherConnectors(t *testing.T) {
	same := [][2]string{
		{"google-cloud-storage", "gcs"},
		{"Google Cloud Storage", "gcs"},
		{"azure-blob-storage", "azure-blob"},
		{"s3", "aws-s3"},
		{"postgres", "postgresql"},
		{"mongo", "mongodb"},
	}
	for _, p := range same {
		if connectorKeyForConnResolution(p[0]) != connectorKeyForConnResolution(p[1]) {
			t.Errorf("%q and %q should resolve to the same connections", p[0], p[1])
		}
	}
	different := [][2]string{
		{"gcs", "aws-s3"},
		{"google-cloud-storage", "azure-blob"},
		{"mongodb", "mysql"},
		{"bigquery", "gcs"},
	}
	for _, p := range different {
		if connectorKeyForConnResolution(p[0]) == connectorKeyForConnResolution(p[1]) {
			t.Errorf("%q and %q must not resolve to each other's connections", p[0], p[1])
		}
	}
}
