package namespacemodel

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/connectorpaths"
)

// goldenPath is shared with frontend/src/lib/pipeline/__tests__/namespaceModel.test.ts.
var goldenPath = filepath.Join("..", "..", "..", "shared", "namespace_model_golden.json")

var repoConnectors = filepath.Join("..", "..", "..", "shared", "mcp-connectors")

func TestEveryNameResolvesAsTheGoldenSays(t *testing.T) {
	raw, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		Defaults Model            `json:"defaults"`
		Cases    map[string]Model `json:"cases"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	if len(golden.Cases) < 50 {
		t.Fatalf("golden has %d cases; it pins every id and alias of the database and storage connectors", len(golden.Cases))
	}
	if got := For("no-such-connector"); got != golden.Defaults {
		t.Errorf("defaults = %+v, golden says %+v", got, golden.Defaults)
	}
	for name, want := range golden.Cases {
		if got := For(name); got != want {
			t.Errorf("For(%q) = %+v, golden says %+v", name, got, want)
		}
	}
}

// A connector that stores tables in, or writes them to, a database, warehouse
// or object store must say what its namespace is. Without the block every
// caller silently treats it as schema-based.
func TestEveryDatabaseConnectorDeclaresANamespaceModel(t *testing.T) {
	needs := map[string]bool{"relational_db": true, "document_db": true, "data_warehouse": true, "cloud_storage": true, "object_storage": true}
	roots := connectorpaths.IterConnectorRoots(repoConnectors)
	if len(roots) < 10 {
		t.Fatalf("found %d connectors under %s; the walk is not reading the repo tree", len(roots), repoConnectors)
	}
	checked := 0
	for _, cr := range roots {
		raw, err := os.ReadFile(cr.MetadataPath)
		if err != nil {
			t.Fatal(err)
		}
		var md struct {
			Category       string          `json:"category"`
			NamespaceModel json.RawMessage `json:"namespace_model"`
		}
		if err := json.Unmarshal(raw, &md); err != nil {
			t.Fatalf("%s: %v", cr.MetadataPath, err)
		}
		if !needs[md.Category] {
			continue
		}
		checked++
		if len(md.NamespaceModel) == 0 {
			t.Errorf("%s (category %s) has no namespace_model block in %s", cr.ID, md.Category, cr.MetadataPath)
			continue
		}
		var m map[string]string
		if err := json.Unmarshal(md.NamespaceModel, &m); err != nil {
			t.Errorf("%s: namespace_model is not an object of strings: %v", cr.ID, err)
			continue
		}
		if _, ok := m["destination_namespace"]; !ok {
			t.Errorf("%s: namespace_model has no destination_namespace", cr.ID)
		}
		for k := range m {
			if k != "table_namespace" && k != "destination_namespace" && k != "destination_default" {
				t.Errorf("%s: namespace_model has unknown key %q", cr.ID, k)
			}
		}
	}
	if checked < 14 {
		t.Fatalf("checked %d database/storage connectors, want at least 14", checked)
	}
	if _, problems := Load(repoConnectors); len(problems) > 0 {
		t.Errorf("namespace_model problems:\n  %s", strings.Join(problems, "\n  "))
	}
}

// A connector whose tables sit under a namespace must be able to list those
// namespaces: the gateway's GET /connections/:id/namespaces calls
// <type>_list_namespaces on it, and a missing method only shows up there, as a
// "tool not found" at pipeline set-up.
func TestEveryTableNamespaceConnectorListsItsNamespaces(t *testing.T) {
	checked := 0
	for _, cr := range connectorpaths.IterConnectorRoots(repoConnectors) {
		raw, err := os.ReadFile(cr.MetadataPath)
		if err != nil {
			t.Fatal(err)
		}
		var md struct {
			NamespaceModel struct {
				TableNamespace string `json:"table_namespace"`
			} `json:"namespace_model"`
		}
		if err := json.Unmarshal(raw, &md); err != nil {
			t.Fatalf("%s: %v", cr.MetadataPath, err)
		}
		if md.NamespaceModel.TableNamespace == "" {
			continue
		}
		checked++
		src := filepath.Join(filepath.Dir(cr.MetadataPath), "connector.py")
		code, err := os.ReadFile(src)
		if err != nil {
			t.Errorf("%s declares table_namespace %q but has no %s: %v", cr.ID, md.NamespaceModel.TableNamespace, src, err)
			continue
		}
		if !strings.Contains(string(code), "def list_namespaces(") {
			t.Errorf("%s declares table_namespace %q but %s has no list_namespaces method", cr.ID, md.NamespaceModel.TableNamespace, src)
		}
	}
	if checked < 10 {
		t.Fatalf("checked %d connectors with a table_namespace, want at least 10", checked)
	}
}

// ListsNamespaces is what the gateway asks before calling <type>_list_namespaces
// and what the connection form asks before showing a Scope step. It must be
// true for exactly the connectors that implement list_namespaces: a false
// negative hides Scope from a database, a false positive sends an object store
// to a tool it does not have (the 502 GET /connections/:id/namespaces gave GCS).
func TestListsNamespacesMatchesTheConnectorsThatDo(t *testing.T) {
	idx, lists, problems := load(repoConnectors)
	if len(problems) > 0 {
		t.Fatalf("load problems: %v", problems)
	}
	implementing := 0
	for _, cr := range connectorpaths.IterConnectorRoots(repoConnectors) {
		code, err := os.ReadFile(filepath.Join(filepath.Dir(cr.MetadataPath), "connector.py"))
		if err != nil {
			continue
		}
		has := strings.Contains(string(code), "def list_namespaces(")
		if has {
			implementing++
		}
		if _, declared := idx[Key(cr.ID)]; !declared && !has {
			continue
		}
		if got := lists[Key(cr.ID)]; got != has {
			t.Errorf("%s: ListsNamespaces = %v, but connector.py list_namespaces present = %v", cr.ID, got, has)
		}
	}
	if implementing < 10 {
		t.Fatalf("found %d connectors implementing list_namespaces, want at least 10", implementing)
	}
	// Aliases follow their connector; object stores and unknown names never list.
	for name, want := range map[string]bool{
		"mysql": true, "aurora-mysql": true, "MongoDB": true, "atlas": true, "postgresql": true, "bq": true,
		"gcs": false, "aws-s3": false, "AWS S3": false, "azure-blob": false, "minio": false, "salesforce": false, "": false,
	} {
		if got := lists[Key(name)]; got != want {
			t.Errorf("ListsNamespaces(%q) = %v, want %v", name, got, want)
		}
	}
}

func writeConnector(t *testing.T, dir, id, body string) {
	t.Helper()
	root := filepath.Join(dir, "public", id)
	if err := os.MkdirAll(filepath.Join(root, "versions", "v1.0.0"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "latest.json"), []byte(`{"current_version":"v1.0.0"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "versions", "v1.0.0", "metadata.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	writeConnector(t, dir, "alpha", `{"id":"alpha","aliases":["Alpha DB","beta"],"namespace_model":{"destination_namespace":"database"}}`)
	writeConnector(t, dir, "beta", `{"id":"beta","aliases":["shared-name"],"namespace_model":{"table_namespace":"dataset","destination_namespace":"dataset"}}`)
	writeConnector(t, dir, "gamma", `{"id":"gamma","aliases":["shared-name"],"namespace_model":{"destination_namespace":"path"}}`)
	writeConnector(t, dir, "delta", `{"id":"delta","namespace_model":{"destination_namespace":"folder"}}`)
	writeConnector(t, dir, "saas", `{"id":"saas","aliases":["saas-api"]}`)

	idx, problems := Load(dir)

	if got := idx[Key("alpha_db")]; got != (Model{"schema", "database", ""}) {
		t.Errorf("alpha by alias = %+v; empty fields default, aliases fold spaces and underscores", got)
	}
	if got := idx[Key("beta")]; got.TableNamespace != "dataset" {
		t.Errorf("beta = %+v; an alias must never take over another connector's id", got)
	}
	if _, ok := idx[Key("delta")]; ok {
		t.Error("delta declares an unknown destination_namespace and must be left out, not guessed")
	}
	if _, ok := idx[Key("saas-api")]; ok {
		t.Error("a connector without the block must not be indexed")
	}
	joined := strings.Join(problems, "\n")
	for _, want := range []string{`alpha: alias "beta" is already beta's`, `gamma: alias "shared-name" is already beta's`, `delta: destination_namespace "folder"`} {
		if !strings.Contains(joined, want) {
			t.Errorf("problems do not mention %q:\n%s", want, joined)
		}
	}
	if len(problems) != 3 {
		t.Errorf("got %d problems, want 3:\n%s", len(problems), joined)
	}
}

func TestOnlyADeclaredTableNamespaceListsNamespaces(t *testing.T) {
	dir := t.TempDir()
	writeConnector(t, dir, "pg", `{"id":"pg","aliases":["postgres"],"namespace_model":{"table_namespace":"schema"}}`)
	writeConnector(t, dir, "bucket", `{"id":"bucket","namespace_model":{"destination_namespace":"prefix"}}`)

	idx, declared, _ := load(dir)
	if !declared[Key("pg")] || !declared[Key("postgres")] {
		t.Errorf("pg names its table_namespace; its id and alias must list namespaces: %v", declared)
	}
	if declared[Key("bucket")] {
		t.Error("bucket leaves table_namespace out and has no list_namespaces tool; it must not be asked")
	}
	if got := idx[Key("bucket")].TableNamespace; got != DefaultTableNamespace {
		t.Errorf("bucket table_namespace = %q; the index still defaults it (the golden pins that)", got)
	}
}

func TestObjectStoresDoNotListNamespaces(t *testing.T) {
	for _, name := range []string{"gcs", "aws-s3"} {
		if ListsNamespaces(name) {
			t.Errorf("%s has no list_namespaces tool; ListsNamespaces must be false", name)
		}
	}
	for _, name := range []string{"postgresql", "mongodb", "mysql"} {
		if !ListsNamespaces(name) {
			t.Errorf("%s names its table_namespace; ListsNamespaces must be true", name)
		}
	}
}

// Listing is what GET /connector-namespace-models sends the connection form as
// lists_namespaces: exactly the keys ListsNamespaces is true for, sorted.
func TestListingIsTheListersSorted(t *testing.T) {
	got := Listing()
	if !sort.StringsAreSorted(got) {
		t.Errorf("Listing() is not sorted: %v", got)
	}
	in := map[string]bool{}
	for _, k := range got {
		in[k] = true
		if !ListsNamespaces(k) {
			t.Errorf("Listing() has %q, but ListsNamespaces(%q) is false", k, k)
		}
	}
	for _, name := range []string{"mysql", "mongodb", "postgresql"} {
		if !in[Key(name)] {
			t.Errorf("Listing() leaves out %s", name)
		}
	}
	for _, name := range []string{"gcs", "aws-s3"} {
		if in[Key(name)] {
			t.Errorf("Listing() has %s, an object store", name)
		}
	}
}
