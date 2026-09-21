// Package namespacemodel reads what each connector calls the level above a
// table, from the "namespace_model" block of its metadata.json:
//
//	"namespace_model": {
//	  "table_namespace": "schema",       // as a source: what holds its tables
//	  "destination_namespace": "schema", // as a destination: what the user names
//	  "destination_default": "public"    // the engine's own default, "" for none
//	}
//
// It replaces the per-type switches that used to live in api-gateway
// (destDefaultSchemaName, namespaceKindForConnector), the orchestrator executor
// (tableNamespaceIsDatabase) and the frontend. A new connector declares the
// block and every caller picks it up. The guard test fails the build when a
// database, warehouse or object-store connector does not declare it, and
// shared/namespace_model_golden.json pins the answer for every name the old
// switches knew (the frontend reads the same file).
//
// It lives under pkg/ so api-gateway can import it the same way it imports
// pkg/llmscrub.
package namespacemodel

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rsync-ai/backend-orchestrator/internal/connectorpaths"
	log "github.com/sirupsen/logrus"
)

// Model is one connector's namespace_model block.
type Model struct {
	// TableNamespace is what holds the connector's tables when it is a source:
	// "schema", "database" or "dataset".
	TableNamespace string `json:"table_namespace"`
	// DestinationNamespace is the kind of name the user gives when the
	// connector is a destination: "schema", "database", "dataset", "prefix"
	// or "path".
	DestinationNamespace string `json:"destination_namespace"`
	// DestinationDefault is the engine's own default namespace ("public" on
	// PostgreSQL), or "" when there is none and the user must name one.
	DestinationDefault string `json:"destination_default"`
}

// Defaults for a connector without a namespace_model (SaaS APIs) or a name no
// connector claims. They are what the old switches returned for an unknown type.
const (
	DefaultTableNamespace       = "schema"
	DefaultDestinationNamespace = "schema"
	DefaultDestinationDefault   = ""
)

// TableNamespaces and DestinationNamespaces are the values the block may use.
var (
	TableNamespaces       = []string{"schema", "database", "dataset"}
	DestinationNamespaces = []string{"schema", "database", "dataset", "prefix", "path"}
)

// Index maps Key(id) and Key(alias) of every connector that declares a
// namespace_model to its model, with empty fields already defaulted.
type Index map[string]Model

// Key folds a connector name the way callers spell it — "aurora_mysql",
// "Aurora-MySQL", "AWS S3" — onto one lookup key: lower case with spaces,
// hyphens and underscores removed. api-gateway's connectorKeyForConnResolution
// folds the same way.
func Key(name string) string {
	k := strings.ToLower(strings.TrimSpace(name))
	return strings.NewReplacer(" ", "", "-", "", "_", "").Replace(k)
}

// Load reads every connector under toolsDir. Blocks with an unknown value, and
// aliases claimed by two connectors, are reported and left out rather than
// guessed at.
func Load(toolsDir string) (Index, []string) {
	idx, _, problems := load(toolsDir)
	return idx, problems
}

// load is Load plus the keys whose connector names its table_namespace itself.
// Only those connectors hold tables in a listable namespace and ship a
// list_namespaces tool (the guard test requires one); an object store that
// leaves the field out still reads as "schema" through the defaults.
func load(toolsDir string) (Index, map[string]bool, []string) {
	idx := Index{}
	declared := map[string]bool{}
	var problems []string
	claimedBy := map[string]string{}
	type entry struct {
		id       string
		aliases  []string
		model    Model
		declared bool
	}
	var entries []entry
	for _, cr := range connectorpaths.IterConnectorRoots(toolsDir) {
		raw, err := os.ReadFile(cr.MetadataPath)
		if err != nil {
			continue
		}
		var md struct {
			Aliases        []string `json:"aliases"`
			NamespaceModel *Model   `json:"namespace_model"`
		}
		if err := json.Unmarshal(raw, &md); err != nil || md.NamespaceModel == nil {
			continue
		}
		m, err := normalize(*md.NamespaceModel)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", cr.ID, err))
			continue
		}
		entries = append(entries, entry{cr.ID, md.Aliases, m, strings.TrimSpace(md.NamespaceModel.TableNamespace) != ""})
	}
	// Ids first, so an alias can never take over another connector's id.
	for _, e := range entries {
		idx[Key(e.id)] = e.model
		declared[Key(e.id)] = e.declared
		claimedBy[Key(e.id)] = e.id
	}
	for _, e := range entries {
		for _, a := range e.aliases {
			k := Key(a)
			if k == "" {
				continue
			}
			if owner, ok := claimedBy[k]; ok {
				if owner != e.id {
					problems = append(problems, fmt.Sprintf("%s: alias %q is already %s's", e.id, a, owner))
				}
				continue
			}
			idx[k] = e.model
			declared[k] = e.declared
			claimedBy[k] = e.id
		}
	}
	sort.Strings(problems)
	return idx, declared, problems
}

func normalize(m Model) (Model, error) {
	m.TableNamespace = strings.ToLower(strings.TrimSpace(m.TableNamespace))
	m.DestinationNamespace = strings.ToLower(strings.TrimSpace(m.DestinationNamespace))
	m.DestinationDefault = strings.TrimSpace(m.DestinationDefault)
	if m.TableNamespace == "" {
		m.TableNamespace = DefaultTableNamespace
	}
	if m.DestinationNamespace == "" {
		m.DestinationNamespace = DefaultDestinationNamespace
	}
	if !contains(TableNamespaces, m.TableNamespace) {
		return m, fmt.Errorf("table_namespace %q is not one of %v", m.TableNamespace, TableNamespaces)
	}
	if !contains(DestinationNamespaces, m.DestinationNamespace) {
		return m, fmt.Errorf("destination_namespace %q is not one of %v", m.DestinationNamespace, DestinationNamespaces)
	}
	return m, nil
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

// The process-wide index is read lazily and re-read after cacheTTL, so a
// connector added to the mounted tree is picked up without a restart.
const cacheTTL = time.Minute

var (
	mu             sync.Mutex
	cached         Index
	cachedDeclared map[string]bool
	loadedAt       time.Time
	warned         = map[string]bool{}
)

func current() Index {
	idx, _ := currentWithDeclared()
	return idx
}

func currentWithDeclared() (Index, map[string]bool) {
	mu.Lock()
	defer mu.Unlock()
	if cached != nil && time.Since(loadedAt) < cacheTTL {
		return cached, cachedDeclared
	}
	dir := toolsDir()
	idx, declared, problems := load(dir)
	for _, p := range problems {
		warnOnce("namespace_model: " + p)
	}
	if len(idx) == 0 {
		warnOnce(fmt.Sprintf("namespace_model: no connector metadata found under %q; every connector reads as schema-based until the connector tree is mounted", dir))
		if cached != nil {
			// Keep the last good index through a transient read failure.
			loadedAt = time.Now()
			return cached, cachedDeclared
		}
	}
	cached, cachedDeclared, loadedAt = idx, declared, time.Now()
	return cached, cachedDeclared
}

func warnOnce(msg string) {
	if warned[msg] {
		return
	}
	warned[msg] = true
	log.Warn(msg)
}

// toolsDir is the shared connector tree: the one connectorpaths resolves in a
// container, else the nearest shared/mcp-connectors above the working
// directory (tests run from a package directory deep in the repo).
func toolsDir() string {
	if d := connectorpaths.ToolsDir(); d != "" {
		return d
	}
	wd, err := os.Getwd()
	if err != nil {
		return ""
	}
	for dir := wd; ; dir = filepath.Dir(dir) {
		c := filepath.Join(dir, "shared", "mcp-connectors")
		if info, err := os.Stat(c); err == nil && info.IsDir() {
			return c
		}
		if filepath.Dir(dir) == dir {
			return ""
		}
	}
}

// Lookup returns the model a connector type declares, or false when no
// connector claims the name or its connector declares none.
func Lookup(connectorType string) (Model, bool) {
	m, ok := current()[Key(connectorType)]
	return m, ok
}

// Defaults is the model of a name no connector claims.
func Defaults() Model {
	return Model{DefaultTableNamespace, DefaultDestinationNamespace, DefaultDestinationDefault}
}

// For returns the connector's model, or the defaults.
func For(connectorType string) Model {
	if m, ok := Lookup(connectorType); ok {
		return m
	}
	return Defaults()
}

// ListsNamespaces reports whether the connector names its table_namespace in
// its own namespace_model, which is what ships a list_namespaces tool. An
// object store (gcs, s3) or a SaaS API does not, so asking it for namespaces
// can only fail; callers answer "none" instead of calling the connector.
func ListsNamespaces(connectorType string) bool {
	_, declared := currentWithDeclared()
	return declared[Key(connectorType)]
}

// All returns a copy of the index, for the gateway endpoint the frontend reads.
func All() Index {
	idx := current()
	out := make(Index, len(idx))
	for k, v := range idx {
		out[k] = v
	}
	return out
}

// Listing returns the keys ListsNamespaces is true for, sorted, for the gateway
// endpoint the frontend reads.
func Listing() []string {
	_, declared := currentWithDeclared()
	out := make([]string, 0, len(declared))
	for k, lists := range declared {
		if lists {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
