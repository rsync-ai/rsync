package registry

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/connectorpaths"
)

// ConnectorCapabilities represents a connector's capabilities
type ConnectorCapabilities struct {
	ConnectorType               string                 `json:"connector_type"`
	ConnectorCategory           string                 `json:"connector_category"`
	DisplayName                 string                 `json:"display_name"`
	SupportsSource              bool                   `json:"supports_source"`
	SupportsDestination         bool                   `json:"supports_destination"`
	DefaultSourceOperation      string                 `json:"default_source_operation"`
	DefaultDestinationOperation string                 `json:"default_destination_operation"`
	Operations                  []Operation            `json:"operations"`
	Terminology                 map[string]string      `json:"terminology"`
	Features                    map[string]interface{} `json:"features,omitempty"`
	// SupportedModalities is the move-capability contract (universal-blob-passthrough
	// plan §2/§4): which KINDS of data the connector can move — "structured" (row
	// records, the universal default) and/or "blob" (opaque raw bytes). Distinct from
	// supported_formats (serialization encodings). The orchestrator capability gate
	// keys off this; surfacing it here turns CAPABILITY_MISMATCH from a runtime error
	// into a pre-flight UI signal (why a given source→dest pair is/ isn't allowed).
	// Defaults to ["structured"] for any connector that doesn't declare it.
	SupportedModalities []string `json:"supported_modalities"`
	// Internal connectors (like MinIO) are not visible to users in UI
	// They are used for system-internal operations (staging, buffering)
	Internal bool `json:"internal,omitempty"`
}

// Operation represents a connector operation
type Operation struct {
	Name        string `json:"name"`
	Type        string `json:"type"` // "core", "source", "destination"
	Description string `json:"description,omitempty"`
}

// CategoryDefaults represents category-level operation defaults
type CategoryDefaults struct {
	Category              string            `json:"category"`
	SourceOperations      []string          `json:"source_operations"`
	DestinationOperations []string          `json:"destination_operations"`
	DefaultSourceOp       string            `json:"default_source_op"`
	DefaultDestOp         string            `json:"default_dest_op"`
	Terminology           map[string]string `json:"terminology"`
}

// ConnectorRegistry provides capability discovery for MCP connectors
// This replaces ALL hardcoded connector type lists in agents
type ConnectorRegistry struct {
	db            *sql.DB
	dbDisabled    bool
	dbDisabledWhy string
	cache         map[string]*ConnectorCapabilities
	categoryCache map[string]*CategoryDefaults
	mu            sync.RWMutex
	cacheExpiry   time.Time
	cacheTTL      time.Duration
}

// NewConnectorRegistry creates a new connector registry
func NewConnectorRegistry(db *sql.DB) *ConnectorRegistry {
	reg := &ConnectorRegistry{
		db:            db,
		cache:         make(map[string]*ConnectorCapabilities),
		categoryCache: make(map[string]*CategoryDefaults),
		cacheTTL:      5 * time.Minute, // Cache for 5 minutes
	}

	// Pre-load cache
	reg.RefreshCache()

	return reg
}

// RefreshCache reloads all connector capabilities from database
func (r *ConnectorRegistry) RefreshCache() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.db == nil || r.dbDisabled {
		if r.dbDisabled {
			log.Warnf("⚠️  ConnectorRegistry: DB capability registry disabled (%s); loading MCP connector metadata from filesystem", r.dbDisabledWhy)
		} else {
			log.Warn("⚠️  ConnectorRegistry: No database connection, loading MCP connector metadata from filesystem")
		}
		if err := r.loadFromFilesystem(); err != nil {
			log.Warnf("⚠️  ConnectorRegistry: Failed to load from filesystem: %v (using minimal defaults)", err)
			r.loadDefaults()
		}
		r.cacheExpiry = time.Now().Add(r.cacheTTL)
		return nil
	}

	// If the DB registry tables are not present (common in this repo after cleanup),
	// avoid issuing queries that would spam Postgres logs with "relation ... does not exist".
	var regTable sql.NullString
	if err := r.db.QueryRow(`SELECT to_regclass('public.connector_registry')`).Scan(&regTable); err == nil {
		if !regTable.Valid || strings.TrimSpace(regTable.String) == "" {
			r.dbDisabled = true
			r.dbDisabledWhy = "connector_registry table not present"
			log.Warn("⚠️  ConnectorRegistry: connector_registry table not present; loading MCP connector metadata from filesystem")
			if fsErr := r.loadFromFilesystem(); fsErr != nil {
				log.Warnf("⚠️  ConnectorRegistry: Failed to load from filesystem: %v (using minimal defaults)", fsErr)
				r.loadDefaults()
			}
			r.cacheExpiry = time.Now().Add(r.cacheTTL)
			return nil
		}
	}

	// Load connectors
	rows, err := r.db.Query(`
		SELECT connector_type, connector_category, display_name,
		       supports_source, supports_destination,
		       default_source_operation, default_destination_operation,
		       capabilities
		FROM connector_registry
		WHERE status = 'active'
	`)
	if err != nil {
		// If the DB tables were removed by cleanup (or never created), stop trying on every cache refresh.
		// This prevents noisy Postgres errors like: relation "connector_registry" does not exist.
		msg := err.Error()
		if strings.Contains(msg, "connector_registry") && strings.Contains(msg, "does not exist") {
			r.dbDisabled = true
			r.dbDisabledWhy = "connector_registry table not present"
		}
		log.Warnf("⚠️  Failed to load connector registry from DB: %v, loading MCP connector metadata from filesystem", err)
		if fsErr := r.loadFromFilesystem(); fsErr != nil {
			log.Warnf("⚠️  ConnectorRegistry: Failed to load from filesystem: %v (using minimal defaults)", fsErr)
			r.loadDefaults()
		}
		r.cacheExpiry = time.Now().Add(r.cacheTTL)
		return nil
	}
	defer rows.Close()

	r.cache = make(map[string]*ConnectorCapabilities)
	for rows.Next() {
		var caps ConnectorCapabilities
		var defaultSourceOp, defaultDestOp sql.NullString
		var capabilitiesJSON []byte

		err := rows.Scan(
			&caps.ConnectorType, &caps.ConnectorCategory, &caps.DisplayName,
			&caps.SupportsSource, &caps.SupportsDestination,
			&defaultSourceOp, &defaultDestOp, &capabilitiesJSON,
		)
		if err != nil {
			log.Warnf("⚠️  Failed to scan connector: %v", err)
			continue
		}

		caps.DefaultSourceOperation = defaultSourceOp.String
		caps.DefaultDestinationOperation = defaultDestOp.String

		// Parse capabilities JSON
		if len(capabilitiesJSON) > 0 {
			var rawCaps map[string]interface{}
			if err := json.Unmarshal(capabilitiesJSON, &rawCaps); err == nil {
				// Extract operations
				if ops, ok := rawCaps["operations"].([]interface{}); ok {
					for _, op := range ops {
						if opMap, ok := op.(map[string]interface{}); ok {
							caps.Operations = append(caps.Operations, Operation{
								Name:        fmt.Sprintf("%v", opMap["name"]),
								Type:        fmt.Sprintf("%v", opMap["type"]),
								Description: fmt.Sprintf("%v", opMap["description"]),
							})
						}
					}
				}
				// Extract terminology
				if term, ok := rawCaps["terminology"].(map[string]interface{}); ok {
					caps.Terminology = make(map[string]string)
					for k, v := range term {
						caps.Terminology[k] = fmt.Sprintf("%v", v)
					}
				}
				// Extract features
				if features, ok := rawCaps["features"].(map[string]interface{}); ok {
					caps.Features = features
				}
			}
		}

		r.cache[caps.ConnectorType] = &caps
	}

	// Load categories
	catRows, err := r.db.Query(`
		SELECT category, source_operations, destination_operations,
		       default_source_op, default_dest_op, terminology
		FROM connector_categories
	`)
	if err == nil {
		defer catRows.Close()
		r.categoryCache = make(map[string]*CategoryDefaults)
		for catRows.Next() {
			var cat CategoryDefaults
			var sourceOpsJSON, destOpsJSON, terminologyJSON []byte

			err := catRows.Scan(
				&cat.Category, &sourceOpsJSON, &destOpsJSON,
				&cat.DefaultSourceOp, &cat.DefaultDestOp, &terminologyJSON,
			)
			if err != nil {
				continue
			}

			json.Unmarshal(sourceOpsJSON, &cat.SourceOperations)
			json.Unmarshal(destOpsJSON, &cat.DestinationOperations)

			var termMap map[string]interface{}
			if json.Unmarshal(terminologyJSON, &termMap) == nil {
				cat.Terminology = make(map[string]string)
				for k, v := range termMap {
					cat.Terminology[k] = fmt.Sprintf("%v", v)
				}
			}

			r.categoryCache[cat.Category] = &cat
		}
	}

	r.cacheExpiry = time.Now().Add(r.cacheTTL)
	log.Infof("✅ ConnectorRegistry loaded %d connectors and %d categories", len(r.cache), len(r.categoryCache))

	return nil
}

// getMCPConnectorsPath returns the directory that contains MCP connector folders.
//
// Delegates to connectorpaths.ToolsDir() so this package and the sentinel's
// connector health check cannot disagree about where the tree is. The candidate
// list used to be duplicated here verbatim; a reader resolving a path nobody
// else resolves is the shape of #807.
func getMCPConnectorsPath() string {
	return connectorpaths.ToolsDir()
}

// loadFromFilesystem loads connector capabilities from `metadata.json` in the MCP connectors directory.
// This makes capability resolution generic even when the DB tables aren't present.
func (r *ConnectorRegistry) loadFromFilesystem() error {
	toolsDir := getMCPConnectorsPath()
	if toolsDir == "" {
		return fmt.Errorf("MCP connectors path not found (set MCP_CONNECTORS_PATH)")
	}

	type tempMetadata struct {
		Name                string                 `json:"name"`
		ConnectorType       string                 `json:"connector_type"`
		DisplayName         string                 `json:"display_name"`
		Category            string                 `json:"category"`
		Internal            bool                   `json:"internal,omitempty"`
		Capabilities        interface{}            `json:"capabilities"`
		ConfigSchema        map[string]interface{} `json:"config_schema"`
		SupportsSource      bool                   `json:"supports_source"`
		SupportsDestination bool                   `json:"supports_destination"`
	}

	cache := make(map[string]*ConnectorCapabilities)

	// Connectors live at variable depth under the tools dir:
	//   public/<vendor>/metadata.json          (e.g. postgresql, shopify-admin-graphql)
	//   public/<category>/<vendor>/metadata.json  (e.g. database/mysql, storage/aws-s3)
	//   internal/<vendor>/metadata.json
	// We previously only walked the top level which silently produced
	// an empty cache for the entire `public/` subtree — the chat
	// pipeline then died at the resolver because nothing was registered.
	//
	// Walk the directory tree but skip cost-heavy or known-irrelevant
	// subdirs: per-version snapshots (`versions/`), Python build noise
	// (`__pycache__`), shared schemas, generators, oauth helpers.
	skipDirs := map[string]bool{
		"versions":     true,
		"__pycache__":  true,
		"schemas":      true,
		"generators":   true,
		"oauth":        true,
		"node_modules": true,
		"db_drivers":   true,
		".git":         true,
	}

	err := filepath.Walk(toolsDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			// Skip unreadable subtrees rather than aborting the whole load.
			return nil
		}
		if info.IsDir() {
			if skipDirs[info.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		// Discover connector roots via latest.json (the version pointer) and read
		// metadata from the canonical versions/<current_version>/metadata.json —
		// there are no root metadata.json copies anymore.
		if info.Name() != "latest.json" {
			return nil
		}
		metaPath, ok := connectorpaths.ResolveVersionedMetadataPath(filepath.Dir(path))
		if !ok {
			return nil
		}
		data, err := os.ReadFile(metaPath)
		if err != nil {
			return nil
		}
		var tm tempMetadata
		if err := json.Unmarshal(data, &tm); err != nil {
			return nil
		}

		// IMPORTANT: use canonical connector identifier for ConnectorType.
		// In metadata.json, `name` is a human-friendly label (e.g. "AWS S3"),
		// while `connector_type` is the stable ID used by MCP (e.g. "aws-s3").
		connType := tm.ConnectorType
		if connType == "" {
			// Best-effort fallback to the immediate parent dir name
			// (e.g. .../public/shopify-admin-graphql/metadata.json
			// gives us "shopify-admin-graphql"). Replace underscores
			// with dashes to keep the canonical key form stable.
			connType = strings.ReplaceAll(filepath.Base(filepath.Dir(path)), "_", "-")
		}
		connType = normalizeConnectorKey(connType)

		// Display name is what humans see in UI.
		display := tm.DisplayName
		if display == "" {
			display = tm.Name
		}
		if display == "" {
			display = connType
		}

		capsLocal := &ConnectorCapabilities{
			ConnectorType:       connType,
			ConnectorCategory:   tm.Category,
			DisplayName:         display,
			SupportsSource:      tm.SupportsSource,
			SupportsDestination: tm.SupportsDestination,
			Terminology:         extractTerminologyFromCapabilities(tm.Capabilities),
			Operations:          extractOperationsFromCapabilities(tm.Capabilities),
			SupportedModalities: extractModalitiesFromCapabilities(tm.Capabilities),
			Internal:            tm.Internal,
		}

		// If no terminology provided, fall back to category defaults (generic).
		if len(capsLocal.Terminology) == 0 {
			capsLocal.Terminology = defaultTerminologyForCategory(capsLocal.ConnectorCategory)
		}

		// Provide sensible defaults; not required for existence checks, but helpful.
		if capsLocal.DefaultSourceOperation == "" && capsLocal.SupportsSource {
			capsLocal.DefaultSourceOperation = "read"
		}
		if capsLocal.DefaultDestinationOperation == "" && capsLocal.SupportsDestination {
			capsLocal.DefaultDestinationOperation = "write"
		}

		cache[capsLocal.ConnectorType] = capsLocal
		return nil
	})

	if err != nil {
		return fmt.Errorf("walk connectors dir: %w", err)
	}

	if len(cache) == 0 {
		return fmt.Errorf("no connectors loaded from %s", toolsDir)
	}

	r.cache = cache
	// Keep any DB-loaded categoryCache; otherwise set empty.
	if r.categoryCache == nil {
		r.categoryCache = make(map[string]*CategoryDefaults)
	}
	r.cacheExpiry = time.Now().Add(r.cacheTTL)
	log.Infof("✅ ConnectorRegistry loaded %d connectors from filesystem (%s)", len(r.cache), toolsDir)
	return nil
}

func extractTerminologyFromCapabilities(capabilities interface{}) map[string]string {
	out := make(map[string]string)

	merge := func(m map[string]interface{}) {
		if term, ok := m["terminology"].(map[string]interface{}); ok {
			for k, v := range term {
				out[k] = fmt.Sprintf("%v", v)
			}
		}
	}

	switch v := capabilities.(type) {
	case map[string]interface{}:
		merge(v)
	case []interface{}:
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				merge(m)
			}
		}
	}

	return out
}

func extractOperationsFromCapabilities(capabilities interface{}) []Operation {
	var ops []Operation

	extract := func(m map[string]interface{}) {
		rawOps, ok := m["operations"].([]interface{})
		if !ok {
			return
		}
		for _, item := range rawOps {
			if opMap, ok := item.(map[string]interface{}); ok {
				ops = append(ops, Operation{
					Name:        fmt.Sprintf("%v", opMap["name"]),
					Type:        fmt.Sprintf("%v", opMap["type"]),
					Description: fmt.Sprintf("%v", opMap["description"]),
				})
			}
		}
	}

	switch v := capabilities.(type) {
	case map[string]interface{}:
		extract(v)
	case []interface{}:
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				extract(m)
			}
		}
	}

	return ops
}

// extractModalitiesFromCapabilities reads capabilities.supported_modalities (the
// move-capability contract — universal-blob-passthrough plan §2/§4). Mirrors the
// executor gate's modalitiesFromCapabilities reader. Defaults to ["structured"]
// when absent/empty so every connector declares at least the row lane.
func extractModalitiesFromCapabilities(capabilities interface{}) []string {
	var mods []string
	extract := func(m map[string]interface{}) {
		raw, ok := m["supported_modalities"].([]interface{})
		if !ok {
			return
		}
		for _, item := range raw {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				mods = append(mods, strings.TrimSpace(s))
			}
		}
	}
	switch v := capabilities.(type) {
	case map[string]interface{}:
		extract(v)
	case []interface{}:
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				extract(m)
			}
		}
	}
	if len(mods) == 0 {
		return []string{"structured"}
	}
	return mods
}

func defaultTerminologyForCategory(category string) map[string]string {
	switch category {
	case "relational_db":
		return map[string]string{"entity": "table", "record": "row", "field": "column"}
	case "document_db":
		return map[string]string{"entity": "collection", "record": "document", "field": "field"}
	case "cloud_storage":
		return map[string]string{"entity": "bucket", "record": "file", "field": "key"}
	case "data_warehouse":
		return map[string]string{"entity": "table", "record": "row", "field": "column"}
	default:
		return map[string]string{"entity": "entity", "record": "record", "field": "field"}
	}
}

// loadDefaults loads hardcoded defaults when database is unavailable
func (r *ConnectorRegistry) loadDefaults() {
	// These are fallbacks only - agents should query the database
	defaults := map[string]*ConnectorCapabilities{
		"mysql": {
			ConnectorType:               "mysql",
			ConnectorCategory:           "relational_db",
			DisplayName:                 "MySQL",
			SupportsSource:              true,
			SupportsDestination:         true,
			DefaultSourceOperation:      "export",
			DefaultDestinationOperation: "import",
			Terminology:                 map[string]string{"entity": "table", "record": "row", "field": "column"},
		},
		"postgresql": {
			ConnectorType:               "postgresql",
			ConnectorCategory:           "relational_db",
			DisplayName:                 "PostgreSQL",
			SupportsSource:              true,
			SupportsDestination:         true,
			DefaultSourceOperation:      "export",
			DefaultDestinationOperation: "import",
			Terminology:                 map[string]string{"entity": "table", "record": "row", "field": "column"},
		},
		"mongodb": {
			ConnectorType:               "mongodb",
			ConnectorCategory:           "document_db",
			DisplayName:                 "MongoDB",
			SupportsSource:              true,
			SupportsDestination:         true,
			DefaultSourceOperation:      "export",
			DefaultDestinationOperation: "import",
			Terminology:                 map[string]string{"entity": "collection", "record": "document", "field": "field"},
		},
		"aws-s3": {
			ConnectorType:               "aws-s3",
			ConnectorCategory:           "cloud_storage",
			DisplayName:                 "AWS S3",
			SupportsSource:              true,
			SupportsDestination:         true,
			DefaultSourceOperation:      "read",
			DefaultDestinationOperation: "import_data",
			Terminology:                 map[string]string{"entity": "bucket", "record": "file", "field": "key"},
			SupportedModalities:         []string{"structured", "blob"},
		},
		"snowflake": {
			ConnectorType:               "snowflake",
			ConnectorCategory:           "data_warehouse",
			DisplayName:                 "Snowflake",
			SupportsSource:              true,
			SupportsDestination:         true,
			DefaultSourceOperation:      "query",
			DefaultDestinationOperation: "load",
			Terminology:                 map[string]string{"entity": "table", "record": "row", "field": "column"},
		},
	}

	// Every connector moves at least structured records; normalize the fallback
	// defaults so the API never returns a null modality list (universal-blob-
	// passthrough plan §4).
	for _, c := range defaults {
		if len(c.SupportedModalities) == 0 {
			c.SupportedModalities = []string{"structured"}
		}
	}

	r.cache = defaults

	// Category defaults
	r.categoryCache = map[string]*CategoryDefaults{
		"relational_db": {
			Category:              "relational_db",
			SourceOperations:      []string{"export", "query", "discover_schema"},
			DestinationOperations: []string{"import", "execute"},
			DefaultSourceOp:       "export",
			DefaultDestOp:         "import",
			Terminology:           map[string]string{"entity": "table", "record": "row", "field": "column"},
		},
		"document_db": {
			Category:              "document_db",
			SourceOperations:      []string{"export", "query", "discover_schema"},
			DestinationOperations: []string{"import", "upsert"},
			DefaultSourceOp:       "export",
			DefaultDestOp:         "import",
			Terminology:           map[string]string{"entity": "collection", "record": "document", "field": "field"},
		},
		"cloud_storage": {
			Category:              "cloud_storage",
			SourceOperations:      []string{"read", "list", "discover_schema"},
			DestinationOperations: []string{"import_data", "write"},
			DefaultSourceOp:       "read",
			DefaultDestOp:         "import_data",
			Terminology:           map[string]string{"entity": "bucket", "record": "file", "field": "key"},
		},
		"data_warehouse": {
			Category:              "data_warehouse",
			SourceOperations:      []string{"query", "export", "discover_schema"},
			DestinationOperations: []string{"load", "merge"},
			DefaultSourceOp:       "query",
			DefaultDestOp:         "load",
			Terminology:           map[string]string{"entity": "table", "record": "row", "field": "column"},
		},
		"streaming": {
			Category:              "streaming",
			SourceOperations:      []string{"consume", "subscribe"},
			DestinationOperations: []string{"produce", "publish"},
			DefaultSourceOp:       "consume",
			DefaultDestOp:         "produce",
			Terminology:           map[string]string{"entity": "topic", "record": "message", "field": "field"},
		},
		"api_saas": {
			Category:              "api_saas",
			SourceOperations:      []string{"fetch", "list", "discover_schema"},
			DestinationOperations: []string{"push", "create", "update"},
			DefaultSourceOp:       "fetch",
			DefaultDestOp:         "push",
			Terminology:           map[string]string{"entity": "endpoint", "record": "record", "field": "field"},
		},
	}

	r.cacheExpiry = time.Now().Add(r.cacheTTL)
}

// GetCapabilities returns capabilities for a connector type.
//
// Vendor-prefix alias resolution: when the exact key misses, falls back
// to "<query>-*" matches in the cache. This lets chat-driven pipelines
// say "shopify" and resolve to "shopify-admin-graphql" without the user
// having to know the exact connector_type string.
//
// Without this, pipelines created from the chat NL flow died at the
// resolver with `No connection found for 'shopify'` even when the user
// had a perfectly valid "shopify-admin-graphql" connection — because
// the connector_resolver agent does an exact-match registry lookup
// before falling through to HITL.
//
// The fallback is strict (prefix + dash) to avoid false positives:
//
//   - GetCapabilities("shopify-admin-graphql") → exact match
//   - GetCapabilities("shopify")               → matches "shopify-admin-graphql"
//   - GetCapabilities("shop")                  → no match (too short)
//   - GetCapabilities("post")                  → no match (no "post-" connector)
//
// A 4-character minimum + dash boundary keeps "post" from matching
// "postgresql" or "p" from matching every connector.
func (r *ConnectorRegistry) GetCapabilities(connectorType string) *ConnectorCapabilities {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check cache expiry
	if time.Now().After(r.cacheExpiry) {
		go r.RefreshCache() // Async refresh
	}

	if caps, ok := r.cache[connectorType]; ok {
		return caps
	}

	// Vendor-prefix alias fallback. Normalise both sides so users typing
	// "Shopify" or "shopify_admin_graphql" still match the canonical
	// "shopify-admin-graphql" key.
	q := normalizeConnectorKey(connectorType)
	if len(q) >= 4 {
		for key, caps := range r.cache {
			normKey := normalizeConnectorKey(key)
			if normKey == q {
				return caps
			}
			if strings.HasPrefix(normKey, q+"-") {
				return caps
			}
		}
	}

	// Connector not found - return nil (agents should handle gracefully)
	log.Warnf("⚠️  Connector '%s' not found in registry", connectorType)
	return nil
}

func normalizeConnectorKey(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "_", "-")
	s = strings.ReplaceAll(s, " ", "-")
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	return s
}

// FindConnector attempts to resolve a user/LLM-provided identifier (e.g. "s3", "postgres", "aws_s3")
// into an actual connector_type present in the registry cache.
//
// Matching strategy (in order):
// 1) Exact match on connector_type (normalized)
// 2) Substring match on connector_type
// 3) Substring match on display_name
func (r *ConnectorRegistry) FindConnector(query string) (*ConnectorCapabilities, bool) {
	q := normalizeConnectorKey(query)
	if q == "" {
		return nil, false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	// 1) exact connector_type match
	for _, caps := range r.cache {
		if normalizeConnectorKey(caps.ConnectorType) == q {
			return caps, true
		}
	}

	// 2) substring match on connector_type
	for _, caps := range r.cache {
		if strings.Contains(normalizeConnectorKey(caps.ConnectorType), q) {
			return caps, true
		}
	}

	// 3) substring match on display name
	for _, caps := range r.cache {
		if strings.Contains(normalizeConnectorKey(caps.DisplayName), q) {
			return caps, true
		}
	}

	return nil, false
}

// GetSourceOperation returns the operation to use for a connector as source
func (r *ConnectorRegistry) GetSourceOperation(connectorType string) string {
	caps := r.GetCapabilities(connectorType)
	if caps != nil && caps.DefaultSourceOperation != "" {
		return caps.DefaultSourceOperation
	}

	// Fallback to category default
	if caps != nil {
		catDef := r.GetCategoryDefaults(caps.ConnectorCategory)
		if catDef != nil && catDef.DefaultSourceOp != "" {
			return catDef.DefaultSourceOp
		}
	}

	// Ultimate fallback
	return "export"
}

// GetDestinationOperation returns the operation to use for a connector as destination
func (r *ConnectorRegistry) GetDestinationOperation(connectorType string) string {
	caps := r.GetCapabilities(connectorType)
	if caps != nil && caps.DefaultDestinationOperation != "" {
		return caps.DefaultDestinationOperation
	}

	// Fallback to category default
	if caps != nil {
		catDef := r.GetCategoryDefaults(caps.ConnectorCategory)
		if catDef != nil && catDef.DefaultDestOp != "" {
			return catDef.DefaultDestOp
		}
	}

	// Ultimate fallback
	return "import"
}

// GetCategoryDefaults returns defaults for a connector category
func (r *ConnectorRegistry) GetCategoryDefaults(category string) *CategoryDefaults {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if cat, ok := r.categoryCache[category]; ok {
		return cat
	}
	return nil
}

// GetConnectorCategory returns the category for a connector type
func (r *ConnectorRegistry) GetConnectorCategory(connectorType string) string {
	caps := r.GetCapabilities(connectorType)
	if caps != nil {
		return caps.ConnectorCategory
	}

	// Try to infer category from connector name
	// This is a fallback for dynamically generated connectors
	switch connectorType {
	case "mysql", "postgresql", "postgres", "mariadb", "oracle", "sqlserver", "sqlite":
		return "relational_db"
	case "mongodb", "elasticsearch", "couchdb":
		return "document_db"
	case "aws-s3", "s3", "gcs", "azure_blob", "minio":
		return "cloud_storage"
	case "snowflake", "bigquery", "redshift", "databricks":
		return "data_warehouse"
	case "kafka", "kinesis", "pubsub", "eventhub":
		return "streaming"
	default:
		return "api_saas" // Default for unknown connectors
	}
}

// IsSourceSupported checks if a connector can be used as a source
func (r *ConnectorRegistry) IsSourceSupported(connectorType string) bool {
	caps := r.GetCapabilities(connectorType)
	if caps != nil {
		return caps.SupportsSource
	}
	return true // Default to true for unknown connectors
}

// IsDestinationSupported checks if a connector can be used as a destination
func (r *ConnectorRegistry) IsDestinationSupported(connectorType string) bool {
	caps := r.GetCapabilities(connectorType)
	if caps != nil {
		return caps.SupportsDestination
	}
	return true // Default to true for unknown connectors
}

// IsOperationSupported checks if an operation is supported for a connector
func (r *ConnectorRegistry) IsOperationSupported(connectorType, operation string) bool {
	caps := r.GetCapabilities(connectorType)
	if caps == nil {
		return true // Unknown connector - allow operation
	}

	// Check if operation is in the connector's operations list
	for _, op := range caps.Operations {
		if op.Name == operation {
			return true
		}
	}

	// Check category operations
	catDef := r.GetCategoryDefaults(caps.ConnectorCategory)
	if catDef != nil {
		for _, op := range catDef.SourceOperations {
			if op == operation {
				return true
			}
		}
		for _, op := range catDef.DestinationOperations {
			if op == operation {
				return true
			}
		}
	}

	// Core operations always supported
	coreOps := []string{"test_connection", "discover_schema", "validate_config", "get_capabilities"}
	for _, op := range coreOps {
		if op == operation {
			return true
		}
	}

	return false
}

// GetTerminology returns terminology for a connector
func (r *ConnectorRegistry) GetTerminology(connectorType string) map[string]string {
	caps := r.GetCapabilities(connectorType)
	if caps != nil && len(caps.Terminology) > 0 {
		return caps.Terminology
	}

	// Fallback to category
	if caps != nil {
		catDef := r.GetCategoryDefaults(caps.ConnectorCategory)
		if catDef != nil && len(catDef.Terminology) > 0 {
			return catDef.Terminology
		}
	}

	// Default terminology
	return map[string]string{
		"entity": "table",
		"record": "row",
		"field":  "column",
	}
}
