package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"
	"api-gateway/internal/services"
	"api-gateway/internal/telemetry"

	log "github.com/sirupsen/logrus"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rsync-ai/backend-orchestrator/pkg/namespacemodel"
	"go.temporal.io/sdk/client"
)

// KafkaProducer interface for sending messages
type KafkaProducer interface {
	SendPipelineRequest(topic string, traceID string, request map[string]interface{}) error
	SendPipelineRequestWithContext(ctx context.Context, topic string, traceID string, request map[string]interface{}) error
}

var kafkaProducer KafkaProducer

var (
	temporalMu        sync.Mutex
	temporalClient    client.Client
	temporalAddress   string
	temporalNamespace string
)

func normalizeSelectedTables(tables []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(tables))
	for _, v := range tables {
		s := strings.TrimSpace(v)
		if s == "" {
			continue
		}
		parts := strings.Split(s, ".")
		// Normalize common duplication patterns (e.g. "db.db.table").
		for len(parts) >= 2 && parts[0] != "" && parts[0] == parts[1] {
			parts = append(parts[:1], parts[2:]...)
		}
		s = strings.Join(parts, ".")
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

func extractSelectedTablesFromConfigJSON(configJSON []byte) []string {
	if len(configJSON) == 0 {
		return nil
	}
	var cfg map[string]interface{}
	if err := json.Unmarshal(configJSON, &cfg); err != nil || cfg == nil {
		return nil
	}
	raw, ok := cfg["selected_tables"]
	if !ok || raw == nil {
		return nil
	}
	var tables []string
	switch v := raw.(type) {
	case []interface{}:
		for _, it := range v {
			s := strings.TrimSpace(fmt.Sprint(it))
			if s != "" {
				tables = append(tables, s)
			}
		}
	case []string:
		for _, it := range v {
			s := strings.TrimSpace(it)
			if s != "" {
				tables = append(tables, s)
			}
		}
	default:
		return nil
	}
	return normalizeSelectedTables(tables)
}

// sourceSchemaCanonicalName returns the source connector's own internal default
// schema/database name (e.g. MySQL calls its default namespace "default", Postgres
// calls it "public", Shopify has no schema so we use "shopify"). This is the
// raw source-side value — call seedDestinationNamespace to get a value that is
// appropriate for a specific destination engine.
func sourceSchemaCanonicalName(connectorType string) string {
	ct := strings.ToLower(strings.TrimSpace(connectorType))
	switch ct {
	case "shopify-admin-graphql", "shopify", "shopify-admin":
		return "shopify"
	case "postgresql", "postgres", "pg":
		return "public"
	case "mysql", "mariadb":
		return "default"
	case "clickhouse":
		return "default"
	case "sqlite":
		return "main"
	}
	if ct == "" {
		return "default"
	}
	// Sanitise to a SQL-identifier-safe slug.
	var b strings.Builder
	for _, r := range ct {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else if r >= 'A' && r <= 'Z' {
			b.WriteRune(r + 32)
		} else if r == '-' || r == '.' || r == ' ' {
			b.WriteRune('_')
		}
	}
	out := b.String()
	if out == "" {
		return "default"
	}
	return out
}

// genericSourceDefaults is the set of names that are a source engine's
// internal "I have no meaningful schema identity" placeholder. These names are
// NOT useful as destination schema labels when the destination is a different
// engine — they must be translated to the destination's own default name.
// Source-specific labels (e.g. "shopify", "stripe", "hubspot") are never in
// this set and always pass through unchanged.
var genericSourceDefaults = map[string]struct{}{
	"default": {}, // MySQL, ClickHouse, ClickHouse Cloud
	"public":  {}, // PostgreSQL, Redshift
	"main":    {}, // SQLite
}

// destDefaultSchemaName returns the name the destination engine uses for its
// default schema/database/dataset: the destination_default of the connector's
// namespace_model block (pkg/namespacemodel). Returns "" for destinations that
// have no universal default (e.g. BigQuery datasets are user-defined) and for
// unknown types — callers should leave the field blank so the user is prompted
// to supply a name.
func destDefaultSchemaName(destConnType string) string {
	return namespacemodel.For(destConnType).DestinationDefault
}

// seedDestinationNamespace returns the recommended namespace label for a newly-
// created pipeline, given both source and destination connector types.
//
// Design rule:
//   - Source-specific labels ("shopify", "stripe", "hubspot", etc.) are always
//     meaningful in any destination and are returned unchanged.
//   - Generic engine-default names ("default" from MySQL/ClickHouse, "public"
//     from Postgres, "main" from SQLite) are NOT meaningful in a different
//     destination engine. They are translated to whatever the destination engine
//     calls its own default schema so the user sees the right value in the UI.
//   - When the destination has no universal default (BigQuery, object stores),
//     "" is returned so the HITL field starts empty and forces the user to type
//     a name.
//
// This is the single function callers should use — not sourceSchemaCanonicalName
// directly. Adding a new destination type only requires one entry in
// destDefaultSchemaName; nothing else changes.
func seedDestinationNamespace(srcConnType, destConnType string) string {
	// Object storage (#13): the namespace is the <db_or_schema> segment of the
	// object key, and left empty the writers fill it from the SOURCE database or
	// schema (the batch path from the source's database, the CDC sink from the
	// event's db). Any seed replaces that with a fixed name — for a sqlserver or
	// snowflake source the connector type below, so every database would share one
	// "sqlserver/" folder. The user can still type a path prefix.
	if namespaceKindForConnector(destConnType) == "path" {
		return ""
	}
	candidate := sourceSchemaCanonicalName(srcConnType)
	if _, isGeneric := genericSourceDefaults[strings.ToLower(candidate)]; isGeneric {
		// Always use the destination's own default name (may be "" for BigQuery
		// etc. — that is intentional: an empty pre-fill forces the user to type
		// a dataset name, which is correct for destinations with no universal default).
		return destDefaultSchemaName(destConnType)
	}
	// Non-database (SaaS / API) sources — Shopify, Stripe, HubSpot, etc. — have NO
	// meaningful schema/namespace to carry to the destination. Using the source
	// connector's canonical name (e.g. "shopify") as the destination namespace is
	// wrong: for a DB destination it becomes the target DATABASE, landing every row
	// in a source-named DB (e.g. mysql `shopify`) instead of the destination
	// connection's configured database. Defer to the destination's own default
	// (which downstream isRealNamespace() treats as "no namespace" → the connector
	// falls back to config["database"]). The user can still set an explicit
	// destination namespace via the table-selection HITL, which overrides this seed.
	if !isDBConnector(srcConnType) {
		return destDefaultSchemaName(destConnType)
	}
	return candidate
}

// resolveDestinationNamespace computes the final destination namespace for a
// newly-created pipeline. If the user supplied an explicit value it is used
// verbatim. Otherwise we pick a destination-aware default via
// seedDestinationNamespace, which translates generic source-engine defaults
// ("default", "public", "main") to the destination engine's equivalent.
// Best-effort: any DB errors fall back to the unsuffixed candidate (the
// destination's `_rsync_pipelines` ownership check will catch real collisions
// at first-write time).
func resolveDestinationNamespace(database *sql.DB, pipelineID, sourceConnectorType, destConnectorType, destinationConnectionID string, override *string) string {
	if override != nil {
		s := strings.TrimSpace(*override)
		if s != "" {
			return s
		}
	}
	// This returns the bare default and probes NOTHING, on purpose. The old
	// creation-time collision check was control-plane-only and emitted
	// false-positives — a user who manually wiped the destination schema still got
	// flagged, because stale `pipelines.config.destination_namespace` rows live on
	// in pipeline_db regardless of what the destination actually holds.
	//
	// Collision detection now lives where the answer is knowable: first run, against
	// the LIVE destination, once the selected tables are known —
	// resolveFirstRunNamespace → probeNamespaceCollision, which connects to the
	// destination and asks it. The result is frozen into
	// config.destination_namespace_locked so it is decided exactly once per pipeline.
	// Creation only has to seed a sensible starting name.
	return seedDestinationNamespace(sourceConnectorType, destConnectorType)
}

// namespaceKindForConnector reports what a "namespace" means for a given
// destination engine, so the destination-mapping HITL can label its field
// correctly (PR-C): the destination_namespace of the connector's
// namespace_model block — "schema", "database", "dataset", "prefix" or "path".
// Unknown types default to "schema", the most common relational case.
func namespaceKindForConnector(connectorType string) string {
	return namespacemodel.For(connectorType).DestinationNamespace
}

// DestinationConfig is the per-pipeline destination mapping persisted under
// pipelines.config.destination_config (JSONB). It is ALWAYS pipeline-scoped —
// never written to the connection config — so one pipeline's namespace choice
// can never bleed into another (the PR #130 invariant). Namespace stays mirrored
// to the legacy config.destination_namespace string for executor back-compat.
type DestinationConfig struct {
	Namespace         string `json:"namespace"`
	NamespaceKind     string `json:"namespace_kind"`
	CreateIfNotExists bool   `json:"create_if_not_exists"`
	// SchemaMode, when "preserve"/"mirror"/"flatten", is an explicit request-time
	// override for how source schemas map to the destination. The table-selection
	// HITL sends "preserve" (with an empty Namespace) when a multi-schema
	// selection leaves the namespace blank; the handler persists it to the
	// top-level config.destination_schema_mode key that the executor reads FIRST
	// (executor.preserveSourceSchemaLayout), so mirroring holds even when the
	// seeded destination namespace is not an engine default. Not persisted into
	// the destination_config object itself (omitempty keeps it out of the JSONB).
	SchemaMode string `json:"schema_mode,omitempty"`
}

// persistDestinationConfig writes both the structured destination_config object
// and the legacy destination_namespace string in a single statement, keeping
// them in lockstep. Best-effort: a failure is logged, not fatal — the run-time
// resolver falls back to the source-derived default.
func persistDestinationConfig(database *sql.DB, pipelineID string, cfg DestinationConfig) error {
	raw, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	_, err = database.Exec(`
		UPDATE pipelines
		SET config = jsonb_set(
		        jsonb_set(COALESCE(config, '{}'::jsonb), '{destination_namespace}', to_jsonb($2::text), true),
		        '{destination_config}', $3::jsonb, true),
		    updated_at = NOW()
		WHERE id = $1
	`, pipelineID, cfg.Namespace, string(raw))
	return err
}

// persistResolvedDestinationConfig writes the destination config + legacy
// namespace string AND sets destination_namespace_locked, all in one statement.
// Used at first run after table-level namespace resolution: once locked, the
// namespace is frozen so reloads / incremental / scheduled runs reuse the same
// namespace + tables and NEVER re-prompt or re-prefix (Fivetran/Airbyte pin
// semantics). cfg.Namespace must already be the RESOLVED value.
func persistResolvedDestinationConfig(database *sql.DB, pipelineID string, cfg DestinationConfig) error {
	raw, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	_, err = database.Exec(`
		UPDATE pipelines
		SET config = jsonb_set(
		        jsonb_set(
		            jsonb_set(COALESCE(config, '{}'::jsonb), '{destination_namespace}', to_jsonb($2::text), true),
		            '{destination_config}', $3::jsonb, true),
		        '{destination_namespace_locked}', 'true'::jsonb, true),
		    updated_at = NOW()
		WHERE id = $1
	`, pipelineID, cfg.Namespace, string(raw))
	return err
}

// destinationNamespaceLock reports whether this pipeline's destination namespace
// has already been resolved + locked, and returns the locked namespace. When
// locked, callers MUST reuse the returned namespace verbatim and skip first-run
// resolution. Fail-soft: any read error returns (false, "") so a transient DB
// blip degrades to "treat as first run" rather than crashing the resume path.
func destinationNamespaceLock(database *sql.DB, pipelineID string) (locked bool, namespace string) {
	if database == nil || strings.TrimSpace(pipelineID) == "" {
		return false, ""
	}
	var lockedNull sql.NullBool
	var nsNull sql.NullString
	err := database.QueryRow(`
		SELECT
		    COALESCE((config->>'destination_namespace_locked')::bool, false),
		    NULLIF(TRIM(COALESCE(config->>'destination_namespace','')), '')
		FROM pipelines WHERE id = $1::uuid
	`, pipelineID).Scan(&lockedNull, &nsNull)
	if err != nil {
		return false, ""
	}
	return lockedNull.Valid && lockedNull.Bool, nsNull.String
}

// destinationConnIDAndType returns the pipeline's destination connection id and
// its connector_type, for the live first-run namespace collision probe. Returns
// ("","") on any miss so callers degrade to "lock the chosen namespace without a
// collision probe" rather than failing the resume.
func destinationConnIDAndType(database *sql.DB, pipelineID string) (connID, connType string) {
	if database == nil || strings.TrimSpace(pipelineID) == "" {
		return "", ""
	}
	var idNull, typeNull sql.NullString
	err := database.QueryRow(`
		SELECT c.id::text, c.connector_type
		FROM pipelines p
		JOIN connections c ON c.id = p.destination_connection_id
		WHERE p.id = $1::uuid
	`, pipelineID).Scan(&idNull, &typeNull)
	if err != nil {
		return "", ""
	}
	return strings.TrimSpace(idNull.String), strings.TrimSpace(typeNull.String)
}

func getLatestPipelineSchedule(database *sql.DB, pipelineID string) (status string, scheduleType string, ok bool) {
	if database == nil || strings.TrimSpace(pipelineID) == "" {
		return "", "", false
	}
	var st sql.NullString
	var typ sql.NullString
	err := database.QueryRow(`
		SELECT status, schedule_type
		FROM pipeline_schedules
		WHERE pipeline_id = $1 AND status != 'deleted'
		ORDER BY created_at DESC
		LIMIT 1
	`, pipelineID).Scan(&st, &typ)
	if err != nil {
		return "", "", false
	}
	if st.Valid {
		status = strings.TrimSpace(st.String)
	}
	if typ.Valid {
		scheduleType = strings.TrimSpace(typ.String)
	}
	return status, scheduleType, strings.TrimSpace(status) != ""
}

func getSourceConnectionModes(database *sql.DB, sourceConnID string) (syncMode string, cdcMode string) {
	if database == nil || strings.TrimSpace(sourceConnID) == "" {
		return "", ""
	}
	var sm sql.NullString
	var cm sql.NullString
	_ = database.QueryRow(`
		SELECT sync_mode, cdc_mode
		FROM connections
		WHERE id = $1
	`, sourceConnID).Scan(&sm, &cm)
	if sm.Valid {
		syncMode = strings.ToLower(strings.TrimSpace(sm.String))
	}
	if cm.Valid {
		cdcMode = strings.ToLower(strings.TrimSpace(cm.String))
	}
	return syncMode, cdcMode
}

func computeDataLoadingStrategy(database *sql.DB, p *Pipeline) *DataLoadingStrategy {
	if p == nil {
		return nil
	}

	// Defaults
	dataset := ""
	if p.Dataset != nil {
		dataset = strings.TrimSpace(*p.Dataset)
	}
	rerunDefault := "resume"
	if p.DefaultRunMode != nil && strings.TrimSpace(*p.DefaultRunMode) != "" {
		rerunDefault = strings.ToLower(strings.TrimSpace(*p.DefaultRunMode))
	}
	if rerunDefault != "resume" && rerunDefault != "reload" {
		rerunDefault = "resume"
	}

	// Effective mode resolution: pipeline override > pipeline cdc_mode > source connection default.
	sourceConnID := strings.TrimSpace(p.SourceConnectionID)
	connSyncMode, connCDCMode := getSourceConnectionModes(database, sourceConnID)

	effectiveSyncMode := resolveEffectiveSyncMode(database, p.ID, sourceConnID, p.SyncMode, p.CDCMode)
	effectiveCDCMode := ""
	if effectiveSyncMode == "cdc" {
		// A CDC mode is only meaningful for a CDC pipeline: batch pipelines (and
		// batch source connections) carry the cdc_mode='initial' column default,
		// which used to surface here as a CDC option on a batch pipeline.
		if p.CDCMode != nil && strings.TrimSpace(*p.CDCMode) != "" {
			effectiveCDCMode = strings.ToLower(strings.TrimSpace(*p.CDCMode))
		} else if connCDCMode != "" {
			effectiveCDCMode = connCDCMode
		}
	}

	// Schedule presence (for batch pipelines)
	scheduleStatus, scheduleType, hasSchedule := getLatestPipelineSchedule(database, p.ID)

	strategy := &DataLoadingStrategy{
		Mode:              effectiveSyncMode,
		Dataset:           dataset,
		RerunDefault:      rerunDefault,
		SelectedTables:    p.SelectedTables,
		ScheduleStatus:    scheduleStatus,
		ScheduleType:      scheduleType,
		EffectiveCDCMode:  effectiveCDCMode,
		EffectiveSyncMode: effectiveSyncMode,
		Evidence: map[string]interface{}{
			"pipeline_sync_mode":        safeDeref(p.SyncMode),
			"pipeline_cdc_mode":         safeDeref(p.CDCMode),
			"pipeline_default_run_mode": safeDeref(p.DefaultRunMode),
			"pipeline_dataset":          dataset,
			"source_connection_id":      sourceConnID,
			"source_connection_sync":    connSyncMode,
			"source_connection_cdc":     connCDCMode,
			"has_schedule":              hasSchedule,
			"schedule_status":           scheduleStatus,
			"schedule_type":             scheduleType,
		},
	}

	// Normalize mode
	if effectiveSyncMode != "cdc" && effectiveSyncMode != "batch" {
		effectiveSyncMode = "batch"
		strategy.Mode = "batch"
		strategy.EffectiveSyncMode = "batch"
	}

	if effectiveSyncMode == "cdc" {
		strategy.OngoingSync = "cdc_stream"
		if strings.EqualFold(strings.TrimSpace(effectiveCDCMode), "streaming_only") {
			strategy.InitialLoad = "none"
		} else {
			strategy.InitialLoad = "full_snapshot"
		}
	} else {
		strategy.InitialLoad = "full_snapshot"
		if hasSchedule && strings.EqualFold(strings.TrimSpace(scheduleStatus), "active") {
			strategy.OngoingSync = "scheduled_batch"
		} else {
			strategy.OngoingSync = "manual_batch"
		}
	}

	strategy.ExplanationSteps = dataLoadingStrategySteps(effectiveSyncMode, effectiveCDCMode, scheduleStatus, rerunDefault, dataset)
	return strategy
}

// dataLoadingStrategySteps builds the strategy card's explanation lines. The
// frontend builds the same lines itself when a strategy arrives without them
// (computeStrategySteps in frontend/src/lib/pipeline/dataLoadingStrategy.ts), so
// both are pinned to shared/data_loading_strategy_golden.json
// (data_loading_strategy_golden_test.go and dataLoadingStrategyGolden.test.ts).
// Inputs are normalised here the way the frontend normalises them.
func dataLoadingStrategySteps(syncMode, cdcMode, scheduleStatus, rerunDefault, dataset string) []string {
	norm := func(s string) string { return strings.ToLower(strings.TrimSpace(s)) }
	dataset = strings.TrimSpace(dataset)

	steps := make([]string, 0, 4)
	if norm(syncMode) == "cdc" {
		if norm(cdcMode) == "streaming_only" {
			steps = append(steps, "Backfill: Skip historical backfill (start streaming new changes only)")
		} else {
			steps = append(steps, "Backfill: Take an initial snapshot of selected tables")
		}
		steps = append(steps, "Keep in sync: Stream INSERT/UPDATE/DELETE continuously (CDC)")
		steps = append(steps, "Reruns: CDC is continuous; use Restart CDC to recover if needed")
	} else {
		steps = append(steps, "Backfill: Copy selected tables in batch runs (historical snapshot)")
		if norm(scheduleStatus) == "active" {
			steps = append(steps, "Keep in sync: Runs automatically on schedule")
		} else {
			steps = append(steps, "Keep in sync: Runs when you click Run")
		}
		if norm(rerunDefault) == "reload" {
			steps = append(steps, "Reruns: Default is Reload (rebuild from scratch)")
		} else {
			steps = append(steps, "Reruns: Default is Resume (continue from last checkpoint)")
		}
	}
	if dataset != "" {
		// Plain quotes, not %q: this is display text, and %q's Go escaping showed a
		// dataset with a quote or backslash differently from the frontend's copy.
		steps = append(steps, `Destination layout: Write under dataset "`+dataset+`"`)
	}
	return steps
}

func safeDeref(p *string) string {
	if p == nil {
		return ""
	}
	return strings.TrimSpace(*p)
}

// inferSelectedTablesFromLastRun tries to infer tables from the latest successful/stopped execution
// by reading pipeline_run_table_stats (best-effort). This makes sticky selection work for older
// pipelines where selected_tables was never persisted.
func inferSelectedTablesFromLastRun(database *sql.DB, pipelineID string) []string {
	if database == nil || strings.TrimSpace(pipelineID) == "" {
		return nil
	}
	// Prefer table-stats as the source of truth for "what tables did we actually run?"
	// This avoids relying on executions.status, which can vary (completed/stopped/success).
	var lastExecID string
	_ = database.QueryRow(`
		SELECT execution_id
		FROM pipeline_run_table_stats
		WHERE pipeline_id = $1
		ORDER BY updated_at DESC NULLS LAST, completed_at DESC NULLS LAST
		LIMIT 1
	`, pipelineID).Scan(&lastExecID)

	// Fallback: try executions table for a last finished run.
	if strings.TrimSpace(lastExecID) == "" {
		_ = database.QueryRow(`
			SELECT id
			FROM executions
			WHERE pipeline_id = $1 AND status IN ('completed', 'stopped', 'success')
			ORDER BY COALESCE(end_time, start_time) DESC, start_time DESC
			LIMIT 1
		`, pipelineID).Scan(&lastExecID)
	}

	if strings.TrimSpace(lastExecID) == "" {
		return nil
	}

	rows, err := database.Query(`
		SELECT DISTINCT qualified_name
		FROM pipeline_run_table_stats
		WHERE pipeline_id = $1 AND execution_id = $2
		ORDER BY qualified_name
		LIMIT 10000
	`, pipelineID, lastExecID)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var q sql.NullString
		if err := rows.Scan(&q); err != nil {
			continue
		}
		if q.Valid {
			s := strings.TrimSpace(q.String)
			if s != "" {
				out = append(out, s)
			}
		}
	}
	return normalizeSelectedTables(out)
}

// slugifyPipelineName converts a pipeline name to a URL/filesystem-safe slug
// Used for auto-generating dataset namespaces for cloud storage paths
func slugifyPipelineName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}

	// Convert to lowercase
	name = strings.ToLower(name)

	// Replace common separators with hyphens
	name = strings.ReplaceAll(name, " ", "-")
	name = strings.ReplaceAll(name, "_", "-")

	// Keep only alphanumeric and hyphens
	var result strings.Builder
	lastWasHyphen := false
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			result.WriteRune(r)
			lastWasHyphen = false
		} else if r == '-' && !lastWasHyphen && result.Len() > 0 {
			result.WriteRune('-')
			lastWasHyphen = true
		}
	}

	// Trim trailing hyphens and limit length
	slug := strings.TrimRight(result.String(), "-")
	if len(slug) > 63 {
		slug = slug[:63]
		slug = strings.TrimRight(slug, "-")
	}
	return slug
}

// SetKafkaProducer sets the Kafka producer instance
func SetKafkaProducer(producer KafkaProducer) {
	kafkaProducer = producer
}

// SetTemporalClient sets the Temporal client instance
func SetTemporalClient(tc client.Client) {
	temporalMu.Lock()
	defer temporalMu.Unlock()
	temporalClient = tc
}

// SetTemporalConfig records the connection parameters so the client can be
// lazily (re)created on demand. This matters when api-gateway boots before
// Temporal is accepting connections: the startup Dial fails, temporalClient
// stays nil, and without lazy reconnect every pipeline run silently falls
// through to a dead Kafka topic. See getTemporalClient().
func SetTemporalConfig(address, namespace string) {
	temporalMu.Lock()
	defer temporalMu.Unlock()
	temporalAddress = address
	temporalNamespace = namespace
}

// getTemporalClient returns a live Temporal client, lazily dialing if the
// startup connection failed or was never established (e.g. api-gateway won the
// boot race against Temporal). Returns nil only when a fresh dial also fails —
// callers MUST treat nil as "orchestration unavailable" and fail loudly rather
// than silently publishing to an unconsumed Kafka topic.
func getTemporalClient() client.Client {
	temporalMu.Lock()
	defer temporalMu.Unlock()
	if temporalClient != nil {
		return temporalClient
	}
	if temporalAddress == "" {
		return nil
	}
	tc, err := client.Dial(client.Options{
		HostPort:  temporalAddress,
		Namespace: temporalNamespace,
	})
	if err != nil {
		log.Warnf("⚠️  Lazy Temporal client dial failed (address=%s): %v", temporalAddress, err)
		return nil
	}
	log.Infof("✅ Temporal client lazily initialized: %s", temporalAddress)
	temporalClient = tc
	return tc
}

// Pipeline represents a data pipeline
type Pipeline struct {
	ID                           string                      `json:"id"`
	Name                         string                      `json:"name"`
	Description                  string                      `json:"description"`
	Status                       string                      `json:"status"`      // pending, running, completed, failed
	Source                       string                      `json:"source"`      // Legacy field
	Destination                  string                      `json:"destination"` // Legacy field
	SourceConnectionID           string                      `json:"source_connection_id,omitempty"`
	DestinationConnectionID      string                      `json:"destination_connection_id,omitempty"`
	SourceConnection             *Connection                 `json:"source_connection,omitempty"`      // Populated on GET
	DestinationConnection        *Connection                 `json:"destination_connection,omitempty"` // Populated on GET
	SourceConnectorSnapshot      *services.ConnectorSnapshot `json:"source_connector_snapshot,omitempty" gorm:"type:jsonb"`
	DestinationConnectorSnapshot *services.ConnectorSnapshot `json:"destination_connector_snapshot,omitempty" gorm:"type:jsonb"`
	SyncMode                     *string                     `json:"sync_mode,omitempty"`        // Optional pipeline-level override
	CDCMode                      *string                     `json:"cdc_mode,omitempty"`         // Optional pipeline-level override
	SyncModeSource               *string                     `json:"sync_mode_source,omitempty"` // Tracks origin of sync_mode
	CDCInitialLoad               *string                     `json:"cdc_initial_load,omitempty"` // "batch" (hybrid resumable backfill) | "debezium" (default snapshot); NULL = debezium
	// Dataset namespace for stable cloud storage paths: {prefix}/{dataset}/{db}/{table}/dt=.../
	// Auto-derived from pipeline name if not specified. Must be unique per destination connection.
	Dataset *string `json:"dataset,omitempty"`
	// Default run mode: "resume" (continue from checkpoints) or "reload" (rebuild from scratch)
	DefaultRunMode *string `json:"default_run_mode,omitempty"`
	// Destination namespace (schema/database) used when writing to the destination.
	// Resolved at create time (user override or auto-picked + collision-suffixed)
	// and persisted into pipelines.config.destination_namespace.
	DestinationNamespace *string `json:"destination_namespace,omitempty"`
	// DestinationConfig is the structured per-pipeline destination mapping
	// (namespace + kind + create-if-not-exists) the destination-mapping HITL
	// edits. Persisted under pipelines.config.destination_config (PR-C).
	DestinationConfig *DestinationConfig `json:"destination_config,omitempty"`
	// Selected tables persisted at pipeline level (pipelines.config.selected_tables).
	// This enables the UI to preselect tables for "Edit tables" on older pipelines.
	SelectedTables []string `json:"selected_tables,omitempty"`
	// DataLoadingStrategy is a server-derived, human-readable summary of how data will be loaded.
	// It is computed from sync_mode/cdc_mode/default_run_mode, schedule presence, and connection defaults.
	DataLoadingStrategy *DataLoadingStrategy `json:"data_loading_strategy,omitempty"`
	CreatedAt           time.Time            `json:"created_at"`
	UpdatedAt           time.Time            `json:"updated_at"`
	CreatedBy           string               `json:"created_by"`
	RowsProcessed       int64                `json:"rows_processed"`
	Cost                float64              `json:"cost"`
	Duration            int                  `json:"duration_seconds"`
	// F-16: latest execution row, populated by GetPipeline's LEFT JOIN against
	// latest_exec. Always serialised — `null` when no execution exists yet so
	// callers can distinguish "no run" from "field missing".
	LastExecution json.RawMessage `json:"last_execution"`
}

// DataLoadingStrategy is a compact explanation the UI can render consistently across pages.
// It is intentionally simple (strings + steps) so older UIs can ignore it safely.
type DataLoadingStrategy struct {
	Mode              string                 `json:"mode"` // batch|cdc
	InitialLoad       string                 `json:"initial_load,omitempty"`
	OngoingSync       string                 `json:"ongoing_sync,omitempty"`
	RerunDefault      string                 `json:"rerun_default,omitempty"` // resume|reload (batch), may be empty for cdc
	Dataset           string                 `json:"dataset,omitempty"`
	ExplanationSteps  []string               `json:"explanation_steps,omitempty"`
	Evidence          map[string]interface{} `json:"evidence,omitempty"`
	SelectedTables    []string               `json:"selected_tables,omitempty"`
	ScheduleStatus    string                 `json:"schedule_status,omitempty"`
	ScheduleType      string                 `json:"schedule_type,omitempty"`
	EffectiveCDCMode  string                 `json:"effective_cdc_mode,omitempty"`
	EffectiveSyncMode string                 `json:"effective_sync_mode,omitempty"`
}

// CreatePipelineRequest represents a request to create a pipeline
type CreatePipelineRequest struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
	Request     string `json:"request" binding:"required"` // Natural language request
	Source      string `json:"source"`                     // Legacy
	Destination string `json:"destination"`                // Legacy
	// NEW: Can specify either name or UUID (automatically resolved)
	SourceConnection      string `json:"source_connection"`      // Name or UUID
	DestinationConnection string `json:"destination_connection"` // Name or UUID
	// Legacy UUID fields (still supported)
	SourceConnectionID      string `json:"source_connection_id"`
	DestinationConnectionID string `json:"destination_connection_id"`
	// Phase 2 pre-flight: connector_type may be specified without a
	// specific connection. The ConnectionAgent then handles 0 / 1 / >1
	// connection cases via HITL prompts.
	SourceConnectorType      string `json:"source_connector_type,omitempty"`
	DestinationConnectorType string `json:"destination_connector_type,omitempty"`
	// Pipeline-level sync mode overrides
	SyncMode       *string `json:"sync_mode"`        // Optional: override connection default
	CDCMode        *string `json:"cdc_mode"`         // Optional: override connection default
	SyncModeSource *string `json:"sync_mode_source"` // Optional: track origin of setting
	// Optional CDC historical-load strategy: "batch" (hybrid resumable backfill) |
	// "debezium" (default snapshot). Unset/invalid → NULL → default Debezium snapshot.
	CDCInitialLoad *string `json:"cdc_initial_load,omitempty"`
	// Dataset namespace for cloud storage paths (auto-derived from name if not specified)
	Dataset *string `json:"dataset,omitempty"`
	// Default run mode for executions: "resume" or "reload"
	DefaultRunMode *string `json:"default_run_mode,omitempty"`
	// Tables to sync, persisted into pipelines.config.selected_tables. When set
	// at create time the run path uses them directly (no separate update call).
	SelectedTables []string `json:"selected_tables,omitempty"`
	// Optional destination namespace override (schema/database name in the
	// destination engine). Empty = auto-pick from source connector default
	// with collision-avoidance suffix.
	DestinationNamespace *string `json:"destination_namespace,omitempty"`
}

// PipelineListItem represents a pipeline in the list view with all joined data
type PipelineListItem struct {
	ID                    string            `json:"id"`
	Name                  string            `json:"name"`
	Description           string            `json:"description"`
	PipelineStatus        string            `json:"pipeline_status"`
	CreatedAt             time.Time         `json:"created_at"`
	UpdatedAt             time.Time         `json:"updated_at"`
	CreatedBy             string            `json:"created_by"`
	SyncMode              *string           `json:"sync_mode,omitempty"`
	CDCMode               *string           `json:"cdc_mode,omitempty"`
	SourceConnection      json.RawMessage   `json:"source_connection"`
	DestinationConnection json.RawMessage   `json:"destination_connection"`
	Schedule              json.RawMessage   `json:"schedule"`
	LastExecution         json.RawMessage   `json:"last_execution"`
	DerivedStatus         string            `json:"derived_status"`
	StatusContext         map[string]string `json:"status_context,omitempty"`
}

// scheduleStatusHint renders the one-line schedule hint the pipelines list shows
// under a row's status badge ("cron: 0 9 * * 1", "every 15m"), or "" when there is
// nothing useful to say.
//
// It returns "" for a failed pipeline on purpose. That single line is shared: the
// list renders this hint when present and only falls back to the last run's error
// message when it is absent (PipelinesTable.tsx). An unconditional hint therefore
// hid the failure reason of every scheduled pipeline behind its own cron string —
// on the exact surface an operator scans to find what is broken. A cron expression
// is worth saying while a pipeline is healthy; the moment it fails, the error wins.
func scheduleStatusHint(scheduleJSON, derivedStatus string) string {
	if derivedStatus == "failed" {
		return ""
	}

	var sched map[string]interface{}
	if err := json.Unmarshal([]byte(scheduleJSON), &sched); err != nil {
		return ""
	}
	if strings.ToLower(fmt.Sprintf("%v", sched["status"])) != "active" {
		return ""
	}

	stype := strings.ToLower(fmt.Sprintf("%v", sched["schedule_type"]))
	spec, _ := sched["schedule_spec"].(map[string]interface{})

	hint := "Scheduled"
	switch stype {
	case "cron":
		if cron, ok := spec["cron"].(string); ok && cron != "" {
			hint = fmt.Sprintf("cron: %s", cron)
		}
	case "interval":
		// schedule_spec may store numbers as float64 via json.Unmarshal
		if v, ok := spec["every_seconds"].(float64); ok && v > 0 {
			secs := int64(v)
			hint = fmt.Sprintf("every %ds", secs)
			if secs%60 == 0 {
				hint = fmt.Sprintf("every %dm", secs/60)
			}
			if secs%3600 == 0 {
				hint = fmt.Sprintf("every %dh", secs/3600)
			}
			if secs%86400 == 0 {
				hint = fmt.Sprintf("every %dd", secs/86400)
			}
		}
	}

	if tz := fmt.Sprintf("%v", sched["timezone"]); tz != "" && tz != "<nil>" && tz != "UTC" {
		hint = fmt.Sprintf("%s (%s)", hint, tz)
	}
	return hint
}

// pipelineDerivedStatusCaseSQL is the ONE definition of a pipeline's list badge
// (derived_status). It is spliced into three queries that used to carry their own
// hand-copied CASE and had drifted apart: the list page query, the list COUNT
// query (so a ?status= filter's total matches the rows it pages over) and the
// "Running" stat card in GetPipelineStats. The COUNT copy had lost the first
// branch entirely, so a status filter could count a pipeline the page badged
// differently.
//
// Every query splicing it must expose the same aliases:
//
//	p  = pipelines
//	pp = pipeline_progress        (LEFT JOIN, one row per pipeline)
//	le = latest executions row    (LEFT JOIN; execution_id, execution_status,
//	                               started_at, completed_at)
//	so = latest live schedule     (LEFT JOIN; schedule_id, schedule_status)
//
// CDC vs the finished snapshot run (issue #6: list "Completed", detail
// "Running"). The temporal-adapter CLOSES a CDC pipeline's executions row as
// 'completed' at the backfill→streaming handoff and rewrites pipeline_progress to
// status='running' / 'Streaming pipeline active' for that same execution
// (pipeline_status_activity.go "streaming_active"), while the stream keeps
// running. Two branches used to turn that closed snapshot row into 'passed':
//
//  1. the stale-heartbeat branch below, whenever a later progress event left
//     pp.status at 'processing' on the closed execution — now skipped for CDC
//     when the closed row is a success (a failed/cancelled close still wins);
//  2. the 24h success fallback, for a streaming pipeline whose sync_mode/cdc_mode
//     was never persisted — now pre-empted by `pp.status = 'running'`, a status
//     only the streaming path writes (nl_pipeline_v2_workflow.go executor stage
//     and the streaming_active reconcile). The detail page's /state reads that
//     same pp.status, so the two surfaces now give the same answer.
//
// `pp.status = 'running'` sits AFTER the explicit pipelines.status branches and
// the CDC dead-dependency downgrade on purpose: pausing/stopping writes only
// pipelines.status (pipeline_progress still says 'running'), and /state lets
// pipelines.status win the same way, so a paused stream must stay 'paused' and a
// dead one 'failed'. The CDC predicates use the NULL-safe
// `IS DISTINCT FROM` / `IS NULL` negation — a plain NOT(p.sync_mode = 'cdc' OR
// …) is NULL for a batch row with a NULL sync_mode and would silently drop it.
// Never reference p.mode here: pipelines has no such column and PostgreSQL
// resolves it to the mode() ordered-set aggregate instead of failing.
// pipelineRowIsCDCSQL: is pipeline row p CDC? An explicit sync_mode wins; a
// cdc_mode only counts when sync_mode is unset (legacy rows). A batch pipeline
// created from chat used to persist the source connection's cdc_mode column
// default ('initial'), and `p.cdc_mode IS NOT NULL` then made that batch
// pipeline "CDC": its list badge stayed 'running' forever after it completed
// and GetPipeline skipped its status reconciliation. Never NULL (every operand
// is COALESCEd or IS [NOT] NULL), so NOT (...) is NULL-safe for batch rows.
const pipelineRowIsCDCSQL = `(LOWER(COALESCE(TRIM(p.sync_mode), '')) = 'cdc' OR (COALESCE(TRIM(p.sync_mode), '') = '' AND p.cdc_mode IS NOT NULL))`

// pipelineListIsCDCSQL is the list's ?type= predicate: the pipeline row first,
// then the source connection's sync_mode only when the pipeline has no mode of
// its own. connections.cdc_mode is not a signal: it defaults to 'initial' on
// every connection, so `sc.cdc_mode IS NOT NULL` filed every pipeline as CDC.
const pipelineListIsCDCSQL = `(` + pipelineRowIsCDCSQL + ` OR (COALESCE(TRIM(p.sync_mode), '') = '' AND p.cdc_mode IS NULL AND LOWER(COALESCE(TRIM(sc.sync_mode), '')) = 'cdc'))`

const pipelineDerivedStatusCaseSQL = `CASE
      -- If the latest execution is terminal but pipeline_progress is still "processing" for the same execution,
      -- prefer the executions row. This prevents the list UI getting stuck on "Running" due to late heartbeats
      -- or missed terminal events in the progress projector. NOT for a CDC pipeline whose execution closed
      -- successfully: that close is the backfill→streaming handoff, not the end of the pipeline.
      WHEN le.execution_id IS NOT NULL
        AND le.completed_at IS NOT NULL
        AND pp.execution_id = le.execution_id
        AND pp.status IN ('processing', 'waiting_for_user')
        AND (
          NOT ` + pipelineRowIsCDCSQL + `
          OR COALESCE(le.execution_status, '') NOT IN ('success', 'completed')
        )
      THEN CASE
        WHEN le.execution_status IN ('success', 'completed') THEN 'passed'
        WHEN le.execution_status = 'failed' THEN 'failed'
        WHEN le.execution_status IN ('cancelled', 'stopped') THEN 'stopped'
        ELSE 'idle'
      END
      -- Prefer pipeline_progress (what the detail page uses) for active in-flight runs.
      WHEN pp.status = 'processing' THEN 'running'
      -- Surface HITL pauses distinctly so a run parked on user input doesn't
      -- masquerade as 'running' in the list (the detail page already shows
      -- "Needs input"). The frontend badge delegates unknown list tokens to the
      -- shared execution-status helper, which renders waiting_for_user as amber.
      WHEN pp.status = 'waiting_for_user' THEN 'waiting_for_user'
      WHEN pp.status = 'completed' THEN 'passed'
      WHEN pp.status = 'failed' THEN 'failed'
      WHEN pp.status = 'cancelled' THEN 'stopped'
      -- Prefer pipeline-level terminal status (authoritative) over a potentially stale executions row.
      WHEN LOWER(p.status) = 'stopped' THEN 'stopped'
      WHEN LOWER(p.status) = 'paused' THEN 'paused'
      WHEN LOWER(p.status) = 'failed' THEN 'failed'
      WHEN LOWER(p.status) = 'completed' THEN 'passed'
      -- A CDC/streaming pipeline whose required dependency (Debezium/MCP/sink) is
      -- currently unhealthy is a dead stream, not a live one. Mirror /runtime's
      -- dependency-aware verdict (phase='failed' when any dependency is unhealthy)
      -- so the list card doesn't optimistically show 'running'/'passed' for a feed
      -- that has silently died. Explicit terminal/paused states above still win.
      WHEN ` + pipelineRowIsCDCSQL + `
           AND EXISTS (
             SELECT 1 FROM pipeline_dependencies d
             JOIN pipeline_dependency_health h ON h.dependency_id = d.id
             WHERE d.pipeline_id = p.id AND h.status = 'unhealthy'
           )
      THEN 'failed'
      -- The streaming handoff writes pipeline_progress.status = 'running' (and only
      -- the streaming path does). It must win over the closed snapshot execution row
      -- below, which would otherwise read as a finished run.
      WHEN pp.status = 'running' THEN 'running'
      -- Fallback to execution status for non-terminal pipelines.
      WHEN le.execution_status = 'running' THEN 'running'
      WHEN le.execution_status = 'failed' AND le.started_at >= NOW() - INTERVAL '24 hours' THEN 'failed'
      -- A "completed"/"success" execution row means a finished run only for BATCH
      -- pipelines. For CDC/streaming, the temporal-adapter deliberately CLOSES the
      -- executions row at the backfill→streaming handoff (pipeline_status_activity.go)
      -- while the feed keeps running under CDC sentinel supervision. Exclude CDC here
      -- and fall through to the CDC-continuous 'running' branch below; the
      -- dependency-aware check above already downgrades a genuinely dead stream to
      -- 'failed'. NULL-safe so a batch pipeline with a NULL sync_mode is kept.
      WHEN (le.execution_status = 'success' OR le.execution_status = 'completed')
           AND le.started_at >= NOW() - INTERVAL '24 hours'
           AND NOT ` + pipelineRowIsCDCSQL + ` THEN 'passed'
      -- CDC pipelines are continuous; treat as running by default (unless explicitly paused/stopped/failed above).
      WHEN ` + pipelineRowIsCDCSQL + ` THEN 'running'
      WHEN so.schedule_id IS NOT NULL AND so.schedule_status = 'active' THEN 'scheduled'
      ELSE 'idle'
    END`

// executionLiveCDCStreamSQL (aliases e, p, pp) is true for the run a live CDC
// stream belongs to. At the backfill→streaming handoff the temporal-adapter closes
// that run's executions row as 'completed' (pipeline_status_activity.go) and writes
// pipeline_progress.status='running' for the same execution id; the feed keeps
// running. Read raw, the row said "Success" with an end time and Re-run buttons while
// the pipeline page said LIVE (UI #35). The conditions that end the stream are the
// ones pipelineDerivedStatusCaseSQL uses to stop calling a CDC pipeline 'running':
// an explicit stopped/paused/failed/completed pipeline, or an unhealthy dependency.
// pp.status='running' is written only by the streaming handoff, so it counts even
// for a pipeline whose sync_mode was never persisted (the list reads it the same
// way); 'processing' is also a batch heartbeat, so it counts only for a CDC row.
// Never NULL, so it is safe to scan and to negate.
const executionLiveCDCStreamSQL = `COALESCE((
      e.status IN ('completed', 'success')
      AND pp.execution_id = e.id
      AND (pp.status = 'running' OR (pp.status = 'processing' AND ` + pipelineRowIsCDCSQL + `))
      AND LOWER(COALESCE(p.status, '')) NOT IN ('stopped', 'paused', 'failed', 'completed')
      AND NOT EXISTS (
        SELECT 1 FROM pipeline_dependencies d
        JOIN pipeline_dependency_health h ON h.dependency_id = d.id
        WHERE d.pipeline_id = p.id AND h.status = 'unhealthy'
      )
    ), false)`

// executionFromSQL is the FROM clause every executions read shares, so a list, its
// COUNT, and the single-run read agree on status. ls.live_stream is computed once
// per row and read by executionStatusSQL / executionEndTimeSQL /
// executionRecordsProcessedSQL.
const executionFromSQL = `
		FROM executions e
		LEFT JOIN pipelines p ON e.pipeline_id = p.id
		LEFT JOIN pipeline_progress pp ON pp.pipeline_id = e.pipeline_id
		CROSS JOIN LATERAL (SELECT ` + executionLiveCDCStreamSQL + ` AS live_stream) ls`

// executionStatusSQL is the status the executions endpoints report.
const executionStatusSQL = `CASE
         WHEN ls.live_stream THEN 'running'
         -- An execution's own TERMINAL status wins over pipeline_progress,
         -- for SUCCESS as well as failure. pipeline_progress is a real-time
         -- UI projection that can lag/disagree in BOTH directions:
         --   (a) Phase 1's postflight silent-drop guard flips
         --       executions.status='failed' AFTER the projector wrote
         --       pp.status='completed' (was masking real failures as
         --       "Success"); and
         --   (b) a genuinely completed execution whose pp row is still
         --       'processing' was painted "Running" with a live Cancel
         --       button (R3). Terminal exec status wins either way.
         WHEN e.status IN ('completed', 'success', 'cancelled', 'failed', 'error',
                           'silent_drop_detected',
                           'silent_partial_drop_detected',
                           'credential_check_failed') THEN e.status
         WHEN pp.execution_id = e.id THEN
           CASE pp.status
             WHEN 'processing' THEN 'running'
             WHEN 'waiting_for_user' THEN 'waiting_for_user'
             ELSE pp.status
           END
         ELSE e.status
       END`

// executionEndTimeSQL: a live stream has not ended, whatever end_time the handoff
// stamped on its row.
const executionEndTimeSQL = `CASE
         WHEN ls.live_stream THEN NULL
         ELSE COALESCE(
           e.end_time,
           CASE
             WHEN pp.execution_id = e.id AND pp.status IN ('completed','failed','cancelled') THEN pp.updated_at
             ELSE NULL
           END
         )
       END`

// executionRecordsProcessedSQL: rows written for this run, summed from
// destination-truth table stats. GREATEST() picks the batch (inserted_rows) or CDC
// (applied_*) family per table; SUM is NOT gated on table status, so partial /
// degraded runs still report a count (BUG-8). CDC stats are keyed by
// execution_id = pipeline_id (migration 090), so a CDC run's rows are found through
// orchestration_execution_id, or — for the live stream on a sink too old to send
// that id — through the pipeline key itself. Matching only execution_id = e.id
// showed "—" Records for a streaming run (UI #43).
const executionRecordsProcessedSQL = `COALESCE((
         SELECT SUM(
           GREATEST(
             COALESCE(s.inserted_rows, 0),
             COALESCE(s.applied_inserts, 0) + COALESCE(s.applied_updates, 0) + COALESCE(s.applied_deletes, 0)
           )
         )
         FROM pipeline_run_table_stats s
         WHERE s.pipeline_id = e.pipeline_id
           AND (
             s.execution_id = e.id
             OR (s.execution_id = e.pipeline_id AND (s.orchestration_execution_id = e.id OR ls.live_stream))
           )
       ), 0)`

// ListPipelines returns all pipelines with enriched data (no N+1)
// GET /api/v1/pipelines?page=1&per_page=25&q=&created_from=&created_to=&status=
func ListPipelines(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	// Parse query parameters
	searchQuery := strings.TrimSpace(c.Query("q"))
	derivedStatusFilter := strings.TrimSpace(c.Query("status"))
	typeFilter := strings.ToLower(strings.TrimSpace(c.Query("type"))) // all|etl|cdc
	createdFrom := strings.TrimSpace(c.Query("created_from"))
	createdTo := strings.TrimSpace(c.Query("created_to"))

	// Pagination: support both page/per_page (new) and limit/offset (back-compat)
	var limit, offset int
	if pageStr := strings.TrimSpace(c.Query("page")); pageStr != "" {
		page, _ := strconv.Atoi(pageStr)
		if page < 1 {
			page = 1
		}
		perPage := 25
		if ppStr := strings.TrimSpace(c.Query("per_page")); ppStr != "" {
			if v, err := strconv.Atoi(ppStr); err == nil && v > 0 && v <= 100 {
				perPage = v
			}
		}
		offset = (page - 1) * perPage
		limit = perPage
	} else {
		// Back-compat: limit/offset
		limit = 50
		if s := strings.TrimSpace(c.Query("limit")); s != "" {
			if v, err := strconv.Atoi(s); err == nil && v > 0 && v <= 200 {
				limit = v
			}
		}
		offset = 0
		if s := strings.TrimSpace(c.Query("offset")); s != "" {
			if v, err := strconv.Atoi(s); err == nil && v >= 0 {
				offset = v
			}
		}
	}

	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	// Workspace scoping: list pipelines shared within the caller's ACTIVE
	// workspace, not just the ones the caller created. Membership in the active
	// workspace is already proven by WorkspaceContextMiddleware, and the active
	// workspace id is a DB-generated UUID (so the `$1::uuid` cast is always
	// valid). With no active workspace, return an empty page rather than passing
	// NULL — which would disable the filter and leak every pipeline.
	wsID := activeWorkspaceID(c)
	if wsID == "" {
		log.Printf("[ListPipelines] no active workspace for user %q → empty result", userID)
		c.JSON(http.StatusOK, gin.H{
			"pipelines": []PipelineListItem{},
			"pagination": gin.H{
				"page": 1, "per_page": limit, "total": 0,
				"total_pages": 0, "has_next": false, "has_prev": false,
			},
			// Back-compat
			"total": 0, "limit": limit, "offset": offset,
		})
		return
	}

	// Build the no-N+1 query with CTEs
	query := `
WITH latest_exec AS (
  SELECT DISTINCT ON (e.pipeline_id)
    e.pipeline_id,
    e.id            AS execution_id,
    e.status        AS execution_status,
    e.start_time    AS started_at,
    e.end_time      AS completed_at,
    e.error_message AS error_message,
    e.metrics       AS metrics,
    -- BUG-1: same authoritative rows-written count as /executions + GetPipeline,
    -- summed from destination-truth table stats (not the stale executions.metrics).
    COALESCE((
      SELECT SUM(GREATEST(
        COALESCE(s.inserted_rows, 0),
        COALESCE(s.applied_inserts, 0) + COALESCE(s.applied_updates, 0) + COALESCE(s.applied_deletes, 0)
      ))
      FROM pipeline_run_table_stats s
      WHERE s.execution_id = e.id
    ), 0) AS records_processed
  FROM executions e
  ORDER BY e.pipeline_id, e.start_time DESC
),
schedule_one AS (
  SELECT DISTINCT ON (ps.pipeline_id)
    ps.pipeline_id,
    ps.schedule_id,
    ps.schedule_type,
    ps.schedule_spec,
    ps.status AS schedule_status,
    ps.created_at AS schedule_created_at
  FROM pipeline_schedules ps
  WHERE ps.status != 'deleted'
  ORDER BY ps.pipeline_id, ps.created_at DESC
),
base AS (
  SELECT
    p.id,
    p.name,
    p.description,
    p.status AS pipeline_status,
    p.created_at,
    p.updated_at,
    p.created_by,
    p.sync_mode,
    p.cdc_mode,
    jsonb_build_object(
      'id', sc.id,
      'name', sc.name,
      'connector_type', sc.connector_type,
      'status', sc.status
    ) AS source_connection,
    jsonb_build_object(
      'id', dc.id,
      'name', dc.name,
      'connector_type', dc.connector_type,
      'status', dc.status
    ) AS destination_connection,
    CASE 
      WHEN so.schedule_id IS NOT NULL THEN
        jsonb_build_object(
          'schedule_id', so.schedule_id,
          'schedule_type', so.schedule_type,
          'schedule_spec', so.schedule_spec,
          'status', so.schedule_status,
          'timezone', COALESCE(so.schedule_spec->>'timezone', 'UTC')
        )
      ELSE NULL
    END AS schedule,
    CASE 
      WHEN le.execution_id IS NOT NULL THEN
        jsonb_build_object(
          'id', le.execution_id,
          'status', le.execution_status,
          'started_at', le.started_at,
          'completed_at', le.completed_at,
          'error_message', le.error_message,
          -- BUG-1: metrics is always a non-null object carrying the authoritative
          -- records_processed (merged over executions.metrics), plus flat sibling.
          'metrics', COALESCE(le.metrics, '{}'::jsonb) || jsonb_build_object('records_processed', le.records_processed),
          'records_processed', le.records_processed
        )
      ELSE NULL
    END AS last_execution,
    ` + pipelineDerivedStatusCaseSQL + ` AS derived_status
  FROM pipelines p
  LEFT JOIN pipeline_progress pp ON pp.pipeline_id = p.id
  LEFT JOIN connections sc ON p.source_connection_id = sc.id
  LEFT JOIN connections dc ON p.destination_connection_id = dc.id
  LEFT JOIN latest_exec le ON le.pipeline_id = p.id
  LEFT JOIN schedule_one so ON so.pipeline_id = p.id
  WHERE 1=1
    AND ($1::uuid IS NULL OR p.workspace_id = $1)
    AND (
      $2::text IS NULL
      OR p.name ILIKE '%' || $2 || '%'
      OR p.id::text ILIKE '%' || $2 || '%'
    )
    AND ($3::timestamptz IS NULL OR p.created_at >= $3)
    AND ($4::timestamptz IS NULL OR p.created_at <= $4)
    AND (
      $6::text IS NULL
      OR (
        $6 = 'cdc'
        AND (
          ` + pipelineListIsCDCSQL + `
        )
      )
      OR (
        $6 = 'etl'
        AND NOT (
          ` + pipelineListIsCDCSQL + `
        )
      )
    )
),
filtered AS (
  SELECT *
  FROM base
  WHERE ($5::text IS NULL OR base.derived_status = $5)
)
SELECT 
  id, name, description, pipeline_status, created_at, updated_at, created_by, sync_mode, cdc_mode,
  source_connection, destination_connection, schedule, last_execution, derived_status
FROM filtered
ORDER BY created_at DESC
LIMIT $7 OFFSET $8
`

	// Build args
	args := []any{}

	// $1 - active workspace (scoping). Always non-empty here; an empty active
	// workspace returned an empty page above.
	args = append(args, wsID)

	// $2 - search query
	if searchQuery != "" {
		args = append(args, searchQuery)
	} else {
		args = append(args, nil)
	}

	// $3 - created_from
	if createdFrom != "" {
		args = append(args, createdFrom)
	} else {
		args = append(args, nil)
	}

	// $4 - created_to
	if createdTo != "" {
		args = append(args, createdTo)
	} else {
		args = append(args, nil)
	}

	// $5 - derived_status filter
	if derivedStatusFilter != "" {
		args = append(args, derivedStatusFilter)
	} else {
		args = append(args, nil)
	}

	// $6 - type filter (etl|cdc)
	switch typeFilter {
	case "etl", "batch":
		args = append(args, "etl")
	case "cdc":
		args = append(args, "cdc")
	default:
		args = append(args, nil)
	}

	// $7, $8 - limit, offset
	args = append(args, limit, offset)

	// Count query (same filters)
	countQuery := `
WITH base AS (
  SELECT
    p.id,
    ` + pipelineDerivedStatusCaseSQL + ` AS derived_status
  FROM pipelines p
  LEFT JOIN pipeline_progress pp ON pp.pipeline_id = p.id
  LEFT JOIN connections sc ON p.source_connection_id = sc.id
  LEFT JOIN (
    SELECT DISTINCT ON (e.pipeline_id) e.pipeline_id, e.id AS execution_id, e.status AS execution_status,
           e.start_time AS started_at, e.end_time AS completed_at
    FROM executions e ORDER BY e.pipeline_id, e.start_time DESC
  ) le ON le.pipeline_id = p.id
  LEFT JOIN (
    SELECT DISTINCT ON (ps.pipeline_id) ps.pipeline_id, ps.schedule_id, ps.status AS schedule_status
    FROM pipeline_schedules ps WHERE ps.status != 'deleted' ORDER BY ps.pipeline_id, ps.created_at DESC
  ) so ON so.pipeline_id = p.id
  WHERE 1=1
    AND ($1::uuid IS NULL OR p.workspace_id = $1)
    AND ($2::text IS NULL OR p.name ILIKE '%' || $2 || '%' OR p.id::text ILIKE '%' || $2 || '%')
    AND ($3::timestamptz IS NULL OR p.created_at >= $3)
    AND ($4::timestamptz IS NULL OR p.created_at <= $4)
    AND (
      $6::text IS NULL
      OR (
        $6 = 'cdc'
        AND (
          ` + pipelineListIsCDCSQL + `
        )
      )
      OR (
        $6 = 'etl'
        AND NOT (
          ` + pipelineListIsCDCSQL + `
        )
      )
    )
)
SELECT COUNT(*) FROM base WHERE ($5::text IS NULL OR base.derived_status = $5)
`

	var total int
	if err := database.QueryRow(countQuery, args[:6]...).Scan(&total); err != nil {
		log.Printf("[ListPipelines] Count query error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch pipelines count"})
		return
	}

	// Execute main query
	rows, err := database.Query(query, args...)
	if err != nil {
		log.Printf("[ListPipelines] Query error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch pipelines"})
		return
	}
	defer rows.Close()

	result := make([]PipelineListItem, 0)
	for rows.Next() {
		var item PipelineListItem
		var description sql.NullString
		var createdBy sql.NullString
		var syncMode sql.NullString
		var cdcMode sql.NullString
		var sourceConn, destConn, schedule, lastExec sql.NullString

		err := rows.Scan(
			&item.ID,
			&item.Name,
			&description,
			&item.PipelineStatus,
			&item.CreatedAt,
			&item.UpdatedAt,
			&createdBy,
			&syncMode,
			&cdcMode,
			&sourceConn,
			&destConn,
			&schedule,
			&lastExec,
			&item.DerivedStatus,
		)
		if err != nil {
			log.Printf("[ListPipelines] Scan error: %v", err)
			continue
		}

		if description.Valid {
			item.Description = description.String
		}
		if createdBy.Valid {
			item.CreatedBy = createdBy.String
		}
		if syncMode.Valid {
			v := strings.TrimSpace(syncMode.String)
			if v != "" {
				item.SyncMode = &v
			}
		}
		if cdcMode.Valid {
			v := strings.TrimSpace(cdcMode.String)
			if v != "" {
				item.CDCMode = &v
			}
		}
		if sourceConn.Valid {
			item.SourceConnection = json.RawMessage(sourceConn.String)
		} else {
			item.SourceConnection = json.RawMessage("{}")
		}
		if destConn.Valid {
			item.DestinationConnection = json.RawMessage(destConn.String)
		} else {
			item.DestinationConnection = json.RawMessage("{}")
		}
		if schedule.Valid {
			item.Schedule = json.RawMessage(schedule.String)
		} else {
			item.Schedule = json.RawMessage("null")
		}
		if lastExec.Valid {
			item.LastExecution = json.RawMessage(lastExec.String)
		} else {
			item.LastExecution = json.RawMessage("null")
		}

		// Build status_context for UI
		item.StatusContext = map[string]string{
			"primary": item.DerivedStatus,
		}

		// Secondary status context: schedule hints (do not override primary Passed/Failed/etc).
		// We intentionally keep this lightweight (list view can compute "next run" estimates client-side).
		if schedule.Valid {
			if hint := scheduleStatusHint(schedule.String, item.DerivedStatus); hint != "" {
				item.StatusContext["secondary"] = hint
			}
		}

		result = append(result, item)
	}

	// Build pagination response
	totalPages := (total + limit - 1) / limit
	page := (offset / limit) + 1

	pagination := gin.H{
		"page":        page,
		"per_page":    limit,
		"total":       total,
		"total_pages": totalPages,
		"has_next":    offset+limit < total,
		"has_prev":    offset > 0,
	}

	log.Printf("[ListPipelines] Found %d pipelines (total=%d, page=%d)", len(result), total, page)

	c.JSON(http.StatusOK, gin.H{
		"pipelines":  result,
		"pagination": pagination,
		// Back-compat
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// GetPipeline returns a single pipeline by ID
func GetPipeline(c *gin.Context) {
	id, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, id, security.WSViewer); !ok {
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	var p Pipeline
	var sourceID, destID *string
	var createdBy *string
	var description *string
	var syncMode, cdcMode, syncModeSource *string
	var configBytes []byte
	var sourceConnJSON, destConnJSON []byte
	var lastExecJSON sql.NullString

	// F-16: previously this query never joined the latest execution row,
	// so `last_execution` was hard-coded to null even when /pipelines (list)
	// returned the same field populated. The UI's pipeline detail and the
	// chat monitor both rely on this to surface in-flight vs completed
	// state; without it they could only show pipeline.status which lags
	// terminal-state transitions until the progress projector catches up.
	query := `
		WITH latest_exec AS (
		  SELECT DISTINCT ON (e.pipeline_id)
		    e.pipeline_id,
		    e.id          AS execution_id,
		    e.status      AS execution_status,
		    e.start_time  AS started_at,
		    e.end_time    AS completed_at,
		    e.error_message,
		    e.metrics,
		    -- BUG-1: authoritative rows-written count for this execution, summed
		    -- from destination-truth table stats — the SAME expression /executions
		    -- uses. GREATEST() picks the batch (inserted_rows) or CDC (applied_*)
		    -- family per table; NOT gated on table status so partial runs still
		    -- report a count. Replaces the stale, usually-null executions.metrics.
		    COALESCE((
		      SELECT SUM(GREATEST(
		        COALESCE(s.inserted_rows, 0),
		        COALESCE(s.applied_inserts, 0) + COALESCE(s.applied_updates, 0) + COALESCE(s.applied_deletes, 0)
		      ))
		      FROM pipeline_run_table_stats s
		      WHERE s.execution_id = e.id
		    ), 0) AS records_processed
		  FROM executions e
		  WHERE e.pipeline_id = $1
		  ORDER BY e.pipeline_id, e.start_time DESC
		)
		SELECT p.id, p.name, p.description, p.status, p.created_at, p.updated_at, p.created_by,
		       p.source_connection_id, p.destination_connection_id, p.sync_mode, p.cdc_mode, p.sync_mode_source,
		       p.config,
		       CASE WHEN sc.id IS NOT NULL THEN jsonb_build_object(
		           'id', sc.id, 'name', sc.name, 'connector_type', sc.connector_type, 'status', sc.status
		       ) END AS source_connection,
		       CASE WHEN dc.id IS NOT NULL THEN jsonb_build_object(
		           'id', dc.id, 'name', dc.name, 'connector_type', dc.connector_type, 'status', dc.status
		       ) END AS destination_connection,
		       CASE WHEN le.execution_id IS NOT NULL THEN jsonb_build_object(
		           'id',            le.execution_id,
		           'status',        le.execution_status,
		           'started_at',    le.started_at,
		           'completed_at',  le.completed_at,
		           'error_message', le.error_message,
		           -- BUG-1: guarantee metrics is a non-null object carrying the
		           -- authoritative records_processed (merged over whatever the
		           -- executions.metrics JSON had), plus a sibling for callers that
		           -- read it flat.
		           'metrics',       COALESCE(le.metrics, '{}'::jsonb) || jsonb_build_object('records_processed', le.records_processed),
		           'records_processed', le.records_processed
		       ) END AS last_execution,
		       COALESCE(le.records_processed, 0) AS rows_processed
		FROM pipelines p
		LEFT JOIN connections sc ON p.source_connection_id = sc.id
		LEFT JOIN connections dc ON p.destination_connection_id = dc.id
		LEFT JOIN latest_exec le ON le.pipeline_id = p.id
		WHERE p.id = $1
	`

	err := database.QueryRow(query, id).Scan(
		&p.ID, &p.Name, &description, &p.Status, &p.CreatedAt, &p.UpdatedAt,
		&createdBy, &sourceID, &destID, &syncMode, &cdcMode, &syncModeSource,
		&configBytes, &sourceConnJSON, &destConnJSON, &lastExecJSON,
		&p.RowsProcessed,
	)

	if err != nil {
		log.Printf("[GetPipeline] Query error: %v", err)
		// Only a missing row is "not found": the gate saw the pipeline, so this is
		// a delete that landed in between. Any other error (connection, timeout,
		// scan) is a failed read — a 404 there tells the UI a live pipeline is gone.
		if errors.Is(err, sql.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{
				"error":   "pipeline_not_found",
				"message": "Pipeline not found",
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "pipeline_fetch_failed",
			"message": "Failed to load pipeline",
		})
		return
	}

	if description != nil {
		p.Description = *description
	}
	if sourceID != nil {
		p.SourceConnectionID = *sourceID
	}
	if destID != nil {
		p.DestinationConnectionID = *destID
	}
	if createdBy != nil {
		p.CreatedBy = *createdBy
	}
	if syncMode != nil {
		p.SyncMode = syncMode
	}
	if cdcMode != nil {
		p.CDCMode = cdcMode
	}
	if syncModeSource != nil {
		p.SyncModeSource = syncModeSource
	}

	// F-16: surface the latest execution alongside the pipeline row.
	if lastExecJSON.Valid && lastExecJSON.String != "" {
		p.LastExecution = json.RawMessage(lastExecJSON.String)
	} else {
		p.LastExecution = json.RawMessage("null")
	}

	// Populate full connection objects (name + connector_type) so the UI can show which connection is used.
	if len(sourceConnJSON) > 0 {
		var conn Connection
		if err := json.Unmarshal(sourceConnJSON, &conn); err == nil {
			p.SourceConnection = &conn
		}
	}
	if len(destConnJSON) > 0 {
		var conn Connection
		if err := json.Unmarshal(destConnJSON, &conn); err == nil {
			p.DestinationConnection = &conn
		}
	}

	// Backfill the legacy top-level Source/Destination display strings
	// from the nested connection objects. The UI list view reads these
	// flat fields and used to render blanks because the struct was zero-
	// valued. Prefer connector_type ("postgresql"), fall back to name.
	if p.Source == "" && p.SourceConnection != nil {
		if v := strings.TrimSpace(p.SourceConnection.ConnectorType); v != "" {
			p.Source = v
		} else if v := strings.TrimSpace(p.SourceConnection.Name); v != "" {
			p.Source = v
		}
	}
	if p.Destination == "" && p.DestinationConnection != nil {
		if v := strings.TrimSpace(p.DestinationConnection.ConnectorType); v != "" {
			p.Destination = v
		} else if v := strings.TrimSpace(p.DestinationConnection.Name); v != "" {
			p.Destination = v
		}
	}

	// Best-effort: extract selected tables from pipelines.config.selected_tables (if present).
	// We return them as a top-level field so the UI can preselect tables in "Edit tables".
	if len(configBytes) > 0 {
		var cfg map[string]interface{}
		if err := json.Unmarshal(configBytes, &cfg); err == nil && cfg != nil {
			if raw, ok := cfg["selected_tables"]; ok {
				var tables []string
				switch v := raw.(type) {
				case []interface{}:
					for _, it := range v {
						s := strings.TrimSpace(fmt.Sprint(it))
						if s != "" {
							tables = append(tables, s)
						}
					}
				case []string:
					for _, it := range v {
						s := strings.TrimSpace(it)
						if s != "" {
							tables = append(tables, s)
						}
					}
				default:
					// ignore
				}
				if len(tables) > 0 {
					seen := map[string]bool{}
					clean := make([]string, 0, len(tables))
					for _, t := range tables {
						if seen[t] {
							continue
						}
						seen[t] = true
						clean = append(clean, t)
					}
					p.SelectedTables = clean
				}
			}

			// Surface the structured destination_config so the destination-mapping
			// HITL can pre-fill the namespace field and toggle. Falls back to the
			// legacy destination_namespace string for pipelines created before PR-C.
			if raw, ok := cfg["destination_config"]; ok {
				if obj, ok := raw.(map[string]interface{}); ok {
					dc := &DestinationConfig{}
					if v, ok := obj["namespace"].(string); ok {
						dc.Namespace = v
					}
					if v, ok := obj["namespace_kind"].(string); ok {
						dc.NamespaceKind = v
					}
					if v, ok := obj["create_if_not_exists"].(bool); ok {
						dc.CreateIfNotExists = v
					}
					p.DestinationConfig = dc
				}
			}
			if p.DestinationConfig == nil {
				if ns, ok := cfg["destination_namespace"].(string); ok && strings.TrimSpace(ns) != "" {
					p.DestinationConfig = &DestinationConfig{Namespace: ns}
				}
			}
		}
	}

	// Compute and attach the human-readable data loading strategy so UIs can show it consistently.
	p.DataLoadingStrategy = computeDataLoadingStrategy(database, &p)

	// Derive an "effective" pipeline status for UI so it doesn't get stuck on "running"
	// when the latest execution has already completed but pipeline.status wasn't updated.
	//
	// The UI's pipeline detail page reads /pipelines/:id and shows p.Status directly.
	// We keep p.Status in the legacy enum expected by the UI: running|completed|failed|stopped|paused|draft|active.
	mapExecStatus := func(s string) string {
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "success", "completed":
			return "completed"
		case "failed":
			return "failed"
		case "cancelled", "canceled", "stopped":
			return "stopped"
		case "running", "processing":
			return "running"
		default:
			return ""
		}
	}
	mapProgressStatus := func(s string) string {
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "processing":
			return "running"
		// R2: surface the HITL pause distinctly instead of collapsing it into
		// "running", so GET /pipelines/:id agrees with the list's derived_status
		// and the /state reconciler (both already expose waiting_for_user). The
		// detail page's buttons + badge are driven by GET /pipelines/:id/state,
		// so this only corrects the one-render seed the page paints before its
		// first /state poll — verified against every consumer (normalize maps
		// waiting_for_user explicitly; no button gates on this field).
		case "waiting_for_user":
			return "waiting_for_user"
		case "completed":
			return "completed"
		case "failed":
			return "failed"
		case "cancelled", "canceled":
			return "stopped"
		default:
			return ""
		}
	}

	// Skip reconciliation for CDC pipelines (continuous) unless explicitly paused/stopped/failed.
	// Same rule as pipelineRowIsCDCSQL: an explicit sync_mode wins over a stray
	// cdc_mode, or a completed batch pipeline carrying cdc_mode='initial' skipped
	// this reconciliation and kept reading "running" on its detail page.
	isCDC := pipelineModeIsCDC(p.SyncMode, p.CDCMode)

	if !isCDC {
		// Best-effort lookup of pipeline_progress status (used for active runs).
		var ppStatus sql.NullString
		var ppExecID sql.NullString
		_ = database.QueryRow(`SELECT status, execution_id FROM pipeline_progress WHERE pipeline_id = $1`, id).Scan(&ppStatus, &ppExecID)

		// Latest execution (source of truth when pipeline_progress is stale).
		var leID sql.NullString
		var leStatus sql.NullString
		var leEnd sql.NullTime
		_ = database.QueryRow(`
			SELECT id, status, end_time
			FROM executions
			WHERE pipeline_id = $1
			ORDER BY start_time DESC
			LIMIT 1
		`, id).Scan(&leID, &leStatus, &leEnd)

		current := strings.ToLower(strings.TrimSpace(p.Status))

		// Prefer pipeline-level terminal statuses that should never be overridden.
		if current != "paused" && current != "stopped" && current != "failed" {
			// If pipeline_progress says running, but latest execution already ended for the same execution,
			// treat execution status as authoritative (prevents "stuck running").
			if ppStatus.Valid && ppExecID.Valid && leID.Valid && leEnd.Valid &&
				(ppExecID.String == leID.String) &&
				(strings.EqualFold(strings.TrimSpace(ppStatus.String), "processing") || strings.EqualFold(strings.TrimSpace(ppStatus.String), "waiting_for_user")) {
				if leStatus.Valid {
					if s := mapExecStatus(leStatus.String); s != "" {
						p.Status = s
					}
				}
			} else if ppStatus.Valid {
				if s := mapProgressStatus(ppStatus.String); s != "" {
					p.Status = s
				}
			}

			// Final fallback: if p.Status is "running" but latest execution already ended, use execution status.
			if strings.EqualFold(strings.TrimSpace(p.Status), "running") && leEnd.Valid && leStatus.Valid {
				if s := mapExecStatus(leStatus.String); s != "" && s != "running" {
					p.Status = s
				}
			}
		}
	}

	c.JSON(http.StatusOK, &p)
}

// CreatePipeline creates a new pipeline
func CreatePipeline(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	var req CreatePipelineRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		// Same translation pass as CreateConnection — turn the
		// "Key: 'CreatePipelineRequest.Request'..." Go-binding string
		// into a user-friendly per-field breakdown.
		userMsg, fieldErrs := translateBindError(err)
		resp := gin.H{
			"error":   "invalid_request",
			"message": userMsg,
		}
		if len(fieldErrs) > 0 {
			resp["validation_errors"] = fieldErrs
		}
		c.JSON(http.StatusBadRequest, resp)
		return
	}

	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	// Workspace scoping: the new pipeline belongs to the caller's ACTIVE
	// workspace (migration 069 made pipelines.workspace_id NOT NULL). Resolve it
	// up front so we fail fast before the plan gate / preflight side effects.
	workspaceID, ok := resolveActiveWorkspace(c)
	if !ok {
		return
	}

	// RBAC: creating a pipeline is a MUTATION; viewers are read-only. Require at
	// least `member` in the active workspace (mirrors UpdatePipeline / RunPipeline
	// and the roles.ts capability matrix).
	if _, ok := requireWorkspaceRole(c, security.WSMember); !ok {
		return
	}

	// Plan pipeline-count gate. Refuse to create another pipeline once the
	// WORKSPACE is at its plan's limit (or its trial/free window has expired).
	// Scoped to the active workspace (the billable tenant), not the caller.
	// Limits live in the plans table; see plan_quota.go / migration 060.
	if allowed, errBody := checkPipelineCreateOK(c.Request.Context(), database, workspaceID); !allowed {
		c.JSON(http.StatusPaymentRequired, errBody)
		return
	}

	// Resolve connection names/IDs to UUIDs
	var sourceConnectionID, destinationConnectionID string

	// Prioritize new name/ID fields, fallback to legacy UUID fields
	sourceInput := req.SourceConnection
	if sourceInput == "" {
		sourceInput = req.SourceConnectionID
	}

	destinationInput := req.DestinationConnection
	if destinationInput == "" {
		destinationInput = req.DestinationConnectionID
	}

	// Resolve source connection (name or UUID → UUID)
	if sourceInput != "" {
		resolvedID, err := resolveConnectionID(sourceInput, workspaceID, database)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "invalid_source_connection",
				"message": fmt.Sprintf("Source connection error: %v", err),
			})
			return
		}
		sourceConnectionID = resolvedID
		// Defense-in-depth: resolveConnectionID's UUID fast-path does not verify
		// tenancy, so confirm the connection lives in the active workspace before
		// wiring a pipeline to it (cross-workspace IDOR guard).
		if !connectionInWorkspace(database, sourceConnectionID, workspaceID) {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "invalid_source_connection",
				"message": "Source connection does not exist in your workspace",
			})
			return
		}
	}

	// Resolve destination connection (name or UUID → UUID)
	if destinationInput != "" {
		resolvedID, err := resolveConnectionID(destinationInput, workspaceID, database)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "invalid_destination_connection",
				"message": fmt.Sprintf("Destination connection error: %v", err),
			})
			return
		}
		destinationConnectionID = resolvedID
		if !connectionInWorkspace(database, destinationConnectionID, workspaceID) {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "invalid_destination_connection",
				"message": "Destination connection does not exist in your workspace",
			})
			return
		}
	}

	// Lifecycle=draft gate (canonical for all entry points — see
	// checkConnectionLifecycleDraft helper).
	if blocked := checkConnectionLifecycleDraft(c, database, sourceConnectionID, destinationConnectionID); blocked {
		return
	}

	// Phase 2 pre-flight gates — run BEFORE inserting the pipeline so
	// invalid configs don't leave orphan rows. Each agent either
	// passes (continue), returns a 422 HITL response, or returns a
	// 500 for unexpected DB errors.
	if hitl, err := runPipelinePreflight(database, workspaceID, &req, &sourceConnectionID, &destinationConnectionID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "preflight_failed",
			"message": err.Error(),
		})
		return
	} else if hitl != nil {
		c.JSON(http.StatusUnprocessableEntity, hitl)
		return
	}

	// Create new pipeline
	id := uuid.New().String()
	now := time.Now()

	// Derive dataset from request or pipeline name
	dataset := req.Dataset
	if dataset == nil || *dataset == "" {
		slug := slugifyPipelineName(req.Name)
		if slug != "" {
			dataset = &slug
		}
	}

	// Default run mode to "resume" if not specified
	defaultRunMode := req.DefaultRunMode
	if defaultRunMode == nil {
		resumeMode := "resume"
		defaultRunMode = &resumeMode
	}

	// Persist selected_tables into pipelines.config so the run path (and
	// re-runs) can read them. Previously the CreatePipeline INSERT omitted the
	// config column entirely, so selected_tables supplied at create time were
	// silently dropped and callers had to issue a separate update.
	var configJSON []byte
	if len(req.SelectedTables) > 0 {
		createTables := normalizeSelectedTables(req.SelectedTables)
		// The rule the user expressed, captured before expansion: a pipeline
		// created as "this whole database" must keep matching tables created
		// later, which is what the CDC auto-pickup watcher re-applies.
		createRule := selectionRuleTokens(createTables)
		// Expand a "*" / "<ns>.*" sentinel supplied at create time into an
		// explicit list so config.selected_tables never persists a raw sentinel.
		if resolved, _, rerr := resolveSelectionForPipeline(c, sourceConnectionID, createTables); rerr != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": "table_resolution_failed", "message": rerr.Error()})
			return
		} else {
			createTables = resolved
		}
		if b, mErr := json.Marshal(map[string]interface{}{
			"selected_tables":     createTables,
			tableSelectionRuleKey: createRule,
		}); mErr == nil {
			configJSON = b
		}
	}

	// Insert into DB
	// Note: created_by must never be NULL to ensure proper event visibility
	_, err := database.Exec(`
		INSERT INTO pipelines (id, name, description, natural_language_request, status, created_at, updated_at, created_by, source_connection_id, destination_connection_id, sync_mode, cdc_mode, sync_mode_source, dataset, default_run_mode, config, workspace_id, cdc_initial_load)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16::jsonb, $17, $18)
	`, id, req.Name, req.Description, req.Request, "pending", now, now, userID,
		nullString(sourceConnectionID), nullString(destinationConnectionID),
		req.SyncMode, req.CDCMode, req.SyncModeSource, dataset, defaultRunMode,
		nullJSONString(configJSON), workspaceID, sanitizeCDCInitialLoad(req.CDCInitialLoad))

	if err != nil {
		log.Printf("Failed to insert pipeline: %v", err)
		// Check for unique constraint violation on dataset
		if strings.Contains(err.Error(), "idx_pipelines_dest_dataset_unique") {
			c.JSON(http.StatusConflict, gin.H{
				"error":   "dataset_conflict",
				"message": fmt.Sprintf("Dataset '%s' is already used by another pipeline on this destination", *dataset),
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create pipeline in database"})
		return
	}

	// Resolve and persist the destination namespace for this pipeline.
	// See .design/destination-namespace.md. The collision check is a single
	// best-effort SQL statement against the pipelines table; live destination
	// ownership is enforced by the sink at first-write time.
	var sourceConnectorType string
	if sourceConnectionID != "" {
		_ = database.QueryRow(`SELECT connector_type FROM connections WHERE id = $1`, sourceConnectionID).Scan(&sourceConnectorType)
	}
	if strings.TrimSpace(sourceConnectorType) == "" {
		sourceConnectorType = strings.TrimSpace(req.SourceConnectorType)
	}
	// Fetch dest connector type before resolving the namespace so
	// seedDestinationNamespace can translate source-engine defaults
	// ("default", "public") to the destination's own default name.
	var destConnectorType string
	if destinationConnectionID != "" {
		_ = database.QueryRow(`SELECT connector_type FROM connections WHERE id = $1`, destinationConnectionID).Scan(&destConnectorType)
	}
	destinationNamespace := resolveDestinationNamespace(database, id, sourceConnectorType, destConnectorType, destinationConnectionID, req.DestinationNamespace)
	// Seed the per-pipeline destination_config with the smart default. The
	// first-run table-selection HITL lets the user edit the namespace name before
	// the run-gate. The destination connector auto-creates a missing
	// schema/database at write time (CREATE … IF NOT EXISTS), so create defaults
	// on — we just collect the name and provision it for the user.
	if err := persistDestinationConfig(database, id, DestinationConfig{
		Namespace:         destinationNamespace,
		NamespaceKind:     namespaceKindForConnector(destConnectorType),
		CreateIfNotExists: true,
	}); err != nil {
		log.Printf("[CreatePipeline] failed to persist destination_config (best-effort, will fall back at run time): %v", err)
	}

	pipeline := &Pipeline{
		ID:                      id,
		Name:                    req.Name,
		Description:             req.Description,
		Status:                  "pending",
		SourceConnectionID:      sourceConnectionID,
		DestinationConnectionID: destinationConnectionID,
		SyncMode:                req.SyncMode,
		CDCMode:                 req.CDCMode,
		SyncModeSource:          req.SyncModeSource,
		Dataset:                 dataset,
		DefaultRunMode:          defaultRunMode,
		DestinationNamespace:    &destinationNamespace,
		CreatedAt:               now,
		UpdatedAt:               now,
		CreatedBy:               userID,
	}

	// NOTE: We don't send to Kafka here - only when user clicks "Run" (RunPipeline handler)
	// This ensures the pipeline is fully configured before agent processing begins
	log.Printf("✅ Created pipeline %s (name: %s) - ready to run", id, req.Name)

	// Best-effort: seed pipeline_progress so /pipelines/:id/state is never "missing".
	// The Temporal adapter will update this authoritatively once execution starts.
	if _, err := database.Exec(`
		INSERT INTO pipeline_progress (
			pipeline_id,
			status,
			message,
			progress_percent,
			progress_current_step,
			progress_total_steps,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, 0, 0, 7, NOW(), NOW())
		ON CONFLICT (pipeline_id) DO NOTHING
	`, id, "pending", "Pipeline created, ready to run."); err != nil {
		log.Printf("⚠️ [CreatePipeline] failed to seed pipeline_progress (ignored): %v", err)
	}

	logAudit(c, "create_pipeline", "pipeline", id, map[string]interface{}{
		"name":                      req.Name,
		"source_connection_id":      sourceConnectionID,
		"destination_connection_id": destinationConnectionID,
		"destination_namespace":     destinationNamespace,
	})

	// Best-effort: Auto-create a schedule when the NL request includes a schedule intent,
	// but ONLY for batch pipelines. CDC pipelines are continuous and must not be scheduled.
	//
	// This makes the NL flow feel complete: "sync ... every hour" immediately becomes scheduled.
	if temporalClient != nil {
		effectiveMode := resolveEffectiveSyncMode(database, id, sourceConnectionID, req.SyncMode, req.CDCMode)
		if effectiveMode != "cdc" {
			if stype, spec, ok := inferScheduleFromNaturalLanguage(req.Request); ok {
				// Reuse the schedules handler logic by inserting a schedule row and creating a Temporal schedule.
				// We do this best-effort; pipeline creation should not fail if scheduling fails.
				_, schedErr := createPipelineScheduleBestEffort(c.Request.Context(), database, userID, id, stype, spec)
				if schedErr != nil {
					log.Printf("[CreatePipeline] auto-schedule failed (ignored): %v", schedErr)
				} else {
					log.Printf("[CreatePipeline] auto-schedule created (%s) for pipeline=%s", stype, id)
				}
			}
		}
	}

	c.JSON(http.StatusCreated, pipeline)
}

// resolveEffectiveSyncMode resolves whether a pipeline should be treated as batch or cdc.
// Priority: pipeline sync_mode override, pipeline cdc_mode presence, source connection sync_mode, default batch.
// pipelineModeIsCDC mirrors pipelineRowIsCDCSQL for a loaded pipeline row.
func pipelineModeIsCDC(syncMode, cdcMode *string) bool {
	if syncMode != nil && strings.TrimSpace(*syncMode) != "" {
		return strings.EqualFold(strings.TrimSpace(*syncMode), "cdc")
	}
	return cdcMode != nil && strings.TrimSpace(*cdcMode) != ""
}

func resolveEffectiveSyncMode(database *sql.DB, pipelineID string, sourceConnectionID string, syncMode *string, cdcMode *string) string {
	if syncMode != nil && strings.TrimSpace(*syncMode) != "" {
		return strings.ToLower(strings.TrimSpace(*syncMode))
	}
	if cdcMode != nil && strings.TrimSpace(*cdcMode) != "" {
		return "cdc"
	}
	if sourceConnectionID != "" {
		var scMode sql.NullString
		_ = database.QueryRow(`SELECT sync_mode FROM connections WHERE id = $1`, sourceConnectionID).Scan(&scMode)
		if scMode.Valid {
			m := strings.ToLower(strings.TrimSpace(scMode.String))
			if m == "cdc" || m == "batch" {
				return m
			}
		}
	}
	return "batch"
}

var (
	_reEveryN    = regexp.MustCompile(`(?i)\bevery\s+(\d+)\s*(minute|minutes|min|hour|hours|hr|hrs|day|days)\b`)
	_reEveryHour = regexp.MustCompile(`(?i)\bevery\s+hour\b`)
	_reEveryDay  = regexp.MustCompile(`(?i)\b(daily|every\s+day)\b`)
)

// inferScheduleFromNaturalLanguage extracts a schedule intent from a free-form request.
// This is intentionally conservative and only supports a small set of patterns.
func inferScheduleFromNaturalLanguage(req string) (scheduleType string, spec ScheduleSpec, ok bool) {
	q := strings.TrimSpace(req)
	if q == "" {
		return "", ScheduleSpec{}, false
	}

	// every hour
	if _reEveryHour.MatchString(q) {
		return "interval", ScheduleSpec{EverySeconds: 3600, Timezone: "UTC"}, true
	}

	// every N <unit>
	if m := _reEveryN.FindStringSubmatch(q); len(m) == 3 {
		n, _ := strconv.Atoi(m[1])
		unit := strings.ToLower(m[2])
		if n <= 0 {
			return "", ScheduleSpec{}, false
		}
		secs := 0
		switch unit {
		case "minute", "minutes", "min":
			secs = n * 60
		case "hour", "hours", "hr", "hrs":
			secs = n * 3600
		case "day", "days":
			secs = n * 86400
		}
		if secs > 0 {
			return "interval", ScheduleSpec{EverySeconds: secs, Timezone: "UTC"}, true
		}
	}

	// daily / every day → default cron at midnight UTC
	if _reEveryDay.MatchString(q) {
		return "cron", ScheduleSpec{Cron: "0 0 * * *", Timezone: "UTC"}, true
	}

	return "", ScheduleSpec{}, false
}

// createPipelineScheduleBestEffort inserts a schedule row and creates the Temporal schedule.
// This intentionally does not fail the caller if Temporal operations fail.
func createPipelineScheduleBestEffort(ctx context.Context, database *sql.DB, userID, pipelineID, scheduleType string, spec ScheduleSpec) (string, error) {
	// Validate + normalize (reuse schedule validator)
	if err := validateScheduleSpec(scheduleType, spec); err != nil {
		return "", err
	}
	if spec.Timezone == "" {
		spec.Timezone = "UTC"
	}

	// Uniqueness: only one schedule per pipeline.
	var existingCount int
	if err := database.QueryRow(`
		SELECT COUNT(*)
		FROM pipeline_schedules
		WHERE pipeline_id = $1 AND status != 'deleted'
	`, pipelineID).Scan(&existingCount); err != nil {
		return "", err
	}
	if existingCount > 0 {
		return "", nil
	}

	// Transaction to insert schedule.
	tx, err := database.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return "", err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	scheduleID := uuid.New().String()
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}

	if _, err := tx.Exec(`
		INSERT INTO pipeline_schedules (schedule_id, pipeline_id, schedule_type, schedule_spec, temporal_schedule_id, created_by)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, scheduleID, pipelineID, scheduleType, specJSON, scheduleID, userID); err != nil {
		return "", err
	}

	// Create Temporal schedule before commit; if this fails we abort to avoid orphan schedule rows.
	if err := createTemporalSchedule(ctx, scheduleID, pipelineID, scheduleType, spec); err != nil {
		return "", err
	}

	if err := tx.Commit(); err != nil {
		// Commit failures can be ambiguous (e.g., network issues). Only cleanup Temporal if we
		// successfully rolled back the DB transaction (i.e., we know the row is not persisted).
		if rerr := tx.Rollback(); rerr == nil {
			_ = deleteTemporalSchedule(ctx, scheduleID)
		}
		return "", err
	}
	committed = true

	return scheduleID, nil
}

// sanitizeCDCInitialLoad coerces a caller-supplied cdc_initial_load to a value that
// satisfies the pipelines_cdc_initial_load_check constraint: "batch" or "debezium", else
// nil (NULL = default Debezium snapshot). Nil-safe.
func sanitizeCDCInitialLoad(v *string) *string {
	if v == nil {
		return nil
	}
	s := strings.ToLower(strings.TrimSpace(*v))
	if s == "batch" || s == "debezium" {
		return &s
	}
	return nil
}

// Helper to handle empty strings as NULL for DB
func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// Helper to handle empty JSON bytes as NULL for DB (and ensure jsonb casts work)
func nullJSONString(b []byte) *string {
	if len(b) == 0 {
		return nil
	}
	s := string(b)
	return &s
}

// UpdatePipeline updates a pipeline
func UpdatePipeline(c *gin.Context) {
	id, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	wsID, ok := requirePipelineWorkspaceRole(c, id, security.WSMember)
	if !ok {
		return
	}
	database := db.GetDB()

	var req struct {
		Name           *string `json:"name"`
		Description    *string `json:"description"`
		Status         *string `json:"status"`
		DefaultRunMode *string `json:"default_run_mode"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request"})
		return
	}

	updates := map[string]interface{}{}

	// Status follows what the pipeline is doing, so it is not settable here. Writing
	// "archived" left CDC running with its replication slot, and archived pipelines do
	// not block a connection delete, whose cascade then erased the slot's record. A
	// client echoing the current status back is accepted as a no-op. Checked before
	// any write so a rejected request changes nothing.
	if req.Status != nil {
		status := strings.TrimSpace(*req.Status)
		if status == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request"})
			return
		}
		var current string
		err := database.QueryRow("SELECT status FROM pipelines WHERE id = $1 AND workspace_id = $2", id, wsID).Scan(&current)
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
			return
		}
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update pipeline"})
			return
		}
		if !strings.EqualFold(status, strings.TrimSpace(current)) {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "status_not_settable",
				"message": "A pipeline's status follows what it is doing and cannot be set directly. Use POST /pipelines/:id/stop, /pause or /resume, or DELETE /pipelines/:id.",
			})
			return
		}
	}

	// Defense-in-depth: every UPDATE re-filters on workspace_id so a race
	// between the membership gate and the write cannot cross workspaces. A
	// pipeline is workspace-owned, so any member may edit a teammate's pipeline.
	if req.Name != nil {
		name := strings.TrimSpace(*req.Name)
		if name == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "name cannot be empty"})
			return
		}
		if len(name) > 255 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "name too long"})
			return
		}
		updates["name"] = name
		if _, err := database.Exec("UPDATE pipelines SET name=$1, updated_at=NOW() WHERE id=$2 AND workspace_id=$3", name, id, wsID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update pipeline"})
			return
		}
	}

	if req.Description != nil {
		desc := *req.Description
		updates["description"] = desc
		if _, err := database.Exec("UPDATE pipelines SET description=$1, updated_at=NOW() WHERE id=$2 AND workspace_id=$3", desc, id, wsID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update pipeline"})
			return
		}
	}

	if req.DefaultRunMode != nil {
		mode := strings.ToLower(strings.TrimSpace(*req.DefaultRunMode))
		if mode == "resume" || mode == "reload" {
			updates["default_run_mode"] = mode
			_, err := database.Exec("UPDATE pipelines SET default_run_mode=$1, updated_at=NOW() WHERE id=$2 AND workspace_id=$3", mode, id, wsID)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update pipeline"})
				return
			}
		} else if mode != "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request"})
			return
		}
	}

	logAudit(c, "update_pipeline", "pipeline", id, updates)

	c.JSON(http.StatusOK, gin.H{"id": id, "status": "updated"})
}

// DeletePipeline deletes a pipeline
func DeletePipeline(c *gin.Context) {
	id, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	wsID, ok := requirePipelineWorkspaceRole(c, id, security.WSMember)
	if !ok {
		return
	}
	database := db.GetDB()

	// Drop CDC replication slots / publications / connector on the SOURCE DB
	// BEFORE deleting the pipeline row. Ordering is critical: cdc_resources.
	// pipeline_id is a FK to pipelines; deleting the pipeline first detaches
	// (064: SET NULL) or — before that migration — cascade-removed those rows,
	// so a post-delete cleanup read EMPTY and the physical slot leaked on the
	// source server (saturating Azure max_replication_slots). Running cleanup
	// first, while cdc_resources still resolves by pipeline_id, lets the
	// orchestrator drop the real slot. Bounded + synchronous; failures don't
	// block the delete because the CDC reconciler reaps any orphaned slot as a
	// safety net (065/reconciler sweep).
	var teardownWarnings []string
	if w := runCDCCleanupSync(c.Request.Context(), id); w != "" {
		teardownWarnings = append(teardownWarnings, w)
	}

	// Collect any Temporal schedule IDs attached to this pipeline BEFORE deleting
	// the row. pipeline_schedules has an ON DELETE CASCADE FK to pipelines
	// (migration 023), so those rows vanish the instant the pipeline is deleted —
	// but the Temporal schedule is an EXTERNAL resource with no cascade. Without
	// removing it, the cron keeps firing forever for a pipeline that no longer
	// exists (each run then no-ops on the missing pipeline via the workflow's
	// existence guard, but still burns worker slots and pollutes usage meters).
	// We delete them from Temporal only AFTER the pipeline delete commits (below),
	// so a 404 or a rolled-back delete never tears down a live pipeline's schedule.
	temporalScheduleIDs := listTemporalScheduleIDs(c.Request.Context(), database, id)

	// Terminate any workflow still executing for this pipeline, BEFORE the
	// executions rows are deleted (they are what names the workflows: WorkflowID
	// == execution ID). Deleting a pipeline mid-run used to leave its Temporal
	// workflow running against a pipeline that no longer exists — it keeps
	// occupying a worker slot and keeps writing progress rows for a dead parent
	// until it times out on its own, hours later.
	cancelRunningWorkflowsBestEffort(c.Request.Context(), database, id)

	// Start a transaction to ensure both deletes succeed or both fail
	tx, err := database.BeginTx(c.Request.Context(), &sql.TxOptions{})
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete pipeline"})
		return
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// Defense-in-depth: scope the executions delete via a subquery on
	// pipelines.workspace_id so a TOCTOU race after the membership gate cannot
	// drop another workspace's executions.
	_, err = tx.Exec(`DELETE FROM executions
		WHERE pipeline_id = $1
		  AND pipeline_id IN (SELECT id FROM pipelines WHERE id = $1 AND workspace_id = $2)`,
		id, wsID)
	if err != nil {
		log.Printf("Failed to delete pipeline executions: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete pipeline executions"})
		return
	}

	// Pipeline delete is also workspace-scoped — a row in another workspace will
	// not match and we'll 404 below.
	// Models that rebuilt only after this pipeline would otherwise stay active with no
	// upstream at all. In this transaction so a delete that rolls back pauses nothing.
	if _, err := pauseTriggersOrphanedBy(c.Request.Context(), tx, upstreamKindPipeline, id); err != nil {
		log.Printf("Failed to pause model schedules orphaned by pipeline delete: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete pipeline"})
		return
	}

	// Record who owned this pipeline's destination namespace BEFORE the row that
	// answers that question disappears. Nothing on the destination is dropped —
	// the data stays exactly where it is — but ownership of it is answered from
	// `pipelines` (destination_mapping.go namespaceTableOwner), so deleting the
	// row would silently hand the namespace to the next pipeline pointed at the
	// same connection, which adopts the tables and can then reload-drop them.
	// Same transaction as the delete, so the pipeline and its tombstone can never
	// disagree. Best-effort: a gateway running ahead of migration 110 has no such
	// table, and failing the delete over it would be worse than the leak.
	writeDestinationNamespaceTombstone(c.Request.Context(), tx, id, wsID)

	result, err := tx.Exec("DELETE FROM pipelines WHERE id=$1 AND workspace_id=$2", id, wsID)
	if err != nil {
		log.Printf("Failed to delete pipeline: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete pipeline"})
		return
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
		return
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		// Best-effort: commit errors can be ambiguous. If the desired end-state is present,
		// treat it as success to avoid spurious retries.
		var remaining int
		if verr := database.QueryRow(`SELECT COUNT(*) FROM pipelines WHERE id = $1`, id).Scan(&remaining); verr == nil && remaining == 0 {
			committed = true
			deleteTemporalSchedulesBestEffort(c.Request.Context(), temporalScheduleIDs)
			if w := runKafkaTeardownSync(c.Request.Context(), id); w != "" {
				teardownWarnings = append(teardownWarnings, w)
			}
			logAudit(c, "delete_pipeline", "pipeline", id, map[string]interface{}{"commit_error": err.Error()})
			c.JSON(http.StatusOK, deletePipelineResponse(teardownWarnings))
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete pipeline"})
		return
	}
	committed = true

	// Pipeline is gone: tear down its orphaned Temporal schedules (best-effort).
	deleteTemporalSchedulesBestEffort(c.Request.Context(), temporalScheduleIDs)

	// …and its Kafka topics and consumer groups. This one runs AFTER the delete
	// on purpose: while the pipelines row exists, the CDC table-stats agent and
	// the sink workers recreate any consumer group we remove.
	if w := runKafkaTeardownSync(c.Request.Context(), id); w != "" {
		teardownWarnings = append(teardownWarnings, w)
	}

	logAudit(c, "delete_pipeline", "pipeline", id, nil)

	c.JSON(http.StatusOK, deletePipelineResponse(teardownWarnings))
}

// deletePipelineResponse is the delete's success body. The row is gone either way;
// warnings name any source-side or Kafka cleanup that did not finish, so a leaked
// replication slot or sink worker is visible to the caller instead of only in logs.
func deletePipelineResponse(warnings []string) gin.H {
	body := gin.H{"message": "Pipeline deleted successfully"}
	if len(warnings) > 0 {
		body["warnings"] = warnings
	}
	return body
}

// listTemporalScheduleIDs returns the Temporal schedule IDs currently attached to
// a pipeline. It MUST be called before the pipeline row is deleted: the
// pipeline_schedules rows are removed by ON DELETE CASCADE (migration 023) the
// moment the pipeline is gone, so reading them afterward returns nothing. Errors
// are logged and yield an empty slice — a failure here must never block a delete.
func listTemporalScheduleIDs(ctx context.Context, database *sql.DB, pipelineID string) []string {
	rows, err := database.QueryContext(ctx,
		`SELECT temporal_schedule_id FROM pipeline_schedules WHERE pipeline_id = $1`, pipelineID)
	if err != nil {
		log.Printf("orphan-schedule cleanup: list schedules for pipeline %s: %v", pipelineID, err)
		return nil
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var tsID string
		if err := rows.Scan(&tsID); err != nil {
			log.Printf("orphan-schedule cleanup: scan schedule id for pipeline %s: %v", pipelineID, err)
			continue
		}
		if tsID != "" {
			ids = append(ids, tsID)
		}
	}
	return ids
}

// deleteTemporalSchedulesBestEffort removes the given Temporal schedules after a
// pipeline has been deleted. This closes the orphaned-schedule leak: DeletePipeline
// used to delete only the DB rows (which then cascaded away) while the Temporal
// cron kept firing indefinitely. Best-effort by design — the pipeline is already
// gone, so a delete failure is logged, not surfaced, and the per-run existence
// guard in ScheduledPipelineRunWorkflow remains the backstop.
func deleteTemporalSchedulesBestEffort(ctx context.Context, temporalScheduleIDs []string) {
	if temporalClient == nil || len(temporalScheduleIDs) == 0 {
		return
	}
	for _, tsID := range temporalScheduleIDs {
		if err := deleteTemporalSchedule(ctx, tsID); err != nil {
			log.Printf("orphan-schedule cleanup: delete Temporal schedule %s: %v", tsID, err)
		}
	}
}

// runCDCCleanupSync drops a pipeline's CDC resources (replication slot,
// publication, Debezium connector) on the source DB by calling the orchestrator
// synchronously. It MUST be invoked BEFORE the pipeline row is deleted so the
// orchestrator's cdc_resources lookup (keyed by pipeline_id) still resolves.
// Errors are logged, not fatal: the CDC reconciler reaps any slot left behind,
// so a transient orchestrator outage cannot block a user's delete. Bounded so a
// slow/unreachable orchestrator never hangs the request indefinitely.
func runCDCCleanupSync(parent context.Context, pipelineID string) string {
	return postOrchestratorTeardown(parent, "/api/v1/cdc/cleanup", pipelineID, 30*time.Second,
		"CDC cleanup", "reconciler will reap any orphaned slot")
}

// runKafkaTeardownSync deletes a pipeline's Kafka topics and consumer groups.
//
// It MUST be invoked AFTER the pipeline row is deleted — the mirror image of
// runCDCCleanupSync's constraint. While the row still exists, the orchestrator's
// CDC table-stats agent (30s reconcile tick) and the sink workers treat the
// pipeline as live and recreate any consumer group the teardown removes; once
// the row is gone, every recreate path is closed because they all key on a live
// pipelines row.
//
// Best-effort: nothing here can fail the delete. A leaked topic costs disk and
// shows up in the broker listing; it never breaks a pipeline.
func runKafkaTeardownSync(parent context.Context, pipelineID string) string {
	return postOrchestratorTeardown(parent, "/api/v1/cdc/kafka-teardown", pipelineID, 45*time.Second,
		"Kafka teardown", "topics and consumer groups will be left behind")
}

// postOrchestratorTeardown is the shared bounded, best-effort POST behind the two
// teardown calls above: {"pipeline_id": …} to an internal orchestrator endpoint,
// authenticated with the internal service secret. Nothing here fails the delete.
// `label` names the step and `fallback` describes what happens if it doesn't run,
// so an operator reading the log knows the blast radius without opening the code.
//
// It returns "" when the orchestrator reports success, otherwise a short warning
// for the delete response. Full detail goes to the log only: the orchestrator's
// error strings come from source-database drivers and are not for the API body.
func postOrchestratorTeardown(parent context.Context, path, pipelineID string, timeout time.Duration, label, fallback string) string {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	body, _ := json.Marshal(map[string]string{"pipeline_id": pipelineID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		orchestratorBaseURL()+path, strings.NewReader(string(body)))
	if err != nil {
		log.Warnf("%s: failed to build request for pipeline %s: %v", label, pipelineID, err)
		return fmt.Sprintf("%s did not run: %s", label, fallback)
	}
	req.Header.Set("Content-Type", "application/json")
	setInternalServiceSecret(req)
	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		log.Warnf("%s: request failed for pipeline %s: %v (%s)", label, pipelineID, err, fallback)
		return fmt.Sprintf("%s did not run (orchestrator unreachable): %s", label, fallback)
	}
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	summary, detail := teardownOutcome(resp.StatusCode, respBody)
	if summary != "" {
		log.Warnf("%s: FAILED for pipeline %s: %s (%s)", label, pipelineID, detail, fallback)
		return fmt.Sprintf("%s did not finish (%s): %s", label, summary, fallback)
	}
	log.Printf("%s: completed for pipeline %s (HTTP %d)", label, pipelineID, resp.StatusCode)
	return ""
}

// teardownOutcome reads an orchestrator teardown response. Both endpoints answer
// 200 with {"success": false, "errors": [...]} when a step fails, so the status
// code alone reports a leaked slot or sink worker as "completed". It returns a
// short summary ("" on success) and the detail for the log.
func teardownOutcome(status int, body []byte) (summary, detail string) {
	var parsed struct {
		Success *bool    `json:"success"`
		Errors  []string `json:"errors"`
		Error   string   `json:"error"`
	}
	_ = json.Unmarshal(body, &parsed)
	if status < 200 || status >= 300 {
		detail = strings.TrimSpace(parsed.Error)
		if detail == "" {
			detail = strings.Join(parsed.Errors, "; ")
		}
		return fmt.Sprintf("HTTP %d", status), fmt.Sprintf("HTTP %d: %s", status, detail)
	}
	if parsed.Success != nil && !*parsed.Success {
		return fmt.Sprintf("%d error(s)", len(parsed.Errors)), strings.Join(parsed.Errors, "; ")
	}
	return "", ""
}

// cancelRunningWorkflowsBestEffort cancels the Temporal workflow of every
// non-terminal execution of a pipeline. WorkflowID == execution ID in this
// system (same identity CancelExecution relies on), so the execution IDs are the
// workflow IDs.
//
// MUST run BEFORE the executions rows are deleted — afterwards there is no
// record of which workflows belonged to this pipeline, and a running workflow
// would keep going with nothing left to name it.
//
// Best-effort: a Temporal outage must not block the delete. The workflow's own
// pipeline-existence guard is the backstop — it just takes until the next
// activity boundary to notice.
func cancelRunningWorkflowsBestEffort(ctx context.Context, database *sql.DB, pipelineID string) {
	if temporalClient == nil {
		return
	}
	rows, err := database.QueryContext(ctx, `
		SELECT id::text FROM executions
		WHERE pipeline_id = $1
		  AND COALESCE(status, '') NOT IN ('completed', 'failed', 'cancelled')`, pipelineID)
	if err != nil {
		log.Printf("delete_pipeline: list running executions for %s: %v", pipelineID, err)
		return
	}
	defer rows.Close()

	var execIDs []string
	for rows.Next() {
		var execID string
		if err := rows.Scan(&execID); err != nil {
			log.Printf("delete_pipeline: scan execution id for %s: %v", pipelineID, err)
			continue
		}
		if execID != "" {
			execIDs = append(execIDs, execID)
		}
	}

	for _, execID := range execIDs {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := temporalClient.CancelWorkflow(cctx, execID, "")
		cancel()
		if err != nil {
			// A workflow that already completed is not found — the common case
			// here, since most executions are done long before the delete.
			log.Printf("delete_pipeline: Temporal cancel for execution %s: %v", execID, err)
			continue
		}
		log.Printf("delete_pipeline: cancelled Temporal workflow %s for deleted pipeline %s", execID, pipelineID)
	}
}

// GetPipelineStats returns statistics about the caller's pipelines.
// All counts and recent-run listings are scoped via pipelines.created_by so
// stats from other tenants never appear here.
func GetPipelineStats(c *gin.Context) {
	if _, ok := resolveUserID(c); !ok {
		return
	}
	database := db.GetDB()

	// Scope the stat cards to the active workspace so they agree with the list
	// on this same page. These counts previously used WHERE created_by = user,
	// so they summed every pipeline the user created across ALL their
	// workspaces — Total showed 24 while the workspace-scoped list showed 22/2,
	// and Running (raw e.status) disagreed with the workspace-scoped list too.
	// Mirror ListPipelines: with no active workspace, return zeroed counts
	// rather than passing NULL (which would leak cross-workspace totals). (R5)
	wsID := activeWorkspaceID(c)
	if wsID == "" {
		c.JSON(http.StatusOK, gin.H{
			"pipelines":   gin.H{"total": 0, "active": 0},
			"executions":  gin.H{"total": 0, "running": 0, "completed": 0, "failed": 0},
			"recent_runs": []any{},
		})
		return
	}

	var total, active int
	if err := database.QueryRow(
		`SELECT COUNT(*), COUNT(*) FILTER (WHERE status = 'active') FROM pipelines WHERE workspace_id = $1`,
		wsID,
	).Scan(&total, &active); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query pipelines"})
		return
	}

	// `AND e.id <> e.pipeline_id` here and on every other user-visible executions
	// read below excludes SYNTHETIC rows. CDC has no per-run execution, so the
	// streaming audit writers (kafka-mcp-sink's transform-log and CDC-ack
	// persisters) insert a placeholder `executions` row keyed `id = pipeline_id`
	// purely to satisfy the audit tables' foreign key. Those rows are never a
	// run: they are created once per CDC pipeline, carry status 'running'
	// forever, and would otherwise inflate "Total runs", the Running card, and
	// the run-history list. Real executions get a fresh uuid.New(), so
	// `id = pipeline_id` cannot collide with one.
	//
	// Completed/failed read the same derived status as the executions list, so a
	// CDC run that is still streaming (its row closed 'completed' at the
	// backfill→streaming handoff) is not counted as completed here while the list
	// and the pipeline page call it running.
	var completed, failed, totalExecs int
	if err := database.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE (`+executionStatusSQL+`) IN ('completed', 'success')),
			COUNT(*) FILTER (WHERE (`+executionStatusSQL+`) = 'failed'),
			COUNT(*)`+executionFromSQL+`
		WHERE p.workspace_id = $1
		  AND e.id <> e.pipeline_id
	`, wsID).Scan(&completed, &failed, &totalExecs); err != nil {
		completed, failed, totalExecs = 0, 0, 0
	}

	// The "Running" card must count PIPELINES the list would badge 'running', not
	// open executions rows. A streaming CDC pipeline has NO executions row in
	// status 'running' (the temporal-adapter closes it at the backfill→streaming
	// handoff), so the old COUNT(*) FILTER (WHERE e.status = 'running') read 0 on
	// the very page whose row badge said "Running".
	//
	// It evaluates the list's own derived_status CASE (pipelineDerivedStatusCaseSQL)
	// over the same joins, instead of a hand-mirrored copy of its predicates: that
	// copy had already drifted from the list (it ignored the stale-heartbeat branch
	// and could not follow the pp.status='running' streaming branch), which is how
	// the card and the row badges disagreed. `le` is the latest executions row
	// exactly as the list reads it — including a synthetic `id = pipeline_id` CDC
	// audit row when that is the newest, because the list does not exclude it
	// either and the card's contract is "what the list badges". pipeline_progress's
	// PK is pipeline_id and both subqueries are DISTINCT ON pipeline_id, so each
	// pipeline is counted at most once.
	var running int
	if err := database.QueryRow(`
		SELECT COUNT(*)
		FROM pipelines p
		LEFT JOIN pipeline_progress pp ON pp.pipeline_id = p.id
		LEFT JOIN (
		  SELECT DISTINCT ON (e.pipeline_id) e.pipeline_id, e.id AS execution_id, e.status AS execution_status,
		         e.start_time AS started_at, e.end_time AS completed_at
		  FROM executions e
		  JOIN pipelines ep ON ep.id = e.pipeline_id AND ep.workspace_id = $1
		  ORDER BY e.pipeline_id, e.start_time DESC
		) le ON le.pipeline_id = p.id
		LEFT JOIN (
		  SELECT DISTINCT ON (ps.pipeline_id) ps.pipeline_id, ps.schedule_id, ps.status AS schedule_status
		  FROM pipeline_schedules ps WHERE ps.status != 'deleted' ORDER BY ps.pipeline_id, ps.created_at DESC
		) so ON so.pipeline_id = p.id
		WHERE p.workspace_id = $1
		  AND (`+pipelineDerivedStatusCaseSQL+`) = 'running'
	`, wsID).Scan(&running); err != nil {
		running = 0
	}

	type recentRun struct {
		PipelineID string  `json:"pipeline_id"`
		Status     string  `json:"status"`
		StartTime  string  `json:"start_time"`
		EndTime    *string `json:"end_time,omitempty"`
	}

	rows, err := database.Query(`
		SELECT e.pipeline_id::text, e.status,
		       e.start_time::text,
		       CASE WHEN e.end_time IS NOT NULL THEN e.end_time::text ELSE NULL END
		FROM executions e
		JOIN pipelines p ON p.id = e.pipeline_id
		WHERE p.workspace_id = $1
		  AND e.id <> e.pipeline_id
		ORDER BY e.start_time DESC LIMIT 10
	`, wsID)
	var recentRuns []recentRun
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var r recentRun
			if scanErr := rows.Scan(&r.PipelineID, &r.Status, &r.StartTime, &r.EndTime); scanErr == nil {
				recentRuns = append(recentRuns, r)
			}
		}
	}
	if recentRuns == nil {
		recentRuns = []recentRun{}
	}

	c.JSON(http.StatusOK, gin.H{
		"pipelines": gin.H{
			"total":  total,
			"active": active,
		},
		// NOTE: "running" is now a PIPELINE count (it matches the list's per-row
		// derived_status badge, streaming CDC included); total/completed/failed
		// remain executions-row counts, which is what the cards label them
		// ("Successful runs"/"Failed runs"). The key keeps its name and place so
		// the stat cards need no frontend change — PipelinesPageClient.tsx reads
		// executions.running for the card labelled "Running".
		"executions": gin.H{
			"total":     totalExecs,
			"running":   running,
			"completed": completed,
			"failed":    failed,
		},
		"recent_runs": recentRuns,
	})
}

// RunPipelineRequest documents the optional JSON body of POST
// /api/v1/pipelines/:id/run. The endpoint also accepts the same fields
// as URL query parameters (`?run_mode=reload`) — query params take
// precedence over body to keep curl-style usage simple. An empty body
// is valid and starts a default run.
type RunPipelineRequest struct {
	// RunMode controls checkpoint behaviour.
	//   - "resume"  (default): continue from the latest checkpoint
	//   - "reload": rebuild the destination tables from scratch
	RunMode string `json:"run_mode,omitempty"`
	// AckWarnings acknowledges the pre-migration assessment and tells
	// the server to skip the warning-gate. Required when the
	// assessment reports any non-info findings. Errors are always
	// blocking regardless of this flag.
	AckWarnings bool `json:"ack_warnings,omitempty"`
	// DestinationConfig, when present, is the destination mapping confirmed in
	// the destination-mapping HITL (PR-C). It is persisted to the pipeline's
	// config BEFORE the assessment runs, so the namespace existence / privilege
	// / validity checks validate exactly what the user chose. Pipeline-scoped
	// only — never touches the connection config.
	DestinationConfig *DestinationConfig `json:"destination_config,omitempty"`
	// NominatedKeys lets the user designate identifying column(s) as the key for
	// keyless / GIPK source tables (PR-D column nomination). Shape:
	// { "<table>": ["col1", "col2"] }. Persisted to config.nominated_keys BEFORE
	// the assessment runs, so the keyless WARNING is suppressed for nominated
	// tables, and consumed by the executor so the sink upserts on these columns
	// instead of the content-hash surrogate key. Pipeline-scoped only.
	NominatedKeys map[string][]string `json:"nominated_keys,omitempty"`
}

// RunPipeline triggers execution of a pipeline
//
// Accepts an optional JSON body (see RunPipelineRequest) OR equivalent
// query parameters (`?run_mode=...`). With no arguments, defaults to
// run_mode="resume".
func RunPipeline(c *gin.Context) {
	id, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	// Authorize by the active workspace (shared resource): any member may run a
	// teammate's pipeline. userID is still resolved because the COST quota below
	// is per-USER (the caller bears the LLM spend); the PLAN quota, in contrast,
	// is per-WORKSPACE (the billable tenant) and keys on wsID.
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	wsID, ok := requirePipelineWorkspaceRole(c, id, security.WSMember)
	if !ok {
		return
	}
	database := db.GetDB()

	// Per-user monthly cost quota gate. Refuse new runs when the user is
	// already over their LLM-spend cap so a single buggy pipeline can't
	// drain cloud credits. The actual charge happens in llm-service per
	// completion via check_and_charge_user(); this is just a soft pre-check.
	if !checkUserCostQuotaOK(c.Request.Context(), database, userID) {
		c.JSON(http.StatusPaymentRequired, gin.H{
			"error":   "quota_exceeded",
			"message": "Monthly LLM spend quota exceeded. Contact admin to raise cost_quota_usd_cents.",
		})
		return
	}

	// Plan run gate. A workspace whose trial/free window has fully expired (or
	// whose plan is marked can_run=false) may not start new runs. Scoped to the
	// active workspace (the billable tenant), not the caller. See plan_quota.go.
	if q := resolvePlanQuota(c.Request.Context(), database, wsID); !q.canRun {
		c.JSON(http.StatusPaymentRequired, gin.H{
			"error":   "trial_expired",
			"message": "Your free trial has ended. Upgrade to Pro to run pipelines.",
			"plan":    q.plan,
		})
		return
	}

	// Plan GB run gate (Ship 2 Phase 2). Refuse a new run when the workspace is
	// already over its monthly data-transfer allowance. Per-WORKSPACE; a soft
	// pre-check like the cost gate above (bytes accrue post-run). Fail-open inside.
	if allowed, errBody := checkWorkspaceGBOK(c.Request.Context(), database, wsID); !allowed {
		c.JSON(http.StatusPaymentRequired, errBody)
		return
	}

	// Per-pipeline cooldown. Prevents a runaway client or UI double-click
	// from spawning many concurrent runs of the same heavy pipeline.
	cooldown := 10
	if v := strings.TrimSpace(os.Getenv("PIPELINE_RUN_COOLDOWN_SECONDS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cooldown = n
		}
	}
	if !checkPipelineRunCooldown(c.Request.Context(), database, id, cooldown) {
		c.JSON(http.StatusTooManyRequests, gin.H{
			"error":   "pipeline_cooldown",
			"message": "Another run for this pipeline started in the last " + strconv.Itoa(cooldown) + " seconds. Wait before retrying.",
		})
		return
	}

	// Parse the optional run body up front so we can pick up
	// ack_warnings before any side-effects (status updates,
	// execution records). The body is also used later for run_mode;
	// ignore decode errors here — an empty body is valid.
	var runReq RunPipelineRequest
	_ = c.ShouldBindJSON(&runReq)
	ackWarnings := runReq.AckWarnings || strings.EqualFold(c.Query("ack_warnings"), "true")

	// Persist the destination mapping confirmed in the HITL BEFORE assessing, so
	// the namespace existence/privilege/validity checks evaluate the user's
	// actual choice. Pipeline-scoped only (never the connection). Best-effort:
	// a write failure falls through to the previously-persisted config.
	if runReq.DestinationConfig != nil {
		dc := *runReq.DestinationConfig
		dc.Namespace = strings.TrimSpace(dc.Namespace)
		if dc.NamespaceKind == "" {
			var destConnID, destConnType string
			_ = database.QueryRow(`SELECT destination_connection_id FROM pipelines WHERE id = $1`, id).Scan(&destConnID)
			if destConnID != "" {
				_ = database.QueryRow(`SELECT connector_type FROM connections WHERE id = $1`, destConnID).Scan(&destConnType)
			}
			dc.NamespaceKind = namespaceKindForConnector(destConnType)
		}
		if err := persistDestinationConfig(database, id, dc); err != nil {
			log.Printf("[RunPipeline] failed to persist destination_config override (best-effort): %v", err)
		}
	}

	// Persist user-nominated key columns (PR-D) BEFORE assessing so the keyless
	// WARNING is suppressed for nominated tables and the executor upserts on
	// them. Best-effort: a write failure falls through to the previously-
	// persisted config (or the synthetic-hash path if never set).
	if clean := sanitizeNominatedKeys(runReq.NominatedKeys); len(clean) > 0 {
		if b, err := json.Marshal(clean); err == nil {
			if _, err := database.Exec(`
				UPDATE pipelines
				SET config = jsonb_set(COALESCE(config, '{}'::jsonb), '{nominated_keys}', $1::jsonb, true),
				    updated_at = NOW()
				WHERE id = $2::uuid
			`, string(b), id); err != nil {
				log.Printf("[RunPipeline] failed to persist nominated_keys (best-effort): %v", err)
			}
		}
	}

	// Pre-migration assessment gate. The assessment ALWAYS runs: `ack_warnings`
	// (body flag or curl-friendly ?ack_warnings=true) waives WARNINGS only.
	// Errors are blocking and are never waivable — see evaluateAssessmentGate.
	// Info findings pass through silently.
	{
		assessCtx, cancel := context.WithTimeout(c.Request.Context(), 90*time.Second)
		report, status, errResp, err := buildPipelineAssessment(assessCtx, database, c.GetString("workspace_id"), id, userID)
		cancel()
		if err == nil && report != nil && errResp == nil {
			// Keep this run in the Assessment tab's history. Best-effort:
			// a failed write never changes what the gate decides.
			recCtx, recCancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
			if runID, rerr := recordAssessmentRun(recCtx, database, id, AssessmentTriggerRunGate, userID, report); rerr != nil {
				log.Printf("[RunPipeline] record assessment run pipeline=%s: %v", id, rerr)
			} else {
				report.RunID = runID
			}
			recCancel()
			switch evaluateAssessmentGate(report, ackWarnings) {
			case assessmentGateBlocked:
				c.JSON(http.StatusUnprocessableEntity, gin.H{
					"error":      "pre_migration_assessment_blocked",
					"message":    "Pre-migration assessment found blocking errors",
					"assessment": report,
					"hint":       "These are errors, not warnings — ack_warnings does not clear them. Fix the reported problems, then re-run.",
				})
				return
			case assessmentGateNeedsAck:
				c.JSON(http.StatusUnprocessableEntity, gin.H{
					"error":      "pre_migration_assessment",
					"message":    "Pre-migration assessment requires acknowledgement before run",
					"assessment": report,
					"hint":       "Re-submit with ack_warnings: true after the user reviews the warnings",
				})
				return
			}
		}
		// errResp / err paths intentionally fall through — if the
		// assessment itself can't run (404, source unreachable, etc.)
		// don't block the run. The downstream pipeline will surface
		// the same issue with its own error.
		_ = status
	}

	enqueuePipelineRun(c, database, id, wsID, userID, runReq)
}

// enqueuePipelineRun performs the shared "load pipeline → snapshot connectors →
// flip to running → create execution + progress rows → enqueue the Temporal
// workflow → respond" tail used by BOTH entry points: the user-facing
// RunPipeline and the service-to-service RunPipelineInternal (self-healer
// BackoffRetry). Factoring it out keeps the enqueue path byte-for-byte
// identical across the two callers; they differ only in their prologues —
// RunPipeline resolves the caller + runs the per-user cost quota / CSRF /
// pre-migration assessment, while RunPipelineInternal resolves the workspace
// straight from the pipeline row and skips those user-session concerns. Writes
// the HTTP response on every path (success and error), so callers must not
// write their own response after invoking it.
// resolveRunMode decides which run mode a dispatch actually runs under.
//
// Precedence, highest first: an explicit ?run_mode= query value, then the
// request body's run_mode, then the pipeline's stored default_run_mode, then
// "resume". Only "resume" and "reload" are honoured as overrides — anything
// else falls through to the pipeline default, which is why an unrecognised
// value cannot silently become a reload.
//
// Extracted from enqueuePipelineRun so the precedence is unit-testable: the
// third rung is load-bearing and is exactly what made the unattended healer
// path destructive before RunPipelineInternal started pinning RunMode
// (see the comment at the pin). A caller that leaves RunMode empty inherits
// the pipeline's default — for a "Reload" pipeline that means a full
// destination wipe.
func resolveRunMode(queryOverride, bodyRunMode string, defaultRunMode *string) string {
	override := strings.ToLower(strings.TrimSpace(queryOverride))
	// Query > body matches the documented precedence on RunPipelineRequest.
	if override == "" {
		override = strings.ToLower(strings.TrimSpace(bodyRunMode))
	}

	runMode := "resume"
	if defaultRunMode != nil && *defaultRunMode != "" {
		runMode = *defaultRunMode
	}
	if override == "resume" || override == "reload" {
		runMode = override
	}
	return runMode
}

func enqueuePipelineRun(c *gin.Context, database *sql.DB, id, wsID, userID string, runReq RunPipelineRequest) {
	// Get full pipeline details including connections
	var name, status string
	var sourceConnID, destConnID, nlRequest, createdBy, dataset, defaultRunMode *string // All nullable fields as pointers
	var configJSON []byte
	err := database.QueryRow(`
		SELECT name, status, source_connection_id, destination_connection_id, natural_language_request, created_by, config, dataset, default_run_mode
		FROM pipelines WHERE id = $1 AND workspace_id = $2
	`, id, wsID).Scan(&name, &status, &sourceConnID, &destConnID, &nlRequest, &createdBy, &configJSON, &dataset, &defaultRunMode)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
		return
	}

	// Get connection details (connector types)
	var sourceType, destType string
	if sourceConnID != nil && *sourceConnID != "" {
		database.QueryRow("SELECT connector_type FROM connections WHERE id = $1", *sourceConnID).Scan(&sourceType)
	}
	if destConnID != nil && *destConnID != "" {
		database.QueryRow("SELECT connector_type FROM connections WHERE id = $1", *destConnID).Scan(&destType)
	}

	// NEW: Resolve and snapshot connector versions if connections are known
	var sourceSnapshotJSON, destSnapshotJSON []byte
	var sourceSnapshotObj, destSnapshotObj *services.ConnectorSnapshot
	versionResolver := services.NewVersionResolver("")

	if sourceConnID != nil && *sourceConnID != "" {
		sourceSnapshot, err := versionResolver.ResolveConnectorVersion(*sourceConnID)
		if err != nil {
			log.Printf("⚠️ Failed to resolve source connector version: %v (will retry in workflow)", err)
		} else {
			sourceSnapshotObj = sourceSnapshot
			if b, mErr := json.Marshal(sourceSnapshot); mErr != nil {
				log.Printf("⚠️ Failed to marshal source connector snapshot: %v (snapshot will be omitted)", mErr)
			} else {
				sourceSnapshotJSON = b
			}
			log.Printf("✅ Resolved source connector: %s@%s", sourceSnapshot.Type, sourceSnapshot.Version)
		}
	}

	if destConnID != nil && *destConnID != "" {
		destSnapshot, err := versionResolver.ResolveConnectorVersion(*destConnID)
		if err != nil {
			log.Printf("⚠️ Failed to resolve destination connector version: %v (will retry in workflow)", err)
		} else {
			destSnapshotObj = destSnapshot
			if b, mErr := json.Marshal(destSnapshot); mErr != nil {
				log.Printf("⚠️ Failed to marshal destination connector snapshot: %v (snapshot will be omitted)", mErr)
			} else {
				destSnapshotJSON = b
			}
			log.Printf("✅ Resolved destination connector: %s@%s", destSnapshot.Type, destSnapshot.Version)
		}
	}

	// Update status to running and save snapshots
	if sourceSnapshotJSON != nil || destSnapshotJSON != nil {
		// Update with snapshots
		_, err = database.Exec(`
			UPDATE pipelines 
			SET status = 'running', 
			    updated_at = NOW(),
			    source_connector_snapshot = COALESCE($2::jsonb, source_connector_snapshot),
			    destination_connector_snapshot = COALESCE($3::jsonb, destination_connector_snapshot)
			WHERE id = $1
		`, id, nullJSONString(sourceSnapshotJSON), nullJSONString(destSnapshotJSON))
	} else {
		// Update without snapshots (will be resolved in workflow)
		_, err = database.Exec("UPDATE pipelines SET status = 'running', updated_at = NOW() WHERE id = $1", id)
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update pipeline status"})
		return
	}

	// Create execution record
	execID := uuid.New().String()
	if _, err := database.Exec(`
		INSERT INTO executions (id, pipeline_id, status, start_time)
		VALUES ($1, $2, 'running', NOW())
	`, execID, id); err != nil {
		log.Printf("❌ Failed to create execution record (pipeline_id=%s execution_id=%s pipeline_name=%s): %v", id, execID, name, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create execution record"})
		return
	}

	// Sticky table selection: use persisted selection when available, otherwise infer from last run table-stats.
	// This prevents re-prompting for table selection on reruns (manual + scheduled).
	selectedTables := extractSelectedTablesFromConfigJSON(configJSON)
	configHadTables := len(selectedTables) > 0
	if len(selectedTables) == 0 {
		selectedTables = inferSelectedTablesFromLastRun(database, id)
	}

	// Propagate connection IDs to downstream (also used for fast rerun detection).
	sourceConnIDVal := ""
	if sourceConnID != nil {
		sourceConnIDVal = strings.TrimSpace(*sourceConnID)
	}
	destConnIDVal := ""
	if destConnID != nil {
		destConnIDVal = strings.TrimSpace(*destConnID)
	}

	fastRerun := sourceConnIDVal != "" && sourceConnIDVal != "auto" &&
		destConnIDVal != "" && destConnIDVal != "auto" &&
		len(selectedTables) > 0

	// Lifecycle=draft gate (mirrors CreatePipeline). Refuse to re-run
	// a pipeline whose source or destination connector has slipped
	// back into "draft" (e.g. all recent runs failed). Escape via
	// `?allow_draft=true`. ARCH-1 audit flagged that this gate
	// previously fired only on CreatePipeline.
	if sourceConnIDVal != "auto" && destConnIDVal != "auto" {
		if blocked := checkConnectionLifecycleDraft(c, database, sourceConnIDVal, destConnIDVal); blocked {
			return
		}
	}

	// Best-effort: mark pipeline_progress as started (authoritative updates come from Temporal adapter).
	initialStage := "intent"
	initialTotalSteps := 7
	initialMessage := "Starting pipeline execution..."
	if fastRerun {
		initialStage = "executor"
		initialTotalSteps = 1
		initialMessage = "Starting execution (fast rerun)..."
	}
	if _, err := database.Exec(`
		INSERT INTO pipeline_progress (
			pipeline_id,
			execution_id,
			status,
			current_stage,
			progress_percent,
			progress_current_step,
			progress_total_steps,
			message,
			created_at,
			updated_at
		)
		VALUES ($1, $2, 'processing', $3, 0, 0, $4, $5, NOW(), NOW())
		ON CONFLICT (pipeline_id) DO UPDATE
		SET execution_id = EXCLUDED.execution_id,
		    status = EXCLUDED.status,
		    current_stage = EXCLUDED.current_stage,
		    progress_percent = EXCLUDED.progress_percent,
		    progress_current_step = EXCLUDED.progress_current_step,
		    progress_total_steps = EXCLUDED.progress_total_steps,
		    message = EXCLUDED.message,
		    updated_at = NOW()
	`, id, execID, initialStage, initialTotalSteps, initialMessage); err != nil {
		log.Printf("⚠️ [RunPipeline] failed to upsert pipeline_progress (ignored): %v", err)
	}

	// Build intent request for the full agentic flow
	// AGENTIC PIPELINE: Always use the full agent flow
	// Intent → Resolver → Discovery → Planner → Validator → Executor
	var requestText string
	if nlRequest != nil && *nlRequest != "" {
		requestText = *nlRequest
	} else {
		// Construct a meaningful NL request from connection types
		requestText = fmt.Sprintf("Transfer data from %s to %s for pipeline: %s", sourceType, destType, name)
	}

	log.Printf("📤 Starting pipeline execution: request='%s'", requestText)

	// NEW ARCHITECTURE: Use Temporal workflows for orchestration
	// API Gateway → Temporal → Adapter → Kafka → Agents
	//
	// Lazily (re)dial Temporal if the startup connection failed — otherwise an
	// api-gateway that booted before Temporal was ready would stay permanently
	// "workflows disabled" until a manual restart.
	tc := getTemporalClient()
	if tc != nil {
		// Start Temporal workflow for pipeline execution
		// CRITICAL: Workflow ID must be unique per execution
		// Using execution_id ensures each run is a fresh workflow
		workflowOptions := client.StartWorkflowOptions{
			ID:        execID, // Use execution ID for uniqueness
			TaskQueue: "pipeline-workflows",
			// Ensure HITL flows (e.g., table selection) have enough time for a human to respond.
			// Without this, the Temporal namespace default timeout can terminate the workflow while
			// the UI still shows a "waiting_for_user" state.
			WorkflowExecutionTimeout: getPipelineWorkflowTimeout(),
			WorkflowRunTimeout:       getPipelineWorkflowTimeout(),
		}

		// Derive dataset for storage paths if not set
		datasetVal := ""
		if dataset != nil && *dataset != "" {
			datasetVal = *dataset
		} else {
			datasetVal = slugifyPipelineName(name)
		}

		// Optional request override: allow callers to choose run_mode per execution.
		// Supported: "resume" (continue from checkpoints) or "reload" (rebuild from scratch).
		//
		// IMPORTANT: gin's request body is a one-shot stream. The earlier
		// pre-migration assessment gate (top of this handler) already
		// called `c.ShouldBindJSON(&runReq)` to extract `ack_warnings`.
		// Reading the body again here returns empty — that silently
		// dropped `run_mode: reload` from the body for months, sending
		// every "Reload" click through as a `resume`. We now read the
		// already-decoded `runReq` from the gate instead of re-parsing
		// the body.
		// Query > body > pipeline default > "resume". See resolveRunMode.
		runModeVal := resolveRunMode(c.Query("run_mode"), runReq.RunMode, defaultRunMode)

		workflowInput := map[string]interface{}{
			"pipeline_id":  id,
			"execution_id": execID,
			"message":      requestText, // V2: Just the message, workflow handles rest
			"user_id":      userID,
			// Tenant scope for the orchestrator's workspace resolver — the same
			// workspace this handler already scoped the pipeline SELECT by.
			"workspace_id":              wsID,
			"source_connection_id":      sourceConnIDVal,
			"destination_connection_id": destConnIDVal,
			// Dataset namespace for cloud storage paths
			"dataset":       datasetVal,
			"pipeline_name": name, // Also pass name for executor fallback
			// Run mode: "resume" (continue from checkpoints) or "reload" (rebuild from scratch)
			"run_mode": runModeVal,
			// Optional: provide snapshots up-front for versioned routing (executor will use these)
			"source_connector_snapshot":      sourceSnapshotObj,
			"destination_connector_snapshot": destSnapshotObj,
		}

		if len(selectedTables) > 0 {
			workflowInput["selected_tables"] = selectedTables
			// Best-effort: persist inferred selection back to pipelines.config.selected_tables so future runs are sticky.
			if !configHadTables {
				if b, err := json.Marshal(selectedTables); err == nil {
					if _, err := database.Exec(`
						UPDATE pipelines
						SET config = jsonb_set(COALESCE(config, '{}'::jsonb), '{selected_tables}', $2::jsonb, true),
						    updated_at = NOW()
						WHERE id = $1
					`, id, string(b)); err != nil {
						log.Printf("⚠️ [RunPipeline] failed to persist inferred selected_tables (ignored): %v", err)
					}
				}
			}
			log.Printf("✅ Sticky table selection enabled for run: %d tables (fast_rerun=%v)", len(selectedTables), fastRerun)
		} else {
			log.Printf("⚠️ No selected_tables found for run (pipeline_id=%s) — workflow may prompt for table selection", id)
		}

		// Propagate distributed trace context into Temporal workflow input so Temporal adapter
		// can attach trace headers to Kafka commands/events.
		otelCtx := telemetry.GetOTelContext(c)
		reqCtx := context.WithoutCancel(otelCtx)
		traceHeaders := telemetry.InjectTraceToHeaders(reqCtx)
		if v := traceHeaders["trace_id"]; v != "" {
			workflowInput["trace_id"] = v
		}
		if v := traceHeaders["traceparent"]; v != "" {
			workflowInput["traceparent"] = v
		}
		if v := traceHeaders["tracestate"]; v != "" {
			workflowInput["tracestate"] = v
		}

		workflowRun, err := tc.ExecuteWorkflow(
			reqCtx,
			workflowOptions,
			"NLPipelineWorkflowV2", // V2 workflow (deterministic, state machine)
			workflowInput,
		)

		if err != nil {
			log.Printf("❌ Failed to start Temporal workflow: %v", err)
			if _, uerr := database.Exec("UPDATE pipelines SET status = 'failed', updated_at = NOW() WHERE id = $1", id); uerr != nil {
				log.Printf("⚠️ [RunPipeline] failed to mark pipeline failed (ignored): %v", uerr)
			}
			if _, uerr := database.Exec("UPDATE executions SET status = 'failed', end_time = NOW(), error_message = $1 WHERE id = $2", err.Error(), execID); uerr != nil {
				log.Printf("⚠️ [RunPipeline] failed to mark execution failed (ignored): %v", uerr)
			}
			if _, uerr := database.Exec(`
				UPDATE pipeline_progress
				SET status = 'failed',
				    current_stage = 'intent',
				    message = $1,
				    updated_at = NOW()
				WHERE pipeline_id = $2
			`, err.Error(), id); uerr != nil {
				log.Printf("⚠️ [RunPipeline] failed to mark pipeline_progress failed (ignored): %v", uerr)
			}
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":   "Failed to start workflow",
				"message": err.Error(),
			})
			return
		}

		log.Printf("✅ Started Temporal workflow: %s (run: %s)", workflowOptions.ID, workflowRun.GetRunID())
		log.Printf("   Pattern: Temporal → Kafka → Agents")

	} else {
		// Temporal is the ONLY supported orchestration path. The old fallback
		// here published `intentRequest` to the Kafka topic `agent.intent.requests`,
		// which NO consumer reads (the orchestrator IntentWorker listens on
		// `agent.control.commands.intent`). That silently hung every run in
		// "processing" forever with no error surfaced to the user.
		//
		// getTemporalClient() already attempted a fresh dial above, so reaching
		// here means Temporal is genuinely unreachable. Fail loudly with 503 and
		// mark the run failed so the state is visible and the user can retry once
		// Temporal is healthy — never silently publish to a dead topic.
		log.Printf("❌ Temporal orchestration unavailable — refusing to start run (pipeline_id=%s exec=%s)", id, execID)
		errMsg := "Workflow orchestration service (Temporal) is unavailable. Please retry shortly."
		if _, uerr := database.Exec("UPDATE pipelines SET status = 'failed', updated_at = NOW() WHERE id = $1", id); uerr != nil {
			log.Printf("⚠️ [RunPipeline] failed to mark pipeline failed (ignored): %v", uerr)
		}
		if _, uerr := database.Exec("UPDATE executions SET status = 'failed', end_time = NOW(), error_message = $1 WHERE id = $2", errMsg, execID); uerr != nil {
			log.Printf("⚠️ [RunPipeline] failed to mark execution failed (ignored): %v", uerr)
		}
		if _, uerr := database.Exec(`
			UPDATE pipeline_progress
			SET status = 'failed',
			    current_stage = 'intent',
			    message = $1,
			    updated_at = NOW()
			WHERE pipeline_id = $2
		`, errMsg, id); uerr != nil {
			log.Printf("⚠️ [RunPipeline] failed to mark pipeline_progress failed (ignored): %v", uerr)
		}
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   "orchestration_unavailable",
			"message": errMsg,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":      "Pipeline execution started",
		"pipeline_id":  id,
		"execution_id": execID,
		"status":       "running",
	})
}

// RunPipelineInternal is the service-to-service (self-healer) re-run entry point,
// mounted under the InternalServiceMiddleware-guarded /api/v1/internal group and
// authenticated with X-Internal-Secret rather than a user session. It exists
// because the user-facing POST /pipelines/:id/run sits under AuthRequiredMiddleware,
// which fail-closes 401 in prod — so the healer's BackoffRetryExecutor could never
// actually re-run a pipeline there (KI-HEAL-RERUN-UNAUTH).
//
// There is no caller identity on this path, so it resolves the owning workspace
// and the pipeline creator from the pipeline row instead of from the workspace
// context / session. It KEEPS the per-workspace plan gates (plan can_run + monthly
// GB) and the per-pipeline run cooldown — a healer retry is still a billable run and
// must respect the tenant's plan + anti-storm cooldown — and SKIPS the per-user LLM
// cost quota (no user bears the spend here) and the pre-migration assessment gate
// (that gate is a human-facing warning prompt; an automatic retry has no human to
// acknowledge, and the workflow re-assesses internally). CSRF is skipped structurally
// because the internal group does not apply CSRFMiddleware.
func RunPipelineInternal(c *gin.Context) {
	id, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	// No user session on this path: resolve the owning workspace (for the plan
	// gates + workflow scoping) and the creator (used as the workflow's user_id
	// for cost attribution) straight from the pipeline row.
	// created_by + workspace_id are uuid columns; cast to text so COALESCE with a
	// text default is valid (COALESCE(uuid, '') errors: invalid input syntax for
	// type uuid) and the values scan cleanly into strings.
	var wsID, createdBy string
	if err := database.QueryRow(
		`SELECT workspace_id::text, COALESCE(created_by::text, '') FROM pipelines WHERE id = $1`, id,
	).Scan(&wsID, &createdBy); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
		return
	}

	// Per-WORKSPACE plan run gate (trial expired / can_run=false) — identical to
	// RunPipeline, keyed by the pipeline's workspace.
	if q := resolvePlanQuota(c.Request.Context(), database, wsID); !q.canRun {
		c.JSON(http.StatusPaymentRequired, gin.H{
			"error":   "trial_expired",
			"message": "Your free trial has ended. Upgrade to Pro to run pipelines.",
			"plan":    q.plan,
		})
		return
	}

	// Per-WORKSPACE monthly GB gate — identical to RunPipeline.
	if allowed, errBody := checkWorkspaceGBOK(c.Request.Context(), database, wsID); !allowed {
		c.JSON(http.StatusPaymentRequired, errBody)
		return
	}

	// Per-pipeline cooldown. The healer already caps retries at 3/24h, but the
	// cooldown still blunts a tight re-enqueue racing a run that just started.
	cooldown := 10
	if v := strings.TrimSpace(os.Getenv("PIPELINE_RUN_COOLDOWN_SECONDS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cooldown = n
		}
	}
	if !checkPipelineRunCooldown(c.Request.Context(), database, id, cooldown) {
		c.JSON(http.StatusTooManyRequests, gin.H{
			"error":   "pipeline_cooldown",
			"message": "Another run for this pipeline started in the last " + strconv.Itoa(cooldown) + " seconds. Wait before retrying.",
		})
		return
	}

	// The healer posts {triggered_by, reason}; neither maps onto RunPipelineRequest,
	// so the enqueue runs with defaults (no destination_config / nominated_keys
	// overrides). Attribute the workflow to the pipeline creator.
	//
	// run_mode is PINNED to "resume" and must stay pinned. Leaving it zero-valued
	// let enqueuePipelineRun fall through to the pipeline's default_run_mode
	// (see runModeVal resolution in enqueuePipelineRun), so a pipeline whose owner
	// had chosen "Reload" got an UNATTENDED full destination wipe on every healer
	// retry: the executor's reload path calls cdc.DeleteCheckpoints and then, per
	// table, delete_prefix on object storage or DROP TABLE ... CASCADE on relational
	// destinations. That fired 5s after any transient network blip, with no human in
	// the loop and no assessment gate, up to the healer's 3-per-24h cap.
	//
	// Reload is a deliberate, destructive, user-initiated operation. This route is
	// neither — it is the unattended healer path. A retry here can only ever mean
	// "continue from where the failed run stopped". The user-facing
	// /api/v1/pipelines/:id/run is untouched and still honours an explicit override.
	runReq := RunPipelineRequest{RunMode: "resume"}
	enqueuePipelineRun(c, database, id, wsID, createdBy, runReq)
}

// StopPipeline stops a running pipeline
func StopPipeline(c *gin.Context) {
	id, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	wsID, ok := requirePipelineWorkspaceRole(c, id, security.WSMember)
	if !ok {
		return
	}
	database := db.GetDB()

	// Get current status (scoped to the active workspace — defense in depth)
	var status string
	err := database.QueryRow("SELECT status FROM pipelines WHERE id = $1 AND workspace_id = $2", id, wsID).Scan(&status)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
		return
	}

	if status != "running" && status != "pending" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":          "Pipeline is not running",
			"current_status": status,
		})
		return
	}

	// Send cancel signal to Temporal workflow (best-effort).
	// We try execution_id first, then legacy chat-v2 workflow ID pattern.
	execID := ""
	if temporalClient != nil {
		var resolveErr error
		execID, resolveErr = resolveExecutionIDForPipeline(database, id)
		if resolveErr == nil && execID != "" {
			payload := map[string]interface{}{
				"pipeline_id":  id,
				"execution_id": execID,
				"timestamp":    time.Now().UTC().Format(time.RFC3339),
				"action":       "stop",
			}
			signalErr := signalPipelineWorkflowWithFallback(c.Request.Context(), id, execID, "cancel", payload)
			if signalErr != nil {
				log.Printf("⚠️  Failed to signal cancel for pipeline %s (exec %s): %v", id, execID, signalErr)
			}
		}
	}

	// Update status to stopped (workflow will also converge to stopped via StateCancelled)
	_, err = database.Exec("UPDATE pipelines SET status = 'stopped', updated_at = NOW() WHERE id = $1 AND workspace_id = $2", id, wsID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to stop pipeline"})
		return
	}

	// CRITICAL: Also mark the active execution as cancelled so Execution History doesn't get stuck on "Running".
	// This is best-effort and safe even if Temporal cancel races with a natural completion.
	if execID == "" {
		if v, rerr := resolveExecutionIDForPipeline(database, id); rerr == nil {
			execID = v
		}
	}
	if execID != "" {
		if _, err := database.Exec(`
			UPDATE executions
			SET status = 'cancelled',
			    end_time = NOW(),
			    error_message = COALESCE(error_message, 'Cancelled by user')
			WHERE id = $1 AND status IN ('running','pending')
		`, execID); err != nil {
			log.Printf("⚠️ [StopPipeline] failed to mark execution cancelled (ignored): %v", err)
		}
		// Best-effort: if pipeline_progress still points at this execution, mark it cancelled too.
		if _, err := database.Exec(`
			UPDATE pipeline_progress
			SET status = 'cancelled',
			    message = 'Cancelled by user',
			    updated_at = NOW()
			WHERE pipeline_id = $1 AND execution_id = $2
		`, id, execID); err != nil {
			log.Printf("⚠️ [StopPipeline] failed to mark pipeline_progress cancelled (ignored): %v", err)
		}
	}

	log.Printf("✓ Pipeline %s stopped", id)

	c.JSON(http.StatusOK, gin.H{
		"message":     "Pipeline stopped",
		"pipeline_id": id,
		"status":      "stopped",
	})
}

type ControlPlaneRequest struct {
	ExecutionID string `json:"execution_id,omitempty"`
}

// PausePipeline pauses a running pipeline (best-effort between activities).
func PausePipeline(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	wsID, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember)
	if !ok {
		return
	}
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}
	if temporalClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "temporal_client_not_configured"})
		return
	}

	// Must be running or waiting_for_user to pause (scoped to the active workspace).
	var status string
	if err := database.QueryRow("SELECT status FROM pipelines WHERE id = $1 AND workspace_id = $2", pipelineID, wsID).Scan(&status); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
		return
	}
	if status != "running" && status != "waiting_for_user" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Pipeline is not in a pausable state", "current_status": status})
		return
	}

	var req ControlPlaneRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.WithContext(c.Request.Context()).WithError(err).Debug("ControlPlane: optional body parse failed — execution_id will be resolved from DB")
	}

	execID := strings.TrimSpace(req.ExecutionID)
	if execID == "" {
		if v, err := resolveExecutionIDForPipeline(database, pipelineID); err == nil {
			execID = v
		}
	}
	if execID == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "execution_not_found"})
		return
	}

	payload := map[string]interface{}{
		"pipeline_id":  pipelineID,
		"execution_id": execID,
		"timestamp":    time.Now().UTC().Format(time.RFC3339),
		"action":       "pause",
	}
	if err := signalPipelineWorkflowWithFallback(c.Request.Context(), pipelineID, execID, "pause", payload); err != nil {
		respondError(c, http.StatusInternalServerError, "failed_to_pause", "Failed to pause pipeline", err)
		return
	}

	if _, err := database.Exec("UPDATE pipelines SET status = 'paused', updated_at = NOW() WHERE id = $1 AND workspace_id = $2", pipelineID, wsID); err != nil {
		log.Printf("⚠️ [PausePipeline] failed to update pipeline status (ignored): %v", err)
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "pipeline_id": pipelineID, "execution_id": execID, "status": "paused"})
}

// ResumePipeline resumes a paused pipeline.
func ResumePipeline(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	wsID, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember)
	if !ok {
		return
	}
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}
	if temporalClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "temporal_client_not_configured"})
		return
	}

	var status string
	if err := database.QueryRow("SELECT status FROM pipelines WHERE id = $1 AND workspace_id = $2", pipelineID, wsID).Scan(&status); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
		return
	}
	if status != "paused" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Pipeline is not paused", "current_status": status})
		return
	}

	var req ControlPlaneRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.WithContext(c.Request.Context()).WithError(err).Debug("ControlPlane: optional body parse failed — execution_id will be resolved from DB")
	}

	execID := strings.TrimSpace(req.ExecutionID)
	if execID == "" {
		if v, err := resolveExecutionIDForPipeline(database, pipelineID); err == nil {
			execID = v
		}
	}
	if execID == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "execution_not_found"})
		return
	}

	payload := map[string]interface{}{
		"pipeline_id":  pipelineID,
		"execution_id": execID,
		"timestamp":    time.Now().UTC().Format(time.RFC3339),
		"action":       "resume",
	}
	if err := signalPipelineWorkflowWithFallback(c.Request.Context(), pipelineID, execID, "resume", payload); err != nil {
		respondError(c, http.StatusInternalServerError, "failed_to_resume", "Failed to resume pipeline", err)
		return
	}

	if _, err := database.Exec("UPDATE pipelines SET status = 'running', updated_at = NOW() WHERE id = $1 AND workspace_id = $2", pipelineID, wsID); err != nil {
		log.Printf("⚠️ [ResumePipeline] failed to update pipeline status (ignored): %v", err)
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "pipeline_id": pipelineID, "execution_id": execID, "status": "running"})
}

func signalPipelineWorkflowWithFallback(reqCtx context.Context, pipelineID string, execID string, signal string, payload map[string]interface{}) error {
	ctx, cancel := context.WithTimeout(reqCtx, 10*time.Second)
	defer cancel()

	candidates := []string{}
	if execID != "" {
		candidates = append(candidates, execID)
	}
	if pipelineID != "" {
		candidates = append(candidates, fmt.Sprintf("pipeline-workflow-%s", pipelineID))
	}

	var lastErr error
	for _, wfID := range candidates {
		if wfID == "" {
			continue
		}
		err := temporalClient.SignalWorkflow(ctx, wfID, "", signal, payload)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

// Execution represents a pipeline execution
type Execution struct {
	ID           string     `json:"id"`
	PipelineID   string     `json:"pipeline_id"`
	Status       string     `json:"status"` // pending, running, completed, failed
	StartTime    *time.Time `json:"start_time"`
	EndTime      *time.Time `json:"end_time"`
	ErrorMessage string     `json:"error_message,omitempty"`
	// Metrics carries derived per-run counters for the UI (e.g. the
	// "Records" column in Execution History). BUG-8: this is populated from
	// pipeline_run_table_stats and intentionally NOT gated on a terminal
	// status, so partial-sync / degraded runs that wrote some rows still
	// report a count instead of "—".
	Metrics *ExecutionMetrics `json:"metrics,omitempty"`
	// LiveStream marks the run a live CDC stream belongs to (executionLiveCDCStreamSQL).
	// Its row reports 'running', but it is not a cancellable run: CancelExecution
	// refuses it (the row is closed) and the stream is stopped from the pipeline.
	LiveStream bool `json:"live_stream,omitempty"`
	// Scheduling metadata (optional)
	TriggerSource string     `json:"trigger_source,omitempty"` // manual, scheduled
	ScheduleID    *string    `json:"schedule_id,omitempty"`
	ScheduledTime *time.Time `json:"scheduled_time,omitempty"`
	// Joined pipeline info
	PipelineName string `json:"pipeline_name,omitempty"`
}

// ExecutionMetrics holds derived per-run counters surfaced to the UI.
type ExecutionMetrics struct {
	RecordsProcessed int64 `json:"records_processed"`
}

// ListExecutions returns all executions
func ListExecutions(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	// Executions are workspace-owned: a member sees every run in their ACTIVE
	// workspace, not just ones they personally started. Scope by p.workspace_id.
	if _, ok := resolveUserID(c); !ok {
		return
	}
	scopeWS, ok := resolveActiveWorkspace(c)
	if !ok {
		return
	}

	status := c.Query("status")
	pipelineID := c.Query("pipeline_id")
	// Support the nested REST route GET /pipelines/:id/executions — the pipeline
	// id comes from the path there, not the query string.
	if pipelineID == "" {
		pipelineID = c.Param("id")
	}

	// BUG-2: when a specific pipeline is addressed (nested route :id or the
	// ?pipeline_id query param), 404 if it is not visible in the caller's active
	// workspace — matching GET /pipelines/{id} and /state. Pre-fix this returned
	// 200 with an empty list, conflating "unknown / not yours" with "no runs yet".
	// The check is workspace-scoped so it leaks nothing. Fail-open on a transient
	// DB error (fall through to the scoped list, which returns empty) rather than a
	// false 404.
	if pipelineID != "" {
		var exists bool
		if err := database.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM pipelines WHERE id = $1 AND workspace_id = $2)`,
			pipelineID, scopeWS,
		).Scan(&exists); err == nil && !exists {
			c.JSON(http.StatusNotFound, gin.H{"error": "pipeline_not_found", "message": "Pipeline not found"})
			return
		}
	}

	limitStr := c.Query("limit")
	limit := 100
	if limitStr != "" {
		if v, err := strconv.Atoi(limitStr); err == nil && v > 0 && v <= 500 {
			limit = v
		}
	}

	query := `
		SELECT
		       e.id,
		       e.pipeline_id,
		       ` + executionStatusSQL + ` AS status,
		       ls.live_stream,
		       COALESCE(e.trigger_source, 'manual') as trigger_source,
		       e.schedule_id,
		       e.scheduled_time,
		       e.start_time,
		       ` + executionEndTimeSQL + ` AS end_time,
		       e.error_message,
		       COALESCE(p.name, 'Unknown Pipeline') as pipeline_name,
		       ` + executionRecordsProcessedSQL + ` AS records_processed
	` + executionFromSQL + `
		WHERE p.workspace_id = $1
		  AND e.id <> e.pipeline_id
	`
	args := []interface{}{scopeWS}
	argIdx := 2

	// The filter matches the status the row reports, so ?status=running finds a
	// live CDC stream and ?status=completed no longer does.
	if status != "" {
		query += " AND (" + executionStatusSQL + ") = $" + strconv.Itoa(argIdx)
		args = append(args, status)
		argIdx++
	}

	if pipelineID != "" {
		// Best-effort repair: if a newer execution has already finished for this pipeline,
		// mark any older "running" execution rows as cancelled so the Execution History doesn't
		// keep showing ghost runs forever.
		// The pipelineID here comes straight off the URL path (:id) and is NOT
		// pre-validated against the active workspace. The SELECT below is scoped
		// (p.workspace_id = $1), but this repair WRITE must be too — otherwise a
		// member of one workspace hitting /pipelines/<foreign-pipeline>/executions
		// would silently cancel another workspace's running rows (the SELECT
		// returns nothing, masking the cross-workspace write). Gate the UPDATE on
		// the pipeline belonging to the active workspace ($2), matching the global
		// branch's tenancy guard.
		if _, err := database.Exec(`
			WITH pp AS (
				SELECT execution_id, status
				FROM pipeline_progress
				WHERE pipeline_id = $1
			)
			UPDATE executions e
			SET status = 'cancelled',
			    end_time = COALESCE(e.end_time, NOW()),
			    error_message = COALESCE(e.error_message, 'Cancelled (stale run)')
			WHERE e.pipeline_id = $1
			  AND e.status = 'running'
			  -- Never "repair" the synthetic CDC audit row (id = pipeline_id): it is
			  -- a permanent FK anchor, not a run, so cancelling it would stamp a
			  -- misleading 'Cancelled (stale run)' on a row that never ran.
			  AND e.id <> e.pipeline_id
			  -- Age guard (matches the global branch): only cancel runs old enough
			  -- that they cannot be a just-launched run. Without this, polling this
			  -- endpoint in the brief window after launch — before the new run seeds
			  -- pipeline_progress.execution_id, while pp still points at the previous
			  -- terminal run — would cancel the FRESH run as "stale".
			  AND e.start_time < NOW() - INTERVAL '5 minutes'
			  AND EXISTS (SELECT 1 FROM pp WHERE pp.status IN ('completed','failed','cancelled'))
			  AND (SELECT pp.execution_id FROM pp LIMIT 1) IS NOT NULL
			  AND e.id <> (SELECT pp.execution_id FROM pp LIMIT 1)
			  AND EXISTS (
			      SELECT 1 FROM pipelines p
			      WHERE p.id = $1 AND p.workspace_id = $2
			  )
		`, pipelineID, scopeWS); err != nil {
			log.Printf("⚠️ [ListExecutions] stale execution repair failed (ignored): %v", err)
		}

		query += fmt.Sprintf(" AND e.pipeline_id = $%d", argIdx)
		args = append(args, pipelineID)
		argIdx++
	} else {
		// Global view (GET /executions, no pipeline filter): run the same stale-run
		// repair across the active workspace's pipelines so the Executions list and the "Running"
		// stat card don't show ghost runs forever (ISSUE-005 / ISSUE-019). pipeline_progress
		// has one row per pipeline (pipeline_id PRIMARY KEY), so we correlate each
		// pp row to the execution's own pipeline_id. Same stale-detection condition as
		// the per-pipeline branch: only flip 'running' rows whose pipeline_progress is
		// terminal AND that are NOT the execution pp currently points at. Bounded to
		// runs started over a few minutes ago so we never cancel a just-launched run
		// before its pp row is seeded, and scoped to the active workspace
		// (p.workspace_id = $1) so the write matches the tenancy of the SELECT below.
		if _, err := database.Exec(`
			UPDATE executions e
			SET status = 'cancelled',
			    end_time = COALESCE(e.end_time, NOW()),
			    error_message = COALESCE(e.error_message, 'Cancelled (stale run)')
			FROM pipeline_progress pp
			WHERE pp.pipeline_id = e.pipeline_id
			  AND e.status = 'running'
			  AND e.id <> e.pipeline_id
			  AND e.start_time < NOW() - INTERVAL '5 minutes'
			  AND pp.status IN ('completed','failed','cancelled')
			  AND pp.execution_id IS NOT NULL
			  AND e.id <> pp.execution_id
			  AND EXISTS (
			      SELECT 1 FROM pipelines p
			      WHERE p.id = e.pipeline_id AND p.workspace_id = $1
			  )
		`, scopeWS); err != nil {
			log.Printf("⚠️ [ListExecutions] global stale execution repair failed (ignored): %v", err)
		}
	}

	// COUNT query — same WHERE conditions, no LIMIT, so total reflects reality even when
	// the page is capped at `limit`.
	var totalCount int
	countQuery := `SELECT COUNT(*)` + executionFromSQL + ` WHERE p.workspace_id = $1 AND e.id <> e.pipeline_id`
	countArgs := []interface{}{scopeWS}
	if status != "" {
		countQuery += " AND (" + executionStatusSQL + ") = $2"
		countArgs = append(countArgs, status)
	}
	if pipelineID != "" {
		countQuery += fmt.Sprintf(" AND e.pipeline_id = $%d", len(countArgs)+1)
		countArgs = append(countArgs, pipelineID)
	}
	if err := database.QueryRow(countQuery, countArgs...).Scan(&totalCount); err != nil {
		log.Printf("[ListExecutions] COUNT query error (ignored): %v", err)
		totalCount = 0 // corrected after rows scan below if count fails
	}

	query += fmt.Sprintf(" ORDER BY e.start_time DESC LIMIT %d", limit)

	rows, err := database.Query(query, args...)
	if err != nil {
		log.Printf("[ListExecutions] Query error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch executions"})
		return
	}
	defer rows.Close()

	result := make([]*Execution, 0)
	for rows.Next() {
		e := &Execution{}
		var errorMessage *string
		var scheduleID sql.NullString
		var scheduledTime sql.NullTime
		var recordsProcessed int64
		err := rows.Scan(
			&e.ID, &e.PipelineID, &e.Status, &e.LiveStream,
			&e.TriggerSource, &scheduleID, &scheduledTime,
			&e.StartTime, &e.EndTime, &errorMessage,
			&e.PipelineName, &recordsProcessed,
		)
		if err != nil {
			log.Printf("[ListExecutions] Scan error: %v", err)
			continue
		}
		// Only attach metrics when we actually have a positive count; a zero
		// stays "—" in the UI (no stats projected yet) rather than a noisy "0".
		if recordsProcessed > 0 {
			e.Metrics = &ExecutionMetrics{RecordsProcessed: recordsProcessed}
		}
		if errorMessage != nil {
			e.ErrorMessage = *errorMessage
		}
		if scheduleID.Valid {
			v := scheduleID.String
			e.ScheduleID = &v
		}
		if scheduledTime.Valid {
			t := scheduledTime.Time
			e.ScheduledTime = &t
		}
		result = append(result, e)
	}

	// Calculate stats
	running := 0
	success := 0
	failed := 0
	for _, e := range result {
		switch e.Status {
		case "running":
			running++
		case "completed", "success":
			success++
		case "failed":
			failed++
		}
	}

	log.Printf("[ListExecutions] Found %d executions (total=%d)", len(result), totalCount)
	// If COUNT query fell back to 0 but we have rows, use len(result) as a safe minimum.
	if totalCount == 0 && len(result) > 0 {
		totalCount = len(result)
	}

	c.JSON(http.StatusOK, gin.H{
		"executions": result,
		"total":      totalCount,
		"stats": gin.H{
			"running": running,
			"success": success,
			"failed":  failed,
		},
	})
}

// GetExecution returns a single execution by ID
func GetExecution(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	if _, ok := resolveUserID(c); !ok {
		return
	}
	scopeWS, ok := resolveActiveWorkspace(c)
	if !ok {
		return
	}
	// On the nested route GET /pipelines/:id/executions/:execId the execution id
	// is in :execId (:id is the pipeline). The flat route /executions/:id uses :id.
	execParam := "id"
	if c.Param("execId") != "" {
		execParam = "execId"
	}
	id, ok := requireUUIDParam(c, execParam, "invalid_execution_id", "Invalid execution ID format")
	if !ok {
		return
	}

	e := &Execution{}
	var errorMessage *string
	var scheduleID sql.NullString
	var scheduledTime sql.NullTime
	var recordsProcessed int64
	err := database.QueryRow(`
		SELECT
		       e.id,
		       e.pipeline_id,
		       `+executionStatusSQL+` AS status,
		       ls.live_stream,
		       COALESCE(e.trigger_source, 'manual') as trigger_source,
		       e.schedule_id,
		       e.scheduled_time,
		       e.start_time,
		       `+executionEndTimeSQL+` AS end_time,
		       e.error_message,
		       COALESCE(p.name, 'Unknown Pipeline') as pipeline_name,
		       -- Same derivation as ListExecutions. executions.metrics is a vestigial
		       -- jsonb column with no writer; the real count lives in
		       -- pipeline_run_table_stats and is summed at read time. Without this
		       -- the detail page showed no record count while the list row it was
		       -- opened from showed one — the same execution, two answers.
		       `+executionRecordsProcessedSQL+` AS records_processed
	`+executionFromSQL+`
		WHERE e.id = $1 AND p.workspace_id = $2
	`, id, scopeWS).Scan(
		&e.ID, &e.PipelineID, &e.Status, &e.LiveStream,
		&e.TriggerSource, &scheduleID, &scheduledTime,
		&e.StartTime, &e.EndTime, &errorMessage, &e.PipelineName,
		&recordsProcessed,
	)

	if err != nil {
		log.Printf("[GetExecution] Query error: %v", err)
		c.JSON(http.StatusNotFound, gin.H{"error": "Execution not found"})
		return
	}

	// Only attach metrics when we actually have a positive count; a zero stays
	// "—" in the UI (no stats projected yet) rather than a noisy "0". Mirrors
	// ListExecutions so the detail page and the list row agree.
	if recordsProcessed > 0 {
		e.Metrics = &ExecutionMetrics{RecordsProcessed: recordsProcessed}
	}
	if errorMessage != nil {
		e.ErrorMessage = *errorMessage
	}
	if scheduleID.Valid {
		v := scheduleID.String
		e.ScheduleID = &v
	}
	if scheduledTime.Valid {
		t := scheduledTime.Time
		e.ScheduledTime = &t
	}

	c.JSON(http.StatusOK, e)
}

type TransformExecutionLog struct {
	ID             string                 `json:"id"`
	PipelineID     string                 `json:"pipeline_id"`
	ExecutionID    string                 `json:"execution_id"`
	TableName      string                 `json:"table_name"`
	TransformID    *string                `json:"transform_id,omitempty"`
	TransformOrder int                    `json:"transform_order"`
	TransformType  string                 `json:"transform_type"`
	Status         string                 `json:"status"`
	ErrorMessage   *string                `json:"error_message,omitempty"`
	InputRows      int64                  `json:"input_rows"`
	OutputRows     int64                  `json:"output_rows"`
	DurationMs     int64                  `json:"duration_ms"`
	ConfigSnapshot map[string]interface{} `json:"config_snapshot"`
	CreatedAt      time.Time              `json:"created_at"`
	UpdatedAt      time.Time              `json:"updated_at"`
}

// GetExecutionTransformLogs returns aggregated per-transform stats for an execution.
func GetExecutionTransformLogs(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	if _, ok := resolveUserID(c); !ok {
		return
	}
	// Transform logs are WORKSPACE-owned, like the executions they describe:
	// scope by p.workspace_id (any member of the active workspace, incl. a
	// viewer, may read a teammate's run) — not by p.created_by, which would hide
	// the run from everyone but its original author (collaboration-breaking and
	// inconsistent with GetExecution / ListExecutions).
	scopeWS, ok := resolveActiveWorkspace(c)
	if !ok {
		return
	}

	execID, ok := requireUUIDParam(c, "id", "invalid_execution_id", "Invalid execution ID format")
	if !ok {
		return
	}

	rows, err := database.Query(`
		SELECT
			tel.id::text,
			tel.pipeline_id::text,
			tel.execution_id::text,
			tel.table_name,
			NULLIF(tel.transform_id, '') as transform_id,
			tel.transform_order,
			tel.transform_type,
			tel.status,
			tel.error_message,
			tel.input_rows,
			tel.output_rows,
			tel.duration_ms,
			tel.config_snapshot,
			tel.created_at,
			tel.updated_at
		FROM transform_execution_logs tel
		JOIN executions e ON e.id = tel.execution_id
		JOIN pipelines p ON p.id = tel.pipeline_id
		WHERE tel.execution_id = $1 AND p.workspace_id = $2
		ORDER BY tel.table_name ASC, tel.transform_order ASC
	`, execID, scopeWS)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch transform execution logs"})
		return
	}
	defer rows.Close()

	out := make([]TransformExecutionLog, 0)
	for rows.Next() {
		var l TransformExecutionLog
		var transformID sql.NullString
		var errMsg sql.NullString
		var snapJSON []byte

		if err := rows.Scan(
			&l.ID,
			&l.PipelineID,
			&l.ExecutionID,
			&l.TableName,
			&transformID,
			&l.TransformOrder,
			&l.TransformType,
			&l.Status,
			&errMsg,
			&l.InputRows,
			&l.OutputRows,
			&l.DurationMs,
			&snapJSON,
			&l.CreatedAt,
			&l.UpdatedAt,
		); err != nil {
			continue
		}

		if transformID.Valid {
			v := transformID.String
			l.TransformID = &v
		}
		if errMsg.Valid {
			v := errMsg.String
			l.ErrorMessage = &v
		}

		l.ConfigSnapshot = map[string]interface{}{}
		if len(snapJSON) > 0 {
			_ = json.Unmarshal(snapJSON, &l.ConfigSnapshot)
		}

		out = append(out, l)
	}

	c.JSON(http.StatusOK, gin.H{
		"execution_id": execID,
		"logs":         out,
	})
}

// CancelExecution cancels a running execution
func CancelExecution(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	if _, ok := resolveUserID(c); !ok {
		return
	}
	id, ok := requireUUIDParam(c, "id", "invalid_execution_id", "Invalid execution ID format")
	if !ok {
		return
	}
	scopeWS, ok := resolveActiveWorkspace(c)
	if !ok {
		return
	}

	// Resolve the run within the ACTIVE workspace (IDOR-safe: a run living in
	// another workspace is invisible here → 404, never leaking its existence).
	var status string
	var pipelineID string
	err := database.QueryRow(`
		SELECT e.status, e.pipeline_id
		FROM executions e
		JOIN pipelines p ON p.id = e.pipeline_id
		WHERE e.id = $1 AND p.workspace_id = $2
	`, id, scopeWS).Scan(&status, &pipelineID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Execution not found"})
		return
	}

	// Cancelling a teammate's run is a MUTATION → require >= member in the active
	// workspace. A viewer may watch a run but not cancel it.
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}

	if status != "running" && status != "pending" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":          "Execution is not running",
			"current_status": status,
		})
		return
	}

	// Cancel Temporal workflow (best-effort). WorkflowID == execution ID in this system.
	temporalCancelled := false
	if temporalClient != nil {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
		defer cancel()
		if err := temporalClient.CancelWorkflow(ctx, id, ""); err != nil {
			log.Printf("[CancelExecution] Temporal cancel failed for %s: %v", id, err)
		} else {
			temporalCancelled = true
		}
	}

	// Update status to cancelled (DB)
	_, err = database.Exec("UPDATE executions SET status = 'cancelled', end_time = NOW(), error_message = 'Cancelled by user' WHERE id = $1", id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to cancel execution"})
		return
	}

	// Update pipeline_progress (best-effort) so UIs stop showing "Running" for the cancelled execution.
	if _, err := database.Exec(`
		UPDATE pipeline_progress
		SET status = 'cancelled', message = 'Cancelled by user', updated_at = NOW()
		WHERE pipeline_id = $1 AND execution_id = $2
	`, pipelineID, id); err != nil {
		log.Printf("⚠️ [CancelExecution] failed to mark pipeline_progress cancelled (ignored): %v", err)
	}

	// Update pipeline status to 'stopped' (best-effort)
	if _, err := database.Exec("UPDATE pipelines SET status = 'stopped', updated_at = NOW() WHERE id = $1", pipelineID); err != nil {
		log.Printf("⚠️ [CancelExecution] failed to mark pipeline stopped (ignored): %v", err)
	}

	log.Printf("✓ Execution %s cancelled (pipeline %s stopped)", id, pipelineID)

	c.JSON(http.StatusOK, gin.H{
		"message":            "Execution cancelled",
		"execution_id":       id,
		"status":             "cancelled",
		"temporal_cancelled": temporalCancelled,
	})
}
