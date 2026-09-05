package executor

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rsync-ai/shared/kafkaclient"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"math/rand"
	"time"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/common"
	"github.com/rsync-ai/backend-orchestrator/internal/cdc"
	"github.com/rsync-ai/backend-orchestrator/internal/connections"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
	appmetrics "github.com/rsync-ai/backend-orchestrator/internal/metrics"
	"github.com/rsync-ai/backend-orchestrator/internal/storage"
	"github.com/rsync-ai/backend-orchestrator/internal/telemetry"
	"github.com/rsync-ai/backend-orchestrator/internal/utils"
	"github.com/rsync-ai/backend-orchestrator/pkg/llmscrub"
	"github.com/rsync-ai/shared/naming"
	"github.com/rsync-ai/shared/transforms"
)

var executorTracer = otel.Tracer("executor-agent")

const (
	MaxRetries     = 3
	InitialBackoff = 500 * time.Millisecond
	MaxBackoff     = 5 * time.Second
)

// extractSnapshotVersion reads a plan-time connector-version snapshot out of
// the task payload. The api-gateway records concrete versions in
// pipelines.{source,destination}_connector_snapshot at RunPipeline time and
// forwards the same struct into Temporal's workflowInput; this helper
// surfaces that pinned version so downstream call sites don't fall back to
// the connection record's "connector_version" (which can still be "latest"
// and would silently drift on a connector upgrade).
//
// Returns the empty string if the snapshot is missing or malformed; callers
// are expected to fall through to their secondary resolution path.
func extractSnapshotVersion(params map[string]interface{}, key string) string {
	if params == nil || key == "" {
		return ""
	}
	raw, ok := params[key]
	if !ok || raw == nil {
		return ""
	}
	m, ok := raw.(map[string]interface{})
	if !ok || m == nil {
		return ""
	}
	v, _ := m["version"].(string)
	return strings.TrimSpace(v)
}

func looksLikeUUIDString(s string) bool {
	s = strings.TrimSpace(s)
	if len(s) != 36 {
		return false
	}
	// UUID v4-ish shape check (8-4-4-4-12).
	for _, pos := range []int{8, 13, 18, 23} {
		if s[pos] != '-' {
			return false
		}
	}
	return true
}

// loadPipelineModes best-effort loads sync_mode/cdc_mode using either full UUID pipeline id
// or a short ID prefix (the UI sometimes uses the first 8 chars).
func loadPipelineModes(ctx context.Context, db *sql.DB, pipelineID string) (syncMode string, cdcMode string) {
	if db == nil {
		return "", ""
	}
	pid := strings.TrimSpace(pipelineID)
	if pid == "" {
		return "", ""
	}

	read := func(whereSQL string, arg interface{}) (string, string, bool) {
		var sm, cm sql.NullString
		if err := db.QueryRowContext(ctx, whereSQL, arg).Scan(&sm, &cm); err != nil {
			return "", "", false
		}
		outSM := ""
		outCM := ""
		if sm.Valid {
			outSM = strings.ToLower(strings.TrimSpace(sm.String))
		}
		if cm.Valid {
			outCM = strings.ToLower(strings.TrimSpace(cm.String))
		}
		return outSM, outCM, true
	}

	// Preferred: exact UUID match.
	if looksLikeUUIDString(pid) {
		if sm, cm, ok := read(`SELECT sync_mode, cdc_mode FROM pipelines WHERE id = $1`, pid); ok {
			return sm, cm
		}
	}

	// Fallback: match by prefix (first 8 chars is what UI displays).
	short := pid
	if len(short) > 8 {
		short = short[:8]
	}
	if len(short) >= 4 {
		if sm, cm, ok := read(`SELECT sync_mode, cdc_mode FROM pipelines WHERE id::text LIKE $1 || '%' ORDER BY created_at DESC LIMIT 1`, short); ok {
			return sm, cm
		}
	}

	return "", ""
}

// loadPipelineCDCInitialLoad best-effort loads the cdc_initial_load column for a
// pipeline (UUID or short-prefix id). Returns "debezium" | "batch" | "" (column
// absent / NULL → empty, meaning use the default Debezium snapshot). The hybrid
// batch-historical + position-anchored-CDC path engages only when this is "batch".
func loadPipelineCDCInitialLoad(ctx context.Context, db *sql.DB, pipelineID string) string {
	if db == nil {
		return ""
	}
	pid := strings.TrimSpace(pipelineID)
	if pid == "" {
		return ""
	}
	read := func(whereSQL string, arg interface{}) (string, bool) {
		var v sql.NullString
		if err := db.QueryRowContext(ctx, whereSQL, arg).Scan(&v); err != nil {
			return "", false
		}
		if v.Valid {
			return strings.ToLower(strings.TrimSpace(v.String)), true
		}
		return "", true
	}
	if looksLikeUUIDString(pid) {
		if v, ok := read(`SELECT cdc_initial_load FROM pipelines WHERE id = $1`, pid); ok {
			return v
		}
	}
	short := pid
	if len(short) > 8 {
		short = short[:8]
	}
	if len(short) >= 4 {
		if v, ok := read(`SELECT cdc_initial_load FROM pipelines WHERE id::text LIKE $1 || '%' ORDER BY created_at DESC LIMIT 1`, short); ok {
			return v
		}
	}
	return ""
}

// loadNominatedKeys reads user-nominated key columns for keyless / GIPK tables
// from pipelines.config.nominated_keys — a JSON object { "<table>": ["col", …] }
// set in the pre-migration assessment HITL (PR-D column nomination). Returns nil
// when none are set. Table keys may be bare ("orders") or schema-qualified
// ("public.orders"); callers match both shapes. These columns become the
// upsert key, replacing the content-hash surrogate for that table.
func loadNominatedKeys(ctx context.Context, db *sql.DB, pipelineID string) map[string][]string {
	if db == nil || strings.TrimSpace(pipelineID) == "" {
		return nil
	}
	var raw sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(config->'nominated_keys', 'null'::jsonb)::text FROM pipelines WHERE id = $1`,
		strings.TrimSpace(pipelineID),
	).Scan(&raw); err != nil {
		return nil
	}
	if !raw.Valid || strings.TrimSpace(raw.String) == "" || raw.String == "null" {
		return nil
	}
	parsed := map[string][]string{}
	if err := json.Unmarshal([]byte(raw.String), &parsed); err != nil {
		return nil
	}
	out := map[string][]string{}
	for tbl, cols := range parsed {
		t := strings.TrimSpace(tbl)
		clean := make([]string, 0, len(cols))
		for _, c := range cols {
			if c = strings.TrimSpace(c); c != "" {
				clean = append(clean, c)
			}
		}
		if t != "" && len(clean) > 0 {
			out[t] = clean
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// cdcInitialLoadChoice returns the EXPLICIT cdc_initial_load selection, normalized to
// "batch", "debezium", or "" when unset. Precedence mirrors sync_mode/cdc_mode resolution:
// task.Params["cdc_initial_load"] → pipelines.cdc_initial_load → "". Unlike
// resolveCDCInitialLoad it does not collapse the value to a bool, so callers can distinguish
// "explicitly debezium" (honor it) from "unset" (eligible for the resumable auto-default).
func cdcInitialLoadChoice(ctx context.Context, db *sql.DB, task ExecutorTask) string {
	v := ""
	if task.Params != nil {
		if s, ok := task.Params["cdc_initial_load"].(string); ok {
			v = strings.ToLower(strings.TrimSpace(s))
		}
	}
	if v == "" {
		v = strings.ToLower(strings.TrimSpace(loadPipelineCDCInitialLoad(ctx, db, task.PipelineID)))
	}
	return v
}

// resolveCDCInitialLoad determines whether a CDC pipeline should use the hybrid
// batch historical load. Returns true only for the explicit "batch" value.
func resolveCDCInitialLoad(ctx context.Context, db *sql.DB, task ExecutorTask) bool {
	return cdcInitialLoadChoice(ctx, db, task) == "batch"
}

// isRealNamespace reports whether s is a usable per-pipeline destination
// namespace. The planner stores the literal "default" as a generic placeholder
// when no namespace was chosen — it is NOT a real database/schema name and MUST
// NOT be propagated to drop_table (where the connector would skip it and fall
// back to the shared connection default, e.g. MySQL "pipeline_test", causing a
// reload to DROP the wrong database while the sink WROTE to the real namespace).
// See the namespace-divergence bug: write landed in shopify_sync but drop hit
// pipeline_test because resolveDestinationNamespace returned the params "default".
func isRealNamespace(s string) bool {
	s = strings.TrimSpace(s)
	return s != "" && !strings.EqualFold(s, "default")
}

// resolveDestinationNamespace returns the pipeline's per-pipeline destination
// namespace (Fivetran/Airbyte-style isolation; see .design/destination-namespace.md).
//
// Resolution order (first REAL namespace wins; the planner's generic "default"
// placeholder is skipped at every tier so we fall through to the authoritative
// pipelines.config value rather than emitting "default" downstream):
//  1. task.Params["destination_namespace"]   (set by api-gateway / temporal-adapter
//     when launching the workflow from pipelines.config.destination_namespace)
//  2. task.Payload["destination_namespace"]  (legacy payload channel)
//  3. pipelines.config->>'destination_namespace'  (authoritative DB value;
//     Fixer D / first-run namespace lock persists it here)
//
// Empty return = legacy pipeline (no per-pipeline namespace persisted). Callers
// MUST fall back to the existing F-26 source-derived-schema behavior in that
// case to avoid breaking pre-round-4 pipelines.
func resolveDestinationNamespace(ctx context.Context, db *sql.DB, task ExecutorTask) string {
	var paramsVal, payloadVal, dbVal string
	if task.Params != nil {
		if v, ok := task.Params["destination_namespace"].(string); ok {
			paramsVal = strings.TrimSpace(v)
		}
	}
	if task.Payload != nil {
		if v, ok := task.Payload["destination_namespace"].(string); ok {
			payloadVal = strings.TrimSpace(v)
		}
	}
	// Authoritative DB value: pipelines.config.destination_namespace.
	if db != nil && strings.TrimSpace(task.PipelineID) != "" {
		var ns sql.NullString
		_ = db.QueryRowContext(ctx,
			`SELECT NULLIF(TRIM(COALESCE(config->>'destination_namespace','')), '') FROM pipelines WHERE id = $1`,
			task.PipelineID,
		).Scan(&ns)
		if ns.Valid {
			dbVal = strings.TrimSpace(ns.String)
		}
	}

	// First REAL namespace wins. "default"/empty are skipped so a planner
	// placeholder never shadows the authoritative DB value.
	resolved, source := "", "none"
	switch {
	case isRealNamespace(paramsVal):
		resolved, source = paramsVal, "task.Params"
	case isRealNamespace(payloadVal):
		resolved, source = payloadVal, "task.Payload"
	case isRealNamespace(dbVal):
		resolved, source = dbVal, "pipelines.config"
	}

	// Observability: every namespace decision is traceable in SigNoz. This is
	// the single point that determines where reload DROP + ownership land.
	log.WithFields(log.Fields{
		"pipeline_id": task.PipelineID,
		"params_val":  paramsVal,
		"payload_val": payloadVal,
		"db_val":      dbVal,
		"resolved":    resolved,
		"source":      source,
	}).Info("resolveDestinationNamespace: destination namespace resolved")

	return resolved
}

// distinctSourceSchemas returns the set of non-empty source schemas among the
// selected (schema-qualified) tables, e.g. {"sales","hr"} for
// ["sales.orders","hr.employees","sales.customers"]. Bare tables (no dot, e.g.
// SaaS sources) contribute no schema.
func distinctSourceSchemas(tables []interface{}) map[string]struct{} {
	set := map[string]struct{}{}
	for _, ti := range tables {
		name := strings.TrimSpace(fmt.Sprintf("%v", ti))
		if name == "" {
			continue
		}
		if schema, _ := storage.ExtractSchemaAndTable(name); strings.TrimSpace(schema) != "" {
			set[strings.TrimSpace(schema)] = struct{}{}
		}
	}
	return set
}

// preserveSourceSchemaLayout decides whether a run MIRRORS the source schema
// layout at the destination — each source schema becomes a same-named
// destination schema/namespace — instead of FLATTENING every source schema into
// one destination namespace.
//
// Flattening silently loses data when two source schemas hold a same-named table:
// e.g. sales.orders + procurement.orders both collapse to the bare "orders" and
// upsert over each other (union columns, PK clobber, ~half the rows vanish, run
// still reports success). Preserving mirrors the source and eliminates the
// collision; it is implemented by setting each table's effective destination
// namespace to its source schema (the connectors CREATE SCHEMA/DATABASE IF NOT
// EXISTS for a namespace, so nothing else is needed).
//
// Policy (first match wins):
//   - A real, user-chosen destination namespace  -> flatten (honor the single target).
//   - pipelines.config.destination_schema_mode = "preserve"/"mirror" -> preserve.
//   - pipelines.config.destination_schema_mode = "flatten"           -> flatten.
//   - Auto: preserve when the selection spans more than one source schema.
//     Single-schema selections keep the historical flatten behavior for backward
//     compatibility.
func (a *Agent) preserveSourceSchemaLayout(ctx context.Context, task ExecutorTask, tables []interface{}, destinationNamespace string) bool {
	// 1. Explicit per-pipeline override wins over everything.
	if a.db != nil && strings.TrimSpace(task.PipelineID) != "" {
		var mode sql.NullString
		_ = a.db.QueryRowContext(ctx,
			`SELECT NULLIF(TRIM(LOWER(COALESCE(config->>'destination_schema_mode',''))),'') FROM pipelines WHERE id = $1`,
			task.PipelineID,
		).Scan(&mode)
		if mode.Valid {
			switch mode.String {
			case "preserve", "mirror":
				return true
			case "flatten":
				return false
			}
		}
	}
	// 2. A DELIBERATE, non-default destination namespace means "put everything
	//    here" -> flatten. The engine-default seed (public/dbo/empty/"default")
	//    is applied automatically to every pipeline, so it is NOT a deliberate
	//    choice and must not silently defeat multi-schema mirroring.
	if isRealNamespace(destinationNamespace) && !isEngineDefaultNamespace(destinationNamespace) {
		return false
	}
	// 3. Auto: mirror when the source genuinely spans multiple schemas.
	return len(distinctSourceSchemas(tables)) > 1
}

// isEngineDefaultNamespace reports whether ns is a database engine's DEFAULT
// schema — the value seedDestinationNamespace stamps in when the user did NOT
// choose a destination namespace — as opposed to a deliberate single-target
// namespace. Seeding "public" onto every Postgres pipeline must not silently
// block auto-preserve for a multi-schema source.
func isEngineDefaultNamespace(ns string) bool {
	switch strings.TrimSpace(strings.ToLower(ns)) {
	case "", "public", "dbo", "default":
		return true
	}
	return false
}

// MCPManager exposes the executor's MCP server manager. Used by the dependency
// liveness probe so it can introspect (and HTTP-probe) the same in-memory
// registry that started the servers.
func (a *Agent) MCPManager() *mcp.ServerManager {
	if a == nil {
		return nil
	}
	return a.mcpManager
}

// Agent represents the Executor agent
type Agent struct {
	kafkaManager         *kafka.Manager
	db                   *sql.DB
	mcpClient            *mcp.Client
	mcpManager           *mcp.ServerManager
	connectionMgr        *connections.Manager
	heartbeatPublisher   *common.HeartbeatPublisher
	ctx                  context.Context
	cancel               context.CancelFunc
	streamingPipelines   map[string]*StreamingPipelineInfo // Track long-running pipelines
	streamingPipelinesMu sync.RWMutex                      // Protects streamingPipelines map
}

// NewAgent creates a new Executor agent
func NewAgent(kafkaManager *kafka.Manager, db *sql.DB, toolsDir string) *Agent {
	ctx, cancel := context.WithCancel(context.Background())

	mcpManager := mcp.NewServerManager(toolsDir)
	mcpClient := mcp.NewClient(mcpManager)
	connectionMgr := connections.NewManager(db)

	agent := &Agent{
		kafkaManager:       kafkaManager,
		db:                 db,
		mcpClient:          mcpClient,
		mcpManager:         mcpManager,
		connectionMgr:      connectionMgr,
		heartbeatPublisher: common.NewHeartbeatPublisher("executor", kafkaManager),
		ctx:                ctx,
		cancel:             cancel,
		streamingPipelines: make(map[string]*StreamingPipelineInfo),
	}

	// Phase 1 monitoring: run background health checks for streaming pipelines.
	go agent.StartSinkHealthChecker()

	return agent
}

// StartSinkHealthChecker runs background health checks.
// Phase 1 scope: validate CDC/streaming connectors stay healthy.
func (a *Agent) StartSinkHealthChecker() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			// Streaming/CDC connector health checks (Debezium etc.)
			a.HealthCheckStreamingPipelines()
		}
	}
}

// ExecutorTask represents a task for the executor
type ExecutorTask struct {
	TaskID      string                 `json:"task_id"`
	PipelineID  string                 `json:"pipeline_id"`
	Operation   string                 `json:"operation"` // "export", "import", "query"
	Source      *ConnectorConfig       `json:"source,omitempty"`
	Destination *ConnectorConfig       `json:"destination,omitempty"`
	Params      map[string]interface{} `json:"params,omitempty"`
	Payload     map[string]interface{} `json:"payload,omitempty"` // From Orchestrator
}

// PlanStep represents a step in a generic plan
type PlanStep struct {
	ID           string                 `json:"id"`
	Tool         string                 `json:"tool"`
	Method       string                 `json:"method"`
	Params       map[string]interface{} `json:"params"`
	ConnectionID string                 `json:"connection_id"` // Connection ID for credential lookup
}

// ConnectorConfig holds connector configuration
type ConnectorConfig struct {
	Type    string            `json:"type"`              // "mysql", "s3", etc.
	Version string            `json:"version,omitempty"` // "v1.1.0" or "latest" (optional, defaults to "latest")
	Config  map[string]string `json:"config"`            // Connection config
}

// ExecutorResponse represents the executor's response
type ExecutorResponse struct {
	TaskID        string                 `json:"task_id"`
	PipelineID    string                 `json:"pipeline_id"`
	Status        string                 `json:"status"` // "success", "failed", "running", "stopped"
	Result        map[string]interface{} `json:"result,omitempty"`
	Error         string                 `json:"error,omitempty"`
	RowsProcessed int                    `json:"rows_processed,omitempty"`
	// Streaming-specific fields
	PipelineType  string `json:"pipeline_type,omitempty"`  // "batch" or "streaming"
	ConnectorName string `json:"connector_name,omitempty"` // For CDC connectors
	KafkaTopic    string `json:"kafka_topic,omitempty"`    // Kafka topic for CDC events
}

// StreamingPipelineInfo tracks long-running streaming pipelines
type StreamingPipelineInfo struct {
	PipelineID      string    `json:"pipeline_id"`
	ConnectorName   string    `json:"connector_name"`
	ConnectorType   string    `json:"connector_type"` // "debezium"
	Status          string    `json:"status"`         // "running", "paused", "failed", "stopped"
	KafkaTopic      string    `json:"kafka_topic"`
	StartedAt       time.Time `json:"started_at"`
	LastHealthCheck time.Time `json:"last_health_check"`
	HealthStatus    string    `json:"health_status"` // "healthy", "unhealthy", "unknown"
}

func extractFirstTableFromPlan(planAny interface{}) string {
	// planAny shape (common):
	// { "plan": { "steps": [ { "method": "start_export", "params": { "table": "..." } }, ... ] } }
	if planAny == nil {
		return ""
	}
	outer, ok := planAny.(map[string]interface{})
	if !ok || outer == nil {
		return ""
	}
	// Unwrap optional "plan" wrapper
	planObj := outer
	if inner, ok := outer["plan"].(map[string]interface{}); ok && inner != nil {
		planObj = inner
	}
	stepsAny, ok := planObj["steps"].([]interface{})
	if !ok || stepsAny == nil {
		return ""
	}
	for _, s := range stepsAny {
		step, ok := s.(map[string]interface{})
		if !ok || step == nil {
			continue
		}
		params, ok := step["params"].(map[string]interface{})
		if !ok || params == nil {
			continue
		}
		if tbl, ok := params["table"].(string); ok && strings.TrimSpace(tbl) != "" {
			return strings.TrimSpace(tbl)
		}
	}
	return ""
}

func inferTablesFromUserRequest(userReq string, sourceConnector string, destConnector string) (sourceTable string, destTable string) {
	s := strings.TrimSpace(userReq)
	if s == "" {
		return "", ""
	}

	// isConnectorName reports whether a captured token is really one of the
	// pipeline's connector types rather than a table — e.g. "...into postgres"
	// must not yield destTable="postgres". Uses normConnectorType so the
	// postgres/postgresql alias (prompt says "postgres", connector type is
	// "postgresql") is folded; bare connectorKey missed it, which let the
	// destination connector type leak in as the destination table name even
	// after #314 (that PR scrubbed the selected_tables path only, not this one).
	isConnectorName := func(cand string) bool {
		k := normConnectorType(cand)
		return k != "" && (k == normConnectorType(sourceConnector) || k == normConnectorType(destConnector))
	}

	// Strongest signal for modern prompts:
	// "from <...> table <srcTable> to/into <...> table <destTable>"
	// Example:
	// "Real-time CDC sync from MySQL connection e2e-mysql2 table e2e_db.cdc_test
	//  to PostgreSQL connection e2e-pg2-dest table cdc_test_dest_mysql ..."
	reFromTable := regexp.MustCompile(`(?is)\bfrom\b.*?\btable\s+([a-zA-Z0-9_.]+)\b`)
	if m := reFromTable.FindStringSubmatch(s); len(m) == 2 {
		sourceTable = strings.TrimSpace(m[1])
	}
	reToTable := regexp.MustCompile(`(?is)\b(?:to|into)\b.*?\btable\s+([a-zA-Z0-9_.]+)\b`)
	if m := reToTable.FindStringSubmatch(s); len(m) == 2 {
		// Guard like the reInto/reSrc branches: a prompt such as
		// "...into postgres table" must not yield destTable="postgres".
		if cand := strings.TrimSpace(m[1]); !isConnectorName(cand) {
			destTable = cand
		}
	}

	// Prefer explicit "table <name>" phrasing first (least ambiguous).
	reExplicit := regexp.MustCompile(`(?i)\b(?:migrate|sync|copy|transfer)\s+table\s+([a-zA-Z0-9_.]+)\b`)
	if m := reExplicit.FindStringSubmatch(s); len(m) == 2 {
		if sourceTable == "" {
			sourceTable = strings.TrimSpace(m[1])
		}
	}

	// Secondary: "<verb> <name> to/into ..." (only if <name> is NOT the connector name).
	// This avoids interpreting "sync mysql to s3" as table=mysql.
	reSrcFrom := regexp.MustCompile(`(?i)\b(?:migrate|sync|copy|transfer)\s+([a-zA-Z0-9_.]+)\s+from\b`)
	if m := reSrcFrom.FindStringSubmatch(s); len(m) == 2 {
		cand := m[1]
		if !isConnectorName(cand) {
			if sourceTable == "" {
				sourceTable = cand
			}
		}
	}

	reSrc := regexp.MustCompile(`(?i)\b(?:migrate|sync|copy|transfer)\s+([a-zA-Z0-9_.]+)\s+(?:to|into)\b`)
	if m := reSrc.FindStringSubmatch(s); len(m) == 2 {
		cand := m[1]
		if !isConnectorName(cand) {
			if sourceTable == "" {
				sourceTable = cand
			}
		}
	}

	reAs := regexp.MustCompile(`(?i)\bas\s+([a-zA-Z0-9_]+)\b`)
	if m := reAs.FindStringSubmatch(s); len(m) == 2 {
		if destTable == "" {
			destTable = m[1]
		}
	}

	if destTable == "" {
		reInto := regexp.MustCompile(`(?i)\binto\s+([a-zA-Z0-9_]+)\b`)
		if m := reInto.FindStringSubmatch(s); len(m) == 2 {
			cand := m[1]
			// Skip if the captured word is one of the connector names —
			// "sync products into PostgreSQL" should NOT set destTable="PostgreSQL".
			// The downstream sink would try INSERT INTO public.PostgreSQL and silently
			// drop every row (table doesn't exist), then the postflight guard fires.
			// isConnectorName folds the postgres/postgresql alias that bare
			// connectorKey missed (prompt "into postgres", connector "postgresql").
			if !isConnectorName(cand) {
				destTable = cand
			}
		}
	}

	// Never let an English article / filler word leak through as a table name.
	// A prompt like "...into the <conn> destination" makes the `into <word>`
	// pattern capture the article "the"; left unguarded it became destTable="the",
	// was forced onto destCfg["table"], and the sink wrote EVERY pipeline's rows
	// into a junk table named "the" instead of the selected table (silent
	// cross-table misrouting). An article is never a real table name — drop it so
	// callers fall back to the user-selected table.
	if isStopwordTableToken(sourceTable) {
		sourceTable = ""
	}
	if isStopwordTableToken(destTable) {
		destTable = ""
	}

	return sourceTable, destTable
}

// isStopwordTableToken reports whether s is an English article / filler word a
// loose NL pattern may capture but which is never a real table name. It
// delegates to the shared naming guard so the word list stays aligned with the
// destination-mapping namespace validation (PR #130 / PR-C single source).
func isStopwordTableToken(s string) bool {
	return naming.IsSuspiciousIdentifier(s)
}

func splitQualifiedTable(table string) (schema string, name string) {
	t := strings.TrimSpace(table)
	if t == "" {
		return "", ""
	}
	parts := strings.Split(t, ".")
	if len(parts) < 2 {
		return "", t
	}
	// Support db.schema.table etc by treating everything except the last token as "schema-ish"
	return strings.Join(parts[:len(parts)-1], "."), parts[len(parts)-1]
}

// qualifySelectedTablesForSource normalizes the selected/`tables` list on a task
// into fully-qualified `qualifier.table` strings based on the SOURCE connector.
//
//   - MySQL / SQLServer / Oracle: qualifier is the database name (e.g. "e2e_db.big_table").
//   - PostgreSQL: qualifier is the SCHEMA name (e.g. "public.big_table"), NOT the
//     database name. Using the db name for PG produces "e2e_db.big_table" which makes
//     the destination connector emit `SELECT * FROM e2e_db.big_table` → "relation does
//     not exist". The schema defaults to "public" only as the final fallback.
//
// Tables that already contain a "." are left untouched (caller pre-qualified them).
// The normalized list is written back to task.Params["tables"], ["selected_tables"],
// and task.Payload["selected_tables"] so both the batch and CDC paths stay in sync.
//
// This is the single source of truth for table qualification; both the plan-less CDC
// branch and executeDataTransfer call it so batch and CDC behave identically. Requires
// task.Source to be hydrated; a no-op otherwise.
// isSelectionSentinelToken reports whether t is an unresolved whole-source ("*")
// or whole-namespace ("<ns>.*") selection sentinel. These are expanded to explicit
// table names upstream (api-gateway resolveSelectionForPipeline); if one reaches
// the executor it signals an unresolved path and must be dropped, never qualified
// into a bogus "<schema>.*" table.
func isSelectionSentinelToken(t string) bool {
	s := strings.TrimSpace(t)
	return s == "*" || strings.HasSuffix(s, ".*")
}

func qualifySelectedTablesForSource(task *ExecutorTask) {
	if task == nil || task.Params == nil || task.Source == nil {
		return
	}

	// Object-storage sources need special handling for the destination table name.
	// Their `single` file_mapping serves the one logical table even when the
	// requested table name doesn't match, so a wrong/empty selected_tables value
	// lands data under the wrong name *silently* (e.g. the destination
	// connector_type "postgres" leaking in as the table name) instead of erroring.
	// Relational sources fail loud ("relation does not exist") and may legitimately
	// have a table named like a connector type, so the guard + fallback below are
	// deliberately scoped to object storage. Detection mirrors the dest-side check
	// (~:3340).
	srcTypeLower := strings.ToLower(strings.TrimSpace(task.Source.Type))
	srcIsObjectStorage := srcTypeLower == "minio" || strings.Contains(srcTypeLower, "s3") ||
		srcTypeLower == "gcs" || srcTypeLower == "google-cloud-storage" || srcTypeLower == "azure-blob"

	rawTables, ok := task.Params["tables"]
	if !ok || rawTables == nil {
		if v, ok := task.Params["selected_tables"]; ok && v != nil {
			rawTables = v
		} else if !srcIsObjectStorage {
			return
		}
		// Object-storage with no table list falls through: rawTables stays nil and
		// the source_table_name fallback below supplies the name.
	}

	qualifier := strings.TrimSpace(task.Source.Config["database"])
	srcType := strings.ReplaceAll(srcTypeLower, "-", "_")
	if srcType == "postgres" {
		srcType = "postgresql"
	}
	if srcType == "postgresql" {
		schemaName := strings.TrimSpace(task.Source.Config["schema"])
		if schemaName == "" {
			schemaName = "public"
		}
		qualifier = schemaName
	}

	qualify := func(t string) string {
		t = strings.TrimSpace(t)
		if t == "" {
			return ""
		}
		if qualifier != "" && !strings.Contains(t, ".") {
			return qualifier + "." + t
		}
		return t
	}

	// normType folds a connector/table token to a comparable key (connectorKey +
	// the postgres/postgresql alias). isLeakedConnectorType is true when a token,
	// stripped of any namespace, is just the source or destination connector_type —
	// never a real table, always a leaked connector name (this is how the dest type
	// "postgres" became the table name). Shares normConnectorType with the
	// inferTablesFromUserRequest guards so both leak paths fold types identically.
	normType := normConnectorType
	destType := ""
	if task.Destination != nil {
		destType = task.Destination.Type
	}
	isLeakedConnectorType := func(t string) bool {
		bare := strings.TrimSpace(t)
		if i := strings.LastIndex(bare, "."); i >= 0 {
			bare = bare[i+1:]
		}
		k := normType(bare)
		if k == "" {
			return false
		}
		return k == normType(task.Source.Type) || (destType != "" && k == normType(destType))
	}

	norm := make([]string, 0, 8)
	sentinelDropped := false
	appendTok := func(t string) {
		// Safety net: never qualify an unresolved selection sentinel ("*" /
		// "<ns>.*") into a real table — it must have been expanded upstream by
		// the api-gateway resolver. Dropping it (rather than producing a bogus
		// "<schema>.*" table) fails safe; the empty-list handling then applies.
		if isSelectionSentinelToken(t) {
			sentinelDropped = true
			log.Warnf("qualifySelectedTablesForSource: dropping unresolved selection sentinel %q (expected api-gateway to expand it)", t)
			return
		}
		// Guard (object storage only): drop a connector_type that leaked into the
		// table list so it can't become the destination table name.
		if srcIsObjectStorage && isLeakedConnectorType(t) {
			return
		}
		if q := qualify(t); q != "" {
			norm = append(norm, q)
		}
	}
	switch vv := rawTables.(type) {
	case []string:
		for _, t := range vv {
			appendTok(t)
		}
	case []interface{}:
		for _, it := range vv {
			appendTok(fmt.Sprintf("%v", it))
		}
	case nil:
		// Object-storage with no table list — handled by the fallback below.
	default:
		appendTok(fmt.Sprintf("%v", vv))
	}

	// Fallback (object storage only): if no usable table survived — empty list, or
	// every entry was a leaked connector type — name the table from the connector's
	// configured source_table_name (then bucket/container). Without this the
	// destination is named from whatever stray value reached selected_tables;
	// source_table_name is otherwise never consulted for destination naming.
	if len(norm) == 0 && srcIsObjectStorage {
		fb := strings.TrimSpace(task.Source.Config["source_table_name"])
		if fb == "" {
			fb = strings.TrimSpace(task.Source.Config["bucket"])
		}
		if fb == "" {
			fb = strings.TrimSpace(task.Source.Config["container"])
		}
		if q := qualify(fb); q != "" {
			norm = append(norm, q)
		}
	}

	if len(norm) > 0 {
		task.Params["tables"] = norm
		task.Params["selected_tables"] = norm
		if task.Payload != nil {
			task.Payload["selected_tables"] = norm
		}
	} else if sentinelDropped {
		// Every entry was an unresolved sentinel — clear the raw list so a raw
		// "*" cannot leak into the per-table loop; the downstream empty-selection
		// handling (HITL gate / no-op) applies instead.
		empty := []string{}
		task.Params["tables"] = empty
		task.Params["selected_tables"] = empty
		if task.Payload != nil {
			task.Payload["selected_tables"] = empty
		}
	}
}

// =============================================================================
// PHASE 1: Plan Metadata Propagation
// Ensures cdc_provider, topic_provisioning, sync_mode are passed through pipeline
// =============================================================================

// PlanMetadata holds extracted plan-level configuration
type PlanMetadata struct {
	CDCProvider      string `json:"cdc_provider"`
	ConnectorClass   string `json:"connector_class"`
	SyncMode         string `json:"sync_mode"`
	CDCMode          string `json:"cdc_mode"`
	TopicName        string `json:"topic_name"`
	TopicPartitions  int    `json:"topic_partitions"`
	TopicProvisioned bool   `json:"topic_provisioned"`
}

// extractPlanMetadata extracts plan-level metadata from the plan data
func (a *Agent) extractPlanMetadata(planData map[string]interface{}) PlanMetadata {
	meta := PlanMetadata{}

	// Extract CDC provider
	if provider, ok := planData["cdc_provider"].(string); ok {
		meta.CDCProvider = provider
	}

	// Extract sync mode
	if mode, ok := planData["sync_mode"].(string); ok {
		meta.SyncMode = mode
	}

	// Extract CDC mode
	if mode, ok := planData["cdc_mode"].(string); ok {
		meta.CDCMode = mode
	}

	// Extract topic provisioning info
	if topicInfo, ok := planData["topic_provisioning"].(map[string]interface{}); ok {
		if name, ok := topicInfo["topic_name"].(string); ok {
			meta.TopicName = name
		}
		if partitions, ok := topicInfo["partitions"].(float64); ok {
			meta.TopicPartitions = int(partitions)
		}
		if provisioned, ok := topicInfo["provisioned"].(bool); ok {
			meta.TopicProvisioned = provisioned
		}
	}

	// Extract from data_path if present
	if dataPath, ok := planData["data_path"].(map[string]interface{}); ok {
		if meta.CDCMode == "" {
			if mode, ok := dataPath["cdc_mode"].(string); ok {
				meta.CDCMode = mode
			}
		}
	}

	// Extract connector_class from steps if not at plan level
	if meta.ConnectorClass == "" {
		if steps, ok := planData["steps"].([]interface{}); ok {
			for _, step := range steps {
				if stepMap, ok := step.(map[string]interface{}); ok {
					if params, ok := stepMap["params"].(map[string]interface{}); ok {
						if class, ok := params["connector_class"].(string); ok && class != "" {
							meta.ConnectorClass = class
							break
						}
						if provider, ok := params["cdc_provider"].(string); ok && provider != "" && meta.CDCProvider == "" {
							meta.CDCProvider = provider
						}
					}
				}
			}
		}
	}

	return meta
}

// injectPlanMetadataToTask injects plan metadata into task.Params for downstream use
func (a *Agent) injectPlanMetadataToTask(task *ExecutorTask, meta PlanMetadata) {
	if task.Params == nil {
		task.Params = make(map[string]interface{})
	}

	// Inject CDC provider
	if meta.CDCProvider != "" {
		task.Params["cdc_provider"] = meta.CDCProvider
	}

	// Inject connector class
	if meta.ConnectorClass != "" {
		task.Params["connector_class"] = meta.ConnectorClass
	}

	// Inject sync mode
	if meta.SyncMode != "" {
		task.Params["sync_mode"] = meta.SyncMode
	}

	// Inject CDC mode
	if meta.CDCMode != "" {
		task.Params["cdc_mode"] = meta.CDCMode
	}

	// Inject topic provisioning
	if meta.TopicName != "" {
		task.Params["topic_provisioning"] = map[string]interface{}{
			"topic_name":  meta.TopicName,
			"partitions":  meta.TopicPartitions,
			"provisioned": meta.TopicProvisioned,
		}
	}

	log.Debugf("📋 Injected plan metadata into task: cdc_provider=%s, topic=%s",
		meta.CDCProvider, meta.TopicName)
}

// Start starts the executor agent
func (a *Agent) Start() error {
	log.Info("🚀 Starting Executor Agent (Go)")

	// Start heartbeat publisher
	a.heartbeatPublisher.Start(a.ctx)
	log.Info("📡 Executor Agent heartbeat publisher started")

	// Subscribe to executor requests (consistent with other agents)
	err := a.kafkaManager.Consume("agent.executor.requests", a.handleMessage)
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	log.Info("✅ Executor Agent started, listening to agent.executor.requests")
	return nil
}

// handleMessage processes incoming Kafka messages
func (a *Agent) handleMessage(message *sarama.ConsumerMessage) error {
	// Track message for heartbeat
	a.heartbeatPublisher.IncrementMessagesProcessed()

	// Extract headers for tracing
	headers := make(map[string]string)
	for _, h := range message.Headers {
		headers[string(h.Key)] = string(h.Value)
	}

	// Create span from headers
	ctx, span := telemetry.CreateSpanFromKafkaHeaders(context.Background(), headers, "executor.handle_message")
	defer span.End()

	log.Infof("📩 Received executor task (offset: %d) [trace_id=%s]", message.Offset, telemetry.TraceIDFromContext(ctx))

	// Use smart deserialization that handles both Avro and JSON formats
	var task ExecutorTask
	if err := a.deserializeMessage(message.Value, &task); err != nil {
		a.heartbeatPublisher.IncrementErrorCount()
		log.Errorf("Failed to parse task: %v", err)
		telemetry.RecordError(ctx, err)
		return err
	}

	// Add task attributes to span
	telemetry.AddSpanAttributes(ctx,
		attribute.String("task.id", task.TaskID),
		attribute.String("pipeline.id", task.PipelineID),
		attribute.String("operation", task.Operation),
	)

	log.Infof("Task ID: %s, Pipeline: %s, Operation: %s", task.TaskID, task.PipelineID, task.Operation)

	// Execute task with trace context propagation
	// Context is passed through to all inner operations (MCP calls, DB queries, etc.)
	response := a.executeTask(ctx, task)

	// Publish response with trace context
	responseJSON, _ := json.Marshal(response)

	// Inject trace context into headers
	responseHeaders := telemetry.InjectTraceToHeaders(ctx)

	if err := a.kafkaManager.ProduceWithHeaders("agent.executor.responses", []byte(task.TaskID), responseJSON, responseHeaders); err != nil {
		log.Errorf("Failed to publish response: %v", err)
		telemetry.RecordError(ctx, err)
		return err
	}

	if response.Status == "success" {
		log.Infof("✅ Task %s completed successfully", task.TaskID)
	} else {
		// Scrub row values / PII from the connector/DB error before it hits the
		// log stream (ships to SigNoz). Raw driver errors embed offending row
		// data (e.g. "Duplicate entry 'jane@acme.com'", "Failing row contains …").
		log.Errorf("❌ Task %s failed: %s", task.TaskID, llmscrub.Scrub(response.Error))
	}

	return nil
}

// deserializeMessage handles both Avro and JSON message formats
// It uses the shared kafka.SmartDeserialize function which automatically
// detects the format and deserializes accordingly
func (a *Agent) deserializeMessage(data []byte, task *ExecutorTask) error {
	return kafka.SmartDeserialize(data, task)
}

// executeWithOAuthRetry wraps MCP execution with OAuth token refresh on 401
func (a *Agent) executeWithOAuthRetry(ctx context.Context, req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error) {
	maxRetries := 2 // Initial attempt + 1 retry after refresh

	for attempt := 0; attempt < maxRetries; attempt++ {
		resp, err := a.mcpClient.ExecuteWithContext(ctx, req)

		// Success - return immediately
		if err == nil && resp.Success {
			return resp, nil
		}

		// Check if this is a 401 OAuth error (token expired/invalid)
		is401 := false
		if err != nil {
			// Check error message for 401 indicators
			errStr := strings.ToLower(err.Error())
			is401 = strings.Contains(errStr, "401") ||
				strings.Contains(errStr, "unauthorized") ||
				strings.Contains(errStr, "token expired")
		} else if !resp.Success && resp.Error != "" {
			// Check response error for 401 indicators
			errStr := strings.ToLower(resp.Error)
			is401 = strings.Contains(errStr, "401") ||
				strings.Contains(errStr, "unauthorized") ||
				strings.Contains(errStr, "token expired")
		}

		// If this is a 401 and we have retries left, attempt refresh
		if is401 && attempt < maxRetries-1 {
			// Extract oauth_token_id from config
			tokenID, hasTokenID := req.Config["oauth_token_id"]
			if hasTokenID && tokenID != "" {
				log.Warnf("⚠️  401 detected for connector %s, attempting OAuth refresh for token: %s", req.Connector, tokenID)

				// Attempt refresh via API Gateway
				refreshClient := a.connectionMgr.GetRefreshClient()
				refreshErr := refreshClient.RefreshTokenViaAPIGateway(ctx, tokenID)

				if refreshErr != nil {
					log.Errorf("❌ OAuth refresh failed: %v", refreshErr)
					// Return the original error since refresh failed
					if err != nil {
						return nil, fmt.Errorf("401 unauthorized and refresh failed: %w", err)
					}
					return resp, nil
				}

				// Refresh succeeded - fetch the new token and retry
				tokenManager := a.connectionMgr.GetTokenManager()
				newAccessToken, fetchErr := tokenManager.GetAccessToken(tokenID)
				if fetchErr != nil {
					log.Errorf("❌ Failed to fetch refreshed token: %v", fetchErr)
					if err != nil {
						return nil, err
					}
					return resp, nil
				}

				// Update config with new token
				req.Config["access_token"] = newAccessToken
				req.Config["api_key"] = newAccessToken // compat alias used by some connectors
				log.Infof("✅ OAuth token refreshed, retrying request for connector %s", req.Connector)

				// Small backoff before retry
				time.Sleep(500 * time.Millisecond)
				continue
			} else {
				log.Warnf("⚠️  401 detected but no oauth_token_id in config, cannot refresh")
			}
		}

		// Non-401 error or no retries left - return the error/response
		if err != nil {
			return nil, err
		}
		return resp, nil
	}

	// Should never reach here, but return error if we do
	return nil, fmt.Errorf("max retries exceeded for connector %s", req.Connector)
}

// ExecuteTask executes a task (exported for worker integration)
func (a *Agent) ExecuteTask(ctx context.Context, task ExecutorTask) ExecutorResponse {
	return a.executeTask(ctx, task)
}

// executeTask executes a task (internal method)
func (a *Agent) executeTask(ctx context.Context, task ExecutorTask) ExecutorResponse {
	// PHASE 2.4: Check for cancellation at task start
	select {
	case <-ctx.Done():
		log.Warnf("⚠️  Task %s cancelled before execution", task.TaskID)
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "cancelled",
			Error:      fmt.Sprintf("task cancelled: %v", ctx.Err()),
		}
	default:
	}
	log.WithFields(log.Fields{
		"task_id":     task.TaskID,
		"pipeline_id": task.PipelineID,
		"operation":   task.Operation,
	}).Debug("executor executeTask entry")

	// Prefer generic plan execution when available.
	// This keeps the executor connector-agnostic and avoids guessing resource names (table/sheet/object).
	if task.Payload != nil {
		planData, ok := task.Payload["plan"].(map[string]interface{})
		if !ok {
			// No plan at top level, skip plan execution
			goto fallback
		}

		// UNWRAP nested plan: V2 payloads have {plan: {plan: {steps: ...}}}
		// Normalize to {plan: {steps: ...}} for consistent processing.
		if nestedPlan, ok := planData["plan"].(map[string]interface{}); ok {
			planData = nestedPlan
		}

		if _, ok := planData["steps"]; ok {
			// If the plan indicates CDC/streaming, DO NOT execute plan steps.
			// CDC streaming requires implicit sink wiring (Debezium -> Kafka -> kafka-mcp-sink)
			// which is implemented in executeDataTransfer(), not executePlan().
			//
			// Batch pipelines should ALSO prefer executeDataTransfer() so we can use the MinIO claim-check
			// path for large payloads (MinIO staging -> Kafka -> kafka-mcp-sink), rather than trying to
			// inline megabytes of rows via plan step outputs.
			planSyncMode := ""
			if v, ok := planData["sync_mode"].(string); ok {
				planSyncMode = strings.ToLower(strings.TrimSpace(v))
			}
			isStreaming := false
			if v, ok := planData["is_streaming"].(bool); ok {
				isStreaming = v
			}
			if planSyncMode == "cdc" || planSyncMode == "streaming" || isStreaming {
				log.Infof("Detected CDC plan in payload; executing via data_transfer path for %s", task.TaskID)

				// Inject plan-level metadata into Params so executeDataTransfer has sync_mode/cdc_provider/etc.
				meta := a.extractPlanMetadata(planData)
				if meta.SyncMode == "" {
					meta.SyncMode = "cdc"
				}
				a.injectPlanMetadataToTask(&task, meta)

				// Hydrate task.Source and task.Destination from connection IDs if missing.
				// This is critical for CDC execution which requires full connection configs.
				//
				// Version pinning: the api-gateway records a concrete version in
				// pipelines.{source,destination}_connector_snapshot at RunPipeline
				// time and forwards it to Temporal via workflowInput. Use that
				// snapshot here in preference to the connection's stored
				// "connector_version" — the latter can be "latest", which
				// re-resolves at execution time and silently drifts when a new
				// connector version ships mid-pipeline.
				// Read the pipeline owner from task params so connection
				// hydration is tenant-scoped. The temporal-adapter activity
				// always sets `user_id` (nl_pipeline_v2_activities.go ~line
				// 144/203/269). If absent (system / replay scenarios), the
				// GetForUser fallback emits a deprecation warning and reads
				// without tenant filter.
				taskUserID, _ := task.Params["user_id"].(string)

				if task.Source == nil && task.Params != nil {
					if srcConnID, ok := task.Params["source_connection_id"].(string); ok && srcConnID != "" && srcConnID != "auto" {
						srcConfig, err := a.connectionMgr.GetForUser(ctx, taskUserID, srcConnID)
						if err == nil {
							task.Source = &ConnectorConfig{
								Type:   srcConfig["type"],
								Config: srcConfig,
							}
							if snapVer := extractSnapshotVersion(task.Params, "source_connector_snapshot"); snapVer != "" {
								task.Source.Version = snapVer
							} else if v, ok := srcConfig["connector_version"]; ok {
								task.Source.Version = v
							}
							log.Infof("✅ Hydrated task.Source from connection %s: type=%s version=%s", srcConnID, task.Source.Type, task.Source.Version)
						} else {
							log.Warnf("⚠️  Failed to hydrate source connection %s: %v", srcConnID, err)
						}
					}
				}
				if task.Destination == nil && task.Params != nil {
					if destConnID, ok := task.Params["destination_connection_id"].(string); ok && destConnID != "" && destConnID != "auto" {
						destConfig, err := a.connectionMgr.GetForUser(ctx, taskUserID, destConnID)
						if err == nil {
							task.Destination = &ConnectorConfig{
								Type:   destConfig["type"],
								Config: destConfig,
							}
							if snapVer := extractSnapshotVersion(task.Params, "destination_connector_snapshot"); snapVer != "" {
								task.Destination.Version = snapVer
							} else if v, ok := destConfig["connector_version"]; ok {
								task.Destination.Version = v
							}
							log.Infof("✅ Hydrated task.Destination from connection %s: type=%s version=%s", destConnID, task.Destination.Type, task.Destination.Version)
						} else {
							log.Warnf("⚠️  Failed to hydrate destination connection %s: %v", destConnID, err)
						}
					}
				}

				// Only run the data_transfer path if we have source/destination configs.
				// Otherwise fall back to executePlan for backward compatibility.
				if task.Source != nil && task.Destination != nil {
					// HITL POLICY: CDC must not infer a table from prompt/plan.
					// Require explicit selected_tables from HITL (or saved config).
					tables := []string{}
					if task.Params != nil {
						if arr, ok := task.Params["selected_tables"].([]string); ok && len(arr) > 0 {
							tables = arr
						} else if iarr, ok := task.Params["selected_tables"].([]interface{}); ok && len(iarr) > 0 {
							for _, v := range iarr {
								if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
									tables = append(tables, strings.TrimSpace(s))
								}
							}
						}
					}
					if len(tables) == 0 && task.Payload != nil {
						if iarr, ok := task.Payload["selected_tables"].([]interface{}); ok && len(iarr) > 0 {
							for _, v := range iarr {
								if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
									tables = append(tables, strings.TrimSpace(s))
								}
							}
						}
					}
					if len(tables) == 0 {
						// Ask user to select tables (discover_schema if available, else manual entry).
						cfg := make(map[string]interface{})
						if task.Source != nil && task.Source.Config != nil {
							for k, v := range task.Source.Config {
								cfg[k] = v
							}
						}
						discovered, err := a.DiscoverSchema(ctx, task.Source.Type, cfg)
						if err != nil {
							return ExecutorResponse{
								TaskID:     task.TaskID,
								PipelineID: task.PipelineID,
								Status:     "waiting_for_table_selection",
								Error:      "We couldn't list tables automatically. Please enter the source table/resource to sync.",
								Result: map[string]interface{}{
									"available_tables":   []map[string]interface{}{},
									"source_type":        task.Source.Type,
									"action_needed":      "table_selection",
									"reason":             fmt.Sprintf("Schema discovery failed (%v). Enter a table/resource name manually (e.g. `users` or `mydb.users`).", err),
									"allow_manual_entry": true,
								},
							}
						}
						// Drop rsync's own bookkeeping/staging tables (`_rsync_*`, `flat_*`)
						// so they never appear as (or get auto-selected in) HITL options.
						discovered = filterInternalTables(discovered)
						if len(discovered) == 0 {
							return ExecutorResponse{
								TaskID:     task.TaskID,
								PipelineID: task.PipelineID,
								Status:     "waiting_for_table_selection",
								Error:      "No tables were discovered. Please enter the source table/resource to sync.",
								Result: map[string]interface{}{
									"available_tables":   []map[string]interface{}{},
									"source_type":        task.Source.Type,
									"action_needed":      "table_selection",
									"reason":             fmt.Sprintf("No tables found in %s. Select/enter what to sync.", task.Source.Type),
									"allow_manual_entry": true,
								},
							}
						}

						tableOptions := make([]map[string]interface{}, 0, len(discovered))
						for _, tbl := range discovered {
							tableOptions = append(tableOptions, map[string]interface{}{
								"name":      tbl.Name,
								"schema":    tbl.Schema,
								"row_count": tbl.RowCount,
								"columns":   len(tbl.Columns),
							})
						}
						return ExecutorResponse{
							TaskID:     task.TaskID,
							PipelineID: task.PipelineID,
							Status:     "waiting_for_table_selection",
							Error:      fmt.Sprintf("Select which table(s)/resource(s) to sync (%d available).", len(discovered)),
							Result: map[string]interface{}{
								"available_tables": tableOptions,
								"source_type":      task.Source.Type,
								"action_needed":    "table_selection",
								"reason":           fmt.Sprintf("Select what to sync from %s before execution.", task.Source.Type),
							},
						}
					}
					// Inject tables into Params for CDC execution.
					if task.Params == nil {
						task.Params = map[string]interface{}{}
					}
					task.Params["tables"] = tables
					log.Infof("✅ Using HITL selected tables for CDC: %v", tables)
					return a.executeDataTransfer(ctx, task)
				}
				log.Warnf("⚠️  CDC plan present but source/destination missing; falling back to executePlan for %s", task.TaskID)
			} else if planSyncMode == "batch" || planSyncMode == "" {
				// Batch transfer routing: prefer executeDataTransfer when we have explicit connection IDs.
				// This enables MinIO staging for large payloads and keeps execution anchored to user-selected connections.
				if task.Params == nil {
					task.Params = map[string]interface{}{}
				}

				meta := a.extractPlanMetadata(planData)
				if meta.SyncMode == "" {
					meta.SyncMode = "batch"
				}
				// Ensure we do not accidentally take streaming path
				task.Params["sync_mode"] = "batch"
				task.Params["is_streaming"] = false
				a.injectPlanMetadataToTask(&task, meta)

				// Hydrate connections if missing. Same version-pinning rule as
				// the primary hydration branch above: prefer the plan-time
				// snapshot over the connection's stored "connector_version" so
				// `:latest` doesn't re-resolve mid-pipeline.
				if task.Source == nil {
					if srcConnID, ok := task.Params["source_connection_id"].(string); ok && srcConnID != "" && srcConnID != "auto" {
						srcConfig, err := a.getConnectionConfigForTask(ctx, task, srcConnID)
						if err == nil {
							task.Source = &ConnectorConfig{
								Type:   srcConfig["type"],
								Config: srcConfig,
							}
							if snapVer := extractSnapshotVersion(task.Params, "source_connector_snapshot"); snapVer != "" {
								task.Source.Version = snapVer
							} else if v, ok := srcConfig["connector_version"]; ok {
								task.Source.Version = v
							}
							log.Infof("✅ Hydrated task.Source from connection %s: type=%s version=%s", srcConnID, task.Source.Type, task.Source.Version)
						} else {
							log.Warnf("⚠️  Failed to hydrate source connection %s: %v", srcConnID, err)
						}
					}
				}
				if task.Destination == nil {
					if destConnID, ok := task.Params["destination_connection_id"].(string); ok && destConnID != "" && destConnID != "auto" {
						destConfig, err := a.getConnectionConfigForTask(ctx, task, destConnID)
						if err == nil {
							task.Destination = &ConnectorConfig{
								Type:   destConfig["type"],
								Config: destConfig,
							}
							if snapVer := extractSnapshotVersion(task.Params, "destination_connector_snapshot"); snapVer != "" {
								task.Destination.Version = snapVer
							} else if v, ok := destConfig["connector_version"]; ok {
								task.Destination.Version = v
							}
							log.Infof("✅ Hydrated task.Destination from connection %s: type=%s version=%s", destConnID, task.Destination.Type, task.Destination.Version)
						} else {
							log.Warnf("⚠️  Failed to hydrate destination connection %s: %v", destConnID, err)
						}
					}
				}

				// If hydration didn't recover Source/Destination — the most
				// common cause is a connection that was deleted while the
				// task was in flight — bail out cleanly. The code path below
				// dereferences task.Source.Type / task.Destination.Type
				// without checking and previously crashed the entire
				// orchestrator container with a nil-pointer panic.
				if resp := requireSourceAndDestination(task); resp != nil {
					return *resp
				}

				// HITL POLICY: batch must not infer a table from prompt/plan/config.table.
				// Require explicit selected_tables from HITL (or saved config).
				tables := []string{}
				if arr, ok := task.Params["selected_tables"].([]string); ok && len(arr) > 0 {
					tables = arr
				} else if iarr, ok := task.Params["selected_tables"].([]interface{}); ok && len(iarr) > 0 {
					for _, v := range iarr {
						if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
							tables = append(tables, strings.TrimSpace(s))
						}
					}
				}
				if len(tables) == 0 && task.Payload != nil {
					if iarr, ok := task.Payload["selected_tables"].([]interface{}); ok && len(iarr) > 0 {
						for _, v := range iarr {
							if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
								tables = append(tables, strings.TrimSpace(s))
							}
						}
					}
				}
				if len(tables) > 0 {
					task.Params["tables"] = tables
					log.Infof("✅ Populated task.Params[\"tables\"] for batch: %v", tables)
				} else {
					// If the user didn't explicitly specify tables and we couldn't resolve any,
					// DO NOT silently fall back to a planner-guessed table in executePlan().
					// Instead, request HITL table selection using schema discovery.
					//
					// This prevents surprising behavior like "sync mysql to s3" exporting an arbitrary table (e.g., "products").
					log.Warnf("⏸️  No tables resolved for batch transfer; requesting HITL table selection")

					cfg := make(map[string]interface{})
					if task.Source != nil && task.Source.Config != nil {
						for k, v := range task.Source.Config {
							cfg[k] = v
						}
					}

					discovered, err := a.DiscoverSchema(ctx, task.Source.Type, cfg)
					if err != nil {
						return ExecutorResponse{
							TaskID:     task.TaskID,
							PipelineID: task.PipelineID,
							Status:     "waiting_for_table_selection",
							Error:      "We couldn't list tables automatically. Please enter the source table/resource to sync.",
							Result: map[string]interface{}{
								"available_tables": []map[string]interface{}{},
								"source_type":      task.Source.Type,
								"action_needed":    "table_selection",
								"reason": fmt.Sprintf(
									"Schema discovery failed (%v). Enter a table/resource name manually (e.g. `users` or `mydb.users`).",
									err,
								),
								"allow_manual_entry": true,
							},
						}
					}
					// Drop rsync's own bookkeeping/staging tables (`_rsync_*`, `flat_*`)
					// so they never appear as (or get auto-selected in) HITL options.
					discovered = filterInternalTables(discovered)
					if len(discovered) == 0 {
						return ExecutorResponse{
							TaskID:     task.TaskID,
							PipelineID: task.PipelineID,
							Status:     "waiting_for_table_selection",
							Error:      "No tables were discovered. Please enter the source table/resource to sync.",
							Result: map[string]interface{}{
								"available_tables": []map[string]interface{}{},
								"source_type":      task.Source.Type,
								"action_needed":    "table_selection",
								"reason": fmt.Sprintf(
									"No tables found in %s. Ensure the connection points to a database/schema with tables and has permissions. You can also enter a qualified name like `db.table`.",
									task.Source.Type,
								),
								"allow_manual_entry": true,
							},
						}
					}

					tableOptions := make([]map[string]interface{}, 0, len(discovered))
					for _, tbl := range discovered {
						tableOptions = append(tableOptions, map[string]interface{}{
							"name":      tbl.Name,
							"schema":    tbl.Schema,
							"row_count": tbl.RowCount,
							"columns":   len(tbl.Columns),
						})
					}

					return ExecutorResponse{
						TaskID:     task.TaskID,
						PipelineID: task.PipelineID,
						Status:     "waiting_for_table_selection",
						Error:      fmt.Sprintf("Select which table(s)/resource(s) to sync (%d available).", len(discovered)),
						Result: map[string]interface{}{
							"available_tables": tableOptions,
							"source_type":      task.Source.Type,
							"action_needed":    "table_selection",
							"reason":           fmt.Sprintf("You didn’t specify a table. Found %d tables/resources in %s. Select what to sync.", len(discovered), task.Source.Type),
						},
					}
				}

				if task.Source != nil && task.Destination != nil {
					log.Infof("Detected batch plan in payload; executing via data_transfer path for %s", task.TaskID)
					return a.executeDataTransfer(ctx, task)
				}
			}

			// For non-CDC plans, only execute steps when the task operation is the generic execute mode.
			if task.Operation == "" || task.Operation == "execute" {
				log.Infof("Detected generic plan execution for %s", task.TaskID)
				return a.executePlan(ctx, task, planData)
			}
		}
	}

fallback:

	// Fallback: If we have direct connection IDs in Params and NO plan, attempt direct transfer.
	// Direct transfer is only safe when we can identify the source resource (e.g., table) unambiguously.
	if task.Params != nil {
		sourceConnID, hasSource := task.Params["source_connection_id"].(string)
		destConnID, hasDest := task.Params["destination_connection_id"].(string)
		log.WithFields(log.Fields{
			"task_id":        task.TaskID,
			"pipeline_id":    task.PipelineID,
			"has_source":     hasSource,
			"has_dest":       hasDest,
			"source_conn_id": sourceConnID,
			"dest_conn_id":   destConnID,
		}).Debug("direct transfer fallback params")

		if hasSource && hasDest && sourceConnID != "" && destConnID != "" && sourceConnID != "auto" && destConnID != "auto" {
			// If this pipeline is configured for CDC, do NOT fall back to direct transfer.
			// Direct transfer is batch-oriented and will:
			// - ignore CDC intent,
			// - fail on duplicate keys during repeated snapshot/application,
			// - never emit CDC captured/applied table stats.
			if a.db != nil && strings.TrimSpace(task.PipelineID) != "" {
				var dbSyncMode sql.NullString
				var dbCDCMode sql.NullString
				if err := a.db.QueryRowContext(ctx, `SELECT sync_mode, cdc_mode FROM pipelines WHERE id = $1`, task.PipelineID).Scan(&dbSyncMode, &dbCDCMode); err == nil {
					if dbSyncMode.Valid && strings.EqualFold(strings.TrimSpace(dbSyncMode.String), "cdc") {
						// Hydrate task.Source / task.Destination from connection configs (plan-less execution).
						if task.Source == nil || task.Destination == nil {
							sourceConfig, err := a.getConnectionConfigForTask(ctx, task, sourceConnID)
							if err == nil && sourceConfig != nil {
								sourceType := strings.TrimSpace(sourceConfig["type"])
								sourceVer := strings.TrimSpace(sourceConfig["connector_version"])
								if sourceVer == "" {
									sourceVer = "latest"
								}
								task.Source = &ConnectorConfig{Type: sourceType, Version: sourceVer, Config: sourceConfig}
							}
							destConfig, err := a.getConnectionConfigForTask(ctx, task, destConnID)
							if err == nil && destConfig != nil {
								destType := strings.TrimSpace(destConfig["type"])
								destVer := strings.TrimSpace(destConfig["connector_version"])
								if destVer == "" {
									destVer = "latest"
								}
								task.Destination = &ConnectorConfig{Type: destType, Version: destVer, Config: destConfig}
							}
						}

						// Backfill required CDC params.
						task.Params["sync_mode"] = "cdc"
						if dbCDCMode.Valid && strings.TrimSpace(dbCDCMode.String) != "" {
							task.Params["cdc_mode"] = strings.TrimSpace(dbCDCMode.String)
						}
						if _, ok := task.Params["cdc_provider"]; !ok {
							task.Params["cdc_provider"] = "debezium"
						}
						// Ensure tables are present (Debezium MCP expects `tables`).
						if _, ok := task.Params["tables"]; !ok {
							task.Params["tables"] = task.Params["selected_tables"]
						}
						// Normalize selected tables into fully-qualified qualifier.table strings.
						// Shared helper (see qualifySelectedTablesForSource) so batch + CDC
						// behave identically: MySQL/SQLServer/Oracle qualify by database name,
						// PostgreSQL by schema (default "public"). executeDataTransfer also
						// calls this, but doing it here keeps the log line below accurate.
						qualifySelectedTablesForSource(&task)

						log.WithFields(log.Fields{
							"pipeline_id": task.PipelineID,
							"sync_mode":   "cdc",
							"cdc_mode":    task.Params["cdc_mode"],
							"tables":      task.Params["tables"],
						}).Info("Plan-less execution: routing to CDC data_transfer (not direct transfer)")
						return a.executeDataTransfer(ctx, task)
					}
				}
			}

			// Plan-less batch execution: route through the canonical batch
			// data plane (executeDataTransfer → executeBatchDataTransfer)
			// instead of the legacy Path A direct-transfer. This unifies
			// chat-created and planner-created pipelines onto the same
			// kafka-mcp-sink + MinIO claim-check pipeline so they all get:
			//   * MinIO claim-check staging for large batches
			//   * Per-table parallelism (EXECUTOR_TABLE_CONCURRENCY)
			//   * Destination-truth TABLE_STATS from kafka-mcp-sink
			//   * SAFETY LIMIT REACHED fail-loud on truncation risk
			//   * Incremental sync via stats.watermark contract
			// executeDataTransfer hydrates Source/Destination from
			// {source,destination}_connection_id (see lines 3417-3454) so
			// we don't need to do that here.
			if _, ok := task.Params["tables"]; !ok {
				if v, ok := task.Params["selected_tables"]; ok && v != nil {
					task.Params["tables"] = v
				}
			}
			log.WithFields(log.Fields{
				"pipeline_id": task.PipelineID,
				"source":      sourceConnID,
				"dest":        destConnID,
			}).Info("Plan-less batch execution: routing to data_transfer (Path B)")
			return a.executeDataTransfer(ctx, task)
		}
	}

	switch task.Operation {
	case "export":
		return a.executeExport(ctx, task)
	case "import":
		return a.executeImport(ctx, task)
	case "query":
		return a.executeQuery(ctx, task)
	case "execute":
		return a.executeCommand(ctx, task)
	case "data_transfer":
		return a.executeDataTransfer(ctx, task)
	// Streaming/CDC operations
	case "start_streaming", "start_cdc":
		return a.executeStartStreaming(ctx, task)
	case "stop_streaming", "stop_cdc":
		return a.executeStopStreaming(ctx, task)
	case "streaming_status", "cdc_status":
		return a.executeStreamingStatus(ctx, task)
	case "restart_streaming", "restart_cdc":
		return a.executeRestartStreaming(ctx, task)
	default:
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      fmt.Sprintf("unknown operation: %s", task.Operation),
		}
	}
}

// connectorKey normalizes connector/resource names for safe comparisons.
// Removes separators and lowercases so we can detect when a "table" equals the connector name
// (e.g., user prompt "sync mysql to s3" should NOT export table "mysql").
func connectorKey(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, "_", "")
	return s
}

// normConnectorType folds a token to a comparable connector-type key: connectorKey
// plus the postgres/postgresql alias (a prompt commonly says "postgres" while the
// connector type is "postgresql"). Bare connectorKey treats those as different,
// which let the destination connector type leak in as a table name via the NL
// table-inference guards (see inferTablesFromUserRequest) and the selected_tables
// guard (qualifySelectedTablesForSource). Single source of truth for both.
func normConnectorType(s string) string {
	k := connectorKey(s)
	if k == "postgres" {
		k = "postgresql"
	}
	return k
}

// executeWithRetry executes an MCP request with exponential backoff
func (a *Agent) executeWithRetry(ctx context.Context, req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error) {
	var err error
	var resp *mcp.ExecuteResponse

	// F-Obs-2: instrument MCP call latency + outcome. `connector` and
	// `operation` are small fixed cardinalities by design (see
	// metrics/metrics.go comment); `status` is success|failure.
	mcpStart := time.Now()
	defer func() {
		appmetrics.MCPCallDurationSeconds.WithLabelValues(req.Connector, req.Operation).Observe(time.Since(mcpStart).Seconds())
		statusLabel := "success"
		if err != nil || (resp != nil && !resp.Success) {
			statusLabel = "failure"
		}
		appmetrics.MCPCallsTotal.WithLabelValues(req.Connector, req.Operation, statusLabel).Inc()
	}()

	backoff := InitialBackoff

	for i := 0; i <= MaxRetries; i++ {
		// PHASE 2.4: Check for cancellation before each retry
		select {
		case <-ctx.Done():
			log.Warnf("⚠️  Retry cancelled for %s operation on %s", req.Operation, req.Connector)
			return nil, fmt.Errorf("operation cancelled during retry: %w", ctx.Err())
		default:
		}

		if i > 0 {
			log.Warnf("🔄 Retry %d/%d for %s operation on %s (backoff: %v)", i, MaxRetries, req.Operation, req.Connector, backoff)

			// PHASE 2.4: Use context-aware sleep
			select {
			case <-time.After(backoff):
				// Continue with retry
			case <-ctx.Done():
				log.Warnf("⚠️  Retry cancelled during backoff for %s", req.Connector)
				return nil, fmt.Errorf("operation cancelled during backoff: %w", ctx.Err())
			}

			// Exponential backoff with jitter
			backoff = time.Duration(float64(backoff) * 2)
			if backoff > MaxBackoff {
				backoff = MaxBackoff
			}
			// Add jitter (+/- 20%)
			jitter := time.Duration(rand.Int63n(int64(backoff) / 5))
			backoff += jitter
		}

		resp, err = a.executeWithOAuthRetry(ctx, req)

		// Success case
		if err == nil && resp.Success {
			return resp, nil
		}

		// Decide retryability. The healer is deliberately conservative:
		// we retry only what we can plausibly distinguish as transient.
		// HTTP 4xx errors (401/403/404 from the SaaS source) are the
		// most common credential / config bug and previously fell through
		// to "retry with backoff" + a useless `<nil>` log line.
		isRetryable := false
		var classifyMsg string
		if err != nil {
			classifyMsg = err.Error()
		} else if resp != nil {
			classifyMsg = resp.Error
		}
		lower := strings.ToLower(classifyMsg)

		// Look for a 4xx status code anywhere in the message. We
		// special-case 408 (timeout) and 429 (rate limit) because both
		// are legitimately transient. Everything else in the 4xx range
		// is a client-side error — almost always credentials or a wrong
		// table/resource id — and retrying just delays a HITL prompt.
		isClient4xx := false
		for _, code := range []string{"400", "401", "402", "403", "404", "405", "406", "409", "410", "415", "422"} {
			if strings.Contains(lower, "http "+code) || strings.Contains(lower, "status "+code) || strings.Contains(lower, "status_code: "+code) || strings.Contains(lower, "status: "+code) {
				isClient4xx = true
				break
			}
		}

		// Spec-compliant connector contract failures: a connector that returns
		// JSON-RPC `{success: false, error: "unknown operation '<op>'"}` over
		// HTTP 200 is reporting a deterministic client error (caller asked for
		// an op that doesn't exist). Treat these as 4xx-equivalent — retrying
		// will produce the same answer and just wastes ~70s in backoff before
		// the postflight silent_drop guard catches it.
		if !isClient4xx {
			for _, marker := range []string{"unknown operation", "no such operation", "operation not found"} {
				if strings.Contains(lower, marker) {
					isClient4xx = true
					break
				}
			}
		}

		switch {
		case isClient4xx:
			isRetryable = false
		case strings.Contains(lower, "timeout") || strings.Contains(lower, "connection") || strings.Contains(lower, "network") || strings.Contains(lower, "rate limit") || strings.Contains(lower, "http 408") || strings.Contains(lower, "http 429") || strings.Contains(lower, "http 500") || strings.Contains(lower, "http 502") || strings.Contains(lower, "http 503") || strings.Contains(lower, "http 504"):
			isRetryable = true
		case err != nil && resp == nil:
			// Pure transport / infrastructure failure with no response —
			// almost always transient. Retry once with backoff.
			isRetryable = true
		default:
			// Unknown shape — fail closed so we don't burn retries on
			// non-transient problems. Healer can still upgrade to a
			// retry in a downstream stage if it has more context.
			isRetryable = false
		}

		if !isRetryable {
			// Surface the most informative error we have. The previous
			// "%v" of a nil err produced "<nil>"; explicitly choose
			// classifyMsg so logs are actionable.
			if classifyMsg == "" {
				classifyMsg = "unspecified failure (err and resp.Error both empty)"
			}
			log.Warnf("❌ Non-retryable error (client_4xx=%v): %s", isClient4xx, llmscrub.Scrub(classifyMsg))
			break
		}
	}

	return resp, err
}

// executePlan executes a generic plan with multiple steps (supports pagination)
//
// PHASE 1 FIX: Extracts plan-level metadata (cdc_provider, topic_provisioning, sync_mode)
// and propagates them to individual step execution so they're not lost.
func (a *Agent) executePlan(ctx context.Context, task ExecutorTask, planData map[string]interface{}) ExecutorResponse {
	// ==========================================================================
	// PHASE 1: Extract plan-level metadata for CDC and topic provisioning
	// ==========================================================================
	planMeta := a.extractPlanMetadata(planData)
	if planMeta.CDCProvider != "" {
		log.Infof("📋 Plan metadata: cdc_provider=%s, sync_mode=%s", planMeta.CDCProvider, planMeta.SyncMode)
	}
	if planMeta.TopicName != "" {
		log.Infof("📦 Plan metadata: topic=%s, partitions=%d", planMeta.TopicName, planMeta.TopicPartitions)
	}

	// Parse steps
	stepsBytes, _ := json.Marshal(planData["steps"])
	var steps []PlanStep
	if err := json.Unmarshal(stepsBytes, &steps); err != nil {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      fmt.Sprintf("failed to parse plan steps: %v", err),
		}
	}

	log.Infof("🚀 Executing generic plan with %d steps", len(steps))

	// Inject plan-level metadata into task.Params for downstream use
	if task.Params == nil {
		task.Params = make(map[string]interface{})
	}
	a.injectPlanMetadataToTask(&task, planMeta)

	// Context to store results from previous steps
	// Format: $step_id.output
	planCtx := make(map[string]interface{})

	// Track total metrics
	totalRows := int64(0)
	totalBytes := int64(0)

	// Validate plan has executable steps
	if len(steps) == 0 {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      "plan has zero steps - nothing to execute",
		}
	}

	for i, step := range steps {
		// PHASE 2.4: Check for cancellation before each step
		select {
		case <-ctx.Done():
			log.Warnf("⚠️  Plan execution cancelled after step %d/%d", i, len(steps))
			return ExecutorResponse{
				TaskID:     task.TaskID,
				PipelineID: task.PipelineID,
				Status:     "cancelled",
				Error:      fmt.Sprintf("plan cancelled at step %d: %v", i+1, ctx.Err()),
				Result: map[string]interface{}{
					"steps_completed": i,
					"total_steps":     len(steps),
					"rows_processed":  totalRows,
				},
			}
		default:
		}

		log.Infof("▶️ Step %d/%d: %s (%s)", i+1, len(steps), step.ID, step.Method)

		// 1. Resolve parameters (variable substitution)
		resolvedParams := a.resolveParams(step.Params, planCtx)

		// 2. Get config for tool from Connection Manager
		var toolConfig map[string]string
		var err error

		if step.ConnectionID != "" {
			// PRIMARY PATH: Use Connection Registry (fully generic)
			log.WithField("trace_id", telemetry.TraceIDFromContext(ctx)).Infof("🔐 Fetching credentials for %s from connection: %s", step.Tool, step.ConnectionID)
			toolConfig, err = a.getConnectionConfigForTask(ctx, task, step.ConnectionID)
			if err != nil {
				return ExecutorResponse{
					TaskID:     task.TaskID,
					PipelineID: task.PipelineID,
					Status:     "failed",
					Error:      fmt.Sprintf("failed to get credentials for %s: %v", step.Tool, err),
				}
			}
			// SECURITY: Only log config key count, never log actual values
			log.WithField("trace_id", telemetry.TraceIDFromContext(ctx)).Infof("✅ Loaded %d config keys for %s from connection %s", len(toolConfig), step.Tool, step.ConnectionID)
		} else {
			// FALLBACK: Legacy support (Source/Dest for backward compatibility)
			log.Warnf("⚠️  No connection_id in step, using legacy Source/Dest lookup")
			if task.Source != nil && task.Source.Type == step.Tool {
				toolConfig = task.Source.Config
				log.Debugf("Using Source config for %s", step.Tool)
			} else if task.Destination != nil && task.Destination.Type == step.Tool {
				toolConfig = task.Destination.Config
				log.Debugf("Using Destination config for %s", step.Tool)
			} else {
				// Last resort: empty config (for tools that don't need auth)
				toolConfig = make(map[string]string)
				log.Warnf("⚠️  No credentials available for %s, using empty config", step.Tool)
			}
		}

		// Guardrail: avoid executing DB export with an obviously wrong "table" like the connector name.
		// This keeps the system generic and prevents failures like: Table 'db.mysql' doesn't exist.
		if (strings.EqualFold(step.Method, "export") || strings.EqualFold(step.Method, "export_data")) && resolvedParams != nil {
			if t, ok := resolvedParams["table"].(string); ok && t != "" && connectorKey(t) == connectorKey(step.Tool) {
				// Attempt a safe auto-selection: if there's exactly one table, use it.
				cfg := make(map[string]interface{})
				for k, v := range toolConfig {
					cfg[k] = v
				}
				tables, derr := a.DiscoverSchema(ctx, step.Tool, cfg)
				// Drop rsync's own bookkeeping/staging tables so single-table
				// auto-selection and the multi-table HITL pause below both act on
				// real user tables only (never `_rsync_*`/`flat_*`).
				if derr == nil {
					tables = filterInternalTables(tables)
				}
				if derr == nil && len(tables) == 1 {
					resolvedParams["table"] = tables[0].Name
					log.WithField("trace_id", telemetry.TraceIDFromContext(ctx)).Infof("✅ Auto-corrected export table to only available table: %s", tables[0].Name)
				} else {
					preview := ""
					if derr == nil && len(tables) > 0 {
						names := make([]string, 0, 10)
						for j := 0; j < len(tables) && j < 10; j++ {
							names = append(names, tables[j].Name)
						}
						preview = fmt.Sprintf(" Available tables include: %s", strings.Join(names, ", "))
					}
					// If multiple tables exist, pause for user selection (agentic HITL).
					if derr == nil && len(tables) > 1 {
						tableOptions := make([]map[string]interface{}, 0, len(tables))
						for _, tbl := range tables {
							tableOptions = append(tableOptions, map[string]interface{}{
								"name":      tbl.Name,
								"schema":    tbl.Schema,
								"row_count": tbl.RowCount,
								"columns":   len(tbl.Columns),
							})
						}
						return ExecutorResponse{
							TaskID:     task.TaskID,
							PipelineID: task.PipelineID,
							Status:     "waiting_for_table_selection",
							Error:      fmt.Sprintf("Found %d tables in %s. Please select which table(s) to sync.", len(tables), step.Tool),
							Result: map[string]interface{}{
								"available_tables": tableOptions,
								"source_type":      step.Tool,
								"action_needed":    "table_selection",
								"reason":           fmt.Sprintf("Found %d tables in %s. Select which table(s) to sync.", len(tables), step.Tool),
							},
						}
					}

					return ExecutorResponse{
						TaskID:     task.TaskID,
						PipelineID: task.PipelineID,
						Status:     "failed",
						Error:      fmt.Sprintf("missing/ambiguous source table. Please re-run with an explicit table, e.g. 'sync table users from %s to <destination>'.%s", step.Tool, preview),
					}
				}
			}
		}

		// 3. Check if this step needs pagination (export operations from databases)
		needsPagination := a.shouldUsePagination(step, resolvedParams)

		var resp *mcp.ExecuteResponse
		if needsPagination {
			// Execute with pagination loop
			resp, err = a.executeWithPagination(ctx, step, toolConfig, resolvedParams)
		} else {
			// Execute Step with Retry (single shot)
			req := mcp.ExecuteRequest{
				Connector: step.Tool,
				Operation: step.Method,
				Config:    toolConfig,
				Params:    resolvedParams,
			}
			resp, err = a.executeWithRetry(ctx, req)
		}

		if err != nil {
			return ExecutorResponse{
				TaskID:     task.TaskID,
				PipelineID: task.PipelineID,
				Status:     "failed",
				Error:      fmt.Sprintf("step %s failed: %v", step.ID, err),
			}
		}

		if !resp.Success {
			return ExecutorResponse{
				TaskID:     task.TaskID,
				PipelineID: task.PipelineID,
				Status:     "failed",
				Error:      fmt.Sprintf("step %s failed: %s", step.ID, resp.Error),
			}
		}

		// 4. Store output in context
		planCtx[step.ID] = resp.Result
		// Also allow referencing as $last.output
		planCtx["last"] = resp.Result

		// Track metrics
		if rowCount, ok := resp.Result["row_count"].(float64); ok {
			totalRows += int64(rowCount)
		}
		if bytes, ok := resp.Result["bytes"].(float64); ok {
			totalBytes += int64(bytes)
		}

		log.Infof("✅ Step %s completed", step.ID)
	}

	// Build final result (safe access since we validated len(steps) > 0)
	finalResult, ok := planCtx["last"].(map[string]interface{})
	if !ok || finalResult == nil {
		// Fallback to empty result if last step didn't produce expected output
		finalResult = make(map[string]interface{})
	}
	finalResult["total_rows"] = totalRows
	finalResult["total_bytes"] = totalBytes

	return ExecutorResponse{
		TaskID:        task.TaskID,
		PipelineID:    task.PipelineID,
		Status:        "success",
		Result:        finalResult,
		RowsProcessed: int(totalRows),
	}
}

// shouldUsePagination determines if a step should use pagination
func (a *Agent) shouldUsePagination(step PlanStep, params map[string]interface{}) bool {
	// Never paginate API SaaS sources unless explicitly requested.
	// Most API connectors (HubSpot, Shopify, etc.) implement their own paging and do NOT
	// accept offset/limit in a DB-style manner.
	if strings.EqualFold(step.Tool, "hubspot") {
		return false
	}

	// GENERIC: If batch_size is present and > 0, use pagination
	// This replaces the hardcoded list of database types
	if batchSize, ok := params["batch_size"]; ok {
		if bs, ok := batchSize.(float64); ok && bs > 0 {
			return true
		}
		if bs, ok := batchSize.(int); ok && bs > 0 {
			return true
		}
	}

	return false
}

// executeWithPagination executes a step with offset/limit pagination for large datasets
func (a *Agent) executeWithPagination(ctx context.Context, step PlanStep, config map[string]string, params map[string]interface{}) (*mcp.ExecuteResponse, error) {
	batchSize := a.extractBatchSize(params, 10000)
	offset := 0
	totalRows := int64(0)
	var allData []interface{}
	batchCount := 0

	log.Infof("📄 Starting paginated execution with batch_size=%d", batchSize)

	const maxIterations = 1000 // Safety limit to prevent infinite loops
	for iteration := 0; iteration < maxIterations; iteration++ {
		// PHASE 2.4: Check for cancellation before each batch
		select {
		case <-ctx.Done():
			log.Warnf("⚠️  Pagination cancelled after %d batches (%d rows processed)", batchCount, totalRows)
			return nil, fmt.Errorf("pagination cancelled: %w", ctx.Err())
		default:
		}

		paginatedParams := a.buildPaginatedParams(params, offset, batchSize)

		log.Infof("📄 Batch %d: offset=%d, limit=%d", iteration+1, offset, batchSize)

		req := mcp.ExecuteRequest{
			Connector: step.Tool,
			Operation: step.Method,
			Config:    config,
			Params:    paginatedParams,
		}

		resp, err := a.executeWithRetry(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("pagination batch %d failed: %w", iteration+1, err)
		}

		if !resp.Success {
			return nil, fmt.Errorf("pagination batch %d failed: %s", iteration+1, resp.Error)
		}

		batchCount++
		rowsInBatch := a.extractRowCount(resp.Result)

		// Accumulate data if present
		if data, ok := resp.Result["data"].([]interface{}); ok {
			allData = append(allData, data...)
		}

		totalRows += rowsInBatch
		log.Infof("📄 Batch %d complete: %d rows (total: %d)", iteration+1, rowsInBatch, totalRows)

		// Check termination conditions: no rows returned or partial batch (last page)
		if rowsInBatch == 0 || rowsInBatch < int64(batchSize) {
			log.Infof("📄 Pagination complete: total %d rows in %d batches", totalRows, batchCount)
			break
		}

		offset += batchSize
	}

	return &mcp.ExecuteResponse{
		Success: true,
		Result: map[string]interface{}{
			"row_count": totalRows,
			"data":      allData,
			"paginated": true,
			"batches":   batchCount,
		},
	}, nil
}

// extractBatchSize extracts batch size from params with type coercion
func (a *Agent) extractBatchSize(params map[string]interface{}, defaultSize int) int {
	if bs, ok := params["batch_size"].(float64); ok && bs > 0 {
		return int(bs)
	}
	if bs, ok := params["batch_size"].(int); ok && bs > 0 {
		return bs
	}
	return defaultSize
}

// buildPaginatedParams creates a copy of params with pagination fields
func (a *Agent) buildPaginatedParams(params map[string]interface{}, offset, limit int) map[string]interface{} {
	paginatedParams := make(map[string]interface{}, len(params)+2)
	for k, v := range params {
		paginatedParams[k] = v
	}
	paginatedParams["offset"] = offset
	paginatedParams["limit"] = limit
	return paginatedParams
}

// extractRowCount extracts row count from result with robust type handling
func (a *Agent) extractRowCount(result map[string]interface{}) int64 {
	// Try explicit row_count field with various numeric types
	if rc, ok := result["row_count"].(float64); ok {
		return int64(rc)
	}
	if rc, ok := result["row_count"].(int); ok {
		return int64(rc)
	}
	if rc, ok := result["row_count"].(int64); ok {
		return rc
	}

	// Fallback: count data array length if row_count is missing
	if data, ok := result["data"].([]interface{}); ok {
		count := int64(len(data))
		log.Warnf("⚠️  row_count missing in response, using data array length: %d", count)
		return count
	}

	return 0
}

// resolveParams substitutes variables in params
func (a *Agent) resolveParams(params map[string]interface{}, ctx map[string]interface{}) map[string]interface{} {
	resolved := make(map[string]interface{})
	for k, v := range params {
		if strVal, ok := v.(string); ok && strings.HasPrefix(strVal, "$") {
			// Simple substitution: $step_1.output
			// We only support referencing the whole output object or direct fields for now
			// Format: $step_id.output -> returns the whole result map
			// Format: $step_id.output.field -> returns specific field

			path := strings.TrimPrefix(strVal, "$")
			parts := strings.Split(path, ".")

			if len(parts) >= 2 {
				stepID := parts[0]
				// resource := parts[1] // "output"

				if stepResult, exists := ctx[stepID]; exists {
					if resultMap, ok := stepResult.(map[string]interface{}); ok {
						if len(parts) > 2 {
							// Access specific field
							field := parts[2]
							if val, hasField := resultMap[field]; hasField {
								resolved[k] = val
								continue
							}
						} else {
							// Return whole object
							resolved[k] = stepResult
							continue
						}
					}
				}
			}
			// If resolution fails, keep original string (might be a literal $)
			resolved[k] = v
		} else {
			resolved[k] = v
		}
	}

	// NOTE: We no longer call normalizeParamsForMCP() here as base_connector.py handles normalization now.
	// This makes the Executor truly generic and moves business logic to the MCP layer.

	return resolved
}

// executeDataTransfer handles complete data transfer using Kafka streaming
// This is the GENERIC data path that works with any source → destination combination
//
// FULLY GENERIC: Uses CDC provider and topic from plan, not hardcoded values
func (a *Agent) executeDataTransfer(ctx context.Context, task ExecutorTask) ExecutorResponse {
	// DAG / plan-less execution support: hydrate Source/Destination from connection IDs if needed.
	// This allows node-level execution (Temporal DAG nodes) to invoke `data_transfer` without
	// embedding secrets in workflow payloads.
	if task.Params != nil {
		// Pipeline owner — passed through from the temporal-adapter
		// activity (nl_pipeline_v2_activities.go sets "user_id"). When
		// missing (legacy / replay), GetForUser falls back to the
		// tenant-blind path with a deprecation warning.
		taskUserID, _ := task.Params["user_id"].(string)

		if task.Source == nil {
			if srcConnID, ok := task.Params["source_connection_id"].(string); ok {
				srcConnID = strings.TrimSpace(srcConnID)
				if srcConnID != "" && srcConnID != "auto" {
					if cfg, err := a.connectionMgr.GetForUser(ctx, taskUserID, srcConnID); err == nil && cfg != nil {
						sourceType := strings.TrimSpace(cfg["type"])
						sourceVer := strings.TrimSpace(cfg["connector_version"])
						if sourceVer == "" {
							sourceVer = "latest"
						}
						task.Source = &ConnectorConfig{Type: sourceType, Version: sourceVer, Config: cfg}
						log.Infof("✅ Hydrated task.Source from connection %s: type=%s", srcConnID, task.Source.Type)
					} else if err != nil {
						log.Warnf("⚠️  Failed to hydrate source connection %s: %v", srcConnID, err)
					}
				}
			}
		}
		if task.Destination == nil {
			if destConnID, ok := task.Params["destination_connection_id"].(string); ok {
				destConnID = strings.TrimSpace(destConnID)
				if destConnID != "" && destConnID != "auto" {
					if cfg, err := a.connectionMgr.GetForUser(ctx, taskUserID, destConnID); err == nil && cfg != nil {
						destType := strings.TrimSpace(cfg["type"])
						destVer := strings.TrimSpace(cfg["connector_version"])
						if destVer == "" {
							destVer = "latest"
						}
						task.Destination = &ConnectorConfig{Type: destType, Version: destVer, Config: cfg}
						log.Infof("✅ Hydrated task.Destination from connection %s: type=%s", destConnID, task.Destination.Type)
					} else if err != nil {
						log.Warnf("⚠️  Failed to hydrate destination connection %s: %v", destConnID, err)
					}
				}
			}
		}
	}

	if task.Source == nil {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      "source not specified",
		}
	}
	if task.Destination == nil {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      "destination not specified",
		}
	}

	traceID := telemetry.TraceIDFromContext(ctx)
	log.WithField("trace_id", traceID).Infof("🔄 Data transfer: %s → %s", task.Source.Type, task.Destination.Type)

	// Ensure Params map exists so we can backfill defaults safely.
	if task.Params == nil {
		task.Params = map[string]interface{}{}
	}

	// Schema/database qualification for the selected tables. Source is hydrated by
	// this point, so we can resolve the correct qualifier (PG schema vs DB name).
	// Bug #13: the batch path previously skipped this (only the plan-less CDC branch
	// qualified), so PG batch transfers emitted `SELECT * FROM <db>.<table>` →
	// "relation does not exist". Running it here covers BOTH batch and CDC uniformly.
	qualifySelectedTablesForSource(&task)

	// Capability gate (universal-blob-passthrough plan §2): reject a move whose
	// modality the destination connector cannot accept (e.g. a blob/raw-file copy
	// into a structured-only destination) BEFORE any object is staged — fail loud,
	// never silently drop. No-op for the default "structured" modality, so every
	// existing pipeline is unaffected and no destination metadata is read on that
	// hot path. Covers both batch and CDC (runs ahead of the sync-mode branch).
	if resp := a.enforceCapabilityGate(&task); resp != nil {
		return *resp
	}

	// NL transform gate (masking + type conversion). Path-independent: writes
	// transform_definitions BEFORE the batch export loader / CDC sink consume them,
	// so an NL "mask email" / "convert string columns" request applies for BOTH
	// modes even when the frontend suggestions dialog is bypassed (the CDC chat
	// path). Fail-closed on masking so PII can never silently pass through
	// unmasked; a no-op unless the pipeline carries an nl_transforms intent.
	if err := a.planNLTransforms(ctx, task); err != nil {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      fmt.Sprintf("nl-transform planning failed: %v", err),
		}
	}

	// Determine sync mode (batch or streaming/CDC)
	syncMode := "batch"
	if mode, ok := task.Params["sync_mode"].(string); ok {
		syncMode = mode
	}

	// Backstop: if sync_mode wasn't propagated in the task payload (fast-rerun / plan-less execution),
	// consult the pipelines table so CDC pipelines don't silently run as batch.
	if strings.TrimSpace(syncMode) == "" || strings.EqualFold(strings.TrimSpace(syncMode), "batch") {
		if a.db != nil && strings.TrimSpace(task.PipelineID) != "" {
			var dbSyncMode sql.NullString
			var dbCDCMode sql.NullString
			if err := a.db.QueryRowContext(ctx, `SELECT sync_mode, cdc_mode FROM pipelines WHERE id = $1`, task.PipelineID).Scan(&dbSyncMode, &dbCDCMode); err == nil {
				if dbSyncMode.Valid && strings.EqualFold(dbSyncMode.String, "cdc") {
					syncMode = "cdc"
					task.Params["sync_mode"] = "cdc"
					if dbCDCMode.Valid && strings.TrimSpace(dbCDCMode.String) != "" {
						task.Params["cdc_mode"] = strings.TrimSpace(dbCDCMode.String)
					}
					// Ensure tables are present for Debezium MCP start_sync.
					if _, ok := task.Params["tables"]; !ok {
						if v, ok := task.Params["selected_tables"]; ok && v != nil {
							task.Params["tables"] = v
						}
					}
					// Ensure CDC provider default is present if plan metadata was skipped.
					if _, ok := task.Params["cdc_provider"]; !ok {
						task.Params["cdc_provider"] = "debezium"
					}
					log.WithFields(log.Fields{
						"pipeline_id": task.PipelineID,
						"sync_mode":   syncMode,
						"cdc_mode":    task.Params["cdc_mode"],
					}).Info("✅ Backfilled CDC sync_mode from DB for plan-less execution")
				}
			}
		}
	}

	// ==========================================================================
	// PHASE 2 & 3: Use pre-provisioned topic from plan (not hardcoded)
	// ==========================================================================
	kafkaTopic := resolvePipelineTopic(task.Params, task.PipelineID)

	// Blob (raw-bytes passthrough) lane — universal-blob-passthrough plan §3. The
	// capability gate above already proved the destination accepts the "blob"
	// modality. Blob is a batch-only object copy in v1 (CDC of blobs is a
	// documented non-goal, plan §7), so reject blob+CDC LOUDLY rather than
	// silently degrading to the structured path (which would parse/flatten bytes).
	if deriveMoveModality(task.Params) == ModalityBlob {
		if syncMode == "cdc" || syncMode == "streaming" {
			return ExecutorResponse{
				TaskID:     task.TaskID,
				PipelineID: task.PipelineID,
				Status:     "failed",
				Error:      "blob (raw-file) passthrough is batch-only in v1; CDC of blobs is not supported",
			}
		}
		return a.executeBlobDataTransfer(ctx, task, kafkaTopic, traceID)
	}

	if syncMode == "cdc" || syncMode == "streaming" {
		// HYBRID CDC (Path C, batch historical load): when cdc_initial_load=batch, load
		// history via the fast/resumable batch data plane and have Debezium stream ONLY
		// incremental changes from a captured source position P (position-anchored handoff).
		// Avoids Debezium's slow, lock-prone, non-resumable initial snapshot for large tables.
		initialLoad := cdcInitialLoadChoice(ctx, a.db, task)
		useBatch := initialLoad == "batch"
		if !useBatch && initialLoad == "" {
			// No explicit choice → auto-default to the RESUMABLE batch load when the Debezium
			// path would otherwise run a NON-resumable blocking snapshot on a large PostgreSQL
			// load. A selected table without a primary key forces the whole pipeline to the
			// blocking snapshot (incremental snapshots chunk by PK); that restart-loops to the
			// Sentinel cap and reaps the replication slot, losing WAL (data loss). The batch
			// path pins WAL at P, back-fills resumably (no-PK-safe), then streams from P — no
			// gap, no loss. All-PK large loads stay on the Debezium path (they get the
			// concurrent, resumable incremental snapshot); small loads stay blocking (instant).
			if sel := hybridTablesFromTask(task); a.shouldAutoUseBatchInitialLoad(ctx, task, sel) {
				log.WithFields(log.Fields{
					"pipeline_id": task.PipelineID,
					"tables":      len(sel),
				}).Info("📸 CDC: auto-selected resumable batch initial load (large PostgreSQL load with a no-PK table would otherwise use a non-resumable blocking snapshot → slot-reap data-loss risk)")
				useBatch = true
			}
		}
		if useBatch {
			return a.executeHybridCDCDataTransfer(ctx, task, kafkaTopic, traceID)
		}
		// CDC MODE (Path C): Start CDC provider → data flows to Kafka → Kafka-MCP-Sink
		return a.executeStreamingDataTransfer(ctx, task, kafkaTopic, traceID)
	}

	// BATCH MODE (Path B): Kafka-sink data plane with per-batch claim-check
	// (MinIO staging) and optional inline Kafka for small payloads. This
	// scales better for unknown customer sizes, and table statistics are
	// destination-truth (emitted by kafka-mcp-sink after successful write).
	//
	// Path A (executeDirectTransfer) was deleted — all batch flows now go
	// through this single path. See INCREMENTAL.md and CAPABILITIES.md for
	// the consolidation rationale.
	return a.executeBatchDataTransfer(ctx, task, kafkaTopic, traceID)
}

// resolvePipelineTopic picks the batch data topic for a run: the name the plan
// pre-provisioned when there is one, otherwise the deterministic per-pipeline
// name. Either way the result is qualified with this deployment's namespace.
//
// The qualification is the point. The planner names the topic and already
// qualifies it, so on a matched deployment kafkaclient.Topic is a no-op here
// (it is idempotent). It becomes load-bearing when the two halves disagree,
// which happens for two reasons that both present as a healthy pipeline moving
// zero rows:
//
//  1. An upgrade. Plans are persisted, so a pipeline planned before the
//     namespace existed replays its unprefixed name into a namespaced
//     orchestrator on its very next run.
//  2. Version skew between the Go and Python halves of the naming contract
//     (shared/go/kafkaclient/topics.go <-> llm-service src/utils/kafka_topics.py).
//
// Left unqualified the split is silent and asymmetric: every produce funnels
// through Manager.ProduceWithContext, which qualifies (kafka/manager.go:321),
// while this same name reaches the sink verbatim as its SUBSCRIPTION. The rows
// land in "rsync.pipeline.<id8>.data" and the sink waits on an empty
// "pipeline.<id8>.data" forever. The run then fails closed with "dispatched N
// rows via sink but no acks were recorded", which reads as an unreachable
// destination rather than as two components disagreeing about a name.
func resolvePipelineTopic(params map[string]interface{}, pipelineID string) string {
	if topicInfo, ok := params["topic_provisioning"].(map[string]interface{}); ok {
		if planned, ok := topicInfo["topic_name"].(string); ok && strings.TrimSpace(planned) != "" {
			planned = strings.TrimSpace(planned)
			qualified := kafkaclient.Topic(planned)
			if qualified != planned {
				log.WithFields(log.Fields{
					"planned_topic": planned,
					"topic":         qualified,
					"topic_prefix":  kafkaclient.TopicPrefix(),
				}).Warn("⚠️  Plan named an unqualified Kafka topic — qualifying it so the sink subscribes where the export actually produces. Whatever named it does not share this deployment's KAFKA_TOPIC_PREFIX: a plan stored before the namespace existed, or a planner/orchestrator version skew")
			}
			log.Infof("📦 Using pre-provisioned topic: %s", qualified)
			return qualified
		}
	}
	topic := kafkaclient.Topic(fmt.Sprintf("pipeline.%s.data", utils.SafeID8(pipelineID)))
	log.Infof("📦 Using generated topic: %s", topic)
	return topic
}

// kafkaTopicFromResult digs the Debezium first-table topic out of a CDC provider
// start response, tolerating the envelope shapes an MCP result can arrive in
// (top-level, nested "result", or nested "structuredContent"). Returns "" when
// no kafka_topic is present anywhere.
func kafkaTopicFromResult(res map[string]interface{}) string {
	if res == nil {
		return ""
	}
	if v, ok := res["kafka_topic"].(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	for _, key := range []string{"result", "structuredContent"} {
		if nested, ok := res[key].(map[string]interface{}); ok {
			if v, ok := nested["kafka_topic"].(string); ok && strings.TrimSpace(v) != "" {
				return strings.TrimSpace(v)
			}
		}
	}
	return ""
}

// resolveCDCStreamTopic returns the Kafka topic the CDC *streaming* sink must
// consume. It is ALWAYS a Debezium topic — the concrete first-table topic
// "<topic.prefix>.<db>.<table>" when the provider reports one, else the bare
// topic prefix (== the connector name "cdc-<id>"). It is NEVER the plan-level
// pre-provisioned "pipeline.<id>.data" batch topic.
//
// Why this matters: startKafkaMCPSink derives the per-table CDC subscription by
// splitting a Debezium prefix off the topic, and it DISABLES that fan-out for any
// topic beginning with "pipeline." (the hybrid batch-backfill sink's topic). If a
// "pipeline.<id>.data" value leaks in here, the streaming sink subscribes to that
// single, empty topic instead of the populated cdc-<id>.* topics — the CDC ->
// object-storage "every component healthy, zero rows delivered" failure. Guard
// against it explicitly and fall back to the Debezium prefix.
func resolveCDCStreamTopic(debeziumConnName string, startResult map[string]interface{}) string {
	if t := kafkaTopicFromResult(startResult); t != "" && !kafkaclient.InNamespace(t, "pipeline.") {
		return t
	}
	// Fallback path: no topic came back from the provider, so predict the one
	// Debezium will write. The connector qualifies its topic.prefix (see
	// debezium versions/v1.0.0/connector.py _qualify_topic), so predicting the
	// bare connector name here would subscribe the sink to a topic that does
	// not exist -- which drains nothing and reports no error.
	return kafkaclient.Topic(strings.TrimSpace(debeziumConnName))
}

// debeziumSafeName is the Go copy of _safe_name() in the Debezium connector
// (shared/mcp-connectors/internal/debezium/versions/v1.0.0/connector.py:67).
//
// It is duplicated rather than imported because the two run in different processes in
// different languages, and it is copied EXACTLY — including the parts that look like
// accidents — because the only thing that matters is that both sides compute the same
// string. In particular: a dot is NOT a legal character here (unlike kafkaclient.Topic's
// rule), runs of illegal characters collapse to a single underscore, leading and
// trailing underscores are stripped, an empty result becomes "rsync", and truncation
// happens LAST so a truncated name may legitimately end in "_".
//
// test_topic_naming.py and cdc_schema_history_topic_test.go assert the two agree.
func debeziumSafeName(s string, maxLen int) string {
	s = strings.ToLower(strings.TrimSpace(s))

	var b strings.Builder
	b.Grow(len(s))
	prevUnderscore := false
	for _, r := range s {
		legal := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-'
		if !legal {
			r = '_'
		}
		// Python does this in two passes ([^a-z0-9_-]+ -> "_", then _+ -> "_"); one
		// pass that collapses every run of underscores, however produced, is the same
		// function.
		if r == '_' {
			if prevUnderscore {
				continue
			}
			prevUnderscore = true
		} else {
			prevUnderscore = false
		}
		b.WriteRune(r)
	}

	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "rsync"
	}
	// Every surviving character is single-byte ASCII, so a byte slice is a character
	// slice — which is what Python's s[:max_len] does.
	if maxLen > 0 && len(out) > maxLen {
		out = out[:maxLen]
	}
	return out
}

// schemaHistoryTopicFor predicts the topic the Debezium connector configures as
// schema.history.internal.kafka.topic for a given connector name.
//
// This exists so the orchestrator can CREATE that topic before the connector starts.
// Nothing else in the repo creates it: the connector sets the property but has no Kafka
// client in its image (requirements.txt is fastapi/uvicorn/httpx), no topic.creation.*
// policy is configured on it, and EnsureAgentControlTopics does not know the name. Until
// now it existed only because the broker auto-created it on first write — a setting this
// platform does not own on a customer-managed cluster, and one that also decides the
// topic's retention and cleanup policy, both of which Debezium is strict about.
//
// Keep in lockstep with connector.py:
//
//	"schema.history.internal.kafka.topic": _qualify_topic(f"schemahistory.{_safe_name(connector_name, 80)}")
func schemaHistoryTopicFor(connectorName string) string {
	return kafkaclient.Topic("schemahistory." + debeziumSafeName(connectorName, 80))
}

// executeStreamingDataTransfer handles CDC/streaming mode via dynamic CDC provider
//
// FULLY GENERIC: Uses CDC provider from plan params (set by Planner's CDCProviderRegistry)
// instead of hardcoding "debezium". Supports future CDC providers like native replication.
func (a *Agent) executeStreamingDataTransfer(ctx context.Context, task ExecutorTask, kafkaTopic string, traceID string) ExecutorResponse {
	log.Infof("Starting CDC streaming pipeline: %s", task.PipelineID)

	normalizeDBType := func(s string) string {
		v := strings.ToLower(strings.TrimSpace(s))
		v = strings.ReplaceAll(v, "-", "_")
		switch v {
		case "postgres":
			return "postgresql"
		case "mariadb":
			return "mysql"
		default:
			return v
		}
	}

	getTablesList := func(v interface{}) []string {
		out := []string{}
		switch tv := v.(type) {
		case []string:
			for _, s := range tv {
				if strings.TrimSpace(s) != "" {
					out = append(out, strings.TrimSpace(s))
				}
			}
		case []interface{}:
			for _, it := range tv {
				s := strings.TrimSpace(fmt.Sprint(it))
				if s != "" {
					out = append(out, s)
				}
			}
		}
		return out
	}

	// ==========================================================================
	// PHASE 1: Get CDC provider from plan (metadata-driven, not hardcoded)
	// ==========================================================================
	cdcProvider := "" // No default, must be provided or derived
	if provider, ok := task.Params["cdc_provider"].(string); ok && provider != "" {
		cdcProvider = provider
	} else {
		// Warning instead of hard error for backward compatibility, but in pure agentic we should fail
		cdcProvider = "debezium"
		log.Warnf("⚠️  No cdc_provider specified, falling back to default: %s", cdcProvider)
	}

	// Get connector class from plan if available (e.g., io.debezium.connector.mysql.MySqlConnector)
	connectorClass := ""
	if class, ok := task.Params["connector_class"].(string); ok {
		connectorClass = class
	}

	// Get source connector type for routing
	if task.Source == nil {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      "missing source config for CDC streaming task",
		}
	}
	sourceConnector := task.Source.Type
	if sc, ok := task.Params["source_connector"].(string); ok && sc != "" {
		sourceConnector = sc
	}

	log.Infof("🔧 Using CDC provider: %s for source: %s", cdcProvider, sourceConnector)

	// -------------------------------------------------------------------------
	// P0 guards: Primary-key validation (DB destinations) + Postgres resource provisioning
	// (Destination MCP pre-warm is handled by the infra_preflight stage in ExecutorWorker
	// before this function is called, so no background goroutine needed here.)
	// -------------------------------------------------------------------------
	sourceConnID := ""
	if task.Params != nil {
		if v, ok := task.Params["source_connection_id"].(string); ok {
			sourceConnID = strings.TrimSpace(v)
		}
	}
	if sourceConnID == "" && task.Payload != nil {
		if v, ok := task.Payload["source_connection_id"].(string); ok {
			sourceConnID = strings.TrimSpace(v)
		}
	}

	destConnector := ""
	if task.Destination != nil {
		destConnector = task.Destination.Type
	}

	tablesForRouting := []string{}
	if task.Params != nil {
		tablesForRouting = getTablesList(task.Params["tables"])
	}
	sourceDBName := ""
	sourceSchemaName := "" // PostgreSQL schema (default "public"); unused for MySQL
	if task.Source != nil && task.Source.Config != nil {
		sourceDBName = strings.TrimSpace(task.Source.Config["database"])
		if sourceDBName == "" {
			sourceDBName = strings.TrimSpace(task.Source.Config["db_name"])
		}
		sourceSchemaName = strings.TrimSpace(task.Source.Config["schema"])
	}
	// Prefer decrypted connection config if db/schema weren't in task config.
	if (strings.TrimSpace(sourceDBName) == "" || strings.TrimSpace(sourceSchemaName) == "") &&
		a.connectionMgr != nil && strings.TrimSpace(sourceConnID) != "" && sourceConnID != "auto" {
		if cfg, err := a.getConnectionConfigForTask(ctx, task, sourceConnID); err == nil && cfg != nil {
			if sourceDBName == "" {
				sourceDBName = strings.TrimSpace(cfg["database"])
				if sourceDBName == "" {
					sourceDBName = strings.TrimSpace(cfg["db_name"])
				}
				if sourceDBName == "" {
					sourceDBName = strings.TrimSpace(cfg["dbname"])
				}
			}
			if sourceSchemaName == "" {
				sourceSchemaName = strings.TrimSpace(cfg["schema"])
			}
		}
	}
	// PostgreSQL default schema is "public", but ONLY use this as the final
	// fallback after both task.Source.Config and the decrypted connection
	// config have been checked. Many production PostgreSQL deployments use
	// non-default schemas (analytics, tenant_*, app_*) and a hardcoded
	// "public" silently routes all PK validation / Debezium config to the
	// wrong schema, causing "table not found" failures.
	if sourceSchemaName == "" {
		sourceSchemaName = "public"
	}

	normalizedSource := normalizeDBType(sourceConnector)
	normalizedDest := normalizeDBType(destConnector)

	// Hard-block: relational destinations require PKs for CDC correctness.
	if normalizedDest == "postgresql" || normalizedDest == "mysql" {
		if strings.TrimSpace(sourceConnID) == "" || sourceConnID == "auto" {
			return ExecutorResponse{
				TaskID:     task.TaskID,
				PipelineID: task.PipelineID,
				Status:     "failed",
				Error:      "source_connection_id is required for CDC PK validation",
			}
		}
		if len(tablesForRouting) == 0 {
			return ExecutorResponse{
				TaskID:     task.TaskID,
				PipelineID: task.PipelineID,
				Status:     "failed",
				Error:      "tables are required for CDC PK validation",
			}
		}

		// Pre-flight: validate the SOURCE server is configured for CDC BEFORE we
		// create the Debezium connector. Without this, a misconfigured source
		// (MySQL binlog_format!=ROW / binlog_row_image!=FULL, PostgreSQL
		// wal_level!=logical, or missing replication grants) is only discovered
		// deep inside the Debezium connector task, which fails opaquely and loops
		// in the healer. We fail fast here with the remediation action so the user
		// can fix the server parameter (ActionEscalate / ActionRequestUserConfig).
		//
		// Only Severity=="error" ValidationErrors block; warnings are logged.
		if prereqErr := a.validateCDCSourcePrerequisites(ctx, normalizedSource, sourceConnID); prereqErr != "" {
			log.Errorf("❌ CDC source prerequisite check failed for pipeline %s: %s", task.PipelineID, prereqErr)
			return ExecutorResponse{
				TaskID:     task.TaskID,
				PipelineID: task.PipelineID,
				Status:     "failed",
				Error:      prereqErr,
			}
		}

		switch normalizedSource {
		case "mysql":
			mgr := cdc.NewMySQLManager(a.db)
			missing, err := mgr.ValidateTablesHavePrimaryKeys(ctx, sourceConnID, sourceDBName, tablesForRouting)
			if err != nil {
				return ExecutorResponse{TaskID: task.TaskID, PipelineID: task.PipelineID, Status: "failed", Error: err.Error()}
			}
			if len(missing) > 0 {
				return ExecutorResponse{
					TaskID:     task.TaskID,
					PipelineID: task.PipelineID,
					Status:     "failed",
					Error:      fmt.Sprintf("CDC requires PRIMARY KEY for DB destinations; missing PK on: %s", strings.Join(missing, ", ")),
				}
			}
		case "postgresql":
			mgr := cdc.NewPostgreSQLManager(a.db)
			// Use the source schema from connection config (default "public") so that
			// non-default schemas (e.g. "myschema.big_table") resolve correctly.
			// tablesForRouting is pre-qualified (e.g. "public.driverb_big") by the
			// normalization block above, so the defaultSchema is only a fallback for
			// any unqualified table name that slipped through.
			missing, err := mgr.ValidateTablesHavePrimaryKeys(ctx, sourceConnID, sourceSchemaName, tablesForRouting)
			if err != nil {
				return ExecutorResponse{TaskID: task.TaskID, PipelineID: task.PipelineID, Status: "failed", Error: err.Error()}
			}
			if len(missing) > 0 {
				return ExecutorResponse{
					TaskID:     task.TaskID,
					PipelineID: task.PipelineID,
					Status:     "failed",
					Error:      fmt.Sprintf("CDC requires PRIMARY KEY for DB destinations; missing PK on: %s", strings.Join(missing, ", ")),
				}
			}
		case "sqlserver":
			mgr := cdc.NewSQLServerManager(a.db)
			// SQL Server namespace is the schema (default "dbo"); tables are
			// pre-qualified (e.g. "dbo.cdc_test") by the normalization block, so
			// the default is only a fallback for any unqualified table name.
			missing, err := mgr.ValidateTablesHavePrimaryKeys(ctx, sourceConnID, sourceSchemaName, tablesForRouting)
			if err != nil {
				return ExecutorResponse{TaskID: task.TaskID, PipelineID: task.PipelineID, Status: "failed", Error: err.Error()}
			}
			if len(missing) > 0 {
				return ExecutorResponse{
					TaskID:     task.TaskID,
					PipelineID: task.PipelineID,
					Status:     "failed",
					Error:      fmt.Sprintf("CDC requires PRIMARY KEY for DB destinations; missing PK on: %s", strings.Join(missing, ", ")),
				}
			}
		case "mongodb":
			// MongoDB: _id is a mandatory, always-present field and is the primary
			// key of the packed destination table (_id + document), so no collection
			// can lack a PK. The manager's ValidateTablesHavePrimaryKeys is a no-op
			// that never blocks; call it through the registry manager for symmetry.
			// Namespace is the MongoDB database (mongo has no schema level).
			mgr := cdc.NewMongoDBManager(a.db)
			missing, err := mgr.ValidateTablesHavePrimaryKeys(ctx, sourceConnID, sourceDBName, tablesForRouting)
			if err != nil {
				return ExecutorResponse{TaskID: task.TaskID, PipelineID: task.PipelineID, Status: "failed", Error: err.Error()}
			}
			if len(missing) > 0 {
				return ExecutorResponse{
					TaskID:     task.TaskID,
					PipelineID: task.PipelineID,
					Status:     "failed",
					Error:      fmt.Sprintf("CDC requires PRIMARY KEY for DB destinations; missing PK on: %s", strings.Join(missing, ", ")),
				}
			}
		case "oracle":
			mgr := cdc.NewOracleManager(a.db)
			// Oracle namespace is the schema/owner (uppercase); tables are
			// pre-qualified by the normalization block, so the default is only a
			// fallback for any unqualified table name.
			missing, err := mgr.ValidateTablesHavePrimaryKeys(ctx, sourceConnID, sourceSchemaName, tablesForRouting)
			if err != nil {
				return ExecutorResponse{TaskID: task.TaskID, PipelineID: task.PipelineID, Status: "failed", Error: err.Error()}
			}
			if len(missing) > 0 {
				return ExecutorResponse{
					TaskID:     task.TaskID,
					PipelineID: task.PipelineID,
					Status:     "failed",
					Error:      fmt.Sprintf("CDC requires PRIMARY KEY for DB destinations; missing PK on: %s", strings.Join(missing, ", ")),
				}
			}
		default:
			// If we don't know how to validate, fail closed for DB destinations.
			return ExecutorResponse{
				TaskID:     task.TaskID,
				PipelineID: task.PipelineID,
				Status:     "failed",
				Error:      fmt.Sprintf("unsupported CDC source for PK validation: %s", sourceConnector),
			}
		}
	}

	// Step 1: Start CDC provider connector (Debezium via Kafka Connect)
	// IMPORTANT: We consume from the topic returned by the CDC provider (Debezium topic.prefix.db.table),
	// not the generic pre-provisioned topic (unless/until we add SMT routing).
	cdcMode := ""
	if task.Params != nil {
		if v, ok := task.Params["cdc_mode"].(string); ok {
			cdcMode = strings.ToLower(strings.TrimSpace(v))
		}
	}
	// Hybrid CDC handoff pins a schema-recovery mode (via task.Params) so Debezium
	// REBUILDS schema history from the live DB and RESUMES from the seeded position
	// instead of re-snapshotting. That per-run handoff mode MUST win over the
	// DB-persisted pipeline cdc_mode (the user's initial/streaming_only choice).
	// Without this, MySQL hybrid runs skip the snapshot (seeded offset) yet never build
	// schema history → "The db history topic is missing". The debezium MCP maps these
	// aliases to Debezium 'recovery' (MySQL) / 'no_data' (PG).
	isHybridRecovery := false
	switch cdcMode {
	case "schema_recovery", "schema-recovery", "schema_only_recovery", "recovery", "no_data":
		isHybridRecovery = true
	}
	// Prefer DB-persisted cdc_mode when available (authoritative, matches UI selection)
	// — unless the hybrid handoff pinned a schema-recovery mode above.
	if !isHybridRecovery && a.db != nil {
		_, cm := loadPipelineModes(ctx, a.db, task.PipelineID)
		if strings.TrimSpace(cm) != "" {
			cdcMode = strings.ToLower(strings.TrimSpace(cm))
		}
	}
	// Normalize/validate.
	switch cdcMode {
	case "", "auto":
		cdcMode = "initial"
	case "initial", "streaming_only", "never",
		"schema_recovery", "schema-recovery", "schema_only_recovery", "recovery", "no_data":
		// pass through; debezium MCP _map_snapshot_mode normalizes per DB type.
	default:
		cdcMode = "initial"
	}

	// Use the resolved tables list (selected_tables OR tables) rather than assuming task.Params["tables"] exists.
	tablesArg := interface{}(nil)
	if len(tablesForRouting) > 0 {
		tablesArg = tablesForRouting
	} else if task.Params != nil {
		if v, ok := task.Params["tables"]; ok && v != nil {
			tablesArg = v
		} else if v, ok := task.Params["selected_tables"]; ok && v != nil {
			tablesArg = v
		}
	}

	// ── Snapshot strategy (size-gated) ───────────────────────────────────────
	// For large, PK'd PostgreSQL CDC pipelines, backfill history with a lock-free,
	// resumable INCREMENTAL snapshot that runs concurrently with streaming (no long
	// transaction, no xmin/WAL pin) instead of the blocking initial snapshot. Small
	// pipelines and non-PG sources keep the blocking snapshot. Any uncertainty (can't
	// measure size/PKs, discovery fails) → blocking (safe, == today's behavior). Only
	// relevant when an initial snapshot was going to run at all (cdc_mode=initial).
	snapshotStrategy := snapshotStrategyBlocking
	if cdcMode == "initial" && hybridIsPostgresFamily(sourceConnector) {
		est := a.estimateCDCSourceSize(ctx, task, tablesForRouting)
		threshold := incrementalSnapshotRowThreshold()
		if est.measured {
			snapshotStrategy = computeCDCSnapshotStrategy(sourceConnector, est.totalRows, threshold, est.allHavePK)
		}
		// Always log the decision — including WHY incremental was skipped — so a
		// non-resumable blocking snapshot on a large multi-table load is never silent.
		fields := log.Fields{
			"pipeline_id": task.PipelineID,
			"strategy":    snapshotStrategy,
			"measured":    est.measured,
			"est_rows":    est.totalRows,
			"threshold":   threshold,
			"all_pk":      est.allHavePK,
			"matched":     est.matched,
		}
		if len(est.noPKTables) > 0 {
			fields["tables_without_pk"] = est.noPKTables
		}
		if len(est.unmatched) > 0 {
			fields["tables_unmatched"] = est.unmatched
		}
		if snapshotStrategy == snapshotStrategyIncremental {
			log.WithFields(fields).Info("📸 CDC snapshot strategy = incremental (resumable, size-gated)")
		} else {
			fields["blocking_reason"] = cdcBlockingReason(est, threshold)
			log.WithFields(fields).Warn("📸 CDC snapshot strategy = blocking (non-resumable) — incremental not used; see blocking_reason")
		}
	}
	incrementalSignalTopic := ""
	if snapshotStrategy == snapshotStrategyIncremental {
		incrementalSignalTopic = kafkaclient.Topic(fmt.Sprintf("signals.%s", utils.SafeID8(task.PipelineID)))
	}

	params := map[string]interface{}{
		"connector_name": fmt.Sprintf("cdc-%s", utils.SafeID8(task.PipelineID)),
		"database_type":  sourceConnector,
		// Debezium MCP maps these values to Debezium snapshot.mode (e.g. "streaming_only" -> "no_data").
		"cdc_mode":      cdcMode,
		"snapshot_mode": cdcMode,
		"tables":        tablesArg,
	}
	// Incremental strategy: the Debezium MCP overrides snapshot.mode=no_data and wires the
	// Kafka signal channel; the orchestrator sends the execute-snapshot signal below, once
	// the connector task is RUNNING.
	if snapshotStrategy == snapshotStrategyIncremental {
		params["snapshot_strategy"] = "incremental"
		params["signal_kafka_topic"] = incrementalSignalTopic
	}
	log.WithFields(log.Fields{
		"pipeline_id":       task.PipelineID,
		"cdc_mode":          cdcMode,
		"snapshot_mode":     cdcMode,
		"snapshot_strategy": snapshotStrategy,
	}).Info("CDC Step 1: starting Debezium connector (snapshot.mode derived from cdc_mode by the Debezium MCP; streaming_only → no_data)")

	// -------------------------------------------------------------------------
	// Kafka topic strategy (CDC): HYBRID routing (dimensions unified, facts per-table)
	//
	// - Debezium default is per-table topic: <topic.prefix>.<db_or_schema>.<table>
	// - For "dimension" tables we route (via RegexRouter SMT) into a single unified topic:
	//     <topic.prefix>.dimensions
	//
	// This reduces topic explosion for many small tables while keeping high-volume tables isolated.
	// Heuristic: last segment looks like a dimension/lookup table (name prefix).
	// -------------------------------------------------------------------------
	isDimensionTable := func(t string) bool {
		s := strings.TrimSpace(t)
		if s == "" {
			return false
		}
		// Expect qualified "db.table" or "schema.table"; take last segment.
		seg := s
		if i := strings.LastIndex(seg, "."); i >= 0 && i+1 < len(seg) {
			seg = seg[i+1:]
		}
		seg = strings.ToLower(strings.TrimSpace(seg))
		return strings.HasPrefix(seg, "dim_") ||
			strings.HasPrefix(seg, "dimension_") ||
			strings.HasPrefix(seg, "lookup_") ||
			strings.HasPrefix(seg, "lkp_") ||
			strings.HasPrefix(seg, "ref_")
	}
	dimCount := 0
	for _, t := range tablesForRouting {
		if isDimensionTable(t) {
			dimCount++
		}
	}
	if dimCount > 0 {
		// Debezium MCP uses args.topic_prefix (if set) else args.connector_name.
		topicPrefix := ""
		if task.Params != nil {
			if v, ok := task.Params["topic_prefix"].(string); ok {
				topicPrefix = strings.TrimSpace(v)
			}
		}
		if topicPrefix == "" {
			// Same prediction as resolveCDCStreamTopic: the connector qualifies
			// its topic.prefix, so the bare connector name is not what Debezium
			// writes to.
			topicPrefix = kafkaclient.Topic(strings.TrimSpace(fmt.Sprint(params["connector_name"])))
		}
		if topicPrefix != "" {
			unifiedTopic := topicPrefix + ".dimensions"
			// Expose for sink subscription calculation
			if task.Params == nil {
				task.Params = map[string]interface{}{}
			}
			task.Params["cdc_unified_topic"] = unifiedTopic

			// Merge any caller-provided overrides with our routing transform.
			overrides := map[string]interface{}{}
			if task.Params != nil {
				if ov, ok := task.Params["connector_config_overrides"].(map[string]interface{}); ok && ov != nil {
					for k, v := range ov {
						overrides[k] = v
					}
				}
				if ov, ok := task.Params["config_overrides"].(map[string]interface{}); ok && ov != nil {
					for k, v := range ov {
						overrides[k] = v
					}
				}
			}
			// Route only dimension-like tables into the unified topic.
			// Preserve any existing SMT chain by appending routeDims.
			existingTransforms := strings.TrimSpace(fmt.Sprint(overrides["transforms"]))
			if existingTransforms == "" {
				overrides["transforms"] = "routeDims"
			} else {
				parts := strings.Split(existingTransforms, ",")
				has := false
				for _, p := range parts {
					if strings.TrimSpace(p) == "routeDims" {
						has = true
						break
					}
				}
				if !has {
					overrides["transforms"] = existingTransforms + ",routeDims"
				}
			}
			overrides["transforms.routeDims.type"] = "org.apache.kafka.connect.transforms.RegexRouter"
			overrides["transforms.routeDims.regex"] = fmt.Sprintf(
				"^%s\\.[^.]+\\.(dim_|dimension_|lookup_|lkp_|ref_).*$",
				regexp.QuoteMeta(topicPrefix),
			)
			overrides["transforms.routeDims.replacement"] = unifiedTopic
			params["connector_config_overrides"] = overrides
		}
	}
	// If planner provides connector_class, pass through; Debezium MCP validates/normalizes.
	if strings.TrimSpace(connectorClass) != "" {
		params["connector_class"] = connectorClass
	}
	// ─────────────────────────────────────────────────────────────────────────
	// P0: PostgreSQL CDC ORDERING INVARIANT
	//
	// The publication MUST be created before the replication slot.
	// If the order is reversed, pgoutput decodes the first WAL batch
	// without knowing which tables are included — silent data loss.
	//
	// This block enforces the invariant for EVERY PostgreSQL-family source:
	//   1. Call ProvisionResources() → creates publication first, slot second.
	//   2. Pass the exact provisioned names to Debezium.
	//   3. Set publication_autocreate_mode = "disabled" so Debezium NEVER
	//      tries to create its own resources (it would create slot first).
	//
	// When adding a new PostgreSQL-compatible source (CockroachDB, Aurora PG,
	// AlloyDB, Neon, Supabase …), add its normalised name to the isPostgresFamily
	// check below. Never rely on Debezium's autocreate modes for these sources.
	// ─────────────────────────────────────────────────────────────────────────
	isPostgresFamily := func(t string) bool {
		// normalizeDBType lower-cases and replaces "-" with "_".
		switch normalizeDBType(t) {
		case "postgresql",
			"cockroachdb", "cockroach_db", // CockroachDB
			"aurora_postgresql", // AWS Aurora PostgreSQL
			"alloydb",           // GCP AlloyDB
			"neon",              // Neon serverless Postgres
			"supabase":          // Supabase (Postgres under the hood)
			return true
		}
		return false
	}
	if isPostgresFamily(sourceConnector) {
		if strings.TrimSpace(sourceConnID) == "" || sourceConnID == "auto" {
			return ExecutorResponse{
				TaskID:     task.TaskID,
				PipelineID: task.PipelineID,
				Status:     "failed",
				Error:      "source_connection_id is required for postgresql CDC provisioning",
			}
		}
		cfg := cdc.CDCResourceConfig{
			PipelineID:   task.PipelineID,
			ConnectionID: sourceConnID,
			DatabaseType: "postgresql",
			Database:     sourceDBName,
		}
		pgMgr := cdc.NewPostgreSQLManager(a.db)
		if _, err := pgMgr.ProvisionResources(ctx, cfg, tablesForRouting); err != nil {
			return ExecutorResponse{TaskID: task.TaskID, PipelineID: task.PipelineID, Status: "failed", Error: fmt.Sprintf("failed to provision postgresql CDC resources: %v", err)}
		}

		params["publication_name"] = cdc.GenerateResourceName(cfg, "publication")
		params["slot_name"] = cdc.GenerateResourceName(cfg, "replication_slot")
		params["publication_autocreate_mode"] = "disabled" // MUST be disabled — orchestrator pre-provisions
		params["plugin_name"] = "pgoutput"
		// Pass the schema explicitly so Debezium doesn't have to infer it from
		// the first table name. Belt-and-suspenders: the table names are already
		// qualified (e.g. "public.driverb_big"), but the Debezium MCP connector
		// also reads args["schema"] as a fallback for unqualified names.
		params["schema"] = sourceSchemaName
	} else if normalizeDBType(sourceConnector) == "sqlserver" {
		// SQL Server CDC requires a per-table capture instance (sys.sp_cdc_enable_table)
		// before Debezium can stream. Unlike MySQL (binlog) and MongoDB (change streams),
		// which need no per-table provisioning, SQL Server produces NO change tables until
		// CDC is enabled — so without this the initial snapshot lands (base-table read) but
		// streaming captures nothing (silent no-op). Provision is idempotent per capture
		// instance, so a rerun is safe. See CAPABILITIES.md — SQL Server CDC.
		if strings.TrimSpace(sourceConnID) == "" || sourceConnID == "auto" {
			return ExecutorResponse{
				TaskID:     task.TaskID,
				PipelineID: task.PipelineID,
				Status:     "failed",
				Error:      "source_connection_id is required for sqlserver CDC provisioning",
			}
		}
		cfg := cdc.CDCResourceConfig{
			PipelineID:   task.PipelineID,
			ConnectionID: sourceConnID,
			DatabaseType: "sqlserver",
			Database:     sourceDBName,
		}
		ssMgr := cdc.NewSQLServerManager(a.db)
		if _, err := ssMgr.ProvisionResources(ctx, cfg, tablesForRouting); err != nil {
			return ExecutorResponse{TaskID: task.TaskID, PipelineID: task.PipelineID, Status: "failed", Error: fmt.Sprintf("failed to provision sqlserver CDC resources: %v", err)}
		}
	} else if normalizeDBType(sourceConnector) == "oracle" {
		// Oracle CDC (Debezium LogMiner) needs per-table supplemental logging
		// (ALL COLUMNS) so UPDATE/DELETE change events carry full before-images —
		// the analog of SQL Server's per-table capture instance. Without it the
		// snapshot lands but streaming UPDATE/DELETE arrive without before-images.
		// DBA-only prerequisites (ARCHIVELOG, DB-level supplemental logging,
		// LogMiner grants) are checked by validateCDCSourcePrerequisites above and
		// escalate on failure. Oracle is NOT postgres-family: no publication/slot,
		// so no publication_autocreate_mode is set. Provision is idempotent.
		if strings.TrimSpace(sourceConnID) == "" || sourceConnID == "auto" {
			return ExecutorResponse{
				TaskID:     task.TaskID,
				PipelineID: task.PipelineID,
				Status:     "failed",
				Error:      "source_connection_id is required for oracle CDC provisioning",
			}
		}
		cfg := cdc.CDCResourceConfig{
			PipelineID:   task.PipelineID,
			ConnectionID: sourceConnID,
			DatabaseType: "oracle",
			Database:     sourceDBName,
		}
		oraMgr := cdc.NewOracleManager(a.db)
		if _, err := oraMgr.ProvisionResources(ctx, cfg, tablesForRouting); err != nil {
			return ExecutorResponse{TaskID: task.TaskID, PipelineID: task.PipelineID, Status: "failed", Error: fmt.Sprintf("failed to provision oracle CDC resources: %v", err)}
		}
		// Forward the multitenant PDB name so the Debezium Oracle connector sets
		// database.pdb.name (no-op for non-CDB / single-tenant Oracle).
		if task.Source != nil && task.Source.Config != nil {
			if v := task.Source.Config["oracle_pdb_name"]; v != "" {
				params["oracle_pdb_name"] = v
			} else if v := task.Source.Config["pdb_name"]; v != "" {
				params["oracle_pdb_name"] = v
			}
		}
	}
	// Database creds (Debezium MCP expects db_* keys)
	if task.Source != nil && task.Source.Config != nil {
		if v := task.Source.Config["host"]; v != "" {
			params["db_host"] = v
		}
		if v := task.Source.Config["port"]; v != "" {
			params["db_port"] = v
		}
		if v := task.Source.Config["user"]; v != "" {
			params["db_user"] = v
		} else if v := task.Source.Config["username"]; v != "" {
			params["db_user"] = v
		}
		if v := task.Source.Config["password"]; v != "" {
			params["db_password"] = v
		}
		if v := task.Source.Config["database"]; v != "" {
			params["db_name"] = v
		} else if v := task.Source.Config["db_name"]; v != "" {
			params["db_name"] = v
		}
		// SSL mode for the Debezium source connector. Stored DB connections
		// persist only host/port/user/password/db — no sslmode — so without a
		// host-aware default Debezium connects to a managed source (Azure/RDS)
		// without TLS and is rejected ("no pg_hba.conf entry … no encryption").
		// An explicit ssl_mode/sslmode wins; otherwise default by host: remote →
		// "require", local/docker → left unset (connector default). cdc.Resolve
		// PostgresSSLMode normalises the value; for a MySQL source the Debezium
		// MCP maps "require" onto database.ssl.mode itself.
		sslSrc := map[string]interface{}{}
		if v := task.Source.Config["sslmode"]; v != "" {
			sslSrc["sslmode"] = v
		}
		if v := task.Source.Config["ssl_mode"]; v != "" {
			sslSrc["ssl_mode"] = v
		}
		if len(sslSrc) > 0 || !cdc.IsLocalDBHost(task.Source.Config["host"]) {
			params["sslmode"] = cdc.ResolvePostgresSSLMode(sslSrc, task.Source.Config["host"])
		}
		// MongoDB-specific source hints. The Debezium MongoDB branch builds
		// mongodb://…?replicaSet=…&authSource=… from these (or uses an explicit
		// connection_string verbatim — required for Atlas mongodb+srv://). Without
		// the replica set the Mongo driver connects in single-server topology and
		// change streams fail ("not a replica set"). Relational Debezium branches
		// ignore these keys, so forwarding them unconditionally is safe.
		if v := task.Source.Config["replica_set"]; v != "" {
			params["replica_set"] = v
		} else if v := task.Source.Config["replicaSet"]; v != "" {
			params["replica_set"] = v
		}
		if v := task.Source.Config["connection_string"]; v != "" {
			params["connection_string"] = v
		} else if v := task.Source.Config["mongodb_connection_string"]; v != "" {
			params["connection_string"] = v
		}
		if v := task.Source.Config["auth_source"]; v != "" {
			params["auth_source"] = v
		}
	}

	// Pre-flight: verify kafka-connect is reachable before calling Debezium MCP.
	// Debezium MCP calls kafka-connect REST API; if kafka-connect is down, the MCP
	// returns a raw DNS/connection error that's hard to diagnose from the UI.
	// We retry up to 3 times with 5s backoff to tolerate kafka-connect mid-restart
	// (Docker's restart: unless-stopped means it recovers automatically after crashes).
	kafkaConnectURL := strings.TrimRight(os.Getenv("KAFKA_CONNECT_URL"), "/")
	if kafkaConnectURL == "" {
		kafkaConnectURL = "http://kafka-connect:8083"
	}
	{
		const maxAttempts = 3
		const retryDelay = 5 * time.Second
		var lastErr error
		available := false
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			checkCtx, checkCancel := context.WithTimeout(ctx, 5*time.Second)
			req, reqErr := http.NewRequestWithContext(checkCtx, http.MethodGet, kafkaConnectURL+"/", nil)
			if reqErr != nil {
				checkCancel()
				break
			}
			resp, doErr := http.DefaultClient.Do(req)
			checkCancel()
			if doErr == nil && resp != nil && resp.StatusCode < 500 {
				resp.Body.Close()
				available = true
				break
			}
			if resp != nil {
				resp.Body.Close()
			}
			lastErr = doErr
			if attempt < maxAttempts {
				log.WithFields(log.Fields{
					"pipeline_id":       task.PipelineID,
					"kafka_connect_url": kafkaConnectURL,
					"attempt":           attempt,
					"max_attempts":      maxAttempts,
				}).Warnf("⏳ CDC pre-flight: kafka-connect not ready, retrying in %s...", retryDelay)
				select {
				case <-ctx.Done():
				case <-time.After(retryDelay):
				}
			}
		}
		if !available {
			errMsg := "Kafka Connect (Debezium) is not running or not reachable"
			if lastErr != nil {
				errMsg = fmt.Sprintf("%s: %v", errMsg, lastErr)
			}
			log.WithFields(log.Fields{
				"pipeline_id":       task.PipelineID,
				"kafka_connect_url": kafkaConnectURL,
				"cdc_provider":      cdcProvider,
			}).Error("❌ CDC pre-flight: kafka-connect unavailable after retries")
			return ExecutorResponse{
				TaskID:     task.TaskID,
				PipelineID: task.PipelineID,
				Status:     "failed",
				Error:      "connect_unavailable",
				Result: map[string]interface{}{
					"error_code":        "connect_unavailable",
					"connect_available": false,
					"message":           errMsg,
				},
			}
		}
	}

	// Pre-create the Debezium SCHEMA HISTORY topic before the connector starts.
	//
	// Nothing else in this repo creates it. The connector configures
	// schema.history.internal.kafka.topic and then relies on Kafka Connect writing to
	// it, which only works while the broker auto-creates topics — a setting this
	// platform does not own on a customer-managed cluster. The geometry below is not a
	// preference:
	//
	//   1 partition      Debezium replays this topic in order to rebuild the source
	//                    DDL; more than one partition has no total order and the
	//                    connector refuses to start against it.
	//   cleanup.policy   MUST be "delete", never "compact". The records are not keyed
	//                    per schema object, so compaction drops DDL entries that are
	//                    still needed.
	//   retention.ms=-1  Retain forever. The default (7 days on the bundled broker)
	//                    silently expires the history, and the connector then fails on
	//                    its FIRST RESTART rather than on first run — days later, with
	//                    an error that names nothing about retention.
	//
	// This must run BEFORE start_sync: after the connector is up, Connect writes the
	// history itself and whatever the topic was born with is already fixed.
	// cdc_schema_history_topic_test.go pins that ordering.
	//
	// Best-effort, matching the other two pre-creation sites in this file: a broker that
	// still auto-creates behaves exactly as it did before.
	shTopic := schemaHistoryTopicFor(fmt.Sprint(params["connector_name"]))
	if a.kafkaManager != nil && strings.TrimSpace(shTopic) != "" {
		if err := a.kafkaManager.EnsureTopicExistsWithConfig(shTopic, 1, map[string]string{
			"cleanup.policy": "delete",
			"retention.ms":   "-1",
		}); err != nil {
			log.WithError(err).WithField("topic", shTopic).
				Warn("⚠️  Could not pre-create the Debezium schema-history topic — if the broker auto-creates it, it will inherit the broker's retention and cleanup policy, and the connector will fail on a later RESTART rather than now")
		}
	}
	// Tell the connector the name rather than letting it re-derive one. Two independent
	// implementations of the same naming rule that disagree would have the orchestrator
	// create one topic and Connect write to another — a connector that works until its
	// first restart.
	params["schema_history_topic"] = shTopic

	cdcReq := mcp.ExecuteRequest{
		Connector: cdcProvider,
		Operation: "start_sync",
		Config:    map[string]string{}, // Debezium MCP uses env/default for Kafka Connect URL
		Params:    params,
	}

	startResp, err := a.executeWithRetry(ctx, cdcReq)
	if err != nil {
		// In CDC mode, do not silently downgrade to batch. This would violate user intent
		// and makes CDC table stats appear "broken" in the UI.
		log.WithFields(log.Fields{
			"pipeline_id":  task.PipelineID,
			"cdc_provider": cdcProvider,
			"source":       sourceConnector,
			"error":        err.Error(),
			"sync_mode":    "cdc",
		}).Error("❌ CDC provider start failed")
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      fmt.Sprintf("%s start failed: %v", cdcProvider, err),
		}
	}
	if startResp == nil || !startResp.Success {
		msg := ""
		if startResp != nil {
			msg = startResp.Error
		}
		log.WithFields(log.Fields{
			"pipeline_id":  task.PipelineID,
			"cdc_provider": cdcProvider,
			"source":       sourceConnector,
			"error":        msg,
			"sync_mode":    "cdc",
		}).Error("❌ CDC provider start returned failure")
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      fmt.Sprintf("%s start failed: %s", cdcProvider, msg),
		}
	}

	// Resolve the Debezium connector name (for the runtime dependency registered
	// after the execution id is known, below).
	debeziumConnName := fmt.Sprintf("cdc-%s", utils.SafeID8(task.PipelineID))
	if startResp.Result != nil {
		if cn, ok := startResp.Result["connector_name"].(string); ok && strings.TrimSpace(cn) != "" {
			debeziumConnName = strings.TrimSpace(cn)
		} else if nested, ok := startResp.Result["result"].(map[string]interface{}); ok {
			if cn, ok := nested["connector_name"].(string); ok && strings.TrimSpace(cn) != "" {
				debeziumConnName = strings.TrimSpace(cn)
			}
		}
	}

	// The CDC streaming sink must consume the Debezium per-table topics
	// (<topic.prefix>.<db>.<table>), NOT the plan-level pre-provisioned
	// "pipeline.<id>.data" batch topic (only the batch / hybrid historical-load
	// plane produces there). Passing the batch topic to startKafkaMCPSink trips
	// its "pipeline."-prefix batch-backfill guard, which disables the per-table
	// fan-out and leaves the sink subscribed to an empty topic — 0 rows to the
	// destination while every component reports healthy. resolveCDCStreamTopic
	// prefers the provider-reported first-table topic and otherwise falls back to
	// the Debezium topic prefix (== connector name), never a "pipeline." topic.
	cdcTopic := resolveCDCStreamTopic(debeziumConnName, startResp.Result)

	// Step 2: Start Kafka-MCP-Sink to consume and write to destination
	streamExecID := ""
	if task.Params != nil {
		if v, ok := task.Params["execution_id"].(string); ok {
			streamExecID = strings.TrimSpace(v)
		}
	}
	if streamExecID == "" && task.Payload != nil {
		if v, ok := task.Payload["execution_id"].(string); ok {
			streamExecID = strings.TrimSpace(v)
		}
	}

	// Register the Debezium connector as a runtime dependency so the health panel
	// and the LLM Diagnose evidence include CDC source-stream health (connector +
	// tasks RUNNING), not just the source/dest MCPs. Keyed by the same execution
	// id as the sink/MCP deps so the idempotent upsert dedupes (a NULL execution
	// id would not — Postgres treats NULLs as distinct in the unique constraint).
	upsertDependency(
		a.db,
		task.PipelineID,
		streamExecID,
		"debezium_task",
		debeziumConnName,
		[]string{"syncing", "streaming"},
		map[string]interface{}{
			"cdc_provider": cdcProvider,
			"source":       sourceConnector,
		},
	)

	sinkResult := a.startKafkaMCPSink(ctx, task, cdcTopic, streamExecID, traceID)
	if !sinkResult.Success {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      fmt.Sprintf("Failed to start sink: %s", sinkResult.Error),
		}
	}

	// Incremental snapshot: now that the connector + sink are up, trigger the chunked
	// historical backfill over the Kafka signal channel (the connector was started with
	// snapshot.mode=no_data). triggerIncrementalSnapshot waits for the task to reach
	// RUNNING first so the signal isn't dropped. This signal is the ONLY historical-backfill
	// path for the incremental strategy, so a failure means history would be silently
	// missing — fail the run loudly rather than reporting "running" (mirrors the sink-start
	// failure branch above). The connector reports whether it configured this.
	if inc, _ := startResp.Result["incremental_snapshot"].(bool); inc {
		sigTopic := incrementalSignalTopic
		if v, _ := startResp.Result["signal_topic"].(string); strings.TrimSpace(v) != "" {
			sigTopic = strings.TrimSpace(v)
		}
		dcs := stringSliceFromResult(startResp.Result["data_collections"])
		if len(dcs) == 0 {
			dcs = tablesForRouting
		}
		if err := a.triggerIncrementalSnapshot(ctx, task, debeziumConnName, sigTopic, dcs); err != nil {
			log.WithError(err).WithField("pipeline_id", task.PipelineID).
				Error("❌ CDC incremental snapshot signal failed — historical backfill will NOT run; failing the run")
			return ExecutorResponse{
				TaskID:     task.TaskID,
				PipelineID: task.PipelineID,
				Status:     "failed",
				Error:      fmt.Sprintf("CDC started but historical backfill (incremental snapshot) could not be triggered — history would be silently missing: %v", err),
			}
		}
	}

	log.Infof("✅ CDC pipeline started - %s → Kafka (%s) → Sink → %s", cdcProvider, cdcTopic, task.Destination.Type)

	return ExecutorResponse{
		TaskID:       task.TaskID,
		PipelineID:   task.PipelineID,
		Status:       "running",
		PipelineType: "streaming",
		// IMPORTANT: for CDC, the actual stream topic is the Debezium topic (topic.prefix.db.table),
		// not the plan-level "topic_provisioning" topic (unless we add SMT routing).
		KafkaTopic:    cdcTopic,
		ConnectorName: fmt.Sprintf("cdc-%s", utils.SafeID8(task.PipelineID)),
		Result: map[string]interface{}{
			"message":      "CDC streaming pipeline started",
			"kafka_topic":  cdcTopic,
			"cdc_provider": cdcProvider,
			"mode":         "streaming",
		},
	}
}

// validateCDCSourcePrerequisites runs the per-DB CDC server-config pre-flight
// (MySQLManager/PostgreSQLManager.ValidatePrerequisites) and returns a single
// aggregated, actionable error string if any blocking (Severity=="error")
// ValidationError is found. It returns "" when the source is ready for CDC.
//
// Blocking checks: MySQL binlog_format=ROW, binlog_row_image=FULL, REPLICATION
// CLIENT+SLAVE grants; PostgreSQL wal_level=logical, REPLICATION privilege.
// Warnings are logged and never block. The returned message embeds the source
// config keywords (binlog / wal_level) so the CDC healer's RuleBasedDiagnoser
// classifies it as ActionEscalate rather than a transient backoff-retry.
func (a *Agent) validateCDCSourcePrerequisites(ctx context.Context, normalizedSource, sourceConnID string) string {
	if a.db == nil || strings.TrimSpace(sourceConnID) == "" || sourceConnID == "auto" {
		// No connection to validate against; PK validation below already fails
		// closed for missing source_connection_id, so don't double-block here.
		return ""
	}

	var (
		vErrs []cdc.ValidationError
		err   error
	)
	switch normalizedSource {
	case "mysql":
		vErrs, err = cdc.NewMySQLManager(a.db).ValidatePrerequisites(ctx, sourceConnID)
	case "postgresql":
		vErrs, err = cdc.NewPostgreSQLManager(a.db).ValidatePrerequisites(ctx, sourceConnID)
	case "sqlserver":
		vErrs, err = cdc.NewSQLServerManager(a.db).ValidatePrerequisites(ctx, sourceConnID)
	case "mongodb":
		// MongoDB defers the replica-set/privilege check to Debezium connector
		// start (dependency-free provider); ValidatePrerequisites returns no
		// blocking errors. Wire it so mongodb is a known source, not the fail-
		// closed default.
		vErrs, err = cdc.NewMongoDBManager(a.db).ValidatePrerequisites(ctx, sourceConnID)
	case "oracle":
		// Oracle LogMiner CDC pre-flight: ARCHIVELOG mode, DB-level supplemental
		// logging, and LogMiner V$ access. All are DBA-only (and ARCHIVELOG needs
		// an instance restart), so a failure blocks with an ActionEscalate-worthy
		// message rather than a silent retry (the codes/keywords — ARCHIVELOG,
		// supplemental log, logminer — are matched by the healer's diagnoser).
		vErrs, err = cdc.NewOracleManager(a.db).ValidatePrerequisites(ctx, sourceConnID)
	default:
		// Unknown source types are handled (fail-closed) by the PK switch below.
		return ""
	}
	if err != nil {
		// Could not reach the source to validate. Do NOT block on a transient
		// connectivity error here — Debezium connection attempts and the healer
		// will surface/retry connectivity issues. Log and continue.
		log.Warnf("CDC source prerequisite check could not run for %s conn %s: %v", normalizedSource, sourceConnID, err)
		return ""
	}

	var blocking []string
	for _, ve := range vErrs {
		if strings.EqualFold(ve.Severity, "error") {
			blocking = append(blocking, fmt.Sprintf("[%s] %s — fix: %s", ve.Code, ve.Message, ve.Action))
		} else {
			log.Warnf("CDC source prerequisite warning (%s): %s", ve.Code, ve.Message)
		}
	}
	if len(blocking) == 0 {
		return ""
	}
	return fmt.Sprintf("CDC source (%s) is not configured for change data capture. Resolve the following on the source server before starting this pipeline: %s",
		normalizedSource, strings.Join(blocking, "; "))
}

// Default policy: claim-check (MinIO staging) is the default for batch export payloads.
// We only inline to Kafka for small payloads to reduce staging overhead.
// Override via KAFKA_INLINE_MAX_BYTES.
const DefaultKafkaInlineMaxBytes int64 = 256 * 1024 // 256KB

func kafkaInlineMaxBytes() int64 {
	raw := strings.TrimSpace(os.Getenv("KAFKA_INLINE_MAX_BYTES"))
	if raw == "" {
		return DefaultKafkaInlineMaxBytes
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err == nil && n > 0 {
		return n
	}
	return DefaultKafkaInlineMaxBytes
}

// isSourcePermissionError reports whether an export error is a source-side
// PERMISSION/authorization wall (a missing OAuth scope or an object the app
// isn't approved for) as opposed to a transient or structural failure. Such a
// table can be safely skipped in a multi-table batch — retrying won't help and
// the user simply lacks access — whereas network/schema/write errors must still
// fail the run. Kept deliberately narrow so it never masks a real failure.
func isSourcePermissionError(s string) bool {
	l := strings.ToLower(s)
	return strings.Contains(l, "access denied") ||
		strings.Contains(l, "not approved to access") ||
		strings.Contains(l, "required access:") ||
		strings.Contains(l, "protected customer data") ||
		strings.Contains(l, "insufficient scope") ||
		strings.Contains(l, "requires merchant approval")
}

// maxCursorValue returns the larger of two keyset cursor values, used to track
// the PK high-water across incremental sweeps (INCREMENTAL.md §5).
//
// Cursor values arrive as whatever the connector put in JSON — float64 for
// numeric PKs, string for UUID/text PKs, occasionally json.Number. Comparison
// only happens within a type family; a mixed or unrecognized pair falls back to
// `b` (the newer value), which is exactly the pre-existing behavior of carrying
// the last cursor forward. That fallback is safe in the direction that matters:
// this value only ever widens a delta predicate, so being slightly low costs a
// redundant re-read (the destination upserts) while nothing is skipped.
func maxCursorValue(a, b interface{}) interface{} {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	af, aok := cursorAsFloat(a)
	bf, bok := cursorAsFloat(b)
	if aok && bok {
		if af >= bf {
			return a
		}
		return b
	}
	as, aok := a.(string)
	bs, bok := b.(string)
	if aok && bok {
		if as >= bs {
			return a
		}
		return b
	}
	return b
}

// cursorAsFloat coerces a JSON-decoded cursor value to a float64 when it is
// numeric. Reports false for anything else so maxCursorValue can fall through
// to string comparison.
func cursorAsFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}

// resolveDestTableName computes the table identifier the sink should write to,
// enforcing the "destination-driven ONLY, never the source schema" rule for
// relational destinations.
//
//   - destinationNamespace set (HITL): qualify the BARE source table with the
//     namespace → "<namespace>.<table>" so the sink routes to the chosen schema.
//   - destinationNamespace empty + relational dest: return the BARE table name so
//     the destination connector resolves its own configured database/public schema.
//     Keeping the source-qualified name here leaked the SOURCE schema into the
//     destination: a PostgreSQL source schema (e.g. "rsync_public.customers") has a
//     dot, but its prefix is a SCHEMA, not the destination DB name, so the sink's
//     normalizeTargetTable does NOT strip it and the connector writes to
//     "rsync_public.customers" at the destination — leaving the expected
//     public.customers empty (the read>0/landed=0 silent drop).
//   - object storage: preserved unchanged — those dests intentionally keep the
//     source-derived name and partition via dbOrSchema.
func resolveDestTableName(tableName, destinationNamespace string, destIsObjectStorage bool) string {
	if destinationNamespace != "" {
		_, bare := storage.ExtractSchemaAndTable(tableName)
		if bare == "" {
			bare = tableName
		}
		return destinationNamespace + "." + bare
	}
	if !destIsObjectStorage {
		if _, bare := storage.ExtractSchemaAndTable(tableName); bare != "" {
			return bare
		}
	}
	return tableName
}

// resolveBatchDataset picks the top-level object-key prefix segment ("dataset")
// a table's batch output must land under.
//
// For OBJECT-STORAGE destinations the dataset is the FIRST path segment of the
// batch bronze layout ({prefix}/{dataset}/{db_or_schema}/{table}/dt=…/part-…), so
// it must be identical for EVERY table of a pipeline — otherwise a reader scoped
// to one prefix silently misses the tables written under the other. The batch
// data plane has two producer paths that disagreed: the claim-check path (large
// tables) stamped the human-readable pipelines.dataset (NAME slug), while the
// inline path (sendChunkedToKafka, small tables) stamped NO dataset so the sink's
// batch partKey fell back to slugify(pipeline_id) (the ID slug). So large
// (claim-checked) tables landed under the NAME prefix and small ones under the ID
// prefix — splitting one pipeline's batch output in two. Pinning the id slug here
// makes claim-check, inline, EOF, and the reload delete-prefix all agree.
//
// NOTE: this governs the BATCH bronze layout only. Streaming CDC output is a
// SEPARATE, deliberately different layout — the AWS DMS S3 target style
// ({path_prefix}/{schema}/{table}/{date}/…, no pipeline-id segment; see #585
// kafka-sink-worker cdcObjectKey). Batch (pipeline-scoped bronze) and CDC (DMS)
// prefixes are intentionally distinct; this helper does not touch CDC.
//
// Relational destinations ignore `dataset` entirely (they key by the db_or_schema
// namespace), so their resolved value is passed through unchanged.
func resolveBatchDataset(destIsObjectStorage bool, pipelineID, resolvedDataset string) string {
	if destIsObjectStorage {
		return storage.Slugify(pipelineID)
	}
	return resolvedDataset
}

// executeBatchDataTransfer handles batch mode with smart data path:
// - Default: Claim-check (MinIO) → Kafka pointer → kafka-mcp-sink
// - Optimization: Inline to Kafka for small payloads (<= KAFKA_INLINE_MAX_BYTES)
func (a *Agent) executeBatchDataTransfer(ctx context.Context, task ExecutorTask, kafkaTopic string, traceID string) ExecutorResponse {
	log.Infof("📦 Starting batch data transfer: %s", task.PipelineID)
	// (MCP containers are started by the infra_preflight stage in ExecutorWorker before this runs.)

	// Step 1: Export from source using MCP connector
	tables, _ := task.Params["tables"].([]interface{})
	if len(tables) == 0 {
		// Try string slice
		if strTables, ok := task.Params["tables"].([]string); ok {
			for _, t := range strTables {
				tables = append(tables, t)
			}
		}
	}

	totalRows := int64(0)
	totalBytes := int64(0)
	minioFilesCreated := 0
	directKafkaMessages := 0
	hadExportError := false
	lastExportError := ""
	// Graceful partial-sync: a table the source denies by PERMISSION (e.g. a
	// Shopify object the app isn't approved/scoped for) is skipped rather than
	// failing the whole run, PROVIDED at least one other table syncs. Real
	// errors (network/schema/write) still set hadExportError → hard fail, so the
	// T2-4 "never silently drop" invariant is preserved. skippedTables records
	// the permission-denied tables so the user is explicitly told what was left out.
	skippedTables := []string{}
	skipReasons := map[string]string{}

	executionID := ""
	if task.Params != nil {
		if v, ok := task.Params["execution_id"].(string); ok {
			executionID = strings.TrimSpace(v)
		}
	}

	// run_mode handling for batch transfers:
	// - reload: rebuild from scratch (ignore checkpoints)
	// - resume: continue from checkpoints
	runModeStr := ""
	runModeProvided := false
	if task.Params != nil {
		if v, ok := task.Params["run_mode"].(string); ok && strings.TrimSpace(v) != "" {
			runModeStr = v
			runModeProvided = true
		}
	}
	if !runModeProvided && task.Payload != nil {
		if v, ok := task.Payload["run_mode"].(string); ok && strings.TrimSpace(v) != "" {
			runModeStr = v
			runModeProvided = true
		}
	}
	if !runModeProvided && a.db != nil && task.PipelineID != "" {
		var dbRunMode string
		_ = a.db.QueryRow("SELECT COALESCE(default_run_mode,'') FROM pipelines WHERE id = $1", task.PipelineID).Scan(&dbRunMode)
		if strings.TrimSpace(dbRunMode) != "" {
			runModeStr = dbRunMode
		}
	}
	runModeGlobal := storage.ParseRunMode(runModeStr)
	if runModeGlobal == storage.RunModeReload && a.db != nil {
		// Best-effort: clear all checkpoints for this pipeline to avoid "instant success" no-op runs.
		_ = cdc.DeleteCheckpoints(ctx, a.db, task.PipelineID)
	}

	// Track per-table stats for DMS-like table statistics view.
	type tableStats struct {
		tableName    string
		insertedRows int64
		bytesRead    int64
		filesWritten int64
		startedAt    time.Time
	}
	perTableStats := make(map[string]*tableStats)

	// Optional: emit file/object write metrics (recent_files[], files_written) in DATA_PLANE_METRICS.
	// Default OFF to keep current behavior unchanged.
	var fileMetrics *MetricsEmitter
	if os.Getenv("ENABLE_FILE_METRICS_EVENTS") == "true" {
		fileMetrics = NewMetricsEmitter(MetricsEmitterConfig{
			PipelineID:              task.PipelineID,
			ExecutionID:             executionID,
			TraceID:                 traceID,
			Source:                  "executor_batch",
			MinFlushInterval:        2 * time.Second,
			MaxFlushInterval:        10 * time.Second,
			DefaultMaxFilesPerFlush: 100,
			MaxPayloadBytes:         300 * 1024, // 300KB safety cap for recent_files[]
		}, func(event []byte, headers map[string]string) error {
			return a.kafkaManager.ProduceWithHeaders("pipeline.domain.events", []byte(task.PipelineID), event, headers)
		})
	}

	// Respect pinned connector versions stored on connection records / planner output.
	// If not specified, fall back to "latest".
	sourceVersion := ""
	if task.Source != nil {
		sourceVersion = strings.TrimSpace(task.Source.Version)
		if sourceVersion == "" && task.Source.Config != nil {
			sourceVersion = strings.TrimSpace(task.Source.Config["connector_version"])
			if sourceVersion == "" {
				sourceVersion = strings.TrimSpace(task.Source.Config["version"])
			}
		}
	}
	if sourceVersion == "" {
		sourceVersion = "latest"
	}

	// Export pagination defaults (best-effort; connectors that ignore will still return first page).
	// Page size = rows fetched per export batch. Larger pages cut the number of
	// staging round-trips (MinIO claim-check PUT + Kafka hop + sink poll cycle
	// dominate per-batch latency), trading bigger MinIO objects and more memory
	// per page (each page is fetchall'd, so memory ~ exportBatchSize * row size).
	// Default 10000 leaves prod behavior unchanged; raise only for sources whose
	// connector honors a larger SQL LIMIT (MySQL/Postgres build `... LIMIT {limit}`
	// directly with no clamp). A connector that silently caps the page below this
	// would trip the `sourceRowCount < exportBatchSize` EOF heuristic and truncate,
	// so it stays opt-in via env.
	//   EXECUTOR_TABLE_BATCH_ROWS — rows per export page (>0; default 10000)
	exportBatchSize := 10000
	if raw := strings.TrimSpace(os.Getenv("EXECUTOR_TABLE_BATCH_ROWS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			exportBatchSize = n
		}
	}
	// Chunked continuation: there is NO per-table row ceiling. Each executor
	// dispatch exports up to chunkBatches batches (default 200 * 10k = 2M rows),
	// then if the table still has data it persists its durable checkpoint and
	// returns Status "needs_continuation" so the workflow re-dispatches and
	// resumes from the checkpoint until natural EOF — moving arbitrarily large
	// tables in bounded chunks with no flat cap. Genuine infinite-loop
	// protection comes from the per-batch stuck-cursor/offset guards below
	// (which break when a connector ignores pagination) plus the optional
	// maxTableRows runaway backstop. Memory stays flat regardless of table size
	// because exportBatchSize bounds each page.
	//   EXECUTOR_TABLE_CHUNK_BATCHES — batches exported per dispatch (>0; default 200)
	//   EXECUTOR_TABLE_MAX_ROWS      — hard per-table runaway ceiling (>0 to enable; default 0 = unlimited)
	chunkBatches := 200
	if raw := strings.TrimSpace(os.Getenv("EXECUTOR_TABLE_CHUNK_BATCHES")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			chunkBatches = n
		}
	}
	maxTableRows := 0
	if raw := strings.TrimSpace(os.Getenv("EXECUTOR_TABLE_MAX_ROWS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			maxTableRows = n
		}
	}

	// Step 0: Ensure topic exists BEFORE starting the sink consumer group.
	//
	// A consumer group that starts before the topic exists can end up with 0 assigned
	// partitions and never consume (until restarted).
	//
	// Create it explicitly through the admin API first. The bootstrap marker below used
	// to be the ONLY thing that brought this topic into being, which made the batch path
	// silently dependent on auto.create.topics.enable — a broker setting this platform
	// does not own on a customer-managed cluster, and one that is off by default on
	// several managed offerings. It also meant the topic was born with whatever the
	// broker's defaults are, including min.insync.replicas: on a broker defaulting to 2,
	// an auto-created RF=1 topic is permanently unwritable, and the produce below fails
	// with NOT_ENOUGH_REPLICAS naming nothing useful. EnsureTopicExists sends an explicit
	// replication factor and floor.
	//
	// One partition, because that is exactly what auto-creation gave this topic
	// (num.partitions defaults to 1) and the records are keyed — pre-creating it wider
	// would change which partition a key lands on. Best-effort: the bootstrap produce
	// still follows, so a cluster that does allow auto-create behaves as it always did.
	if a.kafkaManager != nil {
		if err := a.kafkaManager.EnsureTopicExists(kafkaTopic, 1); err != nil {
			log.WithError(err).WithField("topic", kafkaTopic).
				Warn("⚠️  Could not pre-create the batch data topic — the bootstrap marker below will only create it if the broker allows auto-creation")
		}
	}

	// We also publish a tiny bootstrap marker that the sink will ignore.
	bootstrap := map[string]interface{}{
		"pipeline_id":    task.PipelineID,
		"execution_id":   executionID,
		"storage_type":   "bootstrap",
		"trace_id":       traceID,
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
		"schema_version": 1,
	}
	if b, err := json.Marshal(bootstrap); err == nil {
		_ = a.kafkaManager.ProduceWithHeaders(kafkaTopic, []byte("__bootstrap__"), b, map[string]string{
			"pipeline_id":  task.PipelineID,
			"execution_id": executionID,
			"trace_id":     traceID,
			"storage_type": "bootstrap",
		})
	}

	// Step 1: Start Kafka-MCP-Sink so it can consume while we export.
	// This avoids the UX of "pipeline executing but no data written yet" for large tables.
	// If sink cannot start, fail fast (otherwise we'd produce to Kafka but never write to destination).
	sinkResult := a.startKafkaMCPSink(ctx, task, kafkaTopic, executionID, traceID)
	if sinkResult == nil || !sinkResult.Success {
		msg := "unknown error"
		if sinkResult != nil && sinkResult.Error != "" {
			msg = sinkResult.Error
		}
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      fmt.Sprintf("Failed to start sink: %s", msg),
		}
	}
	log.Infof("✅ Kafka-MCP-Sink started; exporting batches to topic=%s", kafkaTopic)

	// Best-effort: discover primary keys and column types so the sink can create correct
	// unique constraints, apply idempotent upserts, and use typed columns instead of TEXT.
	pkByTable := map[string][]string{}
	// colTypesByTable maps (qualified or bare table name) → (column name → source type string).
	// Source type strings are passed verbatim to the destination connector's ensure_table /
	// import_data which maps them to destination-native types (e.g. PostgreSQL _pg_type_for
	// handles MySQL dialect names like "int", "varchar(255)", "timestamp").
	colTypesByTable := map[string]map[string]string{}
	if task.Source != nil && task.Source.Config != nil {
		srcCfg := make(map[string]interface{}, len(task.Source.Config))
		for k, v := range task.Source.Config {
			srcCfg[k] = v
		}
		discovered, err := a.DiscoverSchema(ctx, task.Source.Type, srcCfg)
		if err != nil {
			log.Warnf("⚠️  Could not discover schema for PK/type metadata (continuing without PKs/types): %v", err)
		} else {
			for _, tbl := range discovered {
				full := strings.TrimSpace(tbl.Name)
				if strings.TrimSpace(tbl.Schema) != "" {
					full = strings.TrimSpace(tbl.Schema) + "." + strings.TrimSpace(tbl.Name)
				}
				if full != "" && len(tbl.PrimaryKeys) > 0 {
					pkByTable[full] = append([]string{}, tbl.PrimaryKeys...)
				}
				// Also store unqualified name for connectors that omit schema.
				if strings.TrimSpace(tbl.Name) != "" && len(tbl.PrimaryKeys) > 0 {
					pkByTable[strings.TrimSpace(tbl.Name)] = append([]string{}, tbl.PrimaryKeys...)
				}
				// Build column type map from discover_schema result.
				if len(tbl.Columns) > 0 {
					colTypes := make(map[string]string, len(tbl.Columns))
					for _, col := range tbl.Columns {
						if strings.TrimSpace(col.Name) != "" && strings.TrimSpace(col.Type) != "" {
							colTypes[strings.TrimSpace(col.Name)] = strings.TrimSpace(col.Type)
						}
					}
					if len(colTypes) > 0 {
						if full != "" {
							colTypesByTable[full] = colTypes
						}
						if strings.TrimSpace(tbl.Name) != "" {
							colTypesByTable[strings.TrimSpace(tbl.Name)] = colTypes
						}
					}
				}
			}

			// Self-healing schema drift (P1): snapshot the discovered schema (scoped
			// to the pipeline's selected tables) and diff vs the persisted baseline,
			// emitting SchemaChangeEvents for the drift -> approve loop. Internally
			// gated by RSYNC_SCHEMA_DRIFT_ENABLED; safe no-op when off or on first run.
			a.detectAndEmitSchemaDrift(ctx, task, executionID, discovered)
		}
	}

	// PR-D (column nomination): user-nominated key columns for keyless / GIPK
	// tables OVERRIDE the discovered (empty) PK. This is the single point where
	// nominated keys enter the data plane — pkByTable feeds primary_keys /
	// key_fields into every Kafka path (claim-check, inline, CDC), so the sink
	// upserts on the real columns instead of the content-hash surrogate. Match
	// both qualified ("public.orders") and bare ("orders") table names.
	if nominated := loadNominatedKeys(ctx, a.db, task.PipelineID); len(nominated) > 0 {
		for tbl, cols := range nominated {
			t := strings.TrimSpace(tbl)
			pkByTable[t] = append([]string{}, cols...)
			if _, bare := storage.ExtractSchemaAndTable(t); strings.TrimSpace(bare) != "" && bare != t {
				pkByTable[strings.TrimSpace(bare)] = append([]string{}, cols...)
			}
			log.Infof("🔑 Nominated key for %s: %v (overrides discovered PK; sink upserts on these)", t, cols)
		}
	}

	// PERF-ParallelTables: dispatch per-table work concurrently.
	//
	// Historically this was a strict serial loop. For a 6-table Shopify sync each
	// table costs ~20s of framework overhead (sink wait, schema discovery, MCP
	// dispatch) regardless of row count, so 6 tables × 20s = 120s of pure tax
	// before any data movement. We now fan out tables across a small worker
	// pool whose size is derived from the container's CPU and memory limits
	// (EXECUTOR_TABLE_CONCURRENCY overrides it). It is NOT a fixed 4 -- see
	// resolveTableConcurrency; on a memory-capped orchestrator it can be 1.
	//
	// Safety constraints baked in:
	//   - Single-table runs short-circuit and run inline (no goroutine cost).
	//   - Shared accumulators (totalRows, totalBytes, perTableStats, file counters)
	//     are guarded by an explicit mutex.
	//   - Fatal per-table errors (transform / checkpoint) are surfaced to the
	//     outer function via a captured error; remaining tables continue (so we
	//     don't silently abandon partial work), then we return the first fatal.
	//   - ensure_table / drop_table / _rsync_pipelines ownership logic is NOT
	//     touched — drop_table runs inside the per-table closure exactly once
	//     per table (same as before), and the namespace ownership row is
	//     written by the destination connector keyed by (pipeline_id, namespace)
	//     so concurrent INSERT ... ON CONFLICT DO NOTHING is safe.
	// Resource-aware per-table worker-pool sizing: scales with the container's CPU
	// (GOMAXPROCS, cgroup-aware on Go 1.25+) and memory limit, so a bigger box moves
	// more tables in parallel and a small/memory-capped box does not oversubscribe.
	// EXECUTOR_TABLE_CONCURRENCY still overrides everything. See resolveTableConcurrency.
	tableConcurrency, concurrencyReason := resolveTableConcurrencyWithReason()
	if len(tables) <= 1 {
		tableConcurrency = 1
		concurrencyReason = "single table (inline, no pool)"
	} else if tableConcurrency > len(tables) {
		tableConcurrency = len(tables)
		concurrencyReason = "bound by table count"
	}
	// The reason matters as much as the number: the memory guard can pin the pool
	// at 1 regardless of CPU, and without this an operator adding vCPU to a serial
	// sync has no way to see that mem_limit -- not cores -- is the constraint.
	log.Infof("📊 Per-table dispatch: %d tables, concurrency=%d (%s)", len(tables), tableConcurrency, concurrencyReason)

	// Destination schema layout is decided ONCE for the whole run: mirror the
	// source schemas (preserve) vs flatten every source schema into a single
	// destination namespace. In preserve mode each table's effective destination
	// namespace becomes its source schema (applied per-table below), so a
	// multi-schema source no longer collides on same-named tables. See
	// preserveSourceSchemaLayout.
	pipelineDestNamespace := resolveDestinationNamespace(ctx, a.db, task)
	preserveSchemas := a.preserveSourceSchemaLayout(ctx, task, tables, pipelineDestNamespace)
	if preserveSchemas {
		log.Infof("🗂️ Destination schema layout: PRESERVE — mirroring %d source schema(s) at the destination (per-table namespace = source schema)", len(distinctSourceSchemas(tables)))
	}

	var accMu sync.Mutex           // guards: totalRows, totalBytes, minioFilesCreated, directKafkaMessages, hadExportError, lastExportError, perTableStats
	var fatalErrMu sync.Mutex      // guards fatalErr
	var fatalErr *ExecutorResponse // first fatal per-table error (transform / checkpoint)
	var contMu sync.Mutex          // guards needsContinuation
	needsContinuation := false     // set when any table hit its per-dispatch chunk budget with more data
	sem := make(chan struct{}, tableConcurrency)
	var wg sync.WaitGroup

	// OBS1: per-table progress. tablesCompleted is bumped atomically as each table
	// finishes its transfer; totalTables is set just before dispatch (below) so the
	// closure reads the final table count.
	var tablesCompleted int32
	var totalTables int

	processTable := func(tableInterface interface{}) {
		defer wg.Done()
		defer func() { <-sem }()

		tableName := fmt.Sprintf("%v", tableInterface)
		log.Infof("  Exporting table: %s", tableName)

		// Initialize per-table stats tracker (mutex-guarded; map is shared).
		accMu.Lock()
		if perTableStats[tableName] == nil {
			perTableStats[tableName] = &tableStats{
				tableName: tableName,
				startedAt: time.Now(),
			}
		}
		tableS := perTableStats[tableName]
		accMu.Unlock()
		runDate := tableS.startedAt.UTC().Format("2006-01-02")

		// Resolve dataset/db/run_mode for downstream sinks (kafka-mcp-sink) to build stable destination paths.
		dataset := ""
		if v, ok := task.Params["dataset"].(string); ok {
			dataset = strings.TrimSpace(v)
		}
		// Prefer persisted pipeline dataset when task params don't include it.
		if dataset == "" && a.db != nil {
			var ds sql.NullString
			_ = a.db.QueryRowContext(ctx, `SELECT dataset FROM pipelines WHERE id = $1`, task.PipelineID).Scan(&ds)
			if ds.Valid {
				dataset = strings.TrimSpace(ds.String)
			}
		}
		if dataset == "" {
			// Final fallback: stable-ish, but human readability is best-effort.
			dataset = storage.Slugify(task.PipelineID)
		}
		// The db_or_schema stamped on each Kafka message serves TWO different roles
		// depending on the destination kind, and they MUST NOT be conflated:
		//
		//   - Object storage (minio/s3/gcs/azure-blob): db_or_schema is a PATH SEGMENT
		//     in the object-key layout. The source-derived schema is the intended
		//     partitioning here, so we keep the legacy source-first behavior.
		//
		//   - Relational DB (mysql/postgres/...): db_or_schema is the DESTINATION
		//     namespace (the database/schema to write into). The SOURCE schema must
		//     NEVER leak in — doing so created tables in a source-named database
		//     (e.g. "shopify") instead of the destination connection's configured
		//     database, silently landing rows in the wrong DB. The destination
		//     namespace is user-driven (HITL → pipelines.config.destination_namespace,
		//     read by resolveDestinationNamespace). When the user did not specify one,
		//     leave db_or_schema EMPTY so the sink skips addNamespaceParam and the
		//     connector falls back to its configured config["database"]. The MySQL/PG
		//     connectors run CREATE DATABASE/SCHEMA IF NOT EXISTS, so a user-specified
		//     destination namespace is auto-created on first ensure_table.
		// Destination namespace: normally the single pipeline namespace; in
		// preserve mode it becomes THIS table's source schema so a multi-schema
		// source mirrors into per-schema destination tables (no cross-schema
		// same-name collision). The relational branch below forwards it as the
		// namespace param (connectors CREATE SCHEMA/DATABASE IF NOT EXISTS); the
		// object-storage branch uses it as the {db_or_schema} path segment.
		destinationNamespace := pipelineDestNamespace
		if preserveSchemas {
			if srcSchema, _ := storage.ExtractSchemaAndTable(tableName); strings.TrimSpace(srcSchema) != "" {
				destinationNamespace = strings.TrimSpace(srcSchema)
			}
		}
		destTypeForNS := ""
		if task.Destination != nil {
			destTypeForNS = strings.TrimSpace(strings.ToLower(task.Destination.Type))
		}
		destIsObjectStorage := destTypeForNS == "minio" || strings.Contains(destTypeForNS, "s3") ||
			destTypeForNS == "gcs" || destTypeForNS == "google-cloud-storage" || destTypeForNS == "azure-blob"
		// Pin the object-storage BATCH bronze prefix to the pipeline-id slug so every
		// table's batch output shares ONE deterministic prefix — matching the inline
		// batch path and the reload delete-prefix (see resolveBatchDataset). Without
		// this, claim-checked (large) tables leaked the human-readable
		// pipelines.dataset name slug and split off under a second top-level prefix.
		// (Streaming CDC uses the separate DMS-style layout, #585 — not touched here.)
		// Relational dests are unchanged.
		dataset = resolveBatchDataset(destIsObjectStorage, task.PipelineID, dataset)
		dbOrSchema := ""
		if destIsObjectStorage {
			// Object storage: preserve source-derived path partitioning (unchanged).
			if task.Source != nil && task.Source.Config != nil {
				dbOrSchema = strings.TrimSpace(task.Source.Config["database"])
			}
			if dbOrSchema == "" {
				schema, _ := storage.ExtractSchemaAndTable(tableName)
				dbOrSchema = strings.TrimSpace(schema)
			}
			if destinationNamespace != "" {
				dbOrSchema = destinationNamespace
			}
			if dbOrSchema == "" {
				dbOrSchema = "default"
			}
		} else {
			// Relational destination: destination-driven ONLY. User HITL namespace
			// wins; otherwise EMPTY → connector resolves config["database"]. Never the
			// source schema.
			dbOrSchema = destinationNamespace
		}
		// destTableName is the table identifier the sink should write to. See
		// resolveDestTableName: namespaced → "<namespace>.<table>"; relational without a
		// namespace → BARE table (connector resolves its own database/public schema, so
		// the source schema never leaks); object storage → source-derived name unchanged.
		destTableName := resolveDestTableName(tableName, destinationNamespace, destIsObjectStorage)
		runMode := string(runModeGlobal)

		// run_mode=reload: eagerly clean destination scope before producing
		// any batches. Two sink kinds:
		//
		//   - object storage (minio/s3): delete objects under the table prefix
		//   - relational (postgres/mysql/...): DROP TABLE on the destination.
		//     ensure_table recreates fresh from latest discover_schema once the
		//     first batch lands — so column adds/drops/type-changes propagate
		//     across reloads instead of silently sticking to the old schema
		//     like TRUNCATE would.
		//
		// Without this, a reload re-runs INSERT/UPSERT into a populated table
		// and either silently appends duplicates or hits unique-key conflicts
		// (the bug observed on Shopify→Postgres pipelines pre-fix).
		if runModeGlobal == storage.RunModeReload && task.Destination != nil {
			destType := strings.TrimSpace(task.Destination.Type)
			looksLikeObjectStorage := destType == "minio" || strings.Contains(destType, "s3")
			destConfig := task.Destination.Config
			destVersion := strings.TrimSpace(task.Destination.Version)
			if destVersion == "" {
				destVersion = "latest"
			}
			traceLog := log.WithField("trace_id", telemetry.TraceIDFromContext(ctx))

			if looksLikeObjectStorage && destConfig != nil {
				bucket := strings.TrimSpace(destConfig["bucket"])
				if bucket == "" {
					bucket = strings.TrimSpace(destConfig["bucket_name"])
				}
				prefix := strings.TrimSpace(destConfig["path_prefix"])
				if prefix == "" {
					prefix = strings.TrimSpace(destConfig["prefix"])
				}

				kb := storage.NewKeyBuilder().
					WithPathPrefix(prefix).
					WithDataset(dataset).
					WithDBOrSchema(dbOrSchema).
					WithTable(tableName)
				tablePrefixToDelete := kb.TablePrefix()

				traceLog.Infof("🗑️ Reload mode: cleaning destination scope %s", tablePrefixToDelete)
				_, _ = a.executeWithRetry(ctx, mcp.ExecuteRequest{
					Connector: destType,
					Version:   destVersion,
					Operation: "delete_prefix",
					Config:    destConfig,
					Params: map[string]interface{}{
						"bucket": bucket,
						"prefix": tablePrefixToDelete,
					},
				})
			} else if !looksLikeObjectStorage && destConfig != nil {
				// Relational sink: DROP TABLE so the next batch's ensure_table
				// rebuilds from scratch. CASCADE matches DMS/Fivetran semantics.
				//
				// Round-4: when a per-pipeline destination_namespace is set, the
				// sink scopes the drop to that namespace and refuses if the
				// namespace is owned by a different pipeline (ownership row in
				// _rsync_pipelines). pipeline_id + namespace are passed so the
				// sink can enforce that gate.
				// Pass only the bare table name to drop_table so each destination
				// connector resolves the database from its own config (e.g. MySQL uses
				// config["database"] = "pipeline_test"). Never qualify with source-side
				// schema ("public.", "default.") or the pipeline destination_namespace
				// because: (a) source schema is irrelevant to the destination, and
				// (b) destination_namespace may be the generic "default" fallback stored
				// by the planner — not a real database name. The namespace param is still
				// forwarded separately for the connector's ownership-gate check.
				_, bareDropTable := storage.ExtractSchemaAndTable(tableName)
				if strings.TrimSpace(bareDropTable) == "" {
					bareDropTable = tableName
				}
				traceLog.Infof("🗑️ Reload mode (relational sink): dropping %s on %s", bareDropTable, destType)
				dropResp, dropErr := a.executeWithRetry(ctx, mcp.ExecuteRequest{
					Connector: destType,
					Version:   destVersion,
					Operation: "drop_table",
					Config:    destConfig,
					Params: map[string]interface{}{
						"table":       bareDropTable,
						"namespace":   destinationNamespace,
						"pipeline_id": task.PipelineID,
					},
				})
				if fatal, detail := classifyReloadDropResult(destType, dropResp, dropErr); fatal {
					// Relational reload MUST rebuild from scratch. A failed drop_table
					// here would leave the populated/stale-schema table in place, so the
					// next batch silently appends duplicates or writes against the old
					// schema (see block header above). Fail loud instead of continuing.
					// Inside the concurrent per-table closure we cannot `return` a value,
					// so record the first fatal via fatalErr (transform-fail precedent).
					fatalErrMu.Lock()
					if fatalErr == nil {
						fatalErr = &ExecutorResponse{
							TaskID:     task.TaskID,
							PipelineID: task.PipelineID,
							Status:     "failed",
							Error:      fmt.Sprintf("reload drop_table failed on relational destination %s (table=%s): %s", destType, bareDropTable, detail),
						}
					}
					fatalErrMu.Unlock()
					traceLog.Errorf("❌ reload drop_table failed on relational %s (table=%s): %s — aborting to avoid stale/duplicate rows", destType, bareDropTable, detail)
					return
				} else if detail != "" {
					// Non-relational connector that may legitimately lack drop_table:
					// keep the historical warn-and-continue (reload won't rebuild).
					traceLog.Warnf("⚠️ drop_table did not complete on non-relational %s — reload will NOT rebuild destination: %s", destType, detail)
				} else {
					dropped := false
					if v, ok := dropResp.Result["dropped"].(bool); ok {
						dropped = v
					}
					// Log the ACTUAL dropped identifier (bareDropTable), not the
					// source-qualified tableName — the drop targets bareDropTable +
					// namespace (see 3903/3910), so logging tableName here misreports
					// the operation and sends debuggers chasing the wrong schema.
					traceLog.Infof("✅ Destination dropped for reload (table=%s existed=%v)", bareDropTable, dropped)
				}
			}
		}

		// Table/resource normalization is handled by connectors (MCP) to keep executor generic.
		exportTableName := tableName

		// Attach best-effort PK metadata for this table.
		primaryKeys := pkByTable[tableName]
		if len(primaryKeys) == 0 {
			// Fallback to unqualified name match.
			_, t := storage.ExtractSchemaAndTable(tableName)
			if strings.TrimSpace(t) != "" {
				primaryKeys = pkByTable[strings.TrimSpace(t)]
			}
		}
		// Attach best-effort column type metadata for this table.
		colTypesForTable := colTypesByTable[tableName]
		if len(colTypesForTable) == 0 {
			_, t := storage.ExtractSchemaAndTable(tableName)
			if strings.TrimSpace(t) != "" {
				colTypesForTable = colTypesByTable[strings.TrimSpace(t)]
			}
		}

		startBatchIdx := 0
		startOffset := 0
		startKeyOrdinal := 0
		startRowsSoFar := 0
		var resumeCursor interface{} = nil
		resumeCursorJSON := ""
		// Incremental sync state (cross-connector contract — see
		// shared/mcp-connectors/INCREMENTAL.md). When a prior run wrote a
		// `mode` + `watermark.value` into the checkpoint, the executor sends
		// that value back as `since` on the next run so the connector returns
		// only rows updated AFTER the watermark instead of the full table.
		var incrementalSince string
		var incrementalField string
		// Delta baseline for THIS run (INCREMENTAL.md §5). Two values that are
		// easy to confuse and must stay apart:
		//   * `sinceCursor` — the PK high-water the last COMPLETED sweep
		//     reached. It is one half of the connector's delta predicate and
		//     stays constant for the whole logical run.
		//   * `cursor` (below) — the intra-run paging position, which moves
		//     with every page.
		// Collapsing them is what made DB-source batch incremental
		// append-only: the paging cursor was carried into the next run and
		// AND'ed as a permanent `pk >` filter, so an UPDATE to an
		// already-synced row (which keeps its old PK) could never come back.
		var sinceCursor interface{} = nil
		var pkHighWater interface{} = nil
		if runModeGlobal != storage.RunModeReload {
			// Check for existing checkpoint (resume support for batch transfers)
			existingCheckpoint, checkpointErr := cdc.GetCheckpointForTable(ctx, a.db, task.PipelineID, tableName)
			if checkpointErr == nil && existingCheckpoint != nil && existingCheckpoint.Position != nil {
				if batchIdxVal, ok := existingCheckpoint.Position["batch_idx"].(float64); ok && batchIdxVal > 0 {
					startBatchIdx = int(batchIdxVal)
				}
				if offsetVal, ok := existingCheckpoint.Position["offset"].(float64); ok && offsetVal > 0 {
					startOffset = int(offsetVal)
				}
				// See batch_key_ordinal.go for why the batch identity is a
				// separate number from the source-query offset.
				startKeyOrdinal = seedKeyOrdinal(existingCheckpoint.Position, startOffset)
				// Keyset resume: store last seen cursor (typically primary key value).
				if c, ok := existingCheckpoint.Position["cursor"]; ok && c != nil {
					resumeCursor = c
					if b, err := json.Marshal(c); err == nil {
						resumeCursorJSON = string(b)
					}
				}
				// Cumulative rows already transferred for this table across prior
				// chunks — used by the EXECUTOR_TABLE_MAX_ROWS runaway backstop.
				if rsf, ok := existingCheckpoint.Position["rows_so_far"].(float64); ok && rsf > 0 {
					startRowsSoFar = int(rsf)
				}

				// Fresh sweep vs mid-table continuation (INCREMENTAL.md §5).
				// `table_complete` is written by the checkpoint save when a
				// batch came back short — i.e. that sweep reached the end of
				// the table. On the NEXT run that means we start a new sweep
				// from the top: reset the paging cursor and promote the PK
				// high-water into the delta predicate instead. Without the
				// reset, the paging cursor survives as a permanent `pk >`
				// filter and the sync is append-only forever.
				//
				// Checkpoints written before this field existed simply lack
				// it, so the first post-deploy run behaves exactly as it did
				// before and starts emitting the marker from then on.
				pkHighWater = maxCursorValue(existingCheckpoint.Position["pk_high_water"], resumeCursor)
				if tableComplete, _ := existingCheckpoint.Position["table_complete"].(bool); tableComplete {
					sinceCursor = pkHighWater
					resumeCursor = nil
					resumeCursorJSON = ""
					startBatchIdx = 0
					startOffset = 0
					startRowsSoFar = 0
					// startKeyOrdinal deliberately NOT reset. batch_idx/offset are
					// positions within a sweep and the sweep is finished; key_ordinal
					// is a batch *identity* that names a destination object. Zeroing
					// it would make the next incremental sweep's first page reuse
					// `part-000000` and overwrite the previous sweep's — the same
					// silent overwrite this fix removes, one axis over. It only
					// resets for a reload, which skips this block entirely (and
					// wants the dataset rebuilt from scratch anyway).
				} else if sc, ok := existingCheckpoint.Position["since_cursor"]; ok && sc != nil {
					// Mid-table continuation — keep the predicate this logical
					// run started with, or later chunks would filter on a
					// different baseline than the earlier ones.
					sinceCursor = sc
				}

				// Incremental sync: translate persisted mode + watermark into a
				// `since` value the connector will use as an `updated_at > X`
				// filter. Mirrors the equivalent translation in Path A — see
				// the now-removed direct-transfer's checkpoint block for the
				// reference (history) and INCREMENTAL.md §4 for the contract.
				if mode, ok := existingCheckpoint.Position["mode"].(string); ok {
					switch mode {
					case "cloud_incremental":
						if modifiedSince, ok := existingCheckpoint.Position["modified_since"].(string); ok {
							incrementalSince = modifiedSince
						}
						if wm, ok := existingCheckpoint.Position["watermark"].(map[string]interface{}); ok {
							if value, ok := wm["value"].(string); ok {
								incrementalSince = value
							}
						}
						if incrementalSince != "" {
							log.WithField("trace_id", telemetry.TraceIDFromContext(ctx)).Infof(
								"📍 Resuming cloud incremental: modified_since=%s (table=%s)",
								incrementalSince, tableName,
							)
						}
					case "db_incremental":
						if wm, ok := existingCheckpoint.Position["watermark"].(map[string]interface{}); ok {
							if field, ok := wm["field"].(string); ok {
								incrementalField = field
							}
							if value, ok := wm["value"].(string); ok {
								incrementalSince = value
							}
						}
						if incrementalSince != "" {
							log.WithField("trace_id", telemetry.TraceIDFromContext(ctx)).Infof(
								"📍 Resuming DB incremental: watermark=%s field=%s (table=%s)",
								incrementalSince, incrementalField, tableName,
							)
						}
					}
				}

				if startBatchIdx > 0 || startOffset > 0 {
					log.WithField("trace_id", telemetry.TraceIDFromContext(ctx)).Infof(
						"📍 Resuming batch transfer from checkpoint: batch_idx=%d, offset=%d, cursor=%s (table=%s)",
						startBatchIdx, startOffset, resumeCursorJSON, tableName,
					)
				}
			}
		}

		// Best-effort pagination for DB-like connectors. Prefer keyset paging where supported (e.g. MySQL)
		// to avoid slow OFFSET scans on large tables.
		offset := startOffset
		prevFirstRowSig := ""
		var cursor interface{} = resumeCursor
		prevCursorJSON := ""
		if resumeCursorJSON != "" {
			prevCursorJSON = resumeCursorJSON
			// For keyset paging, offset should not be used.
			offset = 0
		}

		// `offset` positions the SOURCE QUERY, which is why keyset paging pins it
		// to 0 above. It must NOT also serve as the batch's identity downstream:
		// the published `batch_offset` becomes the destination object key
		// (`part-%06d`), the outbox conflict key, and the ack ledger's batch
		// column. A pinned offset collapses all of those onto one value, so every
		// page of a keyset export writes `part-000000.<ext>` over its predecessor
		// and the run still reports `completed`. Two downstream consumers already
		// had to work around this collapse rather than fix it — the ack ledger's
		// idempotency key (migration 054) and the sink's running totals — so the
		// two meanings get two variables here instead.
		//
		// keyOrdinal is the ordinal of this page's first row within the table.
		// Under offset paging it is seeded and stepped identically to `offset`,
		// so every published value is unchanged and existing object keys stay
		// byte-identical; under keyset paging it advances while `offset` stays 0.
		keyOrdinal := startKeyOrdinal

		// Chunk-boundary tracking: when the per-table loop exits because it
		// reached its per-dispatch budget (startBatchIdx + chunkBatches) AND the
		// final batch was full, the table still has rows. We persist the durable
		// checkpoint (saved every batch below) and signal "needs_continuation"
		// so the workflow re-dispatches and resumes — NOT a silent truncation and
		// NOT a fatal. A full natural EOF (sourceRowCount < exportBatchSize) ends
		// the table normally.
		hitChunkBoundary := false
		lastBatchFull := false
		dispatchRows := 0 // rows exported in THIS dispatch (for the runaway backstop)

		// Incremental sync: tracks the connector's emitted watermark across
		// batches. Persisted into the checkpoint on each successful batch
		// save so the next scheduled run can resume from the high-water
		// mark. See INCREMENTAL.md §2.
		var currentWatermark map[string]interface{}

		for batchIdx := startBatchIdx; batchIdx < startBatchIdx+chunkBatches; batchIdx++ {
			// Read data from source via MCP
			exportReq := mcp.ExecuteRequest{
				Connector: task.Source.Type,
				Version:   sourceVersion,
				Operation: "export",
				Config:    task.Source.Config,
				Params: map[string]interface{}{
					"table":  exportTableName,
					"format": "json",
					"limit":  exportBatchSize,
					"offset": offset,
				},
			}
			// Hint connectors to use keyset/cursor paging when available.
			// This is additive; connectors that don't support it will ignore it.
			exportReq.Params["use_keyset_paging"] = true
			if cursor != nil {
				exportReq.Params["cursor"] = cursor
				// For keyset paging, offset is not used by the connector.
				exportReq.Params["offset"] = 0
			}

			// Incremental sync: forward the watermark as `since` (+ aliases)
			// so the connector returns only rows updated AFTER the prior
			// run's high-water mark. Connectors that don't honor `since`
			// simply ignore it. Four alias names because different SaaS
			// vendors use different conventions; see INCREMENTAL.md §1.
			if incrementalSince != "" {
				exportReq.Params["since"] = incrementalSince
				exportReq.Params["updated_since"] = incrementalSince
				exportReq.Params["modified_since"] = incrementalSince
				exportReq.Params["modified_after"] = incrementalSince
			}
			if incrementalField != "" {
				exportReq.Params["incremental_field"] = incrementalField
			}
			// Delta baseline: the PK high-water of the last completed sweep.
			// The connector OR's this with the `since` timestamp, so both an
			// UPDATE to an already-synced row (old PK, new updated_at) and an
			// INSERT whose incremental column is NULL or hand-maintained come
			// back. Deliberately NOT folded into `cursor` — that one is this
			// run's paging position and is AND'ed, not OR'ed.
			if sinceCursor != nil {
				exportReq.Params["since_cursor"] = sinceCursor
			}

			exportResp, err := a.executeWithRetry(ctx, exportReq)
			if err != nil {
				log.Warnf("Export of table %s failed: %v", tableName, err)
				accMu.Lock()
				if isSourcePermissionError(err.Error()) {
					// Skip this table (permission/scope wall), keep syncing the rest.
					skippedTables = append(skippedTables, tableName)
					skipReasons[tableName] = err.Error()
				} else {
					hadExportError = true
					lastExportError = err.Error()
				}
				accMu.Unlock()
				break
			}

			if !exportResp.Success {
				// Scrub before logging / persisting: an upstream error body can
				// carry source-row values / PII. The raw string is used only for
				// the in-process permission-error classification below.
				scrubbedExportErr := llmscrub.Scrub(exportResp.Error)
				log.Warnf("Export of table %s failed: %s", tableName, scrubbedExportErr)
				accMu.Lock()
				if isSourcePermissionError(exportResp.Error) {
					skippedTables = append(skippedTables, tableName)
					skipReasons[tableName] = scrubbedExportErr
				} else {
					hadExportError = true
					lastExportError = scrubbedExportErr
				}
				accMu.Unlock()
				break
			}

			// Get exported data (support multiple shapes)
			var rows []map[string]interface{}
			res := exportResp.Result
			if res == nil {
				res = map[string]interface{}{}
			}
			// Unwrap optional nested "result"
			if nested, ok := res["result"].(map[string]interface{}); ok && nested != nil {
				res = nested
			}
			if data, ok := res["data"].([]interface{}); ok {
				for _, row := range data {
					if rowMap, ok := row.(map[string]interface{}); ok {
						rows = append(rows, rowMap)
					}
				}
			}
			if len(rows) == 0 {
				if data, ok := res["rows"].([]interface{}); ok {
					for _, row := range data {
						if rowMap, ok := row.(map[string]interface{}); ok {
							rows = append(rows, rowMap)
						}
					}
				}
			}
			// SaaS/REST (ApiHandler) connectors return the canonical ExportResult
			// shape, which keys rows under "records" (base_connector.ExportResult.
			// to_dict). DB connectors use "data"; some use "rows". Read all three so
			// every connector category lands rows uniformly. github-rest was the first
			// REST connector through this batch path and surfaced the missing "records"
			// branch (connector extracted 13 rows, executor saw 0).
			if len(rows) == 0 {
				if data, ok := res["records"].([]interface{}); ok {
					for _, row := range data {
						if rowMap, ok := row.(map[string]interface{}); ok {
							rows = append(rows, rowMap)
						}
					}
				}
			}
			if len(rows) == 0 {
				if batchIdx == 0 {
					log.Infof("  Table %s: no rows to export", tableName)
				}
				break
			}

			// Extract incremental-sync watermark (stats.watermark in response).
			// This is the connector's report of the max(updatedAt) it observed
			// across the rows in this batch. Persisted into the checkpoint
			// below so the next scheduled run can use it as `since`. See
			// INCREMENTAL.md §2. Only the LAST batch's watermark survives —
			// connectors that sort by updatedAt ASC (Shopify default, see
			// INCREMENTAL.md §"Two quirks") will produce a monotonically
			// increasing value; for connectors that don't sort, this falls
			// back to whichever batch wrote it last, which is still safe but
			// may force one extra full refetch on next run.
			if stats, ok := res["stats"].(map[string]interface{}); ok {
				if wm, ok := stats["watermark"].(map[string]interface{}); ok {
					currentWatermark = wm
				}
			}

			// Capture next_cursor if connector provides it (keyset paging).
			if nextCursor, ok := res["next_cursor"]; ok && nextCursor != nil {
				// Detect non-advancing cursor (prevents infinite loops)
				if b, err := json.Marshal(nextCursor); err == nil {
					nextJSON := string(b)
					if prevCursorJSON != "" && nextJSON == prevCursorJSON && len(rows) == exportBatchSize {
						log.Warnf("  Table %s: export cursor did not advance; stopping after current page", tableName)
						break
					}
					prevCursorJSON = nextJSON
				}
				cursor = nextCursor
				// Track the largest PK ever seen, not just where this sweep
				// happened to stop. With the delta predicate a sweep can end
				// BELOW the previous high-water (e.g. only an old row was
				// updated and nothing was inserted); promoting that lower
				// value would make the next run re-read the table's whole
				// tail. Upserts make that harmless but it is not free.
				pkHighWater = maxCursorValue(pkHighWater, cursor)
			}

			// Detect "offset ignored" behavior (prevents infinite loops + duplicate writes)
			if len(rows) > 0 {
				if b, err := json.Marshal(rows[0]); err == nil {
					sig := string(b)
					if offset > 0 && sig == prevFirstRowSig && len(rows) == exportBatchSize {
						// DATA-LOSS GUARD (fail-closed). The source returned an
						// identical FULL first page for a non-zero offset and never
						// advanced a keyset cursor (cursor == nil). Classic cause: a
						// MySQL table whose ONLY primary key is an INVISIBLE generated
						// key (GIPK, e.g. my_row_id) — SHOW KEYS finds it so the
						// connector picks keyset mode, but `SELECT *` omits the
						// invisible column so no next_cursor can be read AND the keyset
						// query ignores OFFSET, so every page repeats the first page.
						// This used to `break` and fall through to the per-table EOF
						// marker, so the run reported `completed` with only the first
						// page — silent data loss. Reconciliation can't catch it
						// (read == written == firstPage; the true-source-count probe is
						// gated on read == 0). A partial sync must NEVER report success:
						// fail the table. The mysql connector now projects an invisible
						// GIPK explicitly so working tables advance the cursor and never
						// reach this guard; offset > 0 also can't hold once a cursor
						// advances, since cursor paging forces offset to 0.
						if cursor == nil {
							fatalErrMu.Lock()
							if fatalErr == nil {
								fatalErr = &ExecutorResponse{
									TaskID:     task.TaskID,
									PipelineID: task.PipelineID,
									Status:     "failed",
									Error: fmt.Sprintf(
										"DATA-LOSS GUARD: table %s source connector ignored OFFSET and exposed no usable keyset cursor (commonly an INVISIBLE generated primary key / GIPK is the only PK) — refusing to silently truncate at %d rows and report success. Fix the source connector to expose a usable cursor, or nominate a key column for this table.",
										tableName, dispatchRows,
									),
								}
							}
							fatalErrMu.Unlock()
							return
						}
						log.Warnf("  Table %s: export appears to ignore offset; stopping after first page", tableName)
						break
					}
					if prevFirstRowSig == "" {
						prevFirstRowSig = sig
					}
				}
			}

			// Apply transforms (if configured) BEFORE staging/sending to Kafka.
			//
			// IMPORTANT:
			// - Pagination (offset/cursor) must advance based on the SOURCE page size, not the transformed size.
			// - Transforms like filter/exclude can reduce row count, including to 0.
			sourceRowCount := len(rows)
			transformedRows, transformErr := applyTransformsToData(ctx, a.db, executionID, rows, task, tableName)
			if transformErr != nil {
				// PERF-ParallelTables: cannot `return` from inside a goroutine.
				// Record the first fatal and unwind this table's loop; the
				// outer dispatcher returns the response after Wait().
				fatalErrMu.Lock()
				if fatalErr == nil {
					fatalErr = &ExecutorResponse{
						TaskID:     task.TaskID,
						PipelineID: task.PipelineID,
						Status:     "failed",
						Error:      fmt.Sprintf("transform failed: %v", transformErr),
					}
				}
				fatalErrMu.Unlock()
				return
			}
			rows = transformedRows
			if len(rows) == 0 {
				// All rows were filtered out. Still advance the source paging cursor/offset and continue.
				// (We intentionally do not emit an empty batch message.)
				// Unconditionally for keyOrdinal: the rows were consumed from the
				// source even though none were published, so the next page's
				// ordinal has moved on.
				offset, keyOrdinal = advancePage(offset, keyOrdinal, sourceRowCount, cursor != nil)
				if sourceRowCount < exportBatchSize {
					break
				}
				continue
			}

			// Estimate data size (avoid full marshal for very large payloads)
			dataSize := int64(0)
			if len(rows) <= 50 {
				if b, err := json.Marshal(rows); err == nil {
					dataSize = int64(len(b))
				}
			} else {
				// Sample first N rows and extrapolate.
				sampleN := 10
				if len(rows) < sampleN {
					sampleN = len(rows)
				}
				if b, err := json.Marshal(rows[:sampleN]); err == nil && sampleN > 0 {
					avg := float64(len(b)) / float64(sampleN)
					dataSize = int64(avg * float64(len(rows)))
				}
			}

			// key_ordinal is logged alongside offset because under keyset paging they
			// diverge by design (offset pinned at 0, key_ordinal advancing) — this
			// line is the cheapest way to confirm part-file keys are distinct.
			log.Infof("  Table %s: batch %d: %d rows, %d bytes (offset=%d key_ordinal=%d)", tableName, batchIdx+1, len(rows), dataSize, offset, keyOrdinal)

			// DECISION: Default to claim-check (MinIO staging). Inline only for small payloads.
			inlineMax := kafkaInlineMaxBytes()
			useInline := dataSize > 0 && dataSize <= inlineMax

			if !useInline {
				// CLAIM-CHECK PATH: Upload to MinIO → Send URL to Kafka
				log.Infof("  📁 Claim-check payload (%d KB, inline_max=%d KB), using MinIO staging", dataSize/1024, inlineMax/1024)

				claimCheckURL, err := a.stageDataToMinIO(ctx, rows, tableName, primaryKeys, task.PipelineID, executionID, traceID)
				if err != nil {
					// Retry once before abandoning the claim-check contract: a single
					// transient blip (pod restart, brief network hiccup) should not push the
					// rest of this table's batches onto the inline Kafka path, which bypasses
					// MinIO staging and puts large payloads directly on the bus.
					log.Warnf("MinIO staging failed: %v - retrying once before Kafka-chunk fallback", err)
					time.Sleep(500 * time.Millisecond)
					claimCheckURL, err = a.stageDataToMinIO(ctx, rows, tableName, primaryKeys, task.PipelineID, executionID, traceID)
				}
				if err != nil {
					log.Warnf("MinIO staging failed after retry: %v - falling back to Kafka chunks", err)
					// Fallback to chunked Kafka
					a.sendChunkedToKafka(ctx, rows, tableName, destTableName, dbOrSchema, primaryKeys, colTypesForTable, kafkaTopic, traceID, task.PipelineID, executionID, batchIdx, keyOrdinal, runMode)
					accMu.Lock()
					directKafkaMessages++
					accMu.Unlock()
				} else {
					// Send claim check reference to Kafka
					message := map[string]interface{}{
						"pipeline_id":     task.PipelineID,
						"execution_id":    executionID,
						"table":           destTableName,
						"primary_keys":    primaryKeys,
						"key_fields":      primaryKeys, // alias for sink compatibility
						"dataset":         dataset,
						"db_or_schema":    dbOrSchema,
						"dt":              runDate,
						"run_mode":        runMode,
						"claim_check_url": claimCheckURL,
						"storage_type":    "minio",
						"row_count":       len(rows),
						"batch_idx":       batchIdx,
						"batch_offset":    keyOrdinal,
						"trace_id":        traceID,
						"timestamp":       time.Now().UTC().Format(time.RFC3339),
					}
					if len(colTypesForTable) > 0 {
						message["column_types"] = colTypesForTable
					}

					msgBytes, _ := json.Marshal(message)
					headers := map[string]string{
						"pipeline_id":  task.PipelineID,
						"execution_id": executionID,
						"trace_id":     traceID,
						"table":        destTableName,
						"primary_keys": strings.Join(primaryKeys, ","),
						"key_fields":   strings.Join(primaryKeys, ","),
						"dataset":      dataset,
						"db_or_schema": dbOrSchema,
						"dt":           runDate,
						"run_mode":     runMode,
						"storage_type": "minio",
						"batch_offset": strconv.Itoa(keyOrdinal),
					}

					// Use outbox pattern for reliable delivery
					err := a.produceBatchWithOutbox(ctx, kafkaTopic, []byte(tableName), msgBytes, headers,
						task.PipelineID, executionID, destTableName, int64(keyOrdinal), int64(len(rows)), dataSize, "minio", claimCheckURL)
					if err != nil {
						log.Warnf("Failed to send MinIO reference to Kafka: %v", err)
					} else {
						accMu.Lock()
						minioFilesCreated++
						tableS.filesWritten++
						observedTotalRows := totalRows + int64(len(rows))
						observedTotalBytes := totalBytes + dataSize
						accMu.Unlock()
						log.Infof("  ✅ Staged to MinIO: %s", claimCheckURL)
						if fileMetrics != nil {
							// Observe *post-batch* totals (include current rows) and the file write itself.
							// MetricsEmitter is internally mutex-guarded (see metrics_emitter.go).
							fileMetrics.ObserveTotals(tableName, observedTotalRows, observedTotalBytes)
							fileMetrics.ObserveFileWrite(tableName, claimCheckURL, int64(len(rows)), dataSize)
							_ = fileMetrics.MaybeFlush(false)
						}
					}
				}
			} else {
				// INLINE PATH: Send directly to Kafka
				log.Infof("  📨 Inline payload (%d KB, inline_max=%d KB), sending directly to Kafka", dataSize/1024, inlineMax/1024)
				a.sendChunkedToKafka(ctx, rows, tableName, destTableName, dbOrSchema, primaryKeys, colTypesForTable, kafkaTopic, traceID, task.PipelineID, executionID, batchIdx, keyOrdinal, runMode)
				accMu.Lock()
				directKafkaMessages++
				accMu.Unlock()
			}

			// Accumulate shared totals + per-table stats under the mutex.
			accMu.Lock()
			totalRows += int64(len(rows))
			totalBytes += dataSize
			tableS.insertedRows += int64(len(rows))
			tableS.bytesRead += dataSize
			snapshotTotalRows := totalRows
			snapshotTotalBytes := totalBytes
			accMu.Unlock()

			// Emit periodic DATA_PLANE_METRICS for live UI updates (after each batch)
			// This is optional and gated behind ENABLE_REALTIME_DATA_PLANE_METRICS (default: off)
			if os.Getenv("ENABLE_REALTIME_DATA_PLANE_METRICS") == "true" {
				a.emitBatchMetrics(ctx, task.PipelineID, executionID, snapshotTotalRows, snapshotTotalBytes, tableName)
			}
			if fileMetrics != nil {
				fileMetrics.ObserveTotals(tableName, snapshotTotalRows, snapshotTotalBytes)
				_ = fileMetrics.MaybeFlush(false)
			}

			// Advance source pagination position (source rows, not transformed rows),
			// and the batch identity alongside it. Under keyset paging `offset`
			// stays pinned and only keyOrdinal moves — that is what keeps each
			// page's destination object key distinct.
			offset, keyOrdinal = advancePage(offset, keyOrdinal, sourceRowCount, cursor != nil)

			// Save checkpoint after successful batch (for resume on retry/failure)
			if a.db != nil && task.Source != nil && task.Source.Config != nil {
				sourceConnID := ""
				if connID, ok := task.Params["source_connection_id"].(string); ok {
					sourceConnID = connID
				}
				// Persist keyset cursor when available so reruns can be incremental and avoid PK conflicts.
				cursorColumn := ""
				if cc, ok := res["cursor_column"].(string); ok && strings.TrimSpace(cc) != "" {
					cursorColumn = strings.TrimSpace(cc)
				}
				pagingMode := ""
				if pm, ok := res["paging_mode"].(string); ok && strings.TrimSpace(pm) != "" {
					pagingMode = strings.TrimSpace(pm)
				}
				var cursorJSON string
				if cursor != nil {
					if b, err := json.Marshal(cursor); err == nil {
						cursorJSON = string(b)
					}
				}
				checkpointPosition := map[string]interface{}{
					"batch_idx":   batchIdx + 1,
					"offset":      offset,
					"key_ordinal": keyOrdinal,
					// Reuse the snapshots taken under accMu above; the bare
					// totalRows/totalBytes reads are unsafe under concurrency
					// (sibling table goroutines may be mid-add).
					"rows_so_far":   snapshotTotalRows,
					"bytes_so_far":  snapshotTotalBytes,
					"updated_at":    time.Now().UTC().Format(time.RFC3339),
					"paging_mode":   pagingMode,
					"cursor_column": cursorColumn,
					"cursor":        cursor,
					"cursor_json":   cursorJSON,
					// Incremental delta state (INCREMENTAL.md §5).
					// `table_complete` says this batch came back short, i.e.
					// the sweep reached the end of the table — the resume path
					// reads it to decide "start a fresh sweep" vs "continue
					// mid-table". It is computed here rather than after the
					// loop because the loop breaks on the short batch and this
					// save runs first.
					"table_complete": sourceRowCount < exportBatchSize,
					"since_cursor":   sinceCursor,
					"pk_high_water":  pkHighWater,
				}

				// Persist incremental-sync state when the connector emitted a
				// watermark on this batch. The next scheduled run reads
				// `mode` + `watermark.value` (or `modified_since` as a
				// surface-aliased copy for cloud_incremental) at the top of
				// processTable() to populate `incrementalSince`. See
				// INCREMENTAL.md §4 for the mode→replay-path mapping.
				if len(currentWatermark) > 0 {
					checkpointPosition["watermark"] = currentWatermark
					if field, ok := currentWatermark["field"].(string); ok {
						if field == "last_modified" {
							checkpointPosition["mode"] = "cloud_incremental"
							if value, ok := currentWatermark["value"].(string); ok {
								checkpointPosition["modified_since"] = value
							}
						} else {
							// Resume is still correct (db_incremental replays from field+value),
							// but an unexpected watermark field name silently downgrades the mode.
							// Surface it so a connector emitting a novel field (e.g. "created_at",
							// "event_time") is visible rather than quietly treated as db_incremental.
							log.Warnf("watermark field %q is not a recognized incremental mode key; defaulting checkpoint mode to db_incremental", field)
							checkpointPosition["mode"] = "db_incremental"
						}
					}
				}

				checkpointErr := cdc.SaveCheckpoint(ctx, a.db, cdc.Checkpoint{
					PipelineID:   task.PipelineID,
					ConnectionID: sourceConnID,
					SourceTable:  tableName,
					Position:     checkpointPosition,
				})
				if checkpointErr != nil {
					// CRITICAL FIX (#2): checkpoint failures must be fatal to prevent silent data loss
					log.WithError(checkpointErr).Error("Failed to save batch checkpoint")
					// PERF-ParallelTables: capture fatal and unwind this table's loop.
					fatalErrMu.Lock()
					if fatalErr == nil {
						fatalErr = &ExecutorResponse{
							TaskID:     task.TaskID,
							PipelineID: task.PipelineID,
							Status:     "failed",
							Error:      fmt.Sprintf("FATAL: Failed to save batch checkpoint: %v", checkpointErr),
						}
					}
					fatalErrMu.Unlock()
					return
				}
			}
			// Track cumulative rows for the runaway backstop (resumed prior
			// chunks + this dispatch). exportBatchSize bounds each page; total
			// table size is unbounded across chunks.
			dispatchRows += sourceRowCount
			// Heuristic: if batch returned fewer than the requested limit, assume we're done.
			if sourceRowCount < exportBatchSize {
				lastBatchFull = false
				break
			}
			lastBatchFull = true
			// Runaway backstop (EXECUTOR_TABLE_MAX_ROWS, default 0 = off): bound a
			// connector that returns full pages forever. The per-batch stuck-cursor
			// and offset-ignored guards above catch the common ignored-pagination
			// case; this catches genuine runaway and is a true fail-loud.
			if maxTableRows > 0 && startRowsSoFar+dispatchRows >= maxTableRows {
				fatalErrMu.Lock()
				if fatalErr == nil {
					fatalErr = &ExecutorResponse{
						TaskID:     task.TaskID,
						PipelineID: task.PipelineID,
						Status:     "failed",
						Error: fmt.Sprintf(
							"RUNAWAY EXPORT: table %s exported %d cumulative rows (>= EXECUTOR_TABLE_MAX_ROWS=%d) with a still-full final batch — likely a pagination bug (cursor/offset not advancing). Investigate the connector or raise EXECUTOR_TABLE_MAX_ROWS.",
							tableName, startRowsSoFar+dispatchRows, maxTableRows,
						),
					}
				}
				fatalErrMu.Unlock()
				return
			}
			// Per-dispatch chunk budget exhausted with a full final batch — the
			// table still has data; signal continuation after the loop.
			if batchIdx == startBatchIdx+chunkBatches-1 {
				hitChunkBoundary = true
			}
		}

		// CHUNK BOUNDARY (not a failure): this dispatch exhausted its batch
		// budget while the table still has rows (full final batch). Every batch
		// is already durably checkpointed, so signal "needs_continuation" and
		// return WITHOUT emitting the per-table EOF marker — the workflow
		// re-dispatches and the resume path at the top of processTable picks up
		// from batch_idx/cursor. This replaces the old fixed-2M SAFETY-LIMIT
		// fail-loud: large tables now continue in bounded chunks.
		if hitChunkBoundary && lastBatchFull {
			contMu.Lock()
			needsContinuation = true
			contMu.Unlock()
			log.WithField("trace_id", telemetry.TraceIDFromContext(ctx)).Infof(
				"📦 Table %s reached chunk budget (%d batches, %d rows this chunk) with more data — checkpoint saved, signaling continuation",
				tableName, chunkBatches, dispatchRows,
			)
			return
		}

		// Emit a per-table EOF marker so the sink can finalize destination-truth stats.
		// This is best-effort and does not affect execution correctness.
		eofMsg := map[string]interface{}{
			"pipeline_id":      task.PipelineID,
			"execution_id":     executionID,
			"table":            destTableName,
			"dataset":          dataset,
			"db_or_schema":     dbOrSchema,
			"dt":               runDate,
			"run_mode":         runMode,
			"storage_type":     "eof",
			"eof":              true,
			"total_read_rows":  tableS.insertedRows,
			"total_bytes_read": tableS.bytesRead,
			"trace_id":         traceID,
			"timestamp":        time.Now().UTC().Format(time.RFC3339),
		}
		if b, err := json.Marshal(eofMsg); err == nil {
			_ = a.kafkaManager.ProduceWithHeaders(kafkaTopic, []byte(tableName), b, map[string]string{
				"pipeline_id":  task.PipelineID,
				"execution_id": executionID,
				"trace_id":     traceID,
				"table":        destTableName,
				"dataset":      dataset,
				"db_or_schema": dbOrSchema,
				"dt":           runDate,
				"run_mode":     runMode,
				"storage_type": "eof",
				"batch_offset": strconv.FormatInt(int64(keyOrdinal), 10),
				"eof":          "true",
			})
		}

		// OBS1: this table finished its transfer — advance the executor progress
		// bar (80→99 band) so long multi-table runs don't sit at a static 80%
		// "Executor working…". Monotonic via the projector's GREATEST clamp; the
		// worker still emits the terminal 100 on STAGE_COMPLETED.
		a.emitExecutorTableProgress(task.PipelineID, executionID, traceID, int(atomic.AddInt32(&tablesCompleted, 1)), totalTables)

		// NOTE: Do NOT emit TABLE_STATS from the executor for the Kafka-sink path.
		// kafka-mcp-sink emits destination-truth TABLE_STATS only after successful destination writes,
		// including degraded status if written != read (EOF finalize).
	}

	// PERF-ParallelTables: dispatch tables across the worker pool.
	// Single-table runs degenerate to inline execution (sem cap == 1, no real
	// goroutine overhead — Go schedules immediately).
	totalTables = len(tables) // OBS1: denominator for per-table progress
	for _, tableInterface := range tables {
		sem <- struct{}{}
		wg.Add(1)
		go processTable(tableInterface)
	}
	wg.Wait()

	// Surface the first per-table fatal error (transform or checkpoint failure)
	// captured while goroutines were running. Other tables may have completed
	// successfully; we still return failed because the overall pipeline can't
	// reconcile partial state.
	if fatalErr != nil {
		return *fatalErr
	}

	// Chunked continuation: at least one table hit its per-dispatch chunk budget
	// with more data. Every batch up to here is durably checkpointed, so return
	// a non-error continuation signal. The Temporal workflow re-dispatches the
	// executor (classifyExecutorResponse -> PolicyCodeNeedsContinuation ->
	// deterministic re-invoke, NOT an error-retry) until every table reaches
	// natural EOF and this returns success. fatalErr takes precedence so a real
	// failure is never masked by a continuation.
	if needsContinuation {
		log.Infof("📦 Batch export reached chunk budget on one or more tables — %d rows dispatched this chunk; signaling needs_continuation for resume", totalRows)
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "needs_continuation",
		}
	}

	// Any per-table export error fails the whole pipeline — partial syncs
	// must NEVER report success. Before the fix, the check was
	// `totalRows == 0 && hadExportError`, so if table A exported successfully
	// and table B then errored mid-batch the pipeline reported success and
	// table B's missing rows were silently absent from the destination.
	// Audit finding T2-4.
	if hadExportError {
		if strings.TrimSpace(lastExportError) == "" {
			lastExportError = "export failed (no details)"
		}
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      fmt.Sprintf("batch export failed (partial sync, %d rows landed before error): %s", totalRows, lastExportError),
		}
	}

	// Graceful partial-sync resolution (only reached when there were NO hard
	// errors). Some tables may have been skipped due to source permission walls.
	if len(skippedTables) > 0 {
		skipMsg := fmt.Sprintf("%d table(s) skipped (source denied access): %s", len(skippedTables), strings.Join(skippedTables, ", "))
		if totalRows == 0 {
			// Nothing synced at all — every selected table was access-denied.
			// Surface as a failure so the user knows the run produced no data.
			log.Warnf("Batch export: all selected tables access-denied — %s", skipMsg)
			return ExecutorResponse{
				TaskID:     task.TaskID,
				PipelineID: task.PipelineID,
				Status:     "failed",
				Error:      fmt.Sprintf("no data synced — %s. Grant the missing source permissions/scopes and retry.", skipMsg),
			}
		}
		// At least one table synced — complete with a clear warning rather than
		// failing the whole run on tables the user has no permission for.
		log.Warnf("Batch export completed with skips: %d rows landed; %s", totalRows, skipMsg)
	}

	// Phase 1 + premature-completion gate: silent-drop detection.
	//
	// totalRows on the kafka-sink batch path is what the executor READ +
	// DISPATCHED to Kafka — NOT what landed at the destination (the sink writes
	// async). Passing totalRows as BOTH read and written (the old behavior) made
	// the read>0/wrote=0 detection structurally impossible: it could never fire,
	// so a crash-looping sink that landed 0 rows still reported success. We now
	// reconcile against the destination-truth ack ledger (pipeline_batch_acks):
	// bound-wait for the sink to drain, then feed the REAL landed count as
	// writtenRows. When the ledger has no ack rows for this run (a path that
	// doesn't use it, or ledger unavailable) we fall back to the prior
	// totalRows/totalRows behavior so we never false-fail.
	unverifiedCompletion := false
	if task.Source != nil {
		var srcStrCfg map[string]string
		if task.Source.Config != nil {
			srcStrCfg = make(map[string]string, len(task.Source.Config))
			for k, v := range task.Source.Config {
				srcStrCfg[k] = v
			}
		}
		probeTable := ""
		if task.Params != nil {
			if tables, ok := task.Params["tables"].([]string); ok && len(tables) > 0 {
				probeTable = tables[0]
			} else if itables, ok := task.Params["tables"].([]interface{}); ok && len(itables) > 0 {
				if s, ok := itables[0].(string); ok {
					probeTable = s
				}
			}
		}
		// Destination-truth landed count. Defaults to totalRows so any path that
		// did NOT dispatch through the kafka sink keeps prior behavior exactly.
		landedRows := totalRows
		// KI-SILENTDROP-ACK-SUM-SPANS-CHUNKS: totalRows is THIS dispatch's count, but
		// a chunked continuation re-invokes this stage under the same execution id and
		// the ack ledger sums every chunk — so a per-chunk numerator was being weighed
		// against a whole-execution denominator, and earlier chunks' acks covered a
		// later chunk's drop. sumDispatchedRows reads the producer outbox (the
		// dispatch-side twin of the ack ledger, same execution key); reconcileInputs
		// then puts both sides of the comparison on the whole run — and carries the
		// sink-lane signal across chunks too. See reconcileInputs for why each of its
		// two rules exists and how both degrade to the pre-fix behaviour.
		outboxRows, outboxBatches := sumDispatchedRows(ctx, a.db, task.PipelineID, executionID)
		dispatchedRows, dispatchedViaSink := reconcileInputs(totalRows, directKafkaMessages, minioFilesCreated, outboxRows, outboxBatches)
		if dispatchedViaSink && dispatchedRows > 0 {
			if executionID == "" {
				log.WithField("pipeline_id", task.PipelineID).Warn("⚠️ Dispatched via sink but execution_id is empty — ack-ledger reconciliation bypassed; destination landing unverifiable")
			}
			landed, received, ackRows, sinkErr := a.reconcileLandedRows(ctx, task.PipelineID, executionID, dispatchedRows, batchAckReconcileDeadline())
			decision := classifyLandedReconcile(dispatchedRows, landed, received, ackRows, directKafkaMessages, minioFilesCreated, outboxBatches, sinkErr, executionID)
			landedRows = decision.LandedRows
			switch {
			case decision.AckEvidencedDrop:
				// Definitive drop evidence: the ack ledger sums to 0 landed, a
				// kafka-dispatched run produced zero acks within the deadline, or a
				// partial shortfall carries a negative-ack sink error (permanent
				// batch loss). Bypasses the probeTable guard — the sink already
				// confirmed the loss. Surfaces the sink's REAL failure reason
				// instead of the empty "Data transfer failed:". decision.Status is
				// silent_drop_detected (total) or silent_partial_drop_detected.
				log.Warnf("🚨 Silent drop detected (ack-ledger evidence): %s", decision.Reason)
				return ExecutorResponse{
					TaskID:     task.TaskID,
					PipelineID: task.PipelineID,
					Status:     decision.Status,
					Error:      decision.Reason,
					Result: map[string]interface{}{
						// Whole-execution counts, matching decision.Reason: on a chunked
						// run the failure is about every row the execution dispatched,
						// not just the final chunk's.
						"rows_transferred": dispatchedRows,
						"source_row_count": dispatchedRows,
						"source_table":     probeTable,
					},
				}
			case decision.UnverifiedCompletion:
				// Either no acks within the deadline, or a RECEIPT shortfall
				// (SUM(rows_read) < dispatched) meaning the destination lane never
				// received some rows — ambiguous, fail-soft rather than a silent success.
				unverifiedCompletion = true
				if strings.TrimSpace(decision.Reason) != "" {
					log.Warnf("⚠️ Landed-row reconciliation: %s — marking unverified_completion.", decision.Reason)
				} else {
					log.Warnf("⚠️ Landed-row reconciliation: dispatched %d rows but found 0 ack batches within the deadline (execution %s) — ledger lag or unavailable; marking unverified_completion.", dispatchedRows, executionID)
				}
			case landed < dispatchedRows:
				// Benign undercount only reaches here now (tier 3: received>=dispatched,
				// upsert/dedup merged, or version-skew received==0). Surface for SigNoz but
				// do NOT fail — real receipt shortfalls were caught as UnverifiedCompletion.
				log.Warnf("⚠️ Landed-row reconciliation: dispatched %d but ack ledger shows %d landed (execution %s); received=%d (benign upsert/dedup or ledger skew)", dispatchedRows, landed, executionID, received)
			}
		}
		if probeTable != "" {
			if drop := CheckForSilentDrop(ctx, a, task.Source.Type, srcStrCfg, probeTable, totalRows, landedRows); drop.SilentDrop {
				log.Warnf("🚨 Silent drop detected: %s", drop.Reason)
				return ExecutorResponse{
					TaskID:     task.TaskID,
					PipelineID: task.PipelineID,
					Status:     drop.Status,
					Error:      drop.Reason,
					Result: map[string]interface{}{
						"rows_transferred": totalRows,
						"source_row_count": drop.SourceRowCount,
						"source_table":     probeTable,
					},
				}
			}
		}
	}

	log.Infof("✅ Batch transfer completed: %d rows, %d MinIO files, %d direct messages",
		totalRows, minioFilesCreated, directKafkaMessages)

	// Emit final DATA_PLANE_METRICS (completion marker)
	// This final emission is ALWAYS enabled (regardless of ENABLE_REALTIME_DATA_PLANE_METRICS)
	// so the UI always shows final totals.
	a.emitBatchMetrics(ctx, task.PipelineID, executionID, totalRows, totalBytes, "")
	if fileMetrics != nil {
		fileMetrics.ObserveTotals("", totalRows, totalBytes)
		_ = fileMetrics.MaybeFlush(true) // flush-on-completion for accuracy
	}

	finalStatus := "success"
	if unverifiedCompletion {
		finalStatus = "unverified_completion"
	}
	result := map[string]interface{}{
		"message":               "Batch transfer completed",
		"rows_processed":        totalRows,
		"bytes_processed":       totalBytes,
		"kafka_topic":           kafkaTopic,
		"minio_files_created":   minioFilesCreated,
		"direct_kafka_messages": directKafkaMessages,
	}
	finalError := ""
	// KI-SHOPIFY-PARTIAL-1: some selected tables were source-permission-denied
	// (e.g. Shopify protected-customer-data scope) while others synced. The
	// per-table skips were only logged (above) and the run fell through to
	// "success" → the UI showed "completed" and the missing tables were invisible.
	// Surface it as a terminal partial drop so the skipped tables reach the user.
	// `silent_partial_drop_detected` is already wired end-to-end: status_manager
	// maps it to "failed", and chat-diagnose / the healer / the frontend
	// "Partial Silent Drop" badge all recognize the prefix. The rows that DID land
	// stay landed; we only change how the run is reported.
	if len(skippedTables) > 0 {
		result["skipped_tables"] = skippedTables
		result["skip_reasons"] = skipReasons
		result["partial_success"] = true
		skipMsg := fmt.Sprintf("%d table(s) skipped (source denied access): %s",
			len(skippedTables), strings.Join(skippedTables, ", "))
		finalStatus = "silent_partial_drop_detected"
		finalError = fmt.Sprintf("silent_partial_drop_detected: %s. Grant the missing source permissions/scopes and retry; the remaining tables landed.", skipMsg)
	}
	return ExecutorResponse{
		TaskID:        task.TaskID,
		PipelineID:    task.PipelineID,
		Status:        finalStatus,
		PipelineType:  "batch",
		KafkaTopic:    kafkaTopic,
		RowsProcessed: int(totalRows),
		Error:         finalError,
		Result:        result,
	}
}

// stageDataToMinIO uploads large data to MinIO and returns the claim check URL
func (a *Agent) stageDataToMinIO(ctx context.Context, data []map[string]interface{}, tableName string, primaryKeys []string, pipelineID string, executionID string, traceID string) (string, error) {
	log.Infof("📁 Staging data to MinIO: %d rows for table %s", len(data), tableName)

	// Call MinIO connector to stage the data
	stageReq := mcp.ExecuteRequest{
		Connector: "minio",
		Operation: "upload_for_staging",
		Config:    map[string]string{}, // Use internal defaults
		Params: map[string]interface{}{
			"data": map[string]interface{}{
				"table":        tableName,
				"pipeline_id":  pipelineID,
				"execution_id": executionID,
				"primary_keys": primaryKeys,
				"key_fields":   primaryKeys, // alias for sink compatibility
				// Contract: kafka-mcp-sink expects staged payloads to contain a top-level "data" array.
				// Keep the payload self-describing (table/pipeline/trace) so we can debug from the object alone.
				"data":      data,
				"row_count": len(data),
				"trace_id":  traceID,
				"timestamp": time.Now().UTC().Format(time.RFC3339),
			},
		},
	}

	resp, err := a.executeWithOAuthRetry(ctx, stageReq)
	if err != nil {
		return "", fmt.Errorf("MinIO staging failed: %w", err)
	}

	if !resp.Success {
		return "", fmt.Errorf("MinIO staging failed: %s", resp.Error)
	}

	claimCheckURL, ok := resp.Result["claim_check_url"].(string)
	if !ok {
		return "", fmt.Errorf("MinIO did not return claim_check_url")
	}

	return claimCheckURL, nil
}

// sendChunkedToKafka sends data directly to Kafka in chunks
func (a *Agent) sendChunkedToKafka(ctx context.Context, rows []map[string]interface{}, tableName string, destTableName string, dbOrSchema string, primaryKeys []string, columnTypes map[string]string, kafkaTopic string, traceID string, pipelineID string, executionID string, exportBatchIdx int, exportBatchOffset int, runMode string) {
	// Target <= ~750KB per Kafka message to reduce overhead while staying below typical broker limits.
	// (Kafka defaults vary; 1MB is common.)
	const targetBytes = 750 * 1024
	if len(rows) == 0 {
		return
	}

	start := 0
	for start < len(rows) {
		// Greedy pack by estimated JSON size.
		end := start + 1
		estBytes := 0

		for end <= len(rows) {
			// Estimate size of next row (marshal row only; avoids repeated full batch marshals).
			rowBytes, err := json.Marshal(rows[end-1])
			if err != nil {
				// If marshal fails, still include the row; let downstream handle it.
				rowBytes = []byte("{}")
			}
			// Rough overhead per row in JSON array + object.
			estBytes += len(rowBytes) + 2
			if estBytes >= targetBytes {
				break
			}
			end++
		}

		// IMPORTANT: Clamp end to len(rows). The loop above can increment `end` to len(rows)+1
		// when the payload never reaches targetBytes. Slicing beyond len is allowed up to cap,
		// which would introduce a trailing nil map -> JSON `null` and inflate row_count by 1.
		if end > len(rows) {
			end = len(rows)
		}

		batch := rows[start:end]
		batchOffset := exportBatchOffset + start
		message := map[string]interface{}{
			"pipeline_id":  pipelineID,
			"execution_id": executionID,
			"table":        destTableName,
			"primary_keys": primaryKeys,
			"key_fields":   primaryKeys, // alias for sink compatibility
			"data":         batch,
			"storage_type": "inline", // Data is inline in Kafka message
			"row_count":    len(batch),
			"batch_idx":    exportBatchIdx,
			"batch_offset": batchOffset,
			"trace_id":     traceID,
			"timestamp":    time.Now().UTC().Format(time.RFC3339),
		}
		// Carry the authoritative per-pipeline destination namespace on the inline
		// path too. The claim-check path sets this (see ~3732); without it here, any
		// payload <= KAFKA_INLINE_MAX_BYTES (or the MinIO-failure fallback) reaches
		// the sink with an empty db_or_schema and the MySQL connector falls back to
		// config["database"] (e.g. "pipeline_test") — silent wrong-DB landing.
		if dbOrSchema != "" {
			message["db_or_schema"] = dbOrSchema
		}
		if runMode != "" {
			message["run_mode"] = runMode
		}
		if len(columnTypes) > 0 {
			message["column_types"] = columnTypes
		}

		msgBytes, _ := json.Marshal(message)
		headers := map[string]string{
			"pipeline_id":  pipelineID,
			"execution_id": executionID,
			"trace_id":     traceID,
			"table":        destTableName,
			"storage_type": "inline",
			"batch_offset": strconv.Itoa(batchOffset),
		}
		if dbOrSchema != "" {
			headers["db_or_schema"] = dbOrSchema
		}
		if runMode != "" {
			headers["run_mode"] = runMode
		}

		// Estimate byte size for outbox
		byteSize := int64(len(msgBytes))

		// Use outbox pattern for reliable delivery
		if err := a.produceBatchWithOutbox(ctx, kafkaTopic, []byte(tableName), msgBytes, headers,
			pipelineID, executionID, destTableName, int64(batchOffset), int64(len(batch)), byteSize, "inline", ""); err != nil {
			log.Warnf("Failed to send batch to Kafka: %v", err)
		}

		start = end
	}
}

// emitBatchMetrics emits DATA_PLANE_METRICS for batch pipelines (periodic + final).
// This provides live UI updates during execution.
func (a *Agent) emitBatchMetrics(ctx context.Context, pipelineID string, executionID string, totalRows, totalBytes int64, currentTable string) {
	traceID := telemetry.TraceIDFromContext(ctx)
	if traceID == "" {
		traceID = pipelineID
	}

	event := map[string]interface{}{
		"schema_version": 2,
		"event_type":     "DATA_PLANE_METRICS",
		"pipeline_id":    pipelineID,
		"execution_id":   executionID,
		"trace_id":       traceID,
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
		"stage":          "executor",
		"stage_group":    "executing",
		"status":         "processing",
		"message":        "Batch data plane metrics update",
		"metadata": map[string]interface{}{
			"source":         "executor_batch",
			"metrics_schema": "v2", // Standardized schema version
			"metrics": map[string]interface{}{
				"records_read": totalRows,
				"bytes_read":   totalBytes,
			},
			// Legacy fields for backward compatibility
			"rows_processed":  totalRows,
			"bytes_processed": totalBytes,
			"table_name":      currentTable, // Empty string for final metrics
		},
	}

	b, err := json.Marshal(event)
	if err != nil {
		return
	}

	_ = a.kafkaManager.ProduceWithHeaders("pipeline.domain.events", []byte(pipelineID), b, map[string]string{
		"trace_id": traceID,
	})
}

// executorProgressPercent maps table completion to the executor stage's progress
// band (80→99). The worker emits the terminal 100 on STAGE_COMPLETED, so we never
// emit 100 mid-transfer — a tick must not pre-empt the terminal event. Integer
// math; result is clamped to [80,99] and is monotonic non-decreasing in tablesDone.
func executorProgressPercent(tablesDone, totalTables int) int {
	if totalTables <= 0 {
		return 80
	}
	if tablesDone < 0 {
		tablesDone = 0
	}
	if tablesDone > totalTables {
		tablesDone = totalTables
	}
	p := 80 + (20*tablesDone)/totalTables
	if p > 99 {
		p = 99
	}
	if p < 80 {
		p = 80
	}
	return p
}

// buildExecutorTableProgressEvent builds a STAGE_PROGRESS domain event (OBS1) in
// the exact shape the pipeline_progress projector parses (ProgressEvent /
// ProgressInfo), reporting per-table transfer progress in the executor stage's
// 80→99 band. Pure / no-IO so the percent math + schema stay unit-testable without
// a Kafka/projector round-trip.
func buildExecutorTableProgressEvent(pipelineID, executionID, traceID string, tablesDone, totalTables int) map[string]interface{} {
	return map[string]interface{}{
		"schema_version": 2,
		"event_type":     "STAGE_PROGRESS",
		"pipeline_id":    pipelineID,
		"execution_id":   executionID,
		"trace_id":       traceID,
		"stage":          "executor",
		"stage_group":    "executing",
		"status":         "processing",
		"summary":        "Executing pipeline",
		"message":        fmt.Sprintf("Transferred %d of %d tables", tablesDone, totalTables),
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
		"progress": map[string]interface{}{
			"percent":      executorProgressPercent(tablesDone, totalTables),
			"current_step": 7,
			"total_steps":  8,
			"stage":        "executor",
		},
	}
}

// emitExecutorTableProgress publishes a per-table STAGE_PROGRESS event (OBS1).
// Best-effort: a nil manager or marshal/produce error is swallowed — progress is
// UX only and must never affect transfer correctness.
func (a *Agent) emitExecutorTableProgress(pipelineID, executionID, traceID string, tablesDone, totalTables int) {
	if a.kafkaManager == nil {
		return
	}
	b, err := json.Marshal(buildExecutorTableProgressEvent(pipelineID, executionID, traceID, tablesDone, totalTables))
	if err != nil {
		return
	}
	_ = a.kafkaManager.ProduceWithHeaders("pipeline.domain.events", []byte(pipelineID), b, map[string]string{
		"trace_id": traceID,
	})
}

// =============================================================================
// OUTBOX FUNCTIONS
// These implement the producer-side outbox pattern for reliable batch delivery.
// =============================================================================

// OutboxEntry represents a batch entry in the outbox table
type OutboxEntry struct {
	PipelineID       string
	ExecutionID      string
	TableName        string
	BatchOffset      int64
	RowCount         int64
	ByteSize         int64
	StorageType      string // "inline" or "minio"
	StorageReference string // MinIO URL for claim-check
	Status           string // "pending", "produced", "acked", "failed"
}

// writeOutboxEntry writes a batch entry to the outbox table (status = pending)
func (a *Agent) writeOutboxEntry(ctx context.Context, entry OutboxEntry) error {
	if a.db == nil {
		return nil // No DB connection, skip outbox
	}

	query := `
		INSERT INTO pipeline_batch_outbox (
			pipeline_id, execution_id, table_name, batch_offset,
			row_count, byte_size, storage_type, storage_reference, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (pipeline_id, execution_id, table_name, batch_offset) 
		DO UPDATE SET 
			row_count = EXCLUDED.row_count,
			byte_size = EXCLUDED.byte_size,
			storage_type = EXCLUDED.storage_type,
			storage_reference = EXCLUDED.storage_reference,
			status = CASE WHEN pipeline_batch_outbox.status = 'acked' THEN 'acked' ELSE EXCLUDED.status END,
			updated_at = CURRENT_TIMESTAMP
	`
	_, err := a.db.ExecContext(ctx, query,
		entry.PipelineID, entry.ExecutionID, entry.TableName, entry.BatchOffset,
		entry.RowCount, entry.ByteSize, entry.StorageType, entry.StorageReference, entry.Status,
	)
	return err
}

// markOutboxProduced updates outbox entry to 'produced' status with Kafka position
func (a *Agent) markOutboxProduced(ctx context.Context, pipelineID, executionID, tableName string, batchOffset int64, kafkaTopic string) error {
	if a.db == nil {
		return nil
	}

	query := `
		UPDATE pipeline_batch_outbox 
		SET status = 'produced', kafka_topic = $5, produced_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE pipeline_id = $1 AND execution_id = $2 AND table_name = $3 AND batch_offset = $4
		AND status != 'acked'
	`
	_, err := a.db.ExecContext(ctx, query, pipelineID, executionID, tableName, batchOffset, kafkaTopic)
	return err
}

// markOutboxFailed updates outbox entry to 'failed' status with error details
func (a *Agent) markOutboxFailed(ctx context.Context, pipelineID, executionID, tableName string, batchOffset int64, errMsg string) error {
	if a.db == nil {
		return nil
	}

	query := `
		UPDATE pipeline_batch_outbox 
		SET status = 'failed', 
			produce_attempts = produce_attempts + 1,
			last_produce_error = $5,
			updated_at = CURRENT_TIMESTAMP
		WHERE pipeline_id = $1 AND execution_id = $2 AND table_name = $3 AND batch_offset = $4
	`
	_, err := a.db.ExecContext(ctx, query, pipelineID, executionID, tableName, batchOffset, errMsg)
	return err
}

// produceBatchWithOutbox writes to outbox, produces to Kafka, then marks as produced
// This provides at-least-once delivery semantics for batch messages
func (a *Agent) produceBatchWithOutbox(ctx context.Context, kafkaTopic string, key []byte, value []byte, headers map[string]string,
	pipelineID, executionID, tableName string, batchOffset, rowCount, byteSize int64, storageType, storageRef string) error {

	// Step 1: Write to outbox (pending)
	entry := OutboxEntry{
		PipelineID:       pipelineID,
		ExecutionID:      executionID,
		TableName:        tableName,
		BatchOffset:      batchOffset,
		RowCount:         rowCount,
		ByteSize:         byteSize,
		StorageType:      storageType,
		StorageReference: storageRef,
		Status:           "pending",
	}
	if err := a.writeOutboxEntry(ctx, entry); err != nil {
		log.WithError(err).Warn("⚠️ Failed to write outbox entry (non-fatal)")
	}

	// Step 2: Produce to Kafka
	err := a.kafkaManager.ProduceWithHeaders(kafkaTopic, key, value, headers)

	// Step 3: Update outbox status based on result
	if err != nil {
		_ = a.markOutboxFailed(ctx, pipelineID, executionID, tableName, batchOffset, err.Error())
		return err
	}

	_ = a.markOutboxProduced(ctx, pipelineID, executionID, tableName, batchOffset, kafkaTopic)
	return nil
}

// startKafkaMCPSink starts the generic Kafka sink to write to destination
// buildCDCSinkTopics maps HITL-selected tables to the exact Debezium topic names the
// kafka-mcp-sink must subscribe to.
//
// Debezium emits one topic per (schema, table): "{prefix}.{schema}.{table}" for
// PostgreSQL/MySQL/Oracle/MongoDB. SQL Server carries an EXTRA leading database
// segment: "{prefix}.{database}.{schema}.{table}". dbQualifier is the leading segment
// taken from the first table's live topic; sourceType selects the SQL-Server-only
// re-qualification.
//
// The SQL-Server prepend (added in #383) MUST NOT run for other engines: a
// multi-schema PostgreSQL/MySQL source lists tables as "{schema}.{table}", and
// dbQualifier is only the FIRST table's schema — prepending it to a foreign-schema
// table yields a phantom topic Debezium never writes to (e.g.
// "cdc-x.blended_cost.fpa.line_items"), stranding every non-first-schema table's rows.
// Gating on sourceType=="sqlserver" preserves SQL Server's 4-segment behavior
// byte-for-byte while fixing multi-schema PG/MySQL/Oracle.
func buildCDCSinkTopics(prefix, dbQualifier, sourceType string, tablesList []string, unifiedTopic string) []string {
	isDimensionTable := func(t string) bool {
		s := strings.TrimSpace(t)
		if s == "" {
			return false
		}
		seg := s
		if i := strings.LastIndex(seg, "."); i >= 0 && i+1 < len(seg) {
			seg = seg[i+1:]
		}
		seg = strings.ToLower(strings.TrimSpace(seg))
		return strings.HasPrefix(seg, "dim_") ||
			strings.HasPrefix(seg, "dimension_") ||
			strings.HasPrefix(seg, "lookup_") ||
			strings.HasPrefix(seg, "lkp_") ||
			strings.HasPrefix(seg, "ref_")
	}
	seen := map[string]struct{}{}
	topics := []string{}
	for _, t := range tablesList {
		tt := strings.TrimSpace(t)
		if tt == "" {
			continue
		}
		var topic string
		switch {
		case unifiedTopic != "" && isDimensionTable(tt):
			topic = unifiedTopic
		case strings.HasPrefix(tt, prefix+"."):
			// Already fully qualified with the topic prefix.
			topic = tt
		default:
			// Re-qualify bare table names with the db/schema from the live topic so the
			// sink subscribes to "{prefix}.{db}.{table}" rather than "{prefix}.{table}".
			qualifiedTT := tt
			if dbQualifier != "" {
				switch {
				case !strings.Contains(tt, "."):
					// Bare HITL table name -> prepend the db/schema qualifier.
					qualifiedTT = dbQualifier + "." + tt
				case strings.EqualFold(sourceType, "sqlserver") && !strings.HasPrefix(tt, dbQualifier+"."):
					// SQL Server ONLY: Debezium adds the database as a leading segment,
					// so a "{schema}.{table}" selection needs the database prepended.
					// PG/MySQL/Oracle keep their already-dotted real "{schema}.{table}".
					qualifiedTT = dbQualifier + "." + tt
				}
			}
			topic = prefix + "." + qualifiedTT
		}
		if _, ok := seen[topic]; ok {
			continue
		}
		seen[topic] = struct{}{}
		topics = append(topics, topic)
	}
	return topics
}

func (a *Agent) startKafkaMCPSink(ctx context.Context, task ExecutorTask, kafkaTopic string, executionID string, traceID string) *mcp.ExecuteResponse {
	log.Infof("🔌 Starting Kafka-MCP-Sink for destination: %s", task.Destination.Type)

	// WRITE BOUNDARY — resolve + lock the first-run destination namespace here,
	// before the sink that performs the write.
	//
	// This is the single choke point every lane passes through on its way to the
	// destination: batch (executeBatchDataTransfer), CDC (streaming), and the blob
	// lane all start their sink through this function, and none of them writes a row
	// without it. Hanging the probe off the table-selection HITL instead made it
	// reachable only for pipelines that happened to park there
	// (KI-NSLOCK-PROBE-UNREACHABLE-WITHOUT-HITL).
	//
	// task is a value copy but Params/Payload are maps, so a namespace adopted here
	// is visible to the CALLER too — which is the point: executeBatchDataTransfer
	// resolves the namespace again further down for ensure_table / drop_table, and
	// those must not disagree with what the sink was handed.
	a.ensureDestinationNamespaceLocked(ctx, &task)

	// Destination namespace (schema/db) for relational sinks. The sink forwards this
	// to the destination connector so BOTH batch and CDC land in <namespace>.<table>.
	// Empty for non-namespaced pipelines (connector falls back to config["database"]).
	destinationNamespace := resolveDestinationNamespace(ctx, a.db, task)

	// Runtime is versioned-only; resolve destination version to a concrete vX.Y.Z so the sink can
	// route to `rsync-ai-<dest>-vX-Y-Z-mcp` (no stable `rsync-ai-<dest>-mcp` container).
	destRequestedVer := ""
	if task.Destination != nil {
		destRequestedVer = strings.TrimSpace(task.Destination.Version)
		if destRequestedVer == "" && task.Destination.Config != nil {
			if v, ok := task.Destination.Config["connector_version"]; ok {
				destRequestedVer = strings.TrimSpace(v)
			}
		}
	}
	if destRequestedVer == "" {
		destRequestedVer = "latest"
	}
	destConcreteVer, err := a.mcpManager.ResolveConcreteVersion(task.Destination.Type, destRequestedVer)
	if err != nil {
		return &mcp.ExecuteResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to resolve destination connector version (%s@%s): %v", task.Destination.Type, destRequestedVer, err),
		}
	}

	// Pre-flight: ensure the destination MCP server is running AS A DOCKER HTTP CONTAINER
	// before handing off to kafka-mcp-sink. The kafka-sink worker lives in a separate
	// container and can ONLY reach destination connectors via Docker HTTP (no stdio
	// fallback works across container boundaries). RequireHTTP=true prevents the
	// orchestrator from silently falling back to an in-process stdio subprocess that
	// kafka-mcp-sink would never be able to see — a failure mode that previously
	// manifested as "Live streaming, 0 rows written" with no visible error.
	destMCPConfig := mcp.ServerConfig{
		Name:        task.Destination.Type,
		Version:     destConcreteVer,
		RequireHTTP: true,
	}
	if task.Destination.Config != nil {
		destMCPConfig.Config = make(map[string]string, len(task.Destination.Config))
		for k, v := range task.Destination.Config {
			destMCPConfig.Config[k] = v
		}
	}
	preflight, preflightErr := a.mcpManager.StartServer(destMCPConfig)
	if preflightErr != nil || preflight == nil {
		errMsg := fmt.Sprintf("destination connector %s@%s is not available over Docker HTTP (kafka-mcp-sink cannot reach stdio subprocesses): %v", task.Destination.Type, destConcreteVer, preflightErr)
		log.Errorf("❌ Destination MCP pre-flight failed: %s", errMsg)
		return &mcp.ExecuteResponse{
			Success: false,
			Error:   errMsg,
		}
	}
	if preflight.ConnType != "http" {
		errMsg := fmt.Sprintf("destination connector %s@%s started in %q mode but kafka-mcp-sink requires HTTP", task.Destination.Type, destConcreteVer, preflight.ConnType)
		log.Errorf("❌ Destination MCP pre-flight returned unsupported transport: %s", errMsg)
		return &mcp.ExecuteResponse{
			Success: false,
			Error:   errMsg,
		}
	}
	log.Infof("✅ Destination MCP pre-flight OK: %s@%s (HTTP %s:%d)", task.Destination.Type, destConcreteVer, preflight.Host, preflight.Port)

	// Record the destination MCP as a runtime dependency so the liveness probe + the
	// canonical /runtime view know to watch it. Required for streaming phase only —
	// once a batch run finishes, the destination MCP is no longer needed.
	upsertDependency(
		a.db,
		task.PipelineID,
		executionID,
		"mcp_dest",
		task.Destination.Type+"@"+destConcreteVer,
		[]string{"syncing", "streaming"},
		map[string]interface{}{
			"host":      preflight.Host,
			"port":      preflight.Port,
			"transport": preflight.ConnType,
		},
	)
	if task.Source != nil && task.Source.Type != "" {
		// Source MCP is needed during setup (intent → table discovery → debezium config).
		// We don't strictly need it for steady-state CDC streaming (Debezium owns that),
		// but we still record it so an unhealthy source MCP surfaces as a degraded run.
		upsertDependency(
			a.db,
			task.PipelineID,
			executionID,
			"mcp_source",
			task.Source.Type+"@"+strings.TrimSpace(task.Source.Version),
			[]string{"validating", "syncing"},
			nil,
		)
	}

	// Try to infer destination table/resource from the user request (UI pipelines include this).
	// This is required for DB destinations because the sink worker calls destination connectors generically.
	// For PostgreSQL we normalize to public.<table> if no schema provided.
	userReq := ""
	if task.Params != nil {
		if s, ok := task.Params["user_request"].(string); ok && s != "" {
			userReq = s
		} else if s, ok := task.Params["request"].(string); ok && s != "" {
			userReq = s
		}
	}
	if userReq == "" && task.Payload != nil {
		if s, ok := task.Payload["user_request"].(string); ok && s != "" {
			userReq = s
		} else if s, ok := task.Payload["request"].(string); ok && s != "" {
			userReq = s
		}
	}
	_, inferredDestTable := inferTablesFromUserRequest(userReq, task.Source.Type, task.Destination.Type)

	// IMPORTANT:
	// - For CDC pipelines with multiple selected tables, we MUST NOT force a single destination table,
	//   otherwise all Debezium events get written into one table and applied stats stay at 0 (writes fail).
	// - In that case, the sink worker will route per-event using the Debezium source table (topic/schema/table).
	syncMode := ""
	if task.Params != nil {
		if v, ok := task.Params["sync_mode"].(string); ok {
			syncMode = strings.ToLower(strings.TrimSpace(v))
		}
	}
	// task.Params may not carry sync_mode (e.g. CDC streaming task built
	// from the workflow's persisted state, where only the pipelines row
	// has the authoritative value). Without this fallback,
	// `syncMode == "cdc"` was false even for CDC pipelines and the
	// multi-topic subscription block below was skipped — so the sink
	// only subscribed to the FIRST Debezium topic and every other
	// table's CDC events stayed unconsumed (queued in Kafka, never
	// applied to the destination). The CDC-mode override read on line
	// ~5282 already does this for sinkMode; mirror it here for syncMode
	// so all per-syncMode branches in this function see the same value.
	if strings.TrimSpace(syncMode) == "" && a.db != nil {
		if dbSyncMode, _ := loadPipelineModes(ctx, a.db, task.PipelineID); strings.TrimSpace(dbSyncMode) != "" {
			syncMode = strings.ToLower(strings.TrimSpace(dbSyncMode))
		}
	}

	coerceStringList := func(v interface{}) []string {
		if v == nil {
			return nil
		}
		out := make([]string, 0, 4)
		switch tv := v.(type) {
		case []string:
			for _, it := range tv {
				s := strings.TrimSpace(it)
				if s != "" {
					out = append(out, s)
				}
			}
		case []interface{}:
			for _, it := range tv {
				s := strings.TrimSpace(fmt.Sprint(it))
				if s != "" {
					out = append(out, s)
				}
			}
		default:
			// ignore unsupported shapes
		}
		return out
	}

	// Tables may be provided as either `tables` or `selected_tables` depending on the caller
	// (planner, HITL resume, legacy direct calls). For CDC multi-table correctness we must
	// treat both as authoritative.
	getTables := func() []string {
		if task.Params != nil {
			if v, ok := task.Params["tables"]; ok && v != nil {
				if out := coerceStringList(v); len(out) > 0 {
					return out
				}
			}
			if v, ok := task.Params["selected_tables"]; ok && v != nil {
				if out := coerceStringList(v); len(out) > 0 {
					return out
				}
			}
		}
		if task.Payload != nil {
			if v, ok := task.Payload["tables"]; ok && v != nil {
				if out := coerceStringList(v); len(out) > 0 {
					return out
				}
			}
			if v, ok := task.Payload["selected_tables"]; ok && v != nil {
				if out := coerceStringList(v); len(out) > 0 {
					return out
				}
			}
		}
		return nil
	}

	tablesList := getTables()
	tablesCount := len(tablesList)
	if syncMode == "cdc" && tablesCount > 1 && strings.TrimSpace(inferredDestTable) != "" {
		log.WithFields(log.Fields{
			"pipeline_id": task.PipelineID,
			"tables":      tablesCount,
			"dest_table":  inferredDestTable,
			"action":      "ignore_dest_table_override",
		}).Warn("⚠️ CDC multi-table run: ignoring inferred destination table override; sink will route per source table")
		inferredDestTable = ""
	}

	// Destination config passed to sink (copy so we can add table without mutating the saved connection)
	destCfg := map[string]string{}
	if task.Destination != nil && task.Destination.Config != nil {
		for k, v := range task.Destination.Config {
			destCfg[k] = v
		}
	}
	// Table routing rules:
	// - CDC multi-table: MUST NOT force a single destination table (sink routes per Debezium event).
	// - Batch multi-table: MUST NOT force a single destination table (sink routes per message.table).
	// - Batch single-table: override connection-level defaults to avoid cross-pipeline bleed.
	//
	// Connection-level destCfg["table"] is treated as a legacy/default hint. When a pipeline selects
	// a specific table (tablesCount==1), we prefer the selected table name unless the user explicitly
	// specified a destination table in the prompt.
	if tablesCount != 1 {
		// Multi-table runs (CDC or batch): route per-table and don't pin a single destination.
		delete(destCfg, "table")
	} else if syncMode == "cdc" {
		// CDC single-table: prefer explicit destination table from the prompt; else route to the selected table.
		// This prevents cross-pipeline bleed where a connection-level "table" default writes into the wrong table.
		override := strings.TrimSpace(inferredDestTable)
		if override == "" && len(tablesList) == 1 {
			only := strings.TrimSpace(tablesList[0])
			if only != "" {
				_, t := storage.ExtractSchemaAndTable(only)
				if strings.TrimSpace(t) != "" {
					override = strings.TrimSpace(t)
				} else {
					override = only
				}
			}
		}
		if override != "" {
			destCfg["table"] = override
		} else {
			delete(destCfg, "table")
		}
	} else {
		// Batch single-table: override connection-level defaults to avoid cross-pipeline bleed.
		override := strings.TrimSpace(inferredDestTable)
		if override == "" && len(tablesList) == 1 {
			only := strings.TrimSpace(tablesList[0])
			if only != "" {
				_, t := storage.ExtractSchemaAndTable(only)
				if strings.TrimSpace(t) != "" {
					override = strings.TrimSpace(t)
				} else {
					override = only
				}
			}
		}
		if override != "" {
			destCfg["table"] = override
		} else {
			delete(destCfg, "table")
		}
	}

	// CDC streaming-only semantics: avoid backfilling old topic data when starting a new execution.
	// - Use a fresh consumer group per execution so offsets don't stick across reruns.
	// - Start from latest offset so applied stats reflect new changes quickly.
	sinkMode := ""
	cdcMode := ""
	if task.Params != nil {
		if v, ok := task.Params["sync_mode"].(string); ok {
			sinkMode = strings.ToLower(strings.TrimSpace(v))
		}
		if v, ok := task.Params["cdc_mode"].(string); ok {
			cdcMode = strings.ToLower(strings.TrimSpace(v))
		}
	}
	// Prefer DB-persisted mode when available (authoritative, matches UI selection).
	if a.db != nil {
		dbSyncMode, dbCDCMode := loadPipelineModes(ctx, a.db, task.PipelineID)
		if strings.TrimSpace(dbSyncMode) != "" {
			sinkMode = strings.ToLower(strings.TrimSpace(dbSyncMode))
		}
		if strings.TrimSpace(dbCDCMode) != "" {
			cdcMode = strings.ToLower(strings.TrimSpace(dbCDCMode))
		}
	}
	// isBatchBackfillTopic distinguishes the hybrid-CDC batch-backfill sink (kafkaTopic
	// "pipeline.<id>.data") from the CDC streaming sink ("cdc-<id>.<db>.<table>"). It
	// gates three things below: the per-table topic rebuild, the sink parse mode, and
	// the consumer group.
	isBatchBackfillTopic := kafkaclient.InNamespace(kafkaTopic, "pipeline.")

	// Distinct consumer group per phase. The batch-backfill sink and the CDC streaming
	// sink would otherwise both use "sink-<id>"; the kafka-mcp-sink dedups start_sink by
	// consumer_group (connector.py: worker_id = consumer_group), so the second start is a
	// silent no-op and the CDC topic is never drained after the handoff. Give the
	// batch-backfill sink its own group ("sink-<id>-batch"). Industry practice is a
	// distinct group.id per phase; ordering stays correct because the backfill fully
	// drains before CDC streaming starts (log-wins), with idempotent PK-upsert apply.
	//
	// All three shapes are a function of the PIPELINE, never of the execution — see
	// sink_consumer_group.go, which also records why the streaming_only group's old
	// per-execution name (and the offset reset that fell out of it) is not preserved.
	streamingOnlySink := isStreamingOnlySink(sinkMode, cdcMode)
	consumerGroup := sinkConsumerGroup(task.PipelineID, executionID, isBatchBackfillTopic, streamingOnlySink)
	startOffset := "earliest"
	if streamingOnlySink {
		startOffset = "latest"
	}

	// Register the kafka-mcp-sink as a runtime dependency so the health panel and
	// the LLM Diagnose evidence include sink-writer health — it's the component
	// that actually applies rows to the destination, and its absence is exactly
	// what made Diagnose speculate about a "silent sink failure." Applies to both
	// CDC and batch-via-sink; required through streaming for CDC, syncing for batch.
	// Deferred until start_sink actually succeeds, NOT written here. The manifest row
	// is a claim that a worker exists under this consumer group, and everything that
	// reads it treats it as one: the batch sentinel probes the container for that group
	// and raises a CRITICAL "nothing is writing to the destination" when it is absent.
	// Writing the row before the worker exists makes that claim true-in-the-database and
	// false-in-the-container for the whole span between here and the retry loop below —
	// topic creation, the bootstrap produce, and up to five start attempts with backoff.
	// A 60s sentinel tick landing in that span alarms on a healthy run.
	//
	// It also leaked a permanent row: on five consecutive failures this function returns
	// Success:false and the run is failed by the caller, but the row it had already
	// written was never removed and nothing anywhere DELETEs from pipeline_dependencies.
	// Registering on the success path only means the row exists exactly when the claim
	// it makes is true. Sibling of the cross-execution half fixed in the batch sentinel
	// query — same defect class, other end of the same window.
	sinkPhases := []string{"syncing"}
	if sinkMode == "cdc" {
		sinkPhases = []string{"syncing", "streaming"}
	}
	registerSinkWorker := func() {
		upsertDependency(
			a.db,
			task.PipelineID,
			executionID,
			"kafka_sink_worker",
			consumerGroup,
			sinkPhases,
			// "backfill" is the ONLY thing in this row that separates the two sinks a
			// hybrid-CDC pipeline registers under one pipeline_id. This function is the
			// single component that knows which phase a given sink start is for, so if it
			// does not record it, nothing downstream can recover it: for a hybrid both
			// rows carry sink_mode "cdc" AND start_offset "earliest" (startOffset only
			// differs for a streaming_only sink, above), so neither existing key
			// discriminates. handlers.sinkConsumerGroupQuery reads it back as
			// metadata->>'backfill' to prefer the streaming row when a CDC sink restart
			// lands mid-backfill. Reuse isBatchBackfillTopic as computed above — do not
			// re-derive the phase from the topic or from the group's "-batch" suffix,
			// which would put a second copy of the naming rule in play.
			map[string]interface{}{
				"consumer_group": consumerGroup,
				"sink_mode":      sinkMode,
				"start_offset":   startOffset,
				"backfill":       isBatchBackfillTopic,
			},
		)
	}

	// CDC multi-table runs: consume ALL Debezium topics for the selected tables.
	// Debezium emits one topic per table: <topic_prefix>.<db>.<table>.
	// Historically we passed only the first returned kafka_topic, which meant only one table would be applied
	// (others would show captured activity but applied_* stayed at 0).
	topicsParam := interface{}(kafkaTopic)
	// Only rebuild per-table Debezium topics for the actual CDC STREAMING sink, whose
	// kafkaTopic is a Debezium topic (e.g. "cdc-<id>.<db>.<table>"). The hybrid-CDC
	// batch-backfill sink is started via this same function but with the BATCH topic
	// "pipeline.<id>.data" — and the pipeline's sync_mode is "cdc" — so without this
	// guard the rebuild derives a bogus "pipeline.<db>.<table>" topic (prefix taken from
	// "pipeline.<id>.data") that does not exist. The sink then subscribes to nothing and
	// the backfilled rows in "pipeline.<id>.data" are never applied to the destination.
	// (isBatchBackfillTopic is computed once above, near the consumer-group selection.)
	if syncMode == "cdc" && tablesCount > 0 && !isBatchBackfillTopic {
		// Derive topic prefix from the first table topic (e.g. "cdc-4631bd14").
		prefix := strings.TrimSpace(kafkaTopic)
		if i := strings.Index(prefix, "."); i > 0 {
			prefix = strings.TrimSpace(prefix[:i])
		}

		// Extract the database/schema qualifier from kafkaTopic.
		// Debezium topic format: "{prefix}.{db}.{table}" (MySQL) or "{prefix}.{schema}.{table}" (PG).
		// When HITL-selected table names are bare (e.g. "big_table"), we need to re-qualify them
		// so the sink subscribes to the correct topic (e.g. "cdc-xxxx.e2e_db.big_table").
		dbQualifier := ""
		if remainder := strings.TrimPrefix(strings.TrimSpace(kafkaTopic), prefix+"."); remainder != "" {
			if i := strings.Index(remainder, "."); i > 0 {
				dbQualifier = remainder[:i]
			}
		}

		// Build topic list from selected tables.
		if prefix != "" && len(tablesList) > 0 {
			unifiedTopic := ""
			if task.Params != nil {
				if v, ok := task.Params["cdc_unified_topic"].(string); ok {
					unifiedTopic = strings.TrimSpace(v)
				}
			}
			srcType := ""
			if task.Source != nil {
				srcType = task.Source.Type
			}
			if topics := buildCDCSinkTopics(prefix, dbQualifier, srcType, tablesList, unifiedTopic); len(topics) > 0 {
				topicsParam = topics
			}
		}
	}

	// Pre-create the CDC topic(s) before starting the sink. Debezium only creates a CDC
	// topic on its first change event; with snapshot.mode=recovery/no_data (hybrid CDC,
	// no snapshot) that can be much later, so a sink started against a not-yet-existent
	// topic gets 0 partitions and never consumes. The batch topic is pre-created via its
	// bootstrap marker; the CDC sink rejects non-Debezium messages, so we create the CDC
	// topic empty via the admin API instead. Skip for the batch-backfill sink (its topic
	// already exists from the bootstrap marker). Best-effort — never block the sink start.
	if !isBatchBackfillTopic && a.kafkaManager != nil {
		ensureTopic := func(name string) {
			if strings.TrimSpace(name) == "" {
				return
			}
			if err := a.kafkaManager.EnsureTopicExists(name, 3); err != nil {
				// Deliberately not "will rely on auto-create": auto-creation is a broker
				// setting this platform does not control on a customer-managed cluster,
				// and when it is off the sink simply consumes nothing forever while the
				// pipeline reports running. Say what actually happens.
				log.WithError(err).WithField("topic", name).
					Warn("⚠️  Could not pre-create CDC topic for sink — the sink will consume nothing unless the broker auto-creates it")
			}
		}
		switch tp := topicsParam.(type) {
		case string:
			ensureTopic(tp)
		case []string:
			for _, t := range tp {
				ensureTopic(t)
			}
		}
	}

	sinkReq := mcp.ExecuteRequest{
		Connector: "kafka-mcp-sink",
		Operation: "start_sink",
		Config:    map[string]string{}, // Sink uses its own config
		Params: map[string]interface{}{
			"config": map[string]interface{}{
				// Inside the Docker network we must use the internal listener
				// (9092 is typically the host/externally-advertised listener),
				// which is what KAFKA_BROKERS is set to in every shipped
				// compose file. Deriving it means a customer-managed cluster
				// reaches the sink too.
				"kafka_bootstrap_servers": kafka.SinkBootstrapServers(),
				"topics":                  topicsParam,
				"consumer_group":          consumerGroup,
				"start_offset":            startOffset,
				"sink_mode": func() string {
					// The hybrid-CDC batch-backfill sink consumes the batch topic
					// "pipeline.<id>.data" (plain row batches, NOT Debezium envelopes),
					// even though the pipeline's sync_mode is "cdc". Running it in "cdc"
					// mode makes the sink reject every batch row with "CDC message missing
					// required payload fields (op/source)". Force "auto" (batch) parsing for
					// the batch-backfill topic; only the real CDC streaming sink uses "cdc".
					if sinkMode == "cdc" && !isBatchBackfillTopic {
						return "cdc"
					}
					return "auto"
				}(),
				"pipeline_id":           task.PipelineID,
				"execution_id":          executionID,
				"destination_connector": task.Destination.Type,
				"destination_version":   destConcreteVer,
				"destination_config":    destCfg,
				"destination_namespace": destinationNamespace,
			},
			"trace_id": traceID,
		},
	}

	// Fix #15 (Resilience): retry sink startup with exponential backoff
	var lastErr error
	var lastResp *mcp.ExecuteResponse
	for attempt := 1; attempt <= 5; attempt++ {
		resp, err := a.executeWithOAuthRetry(ctx, sinkReq)
		lastResp = resp
		lastErr = err

		if err == nil && resp != nil && resp.Success {
			// The worker exists now, so the manifest claim is true now.
			registerSinkWorker()
			return resp
		}

		// Backoff before retry (except after final attempt)
		if attempt < 5 {
			backoff := time.Duration(attempt*attempt) * 500 * time.Millisecond
			if backoff > 10*time.Second {
				backoff = 10 * time.Second
			}
			// jitter +-20%
			jitter := time.Duration(float64(backoff) * 0.2 * (rand.Float64()*2 - 1))
			sleep := backoff + jitter
			if sleep < 0 {
				sleep = 0
			}
			log.WithFields(log.Fields{
				"attempt": attempt,
				"max":     5,
				"sleep":   sleep.String(),
				"error":   fmt.Sprintf("%v", err),
			}).Warn("⚠️ Kafka sink start failed, retrying with backoff")

			select {
			case <-ctx.Done():
				return &mcp.ExecuteResponse{Success: false, Error: fmt.Sprintf("sink start cancelled: %v", ctx.Err())}
			case <-time.After(sleep):
			}
		}
	}

	if lastErr != nil {
		return &mcp.ExecuteResponse{Success: false, Error: lastErr.Error()}
	}
	if lastResp != nil {
		return lastResp
	}
	return &mcp.ExecuteResponse{Success: false, Error: "sink start failed (unknown error)"}
}

// executeExport exports data from source
func (a *Agent) executeExport(ctx context.Context, task ExecutorTask) ExecutorResponse {
	if task.Source == nil {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      "source not specified",
		}
	}

	log.Infof("Exporting from %s", task.Source.Type)

	// Execute export via MCP
	req := mcp.ExecuteRequest{
		Connector: task.Source.Type,
		Operation: "export",
		Config:    task.Source.Config,
		Params:    task.Params,
	}

	resp, err := a.executeWithOAuthRetry(ctx, req)
	if err != nil {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      err.Error(),
		}
	}

	if !resp.Success {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      resp.Error,
		}
	}

	return ExecutorResponse{
		TaskID:     task.TaskID,
		PipelineID: task.PipelineID,
		Status:     "success",
		Result:     resp.Result,
	}
}

// executeImport imports data to destination
func (a *Agent) executeImport(ctx context.Context, task ExecutorTask) ExecutorResponse {
	if task.Destination == nil {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      "destination not specified",
		}
	}

	log.Infof("Importing to %s", task.Destination.Type)

	// Execute import via MCP
	req := mcp.ExecuteRequest{
		Connector: task.Destination.Type,
		Operation: "import",
		Config:    task.Destination.Config,
		Params:    task.Params,
	}

	resp, err := a.executeWithOAuthRetry(ctx, req)
	if err != nil {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      err.Error(),
		}
	}

	if !resp.Success {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      resp.Error,
		}
	}

	return ExecutorResponse{
		TaskID:     task.TaskID,
		PipelineID: task.PipelineID,
		Status:     "success",
		Result:     resp.Result,
	}
}

// executeQuery executes a query operation
func (a *Agent) executeQuery(ctx context.Context, task ExecutorTask) ExecutorResponse {
	if task.Source == nil {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      "source not specified",
		}
	}

	log.Infof("Querying %s", task.Source.Type)

	// Execute query via MCP
	req := mcp.ExecuteRequest{
		Connector: task.Source.Type,
		Operation: "query",
		Config:    task.Source.Config,
		Params:    task.Params,
	}

	resp, err := a.executeWithOAuthRetry(ctx, req)
	if err != nil {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      err.Error(),
		}
	}

	if !resp.Success {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      resp.Error,
		}
	}

	return ExecutorResponse{
		TaskID:     task.TaskID,
		PipelineID: task.PipelineID,
		Status:     "success",
		Result:     resp.Result,
	}
}

// executeCommand executes a command operation
func (a *Agent) executeCommand(ctx context.Context, task ExecutorTask) ExecutorResponse {
	if task.Destination == nil {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      "destination not specified",
		}
	}

	log.Infof("Executing command on %s", task.Destination.Type)

	// Execute command via MCP
	req := mcp.ExecuteRequest{
		Connector: task.Destination.Type,
		Operation: "execute",
		Config:    task.Destination.Config,
		Params:    task.Params,
	}

	resp, err := a.executeWithOAuthRetry(ctx, req)
	if err != nil {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      err.Error(),
		}
	}

	if !resp.Success {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      resp.Error,
		}
	}

	return ExecutorResponse{
		TaskID:     task.TaskID,
		PipelineID: task.PipelineID,
		Status:     "success",
		Result:     resp.Result,
	}
}

// ============================================================================
// STREAMING/CDC OPERATIONS
// ============================================================================

// executeStartStreaming starts a CDC/streaming pipeline via dynamic CDC provider
//
// FULLY GENERIC: Uses CDC provider from task params (set by Planner) instead of hardcoding
func (a *Agent) executeStartStreaming(ctx context.Context, task ExecutorTask) ExecutorResponse {
	log.Infof("🔄 Starting streaming pipeline: %s", task.PipelineID)

	// Get parameters
	table := ""
	connectorName := ""
	dbType := "" // Removed default "mysql" to enforce explicit type
	snapshotMode := "initial"

	if task.Params != nil {
		if t, ok := task.Params["table"].(string); ok {
			table = t
		}
		if cn, ok := task.Params["connector_name"].(string); ok {
			connectorName = cn
		}
		if dt, ok := task.Params["database_type"].(string); ok {
			dbType = dt
		}
		// Prefer explicit snapshot_mode, else fall back to cdc_mode for "changes only" semantics.
		if sm, ok := task.Params["snapshot_mode"].(string); ok && strings.TrimSpace(sm) != "" {
			snapshotMode = sm
		} else if cm, ok := task.Params["cdc_mode"].(string); ok && strings.TrimSpace(cm) != "" {
			snapshotMode = cm
		}
	}

	// Try to derive dbType from source if not provided
	if dbType == "" && task.Source != nil {
		dbType = task.Source.Type
		// NOTE: Removed hardcoded "postgres" -> "postgresql" mapping.
		// Connectors must align or Planner must normalize.
	}

	if dbType == "" {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      "database_type parameter is required for streaming (or valid source type)",
		}
	}

	if table == "" {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      "table parameter is required for streaming",
		}
	}

	// Prefer DB-persisted cdc_mode when available (authoritative).
	if a.db != nil {
		_, cm := loadPipelineModes(ctx, a.db, task.PipelineID)
		if strings.TrimSpace(cm) != "" {
			snapshotMode = cm
		}
	}
	snapshotMode = strings.ToLower(strings.TrimSpace(snapshotMode))
	switch snapshotMode {
	case "", "auto":
		snapshotMode = "initial"
	case "initial", "streaming_only", "never", "initial_only":
	default:
		snapshotMode = "initial"
	}

	// Get source config (database credentials)
	var dbConfig map[string]string
	if task.Source != nil {
		dbConfig = task.Source.Config
	} else {
		// Try to get from connection manager if connection_id in params
		if connID, ok := task.Params["connection_id"].(string); ok && connID != "" {
			var err error
			dbConfig, err = a.getConnectionConfigForTask(ctx, task, connID)
			if err != nil {
				return ExecutorResponse{
					TaskID:     task.TaskID,
					PipelineID: task.PipelineID,
					Status:     "failed",
					Error:      fmt.Sprintf("failed to get connection config: %v", err),
				}
			}
		} else {
			dbConfig = make(map[string]string)
		}
	}

	// Build Debezium start_sync request
	params := map[string]interface{}{
		"table":         table,
		"database_type": dbType,
		// Debezium MCP maps these values to Debezium snapshot.mode ("streaming_only" -> "never").
		"cdc_mode":      snapshotMode,
		"snapshot_mode": snapshotMode,
	}

	if connectorName != "" {
		params["connector_name"] = connectorName
	} else {
		// Auto-generate connector name
		connectorName = fmt.Sprintf("cdc-%s-%d", task.PipelineID, time.Now().Unix())
		params["connector_name"] = connectorName
	}

	// Add database config to params
	if host, ok := dbConfig["host"]; ok {
		params["db_host"] = host
	}
	if port, ok := dbConfig["port"]; ok {
		params["db_port"] = port
	}
	if user, ok := dbConfig["user"]; ok || dbConfig["username"] != "" {
		if user == "" {
			user = dbConfig["username"]
		}
		params["db_user"] = user
	}
	if password, ok := dbConfig["password"]; ok {
		params["db_password"] = password
	}
	if dbName, ok := dbConfig["database"]; ok || dbConfig["db_name"] != "" {
		if dbName == "" {
			dbName = dbConfig["db_name"]
		}
		params["db_name"] = dbName
	}

	// Enable Debezium ad-hoc snapshots (DMS-like reload/backfill) for MySQL by default.
	// This configures the connector to listen for "execute-snapshot" signals via a table.
	if strings.EqualFold(dbType, "mysql") {
		if dbName, ok := params["db_name"].(string); ok && strings.TrimSpace(dbName) != "" {
			params["signal_data_collection"] = fmt.Sprintf("%s.debezium_signal", strings.TrimSpace(dbName))
		}
	}

	// ==========================================================================
	// PHASE 1: Get CDC provider from task params (metadata-driven)
	// ==========================================================================
	cdcProvider := "" // No default, must be provided or derived
	if provider, ok := task.Params["cdc_provider"].(string); ok && provider != "" {
		cdcProvider = provider
	} else {
		// Warning instead of hard error for backward compatibility
		cdcProvider = "debezium"
		log.Warnf("⚠️  No cdc_provider specified, falling back to default: %s", cdcProvider)
	}

	// Get connector class from plan if available
	if connClass, ok := task.Params["connector_class"].(string); ok && connClass != "" {
		params["connector_class"] = connClass
	}

	// Execute via dynamic CDC provider MCP connector
	req := mcp.ExecuteRequest{
		Connector: cdcProvider, // GENERIC: Uses provider from plan, not hardcoded
		Operation: "start_sync",
		Config:    map[string]string{}, // CDC connector uses env vars for Kafka Connect URL
		Params:    params,
	}

	log.Infof("📤 Calling %s MCP: start_sync for table %s", cdcProvider, table)
	resp, err := a.executeWithRetry(ctx, req)
	if err != nil {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      fmt.Sprintf("failed to start CDC: %v", err),
		}
	}

	if !resp.Success {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      fmt.Sprintf("CDC start failed: %s", resp.Error),
		}
	}

	// Extract connector name and Kafka topic from response
	actualConnectorName := connectorName
	kafkaTopic := ""
	if cn, ok := resp.Result["connector_name"].(string); ok {
		actualConnectorName = cn
	}
	if kt, ok := resp.Result["kafka_topic"].(string); ok {
		kafkaTopic = kt
	}

	// Track the streaming pipeline with dynamic CDC provider
	a.AddStreamingPipeline(task.PipelineID, &StreamingPipelineInfo{
		PipelineID:      task.PipelineID,
		ConnectorName:   actualConnectorName,
		ConnectorType:   cdcProvider, // GENERIC: Uses provider from plan
		Status:          "running",
		KafkaTopic:      kafkaTopic,
		StartedAt:       time.Now(),
		LastHealthCheck: time.Now(),
		HealthStatus:    "healthy",
	})

	log.Infof("✅ Streaming pipeline started: %s (connector: %s, topic: %s)", task.PipelineID, actualConnectorName, kafkaTopic)

	return ExecutorResponse{
		TaskID:        task.TaskID,
		PipelineID:    task.PipelineID,
		Status:        "running", // Note: "running" not "success" for long-running pipelines
		Result:        resp.Result,
		PipelineType:  "streaming",
		ConnectorName: actualConnectorName,
		KafkaTopic:    kafkaTopic,
	}
}

// executeStopStreaming stops a CDC/streaming pipeline
//
// FULLY GENERIC: Uses CDC provider from tracked pipeline info
func (a *Agent) executeStopStreaming(ctx context.Context, task ExecutorTask) ExecutorResponse {
	log.Infof("🛑 Stopping streaming pipeline: %s", task.PipelineID)

	// Get connector name and CDC provider from params or tracked pipelines
	connectorName := ""
	cdcProvider := "" // No default, must be derived

	if cn, ok := task.Params["connector_name"].(string); ok && cn != "" {
		connectorName = cn
	}
	if provider, ok := task.Params["cdc_provider"].(string); ok && provider != "" {
		cdcProvider = provider
	}

	// Check tracked pipelines for connector info (thread-safe)
	if info, exists := a.getStreamingPipelineInfo(task.PipelineID); exists {
		if connectorName == "" {
			connectorName = info.ConnectorName
		}
		if info.ConnectorType != "" {
			cdcProvider = info.ConnectorType // Use tracked provider
		}
	}

	if cdcProvider == "" {
		cdcProvider = "debezium"
		log.Warnf("⚠️  No cdc_provider known for pipeline %s, falling back to: %s", task.PipelineID, cdcProvider)
	}

	if connectorName == "" {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      "connector_name is required to stop streaming",
		}
	}

	// Execute via dynamic CDC provider MCP connector
	req := mcp.ExecuteRequest{
		Connector: cdcProvider, // GENERIC: Uses provider from tracking
		Operation: "stop_sync",
		Config:    map[string]string{},
		Params: map[string]interface{}{
			"connector_name": connectorName,
		},
	}

	log.Infof("📤 Calling %s MCP: stop_sync for connector %s", cdcProvider, connectorName)
	resp, err := a.executeWithRetry(ctx, req)
	if err != nil {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      fmt.Sprintf("failed to stop CDC: %v", err),
		}
	}

	if !resp.Success {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      fmt.Sprintf("CDC stop failed: %s", resp.Error),
		}
	}

	// Update tracked pipeline status (thread-safe)
	a.updateStreamingPipelineStatus(task.PipelineID, "stopped", "stopped")
	a.RemoveStreamingPipeline(task.PipelineID)

	log.Infof("✅ Streaming pipeline stopped: %s (connector: %s)", task.PipelineID, connectorName)

	return ExecutorResponse{
		TaskID:        task.TaskID,
		PipelineID:    task.PipelineID,
		Status:        "stopped",
		Result:        resp.Result,
		PipelineType:  "streaming",
		ConnectorName: connectorName,
	}
}

// executeStreamingStatus gets the status of a streaming pipeline
//
// FULLY GENERIC: Uses CDC provider from tracked pipeline info
func (a *Agent) executeStreamingStatus(ctx context.Context, task ExecutorTask) ExecutorResponse {
	log.Infof("📊 Getting streaming status for: %s", task.PipelineID)

	// Get connector name and CDC provider from params or tracked pipelines
	connectorName := ""
	cdcProvider := ""

	if cn, ok := task.Params["connector_name"].(string); ok && cn != "" {
		connectorName = cn
	}
	if provider, ok := task.Params["cdc_provider"].(string); ok && provider != "" {
		cdcProvider = provider
	}

	// Check tracked pipelines for connector info (thread-safe)
	if info, exists := a.getStreamingPipelineInfo(task.PipelineID); exists {
		if connectorName == "" {
			connectorName = info.ConnectorName
		}
		if info.ConnectorType != "" {
			cdcProvider = info.ConnectorType // Use tracked provider
		}
	}

	if cdcProvider == "" {
		cdcProvider = "debezium"
		log.Warnf("⚠️  No cdc_provider known for pipeline %s, falling back to: %s", task.PipelineID, cdcProvider)
	}

	if connectorName == "" {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      "connector_name is required for status check",
		}
	}

	// Execute via dynamic CDC provider MCP connector
	req := mcp.ExecuteRequest{
		Connector: cdcProvider, // GENERIC: Uses provider from tracking
		Operation: "get_status",
		Config:    map[string]string{},
		Params: map[string]interface{}{
			"connector_name": connectorName,
		},
	}

	resp, err := a.executeWithRetry(ctx, req)
	if err != nil {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      fmt.Sprintf("failed to get CDC status: %v", err),
		}
	}

	if !resp.Success {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      fmt.Sprintf("CDC status check failed: %s", resp.Error),
		}
	}

	// Debezium MCP returns:
	//   { success: true, data: { connector:{state,...}, tasks:[...], ... }, status_code: 200 }
	raw := map[string]interface{}{}
	if resp.Result != nil {
		raw = resp.Result
	}

	// Extract Kafka Connect status payload
	var statusPayload map[string]interface{}
	if d, ok := raw["data"].(map[string]interface{}); ok && d != nil {
		statusPayload = d
	} else if nested, ok := raw["result"].(map[string]interface{}); ok && nested != nil {
		// Defensive: some connectors wrap payload under result
		if d, ok := nested["data"].(map[string]interface{}); ok && d != nil {
			statusPayload = d
		}
	}

	connectorState := "UNKNOWN"
	taskStates := []string{}
	if statusPayload != nil {
		if c, ok := statusPayload["connector"].(map[string]interface{}); ok && c != nil {
			if s, ok := c["state"].(string); ok && s != "" {
				connectorState = s
			}
		}
		if tasks, ok := statusPayload["tasks"].([]interface{}); ok {
			for _, t := range tasks {
				if tm, ok := t.(map[string]interface{}); ok && tm != nil {
					if s, ok := tm["state"].(string); ok && s != "" {
						taskStates = append(taskStates, s)
					}
				}
			}
		}
	}

	// Derive health
	healthy := connectorState == "RUNNING"
	if len(taskStates) > 0 {
		for _, s := range taskStates {
			if s != "RUNNING" {
				healthy = false
				break
			}
		}
	}
	healthStatus := "unknown"
	if healthy {
		healthStatus = "healthy"
	} else {
		healthStatus = "unhealthy"
	}

	pipelineStatus := strings.ToLower(connectorState)
	if pipelineStatus == "unknown" || pipelineStatus == "" {
		pipelineStatus = "unknown"
	}
	if connectorState == "RUNNING" {
		pipelineStatus = "running"
	}
	if connectorState == "FAILED" {
		pipelineStatus = "failed"
	}
	if connectorState == "PAUSED" {
		pipelineStatus = "paused"
	}

	// Update tracked pipeline info (thread-safe)
	a.updateStreamingPipelineStatus(task.PipelineID, pipelineStatus, healthStatus)

	return ExecutorResponse{
		TaskID:     task.TaskID,
		PipelineID: task.PipelineID,
		Status:     pipelineStatus,
		Result: map[string]interface{}{
			"connector_name":  connectorName,
			"cdc_provider":    cdcProvider,
			"connector_state": connectorState,
			"task_states":     taskStates,
			"healthy":         healthy,
			"health_status":   healthStatus,
			"raw":             raw,
		},
		PipelineType:  "streaming",
		ConnectorName: connectorName,
	}
}

// executeRestartStreaming restarts the underlying CDC connector (best-effort).
// This is used for demo-grade self-healing: a one-click "restart CDC pipeline".
func (a *Agent) executeRestartStreaming(ctx context.Context, task ExecutorTask) ExecutorResponse {
	log.Infof("🔁 Restarting streaming pipeline: %s", task.PipelineID)

	// Derive connector/provider (same as status) - thread-safe
	connectorName := ""
	cdcProvider := ""

	if cn, ok := task.Params["connector_name"].(string); ok && cn != "" {
		connectorName = cn
	}
	if provider, ok := task.Params["cdc_provider"].(string); ok && provider != "" {
		cdcProvider = provider
	}
	if info, exists := a.getStreamingPipelineInfo(task.PipelineID); exists {
		if connectorName == "" {
			connectorName = info.ConnectorName
		}
		if info.ConnectorType != "" {
			cdcProvider = info.ConnectorType
		}
	}
	if cdcProvider == "" {
		cdcProvider = "debezium"
	}
	if connectorName == "" {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      "connector_name is required to restart streaming",
		}
	}

	// Restart connector via CDC provider MCP (Debezium -> Kafka Connect /restart)
	restartReq := mcp.ExecuteRequest{
		Connector: cdcProvider,
		Operation: "restart_connector",
		Config:    map[string]string{},
		Params: map[string]interface{}{
			"connector_name": connectorName,
		},
	}
	restartResp, err := a.executeWithRetry(ctx, restartReq)
	if err != nil {
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      fmt.Sprintf("failed to restart CDC connector: %v", err),
		}
	}
	if restartResp == nil || !restartResp.Success {
		msg := ""
		if restartResp != nil {
			msg = restartResp.Error
		}
		return ExecutorResponse{
			TaskID:     task.TaskID,
			PipelineID: task.PipelineID,
			Status:     "failed",
			Error:      fmt.Sprintf("CDC restart failed: %s", msg),
		}
	}

	// Best-effort: re-check status after a short delay (Connect restarts asynchronously)
	time.Sleep(2 * time.Second)
	statusTask := task
	if statusTask.Params == nil {
		statusTask.Params = map[string]interface{}{}
	}
	statusTask.Params["connector_name"] = connectorName
	statusTask.Params["cdc_provider"] = cdcProvider
	statusTask.Operation = "cdc_status"
	statusResp := a.executeStreamingStatus(ctx, statusTask)

	// Surface both restart response and current status
	return ExecutorResponse{
		TaskID:        task.TaskID,
		PipelineID:    task.PipelineID,
		Status:        statusResp.Status,
		PipelineType:  "streaming",
		ConnectorName: connectorName,
		Result: map[string]interface{}{
			"connector_name": connectorName,
			"cdc_provider":   cdcProvider,
			"restart":        restartResp.Result,
			"status":         statusResp.Result,
		},
	}
}

// GetStreamingPipelines returns all tracked streaming pipelines
func (a *Agent) GetStreamingPipelines() map[string]*StreamingPipelineInfo {
	a.streamingPipelinesMu.RLock()
	defer a.streamingPipelinesMu.RUnlock()
	// Return a deep copy so callers can't observe races on shared pointers.
	copy := make(map[string]*StreamingPipelineInfo, len(a.streamingPipelines))
	for k, v := range a.streamingPipelines {
		if v == nil {
			continue
		}
		c := *v
		copy[k] = &c
	}
	return copy
}

// AddStreamingPipeline adds/overwrites pipeline info in a thread-safe way.
func (a *Agent) AddStreamingPipeline(pipelineID string, info *StreamingPipelineInfo) {
	a.streamingPipelinesMu.Lock()
	defer a.streamingPipelinesMu.Unlock()
	a.streamingPipelines[pipelineID] = info
}

// RemoveStreamingPipeline removes a pipeline from the tracker in a thread-safe way.
func (a *Agent) RemoveStreamingPipeline(pipelineID string) {
	a.streamingPipelinesMu.Lock()
	defer a.streamingPipelinesMu.Unlock()
	delete(a.streamingPipelines, pipelineID)
}

// getStreamingPipelineInfo safely gets info for a specific pipeline
func (a *Agent) getStreamingPipelineInfo(pipelineID string) (*StreamingPipelineInfo, bool) {
	a.streamingPipelinesMu.RLock()
	defer a.streamingPipelinesMu.RUnlock()
	info, exists := a.streamingPipelines[pipelineID]
	if !exists || info == nil {
		return nil, false
	}
	c := *info
	return &c, true
}

// updateStreamingPipelineStatus safely updates pipeline status
func (a *Agent) updateStreamingPipelineStatus(pipelineID, status, healthStatus string) {
	a.streamingPipelinesMu.Lock()
	defer a.streamingPipelinesMu.Unlock()
	if info, exists := a.streamingPipelines[pipelineID]; exists {
		info.Status = status
		info.HealthStatus = healthStatus
		info.LastHealthCheck = time.Now()
	}
}

// HealthCheckStreamingPipelines performs health checks on all streaming pipelines
//
// FULLY GENERIC: Uses CDC provider from tracked pipeline info
func (a *Agent) HealthCheckStreamingPipelines() {
	// Get a snapshot of pipelines to check
	a.streamingPipelinesMu.RLock()
	pipelinesToCheck := make([]struct {
		id   string
		info StreamingPipelineInfo
	}, 0, len(a.streamingPipelines))
	for pipelineID, info := range a.streamingPipelines {
		if info != nil && info.Status == "running" {
			c := *info
			pipelinesToCheck = append(pipelinesToCheck, struct {
				id   string
				info StreamingPipelineInfo
			}{pipelineID, c})
		}
	}
	a.streamingPipelinesMu.RUnlock()

	// Check each pipeline without holding the lock
	for _, p := range pipelinesToCheck {
		pipelineID := p.id
		info := p.info
		if info.Status != "running" {
			continue // Skip non-running pipelines
		}

		// Use CDC provider from tracked pipeline
		cdcProvider := info.ConnectorType
		if cdcProvider == "" {
			cdcProvider = "debezium" // Default fallback
		}

		// Execute status check via dynamic CDC provider
		req := mcp.ExecuteRequest{
			Connector: cdcProvider, // GENERIC: Uses provider from tracking
			Operation: "get_status",
			Config:    map[string]string{},
			Params: map[string]interface{}{
				"connector_name": info.ConnectorName,
			},
		}

		resp, err := a.mcpClient.Execute(req)
		if err != nil {
			log.Warnf("⚠️  Health check failed for %s (%s): %v", pipelineID, cdcProvider, err)
			a.updateStreamingPipelineStatus(pipelineID, info.Status, "unhealthy")
			continue
		}

		if !resp.Success {
			log.Warnf("⚠️  Health check failed for %s (%s): %s", pipelineID, cdcProvider, resp.Error)
			a.updateStreamingPipelineStatus(pipelineID, info.Status, "unhealthy")
			continue
		}

		// Update status (Debezium MCP wraps Kafka Connect status under result.data)
		connectorState := ""
		taskHealthy := true
		if resp.Result != nil {
			if d, ok := resp.Result["data"].(map[string]interface{}); ok && d != nil {
				if c, ok := d["connector"].(map[string]interface{}); ok && c != nil {
					if s, ok := c["state"].(string); ok {
						connectorState = s
					}
				}
				if tasks, ok := d["tasks"].([]interface{}); ok {
					for _, t := range tasks {
						if tm, ok := t.(map[string]interface{}); ok && tm != nil {
							if s, ok := tm["state"].(string); ok && s != "RUNNING" {
								taskHealthy = false
								break
							}
						}
					}
				}
			}
		}
		health := "healthy"
		if !(connectorState == "RUNNING" && taskHealthy) {
			health = "unhealthy"
		}
		// Thread-safe update (avoid mutating shared pointers without lock)
		a.updateStreamingPipelineStatus(pipelineID, info.Status, health)
	}
}

// getConnectionConfigForTask is the tenant-aware connection fetch used by all
// in-executor hydration paths. When the task carries a user_id in Params, we
// route through connections.Manager.GetForUser so a cross-tenant connection id
// (legitimate bug or malicious smuggling) fails closed instead of returning
// another user's secrets. Falls back to the tenant-blind Get only when the
// task pre-dates the user_id propagation — emits a warning so we can track
// and remove those callers over time.
func (a *Agent) getConnectionConfigForTask(ctx context.Context, task ExecutorTask, connID string) (map[string]string, error) {
	connID = strings.TrimSpace(connID)
	if connID == "" {
		return nil, fmt.Errorf("empty connection id")
	}
	if task.Params != nil {
		if uid, _ := task.Params["user_id"].(string); strings.TrimSpace(uid) != "" {
			return a.connectionMgr.GetForUser(ctx, uid, connID)
		}
	}
	log.Warnf("⚠️  getConnectionConfigForTask: tenant-blind fetch for connection %s — task missing user_id (pipeline=%s)", connID, task.PipelineID)
	return a.connectionMgr.Get(ctx, connID)
}

// TestConnection tests a connection by invoking the MCP connector's test/health functionality.
// If connectorVersion is empty, defaults to "latest".
func (a *Agent) TestConnection(ctx context.Context, connectorType string, connectorVersion string, config map[string]string) (bool, string) {
	traceID := telemetry.TraceIDFromContext(ctx)
	// SECURITY: Never log config values, only key count
	log.WithField("trace_id", traceID).Infof("Testing connection for connector: %s (config_keys=%d)", connectorType, len(config))
	if strings.TrimSpace(connectorVersion) == "" {
		connectorVersion = "latest"
	}

	// Convert config to interface{} map for passing as params
	configParams := make(map[string]interface{})
	for k, v := range config {
		configParams[k] = v
	}

	// Construct a test request - pass config as params so connector can test actual connection
	req := mcp.ExecuteRequest{
		Connector: connectorType,
		Version:   connectorVersion,
		Operation: "test_connection",
		Config:    config,
		Params: map[string]interface{}{
			"config": configParams, // Pass config as params so connector can test actual connection
		},
	}

	// For connection testing, use executeWithOAuthRetry (no retries on connection failures)
	// This provides fast feedback to users instead of waiting for 4× timeout retries.
	// OAuth refresh is still supported for API connectors that need it.
	resp, err := a.executeWithOAuthRetry(ctx, req)
	if err != nil {
		return false, fmt.Sprintf("Connection test failed: %v", err)
	}

	// Return the actual result from the connector
	if !resp.Success {
		errorMsg := resp.Error
		if errorMsg == "" {
			errorMsg = "Connection test failed - check credentials and network connectivity"
		}
		log.Warnf("test_connection failed: %s", errorMsg)
		return false, errorMsg
	}

	log.Infof("✅ Connection test passed for %s", connectorType)
	return true, ""
}

// TableMetadata represents discovered table schema
type TableMetadata struct {
	Name            string           `json:"name"`
	Schema          string           `json:"schema,omitempty"`
	Columns         []ColumnMetadata `json:"columns"`
	RowCount        int64            `json:"row_count,omitempty"`
	IsExactCount    bool             `json:"is_exact_count,omitempty"`
	PrimaryKeys     []string         `json:"primary_keys,omitempty"`
	ForeignKeys     []ForeignKeyMeta `json:"foreign_keys,omitempty"`
	Indexes         []IndexMeta      `json:"indexes,omitempty"`
	DiscoveryStatus string           `json:"discovery_status,omitempty"`
	DiscoveryError  string           `json:"discovery_error,omitempty"`
}

type ColumnMetadata struct {
	Name string `json:"name"`
	Type string `json:"type"`
	// SourceType is the source dialect's DECLARED type, qualifier included
	// ("varchar(50)", "decimal(8,2)", "int unsigned") — not the canonical token
	// in Type ("string", "number"). The connectors have always emitted it; until
	// B1 nothing on this side captured it, so the field was silently discarded at
	// unmarshal and every drift comparison ran on the canonical token alone. That
	// made narrowing WITHIN one canonical type invisible: VARCHAR(50)→VARCHAR(10)
	// reads as "string"→"string". Proven on prod — that exact narrowing produced
	// zero drift rows while a cross-type change on a sibling column in the same
	// run produced one. Consumed by diffSchemas via nativeTypeDrifted.
	SourceType   string `json:"source_type,omitempty"`
	Nullable     bool   `json:"nullable"`
	IsPrimaryKey bool   `json:"is_primary_key,omitempty"`
	IsForeignKey bool   `json:"is_foreign_key,omitempty"`
	IsIndexed    bool   `json:"is_indexed,omitempty"`
}

type ForeignKeyMeta struct {
	Column           string `json:"column"`
	ReferencesTable  string `json:"references_table"`
	ReferencesColumn string `json:"references_column"`
	ConstraintName   string `json:"constraint_name,omitempty"`
	OnDelete         string `json:"on_delete,omitempty"`
	OnUpdate         string `json:"on_update,omitempty"`
}

type IndexMeta struct {
	Name      string   `json:"name"`
	Columns   []string `json:"columns"`
	Unique    bool     `json:"unique"`
	Type      string   `json:"type,omitempty"`
	IsPrimary bool     `json:"is_primary,omitempty"`
}

// DiscoverSchemaEnvelope returns the raw discover_schema response map from an MCP connector.
// This preserves v2 fields like schema_version/database_version/warnings while remaining
// backward compatible (v1 connectors return {"success":true,"tables":[...]}).
func (a *Agent) DiscoverSchemaEnvelope(ctx context.Context, connectorType string, config map[string]interface{}, params map[string]interface{}) (map[string]interface{}, error) {
	traceID := telemetry.TraceIDFromContext(ctx)
	// SECURITY: Never log config values, only key count
	log.WithField("trace_id", traceID).Infof("🔍 Discovering schema (envelope) for connector: %s (config_keys=%d)", connectorType, len(config))

	// Convert config to string map for MCP
	stringConfig := make(map[string]string)
	for k, v := range config {
		stringConfig[k] = fmt.Sprintf("%v", v)
	}

	mergedParams := map[string]interface{}{
		"config":             stringConfig,
		"include_row_counts": true,
		"include_columns":    true,
		// Always request PKs so every caller (assessor, batch executor, UI explorer)
		// gets primary_keys populated without needing to pass the flag explicitly.
		"include_relationships": true,
		"max_tables":            100,
	}
	for k, v := range params {
		mergedParams[k] = v
	}

	req := mcp.ExecuteRequest{
		Connector: connectorType,
		Operation: "discover_schema",
		Config:    stringConfig,
		Params:    mergedParams,
	}

	resp, err := a.executeWithRetry(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("schema discovery failed: %w", err)
	}
	if !resp.Success {
		errorMsg := resp.Error
		if errorMsg == "" {
			errorMsg = "Schema discovery failed"
		}
		return nil, errors.New(errorMsg)
	}
	if resp.Result == nil {
		return nil, fmt.Errorf("schema discovery returned empty result")
	}
	return resp.Result, nil
}

// SampleRows fetches the first N rows of a table for UI preview.
//
// Generic: invokes the source connector's standard `export` operation
// with `limit=N`. Every connector (REST, GraphQL, DB) implements
// `export` as part of the canonical contract — this is the same code
// path the executor uses for real transfers, so the preview reflects
// what the pipeline will actually move.
//
// Before this method existed, the api-gateway sample handler only
// supported DB connectors (mysql, postgres) via direct
// database/sql.Open — every other connector returned HTTP 500. Users
// saw "HTTP 500" next to the eye icon for shopify-admin-graphql and
// other SaaS sources with no actionable guidance.
//
// Returns the raw `data` array plus the inferred `columns` list. The
// MCP `export` response shape is:
//
//	{success: bool, data: [{...}], columns: [...], row_count: int}
//
// Limit is capped at 100 for safety — preview UIs only need a handful
// of rows.
func (a *Agent) SampleRows(ctx context.Context, connectorType string, config map[string]interface{}, table string, limit int) ([]map[string]interface{}, []string, error) {
	traceID := telemetry.TraceIDFromContext(ctx)
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	log.WithField("trace_id", traceID).Infof("👁️ Sample rows for %s table=%s limit=%d (config_keys=%d)", connectorType, table, limit, len(config))

	stringConfig := make(map[string]string)
	for k, v := range config {
		stringConfig[k] = fmt.Sprintf("%v", v)
	}

	// Honour the connector_version pin from the connection record so
	// preview hits the same connector instance the pipeline will run
	// against.
	version := strings.TrimSpace(stringConfig["connector_version"])
	if version == "" {
		version = strings.TrimSpace(stringConfig["version"])
	}

	req := mcp.ExecuteRequest{
		Connector: connectorType,
		Version:   version,
		Operation: "export",
		Config:    stringConfig,
		Params: map[string]interface{}{
			// HITL-persisted form may carry a schema prefix
			// (e.g. "shopify.products"). The shopify connector strips
			// it; pass through verbatim and let the source canonicalise.
			"table":  table,
			"limit":  limit,
			"config": stringConfig,
		},
	}

	resp, err := a.executeWithRetry(ctx, req)
	if err != nil {
		return nil, nil, fmt.Errorf("sample_rows: export failed for %s: %w", connectorType, err)
	}
	if resp == nil || !resp.Success {
		errorMsg := ""
		if resp != nil {
			errorMsg = resp.Error
		}
		if errorMsg == "" {
			errorMsg = "Sample rows: export reported failure"
		}
		return nil, nil, errors.New(errorMsg)
	}
	if resp.Result == nil {
		return []map[string]interface{}{}, []string{}, nil
	}

	// Extract rows. The export contract returns `data: [...]` for
	// REST/GraphQL connectors and `records: [...]` for some legacy
	// connectors — accept both for forward compatibility.
	var rawRows []interface{}
	if v, ok := resp.Result["data"].([]interface{}); ok {
		rawRows = v
	} else if v, ok := resp.Result["records"].([]interface{}); ok {
		rawRows = v
	}

	rows := make([]map[string]interface{}, 0, len(rawRows))
	for _, r := range rawRows {
		if m, ok := r.(map[string]interface{}); ok {
			rows = append(rows, m)
		}
	}

	// Trim to the requested limit — some connectors return more than
	// asked (e.g. a full Relay page). The UI only renders a handful.
	if len(rows) > limit {
		rows = rows[:limit]
	}

	// Extract columns. Prefer the connector's explicit column list;
	// fall back to keys of the first row.
	columns := []string{}
	if v, ok := resp.Result["columns"].([]interface{}); ok {
		for _, c := range v {
			if s, ok := c.(string); ok && s != "" {
				columns = append(columns, s)
			}
		}
	}
	if len(columns) == 0 && len(rows) > 0 {
		for k := range rows[0] {
			columns = append(columns, k)
		}
	}

	return rows, columns, nil
}

// explorerQueryMaxRows caps how many rows a delegated Data Explorer query may
// return. Unlike SampleRows (a small preview capped at 100), the Explorer runs a
// user's SELECT and needs the full result set, so it uses a much larger cap.
const explorerQueryMaxRows = 10000

// ExplorerQuery runs a read-only SQL query for the Data Explorer through a
// connector's MCP `export` tool and returns {rows, columns}. It is the delegated
// counterpart to the api-gateway's direct-driver explorer executors, used for
// warehouses the gateway has no native driver for (e.g. BigQuery). The connector's
// `export` runs the SQL passed in params["query"]/["sql"] verbatim (single "query"
// page). The SELECT-only guard is enforced UPSTREAM in the api-gateway
// (validators.ValidateExplorerSQL) before this endpoint is ever reached; this method
// is reachable only via requirePrincipal (S2S secret / valid session).
//
// Unlike SampleRows this does NOT cap at 100 rows.
func (a *Agent) ExplorerQuery(ctx context.Context, connectorType string, config map[string]interface{}, query string, limit int) ([]map[string]interface{}, []string, error) {
	traceID := telemetry.TraceIDFromContext(ctx)
	if limit <= 0 {
		limit = 100
	}
	if limit > explorerQueryMaxRows {
		limit = explorerQueryMaxRows
	}
	// SECURITY: never log the query text or config values — only shape metadata.
	log.WithField("trace_id", traceID).Infof("🔎 Explorer query for %s limit=%d (config_keys=%d)", connectorType, limit, len(config))

	stringConfig := make(map[string]string)
	for k, v := range config {
		stringConfig[k] = fmt.Sprintf("%v", v)
	}
	version := strings.TrimSpace(stringConfig["connector_version"])
	if version == "" {
		version = strings.TrimSpace(stringConfig["version"])
	}

	req := mcp.ExecuteRequest{
		Connector: connectorType,
		Version:   version,
		Operation: "export",
		Config:    stringConfig,
		Params: map[string]interface{}{
			// Warehouse adapters (e.g. BigQuery) run params["query"]/["sql"] verbatim
			// in single-page "query" mode. Both keys are set for compatibility.
			"query":  query,
			"sql":    query,
			"limit":  limit,
			"config": stringConfig,
		},
	}

	resp, err := a.executeWithRetry(ctx, req)
	if err != nil {
		return nil, nil, fmt.Errorf("explorer_query: export failed for %s: %w", connectorType, err)
	}
	if resp == nil || !resp.Success {
		errorMsg := ""
		if resp != nil {
			errorMsg = resp.Error
		}
		if errorMsg == "" {
			errorMsg = "explorer_query: export reported failure"
		}
		return nil, nil, errors.New(errorMsg)
	}
	rows, columns := extractExportRowsColumns(resp.Result, limit)
	return rows, columns, nil
}

// ExplorerExecute runs an authorized write / DDL / destructive statement for the Data
// Explorer through a connector's MCP `execute` tool and returns the affected-row count
// (nil when the connector does not report one). It is the delegated counterpart to the
// api-gateway's direct-driver write executor, used for warehouses the gateway has no
// native driver for (e.g. BigQuery). Authorization (the workspace-role gate) and
// single-statement validation are enforced UPSTREAM in the api-gateway
// (validators.ValidateExplorerStatement) before this endpoint is reached; this method
// is reachable only via requirePrincipal (S2S secret / valid session). A connector that
// does not implement `execute` surfaces a clear error that the gateway relays verbatim.
func (a *Agent) ExplorerExecute(ctx context.Context, connectorType string, config map[string]interface{}, statement string) (*int64, error) {
	traceID := telemetry.TraceIDFromContext(ctx)
	// SECURITY: never log the statement text or config values — only shape metadata.
	log.WithField("trace_id", traceID).Infof("✍️  Explorer write for %s (config_keys=%d)", connectorType, len(config))

	stringConfig := make(map[string]string)
	for k, v := range config {
		stringConfig[k] = fmt.Sprintf("%v", v)
	}
	version := strings.TrimSpace(stringConfig["connector_version"])
	if version == "" {
		version = strings.TrimSpace(stringConfig["version"])
	}

	req := mcp.ExecuteRequest{
		Connector: connectorType,
		Version:   version,
		Operation: "execute",
		Config:    stringConfig,
		Params: map[string]interface{}{
			// Connectors read the statement from any of these keys for compatibility.
			"query":     statement,
			"sql":       statement,
			"statement": statement,
			"config":    stringConfig,
		},
	}

	resp, err := a.executeWithRetry(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("explorer_execute: execute failed for %s: %w", connectorType, err)
	}
	if resp == nil || !resp.Success {
		errorMsg := ""
		if resp != nil {
			errorMsg = resp.Error
		}
		if errorMsg == "" {
			errorMsg = "explorer_execute: execute reported failure"
		}
		return nil, errors.New(errorMsg)
	}
	return extractRowsAffected(resp.Result), nil
}

// extractRowsAffected pulls an affected-row count out of an MCP `execute` result,
// tolerating the common key spellings connectors use (JSON numbers decode to float64).
// Returns nil when no count is present.
func extractRowsAffected(result map[string]interface{}) *int64 {
	if result == nil {
		return nil
	}
	for _, key := range []string{"rows_affected", "affected_rows", "num_affected_rows", "row_count", "rowcount"} {
		v, ok := result[key]
		if !ok {
			continue
		}
		switch n := v.(type) {
		case float64:
			x := int64(n)
			return &x
		case int64:
			return &n
		case int:
			x := int64(n)
			return &x
		}
	}
	return nil
}

// extractExportRowsColumns normalizes an MCP `export` result into {rows, columns}.
// It accepts both the `data` and `records` row keys, trims to limit, and backfills
// columns from the first row when the connector omits an explicit column list.
func extractExportRowsColumns(result map[string]interface{}, limit int) ([]map[string]interface{}, []string) {
	if result == nil {
		return []map[string]interface{}{}, []string{}
	}
	var rawRows []interface{}
	if v, ok := result["data"].([]interface{}); ok {
		rawRows = v
	} else if v, ok := result["records"].([]interface{}); ok {
		rawRows = v
	}
	rows := make([]map[string]interface{}, 0, len(rawRows))
	for _, r := range rawRows {
		if m, ok := r.(map[string]interface{}); ok {
			rows = append(rows, m)
		}
	}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	columns := []string{}
	if v, ok := result["columns"].([]interface{}); ok {
		for _, c := range v {
			if s, ok := c.(string); ok && s != "" {
				columns = append(columns, s)
			}
		}
	}
	if len(columns) == 0 && len(rows) > 0 {
		for k := range rows[0] {
			columns = append(columns, k)
		}
	}
	return rows, columns
}

// isInternalDiscoveredTable reports whether a discovered table is one of rsync's
// own bookkeeping (`_rsync_*`, `rsync_*`) or pipeline-staging (`flat_mysql_*`,
// `flat_pg_*`, `flat_postgres_*`) tables. These land in user source/destination
// databases to track CDC offsets and pipeline state but are never user data, so
// they must not surface as HITL "select tables to sync" options. Matching is
// case-insensitive and tolerates a leading schema qualifier. Mirrors api-gateway
// isInternalExplorerTable and frontend src/lib/explorer/internalTables.ts.
func isInternalDiscoveredTable(name string) bool {
	n := strings.TrimSpace(name)
	if n == "" {
		return false
	}
	// Strip a leading schema qualifier so `schema._rsync_pipelines` matches too.
	if dot := strings.LastIndex(n, "."); dot >= 0 {
		n = n[dot+1:]
	}
	n = strings.ToLower(n)
	return strings.HasPrefix(n, "_rsync") ||
		strings.HasPrefix(n, "rsync_") ||
		strings.HasPrefix(n, "flat_mysql_") ||
		strings.HasPrefix(n, "flat_pg_") ||
		strings.HasPrefix(n, "flat_postgres_")
}

// filterInternalTables drops rsync-internal bookkeeping/staging tables from a
// discovered table list, preserving order. Applied at HITL table-selection sites
// so users never see (or accidentally sync) rsync's own `_rsync_*` tables. Note
// this is intentionally NOT applied inside DiscoverSchema itself — callers like
// connection validation must still see internal tables to judge a connection's
// reachability (an rsync destination may legitimately hold only `_rsync_*` rows).
func filterInternalTables(tables []TableMetadata) []TableMetadata {
	out := make([]TableMetadata, 0, len(tables))
	for _, t := range tables {
		if isInternalDiscoveredTable(t.Name) {
			continue
		}
		out = append(out, t)
	}
	return out
}

// DiscoverSchema discovers tables and schemas from a connection using MCP
func (a *Agent) DiscoverSchema(ctx context.Context, connectorType string, config map[string]interface{}) ([]TableMetadata, error) {
	traceID := telemetry.TraceIDFromContext(ctx)
	// SECURITY: Never log config values, only key count
	log.WithField("trace_id", traceID).Infof("🔍 Discovering schema for connector: %s (config_keys=%d)", connectorType, len(config))

	// Convert config to string map for MCP
	stringConfig := make(map[string]string)
	for k, v := range config {
		stringConfig[k] = fmt.Sprintf("%v", v)
	}

	// Respect pinned connector versions stored on connection records.
	// Many connectors evolve required config fields; using "latest" here can break
	// pinned-version pipelines and lead to confusing mismatches between discovery and export.
	version := strings.TrimSpace(stringConfig["connector_version"])
	if version == "" {
		version = strings.TrimSpace(stringConfig["version"])
	}

	// Construct a discover_schema request
	// The config needs to be in Params for the MCP connector to use it
	req := mcp.ExecuteRequest{
		Connector: connectorType,
		Version:   version,
		Operation: "discover_schema",
		Config:    stringConfig,
		Params: map[string]interface{}{
			"config":             stringConfig,
			"include_row_counts": true,
			// Needed for primary_keys (idempotent upserts and correct destination DDL).
			"include_relationships": true,
			// Needed for column type propagation to destination (ensures typed columns instead of TEXT).
			"include_columns": true,
		},
	}

	resp, err := a.executeWithRetry(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("schema discovery failed: %w", err)
	}

	if !resp.Success {
		errorMsg := resp.Error
		if errorMsg == "" {
			errorMsg = "Schema discovery failed"
		}
		return nil, errors.New(errorMsg)
	}

	// Parse the result into TableMetadata
	var tables []TableMetadata

	// The MCP connector should return tables in the result
	if tablesData, ok := resp.Result["tables"]; ok {
		// Convert to JSON and back to parse properly
		jsonData, err := json.Marshal(tablesData)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal tables data: %w", err)
		}

		if err := json.Unmarshal(jsonData, &tables); err != nil {
			return nil, fmt.Errorf("failed to parse tables data: %w", err)
		}
	} else {
		// Fallback: connector might return direct schema info
		log.Warn("No 'tables' key in response, attempting direct schema parse")
		return nil, fmt.Errorf("connector did not return table information")
	}

	// Surface the distinction between "discovery failed" and "discovery
	// succeeded but database is empty" — they look identical to the caller
	// and produce the same misleading "We couldn't list tables" UI message.
	if len(tables) == 0 {
		totalAvail, _ := resp.Result["total_tables_available"].(float64)
		log.Infof("ℹ️  discover_schema: %s returned 0 tables (total_available=%v) — database may be empty or contain no BASE TABLEs", connectorType, totalAvail)
	}

	log.Infof("✅ Discovered %d tables from %s", len(tables), connectorType)
	return tables, nil
}

// isRelationalDB checks if a connector type is a relational database
func isRelationalDB(connectorType string) bool {
	lower := strings.ToLower(strings.TrimSpace(connectorType))
	dbTypes := []string{"mysql", "postgresql", "postgres", "oracle", "sqlserver", "mariadb", "sqlite", "clickhouse"}
	for _, t := range dbTypes {
		if strings.Contains(lower, t) {
			return true
		}
	}
	return false
}

// classifyReloadDropResult decides what to do with a reload-mode drop_table result.
// A relational destination MUST be rebuilt from scratch on reload; if drop_table did
// not succeed, the subsequent batch would silently append duplicates or write into a
// stale-schema table — so we fail loud. Non-relational connectors that legitimately
// lack drop_table keep the historical warn-and-continue behavior.
//
// Returns (fatal, detail):
//   - fatal=true              → abort the run; `detail` is the underlying drop error.
//   - fatal=false, detail!="" → drop failed but the destination is not relational;
//     caller should warn and continue (reload won't rebuild).
//   - fatal=false, detail=""  → drop succeeded; no action.
func classifyReloadDropResult(destType string, resp *mcp.ExecuteResponse, err error) (bool, string) {
	dropFailed := false
	detail := ""
	if err != nil {
		dropFailed = true
		detail = err.Error()
	} else if resp == nil || !resp.Success {
		dropFailed = true
		if resp != nil {
			detail = resp.Error
		}
	}
	if !dropFailed {
		return false, ""
	}
	return isRelationalDB(destType), detail
}

// applyTransformsToData applies user-approved transforms to extracted data
func applyTransformsToData(ctx context.Context, database *sql.DB, executionID string, data []map[string]interface{}, task ExecutorTask, currentTable string) ([]map[string]interface{}, error) {
	// Extract transforms from HITL metadata
	var transformConfigs []map[string]interface{}

	// Try Params.metadata.transforms first
	if task.Params != nil {
		if metadata, ok := task.Params["metadata"].(map[string]interface{}); ok {
			if transformsRaw, ok := metadata["transforms"]; ok {
				if transformsList, ok := transformsRaw.([]interface{}); ok {
					for _, t := range transformsList {
						if tMap, ok := t.(map[string]interface{}); ok {
							transformConfigs = append(transformConfigs, tMap)
						}
					}
				}
			}
		}
	}

	// Fallback: Payload.metadata.transforms
	if len(transformConfigs) == 0 && task.Payload != nil {
		if metadata, ok := task.Payload["metadata"].(map[string]interface{}); ok {
			if transformsRaw, ok := metadata["transforms"]; ok {
				if transformsList, ok := transformsRaw.([]interface{}); ok {
					for _, t := range transformsList {
						if tMap, ok := t.(map[string]interface{}); ok {
							transformConfigs = append(transformConfigs, tMap)
						}
					}
				}
			}
		}
	}

	// Fallback: load producer transforms from the transform_definitions table.
	// The suggestions modal persists transforms there, but RunPipeline does not
	// inject them into the task payload — so without this, batch pipelines would
	// silently run UNTRANSFORMED (a PII-leak risk for masking suggestions).
	// Shape mirrors the sink's consumer-transform loader so NormalizeAndValidate
	// parses it identically.
	if len(transformConfigs) == 0 && database != nil && looksLikeUUID(strings.TrimSpace(task.PipelineID)) {
		pipelineID := strings.TrimSpace(task.PipelineID)
		dbRows, dbErr := database.QueryContext(ctx, `
			SELECT id::text, transform_order, transform_config, enabled
			FROM transform_definitions
			WHERE pipeline_id = $1 AND transform_type = 'producer'
			ORDER BY transform_order ASC
		`, pipelineID)
		if dbErr != nil {
			// Fail-closed: a DB error must not let data reach the destination
			// untransformed. Abort the export rather than silently skip masking.
			log.WithField("pipeline_id", pipelineID).Errorf("failed to load producer transforms: %v", dbErr)
			return nil, fmt.Errorf("load producer transforms: %w", dbErr)
		}
		defer dbRows.Close()
		for dbRows.Next() {
			var id string
			var order int
			var cfgJSON []byte
			var enabled bool
			if scanErr := dbRows.Scan(&id, &order, &cfgJSON, &enabled); scanErr != nil {
				// Structural scan failure → fail-closed (same rationale as above).
				log.WithField("pipeline_id", pipelineID).Errorf("scan producer transform row: %v", scanErr)
				return nil, fmt.Errorf("scan producer transform: %w", scanErr)
			}
			cfg := map[string]interface{}{}
			if len(cfgJSON) > 0 {
				if umErr := json.Unmarshal(cfgJSON, &cfg); umErr != nil {
					// A single malformed blob (likely a manual DB edit) is logged
					// and skipped; the remaining transforms still apply.
					log.WithField("pipeline_id", pipelineID).Warnf("malformed producer transform %s, skipping: %v", id, umErr)
					continue
				}
			}
			transformConfigs = append(transformConfigs, map[string]interface{}{
				"id":               strings.TrimSpace(id),
				"transform_order":  order,
				"enabled":          enabled,
				"transform_config": cfg,
			})
		}
		if len(transformConfigs) > 0 {
			log.WithField("pipeline_id", pipelineID).Infof("loaded %d producer transform(s) from transform_definitions", len(transformConfigs))
		}
	}

	if len(transformConfigs) == 0 {
		// No transforms configured, return data as-is
		return data, nil
	}

	log.Infof("🔄 Applying transforms to %d rows (table: %s)", len(data), currentTable)

	canonical, warnings, err := transforms.NormalizeAndValidate(transformConfigs, currentTable, transforms.NormalizeModeExecution)
	if err != nil {
		return nil, err
	}
	for _, w := range warnings {
		log.WithField("pipeline_id", task.PipelineID).Warnf("transform normalize warning: %s", w)
	}

	if len(canonical) == 0 {
		return data, nil
	}

	// Convert data to transforms.Row format
	rows := make([]transforms.Row, len(data))
	for i, row := range data {
		rows[i] = transforms.Row(row)
	}

	// Apply transforms using the transform engine
	tier1 := transforms.NewSimpleTransformEngine()
	tier2 := transforms.NewDuckDBTransformEngine()
	coordinator := transforms.NewTransformCoordinator(tier1, tier2)

	transformedRows := rows
	for _, t := range canonical {
		inRows := len(transformedRows)
		start := time.Now()
		stepOut, stepErr := coordinator.Apply(ctx, transformedRows, []transforms.Transform{t.EngineTransform()})
		dur := time.Since(start)

		outRows := 0
		if stepOut != nil {
			outRows = len(stepOut)
		}

		upsertTransformExecutionLog(ctx, database, task.PipelineID, executionID, currentTable, t, inRows, outRows, dur, stepErr)

		if stepErr != nil {
			return nil, fmt.Errorf("transform execution failed (%s): %w", t.Type, stepErr)
		}
		transformedRows = stepOut
	}

	// Convert back to []map[string]interface{}
	result := make([]map[string]interface{}, len(transformedRows))
	for i, row := range transformedRows {
		result[i] = map[string]interface{}(row)
	}

	log.Infof("✅ Transforms applied: %d rows in, %d rows out", len(data), len(result))
	return result, nil
}

func looksLikeUUID(s string) bool {
	s = strings.TrimSpace(s)
	if len(s) != 36 {
		return false
	}
	for _, pos := range []int{8, 13, 18, 23} {
		if s[pos] != '-' {
			return false
		}
	}
	return true
}

func upsertTransformExecutionLog(
	ctx context.Context,
	database *sql.DB,
	pipelineID string,
	executionID string,
	tableName string,
	t transforms.CanonicalTransform,
	inputRows int,
	outputRows int,
	duration time.Duration,
	stepErr error,
) {
	if database == nil {
		return
	}
	if strings.TrimSpace(pipelineID) == "" || strings.TrimSpace(executionID) == "" || strings.TrimSpace(tableName) == "" {
		return
	}
	if !looksLikeUUID(pipelineID) || !looksLikeUUID(executionID) {
		return
	}

	status := "success"
	var errMsg interface{} = nil
	if stepErr != nil {
		status = "failed"
		errMsg = stepErr.Error()
	}

	snap, mErr := json.Marshal(t)
	if mErr != nil || len(snap) == 0 {
		snap = []byte(`{}`)
	}
	durMs := duration.Milliseconds()
	if durMs < 0 {
		durMs = 0
	}

	_, err := database.ExecContext(ctx, `
		INSERT INTO transform_execution_logs (
			pipeline_id, execution_id, table_name,
			transform_id, transform_order, transform_type,
			status, error_message,
			input_rows, output_rows, duration_ms,
			config_snapshot, updated_at
		) VALUES (
			$1, $2, $3,
			$4, $5, $6,
			$7, $8,
			$9, $10, $11,
			$12, now()
		)
		ON CONFLICT (pipeline_id, execution_id, table_name, transform_order)
		DO UPDATE SET
			transform_id = EXCLUDED.transform_id,
			transform_type = EXCLUDED.transform_type,
			status = CASE WHEN EXCLUDED.status = 'failed' THEN 'failed' ELSE transform_execution_logs.status END,
			error_message = COALESCE(EXCLUDED.error_message, transform_execution_logs.error_message),
			input_rows = transform_execution_logs.input_rows + EXCLUDED.input_rows,
			output_rows = transform_execution_logs.output_rows + EXCLUDED.output_rows,
			duration_ms = transform_execution_logs.duration_ms + EXCLUDED.duration_ms,
			config_snapshot = EXCLUDED.config_snapshot,
			updated_at = now()
	`, pipelineID, executionID, tableName, strings.TrimSpace(t.ID), t.Order, t.Type, status, errMsg, inputRows, outputRows, durMs, string(snap))
	if err != nil {
		// Best-effort: never fail the pipeline due to missing migrations / logging issues.
		log.WithError(err).WithFields(log.Fields{
			"pipeline_id":     pipelineID,
			"execution_id":    executionID,
			"table_name":      tableName,
			"transform_type":  t.Type,
			"transform_order": t.Order,
		}).Warn("failed to write transform_execution_logs (best-effort)")
	}
}

// tableMatches checks if currentTable matches the configured table name
// (supports both qualified "schema.table" and unqualified "table" names)
func tableMatches(current, configured string) bool {
	current = strings.TrimSpace(current)
	configured = strings.TrimSpace(configured)

	if current == configured {
		return true
	}

	// Try unqualified match (e.g., "users" matches "public.users")
	currentParts := strings.Split(current, ".")
	configuredParts := strings.Split(configured, ".")

	currentName := currentParts[len(currentParts)-1]
	configuredName := configuredParts[len(configuredParts)-1]

	return strings.EqualFold(currentName, configuredName)
}

// Stop stops the executor agent
func (a *Agent) Stop() error {
	log.Info("Stopping Executor Agent...")

	// Stop heartbeat publisher
	if a.heartbeatPublisher != nil {
		a.heartbeatPublisher.Stop()
	}

	a.cancel()

	// Stop all MCP servers
	if err := a.mcpManager.StopAll(); err != nil {
		log.Errorf("Error stopping MCP servers: %v", err)
	}

	log.Info("✅ Executor Agent stopped")
	return nil
}
