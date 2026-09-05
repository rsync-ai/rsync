package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/smtp"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"api-gateway/internal/cache"
	"api-gateway/internal/db"
	"api-gateway/internal/safehttp"
	"api-gateway/internal/security"
	"api-gateway/internal/telemetry"
	"api-gateway/internal/validators"

	"github.com/rsync-ai/shared/crypto"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/rsync-ai/shared/pgdriver"
	_ "github.com/microsoft/go-mssqldb"
	log "github.com/sirupsen/logrus"
)

// Global explorer cache instance
var explorerCache *cache.ExplorerCache

// SetExplorerCache sets the global explorer cache instance
func SetExplorerCache(c *cache.ExplorerCache) {
	explorerCache = c
}

// =============================================================================
// SQL GENERATION (NL → SQL)
// =============================================================================

// SQLGenerateRequest represents a natural language to SQL request
type SQLGenerateRequest struct {
	Question string `json:"question" binding:"required"`
	// ConnectionID is optional; when present the dialect is derived from the
	// connection's connector_type instead of trusting the client / defaulting.
	ConnectionID string `json:"connection_id"`
	Dialect      string `json:"dialect"` // postgresql, mysql, etc. (default: postgresql)
	Schema       string `json:"schema"`  // Database schema context (tables + columns)
	Tables       string `json:"tables"`  // Comma-separated table names to focus on
	// New typed schema (preferred when present)
	TablesTyped []cache.ExplorerTableIndex `json:"tables_typed,omitempty"`
	ForeignKeys []map[string]interface{}   `json:"foreign_keys,omitempty"` // filled in Phase 2/3; keep flexible for now
	MaxTokens   int                        `json:"max_tokens"`
	Temperature float64                    `json:"temperature"`
}

// SQLGenerateResponse represents the SQL generation response
type SQLGenerateResponse struct {
	SQL         string   `json:"sql"`
	Explanation string   `json:"explanation,omitempty"`
	Confidence  float64  `json:"confidence,omitempty"`
	Tables      []string `json:"tables,omitempty"`
	Warnings    []string `json:"warnings,omitempty"`
}

// dialectFromConnectorType maps a connection's connector_type to the SQL
// dialect the text2sql service expects. Mirrors the isMySQL/isPostgres checks
// in ExecuteExplorerQuery. Returns "" for types with no SQL dialect (e.g. SaaS
// sources), in which case the caller keeps the client-provided/default dialect.
func dialectFromConnectorType(connectorType string) string {
	ct := strings.ToLower(connectorType)
	switch {
	case strings.Contains(ct, "mysql") || strings.Contains(ct, "mariadb"):
		return "mysql"
	case strings.Contains(ct, "postgres") || strings.Contains(ct, "redshift"):
		return "postgresql"
	case strings.Contains(ct, "databricks"):
		return "databricks"
	case strings.Contains(ct, "sqlserver") || strings.Contains(ct, "mssql"):
		return "tsql"
	case strings.Contains(ct, "clickhouse"):
		// text2sql (llm-service compiler.py) supports the "clickhouse" sqlglot dialect.
		return "clickhouse"
	default:
		return ""
	}
}

// GenerateSQL handles POST /api/v1/sql/generate
// Proxies to internal Text2SQL endpoint or uses LLM service
func GenerateSQL(c *gin.Context) {
	var req SQLGenerateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Query-limit enforcement (Ship 3 Phase 2). Refuse generation when the ACTIVE
	// workspace is over its monthly NL→SQL allowance — BEFORE building/sending the
	// text2sql request, so a blocked workspace pays no LLM cost. Fail-open: empty
	// workspace / nil DB / query error all fall through (checkWorkspaceQueryOK →
	// resolvePlanQuota treats them as unlimited). Direct SQL is never gated here.
	if allowed, errBody := checkWorkspaceQueryOK(c.Request.Context(), db.GetDB(), activeWorkspaceID(c)); !allowed {
		c.JSON(http.StatusPaymentRequired, errBody)
		return
	}

	// When a connection_id is supplied, derive the dialect authoritatively from
	// the connection's type. The Explorer UI already sends the correct dialect,
	// but any caller that omits or mis-sends it would otherwise get SQL in the
	// wrong dialect (the old code blindly defaulted to postgresql). Best-effort:
	// if auth/connection lookup is unavailable we fall through to the
	// client-provided dialect, so this never breaks an existing caller.
	if req.ConnectionID != "" {
		if wsID := activeWorkspaceID(c); wsID != "" {
			if database := db.GetDB(); database != nil {
				var connectorType string
				if err := database.QueryRow(
					`SELECT connector_type FROM connections WHERE id = $1 AND workspace_id = $2`,
					req.ConnectionID, wsID,
				).Scan(&connectorType); err == nil {
					if d := dialectFromConnectorType(connectorType); d != "" {
						req.Dialect = d
					}
				}
			}
		}
	}

	// Default dialect to postgresql
	if req.Dialect == "" {
		req.Dialect = "postgresql"
	}

	// Without schema context the generator guesses table/column names and can
	// emit SQL that fails at execution (e.g. `active` vs `is_active`). Surface a
	// warning so the failure is visible rather than silently wrong.
	var schemaWarnings []string
	if strings.TrimSpace(req.Schema) == "" && len(req.TablesTyped) == 0 {
		schemaWarnings = append(schemaWarnings,
			"No schema context provided — generated SQL may reference incorrect table or column names. Pass `schema` or `tables_typed` for accurate results.")
	}

	// Get internal Text2SQL endpoint URL
	text2sqlURL := os.Getenv("TEXT2SQL_ENDPOINT")
	if text2sqlURL == "" {
		// Fallback to LLM service if no dedicated Text2SQL endpoint
		text2sqlURL = os.Getenv("LLM_SERVICE_URL")
		if text2sqlURL == "" {
			text2sqlURL = "http://llm-service:5000"
		}
		text2sqlURL = text2sqlURL + "/api/v1/sql/generate"
	}

	// Build request for Text2SQL endpoint
	text2sqlReq := map[string]interface{}{
		"question": req.Question,
		"dialect":  req.Dialect,
		"schema":   req.Schema,
	}
	if req.Tables != "" {
		text2sqlReq["tables"] = req.Tables
	}
	if len(req.TablesTyped) > 0 {
		text2sqlReq["tables_typed"] = req.TablesTyped
	}
	if len(req.ForeignKeys) > 0 {
		text2sqlReq["foreign_keys"] = req.ForeignKeys
	}
	if req.MaxTokens > 0 {
		text2sqlReq["max_tokens"] = req.MaxTokens
	}
	if req.Temperature > 0 {
		text2sqlReq["temperature"] = req.Temperature
	}

	reqBody, _ := json.Marshal(text2sqlReq)

	// Create request with timeout (offline Text2SQL can be slow for complex multi-table queries)
	ctx, cancel := context.WithTimeout(c.Request.Context(), 120*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, "POST", text2sqlURL, bytes.NewBuffer(reqBody))
	if err != nil {
		log.Errorf("[GenerateSQL] Failed to create request: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create request"})
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")

	// Propagate trace headers
	for k, v := range telemetry.InjectTraceToHeaders(ctx) {
		httpReq.Header.Set(k, v)
	}

	// Execute request
	client := &http.Client{Timeout: 130 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		log.Errorf("[GenerateSQL] Text2SQL request failed: %v", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "SQL generation service unavailable",
			"hint":  "Ensure TEXT2SQL_ENDPOINT or LLM_SERVICE_URL is configured",
		})
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		log.Errorf("[GenerateSQL] Text2SQL returned %d: %s", resp.StatusCode, string(body))
		c.JSON(resp.StatusCode, gin.H{
			"error":   "SQL generation failed",
			"details": string(body),
		})
		return
	}

	// Parse response
	var sqlResp SQLGenerateResponse
	if err := json.Unmarshal(body, &sqlResp); err != nil {
		// Try to extract SQL from raw response
		var rawResp map[string]interface{}
		if json.Unmarshal(body, &rawResp) == nil {
			if sql, ok := rawResp["sql"].(string); ok {
				sqlResp.SQL = sql
			}
			if sql, ok := rawResp["query"].(string); ok && sqlResp.SQL == "" {
				sqlResp.SQL = sql
			}
			if expl, ok := rawResp["explanation"].(string); ok {
				sqlResp.Explanation = expl
			}
		}
	}

	if sqlResp.SQL == "" {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":        "No SQL generated",
			"raw_response": string(body),
		})
		return
	}

	// Prepend any schema-context warning so callers (and the UI) can surface it.
	if len(schemaWarnings) > 0 {
		sqlResp.Warnings = append(schemaWarnings, sqlResp.Warnings...)
	}

	// Meter one NL→SQL query against the workspace's monthly allowance (the metered
	// "queries/month" dimension the pricing page sells). Counted once per SUCCESSFUL
	// generation — reached only after the non-empty-SQL guard above, so failed/empty
	// generations burn nothing — and exactly once regardless of the llm-service's
	// internal retry/repair fan-out, because the gateway sees one request/response.
	// Direct SQL (/explorer/query) has no LLM call and is never metered. Best-effort +
	// fail-open: a metering hiccup must never fail a generation the user already got.
	recordNLQueryUsage(c, req.ConnectionID)

	c.JSON(http.StatusOK, sqlResp)
}

// recordNLQueryUsage increments the active workspace's monthly NL→SQL query counter
// (display meter of record for the query dimension) and appends a ledger row. Keyed
// on the ACTIVE (billable) workspace. Fail-open on every hop: no workspace / nil DB /
// query error → count nothing, never error. The INSERT + charge run in one round-trip
// (data-modifying CTE) so the ledger row and the counter stay consistent.
func recordNLQueryUsage(c *gin.Context, connectionID string) {
	wsID := activeWorkspaceID(c)
	if wsID == "" {
		return // fail-open: never attribute an unscoped query to the wrong tenant
	}
	database := db.GetDB()
	if database == nil {
		return
	}
	// user_id / connection_id are UUID columns — pass NULL (not "") when absent.
	var uid, cid interface{}
	if userID := c.GetString("user_id"); userID != "" {
		uid = userID
	}
	if connectionID != "" {
		cid = connectionID
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	if _, err := database.ExecContext(ctx,
		`WITH ins AS (
		   INSERT INTO nl_query_usage_events (workspace_id, user_id, connection_id)
		   VALUES ($1, $2, $3)
		   RETURNING workspace_id
		 )
		 SELECT charge_workspace_queries(workspace_id) FROM ins`,
		wsID, uid, cid,
	); err != nil {
		log.WithError(err).Warnf("nl_query_usage: meter failed for workspace %s; ignoring", wsID)
	}
}

// =============================================================================
// QUERY EXECUTION (Safe SELECT-only)
// =============================================================================

// ExplorerQueryRequest represents a query execution request
type ExplorerQueryRequest struct {
	ConnectionID string `json:"connection_id" binding:"required"`
	SQL          string `json:"sql" binding:"required"`
	Limit        int    `json:"limit"` // Max rows to return (default: 100, max: 500)
}

// ExplorerQueryResponse represents the query execution response
type ExplorerQueryResponse struct {
	Columns         []string                 `json:"columns"`
	Rows            []map[string]interface{} `json:"rows"`
	RowCount        int                      `json:"row_count"`
	ExecutionTimeMs int64                    `json:"execution_time_ms"`
	Truncated       bool                     `json:"truncated"`
	Warnings        []string                 `json:"warnings,omitempty"`
	// RowsAffected is set only for write statements (INSERT/UPDATE/DELETE/DDL) and
	// carries the number of rows the statement changed. nil for reads and for engines
	// that don't report an affected-row count. Pointer so reads omit it entirely.
	RowsAffected *int64 `json:"rows_affected,omitempty"`
	// StatementType echoes the classified statement (SELECT, INSERT, DROP, …) so the
	// UI can render a write result ("42 rows affected") vs a result grid.
	StatementType string `json:"statement_type,omitempty"`
}

// queryViaOrchestrator runs an Explorer statement through the orchestrator's
// /api/v1/agent/explorer-query endpoint, which invokes the connector's MCP tool.
// Used for delegated engines (data warehouses such as BigQuery) the gateway has no
// native driver for. When isWrite is false the orchestrator runs the connector's
// `export` (rows-returning) tool; when true it runs `execute` (no-rows write) and
// returns an affected-row count. The SQL has ALREADY passed the gateway's role-aware
// statement guard (validators.ValidateExplorerStatement in ExecuteExplorerQuery) and
// the connection was loaded workspace-scoped before this is called — the delegated
// path adds no new trust boundary beyond the existing direct-driver path.
func queryViaOrchestrator(ctx context.Context, connectorType string, config map[string]interface{}, sqlQuery string, limit int, redact bool, isWrite bool) (*ExplorerQueryResponse, error) {
	orchestratorURL := strings.TrimRight(os.Getenv("ORCHESTRATOR_URL"), "/")
	if orchestratorURL == "" {
		orchestratorURL = "http://orchestrator:8080"
	}

	reqBody, err := json.Marshal(map[string]interface{}{
		"connector_type": connectorType,
		"config":         config,
		"query":          sqlQuery,
		"limit":          limit,
		// write=true tells the orchestrator to run the connector's MCP `execute`
		// (no-rows) tool instead of `export`. Older orchestrators ignore the field and
		// keep read behavior — writes then surface a clear "not supported" error.
		"write": isWrite,
	})
	if err != nil {
		return nil, fmt.Errorf("explorer_query: build request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		orchestratorURL+"/api/v1/agent/explorer-query",
		bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("explorer_query: build http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	setInternalServiceSecret(httpReq)

	// Delegated warehouse queries can be slower than a direct-driver preview; give the
	// connector's MCP export (30s ceiling) headroom without hanging the UI.
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("explorer_query: orchestrator unreachable: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		// Surface the orchestrator's error details verbatim so ClassifyExecutionError
		// upstream can give the user something specific.
		var errEnv struct {
			Error   string `json:"error"`
			Details string `json:"details"`
		}
		_ = json.Unmarshal(body, &errEnv)
		msg := strings.TrimSpace(errEnv.Details)
		if msg == "" {
			msg = strings.TrimSpace(errEnv.Error)
		}
		if msg == "" {
			msg = fmt.Sprintf("orchestrator returned %d", resp.StatusCode)
		}
		return nil, fmt.Errorf("%s", msg)
	}

	var envelope struct {
		Rows         []map[string]interface{} `json:"rows"`
		Columns      []string                 `json:"columns"`
		RowCount     int                      `json:"row_count"`
		RowsAffected *int64                   `json:"rows_affected"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("explorer_query: decode response: %w", err)
	}

	// Write path: the orchestrator ran the connector's `execute` tool. Return the
	// affected-row count (no rows/columns to scan or redact).
	if isWrite {
		return &ExplorerQueryResponse{
			Columns:      []string{},
			Rows:         []map[string]interface{}{},
			RowsAffected: envelope.RowsAffected,
		}, nil
	}

	// Backfill columns from the first row if the connector didn't emit them.
	if len(envelope.Columns) == 0 && len(envelope.Rows) > 0 {
		for k := range envelope.Rows[0] {
			envelope.Columns = append(envelope.Columns, k)
		}
	}

	// Apply the same PII-by-column-name redaction the direct-driver preview path uses
	// (executeXQueryWithRedact) so delegated warehouse previews are consistent. Export
	// callers pass redact=false — the data owner sees raw values (T2-7).
	if redact {
		for _, row := range envelope.Rows {
			for col, val := range row {
				if shouldRedactColumnName(col) {
					row[col] = redactForPreview(val)
				}
			}
		}
	}

	return &ExplorerQueryResponse{
		Columns:   envelope.Columns,
		Rows:      envelope.Rows,
		RowCount:  envelope.RowCount,
		Truncated: limit > 0 && envelope.RowCount >= limit,
	}, nil
}

// ExecuteExplorerQuery handles POST /api/v1/explorer/query
// Executes SELECT-only queries against PostgreSQL connections
func ExecuteExplorerQuery(c *gin.Context) {
	_, ok := resolveUserID(c)
	if !ok {
		return
	}

	var req ExplorerQueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Validate and sanitize limit
	if req.Limit <= 0 {
		req.Limit = 100
	}
	if req.Limit > 500 {
		req.Limit = 500
	}

	// Validate SQL and authorize the statement class against the caller's workspace
	// role. Reads (SELECT/WITH) stay open to any member; writes require admin, and
	// destructive statements (DROP/TRUNCATE) require owner. See
	// validators.ValidateExplorerStatement for the full matrix.
	callerRole := security.WorkspaceRole(c.GetString(ctxWorkspaceRole))
	validation := validators.ValidateExplorerStatement(req.SQL, callerRole)
	if !validation.Valid {
		// A role shortfall is a 403 (authenticated but not permitted); everything else
		// (bad SQL, blocked statement class, multiple statements) is a 400.
		status := http.StatusBadRequest
		hint := "Explorer supports SELECT reads for all members; INSERT/UPDATE/DELETE and DDL require the admin role, DROP/TRUNCATE and ALTER … DROP require owner."
		if validation.ErrorCode == validators.ErrCodeInsufficientRole {
			status = http.StatusForbidden
		}
		c.JSON(status, gin.H{
			"error":      validation.ErrorMessage,
			"error_code": validation.ErrorCode,
			"hint":       hint,
		})
		return
	}
	// Classify on the statement text (not just its verb) so this agrees with the class
	// ValidateExplorerStatement already authorized above — see ClassifyStatementSQL.
	stmtClass := validators.ClassifyStatementSQL(req.SQL)
	isWrite := validators.IsWriteClass(stmtClass)

	// Get connection details
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	}

	var connectorType, configEncrypted string
	err := database.QueryRow(`
		SELECT connector_type, config
		FROM connections
		WHERE id = $1 AND workspace_id = $2
	`, req.ConnectionID, activeWorkspaceID(c)).Scan(&connectorType, &configEncrypted)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}
	if err != nil {
		log.Errorf("[ExecuteExplorerQuery] Failed to load connection: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load connection"})
		return
	}

	// Explorer support is gated by the capability resolver (single source of truth):
	// direct-driver SQL engines (PostgreSQL/Redshift, MySQL/MariaDB, Databricks, SQL
	// Server) + delegated warehouses (BigQuery, run through the connector's MCP export
	// tool via the orchestrator). Unsupported connectors 400 here.
	ct := strings.ToLower(connectorType)
	ecap := ResolveExplorerCapability(connectorType)
	if !ecap.Supported {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("Unsupported connection type: %s", connectorType),
			"hint":  "Explorer currently supports PostgreSQL/Redshift, MySQL, Databricks, SQL Server, and BigQuery",
		})
		return
	}
	isPostgres := strings.Contains(ct, "postgres") || strings.Contains(ct, "redshift")
	isMySQL := strings.Contains(ct, "mysql") || strings.Contains(ct, "mariadb")
	isSQLServer := strings.Contains(ct, "sqlserver") || strings.Contains(ct, "mssql")

	// Decrypt config
	configJSON, err := crypto.DecryptString(configEncrypted)
	if err != nil {
		log.Errorf("[ExecuteExplorerQuery] Failed to decrypt config: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decrypt connection config"})
		return
	}

	var config map[string]interface{}
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse connection config"})
		return
	}

	// Execute the statement. Delegated engines (BigQuery) route through the
	// orchestrator's MCP tool; direct-driver engines run in-gateway, routed by
	// connector type. Reads scan rows (rows-returning "query"); writes run through the
	// no-rows "execute" path and report an affected-row count.
	var result *ExplorerQueryResponse
	switch {
	case ecap.ExecStrategy == execDelegated:
		// The delegated executor picks export (read) vs execute (write) from isWrite.
		result, err = queryViaOrchestrator(c.Request.Context(), connectorType, config, req.SQL, req.Limit, true, isWrite)
	case isWrite && isPostgres:
		result, err = executeDirectWrite(c.Request.Context(), "postgres", postgresExplorerDSN(config), req.SQL)
	case isWrite && isMySQL:
		result, err = executeDirectWrite(c.Request.Context(), "mysql", mysqlExplorerDSN(config), req.SQL)
	case isWrite && isSQLServer:
		result, err = executeDirectWrite(c.Request.Context(), "sqlserver", sqlServerExplorerDSN(config), req.SQL)
	case isWrite:
		result, err = executeDatabricksWrite(c.Request.Context(), config, req.SQL)
	case isPostgres:
		result, err = executePostgresQuery(c.Request.Context(), config, req.SQL, req.Limit)
	case isMySQL:
		result, err = executeMySQLQuery(c.Request.Context(), config, req.SQL, req.Limit)
	case isSQLServer:
		result, err = executeSQLServerQuery(c.Request.Context(), config, req.SQL, req.Limit)
	default:
		result, err = executeDatabricksQuery(c.Request.Context(), config, req.SQL, req.Limit)
	}
	if err != nil {
		log.Errorf("[ExecuteExplorerQuery] Query execution failed: %v", err)
		// Classify the error for better user feedback + an accurate HTTP status: a
		// user's SQL typo or stale schema reference is a client error (4xx), not a
		// 500. Genuine infra failures stay 5xx.
		errorType, suggestion := validators.ClassifyExecutionError(err)
		status := http.StatusInternalServerError
		switch errorType {
		case "syntax_error", "missing_table_or_column":
			status = http.StatusBadRequest
		case "permission_denied":
			status = http.StatusForbidden
		case "timeout":
			status = http.StatusGatewayTimeout
		case "network_or_unavailable":
			status = http.StatusBadGateway
		}
		c.JSON(status, gin.H{
			"error":      fmt.Sprintf("Query execution failed: %v", err),
			"error_type": errorType,
			"suggestion": suggestion,
		})
		return
	}

	// Echo the classified statement type so the UI can render a write outcome
	// ("N rows affected") instead of an (empty) result grid.
	if result != nil {
		result.StatementType = validation.StatementType
	}

	// Writes are attributable actions on customer data — record an audit entry.
	if isWrite {
		auditExplorerWrite(c, req.ConnectionID, connectorType, validation.StatementType, req.SQL, result)
	}

	c.JSON(http.StatusOK, result)
}

// auditExplorerWrite records an executed Explorer write to audit_logs for
// attributability (who ran which write, against which connection, and how many rows it
// changed). Best-effort — logAudit never fails the request. The raw statement is stored
// truncated so an admin can see exactly what ran; audit_logs is admin-only and is never
// sent to an LLM, so no scrubbing is required here.
func auditExplorerWrite(c *gin.Context, connectionID, connectorType, statementType, sqlText string, result *ExplorerQueryResponse) {
	details := map[string]interface{}{
		"connector_type": connectorType,
		"statement_type": statementType,
		"sql":            truncateForAudit(sqlText, 2000),
		"workspace_role": c.GetString(ctxWorkspaceRole),
	}
	if result != nil && result.RowsAffected != nil {
		details["rows_affected"] = *result.RowsAffected
	}
	logAudit(c, "explorer.write", "connection", connectionID, details)
}

// truncateForAudit bounds an audit string so a pathological statement can't bloat the
// audit_logs row. Appends an ellipsis marker when it cuts.
func truncateForAudit(s string, max int) string {
	s = strings.TrimSpace(s)
	if len(s) > max {
		return s[:max] + "…(truncated)"
	}
	return s
}

// executeDirectWrite runs an authorized write / DDL / destructive statement against a
// database/sql engine (PostgreSQL/Redshift, MySQL, SQL Server) via ExecContext and
// returns the affected-row count. Authorization (the workspace-role gate) and
// single-statement validation already ran in validators.ValidateExplorerStatement;
// this path applies no LIMIT and scans no rows. Writes get a longer deadline than the
// read preview since DDL and bulk DML can outlast a SELECT sample.
func executeDirectWrite(ctx context.Context, driver, dsn, sqlText string) (*ExplorerQueryResponse, error) {
	execCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(0)
	conn.SetConnMaxLifetime(35 * time.Second)

	if err := conn.PingContext(execCtx); err != nil {
		return nil, fmt.Errorf("connection ping failed: %w", err)
	}

	start := time.Now()
	res, err := conn.ExecContext(execCtx, sqlText)
	if err != nil {
		return nil, fmt.Errorf("statement failed: %w", err)
	}

	resp := &ExplorerQueryResponse{
		Columns:         []string{},
		Rows:            []map[string]interface{}{},
		ExecutionTimeMs: time.Since(start).Milliseconds(),
	}
	if reportsRowsAffected(validators.ClassifyStatementSQL(sqlText)) {
		if n, err := res.RowsAffected(); err == nil {
			resp.RowsAffected = &n
		}
	}
	return resp, nil
}

// reportsRowsAffected answers whether a statement class has a meaningful affected-row
// count to show the user. Only DML does.
//
// DDL and DROP/TRUNCATE return 0 from every driver's RowsAffected() because they affect
// no rows by definition — but the UI rendered that as "DROP succeeded — 0 rows affected",
// which reads like nothing happened at the exact moment a table was destroyed. Leaving
// the field nil makes the UI fall back to "The statement completed successfully."
//
// ClassUnknown is excluded on purpose: if we can't classify the statement we can't
// vouch for the number either.
func reportsRowsAffected(class validators.StatementClass) bool {
	return class == validators.ClassDMLWrite
}

// executeDatabricksWrite runs an authorized write statement through the Databricks SQL
// Statement Execution API (the same endpoint the read path uses), without a LIMIT
// clamp. Databricks does not surface a portable affected-row count on this endpoint,
// so RowsAffected is left nil.
func executeDatabricksWrite(ctx context.Context, config map[string]interface{}, sqlText string) (*ExplorerQueryResponse, error) {
	host := normalizeDatabricksHost(getStringConfig(config, "host", "server_hostname", "workspace_host"))
	token := getStringConfig(config, "access_token", "token", "pat")
	warehouseID := getStringConfig(config, "warehouse_id", "warehouseId", "sql_warehouse_id")
	httpPath := getStringConfig(config, "http_path")
	if warehouseID == "" {
		warehouseID = parseWarehouseIDFromHTTPPath(httpPath)
	}
	catalog := getStringConfig(config, "catalog", "database")
	schemaName := getStringConfig(config, "schema", "schema_name")

	execCtx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()

	start := time.Now()
	resp, err := databricksExecuteStatement(execCtx, host, token, warehouseID, sqlText, catalog, schemaName)
	if err != nil {
		return nil, err
	}
	if resp.Status.State != "" && strings.ToUpper(resp.Status.State) != "SUCCEEDED" {
		msg := strings.TrimSpace(resp.Status.Error.Message)
		if msg == "" {
			msg = fmt.Sprintf("statement status=%s", resp.Status.State)
		}
		return nil, fmt.Errorf("%s", msg)
	}
	return &ExplorerQueryResponse{
		Columns:         []string{},
		Rows:            []map[string]interface{}{},
		ExecutionTimeMs: time.Since(start).Milliseconds(),
	}, nil
}

// executePostgresQuery executes a SELECT query against PostgreSQL with
// PII redaction applied (preview path). Customer-facing query UI uses
// this to keep masked emails / phones / SSNs out of result previews.
func executePostgresQuery(ctx context.Context, config map[string]interface{}, sqlQuery string, limit int) (*ExplorerQueryResponse, error) {
	return executePostgresQueryWithRedact(ctx, config, sqlQuery, limit, true)
}

// executePostgresQueryUnredacted executes a SELECT against PostgreSQL
// WITHOUT applying PII redaction. Use ONLY when the caller is the data
// owner exporting their own data (e.g. ExportCSVHandler), and the
// export endpoint itself has already enforced workspace-scoped ownership
// on the connection (explorer.go:2687). T2-7.
func executePostgresQueryUnredacted(ctx context.Context, config map[string]interface{}, sqlQuery string, limit int) (*ExplorerQueryResponse, error) {
	return executePostgresQueryWithRedact(ctx, config, sqlQuery, limit, false)
}

// isLocalDBHost reports whether host points at a local/dev or docker-internal
// database where TLS is typically not configured. Empty and explicit loopback
// names are local. IP LITERALS are classified by address (loopback/RFC1918/ULA/
// link-local/CGNAT → local), so an IPv6 literal or a public IPv4 literal is
// correctly treated remote instead of by a textual "." heuristic that missed
// them. Non-literal hostnames keep the dotless=local heuristic (docker service
// names). NOTE residual: a dotless single-label hostname that resolves to a
// PUBLIC address is still treated local — closing that needs address resolution
// with an injectable resolver, tracked as a follow-up; the address-based literal
// check plus verify-by-default for dotted remotes covers the realistic surface.
func isLocalDBHost(host string) bool {
	h := strings.ToLower(strings.TrimSpace(host))
	h = strings.Trim(h, "[]") // tolerate bracketed IPv6 literals
	if h == "" {
		return true
	}
	switch h {
	case "localhost", "127.0.0.1", "::1", "host.docker.internal":
		return true
	}
	if ip := net.ParseIP(h); ip != nil {
		return isPrivateOrLoopbackIP(ip)
	}
	// Non-literal hostname: dotless single-label names are docker-internal/local
	// DNS; any dotted hostname is remote so callers default to verified TLS.
	return !strings.Contains(h, ".")
}

// isPrivateOrLoopbackIP reports whether ip is a loopback, RFC1918/ULA private,
// link-local, or CGNAT (100.64.0.0/10) address — i.e. NOT a public destination.
func isPrivateOrLoopbackIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	if v4 := ip.To4(); v4 != nil && v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127 {
		return true // 100.64.0.0/10 carrier-grade NAT
	}
	return false
}

// resolvePostgresSSLMode picks the pgx/libpq sslmode for an outbound PostgreSQL
// connection. An explicit config value wins (ssl_mode or sslmode key, nil-safe
// and trimmed). Otherwise the default is host-based: local/docker-internal hosts
// get "disable" (dev/e2e Postgres rarely runs TLS), while any remote host — e.g.
// Azure Managed PostgreSQL — defaults to "verify-full": encrypt AND verify the
// server certificate + hostname against the image trust store (Dockerfile ships
// ca-certificates covering the standard cloud roots). Verifying by default closes
// the MITM window the previous "require" (encrypt-without-verify) default left
// open on customer-DB traffic. A server whose CA is not in the trust store
// (self-managed / some RDS / GCP) opts out by setting ssl_mode=require on the
// connection — which still encrypts, just without CA/hostname verification.
func resolvePostgresSSLMode(config map[string]interface{}, host string) string {
	for _, key := range []string{"ssl_mode", "sslmode"} {
		if v, ok := config[key]; ok && v != nil {
			if s := strings.TrimSpace(fmt.Sprint(v)); s != "" {
				return s
			}
		}
	}
	if isLocalDBHost(host) {
		return "disable"
	}
	return "verify-full"
}

// resolveMySQLTLSMode picks the go-sql-driver/mysql DSN "tls" parameter for an
// outbound MySQL connection. It mirrors resolvePostgresSSLMode: an explicit
// config value wins (ssl_mode, sslmode, tls, or tls_mode key, nil-safe and
// trimmed) and is normalised to a value the driver accepts; otherwise the
// default is host-based. Local/docker-internal hosts get "false" (dev/e2e
// MySQL rarely runs TLS), while any remote host — e.g. Azure Database for
// MySQL — defaults to "true": encrypt AND verify the server certificate against
// the image trust store (the Dockerfile ships ca-certificates, covering the
// standard cloud roots incl. Azure's DigiCert/Microsoft chain). Verifying by
// default closes the MITM window the previous "skip-verify" default left open
// on customer-DB traffic. If a connection targets a MySQL whose CA is not in
// the trust store (some self-managed / RDS / GCP certs), the user opts out
// explicitly by setting ssl_mode=require (or skip-verify) on the connection —
// which still encrypts, just without CA/hostname verification.
func resolveMySQLTLSMode(config map[string]interface{}, host string) string {
	for _, key := range []string{"ssl_mode", "sslmode", "tls", "tls_mode"} {
		if v, ok := config[key]; ok && v != nil {
			if s := strings.TrimSpace(fmt.Sprint(v)); s != "" {
				return normalizeMySQLTLS(s)
			}
		}
	}
	if isLocalDBHost(host) {
		return "false"
	}
	return "true"
}

// normalizeMySQLTLS maps assorted SSL/TLS spellings — libpq-style ("require",
// "disable", "verify-full"), booleans, and native go-sql-driver values — onto
// the four values the mysql driver understands: "false", "true",
// "skip-verify", "preferred". Unrecognised values pass through unchanged so a
// custom config name registered via mysql.RegisterTLSConfig still works.
func normalizeMySQLTLS(s string) string {
	switch strings.ToLower(s) {
	case "disable", "disabled", "false", "off", "0", "none":
		return "false"
	case "require", "required", "skip-verify", "skip_verify":
		return "skip-verify"
	case "prefer", "preferred":
		return "preferred"
	case "verify-ca", "verify_ca", "verify-full", "verify_full", "verify-identity", "verify_identity", "true", "on", "1":
		return "true"
	default:
		return s
	}
}

// postgresExplorerDSN builds the PostgreSQL DSN for an Explorer connection. Shared by the
// read (executePostgresQueryWithRedact) and write (executeDirectWrite) paths so the
// two never drift on TLS mode, port coercion, or connect timeout.
func postgresExplorerDSN(config map[string]interface{}) string {
	host, _ := config["host"].(string)
	port := config["port"]
	user, _ := config["user"].(string)
	password, _ := config["password"].(string)
	database, _ := config["database"].(string)
	sslMode := resolvePostgresSSLMode(config, host)

	portInt := 5432
	switch v := port.(type) {
	case float64:
		portInt = int(v)
	case int:
		portInt = v
	case string:
		fmt.Sscanf(v, "%d", &portInt)
	}

	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=10",
		host, portInt, user, password, database, sslMode)
}

// executePostgresQueryWithRedact is the shared implementation. When
// redact is true, PII columns (by name match) are masked before the
// rows leave this function. When false, raw values are returned.
func executePostgresQueryWithRedact(ctx context.Context, config map[string]interface{}, sqlQuery string, limit int, redact bool) (*ExplorerQueryResponse, error) {
	dsn := postgresExplorerDSN(config)

	// Open connection with timeout
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	// Set connection limits
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(0)
	conn.SetConnMaxLifetime(15 * time.Second)

	// Use a single session so SET search_path affects the query.
	dbc, err := conn.Conn(queryCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire db connection: %w", err)
	}
	defer dbc.Close()

	// Test connection
	if err := dbc.PingContext(queryCtx); err != nil {
		return nil, fmt.Errorf("connection ping failed: %w", err)
	}

	warnings := []string{}
	// Best-effort: set search_path so unqualified tables work (e.g. `digits` vs `logistics_db.digits`).
	// We infer schemas from referenced tables in the SQL and the current database's information_schema.
	if schemas := inferPostgresSearchPathSchemas(queryCtx, dbc, sqlQuery); len(schemas) > 0 {
		parts := make([]string, 0, len(schemas)+1)
		for _, s := range schemas {
			parts = append(parts, quotePGIdent(s))
		}
		// Always include public as fallback.
		parts = append(parts, quotePGIdent("public"))
		stmt := fmt.Sprintf("SET search_path TO %s", strings.Join(parts, ", "))
		if _, err := dbc.ExecContext(queryCtx, stmt); err != nil {
			// Non-fatal; query might still succeed if fully-qualified.
			warnings = append(warnings, fmt.Sprintf("Failed to set search_path (continuing): %v", err))
		}
	}

	// Clamp query LIMIT using validator
	wrappedSQL := validators.ClampLimit(sqlQuery, limit+1) // +1 row so the Truncated flag below is accurate (ClampLimit caps the DB-side LIMIT)

	startTime := time.Now()

	// Execute query
	rows, err := dbc.QueryContext(queryCtx, wrappedSQL)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Scan rows
	result := []map[string]interface{}{}
	truncated := false

	for rows.Next() {
		if len(result) >= limit {
			truncated = true
			break
		}

		columnValues := make([]interface{}, len(columns))
		columnPointers := make([]interface{}, len(columns))
		for i := range columnValues {
			columnPointers[i] = &columnValues[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to scan row: %v", err))
			continue
		}

		rowMap := make(map[string]interface{})
		for i, colName := range columns {
			val := columnValues[i]

			// Convert []byte to string
			if b, ok := val.([]byte); ok {
				val = string(b)
			}

			// Redact PII + secrets in preview mode (but avoid masking
			// generic ids). Skipped for the export path (T2-7): the
			// data owner expects to see their own raw values in CSV
			// downloads of their own connection.
			if redact && shouldRedactColumnName(colName) {
				val = redactForPreview(val)
			}

			rowMap[colName] = val
		}

		result = append(result, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	executionTime := time.Since(startTime).Milliseconds()

	return &ExplorerQueryResponse{
		Columns:         columns,
		Rows:            result,
		RowCount:        len(result),
		ExecutionTimeMs: executionTime,
		Truncated:       truncated,
		Warnings:        warnings,
	}, nil
}

type sqlQueryer interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

func inferPostgresSearchPathSchemas(ctx context.Context, q sqlQueryer, sqlQuery string) []string {
	if ctx == nil || q == nil {
		return nil
	}
	v := validators.ValidateExplorerSQL(sqlQuery)
	if v == nil || len(v.ReferencedTables) == 0 {
		return nil
	}

	// Collect schema-qualified refs directly from SQL, and unqualified table names for lookup.
	explicitSchemas := map[string]struct{}{}
	tableNames := map[string]struct{}{}
	for _, t := range v.ReferencedTables {
		raw := strings.TrimSpace(t)
		if raw == "" {
			continue
		}
		// Remove surrounding quotes for simple parsing.
		raw = strings.Trim(raw, `"'`)
		if strings.Contains(raw, ".") {
			parts := strings.SplitN(raw, ".", 2)
			s := strings.TrimSpace(strings.Trim(parts[0], `"'`))
			if isSafeSchemaName(s) && !isSystemSchema(s) {
				explicitSchemas[s] = struct{}{}
			}
			continue
		}
		// Table-only reference
		table := strings.TrimSpace(strings.Trim(raw, `"'`))
		if table != "" {
			tableNames[table] = struct{}{}
		}
	}

	out := make([]string, 0, 4)
	seen := map[string]struct{}{}
	for s := range explicitSchemas {
		seen[s] = struct{}{}
		out = append(out, s)
	}

	// If SQL already uses explicit schemas, we still may want to include them in search_path,
	// but it's not required. For unqualified tables, try to find a single non-public schema.
	for table := range tableNames {
		// Prefer non-public schema if public doesn't contain the table.
		// If the table exists in multiple non-system schemas, don't guess.
		rows, err := q.QueryContext(ctx, `
			SELECT table_schema
			FROM information_schema.tables
			WHERE table_name = $1
			  AND table_schema NOT IN ('pg_catalog', 'information_schema')
			ORDER BY CASE WHEN table_schema = 'public' THEN 1 ELSE 0 END, table_schema
		`, table)
		if err != nil {
			continue
		}
		schemas, err := func() ([]string, error) {
			defer rows.Close()
			out := []string{}
			for rows.Next() {
				var s string
				if err := rows.Scan(&s); err != nil {
					return nil, err
				}
				s = strings.TrimSpace(s)
				if s != "" && isSafeSchemaName(s) && !isSystemSchema(s) {
					out = append(out, s)
				}
			}
			if err := rows.Err(); err != nil {
				return nil, err
			}
			return out, nil
		}()
		if err != nil {
			continue
		}
		if len(schemas) == 1 {
			s := schemas[0]
			if s != "public" {
				if _, ok := seen[s]; !ok {
					seen[s] = struct{}{}
					out = append(out, s)
				}
			}
		}
	}

	// Keep deterministic order to avoid surprising behavior.
	sort.Strings(out)
	return out
}

func isSystemSchema(s string) bool {
	ss := strings.ToLower(strings.TrimSpace(s))
	return ss == "pg_catalog" || ss == "information_schema" || strings.HasPrefix(ss, "pg_")
}

// isInternalExplorerTable reports whether name is an rsync-internal bookkeeping
// or pipeline-staging table that must not surface as a Data Explorer query
// candidate (schema index, NL table resolution, HITL). Matching is
// case-insensitive and tolerates a leading schema qualifier
// (`public.flat_mysql_123` matches too). Mirrors the frontend predicate in
// frontend/src/lib/explorer/internalTables.ts.
func isInternalExplorerTable(name string) bool {
	n := strings.TrimSpace(name)
	if n == "" {
		return false
	}
	// Strip a leading schema qualifier so `schema.flat_mysql_123` matches.
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

func isSafeSchemaName(s string) bool {
	// Conservative: schema names we inject into SET search_path must be simple.
	// (We still quote them, but keep this guard to avoid oddities.)
	return regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(strings.TrimSpace(s))
}

func quotePGIdent(s string) string {
	// PostgreSQL identifier quoting: double quotes, with "" escaping.
	v := strings.ReplaceAll(strings.TrimSpace(s), `"`, `""`)
	return `"` + v + `"`
}

// executeMySQLQuery executes a SELECT against MySQL/MariaDB with PII
// redaction applied (preview path). See executePostgresQuery for the
// contract.
func executeMySQLQuery(ctx context.Context, config map[string]interface{}, sqlQuery string, limit int) (*ExplorerQueryResponse, error) {
	return executeMySQLQueryWithRedact(ctx, config, sqlQuery, limit, true)
}

// executeMySQLQueryUnredacted executes a SELECT without PII redaction.
// Use ONLY for the data-owner export path (T2-7).
func executeMySQLQueryUnredacted(ctx context.Context, config map[string]interface{}, sqlQuery string, limit int) (*ExplorerQueryResponse, error) {
	return executeMySQLQueryWithRedact(ctx, config, sqlQuery, limit, false)
}

// mysqlExplorerDSN builds the go-sql-driver/mysql DSN for an Explorer connection.
// Shared by the read (executeMySQLQueryWithRedact) and write (executeDirectWrite)
// paths so the two never drift on TLS mode, port coercion, or timeouts.
func mysqlExplorerDSN(config map[string]interface{}) string {
	host, _ := config["host"].(string)
	port := config["port"]
	user, _ := config["user"].(string)
	password, _ := config["password"].(string)
	database, _ := config["database"].(string)

	portInt := 3306
	switch v := port.(type) {
	case float64:
		portInt = int(v)
	case int:
		portInt = v
	case string:
		fmt.Sscanf(v, "%d", &portInt)
	}

	// Conservative timeouts (match Postgres behavior)
	tlsMode := resolveMySQLTLSMode(config, host)
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&timeout=10s&readTimeout=10s&writeTimeout=10s&tls=%s",
		user, password, host, portInt, database, tlsMode)
}

func executeMySQLQueryWithRedact(ctx context.Context, config map[string]interface{}, sqlQuery string, limit int, redact bool) (*ExplorerQueryResponse, error) {
	dsn := mysqlExplorerDSN(config)

	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(0)
	conn.SetConnMaxLifetime(15 * time.Second)

	if err := conn.PingContext(queryCtx); err != nil {
		return nil, fmt.Errorf("connection ping failed: %w", err)
	}

	wrappedSQL := validators.ClampLimit(sqlQuery, limit+1) // +1 row so the Truncated flag below is accurate (ClampLimit caps the DB-side LIMIT)
	startTime := time.Now()

	rows, err := conn.QueryContext(queryCtx, wrappedSQL)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	result := []map[string]interface{}{}
	warnings := []string{}
	truncated := false

	for rows.Next() {
		if len(result) >= limit {
			truncated = true
			break
		}

		columnValues := make([]interface{}, len(columns))
		columnPointers := make([]interface{}, len(columns))
		for i := range columnValues {
			columnPointers[i] = &columnValues[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to scan row: %v", err))
			continue
		}

		rowMap := make(map[string]interface{})
		for i, colName := range columns {
			val := columnValues[i]
			if b, ok := val.([]byte); ok {
				val = string(b)
			}
			// PII redaction bypassed for data-owner export (T2-7).
			if redact && shouldRedactColumnName(colName) {
				val = redactForPreview(val)
			}
			rowMap[colName] = val
		}

		result = append(result, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	executionTime := time.Since(startTime).Milliseconds()
	return &ExplorerQueryResponse{
		Columns:         columns,
		Rows:            result,
		RowCount:        len(result),
		ExecutionTimeMs: executionTime,
		Truncated:       truncated,
		Warnings:        warnings,
	}, nil
}

// =============================================================================
// SQL SERVER / AZURE SQL (go-mssqldb, read-only)
// =============================================================================

// sqlServerExplorerDSN builds a go-mssqldb URL DSN with host-aware TLS,
// mirroring the sqlserver connector + orchestrator resolveSQLServerEncrypt:
// Azure SQL (*.database.windows.net) presents a real CA cert → Encrypt=true +
// verify (TrustServerCertificate=false); a boxed/self-signed server encrypts
// in transit without CA verification. An explicit encrypt/sslmode/tls key wins.
func sqlServerExplorerDSN(config map[string]interface{}) string {
	host, _ := config["host"].(string)
	user, _ := config["user"].(string)
	password, _ := config["password"].(string)
	database, _ := config["database"].(string)
	host = strings.TrimSpace(host)

	portInt := 1433
	switch v := config["port"].(type) {
	case float64:
		portInt = int(v)
	case int:
		portInt = v
	case string:
		fmt.Sscanf(v, "%d", &portInt)
	}

	hl := strings.ToLower(host)
	isAzure := strings.HasSuffix(hl, ".database.windows.net")
	isLocal := isLocalDBHost(host) // address-aware (IP literals classified by address)

	raw := strings.ToLower(strings.TrimSpace(getStringConfig(config, "encrypt", "sslmode", "tls", "tls_mode")))
	encrypt, trust := "true", "false"
	switch {
	case raw == "disable" || raw == "disabled" || raw == "false" || raw == "no" || raw == "off" || raw == "0":
		encrypt, trust = "false", "true"
	case raw == "require" || raw == "required" || raw == "prefer" || raw == "preferred" || raw == "skip-verify" || raw == "skip_verify":
		encrypt, trust = "true", "true" // encrypt WITHOUT CA/hostname verify (opt-out for self-signed)
	case raw == "verify-ca" || raw == "verify_ca" || raw == "verify-full" || raw == "verify_full" || raw == "strict":
		encrypt, trust = "true", "false" // encrypt AND verify
	case isAzure:
		encrypt, trust = "true", "false" // real CA cert — verify it
	case isLocal:
		encrypt, trust = "true", "true" // local/boxed self-signed — trust
	default:
		encrypt, trust = "true", "false" // remote: encrypt AND verify by default (MITM-safe)
	}

	q := url.Values{}
	if strings.TrimSpace(database) != "" {
		q.Set("database", database)
	}
	q.Set("encrypt", encrypt)
	q.Set("TrustServerCertificate", trust)
	q.Set("dial timeout", "10")
	q.Set("connection timeout", "10")

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(user, password),
		Host:     fmt.Sprintf("%s:%d", host, portInt),
		RawQuery: q.Encode(),
	}
	return u.String()
}

// clampLimitSQLServer injects TOP (n) after the leading SELECT so the DB caps
// the returned rows (SQL Server has no LIMIT). CTE (WITH ...) or already-TOP'd
// queries are left unchanged; the executor's row-loop cap still bounds results.
func clampLimitSQLServer(sqlQuery string, maxLimit int) string {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sqlQuery), ";"))
	if regexp.MustCompile(`(?i)^SELECT\s+TOP\b`).MatchString(trimmed) {
		return trimmed
	}
	re := regexp.MustCompile(`(?i)^SELECT\s+(DISTINCT\s+)?`)
	loc := re.FindStringIndex(trimmed)
	if loc == nil || loc[0] != 0 {
		return trimmed // WITH/CTE or non-leading SELECT — rely on the row-loop cap
	}
	return trimmed[:loc[1]] + fmt.Sprintf("TOP (%d) ", maxLimit) + trimmed[loc[1]:]
}

func executeSQLServerQuery(ctx context.Context, config map[string]interface{}, sqlQuery string, limit int) (*ExplorerQueryResponse, error) {
	return executeSQLServerQueryWithRedact(ctx, config, sqlQuery, limit, true)
}

// executeSQLServerQueryUnredacted executes a SELECT without PII redaction
// (data-owner export path only).
func executeSQLServerQueryUnredacted(ctx context.Context, config map[string]interface{}, sqlQuery string, limit int) (*ExplorerQueryResponse, error) {
	return executeSQLServerQueryWithRedact(ctx, config, sqlQuery, limit, false)
}

func executeSQLServerQueryWithRedact(ctx context.Context, config map[string]interface{}, sqlQuery string, limit int, redact bool) (*ExplorerQueryResponse, error) {
	dsn := sqlServerExplorerDSN(config)

	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(0)
	conn.SetConnMaxLifetime(15 * time.Second)

	if err := conn.PingContext(queryCtx); err != nil {
		return nil, fmt.Errorf("connection ping failed: %w", err)
	}

	wrappedSQL := clampLimitSQLServer(sqlQuery, limit+1) // +1 so Truncated is accurate
	startTime := time.Now()

	rows, err := conn.QueryContext(queryCtx, wrappedSQL)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	result := []map[string]interface{}{}
	warnings := []string{}
	truncated := false

	for rows.Next() {
		if len(result) >= limit {
			truncated = true
			break
		}
		columnValues := make([]interface{}, len(columns))
		columnPointers := make([]interface{}, len(columns))
		for i := range columnValues {
			columnPointers[i] = &columnValues[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to scan row: %v", err))
			continue
		}
		rowMap := make(map[string]interface{})
		for i, colName := range columns {
			val := columnValues[i]
			if b, ok := val.([]byte); ok {
				val = string(b)
			}
			if redact && shouldRedactColumnName(colName) {
				val = redactForPreview(val)
			}
			rowMap[colName] = val
		}
		result = append(result, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	executionTime := time.Since(startTime).Milliseconds()
	return &ExplorerQueryResponse{
		Columns:         columns,
		Rows:            result,
		RowCount:        len(result),
		ExecutionTimeMs: executionTime,
		Truncated:       truncated,
		Warnings:        warnings,
	}, nil
}

// =============================================================================
// DATABRICKS SQL (Statement Execution API)
// =============================================================================

func getStringConfig(config map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := config[k]; ok {
			switch vv := v.(type) {
			case string:
				if strings.TrimSpace(vv) != "" {
					return strings.TrimSpace(vv)
				}
			}
		}
	}
	return ""
}

func normalizeDatabricksHost(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	s = strings.TrimPrefix(s, "https://")
	s = strings.TrimPrefix(s, "http://")
	s = strings.TrimSuffix(s, "/")
	return s
}

func parseWarehouseIDFromHTTPPath(httpPath string) string {
	// Common patterns:
	// - /sql/1.0/warehouses/<warehouse_id>
	// - /sql/1.0/warehouses/<warehouse_id>/
	s := strings.TrimSpace(httpPath)
	if s == "" {
		return ""
	}
	re := regexp.MustCompile(`/warehouses/([^/]+)`)
	m := re.FindStringSubmatch(s)
	if len(m) > 1 {
		return m[1]
	}
	return ""
}

type databricksStatementResponse struct {
	StatementID string `json:"statement_id"`
	Status      struct {
		State string `json:"state"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	} `json:"status"`
	// Column schema lives under the TOP-LEVEL `manifest.schema.columns` in the
	// Databricks SQL Statement Execution API — NOT under `result.schema` (which
	// does not exist). Reading it from `result` yielded empty columns, so every
	// row was built as an empty map — the Data Explorer returned blank results.
	Manifest struct {
		Schema struct {
			Columns []struct {
				Name string `json:"name"`
				Type string `json:"type_text,omitempty"`
			} `json:"columns"`
		} `json:"schema"`
	} `json:"manifest"`
	Result *struct {
		DataArray [][]interface{} `json:"data_array"`
	} `json:"result"`
}

func databricksExecuteStatement(ctx context.Context, host, token, warehouseID, sqlText, catalog, schemaName string) (*databricksStatementResponse, error) {
	if host == "" || token == "" || warehouseID == "" {
		return nil, fmt.Errorf("missing databricks config (host/token/warehouse_id required)")
	}
	url := fmt.Sprintf("https://%s/api/2.0/sql/statements", host)
	body := map[string]interface{}{
		"statement":       sqlText,
		"warehouse_id":    warehouseID,
		"disposition":     "INLINE",
		"format":          "JSON_ARRAY",
		"wait_timeout":    "30s",
		"on_wait_timeout": "CANCEL",
	}
	if strings.TrimSpace(catalog) != "" {
		body["catalog"] = catalog
	}
	if strings.TrimSpace(schemaName) != "" {
		body["schema"] = schemaName
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	// SSRF-guarded: host is user-supplied Databricks connection config and this
	// request carries a Bearer token — must refuse internal/link-local targets
	// (SAFEHTTP_ALLOW_HOSTS opt-out for a private-network workspace).
	client := safehttp.NewClient(35 * time.Second)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("databricks statement failed (HTTP %d): %s", resp.StatusCode, string(b))
	}
	var out databricksStatementResponse
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("failed to parse databricks response: %w", err)
	}
	return &out, nil
}

func executeDatabricksQuery(ctx context.Context, config map[string]interface{}, sqlQuery string, limit int) (*ExplorerQueryResponse, error) {
	return executeDatabricksQueryWithRedact(ctx, config, sqlQuery, limit, true)
}

// executeDatabricksQueryUnredacted bypasses PII redaction. Use ONLY
// for the data-owner export path (T2-7).
func executeDatabricksQueryUnredacted(ctx context.Context, config map[string]interface{}, sqlQuery string, limit int) (*ExplorerQueryResponse, error) {
	return executeDatabricksQueryWithRedact(ctx, config, sqlQuery, limit, false)
}

func executeDatabricksQueryWithRedact(ctx context.Context, config map[string]interface{}, sqlQuery string, limit int, redact bool) (*ExplorerQueryResponse, error) {
	host := normalizeDatabricksHost(getStringConfig(config, "host", "server_hostname", "workspace_host"))
	token := getStringConfig(config, "access_token", "token", "pat")
	warehouseID := getStringConfig(config, "warehouse_id", "warehouseId", "sql_warehouse_id")
	httpPath := getStringConfig(config, "http_path")
	if warehouseID == "" {
		warehouseID = parseWarehouseIDFromHTTPPath(httpPath)
	}
	catalog := getStringConfig(config, "catalog", "database")
	schemaName := getStringConfig(config, "schema", "schema_name")

	queryCtx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()

	// Clamp query LIMIT (Databricks supports LIMIT).
	wrappedSQL := validators.ClampLimit(sqlQuery, limit+1) // +1 row so the Truncated flag below is accurate (ClampLimit caps the DB-side LIMIT)

	start := time.Now()
	resp, err := databricksExecuteStatement(queryCtx, host, token, warehouseID, wrappedSQL, catalog, schemaName)
	if err != nil {
		return nil, err
	}

	if resp.Status.State != "" && strings.ToUpper(resp.Status.State) != "SUCCEEDED" {
		msg := strings.TrimSpace(resp.Status.Error.Message)
		if msg == "" {
			msg = fmt.Sprintf("statement status=%s", resp.Status.State)
		}
		return nil, fmt.Errorf("databricks query failed: %s", msg)
	}
	if resp.Result == nil {
		return nil, fmt.Errorf("databricks query returned no result")
	}

	cols := []string{}
	for _, c := range resp.Manifest.Schema.Columns {
		if c.Name != "" {
			cols = append(cols, c.Name)
		}
	}

	rows := []map[string]interface{}{}
	warnings := []string{}
	truncated := false

	for _, arr := range resp.Result.DataArray {
		if len(rows) >= limit {
			truncated = true
			break
		}
		row := map[string]interface{}{}
		for i, colName := range cols {
			var v interface{} = nil
			if i < len(arr) {
				v = arr[i]
			}
			// PII redaction bypassed for data-owner export (T2-7).
			if redact && shouldRedactColumnName(colName) {
				v = redactForPreview(v)
			}
			row[colName] = v
		}
		rows = append(rows, row)
	}

	return &ExplorerQueryResponse{
		Columns:         cols,
		Rows:            rows,
		RowCount:        len(rows),
		ExecutionTimeMs: time.Since(start).Milliseconds(),
		Truncated:       truncated,
		Warnings:        warnings,
	}, nil
}

// ensureLimit wraps query with LIMIT if not already present
func ensureLimit(sql string, limit int) string {
	normalized := strings.ToUpper(strings.TrimSpace(sql))

	// Check if LIMIT already present
	if regexp.MustCompile(`\bLIMIT\s+\d+`).MatchString(normalized) {
		// Replace with our limit if it's higher
		re := regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)
		matches := re.FindStringSubmatch(sql)
		if len(matches) > 1 {
			existingLimit, _ := strconv.Atoi(matches[1])
			if existingLimit > limit {
				sql = re.ReplaceAllString(sql, fmt.Sprintf("LIMIT %d", limit))
			}
		}
		return sql
	}

	// Add LIMIT
	sql = strings.TrimSuffix(strings.TrimSpace(sql), ";")
	return sql + fmt.Sprintf(" LIMIT %d", limit)
}

// =============================================================================
// METABASE DASHBOARD CREATION
// =============================================================================

// MetabaseDashboardRequest represents a request to create a Metabase dashboard
type MetabaseDashboardRequest struct {
	ConnectionID   string `json:"connection_id"` // Optional: specific Metabase connection
	SQL            string `json:"sql" binding:"required"`
	Name           string `json:"name" binding:"required"`
	Description    string `json:"description"`
	SourceDatabase string `json:"source_database"` // Database the SQL runs against
}

// MetabaseDashboardResponse represents the response after creating a dashboard
type MetabaseDashboardResponse struct {
	Success      bool   `json:"success"`
	DashboardID  int    `json:"dashboard_id,omitempty"`
	DashboardURL string `json:"dashboard_url,omitempty"`
	CardID       int    `json:"card_id,omitempty"`
	Error        string `json:"error,omitempty"`
}

// CreateMetabaseDashboard handles POST /api/v1/explorer/metabase/dashboard
// Creates a saved question (card) and dashboard in Metabase from Explorer SQL
func CreateMetabaseDashboard(c *gin.Context) {
	_, ok := resolveUserID(c)
	if !ok {
		return
	}

	var req MetabaseDashboardRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Get database
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	}

	// Find Metabase connection (use provided ID or find first available)
	var connectionID, configEncrypted, baseURL string
	var query string
	var args []interface{}

	if req.ConnectionID != "" {
		query = `
			SELECT id, config
			FROM connections
			WHERE id = $1 AND workspace_id = $2 AND LOWER(connector_type) = 'metabase'
		`
		args = []interface{}{req.ConnectionID, activeWorkspaceID(c)}
	} else {
		query = `
			SELECT id, config
			FROM connections
			WHERE workspace_id = $1 AND LOWER(connector_type) = 'metabase' AND type = 'destination'
			ORDER BY created_at DESC
			LIMIT 1
		`
		args = []interface{}{activeWorkspaceID(c)}
	}

	err := database.QueryRow(query, args...).Scan(&connectionID, &configEncrypted)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "No Metabase connection found",
			"hint":  "Please create a Metabase connection first (as destination type)",
		})
		return
	}
	if err != nil {
		log.Errorf("[CreateMetabaseDashboard] Failed to find connection: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to find Metabase connection"})
		return
	}

	// Decrypt config
	configJSON, err := crypto.DecryptString(configEncrypted)
	if err != nil {
		log.Errorf("[CreateMetabaseDashboard] Failed to decrypt config: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decrypt connection config"})
		return
	}

	var config map[string]interface{}
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse connection config"})
		return
	}

	// Extract base_url from config
	baseURL, _ = config["base_url"].(string)
	if baseURL == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Metabase connection missing base_url",
			"hint":  "Please update your Metabase connection with a valid base URL",
		})
		return
	}

	// Get API key from config
	apiKey, _ := config["api_key"].(string)
	if apiKey == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Metabase connection missing API key",
			"hint":  "Please update your Metabase connection with a valid API key",
		})
		return
	}

	// Create card (saved question) in Metabase
	cardPayload := map[string]interface{}{
		"name":                   req.Name,
		"display":                "table",
		"visualization_settings": map[string]interface{}{},
		"dataset_query": map[string]interface{}{
			"type": "native",
			"native": map[string]interface{}{
				"query": req.SQL,
			},
		},
	}

	if req.Description != "" {
		cardPayload["description"] = req.Description
	}

	cardBody, _ := json.Marshal(cardPayload)

	// Make request to Metabase API
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	cardReq, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/api/card", bytes.NewBuffer(cardBody))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create card request"})
		return
	}
	cardReq.Header.Set("Content-Type", "application/json")
	cardReq.Header.Set("X-API-Key", apiKey)

	// SSRF-guarded: base_url comes from user-supplied Metabase connection config,
	// so this outbound must refuse internal/link-local targets (SAFEHTTP_ALLOW_HOSTS
	// opt-out covers a self-hosted Metabase on a private network).
	client := safehttp.NewClient(35 * time.Second)
	cardResp, err := client.Do(cardReq)
	if err != nil {
		log.Errorf("[CreateMetabaseDashboard] Card creation failed: %v", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Failed to connect to Metabase",
			"hint":  "Check that your Metabase instance is accessible",
		})
		return
	}
	defer cardResp.Body.Close()

	cardRespBody, _ := io.ReadAll(cardResp.Body)

	if cardResp.StatusCode != http.StatusOK && cardResp.StatusCode != http.StatusCreated {
		log.Errorf("[CreateMetabaseDashboard] Card creation returned %d: %s", cardResp.StatusCode, string(cardRespBody))
		c.JSON(http.StatusBadGateway, gin.H{
			"error":   "Failed to create card in Metabase",
			"details": string(cardRespBody),
		})
		return
	}

	var cardResult map[string]interface{}
	if err := json.Unmarshal(cardRespBody, &cardResult); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse card response"})
		return
	}

	cardID, _ := cardResult["id"].(float64)

	// Create dashboard
	dashboardPayload := map[string]interface{}{
		"name": req.Name + " Dashboard",
	}
	if req.Description != "" {
		dashboardPayload["description"] = req.Description
	}

	dashBody, _ := json.Marshal(dashboardPayload)

	dashReq, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/api/dashboard", bytes.NewBuffer(dashBody))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create dashboard request"})
		return
	}
	dashReq.Header.Set("Content-Type", "application/json")
	dashReq.Header.Set("X-API-Key", apiKey)

	dashResp, err := client.Do(dashReq)
	if err != nil {
		log.Errorf("[CreateMetabaseDashboard] Dashboard creation failed: %v", err)
		// Card was created, return partial success
		c.JSON(http.StatusOK, MetabaseDashboardResponse{
			Success:      true,
			CardID:       int(cardID),
			DashboardURL: fmt.Sprintf("%s/question/%d", baseURL, int(cardID)),
			Error:        "Dashboard creation failed, but card was created",
		})
		return
	}
	defer dashResp.Body.Close()

	dashRespBody, _ := io.ReadAll(dashResp.Body)

	if dashResp.StatusCode != http.StatusOK && dashResp.StatusCode != http.StatusCreated {
		log.Warnf("[CreateMetabaseDashboard] Dashboard creation returned %d: %s", dashResp.StatusCode, string(dashRespBody))
		// Card was created, return partial success
		c.JSON(http.StatusOK, MetabaseDashboardResponse{
			Success:      true,
			CardID:       int(cardID),
			DashboardURL: fmt.Sprintf("%s/question/%d", baseURL, int(cardID)),
			Error:        "Dashboard creation failed, but card was created",
		})
		return
	}

	var dashResult map[string]interface{}
	if err := json.Unmarshal(dashRespBody, &dashResult); err != nil {
		c.JSON(http.StatusOK, MetabaseDashboardResponse{
			Success:      true,
			CardID:       int(cardID),
			DashboardURL: fmt.Sprintf("%s/question/%d", baseURL, int(cardID)),
		})
		return
	}

	dashboardID, _ := dashResult["id"].(float64)

	// Add card to dashboard
	addCardPayload := map[string]interface{}{
		"cardId": int(cardID),
		"row":    0,
		"col":    0,
		"size_x": 12,
		"size_y": 8,
	}

	addCardBody, _ := json.Marshal(addCardPayload)

	addCardReq, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/api/dashboard/%d/cards", baseURL, int(dashboardID)), bytes.NewBuffer(addCardBody))
	if err != nil {
		// Dashboard and card created, just couldn't add card to dashboard
		c.JSON(http.StatusOK, MetabaseDashboardResponse{
			Success:      true,
			DashboardID:  int(dashboardID),
			CardID:       int(cardID),
			DashboardURL: fmt.Sprintf("%s/dashboard/%d", baseURL, int(dashboardID)),
		})
		return
	}
	addCardReq.Header.Set("Content-Type", "application/json")
	addCardReq.Header.Set("X-API-Key", apiKey)

	addCardResp, err := client.Do(addCardReq)
	if err != nil {
		log.Warnf("[CreateMetabaseDashboard] Add card to dashboard failed: %v", err)
	} else {
		addCardResp.Body.Close()
	}

	// Success!
	c.JSON(http.StatusOK, MetabaseDashboardResponse{
		Success:      true,
		DashboardID:  int(dashboardID),
		CardID:       int(cardID),
		DashboardURL: fmt.Sprintf("%s/dashboard/%d", baseURL, int(dashboardID)),
	})
}

// GetRecommendedTablesForExplorer returns table recommendations for Explorer
func GetRecommendedTablesForExplorer(c *gin.Context) {
	connectionID, ok := requireUUIDParam(c, "id", "invalid_connection_id", "Invalid connection ID format")
	if !ok {
		return
	}
	_, ok = resolveUserID(c)
	if !ok {
		return
	}

	// NOTE: do NOT bind the JSON body here — GetRecommendedTables (the delegate
	// below) binds it itself, and binding twice drains the request body so the
	// delegate's bind fails with 400 "Invalid request body". That double-bind made
	// this endpoint always fail; we only verify ownership here, then delegate.

	// Delegate to existing GetRecommendedTables
	// This is a thin wrapper that can be enhanced later
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	}

	// Verify connection exists and belongs to the active workspace
	var exists bool
	err := database.QueryRow(`
		SELECT EXISTS(SELECT 1 FROM connections WHERE id = $1 AND workspace_id = $2)
	`, connectionID, activeWorkspaceID(c)).Scan(&exists)

	if err != nil || !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}

	// Call GetRecommendedTables with the request
	GetRecommendedTables(c)
}

// =============================================================================
// SCHEMA INDEX ENDPOINTS
// =============================================================================

// GetSchemaIndexResponse represents the schema index response
type GetSchemaIndexResponse struct {
	ConnectionID    string                          `json:"connection_id"`
	SchemaHash      string                          `json:"schema_hash"`
	LastRefreshedAt time.Time                       `json:"last_refreshed_at"`
	TableCount      int                             `json:"table_count"`
	Tables          []cache.ExplorerTableIndex      `json:"tables"`
	ForeignKeys     []cache.ExplorerForeignKeyIndex `json:"foreign_keys,omitempty"`
	Cached          bool                            `json:"cached"`
}

// GetSchemaIndex handles GET /api/v1/explorer/connections/:id/schema-index
// Returns a cached schema index or builds one if not cached
func GetSchemaIndex(c *gin.Context) {
	connectionID, ok := requireUUIDParam(c, "id", "invalid_connection_id", "Invalid connection ID format")
	if !ok {
		return
	}
	_, ok = resolveUserID(c)
	if !ok {
		return
	}

	forceRefresh := c.Query("refresh") == "true"
	// scope=server (MySQL only) browses every database on the server, not just
	// the connection's configured one. Cached under a distinct key so the
	// server-scope and database-scope indexes never overwrite each other.
	allDatabases := c.Query("scope") == "server"
	cacheID := schemaIndexCacheID(connectionID, allDatabases)

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	}

	// Verify connection exists and belongs to the active workspace
	var connectorType, configEncrypted string
	err := database.QueryRow(`
		SELECT connector_type, config
		FROM connections
		WHERE id = $1 AND workspace_id = $2
	`, connectionID, activeWorkspaceID(c)).Scan(&connectorType, &configEncrypted)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}
	if err != nil {
		log.Errorf("[GetSchemaIndex] Failed to load connection: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load connection"})
		return
	}

	ctx := c.Request.Context()

	// Check cache first (unless force refresh)
	if !forceRefresh && explorerCache != nil {
		cached, err := explorerCache.GetSchemaIndex(ctx, cacheID)
		if err == nil && cached != nil {
			c.JSON(http.StatusOK, GetSchemaIndexResponse{
				ConnectionID:    connectionID,
				SchemaHash:      cached.SchemaHash,
				LastRefreshedAt: cached.LastRefreshedAt,
				TableCount:      cached.TableCount,
				Tables:          cached.Tables,
				ForeignKeys:     cached.ForeignKeys,
				Cached:          true,
			})
			return
		}
	}

	// Build schema index from metadata
	schemaIndex, err := buildSchemaIndex(ctx, connectionID, connectorType, configEncrypted, allDatabases)
	if err != nil {
		log.Errorf("[GetSchemaIndex] Failed to build schema index: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to build schema index: %v", err)})
		return
	}

	// Cache the result under the scope-aware key.
	if explorerCache != nil {
		cacheCopy := *schemaIndex
		cacheCopy.ConnectionID = cacheID
		if err := explorerCache.SetSchemaIndex(ctx, &cacheCopy); err != nil {
			log.Warnf("[GetSchemaIndex] Failed to cache schema index: %v", err)
		}
	}

	c.JSON(http.StatusOK, GetSchemaIndexResponse{
		ConnectionID:    connectionID,
		SchemaHash:      schemaIndex.SchemaHash,
		LastRefreshedAt: schemaIndex.LastRefreshedAt,
		TableCount:      schemaIndex.TableCount,
		Tables:          schemaIndex.Tables,
		ForeignKeys:     schemaIndex.ForeignKeys,
		Cached:          false,
	})
}

// schemaIndexCacheID namespaces the explorer schema-index cache by browse scope.
// Server-scope (all databases) and the default database-scope index must not
// share a key or one would clobber the other on write.
func schemaIndexCacheID(connectionID string, allDatabases bool) string {
	if allDatabases {
		return connectionID + ":server"
	}
	return connectionID
}

// RefreshSchemaIndex handles POST /api/v1/explorer/connections/:id/schema-index/refresh
// Forces a refresh of the schema index
func RefreshSchemaIndex(c *gin.Context) {
	connectionID, ok := requireUUIDParam(c, "id", "invalid_connection_id", "Invalid connection ID format")
	if !ok {
		return
	}
	_, ok = resolveUserID(c)
	if !ok {
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	}

	// Verify connection exists and belongs to the active workspace
	var connectorType, configEncrypted string
	err := database.QueryRow(`
		SELECT connector_type, config
		FROM connections
		WHERE id = $1 AND workspace_id = $2
	`, connectionID, activeWorkspaceID(c)).Scan(&connectorType, &configEncrypted)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load connection"})
		return
	}

	allDatabases := c.Query("scope") == "server"
	cacheID := schemaIndexCacheID(connectionID, allDatabases)

	ctx := c.Request.Context()

	// Invalidate existing cache for this scope
	if explorerCache != nil {
		_ = explorerCache.InvalidateSchemaIndex(ctx, cacheID)
	}

	// Build fresh schema index
	schemaIndex, err := buildSchemaIndex(ctx, connectionID, connectorType, configEncrypted, allDatabases)
	if err != nil {
		log.Errorf("[RefreshSchemaIndex] Failed to build schema index: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to refresh schema index: %v", err)})
		return
	}

	// Cache the result under the scope-aware key.
	if explorerCache != nil {
		cacheCopy := *schemaIndex
		cacheCopy.ConnectionID = cacheID
		if err := explorerCache.SetSchemaIndex(ctx, &cacheCopy); err != nil {
			log.Warnf("[RefreshSchemaIndex] Failed to cache schema index: %v", err)
		}
	}

	c.JSON(http.StatusOK, GetSchemaIndexResponse{
		ConnectionID:    connectionID,
		SchemaHash:      schemaIndex.SchemaHash,
		LastRefreshedAt: schemaIndex.LastRefreshedAt,
		TableCount:      schemaIndex.TableCount,
		Tables:          schemaIndex.Tables,
		ForeignKeys:     schemaIndex.ForeignKeys,
		Cached:          false,
	})
}

// buildSchemaIndex builds a schema index from database metadata.
//
// allDatabases (MySQL/MariaDB only) controls server-vs-database scope: when
// true we blank the connection's configured `database` so the introspection
// query's `(? = ” OR TABLE_SCHEMA = ?)` clause returns every non-system
// database on the server, letting the Explorer browse cross-namespace tables
// from a single connection (Superset/DBeaver-style). Postgres ignores this —
// its namespaces are already schemas under one DB, all of which are returned.
func buildSchemaIndex(ctx context.Context, connectionID, connectorType, configEncrypted string, allDatabases bool) (*cache.ExplorerSchemaIndex, error) {
	ct := strings.ToLower(connectorType)
	isPostgres := strings.Contains(ct, "postgres") || strings.Contains(ct, "redshift")
	isMySQL := strings.Contains(ct, "mysql") || strings.Contains(ct, "mariadb")
	isDatabricks := strings.Contains(ct, "databricks")
	isSQLServer := strings.Contains(ct, "sqlserver") || strings.Contains(ct, "mssql")
	if !isPostgres && !isMySQL && !isDatabricks && !isSQLServer {
		// Non-SQL connectors (GraphQL/REST: shopify-admin-graphql, github,
		// linear, stripe, ...) have no introspectable relational schema.
		// Return an empty index rather than erroring so the Data Explorer
		// degrades gracefully (200 + no tables) instead of surfacing a 500.
		return &cache.ExplorerSchemaIndex{
			ConnectionID:    connectionID,
			SchemaHash:      "unsupported",
			LastRefreshedAt: time.Now(),
			TableCount:      0,
			Tables:          []cache.ExplorerTableIndex{},
			ForeignKeys:     []cache.ExplorerForeignKeyIndex{},
		}, nil
	}

	// Decrypt config
	configJSON, err := crypto.DecryptString(configEncrypted)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt config: %w", err)
	}

	var config map[string]interface{}
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Get tables and columns from the DB
	var tables []cache.ExplorerTableIndex
	var foreignKeys []cache.ExplorerForeignKeyIndex
	if isPostgres {
		tables, err = fetchPostgresSchemaIndex(ctx, config)
		if err == nil {
			foreignKeys, _ = fetchPostgresForeignKeys(ctx, config)
		}
	} else if isMySQL {
		// Server-scope browsing: blank the configured database so the
		// introspection query returns every non-system schema on the server.
		if allDatabases {
			config["database"] = ""
		}
		tables, err = fetchMySQLSchemaIndex(ctx, config)
		if err == nil {
			foreignKeys, _ = fetchMySQLForeignKeys(ctx, config)
		}
	} else if isSQLServer {
		tables, err = fetchSQLServerSchemaIndex(ctx, config)
		foreignKeys = nil
	} else {
		tables, err = fetchDatabricksSchemaIndex(ctx, config)
		foreignKeys = nil
	}
	if err != nil {
		return nil, err
	}

	// Drop rsync-internal bookkeeping / pipeline-staging tables (e.g.
	// flat_mysql_<ts>, _rsync_*) before caching so they never reach the
	// schema index, NL table resolution, or the HITL candidate list.
	if len(tables) > 0 {
		filtered := tables[:0]
		for _, t := range tables {
			if isInternalExplorerTable(t.Name) {
				continue
			}
			filtered = append(filtered, t)
		}
		tables = filtered
	}

	// Phase 3.5: infer relationships beyond explicit FK constraints (naming + type heuristics).
	inferred := cache.InferForeignKeysFromHeuristics(tables, foreignKeys)
	if len(inferred) > 0 {
		foreignKeys = append(foreignKeys, inferred...)
	}

	// Compute schema hash
	schemaHash := cache.ComputeSchemaHash(tables, foreignKeys)

	return &cache.ExplorerSchemaIndex{
		ConnectionID:    connectionID,
		SchemaHash:      schemaHash,
		LastRefreshedAt: time.Now(),
		TableCount:      len(tables),
		Tables:          tables,
		ForeignKeys:     foreignKeys,
	}, nil
}

// fetchMySQLForeignKeys fetches FK relationships from MySQL/MariaDB.
func fetchMySQLForeignKeys(ctx context.Context, config map[string]interface{}) ([]cache.ExplorerForeignKeyIndex, error) {
	host, _ := config["host"].(string)
	port, _ := config["port"]
	user, _ := config["user"].(string)
	password, _ := config["password"].(string)
	database, _ := config["database"].(string)
	database = strings.TrimSpace(database)

	portInt := 3306
	switch v := port.(type) {
	case float64:
		portInt = int(v)
	case int:
		portInt = v
	case string:
		fmt.Sscanf(v, "%d", &portInt)
	}

	tlsMode := resolveMySQLTLSMode(config, host)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&timeout=10s&readTimeout=10s&writeTimeout=10s&tls=%s",
		user, password, host, portInt, database, tlsMode)

	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	conn.SetMaxOpenConns(1)
	conn.SetConnMaxLifetime(30 * time.Second)
	if err := conn.PingContext(queryCtx); err != nil {
		return nil, fmt.Errorf("connection ping failed: %w", err)
	}

	// FK relationships from information_schema.key_column_usage
	query := `
		SELECT
			k.TABLE_SCHEMA,
			k.TABLE_NAME,
			k.COLUMN_NAME,
			k.REFERENCED_TABLE_SCHEMA,
			k.REFERENCED_TABLE_NAME,
			k.REFERENCED_COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE k
		WHERE k.REFERENCED_TABLE_NAME IS NOT NULL
		  AND k.TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
		  AND k.REFERENCED_TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
		  AND (? = '' OR (k.TABLE_SCHEMA = ? AND k.REFERENCED_TABLE_SCHEMA = ?))
		ORDER BY k.TABLE_SCHEMA, k.TABLE_NAME, k.COLUMN_NAME
	`

	rows, err := conn.QueryContext(queryCtx, query, database, database, database)
	if err != nil {
		return nil, fmt.Errorf("failed to query foreign keys: %w", err)
	}
	defer rows.Close()

	var fks []cache.ExplorerForeignKeyIndex
	for rows.Next() {
		var fromSchema, fromTable, fromCol, toSchema, toTable, toCol string
		if err := rows.Scan(&fromSchema, &fromTable, &fromCol, &toSchema, &toTable, &toCol); err != nil {
			log.Warnf("Failed to scan FK row: %v", err)
			continue
		}
		fks = append(fks, cache.ExplorerForeignKeyIndex{
			FromSchema: fromSchema,
			FromTable:  fromTable,
			FromColumn: fromCol,
			ToSchema:   toSchema,
			ToTable:    toTable,
			ToColumn:   toCol,
			Confidence: 1.0,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}
	return fks, nil
}

// fetchDatabricksSchemaIndex builds a schema index using Databricks SQL over HTTP.
// This is a best-effort MVP:
// - Lists tables via SHOW TABLES
// - Gets columns via DESCRIBE
// - Does not fetch row counts or foreign keys (Databricks generally doesn't expose FKs)
func fetchDatabricksSchemaIndex(ctx context.Context, config map[string]interface{}) ([]cache.ExplorerTableIndex, error) {
	host := normalizeDatabricksHost(getStringConfig(config, "host", "server_hostname", "workspace_host"))
	token := getStringConfig(config, "access_token", "token", "pat")
	warehouseID := getStringConfig(config, "warehouse_id", "warehouseId", "sql_warehouse_id")
	httpPath := getStringConfig(config, "http_path")
	if warehouseID == "" {
		warehouseID = parseWarehouseIDFromHTTPPath(httpPath)
	}
	catalog := strings.TrimSpace(getStringConfig(config, "catalog", "database"))
	schemaName := strings.TrimSpace(getStringConfig(config, "schema", "schema_name"))
	if schemaName == "" {
		schemaName = "default"
	}

	queryCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	qualifiedSchema := schemaName
	if catalog != "" {
		qualifiedSchema = fmt.Sprintf("%s.%s", catalog, schemaName)
	}

	// List tables
	showSQL := fmt.Sprintf("SHOW TABLES IN %s", qualifiedSchema)
	showResp, err := databricksExecuteStatement(queryCtx, host, token, warehouseID, showSQL, catalog, schemaName)
	if err != nil {
		return nil, err
	}
	if showResp.Result == nil {
		return nil, fmt.Errorf("databricks SHOW TABLES returned no result")
	}

	// Identify table name column
	colIdx := -1
	for i, c := range showResp.Manifest.Schema.Columns {
		n := strings.ToLower(strings.TrimSpace(c.Name))
		if n == "tablename" || n == "table_name" || n == "table" || n == "name" {
			colIdx = i
			break
		}
		if n == "tableName" {
			colIdx = i
			break
		}
	}
	if colIdx < 0 && len(showResp.Manifest.Schema.Columns) > 0 {
		// Spark usually returns database, tableName, isTemporary
		for i, c := range showResp.Manifest.Schema.Columns {
			n := strings.ToLower(strings.TrimSpace(c.Name))
			if strings.Contains(n, "table") && strings.Contains(n, "name") {
				colIdx = i
				break
			}
		}
	}
	if colIdx < 0 {
		return nil, fmt.Errorf("could not locate table name column in SHOW TABLES response")
	}

	tableNames := []string{}
	for _, r := range showResp.Result.DataArray {
		if colIdx < len(r) {
			if s, ok := r[colIdx].(string); ok && strings.TrimSpace(s) != "" {
				tableNames = append(tableNames, strings.TrimSpace(s))
			}
		}
	}

	// Describe each table (cap to avoid massive scans)
	maxTables := 200
	if len(tableNames) > maxTables {
		tableNames = tableNames[:maxTables]
	}

	out := []cache.ExplorerTableIndex{}
	for _, t := range tableNames {
		fullName := t
		if catalog != "" {
			fullName = fmt.Sprintf("%s.%s.%s", catalog, schemaName, t)
		} else {
			fullName = fmt.Sprintf("%s.%s", schemaName, t)
		}

		descSQL := fmt.Sprintf("DESCRIBE %s", fullName)
		descResp, err := databricksExecuteStatement(queryCtx, host, token, warehouseID, descSQL, catalog, schemaName)
		if err != nil || descResp.Result == nil {
			// Skip tables we can't describe (permissions etc.)
			continue
		}

		// Identify describe columns: col_name + data_type (Spark typically: col_name, data_type, comment)
		nameIdx := -1
		typeIdx := -1
		for i, c := range descResp.Manifest.Schema.Columns {
			n := strings.ToLower(strings.TrimSpace(c.Name))
			if n == "col_name" || n == "column_name" || n == "name" {
				nameIdx = i
			}
			if n == "data_type" || n == "type" || strings.Contains(n, "data") && strings.Contains(n, "type") {
				typeIdx = i
			}
		}
		if nameIdx < 0 && len(descResp.Manifest.Schema.Columns) > 0 {
			nameIdx = 0
		}
		if typeIdx < 0 && len(descResp.Manifest.Schema.Columns) > 1 {
			typeIdx = 1
		}

		cols := []cache.ExplorerColumnIndex{}
		for _, r := range descResp.Result.DataArray {
			if nameIdx >= len(r) {
				continue
			}
			cn, _ := r[nameIdx].(string)
			cn = strings.TrimSpace(cn)
			if cn == "" || strings.HasPrefix(cn, "#") {
				// Spark DESCRIBE includes section headers like '# Partition Information'
				continue
			}
			ct := ""
			if typeIdx >= 0 && typeIdx < len(r) {
				if s, ok := r[typeIdx].(string); ok {
					ct = strings.TrimSpace(s)
				}
			}
			cols = append(cols, cache.ExplorerColumnIndex{
				Name:       cn,
				Type:       ct,
				IsNullable: true,
			})
		}

		tIdx := cache.ExplorerTableIndex{
			// Prefer a qualified schema when catalog is present (Unity Catalog).
			// This yields identifiers like <catalog>.<schema>.<table> in the UI/LLM context.
			Schema:   qualifiedSchema,
			Name:     t,
			RowCount: 0,
			Columns:  cols,
		}
		tIdx.SearchTokens = cache.BuildSearchTokens(tIdx)
		out = append(out, tIdx)
	}

	return out, nil
}

// fetchPostgresForeignKeys fetches FK relationships from Postgres.
func fetchPostgresForeignKeys(ctx context.Context, config map[string]interface{}) ([]cache.ExplorerForeignKeyIndex, error) {
	host, _ := config["host"].(string)
	port, _ := config["port"]
	user, _ := config["user"].(string)
	password, _ := config["password"].(string)
	database, _ := config["database"].(string)
	sslMode := resolvePostgresSSLMode(config, host)

	portInt := 5432
	switch v := port.(type) {
	case float64:
		portInt = int(v)
	case int:
		portInt = v
	case string:
		fmt.Sscanf(v, "%d", &portInt)
	}

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=10",
		host, portInt, user, password, database, sslMode)

	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	conn.SetMaxOpenConns(1)
	conn.SetConnMaxLifetime(30 * time.Second)
	if err := conn.PingContext(queryCtx); err != nil {
		return nil, fmt.Errorf("connection ping failed: %w", err)
	}

	query := `
		SELECT
			tc.table_schema,
			tc.table_name,
			kcu.column_name,
			ccu.table_schema AS foreign_table_schema,
			ccu.table_name AS foreign_table_name,
			ccu.column_name AS foreign_column_name
		FROM information_schema.table_constraints AS tc
		JOIN information_schema.key_column_usage AS kcu
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage AS ccu
		  ON ccu.constraint_name = tc.constraint_name
		  AND ccu.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
		  AND tc.table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		  AND ccu.table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		ORDER BY tc.table_schema, tc.table_name, kcu.column_name
	`

	rows, err := conn.QueryContext(queryCtx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query foreign keys: %w", err)
	}
	defer rows.Close()

	var fks []cache.ExplorerForeignKeyIndex
	for rows.Next() {
		var fromSchema, fromTable, fromCol, toSchema, toTable, toCol string
		if err := rows.Scan(&fromSchema, &fromTable, &fromCol, &toSchema, &toTable, &toCol); err != nil {
			log.Warnf("Failed to scan FK row: %v", err)
			continue
		}
		fks = append(fks, cache.ExplorerForeignKeyIndex{
			FromSchema: fromSchema,
			FromTable:  fromTable,
			FromColumn: fromCol,
			ToSchema:   toSchema,
			ToTable:    toTable,
			ToColumn:   toCol,
			Confidence: 1.0,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return fks, nil
}

// fetchMySQLSchemaIndex fetches schema metadata from MySQL/MariaDB
func fetchMySQLSchemaIndex(ctx context.Context, config map[string]interface{}) ([]cache.ExplorerTableIndex, error) {
	host, _ := config["host"].(string)
	port, _ := config["port"]
	user, _ := config["user"].(string)
	password, _ := config["password"].(string)
	database, _ := config["database"].(string)
	database = strings.TrimSpace(database)

	portInt := 3306
	switch v := port.(type) {
	case float64:
		portInt = int(v)
	case int:
		portInt = v
	case string:
		fmt.Sscanf(v, "%d", &portInt)
	}

	tlsMode := resolveMySQLTLSMode(config, host)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&timeout=10s&readTimeout=10s&writeTimeout=10s&tls=%s",
		user, password, host, portInt, database, tlsMode)

	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	conn.SetMaxOpenConns(1)
	conn.SetConnMaxLifetime(30 * time.Second)
	if err := conn.PingContext(queryCtx); err != nil {
		return nil, fmt.Errorf("connection ping failed: %w", err)
	}

	// Query tables and columns; filter out system schemas.
	query := `
		SELECT
			t.TABLE_SCHEMA,
			t.TABLE_NAME,
			COALESCE(t.TABLE_ROWS, 0) AS row_count,
			c.COLUMN_NAME,
			c.DATA_TYPE,
			c.IS_NULLABLE,
			CASE WHEN k.COLUMN_NAME IS NOT NULL THEN TRUE ELSE FALSE END AS is_pk,
			c.ORDINAL_POSITION
		FROM information_schema.TABLES t
		LEFT JOIN information_schema.COLUMNS c
			ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
		LEFT JOIN information_schema.KEY_COLUMN_USAGE k
			ON c.TABLE_SCHEMA = k.TABLE_SCHEMA AND c.TABLE_NAME = k.TABLE_NAME AND c.COLUMN_NAME = k.COLUMN_NAME
		LEFT JOIN information_schema.TABLE_CONSTRAINTS tc
			ON tc.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
			AND tc.TABLE_SCHEMA = k.TABLE_SCHEMA AND tc.TABLE_NAME = k.TABLE_NAME
			AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		WHERE t.TABLE_TYPE = 'BASE TABLE'
		  AND t.TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
		  AND (? = '' OR t.TABLE_SCHEMA = ?)
		ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
	`

	rows, err := conn.QueryContext(queryCtx, query, database, database)
	if err != nil {
		return nil, fmt.Errorf("failed to query schema: %w", err)
	}
	defer rows.Close()

	tableMap := make(map[string]*cache.ExplorerTableIndex)
	tableOrder := []string{}

	for rows.Next() {
		var schema, tableName string
		var rowCount int64
		var colName, colType sql.NullString
		var isNullable sql.NullString
		var isPK bool
		var ordPos sql.NullInt64

		if err := rows.Scan(&schema, &tableName, &rowCount, &colName, &colType, &isNullable, &isPK, &ordPos); err != nil {
			log.Warnf("Failed to scan row: %v", err)
			continue
		}

		key := schema + "." + tableName
		if _, exists := tableMap[key]; !exists {
			tableMap[key] = &cache.ExplorerTableIndex{
				Name:     tableName,
				Schema:   schema,
				RowCount: rowCount,
				Columns:  []cache.ExplorerColumnIndex{},
			}
			tableOrder = append(tableOrder, key)
		}

		if colName.Valid {
			tableMap[key].Columns = append(tableMap[key].Columns, cache.ExplorerColumnIndex{
				Name:         colName.String,
				Type:         colType.String,
				IsPrimaryKey: isPK,
				IsNullable:   strings.EqualFold(isNullable.String, "YES"),
			})
			if isPK {
				tableMap[key].PrimaryKey = append(tableMap[key].PrimaryKey, colName.String)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	tables := make([]cache.ExplorerTableIndex, 0, len(tableMap))
	for _, key := range tableOrder {
		t := tableMap[key]
		t.SearchTokens = cache.BuildSearchTokens(*t)
		tables = append(tables, *t)
	}

	return tables, nil
}

// fetchSQLServerSchemaIndex fetches schema metadata from SQL Server / Azure SQL.
// SQL Server's INFORMATION_SCHEMA.TABLE_SCHEMA is the SCHEMA (dbo, ...), not the
// database — the connection is already scoped to one database, so we list every
// user schema (excluding sys / INFORMATION_SCHEMA). Row counts are reported as 0
// (INFORMATION_SCHEMA has no row-count column; not load-bearing for the index).
func fetchSQLServerSchemaIndex(ctx context.Context, config map[string]interface{}) ([]cache.ExplorerTableIndex, error) {
	dsn := sqlServerExplorerDSN(config)

	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	conn.SetMaxOpenConns(1)
	conn.SetConnMaxLifetime(30 * time.Second)
	if err := conn.PingContext(queryCtx); err != nil {
		return nil, fmt.Errorf("connection ping failed: %w", err)
	}

	query := `
		SELECT
			t.TABLE_SCHEMA,
			t.TABLE_NAME,
			CAST(0 AS BIGINT) AS row_count,
			c.COLUMN_NAME,
			c.DATA_TYPE,
			c.IS_NULLABLE,
			CAST(CASE WHEN k.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS BIT) AS is_pk,
			c.ORDINAL_POSITION
		FROM INFORMATION_SCHEMA.TABLES t
		LEFT JOIN INFORMATION_SCHEMA.COLUMNS c
			ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
		LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
			ON tc.TABLE_SCHEMA = c.TABLE_SCHEMA AND tc.TABLE_NAME = c.TABLE_NAME
			AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
			ON k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND k.TABLE_SCHEMA = c.TABLE_SCHEMA
			AND k.TABLE_NAME = c.TABLE_NAME AND k.COLUMN_NAME = c.COLUMN_NAME
		WHERE t.TABLE_TYPE = 'BASE TABLE'
		  AND t.TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
		ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
	`

	rows, err := conn.QueryContext(queryCtx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query schema: %w", err)
	}
	defer rows.Close()

	tableMap := make(map[string]*cache.ExplorerTableIndex)
	tableOrder := []string{}

	for rows.Next() {
		var schema, tableName string
		var rowCount int64
		var colName, colType sql.NullString
		var isNullable sql.NullString
		var isPK bool
		var ordPos sql.NullInt64

		if err := rows.Scan(&schema, &tableName, &rowCount, &colName, &colType, &isNullable, &isPK, &ordPos); err != nil {
			log.Warnf("Failed to scan row: %v", err)
			continue
		}

		key := schema + "." + tableName
		if _, exists := tableMap[key]; !exists {
			tableMap[key] = &cache.ExplorerTableIndex{
				Name:     tableName,
				Schema:   schema,
				RowCount: rowCount,
				Columns:  []cache.ExplorerColumnIndex{},
			}
			tableOrder = append(tableOrder, key)
		}

		if colName.Valid {
			tableMap[key].Columns = append(tableMap[key].Columns, cache.ExplorerColumnIndex{
				Name:         colName.String,
				Type:         colType.String,
				IsPrimaryKey: isPK,
				IsNullable:   strings.EqualFold(isNullable.String, "YES"),
			})
			if isPK {
				tableMap[key].PrimaryKey = append(tableMap[key].PrimaryKey, colName.String)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	tables := make([]cache.ExplorerTableIndex, 0, len(tableMap))
	for _, key := range tableOrder {
		t := tableMap[key]
		t.SearchTokens = cache.BuildSearchTokens(*t)
		tables = append(tables, *t)
	}

	return tables, nil
}

// fetchPostgresSchemaIndex fetches schema metadata from PostgreSQL
func fetchPostgresSchemaIndex(ctx context.Context, config map[string]interface{}) ([]cache.ExplorerTableIndex, error) {
	// Build DSN
	host, _ := config["host"].(string)
	port, _ := config["port"]
	user, _ := config["user"].(string)
	password, _ := config["password"].(string)
	database, _ := config["database"].(string)
	sslMode := resolvePostgresSSLMode(config, host)

	portInt := 5432
	switch v := port.(type) {
	case float64:
		portInt = int(v)
	case int:
		portInt = v
	case string:
		fmt.Sscanf(v, "%d", &portInt)
	}

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=10",
		host, portInt, user, password, database, sslMode)

	// Open connection
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	conn.SetMaxOpenConns(1)
	conn.SetConnMaxLifetime(30 * time.Second)

	if err := conn.PingContext(queryCtx); err != nil {
		return nil, fmt.Errorf("connection ping failed: %w", err)
	}

	// Query tables with columns
	query := `
		WITH table_info AS (
			SELECT 
				t.table_schema,
				t.table_name,
				pg_catalog.obj_description(
					(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass, 'pg_class'
				) as table_comment
			FROM information_schema.tables t
			WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
			  AND t.table_type = 'BASE TABLE'
		),
		column_info AS (
			SELECT 
				c.table_schema,
				c.table_name,
				c.column_name,
				c.data_type,
				c.is_nullable,
				c.ordinal_position,
				CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_pk
			FROM information_schema.columns c
			LEFT JOIN (
				SELECT kcu.table_schema, kcu.table_name, kcu.column_name
				FROM information_schema.table_constraints tc
				JOIN information_schema.key_column_usage kcu 
					ON tc.constraint_name = kcu.constraint_name 
					AND tc.table_schema = kcu.table_schema
				WHERE tc.constraint_type = 'PRIMARY KEY'
			) pk ON c.table_schema = pk.table_schema 
				AND c.table_name = pk.table_name 
				AND c.column_name = pk.column_name
			WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		),
		row_counts AS (
			SELECT 
				schemaname as table_schema,
				relname as table_name,
				n_live_tup as row_count
			FROM pg_stat_user_tables
		)
		SELECT 
			ti.table_schema,
			ti.table_name,
			COALESCE(rc.row_count, 0) as row_count,
			ci.column_name,
			ci.data_type,
			ci.is_nullable,
			ci.is_pk,
			ci.ordinal_position
		FROM table_info ti
		LEFT JOIN column_info ci ON ti.table_schema = ci.table_schema AND ti.table_name = ci.table_name
		LEFT JOIN row_counts rc ON ti.table_schema = rc.table_schema AND ti.table_name = rc.table_name
		ORDER BY ti.table_schema, ti.table_name, ci.ordinal_position
	`

	rows, err := conn.QueryContext(queryCtx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query schema: %w", err)
	}
	defer rows.Close()

	// Build table index
	tableMap := make(map[string]*cache.ExplorerTableIndex)
	tableOrder := []string{}

	for rows.Next() {
		var schema, tableName string
		var rowCount int64
		var colName, colType sql.NullString
		var isNullable string
		var isPK bool
		var ordPos int

		if err := rows.Scan(&schema, &tableName, &rowCount, &colName, &colType, &isNullable, &isPK, &ordPos); err != nil {
			log.Warnf("Failed to scan row: %v", err)
			continue
		}

		key := schema + "." + tableName
		if _, exists := tableMap[key]; !exists {
			tableMap[key] = &cache.ExplorerTableIndex{
				Name:     tableName,
				Schema:   schema,
				RowCount: rowCount,
				Columns:  []cache.ExplorerColumnIndex{},
			}
			tableOrder = append(tableOrder, key)
		}

		if colName.Valid {
			tableMap[key].Columns = append(tableMap[key].Columns, cache.ExplorerColumnIndex{
				Name:         colName.String,
				Type:         colType.String,
				IsPrimaryKey: isPK,
				IsNullable:   isNullable == "YES",
			})

			if isPK {
				tableMap[key].PrimaryKey = append(tableMap[key].PrimaryKey, colName.String)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	// Convert to slice and build search tokens
	tables := make([]cache.ExplorerTableIndex, 0, len(tableMap))
	for _, key := range tableOrder {
		t := tableMap[key]
		t.SearchTokens = cache.BuildSearchTokens(*t)
		tables = append(tables, *t)
	}

	return tables, nil
}

// =============================================================================
// TABLE RETRIEVAL ENDPOINTS (for large schema handling)
// =============================================================================

// RetrieveTablesRequest represents a table retrieval request
type RetrieveTablesRequest struct {
	ConnectionID string `json:"connection_id" binding:"required"`
	Question     string `json:"question" binding:"required"`
	TopK         int    `json:"top_k"` // Default: 20
}

// RetrieveTablesResponse represents the retrieved tables
type RetrieveTablesResponse struct {
	Tables      []cache.ExplorerTableIndex `json:"tables"`
	SchemaHash  string                     `json:"schema_hash"`
	TotalTables int                        `json:"total_tables"`
}

// RetrieveTables handles POST /api/v1/explorer/tables/retrieve
// Retrieves top-K tables matching a natural language question
func RetrieveTables(c *gin.Context) {
	_, ok := resolveUserID(c)
	if !ok {
		return
	}

	var req RetrieveTablesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	if req.TopK <= 0 {
		req.TopK = 20
	}
	if req.TopK > 50 {
		req.TopK = 50
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	}

	// Verify connection
	var exists bool
	err := database.QueryRow(`
		SELECT EXISTS(SELECT 1 FROM connections WHERE id = $1 AND workspace_id = $2)
	`, req.ConnectionID, activeWorkspaceID(c)).Scan(&exists)

	if err != nil || !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}

	ctx := c.Request.Context()

	// Get schema index (from cache or build)
	var schemaIndex *cache.ExplorerSchemaIndex
	if explorerCache != nil {
		schemaIndex, _ = explorerCache.GetSchemaIndex(ctx, req.ConnectionID)
	}

	if schemaIndex == nil {
		// Need to build it - get connection config
		var connectorType, configEncrypted string
		err := database.QueryRow(`
			SELECT connector_type, config
			FROM connections
			WHERE id = $1 AND workspace_id = $2
		`, req.ConnectionID, activeWorkspaceID(c)).Scan(&connectorType, &configEncrypted)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load connection"})
			return
		}

		schemaIndex, err = buildSchemaIndex(ctx, req.ConnectionID, connectorType, configEncrypted, false)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to build schema index: %v", err)})
			return
		}

		// Cache for next time
		if explorerCache != nil {
			_ = explorerCache.SetSchemaIndex(ctx, schemaIndex)
		}
	}

	// Extract search terms from question
	searchTerms := cache.ExtractSearchTerms(req.Question)

	// Retrieve top-K tables
	tables := cache.RetrieveTopTables(schemaIndex, searchTerms, req.TopK)

	c.JSON(http.StatusOK, RetrieveTablesResponse{
		Tables:      tables,
		SchemaHash:  schemaIndex.SchemaHash,
		TotalTables: schemaIndex.TableCount,
	})
}

// =============================================================================
// LLM-ORCHESTRATED ENDPOINTS (Table Link, Column Link, Next Steps)
// =============================================================================

// TableCandidate represents a candidate table with confidence
type TableCandidate struct {
	Table      string  `json:"table"`
	SchemaName string  `json:"schema_name"`
	Confidence float64 `json:"confidence"`
	Reason     string  `json:"reason"`
}

// ResolveTablésRequest represents a table resolution request
type ResolveTablesRequest struct {
	ConnectionID   string `json:"connection_id" binding:"required"`
	Question       string `json:"question" binding:"required"`
	ConversationID string `json:"conversation_id,omitempty"`
}

// ResolveTablesResponse represents the table resolution response
type ResolveTablesResponse struct {
	Candidates        []TableCandidate `json:"candidates"`
	ConfidenceOverall float64          `json:"confidence_overall"`
	NeedsHITL         bool             `json:"needs_hitl"`
	HITLReason        string           `json:"hitl_reason,omitempty"`
	SuggestedJoinKeys []string         `json:"suggested_join_keys,omitempty"`
	SchemaHash        string           `json:"schema_hash"`
}

// ResolveExplorerTables handles POST /api/v1/explorer/nl/resolve-tables
// Orchestrates LLM table linking with schema retrieval
func ResolveExplorerTables(c *gin.Context) {
	_, ok := resolveUserID(c)
	if !ok {
		return
	}

	var req ResolveTablesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	}

	// Verify connection
	var connectorType, configEncrypted string
	err := database.QueryRow(`
		SELECT connector_type, config
		FROM connections
		WHERE id = $1 AND workspace_id = $2
	`, req.ConnectionID, activeWorkspaceID(c)).Scan(&connectorType, &configEncrypted)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load connection"})
		return
	}

	ctx := c.Request.Context()

	// Get schema index (with retrieval-based narrowing for large schemas)
	var schemaIndex *cache.ExplorerSchemaIndex
	if explorerCache != nil {
		schemaIndex, _ = explorerCache.GetSchemaIndex(ctx, req.ConnectionID)
	}

	if schemaIndex == nil {
		schemaIndex, err = buildSchemaIndex(ctx, req.ConnectionID, connectorType, configEncrypted, false)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to build schema index: %v", err)})
			return
		}
		if explorerCache != nil {
			_ = explorerCache.SetSchemaIndex(ctx, schemaIndex)
		}
	}

	// For large schemas (500+ tables), use retrieval to narrow first
	tablesToSend := schemaIndex.Tables
	if len(tablesToSend) > 40 {
		searchTerms := cache.ExtractSearchTerms(req.Question)
		tablesToSend = cache.RetrieveTopTables(schemaIndex, searchTerms, 40)
	}

	// Call LLM service for table linking
	llmServiceURL := os.Getenv("LLM_SERVICE_URL")
	if llmServiceURL == "" {
		llmServiceURL = "http://llm-service:5000"
	}

	// Build request for LLM service
	tablesPayload := make([]map[string]interface{}, 0, len(tablesToSend))
	for _, t := range tablesToSend {
		cols := make([]map[string]interface{}, 0, len(t.Columns))
		for _, c := range t.Columns {
			cols = append(cols, map[string]interface{}{
				"name":           c.Name,
				"type":           c.Type,
				"is_primary_key": c.IsPrimaryKey,
				"is_nullable":    c.IsNullable,
			})
		}
		tablesPayload = append(tablesPayload, map[string]interface{}{
			"name":      t.Name,
			"schema":    t.Schema,
			"row_count": t.RowCount,
			"columns":   cols,
		})
	}

	llmReq := map[string]interface{}{
		"question": req.Question,
		"tables":   tablesPayload,
	}
	if req.ConversationID != "" {
		llmReq["conversation_id"] = req.ConversationID
	}

	reqBody, _ := json.Marshal(llmReq)

	llmCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(llmCtx, "POST", llmServiceURL+"/api/v1/explorer/nl/resolve-tables", bytes.NewBuffer(reqBody))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create request"})
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")

	for k, v := range telemetry.InjectTraceToHeaders(ctx) {
		httpReq.Header.Set(k, v)
	}

	client := &http.Client{Timeout: 35 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		log.Errorf("[ResolveExplorerTables] LLM request failed: %v", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "LLM service unavailable"})
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		log.Errorf("[ResolveExplorerTables] LLM returned %d: %s", resp.StatusCode, string(body))
		c.JSON(resp.StatusCode, gin.H{"error": "Table resolution failed", "details": string(body)})
		return
	}

	var llmResp struct {
		Candidates        []TableCandidate `json:"candidates"`
		ConfidenceOverall float64          `json:"confidence_overall"`
		NeedsHITL         bool             `json:"needs_hitl"`
		HITLReason        string           `json:"hitl_reason"`
		SuggestedJoinKeys []string         `json:"suggested_join_keys"`
	}
	if err := json.Unmarshal(body, &llmResp); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse LLM response"})
		return
	}

	c.JSON(http.StatusOK, ResolveTablesResponse{
		Candidates:        llmResp.Candidates,
		ConfidenceOverall: llmResp.ConfidenceOverall,
		NeedsHITL:         llmResp.NeedsHITL,
		HITLReason:        llmResp.HITLReason,
		SuggestedJoinKeys: llmResp.SuggestedJoinKeys,
		SchemaHash:        schemaIndex.SchemaHash,
	})
}

// ColumnMapping represents column selections for query parts
type ColumnMapping struct {
	SelectCols  []string `json:"select_cols"`
	WhereCols   []string `json:"where_cols"`
	GroupByCols []string `json:"group_by_cols"`
	OrderByCols []string `json:"order_by_cols"`
}

// JoinPlan represents a join between tables
type JoinPlan struct {
	JoinType   string `json:"join_type"`
	LeftTable  string `json:"left_table"`
	RightTable string `json:"right_table"`
	Condition  string `json:"condition"`
}

// ResolveColumnsRequest represents a column resolution request
type ResolveColumnsRequest struct {
	ConnectionID   string                     `json:"connection_id" binding:"required"`
	Question       string                     `json:"question" binding:"required"`
	SelectedTables []cache.ExplorerTableIndex `json:"selected_tables" binding:"required"`
	ConversationID string                     `json:"conversation_id,omitempty"`
}

// ResolveColumnsResponse represents the column resolution response
type ResolveColumnsResponse struct {
	Columns          ColumnMapping `json:"columns"`
	JoinPlan         []JoinPlan    `json:"join_plan"`
	Confidence       float64       `json:"confidence"`
	NeedsHITL        bool          `json:"needs_hitl"`
	HITLReason       string        `json:"hitl_reason,omitempty"`
	AmbiguousColumns []string      `json:"ambiguous_columns,omitempty"`
}

// ResolveExplorerColumns handles POST /api/v1/explorer/nl/resolve-columns
// Orchestrates LLM column linking
func ResolveExplorerColumns(c *gin.Context) {
	_, ok := resolveUserID(c)
	if !ok {
		return
	}

	var req ResolveColumnsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	}

	// Verify connection
	var exists bool
	err := database.QueryRow(`
		SELECT EXISTS(SELECT 1 FROM connections WHERE id = $1 AND workspace_id = $2)
	`, req.ConnectionID, activeWorkspaceID(c)).Scan(&exists)

	if err != nil || !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}

	ctx := c.Request.Context()

	// Call LLM service for column linking
	llmServiceURL := os.Getenv("LLM_SERVICE_URL")
	if llmServiceURL == "" {
		llmServiceURL = "http://llm-service:5000"
	}

	// Build request
	tablesPayload := make([]map[string]interface{}, 0, len(req.SelectedTables))
	for _, t := range req.SelectedTables {
		cols := make([]map[string]interface{}, 0, len(t.Columns))
		for _, c := range t.Columns {
			cols = append(cols, map[string]interface{}{
				"name":           c.Name,
				"type":           c.Type,
				"is_primary_key": c.IsPrimaryKey,
				"is_nullable":    c.IsNullable,
			})
		}
		tablesPayload = append(tablesPayload, map[string]interface{}{
			"name":    t.Name,
			"schema":  t.Schema,
			"columns": cols,
		})
	}

	llmReq := map[string]interface{}{
		"question":        req.Question,
		"selected_tables": tablesPayload,
	}
	if req.ConversationID != "" {
		llmReq["conversation_id"] = req.ConversationID
	}

	reqBody, _ := json.Marshal(llmReq)

	llmCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(llmCtx, "POST", llmServiceURL+"/api/v1/explorer/nl/resolve-columns", bytes.NewBuffer(reqBody))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create request"})
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")

	for k, v := range telemetry.InjectTraceToHeaders(ctx) {
		httpReq.Header.Set(k, v)
	}

	httpClient := &http.Client{Timeout: 35 * time.Second}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		log.Errorf("[ResolveExplorerColumns] LLM request failed: %v", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "LLM service unavailable"})
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		c.JSON(resp.StatusCode, gin.H{"error": "Column resolution failed", "details": string(body)})
		return
	}

	var llmResp struct {
		Columns struct {
			SelectCols  []string `json:"select_cols"`
			WhereCols   []string `json:"where_cols"`
			GroupByCols []string `json:"group_by_cols"`
			OrderByCols []string `json:"order_by_cols"`
		} `json:"columns"`
		JoinPlan         []JoinPlan `json:"join_plan"`
		Confidence       float64    `json:"confidence"`
		NeedsHITL        bool       `json:"needs_hitl"`
		HITLReason       string     `json:"hitl_reason"`
		AmbiguousColumns []string   `json:"ambiguous_columns"`
	}
	if err := json.Unmarshal(body, &llmResp); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse LLM response"})
		return
	}

	c.JSON(http.StatusOK, ResolveColumnsResponse{
		Columns: ColumnMapping{
			SelectCols:  llmResp.Columns.SelectCols,
			WhereCols:   llmResp.Columns.WhereCols,
			GroupByCols: llmResp.Columns.GroupByCols,
			OrderByCols: llmResp.Columns.OrderByCols,
		},
		JoinPlan:         llmResp.JoinPlan,
		Confidence:       llmResp.Confidence,
		NeedsHITL:        llmResp.NeedsHITL,
		HITLReason:       llmResp.HITLReason,
		AmbiguousColumns: llmResp.AmbiguousColumns,
	})
}

// NextStepSuggestion represents a suggested action
type NextStepSuggestion struct {
	ActionType     string   `json:"action_type"`
	Title          string   `json:"title"`
	Description    string   `json:"description"`
	Confidence     float64  `json:"confidence"`
	RequiredInputs []string `json:"required_inputs"`
	CTA            string   `json:"cta"`
}

// GetNextStepsRequest represents a next steps request
type GetNextStepsRequest struct {
	ConnectionID     string                 `json:"connection_id"`
	Question         string                 `json:"question" binding:"required"`
	SQL              string                 `json:"sql" binding:"required"`
	ResultProfile    map[string]interface{} `json:"result_profile" binding:"required"`
	AvailableActions []string               `json:"available_actions"`
}

// GetNextStepsResponse represents the next steps response
type GetNextStepsResponse struct {
	Suggestions []NextStepSuggestion `json:"suggestions"`
}

// GetExplorerNextSteps handles POST /api/v1/explorer/nl/next-steps
// Gets LLM-suggested next actions based on query results
func GetExplorerNextSteps(c *gin.Context) {
	_, ok := resolveUserID(c)
	if !ok {
		return
	}

	var req GetNextStepsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	if len(req.AvailableActions) == 0 {
		req.AvailableActions = []string{"metabase", "download_csv", "slack", "email"}
	}

	ctx := c.Request.Context()

	// Call LLM service
	llmServiceURL := os.Getenv("LLM_SERVICE_URL")
	if llmServiceURL == "" {
		llmServiceURL = "http://llm-service:5000"
	}

	llmReq := map[string]interface{}{
		"question":          req.Question,
		"sql":               req.SQL,
		"result_profile":    req.ResultProfile,
		"available_actions": req.AvailableActions,
	}

	reqBody, _ := json.Marshal(llmReq)

	llmCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(llmCtx, "POST", llmServiceURL+"/api/v1/explorer/nl/next-steps", bytes.NewBuffer(reqBody))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create request"})
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")

	for k, v := range telemetry.InjectTraceToHeaders(ctx) {
		httpReq.Header.Set(k, v)
	}

	httpClient := &http.Client{Timeout: 25 * time.Second}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		log.Errorf("[GetExplorerNextSteps] LLM request failed: %v", err)
		// Return default suggestions on LLM failure
		c.JSON(http.StatusOK, GetNextStepsResponse{
			Suggestions: []NextStepSuggestion{
				{ActionType: "metabase", Title: "Create Dashboard", Description: "Visualize results", Confidence: 0.8, RequiredInputs: []string{"dashboard_name"}, CTA: "Create"},
				{ActionType: "download_csv", Title: "Download CSV", Description: "Export results", Confidence: 0.7, RequiredInputs: []string{}, CTA: "Download"},
			},
		})
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var llmResp struct {
		Suggestions []NextStepSuggestion `json:"suggestions"`
	}
	if err := json.Unmarshal(body, &llmResp); err != nil {
		// Return default on parse failure
		c.JSON(http.StatusOK, GetNextStepsResponse{
			Suggestions: []NextStepSuggestion{
				{ActionType: "metabase", Title: "Create Dashboard", Description: "Visualize results", Confidence: 0.8, RequiredInputs: []string{"dashboard_name"}, CTA: "Create"},
			},
		})
		return
	}

	c.JSON(http.StatusOK, GetNextStepsResponse{
		Suggestions: llmResp.Suggestions,
	})
}

// =============================================================================
// ACTION EXECUTION ENDPOINTS (CSV, Slack, Email)
// =============================================================================

// ExportCSVHandler handles GET /api/v1/explorer/export.csv
// Exports query results as CSV file
func ExportCSVHandler(c *gin.Context) {
	_, ok := resolveUserID(c)
	if !ok {
		return
	}

	connectionID := c.Query("connection_id")
	sqlQuery := c.Query("sql")

	if connectionID == "" || sqlQuery == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "connection_id and sql are required"})
		return
	}

	// Validate SQL
	validation := validators.ValidateExplorerSQL(sqlQuery)
	if !validation.Valid {
		c.JSON(http.StatusBadRequest, gin.H{"error": validation.ErrorMessage})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	}

	var connectorType, configEncrypted string
	err := database.QueryRow(`
		SELECT connector_type, config
		FROM connections
		WHERE id = $1 AND workspace_id = $2
	`, connectionID, activeWorkspaceID(c)).Scan(&connectorType, &configEncrypted)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load connection"})
		return
	}

	ct := strings.ToLower(connectorType)
	ecap := ResolveExplorerCapability(connectorType)
	if !ecap.Supported {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Unsupported connection type for export"})
		return
	}
	isPostgres := strings.Contains(ct, "postgres") || strings.Contains(ct, "redshift")
	isMySQL := strings.Contains(ct, "mysql") || strings.Contains(ct, "mariadb")
	isSQLServer := strings.Contains(ct, "sqlserver") || strings.Contains(ct, "mssql")

	configJSON, err := crypto.DecryptString(configEncrypted)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decrypt config"})
		return
	}

	var config map[string]interface{}
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse config"})
		return
	}

	// Execute query with higher limit for export. PII redaction is
	// bypassed here (T2-7): the export handler's ownership check at
	// line 2687 ensures the caller IS the data owner — they should
	// see their own raw email / phone / SSN values in CSV downloads.
	// The interactive preview path (line 294-298) still redacts.
	var result *ExplorerQueryResponse
	if ecap.ExecStrategy == execDelegated {
		result, err = queryViaOrchestrator(c.Request.Context(), connectorType, config, sqlQuery, 10000, false, false)
	} else if isPostgres {
		result, err = executePostgresQueryUnredacted(c.Request.Context(), config, sqlQuery, 10000)
	} else if isMySQL {
		result, err = executeMySQLQueryUnredacted(c.Request.Context(), config, sqlQuery, 10000)
	} else if isSQLServer {
		result, err = executeSQLServerQueryUnredacted(c.Request.Context(), config, sqlQuery, 10000)
	} else {
		result, err = executeDatabricksQueryUnredacted(c.Request.Context(), config, sqlQuery, 10000)
	}
	if err != nil {
		log.Errorf("explorer query failed conn=%s: %v", connectionID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query execution failed; check your SQL and try again"})
		return
	}

	// Build CSV
	var csvBuilder strings.Builder

	// Header row
	csvBuilder.WriteString(strings.Join(result.Columns, ","))
	csvBuilder.WriteString("\n")

	// Data rows
	for _, row := range result.Rows {
		var values []string
		for _, col := range result.Columns {
			val := row[col]
			valStr := ""
			if val != nil {
				valStr = fmt.Sprintf("%v", val)
				// Escape quotes and commas
				if strings.Contains(valStr, ",") || strings.Contains(valStr, "\"") || strings.Contains(valStr, "\n") {
					valStr = "\"" + strings.ReplaceAll(valStr, "\"", "\"\"") + "\""
				}
			}
			values = append(values, valStr)
		}
		csvBuilder.WriteString(strings.Join(values, ","))
		csvBuilder.WriteString("\n")
	}

	// Set headers for CSV download
	c.Header("Content-Type", "text/csv")
	c.Header("Content-Disposition", "attachment; filename=export.csv")
	c.String(http.StatusOK, csvBuilder.String())
}

// ExportQueryRequest is the POST body for /api/v1/explorer/export.
// Mirrors the shape the frontend already sends from the Download
// dropdown so we don't need to change client code.
type ExportQueryRequest struct {
	ConnectionID string `json:"connection_id" binding:"required"`
	SQL          string `json:"sql"          binding:"required"`
	Format       string `json:"format"` // csv | tsv | json (default: csv)
	Limit        int    `json:"limit"`  // server-clamped to 10000
}

// ExportQueryHandler handles POST /api/v1/explorer/export.
//
// D4 fix: the Download dropdown in Explorer was POSTing to this route
// with {connection_id, sql, format, limit:10000} but the backend only
// had GET /explorer/export.csv with query params — so every download
// returned 404 silently. This handler makes the advertised 10K-row
// multi-format export actually work.
//
// Format support:
//   - csv:  RFC4180-ish, quote+escape commas/quotes/newlines
//   - tsv:  tab-separated, newlines in values replaced with spaces
//   - json: array of objects (one per row)
//   - xlsx: returned as 400 (no Go xlsx dep wired up). The frontend
//     hides this option; this guard is for direct curl calls.
//
// Ownership: scoped to workspace-shared (connection_id, workspace_id) — same
// primitive as ExecuteExplorerQuery. PII redaction is INTENTIONALLY bypassed
// (same call-site rationale as the legacy GET handler).
func ExportQueryHandler(c *gin.Context) {
	_, ok := resolveUserID(c)
	if !ok {
		return
	}

	var req ExportQueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Server-side limit clamp. Client sends 10000; we trust nothing.
	if req.Limit <= 0 {
		req.Limit = 10000
	}
	if req.Limit > 10000 {
		req.Limit = 10000
	}

	format := strings.ToLower(strings.TrimSpace(req.Format))
	if format == "" {
		format = "csv"
	}
	if format != "csv" && format != "tsv" && format != "json" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("Unsupported export format: %q", req.Format),
			"hint":  "Supported formats: csv, tsv, json",
		})
		return
	}

	// Validate SQL using the same AST-based validator as Run Query.
	validation := validators.ValidateExplorerSQL(req.SQL)
	if !validation.Valid {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":      validation.ErrorMessage,
			"error_code": validation.ErrorCode,
			"hint":       "Explorer only supports read-only SELECT queries for safety",
		})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	}

	var connectorType, configEncrypted string
	err := database.QueryRow(`
		SELECT connector_type, config
		FROM connections
		WHERE id = $1 AND workspace_id = $2
	`, req.ConnectionID, activeWorkspaceID(c)).Scan(&connectorType, &configEncrypted)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}
	if err != nil {
		log.Errorf("[ExportQueryHandler] Failed to load connection: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load connection"})
		return
	}

	ct := strings.ToLower(connectorType)
	ecap := ResolveExplorerCapability(connectorType)
	if !ecap.Supported {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("Unsupported connection type for export: %s", connectorType),
			"hint":  "Export currently supports PostgreSQL/Redshift, MySQL, Databricks, SQL Server, and BigQuery",
		})
		return
	}
	isPostgres := strings.Contains(ct, "postgres") || strings.Contains(ct, "redshift")
	isMySQL := strings.Contains(ct, "mysql") || strings.Contains(ct, "mariadb")
	isDatabricks := strings.Contains(ct, "databricks")
	isSQLServer := strings.Contains(ct, "sqlserver") || strings.Contains(ct, "mssql")

	configJSON, err := crypto.DecryptString(configEncrypted)
	if err != nil {
		log.Errorf("[ExportQueryHandler] Failed to decrypt config: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decrypt connection config"})
		return
	}

	var config map[string]interface{}
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse connection config"})
		return
	}

	// PII redaction bypassed: the workspace-scoped (connection_id, workspace_id)
	// ownership check above guarantees the caller owns this data and should see
	// raw values in their CSV/TSV/JSON. Mirrors the GET handler's
	// rationale (T2-7).
	var result *ExplorerQueryResponse
	switch {
	case ecap.ExecStrategy == execDelegated:
		result, err = queryViaOrchestrator(c.Request.Context(), connectorType, config, req.SQL, req.Limit, false, false)
	case isPostgres:
		result, err = executePostgresQueryUnredacted(c.Request.Context(), config, req.SQL, req.Limit)
	case isMySQL:
		result, err = executeMySQLQueryUnredacted(c.Request.Context(), config, req.SQL, req.Limit)
	case isSQLServer:
		result, err = executeSQLServerQueryUnredacted(c.Request.Context(), config, req.SQL, req.Limit)
	case isDatabricks:
		result, err = executeDatabricksQueryUnredacted(c.Request.Context(), config, req.SQL, req.Limit)
	}
	if err != nil {
		log.Errorf("[ExportQueryHandler] Query execution failed: %v", err)
		errorType, suggestion := validators.ClassifyExecutionError(err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":      fmt.Sprintf("Query execution failed: %v", err),
			"error_type": errorType,
			"suggestion": suggestion,
		})
		return
	}

	switch format {
	case "csv":
		writeDelimited(c, result, ',', "text/csv; charset=utf-8", "export.csv")
	case "tsv":
		writeDelimited(c, result, '\t', "text/tab-separated-values; charset=utf-8", "export.tsv")
	case "json":
		c.Header("Content-Type", "application/json; charset=utf-8")
		c.Header("Content-Disposition", "attachment; filename=export.json")
		c.JSON(http.StatusOK, gin.H{
			"columns":   result.Columns,
			"rows":      result.Rows,
			"row_count": result.RowCount,
			"truncated": result.Truncated,
		})
	}
}

// writeDelimited streams CSV-ish output (comma or tab separated)
// using a strings.Builder. Quoting follows the CSV convention:
// if a value contains the separator, a double-quote, or a newline,
// the whole value is wrapped in double-quotes and any internal
// double-quotes are doubled. For TSV we additionally collapse
// embedded newlines / tabs to spaces so the row structure stays
// intact even when consumed by tools that don't honor quoting.
func writeDelimited(c *gin.Context, result *ExplorerQueryResponse, sep byte, contentType, filename string) {
	var b strings.Builder
	// Pre-size: headers + roughly 64 chars per row+column.
	b.Grow(len(result.Columns)*16 + len(result.Rows)*len(result.Columns)*64)

	needsQuoteCSV := func(s string) bool {
		return strings.ContainsAny(s, ",\"\n\r")
	}
	quote := func(s string) string {
		return "\"" + strings.ReplaceAll(s, "\"", "\"\"") + "\""
	}

	// Header row.
	for i, col := range result.Columns {
		if i > 0 {
			b.WriteByte(sep)
		}
		if sep == ',' && needsQuoteCSV(col) {
			b.WriteString(quote(col))
		} else if sep == '\t' {
			b.WriteString(strings.NewReplacer("\t", " ", "\n", " ", "\r", " ").Replace(col))
		} else {
			b.WriteString(col)
		}
	}
	b.WriteByte('\n')

	for _, row := range result.Rows {
		for i, col := range result.Columns {
			if i > 0 {
				b.WriteByte(sep)
			}
			val := row[col]
			if val == nil {
				continue
			}
			s := fmt.Sprintf("%v", val)
			if sep == ',' {
				if needsQuoteCSV(s) {
					b.WriteString(quote(s))
				} else {
					b.WriteString(s)
				}
			} else { // tab
				b.WriteString(strings.NewReplacer("\t", " ", "\n", " ", "\r", " ").Replace(s))
			}
		}
		b.WriteByte('\n')
	}

	c.Header("Content-Type", contentType)
	c.Header("Content-Disposition", "attachment; filename="+filename)
	c.String(http.StatusOK, b.String())
}

// ShareToSlackRequest represents a Slack share request
type ShareToSlackRequest struct {
	WebhookURL string                   `json:"webhook_url" binding:"required"`
	Text       string                   `json:"text"`
	Blocks     []map[string]interface{} `json:"blocks,omitempty"`
	Attachment map[string]interface{}   `json:"attachment,omitempty"`
}

// ShareToSlack handles POST /api/v1/explorer/share/slack
// Sends query results/summary to Slack via webhook
func ShareToSlack(c *gin.Context) {
	_, ok := resolveUserID(c)
	if !ok {
		return
	}

	var req ShareToSlackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Validate webhook URL (basic check)
	if !strings.HasPrefix(req.WebhookURL, "https://hooks.slack.com/") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid Slack webhook URL"})
		return
	}

	// Build Slack payload
	payload := map[string]interface{}{
		"text": req.Text,
	}
	if len(req.Blocks) > 0 {
		payload["blocks"] = req.Blocks
	}
	if req.Attachment != nil {
		payload["attachments"] = []map[string]interface{}{req.Attachment}
	}

	payloadBytes, _ := json.Marshal(payload)

	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, "POST", req.WebhookURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create request"})
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		log.Errorf("[ShareToSlack] Request failed: %v", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Failed to send to Slack"})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		c.JSON(http.StatusBadGateway, gin.H{
			"error":   "Slack returned error",
			"status":  resp.StatusCode,
			"details": string(body),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "message": "Sent to Slack"})
}

// ShareViaEmailRequest represents an email share request
type ShareViaEmailRequest struct {
	To         []string         `json:"to" binding:"required"`
	Subject    string           `json:"subject" binding:"required"`
	Body       string           `json:"body"`
	HTMLBody   string           `json:"html_body,omitempty"`
	Attachment *EmailAttachment `json:"attachment,omitempty"`
}

// EmailAttachment represents an email attachment
type EmailAttachment struct {
	Filename string `json:"filename"`
	Content  string `json:"content"` // Base64 encoded
	MimeType string `json:"mime_type"`
}

// ShareViaEmail handles POST /api/v1/explorer/share/email
// Sends query results via SMTP email
// shareViaEmailLimiter rate-limits ShareViaEmail per user (in-memory).
// In-process limiter is sufficient for first-pilot scale; for HA the
// state needs to move to Redis.
var (
	shareViaEmailLimiter   = make(map[string][]time.Time)
	shareViaEmailLimiterMu sync.Mutex
)

const (
	shareEmailMaxPerHour = 10 // max emails per user per hour
	shareEmailMaxToCount = 5  // max recipients per single send
)

// shareViaEmailRateLimit returns true when the caller has exceeded
// shareEmailMaxPerHour sends in the last hour. Sliding window.
func shareViaEmailRateLimit(userID string) (allowed bool, retryAfter time.Duration) {
	shareViaEmailLimiterMu.Lock()
	defer shareViaEmailLimiterMu.Unlock()
	now := time.Now()
	cutoff := now.Add(-time.Hour)
	hist := shareViaEmailLimiter[userID]
	kept := hist[:0]
	for _, t := range hist {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	if len(kept) >= shareEmailMaxPerHour {
		oldest := kept[0]
		return false, time.Until(oldest.Add(time.Hour))
	}
	kept = append(kept, now)
	shareViaEmailLimiter[userID] = kept
	return true, 0
}

func ShareViaEmail(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	var req ShareViaEmailRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// T1-9 security gate: recipient cap + per-user-hour rate limit +
	// optional domain allowlist. Pre-fix this endpoint accepted
	// arbitrary `to[]` and sent via platform SMTP creds — an open
	// phishing relay that would burn the platform's SPF/DKIM
	// reputation within hours.
	if len(req.To) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "to[] is required"})
		return
	}
	if len(req.To) > shareEmailMaxToCount {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("at most %d recipients per email", shareEmailMaxToCount),
		})
		return
	}

	// Domain allowlist gate. SHARE_EMAIL_ALLOWED_DOMAINS is a comma-
	// separated list (e.g. "rsync-ai.local,example.com"); when set,
	// every recipient must be on the list. When unset, only the
	// caller's own email address is accepted (read from session) to
	// keep the default safe. Operator opts into broader use by
	// setting the env var explicitly.
	allowedDomainsRaw := strings.TrimSpace(os.Getenv("SHARE_EMAIL_ALLOWED_DOMAINS"))
	var allowedDomains []string
	if allowedDomainsRaw != "" {
		for _, d := range strings.Split(allowedDomainsRaw, ",") {
			d = strings.ToLower(strings.TrimSpace(d))
			if d != "" {
				allowedDomains = append(allowedDomains, d)
			}
		}
	}
	callerEmail := strings.ToLower(strings.TrimSpace(c.GetString("user_email")))
	for _, addr := range req.To {
		a := strings.ToLower(strings.TrimSpace(addr))
		if a == "" || !strings.Contains(a, "@") {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid email address: %q", addr)})
			return
		}
		// Allow caller's own address unconditionally (self-send is harmless).
		if callerEmail != "" && a == callerEmail {
			continue
		}
		if len(allowedDomains) == 0 {
			c.JSON(http.StatusForbidden, gin.H{
				"error":   "recipient_not_allowed",
				"message": fmt.Sprintf("Sending to %q is not allowed. By default this endpoint only sends to your own email. Set SHARE_EMAIL_ALLOWED_DOMAINS to permit additional domains.", addr),
			})
			return
		}
		dom := a[strings.LastIndex(a, "@")+1:]
		matched := false
		for _, allowed := range allowedDomains {
			if dom == allowed {
				matched = true
				break
			}
		}
		if !matched {
			c.JSON(http.StatusForbidden, gin.H{
				"error":   "recipient_domain_not_allowed",
				"message": fmt.Sprintf("Domain %q is not in SHARE_EMAIL_ALLOWED_DOMAINS.", dom),
			})
			return
		}
	}

	// Per-user rate limit.
	if allowed, retry := shareViaEmailRateLimit(userID); !allowed {
		retrySec := int(retry.Seconds())
		if retrySec < 1 {
			retrySec = 1
		}
		c.Header("Retry-After", strconv.Itoa(retrySec))
		c.JSON(http.StatusTooManyRequests, gin.H{
			"error":       "rate_limit_exceeded",
			"message":     fmt.Sprintf("max %d emails/hour per user; try again in %ds", shareEmailMaxPerHour, retrySec),
			"retry_after": retrySec,
		})
		return
	}

	// Get SMTP config from environment
	smtpHost := os.Getenv("SMTP_HOST")
	smtpPort := os.Getenv("SMTP_PORT")
	smtpUser := os.Getenv("SMTP_USER")
	smtpPass := os.Getenv("SMTP_PASSWORD")
	smtpFrom := os.Getenv("SMTP_FROM")

	if smtpHost == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Email service not configured",
			"hint":  "Set SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SMTP_FROM environment variables",
		})
		return
	}

	if smtpPort == "" {
		smtpPort = "587"
	}
	if smtpFrom == "" {
		smtpFrom = smtpUser
	}

	// Build email message. Subject and Body are inserted directly
	// after CRLF-strip to prevent header-injection — a CRLF in either
	// could let an attacker append extra headers (Bcc, Reply-To,
	// alternative From) and turn this into a relay anyway.
	sanitizedSubject := strings.ReplaceAll(strings.ReplaceAll(req.Subject, "\r", " "), "\n", " ")
	sanitizedBody := strings.ReplaceAll(req.Body, "\r\n.\r\n", "\r\n. \r\n") // defang SMTP end-of-message
	var msg strings.Builder
	msg.WriteString(fmt.Sprintf("From: %s\r\n", smtpFrom))
	msg.WriteString(fmt.Sprintf("To: %s\r\n", strings.Join(req.To, ", ")))
	msg.WriteString(fmt.Sprintf("Reply-To: %s\r\n", callerEmail))
	msg.WriteString(fmt.Sprintf("Subject: %s\r\n", sanitizedSubject))
	msg.WriteString(fmt.Sprintf("X-Sent-By-rsync-ai-User: %s\r\n", userID))
	msg.WriteString("MIME-Version: 1.0\r\n")
	msg.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
	msg.WriteString("\r\n")
	msg.WriteString(sanitizedBody)
	msg.WriteString("\r\n\r\n--\r\nSent via rsync-ai on behalf of ")
	msg.WriteString(callerEmail)

	// Send via SMTP
	addr := fmt.Sprintf("%s:%s", smtpHost, smtpPort)

	var auth smtp.Auth
	if smtpUser != "" && smtpPass != "" {
		auth = smtp.PlainAuth("", smtpUser, smtpPass, smtpHost)
	}

	err := smtp.SendMail(addr, auth, smtpFrom, req.To, []byte(msg.String()))
	if err != nil {
		log.WithError(err).WithField("user_id", userID).Error("[ShareViaEmail] SMTP send failed")
		// Audit: log the failure server-side, but DON'T return SMTP
		// internals to the client (T1-9-extra: previously leaked
		// SMTP hostname / auth-mode hints to anyone who could
		// trigger an error).
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Failed to send email",
		})
		return
	}

	logAudit(c, "share_email_sent", "explorer", "", map[string]interface{}{
		"recipients":      req.To,
		"recipient_count": len(req.To),
		"subject_len":     len(sanitizedSubject),
	})

	c.JSON(http.StatusOK, gin.H{"success": true, "message": "Email sent", "recipients": req.To})
}
