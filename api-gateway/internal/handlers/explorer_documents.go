package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"api-gateway/internal/cache"
	"api-gateway/internal/db"
	"api-gateway/internal/telemetry"
	"api-gateway/internal/validators"

	"github.com/rsync-ai/shared/crypto"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// Document browse mode (MongoDB) for the Data Explorer. See
// docs/explorer/document-browse-mode-plan.md.
//
// Trust boundary: the filter is validated here (validators.ValidateDocumentFind) and
// again by the connector's own `find` allowlist — the two lists are kept in lockstep
// by TestDocumentFindOperatorsMatchConnector. The connection is loaded workspace-
// scoped, so any workspace member can browse the connections of their own workspace
// and nothing else — the same rule as a SELECT on /explorer/query.

const (
	// documentFindTimeout covers the connector's own max_time_ms ceiling plus the MCP
	// round trip.
	documentFindTimeout = 60 * time.Second
	// documentFindMaxResponseBytes caps what the gateway will read back. The connector
	// already stops adding documents past its own byte budget; this is the backstop.
	documentFindMaxResponseBytes = 32 << 20
	// documentSchemaMaxCollections bounds discover_schema, which samples every
	// collection it lists.
	documentSchemaMaxCollections = 200
)

type explorerDocumentFindRequest struct {
	ConnectionID string          `json:"connection_id"`
	Collection   string          `json:"collection"`
	Filter       json.RawMessage `json:"filter,omitempty"`
	Projection   json.RawMessage `json:"projection,omitempty"`
	Sort         json.RawMessage `json:"sort,omitempty"`
	Limit        int             `json:"limit,omitempty"`
	Cursor       string          `json:"cursor,omitempty"`
	Skip         int             `json:"skip,omitempty"`
}

// FindExplorerDocuments handles POST /api/v1/explorer/documents/find.
func FindExplorerDocuments(c *gin.Context) {
	if _, ok := resolveUserID(c); !ok {
		return
	}

	var req explorerDocumentFindRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error(), "error_code": "invalid_request"})
		return
	}
	if strings.TrimSpace(req.ConnectionID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "connection_id is required", "error_code": "invalid_request", "path": "connection_id"})
		return
	}

	spec, ferr := validators.ValidateDocumentFind(validators.DocumentFindRequest{
		Collection: req.Collection,
		Filter:     req.Filter,
		Projection: req.Projection,
		Sort:       req.Sort,
		Limit:      req.Limit,
		Cursor:     req.Cursor,
		Skip:       req.Skip,
	})
	if ferr != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": ferr.Message, "error_code": ferr.Code, "path": ferr.Path})
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
		log.Errorf("[FindExplorerDocuments] Failed to load connection: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load connection"})
		return
	}

	if ResolveExplorerCapability(connectorType).QueryLanguage != langDocument {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":      fmt.Sprintf("Document browse is not available for %s connections", connectorType),
			"error_code": "not_document_connection",
		})
		return
	}

	configJSON, err := crypto.DecryptString(configEncrypted)
	if err != nil {
		log.Errorf("[FindExplorerDocuments] Failed to decrypt config: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decrypt connection config"})
		return
	}
	var config map[string]interface{}
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse connection config"})
		return
	}

	status, body := findDocumentsViaOrchestrator(c.Request.Context(), req.ConnectionID, connectorType, config, spec)
	c.JSON(status, body)
}

// findDocumentsViaOrchestrator runs the connector's `find` tool and returns the HTTP
// status and body for the browser. Refusals (400) and timeouts (504) keep their status
// and error_code; anything else that is not a 200 becomes a 502.
func findDocumentsViaOrchestrator(ctx context.Context, connectionID, connectorType string, config map[string]interface{}, spec *validators.DocumentFindSpec) (int, gin.H) {
	payload := struct {
		ConnectorType string                 `json:"connector_type"`
		ConnectionID  string                 `json:"connection_id,omitempty"`
		Config        map[string]interface{} `json:"config"`
		*validators.DocumentFindSpec
	}{connectorType, connectionID, config, spec}
	reqBody, err := json.Marshal(payload)
	if err != nil {
		return http.StatusInternalServerError, gin.H{"error": "Failed to build find request"}
	}

	ctx, cancel := context.WithTimeout(ctx, documentFindTimeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(orchestratorBaseURL(), "/")+"/api/v1/agent/explorer-find", bytes.NewReader(reqBody))
	if err != nil {
		return http.StatusInternalServerError, gin.H{"error": "Failed to build find request"}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	for k, v := range telemetry.InjectTraceToHeaders(ctx) {
		httpReq.Header.Set(k, v)
	}
	setInternalServiceSecret(httpReq)

	resp, err := (&http.Client{Timeout: documentFindTimeout + 5*time.Second}).Do(httpReq)
	if err != nil {
		log.Warnf("[FindExplorerDocuments] orchestrator unreachable: %v", err)
		if ctx.Err() == context.DeadlineExceeded {
			return http.StatusGatewayTimeout, gin.H{"error": "The find timed out", "error_code": "query_timeout"}
		}
		return http.StatusBadGateway, gin.H{"error": "Document browse is unavailable right now", "error_code": "connection_failed"}
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, documentFindMaxResponseBytes+1))
	if err != nil {
		return http.StatusBadGateway, gin.H{"error": "Failed to read the find response", "error_code": "query_failed"}
	}
	if len(raw) > documentFindMaxResponseBytes {
		return http.StatusBadGateway, gin.H{"error": "The find response was too large; add a projection or lower the limit", "error_code": "response_too_large"}
	}

	if resp.StatusCode != http.StatusOK {
		var env struct {
			Error     string `json:"error"`
			ErrorCode string `json:"error_code"`
			Path      string `json:"path"`
			Details   string `json:"details"`
		}
		_ = json.Unmarshal(raw, &env)
		msg := strings.TrimSpace(env.Details)
		if msg == "" {
			msg = strings.TrimSpace(env.Error)
		}
		if msg == "" {
			msg = fmt.Sprintf("find failed (orchestrator returned %d)", resp.StatusCode)
		}
		code := env.ErrorCode
		if code == "" {
			code = "query_failed"
		}
		status := resp.StatusCode
		if status != http.StatusBadRequest && status != http.StatusGatewayTimeout {
			status = http.StatusBadGateway
		}
		out := gin.H{"error": msg, "error_code": code}
		if env.Path != "" {
			out["path"] = env.Path
		}
		return status, out
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var result map[string]interface{}
	if err := dec.Decode(&result); err != nil || result == nil {
		return http.StatusBadGateway, gin.H{"error": "Failed to decode the find response", "error_code": "query_failed"}
	}
	delete(result, "success")
	if docs, ok := result["documents"].([]interface{}); ok {
		for i, d := range docs {
			docs[i] = redactDocument(d)
		}
	} else {
		result["documents"] = []interface{}{}
	}
	return http.StatusOK, gin.H(result)
}

// redactDocument applies the preview PII/secret redaction by key name at every depth
// of a document. A redacted scalar keeps the same two-character hint the SQL preview
// gives; a redacted object or array collapses to "***" so nothing of its shape leaks.
func redactDocument(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, val := range t {
			if shouldRedactColumnName(k) {
				switch val.(type) {
				case map[string]interface{}, []interface{}:
					t[k] = "***"
				default:
					t[k] = redactForPreview(val)
				}
				continue
			}
			t[k] = redactDocument(val)
		}
		return t
	case []interface{}:
		for i, val := range t {
			t[i] = redactDocument(val)
		}
		return t
	default:
		return v
	}
}

// buildDocumentSchemaIndex lists a document connection's collections (with sampled
// field names and types) through the connector's discover_schema tool. Row counts are
// not requested: the connector counts with count_documents({}), a full scan per
// collection.
func buildDocumentSchemaIndex(ctx context.Context, connectionID, connectorType, configEncrypted string) (*cache.ExplorerSchemaIndex, error) {
	configJSON, err := crypto.DecryptString(configEncrypted)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt config: %w", err)
	}
	var config map[string]interface{}
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	body, err := json.Marshal(map[string]interface{}{
		"task":               "discover_schema",
		"connection_id":      connectionID,
		"connector_type":     connectorType,
		"config":             config,
		"include_columns":    true,
		"include_row_counts": false,
		"max_tables":         documentSchemaMaxCollections,
	})
	if err != nil {
		return nil, fmt.Errorf("build discovery request: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(orchestratorBaseURL(), "/")+"/api/v1/agent/discover-schema", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build discovery request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range telemetry.InjectTraceToHeaders(ctx) {
		req.Header.Set(k, v)
	}
	setInternalServiceSecret(req)

	resp, err := (&http.Client{Timeout: 95 * time.Second}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("discovery unavailable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("discovery failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var envelope struct {
		Tables []TableMetadata `json:"tables"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("parse discovery response: %w", err)
	}

	tables := make([]cache.ExplorerTableIndex, 0, len(envelope.Tables))
	for _, t := range envelope.Tables {
		pk := map[string]bool{}
		for _, k := range t.PrimaryKeys {
			pk[k] = true
		}
		idx := cache.ExplorerTableIndex{
			Name:       t.Name,
			Schema:     t.Schema,
			RowCount:   t.RowCount,
			PrimaryKey: t.PrimaryKeys,
			Columns:    make([]cache.ExplorerColumnIndex, 0, len(t.Columns)),
		}
		for _, col := range t.Columns {
			idx.Columns = append(idx.Columns, cache.ExplorerColumnIndex{
				Name:         col.Name,
				Type:         col.Type,
				IsPrimaryKey: col.IsPrimaryKey || pk[col.Name],
				IsNullable:   col.Nullable,
			})
		}
		idx.SearchTokens = cache.BuildSearchTokens(idx)
		tables = append(tables, idx)
	}

	return &cache.ExplorerSchemaIndex{
		ConnectionID:    connectionID,
		SchemaHash:      cache.ComputeSchemaHash(tables, nil),
		LastRefreshedAt: time.Now(),
		TableCount:      len(tables),
		Tables:          tables,
		ForeignKeys:     []cache.ExplorerForeignKeyIndex{},
	}, nil
}
