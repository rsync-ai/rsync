package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	"github.com/rsync-ai/backend-orchestrator/pkg/namespacemodel"
	"github.com/rsync-ai/shared/crypto"
	log "github.com/sirupsen/logrus"
)

// namespacesTimeout bounds one listing, including a cold connector start on
// the orchestrator side.
const namespacesTimeout = 30 * time.Second

// ListConnectionNamespaces answers GET /api/v1/connections/:id/namespaces with
// the names one level above the connection's tables: schemas, databases or
// datasets, whichever the connector's metadata.json names as
// namespace_model.table_namespace (returned as namespace_kind). The connector
// lists them itself (its list_namespaces operation, through the orchestrator's
// /agent/list-namespaces) and leaves its engine's system namespaces out.
// current is the namespace the connection itself names, "" when none; it is
// always one of namespaces, even when the listing leaves it out (a database
// the login may connect to but not enumerate).
//
// A connector whose model names no table_namespace (object stores, SaaS APIs)
// has no list_namespaces tool: it answers 200 with lists_namespaces false, no
// namespaces and an empty namespace_kind instead of calling the connector. A
// connector that fails to list answers 424 with its reason — never 502, whose
// body the edge proxy replaces with its own page, losing the reason. The
// gateway's own failures (loading or decrypting the connection) are 500.
func ListConnectionNamespaces(c *gin.Context) {
	connectionID, ok := requireUUIDParam(c, "id", "invalid_connection_id", "Invalid connection ID format")
	if !ok {
		return
	}
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	// Connections are workspace-shared: any member of the active workspace may
	// list a shared connection's namespaces; another workspace's id is a 404.
	wsID := activeWorkspaceID(c)
	if wsID == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	}

	var connectorType, configEncrypted string
	var connectorVersion sql.NullString
	err := database.QueryRow(`
		SELECT connector_type, config, connector_version
		FROM connections
		WHERE id = $1 AND workspace_id = $2
	`, connectionID, wsID).Scan(&connectorType, &configEncrypted, &connectorVersion)
	if errors.Is(err, sql.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"user_id": userID, "connection_id": connectionID}).
			Error("ListConnectionNamespaces failed to load connection")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "listing namespaces failed", "details": "Could not load connection details."})
		return
	}
	if nothingToList(connectorType) {
		c.JSON(http.StatusOK, gin.H{
			"connection_id":    connectionID,
			"connector_type":   connectorType,
			"lists_namespaces": false,
			"namespace_kind":   "",
			"namespaces":       []string{},
			"current":          "",
		})
		return
	}
	configJSON, err := crypto.DecryptString(configEncrypted)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"user_id": userID, "connection_id": connectionID}).
			Warn("ListConnectionNamespaces failed to decrypt config")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "listing namespaces failed", "details": "Connection credentials could not be decrypted."})
		return
	}
	config := map[string]interface{}{}
	_ = json.Unmarshal([]byte(configJSON), &config)
	if config == nil {
		config = map[string]interface{}{}
	}
	// Route to the connector version the connection is pinned to, as the
	// preview does.
	if connectorVersion.Valid && strings.TrimSpace(connectorVersion.String) != "" {
		if _, has := config["connector_version"]; !has {
			config["connector_version"] = connectorVersion.String
		}
	}

	logConnectionAccess(c, connectionID, "list_namespaces", true, "")

	ctx, cancel := context.WithTimeout(c.Request.Context(), namespacesTimeout)
	defer cancel()
	names, current, err := namespacesViaOrchestrator(ctx, connectorType, config, connectionID)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"connection_id": connectionID, "connector_type": connectorType}).
			Warn("ListConnectionNamespaces: connector could not list namespaces")
		c.JSON(http.StatusFailedDependency, gin.H{"error": "listing namespaces failed", "details": err.Error()})
		return
	}
	names = withCurrent(names, current)
	c.JSON(http.StatusOK, gin.H{
		"connection_id":    connectionID,
		"connector_type":   connectorType,
		"lists_namespaces": true,
		"namespace_kind":   namespacemodel.For(connectorType).TableNamespace,
		"namespaces":       names,
		"current":          current,
	})
}

// withCurrent appends current to names when the listing left it out, so a
// picker always offers the namespace the connection already uses.
func withCurrent(names []string, current string) []string {
	if current == "" {
		return names
	}
	for _, n := range names {
		if n == current {
			return names
		}
	}
	return append(names, current)
}

// PreviewConnectionNamespaces answers POST /api/v1/connections/namespaces: the
// same listing for a connection that is not saved yet, from the config the
// form holds, so the Scope step can show which databases or schemas a pattern
// keeps before the connection exists. Like POST /connections/test it exercises
// credentials the caller typed: members only, never an internal connector.
func PreviewConnectionNamespaces(c *gin.Context) {
	if _, ok := resolveUserID(c); !ok {
		return
	}
	if _, ok := requireWorkspaceRole(c, security.WSMember); !ok {
		return
	}
	var req TestConnectionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request payload", err)
		return
	}
	connectorType := req.ConnectorType
	if isInternalConnectorType(connectorType) && !canAccessInternalConnectorsFromUI(c) {
		c.JSON(http.StatusForbidden, gin.H{"error": "This connector is internal-only and cannot be used from the UI"})
		return
	}
	if nothingToList(connectorType) {
		c.JSON(http.StatusOK, gin.H{
			"connector_type":   connectorType,
			"lists_namespaces": false,
			"namespace_kind":   "",
			"namespaces":       []string{},
			"current":          "",
		})
		return
	}
	config := req.Config
	if req.ConnectorVersion != "" {
		if _, has := config["connector_version"]; !has {
			config["connector_version"] = req.ConnectorVersion
		}
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), namespacesTimeout)
	defer cancel()
	names, current, err := namespacesViaOrchestrator(ctx, connectorType, config, "")
	if err != nil {
		log.WithError(err).WithField("connector_type", connectorType).
			Warn("PreviewConnectionNamespaces: connector could not list namespaces")
		c.JSON(http.StatusFailedDependency, gin.H{"error": "listing namespaces failed", "details": err.Error()})
		return
	}
	names = withCurrent(names, current)
	c.JSON(http.StatusOK, gin.H{
		"connector_type":   connectorType,
		"lists_namespaces": true,
		"namespace_kind":   namespacemodel.For(connectorType).TableNamespace,
		"namespaces":       names,
		"current":          current,
	})
}

// nothingToList reports whether a connector has no namespaces to list. Without
// the connector tree mounted every name reads as unknown, so the connector is
// asked, as it was before the tree told the two apart.
func nothingToList(connectorType string) bool {
	return len(namespacemodel.All()) > 0 && !namespacemodel.ListsNamespaces(connectorType)
}

// namespacesViaOrchestrator calls the orchestrator's /api/v1/agent/list-namespaces.
func namespacesViaOrchestrator(ctx context.Context, connectorType string, config map[string]interface{}, connectionID string) ([]string, string, error) {
	orchestratorURL := strings.TrimRight(os.Getenv("ORCHESTRATOR_URL"), "/")
	if orchestratorURL == "" {
		orchestratorURL = "http://orchestrator:8080"
	}
	reqBody, err := json.Marshal(map[string]interface{}{
		"connector_type": connectorType,
		"config":         config,
		"connection_id":  connectionID,
	})
	if err != nil {
		return nil, "", fmt.Errorf("build request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		orchestratorURL+"/api/v1/agent/list-namespaces", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, "", fmt.Errorf("build http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	setInternalServiceSecret(httpReq)

	resp, err := (&http.Client{Timeout: namespacesTimeout}).Do(httpReq)
	if err != nil {
		return nil, "", fmt.Errorf("orchestrator unreachable: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, "", errors.New(orchestratorErrorDetail(resp.StatusCode, body))
	}
	var envelope struct {
		Namespaces []string `json:"namespaces"`
		Current    string   `json:"current"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, "", fmt.Errorf("decode response: %w", err)
	}
	if envelope.Namespaces == nil {
		envelope.Namespaces = []string{}
	}
	return envelope.Namespaces, envelope.Current, nil
}
