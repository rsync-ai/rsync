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
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"api-gateway/internal/cache"
	"api-gateway/internal/db"
	"api-gateway/internal/security"
	"api-gateway/internal/telemetry"
	"api-gateway/internal/validators"

	"github.com/rsync-ai/backend-orchestrator/pkg/llmscrub"
	"github.com/rsync-ai/shared/crypto"
	"github.com/rsync-ai/shared/pgdriver"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// Global schema cache instance (initialized in main)
var schemaCache *cache.SchemaCache

// tokenRefreshFunc is an optional hook called by enrichConfigWithOAuthToken when a token
// is within 15 minutes of expiry. Set at startup via SetTokenRefreshFunc.
var tokenRefreshFunc func(ctx context.Context, tokenID string) error

// SetTokenRefreshFunc wires in the proactive-refresh callback used by
// enrichConfigWithOAuthToken. Call once at startup with oauthHandler.RefreshTokenByID.
func SetTokenRefreshFunc(f func(ctx context.Context, tokenID string) error) {
	tokenRefreshFunc = f
}

// SetSchemaCache sets the global schema cache instance
func SetSchemaCache(c *cache.SchemaCache) {
	schemaCache = c
}

func canAccessInternalConnectorsFromUI(c *gin.Context) bool {
	// Dev/test override: allow using internal-only connectors from UI and tests.
	// Keep default secure behavior unless explicitly enabled.
	if v := strings.TrimSpace(os.Getenv("RSYNC_ALLOW_INTERNAL_CONNECTORS")); v != "" {
		v = strings.ToLower(v)
		if v == "1" || v == "true" || v == "yes" {
			return true
		}
	}
	role := security.GetUserRole(c)
	return role == security.RoleAdmin || role == security.RolePowerUser
}

// isCleanRelDir reports whether name is a relative path that stays within its
// root (no absolute path, no ".." escape). Defense-in-depth for connector-dir
// names before they are joined onto a root and read.
func isCleanRelDir(name string) bool {
	if name == "" || filepath.IsAbs(name) {
		return false
	}
	clean := filepath.Clean(name)
	return clean != ".." && !strings.HasPrefix(clean, ".."+string(filepath.Separator))
}

func isInternalConnectorType(connectorType string) bool {
	// Prefer public lookup first.
	publicPath := GetMCPPublicConnectorsPath()
	if resolvedName, err := resolveConnectorDirName(publicPath, connectorType); err == nil && isCleanRelDir(resolvedName) {
		metadataPath := filepath.Join(publicPath, resolvedName, "metadata.json")
		if metadataBytes, err := os.ReadFile(metadataPath); err == nil {
			var raw map[string]interface{}
			if json.Unmarshal(metadataBytes, &raw) == nil {
				if isInternal, ok := raw["internal"].(bool); ok {
					return isInternal
				}
			}
		}
	}

	// Also check internal connectors root (needed after physical separation).
	internalPath := GetMCPInternalConnectorsPath()
	if internalPath != "" {
		if resolvedName, err := resolveConnectorDirName(internalPath, connectorType); err == nil && isCleanRelDir(resolvedName) {
			metadataPath := filepath.Join(internalPath, resolvedName, "metadata.json")
			if metadataBytes, err := os.ReadFile(metadataPath); err == nil {
				var raw map[string]interface{}
				if json.Unmarshal(metadataBytes, &raw) == nil {
					if isInternal, ok := raw["internal"].(bool); ok {
						return isInternal
					}
					// If it's in the internal directory, treat as internal even if metadata lacks the flag.
					return true
				}
			}
			return true
		}
	}

	// Backward compatible default: unknown connector types are treated as non-internal.
	return false
}

// enrichConfigWithOAuthToken expands an oauth_token_id into access_token/refresh_token fields
// by looking up the oauth_tokens table. This keeps the UI simple (it only needs oauth_token_id)
// while letting generated connectors continue to rely on standard token fields.
//
// userID scopes the lookup to the calling user's tokens — passing empty string falls back to
// the unscoped query for backward-compatibility (internal / background paths).
func enrichConfigWithOAuthToken(database *sql.DB, config map[string]interface{}, userID string) error {
	if database == nil || config == nil {
		return nil
	}

	rawTokenID, ok := config["oauth_token_id"]
	if !ok || rawTokenID == nil {
		return nil
	}
	tokenID, ok := rawTokenID.(string)
	if !ok || strings.TrimSpace(tokenID) == "" {
		return nil
	}

	var provider string
	var accessToken string
	var refreshToken sql.NullString
	var tokenType sql.NullString
	var expiresAt time.Time
	var scopes sql.NullString
	var accountID sql.NullString
	var accountName sql.NullString

	scanToken := func() error {
		if userID != "" {
			return database.QueryRow(`
				SELECT provider, access_token, refresh_token, token_type, expires_at, scopes, account_id, account_name
				FROM oauth_tokens
				WHERE id = $1 AND user_id = $2
			`, tokenID, userID).Scan(&provider, &accessToken, &refreshToken, &tokenType, &expiresAt, &scopes, &accountID, &accountName)
		}
		return database.QueryRow(`
			SELECT provider, access_token, refresh_token, token_type, expires_at, scopes, account_id, account_name
			FROM oauth_tokens
			WHERE id = $1
		`, tokenID).Scan(&provider, &accessToken, &refreshToken, &tokenType, &expiresAt, &scopes, &accountID, &accountName)
	}

	if err := scanToken(); err != nil {
		return err
	}

	// Proactive refresh: if the token expires within 15 minutes, refresh it now so the
	// injected access_token remains valid for the duration of the upcoming pipeline run.
	// Non-fatal: on any failure we log a warning and continue with the current token
	// (the executor's reactive 401 retry is the safety net).
	if tokenRefreshFunc != nil && time.Until(expiresAt) < 15*time.Minute {
		if err := tokenRefreshFunc(context.Background(), tokenID); err != nil {
			// ErrTokenFresh means RefreshTokenByID decided the token is still good (>5 min
			// remaining). Not a warning — just skip the re-read and use what we have.
			if !errors.Is(err, ErrTokenFresh) {
				log.WithError(err).Warnf("enrichConfigWithOAuthToken: proactive refresh failed for token %s (continuing with current token)", tokenID)
			}
		} else {
			// Refresh succeeded — re-read the freshly updated token from DB before injecting.
			if err := scanToken(); err != nil {
				log.WithError(err).Warnf("enrichConfigWithOAuthToken: re-read after refresh failed for %s (continuing with original)", tokenID)
			}
		}
	}

	// Tokens are stored encrypted in oauth_tokens (crypto.EncryptString at store time).
	// Decrypt before injecting into config so connectors receive the raw bearer token.
	decryptedAccess, err := crypto.DecryptString(accessToken)
	if err != nil {
		return fmt.Errorf("enrichConfigWithOAuthToken: failed to decrypt access_token for token %s: %w", tokenID, err)
	}
	accessToken = decryptedAccess

	if refreshToken.Valid && refreshToken.String != "" {
		decryptedRefresh, err := crypto.DecryptString(refreshToken.String)
		if err != nil {
			// Non-fatal: refresh token may be absent for some providers.
			log.WithError(err).Warnf("enrichConfigWithOAuthToken: failed to decrypt refresh_token for token %s (continuing)", tokenID)
		} else {
			refreshToken.String = decryptedRefresh
		}
	}

	// oauth_token_id is the EXPLICIT source of truth — we only reach this code
	// when it is set, which means the caller chose OAuth. The OAuth token must
	// therefore ALWAYS win over any access_token/api_key/refresh_token the caller
	// also submitted; a stale or masked credential in the request must never
	// shadow it. (Previously these were "only fill if missing", so a non-empty
	// stale access_token in the connection form reached the connector and 401'd —
	// that's why test-before-create failed while edit-by-id, which strips the
	// stale token before enriching, succeeded.) We overwrite unconditionally so
	// both paths behave identically. Metadata fields below stay fill-if-missing.
	if _, exists := config["provider"]; !exists {
		config["provider"] = provider
	}
	config["access_token"] = accessToken
	// Compatibility: many generated API connectors historically used "api_key" even for bearer flows.
	config["api_key"] = accessToken
	if refreshToken.Valid {
		config["refresh_token"] = refreshToken.String
	}
	if tokenType.Valid {
		config["token_type"] = tokenType.String
	}
	if _, exists := config["expires_at"]; !exists {
		config["expires_at"] = expiresAt.UTC().Format(time.RFC3339)
	}
	if scopes.Valid {
		if _, exists := config["scopes"]; !exists {
			config["scopes"] = scopes.String
		}
	}
	if accountID.Valid {
		if _, exists := config["account_id"]; !exists {
			config["account_id"] = accountID.String
		}
	}
	if accountName.Valid {
		if _, exists := config["account_name"]; !exists {
			config["account_name"] = accountName.String
		}
	}

	return nil
}

// latestOAuthTokenIDForProvider returns the id of the most-recent OAuth token a
// user holds for an OAuth provider, or "" if none / on any error. It backs the
// "test-before-create" path: a user who just completed an OAuth connect has a
// token stored under (provider, user) but the draft connection form carries no
// oauth_token_id, so the test must look the token up by provider to inject it.
// Scoped to user_id so one tenant can never read another's token.
func latestOAuthTokenIDForProvider(database *sql.DB, provider, userID string) string {
	if database == nil || strings.TrimSpace(provider) == "" || strings.TrimSpace(userID) == "" {
		return ""
	}
	var id string
	if err := database.QueryRow(`
		SELECT id FROM oauth_tokens
		WHERE provider = $1 AND user_id = $2
		ORDER BY created_at DESC
		LIMIT 1
	`, provider, userID).Scan(&id); err != nil {
		return ""
	}
	return id
}

// applyTokenExpiry populates conn.TokenExpiresAt and conn.IsExpired for OAuth-backed
// connections by reading the linked oauth_tokens row. The connection's own `status`
// column does not reflect token expiry, so an expired OAuth connection would otherwise
// still render as "active". No-op for connections without an oauth_token_id. Far-future
// sentinels (offline / never-expiring tokens such as Shopify) naturally read as not
// expired. (BUG-2/10)
func applyTokenExpiry(database *sql.DB, userID string, conn *Connection) {
	if database == nil || conn == nil || conn.Config == nil {
		return
	}
	rawTokenID, ok := conn.Config["oauth_token_id"]
	if !ok {
		return
	}
	tokenID, ok := rawTokenID.(string)
	if !ok || strings.TrimSpace(tokenID) == "" {
		return
	}

	var expiresAt time.Time
	var err error
	if userID != "" {
		err = database.QueryRow(`SELECT expires_at FROM oauth_tokens WHERE id = $1 AND user_id = $2`, tokenID, userID).Scan(&expiresAt)
	} else {
		err = database.QueryRow(`SELECT expires_at FROM oauth_tokens WHERE id = $1`, tokenID).Scan(&expiresAt)
	}
	if err != nil {
		// Token row missing/unreadable — leave IsExpired=false rather than
		// falsely flagging the connection. Runtime 401 retry is the safety net.
		return
	}
	exp := expiresAt.UTC()
	conn.TokenExpiresAt = &exp
	conn.IsExpired = time.Now().After(exp)
}

// applyTokenExpiryBatch is the O(1)-round-trip form of applyTokenExpiry for the
// list path. The per-row applyTokenExpiry made ListConnections O(N) in DB
// round-trips (one `SELECT expires_at FROM oauth_tokens` per OAuth connection),
// which took 38–41s for 14 connections against the control-plane Postgres at
// ~3s/query and in turn blew the planner service's 3s connections-read budget.
//
// It collects the oauth_token_ids referenced by conns and resolves them all in a
// single `id = ANY($1)` query, then backfills TokenExpiresAt/IsExpired in place.
// Connections without an oauth_token_id are skipped (no query is issued when none
// are OAuth-backed). Failure posture matches applyTokenExpiry: on any error we
// leave IsExpired=false rather than falsely flagging connections — the runtime
// 401 retry is the safety net. (PERF: O(N)→O(1))
func applyTokenExpiryBatch(database *sql.DB, userID string, conns []*Connection) {
	if database == nil || len(conns) == 0 {
		return
	}

	// Map each referenced token id to the connections that use it (an id can be
	// shared, e.g. a source and destination on the same OAuth grant).
	idToConns := make(map[string][]*Connection)
	for _, conn := range conns {
		if conn == nil || conn.Config == nil {
			continue
		}
		rawTokenID, ok := conn.Config["oauth_token_id"]
		if !ok {
			continue
		}
		tokenID, ok := rawTokenID.(string)
		if !ok || strings.TrimSpace(tokenID) == "" {
			continue
		}
		idToConns[tokenID] = append(idToConns[tokenID], conn)
	}
	if len(idToConns) == 0 {
		return // no OAuth-backed connections -> zero queries
	}

	tokenIDs := make([]string, 0, len(idToConns))
	for id := range idToConns {
		tokenIDs = append(tokenIDs, id)
	}

	var rows *sql.Rows
	var err error
	if userID != "" {
		rows, err = database.Query(
			`SELECT id, expires_at FROM oauth_tokens WHERE id = ANY($1) AND user_id = $2`,
			pgdriver.StringArray(tokenIDs), userID)
	} else {
		rows, err = database.Query(
			`SELECT id, expires_at FROM oauth_tokens WHERE id = ANY($1)`,
			pgdriver.StringArray(tokenIDs))
	}
	if err != nil {
		return
	}
	defer rows.Close()

	now := time.Now()
	for rows.Next() {
		var id string
		var expiresAt time.Time
		if err := rows.Scan(&id, &expiresAt); err != nil {
			continue
		}
		exp := expiresAt.UTC()
		expired := now.After(exp)
		for _, conn := range idToConns[id] {
			e := exp // distinct pointer per connection
			conn.TokenExpiresAt = &e
			conn.IsExpired = expired
		}
	}
}

// resolveUserID returns the current user ID from context or headers.
// - Prefer user_id set by auth middleware.
// - Fallback to X-User-ID header for backwards compatibility.
// If no user can be resolved, it writes a 401 response and returns ("", false).
func resolveUserID(c *gin.Context) (string, bool) {
	userID := c.GetString("user_id")
	if userID == "" {
		// Auth middleware should always set user_id. Fail closed here to avoid accidental data exposure.
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return "", false
	}
	return userID, true
}

// Connection represents a data source or destination connection
type Connection struct {
	ID     string `json:"id"`
	UserID string `json:"user_id"`
	Name   string `json:"name"`
	// Slug is the URL-safe form of Name (lowercase, spaces→hyphens) that the
	// slug-based routes accept (PR 106). Derived, not stored — see connectionSlug.
	Slug             string                 `json:"slug"`
	Type             string                 `json:"type"`
	ConnectorType    string                 `json:"connector_type"`
	ConnectorVersion string                 `json:"connector_version"` // "latest" or "v1.0.0", etc.
	SyncMode         string                 `json:"sync_mode,omitempty"`
	CDCMode          string                 `json:"cdc_mode,omitempty"`
	Config           map[string]interface{} `json:"config"`
	Description      string                 `json:"description,omitempty"`
	Status           string                 `json:"status"`
	LastTestedAt     *time.Time             `json:"last_tested_at,omitempty"`
	LastTestStatus   *string                `json:"last_test_status,omitempty"` // Nullable
	LastTestError    *string                `json:"last_test_error,omitempty"`  // Nullable
	CreatedAt        time.Time              `json:"created_at"`
	UpdatedAt        time.Time              `json:"updated_at"`
	// TokenExpiresAt / IsExpired are computed for OAuth-backed connections from the
	// linked oauth_tokens row (the connection's own `status` column does not track
	// token expiry). Omitted for non-OAuth connections. (BUG-2/10)
	TokenExpiresAt *time.Time `json:"token_expires_at,omitempty"`
	IsExpired      bool       `json:"is_expired"`
	// IsConnected is derived from last_test_status == "success". Lets the UI show
	// "Connected" badge without relying on browser sessionStorage workarounds.
	IsConnected bool `json:"is_connected"`
	// SupportsExplorer / ExplorerMode / SQLDialect are computed (not stored) from the
	// connector_type via ResolveExplorerCapability. The Data Explorer frontend filters
	// its connection dropdown on SupportsExplorer and switches UI on ExplorerMode
	// ("sql" | "document"), instead of a hardcoded connector_type allowlist — so a new
	// warehouse becomes explorable with a resolver entry alone, no frontend edit.
	SupportsExplorer bool   `json:"supports_explorer"`
	ExplorerMode     string `json:"explorer_mode,omitempty"`
	SQLDialect       string `json:"sql_dialect,omitempty"`
	// SupportsMaterialization is a strict subset of SupportsExplorer: a connection can
	// be fully queryable and still be unable to back a model (Databricks, BigQuery,
	// ClickHouse). The Explorer reads this to disable the materialization control with
	// a reason rather than letting the user fill in the dialog and collect a 400 —
	// SetSavedQueryMaterialization already refuses these, this just stops the UI
	// offering a door that only ever closes.
	SupportsMaterialization bool `json:"supports_materialization"`
}

// CreateConnectionRequest for creating a new connection
type CreateConnectionRequest struct {
	Name             string                 `json:"name" binding:"required"`
	Type             string                 `json:"connection_type" binding:"required,oneof=source destination"`
	ConnectorType    string                 `json:"connector_type" binding:"required"`
	ConnectorVersion string                 `json:"connector_version"` // Optional: "latest" (default), "v1.0.0", etc.
	SyncMode         string                 `json:"sync_mode"`         // "batch" or "cdc" (for sources)
	CDCMode          string                 `json:"cdc_mode"`          // "initial" or "streaming_only" (when sync_mode is cdc)
	Config           map[string]interface{} `json:"config" binding:"required"`
	Description      string                 `json:"description"`
	// ForceSave skips the pre-save connectivity test. By default
	// CreateConnection refuses to persist a connection whose credentials
	// don't validate against the live data source — pipelines built on
	// an unvalidated connection would otherwise only discover the failure
	// at first run. Set force_save=true to opt into at-least-once
	// connection semantics (e.g. for OAuth flows where the validating
	// token has not yet propagated, or for offline registration).
	ForceSave bool `json:"force_save"`
}

// UpdateConnectionRequest for updating a connection.
// Description is a pointer so we can distinguish "unset" (omit from update)
// from "cleared to empty string" — both are legal edits, dropping the field
// from the struct treated them the same and silently lost user input.
type UpdateConnectionRequest struct {
	Name             string                 `json:"name"`
	Description      *string                `json:"description,omitempty"`
	Config           map[string]interface{} `json:"config"`
	Status           string                 `json:"status"`
	ConnectorVersion string                 `json:"connector_version"` // Optional: upgrade connector version
}

// TestConnectionRequest for testing a connection
type TestConnectionRequest struct {
	ConnectorType    string                 `json:"connector_type" binding:"required"`
	Type             string                 `json:"connection_type"` // "source" or "destination"
	Config           map[string]interface{} `json:"config" binding:"required"`
	ConnectorVersion string                 `json:"connector_version"` // Optional: "latest" (default) or pinned e.g. "v1.0.0"
}

// ListConnections returns all connections for the user
func ListConnections(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	canSeeInternal := canAccessInternalConnectorsFromUI(c)
	traceID := telemetry.TraceIDFromContext(c.Request.Context())

	log.WithFields(log.Fields{
		"trace_id": traceID,
		"user_id":  userID,
	}).Debug("ListConnections")

	// Filters (keep backwards compatible):
	// - type or connection_type: "source" | "destination"
	// - connector_type: connector identifier (case-insensitive)
	filterType := c.Query("type")
	if filterType == "" {
		filterType = c.Query("connection_type")
	}
	filterConnectorType := c.Query("connector_type")

	database := db.GetDB()
	if database == nil {
		// Fallback to mock data if DB not available
		c.JSON(http.StatusOK, gin.H{
			"connections": []Connection{},
			"total":       0,
			"note":        "Database not connected (using mock data)",
		})
		return
	}

	// Workspace scoping: list connections shared within the caller's ACTIVE
	// workspace, not just the ones the caller created. Membership in the active
	// workspace is already proven by WorkspaceContextMiddleware. With no active
	// workspace, return an empty list rather than 400-ing a read.
	wsID := activeWorkspaceID(c)
	if wsID == "" {
		c.JSON(http.StatusOK, gin.H{
			"connections": []Connection{},
			"total":       0,
		})
		return
	}

	// Build query
	query := `
		SELECT id, user_id, name, type, connector_type, connector_version, sync_mode, cdc_mode, description, config, status,
		       last_tested_at, last_test_status, last_test_error, created_at, updated_at
		FROM connections
		WHERE workspace_id = $1
	`
	args := []interface{}{wsID}
	argIdx := 2

	if filterType != "" {
		query += " AND type = $" + strconv.Itoa(argIdx)
		args = append(args, filterType)
		argIdx++
	}

	if filterConnectorType != "" {
		// Case-insensitive match so UI can pass normalized connector names without caring about casing.
		query += " AND LOWER(connector_type) = LOWER($" + strconv.Itoa(argIdx) + ")"
		args = append(args, filterConnectorType)
		argIdx++
	}

	query += " ORDER BY created_at DESC"

	rows, err := database.Query(query, args...)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"trace_id": traceID,
			"user_id":  userID,
		}).Error("ListConnections query failed")
		respondError(c, http.StatusInternalServerError, "list_connections_failed", "Failed to fetch connections", err)
		return
	}
	defer rows.Close()

	connections := []Connection{}
	rowCount := 0
	for rows.Next() {
		rowCount++
		var conn Connection
		var configEncrypted string
		var connectorVersion, syncMode, cdcMode, description sql.NullString
		var lastTestedAt sql.NullTime
		var lastTestStatus sql.NullString
		var lastTestError sql.NullString

		err := rows.Scan(
			&conn.ID, &conn.UserID, &conn.Name, &conn.Type, &conn.ConnectorType, &connectorVersion,
			&syncMode, &cdcMode, &description, &configEncrypted, &conn.Status,
			&lastTestedAt, &lastTestStatus, &lastTestError, &conn.CreatedAt, &conn.UpdatedAt,
		)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"trace_id": traceID,
				"user_id":  userID,
				"row":      rowCount,
			}).Warn("ListConnections scan failed")
			continue
		}

		// Set connector_version (default to "latest" if NULL)
		if connectorVersion.Valid {
			conn.ConnectorVersion = connectorVersion.String
		} else {
			conn.ConnectorVersion = "latest"
		}

		// Decrypt config
		configJSON, err := crypto.DecryptString(configEncrypted)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"trace_id":        traceID,
				"user_id":         userID,
				"connection_id":   conn.ID,
				"connection_name": conn.Name,
			}).Warn("ListConnections decrypt config failed")
			continue
		}

		// Parse JSON
		if err := json.Unmarshal([]byte(configJSON), &conn.Config); err != nil {
			log.WithError(err).WithFields(log.Fields{
				"trace_id":        traceID,
				"user_id":         userID,
				"connection_id":   conn.ID,
				"connection_name": conn.Name,
			}).Warn("ListConnections unmarshal config failed")
			continue
		}

		// Mask sensitive fields in response
		conn.Config = maskSensitiveFields(conn.Config)

		// Set nullable fields if valid
		if syncMode.Valid {
			conn.SyncMode = syncMode.String
		}
		if cdcMode.Valid {
			conn.CDCMode = cdcMode.String
		}
		if description.Valid {
			conn.Description = description.String
		}
		if lastTestedAt.Valid {
			conn.LastTestedAt = &lastTestedAt.Time
		}
		if lastTestStatus.Valid {
			conn.LastTestStatus = &lastTestStatus.String
			conn.IsConnected = lastTestStatus.String == "success"
		}
		if lastTestError.Valid {
			conn.LastTestError = &lastTestError.String
		}

		// Security: hide internal connectors from normal UI users.
		if !canSeeInternal && isInternalConnectorType(conn.ConnectorType) {
			continue
		}

		conn.Slug = connectionSlug(conn.Name)
		conn.applyExplorerCapability()
		connections = append(connections, conn)
	}

	// Compute OAuth token expiry so the UI can flag expired connections. (BUG-2/10)
	// Batched into a single oauth_tokens query for the whole page — calling the
	// per-row applyTokenExpiry inside the loop above made this handler O(N) in DB
	// round-trips (38–41s for 14 connections at ~3s/query). (PERF: O(N)→O(1))
	ptrs := make([]*Connection, len(connections))
	for i := range connections {
		ptrs[i] = &connections[i]
	}
	applyTokenExpiryBatch(database, userID, ptrs)

	c.JSON(http.StatusOK, gin.H{
		"connections": connections,
		"total":       len(connections),
	})
}

// GetConnection returns a specific connection by ID
func GetConnection(c *gin.Context) {
	rawID := strings.TrimSpace(c.Param("id"))
	if rawID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "invalid_connection_id",
			"message": "Invalid connection ID format",
		})
		return
	}
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	}

	// Connections are workspace-shared: scope reads to the caller's active
	// workspace (membership is already verified by WorkspaceContextMiddleware),
	// not to created_by/user_id. Resolve by UUID when the param is a UUID;
	// otherwise a name-slug lookup (case-insensitive, hyphen-for-space tolerant).
	// A non-UUID like "test-shopify" no longer 400s before the DB (BUG-1).
	wsID := activeWorkspaceID(c)
	if wsID == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}
	var whereClause string
	if _, err := uuid.Parse(rawID); err == nil {
		whereClause = "id = $1 AND workspace_id = $2"
	} else {
		whereClause = "workspace_id = $2 AND (LOWER(name) = LOWER($1) OR LOWER(REPLACE(name, ' ', '-')) = LOWER($1))"
	}
	connectionID := rawID

	var conn Connection
	var configEncrypted string
	var connectorVersion, syncMode, cdcMode, description sql.NullString
	var lastTestedAt sql.NullTime
	var lastTestStatus sql.NullString
	var lastTestError sql.NullString

	err := database.QueryRow(`
		SELECT id, user_id, name, type, connector_type, connector_version, sync_mode, cdc_mode, description, config, status,
		       last_tested_at, last_test_status, last_test_error, created_at, updated_at
		FROM connections
		WHERE `+whereClause+`
		ORDER BY created_at DESC
		LIMIT 1
	`, connectionID, wsID).Scan(
		&conn.ID, &conn.UserID, &conn.Name, &conn.Type, &conn.ConnectorType, &connectorVersion,
		&syncMode, &cdcMode, &description, &configEncrypted, &conn.Status,
		&lastTestedAt, &lastTestStatus, &lastTestError, &conn.CreatedAt, &conn.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch connection"})
		return
	}

	// Use the resolved UUID for downstream audit logging (rawID may have been a slug).
	connectionID = conn.ID

	// Set connector_version (default to "latest" if NULL)
	if connectorVersion.Valid {
		conn.ConnectorVersion = connectorVersion.String
	} else {
		conn.ConnectorVersion = "latest"
	}

	// Decrypt config
	configJSON, err := crypto.DecryptString(configEncrypted)
	if err != nil {
		logConnectionAccess(c, connectionID, "read_config", false, "decrypt_failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decrypt config"})
		return
	}

	// Log successful credential access
	logConnectionAccess(c, connectionID, "read_config", true, "")

	// Parse JSON
	if err := json.Unmarshal([]byte(configJSON), &conn.Config); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse config"})
		return
	}

	// Mask sensitive fields
	conn.Config = maskSensitiveFields(conn.Config)

	// Security: hide internal connectors from normal UI users.
	if isInternalConnectorType(conn.ConnectorType) && !canAccessInternalConnectorsFromUI(c) {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}

	// Set nullable fields
	if syncMode.Valid {
		conn.SyncMode = syncMode.String
	}
	if cdcMode.Valid {
		conn.CDCMode = cdcMode.String
	}
	if description.Valid {
		conn.Description = description.String
	}
	if lastTestedAt.Valid {
		conn.LastTestedAt = &lastTestedAt.Time
	}
	if lastTestStatus.Valid {
		conn.LastTestStatus = &lastTestStatus.String
		conn.IsConnected = lastTestStatus.String == "success"
	}
	if lastTestError.Valid {
		conn.LastTestError = &lastTestError.String
	}

	// Compute OAuth token expiry so the UI can flag expired connections. (BUG-2/10)
	applyTokenExpiry(database, userID, &conn)

	conn.Slug = connectionSlug(conn.Name)
	conn.applyExplorerCapability()
	c.JSON(http.StatusOK, conn)
}

// CreateConnection creates a new connection
func CreateConnection(c *gin.Context) {
	traceID := getTraceID(c)

	var req CreateConnectionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.WithError(err).WithField("trace_id", traceID).Warn("CreateConnection bind JSON failed")
		// Translate Go-validator binding errors into user-friendly
		// field-level messages so the UI doesn't show
		// "Key: 'CreateConnectionRequest.Type' Error:Field validation
		// for 'Type' failed on the 'required' tag".
		userMsg, fieldErrs := translateBindError(err)
		resp := gin.H{
			"error":    "invalid_request",
			"message":  userMsg,
			"trace_id": traceID,
		}
		if len(fieldErrs) > 0 {
			resp["validation_errors"] = fieldErrs
		}
		c.JSON(http.StatusBadRequest, resp)
		return
	}

	// Security: internal connectors are orchestrator-managed plumbing and should not be used from UI.
	if isInternalConnectorType(req.ConnectorType) && !canAccessInternalConnectorsFromUI(c) {
		c.JSON(http.StatusForbidden, gin.H{
			"error":    "insufficient_permissions",
			"message":  "This connector is internal-only and cannot be used from the UI",
			"trace_id": traceID,
		})
		return
	}

	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	// RBAC: creating a connection is a MUTATION; viewers are read-only. Require
	// at least `member` in the active workspace (mirrors UpdateConnection /
	// DeleteConnection and the roles.ts capability matrix). Fail-fast before any
	// encryption or DB side effects.
	if _, ok := requireWorkspaceRole(c, security.WSMember); !ok {
		return
	}

	log.WithFields(log.Fields{
		"trace_id":          traceID,
		"user_id":           userID,
		"name":              req.Name,
		"type":              req.Type,
		"connector_type":    req.ConnectorType,
		"connector_version": req.ConnectorVersion,
		"sync_mode":         req.SyncMode,
		"cdc_mode":          req.CDCMode,
	}).Info("CreateConnection")

	database := db.GetDB()
	if database == nil {
		log.WithField("trace_id", traceID).Warn("CreateConnection database unavailable; returning mock response")
		// Mock response if DB not available
		c.JSON(http.StatusCreated, gin.H{
			"id":             uuid.New().String(),
			"name":           req.Name,
			"type":           req.Type,
			"connector_type": req.ConnectorType,
			"status":         "active",
			"created_at":     time.Now(),
			"note":           "Database not connected (mock response)",
		})
		return
	}

	// Workspace scoping: the new connection belongs to the caller's ACTIVE
	// workspace (migration 069 made connections.workspace_id NOT NULL). Resolve
	// it before any side effects so we fail fast rather than after encryption.
	workspaceID, ok := resolveActiveWorkspace(c)
	if !ok {
		return
	}

	// If the UI provides an oauth_token_id (from /api/v1/oauth/* callback),
	// expand it to standard token fields before encrypting/storing.
	if err := enrichConfigWithOAuthToken(database, req.Config, userID); err != nil {
		log.WithError(err).WithField("trace_id", traceID).Warn("CreateConnection invalid oauth_token_id")
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid oauth_token_id",
			"details": err.Error(),
		})
		return
	}

	// Convert config to JSON
	configJSON, err := json.Marshal(req.Config)
	if err != nil {
		log.WithError(err).WithField("trace_id", traceID).Warn("CreateConnection marshal config failed")
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid config format"})
		return
	}

	// Encrypt config before storing
	configEncrypted, err := crypto.EncryptString(string(configJSON))
	if err != nil {
		log.WithError(err).WithField("trace_id", traceID).Error("CreateConnection encrypt config failed")
		respondError(c, http.StatusInternalServerError, "encrypt_credentials_failed", "Failed to encrypt credentials", err)
		return
	}

	connectionID := uuid.New().String()
	now := time.Now()

	// Set defaults for sync_mode and cdc_mode
	syncMode := req.SyncMode
	if syncMode == "" {
		syncMode = "batch"
	}

	// Only persist cdc_mode when sync_mode is cdc; otherwise store NULL to satisfy DB check constraint
	var cdcMode interface{}
	if syncMode == "cdc" {
		mode := req.CDCMode
		if mode == "" {
			mode = "initial" // Default to full snapshot for CDC
		}
		cdcMode = mode
	} else {
		cdcMode = nil
	}

	// Set default for connector_version
	connectorVersion := req.ConnectorVersion
	if connectorVersion == "" {
		connectorVersion = "latest"
	}

	// Pre-save connectivity test (fail-closed by default). The previous
	// behaviour persisted any submitted config and surfaced credential
	// problems only at first pipeline run; users routinely shipped
	// pipelines on broken creds and only discovered the failure after the
	// pipeline crashed. ForceSave is the explicit opt-out for OAuth-style
	// flows where the validating token has not yet propagated, or for
	// offline registration.
	//
	// testPassed records whether the pre-save test actually succeeded against
	// the live vendor API. We persist it on the new row (last_test_status=
	// 'success') so computeLifecycleUncached's fallback lifts this
	// connector_type out of "draft" — unblocking the user's first pipeline run
	// without ?allow_draft=true (KI-CONN-TEST-DRAFT). Stays false for
	// force_save, internal connectors, and the OAuth soft-pass path, none of
	// which prove a clean test.
	testPassed := false
	if req.ForceSave {
		// Audit trail: force_save bypasses the credential validation that
		// every other connection creation goes through. Log at INFO so
		// operators reviewing audit logs can identify connections that
		// were never validated against the live data source.
		log.WithFields(log.Fields{
			"trace_id":       traceID,
			"user_id":        userID,
			"connector_type": req.ConnectorType,
			"name":           req.Name,
		}).Info("CreateConnection: force_save=true, skipping pre-save connectivity test")
	} else if !isInternalConnectorType(req.ConnectorType) {
		// 90s, not 30s: a cold connector must be JIT-deployed (image pull/container
		// start) before the test can run, and the orchestrator's container-wait
		// budget is 60s (server_manager.go DeployWaitTimeout). The caller MUST
		// outlast that so the user sees the orchestrator's real verdict instead of
		// "context deadline exceeded" mid-cold-start.
		ctx, cancel := context.WithTimeout(c.Request.Context(), 90*time.Second)
		ok, testErr := performConnectionTest(ctx, req.ConnectorType, req.Config)
		ctxErr := ctx.Err()
		cancel()
		if !ok {
			// Distinguish "the credentials don't work" from "we couldn't
			// finish testing because the caller / network gave up". The
			// 422 contract is for credential-side failures; a request-ctx
			// cancellation should bubble up as a 5xx so the caller knows
			// to retry without thinking their creds are bad.
			if ctxErr != nil {
				log.WithError(ctxErr).WithFields(log.Fields{
					"trace_id":       traceID,
					"user_id":        userID,
					"connector_type": req.ConnectorType,
				}).Warn("CreateConnection: pre-save test interrupted by ctx cancellation")
				c.JSON(http.StatusServiceUnavailable, gin.H{
					"error":    "connection_test_interrupted",
					"message":  "Connectivity test could not complete (request cancelled or timed out). Retry, or set force_save=true to bypass.",
					"trace_id": traceID,
				})
				return
			}
			// OAuth activation grace. When the connection was just authorized via
			// OAuth (config carries an oauth_token_id) and the pre-save test fails
			// with an AUTH error, this is almost always provider-side token
			// activation lag — e.g. Shopify dev stores reject a freshly-minted
			// offline token for ~1-2 min — NOT bad credentials. The user just
			// completed a successful OAuth grant, so persist the connection anyway
			// (soft pass) instead of forcing them to wait and retry; the pipeline
			// run re-validates once the token propagates. Non-auth failures (bad
			// host, SSRF block, schema error) still hard-fail with 422.
			oauthBacked := false
			if v, exists := req.Config["oauth_token_id"]; exists {
				if s, ok2 := v.(string); ok2 && strings.TrimSpace(s) != "" {
					oauthBacked = true
				}
			}
			if oauthBacked && isOAuthAuthError(fmt.Sprintf("%v", testErr)) {
				log.WithFields(log.Fields{
					"trace_id":       traceID,
					"user_id":        userID,
					"connector_type": req.ConnectorType,
					"error":          testErr,
				}).Warn("CreateConnection: OAuth pre-save test returned an auth error — saving anyway (likely provider token-activation lag)")
				// fall through to the INSERT below
			} else {
				log.WithFields(log.Fields{
					"trace_id":       traceID,
					"user_id":        userID,
					"connector_type": req.ConnectorType,
					"error":          llmscrub.Scrub(testErr),
				}).Warn("CreateConnection rejected: pre-save connectivity test failed")
				c.JSON(http.StatusUnprocessableEntity, gin.H{
					"error":      "connection_test_failed",
					"message":    "Connectivity test failed before saving. Fix the credentials and try again, or set force_save=true to persist anyway.",
					"test_error": llmscrub.Scrub(testErr),
					"trace_id":   traceID,
				})
				return
			}
		}
		// Reaching here means the pre-save test passed (ok=true) or it was an
		// OAuth soft-pass (ok=false, fell through above). Only a clean pass
		// persists success and lifts the draft gate (KI-CONN-TEST-DRAFT).
		testPassed = ok
	}

	// Persist the pre-save test verdict on the new row. A non-NULL 'success'
	// here is what computeLifecycleUncached's fallback counts to lift the
	// connector_type from "draft" to "preview" (KI-CONN-TEST-DRAFT). Left NULL
	// (force_save / internal / OAuth soft-pass) the connector stays draft, as
	// before.
	var lastTestStatus, lastTestedAt interface{}
	if testPassed {
		lastTestStatus = "success"
		lastTestedAt = now
	}

	// Insert into database
	_, err = database.Exec(`
		INSERT INTO connections (id, user_id, name, type, connector_type, connector_version, sync_mode, cdc_mode, description, config, status, last_tested_at, last_test_status, created_at, updated_at, workspace_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
	`, connectionID, userID, req.Name, req.Type, req.ConnectorType, connectorVersion, syncMode, cdcMode, req.Description, configEncrypted, "active", lastTestedAt, lastTestStatus, now, now, workspaceID)

	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"trace_id": traceID,
			"user_id":  userID,
			"name":     req.Name,
		}).Error("CreateConnection database insert failed")
		SendDBError(c, err, "connection", req.Name)
		return
	}

	// A persisted successful test means this connector_type is no longer
	// "draft". Drop any cached (stale) draft lifecycle so the user's very next
	// pipeline create/run recomputes to "preview" instead of 422
	// draft_connector_blocked (KI-CONN-TEST-DRAFT). Mirrors TestConnection.
	if testPassed {
		invalidateLifecycleCache(req.ConnectorType)
	}

	log.WithFields(log.Fields{
		"trace_id":       traceID,
		"user_id":        userID,
		"connection_id":  connectionID,
		"connector_type": req.ConnectorType,
		"sync_mode":      syncMode,
		"cdc_mode":       cdcMode,
	}).Info("CreateConnection success")

	// Audit
	logAudit(c, "create_connection", "connection", connectionID, map[string]interface{}{
		"name":           req.Name,
		"type":           req.Type,
		"connector_type": req.ConnectorType,
		"sync_mode":      syncMode,
		"cdc_mode":       cdcMode,
	})

	// Return the full Connection struct so the 201 response matches the shape
	// of GetConnection/ListConnections — including the computed is_expired /
	// token_expires_at fields (BUG-3). Previously this echoed a curated gin.H
	// that bypassed applyTokenExpiry(), so freshly-created OAuth connections
	// never reported expiry until a later list/get. Secrets in config are
	// masked via maskSensitiveFields (covers password/access_token/
	// refresh_token/*_secret/*_key/*_password); oauth_token_id is preserved so
	// applyTokenExpiry can resolve the linked token row.
	conn := Connection{
		ID:               connectionID,
		UserID:           userID,
		Name:             req.Name,
		Type:             req.Type,
		ConnectorType:    req.ConnectorType,
		ConnectorVersion: connectorVersion,
		SyncMode:         syncMode,
		Config:           maskSensitiveFields(req.Config),
		Description:      req.Description,
		Status:           "active",
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	// Mirror the persisted test verdict so the 201 body matches the new DB row
	// (is_connected badge + last_test_status) without a follow-up GET.
	if testPassed {
		s := "success"
		conn.LastTestStatus = &s
		conn.LastTestedAt = &now
		conn.IsConnected = true
	}
	if m, ok := cdcMode.(string); ok {
		conn.CDCMode = m
	}
	conn.Slug = connectionSlug(conn.Name)
	applyTokenExpiry(database, userID, &conn)
	c.JSON(http.StatusCreated, conn)
}

// UpdateConnection updates an existing connection
func UpdateConnection(c *gin.Context) {
	rawID := strings.TrimSpace(c.Param("id"))
	if rawID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "invalid_connection_id",
			"message": "Invalid connection ID format",
		})
		return
	}
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	var req UpdateConnectionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request payload", err)
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	}

	// Accept either a UUID or a name slug (scoped to the active workspace),
	// mirroring GetConnection so a connection addressable by slug on read is also
	// updatable by slug. Resolution failure surfaces as a 404.
	wsID := activeWorkspaceID(c)
	connectionID, err := resolveConnectionID(rawID, wsID, database)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}

	// Authorize against the active workspace: the connection must live there and
	// the caller must hold >= member. Connections are workspace-shared, so any
	// member (not just the creator) may edit a teammate's connection; every write
	// below scopes by workspace_id, never created_by/user_id.
	if _, ok := requireResourceRole(c, "connections", connectionID, security.WSMember); !ok {
		return
	}

	// Security: prevent normal UI users from modifying internal connector connections.
	var existingConnectorType string
	if err := database.QueryRow("SELECT connector_type FROM connections WHERE id = $1 AND workspace_id = $2", connectionID, wsID).Scan(&existingConnectorType); err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load connection"})
		return
	}
	if isInternalConnectorType(existingConnectorType) && !canAccessInternalConnectorsFromUI(c) {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}

	// Build dynamic update query
	updates := []string{}
	args := []interface{}{}
	argIdx := 1

	if req.Name != "" {
		updates = append(updates, "name = $"+string(rune('0'+argIdx)))
		args = append(args, req.Name)
		argIdx++
	}

	// Description: pointer-typed so we can persist explicit clears.
	// Field omitted in JSON → leave existing; field present → write the value
	// (including empty string).
	if req.Description != nil {
		updates = append(updates, "description = $"+string(rune('0'+argIdx)))
		args = append(args, *req.Description)
		argIdx++
	}

	if req.Config != nil {
		// Fetch existing config to merge with new config (preserve empty password fields)
		var existingConfigEncrypted string
		err := database.QueryRow("SELECT config FROM connections WHERE id = $1 AND workspace_id = $2", connectionID, wsID).Scan(&existingConfigEncrypted)

		mergedConfig := req.Config
		if err == nil {
			// Decrypt existing config
			existingConfigJSON, err := crypto.DecryptString(existingConfigEncrypted)
			if err == nil {
				// Log credential access for config update/merge
				logConnectionAccess(c, connectionID, "update_config", true, "")

				var existingConfig map[string]interface{}
				json.Unmarshal([]byte(existingConfigJSON), &existingConfig)

				// Merge: keep existing values for empty password/secret fields
				// IMPORTANT:
				// The API response masks secrets as "••••••••" (see maskSensitiveFields()).
				// When users edit a connection, the UI often sends these masked values back.
				// We must treat masked placeholders as "unchanged" and preserve the existing secret.
				isMaskedOrEmpty := func(v interface{}) bool {
					s, ok := v.(string)
					if !ok {
						return false
					}
					s = strings.TrimSpace(s)
					return s == "" || s == "********" || s == "••••••••"
				}

				sensitiveFields := []string{
					"password",
					"secret_key",
					"api_key",
					"token",
					"security_token",
					"credentials_json",
					// OAuth
					"client_secret",
					"access_token",
					"refresh_token",
					// Cloud creds (some UIs mask these too)
					"secret_access_key",
					"access_key_id",
				}
				for _, field := range sensitiveFields {
					if newVal, exists := req.Config[field]; !exists || isMaskedOrEmpty(newVal) {
						if existingVal, hasExisting := existingConfig[field]; hasExisting {
							mergedConfig[field] = existingVal
						}
					}
				}
			} else {
				logConnectionAccess(c, connectionID, "update_config", false, "decrypt_failed")
			}
		}

		// When the caller provides an explicit oauth_token_id (not masked/empty),
		// clear stale token fields before enrichment so the new oauth_token_id
		// always wins — the merge above would otherwise preserve an old expired
		// access_token, silently breaking OAuth-based connectors.
		if tokenIDVal, exists := req.Config["oauth_token_id"]; exists {
			if s, ok := tokenIDVal.(string); ok {
				s = strings.TrimSpace(s)
				if s != "" && s != "••••••••" && s != "********" {
					delete(mergedConfig, "access_token")
					delete(mergedConfig, "refresh_token")
				}
			}
		}
		// Expand oauth_token_id → access_token/refresh_token, matching CreateConnection.
		if err := enrichConfigWithOAuthToken(database, mergedConfig, userID); err != nil {
			log.WithError(err).Warnf("UpdateConnection: enrichConfigWithOAuthToken failed for %s (continuing)", connectionID)
		}

		configJSON, _ := json.Marshal(mergedConfig)
		configEncrypted, err := crypto.EncryptString(string(configJSON))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to encrypt config"})
			return
		}
		updates = append(updates, "config = $"+string(rune('0'+argIdx)))
		args = append(args, configEncrypted)
		argIdx++
	}

	if req.Status != "" {
		updates = append(updates, "status = $"+string(rune('0'+argIdx)))
		args = append(args, req.Status)
		argIdx++
	}

	// Handle connector version upgrade
	var oldVersion string
	if req.ConnectorVersion != "" {
		// Fetch current version for audit
		err := database.QueryRow("SELECT connector_version FROM connections WHERE id = $1 AND workspace_id = $2", connectionID, wsID).Scan(&oldVersion)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
			return
		}

		// Validate version format
		if err := validators.ValidateVersionFormat(req.ConnectorVersion); err != nil {
			respondError(c, http.StatusBadRequest, "invalid_version_format", "Invalid connector version format", err)
			return
		}

		// Fetch connector type for validation
		var connectorType string
		err = database.QueryRow("SELECT connector_type FROM connections WHERE id = $1 AND workspace_id = $2", connectionID, wsID).Scan(&connectorType)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch connection details"})
			return
		}

		// Validate version exists
		validator := validators.NewConnectorVersionValidator(GetMCPConnectorsPath())
		if err := validator.ValidateVersionExists(connectorType, req.ConnectorVersion); err != nil {
			availableVersions, _ := validator.GetAvailableVersions(connectorType)
			c.JSON(http.StatusNotFound, gin.H{
				"error":              err.Error(),
				"available_versions": availableVersions,
			})
			return
		}

		updates = append(updates, "connector_version = $"+string(rune('0'+argIdx)))
		args = append(args, req.ConnectorVersion)
		argIdx++
	}

	updates = append(updates, "updated_at = $"+string(rune('0'+argIdx)))
	args = append(args, time.Now())
	argIdx++

	args = append(args, connectionID, wsID)

	if len(updates) == 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No fields to update"})
		return
	}

	query := "UPDATE connections SET "
	for i, update := range updates {
		if i > 0 {
			query += ", "
		}
		query += update
	}
	query += " WHERE id = $" + string(rune('0'+argIdx)) + " AND workspace_id = $" + string(rune('0'+argIdx+1))

	result, err := database.Exec(query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update connection"})
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}

	// Log audit with old/new version if connector_version was changed
	auditData := map[string]interface{}{
		"name":   req.Name,
		"status": req.Status,
	}
	if req.ConnectorVersion != "" {
		auditData["old_connector_version"] = oldVersion
		auditData["new_connector_version"] = req.ConnectorVersion

		logAudit(c, "upgrade_connector_version", "connection", connectionID, auditData)

		// Check for breaking changes and add warning
		var connectorType string
		database.QueryRow("SELECT connector_type FROM connections WHERE id = $1", connectionID).Scan(&connectorType)

		validator := validators.NewConnectorVersionValidator(GetMCPConnectorsPath())
		hasBreaking, breakingChanges, _ := validator.CheckBreakingChanges(connectorType, oldVersion, req.ConnectorVersion)

		response := gin.H{
			"id":                connectionID,
			"connector_version": req.ConnectorVersion,
			"message":           "Connection updated successfully",
		}

		if hasBreaking && len(breakingChanges) > 0 {
			response["warning"] = fmt.Sprintf("Version %s contains breaking changes. Running pipelines will continue using %s.", req.ConnectorVersion, oldVersion)
			response["breaking_changes"] = breakingChanges
		}

		c.JSON(http.StatusOK, response)
		return
	}

	logAudit(c, "update_connection", "connection", connectionID, auditData)

	c.JSON(http.StatusOK, gin.H{
		"id":      connectionID,
		"message": "Connection updated successfully",
	})
}

// DeleteConnection deletes a connection
func DeleteConnection(c *gin.Context) {
	rawID := strings.TrimSpace(c.Param("id"))
	if rawID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "invalid_connection_id",
			"message": "Invalid connection ID format",
		})
		return
	}
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	}

	// Accept either a UUID or a name slug (scoped to the active workspace),
	// mirroring GetConnection so a connection addressable by slug on read is also
	// deletable by slug. Resolution failure surfaces as a 404.
	wsID := activeWorkspaceID(c)
	connectionID, err := resolveConnectionID(rawID, wsID, database)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}

	// Authorize against the active workspace: the connection must live there and
	// the caller must hold >= member. Connections are workspace-shared, so any
	// member (not just the creator) may delete a teammate's connection; the
	// DELETE below scopes by workspace_id. requireResourceRole also enforces
	// authentication (401) and writes its own 403/404/503 on failure.
	if _, ok := requireResourceRole(c, "connections", connectionID, security.WSMember); !ok {
		return
	}

	// Check if connection is used by any pipelines
	var pipelineCount int
	err = database.QueryRow(`
		SELECT COUNT(*) FROM pipelines 
		WHERE (source_connection_id = $1 OR destination_connection_id = $1)
		AND status != 'archived'
	`, connectionID).Scan(&pipelineCount)

	if err != nil && err != sql.ErrNoRows {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check connection usage"})
		return
	}

	if pipelineCount > 0 {
		// Include a small, actionable list so the UI can tell the user exactly what to fix.
		type blockingPipeline struct {
			ID     string `json:"id"`
			Name   string `json:"name"`
			Status string `json:"status"`
			Role   string `json:"role"` // "source" | "destination"
		}
		var pipelines []blockingPipeline
		rows, qErr := database.Query(`
			SELECT
				id,
				name,
				status,
				CASE
					WHEN source_connection_id = $1 THEN 'source'
					WHEN destination_connection_id = $1 THEN 'destination'
					ELSE 'unknown'
				END AS role
			FROM pipelines
			WHERE (source_connection_id = $1 OR destination_connection_id = $1)
				AND status != 'archived'
			ORDER BY updated_at DESC
			LIMIT 25
		`, connectionID)
		if qErr == nil {
			defer rows.Close()
			for rows.Next() {
				var p blockingPipeline
				if scanErr := rows.Scan(&p.ID, &p.Name, &p.Status, &p.Role); scanErr == nil {
					pipelines = append(pipelines, p)
				}
			}
		}

		c.JSON(http.StatusConflict, gin.H{
			"error":          "Connection is used by active pipelines",
			"pipeline_count": pipelineCount,
			"pipelines":      pipelines,
			"hint":           "Detach this connection from these pipelines (or delete/archive them) and try again.",
		})
		return
	}

	// Delete connection
	result, err := database.Exec(`
		DELETE FROM connections
		WHERE id = $1 AND workspace_id = $2
	`, connectionID, wsID)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete connection"})
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}

	logAudit(c, "delete_connection", "connection", connectionID, nil)

	c.JSON(http.StatusOK, gin.H{
		"message": "Connection deleted successfully",
	})
}

// SampleResult represents the result of sampling data
type SampleResult struct {
	Rows     []map[string]interface{} `json:"rows"`
	Columns  []string                 `json:"columns"`
	RowCount int                      `json:"row_count"`
	Warnings []string                 `json:"warnings,omitempty"`
}

// sampleViaOrchestrator proxies a preview request to the orchestrator's
// /api/v1/agent/sample-rows endpoint. The orchestrator invokes the
// source connector's MCP `export` operation with `limit=N` — the same
// path the executor uses for real transfers, so the preview reflects
// exactly what the pipeline would move.
//
// Used for non-DB connectors (shopify, stripe, hubspot, ...) where
// direct database/sql.Open isn't applicable. Returns the same
// SampleResult shape as the DB path so the caller stays generic.
func sampleViaOrchestrator(ctx context.Context, connectorType string, config map[string]interface{}, table string, limit int) (*SampleResult, error) {
	orchestratorURL := strings.TrimRight(os.Getenv("ORCHESTRATOR_URL"), "/")
	if orchestratorURL == "" {
		orchestratorURL = "http://orchestrator:8080"
	}

	reqBody, err := json.Marshal(map[string]interface{}{
		"connector_type": connectorType,
		"config":         config,
		"table":          table,
		"limit":          limit,
	})
	if err != nil {
		return nil, fmt.Errorf("sample_rows: build request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		orchestratorURL+"/api/v1/agent/sample-rows",
		bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("sample_rows: build http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	setInternalServiceSecret(httpReq)

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("sample_rows: orchestrator unreachable: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		// Surface the orchestrator's error details verbatim so the UI
		// shows something specific instead of a generic HTTP 500.
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
		return nil, fmt.Errorf("sample_rows: %s", msg)
	}

	var envelope struct {
		Rows     []map[string]interface{} `json:"rows"`
		Columns  []string                 `json:"columns"`
		RowCount int                      `json:"row_count"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("sample_rows: decode response: %w", err)
	}

	// Backfill columns from the first row if the connector didn't
	// emit them explicitly (some legacy connectors return only data).
	if len(envelope.Columns) == 0 && len(envelope.Rows) > 0 {
		for k := range envelope.Rows[0] {
			envelope.Columns = append(envelope.Columns, k)
		}
	}

	return &SampleResult{
		Rows:     envelope.Rows,
		Columns:  envelope.Columns,
		RowCount: envelope.RowCount,
	}, nil
}

// sampleDataFromConnection queries a database connection and returns sample rows.
//
// Two paths:
//   - Relational DBs (mysql, postgres, ...): direct database/sql.Open and
//     `SELECT * FROM table LIMIT N`. Stayed here for low-latency preview
//     of the most common case.
//   - Everything else (REST / GraphQL / SaaS): proxy to the orchestrator's
//     /api/v1/agent/sample-rows endpoint, which invokes the source
//     connector's `export` MCP operation with limit=N. Without this
//     branch, every non-DB connector's preview button returned a
//     generic HTTP 500 with "preview not supported for connector type:
//     <X>" — surfaced to the user as a bare "HTTP 500" badge next to
//     the table name with no actionable hint.
func sampleDataFromConnection(ctx context.Context, connectorType string, config map[string]interface{}, table string, limit int) (*SampleResult, error) {
	// Non-DB connectors: delegate to the orchestrator, which knows
	// about MCP versions and routes the export call to the right
	// container.
	if !isDBConnector(connectorType) {
		return sampleViaOrchestrator(ctx, connectorType, config, table, limit)
	}

	// Build connection string
	var dsn string
	var driverName string

	switch strings.ToLower(connectorType) {
	case "mysql":
		driverName = "mysql"
		host, _ := config["host"].(string)
		port, _ := config["port"]
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

		tlsMode := resolveMySQLTLSMode(config, host)
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&timeout=5s&tls=%s",
			user, password, host, portInt, database, tlsMode)

	case "postgresql", "postgres":
		driverName = "postgres"
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

		dsn = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=5",
			host, portInt, user, password, database, sslMode)

	default:
		return nil, fmt.Errorf("unsupported connector type for sampling: %s", connectorType)
	}

	// Open connection
	conn, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	// Set connection pool settings for short-lived sampling
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(0)
	conn.SetConnMaxLifetime(10 * time.Second)

	// Test connection with context
	if err := conn.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("connection ping failed: %w", err)
	}

	query, args, err := buildSampleQuery(driverName, table, limit)
	if err != nil {
		return nil, err
	}

	// Execute query with timeout
	rows, err := conn.QueryContext(ctx, query, args...)
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
	warnings := []string{}

	for rows.Next() {
		// Create a slice of interface{} to hold each column's value
		columnValues := make([]interface{}, len(columns))
		columnPointers := make([]interface{}, len(columns))
		for i := range columnValues {
			columnPointers[i] = &columnValues[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to scan row: %v", err))
			continue
		}

		// Build row map
		rowMap := make(map[string]interface{})
		for i, colName := range columns {
			val := columnValues[i]

			// Convert []byte to string (common for TEXT/VARCHAR columns)
			if b, ok := val.([]byte); ok {
				val = string(b)
			}

			// Redact PII + secrets in preview (but avoid masking generic ids)
			if shouldRedactColumnName(colName) {
				val = redactForPreview(val)
			}

			rowMap[colName] = val
		}

		result = append(result, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return &SampleResult{
		Rows:     result,
		Columns:  columns,
		RowCount: len(result),
		Warnings: warnings,
	}, nil
}

// isDBConnector checks if a connector type is a database
func isDBConnector(connectorType string) bool {
	// NOTE: only mysql + postgres have a direct Go-driver sample path in the
	// switch below (sampleDataFromConnection). oracle is intentionally NOT listed
	// here so preview routes through sampleViaOrchestrator -> the Oracle MCP
	// connector's export (api-gateway imports no Oracle driver). sqlserver /
	// snowflake share the same latent gap (listed here but with no switch case →
	// "unsupported connector type for sampling") and should be removed likewise.
	dbTypes := []string{"mysql", "postgresql", "postgres", "sqlserver", "snowflake"}
	ct := strings.ToLower(connectorType)
	for _, t := range dbTypes {
		if ct == t {
			return true
		}
	}
	return false
}

func buildSampleQuery(driverName string, table string, limit int) (string, []interface{}, error) {
	driverName = strings.ToLower(strings.TrimSpace(driverName))
	table = strings.TrimSpace(table)
	if table == "" {
		return "", nil, fmt.Errorf("invalid table name")
	}
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}

	// Only allow identifier segments (schema/table or database/table).
	parts := strings.Split(table, ".")
	if len(parts) == 0 || len(parts) > 2 {
		return "", nil, fmt.Errorf("invalid table name")
	}
	identRe := regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" || !identRe.MatchString(p) {
			return "", nil, fmt.Errorf("invalid table name")
		}
	}

	quoteMySQL := func(ident string) string {
		// Backticks are the MySQL identifier quote; escape embedded backticks (defense-in-depth).
		return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
	}

	qualified := ""
	switch driverName {
	case "postgres":
		if len(parts) == 1 {
			qualified = pgdriver.QuoteIdentifier(parts[0])
		} else {
			qualified = pgdriver.QuoteIdentifier(parts[0]) + "." + pgdriver.QuoteIdentifier(parts[1])
		}
		return fmt.Sprintf("SELECT * FROM %s LIMIT $1", qualified), []interface{}{limit}, nil
	case "mysql":
		if len(parts) == 1 {
			qualified = quoteMySQL(parts[0])
		} else {
			qualified = quoteMySQL(parts[0]) + "." + quoteMySQL(parts[1])
		}
		return fmt.Sprintf("SELECT * FROM %s LIMIT ?", qualified), []interface{}{limit}, nil
	default:
		return "", nil, fmt.Errorf("unsupported connector type for sampling: %s", driverName)
	}
}

// isSecretColumnName checks if a column name likely contains secrets/credentials.
// These should always be redacted in previews/Explorer results.
func isSecretColumnName(colName string) bool {
	lowerName := strings.ToLower(strings.TrimSpace(colName))
	secretKeywords := []string{
		"password", "passwd", "pwd",
		"secret", "client_secret",
		"token", "access_token", "refresh_token", "id_token",
		"api_key", "apikey",
		"private_key", "secret_key",
	}
	for _, keyword := range secretKeywords {
		if strings.Contains(lowerName, keyword) {
			return true
		}
	}
	return false
}

// ambiguousPIIKeywords are the PII keywords short enough to appear INSIDE
// ordinary English words, so they are matched per-token instead of as bare
// substrings. Every one of these had a real false positive: "tin" fires on
// dis-TIN-ct / rou-TIN-g / set-TIN-g, "nin" on run-NIN-g / war-NIN-g, "sin" on
// u-SIN-g / bu-SIN-ess, "pan" on com-PAN-y / ex-PAN-sion, "cell" on ex-CELL-ent.
// `SELECT COUNT(DISTINCT id) AS distinct_ids` came back as `***`, which is what
// surfaced this.
var ambiguousPIIKeywords = map[string]bool{
	"tin": true, "nin": true, "sin": true, "pan": true, "cell": true,
}

// columnNameTokens splits a column name into lowercase word tokens, breaking on
// separators, digits, and camelCase transitions: "customerPAN2" → [customer pan 2],
// "distinct_ids" → [distinct ids].
func columnNameTokens(colName string) []string {
	var tokens []string
	var cur []rune
	runes := []rune(strings.TrimSpace(colName))
	flush := func() {
		if len(cur) > 0 {
			tokens = append(tokens, strings.ToLower(string(cur)))
			cur = nil
		}
	}
	for i, r := range runes {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
			// lower→upper marks a camelCase boundary ("customerPan").
			if r >= 'A' && r <= 'Z' && i > 0 && runes[i-1] >= 'a' && runes[i-1] <= 'z' {
				flush()
			}
			cur = append(cur, r)
		case r >= '0' && r <= '9':
			flush()
		default:
			flush()
		}
	}
	flush()
	return tokens
}

// isPIIColumnName checks if a column name likely contains PII.
// Keep this focused on true PII fields (email/phone/id numbers) and avoid generic "id"/"*_id".
//
// This drives PREVIEW display only, and it is keyed on the RESULT column name —
// `SELECT email AS x` already renders unredacted, and the caller can query the
// connection freely in the first place. So it is a legibility guard, not an
// access control: a false positive (an aggregate rendered as `***`) costs more
// than the marginal case it would catch.
func isPIIColumnName(colName string) bool {
	lowerName := strings.ToLower(strings.TrimSpace(colName))
	piiKeywords := []string{
		// Contact
		"email", "e-mail",
		"phone", "mobile", "cell", "msisdn",

		// Government / identity numbers
		"ssn", "social_security", "social-security",
		"national_id", "national-id",
		"id_number", "id-number",
		"passport", "passport_number", "passport-number",
		"driver_license", "driver_licence", "drivers_license", "drivers_licence",
		"tax_id", "tax-id", "tin",
		"aadhar", "aadhaar",
		"nin", "sin",

		// Payments (PII-sensitive)
		"credit_card", "cc_number", "card_number", "pan",
	}

	var tokens []string
	for _, keyword := range piiKeywords {
		if ambiguousPIIKeywords[keyword] {
			if tokens == nil {
				tokens = columnNameTokens(colName)
			}
			for _, tok := range tokens {
				if tok == keyword {
					return true
				}
			}
			continue
		}
		if strings.Contains(lowerName, keyword) {
			return true
		}
	}
	return false
}

// shouldRedactColumnName decides whether to redact a column in previews.
func shouldRedactColumnName(colName string) bool {
	return isSecretColumnName(colName) || isPIIColumnName(colName)
}

// redactForPreview redacts a value for preview display
func redactForPreview(value interface{}) string {
	if value == nil {
		return ""
	}
	str := fmt.Sprintf("%v", value)
	if len(str) > 4 {
		return str[:2] + "***"
	}
	return "***"
}

// SampleConnectionData samples data from a connection for preview purposes
func SampleConnectionData(c *gin.Context) {
	connectionID, ok := requireUUIDParam(c, "id", "invalid_connection_id", "Invalid connection ID format")
	if !ok {
		return
	}
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	table := c.Query("table")
	if table == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "table query parameter is required"})
		return
	}

	limitStr := c.DefaultQuery("limit", "10")
	limit := 10
	fmt.Sscanf(limitStr, "%d", &limit)
	if limit > 100 {
		limit = 100 // Cap at 100 rows for safety
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	}

	// Connections are workspace-shared (P2f): scope the read to the active
	// workspace, not the creator, so any member can sample a shared connection.
	wsID := activeWorkspaceID(c)
	if wsID == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}

	// Get connection details — pull connector_version too so the preview
	// call routes to the same MCP container the pipeline pinned. Without
	// this the orchestrator defaults to "latest" and may route to a
	// regenerated container whose code lags the on-disk version (e.g.
	// v1.0.1 image baked before the F-13 prefix-strip landed), causing
	// `unknown operation 'shopify.<table>'` HTTP 500s on every preview.
	var connectorType, configEncrypted string
	var connectorVersion sql.NullString
	err := database.QueryRow(`
		SELECT connector_type, config, connector_version
		FROM connections
		WHERE id = $1 AND workspace_id = $2
	`, connectionID, wsID).Scan(&connectorType, &configEncrypted, &connectorVersion)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}

	if err != nil {
		// Structured 502 (not a bare 500) so the preview UI shows an actionable
		// message for every failure path, matching the upstream-error handling
		// below. (BUG-13)
		log.WithError(err).WithFields(log.Fields{
			"trace_id":      getTraceID(c),
			"user_id":       userID,
			"connection_id": connectionID,
		}).Error("SampleConnectionData failed to load connection from db")
		c.JSON(http.StatusBadGateway, gin.H{
			"error":   "data sampling failed; check your connection and try again",
			"details": "Could not load connection details. The connection may have been deleted or modified. Try refreshing the connection list.",
		})
		return
	}

	// Decrypt config
	configJSON, err := crypto.DecryptString(configEncrypted)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"trace_id":      getTraceID(c),
			"user_id":       userID,
			"connection_id": connectionID,
		}).Warn("SampleConnectionData failed to decrypt config")
		c.JSON(http.StatusBadGateway, gin.H{
			"error":   "data sampling failed; check your connection and try again",
			"details": "Connection credentials could not be decrypted. Contact support if this persists.",
		})
		return
	}

	var config map[string]interface{}
	json.Unmarshal([]byte(configJSON), &config)
	if config == nil {
		config = map[string]interface{}{}
	}
	// Inject the pinned version into the config map so SampleViaOrchestrator
	// → executor.SampleRows can honour it (executor reads
	// `connector_version` from config and falls back to `version` /
	// "latest" only if absent).
	if connectorVersion.Valid && strings.TrimSpace(connectorVersion.String) != "" {
		if _, has := config["connector_version"]; !has {
			config["connector_version"] = connectorVersion.String
		}
	}

	// Log credential access for sampling
	logConnectionAccess(c, connectionID, "sample_data", true, "")

	// Call sampling service with timeout
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	result, err := sampleDataFromConnection(ctx, connectorType, config, table, limit)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			c.JSON(http.StatusRequestTimeout, gin.H{
				"error": "Sampling timed out",
				"hint":  "The query took longer than 5 seconds. Try a smaller table or check connection performance.",
			})
			return
		}
		log.Errorf("sample data failed for connection: %v", err)
		// Surface the upstream connector/orchestrator detail (built verbatim by
		// sampleViaOrchestrator) instead of a bare HTTP 500, so the UI can show
		// an actionable message (e.g. missing scope, auth expired, table not
		// found) rather than just "HTTP 500". Use 502 because the failure is an
		// upstream connector error, not an api-gateway fault. (BUG-13)
		c.JSON(http.StatusBadGateway, gin.H{
			"error":   "data sampling failed; check your connection and try again",
			"details": cleanSampleError(err),
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

// cleanSampleError strips internal prefixes from a sampling error so the UI shows
// a concise, user-facing detail. (BUG-13)
func cleanSampleError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	msg = strings.TrimPrefix(msg, "sample_rows: ")
	return strings.TrimSpace(msg)
}

// TestConnection tests if a connection is valid
func TestConnection(c *gin.Context) {
	connectionID := c.Param("id")
	traceID := getTraceID(c)
	// Always resolve the caller. Even the "test before create" path runs under
	// the caller's identity for audit logging; the existing-connection path
	// scopes the DB read by user_id to prevent IDOR (peeking at other users'
	// connection configs by guessing the UUID).
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	// RBAC: testing a connection decrypts and uses stored credentials — a
	// write-adjacent action. Require at least `member` in the active workspace;
	// viewers (read-only) must not exercise credentials.
	if _, ok := requireWorkspaceRole(c, security.WSMember); !ok {
		return
	}
	// Connections are workspace-shared (P2f): scope the existing-connection read
	// and the status write to the active workspace, not the creator. userID is
	// kept for per-user OAuth token resolution + audit logs.
	wsID := activeWorkspaceID(c)

	var testConfig map[string]interface{}
	var connectorType string

	database := db.GetDB()

	// Test existing connection or test before creating
	// connectionID is empty or "test" when testing before creation
	shouldLoadFromDB := connectionID != "" && connectionID != "test" && database != nil
	log.WithFields(log.Fields{
		"trace_id":            traceID,
		"connection_id":       connectionID,
		"user_id":             userID,
		"should_load_from_db": shouldLoadFromDB,
	}).Debug("TestConnection")

	if shouldLoadFromDB {
		// Load connection from database — scoped to the active workspace (P2f),
		// not the creator, so any member can test a shared connection. Pull
		// connector_version alongside config so the test routes to the exact same
		// MCP container the pipeline is pinned to (mirrors PreviewConnection).
		if wsID == "" {
			c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
			return
		}
		var configEncrypted string
		var connectorVersion sql.NullString
		err := database.QueryRow(`
			SELECT connector_type, config, connector_version
			FROM connections
			WHERE id = $1 AND workspace_id = $2
		`, connectionID, wsID).Scan(&connectorType, &configEncrypted, &connectorVersion)

		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
			return
		}
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"trace_id":      traceID,
				"connection_id": connectionID,
			}).Error("TestConnection failed to load connection")
			respondError(c, http.StatusInternalServerError, "load_connection_failed", "Failed to load connection", err)
			return
		}

		// Security: block testing internal connectors for normal UI users.
		if isInternalConnectorType(connectorType) && !canAccessInternalConnectorsFromUI(c) {
			c.JSON(http.StatusForbidden, gin.H{
				"success": false,
				"status":  "forbidden",
				"error":   "This connector is internal-only and cannot be used from the UI",
			})
			return
		}

		// Lifecycle=draft gate: refuse to run test_connection on a
		// draft connector unless ?allow_draft=true. Combined with the
		// T1-8 fix (require auth-protected probe in test_connection
		// template), this prevents the "test passes on /, draft
		// auto-promotes to preview, pipeline accidentally runs in
		// prod" chain. The wizard's explicit "validate this draft"
		// flow passes allow_draft=true.
		if blocked := checkConnectionLifecycleDraft(c, database, connectionID, ""); blocked {
			return
		}

		// Decrypt config
		configJSON, err := crypto.DecryptString(configEncrypted)
		if err != nil {
			logConnectionAccess(c, connectionID, "test_connection", false, "decrypt_failed")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decrypt config"})
			return
		}

		// Log credential access for connection testing
		logConnectionAccess(c, connectionID, "test_connection", true, "")

		json.Unmarshal([]byte(configJSON), &testConfig)
		if testConfig == nil {
			testConfig = map[string]interface{}{}
		}

		// Inject the pinned connector_version into testConfig so the orchestrator
		// routes to the same MCP container the pipeline is pinned to.
		// The executor reads connector_version from config and falls back to
		// "latest" only if absent — same convention as PreviewConnection.
		if connectorVersion.Valid && strings.TrimSpace(connectorVersion.String) != "" {
			if _, has := testConfig["connector_version"]; !has {
				testConfig["connector_version"] = connectorVersion.String
			}
		}

		// Refresh OAuth token before testing: if the config has an oauth_token_id,
		// strip the stale access_token/refresh_token baked in at creation time and
		// re-expand from the live oauth_tokens row (same pattern as UpdateConnection).
		// Without this, the stale token from the config blob is sent directly to the
		// connector and fails with 401 if the token has expired since the connection
		// was last saved.
		if database != nil {
			if tokenIDVal, exists := testConfig["oauth_token_id"]; exists {
				if s, ok := tokenIDVal.(string); ok && strings.TrimSpace(s) != "" {
					delete(testConfig, "access_token")
					delete(testConfig, "refresh_token")
				}
			}
			_ = enrichConfigWithOAuthToken(database, testConfig, userID)
		}
	} else {
		// Test before creating (no ID yet)
		var req TestConnectionRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request payload", err)
			return
		}
		connectorType = req.ConnectorType
		testConfig = req.Config
		if testConfig == nil {
			testConfig = map[string]interface{}{}
		}

		// Propagate connector_version from the request into config so the
		// same MCP container is used for test-before-create as will be used
		// after the connection is saved.
		if req.ConnectorVersion != "" {
			if _, has := testConfig["connector_version"]; !has {
				testConfig["connector_version"] = req.ConnectorVersion
			}
		}

		// Security: block testing internal connectors for normal UI users.
		if isInternalConnectorType(connectorType) && !canAccessInternalConnectorsFromUI(c) {
			c.JSON(http.StatusForbidden, gin.H{
				"success": false,
				"status":  "forbidden",
				"error":   "This connector is internal-only and cannot be used from the UI",
			})
			return
		}

		// Allow testing OAuth-based configs by passing oauth_token_id only.
		if database != nil {
			_ = enrichConfigWithOAuthToken(database, testConfig, userID)
			// Fresh-connect fallback: a just-completed OAuth connect stores the
			// token under (provider, user), but the draft form may not carry its
			// oauth_token_id — so the enrich above no-ops and the connector would
			// get no credentials (test-before-create reporting "failed" right
			// after a successful Connect). If the connector is OAuth-based and
			// nothing was injected, resolve its provider and enrich from the
			// user's most-recent token, mirroring the executor/pipeline path.
			if existing, _ := testConfig["access_token"].(string); strings.TrimSpace(existing) == "" {
				if provider := connectorOAuthProvider(connectorType); provider != "" {
					if tokenID := latestOAuthTokenIDForProvider(database, provider, userID); tokenID != "" {
						testConfig["oauth_token_id"] = tokenID
						_ = enrichConfigWithOAuthToken(database, testConfig, userID)
					}
				}
			}
		}
	}

	// Fast-path: Databricks connectivity test directly in API Gateway.
	// This avoids requiring an orchestrator-side MCP connector just to validate credentials.
	if strings.Contains(strings.ToLower(connectorType), "databricks") {
		ctx := c.Request.Context()
		_, err := executeDatabricksQuery(ctx, testConfig, "SELECT 1", 1)
		success := err == nil
		testError := ""
		if err != nil {
			// Scrub host/token/DSN fragments before echoing to the client.
			testError = llmscrub.Scrub(err.Error())
		}

		if success {
			c.JSON(http.StatusOK, gin.H{
				"success":   true,
				"status":    "success",
				"message":   "Connection test successful",
				"tested_at": time.Now(),
			})
		} else {
			errorMessage := testError
			if errorMessage == "" {
				errorMessage = "Databricks connection test failed. Please verify host/token/warehouse_id."
			}
			c.JSON(http.StatusOK, gin.H{
				"success":   false,
				"status":    "failed",
				"message":   "Connection test failed",
				"error":     errorMessage,
				"tested_at": time.Now(),
			})
		}
		return
	}

	// Perform connection testing via MCP Orchestrator.
	// For OAuth-backed configs we retry transient auth (401) failures so the
	// test rides out provider token-activation lag (e.g. Shopify dev-store
	// offline tokens are rejected for ~1-2 min right after the grant).
	ctx := c.Request.Context()
	success, testError := performConnectionTestWithOAuthRetry(ctx, connectorType, testConfig)
	// Scrub DSN/host/credential fragments and any row values from the driver
	// error before it is persisted to last_test_error or echoed to the client.
	// The raw testError is retained only for in-process auth-error classification.
	scrubbedTestError := llmscrub.Scrub(testError)

	// Update connection with test result if it's an existing connection
	if connectionID != "test" && database != nil {
		testStatus := "success"
		if !success {
			testStatus = "failed"
		}

		// Defense-in-depth: the read above already proved the connection is in
		// the active workspace. Scope the UPDATE on workspace_id too so future
		// call sites that skip the read can't write across tenants.
		if _, dbErr := database.Exec(`
			UPDATE connections
			SET last_tested_at = $1, last_test_status = $2, last_test_error = $3
			WHERE id = $4 AND workspace_id = $5
		`, time.Now(), testStatus, scrubbedTestError, connectionID, wsID); dbErr != nil {
			log.WithError(dbErr).WithFields(log.Fields{
				"connection_id": connectionID,
				"test_status":   testStatus,
			}).Error("failed to persist connection test status")
		} else if testStatus == "success" {
			// Invalidate the lifecycle cache for this connector type so the
			// very next RunPipeline check sees the fresh test result and does
			// not block on draft_connector_blocked.
			var connType string
			if scanErr := database.QueryRow(
				"SELECT connector_type FROM connections WHERE id = $1", connectionID,
			).Scan(&connType); scanErr == nil {
				invalidateLifecycleCache(connType)
			}
		}
	}

	if success {
		c.JSON(http.StatusOK, gin.H{
			"success":   true,
			"status":    "success",
			"message":   "Connection test successful",
			"tested_at": time.Now(),
		})
	} else {
		// Ensure we have a meaningful error message
		errorMessage := scrubbedTestError
		if errorMessage == "" {
			errorMessage = fmt.Sprintf("Connection to %s failed. Please check your credentials and network connectivity.", connectorType)
		}

		// Friendly hint for the common OAuth case: a just-authorized token that
		// the provider hasn't activated yet. We already retried for ~20s above;
		// tell the user it's a timing issue, not bad credentials.
		_, oauthBacked := testConfig["oauth_token_id"]
		if oauthBacked && isOAuthAuthError(testError) {
			errorMessage = "Your authorization succeeded, but the provider hasn't activated the access token yet (this can take up to ~1 minute for new Shopify stores). Wait a moment and click Test Connection again — or just Save; the connection will work once the token activates."
		}

		c.JSON(http.StatusOK, gin.H{
			"success":   false,
			"status":    "failed",
			"message":   "Connection test failed",
			"error":     errorMessage,
			"tested_at": time.Now(),
		})
	}
}

// isOAuthAuthError reports whether a connection-test error string looks like a
// provider auth rejection (401/403) — used to decide whether to retry through
// OAuth token-activation lag.
func isOAuthAuthError(s string) bool {
	l := strings.ToLower(s)
	return strings.Contains(l, "401") ||
		strings.Contains(l, "403") ||
		strings.Contains(l, "unauthorized") ||
		strings.Contains(l, "invalid api key") ||
		strings.Contains(l, "access token")
}

// performConnectionTestWithOAuthRetry wraps performConnectionTest with a bounded
// retry for OAuth-backed connections. Freshly-minted provider tokens (notably
// Shopify dev-store offline tokens) are rejected with 401 for ~1-2 min after the
// OAuth grant while the provider activates them. Without a retry the user sees a
// hard "test failed" immediately after a successful Connect. We retry the auth
// error a few times with a short delay so Test eventually goes green on its own.
// Non-OAuth configs and non-auth failures (bad host, schema error, SSRF block)
// fail fast — no retry.
func performConnectionTestWithOAuthRetry(ctx context.Context, connectorType string, config map[string]interface{}) (bool, string) {
	oauthBacked := false
	if v, ok := config["oauth_token_id"]; ok {
		if s, ok2 := v.(string); ok2 && strings.TrimSpace(s) != "" {
			oauthBacked = true
		}
	}

	const maxAttempts = 5
	const delay = 3 * time.Second

	var ok bool
	var testErr string
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ok, testErr = performConnectionTest(ctx, connectorType, config)
		if ok {
			return true, ""
		}
		// Only retry OAuth-backed auth errors (activation lag). Everything else
		// fails fast so real misconfigurations surface immediately.
		if !oauthBacked || !isOAuthAuthError(testErr) {
			return ok, testErr
		}
		if attempt < maxAttempts {
			select {
			case <-ctx.Done():
				return false, testErr
			case <-time.After(delay):
			}
		}
	}
	return ok, testErr
}

// performConnectionTest performs actual connection testing via MCP
func performConnectionTest(ctx context.Context, connectorType string, config map[string]interface{}) (bool, string) {
	// Call Agent Orchestrator's Connection Test endpoint
	testServiceURL := os.Getenv("AGENT_ORCHESTRATOR_URL")
	if testServiceURL == "" {
		testServiceURL = "http://orchestrator:8080"
	}

	// Get trace_id from context for logging
	traceID := telemetry.TraceIDFromContext(ctx)
	log.WithFields(log.Fields{
		"trace_id":       traceID,
		"connector_type": connectorType,
		"service_url":    testServiceURL,
	}).Debug("performConnectionTest")

	// Convert config to map[string]string for orchestrator compatibility.
	// Also extract connector_version if it was injected into config by the
	// TestConnection handler (pinned connections) or passed in the request
	// body (test-before-create). It is forwarded as a top-level field so
	// the orchestrator can pass it to executorAgent.TestConnection without
	// having to parse it back out of the config strings.
	configStrings := make(map[string]string)
	connectorVersion := "latest"
	for k, v := range config {
		strVal := fmt.Sprintf("%v", v)
		if k == "connector_version" && strVal != "" {
			connectorVersion = strVal
			continue // keep out of config to avoid confusing the MCP connector
		}
		configStrings[k] = strVal
	}

	// Prepare request
	requestBody := map[string]interface{}{
		"connector_type":    connectorType,
		"connector_version": connectorVersion,
		"config":            configStrings,
	}

	requestJSON, err := json.Marshal(requestBody)
	if err != nil {
		return false, "Internal error: Failed to prepare test request"
	}

	// Create HTTP request with context
	req, err := http.NewRequestWithContext(
		ctx,
		"POST",
		// Orchestrator exposes connection testing at /api/v1/agent/test-connection.
		// (The old /api/v1/connections/test was removed 2026-05-22 — see orchestrator main.go.)
		fmt.Sprintf("%s/api/v1/agent/test-connection", strings.TrimRight(testServiceURL, "/")),
		bytes.NewBuffer(requestJSON),
	)
	if err != nil {
		return false, "Internal error: Failed to create test request"
	}

	req.Header.Set("Content-Type", "application/json")

	// Propagate trace context to downstream service
	for k, v := range telemetry.InjectTraceToHeaders(ctx) {
		req.Header.Set(k, v)
	}

	// Execute request with timeout.
	// 90s: must exceed the orchestrator's 60s cold-start container-wait
	// (server_manager.go DeployWaitTimeout) so a connector that needs JIT
	// deployment returns its real verdict instead of the client aborting with
	// "context deadline exceeded (Client.Timeout exceeded while awaiting headers)".
	client := &http.Client{
		Timeout: 90 * time.Second,
	}

	setInternalServiceSecret(req)
	resp, err := client.Do(req)
	if err != nil {
		// Network/service error - orchestrator is unreachable
		log.WithError(err).WithFields(log.Fields{
			"trace_id":       traceID,
			"connector_type": connectorType,
			"service_url":    testServiceURL,
		}).Warn("performConnectionTest orchestrator unreachable")
		return false, fmt.Sprintf("Failed to reach test service for %s connector. Error: %v", connectorType, err)
	}
	defer resp.Body.Close()

	// Read response
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, "Failed to read test response from orchestrator"
	}

	// SECURITY: Log metadata only, not full response (to avoid exposing credentials)
	log.WithFields(log.Fields{
		"trace_id":       traceID,
		"connector_type": connectorType,
		"status":         resp.StatusCode,
		"response_bytes": len(responseBody),
	}).Debug("performConnectionTest response received")

	// Check HTTP status
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Sprintf("Test service returned error (HTTP %d): %s", resp.StatusCode, string(responseBody))
	}

	// Parse response
	var testResponse struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
		Message string `json:"message"`
	}

	if err := json.Unmarshal(responseBody, &testResponse); err != nil {
		return false, fmt.Sprintf("Invalid response from test service: %s", string(responseBody))
	}

	// Return detailed error
	errorMsg := testResponse.Error
	if errorMsg == "" && testResponse.Message != "" && !testResponse.Success {
		errorMsg = testResponse.Message
	}

	return testResponse.Success, errorMsg
}

// ============================================================================
// FULLY GENERIC CONNECTION TESTING
// ============================================================================
// All connection tests now go through the MCP connector architecture.
// Each connector implements test_connection() and returns:
//   {"success": bool, "error": string, "message": string}
//
// No connector-specific logic is needed in this API Gateway code.
// ============================================================================

// connectionSlug derives a connection's URL slug from its name, matching the
// normalization resolveConnectionID accepts for slug-based routes (lowercase,
// spaces→hyphens). The slug is computed, not stored — there is no slug column.
func connectionSlug(name string) string {
	return strings.ToLower(strings.ReplaceAll(name, " ", "-"))
}

// resolveConnectionID resolves a connection name or UUID to a UUID
// If input is already a valid UUID, returns it
// If input is a name, looks up the connection and returns its UUID
func resolveConnectionID(nameOrID string, wsID string, database *sql.DB) (string, error) {
	// Check if it's already a valid UUID
	if _, err := uuid.Parse(nameOrID); err == nil {
		// It's a UUID, return as is
		return nameOrID, nil
	}

	// It's a name slug: resolve to the connection UUID, always scoped to the
	// active workspace (connections are workspace-shared, not per-user). The
	// match mirrors GetConnection — case-insensitive and tolerant of
	// hyphen-for-space slugification (e.g. "My Postgres" -> "my-postgres").
	var connectionID string
	err := database.QueryRow(`
		SELECT id FROM connections
		WHERE workspace_id = $2
		  AND (LOWER(name) = LOWER($1) OR LOWER(REPLACE(name, ' ', '-')) = LOWER($1))
		ORDER BY created_at DESC
		LIMIT 1
	`, nameOrID, wsID).Scan(&connectionID)

	if err == sql.ErrNoRows {
		return "", fmt.Errorf("connection '%s' not found", nameOrID)
	}
	if err != nil {
		return "", fmt.Errorf("failed to resolve connection: %v", err)
	}
	return connectionID, nil
}

// connectionInWorkspace reports whether connID belongs to wsID. resolveConnectionID's
// UUID fast-path returns a caller-supplied UUID without checking tenancy, so any
// caller that persists a connection reference (e.g. CreatePipeline) must verify
// the connection lives in the active workspace — otherwise a member of one
// workspace could wire a pipeline to another workspace's connection (IDOR).
// Fails closed: empty ids or any query error → false.
func connectionInWorkspace(database *sql.DB, connID, wsID string) bool {
	if connID == "" || wsID == "" || database == nil {
		return false
	}
	var one int
	err := database.QueryRow(`SELECT 1 FROM connections WHERE id = $1 AND workspace_id = $2`, connID, wsID).Scan(&one)
	return err == nil
}

// translateBindError converts the raw `ShouldBindJSON` failure into a
// user-friendly summary plus a per-field breakdown. When the underlying
// error is a `validator.ValidationErrors` slice we surface each field
// with a human sentence; otherwise we fall back to the raw message but
// strip the leading Go-binding context that leaks struct names. Returned
// message is what we show in `message`; the slice is what we attach to
// `validation_errors` so a UI can highlight individual inputs.
func translateBindError(err error) (string, []map[string]string) {
	if err == nil {
		return "Invalid request", nil
	}
	var vErrs validator.ValidationErrors
	if errors.As(err, &vErrs) && len(vErrs) > 0 {
		out := make([]map[string]string, 0, len(vErrs))
		for _, fe := range vErrs {
			field := fe.Field()
			tag := fe.Tag()
			lf := strings.ToLower(field)
			var msg string
			switch tag {
			case "required":
				msg = fmt.Sprintf("%s is required", field)
			case "oneof":
				msg = fmt.Sprintf("%s must be one of: %s", field, fe.Param())
			case "email":
				msg = fmt.Sprintf("%s must be a valid email", field)
			case "min", "max", "len":
				msg = fmt.Sprintf("%s failed %s validation (param=%s)", field, tag, fe.Param())
			default:
				msg = fmt.Sprintf("%s failed validation (%s)", field, tag)
			}
			out = append(out, map[string]string{"field": lf, "message": msg})
		}
		if len(out) == 1 {
			return out[0]["message"], out
		}
		return fmt.Sprintf("%d fields failed validation", len(out)), out
	}
	// Fallback for non-validator errors (JSON parse, type mismatch, etc.).
	// Common shapes we sanitise:
	//   - "Key: 'StructName.Field' Error:Field validation for 'Field' failed on the 'required' tag"
	//     → strip leading Key:'...' prefix, keep "Field validation …"
	//   - "json: cannot unmarshal number into Go struct field CreatePipelineRequest.name of type string"
	//     → strip the Go struct name, keep "name expected type string"
	//   - "json: unknown field \"foo\""
	//     → keep as-is, it's already user-readable
	raw := err.Error()
	if i := strings.Index(raw, "Error:"); i >= 0 && i+6 < len(raw) {
		raw = strings.TrimSpace(raw[i+6:])
	}
	// Replace "Go struct field <Struct>.<field>" with just "field"
	if idx := strings.Index(raw, "Go struct field "); idx >= 0 {
		tail := raw[idx+len("Go struct field "):]
		if dot := strings.Index(tail, "."); dot >= 0 {
			rest := tail[dot+1:]
			if sp := strings.Index(rest, " "); sp >= 0 {
				field := rest[:sp]
				suffix := rest[sp:]
				raw = strings.TrimSpace(raw[:idx]) + " field '" + field + "'" + suffix
			}
		}
	}
	return raw, nil
}

// maskSensitiveFields masks sensitive fields in config for API responses.
//
// The masking rule is intentionally permissive: we hide anything that
// looks remotely like a secret rather than maintaining an exact-match
// list (which is how Shopify's access_token leaked in plaintext while
// Postgres' password was masked — the original list only knew about
// `token`, not `access_token`/`refresh_token`/`id_token`). A key is
// treated as sensitive when it is either an exact member of the curated
// list below OR ends in one of the well-known secret suffixes
// (`_token`, `_secret`, `_key`, `_password`).
func maskSensitiveFields(config map[string]interface{}) map[string]interface{} {
	masked := make(map[string]interface{}, len(config))
	for key, value := range config {
		if isSensitiveConfigKey(key) {
			masked[key] = "••••••••"
		} else {
			masked[key] = maskSensitiveValue(value)
		}
	}
	return masked
}

// maskSensitiveValue recurses into nested maps/slices so a secret nested below
// the top level (e.g. config.auth.client_secret, or a credentials object inside
// a list) is masked instead of returned verbatim.
func maskSensitiveValue(value interface{}) interface{} {
	switch v := value.(type) {
	case map[string]interface{}:
		return maskSensitiveFields(v)
	case []interface{}:
		out := make([]interface{}, len(v))
		for i, item := range v {
			out[i] = maskSensitiveValue(item)
		}
		return out
	default:
		return value
	}
}

// sensitiveExactKeys lists every config field name known to carry a secret.
// It MUST cover every connector spec.json field marked `"secret": true`
// (see TestMaskSensitiveFields_CoversSpecSecretFields). The old heuristic
// missed `service_account_json` (a full GCP service-account JSON incl. the RSA
// private_key — ends in `_json`, not `_key`) and `access_key_id` (ends in
// `_id`), so both leaked in cleartext in GET/List/Create connection responses.
var sensitiveExactKeys = map[string]struct{}{
	"password":             {},
	"passwd":               {},
	"token":                {},
	"access_token":         {},
	"refresh_token":        {},
	"id_token":             {},
	"api_key":              {},
	"apikey":               {},
	"secret":               {},
	"secret_key":           {},
	"secret_access_key":    {}, // AWS
	"access_key_id":        {}, // AWS (spec secret:true)
	"client_secret":        {},
	"security_token":       {},
	"session_token":        {},
	"private_key":          {},
	"private_key_id":       {},
	"service_account_json": {}, // GCP/BigQuery/GCS (spec secret:true)
	"credentials_json":     {}, // GCP alternate
	"credentials":          {},
	"sas_token":            {}, // Azure
	"account_key":          {}, // Azure
	"connection_string":    {},
}

// sensitiveSuffixes / sensitiveSubstrings catch fields not in the exact list so
// future connectors are covered by pattern. Substrings are deliberately chosen
// to be specific (NOT bare "key", which would over-match primary_key/sort_key).
var sensitiveSuffixes = []string{"_token", "_secret", "_key", "_password", "_passwd"}
var sensitiveSubstrings = []string{"password", "passwd", "secret", "credential", "private_key", "service_account", "access_key", "api_key", "apikey"}

func isSensitiveConfigKey(key string) bool {
	lk := strings.ToLower(strings.TrimSpace(key))
	if _, ok := sensitiveExactKeys[lk]; ok {
		return true
	}
	for _, sfx := range sensitiveSuffixes {
		if strings.HasSuffix(lk, sfx) {
			return true
		}
	}
	for _, sub := range sensitiveSubstrings {
		if strings.Contains(lk, sub) {
			return true
		}
	}
	return false
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
	Name         string `json:"name"`
	Type         string `json:"type"`
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

// GetConnectionMetadata discovers schema/tables from a connection using AI Agent
func GetConnectionMetadata(c *gin.Context) {
	connectionID, ok := requireUUIDParam(c, "id", "invalid_connection_id", "Invalid connection ID format")
	if !ok {
		return
	}
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	traceID := getTraceID(c)
	log.WithFields(log.Fields{
		"trace_id":      traceID,
		"user_id":       userID,
		"connection_id": connectionID,
	}).Debug("GetConnectionMetadata")

	// Cross-tenant guard (IDOR): the schema cache below is keyed by connection
	// UUID ONLY, so a cache hit would otherwise serve another workspace's schema
	// (tables/columns/types/PKs) to any authenticated caller once the owner warmed
	// the cache — the workspace-scoped SELECT further down runs only on a cache
	// MISS. Verify the connection belongs to the caller's active workspace BEFORE
	// the cache read; a cheap indexed existence check (id PK + workspace_id) gates
	// both the cache-hit and DB paths. (GetConnection/Sample/Delete already scope
	// every read; only this cache-first path bypassed it.)
	if gdb := db.GetDB(); gdb == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	} else {
		guardWsID := activeWorkspaceID(c)
		if guardWsID == "" {
			c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
			return
		}
		var connGuard int
		switch gerr := gdb.QueryRow(`SELECT 1 FROM connections WHERE id = $1 AND workspace_id = $2`, connectionID, guardWsID).Scan(&connGuard); gerr {
		case nil:
			// connection is in the caller's workspace — cache read below is safe
		case sql.ErrNoRows:
			c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
			return
		default:
			respondError(c, http.StatusInternalServerError, "connection_query_failed", "Failed to load connection", gerr)
			return
		}
	}

	// Check if client requested cache refresh
	refresh := c.Query("refresh") == "true"

	// Try cache first (if enabled and not refreshing). Safe now: ownership was
	// verified against the caller's workspace immediately above.
	if schemaCache != nil && !refresh {
		ctx := context.Background()
		cached, err := schemaCache.Get(ctx, connectionID)
		if err == nil && cached != nil {
			// Cache hit - return cached response
			//
			// IMPORTANT: Older cached payloads may contain invalid negative row_count values
			// from upstream connectors (e.g. MySQL TABLE_ROWS quirks). Sanitize on read so
			// UIs never see negative counts, even if the cache predates our sanitization.
			// Best-effort: if sanitize fails, fall back to serving cached bytes as-is.
			var payload map[string]interface{}
			if err := json.Unmarshal(cached, &payload); err == nil {
				if tablesAny, ok := payload["tables"].([]interface{}); ok {
					changed := false
					for _, tAny := range tablesAny {
						m, ok := tAny.(map[string]interface{})
						if !ok {
							continue
						}
						rc, ok := m["row_count"]
						if !ok || rc == nil {
							continue
						}
						// json.Unmarshal numbers as float64 in interface{}.
						// But older cached payloads might contain strings; handle both.
						switch v := rc.(type) {
						case float64:
							if v < 0 {
								m["row_count"] = float64(0)
								m["is_exact_count"] = false
								changed = true
							}
						case string:
							if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil && f < 0 {
								m["row_count"] = float64(0)
								m["is_exact_count"] = false
								changed = true
							}
						}
					}
					if changed {
						if sanitized, err := json.Marshal(payload); err == nil {
							cached = sanitized
							// Best-effort: update cache so future reads are already sanitized.
							_ = schemaCache.Set(ctx, connectionID, cached)
						}
					}
				}
			}
			log.WithField("connection_id", connectionID).Debug("Returning cached schema")
			c.Header("X-Cache-Status", "HIT")
			c.Data(http.StatusOK, "application/json", cached)
			return
		}
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Database not available",
		})
		return
	}

	// Connections are workspace-shared (P2f): scope the read to the active
	// workspace, not the creator. userID is kept (forwarded to the schema-
	// discovery orchestrator below as a per-user identity).
	wsID := activeWorkspaceID(c)
	if wsID == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}

	// Fetch connection details
	var configJSON string
	var connectorType, connType string
	err := database.QueryRow(`
		SELECT connector_type, type, config
		FROM connections
		WHERE id = $1 AND workspace_id = $2
	`, connectionID, wsID).Scan(&connectorType, &connType, &configJSON)

	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		} else {
			respondError(c, http.StatusInternalServerError, "connection_query_failed", "Failed to load connection", err)
		}
		return
	}

	// Decrypt config
	decryptedConfig, err := crypto.Decrypt(configJSON)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"trace_id":      traceID,
			"user_id":       userID,
			"connection_id": connectionID,
		}).Warn("GetConnectionMetadata decrypt config failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decrypt connection config"})
		return
	}

	var config map[string]interface{}
	if err := json.Unmarshal([]byte(decryptedConfig), &config); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse connection config"})
		return
	}

	// Call AI Agent for schema discovery
	agentRequest := map[string]interface{}{
		"task":           "discover_schema",
		"connection_id":  connectionID,
		"connector_type": connectorType,
		"config":         config,
		"user_id":        userID,
	}

	agentReqBody, _ := json.Marshal(agentRequest)

	orchestratorURL := os.Getenv("ORCHESTRATOR_URL")
	if orchestratorURL == "" {
		orchestratorURL = "http://orchestrator:8080"
	}
	log.WithFields(log.Fields{
		"trace_id":         traceID,
		"connection_id":    connectionID,
		"connector_type":   connectorType,
		"orchestrator_url": orchestratorURL,
	}).Debug("GetConnectionMetadata calling schema discovery agent")

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", orchestratorURL+"/api/v1/agent/discover-schema", bytes.NewBuffer(agentReqBody))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create agent request"})
		return
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range telemetry.InjectTraceToHeaders(ctx) {
		req.Header.Set(k, v)
	}

	// Schema discovery can take longer (30-60 seconds for large databases)
	setInternalServiceSecret(req)
	client := &http.Client{Timeout: 90 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"trace_id":      traceID,
			"connection_id": connectionID,
		}).Warn("GetConnectionMetadata schema discovery agent unavailable")
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   "Schema discovery agent unavailable",
			"details": err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.WithFields(log.Fields{
			"trace_id":      traceID,
			"connection_id": connectionID,
			"status":        resp.StatusCode,
		}).Warn("GetConnectionMetadata schema discovery failed")
		c.JSON(resp.StatusCode, gin.H{
			"error":   "Schema discovery failed",
			"details": string(body),
		})
		return
	}

	var agentResponse struct {
		// v2 envelope fields (optional)
		SchemaVersion         string                   `json:"schema_version"`
		DiscoveredAt          string                   `json:"discovered_at"`
		ConnectorType         string                   `json:"connector_type"`
		ConnectorVersion      string                   `json:"connector_version"`
		DatabaseVersion       string                   `json:"database_version"`
		TotalTablesAvailable  int                      `json:"total_tables_available"`
		TotalTablesDiscovered int                      `json:"total_tables_discovered"`
		DiscoveryDurationMs   int64                    `json:"discovery_duration_ms"`
		OverallStatus         string                   `json:"overall_status"`
		WarningsObjects       []map[string]interface{} `json:"warnings_objects"`
		WarningsMessages      []string                 `json:"warnings_messages"`

		// always-present key
		Tables []TableMetadata `json:"tables"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&agentResponse); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse agent response"})
		return
	}

	log.WithFields(log.Fields{
		"trace_id":      traceID,
		"connection_id": connectionID,
		"tables":        len(agentResponse.Tables),
	}).Debug("GetConnectionMetadata schema discovery complete")

	// Sanitize row counts: some connectors return negative/invalid estimates (e.g. MySQL TABLE_ROWS).
	// Never surface negative counts to the UI; omit them instead (RowCount=0 is omitted by json tags).
	for i := range agentResponse.Tables {
		if agentResponse.Tables[i].RowCount < 0 {
			agentResponse.Tables[i].RowCount = 0
			agentResponse.Tables[i].IsExactCount = false
		}
	}

	// Parse query parameters for filtering/pagination
	search := c.Query("search")
	limitStr := c.DefaultQuery("limit", "25")
	offsetStr := c.DefaultQuery("offset", "0")
	includeColumns := c.DefaultQuery("include_columns", "true") == "true"
	sortBy := c.DefaultQuery("sort_by", "name") // name, size, relevance
	tablesParam := c.Query("tables")            // Comma-separated exact table names (for post-selection schema fetch)

	limit := 25
	if s := strings.TrimSpace(limitStr); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			limit = v
		}
	}
	if limit > 200 {
		limit = 200
	}
	offset := 0
	if s := strings.TrimSpace(offsetStr); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 0 {
			offset = v
		}
	}

	// Filter to specific tables if requested (for suggestions flow)
	tablesToProcess := agentResponse.Tables
	if tablesParam != "" {
		requestedTables := strings.Split(tablesParam, ",")
		requestedSet := make(map[string]bool)
		for _, t := range requestedTables {
			t = strings.TrimSpace(t)
			if t != "" {
				// Normalize: try both with and without schema prefix
				requestedSet[t] = true
				requestedSet[strings.ToLower(t)] = true
				// Also try just the table name part (for qualified names like "db.table")
				if parts := strings.Split(t, "."); len(parts) > 1 {
					requestedSet[parts[len(parts)-1]] = true
					requestedSet[strings.ToLower(parts[len(parts)-1])] = true
				}
			}
		}

		filtered := make([]TableMetadata, 0)
		for _, table := range agentResponse.Tables {
			tableName := table.Name
			tableNameLower := strings.ToLower(tableName)
			qualifiedName := tableName
			if table.Schema != "" {
				qualifiedName = table.Schema + "." + tableName
			}
			qualifiedNameLower := strings.ToLower(qualifiedName)

			if requestedSet[tableName] || requestedSet[tableNameLower] ||
				requestedSet[qualifiedName] || requestedSet[qualifiedNameLower] {
				filtered = append(filtered, table)
			}
		}
		tablesToProcess = filtered
		log.WithFields(log.Fields{
			"trace_id":      traceID,
			"connection_id": connectionID,
			"filtered":      len(filtered),
			"total":         len(agentResponse.Tables),
		}).Debug("GetConnectionMetadata filtered to requested tables")
	}

	// Apply search/sorting/column filtering
	filteredTables := filterAndSortTables(tablesToProcess, search, sortBy, includeColumns)
	total := len(filteredTables)

	// Apply pagination
	end := offset + limit
	if end > total {
		end = total
	}
	if offset > total {
		offset = total
	}

	paginatedTables := filteredTables[offset:end]

	discoveredAt := time.Now().UTC().Format(time.RFC3339)
	if agentResponse.DiscoveredAt != "" {
		discoveredAt = agentResponse.DiscoveredAt
	}

	response := gin.H{
		"connection_id":  connectionID,
		"connector_type": connectorType,
		"tables":         paginatedTables,
		"total":          total,
		"limit":          limit,
		"offset":         offset,
		"discovered_at":  discoveredAt,
		// v2 envelope fields (additive; safe for older clients)
		"schema_version":          agentResponse.SchemaVersion,
		"connector_version":       agentResponse.ConnectorVersion,
		"database_version":        agentResponse.DatabaseVersion,
		"total_tables_available":  agentResponse.TotalTablesAvailable,
		"total_tables_discovered": agentResponse.TotalTablesDiscovered,
		"discovery_duration_ms":   agentResponse.DiscoveryDurationMs,
		"overall_status":          agentResponse.OverallStatus,
		"warnings_objects":        agentResponse.WarningsObjects,
		"warnings_messages":       agentResponse.WarningsMessages,
	}

	// Cache the full agent response (before pagination) for subsequent requests
	if schemaCache != nil {
		cacheData := gin.H{
			"connection_id":           connectionID,
			"connector_type":          connectorType,
			"tables":                  agentResponse.Tables, // Full table list
			"schema_version":          agentResponse.SchemaVersion,
			"connector_version":       agentResponse.ConnectorVersion,
			"database_version":        agentResponse.DatabaseVersion,
			"total_tables_available":  agentResponse.TotalTablesAvailable,
			"total_tables_discovered": agentResponse.TotalTablesDiscovered,
			"discovery_duration_ms":   agentResponse.DiscoveryDurationMs,
			"overall_status":          agentResponse.OverallStatus,
			"warnings_objects":        agentResponse.WarningsObjects,
			"warnings_messages":       agentResponse.WarningsMessages,
			"discovered_at":           discoveredAt,
		}

		cacheJSON, err := json.Marshal(cacheData)
		if err == nil {
			ctx := context.Background()
			if err := schemaCache.Set(ctx, connectionID, cacheJSON); err != nil {
				log.WithError(err).Warn("Failed to cache schema")
			}
		}
	}

	c.Header("X-Cache-Status", "MISS")
	c.JSON(http.StatusOK, response)
}

// filterAndSortTables applies search, sorting, and column filtering
func filterAndSortTables(tables []TableMetadata, search string, sortBy string, includeColumns bool) []TableMetadata {
	filtered := make([]TableMetadata, 0)

	// Apply search filter
	searchLower := strings.ToLower(search)
	for _, table := range tables {
		if search == "" || strings.Contains(strings.ToLower(table.Name), searchLower) {
			filtered = append(filtered, table)
		}
	}

	// Apply sorting
	switch sortBy {
	case "size":
		sort.Slice(filtered, func(i, j int) bool {
			return filtered[i].RowCount > filtered[j].RowCount
		})
	case "relevance":
		// For now, sort by row count as a proxy for relevance
		// TODO: Implement proper relevance scoring with LLM
		sort.Slice(filtered, func(i, j int) bool {
			return filtered[i].RowCount > filtered[j].RowCount
		})
	default: // "name"
		sort.Slice(filtered, func(i, j int) bool {
			return filtered[i].Name < filtered[j].Name
		})
	}

	// Remove columns if not requested (for lightweight responses)
	if !includeColumns {
		for i := range filtered {
			filtered[i].Columns = nil
		}
	}

	return filtered
}

// GetRecommendedTables returns AI-recommended tables based on user intent
func GetRecommendedTables(c *gin.Context) {
	connectionID, ok := requireUUIDParam(c, "id", "invalid_connection_id", "Invalid connection ID format")
	if !ok {
		return
	}
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	// Parse request body
	var request struct {
		Intent    string `json:"intent"`     // User's goal/intent
		MaxTables int    `json:"max_tables"` // Max tables to recommend (default 10)
		UseCase   string `json:"use_case"`   // e.g., "replication", "analytics", "migration"
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}

	if request.MaxTables == 0 {
		request.MaxTables = 10
	}

	// Get database
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database not available"})
		return
	}

	// Connections are workspace-shared (P2f): scope the read to the active
	// workspace, not the creator. userID is kept (forwarded to the orchestrator).
	wsID := activeWorkspaceID(c)
	if wsID == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		return
	}

	// Fetch connection details
	var configJSON string
	var connectorType, connType string
	err := database.QueryRow(`
		SELECT connector_type, type, config
		FROM connections
		WHERE id = $1 AND workspace_id = $2
	`, connectionID, wsID).Scan(&connectorType, &connType, &configJSON)

	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Connection not found"})
		} else {
			respondError(c, http.StatusInternalServerError, "connection_query_failed", "Failed to load connection", err)
		}
		return
	}

	// Decrypt config
	decryptedConfig, err := crypto.Decrypt(configJSON)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decrypt config"})
		return
	}

	var config map[string]interface{}
	if err := json.Unmarshal([]byte(decryptedConfig), &config); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse config"})
		return
	}

	// Call orchestrator to discover ALL tables first (cached)
	orchestratorURL := os.Getenv("ORCHESTRATOR_URL")
	if orchestratorURL == "" {
		orchestratorURL = "http://orchestrator:8080"
	}

	agentRequest := map[string]interface{}{
		"task":           "discover_schema",
		"connection_id":  connectionID,
		"connector_type": connectorType,
		"config":         config,
		"user_id":        userID,
	}

	agentReqBody, _ := json.Marshal(agentRequest)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", orchestratorURL+"/api/v1/agent/discover-schema", bytes.NewBuffer(agentReqBody))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create request"})
		return
	}
	req.Header.Set("Content-Type", "application/json")
	setInternalServiceSecret(req)

	client := &http.Client{Timeout: 90 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Schema discovery unavailable"})
		return
	}
	defer resp.Body.Close()

	var agentResponse struct {
		Tables []TableMetadata `json:"tables"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&agentResponse); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse response"})
		return
	}

	rankingMethod := "heuristic"
	recommendations, err := rankTablesByLLM(c.Request.Context(), agentResponse.Tables, request.Intent, request.UseCase, request.MaxTables)
	if err != nil {
		log.WithError(err).Warn("LLM table ranking unavailable, falling back to heuristics")
		recommendations = rankTablesByIntent(agentResponse.Tables, request.Intent, request.UseCase, request.MaxTables)
	} else {
		rankingMethod = "llm"
	}

	c.JSON(http.StatusOK, gin.H{
		"connection_id":   connectionID,
		"intent":          request.Intent,
		"recommendations": recommendations,
		"total_available": len(agentResponse.Tables),
		"recommended_at":  time.Now().UTC().Format(time.RFC3339),
		"ranking_method":  rankingMethod,
	})
}

// TableRecommendation represents an AI-recommended table with reasoning
type TableRecommendation struct {
	Name       string   `json:"name"`
	Schema     string   `json:"schema,omitempty"`
	RowCount   int64    `json:"row_count"`
	KeyColumns []string `json:"key_columns"` // Top 3-5 important columns
	Reason     string   `json:"reason"`      // Why this table is recommended
	Confidence float64  `json:"confidence"`  // 0.0 to 1.0
	Category   string   `json:"category"`    // e.g., "user_data", "transactions", "logs"
	HasPII     bool     `json:"has_pii"`     // Does it contain PII?
}

// rankTablesByLLM calls the llm-service /agents/rank-tables endpoint to produce
// semantic table rankings. Returns an error if the service is unreachable or
// returns a non-200 status, so the caller can fall back to heuristics.
func rankTablesByLLM(ctx context.Context, tables []TableMetadata, intent string, useCase string, maxTables int) ([]TableRecommendation, error) {
	llmServiceURL := os.Getenv("LLM_SERVICE_URL")
	if llmServiceURL == "" {
		llmServiceURL = "http://llm-service:5000"
	}

	type columnInfo struct {
		Name     string `json:"name"`
		DataType string `json:"data_type,omitempty"`
	}
	type tableInfo struct {
		Name       string       `json:"name"`
		SchemaName string       `json:"schema_name,omitempty"`
		RowCount   int64        `json:"row_count,omitempty"`
		Columns    []columnInfo `json:"columns,omitempty"`
	}
	type rankRequest struct {
		Tables    []tableInfo `json:"tables"`
		Intent    string      `json:"intent"`
		UseCase   string      `json:"use_case"`
		MaxTables int         `json:"max_tables"`
	}

	reqTables := make([]tableInfo, 0, len(tables))
	for _, t := range tables {
		cols := make([]columnInfo, 0, len(t.Columns))
		for _, c := range t.Columns {
			cols = append(cols, columnInfo{Name: c.Name, DataType: c.Type})
		}
		reqTables = append(reqTables, tableInfo{
			Name:       t.Name,
			SchemaName: t.Schema,
			RowCount:   t.RowCount,
			Columns:    cols,
		})
	}

	body, err := json.Marshal(rankRequest{
		Tables:    reqTables,
		Intent:    intent,
		UseCase:   useCase,
		MaxTables: maxTables,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal rank request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/agents/rank-tables", llmServiceURL),
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("build rank request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 15 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("llm rank request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("llm rank returned status %d", resp.StatusCode)
	}

	type rankedItem struct {
		Name       string  `json:"name"`
		SchemaName string  `json:"schema_name,omitempty"`
		RowCount   *int64  `json:"row_count,omitempty"`
		Confidence float64 `json:"confidence"`
		Reason     string  `json:"reason"`
		Category   string  `json:"category"`
		HasPII     bool    `json:"has_pii"`
	}
	var ranked []rankedItem
	if err := json.NewDecoder(resp.Body).Decode(&ranked); err != nil {
		return nil, fmt.Errorf("decode rank response: %w", err)
	}

	// Enrich with column info from original table metadata
	tableMeta := make(map[string]TableMetadata, len(tables))
	for _, t := range tables {
		tableMeta[t.Name] = t
	}

	recommendations := make([]TableRecommendation, 0, len(ranked))
	for _, item := range ranked {
		meta, ok := tableMeta[item.Name]
		keyColumns := make([]string, 0)
		rowCount := int64(0)
		if ok {
			rowCount = meta.RowCount
			for i, col := range meta.Columns {
				if i >= 5 {
					break
				}
				keyColumns = append(keyColumns, col.Name)
			}
		}
		if item.RowCount != nil {
			rowCount = *item.RowCount
		}
		recommendations = append(recommendations, TableRecommendation{
			Name:       item.Name,
			Schema:     item.SchemaName,
			RowCount:   rowCount,
			KeyColumns: keyColumns,
			Reason:     item.Reason,
			Confidence: item.Confidence,
			Category:   item.Category,
			HasPII:     item.HasPII,
		})
	}
	return recommendations, nil
}

// rankTablesByIntent uses heuristics to rank tables by relevance to user intent
func rankTablesByIntent(tables []TableMetadata, intent string, useCase string, maxTables int) []TableRecommendation {
	intentLower := strings.ToLower(intent)
	useCaseLower := strings.ToLower(useCase)

	recommendations := make([]TableRecommendation, 0)

	// Simple keyword-based scoring (will be replaced with LLM)
	for _, table := range tables {
		tableLower := strings.ToLower(table.Name)
		score := 0.0
		reason := ""
		category := "general"

		// Keyword matching
		if strings.Contains(intentLower, "user") && (strings.Contains(tableLower, "user") || strings.Contains(tableLower, "customer") || strings.Contains(tableLower, "account")) {
			score += 0.8
			reason = "Contains user/customer data relevant to your query"
			category = "user_data"
		}

		if strings.Contains(intentLower, "order") && (strings.Contains(tableLower, "order") || strings.Contains(tableLower, "purchase") || strings.Contains(tableLower, "transaction")) {
			score += 0.8
			reason = "Contains order/transaction data"
			category = "transactions"
		}

		if strings.Contains(intentLower, "log") && strings.Contains(tableLower, "log") {
			score += 0.7
			reason = "Contains log data"
			category = "logs"
		}

		// Use case specific scoring
		if useCaseLower == "replication" && table.RowCount > 1000 {
			score += 0.3
			if reason == "" {
				reason = "Large table suitable for replication"
			}
		}

		// Boost tables with reasonable size
		if table.RowCount > 0 && table.RowCount < 10000000 {
			score += 0.2
		}

		// Detect potential PII
		hasPII := detectPII(table)

		if score > 0 {
			// Get key columns (first 3-5)
			keyColumns := make([]string, 0)
			for i, col := range table.Columns {
				if i >= 5 {
					break
				}
				keyColumns = append(keyColumns, col.Name)
			}

			if reason == "" {
				reason = "Potentially relevant based on table name and structure"
			}

			recommendations = append(recommendations, TableRecommendation{
				Name:       table.Name,
				Schema:     table.Schema,
				RowCount:   table.RowCount,
				KeyColumns: keyColumns,
				Reason:     reason,
				Confidence: score,
				Category:   category,
				HasPII:     hasPII,
			})
		}
	}

	// Sort by confidence score
	sort.Slice(recommendations, func(i, j int) bool {
		return recommendations[i].Confidence > recommendations[j].Confidence
	})

	// Return top N
	if len(recommendations) > maxTables {
		recommendations = recommendations[:maxTables]
	}

	// If no matches, return top tables by size
	if len(recommendations) == 0 {
		for i, table := range tables {
			if i >= maxTables {
				break
			}
			keyColumns := make([]string, 0)
			for j, col := range table.Columns {
				if j >= 5 {
					break
				}
				keyColumns = append(keyColumns, col.Name)
			}

			recommendations = append(recommendations, TableRecommendation{
				Name:       table.Name,
				Schema:     table.Schema,
				RowCount:   table.RowCount,
				KeyColumns: keyColumns,
				Reason:     "Common table in this database",
				Confidence: 0.3,
				Category:   "general",
				HasPII:     detectPII(table),
			})
		}
	}

	return recommendations
}

// detectPII checks if a table likely contains PII data
func detectPII(table TableMetadata) bool {
	piiKeywords := []string{"email", "phone", "ssn", "passport", "address", "credit_card", "password", "dob", "birth", "social_security"}

	for _, col := range table.Columns {
		colLower := strings.ToLower(col.Name)
		for _, keyword := range piiKeywords {
			if strings.Contains(colLower, keyword) {
				return true
			}
		}
	}

	return false
}
