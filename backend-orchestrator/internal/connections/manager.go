package connections

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/shared/crypto"
	"github.com/rsync-ai/backend-orchestrator/internal/oauth"
)

// Connection represents a connection entity
type Connection struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Type          string            `json:"type"`
	ConnectorType string            `json:"connector_type"`
	Config        map[string]string `json:"config"` // Decrypted
	Status        string            `json:"status"`
	OAuthTokenID  *string           `json:"oauth_token_id,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
}

// Manager handles connection storage and retrieval
type Manager struct {
	db            *sql.DB
	tokenManager  *oauth.TokenManager
	refreshClient *oauth.RefreshClient
}

// NewManager creates a new connection manager
func NewManager(db *sql.DB) *Manager {
	tokenManager := oauth.NewTokenManager(db)
	
	// Get API Gateway URL from env (for OAuth refresh)
	apiGatewayURL := strings.TrimSpace(os.Getenv("API_GATEWAY_URL"))
	if apiGatewayURL == "" {
		// In docker-compose, api-gateway listens on 8080 internally; 5001 is host-mapped.
		apiGatewayURL = "http://api-gateway:8080"
	}
	
	refreshClient := oauth.NewRefreshClient(apiGatewayURL, tokenManager)

	// Wire async refresh: when TokenManager sees a near-expiry token it calls back to
	// api-gateway via RefreshClient, keeping the cache warm for the next pipeline run.
	tokenManager.SetRefreshFunc(refreshClient.RefreshTokenViaAPIGateway)

	return &Manager{
		db:            db,
		tokenManager:  tokenManager,
		refreshClient: refreshClient,
	}
}

// GetRefreshClient returns the OAuth refresh client (for executor)
func (m *Manager) GetRefreshClient() *oauth.RefreshClient {
	return m.refreshClient
}

// GetTokenManager returns the OAuth token manager (for executor)
func (m *Manager) GetTokenManager() *oauth.TokenManager {
	return m.tokenManager
}

// GetForUser retrieves a connection by ID, gated on user_id ownership,
// and returns decrypted config.
//
// This is the canonical tenant-aware hydration path. Every consumer that
// has a known pipeline owner (executor task, scheduled-run activity,
// healer-with-pipeline-context) should call this instead of Get(...).
//
// ARCH-3 root cause: Get(ctx, connID) was tenant-blind — same primitive
// for cross-tenant credential reads in cdc/mysql.go, cdc_sink.go, and
// the Temporal activity. Forcing callers to commit a userID closes the
// class of "any connection ID hydrates any tenant's config" bugs.
//
// Pass userID="" only from contexts where no user is available (e.g.
// system-internal reconciliation). The empty-string path emits a
// deprecation warning so we can find every remaining tenant-blind
// caller in logs.
func (m *Manager) GetForUser(ctx context.Context, userID, connectionID string) (map[string]string, error) {
	if strings.TrimSpace(userID) == "" {
		log.Warnf("⚠️  DEPRECATED: Manager.GetForUser called with empty userID for connection %s — falling back to tenant-blind hydration. Tag site with the pipeline owner.", connectionID)
		return m.Get(ctx, connectionID)
	}
	return m.getInternal(ctx, userID, connectionID)
}

// Get retrieves a connection by ID without tenant scoping.
//
// DEPRECATED: prefer GetForUser(ctx, userID, connectionID). Get is kept
// for legacy call sites where no userID is available; new code should
// always pass the pipeline / request owner. Each call emits a debug log
// so the migration progress is observable.
func (m *Manager) Get(ctx context.Context, connectionID string) (map[string]string, error) {
	return m.getInternal(ctx, "", connectionID)
}

// getInternal is the shared implementation. When userID is non-empty,
// the SQL filter includes `AND user_id = $2`. When empty (legacy /
// system path), the filter is omitted.
func (m *Manager) getInternal(ctx context.Context, userID, connectionID string) (map[string]string, error) {
	log.Debugf("📥 Fetching connection: %s (tenant=%q)", connectionID, userID)

	var encryptedConfig string
	var connType string
	var connectorType string
	var connectorVersion string
	var oauthTokenID string

	// NOTE: connections table does NOT have oauth_token_id column in this repo schema.
	// OAuth token linkage is stored inside encrypted config as "oauth_token_id".
	var err error
	if userID != "" {
		query := `SELECT type, connector_type, connector_version, config FROM connections WHERE id = $1 AND user_id = $2`
		err = m.db.QueryRowContext(ctx, query, connectionID, userID).Scan(&connType, &connectorType, &connectorVersion, &encryptedConfig)
	} else {
		query := `SELECT type, connector_type, connector_version, config FROM connections WHERE id = $1`
		err = m.db.QueryRowContext(ctx, query, connectionID).Scan(&connType, &connectorType, &connectorVersion, &encryptedConfig)
	}

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("connection not found: %s", connectionID)
	}
	if err != nil {
		return nil, fmt.Errorf("database query failed: %w", err)
	}

	// Decrypt config
	log.Debugf("🔓 Decrypting config for connection: %s", connectionID)
	decryptedJSON, err := crypto.DecryptString(encryptedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt config: %w", err)
	}

	// Parse JSON to map (robustly handling non-string values)
	var rawConfig map[string]interface{}
	if err := json.Unmarshal([]byte(decryptedJSON), &rawConfig); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON: %w", err)
	}

	config := make(map[string]string)
	for k, v := range rawConfig {
		config[k] = fmt.Sprintf("%v", v)
	}

	// Hydrate OAuth token if present (stored inside encrypted config)
	oauthTokenID = strings.TrimSpace(config["oauth_token_id"])
	if oauthTokenID != "" {
		log.Debugf("🔐 Hydrating OAuth token for connection: %s (token_id: %s)", connectionID, oauthTokenID)
		accessToken, err := m.tokenManager.GetAccessToken(oauthTokenID)
		if err != nil {
			// If token is expired/expiring, proactively refresh via API Gateway so connector calls don't fail
			// with opaque "Operation failed" errors (some connectors don't surface 401 clearly).
			errStr := strings.ToLower(err.Error())
			if strings.Contains(errStr, "expired") || strings.Contains(errStr, "expiring soon") {
				log.Warnf("⚠️  OAuth token for connection %s is expired/expiring; attempting refresh (token_id=%s)", connectionID, oauthTokenID)
				if refreshErr := m.refreshClient.RefreshTokenViaAPIGateway(ctx, oauthTokenID); refreshErr != nil {
					// Log warning but don't fail - executor may still attempt refresh-on-401
					log.Warnf("⚠️  Failed to refresh OAuth token for connection %s (token_id=%s): %v", connectionID, oauthTokenID, refreshErr)
					config["oauth_token_id"] = oauthTokenID
				} else {
					// Refresh succeeded - fetch the new token and inject it
					newAccessToken, fetchErr := m.tokenManager.GetAccessToken(oauthTokenID)
					if fetchErr != nil {
						log.Warnf("⚠️  OAuth token refreshed but failed to fetch hydrated token for connection %s (token_id=%s): %v", connectionID, oauthTokenID, fetchErr)
						config["oauth_token_id"] = oauthTokenID
					} else {
						config["access_token"] = newAccessToken
						config["oauth_token_id"] = oauthTokenID
						log.Debugf("✅ OAuth token refreshed + hydrated for connection: %s", connectionID)
					}
				}
			} else {
				// Log warning but don't fail - the executor can attempt refresh on 401
				log.Warnf("⚠️  Failed to hydrate OAuth token for connection %s: %v", connectionID, err)
				// Keep oauth_token_id so executor knows this is an OAuth connection
				config["oauth_token_id"] = oauthTokenID
			}
		} else {
			// Inject access token into config (connectors expect this)
			config["access_token"] = accessToken
			config["oauth_token_id"] = oauthTokenID
			log.Debugf("✅ OAuth token hydrated for connection: %s", connectionID)
		}
	}

	// Include connection metadata for downstream agents (executor direct transfer, etc.)
	// IMPORTANT: Keep keys stable and lowercase.
	config["type"] = strings.ToLower(connectorType)           // legacy key used by some agents
	config["connector_type"] = strings.ToLower(connectorType) // explicit
	config["connection_type"] = strings.ToLower(connType)     // source|destination
	if strings.TrimSpace(connectorVersion) != "" {
		config["connector_version"] = strings.TrimSpace(connectorVersion) // "latest" or "vX.Y.Z"
	}

	// #region agent log
	// Debug/compat: inside docker, "localhost" means the container itself. For mysql connections
	// created from the host UI, users often set host=localhost expecting the docker mysql service.
	// Rewrite only for mysql connections and only for localhost-like values.
	if strings.EqualFold(connectorType, "mysql") {
		host := strings.TrimSpace(config["host"])
		if host == "" || host == "localhost" || host == "127.0.0.1" {
			dockerHost := strings.TrimSpace(os.Getenv("RSYNC_MYSQL_DOCKER_HOST"))
			if dockerHost == "" {
				dockerHost = "mysql"
			}
			log.Warnf("⚠️  Rewriting mysql host '%s' → '%s' for docker network (connection=%s)", host, dockerHost, connectionID)
			config["host"] = dockerHost
			// If port missing, default to 3306 (safe for mysql).
			if strings.TrimSpace(config["port"]) == "" {
				config["port"] = "3306"
			}
		}
	}
	// #endregion

	log.Debugf("✅ Retrieved config for %s connection with %d keys", connType, len(config))
	return config, nil
}

// GetByName retrieves a connection by name
func (m *Manager) GetByName(ctx context.Context, name string) (map[string]string, error) {
	log.Debugf("📥 Fetching connection by name: %s", name)

	var encryptedConfig string
	var connType string
	var connectorType string
	var connectorVersion string

	query := `SELECT type, connector_type, connector_version, config FROM connections WHERE name = $1`
	err := m.db.QueryRowContext(ctx, query, name).Scan(&connType, &connectorType, &connectorVersion, &encryptedConfig)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("connection not found: %s", name)
	}
	if err != nil {
		return nil, fmt.Errorf("database query failed: %w", err)
	}

	// Decrypt config
	decryptedJSON, err := crypto.DecryptString(encryptedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt config: %w", err)
	}

	// Parse JSON to map (robustly handling non-string values)
	var rawConfig map[string]interface{}
	if err := json.Unmarshal([]byte(decryptedJSON), &rawConfig); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON: %w", err)
	}

	config := make(map[string]string)
	for k, v := range rawConfig {
		config[k] = fmt.Sprintf("%v", v)
	}

	// Include connection metadata for downstream agents (executor direct transfer, etc.)
	config["type"] = strings.ToLower(connectorType)
	config["connector_type"] = strings.ToLower(connectorType)
	config["connection_type"] = strings.ToLower(connType)
	if strings.TrimSpace(connectorVersion) != "" {
		config["connector_version"] = strings.TrimSpace(connectorVersion)
	}

	// #region agent log
	if strings.EqualFold(connectorType, "mysql") {
		host := strings.TrimSpace(config["host"])
		if host == "" || host == "localhost" || host == "127.0.0.1" {
			dockerHost := strings.TrimSpace(os.Getenv("RSYNC_MYSQL_DOCKER_HOST"))
			if dockerHost == "" {
				dockerHost = "mysql"
			}
			log.Warnf("⚠️  Rewriting mysql host '%s' → '%s' for docker network (connection_name=%s)", host, dockerHost, name)
			config["host"] = dockerHost
			if strings.TrimSpace(config["port"]) == "" {
				config["port"] = "3306"
			}
		}
	}
	// #endregion

	log.Debugf("✅ Retrieved config for %s connection (%s) with %d keys", name, connType, len(config))
	return config, nil
}

// List retrieves all connections of a specific type
func (m *Manager) List(ctx context.Context, connectorType string) ([]Connection, error) {
	log.Debugf("📋 Listing connections of type: %s", connectorType)

	var query string
	var rows *sql.Rows
	var err error

	if connectorType == "" {
		query = `SELECT id, name, type, connector_type, config, status, created_at, updated_at FROM connections ORDER BY name`
		rows, err = m.db.QueryContext(ctx, query)
	} else {
		query = `SELECT id, name, type, connector_type, config, status, created_at, updated_at FROM connections WHERE type = $1 ORDER BY name`
		rows, err = m.db.QueryContext(ctx, query, connectorType)
	}

	if err != nil {
		return nil, fmt.Errorf("database query failed: %w", err)
	}
	defer rows.Close()

	var connections []Connection
	for rows.Next() {
		var conn Connection
		var encryptedConfig string

		err := rows.Scan(
			&conn.ID, &conn.Name, &conn.Type, &conn.ConnectorType,
			&encryptedConfig, &conn.Status, &conn.CreatedAt, &conn.UpdatedAt,
		)
		if err != nil {
			log.Warnf("⚠️  Failed to scan connection row: %v", err)
			continue
		}

		// Decrypt config
		decryptedJSON, err := crypto.DecryptString(encryptedConfig)
		if err != nil {
			log.Warnf("⚠️  Failed to decrypt config for %s: %v", conn.Name, err)
			continue
		}

		// Parse JSON (robustly handling non-string values)
		var rawConfig map[string]interface{}
		if err := json.Unmarshal([]byte(decryptedJSON), &rawConfig); err != nil {
			log.Warnf("⚠️  Failed to parse config for %s: %v", conn.Name, err)
			continue
		}

		config := make(map[string]string)
		for k, v := range rawConfig {
			config[k] = fmt.Sprintf("%v", v)
		}

		conn.Config = config
		connections = append(connections, conn)
	}

	log.Infof("✅ Retrieved %d connections of type '%s'", len(connections), connectorType)
	return connections, nil
}

// GetConnectionID retrieves connection ID by name and type
func (m *Manager) GetConnectionID(ctx context.Context, name, connType string) (string, error) {
	var id string
	query := `SELECT id FROM connections WHERE name = $1 AND type = $2 LIMIT 1`
	err := m.db.QueryRowContext(ctx, query, name, connType).Scan(&id)

	if err == sql.ErrNoRows {
		return "", fmt.Errorf("connection not found: name=%s, type=%s", name, connType)
	}
	if err != nil {
		return "", fmt.Errorf("database query failed: %w", err)
	}

	return id, nil
}

// ValidateConnection checks if a connection exists and returns its type
func (m *Manager) ValidateConnection(ctx context.Context, connectionID string) (string, error) {
	var connType string
	query := `SELECT type FROM connections WHERE id = $1`
	err := m.db.QueryRowContext(ctx, query, connectionID).Scan(&connType)

	if err == sql.ErrNoRows {
		return "", fmt.Errorf("connection not found: %s", connectionID)
	}
	if err != nil {
		return "", fmt.Errorf("database query failed: %w", err)
	}

	return connType, nil
}
