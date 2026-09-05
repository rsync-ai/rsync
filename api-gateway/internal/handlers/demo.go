package handlers

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	"api-gateway/internal/db"
)

// The zero-credential try-it path.
//
// `sample-data` is the only auth_type "none" connector in the catalog, so it is
// the one source a brand-new install can connect to without first producing a
// credential. That alone does not get anyone to a completed sync: a pipeline
// needs two steps, and intent.go:417 returns "plan has insufficient steps" for
// len(steps) < 2. Every destination in the catalog wants a credential the new
// user does not have yet.
//
// So the quickstart ships a small Postgres alongside the stack and this endpoint
// wires both halves into the caller's workspace on request.
//
// Two properties this deliberately keeps:
//
//   - The DSN never reaches the browser. The frontend asks whether the demo is
//     available and asks for it to be seeded; the credential is read from the
//     api-gateway's own environment and goes straight into the encrypted config.
//   - No security gate is reimplemented here. Seeding replays the ordinary
//     CreateConnection handler, so RBAC (member+), workspace scoping, the
//     internal-connector block, config encryption and the pre-save connectivity
//     test all apply exactly as they do for a hand-made connection.
const (
	demoSourceName      = "Sample data (demo)"
	demoDestinationName = "Demo warehouse"

	demoSourceConnector      = "sample-data"
	demoDestinationConnector = "postgresql"
)

// demoDestination is the parsed RSYNC_DEMO_DESTINATION_DSN.
type demoDestination struct {
	Host     string
	Port     string
	Database string
	User     string
	Password string
}

// demoDestinationConfig parses RSYNC_DEMO_DESTINATION_DSN.
//
// Unset returns (nil, nil): the demo is simply unavailable. That is the CLOUD
// behavior and the default everywhere — app.rsync.ai ships no bundled warehouse,
// and only docker-compose.quickstart.yml sets the variable. Per the OSS/cloud
// split this is a feature flag defaulting to the cloud behavior, NOT an edition
// gate; nothing here reads EDITION/DEPLOYMENT_MODE/RSYNC_EDITION.
//
// A malformed value returns an error rather than a silent "unavailable", because
// a typo in the compose file would otherwise present as the feature quietly not
// existing — the failure mode this repo keeps getting bitten by.
func demoDestinationConfig() (*demoDestination, error) {
	raw := strings.TrimSpace(os.Getenv("RSYNC_DEMO_DESTINATION_DSN"))
	if raw == "" {
		return nil, nil
	}

	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("RSYNC_DEMO_DESTINATION_DSN is not a valid URL: %w", err)
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, fmt.Errorf("RSYNC_DEMO_DESTINATION_DSN must use the postgres:// scheme, got %q", u.Scheme)
	}

	host := u.Hostname()
	if host == "" {
		return nil, fmt.Errorf("RSYNC_DEMO_DESTINATION_DSN has no host")
	}
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	database := strings.TrimPrefix(u.Path, "/")
	if database == "" {
		return nil, fmt.Errorf("RSYNC_DEMO_DESTINATION_DSN has no database path")
	}
	if u.User == nil || u.User.Username() == "" {
		return nil, fmt.Errorf("RSYNC_DEMO_DESTINATION_DSN has no user")
	}
	password, _ := u.User.Password()

	return &demoDestination{
		Host:     host,
		Port:     port,
		Database: database,
		User:     u.User.Username(),
		Password: password,
	}, nil
}

// GetDemoStatus reports whether the bundled demo can be seeded.
//
// Deliberately says nothing about the credential — only that a destination
// exists and which connectors would be used.
func GetDemoStatus(c *gin.Context) {
	dest, err := demoDestinationConfig()
	if err != nil {
		log.WithError(err).WithField("trace_id", getTraceID(c)).
			Warn("demo destination is configured but unparseable")
		c.JSON(http.StatusOK, gin.H{
			"available": false,
			"reason":    "misconfigured",
		})
		return
	}
	if dest == nil {
		c.JSON(http.StatusOK, gin.H{
			"available": false,
			"reason":    "not_configured",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"available":             true,
		"source_connector":      demoSourceConnector,
		"source_name":           demoSourceName,
		"destination_connector": demoDestinationConnector,
		"destination_name":      demoDestinationName,
		"destination_database":  dest.Database,
	})
}

// SeedDemoConnections creates (or reuses) the demo source and destination in the
// caller's active workspace and returns both connection ids.
//
// Idempotent by name within the workspace: clicking twice reuses what is already
// there instead of stacking duplicates.
func SeedDemoConnections(c *gin.Context) {
	traceID := getTraceID(c)

	dest, err := demoDestinationConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":    "demo_misconfigured",
			"message":  "The bundled demo destination is configured but could not be parsed. Check RSYNC_DEMO_DESTINATION_DSN.",
			"trace_id": traceID,
		})
		return
	}
	if dest == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error":    "demo_unavailable",
			"message":  "This deployment does not ship a demo destination.",
			"trace_id": traceID,
		})
		return
	}

	// Resolve the workspace up front so the idempotency lookup and the replayed
	// creates agree on scope, and so we fail before any side effect.
	workspaceID, ok := resolveActiveWorkspace(c)
	if !ok {
		return
	}

	source, err := ensureDemoConnection(c, workspaceID, CreateConnectionRequest{
		Name:          demoSourceName,
		Type:          "source",
		ConnectorType: demoSourceConnector,
		SyncMode:      "batch",
		Description:   "Bundled in-memory customers/orders/products. No credentials required.",
		Config:        map[string]interface{}{},
	})
	if err != nil {
		respondDemoSeedFailure(c, traceID, "source", err)
		return
	}

	destination, err := ensureDemoConnection(c, workspaceID, CreateConnectionRequest{
		Name:          demoDestinationName,
		Type:          "destination",
		ConnectorType: demoDestinationConnector,
		Description:   "Postgres that ships with the quickstart stack, for trying a pipeline end to end.",
		Config: map[string]interface{}{
			"host":     dest.Host,
			"port":     dest.Port,
			"database": dest.Database,
			"user":     dest.User,
			"password": dest.Password,
		},
	})
	if err != nil {
		respondDemoSeedFailure(c, traceID, "destination", err)
		return
	}

	log.WithFields(log.Fields{
		"trace_id":       traceID,
		"workspace_id":   workspaceID,
		"source_id":      source,
		"destination_id": destination,
	}).Info("Seeded demo connections")

	c.JSON(http.StatusOK, gin.H{
		"source_connection_id":      source,
		"destination_connection_id": destination,
		"source_name":               demoSourceName,
		"destination_name":          demoDestinationName,
	})
}

// ensureDemoConnection returns the id of the workspace's demo connection with
// this name, creating it if absent.
func ensureDemoConnection(c *gin.Context, workspaceID string, payload CreateConnectionRequest) (string, error) {
	if database := db.GetDB(); database != nil {
		var existing string
		err := database.QueryRow(
			`SELECT id FROM connections WHERE workspace_id = $1 AND name = $2 LIMIT 1`,
			workspaceID, payload.Name,
		).Scan(&existing)
		switch {
		case err == nil:
			return existing, nil
		case err == sql.ErrNoRows:
			// fall through to create
		default:
			return "", fmt.Errorf("looking up existing demo connection: %w", err)
		}
	}

	status, body, err := replayCreateConnection(c, payload)
	if err != nil {
		return "", err
	}
	if status < 200 || status >= 300 {
		return "", demoSeedError{status: status, body: body}
	}

	id, _ := body["id"].(string)
	if id == "" {
		return "", fmt.Errorf("connection was created but returned no id")
	}
	return id, nil
}

// demoSeedError carries a failed CreateConnection response so the caller can
// forward the real reason (a connector that will not start, a destination that
// is not up yet) instead of flattening everything to "seeding failed".
type demoSeedError struct {
	status int
	body   map[string]interface{}
}

func (e demoSeedError) Error() string {
	if msg, ok := e.body["message"].(string); ok && msg != "" {
		return msg
	}
	if msg, ok := e.body["error"].(string); ok && msg != "" {
		return msg
	}
	return fmt.Sprintf("connection create returned HTTP %d", e.status)
}

func respondDemoSeedFailure(c *gin.Context, traceID, half string, err error) {
	status := http.StatusBadGateway
	var seedErr demoSeedError
	if ok := asDemoSeedError(err, &seedErr); ok {
		// Forward the authorization outcome verbatim: a viewer who cannot create
		// connections should see 403, not a misleading upstream error.
		if seedErr.status == http.StatusForbidden || seedErr.status == http.StatusUnauthorized {
			status = seedErr.status
		}
	}

	log.WithError(err).WithFields(log.Fields{
		"trace_id": traceID,
		"half":     half,
	}).Warn("Demo seeding failed")

	c.JSON(status, gin.H{
		"error":    "demo_seed_failed",
		"half":     half,
		"message":  err.Error(),
		"trace_id": traceID,
	})
}

func asDemoSeedError(err error, out *demoSeedError) bool {
	if e, ok := err.(demoSeedError); ok {
		*out = e
		return true
	}
	return false
}

// replayCreateConnection runs the ordinary CreateConnection handler against a
// synthesized body while carrying the caller's authenticated request and context
// keys forward unchanged.
//
// The point is that seeding is not a privileged side door: every check
// CreateConnection makes — RBAC, active-workspace resolution, the
// internal-connector refusal, config encryption, the pre-save connectivity test
// — runs here too, against the same caller. Extracting a shared "create without
// the gin layer" helper would mean editing the one production INSERT path for
// connections; replaying the handler leaves it untouched.
//
// Auth and CSRF middleware already ran on the outer request; only the handler is
// re-entered, never the middleware chain.
func replayCreateConnection(c *gin.Context, payload CreateConnectionRequest) (int, map[string]interface{}, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, fmt.Errorf("encoding demo connection payload: %w", err)
	}

	recorder := httptest.NewRecorder()
	sub, _ := gin.CreateTestContext(recorder)

	req := c.Request.Clone(c.Request.Context())
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req.Header.Set("Content-Type", "application/json")
	sub.Request = req

	for k, v := range c.Keys {
		sub.Set(k, v)
	}

	CreateConnection(sub)

	var out map[string]interface{}
	if recorder.Body.Len() > 0 {
		if err := json.Unmarshal(recorder.Body.Bytes(), &out); err != nil {
			return recorder.Code, nil, fmt.Errorf("connection create returned an unreadable response: %w", err)
		}
	}
	return recorder.Code, out, nil
}
