package handlers

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Phase 2 pre-flight agents — synchronous checks that run during
// CreatePipeline before any DB insert. Each agent has exactly three
// possible outcomes:
//
//   PASS              → continue to the next agent
//   HITL              → return 422 with a structured payload the UI
//                       can render directly (form, picker, etc.)
//   FAIL              → return 400/404 with an actionable error
//
// Why synchronous here (not async in the orchestrator):
//   - CapabilityAgent is a file-system lookup, ~microseconds.
//   - ConnectionAgent is a single indexed DB query.
//   - Failing fast prevents orphan pipeline rows for invalid configs.
//   - The async HITL infrastructure in the orchestrator (used by
//     SchemaAgent / CredentialAgent in later phases) is the right
//     tool when a check involves MCP network calls. These two don't.

// PreflightHITLResponse is the structured payload returned with HTTP
// 422 when an agent needs user input before the pipeline can be
// created. The UI matches on `agent` + `action_needed` to render the
// right form.
type PreflightHITLResponse struct {
	Error         string                 `json:"error"`
	Agent         string                 `json:"agent"`
	ActionNeeded  string                 `json:"action_needed"`
	Role          string                 `json:"role,omitempty"` // "source" | "destination"
	ConnectorType string                 `json:"connector_type,omitempty"`
	Message       string                 `json:"message"`
	Details       map[string]interface{} `json:"details,omitempty"`
}

// connectorsRoot is the on-disk directory the CapabilityAgent searches.
// Defaults to the canonical location; overridable via TOOLS_DIR so
// tests can point at a tmpdir.
func connectorsRoot() string {
	if v := strings.TrimSpace(os.Getenv("TOOLS_DIR")); v != "" {
		return v
	}
	return "/app/shared/mcp-connectors"
}

// connectorExistsOnDisk returns true when the given connector_type has
// a corresponding directory containing metadata.json. Looks in both
// the flat layout (public/<id>/) and the category-nested layout
// (public/<category>/<id>/). Matches the discovery logic used by
// scripts/mcp_generate_compose.py.
//
// Public for testing in the same package.
func connectorExistsOnDisk(root, connectorType string) bool {
	id := strings.ToLower(strings.TrimSpace(connectorType))
	if id == "" || root == "" {
		return false
	}
	// internal/ — bypasses public discovery.
	if hasMetadata(filepath.Join(root, "internal", id)) {
		return true
	}
	// public/ flat.
	if hasMetadata(filepath.Join(root, "public", id)) {
		return true
	}
	// public/<category>/<id>/
	pubRoot := filepath.Join(root, "public")
	entries, err := os.ReadDir(pubRoot)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if hasMetadata(filepath.Join(pubRoot, e.Name(), id)) {
			return true
		}
	}
	return false
}

func hasMetadata(dir string) bool {
	if dir == "" {
		return false
	}
	// Accept either metadata.json (old flat-root layout) or latest.json
	// (versioned layout introduced in PR #183/#184 — root copies removed,
	// single source of truth is versions/<current_version>/).
	for _, f := range []string{"metadata.json", "latest.json"} {
		info, err := os.Stat(filepath.Join(dir, f))
		if err == nil && !info.IsDir() {
			return true
		}
	}
	return false
}

// CapabilityAgent asks: does the user's chosen connector_type exist on
// disk and have the artifacts the orchestrator can deploy?
//
// Returns:
//   - (nil, "", true)     → connector exists, proceed
//   - (response, "", false) → HITL response; UI should prompt user to
//     generate the connector
type CapabilityAgent struct {
	Root string
}

func NewCapabilityAgent() *CapabilityAgent {
	return &CapabilityAgent{Root: connectorsRoot()}
}

// Check runs the agent for one connector_type with a role label
// ("source" or "destination") used in the HITL response.
func (a *CapabilityAgent) Check(connectorType, role string) *PreflightHITLResponse {
	if connectorType == "" {
		return &PreflightHITLResponse{
			Error:        "preflight_hitl_required",
			Agent:        "CapabilityAgent",
			ActionNeeded: "specify_connector",
			Role:         role,
			Message:      fmt.Sprintf("No %s connector specified. Pick one or generate from natural language.", role),
		}
	}
	if connectorExistsOnDisk(a.Root, connectorType) {
		return nil
	}
	return &PreflightHITLResponse{
		Error:         "preflight_hitl_required",
		Agent:         "CapabilityAgent",
		ActionNeeded:  "generate_connector",
		Role:          role,
		ConnectorType: connectorType,
		Message: fmt.Sprintf(
			"No %s connector named %q is installed. Generate it from natural "+
				"language at POST /v1/generate, or pick one of the existing connectors.",
			role, connectorType,
		),
		Details: map[string]interface{}{
			"hint": "POST /v1/generate {\"api_name\":\"" + connectorType + "\", ...}",
		},
	}
}

// ConnectionAgent asks: given a connector_type and a role, how many
// connections does this user have for it?
//
//	0 connections     → HITL create-connection prompt with the connector's
//	                    required_config so the UI renders a form
//	1 connection      → auto-resolve, return its ID
//	>1 connections    → HITL picker with all candidate connections
//
// The agent never modifies state — selecting/creating connections is
// the UI's job after the user responds to the HITL prompt.
type ConnectionAgent struct {
	DB *sql.DB
}

func NewConnectionAgent(db *sql.DB) *ConnectionAgent {
	return &ConnectionAgent{DB: db}
}

// ResolvedConnection captures the outcome when exactly one connection
// matched and the agent auto-resolved.
type ResolvedConnection struct {
	ID            string
	Name          string
	ConnectorType string
}

// Check returns either:
//   - resolved   != nil, hitl == nil → auto-resolved single connection
//   - resolved == nil, hitl != nil   → user input needed
//   - both nil and err != nil        → unexpected DB error
func (a *ConnectionAgent) Check(workspaceID, connectorType, role string) (*ResolvedConnection, *PreflightHITLResponse, error) {
	if a.DB == nil {
		return nil, nil, errors.New("ConnectionAgent: DB not configured")
	}
	if connectorType == "" {
		return nil, nil, errors.New("ConnectionAgent: connectorType required")
	}
	// "connection_type" column is the role ("source"|"destination"); we
	// filter on both connector_type and role so a source-only connector
	// of the same name doesn't get auto-resolved as a destination.
	// Scoped to the active workspace (P2f): connections are workspace-shared, so
	// a member auto-resolves a teammate's connection of the requested type.
	rows, err := a.DB.Query(`
		SELECT id, name, connector_type
		FROM connections
		WHERE workspace_id = $1
		  AND lower(connector_type) = lower($2)
		  AND lower(type) = lower($3)
		  AND COALESCE(status, 'active') != 'deleted'
		ORDER BY created_at DESC
	`, workspaceID, connectorType, role)
	if err != nil {
		return nil, nil, fmt.Errorf("ConnectionAgent: list connections: %w", err)
	}
	defer rows.Close()

	type candidate struct {
		ID, Name, Type string
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.ID, &c.Name, &c.Type); err != nil {
			return nil, nil, fmt.Errorf("ConnectionAgent: scan: %w", err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("ConnectionAgent: rows: %w", err)
	}

	switch len(candidates) {
	case 0:
		return nil, &PreflightHITLResponse{
			Error:         "preflight_hitl_required",
			Agent:         "ConnectionAgent",
			ActionNeeded:  "create_connection",
			Role:          role,
			ConnectorType: connectorType,
			Message: fmt.Sprintf(
				"No %s connection exists for %q. Create one before running this pipeline.",
				role, connectorType,
			),
			Details: map[string]interface{}{
				"create_endpoint": "POST /api/v1/connections",
			},
		}, nil
	case 1:
		return &ResolvedConnection{ID: candidates[0].ID, Name: candidates[0].Name, ConnectorType: candidates[0].Type}, nil, nil
	default:
		options := make([]map[string]string, 0, len(candidates))
		for _, c := range candidates {
			options = append(options, map[string]string{"id": c.ID, "name": c.Name})
		}
		return nil, &PreflightHITLResponse{
			Error:         "preflight_hitl_required",
			Agent:         "ConnectionAgent",
			ActionNeeded:  "pick_connection",
			Role:          role,
			ConnectorType: connectorType,
			Message: fmt.Sprintf(
				"Multiple %s connections found for %q. Pick one to use for this pipeline.",
				role, connectorType,
			),
			Details: map[string]interface{}{
				"options": options,
			},
		}, nil
	}
}

// runPipelinePreflight orchestrates the Phase 2 agents in fixed order:
//
//  1. ConnectionAgent (source)      — only when source_connector_type
//     was given without an ID
//  2. ConnectionAgent (destination)
//  3. CapabilityAgent (source)      — verify the connector_type for
//     whatever source we ended up
//     with is installed on disk
//  4. CapabilityAgent (destination)
//
// Mutates *sourceConnectionID / *destinationConnectionID in place when
// ConnectionAgent auto-resolves a single match — the caller's INSERT
// then uses the resolved IDs.
//
// Returns:
//
//	(nil, nil)         → all gates passed, proceed
//	(hitl, nil)        → caller writes 422 with payload
//	(nil, err)         → caller writes 500
func runPipelinePreflight(
	db *sql.DB,
	workspaceID string,
	req *CreatePipelineRequest,
	sourceConnectionID *string,
	destinationConnectionID *string,
) (*PreflightHITLResponse, error) {
	// Step 1: resolve source via ConnectionAgent when ID is empty but
	// connector_type was provided.
	if strings.TrimSpace(*sourceConnectionID) == "" && strings.TrimSpace(req.SourceConnectorType) != "" {
		agent := NewConnectionAgent(db)
		resolved, hitl, err := agent.Check(workspaceID, req.SourceConnectorType, "source")
		if err != nil {
			return nil, fmt.Errorf("source ConnectionAgent: %w", err)
		}
		if hitl != nil {
			return hitl, nil
		}
		*sourceConnectionID = resolved.ID
	}

	// Step 2: same for destination.
	if strings.TrimSpace(*destinationConnectionID) == "" && strings.TrimSpace(req.DestinationConnectorType) != "" {
		agent := NewConnectionAgent(db)
		resolved, hitl, err := agent.Check(workspaceID, req.DestinationConnectorType, "destination")
		if err != nil {
			return nil, fmt.Errorf("destination ConnectionAgent: %w", err)
		}
		if hitl != nil {
			return hitl, nil
		}
		*destinationConnectionID = resolved.ID
	}

	// Step 3: CapabilityAgent (source). Read connector_type from the
	// resolved connection record so we check what the pipeline will
	// actually use, not just what the user typed.
	capability := NewCapabilityAgent()
	if strings.TrimSpace(*sourceConnectionID) != "" {
		ctype, err := LookupConnectorTypeByConnectionID(db, workspaceID, *sourceConnectionID)
		if err != nil {
			return nil, fmt.Errorf("lookup source connector_type: %w", err)
		}
		if hitl := capability.Check(ctype, "source"); hitl != nil {
			return hitl, nil
		}
	}

	// Step 4: CapabilityAgent (destination).
	if strings.TrimSpace(*destinationConnectionID) != "" {
		ctype, err := LookupConnectorTypeByConnectionID(db, workspaceID, *destinationConnectionID)
		if err != nil {
			return nil, fmt.Errorf("lookup destination connector_type: %w", err)
		}
		if hitl := capability.Check(ctype, "destination"); hitl != nil {
			return hitl, nil
		}
	}

	return nil, nil
}

// LookupConnectorTypeByConnectionID fetches the connector_type for a
// resolved connection ID. Used by CapabilityAgent when the caller
// already provided a connection_id and we want to verify its
// connector_type is still installed on disk (e.g. someone manually
// deleted the connector directory after the connection was created).
func LookupConnectorTypeByConnectionID(db *sql.DB, workspaceID, connectionID string) (string, error) {
	if db == nil {
		return "", errors.New("DB not configured")
	}
	var connectorType string
	err := db.QueryRow(
		`SELECT connector_type FROM connections WHERE id = $1 AND workspace_id = $2`,
		connectionID, workspaceID,
	).Scan(&connectorType)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("connection %s not found", connectionID)
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(connectorType), nil
}
