package intent

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/registry"
)

// WorkspaceContextResolver resolves workspace references (production, staging, etc)
// to concrete connection IDs before connector validation.
//
// MUST run BEFORE connector validation to prevent wrong-table selection.
type WorkspaceContextResolver struct {
	db                *sql.DB
	connectorRegistry ConnectorRegistry
}

// ConnectorRegistry is the minimal interface WorkspaceContextResolver needs.
// It is intentionally an interface to keep the resolver testable and decoupled.
type ConnectorRegistry interface {
	GetCapabilities(connectorType string) *registry.ConnectorCapabilities
}

// NewWorkspaceContextResolver creates a new resolver
func NewWorkspaceContextResolver(db *sql.DB, reg ConnectorRegistry) *WorkspaceContextResolver {
	return &WorkspaceContextResolver{
		db:                db,
		connectorRegistry: reg,
	}
}

// isConnectorType checks if the registry has this connector and returns
// its canonical connector_type. The canonical name may differ from the
// input when the registry resolved an alias (e.g. "shopify" →
// "shopify-admin-graphql" via vendor-prefix matching in GetCapabilities).
//
// Callers MUST use the returned canonical name as the connector_type
// downstream, not the original reference. Otherwise the connection
// lookup queries `WHERE connector_type='shopify'` and miss the user's
// real `shopify-admin-graphql` connections, which is the regression
// this method fixes.
func (r *WorkspaceContextResolver) isConnectorType(connectorType string) (string, bool) {
	if r.connectorRegistry == nil {
		return "", false
	}
	caps := r.connectorRegistry.GetCapabilities(connectorType)
	if caps == nil {
		return "", false
	}
	return caps.ConnectorType, true
}

// RawIntent is the output from deterministic/LLM parsing
type RawIntent struct {
	Source      string
	Destination string
	Tables      []string
	Filters     map[string]interface{}
	Confidence  float64
	Method      string
}

// ResolvedIntent is the output after workspace resolution
type ResolvedIntent struct {
	// Authoritative - cannot be changed later
	SourceConnectorType      string
	SourceConnectionID       *string // nil if not resolved to a specific connection
	DestinationConnectorType string
	DestinationConnectionID  *string // nil if not resolved to a specific connection

	// Carried forward
	Tables           []string
	Filters          map[string]interface{}
	Confidence       float64
	ResolutionMethod string
}

// ResolutionError represents various resolution failure modes
type ResolutionError struct {
	Type        string // "not_found" | "ambiguous"
	Reference   string // The original reference that failed
	Context     string // "source" | "destination"
	Message     string
	Suggestions []ConnectionSuggestion // For "not_found"
	Matches     []ConnectionMatch      // For "ambiguous"
}

func (e *ResolutionError) Error() string {
	return e.Message
}

// ConnectionSuggestion is a suggested connection for HITL
type ConnectionSuggestion struct {
	ID            string
	Name          string
	ConnectorType string
	Environment   *string
}

// ConnectionMatch represents an ambiguous match
type ConnectionMatch struct {
	ID            string
	Name          string
	ConnectorType string
	Environment   *string
	Alias         *string
}

// Resolve converts raw intent to resolved intent with connection IDs
func (r *WorkspaceContextResolver) Resolve(
	ctx context.Context,
	raw RawIntent,
	workspaceID string,
) (*ResolvedIntent, error) {
	// Resolve source
	sourceConnector, sourceConnectionID, err := r.resolveReference(
		ctx,
		raw.Source,
		workspaceID,
		"source",
	)
	if err != nil {
		return nil, err
	}

	// Resolve destination
	destConnector, destConnectionID, err := r.resolveReference(
		ctx,
		raw.Destination,
		workspaceID,
		"destination",
	)
	if err != nil {
		return nil, err
	}

	return &ResolvedIntent{
		SourceConnectorType:      sourceConnector,
		SourceConnectionID:       sourceConnectionID,
		DestinationConnectorType: destConnector,
		DestinationConnectionID:  destConnectionID,
		Tables:                   raw.Tables,
		Filters:                  raw.Filters,
		Confidence:               raw.Confidence,
		ResolutionMethod:         raw.Method,
	}, nil
}

// resolveReference resolves a single reference to (connector_type, connection_id)
func (r *WorkspaceContextResolver) resolveReference(
	ctx context.Context,
	reference string,
	workspaceID string,
	context string, // "source" or "destination"
) (string, *string, error) {
	// Check if this is a valid connector type (alias-resolved against
	// the registry — "shopify" → "shopify-admin-graphql"). Return the
	// canonical name so downstream lookups query the right
	// connector_type, not the user-friendly alias.
	if canonical, ok := r.isConnectorType(reference); ok {
		return canonical, nil, nil
	}

	// Not a connector type - must be workspace reference
	// Try to resolve to a connection

	// Strategy 1: Exact name match
	var matches []ConnectionMatch

	rows, err := r.db.QueryContext(ctx,
		`SELECT id, name, connector_type, environment, alias 
		 FROM connections 
		 WHERE workspace_id = $1 AND name = $2`,
		workspaceID, reference,
	)
	if err != nil {
		log.WithError(err).Error("Failed to query connections by name")
		return "", nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var match ConnectionMatch
		var env, alias sql.NullString
		err := rows.Scan(&match.ID, &match.Name, &match.ConnectorType, &env, &alias)
		if err != nil {
			log.WithError(err).Error("Failed to scan connection row")
			continue
		}
		if env.Valid {
			match.Environment = &env.String
		}
		if alias.Valid {
			match.Alias = &alias.String
		}
		matches = append(matches, match)
	}

	if len(matches) == 1 {
		return matches[0].ConnectorType, &matches[0].ID, nil
	}
	if len(matches) > 1 {
		return "", nil, &ResolutionError{
			Type:      "ambiguous",
			Reference: reference,
			Context:   context,
			Message:   fmt.Sprintf("'%s' matches %d connections", reference, len(matches)),
			Matches:   matches,
		}
	}

	// Strategy 2: Alias match
	rows, err = r.db.QueryContext(ctx,
		`SELECT id, name, connector_type, environment, alias 
		 FROM connections 
		 WHERE workspace_id = $1 AND alias = $2`,
		workspaceID, reference,
	)
	if err != nil {
		log.WithError(err).Error("Failed to query connections by alias")
		return "", nil, err
	}
	defer rows.Close()

	matches = nil
	for rows.Next() {
		var match ConnectionMatch
		var env, alias sql.NullString
		err := rows.Scan(&match.ID, &match.Name, &match.ConnectorType, &env, &alias)
		if err != nil {
			log.WithError(err).Error("Failed to scan connection row")
			continue
		}
		if env.Valid {
			match.Environment = &env.String
		}
		if alias.Valid {
			match.Alias = &alias.String
		}
		matches = append(matches, match)
	}

	if len(matches) == 1 {
		return matches[0].ConnectorType, &matches[0].ID, nil
	}
	if len(matches) > 1 {
		return "", nil, &ResolutionError{
			Type:      "ambiguous",
			Reference: reference,
			Context:   context,
			Message:   fmt.Sprintf("'%s' matches %d connections by alias", reference, len(matches)),
			Matches:   matches,
		}
	}

	// Strategy 3: Environment match
	rows, err = r.db.QueryContext(ctx,
		`SELECT id, name, connector_type, environment, alias 
		 FROM connections 
		 WHERE workspace_id = $1 AND environment = $2`,
		workspaceID, reference,
	)
	if err != nil {
		log.WithError(err).Error("Failed to query connections by environment")
		return "", nil, err
	}
	defer rows.Close()

	matches = nil
	for rows.Next() {
		var match ConnectionMatch
		var env, alias sql.NullString
		err := rows.Scan(&match.ID, &match.Name, &match.ConnectorType, &env, &alias)
		if err != nil {
			log.WithError(err).Error("Failed to scan connection row")
			continue
		}
		if env.Valid {
			match.Environment = &env.String
		}
		if alias.Valid {
			match.Alias = &alias.String
		}
		matches = append(matches, match)
	}

	if len(matches) == 1 {
		return matches[0].ConnectorType, &matches[0].ID, nil
	}
	if len(matches) > 1 {
		return "", nil, &ResolutionError{
			Type:      "ambiguous",
			Reference: reference,
			Context:   context,
			Message:   fmt.Sprintf("'%s' matches %d connections in environment", reference, len(matches)),
			Matches:   matches,
		}
	}

	// Strategy 4: Substring match (fuzzy) using trigram indexes
	rows, err = r.db.QueryContext(ctx,
		`SELECT id, name, connector_type, environment, alias 
		 FROM connections 
		 WHERE workspace_id = $1 
		   AND (name ILIKE '%' || $2 || '%' OR description ILIKE '%' || $2 || '%')
		 LIMIT 10`,
		workspaceID, reference,
	)
	if err != nil {
		log.WithError(err).Error("Failed to query connections by substring")
		return "", nil, err
	}
	defer rows.Close()

	matches = nil
	for rows.Next() {
		var match ConnectionMatch
		var env, alias sql.NullString
		err := rows.Scan(&match.ID, &match.Name, &match.ConnectorType, &env, &alias)
		if err != nil {
			log.WithError(err).Error("Failed to scan connection row")
			continue
		}
		if env.Valid {
			match.Environment = &env.String
		}
		if alias.Valid {
			match.Alias = &alias.String
		}
		matches = append(matches, match)
	}

	if len(matches) == 1 {
		return matches[0].ConnectorType, &matches[0].ID, nil
	}
	if len(matches) > 1 {
		return "", nil, &ResolutionError{
			Type:      "ambiguous",
			Reference: reference,
			Context:   context,
			Message:   fmt.Sprintf("'%s' matches %d connections by substring", reference, len(matches)),
			Matches:   matches,
		}
	}

	// Strategy 5: Tag match (if tags column exists and is populated)
	rows, err = r.db.QueryContext(ctx,
		`SELECT id, name, connector_type, environment, alias 
		 FROM connections 
		 WHERE workspace_id = $1 AND $2 = ANY(tags)`,
		workspaceID, strings.ToLower(reference),
	)
	if err != nil {
		// Ignore error if tags column doesn't exist yet (backward compat)
		log.WithError(err).Debug("Failed to query connections by tags (column may not exist yet)")
	} else {
		defer rows.Close()

		matches = nil
		for rows.Next() {
			var match ConnectionMatch
			var env, alias sql.NullString
			err := rows.Scan(&match.ID, &match.Name, &match.ConnectorType, &env, &alias)
			if err != nil {
				log.WithError(err).Error("Failed to scan connection row")
				continue
			}
			if env.Valid {
				match.Environment = &env.String
			}
			if alias.Valid {
				match.Alias = &alias.String
			}
			matches = append(matches, match)
		}

		if len(matches) == 1 {
			return matches[0].ConnectorType, &matches[0].ID, nil
		}
		if len(matches) > 1 {
			return "", nil, &ResolutionError{
				Type:      "ambiguous",
				Reference: reference,
				Context:   context,
				Message:   fmt.Sprintf("'%s' matches %d connections by tag", reference, len(matches)),
				Matches:   matches,
			}
		}
	}

	// Not found anywhere - get suggestions
	suggestions, err := r.getSuggestions(ctx, workspaceID, context)
	if err != nil {
		log.WithError(err).Warn("Failed to get connection suggestions")
		suggestions = nil
	}

	return "", nil, &ResolutionError{
		Type:        "not_found",
		Reference:   reference,
		Context:     context,
		Message:     fmt.Sprintf("No connection found for '%s'", reference),
		Suggestions: suggestions,
	}
}

// getSuggestions returns connection suggestions for HITL
func (r *WorkspaceContextResolver) getSuggestions(
	ctx context.Context,
	workspaceID string,
	context string,
) ([]ConnectionSuggestion, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, name, connector_type, environment 
		 FROM connections 
		 WHERE workspace_id = $1 
		 ORDER BY created_at DESC 
		 LIMIT 10`,
		workspaceID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var suggestions []ConnectionSuggestion
	for rows.Next() {
		var s ConnectionSuggestion
		var env sql.NullString
		err := rows.Scan(&s.ID, &s.Name, &s.ConnectorType, &env)
		if err != nil {
			log.WithError(err).Error("Failed to scan suggestion row")
			continue
		}
		if env.Valid {
			s.Environment = &env.String
		}
		suggestions = append(suggestions, s)
	}

	return suggestions, nil
}
