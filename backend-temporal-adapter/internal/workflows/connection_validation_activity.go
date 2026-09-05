package workflows

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
)

// ErrAmbiguousConnectionMatch indicates multiple active connections match a requested connector/direction.
// We treat this as "requires user selection" instead of silently auto-picking the newest record.
var ErrAmbiguousConnectionMatch = fmt.Errorf("ambiguous connection match")

// IntentSuggestion is an optional UX hint for HITL flows.
// It is designed to be additive/backward-compatible: UIs that don't understand it can ignore it.
type IntentSuggestion struct {
	SourceType      string  `json:"source_connector_type"`
	DestinationType string  `json:"destination_connector_type"`
	Confidence      float64 `json:"confidence,omitempty"`
	Reasoning       string  `json:"reasoning,omitempty"`
}

// ConnectionValidationActivityV2 validates that source/destination connections exist and are active.
// It will:
// - Prefer explicit connection IDs provided in the request
// - Otherwise attempt to find the most recent active connection by (user_id, connector_type, direction)
func ConnectionValidationActivityV2(ctx context.Context, req ConnectionValidationRequest) (*ConnectionValidationResult, error) {
	logger := activity.GetLogger(ctx)

	result := &ConnectionValidationResult{
		CorrelationID:      req.CorrelationID,
		SourceExists:       false,
		DestExists:         false,
		MissingConnections: []MissingConnection{},
		Suggestions:        []IntentSuggestion{},
	}

	db := getDBFromActivityContext()
	if db == nil {
		// Best-effort fallback: only validate presence of IDs.
		if strings.TrimSpace(req.SourceConnectionID) != "" {
			result.SourceExists = true
			result.SourceConnectionID = req.SourceConnectionID
		}
		if strings.TrimSpace(req.DestinationConnectionID) != "" {
			result.DestExists = true
			result.DestinationConnectionID = req.DestinationConnectionID
		}
		result.AllConnectionsValid = result.SourceExists && result.DestExists
		if !result.SourceExists {
			result.MissingConnections = append(result.MissingConnections, MissingConnection{
				Type:           req.SourceType,
				Direction:      "source",
				RequiredFields: getRequiredFields(req.SourceType),
				Message:        fmt.Sprintf("Source %s connection not configured", req.SourceType),
			})
		}
		if !result.DestExists {
			result.MissingConnections = append(result.MissingConnections, MissingConnection{
				Type:           req.DestinationType,
				Direction:      "destination",
				RequiredFields: getRequiredFields(req.DestinationType),
				Message:        fmt.Sprintf("Destination %s connection not configured", req.DestinationType),
			})
		}
		logger.Warn("DB not available for ConnectionValidationActivityV2; using ID-presence checks only")
		return result, nil
	}

	// Source
	sourceID := strings.TrimSpace(req.SourceConnectionID)
	if sourceID == "" {
		found, err := findActiveConnectionID(ctx, db, req.UserID, req.SourceType, "source")
		if err != nil {
			if err == ErrAmbiguousConnectionMatch {
				// Multiple viable connections exist; require explicit user selection.
				logger.Info("Multiple source connections found; requiring user selection", "source_type", req.SourceType)
				result.MissingConnections = append(result.MissingConnections, MissingConnection{
					Type:           req.SourceType,
					Direction:      "source",
					RequiredFields: getRequiredFields(req.SourceType),
					Message:        fmt.Sprintf("Multiple active %s source connections found. Please select one to continue.", req.SourceType),
				})
			} else {
				logger.Warn("Failed to lookup source connection", "error", err, "source_type", req.SourceType)
			}
		} else {
			sourceID = found
		}
	}
	if sourceID != "" {
		ok, err := validateConnectionRow(ctx, db, req.UserID, sourceID, req.SourceType, "source")
		if err != nil {
			logger.Warn("Source connection validation failed", "error", err, "source_connection_id", sourceID)
		} else if ok {
			result.SourceExists = true
			result.SourceConnectionID = sourceID
		}
	}
	if !result.SourceExists {
		// If we already appended an "ambiguous" missing connection above, don't duplicate.
		already := false
		for _, mc := range result.MissingConnections {
			if strings.EqualFold(mc.Direction, "source") {
				already = true
				break
			}
		}
		if already {
			// leave SourceExists=false
			goto destValidation
		}
		result.MissingConnections = append(result.MissingConnections, MissingConnection{
			Type:           req.SourceType,
			Direction:      "source",
			RequiredFields: getRequiredFields(req.SourceType),
			Message:        fmt.Sprintf("Source %s connection not configured", req.SourceType),
		})
	}

destValidation:
	// Destination
	destID := strings.TrimSpace(req.DestinationConnectionID)
	if destID == "" {
		found, err := findActiveConnectionID(ctx, db, req.UserID, req.DestinationType, "destination")
		if err != nil {
			if err == ErrAmbiguousConnectionMatch {
				// Multiple viable connections exist; require explicit user selection.
				logger.Info("Multiple destination connections found; requiring user selection", "destination_type", req.DestinationType)
				result.MissingConnections = append(result.MissingConnections, MissingConnection{
					Type:           req.DestinationType,
					Direction:      "destination",
					RequiredFields: getRequiredFields(req.DestinationType),
					Message:        fmt.Sprintf("Multiple active %s destination connections found. Please select one to continue.", req.DestinationType),
				})
			} else {
				logger.Warn("Failed to lookup destination connection", "error", err, "destination_type", req.DestinationType)
			}
		} else {
			destID = found
		}
	}
	if destID != "" {
		ok, err := validateConnectionRow(ctx, db, req.UserID, destID, req.DestinationType, "destination")
		if err != nil {
			logger.Warn("Destination connection validation failed", "error", err, "destination_connection_id", destID)
		} else if ok {
			result.DestExists = true
			result.DestinationConnectionID = destID
		}
	}
	if !result.DestExists {
		// If we already appended an "ambiguous" missing connection above, don't duplicate.
		already := false
		for _, mc := range result.MissingConnections {
			if strings.EqualFold(mc.Direction, "destination") {
				already = true
				break
			}
		}
		if !already {
			result.MissingConnections = append(result.MissingConnections, MissingConnection{
				Type:           req.DestinationType,
				Direction:      "destination",
				RequiredFields: getRequiredFields(req.DestinationType),
				Message:        fmt.Sprintf("Destination %s connection not configured", req.DestinationType),
			})
		}
	}

	result.AllConnectionsValid = result.SourceExists && result.DestExists

	// Optional: generate quick-pick suggestions for ambiguous UX cases.
	// Only do this when at least one side is missing (otherwise UI doesn't need it).
	if !result.AllConnectionsValid && strings.TrimSpace(req.UserRequest) != "" {
		sugs, err := generateIntentSuggestions(ctx, db, req.UserID, req.UserRequest)
		if err != nil {
			logger.Warn("Failed to generate intent suggestions", "error", err)
		} else if len(sugs) > 0 {
			result.Suggestions = sugs
		}
	}

	return result, nil
}

func getDBFromActivityContext() *sql.DB {
	if activityCtx == nil || activityCtx.DB == nil {
		return nil
	}
	if db, ok := activityCtx.DB.(*sql.DB); ok {
		return db
	}
	return nil
}

// connectorKey normalizes connector names for matching, making it work generically across all connectors.
// Removes spaces, hyphens, underscores and lowercases to handle: "aws-s3", "aws_s3", "AWS S3", etc.
func connectorKey(connectorType string) string {
	normalized := strings.ToLower(connectorType)
	normalized = strings.ReplaceAll(normalized, " ", "")
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, "_", "")
	return normalized
}

func findActiveConnectionID(ctx context.Context, db *sql.DB, userID, connectorType, direction string) (string, error) {
	// Query all active connections for this user and direction, then filter by normalized connector_type
	// This makes it work generically: "aws-s3" will match connections stored as "aws_s3", "AWS S3", etc.
	q := `
		SELECT id, connector_type
		FROM connections
		WHERE user_id = $1
			AND type = $2
			AND status = 'active'
		ORDER BY created_at DESC
	`

	rows, err := db.QueryContext(ctx, q, userID, direction)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	normalizedTarget := connectorKey(connectorType)

	matches := make([]string, 0, 2)
	for rows.Next() {
		var id, dbConnectorType string
		if err := rows.Scan(&id, &dbConnectorType); err != nil {
			continue
		}

		if connectorKey(dbConnectorType) == normalizedTarget {
			matches = append(matches, id)
			// Keep scanning to detect ambiguity; rows are ordered DESC by created_at.
			if len(matches) > 1 {
				// Early exit: we only need to know that it's ambiguous.
				return "", ErrAmbiguousConnectionMatch
			}
		}
	}

	if len(matches) == 1 {
		return matches[0], nil
	}

	return "", sql.ErrNoRows
}

func validateConnectionRow(ctx context.Context, db *sql.DB, userID, connectionID, connectorType, direction string) (bool, error) {
	// Validate using normalized connector_type matching for generic compatibility
	q := `
		SELECT connector_type
		FROM connections
		WHERE id = $1
			AND user_id = $2
			AND type = $3
			AND status = 'active'
	`
	var dbConnectorType string
	err := db.QueryRowContext(ctx, q, connectionID, userID, direction).Scan(&dbConnectorType)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	// Use generic matching: "aws-s3" matches "aws_s3", "AWS S3", etc.
	return connectorKey(dbConnectorType) == connectorKey(connectorType), nil
}

type ConnectionValidationRequest struct {
	CorrelationID           string `json:"correlation_id"`
	UserID                  string `json:"user_id"`
	SourceType              string `json:"source_type"`
	DestinationType         string `json:"destination_type"`
	SourceConnectionID      string `json:"source_connection_id,omitempty"`
	DestinationConnectionID string `json:"destination_connection_id,omitempty"`
	// Optional: raw user request text. Used ONLY for suggestion generation (UX), never required.
	UserRequest string `json:"user_request,omitempty"`
}

type ConnectionValidationResult struct {
	CorrelationID           string              `json:"correlation_id"`
	SourceExists            bool                `json:"source_exists"`
	DestExists              bool                `json:"dest_exists"`
	SourceConnectionID      string              `json:"source_connection_id,omitempty"`
	DestinationConnectionID string              `json:"destination_connection_id,omitempty"`
	AllConnectionsValid     bool                `json:"all_connections_valid"`
	MissingConnections      []MissingConnection `json:"missing_connections,omitempty"`
	Suggestions             []IntentSuggestion  `json:"suggestions,omitempty"`
}

type MissingConnection struct {
	Type           string            `json:"type"`
	Direction      string            `json:"direction"`
	RequiredFields []ConnectionField `json:"required_fields,omitempty"`
	Message        string            `json:"message"`
}

type ConnectionField struct {
	Name      string      `json:"name"`
	Type      string      `json:"type"`
	Required  bool        `json:"required,omitempty"`
	Default   interface{} `json:"default,omitempty"`
	Sensitive bool        `json:"sensitive,omitempty"`
}

func getRequiredFields(connectorType string) []ConnectionField {
	switch strings.ToLower(connectorType) {
	case "mysql":
		return []ConnectionField{
			{Name: "host", Type: "string", Required: true},
			{Name: "port", Type: "integer", Default: 3306},
			{Name: "database", Type: "string", Required: true},
			{Name: "username", Type: "string", Required: true},
			{Name: "password", Type: "string", Required: true, Sensitive: true},
		}
	case "postgresql", "postgres":
		return []ConnectionField{
			{Name: "host", Type: "string", Required: true},
			{Name: "port", Type: "integer", Default: 5432},
			{Name: "database", Type: "string", Required: true},
			{Name: "username", Type: "string", Required: true},
			{Name: "password", Type: "string", Required: true, Sensitive: true},
		}
	case "s3", "aws-s3", "aws_s3":
		return []ConnectionField{
			{Name: "bucket", Type: "string", Required: true},
			{Name: "region", Type: "string", Default: "us-east-1"},
			{Name: "access_key_id", Type: "string", Required: true, Sensitive: true},
			{Name: "secret_access_key", Type: "string", Required: true, Sensitive: true},
		}
	case "minio":
		return []ConnectionField{
			{Name: "endpoint", Type: "string", Required: true},
			{Name: "access_key", Type: "string", Required: true, Sensitive: true},
			{Name: "secret_key", Type: "string", Required: true, Sensitive: true},
			{Name: "bucket", Type: "string", Required: true},
		}
	default:
		return []ConnectionField{}
	}
}

// generateIntentSuggestions produces a small list of (source,destination) connector suggestions
// based ONLY on connections that the user already has configured (status=active).
//
// This avoids suggesting connectors that aren't installed/available in this environment.
func generateIntentSuggestions(ctx context.Context, db *sql.DB, userID string, userRequest string) ([]IntentSuggestion, error) {
	type row struct {
		connectorType string
		direction     string
		updatedAt     time.Time
	}

	// Pull recent active connections (best-effort ordering).
	q := `
		SELECT connector_type, type, COALESCE(updated_at, created_at) AS ts
		FROM connections
		WHERE user_id = $1 AND status = 'active'
		ORDER BY ts DESC
		LIMIT 50
	`
	rows, err := db.QueryContext(ctx, q, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sourceSeen := map[string]bool{}
	destSeen := map[string]bool{}
	sources := []string{}
	dests := []string{}

	for rows.Next() {
		var ct, dir string
		var ts time.Time
		if scanErr := rows.Scan(&ct, &dir, &ts); scanErr != nil {
			continue
		}
		dir = strings.ToLower(strings.TrimSpace(dir))
		ct = strings.TrimSpace(ct)
		if ct == "" {
			continue
		}
		if dir == "source" {
			k := connectorKey(ct)
			if !sourceSeen[k] {
				sourceSeen[k] = true
				sources = append(sources, ct)
			}
		} else if dir == "destination" {
			k := connectorKey(ct)
			if !destSeen[k] {
				destSeen[k] = true
				dests = append(dests, ct)
			}
		}
	}

	// If user has no connections configured, nothing to suggest.
	if len(sources) == 0 && len(dests) == 0 {
		return nil, nil
	}

	req := strings.ToLower(strings.TrimSpace(userRequest))
	reqKey := connectorKey(req)

	mentionedSources := []string{}
	mentionedDests := []string{}

	// Simple mention detection: check if normalized connector token appears in normalized request string.
	for _, s := range sources {
		if s == "" {
			continue
		}
		if strings.Contains(reqKey, connectorKey(s)) {
			mentionedSources = append(mentionedSources, s)
		}
	}
	for _, d := range dests {
		if d == "" {
			continue
		}
		if strings.Contains(reqKey, connectorKey(d)) {
			mentionedDests = append(mentionedDests, d)
		}
	}

	// Fallback alias hints for common shorthands users type.
	aliasToKey := map[string]string{
		"s3":        "awss3",
		"aws_s3":    "awss3",
		"aws-s3":    "awss3",
		"postgres":  "postgresql",
		"pg":        "postgresql",
		"mysql":     "mysql",
		"mongo":     "mongodb",
		"mongodb":   "mongodb",
		"pipedrive": "pipedrive",
	}
	for alias, key := range aliasToKey {
		if strings.Contains(reqKey, connectorKey(alias)) {
			// If alias suggests a destination, prefer matching against dests too.
			for _, s := range sources {
				if connectorKey(s) == key && !containsStr(mentionedSources, s) {
					mentionedSources = append(mentionedSources, s)
				}
			}
			for _, d := range dests {
				if connectorKey(d) == key && !containsStr(mentionedDests, d) {
					mentionedDests = append(mentionedDests, d)
				}
			}
		}
	}

	suggestions := []IntentSuggestion{}

	// Strategy A: if both sides mentioned, pair the first mentions.
	if len(mentionedSources) > 0 && len(mentionedDests) > 0 {
		suggestions = append(suggestions, IntentSuggestion{
			SourceType:      mentionedSources[0],
			DestinationType: mentionedDests[0],
			Confidence:      0.85,
			Reasoning:       "Detected both connectors in your request",
		})
	}

	// Strategy B: if only one side mentioned, pair with most recent from the other side.
	if len(suggestions) == 0 {
		if len(mentionedSources) > 0 && len(dests) > 0 {
			suggestions = append(suggestions, IntentSuggestion{
				SourceType:      mentionedSources[0],
				DestinationType: dests[0],
				Confidence:      0.65,
				Reasoning:       "Detected source in your request; paired with your most recent destination",
			})
		} else if len(mentionedDests) > 0 && len(sources) > 0 {
			suggestions = append(suggestions, IntentSuggestion{
				SourceType:      sources[0],
				DestinationType: mentionedDests[0],
				Confidence:      0.65,
				Reasoning:       "Detected destination in your request; paired with your most recent source",
			})
		}
	}

	// Strategy C: Most recent pairs (safe fallback).
	if len(suggestions) == 0 && len(sources) > 0 && len(dests) > 0 {
		suggestions = append(suggestions, IntentSuggestion{
			SourceType:      sources[0],
			DestinationType: dests[0],
			Confidence:      0.55,
			Reasoning:       "Suggested based on your most recent connections",
		})
	}

	// Add one more alternative if available (top-2).
	if len(suggestions) == 1 && len(sources) > 1 && len(dests) > 0 {
		suggestions = append(suggestions, IntentSuggestion{
			SourceType:      sources[1],
			DestinationType: dests[0],
			Confidence:      0.45,
			Reasoning:       "Alternative based on your recent connections",
		})
	}

	// Cap at 3.
	if len(suggestions) > 3 {
		suggestions = suggestions[:3]
	}
	return suggestions, nil
}

func containsStr(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
