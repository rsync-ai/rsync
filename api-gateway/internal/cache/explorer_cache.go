package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

// =============================================================================
// EXPLORER SCHEMA INDEX CACHE
// =============================================================================

// ExplorerSchemaIndex represents a cached schema index for a connection
// Used for LLM table/column retrieval without sending full schema to prompts
type ExplorerSchemaIndex struct {
	ConnectionID    string               `json:"connection_id"`
	SchemaHash      string               `json:"schema_hash"`      // SHA256 fingerprint
	LastRefreshedAt time.Time            `json:"last_refreshed_at"`
	TableCount      int                  `json:"table_count"`
	Tables          []ExplorerTableIndex `json:"tables"`
	ForeignKeys     []ExplorerForeignKeyIndex `json:"foreign_keys,omitempty"`
}

// ExplorerForeignKeyIndex represents a foreign key relationship in the schema index.
type ExplorerForeignKeyIndex struct {
	FromSchema string  `json:"from_schema,omitempty"`
	FromTable  string  `json:"from_table"`
	FromColumn string  `json:"from_column"`
	ToSchema   string  `json:"to_schema,omitempty"`
	ToTable    string  `json:"to_table"`
	ToColumn   string  `json:"to_column"`
	// Confidence is 1.0 for explicit FK constraints; <1.0 for inferred relationships (Phase 3.5).
	Confidence float64 `json:"confidence,omitempty"`
}

// ExplorerTableIndex represents a table in the schema index
type ExplorerTableIndex struct {
	Name       string                  `json:"name"`
	Schema     string                  `json:"schema,omitempty"`
	RowCount   int64                   `json:"row_count,omitempty"`
	Columns    []ExplorerColumnIndex   `json:"columns"`
	PrimaryKey []string                `json:"primary_key,omitempty"`
	// Compact searchable tokens for retrieval
	SearchTokens string `json:"search_tokens"`
}

// ExplorerColumnIndex represents a column in the table index
type ExplorerColumnIndex struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	IsPrimaryKey bool   `json:"is_primary_key,omitempty"`
	IsNullable   bool   `json:"is_nullable,omitempty"`
}

// =============================================================================
// PHASE 3.5: INFERRED RELATIONSHIPS (Heuristics)
// =============================================================================

var identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func normName(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func singularize(token string) string {
	t := normName(token)
	if strings.HasSuffix(t, "ies") && len(t) > 3 {
		return strings.TrimSuffix(t, "ies") + "y"
	}
	// naive plural stripping
	if strings.HasSuffix(t, "s") && len(t) > 3 {
		return strings.TrimSuffix(t, "s")
	}
	return t
}

func tableTokens(name string) []string {
	n := normName(name)
	if n == "" {
		return nil
	}
	var out []string
	out = append(out, n)
	out = append(out, singularize(n))

	parts := strings.Split(n, "_")
	if len(parts) > 1 {
		last := parts[len(parts)-1]
		out = append(out, last, singularize(last))
	}

	// de-dupe
	seen := map[string]bool{}
	var uniq []string
	for _, t := range out {
		t = normName(t)
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		uniq = append(uniq, t)
	}
	return uniq
}

func typeClass(t string) string {
	x := normName(t)
	switch {
	case strings.Contains(x, "uuid") || strings.Contains(x, "uniqueidentifier"):
		return "uuid"
	case strings.Contains(x, "int") || strings.Contains(x, "bigint") || strings.Contains(x, "smallint") || strings.Contains(x, "serial"):
		return "int"
	case strings.Contains(x, "decimal") || strings.Contains(x, "numeric") || strings.Contains(x, "float") || strings.Contains(x, "double") || strings.Contains(x, "real"):
		return "num"
	case strings.Contains(x, "char") || strings.Contains(x, "text") || strings.Contains(x, "varchar") || strings.Contains(x, "string"):
		return "str"
	default:
		return "other"
	}
}

func typesCompatible(a, b string) bool {
	ca := typeClass(a)
	cb := typeClass(b)
	if ca == "other" || cb == "other" {
		return false
	}
	return ca == cb
}

func pkColumn(table ExplorerTableIndex) (name string, typ string, ok bool) {
	// Prefer explicit PK flag.
	for _, c := range table.Columns {
		if c.IsPrimaryKey {
			return c.Name, c.Type, true
		}
	}
	// Fallback: column named id
	for _, c := range table.Columns {
		if strings.EqualFold(c.Name, "id") {
			return c.Name, c.Type, true
		}
	}
	return "", "", false
}

type inferredCandidate struct {
	fk         ExplorerForeignKeyIndex
	confidence float64
}

// InferForeignKeysFromHeuristics generates inferred FK edges using naming + type heuristics.
// It never duplicates explicit constraints, and emits confidence < 1.0 for inferred edges.
func InferForeignKeysFromHeuristics(tables []ExplorerTableIndex, explicit []ExplorerForeignKeyIndex) []ExplorerForeignKeyIndex {
	const (
		// Heuristic bounds to avoid quadratic blowups on very large schemas.
		maxTablesPerToken        = 200
		maxBestTargetsPerColumn  = 500
	)

	explicitKeys := map[string]bool{}
	for _, fk := range explicit {
		k := strings.ToLower(fmt.Sprintf("%s.%s.%s->%s.%s.%s", fk.FromSchema, fk.FromTable, fk.FromColumn, fk.ToSchema, fk.ToTable, fk.ToColumn))
		explicitKeys[k] = true
	}

	// Build token→tables index.
	tokenToTables := map[string][]ExplorerTableIndex{}
	for _, t := range tables {
		for _, tok := range tableTokens(t.Name) {
			// Cap per-token fanout; high-fanout tokens are low-signal anyway.
			if len(tokenToTables[tok]) >= maxTablesPerToken {
				continue
			}
			tokenToTables[tok] = append(tokenToTables[tok], t)
		}
	}

	var inferred []ExplorerForeignKeyIndex

	for _, from := range tables {
		for _, col := range from.Columns {
			// Only infer *id-like* columns.
			if col.IsPrimaryKey {
				continue
			}
			colName := normName(col.Name)
			if colName == "" || colName == "id" {
				continue
			}
			if !strings.HasSuffix(colName, "_id") {
				continue
			}

			base := strings.TrimSuffix(colName, "_id")
			if base == "" {
				continue
			}

			// Candidate tokens: full base and last segment base.
			var candTokens []string
			candTokens = append(candTokens, base, singularize(base))
			if strings.Contains(base, "_") {
				last := base[strings.LastIndex(base, "_")+1:]
				candTokens = append(candTokens, last, singularize(last))
			}

			// Collect candidates for this column, keep best per target table.
			bestPerTarget := map[string]inferredCandidate{}
			for _, tok := range candTokens {
				tok = normName(tok)
				if tok == "" {
					continue
				}
				for _, to := range tokenToTables[tok] {
					if len(bestPerTarget) >= maxBestTargetsPerColumn {
						break
					}
					// Skip self.
					if strings.EqualFold(to.Name, from.Name) {
						continue
					}
					pkName, pkType, ok := pkColumn(to)
					if !ok {
						continue
					}
					// Basic confidence from naming match.
					conf := 0.55
					if tok == normName(to.Name) || tok == singularize(to.Name) {
						conf += 0.15
					} else {
						// weaker match via last-segment
						conf += 0.05
					}
					// Type compatibility bonus.
					if typesCompatible(col.Type, pkType) {
						conf += 0.10
					}
					// Cap inferred confidence
					if conf > 0.85 {
						conf = 0.85
					}
					fk := ExplorerForeignKeyIndex{
						FromSchema: from.Schema,
						FromTable:  from.Name,
						FromColumn: col.Name,
						ToSchema:   to.Schema,
						ToTable:    to.Name,
						ToColumn:   pkName,
						Confidence: conf,
					}
					key := strings.ToLower(fmt.Sprintf("%s.%s.%s->%s.%s.%s", fk.FromSchema, fk.FromTable, fk.FromColumn, fk.ToSchema, fk.ToTable, fk.ToColumn))
					if explicitKeys[key] {
						continue
					}
					targetKey := strings.ToLower(fmt.Sprintf("%s.%s.%s", fk.ToSchema, fk.ToTable, fk.ToColumn))
					prev, exists := bestPerTarget[targetKey]
					if !exists || conf > prev.confidence {
						bestPerTarget[targetKey] = inferredCandidate{fk: fk, confidence: conf}
					}
				}
			}

			// Take up to top-3 inferred targets for this column.
			var cands []inferredCandidate
			for _, c := range bestPerTarget {
				cands = append(cands, c)
			}
			sort.Slice(cands, func(i, j int) bool {
				return cands[i].confidence > cands[j].confidence
			})
			if len(cands) > 3 {
				cands = cands[:3]
			}
			for _, c := range cands {
				inferred = append(inferred, c.fk)
			}
		}
	}

	return inferred
}

// ExplorerCache provides Redis-backed caching for Explorer features
type ExplorerCache struct {
	client *redis.Client
	ttl    time.Duration
}

// NewExplorerCache creates a new explorer cache instance using existing Redis client
func NewExplorerCache(client *redis.Client, ttl time.Duration) *ExplorerCache {
	return &ExplorerCache{
		client: client,
		ttl:    ttl,
	}
}

// =============================================================================
// SCHEMA INDEX METHODS
// =============================================================================

// GetSchemaIndex retrieves the cached schema index for a connection
func (c *ExplorerCache) GetSchemaIndex(ctx context.Context, connectionID string) (*ExplorerSchemaIndex, error) {
	key := fmt.Sprintf("explorer:schema_index:%s", connectionID)

	val, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil // Cache miss
	}
	if err != nil {
		log.WithError(err).Warn("Redis GET schema index failed")
		return nil, err
	}

	var index ExplorerSchemaIndex
	if err := json.Unmarshal(val, &index); err != nil {
		log.WithError(err).Warn("Failed to unmarshal schema index")
		return nil, err
	}

	log.WithFields(log.Fields{
		"connection_id": connectionID,
		"schema_hash":   index.SchemaHash,
		"table_count":   index.TableCount,
	}).Debug("Schema index cache hit")

	return &index, nil
}

// SetSchemaIndex caches a schema index
func (c *ExplorerCache) SetSchemaIndex(ctx context.Context, index *ExplorerSchemaIndex) error {
	key := fmt.Sprintf("explorer:schema_index:%s", index.ConnectionID)

	data, err := json.Marshal(index)
	if err != nil {
		return fmt.Errorf("failed to marshal schema index: %w", err)
	}

	err = c.client.Set(ctx, key, data, c.ttl).Err()
	if err != nil {
		log.WithError(err).Warn("Redis SET schema index failed")
		return err
	}

	log.WithFields(log.Fields{
		"connection_id": index.ConnectionID,
		"schema_hash":   index.SchemaHash,
		"table_count":   index.TableCount,
		"ttl":           c.ttl.String(),
		"size_bytes":    len(data),
	}).Debug("Schema index cached")

	return nil
}

// InvalidateSchemaIndex removes cached schema index
func (c *ExplorerCache) InvalidateSchemaIndex(ctx context.Context, connectionID string) error {
	key := fmt.Sprintf("explorer:schema_index:%s", connectionID)

	err := c.client.Del(ctx, key).Err()
	if err != nil {
		log.WithError(err).Warn("Redis DEL schema index failed")
		return err
	}

	log.WithField("connection_id", connectionID).Info("Schema index cache invalidated")
	return nil
}

// =============================================================================
// SCHEMA FINGERPRINT COMPUTATION
// =============================================================================

// ComputeSchemaHash computes a SHA256 fingerprint of the schema
// Changes in tables, columns, or types will produce a different hash
func ComputeSchemaHash(tables []ExplorerTableIndex, foreignKeys []ExplorerForeignKeyIndex) string {
	// Sort tables by name for consistent ordering
	sortedTables := make([]ExplorerTableIndex, len(tables))
	copy(sortedTables, tables)
	sort.Slice(sortedTables, func(i, j int) bool {
		return sortedTables[i].Name < sortedTables[j].Name
	})

	sortedFKs := make([]ExplorerForeignKeyIndex, len(foreignKeys))
	copy(sortedFKs, foreignKeys)
	sort.Slice(sortedFKs, func(i, j int) bool {
		// Stable canonical ordering.
		a := fmt.Sprintf("%s.%s.%s->%s.%s.%s", sortedFKs[i].FromSchema, sortedFKs[i].FromTable, sortedFKs[i].FromColumn, sortedFKs[i].ToSchema, sortedFKs[i].ToTable, sortedFKs[i].ToColumn)
		b := fmt.Sprintf("%s.%s.%s->%s.%s.%s", sortedFKs[j].FromSchema, sortedFKs[j].FromTable, sortedFKs[j].FromColumn, sortedFKs[j].ToSchema, sortedFKs[j].ToTable, sortedFKs[j].ToColumn)
		return a < b
	})

	// Build canonical representation
	var parts []string
	for _, t := range sortedTables {
		tablePart := fmt.Sprintf("%s.%s", t.Schema, t.Name)

		// Sort columns
		sortedCols := make([]ExplorerColumnIndex, len(t.Columns))
		copy(sortedCols, t.Columns)
		sort.Slice(sortedCols, func(i, j int) bool {
			return sortedCols[i].Name < sortedCols[j].Name
		})

		var colParts []string
		for _, col := range sortedCols {
			colPart := fmt.Sprintf("%s:%s", col.Name, col.Type)
			if col.IsPrimaryKey {
				colPart += ":pk"
			}
			colParts = append(colParts, colPart)
		}

		tablePart += "(" + strings.Join(colParts, ",") + ")"
		parts = append(parts, tablePart)
	}

	if len(sortedFKs) > 0 {
		var fkParts []string
		for _, fk := range sortedFKs {
			fkParts = append(
				fkParts,
				fmt.Sprintf("%s.%s.%s->%s.%s.%s:%.2f", fk.FromSchema, fk.FromTable, fk.FromColumn, fk.ToSchema, fk.ToTable, fk.ToColumn, fk.Confidence),
			)
		}
		parts = append(parts, "FK("+strings.Join(fkParts, ",")+")")
	}

	canonical := strings.Join(parts, "|")
	hash := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(hash[:])
}

// BuildSearchTokens creates searchable tokens for a table (for retrieval)
func BuildSearchTokens(table ExplorerTableIndex) string {
	var tokens []string

	// Table name tokens
	tokens = append(tokens, strings.ToLower(table.Name))
	// Split by underscore and camelCase
	tokens = append(tokens, splitToTokens(table.Name)...)

	// Column name tokens
	for _, col := range table.Columns {
		tokens = append(tokens, strings.ToLower(col.Name))
		tokens = append(tokens, splitToTokens(col.Name)...)
	}

	// Deduplicate
	seen := make(map[string]bool)
	var unique []string
	for _, t := range tokens {
		t = strings.TrimSpace(t)
		if t != "" && !seen[t] {
			seen[t] = true
			unique = append(unique, t)
		}
	}

	return strings.Join(unique, " ")
}

// splitToTokens splits a name into searchable tokens
func splitToTokens(name string) []string {
	var tokens []string

	// Split by underscore
	parts := strings.Split(strings.ToLower(name), "_")
	tokens = append(tokens, parts...)

	// TODO: Add camelCase splitting if needed

	return tokens
}

// =============================================================================
// CONVERSATION CONTEXT CACHE (Multi-turn)
// =============================================================================

// ExplorerConversation stores multi-turn conversation state
type ExplorerConversation struct {
	ID                string               `json:"id"`
	ConnectionID      string               `json:"connection_id"`
	SchemaHash        string               `json:"schema_hash"`
	SelectedTables    []string             `json:"selected_tables,omitempty"`
	SelectedColumns   []string             `json:"selected_columns,omitempty"`
	JoinPlan          []string             `json:"join_plan,omitempty"`
	LastSQL           string               `json:"last_sql,omitempty"`
	LastExplanation   string               `json:"last_explanation,omitempty"`
	ResultProfile     *ResultProfile       `json:"result_profile,omitempty"`
	HITLConfirmations []HITLConfirmation   `json:"hitl_confirmations,omitempty"`
	Messages          []ConversationMessage `json:"messages,omitempty"`
	CreatedAt         time.Time            `json:"created_at"`
	UpdatedAt         time.Time            `json:"updated_at"`
}

// ResultProfile contains lightweight result summary for context
type ResultProfile struct {
	RowCount     int       `json:"row_count"`
	Columns      []string  `json:"columns"`
	MinTimestamp *string   `json:"min_timestamp,omitempty"`
	MaxTimestamp *string   `json:"max_timestamp,omitempty"`
}

// HITLConfirmation records a user's HITL decision
type HITLConfirmation struct {
	StepType string   `json:"step_type"` // table_link, column_link
	Question string   `json:"question"`
	Response []string `json:"response"` // selected options
	At       time.Time `json:"at"`
}

// ConversationMessage represents a message in the conversation
type ConversationMessage struct {
	Role      string    `json:"role"` // user, assistant, system
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

// GetConversation retrieves a conversation by ID
func (c *ExplorerCache) GetConversation(ctx context.Context, conversationID string) (*ExplorerConversation, error) {
	key := fmt.Sprintf("explorer:conversation:%s", conversationID)

	val, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil // Not found
	}
	if err != nil {
		return nil, err
	}

	var conv ExplorerConversation
	if err := json.Unmarshal(val, &conv); err != nil {
		return nil, err
	}

	return &conv, nil
}

// SetConversation stores a conversation
func (c *ExplorerCache) SetConversation(ctx context.Context, conv *ExplorerConversation) error {
	key := fmt.Sprintf("explorer:conversation:%s", conv.ID)
	conv.UpdatedAt = time.Now()

	data, err := json.Marshal(conv)
	if err != nil {
		return err
	}

	// Conversations have shorter TTL (30 minutes default)
	convTTL := 30 * time.Minute
	return c.client.Set(ctx, key, data, convTTL).Err()
}

// =============================================================================
// QUERY RESULT CACHE
// =============================================================================

// CachedQueryResult represents a cached query result
type CachedQueryResult struct {
	CacheKey        string                   `json:"cache_key"`
	ConnectionID    string                   `json:"connection_id"`
	SchemaHash      string                   `json:"schema_hash"`
	SQL             string                   `json:"sql"`
	NormalizedSQL   string                   `json:"normalized_sql"`
	Limit           int                      `json:"limit"`
	Columns         []string                 `json:"columns"`
	Rows            []map[string]interface{} `json:"rows"`
	RowCount        int                      `json:"row_count"`
	ExecutionTimeMs int64                    `json:"execution_time_ms"`
	Truncated       bool                     `json:"truncated"`
	CachedAt        time.Time                `json:"cached_at"`
}

// QueryResultCacheKey builds a cache key for query results
func QueryResultCacheKey(userID, connectionID, schemaHash, normalizedSQL string, limit int) string {
	keyPart := fmt.Sprintf("%s:%s:%s:%s:%d", userID, connectionID, schemaHash, normalizedSQL, limit)
	hash := sha256.Sum256([]byte(keyPart))
	return hex.EncodeToString(hash[:16]) // Use first 16 bytes
}

// GetQueryResult retrieves a cached query result
func (c *ExplorerCache) GetQueryResult(ctx context.Context, cacheKey string) (*CachedQueryResult, error) {
	key := fmt.Sprintf("explorer:query_result:%s", cacheKey)

	val, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var result CachedQueryResult
	if err := json.Unmarshal(val, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

// SetQueryResult caches a query result
func (c *ExplorerCache) SetQueryResult(ctx context.Context, result *CachedQueryResult) error {
	key := fmt.Sprintf("explorer:query_result:%s", result.CacheKey)
	result.CachedAt = time.Now()

	data, err := json.Marshal(result)
	if err != nil {
		return err
	}

	// Query results have shorter TTL (60s default)
	resultTTL := 60 * time.Second
	return c.client.Set(ctx, key, data, resultTTL).Err()
}

// =============================================================================
// TABLE RETRIEVAL METHODS (for large schema handling)
// =============================================================================

// ScoreTableMatch scores how well a table matches search terms
// Returns 0-1 score (higher is better match)
func ScoreTableMatch(table ExplorerTableIndex, searchTerms []string) float64 {
	if len(searchTerms) == 0 {
		return 0
	}

	tokens := strings.ToLower(table.SearchTokens)
	tableName := strings.ToLower(table.Name)
	score := 0.0

	for _, term := range searchTerms {
		term = strings.ToLower(strings.TrimSpace(term))
		if term == "" {
			continue
		}

		// Exact table name match (highest weight)
		if tableName == term {
			score += 3.0
		} else if strings.HasPrefix(tableName, term) {
			score += 2.0
		} else if strings.Contains(tableName, term) {
			score += 1.5
		}

		// Token match
		if strings.Contains(tokens, term) {
			score += 1.0
		}

		// Column name match
		for _, col := range table.Columns {
			colName := strings.ToLower(col.Name)
			if colName == term {
				score += 0.8
			} else if strings.Contains(colName, term) {
				score += 0.4
			}
		}
	}

	// Normalize by number of terms
	return score / float64(len(searchTerms))
}

// RetrieveTopTables returns top-K tables matching search terms
func RetrieveTopTables(index *ExplorerSchemaIndex, searchTerms []string, topK int) []ExplorerTableIndex {
	if index == nil || len(index.Tables) == 0 {
		return nil
	}

	type scoredTable struct {
		table ExplorerTableIndex
		score float64
	}

	var scored []scoredTable
	for _, t := range index.Tables {
		s := ScoreTableMatch(t, searchTerms)
		if s > 0 {
			scored = append(scored, scoredTable{table: t, score: s})
		}
	}

	// Sort by score descending
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})

	// Return top K
	if topK > len(scored) {
		topK = len(scored)
	}

	result := make([]ExplorerTableIndex, topK)
	for i := 0; i < topK; i++ {
		result[i] = scored[i].table
	}

	return result
}

// ExtractSearchTerms extracts searchable terms from a natural language question
func ExtractSearchTerms(question string) []string {
	// Simple tokenization - can be enhanced with NLP
	question = strings.ToLower(question)

	// Remove common stop words
	stopWords := map[string]bool{
		"the": true, "a": true, "an": true, "is": true, "are": true,
		"was": true, "were": true, "be": true, "been": true, "being": true,
		"have": true, "has": true, "had": true, "do": true, "does": true,
		"did": true, "will": true, "would": true, "could": true, "should": true,
		"may": true, "might": true, "must": true, "shall": true,
		"i": true, "you": true, "he": true, "she": true, "it": true,
		"we": true, "they": true, "what": true, "which": true, "who": true,
		"when": true, "where": true, "why": true, "how": true,
		"this": true, "that": true, "these": true, "those": true,
		"am": true, "can": true, "for": true, "and": true, "or": true,
		"but": true, "if": true, "then": true, "else": true,
		"of": true, "at": true, "by": true, "from": true, "to": true, "in": true,
		"on": true, "with": true, "as": true,
		"show": true, "me": true, "get": true, "find": true, "list": true,
		"all": true, "each": true, "every": true, "any": true, "some": true,
		"select": true, "query": true, "data": true,
	}

	// Split into words
	words := strings.FieldsFunc(question, func(r rune) bool {
		return r == ' ' || r == ',' || r == '.' || r == '?' || r == '!' ||
			r == ':' || r == ';' || r == '"' || r == '\'' || r == '(' || r == ')'
	})

	var terms []string
	for _, w := range words {
		w = strings.TrimSpace(w)
		if w == "" || len(w) < 2 {
			continue
		}
		if !stopWords[w] {
			terms = append(terms, w)
		}
	}

	return terms
}
