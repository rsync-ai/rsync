package handlers

import (
	"context"
	"database/sql"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rsync-ai/shared/pgdriver"
	log "github.com/sirupsen/logrus"

	"api-gateway/internal/db"
	"api-gateway/internal/security"
	"api-gateway/internal/validators"
)

// Answers "which pipeline produces the tables this model reads?" so the schedule
// dialog can offer an upstream instead of asking the user to remember one.
//
// The answer is a SUGGESTION and nothing here writes anything. That is the design, not
// a limitation: an inferred edge that re-derives itself whenever someone edits the SQL
// is a schedule that changes without anyone asking for it. A person picks from this
// list, and what they picked is what stays picked until they change it.
//
// Two things make this correct or useless, and both are easy to get backwards:
//
//   1. It matches destination_qualified_name, NEVER qualified_name. A model's SQL runs
//      against the warehouse and names DESTINATION tables; qualified_name is the
//      SOURCE-side name — for CDC it is literally the upstream database's schema, which
//      is what migration 089 was written to correct. Matching on it would answer with
//      whichever pipeline happens to READ a table of that name, which for a
//      MySQL->Postgres pipeline is a different pipeline than the one that WROTE it.
//
//   2. It is scoped to the model's own connection and workspace. A model reads one
//      warehouse; a pipeline landing "analytics.orders" into a different destination
//      produces a different table that merely shares a name.
//
// destination_qualified_name is NULL for object-storage destinations and any sink older
// than 089, so those pipelines cannot be suggested. That is a miss, and a miss is the
// right way to be wrong here: the dialog falls back to the manual picker the user
// already has, whereas a confident wrong answer gets a schedule hung off an unrelated
// pipeline.

// upstreamCandidate is one pipeline that has been observed writing a table this model
// reads.
type upstreamCandidate struct {
	PipelineID   string `json:"pipeline_id"`
	PipelineName string `json:"pipeline_name"`
	// The destination table that matched, as the pipeline recorded it.
	Table string `json:"table"`
	// The reference in the model's SQL that matched it, as written.
	MatchedReference string `json:"matched_reference"`
	// Qualified means the SQL named a schema. An unqualified match is weaker: it
	// matched on table name alone and could belong to another schema entirely.
	Qualified bool `json:"qualified"`
}

type upstreamSuggestionResponse struct {
	// Every table the SQL reads, resolved or not, so the dialog can say "3 of 4 inputs
	// have a known producer" rather than silently showing what it happened to find.
	References []string            `json:"references"`
	Unresolved []string            `json:"unresolved"`
	Candidates []upstreamCandidate `json:"candidates"`
	// Ambiguous is true when some reference matched more than one pipeline. The UI must
	// not pre-select anything in that case.
	Ambiguous bool `json:"ambiguous"`
}

// SuggestSavedQueryUpstreams lists the pipelines that produce this model's inputs.
// GET /api/v1/explorer/saved/:id/upstreams
//
// Read-only, so it needs no more than the role that can read the query itself. It
// reveals which pipelines write into the workspace's own warehouse, which a member can
// already list directly.
func SuggestSavedQueryUpstreams(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid saved query id"})
		return
	}
	if _, ok := requireResourceRole(c, "saved_queries", id, security.WSViewer); !ok {
		return
	}

	var sqlText, connectionID, workspaceID string
	err := database.QueryRowContext(c.Request.Context(),
		`SELECT sql_text, connection_id::text, workspace_id::text
		 FROM saved_queries WHERE id = $1`, id).Scan(&sqlText, &connectionID, &workspaceID)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "saved query not found"})
		return
	}
	if err != nil {
		log.WithError(err).Error("upstream suggestion: could not read the saved query")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load saved query"})
		return
	}

	resp, err := resolveUpstreams(c.Request.Context(), database, sqlText, connectionID, workspaceID)
	if err != nil {
		log.WithError(err).Error("upstream suggestion: could not resolve table references")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to look up upstream pipelines"})
		return
	}
	c.JSON(http.StatusOK, resp)
}

// resolveUpstreams is the whole of the inference, separated from the HTTP layer so it
// can be tested against a database without a router.
func resolveUpstreams(
	ctx context.Context,
	database *sql.DB,
	sqlText, connectionID, workspaceID string,
) (upstreamSuggestionResponse, error) {
	resp := upstreamSuggestionResponse{
		References: []string{},
		Unresolved: []string{},
		Candidates: []upstreamCandidate{},
	}

	refs := validators.ExtractTableReferences(sqlText)
	if len(refs) == 0 {
		return resp, nil
	}

	for _, ref := range refs {
		resp.References = append(resp.References, ref.Qualified())
	}

	// One query for all references rather than one per reference: a model with a dozen
	// inputs should not be a dozen round trips, and the match is a simple membership
	// test on two candidate spellings per reference.
	//
	// Comparison is lower(); table names are matched case-insensitively even though a
	// quoted identifier is case-sensitive to the engine. That direction is deliberate:
	// it can over-match (offering a candidate a stricter comparison would have skipped)
	// and a person confirms the choice, whereas exact matching would silently drop
	// every reference whose case differs from how the sink recorded it.
	wanted := make([]string, 0, len(refs)*2)
	for _, ref := range refs {
		wanted = append(wanted, strings.ToLower(ref.SchemaQualified()))
		// The bare name is fetched only for a reference that named no schema. This is a
		// PREFILTER, not the rule: the pairing loop below decides what actually matches,
		// and it enforces the same condition independently. Widening this line changes
		// how many rows come back, never the answer — whereas dropping the guard in the
		// pairing loop changes the answer. Do not read this as making that one redundant.
		if len(ref.Parts) == 1 {
			wanted = append(wanted, strings.ToLower(ref.Name()))
		}
	}

	// s.table_name is matched alongside destination_qualified_name, and that is safe
	// even though its siblings schema_name/qualified_name are SOURCE-side names:
	// migration 089 defines destination_qualified_name as "destination_schema.table_name",
	// so table_name is the table's own name on BOTH sides — only the namespace differs.
	// The NOT NULL guard keeps this honest: a row with no destination namespace is a
	// pipeline we cannot place in the model's warehouse, so it cannot match either way.
	rows, err := database.QueryContext(ctx, `
		SELECT DISTINCT
		    p.id::text,
		    COALESCE(p.name, ''),
		    COALESCE(s.destination_qualified_name, ''),
		    COALESCE(s.table_name, '')
		FROM pipeline_run_table_stats s
		JOIN pipelines p ON p.id = s.pipeline_id
		WHERE p.workspace_id = $1::uuid
		  AND p.destination_connection_id = $2::uuid
		  AND s.destination_qualified_name IS NOT NULL
		  AND (
		        lower(s.destination_qualified_name) = ANY($3)
		     OR lower(s.table_name) = ANY($3)
		      )`,
		workspaceID, connectionID, pgdriver.StringArray(wanted))
	if err != nil {
		return resp, err
	}
	defer rows.Close()

	type produced struct {
		pipelineID, pipelineName, destQualified, tableName string
	}
	var rowsOut []produced
	for rows.Next() {
		var p produced
		if err := rows.Scan(&p.pipelineID, &p.pipelineName, &p.destQualified, &p.tableName); err != nil {
			return resp, err
		}
		rowsOut = append(rowsOut, p)
	}
	if err := rows.Err(); err != nil {
		return resp, err
	}

	// Pair each reference with the pipelines that match it.
	//
	// A bare table name is only ever matched for a reference that named NO schema —
	// `FROM orders`, which leans on the connection's search_path and is how most ad-hoc
	// SQL is written. When the SQL does name a schema, that schema is information, and
	// falling back to the name alone throws it away: `shop.orders` failing to match
	// `analytics.orders` means the model reads some other table, not that we should
	// offer the pipeline that writes the one we found. Getting this backwards is not a
	// near miss — for a CDC pipeline, `shop.orders` is the SOURCE name of the very table
	// `analytics.orders` is the destination name of, so the resolver would confidently
	// answer for a table the model never reads.
	seen := map[string]bool{}
	for _, ref := range refs {
		wantQualified := strings.ToLower(ref.SchemaQualified())
		wantName := strings.ToLower(ref.Name())
		unqualified := len(ref.Parts) == 1

		var strong, weak []produced
		for _, p := range rowsOut {
			switch {
			case p.destQualified != "" && strings.ToLower(p.destQualified) == wantQualified:
				strong = append(strong, p)
			case unqualified && strings.ToLower(p.tableName) == wantName:
				weak = append(weak, p)
			}
		}
		matches := strong
		qualified := true
		if len(matches) == 0 {
			matches, qualified = weak, false
		}
		if len(matches) == 0 {
			resp.Unresolved = append(resp.Unresolved, ref.Qualified())
			continue
		}
		if len(matches) > 1 {
			resp.Ambiguous = true
		}
		for _, m := range matches {
			key := m.pipelineID + "\x00" + ref.Qualified()
			if seen[key] {
				continue
			}
			seen[key] = true
			table := m.destQualified
			if table == "" {
				table = m.tableName
			}
			resp.Candidates = append(resp.Candidates, upstreamCandidate{
				PipelineID:       m.pipelineID,
				PipelineName:     m.pipelineName,
				Table:            table,
				MatchedReference: ref.Qualified(),
				Qualified:        qualified,
			})
		}
	}

	return resp, nil
}
