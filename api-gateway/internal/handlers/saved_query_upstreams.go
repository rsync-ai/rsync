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

// Answers "which pipeline or model produces the tables this model reads?" so the
// schedule dialog can offer an upstream instead of asking the user to remember one.
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
// Models are producers on the same terms as the asset graph's: a model with
// materialization='table' builds its target_table on its own connection, which is
// already a destination-side name, so rule 1 holds for it by construction. A
// 'statement' model writes the tables its own SQL names (modelProducedTables), which
// are destination-side for the same reason. Neither is an OBSERVED fact — a model is
// offered before it has ever run, because the schedule being set up may well be what
// makes it run.
//
// destination_qualified_name is NULL for object-storage destinations and any sink older
// than 089, so those pipelines cannot be suggested. That is a miss, and a miss is the
// right way to be wrong here: the dialog falls back to the manual picker the user
// already has, whereas a confident wrong answer gets a schedule hung off an unrelated
// pipeline.

// upstreamCandidate is one producer — a pipeline observed writing a table, or a model
// declared to build one — of a table this model reads.
type upstreamCandidate struct {
	// Kind and ID are exactly what a schedule's upstream list takes, so the dialog can
	// hand a candidate to the picker without translating it.
	Kind string `json:"kind"`
	ID   string `json:"id"`
	Name string `json:"name"`
	// The table that matched, as its producer recorded it.
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
	// Ambiguous is true when some reference could be more than one DIFFERENT table —
	// `orders` matching both analytics.orders and staging.orders because the SQL named
	// no schema. Two producers of the SAME table is fan-in, not ambiguity: both really
	// do write it, a schedule can follow both, and flagging it would bury the real
	// uncertainty under every table that happens to have two loaders. This is the
	// asset graph's definition, and deliberately so.
	Ambiguous bool `json:"ambiguous"`
}

// SuggestSavedQueryUpstreams lists the pipelines and models that produce this model's
// inputs.
// GET /api/v1/explorer/saved/:id/upstreams
//
// Read-only, so it needs no more than the role that can read the query itself. It
// reveals which pipelines write into the workspace's own warehouse, which a member can
// already list directly, and which models build tables there — limited to the models
// this user can see, so a private model is never named to anyone but its author.
//
// The query itself is loaded through loadSavedQuery, like every other read of the row.
// Workspace membership alone is not enough: the answer lists every table the SQL reads,
// so another member's private query must 404 here exactly as it does on GET.
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
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	q, ok := loadSavedQuery(c, id, userID)
	if !ok {
		return
	}

	resp, err := resolveUpstreams(c.Request.Context(), database, upstreamLookup{
		SavedQueryID: id,
		SQLText:      q.SQLText,
		ConnectionID: q.ConnectionID,
		WorkspaceID:  q.WorkspaceID,
		UserID:       userID,
	})
	if err != nil {
		log.WithError(err).Error("upstream suggestion: could not resolve table references")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to look up upstream producers"})
		return
	}
	c.JSON(http.StatusOK, resp)
}

// upstreamLookup is the model whose inputs are being resolved, and who is asking.
type upstreamLookup struct {
	SavedQueryID string
	SQLText      string
	ConnectionID string
	WorkspaceID  string
	// UserID decides which private models are visible as producers.
	UserID string
}

// resolveUpstreams loads the producers that could feed this model and hands them to
// buildUpstreamSuggestion. It is separated from the HTTP layer so it can be tested
// against a database without a router; the rule itself lives in the builder, which
// needs no database at all.
func resolveUpstreams(
	ctx context.Context,
	database *sql.DB,
	in upstreamLookup,
) (upstreamSuggestionResponse, error) {
	refs := validators.ExtractTableReferences(in.SQLText)
	if len(refs) == 0 {
		return buildUpstreamSuggestion(nil, nil, in.SavedQueryID), nil
	}

	// One query for all references rather than one per reference: a model with a dozen
	// inputs should not be a dozen round trips, and the match is a simple membership
	// test on two candidate spellings per reference.
	//
	// Comparison is lower() here because matchTableReference compares lower(); the two
	// have to agree or this query fetches rows the matcher then refuses.
	wanted := make([]string, 0, len(refs)*2)
	for _, ref := range refs {
		wanted = append(wanted, strings.ToLower(ref.SchemaQualified()))
		// The bare name is fetched only for a reference that named no schema. This is a
		// PREFILTER, not the rule: matchTableReference decides what actually matches,
		// and it enforces the same condition independently. Widening this line changes
		// how many rows come back, never the answer — whereas dropping the guard in the
		// matcher changes the answer. Do not read this as making that one redundant.
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
		in.WorkspaceID, in.ConnectionID, pgdriver.StringArray(wanted))
	if err != nil {
		return upstreamSuggestionResponse{}, err
	}
	defer rows.Close()

	var producers []tableProducer
	for rows.Next() {
		p := tableProducer{ConnectionID: in.ConnectionID, Kind: assetKindPipeline}
		if err := rows.Scan(&p.Table.ProducerID, &p.Table.ProducerName, &p.Table.DestQualified, &p.Table.TableName); err != nil {
			return upstreamSuggestionResponse{}, err
		}
		producers = append(producers, p)
	}
	if err := rows.Err(); err != nil {
		return upstreamSuggestionResponse{}, err
	}

	// Models are not prefiltered by name the way pipelines are: target_table holds
	// whatever the user typed, and the split that turns it into a schema and a table
	// lives in Go (splitModelTarget). A SQL spelling of that split would be a second
	// copy of it, free to drift from the one the asset graph uses. One connection's
	// materialized models is a list people wrote by hand, so reading all of it is cheap.
	//
	// The visibility predicate is the saved-query list's own: a private model is its
	// author's, and naming it as a producer would disclose it to everyone else. It
	// applies to both materializations alike — a statement model's SQL names its tables
	// as plainly as a table model's target_table does.
	//
	// sql_text is read for statement models, which record no target. Filtering this to
	// 'table' alone is how statement models went unoffered while modelProducedTables,
	// and every unit test of it, already knew how to read them.
	modelRows, err := database.QueryContext(ctx, `
		SELECT sq.id::text,
		       COALESCE(sq.name, ''),
		       sq.materialization,
		       COALESCE(sq.target_table, ''),
		       COALESCE(sq.sql_text, '')
		FROM saved_queries sq
		WHERE sq.workspace_id = $1::uuid
		  AND sq.connection_id = $2::uuid
		  AND sq.materialization IN ('table', 'statement')
		  AND (sq.visibility = 'workspace' OR sq.created_by = $3)`,
		in.WorkspaceID, in.ConnectionID, in.UserID)
	if err != nil {
		return upstreamSuggestionResponse{}, err
	}
	defer modelRows.Close()

	for modelRows.Next() {
		var id, name, materialization, target, sqlText string
		if err := modelRows.Scan(&id, &name, &materialization, &target, &sqlText); err != nil {
			return upstreamSuggestionResponse{}, err
		}
		for _, table := range modelProducedTables(id, name, materialization, target, sqlText) {
			producers = append(producers, tableProducer{
				ConnectionID: in.ConnectionID, Kind: assetKindModel, Table: table,
			})
		}
	}
	if err := modelRows.Err(); err != nil {
		return upstreamSuggestionResponse{}, err
	}

	return buildUpstreamSuggestion(refs, producers, in.SavedQueryID), nil
}

// buildUpstreamSuggestion pairs each table the model reads with the producers that
// write it. Every producer passed in must be on the model's own connection — the
// loader guarantees that, and it is why table identity below needs no connection.
//
// The rule itself lives in matchTableReference, shared with the workspace asset graph:
// the graph is what this suggestion is drawn on, so the two cannot be allowed to
// disagree about which producer feeds which table.
func buildUpstreamSuggestion(
	refs []validators.TableRef,
	producers []tableProducer,
	selfID string,
) upstreamSuggestionResponse {
	resp := upstreamSuggestionResponse{
		References: []string{},
		Unresolved: []string{},
		Candidates: []upstreamCandidate{},
	}

	tables := make([]producedTable, 0, len(producers))
	// A producer id is a uuid, unique across pipelines and models alike, so the id is
	// enough to recover which kind a match came from.
	kindOf := make(map[string]string, len(producers))
	for _, p := range producers {
		tables = append(tables, p.Table)
		kindOf[p.Table.ProducerID] = p.Kind
	}

	seen := map[string]bool{}
	for _, ref := range refs {
		resp.References = append(resp.References, ref.Qualified())

		matches, qualified := matchTableReference(ref, tables)
		// A model that reads the table it builds is an incremental pattern, not its own
		// upstream. Offering it would invite a schedule that waits on itself — which the
		// cycle check refuses anyway, one click after the dialog suggested it.
		kept := matches[:0:0]
		for _, m := range matches {
			if m.ProducerID != selfID {
				kept = append(kept, m)
			}
		}
		if len(kept) == 0 {
			resp.Unresolved = append(resp.Unresolved, ref.Qualified())
			continue
		}

		hitTables := map[string]bool{}
		for _, m := range kept {
			hitTables[producedTableKey("", m)] = true

			key := m.ProducerID + "\x00" + ref.Qualified()
			if seen[key] {
				continue
			}
			seen[key] = true
			table := m.DestQualified
			if table == "" {
				table = m.TableName
			}
			resp.Candidates = append(resp.Candidates, upstreamCandidate{
				Kind:             kindOf[m.ProducerID],
				ID:               m.ProducerID,
				Name:             m.ProducerName,
				Table:            table,
				MatchedReference: ref.Qualified(),
				Qualified:        qualified,
			})
		}
		if len(hitTables) > 1 {
			resp.Ambiguous = true
		}
	}

	return resp
}
