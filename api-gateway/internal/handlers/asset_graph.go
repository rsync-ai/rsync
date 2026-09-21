package handlers

import (
	"context"
	"database/sql"
	"net/http"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	"api-gateway/internal/db"
	"api-gateway/internal/security"
	"api-gateway/internal/validators"
)

// The workspace's data assets and what depends on what, as one graph.
//
// Today the product knows its dependencies one question at a time: a schedule dialog
// asks "which pipeline feeds THIS model?", a pipeline detail page draws THAT pipeline's
// steps. Nothing holds the whole shape, so nothing can answer the questions that are
// only askable about the whole shape — which model is stale because two hops upstream
// failed, which producers refresh nothing downstream, where a chain is one manual step
// away from being automatic.
//
// This endpoint derives that shape and reports it. It is deliberately DERIVATION ONLY:
// it writes nothing, schedules nothing and triggers nothing. The graph is the thing
// orchestration will eventually be hung off, and a graph nobody can look at first is a
// graph nobody should be orchestrating on.
//
// The four edge kinds do not have equal standing, and the response says so on every
// edge rather than flattening them into one "depends on":
//
//   - writes (pipeline -> table)      OBSERVED. A run actually recorded this table.
//   - materializes (model -> table)   DECLARED for a 'table' model: its target_table
//                                     says it builds this table. INFERRED for a
//                                     'statement' model: parsed out of its SQL.
//   - reads (table -> model)          INFERRED. Parsed out of the model's SQL.
//   - triggers (pipeline -> model)    DECLARED. Someone configured this schedule.
//
// An inferred edge is a suggestion; a declared edge is a statement of intent that may
// never have run; only an observed edge is evidence something happened. Collapsing the
// three would make the graph look far more authoritative than it is.
//
// The interesting number in the response is not the edge count. It is
// uncovered_upstreams: producers a model depends on that do not cause it to refresh.
// Every one of those is a model whose freshness depends on someone remembering — which is
// the gap a freshness deadline closes from the other side (saved_query_freshness.go). This
// graph says the wiring is incomplete; the deadline says the table actually went stale.
// Neither subsumes the other: a fully wired model whose rebuild keeps failing is invisible
// here, and a model nobody reads can be uncovered forever without mattering.

const (
	assetKindPipeline = "pipeline"
	assetKindTable    = "table"
	assetKindModel    = "model"

	assetEdgeWrites       = "writes"
	assetEdgeMaterializes = "materializes"
	assetEdgeReads        = "reads"
	assetEdgeTriggers     = "triggers"

	// Evidence grades. See the header: the difference between these is the difference
	// between "this happened" and "someone typed this".
	assetEvidenceObserved = "observed"
	assetEvidenceDeclared = "declared"
	assetEvidenceInferred = "inferred"

	// triggerManualRefresh is the reported trigger for a model with no live schedule.
	// It is not a fourth schedule_type: it is the absence of one, named so the UI does
	// not have to interpret an empty string.
	triggerManualRefresh = "manual"

	// Caps. A workspace with more assets than this gets a truncated graph and is TOLD
	// it was truncated, because a graph silently missing a producer draws a model as
	// having no upstream at all — which is the exact claim this endpoint exists to
	// make, and would be false.
	assetGraphPipelineLimit = 500
	assetGraphModelLimit    = 500
	assetGraphProducedLimit = 5000
)

// assetNode is one thing in the workspace that data lives in or moves through.
type assetNode struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
	Name string `json:"name"`
	// RefID is the underlying pipelines.id / saved_queries.id, so the UI can link to
	// the thing itself. Empty for table nodes: a table is not a row we own.
	RefID string `json:"ref_id,omitempty"`
	// ConnectionID is the warehouse the node lives in. Set on tables and models, and
	// on pipelines it is the DESTINATION connection — where the pipeline writes, not
	// where it reads, because that is the side the rest of the graph joins on.
	ConnectionID string `json:"connection_id,omitempty"`
}

type assetEdge struct {
	From     string `json:"from"`
	To       string `json:"to"`
	Kind     string `json:"kind"`
	Evidence string `json:"evidence"`
}

// modelRefresh answers "when does this model get rebuilt, and does that cover what it
// depends on?" for one model. This is the part of the response that is an argument
// rather than a picture.
type modelRefresh struct {
	ModelID string `json:"model_id"`
	NodeID  string `json:"node_id"`
	Name    string `json:"name"`
	// Trigger is the live schedule_type, or "manual" when the model has no schedule.
	Trigger string `json:"trigger"`
	// TriggerUpstreams are the producer node ids that cause this model to refresh, empty
	// for a clock-scheduled or manual model. A schedule can name several since migration
	// 100, and a named producer that is not in THIS graph — filtered out by connection,
	// or past the node limit — is dropped rather than emitted as an id pointing at no
	// node the caller can draw.
	TriggerUpstreams []string `json:"trigger_upstreams"`
	// TriggerPaused separates "configured but not firing" from "not configured". A
	// paused schedule still describes intent and still occupies the model's one
	// schedule slot, so hiding it would misreport why the model is stale.
	TriggerPaused bool `json:"trigger_paused"`
	// Upstreams are the producer node ids this model reads from, transitively through
	// exactly one table hop.
	Upstreams []string `json:"upstreams"`
	// UncoveredUpstreams are the producers that do NOT cause this model to refresh.
	// For a clock-scheduled or manual model that is every upstream it has: a cron is
	// not a causal link, it is a guess about when the upstream is probably done.
	UncoveredUpstreams []string `json:"uncovered_upstreams"`
	// Unresolved are references in the model's SQL that matched no known producer.
	// Reported rather than dropped, and given no phantom node: the honest answer to
	// "where does this table come from" is sometimes "we do not know".
	Unresolved []string `json:"unresolved"`
	// Ambiguous means some reference in the SQL could be more than one DIFFERENT
	// table, so the upstream list contains a producer that may not be the real one.
	// Two producers of the same table is fan-in and is not reported here.
	Ambiguous bool `json:"ambiguous"`
}

type assetGraphTruncation struct {
	Pipelines      bool `json:"pipelines"`
	ProducedTables bool `json:"produced_tables"`
	Models         bool `json:"models"`
}

type assetGraphStats struct {
	Pipelines int            `json:"pipelines"`
	Tables    int            `json:"tables"`
	Models    int            `json:"models"`
	Edges     map[string]int `json:"edges"`
	// ModelsWithUncoveredUpstreams is the count of models that depend on at least one
	// producer that does not refresh them.
	ModelsWithUncoveredUpstreams int `json:"models_with_uncovered_upstreams"`
	ModelsWithUnresolvedRefs     int `json:"models_with_unresolved_references"`
	// ModelsWithMultipleUpstreams counts models whose lineage has two or more distinct
	// producers. This was reported as fan_in_blocked until migration 100, and the old
	// name was accurate then: a schedule held exactly ONE upstream column, so every model
	// in this count could not be wired to all of its producers however the user
	// configured it. The upstreams are a child table now and that limit is gone, so the
	// number no longer describes an impossibility — it describes a model worth checking,
	// and ModelsWithUncoveredUpstreams is the one that says whether it was wired up.
	ModelsWithMultipleUpstreams int                  `json:"models_with_multiple_upstreams"`
	Truncated                   assetGraphTruncation `json:"truncated"`
}

type assetGraph struct {
	Nodes  []assetNode     `json:"nodes"`
	Edges  []assetEdge     `json:"edges"`
	Models []modelRefresh  `json:"models"`
	Stats  assetGraphStats `json:"stats"`
}

// pipelineAsset is one pipeline, as the graph needs it.
type pipelineAsset struct {
	ID           string
	Name         string
	ConnectionID string
}

// modelAsset is one saved query plus whatever schedule it currently holds.
type modelAsset struct {
	ID              string
	Name            string
	SQLText         string
	Materialization string
	TargetTable     string
	ConnectionID    string
	// ScheduleType is "" when the model holds no schedule.
	ScheduleType   string
	ScheduleStatus string
	// TriggerUpstreamIDs are the raw producer ids the schedule waits on, in no
	// particular kind order — see the aggregate in loadAssetGraphInput.
	TriggerUpstreamIDs []string
}

// tableProducer is one producer of one table, carrying the connection that table lives
// in. Connection is part of the identity and not a detail: see producedTableKey.
type tableProducer struct {
	ConnectionID string
	Kind         string
	Table        producedTable
}

type assetGraphInput struct {
	Pipelines []pipelineAsset
	Produced  []tableProducer
	Models    []modelAsset
	Truncated assetGraphTruncation
}

// GetWorkspaceAssetGraph returns the workspace's asset graph.
// GET /api/v1/explorer/asset-graph?connection_id=<uuid>
//
// Viewer-level: everything here is derived from pipelines and saved queries the caller
// can already list, and the private saved queries of other users are excluded exactly
// as they are in the list endpoint.
func GetWorkspaceAssetGraph(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	if _, ok := requireWorkspaceRole(c, security.WSViewer); !ok {
		return
	}
	workspaceID, ok := resolveActiveWorkspace(c)
	if !ok {
		return
	}
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	connectionID := strings.TrimSpace(c.Query("connection_id"))

	in, err := loadAssetGraphInput(c.Request.Context(), database, workspaceID, userID, connectionID)
	if err != nil {
		log.WithError(err).Error("asset graph: could not load workspace assets")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to build asset graph"})
		return
	}
	c.JSON(http.StatusOK, buildAssetGraph(in))
}

// loadAssetGraphInput fetches everything the derivation needs in three queries.
//
// Three, and not one per model: a workspace with fifty models would otherwise spend
// fifty round trips answering a question that is one join and some string matching.
// The matching itself deliberately happens in Go rather than SQL — it is the same rule
// the schedule dialog uses (matchTableReference), and a second copy of it written in
// SQL is exactly the drift that rule was extracted to prevent.
func loadAssetGraphInput(
	ctx context.Context,
	database *sql.DB,
	workspaceID, userID, connectionID string,
) (assetGraphInput, error) {
	var in assetGraphInput

	pipeRows, err := database.QueryContext(ctx, `
		SELECT p.id::text,
		       COALESCE(p.name, ''),
		       COALESCE(p.destination_connection_id::text, '')
		FROM pipelines p
		WHERE p.workspace_id = $1::uuid
		  AND ($2 = '' OR p.destination_connection_id::text = $2)
		ORDER BY p.created_at, p.id
		LIMIT $3`, workspaceID, connectionID, assetGraphPipelineLimit+1)
	if err != nil {
		return in, err
	}
	defer pipeRows.Close()
	for pipeRows.Next() {
		var p pipelineAsset
		if err := pipeRows.Scan(&p.ID, &p.Name, &p.ConnectionID); err != nil {
			return in, err
		}
		in.Pipelines = append(in.Pipelines, p)
	}
	if err := pipeRows.Err(); err != nil {
		return in, err
	}
	if len(in.Pipelines) > assetGraphPipelineLimit {
		in.Pipelines = in.Pipelines[:assetGraphPipelineLimit]
		in.Truncated.Pipelines = true
	}

	// Every destination table any run has recorded, not only the ones some model
	// happens to read. A pipeline whose output nothing consumes is a finding, and it is
	// invisible if the query is filtered by what the models asked for.
	//
	// destination_qualified_name IS NOT NULL is the same guard the upstream suggestion
	// uses: a row with no destination namespace cannot be placed in any warehouse, so
	// it cannot honestly be drawn as a table in one.
	prodRows, err := database.QueryContext(ctx, `
		SELECT DISTINCT
		    p.id::text,
		    COALESCE(p.name, ''),
		    COALESCE(p.destination_connection_id::text, ''),
		    COALESCE(s.destination_qualified_name, ''),
		    COALESCE(s.table_name, '')
		FROM pipeline_run_table_stats s
		JOIN pipelines p ON p.id = s.pipeline_id
		WHERE p.workspace_id = $1::uuid
		  AND ($2 = '' OR p.destination_connection_id::text = $2)
		  AND s.destination_qualified_name IS NOT NULL
		ORDER BY 1, 4, 5
		LIMIT $3`, workspaceID, connectionID, assetGraphProducedLimit+1)
	if err != nil {
		return in, err
	}
	defer prodRows.Close()
	for prodRows.Next() {
		tp := tableProducer{Kind: assetKindPipeline}
		if err := prodRows.Scan(&tp.Table.ProducerID, &tp.Table.ProducerName,
			&tp.ConnectionID, &tp.Table.DestQualified, &tp.Table.TableName); err != nil {
			return in, err
		}
		in.Produced = append(in.Produced, tp)
	}
	if err := prodRows.Err(); err != nil {
		return in, err
	}
	if len(in.Produced) > assetGraphProducedLimit {
		in.Produced = in.Produced[:assetGraphProducedLimit]
		in.Truncated.ProducedTables = true
	}

	// status != 'deleted' rather than = 'active': a paused schedule still occupies the
	// model's one schedule slot and still says what the user intended, and reporting it
	// as "manual" would answer the freshness question wrongly.
	modelRows, err := database.QueryContext(ctx, `
		SELECT sq.id::text,
		       COALESCE(sq.name, ''),
		       sq.sql_text,
		       sq.materialization,
		       COALESCE(sq.target_table, ''),
		       COALESCE(sq.connection_id::text, ''),
		       COALESCE(s.schedule_type, ''),
		       COALESCE(s.status, ''),
		       COALESCE((
		           SELECT string_agg(COALESCE(u.upstream_pipeline_id::text, u.upstream_saved_query_id::text), ',')
		           FROM saved_query_schedule_upstreams u
		           WHERE u.schedule_id = s.schedule_id
		       ), '')
		FROM saved_queries sq
		LEFT JOIN saved_query_schedules s
		  ON s.saved_query_id = sq.id AND s.status != 'deleted'
		WHERE sq.workspace_id = $1
		  AND (sq.visibility = 'workspace' OR sq.created_by = $2)
		  AND ($3 = '' OR sq.connection_id::text = $3)
		ORDER BY sq.updated_at DESC, sq.id
		LIMIT $4`, workspaceID, userID, connectionID, assetGraphModelLimit+1)
	if err != nil {
		return in, err
	}
	defer modelRows.Close()
	for modelRows.Next() {
		var m modelAsset
		// Aggregated to one comma-joined string rather than returned as an array, and
		// rather than as extra rows. Extra rows would multiply every model by its upstream
		// count and turn the LIMIT above into a limit on upstream rows; an array would
		// need pq.Array, which this module does not have since the move to pgx.
		var upstreamIDs string
		if err := modelRows.Scan(&m.ID, &m.Name, &m.SQLText, &m.Materialization,
			&m.TargetTable, &m.ConnectionID, &m.ScheduleType, &m.ScheduleStatus,
			&upstreamIDs); err != nil {
			return in, err
		}
		if upstreamIDs != "" {
			m.TriggerUpstreamIDs = strings.Split(upstreamIDs, ",")
		}
		in.Models = append(in.Models, m)
	}
	if err := modelRows.Err(); err != nil {
		return in, err
	}
	if len(in.Models) > assetGraphModelLimit {
		in.Models = in.Models[:assetGraphModelLimit]
		in.Truncated.Models = true
	}

	return in, nil
}

// buildAssetGraph is the whole derivation, and takes no database on purpose: every
// claim this endpoint makes is a claim about this function, so it has to be testable
// without a Postgres to hand.
func buildAssetGraph(in assetGraphInput) assetGraph {
	g := assetGraph{
		Nodes:  []assetNode{},
		Edges:  []assetEdge{},
		Models: []modelRefresh{},
		Stats:  assetGraphStats{Edges: map[string]int{}, Truncated: in.Truncated},
	}

	nodes := map[string]assetNode{}
	addNode := func(n assetNode) {
		if _, ok := nodes[n.ID]; !ok {
			nodes[n.ID] = n
		}
	}
	edges := map[assetEdge]bool{}
	addEdge := func(e assetEdge) {
		edges[e] = true
	}

	// Producers indexed by connection, because a table name only identifies a table
	// inside one warehouse. A model is matched against its own connection's producers
	// and nothing else — the same scoping the upstream suggestion enforces in SQL.
	byConnection := map[string][]producedTable{}
	pipelineByID := map[string]pipelineAsset{}
	for _, p := range in.Pipelines {
		pipelineByID[p.ID] = p
		addNode(assetNode{
			ID:           assetKindPipeline + ":" + p.ID,
			Kind:         assetKindPipeline,
			Name:         p.Name,
			RefID:        p.ID,
			ConnectionID: p.ConnectionID,
		})
	}

	registerProducer := func(tp tableProducer, producerNodeID, edgeKind, evidence string) {
		key := producedTableKey(tp.ConnectionID, tp.Table)
		tableNodeID := assetKindTable + ":" + key
		display := tp.Table.DestQualified
		if display == "" {
			display = tp.Table.TableName
		}
		addNode(assetNode{
			ID:           tableNodeID,
			Kind:         assetKindTable,
			Name:         display,
			ConnectionID: tp.ConnectionID,
		})
		byConnection[tp.ConnectionID] = append(byConnection[tp.ConnectionID], tp.Table)
		addEdge(assetEdge{From: producerNodeID, To: tableNodeID, Kind: edgeKind, Evidence: evidence})
	}

	for _, tp := range in.Produced {
		// A stats row whose pipeline fell outside the page is not drawable: its
		// producer node does not exist, and an edge from a node that is not in the
		// response is worse than a missing edge.
		if _, ok := pipelineByID[tp.Table.ProducerID]; !ok {
			continue
		}
		registerProducer(tp, assetKindPipeline+":"+tp.Table.ProducerID, assetEdgeWrites, assetEvidenceObserved)
	}

	// A model that writes a table is a producer of it, exactly like a pipeline: a
	// 'table' model its target_table, a 'statement' model the tables its own SQL names
	// (modelProducedTables — the same rule the upstream suggestion uses, so the graph
	// and the picker cannot disagree about who writes a table). This is what makes
	// model -> table -> model chains derivable at all: the product has no model-to-model
	// trigger, but it does have model-to-model LINEAGE, and the two are different claims.
	for _, m := range in.Models {
		addNode(assetNode{
			ID:           assetKindModel + ":" + m.ID,
			Kind:         assetKindModel,
			Name:         m.Name,
			RefID:        m.ID,
			ConnectionID: m.ConnectionID,
		})
		// A target_table is typed by a person; a statement's target is read out of SQL,
		// which is the same grade as a reads edge.
		evidence := assetEvidenceDeclared
		if m.Materialization == matStatement {
			evidence = assetEvidenceInferred
		}
		for _, table := range modelProducedTables(m.ID, m.Name, m.Materialization, m.TargetTable, m.SQLText) {
			registerProducer(
				tableProducer{ConnectionID: m.ConnectionID, Kind: assetKindModel, Table: table},
				assetKindModel+":"+m.ID, assetEdgeMaterializes, evidence,
			)
		}
	}

	producerKindOf := map[string]string{}
	for _, p := range in.Pipelines {
		producerKindOf[p.ID] = assetKindPipeline
	}
	for _, m := range in.Models {
		producerKindOf[m.ID] = assetKindModel
	}
	producerNodeID := func(id string) string {
		if kind, ok := producerKindOf[id]; ok {
			return kind + ":" + id
		}
		return ""
	}

	for _, m := range in.Models {
		modelNodeID := assetKindModel + ":" + m.ID
		summary := modelRefresh{
			ModelID:            m.ID,
			NodeID:             modelNodeID,
			Name:               m.Name,
			Trigger:            triggerManualRefresh,
			TriggerPaused:      m.ScheduleStatus == "paused",
			Upstreams:          []string{},
			UncoveredUpstreams: []string{},
			TriggerUpstreams:   []string{},
			Unresolved:         []string{},
		}
		if m.ScheduleType != "" {
			summary.Trigger = m.ScheduleType
		}

		upstreams := map[string]bool{}
		for _, ref := range validators.ExtractTableReferences(m.SQLText) {
			matches, _ := matchTableReference(ref, byConnection[m.ConnectionID])
			// A model that materializes into a table it also reads is an incremental
			// pattern, not a dependency on itself. Drawing the self-loop would put a
			// cycle in a graph whose whole purpose is ordering.
			kept := matches[:0:0]
			for _, match := range matches {
				if match.ProducerID != m.ID {
					kept = append(kept, match)
				}
			}
			if len(kept) == 0 {
				summary.Unresolved = append(summary.Unresolved, ref.Qualified())
				continue
			}
			// Two producers of the SAME table is fan-in, not ambiguity: both of them
			// really do write it. Ambiguity is a reference that could be two DIFFERENT
			// tables, which is only reachable through an unqualified match — `orders`
			// hitting both analytics.orders and staging.orders. Conflating the two
			// would report every fan-in as an uncertainty and hide the real ones.
			hitTables := map[string]bool{}
			for _, match := range kept {
				tableNodeID := assetKindTable + ":" + producedTableKey(m.ConnectionID, match)
				hitTables[tableNodeID] = true
				addEdge(assetEdge{
					From: tableNodeID, To: modelNodeID,
					Kind: assetEdgeReads, Evidence: assetEvidenceInferred,
				})
				if nodeID := producerNodeID(match.ProducerID); nodeID != "" {
					upstreams[nodeID] = true
				}
			}
			if len(hitTables) > 1 {
				summary.Ambiguous = true
			}
		}

		// The trigger edges are drawn from the schedule, not from the lineage: a user may
		// point a trigger at a producer this model does not read, and that is a fact about
		// the workspace worth showing rather than correcting.
		triggerNodes := map[string]bool{}
		if m.ScheduleType == scheduleAfterUpstream {
			for _, upstreamID := range m.TriggerUpstreamIDs {
				// producerNodeID answers both questions at once: what node id this producer
				// has, and whether it is in this graph at all. It needs no kind alongside
				// the id because a producer id is a uuid, unique across pipelines and
				// models alike — which is also why the aggregate above does not select one.
				nodeID := producerNodeID(upstreamID)
				if nodeID == "" {
					continue
				}
				triggerNodes[nodeID] = true
				addEdge(assetEdge{
					From: nodeID, To: modelNodeID,
					Kind: assetEdgeTriggers, Evidence: assetEvidenceDeclared,
				})
			}
		}
		for id := range triggerNodes {
			summary.TriggerUpstreams = append(summary.TriggerUpstreams, id)
		}

		for id := range upstreams {
			summary.Upstreams = append(summary.Upstreams, id)
			if !triggerNodes[id] {
				summary.UncoveredUpstreams = append(summary.UncoveredUpstreams, id)
			}
		}
		sort.Strings(summary.Upstreams)
		sort.Strings(summary.UncoveredUpstreams)
		sort.Strings(summary.TriggerUpstreams)

		if len(summary.UncoveredUpstreams) > 0 {
			g.Stats.ModelsWithUncoveredUpstreams++
		}
		if len(summary.Unresolved) > 0 {
			g.Stats.ModelsWithUnresolvedRefs++
		}
		if len(summary.Upstreams) > 1 {
			g.Stats.ModelsWithMultipleUpstreams++
		}
		g.Models = append(g.Models, summary)
	}

	for _, n := range nodes {
		g.Nodes = append(g.Nodes, n)
		switch n.Kind {
		case assetKindPipeline:
			g.Stats.Pipelines++
		case assetKindTable:
			g.Stats.Tables++
		case assetKindModel:
			g.Stats.Models++
		}
	}
	for e := range edges {
		g.Edges = append(g.Edges, e)
		g.Stats.Edges[e.Kind]++
	}

	// Map iteration is randomised, and a graph whose node order changes between two
	// identical requests makes every diff and every test worthless.
	sort.Slice(g.Nodes, func(i, j int) bool {
		if g.Nodes[i].Kind != g.Nodes[j].Kind {
			return g.Nodes[i].Kind < g.Nodes[j].Kind
		}
		return g.Nodes[i].ID < g.Nodes[j].ID
	})
	sort.Slice(g.Edges, func(i, j int) bool {
		if g.Edges[i].Kind != g.Edges[j].Kind {
			return g.Edges[i].Kind < g.Edges[j].Kind
		}
		if g.Edges[i].From != g.Edges[j].From {
			return g.Edges[i].From < g.Edges[j].From
		}
		return g.Edges[i].To < g.Edges[j].To
	})

	return g
}

// splitModelTarget reads a stored target_table into the same shape a pipeline's stats
// row produces, so one matcher serves both.
//
// It is lenient where validateModelTarget is strict, and that asymmetry is deliberate:
// validation guards what goes IN, and refusing to draw a row that is already in the
// table would just make the graph quietly incomplete.
func splitModelTarget(raw string) producedTable {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return producedTable{}
	}
	parts := strings.Split(raw, ".")
	name := strings.TrimSpace(parts[len(parts)-1])
	if name == "" {
		return producedTable{}
	}
	if len(parts) == 1 {
		return producedTable{TableName: name}
	}
	schema := strings.TrimSpace(parts[len(parts)-2])
	if schema == "" {
		return producedTable{TableName: name}
	}
	return producedTable{DestQualified: schema + "." + name, TableName: name}
}
