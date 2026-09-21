package handlers

import (
	"reflect"
	"sort"
	"testing"

	"api-gateway/internal/validators"
)

// buildAssetGraph is deliberately database-free, so every claim the asset-graph
// endpoint makes is provable here, in the DEFAULT test suite. The upstream resolver it
// shares its matching rule with is only covered behind `//go:build integration_pg`,
// which CI never passes — so these are the tests that actually run.

const (
	connA = "11111111-1111-1111-1111-111111111111"
	connB = "22222222-2222-2222-2222-222222222222"
)

func pipeWrites(pipelineID, name, conn, destQualified, table string) tableProducer {
	return tableProducer{
		ConnectionID: conn,
		Kind:         assetKindPipeline,
		Table: producedTable{
			ProducerID:    pipelineID,
			ProducerName:  name,
			DestQualified: destQualified,
			TableName:     table,
		},
	}
}

func edgeKinds(g assetGraph, kind string) []assetEdge {
	var out []assetEdge
	for _, e := range g.Edges {
		if e.Kind == kind {
			out = append(out, e)
		}
	}
	return out
}

func modelByID(t *testing.T, g assetGraph, id string) modelRefresh {
	t.Helper()
	for _, m := range g.Models {
		if m.ModelID == id {
			return m
		}
	}
	t.Fatalf("model %s not in graph", id)
	return modelRefresh{}
}

func hasNode(g assetGraph, id string) bool {
	for _, n := range g.Nodes {
		if n.ID == id {
			return true
		}
	}
	return false
}

// A pipeline that lands a table and a model that reads it is the base case: three
// nodes, two edges, and one upstream that nothing refreshes.
func TestAssetGraph_PipelineToModel(t *testing.T) {
	g := buildAssetGraph(assetGraphInput{
		Pipelines: []pipelineAsset{{ID: "p1", Name: "Orders sync", ConnectionID: connA}},
		Produced:  []tableProducer{pipeWrites("p1", "Orders sync", connA, "analytics.orders", "orders")},
		Models: []modelAsset{{
			ID: "m1", Name: "Daily orders", ConnectionID: connA,
			SQLText: "SELECT * FROM analytics.orders",
		}},
	})

	if !hasNode(g, "pipeline:p1") || !hasNode(g, "model:m1") {
		t.Fatalf("expected pipeline and model nodes, got %+v", g.Nodes)
	}
	if g.Stats.Tables != 1 {
		t.Fatalf("expected one table node, got %d (%+v)", g.Stats.Tables, g.Nodes)
	}
	if got := len(edgeKinds(g, assetEdgeWrites)); got != 1 {
		t.Errorf("writes edges = %d, want 1", got)
	}
	if got := len(edgeKinds(g, assetEdgeReads)); got != 1 {
		t.Errorf("reads edges = %d, want 1", got)
	}
	for _, e := range edgeKinds(g, assetEdgeWrites) {
		if e.Evidence != assetEvidenceObserved {
			t.Errorf("a writes edge came from a recorded run; evidence = %q", e.Evidence)
		}
	}
	for _, e := range edgeKinds(g, assetEdgeReads) {
		if e.Evidence != assetEvidenceInferred {
			t.Errorf("a reads edge is parsed out of SQL; evidence = %q", e.Evidence)
		}
	}

	m := modelByID(t, g, "m1")
	if m.Trigger != triggerManualRefresh {
		t.Errorf("a model with no schedule refreshes manually; trigger = %q", m.Trigger)
	}
	if !reflect.DeepEqual(m.Upstreams, []string{"pipeline:p1"}) {
		t.Errorf("upstreams = %v, want [pipeline:p1]", m.Upstreams)
	}
	if !reflect.DeepEqual(m.UncoveredUpstreams, []string{"pipeline:p1"}) {
		t.Errorf("nothing refreshes this model, so its upstream is uncovered; got %v", m.UncoveredUpstreams)
	}
	if g.Stats.ModelsWithUncoveredUpstreams != 1 {
		t.Errorf("models_with_uncovered_upstreams = %d, want 1", g.Stats.ModelsWithUncoveredUpstreams)
	}
	if g.Stats.ModelsWithMultipleUpstreams != 0 {
		t.Errorf("one upstream is not several; models_with_multiple_upstreams = %d", g.Stats.ModelsWithMultipleUpstreams)
	}
}

// An after_upstream trigger pointed at the pipeline that feeds the model is the one
// arrangement where nothing is uncovered.
func TestAssetGraph_TriggerCoversItsUpstream(t *testing.T) {
	g := buildAssetGraph(assetGraphInput{
		Pipelines: []pipelineAsset{{ID: "p1", Name: "Orders sync", ConnectionID: connA}},
		Produced:  []tableProducer{pipeWrites("p1", "Orders sync", connA, "analytics.orders", "orders")},
		Models: []modelAsset{{
			ID: "m1", Name: "Daily orders", ConnectionID: connA,
			SQLText:      "SELECT * FROM analytics.orders",
			ScheduleType: scheduleAfterUpstream, ScheduleStatus: "active",
			TriggerUpstreamIDs: []string{"p1"},
		}},
	})

	m := modelByID(t, g, "m1")
	if m.Trigger != scheduleAfterUpstream || !reflect.DeepEqual(m.TriggerUpstreams, []string{"pipeline:p1"}) {
		t.Fatalf("trigger = %q/%v, want %s/[pipeline:p1]", m.Trigger, m.TriggerUpstreams, scheduleAfterUpstream)
	}
	if len(m.UncoveredUpstreams) != 0 {
		t.Errorf("the trigger is the upstream, so nothing is uncovered; got %v", m.UncoveredUpstreams)
	}
	if got := len(edgeKinds(g, assetEdgeTriggers)); got != 1 {
		t.Fatalf("triggers edges = %d, want 1", got)
	}
	if e := edgeKinds(g, assetEdgeTriggers)[0]; e.From != "pipeline:p1" || e.To != "model:m1" {
		t.Errorf("trigger edge = %s -> %s, want pipeline:p1 -> model:m1", e.From, e.To)
	}
	if g.Stats.ModelsWithUncoveredUpstreams != 0 {
		t.Errorf("models_with_uncovered_upstreams = %d, want 0", g.Stats.ModelsWithUncoveredUpstreams)
	}
}

// A trigger may point at a pipeline the model does not read. That is a real
// configuration and the graph reports both facts rather than reconciling them.
func TestAssetGraph_TriggerUnrelatedToLineage(t *testing.T) {
	g := buildAssetGraph(assetGraphInput{
		Pipelines: []pipelineAsset{
			{ID: "p1", Name: "Orders sync", ConnectionID: connA},
			{ID: "p2", Name: "Unrelated", ConnectionID: connA},
		},
		Produced: []tableProducer{pipeWrites("p1", "Orders sync", connA, "analytics.orders", "orders")},
		Models: []modelAsset{{
			ID: "m1", Name: "Daily orders", ConnectionID: connA,
			SQLText:      "SELECT * FROM analytics.orders",
			ScheduleType: scheduleAfterUpstream, ScheduleStatus: "active",
			TriggerUpstreamIDs: []string{"p2"},
		}},
	})

	m := modelByID(t, g, "m1")
	if !reflect.DeepEqual(m.Upstreams, []string{"pipeline:p1"}) {
		t.Errorf("upstreams = %v, want [pipeline:p1]", m.Upstreams)
	}
	if !reflect.DeepEqual(m.UncoveredUpstreams, []string{"pipeline:p1"}) {
		t.Errorf("the trigger is a different pipeline, so p1 stays uncovered; got %v", m.UncoveredUpstreams)
	}
	if got := len(edgeKinds(g, assetEdgeTriggers)); got != 1 {
		t.Errorf("the trigger edge is still drawn; triggers = %d", got)
	}
}

// A cron is not a causal link. It fires on the clock whether or not the upstream landed,
// so every upstream of a clock-scheduled model is uncovered.
func TestAssetGraph_ClockScheduleCoversNothing(t *testing.T) {
	g := buildAssetGraph(assetGraphInput{
		Pipelines: []pipelineAsset{{ID: "p1", Name: "Orders sync", ConnectionID: connA}},
		Produced:  []tableProducer{pipeWrites("p1", "Orders sync", connA, "analytics.orders", "orders")},
		Models: []modelAsset{{
			ID: "m1", Name: "Daily orders", ConnectionID: connA,
			SQLText:      "SELECT * FROM analytics.orders",
			ScheduleType: "cron", ScheduleStatus: "active",
		}},
	})

	m := modelByID(t, g, "m1")
	if m.Trigger != "cron" {
		t.Fatalf("trigger = %q, want cron", m.Trigger)
	}
	if !reflect.DeepEqual(m.UncoveredUpstreams, []string{"pipeline:p1"}) {
		t.Errorf("a cron covers no upstream; uncovered = %v", m.UncoveredUpstreams)
	}
	if got := len(edgeKinds(g, assetEdgeTriggers)); got != 0 {
		t.Errorf("a cron draws no trigger edge; triggers = %d", got)
	}
}

// A paused schedule is not the same as no schedule: it still occupies the model's one
// schedule slot and still says what the user intended.
func TestAssetGraph_PausedScheduleIsReported(t *testing.T) {
	g := buildAssetGraph(assetGraphInput{
		Pipelines: []pipelineAsset{{ID: "p1", Name: "Orders sync", ConnectionID: connA}},
		Produced:  []tableProducer{pipeWrites("p1", "Orders sync", connA, "analytics.orders", "orders")},
		Models: []modelAsset{{
			ID: "m1", Name: "Daily orders", ConnectionID: connA,
			SQLText:      "SELECT * FROM analytics.orders",
			ScheduleType: scheduleAfterUpstream, ScheduleStatus: "paused",
			TriggerUpstreamIDs: []string{"p1"},
		}},
	})

	m := modelByID(t, g, "m1")
	if m.Trigger != scheduleAfterUpstream {
		t.Errorf("a paused schedule keeps its type; trigger = %q", m.Trigger)
	}
	if !m.TriggerPaused {
		t.Errorf("trigger_paused = false for a paused schedule")
	}
}

// A model that materializes a table is a producer of it, so model -> table -> model
// lineage is derivable from the SQL alone, with no schedule involved anywhere. Migration
// 100 added model-to-model TRIGGERING on top of that, and the two halves stay
// independent: this test pins the lineage half, TestAssetGraph_ModelTriggersAnotherModel
// pins the trigger half, and a model can have either without the other.
func TestAssetGraph_ModelMaterializesThenAnotherModelReads(t *testing.T) {
	g := buildAssetGraph(assetGraphInput{
		Pipelines: []pipelineAsset{{ID: "p1", Name: "Orders sync", ConnectionID: connA}},
		Produced:  []tableProducer{pipeWrites("p1", "Orders sync", connA, "analytics.orders", "orders")},
		Models: []modelAsset{
			{
				ID: "m1", Name: "Clean orders", ConnectionID: connA,
				SQLText:         "SELECT * FROM analytics.orders",
				Materialization: "table", TargetTable: "analytics.clean_orders",
			},
			{
				ID: "m2", Name: "Weekly revenue", ConnectionID: connA,
				SQLText: "SELECT sum(total) FROM analytics.clean_orders",
			},
		},
	})

	if got := len(edgeKinds(g, assetEdgeMaterializes)); got != 1 {
		t.Fatalf("materializes edges = %d, want 1", got)
	}
	if e := edgeKinds(g, assetEdgeMaterializes)[0]; e.From != "model:m1" || e.Evidence != assetEvidenceDeclared {
		t.Errorf("materializes edge = %s (%s), want model:m1 / declared", e.From, e.Evidence)
	}

	m2 := modelByID(t, g, "m2")
	if !reflect.DeepEqual(m2.Upstreams, []string{"model:m1"}) {
		t.Fatalf("m2 upstreams = %v, want [model:m1]", m2.Upstreams)
	}
	if len(m2.Unresolved) != 0 {
		t.Errorf("the materialized table is a known producer; unresolved = %v", m2.Unresolved)
	}
	// And the chain is a chain: m1 still hangs off the pipeline.
	m1 := modelByID(t, g, "m1")
	if !reflect.DeepEqual(m1.Upstreams, []string{"pipeline:p1"}) {
		t.Errorf("m1 upstreams = %v, want [pipeline:p1]", m1.Upstreams)
	}
}

// A model that reads the table it builds is an incremental pattern, not a dependency on
// itself. A self-loop in a graph whose purpose is ordering is worse than a missing edge.
func TestAssetGraph_ModelReadingItsOwnTargetIsNotAnUpstream(t *testing.T) {
	g := buildAssetGraph(assetGraphInput{
		Models: []modelAsset{{
			ID: "m1", Name: "Incremental", ConnectionID: connA,
			SQLText:         "SELECT * FROM analytics.snapshot WHERE ts > (SELECT max(ts) FROM analytics.snapshot)",
			Materialization: "table", TargetTable: "analytics.snapshot",
		}},
	})

	for _, e := range g.Edges {
		if e.From == e.To {
			t.Fatalf("self edge in graph: %+v", e)
		}
		if e.Kind == assetEdgeReads && e.To == "model:m1" {
			t.Errorf("m1 must not read its own target; edge %+v", e)
		}
	}
	m := modelByID(t, g, "m1")
	if len(m.Upstreams) != 0 {
		t.Errorf("upstreams = %v, want none", m.Upstreams)
	}
	if !reflect.DeepEqual(m.Unresolved, []string{"analytics.snapshot"}) {
		t.Errorf("its own target is not a producer for it; unresolved = %v", m.Unresolved)
	}
}

// A table name only identifies a table inside one warehouse. Two connections holding
// "analytics.orders" hold two different tables, and a model on one must never be drawn
// as reading the other's.
func TestAssetGraph_ConnectionsDoNotShareTables(t *testing.T) {
	g := buildAssetGraph(assetGraphInput{
		Pipelines: []pipelineAsset{{ID: "p1", Name: "Orders sync", ConnectionID: connA}},
		Produced:  []tableProducer{pipeWrites("p1", "Orders sync", connA, "analytics.orders", "orders")},
		Models: []modelAsset{{
			ID: "m1", Name: "Other warehouse", ConnectionID: connB,
			SQLText: "SELECT * FROM analytics.orders",
		}},
	})

	m := modelByID(t, g, "m1")
	if len(m.Upstreams) != 0 {
		t.Errorf("a model on another connection has no upstream here; got %v", m.Upstreams)
	}
	if !reflect.DeepEqual(m.Unresolved, []string{"analytics.orders"}) {
		t.Errorf("unresolved = %v, want [analytics.orders]", m.Unresolved)
	}
	if got := len(edgeKinds(g, assetEdgeReads)); got != 0 {
		t.Errorf("reads edges = %d, want 0", got)
	}
}

// Two pipelines writing the same table is fan-in: both really do write it. Until
// migration 100 a schedule held one upstream and could not name both; it can now, so
// this is the half-wired case and TestAssetGraph_FanInWiredToBothProducers is the other.
func TestAssetGraph_FanInIsCountedAndNotCalledAmbiguous(t *testing.T) {
	g := buildAssetGraph(assetGraphInput{
		Pipelines: []pipelineAsset{
			{ID: "p1", Name: "EU orders", ConnectionID: connA},
			{ID: "p2", Name: "US orders", ConnectionID: connA},
		},
		Produced: []tableProducer{
			pipeWrites("p1", "EU orders", connA, "analytics.orders", "orders"),
			pipeWrites("p2", "US orders", connA, "analytics.orders", "orders"),
		},
		Models: []modelAsset{{
			ID: "m1", Name: "All orders", ConnectionID: connA,
			SQLText:      "SELECT * FROM analytics.orders",
			ScheduleType: scheduleAfterUpstream, ScheduleStatus: "active",
			TriggerUpstreamIDs: []string{"p1"},
		}},
	})

	if g.Stats.Tables != 1 {
		t.Fatalf("both pipelines write one table; tables = %d", g.Stats.Tables)
	}
	if got := len(edgeKinds(g, assetEdgeWrites)); got != 2 {
		t.Errorf("writes edges = %d, want 2", got)
	}
	m := modelByID(t, g, "m1")
	if !reflect.DeepEqual(m.Upstreams, []string{"pipeline:p1", "pipeline:p2"}) {
		t.Fatalf("upstreams = %v, want both pipelines", m.Upstreams)
	}
	if !reflect.DeepEqual(m.UncoveredUpstreams, []string{"pipeline:p2"}) {
		t.Errorf("only the triggering pipeline is covered; uncovered = %v", m.UncoveredUpstreams)
	}
	if m.Ambiguous {
		t.Errorf("two producers of one table is fan-in, not ambiguity")
	}
	if g.Stats.ModelsWithMultipleUpstreams != 1 {
		t.Errorf("models_with_multiple_upstreams = %d, want 1", g.Stats.ModelsWithMultipleUpstreams)
	}
}

// Ambiguity is a reference that could be two DIFFERENT tables, which is only reachable
// through an unqualified name.
func TestAssetGraph_UnqualifiedReferenceAcrossSchemasIsAmbiguous(t *testing.T) {
	g := buildAssetGraph(assetGraphInput{
		Pipelines: []pipelineAsset{
			{ID: "p1", Name: "Analytics", ConnectionID: connA},
			{ID: "p2", Name: "Staging", ConnectionID: connA},
		},
		Produced: []tableProducer{
			pipeWrites("p1", "Analytics", connA, "analytics.orders", "orders"),
			pipeWrites("p2", "Staging", connA, "staging.orders", "orders"),
		},
		Models: []modelAsset{{
			ID: "m1", Name: "Bare read", ConnectionID: connA,
			SQLText: "SELECT * FROM orders",
		}},
	})

	if g.Stats.Tables != 2 {
		t.Fatalf("two schemas are two tables; tables = %d", g.Stats.Tables)
	}
	m := modelByID(t, g, "m1")
	if !m.Ambiguous {
		t.Errorf("`FROM orders` could be either table; ambiguous = false")
	}
	if !reflect.DeepEqual(m.Upstreams, []string{"pipeline:p1", "pipeline:p2"}) {
		t.Errorf("upstreams = %v, want both", m.Upstreams)
	}
}

// The polarity that makes this whole path correct rather than actively misleading:
// naming a schema is information, and a qualified reference never falls back to the
// bare name. For a CDC pipeline `shop.orders` is the SOURCE name of the very table
// `analytics.orders` is the destination name of, so the fallback would answer for a
// table the query never reads.
func TestMatchTableReference_QualifiedNeverFallsBackToBareName(t *testing.T) {
	produced := []producedTable{
		{ProducerID: "p1", DestQualified: "analytics.orders", TableName: "orders"},
	}

	refs := validators.ExtractTableReferences("SELECT * FROM shop.orders")
	if len(refs) != 1 {
		t.Fatalf("expected one reference, got %v", refs)
	}
	matches, _ := matchTableReference(refs[0], produced)
	if len(matches) != 0 {
		t.Fatalf("shop.orders must not match analytics.orders; got %+v", matches)
	}

	bare := validators.ExtractTableReferences("SELECT * FROM orders")
	if len(bare) != 1 {
		t.Fatalf("expected one reference, got %v", bare)
	}
	weak, qualified := matchTableReference(bare[0], produced)
	if len(weak) != 1 {
		t.Fatalf("an unqualified reference matches on the bare name; got %+v", weak)
	}
	if qualified {
		t.Errorf("a bare-name match is not qualified")
	}
}

// A strong match wins outright: the weak candidates are not appended to it, because a
// same-named table in another schema is a different table.
func TestMatchTableReference_StrongMatchExcludesWeakOnes(t *testing.T) {
	produced := []producedTable{
		{ProducerID: "p1", DestQualified: "analytics.orders", TableName: "orders"},
		{ProducerID: "p2", DestQualified: "staging.orders", TableName: "orders"},
	}
	refs := validators.ExtractTableReferences("SELECT * FROM analytics.orders")
	matches, qualified := matchTableReference(refs[0], produced)
	if !qualified {
		t.Errorf("qualified = false for a schema-qualified match")
	}
	if len(matches) != 1 || matches[0].ProducerID != "p1" {
		t.Fatalf("matches = %+v, want only p1", matches)
	}

	// The case above cannot actually reach the strong/weak choice: a qualified
	// reference never collects weak candidates at all, so "strong wins" is vacuous
	// there. The choice is only live for an UNQUALIFIED reference, where a producer
	// that recorded no destination schema has a bare name that IS its whole name and
	// so matches exactly, while same-named tables in real schemas match only weakly.
	produced = []producedTable{
		{ProducerID: "p3", DestQualified: "orders", TableName: "orders"},
		{ProducerID: "p4", DestQualified: "staging.orders", TableName: "orders"},
	}
	refs = validators.ExtractTableReferences("SELECT * FROM orders")
	matches, qualified = matchTableReference(refs[0], produced)
	if !qualified {
		t.Errorf("an exact match on the full recorded name is qualified")
	}
	if len(matches) != 1 || matches[0].ProducerID != "p3" {
		t.Fatalf("matches = %+v, want only p3 — the weak candidate must not ride along", matches)
	}
}

// A producer with no destination namespace stays distinct from one that has a schema:
// nothing here can prove a bare "orders" resolves to the analytics schema.
func TestProducedTableKey_BareNameIsNotTheQualifiedTable(t *testing.T) {
	qualified := producedTableKey(connA, producedTable{DestQualified: "analytics.orders", TableName: "orders"})
	bare := producedTableKey(connA, producedTable{TableName: "orders"})
	if qualified == bare {
		t.Fatalf("bare and qualified tables collapsed onto one key: %q", qualified)
	}
	otherConn := producedTableKey(connB, producedTable{DestQualified: "analytics.orders", TableName: "orders"})
	if qualified == otherConn {
		t.Fatalf("two connections collapsed onto one table key: %q", qualified)
	}
	upper := producedTableKey(connA, producedTable{DestQualified: "Analytics.Orders", TableName: "Orders"})
	if qualified != upper {
		t.Errorf("table keys are case-insensitive; %q != %q", qualified, upper)
	}
}

func TestSplitModelTarget(t *testing.T) {
	cases := []struct {
		raw       string
		qualified string
		table     string
	}{
		{"analytics.clean_orders", "analytics.clean_orders", "clean_orders"},
		{"clean_orders", "", "clean_orders"},
		{"  analytics.clean_orders  ", "analytics.clean_orders", "clean_orders"},
		{"", "", ""},
		{".", "", ""},
		{"analytics.", "", ""},
	}
	for _, tc := range cases {
		got := splitModelTarget(tc.raw)
		if got.DestQualified != tc.qualified || got.TableName != tc.table {
			t.Errorf("splitModelTarget(%q) = %q/%q, want %q/%q",
				tc.raw, got.DestQualified, got.TableName, tc.qualified, tc.table)
		}
	}
}

// A stats row whose pipeline fell outside the page would otherwise produce an edge from
// a node that is not in the response.
func TestAssetGraph_ProducedRowWithoutItsPipelineIsDropped(t *testing.T) {
	g := buildAssetGraph(assetGraphInput{
		Produced: []tableProducer{pipeWrites("p-missing", "Gone", connA, "analytics.orders", "orders")},
		Models: []modelAsset{{
			ID: "m1", Name: "Reader", ConnectionID: connA,
			SQLText: "SELECT * FROM analytics.orders",
		}},
	})

	if g.Stats.Tables != 0 {
		t.Errorf("no pipeline node, so no table node; tables = %d", g.Stats.Tables)
	}
	if got := len(edgeKinds(g, assetEdgeWrites)); got != 0 {
		t.Errorf("writes edges = %d, want 0", got)
	}
	m := modelByID(t, g, "m1")
	if !reflect.DeepEqual(m.Unresolved, []string{"analytics.orders"}) {
		t.Errorf("unresolved = %v, want [analytics.orders]", m.Unresolved)
	}
}

// A truncated graph draws a model as having no upstream at all, which is the exact
// claim this endpoint exists to make. The flag has to survive into the response.
func TestAssetGraph_TruncationSurvivesIntoStats(t *testing.T) {
	g := buildAssetGraph(assetGraphInput{
		Truncated: assetGraphTruncation{Pipelines: true, ProducedTables: true, Models: true},
	})
	want := assetGraphTruncation{Pipelines: true, ProducedTables: true, Models: true}
	if g.Stats.Truncated != want {
		t.Errorf("truncated = %+v, want %+v", g.Stats.Truncated, want)
	}
}

// Nodes and edges come out of Go maps, whose iteration order is randomised. Two
// identical requests must produce byte-identical graphs or every diff of one is noise.
func TestAssetGraph_OutputIsDeterministic(t *testing.T) {
	in := assetGraphInput{
		Pipelines: []pipelineAsset{
			{ID: "p1", Name: "EU", ConnectionID: connA},
			{ID: "p2", Name: "US", ConnectionID: connA},
			{ID: "p3", Name: "APAC", ConnectionID: connA},
		},
		Produced: []tableProducer{
			pipeWrites("p1", "EU", connA, "analytics.orders", "orders"),
			pipeWrites("p2", "US", connA, "analytics.orders", "orders"),
			pipeWrites("p3", "APAC", connA, "analytics.customers", "customers"),
		},
		Models: []modelAsset{
			{ID: "m1", Name: "A", ConnectionID: connA, SQLText: "SELECT * FROM analytics.orders",
				Materialization: "table", TargetTable: "analytics.a"},
			{ID: "m2", Name: "B", ConnectionID: connA,
				SQLText: "SELECT * FROM analytics.a JOIN analytics.customers USING (id)"},
		},
	}
	first := buildAssetGraph(in)
	for i := 0; i < 25; i++ {
		if got := buildAssetGraph(in); !reflect.DeepEqual(got, first) {
			t.Fatalf("run %d differed:\n got %+v\nwant %+v", i, got, first)
		}
	}

	// And the sort is a real order, not incidental input order.
	if !sort.SliceIsSorted(first.Nodes, func(i, j int) bool {
		if first.Nodes[i].Kind != first.Nodes[j].Kind {
			return first.Nodes[i].Kind < first.Nodes[j].Kind
		}
		return first.Nodes[i].ID < first.Nodes[j].ID
	}) {
		t.Errorf("nodes are not sorted: %+v", first.Nodes)
	}
}

// Every graph an empty workspace produces must still marshal as empty lists, never
// null: a UI that has to distinguish [] from null gets it wrong once.
func TestAssetGraph_EmptyWorkspaceHasEmptyCollections(t *testing.T) {
	g := buildAssetGraph(assetGraphInput{})
	if g.Nodes == nil || g.Edges == nil || g.Models == nil || g.Stats.Edges == nil {
		t.Fatalf("nil collection in empty graph: %+v", g)
	}
	if len(g.Nodes) != 0 || len(g.Edges) != 0 {
		t.Errorf("empty input produced %d nodes and %d edges", len(g.Nodes), len(g.Edges))
	}
}

// A model with no materialization is not a producer, however its target_table reads.
func TestAssetGraph_TargetTableWithoutMaterializationProducesNothing(t *testing.T) {
	g := buildAssetGraph(assetGraphInput{
		Models: []modelAsset{
			{ID: "m1", Name: "Statement only", ConnectionID: connA,
				SQLText: "SELECT 1", Materialization: "statement", TargetTable: "analytics.thing"},
			{ID: "m2", Name: "Reader", ConnectionID: connA,
				SQLText: "SELECT * FROM analytics.thing"},
		},
	})

	if got := len(edgeKinds(g, assetEdgeMaterializes)); got != 0 {
		t.Errorf("materializes edges = %d, want 0", got)
	}
	m2 := modelByID(t, g, "m2")
	if len(m2.Upstreams) != 0 {
		t.Errorf("upstreams = %v, want none", m2.Upstreams)
	}
	if g.Stats.ModelsWithUnresolvedRefs != 1 {
		t.Errorf("models_with_unresolved_references = %d, want 1", g.Stats.ModelsWithUnresolvedRefs)
	}
}

// Fan-in is only a gap while the schedule waits on one of the producers. An upstream
// list can name both, and then nothing is uncovered — while the count of models with
// several upstreams stays 1 either way. That is the whole difference between the two
// stats: one says "worth a look", the other says "not wired up".
func TestAssetGraph_FanInWiredToBothProducers(t *testing.T) {
	g := buildAssetGraph(assetGraphInput{
		Pipelines: []pipelineAsset{
			{ID: "p1", Name: "EU orders", ConnectionID: connA},
			{ID: "p2", Name: "US orders", ConnectionID: connA},
		},
		Produced: []tableProducer{
			pipeWrites("p1", "EU orders", connA, "analytics.orders", "orders"),
			pipeWrites("p2", "US orders", connA, "analytics.orders", "orders"),
		},
		Models: []modelAsset{{
			ID: "m1", Name: "All orders", ConnectionID: connA,
			SQLText:            "SELECT * FROM analytics.orders",
			ScheduleType:       scheduleAfterUpstream,
			ScheduleStatus:     "active",
			TriggerUpstreamIDs: []string{"p1", "p2"},
		}},
	})

	m := modelByID(t, g, "m1")
	if !reflect.DeepEqual(m.TriggerUpstreams, []string{"pipeline:p1", "pipeline:p2"}) {
		t.Fatalf("trigger upstreams = %v, want both pipelines", m.TriggerUpstreams)
	}
	if len(m.UncoveredUpstreams) != 0 {
		t.Errorf("both producers refresh the model; uncovered = %v", m.UncoveredUpstreams)
	}
	if got := len(edgeKinds(g, assetEdgeTriggers)); got != 2 {
		t.Fatalf("triggers edges = %d, want one per upstream", got)
	}
	if g.Stats.ModelsWithUncoveredUpstreams != 0 {
		t.Errorf("models_with_uncovered_upstreams = %d, want 0", g.Stats.ModelsWithUncoveredUpstreams)
	}
	if g.Stats.ModelsWithMultipleUpstreams != 1 {
		t.Errorf("the model still reads two producers; models_with_multiple_upstreams = %d", g.Stats.ModelsWithMultipleUpstreams)
	}
}

// A model is a producer, so a model can be an upstream. The trigger edge comes from the
// schedule exactly as a pipeline's does, and the lineage edge comes from the SQL through
// the materialized table. Here the two agree, which is the arrangement that leaves
// nothing uncovered — and the edge has to be model -> model, not model -> table -> model,
// or the graph would show a rebuild order the scheduler does not actually follow.
func TestAssetGraph_ModelTriggersAnotherModel(t *testing.T) {
	g := buildAssetGraph(assetGraphInput{
		Models: []modelAsset{
			{
				ID: "m1", Name: "Clean orders", ConnectionID: connA,
				SQLText:         "SELECT * FROM analytics.orders",
				Materialization: "table", TargetTable: "analytics.clean_orders",
			},
			{
				ID: "m2", Name: "Weekly revenue", ConnectionID: connA,
				SQLText:            "SELECT sum(total) FROM analytics.clean_orders",
				ScheduleType:       scheduleAfterUpstream,
				ScheduleStatus:     "active",
				TriggerUpstreamIDs: []string{"m1"},
			},
		},
	})

	m2 := modelByID(t, g, "m2")
	if !reflect.DeepEqual(m2.TriggerUpstreams, []string{"model:m1"}) {
		t.Fatalf("trigger upstreams = %v, want [model:m1]", m2.TriggerUpstreams)
	}
	if !reflect.DeepEqual(m2.Upstreams, []string{"model:m1"}) {
		t.Fatalf("upstreams = %v, want [model:m1]", m2.Upstreams)
	}
	if len(m2.UncoveredUpstreams) != 0 {
		t.Errorf("the upstream model is what refreshes it; uncovered = %v", m2.UncoveredUpstreams)
	}
	trig := edgeKinds(g, assetEdgeTriggers)
	if len(trig) != 1 {
		t.Fatalf("triggers edges = %d, want 1", len(trig))
	}
	if trig[0].From != "model:m1" || trig[0].To != "model:m2" {
		t.Errorf("trigger edge = %s -> %s, want model:m1 -> model:m2", trig[0].From, trig[0].To)
	}
	// Being someone else's upstream is not a schedule: m1 still refreshes manually.
	if m1 := modelByID(t, g, "m1"); m1.Trigger != triggerManualRefresh {
		t.Errorf("m1 trigger = %q, want %q", m1.Trigger, triggerManualRefresh)
	}
}

// A statement model writes the tables its own SQL names, so it is a producer of them
// exactly like a table model is of its target — and the graph has to say so, or the
// upstream picker offers a model the graph draws as producing nothing. The edge is
// graded INFERRED, not declared: nobody typed that target, it was parsed out of SQL.
func TestAssetGraph_StatementModelProducesWhatItsSQLWrites(t *testing.T) {
	g := buildAssetGraph(assetGraphInput{
		Pipelines: []pipelineAsset{{ID: "p1", Name: "Orders sync", ConnectionID: connA}},
		Produced:  []tableProducer{pipeWrites("p1", "Orders sync", connA, "analytics.orders", "orders")},
		Models: []modelAsset{
			{
				ID: "m1", Name: "Load daily orders", ConnectionID: connA,
				SQLText:         "INSERT INTO analytics.orders_daily SELECT * FROM analytics.orders",
				Materialization: matStatement,
			},
			{
				ID: "m2", Name: "Weekly revenue", ConnectionID: connA,
				SQLText: "SELECT sum(total) FROM analytics.orders_daily",
			},
		},
	})

	edges := edgeKinds(g, assetEdgeMaterializes)
	if len(edges) != 1 {
		t.Fatalf("materializes edges = %d, want 1 — a statement model must produce what its SQL writes", len(edges))
	}
	if e := edges[0]; e.From != "model:m1" || e.To != "table:"+producedTableKey(connA, producedTable{DestQualified: "analytics.orders_daily", TableName: "orders_daily"}) || e.Evidence != assetEvidenceInferred {
		t.Errorf("materializes edge = %+v, want model:m1 -> analytics.orders_daily / inferred", e)
	}

	m2 := modelByID(t, g, "m2")
	if !reflect.DeepEqual(m2.Upstreams, []string{"model:m1"}) {
		t.Fatalf("m2 upstreams = %v, want [model:m1]", m2.Upstreams)
	}
	if len(m2.Unresolved) != 0 {
		t.Errorf("the statement's write target is a known producer; unresolved = %v", m2.Unresolved)
	}
	m1 := modelByID(t, g, "m1")
	if !reflect.DeepEqual(m1.Upstreams, []string{"pipeline:p1"}) {
		t.Errorf("m1 upstreams = %v, want [pipeline:p1]", m1.Upstreams)
	}
}
