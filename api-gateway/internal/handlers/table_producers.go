package handlers

import (
	"strings"

	"api-gateway/internal/validators"
)

// The one rule that decides whether a table a query READS is the same table something
// else WROTE. Two callers ask that question — the upstream suggestion behind the
// schedule dialog and the workspace asset graph — and they must answer it identically,
// because the graph is what the dialog's suggestion is drawn on. Two copies of a
// matching rule drift, and the drift is invisible: both keep returning plausible
// answers, just not the same ones.
//
// So the rule lives here once and both call it. Widen it in one place or neither.

// producedTable is one table some producer has been observed or declared to write.
//
// The producer is opaque: a pipeline that landed the table, or a model that
// materializes into it. The matcher does not care which, and that is the point — a
// model reading a table another model builds is the same question as a model reading a
// table a pipeline landed.
type producedTable struct {
	// ProducerID identifies whatever wrote the table, in the caller's own namespace.
	ProducerID string
	// ProducerName is for display only and never participates in matching.
	ProducerName string
	// DestQualified is "schema.table" as the producer recorded it on the DESTINATION
	// side, or "" when the producer named no schema. Never a source-side name — see
	// the header of saved_query_upstreams.go for why that distinction decides whether
	// this whole path is correct or actively misleading.
	DestQualified string
	// TableName is the table's own name with no namespace. Same on both sides of a
	// pipeline; only the namespace differs.
	TableName string
}

// matchTableReference pairs one table reference from a query against the tables known
// to be produced, and reports whether the match used the schema.
//
// A bare table name is only ever matched for a reference that named NO schema —
// `FROM orders`, which leans on the connection's search_path and is how most ad-hoc SQL
// is written. When the SQL does name a schema, that schema is information, and falling
// back to the name alone throws it away: `shop.orders` failing to match
// `analytics.orders` means the query reads some other table, not that we should offer
// the producer of the one we found. Getting this backwards is not a near miss — for a
// CDC pipeline, `shop.orders` is the SOURCE name of the very table `analytics.orders`
// is the destination name of, so the match would confidently answer for a table the
// query never reads.
//
// Comparison is lower(); table names are matched case-insensitively even though a
// quoted identifier is case-sensitive to the engine. That direction is deliberate: it
// can over-match (offering a candidate a stricter comparison would have skipped) and a
// person confirms the choice, whereas exact matching would silently drop every
// reference whose case differs from how the producer recorded it.
//
// An empty result means the reference is unresolved. Callers must report that rather
// than drop it: "3 of 4 inputs have a known producer" is the answer, not the three.
func matchTableReference(ref validators.TableRef, produced []producedTable) (matches []producedTable, qualified bool) {
	wantQualified := strings.ToLower(ref.SchemaQualified())
	wantName := strings.ToLower(ref.Name())
	unqualified := len(ref.Parts) == 1

	var strong, weak []producedTable
	for _, p := range produced {
		switch {
		case p.DestQualified != "" && strings.ToLower(p.DestQualified) == wantQualified:
			strong = append(strong, p)
		case unqualified && strings.ToLower(p.TableName) == wantName:
			weak = append(weak, p)
		}
	}
	if len(strong) > 0 {
		return strong, true
	}
	return weak, false
}

// producedTableKey is the identity two producers must share for their tables to be the
// same table. It is deliberately conservative: a pipeline landing "analytics.orders"
// and a model materializing a bare "orders" stay DISTINCT, because nothing here can
// prove the model's search_path resolves to the analytics schema. Collapsing them on a
// guess would draw a dependency edge that does not exist.
//
// The caller supplies the connection, and must: a table name is only unique inside the
// warehouse it lives in, and two connections holding a table of the same name hold two
// different tables.
func producedTableKey(connectionID string, p producedTable) string {
	name := p.DestQualified
	if name == "" {
		name = p.TableName
	}
	return connectionID + "\x00" + strings.ToLower(name)
}

// modelProducedTables is every table a model builds. It is the one definition of "a
// model is a producer": the asset graph draws its edges from it and the upstream
// suggestion offers the model from it, so the two cannot disagree.
//
//   - 'table' records its destination in target_table (modelProducedTable).
//   - 'statement' records no destination: it runs its SQL as written, so the tables it
//     writes are read out of that SQL — INSERT, MERGE, UPDATE, CREATE TABLE … AS and
//     the rest listed in validators.ExtractWriteTargets. A target_table left behind
//     from an earlier 'table' life is not a claim about what the statement writes, so
//     it is ignored.
//   - 'none', and any value this build does not recognize, writes nowhere.
//
// This only says what a model's SQL would write. Whether the caller may see the model at
// all is the caller's filter, and it must be the same filter for every materialization:
// a private model's SQL is as private as its target_table.
func modelProducedTables(id, name, materialization, targetTable, sqlText string) []producedTable {
	switch materialization {
	case matTable:
		if table, ok := modelProducedTable(id, name, materialization, targetTable); ok {
			return []producedTable{table}
		}
	case matStatement:
		return statementProducedTables(id, name, sqlText)
	}
	return nil
}

// statementProducedTables reads the tables a statement model writes out of its SQL.
// The extractor names nothing it cannot be sure of, so a statement it cannot read
// produces no table rather than a guessed one: a missing producer leaves an input
// unresolved, where an invented one would schedule a model after something unrelated.
//
// SQL holding more than one statement produces nothing. The runner refuses it
// (authorizeModelRun → ValidateExplorerStatement), so such a model never writes a row,
// and SQL the extractor and that rule could split differently — a backslash in a
// string, a `;` in a dollar-quoted body — is exactly what the rule refuses.
//
// Tables are shaped like a table model's target: the last two parts, so
// `warehouse.analytics.orders` and `analytics.orders` are the same table, and one
// statement that writes it twice is still one producer of it.
func statementProducedTables(id, name, sqlText string) []producedTable {
	if !validators.IsSingleStatement(sqlText) {
		return nil
	}
	var tables []producedTable
	seen := map[string]bool{}
	for _, ref := range validators.ExtractWriteTargets(sqlText) {
		table := producedTable{ProducerID: id, ProducerName: name, TableName: ref.Name()}
		if len(ref.Parts) >= 2 {
			table.DestQualified = ref.SchemaQualified()
		}
		key := producedTableKey("", table)
		if seen[key] {
			continue
		}
		seen[key] = true
		tables = append(tables, table)
	}
	return tables
}

// modelProducedTable is the table a materialization='table' model builds, if its
// target_table names one. Any other materialization returns false here; callers that
// want every model's tables use modelProducedTables.
func modelProducedTable(id, name, materialization, targetTable string) (producedTable, bool) {
	if materialization != "table" {
		return producedTable{}, false
	}
	target := splitModelTarget(targetTable)
	if target.TableName == "" {
		return producedTable{}, false
	}
	target.ProducerID = id
	target.ProducerName = name
	return target, true
}
