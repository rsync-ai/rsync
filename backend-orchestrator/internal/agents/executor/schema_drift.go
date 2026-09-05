package executor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/healer"
)

// Self-healing schema drift — P1 detector.
//
// On each successful batch DiscoverSchema this snapshots the pipeline's SELECTED
// source tables into schema_baselines (migration 068), diffs the snapshot against
// the prior baseline, and emits one healer.SchemaChangeEvent per drift onto
// healer.HealerTopic. The dormant healer.Agent consumer (activated in P0 behind the
// same flag) turns those events into schema_change_approvals rows. We are the ONLY
// writer to the topic for batch drift — we never INSERT into schema_change_approvals
// directly, so the consumer's reasoning/risk enrichment and ON CONFLICT dedup stay
// authoritative [RT-fix #6].
//
// Everything here is gated by RSYNC_SCHEMA_DRIFT_ENABLED and is best-effort: a drift
// bookkeeping failure must never fail the data transfer.

// schemaDriftEnabled reports whether the proactive detector is on (default off).
func schemaDriftEnabled() bool {
	return os.Getenv("RSYNC_SCHEMA_DRIFT_ENABLED") == "true"
}

// SchemaDriftPolicy is the per-pipeline detector policy persisted under
// pipelines.config->'schema_drift_policy' (written by the api-gateway
// schema-drift-policy endpoints; keep the field set in lockstep with
// handlers/schema_evolution.go). Absent, partial, or unparseable policy =
// every field defaults TRUE, so the global RSYNC_SCHEMA_DRIFT_ENABLED flag
// stays the single kill switch and a bad row can never widen a disable.
type SchemaDriftPolicy struct {
	Enabled      bool // gates the whole detector for this pipeline
	NotifyOnAdd  bool // emit additive drift (add_column / create_table)
	NotifyOnDrop bool // emit drop drift (drop_column / drop_table)
}

func defaultSchemaDriftPolicy() SchemaDriftPolicy {
	return SchemaDriftPolicy{Enabled: true, NotifyOnAdd: true, NotifyOnDrop: true}
}

// parseSchemaDriftPolicy decodes the raw JSONB value. Pure. Fields are decoded
// via pointers so an ABSENT field defaults true — a bare {} must not read as
// all-off (JSON's bool zero-value is false).
func parseSchemaDriftPolicy(raw []byte) SchemaDriftPolicy {
	out := defaultSchemaDriftPolicy()
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return out
	}
	var p struct {
		Enabled      *bool `json:"enabled"`
		NotifyOnAdd  *bool `json:"notify_on_add"`
		NotifyOnDrop *bool `json:"notify_on_drop"`
	}
	if err := json.Unmarshal([]byte(trimmed), &p); err != nil {
		log.Warnf("[schema-drift] unparseable schema_drift_policy (treating as enabled): %v", err)
		return out
	}
	if p.Enabled != nil {
		out.Enabled = *p.Enabled
	}
	if p.NotifyOnAdd != nil {
		out.NotifyOnAdd = *p.NotifyOnAdd
	}
	if p.NotifyOnDrop != nil {
		out.NotifyOnDrop = *p.NotifyOnDrop
	}
	return out
}

// loadSchemaDriftPolicy reads the pipeline's policy from pipelines.config.
// Best-effort: any read failure proceeds as enabled (warn) so a DB blip never
// silently disables drift detection. Mirrors the loadNominatedKeys read shape.
func loadSchemaDriftPolicy(ctx context.Context, db *sql.DB, pipelineID string) SchemaDriftPolicy {
	if db == nil || strings.TrimSpace(pipelineID) == "" {
		return defaultSchemaDriftPolicy()
	}
	var raw sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(config->'schema_drift_policy', 'null'::jsonb)::text FROM pipelines WHERE id = $1`,
		strings.TrimSpace(pipelineID),
	).Scan(&raw); err != nil {
		log.Warnf("[schema-drift] load policy (best-effort, proceeding as enabled) %s: %v", pipelineID, err)
		return defaultSchemaDriftPolicy()
	}
	if !raw.Valid {
		return defaultSchemaDriftPolicy()
	}
	return parseSchemaDriftPolicy([]byte(raw.String))
}

// filterChangesByPolicy drops deltas the pipeline's policy opted out of:
// notify_on_add gates additive drift (add_column/create_table), notify_on_drop
// gates drop drift (drop_column/drop_table). modify_column is always kept — a
// type change can silently break the sink, so it is not opt-out-able. Pure.
func filterChangesByPolicy(policy SchemaDriftPolicy, changes []healer.SchemaChange) []healer.SchemaChange {
	if policy.NotifyOnAdd && policy.NotifyOnDrop {
		return changes
	}
	out := make([]healer.SchemaChange, 0, len(changes))
	for _, ch := range changes {
		switch ch.ChangeType {
		case "add_column", "create_table":
			if !policy.NotifyOnAdd {
				continue
			}
		case "drop_column", "drop_table":
			if !policy.NotifyOnDrop {
				continue
			}
		}
		out = append(out, ch)
	}
	return out
}

// detectAndEmitSchemaDrift is the single entry point wired into the executor's batch
// discovery-success branch. `discovered` is the full source-DB schema; it is scoped
// to the pipeline's selected tables before any baseline/diff so unrelated tables
// never produce drift [RT-fix #3].
func (a *Agent) detectAndEmitSchemaDrift(ctx context.Context, task ExecutorTask, executionID string, discovered []TableMetadata) {
	if !schemaDriftEnabled() || a.db == nil || strings.TrimSpace(task.PipelineID) == "" {
		return
	}

	// Per-pipeline policy (pipelines.config->'schema_drift_policy'). Read
	// failure fails OPEN to enabled; an explicit enabled=false skips detection
	// entirely (no baseline write either, so re-enabling diffs against the
	// last enabled run rather than a policy-off window).
	policy := loadSchemaDriftPolicy(ctx, a.db, task.PipelineID)
	if !policy.Enabled {
		log.Debugf("[schema-drift] pipeline %s: per-pipeline policy disabled; skipping", task.PipelineID)
		return
	}

	selected := selectedTableSet(task.Params["tables"])
	if len(selected) == 0 {
		// No selected-table set reachable — never baseline an unscoped whole-DB snapshot.
		log.Debugf("[schema-drift] pipeline %s: no selected tables in params; skipping", task.PipelineID)
		return
	}

	current := filterDiscoveredToSelected(discovered, selected)
	if len(current) == 0 {
		return
	}
	currentMap := tableMapByKey(current)

	prior := loadSchemaBaseline(ctx, a.db, task.PipelineID)
	if len(prior) > 0 {
		// Scope the prior baseline to the CURRENT selection so a table the user
		// de-selected does not read back as a drop_table; only a still-selected
		// table that vanished from the source counts as a drop.
		priorScoped := scopeBaselineToSelected(prior, selected)
		// Resolved here, not inside the diff, so the (pure, unit-tested) differ stays
		// DB-free. Empty = legacy pipeline; the DROP text then falls back to the bare
		// table name, which is what resolves in the destination's default schema.
		destNS := resolveDestinationNamespace(ctx, a.db, task)
		if deltas := filterChangesByPolicy(policy, diffSchemas(priorScoped, currentMap, destNS)); len(deltas) > 0 {
			a.emitSchemaChanges(ctx, task.PipelineID, deltas)
		}
	} else {
		log.Infof("[schema-drift] pipeline %s: captured first baseline for %d table(s); no diff this run", task.PipelineID, len(current))
	}

	// Snapshot AFTER diffing so the next run compares against this run.
	storeSchemaBaseline(ctx, a.db, task.PipelineID, executionID, sourceTypeOf(task), current)
}

// --- pure helpers (unit-tested, no DB/Kafka) ---------------------------------

// baselineKey normalises (schema, table) into the map/diff key: qualified
// "schema.table" when a schema is present, else the bare table. Matches the
// pkByTable/colTypesByTable convention at the discovery seam.
func baselineKey(schema, table string) string {
	t := strings.TrimSpace(table)
	if s := strings.TrimSpace(schema); s != "" {
		return s + "." + t
	}
	return t
}

// selectedTableSet builds the lookup set the detector scopes to from the
// upstream-normalised task.Params["tables"]. Each name is recorded in BOTH its raw
// form and its bare (schema-stripped) form so a discovered table matches whether the
// selection was qualified or not.
func selectedTableSet(raw interface{}) map[string]bool {
	out := map[string]bool{}
	add := func(name string) {
		n := strings.TrimSpace(name)
		if n == "" {
			return
		}
		out[n] = true
		if i := strings.LastIndex(n, "."); i >= 0 && i+1 < len(n) {
			out[strings.TrimSpace(n[i+1:])] = true
		}
	}
	switch v := raw.(type) {
	case []string:
		for _, s := range v {
			add(s)
		}
	case []interface{}:
		for _, e := range v {
			if s, ok := e.(string); ok {
				add(s)
			}
		}
	}
	return out
}

// filterDiscoveredToSelected keeps only tables the pipeline syncs (qualified OR bare
// name in the selected set). Empty selected set -> keep nothing (caller no-ops).
func filterDiscoveredToSelected(discovered []TableMetadata, selected map[string]bool) []TableMetadata {
	if len(selected) == 0 {
		return nil
	}
	out := make([]TableMetadata, 0, len(discovered))
	for _, t := range discovered {
		if selected[baselineKey(t.Schema, t.Name)] || selected[strings.TrimSpace(t.Name)] {
			out = append(out, t)
		}
	}
	return out
}

// scopeBaselineToSelected restricts a loaded baseline map to the current selection.
func scopeBaselineToSelected(prior map[string]TableMetadata, selected map[string]bool) map[string]TableMetadata {
	out := make(map[string]TableMetadata, len(prior))
	for k, t := range prior {
		if selected[k] || selected[strings.TrimSpace(t.Name)] {
			out[k] = t
		}
	}
	return out
}

// tableMapByKey indexes tables by baselineKey for diffing.
func tableMapByKey(tables []TableMetadata) map[string]TableMetadata {
	m := make(map[string]TableMetadata, len(tables))
	for _, t := range tables {
		m[baselineKey(t.Schema, t.Name)] = t
	}
	return m
}

// columnShape is what a drift comparison needs from one column: the canonical
// token the destination is built from, and the source's declared type.
type columnShape struct {
	canonical string // "string", "number" — what the destination column is derived from
	native    string // "varchar(50)", "decimal(8,2)" — what the source actually declares
}

// columnTypeMap projects a table's columns to name->shape.
func columnTypeMap(t TableMetadata) map[string]columnShape {
	m := make(map[string]columnShape, len(t.Columns))
	for _, c := range t.Columns {
		if name := strings.TrimSpace(c.Name); name != "" {
			m[name] = columnShape{
				canonical: strings.TrimSpace(c.Type),
				native:    strings.TrimSpace(c.SourceType),
			}
		}
	}
	return m
}

// typeBase splits a declared type into its base token and its qualifier:
// "varchar(50)" -> ("varchar", "(50)"), "int unsigned" -> ("int unsigned", "").
// Comparison is case-insensitive, so both parts come back lowercased.
func typeBase(declared string) (base, qualifier string) {
	d := strings.ToLower(strings.TrimSpace(declared))
	i := strings.Index(d, "(")
	if i < 0 {
		return d, ""
	}
	return strings.TrimSpace(d[:i]), strings.ReplaceAll(d[i:], " ", "")
}

// nativeTypeDrifted reports whether two DECLARED source types differ in a way we
// are willing to report, for columns whose canonical type is unchanged.
//
// The asymmetric-qualifier case is deliberately NOT drift, and it is the reason
// this is a function rather than a string compare. Connectors used to report a
// bare "varchar" as source_type and now report "varchar(50)"; a baseline snapshot
// taken before that connector upgrade holds the bare form. Comparing the raw
// strings would file one bogus modify_column per column of every table the first
// time a pipeline ran after the upgrade — an inbox full of drift for a schema
// nobody touched. A missing qualifier means "the source did not tell us", not
// "there is no qualifier", so it is never evidence of a change.
//
// What still gets through:
//   - different base tokens (bigint -> smallint, int -> "int unsigned")
//   - same base, both qualified, qualifiers differ (varchar(50) -> varchar(10),
//     decimal(12,2) -> decimal(4,2))
func nativeTypeDrifted(prev, cur string) bool {
	p, pq := typeBase(prev)
	c, cq := typeBase(cur)
	if p == "" || c == "" {
		return false
	}
	if p != c {
		return true
	}
	if pq == "" || cq == "" {
		return false
	}
	return pq != cq
}

// diffSchemas compares the prior baseline against the freshly discovered schema
// (both already scoped to the pipeline's selected tables) and returns one
// SchemaChange per drift. Pure + deterministic (sorted iteration, canonical DDL) so
// the consumer's UNIQUE(pipeline_id, ddl) dedup is idempotent across re-runs.
//
// Routing follows the P0–P3 matrix: additive (add_column/create_table) is
// informational; destructive (drop_column/drop_table/modify_column) is approve-only.
// ChangeType uses the EXACT strings the healer fallbackAnalysis switch consumes.
//
// destNamespace is the pipeline's destination namespace, and it is used for exactly one
// thing: qualifying the DROP statements, which we never execute and the user runs by hand
// on the DESTINATION. See healer.DestinationQualifiedTable for why the source key is the
// wrong — and actively dangerous — name to print there. Everything else keeps the source
// key: Table/SchemaName are the source's identity, and the appliable ALTERs are rewritten
// against the destination at apply time by the healer.
func diffSchemas(prior, current map[string]TableMetadata, destNamespace string) []healer.SchemaChange {
	var changes []healer.SchemaChange

	for _, key := range sortedTableKeys(current) {
		cur := current[key]
		prev, existed := prior[key]
		if !existed {
			changes = append(changes, healer.SchemaChange{
				ChangeType: "create_table",
				Table:      key,
				SchemaName: strings.TrimSpace(cur.Schema),
				DDL:        fmt.Sprintf("-- drift: table %s appeared in source", key),
				RiskLevel:  "low",
			})
			continue
		}
		curCols := columnTypeMap(cur)
		prevCols := columnTypeMap(prev)
		for _, col := range sortedStrKeys(curCols) {
			ccol := curCols[col]
			pcol, had := prevCols[col]
			if !had {
				changes = append(changes, healer.SchemaChange{
					ChangeType: "add_column", Table: key, SchemaName: strings.TrimSpace(cur.Schema),
					ColumnName: col, ColumnType: ccol.canonical,
					DDL:       fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", key, col, ccol.canonical),
					RiskLevel: "low",
				})
				continue
			}
			if !strings.EqualFold(pcol.canonical, ccol.canonical) {
				changes = append(changes, healer.SchemaChange{
					ChangeType: "modify_column", Table: key, SchemaName: strings.TrimSpace(cur.Schema),
					ColumnName: col, ColumnType: ccol.canonical,
					DDL:       fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", key, col, ccol.canonical),
					RiskLevel: "high",
				})
				continue
			}
			// B1: the canonical token is unchanged, so the destination column does
			// not need to change — but the source's DECLARED type did, and that is
			// the truncation case the modify_column card warns about and the
			// detector could not see (VARCHAR(50)->VARCHAR(10) is "string"->
			// "string"). Reported as an advisory, NOT as an appliable statement:
			// the destination is already the right canonical type, and narrowing it
			// to match would truncate rows that are already stored. The `--` prefix
			// is what makes that binding — autoApplicableDDL (api-gateway) and
			// notAutoAppliedDDL (healer) both refuse to execute a comment, so the
			// user gets a notice and a Reject button rather than an Approve button
			// wired to a no-op.
			if nativeTypeDrifted(pcol.native, ccol.native) {
				changes = append(changes, healer.SchemaChange{
					ChangeType: "modify_column", Table: key, SchemaName: strings.TrimSpace(cur.Schema),
					ColumnName: col, ColumnType: ccol.canonical,
					DDL: fmt.Sprintf(
						"-- drift: %s.%s declared type changed in the source from %s to %s (both map to %s, so the destination column is unchanged)",
						key, col, pcol.native, ccol.native, ccol.canonical),
					RiskLevel: "medium",
				})
			}
		}
		for _, col := range sortedStrKeys(prevCols) {
			if _, still := curCols[col]; !still {
				changes = append(changes, healer.SchemaChange{
					ChangeType: "drop_column", Table: key, SchemaName: strings.TrimSpace(cur.Schema),
					ColumnName: col, ColumnType: prevCols[col].canonical,
					DDL:       fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", healer.DestinationQualifiedTable(destNamespace, key), col),
					RiskLevel: "high",
				})
			}
		}
	}

	// Still-selected tables that vanished from the source = drop_table (approve-only).
	//
	// D1: this branch is REACHABLE, but only on the runs where the pre-flight lets
	// the pipeline get this far. When assessor.RequiresTablePrimaryKeys() is true
	// (CDC, or any destination that upserts) the per-table check in
	// assessor/mysql.go oneMySQLTablePKCheck / assessor/postgresql.go raises
	// CDC_TABLE_NOT_FOUND_IN_SOURCE at SeverityError for the missing table and the
	// run never starts — the user is told about the drop by the pre-flight instead,
	// which is the better surface anyway (it blocks before any data moves). What is
	// left for this branch is the append-only case: a batch load into a file/SaaS
	// destination, where RequiresTablePrimaryKeys() is false and nothing else
	// notices the table is gone.
	//
	// So: do not delete this as dead code, and do not "fix" it by weakening the
	// pre-flight. Two surfaces reporting the same fact is fine here because only
	// one of them can fire on any given run.
	for _, key := range sortedTableKeys(prior) {
		if _, still := current[key]; !still {
			changes = append(changes, healer.SchemaChange{
				ChangeType: "drop_table", Table: key,
				SchemaName: strings.TrimSpace(prior[key].Schema),
				DDL:        fmt.Sprintf("DROP TABLE %s", healer.DestinationQualifiedTable(destNamespace, key)),
				RiskLevel:  "high",
			})
		}
	}

	return changes
}

func sortedTableKeys(m map[string]TableMetadata) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func sortedStrKeys(m map[string]columnShape) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func sourceTypeOf(task ExecutorTask) string {
	if task.Source != nil {
		return task.Source.Type
	}
	return ""
}

// --- impure helpers (DB + Kafka; mirror loadNominatedKeys / ProduceWithContext) ---

// emitSchemaChanges publishes one SchemaChangeEvent per delta to healer.HealerTopic,
// keyed by pipeline_id. Best-effort per message. auto_apply stays false for P0–P3
// (propose -> approve only).
func (a *Agent) emitSchemaChanges(ctx context.Context, pipelineID string, changes []healer.SchemaChange) {
	now := time.Now().UTC().Format(time.RFC3339)
	for _, ch := range changes {
		ch.DetectedAt = now
		event := healer.SchemaChangeEvent{
			EventType:    "schema_change_detected",
			PipelineID:   pipelineID,
			Timestamp:    now,
			SchemaChange: ch,
			Context: map[string]interface{}{
				"auto_apply_enabled":       false,
				"skip_destructive_enabled": true,
			},
			ActionNeeded: true,
		}
		b, err := json.Marshal(event)
		if err != nil {
			log.Warnf("[schema-drift] marshal event (%s %s): %v", ch.ChangeType, ch.Table, err)
			continue
		}
		if err := a.kafkaManager.ProduceWithContext(ctx, healer.HealerTopic, []byte(pipelineID), b); err != nil {
			log.Warnf("[schema-drift] emit %s for %s.%s: %v", ch.ChangeType, ch.Table, ch.ColumnName, err)
			continue
		}
		log.Infof("[schema-drift] emitted %s for pipeline %s table %s column %q", ch.ChangeType, pipelineID, ch.Table, ch.ColumnName)
	}
}

// storeSchemaBaseline UPSERTs one row per selected table. Best-effort: persistence
// failures are logged, never fatal.
func storeSchemaBaseline(ctx context.Context, db *sql.DB, pipelineID, executionID, sourceType string, tables []TableMetadata) {
	if db == nil || strings.TrimSpace(pipelineID) == "" {
		return
	}
	for _, t := range tables {
		table := strings.TrimSpace(t.Name)
		if table == "" {
			continue
		}
		b, err := json.Marshal(t)
		if err != nil {
			continue
		}
		var execID interface{} // NULL when empty (execution_id is best-effort)
		if e := strings.TrimSpace(executionID); e != "" {
			execID = e
		}
		var srcType interface{}
		if s := strings.TrimSpace(sourceType); s != "" {
			srcType = s
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO schema_baselines (pipeline_id, schema_name, table_name, baseline, source_type, execution_id, captured_at)
			VALUES ($1, $2, $3, $4::jsonb, $5, $6, NOW())
			ON CONFLICT (pipeline_id, schema_name, table_name)
			DO UPDATE SET baseline = EXCLUDED.baseline, source_type = EXCLUDED.source_type,
			              execution_id = EXCLUDED.execution_id, captured_at = NOW()`,
			strings.TrimSpace(pipelineID), strings.TrimSpace(t.Schema), table, string(b), srcType, execID); err != nil {
			log.Warnf("[schema-drift] persist baseline (best-effort) %s.%s: %v", strings.TrimSpace(t.Schema), table, err)
		}
	}
}

// loadSchemaBaseline reads the persisted baseline for a pipeline, keyed by
// baselineKey. Best-effort: returns nil on any read/parse failure.
func loadSchemaBaseline(ctx context.Context, db *sql.DB, pipelineID string) map[string]TableMetadata {
	if db == nil || strings.TrimSpace(pipelineID) == "" {
		return nil
	}
	rows, err := db.QueryContext(ctx,
		`SELECT schema_name, table_name, baseline::text FROM schema_baselines WHERE pipeline_id = $1`,
		strings.TrimSpace(pipelineID))
	if err != nil {
		log.Warnf("[schema-drift] load baseline (best-effort) %s: %v", pipelineID, err)
		return nil
	}
	defer rows.Close()

	out := map[string]TableMetadata{}
	for rows.Next() {
		var schema, table, raw string
		if err := rows.Scan(&schema, &table, &raw); err != nil {
			continue
		}
		var tm TableMetadata
		if err := json.Unmarshal([]byte(raw), &tm); err != nil {
			continue
		}
		// Trust the row's schema/name for the key even if the stored JSON omitted them.
		if strings.TrimSpace(tm.Name) == "" {
			tm.Name = table
		}
		if strings.TrimSpace(tm.Schema) == "" {
			tm.Schema = schema
		}
		out[baselineKey(schema, table)] = tm
	}
	return out
}
