package cdcstats

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/healer"
)

// Source DDL that CDC will never mirror to the destination.
//
// Debezium is configured with `include.schema.changes: "true"`
// (shared/mcp-connectors/internal/debezium/versions/v1.0.0/connector.py:416, commented
// "Needed for schema evolution visibility") so every source DDL statement is published
// to the bare topic.prefix topic — `cdc-<id>`, with no table suffix. Nothing consumed it.
// The cdcstats worker's topicsForPrefix deliberately matches `prefix + "."`, so the one
// topic carrying schema evolution was the one topic excluded from the only consumer that
// knew the prefix.
//
// The consequence, observed on prod: a source table dropped mid-stream leaves the CDC
// pipeline reporting four green dependencies, a green health check, no lag alert and
// zero failures, while the destination silently freezes at its last known state forever.
// Every instrument the product offers says it is fine, because each instrument is
// answering "is the process up?" — and the process genuinely is. The missing signal was
// never a health probe; it was this topic.
//
// What is emitted here is deliberately narrow: only drops, and only drops. See
// classifySchemaChange for why adds and type changes are excluded.

// schemaChangeGroupPrefix names the DDL consumer group. Separate from the table-stats
// group so the two can be reasoned about, lag-monitored and reset independently.
const schemaChangeGroupPrefix = "cdc-schema-changes-"

// debeziumSchemaChange is the subset of Debezium's SchemaChangeValue envelope we read.
// The full message also carries an inline Connect schema and the post-change column
// list; we need neither, because every field below is either structured (tableChanges)
// or verbatim source text (ddl).
type debeziumSchemaChange struct {
	Payload struct {
		Source struct {
			// "false" for a real streaming DDL; "true"/"first"/"last"/"incremental"
			// while Debezium is replaying its snapshot.
			Snapshot string `json:"snapshot"`
			DB       string `json:"db"`
			Table    string `json:"table"`
		} `json:"source"`
		DatabaseName string `json:"databaseName"`
		SchemaName   string `json:"schemaName"`
		DDL          string `json:"ddl"`
		TableChanges []struct {
			// "CREATE" | "ALTER" | "DROP"
			Type string `json:"type"`
			// Quoted and qualified, e.g. "\"pipeline_test\".\"cdc_drift\"".
			ID string `json:"id"`
		} `json:"tableChanges"`
	} `json:"payload"`
}

// unappliedChange is one source DDL that CDC has decided not to mirror.
type unappliedChange struct {
	changeType string // "drop_table" | "drop_column"
	table      string // unquoted, qualified as far as the source reported it
	column     string // empty for drop_table
	ddl        string // normalized, dialect-neutral — see classifySchemaChange
}

// reDropColumn matches an explicit DROP COLUMN clause. The `COLUMN` keyword is
// required on purpose: `ALTER TABLE t DROP FOREIGN KEY fk`, `DROP INDEX`,
// `DROP PRIMARY KEY` and `DROP CONSTRAINT c` are all "drops" that remove no user
// data from the destination and must not be filed as schema changes. MySQL's
// column-only shorthand (`ALTER TABLE t DROP note`) is likewise skipped rather than
// guessed at — a false drop_column row is worse than a missed one, because the user
// is invited to run destructive DDL by hand.
var reDropColumn = regexp.MustCompile(`(?i)\bDROP\s+COLUMN\s+` + "[`\"\\[]?" + `([A-Za-z0-9_$]+)`)

// unquoteIdentifier strips the quoting Debezium applies to tableChanges[].id
// (`"db"."table"`, MySQL backticks, SQL Server brackets) and keeps at most the last
// two segments, so a three-part SQL Server id (db.schema.table) reads the same way as
// a two-part MySQL one.
func unquoteIdentifier(id string) string {
	cleaned := strings.Map(func(r rune) rune {
		switch r {
		case '"', '`', '[', ']':
			return -1
		}
		return r
	}, strings.TrimSpace(id))
	parts := strings.Split(cleaned, ".")
	if len(parts) > 2 {
		parts = parts[len(parts)-2:]
	}
	return strings.Join(parts, ".")
}

// classifySchemaChange turns one Debezium schema-change message into the set of
// changes worth reporting.
//
// Only drops are reported, and the two exclusions are the whole design:
//
//   - ADD COLUMN is excluded because the sink already reports it, with applied=true,
//     from inside its own reconciler (kafka-mcp-sink reportAppliedSchemaDrift). Both
//     producers writing the same addition would file it twice under two different DDL
//     strings — Debezium's raw source text and the sink's synthesized ALTER — which the
//     schema_change_approvals UNIQUE (pipeline_id, ddl) key cannot collapse.
//
//   - Type changes (MySQL MODIFY COLUMN, PostgreSQL ALTER COLUMN … TYPE) are excluded
//     because the only DDL available here is the SOURCE dialect's, and the healer
//     applies a non-destructive approved DDL verbatim to the DESTINATION. Filing
//     `MODIFY COLUMN amount VARCHAR(50)` against a Postgres destination would hand the
//     user an Approve button that can only end in a syntax error and an alarm — exactly
//     the false-alarm shape just removed from the batch path. Reporting a type change
//     properly needs destination-dialect rendering, which lives with the executor's
//     drift detector, not here.
//
// Drops are safe to report precisely because they are never executed: isDestructiveDDL
// refuses them on auto-apply AND after approval, so the row is a record of a decision
// and the apply is the user's to run by hand.
//
// The emitted DDL is normalized rather than passed through verbatim. `DROP TABLE x` and
// `ALTER TABLE x DROP COLUMN y` are identical in every dialect we target, they are
// stable across source-side formatting (backticks, casing, IF EXISTS), and stability
// matters because the string is the UNIQUE (pipeline_id, ddl) key — a reformatted
// source statement must not file the same drop a second time.
//
// It is also DESTINATION-qualified. Debezium names the object as the SOURCE knows it
// (`inventory.orders`), but the statement we emit is one the user runs by hand against
// the destination, where this pipeline's tables live under its own namespace. See
// healer.DestinationQualifiedTable. `table` keeps the source name — that is the change's
// identity, and what the UI shows as "which table drifted".
func classifySchemaChange(msg *debeziumSchemaChange, destNamespace string) []unappliedChange {
	p := &msg.Payload

	// Snapshot-phase DDL is Debezium describing the tables it is about to read, not the
	// source changing. Without this, every connector start would file its whole catalog.
	if snap := strings.ToLower(strings.TrimSpace(p.Source.Snapshot)); snap != "" && snap != "false" {
		return nil
	}

	out := []unappliedChange{}
	for _, tc := range p.TableChanges {
		table := unquoteIdentifier(tc.ID)
		if table == "" {
			continue
		}
		destTable := healer.DestinationQualifiedTable(destNamespace, table)
		switch strings.ToUpper(strings.TrimSpace(tc.Type)) {
		case "DROP":
			// Structured, no parsing: Debezium reports type=DROP with table=null.
			out = append(out, unappliedChange{
				changeType: "drop_table",
				table:      table,
				ddl:        fmt.Sprintf("DROP TABLE %s", destTable),
			})
		case "ALTER":
			for _, m := range reDropColumn.FindAllStringSubmatch(p.DDL, -1) {
				out = append(out, unappliedChange{
					changeType: "drop_column",
					table:      table,
					column:     m[1],
					ddl:        fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", destTable, m[1]),
				})
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// tableLifecycle is the second, narrower reading of the same schema-change message.
//
// classifySchemaChange answers "what drift is worth filing for a human decision".
// This one answers "is a table this pipeline was told to capture present at the
// source right now" — the fact the `debezium_task` probe needs and has never had.
// They deliberately do not share a return type: classify normalizes and
// destination-qualifies for an operator to read, while this keeps the raw source
// identity because it is about to be matched against the pipeline's own selection.
type tableLifecycle struct {
	// snapshot is true while Debezium is replaying its catalog rather than
	// reporting a live change. It gates RECORDING only; see trackSelectedTableDrops.
	snapshot bool
	dropped  []string
	created  []string
}

func readTableLifecycle(msg *debeziumSchemaChange) tableLifecycle {
	p := &msg.Payload
	out := tableLifecycle{}
	if snap := strings.ToLower(strings.TrimSpace(p.Source.Snapshot)); snap != "" && snap != "false" {
		out.snapshot = true
	}
	for _, tc := range p.TableChanges {
		table := unquoteIdentifier(tc.ID)
		if table == "" {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(tc.Type)) {
		case "DROP":
			out.dropped = append(out.dropped, table)
		case "CREATE":
			out.created = append(out.created, table)
		}
	}
	return out
}

// matchSelectedTable resolves a table name as the SOURCE reported it against the
// names in `pipelines.config->'selected_tables'`, and returns the SELECTION's
// spelling on a hit.
//
// Returning the selection's spelling rather than the reported one is the whole
// reason this helper exists: the row it keys must be identical for the DROP and for
// the CREATE that clears it, and the two records need not report the table the same
// way. Storing the reported form would leave a recreate keyed differently from its
// drop and pin the stream degraded forever — risk 3 in this fix's plan.
//
// Matching mirrors diffSelectedAgainstReported in the temporal adapter
// (backend-temporal-adapter/internal/workflows/pipeline_status_activity.go): case
// insensitive, and form-insensitive in BOTH directions, so a selection of
// `cdc_drift` matches a reported `pipeline_test.cdc_drift` and a selection of
// `pipeline_test.cdc_drift` matches a reported bare `cdc_drift`.
func matchSelectedTable(selected []string, reported string) (string, bool) {
	rep := strings.ToLower(strings.TrimSpace(unquoteIdentifier(reported)))
	if rep == "" {
		return "", false
	}
	repBare := rep
	if i := strings.LastIndex(rep, "."); i >= 0 && i+1 < len(rep) {
		repBare = rep[i+1:]
	}
	for _, sel := range selected {
		trimmed := strings.TrimSpace(sel)
		if trimmed == "" {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(unquoteIdentifier(trimmed)))
		bare := key
		if i := strings.LastIndex(key, "."); i >= 0 && i+1 < len(key) {
			bare = key[i+1:]
		}
		if key == rep || key == repBare || bare == rep || bare == repBare {
			return trimmed, true
		}
	}
	return "", false
}

// trackSelectedTableDrops turns the lifecycle reading above into the durable fact
// the dependency probe reads.
//
// The asymmetry between the two directions is deliberate and is the part worth
// reading twice:
//
//   - RECORDING a drop honours the snapshot guard. Snapshot-phase DDL is Debezium
//     describing what it is about to read, not the source changing, and a connector
//     restart replaying a catalog must never be filed as a live drop.
//
//   - CLEARING a drop does NOT. A CREATE seen during a snapshot is positive proof
//     the table exists right now, whatever phase the connector is in. Without this,
//     the ordinary recovery sequence — recreate the table, restart the connector so
//     it re-snapshots — would leave the stream degraded permanently, because the only
//     CREATE it ever emits for that table arrives inside a snapshot.
//
// Every DB error is logged at Debug and swallowed: on a rolling deploy the migration
// that creates cdc_source_table_drops may not have run yet, and this is a reporting
// side-channel whose failure must never stall the consumer group. Same convention as
// agents/executor/dependency_manifest.go.
func (a *Agent) trackSelectedTableDrops(ctx context.Context, w *pipelineWorker, msg *debeziumSchemaChange) {
	if a.db == nil {
		return
	}
	selected := w.selection()
	if len(selected) == 0 {
		// Nothing selected (or selection not yet resolved): a drop we cannot attribute
		// to this pipeline's own tables is not evidence about this pipeline's stream.
		return
	}
	life := readTableLifecycle(msg)

	for _, reported := range life.created {
		if name, ok := matchSelectedTable(selected, reported); ok {
			a.clearSelectedTableDrop(ctx, w.pipelineID, name)
		}
	}
	if life.snapshot {
		return
	}
	for _, reported := range life.dropped {
		if name, ok := matchSelectedTable(selected, reported); ok {
			a.recordSelectedTableDrop(ctx, w.pipelineID, name)
		}
	}
}

// recordSelectedTableDrop opens (or re-opens) the fact that a selected source table
// is gone. ON CONFLICT re-arms rather than inserting a second row: this table is
// current state, not history — the audit trail of the drop is the
// schema_change_approvals row the same message files.
func (a *Agent) recordSelectedTableDrop(ctx context.Context, pipelineID, table string) {
	if _, err := a.db.ExecContext(ctx, `
		INSERT INTO cdc_source_table_drops (pipeline_id, table_name)
		VALUES ($1::uuid, $2)
		ON CONFLICT (pipeline_id, table_name)
		DO UPDATE SET dropped_at = NOW(), restored_at = NULL
	`, pipelineID, table); err != nil {
		log.WithError(err).WithFields(log.Fields{
			"pipeline_id": pipelineID,
			"table":       table,
		}).Debug("cdc schema changes: could not record dropped source table")
		return
	}
	log.WithFields(log.Fields{
		"pipeline_id": pipelineID,
		"table":       table,
	}).Warn("cdc schema changes: selected source table dropped at origin; CDC source stream will report degraded")
}

// clearSelectedTableDrop closes an open drop. The probe returns the dependency to
// healthy on its next 15s tick with no further action — writeHealth overwrites
// status every tick, so there is nothing to un-stick by hand.
func (a *Agent) clearSelectedTableDrop(ctx context.Context, pipelineID, table string) {
	res, err := a.db.ExecContext(ctx, `
		UPDATE cdc_source_table_drops
		SET restored_at = NOW()
		WHERE pipeline_id = $1::uuid AND table_name = $2 AND restored_at IS NULL
	`, pipelineID, table)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"pipeline_id": pipelineID,
			"table":       table,
		}).Debug("cdc schema changes: could not clear dropped source table")
		return
	}
	if n, aerr := res.RowsAffected(); aerr == nil && n > 0 {
		log.WithFields(log.Fields{
			"pipeline_id": pipelineID,
			"table":       table,
		}).Info("cdc schema changes: dropped source table exists again; CDC source stream will return to healthy")
	}
}

// buildUnappliedChangeEvent renders one change as the healer's SchemaChangeEvent.
//
// applied=false is correct and load-bearing: unlike the sink's additive reconcile, this
// change has NOT been made to the destination and never will be by CDC. The healer's
// normal path is exactly what we want for it — classify, file for approval, notify —
// and it is the same path a batch pipeline's drop takes, which is the point. One
// product, one DDL, one answer.
func buildUnappliedChangeEvent(pipelineID string, c unappliedChange, schemaName string, detectedAt time.Time) healer.SchemaChangeEvent {
	ts := detectedAt.UTC().Format(time.RFC3339)
	return healer.SchemaChangeEvent{
		EventType:  "SCHEMA_CHANGE_DETECTED",
		PipelineID: pipelineID,
		Timestamp:  ts,
		SchemaChange: healer.SchemaChange{
			ChangeType: c.changeType,
			Table:      c.table,
			SchemaName: schemaName,
			ColumnName: c.column,
			DDL:        c.ddl,
			RiskLevel:  "high",
			DetectedAt: ts,
			Applied:    false,
		},
		Context: map[string]interface{}{
			// Names the producer so a reader of the row can tell a source-observed drop
			// from a destination-side reconcile without diffing timestamps.
			"source": "debezium_schema_topic",
			"mode":   "cdc",
			// The destination is untouched; this is the fact that makes the change
			// worth a human decision rather than an alert about a broken pipeline.
			"destination_unchanged": true,
		},
		ActionNeeded: true,
	}
}

// runSchemaChangeWorker consumes the connector's schema-change topic for the lifetime
// of the pipeline worker.
func (a *Agent) runSchemaChangeWorker(w *pipelineWorker) {
	handler := &schemaChangeHandler{agent: a, worker: w}
	for {
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		// The bare topic.prefix topic. Debezium creates it on the first DDL, which for
		// a snapshotting connector is immediate — but a Consume against a topic that
		// does not exist yet fails, so this loop is the retry.
		err := w.ddlConsumer.Consume(w.ctx, []string{w.topicPrefix}, handler)
		if w.ctx.Err() != nil {
			return
		}
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"pipeline_id": w.pipelineID,
				"topic":       w.topicPrefix,
			}).Debug("cdc schema changes: consume ended, retrying")
			time.Sleep(5 * time.Second)
		}
	}
}

type schemaChangeHandler struct {
	agent  *Agent
	worker *pipelineWorker
}

func (h *schemaChangeHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *schemaChangeHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *schemaChangeHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.agent.handleSchemaChangeRecord(sess.Context(), h.worker, msg.Value)
		sess.MarkMessage(msg, "")
	}
	return nil
}

// handleSchemaChangeRecord parses one schema-change record and republishes any
// unmirrored drop onto the healer's topic.
//
// Every failure here is swallowed after a log line, and the message is marked consumed
// regardless. This consumer is a reporting side-channel: the data path is Debezium →
// sink → destination and does not pass through here, so a malformed record or an
// unreachable broker must cost a missing history row, never a stalled consumer group
// that then blocks its own offset commits.
func (a *Agent) handleSchemaChangeRecord(ctx context.Context, w *pipelineWorker, raw []byte) {
	var msg debeziumSchemaChange
	if err := json.Unmarshal(raw, &msg); err != nil {
		log.WithError(err).WithField("pipeline_id", w.pipelineID).
			Debug("cdc schema changes: unparseable record, skipping")
		return
	}

	// Before the reporting path and independent of it: a DROP/CREATE of a SELECTED
	// table is the health fact the debezium_task probe reads. It must run even when
	// classifySchemaChange files nothing — a CREATE is never drift worth approving,
	// but it is exactly what closes an open drop.
	a.trackSelectedTableDrops(ctx, w, &msg)

	changes := classifySchemaChange(&msg, w.destNamespace)
	if len(changes) == 0 {
		return
	}

	schemaName := strings.TrimSpace(msg.Payload.SchemaName)
	if schemaName == "" {
		schemaName = strings.TrimSpace(msg.Payload.DatabaseName)
	}

	for _, c := range changes {
		evt := buildUnappliedChangeEvent(w.pipelineID, c, schemaName, time.Now())
		b, err := json.Marshal(evt)
		if err != nil {
			continue
		}
		if err := a.kafka.ProduceWithHeadersAndContext(ctx, healer.HealerTopic, []byte(w.pipelineID), b, map[string]string{
			"trace_id": w.pipelineID,
		}); err != nil {
			log.WithError(err).WithFields(log.Fields{
				"pipeline_id": w.pipelineID,
				"change_type": c.changeType,
			}).Warn("cdc schema changes: failed to report unmirrored source DDL")
			continue
		}
		log.WithFields(log.Fields{
			"pipeline_id": w.pipelineID,
			"change_type": c.changeType,
			"table":       c.table,
		}).Info("cdc schema changes: reported source DDL that CDC does not mirror")
	}
}

// newSchemaChangeConsumerConfig differs from the table-stats config in exactly one
// respect, and it is the important one: OffsetNewest.
//
// Table stats start from Oldest so a late-joining consumer can backfill the snapshot
// and make captured==applied add up. DDL has the opposite requirement. Every CDC
// connector on this deployment has been publishing to its schema-change topic since it
// was created, with no consumer — so a group joining at Oldest would, on first deploy,
// replay months of accumulated DDL across every pipeline at once and file a backlog of
// stale approvals for drops the user dealt with long ago. Newest reports drift from the
// moment reporting exists. Once the group has committed offsets, restarts resume from
// them, so the only gap is DDL issued while the orchestrator is down.
func newSchemaChangeConsumerConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_3_0_0
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.Consumer.Offsets.AutoCommit.Enable = true
	cfg.Consumer.Offsets.AutoCommit.Interval = 5 * time.Second
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	return cfg
}
