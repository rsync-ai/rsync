package handlers

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/rsync-ai/shared/kafkaclient"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
	"github.com/rsync-ai/backend-orchestrator/internal/utils"
)

// ── Kafka teardown on pipeline delete ─────────────────────────────────────
//
// Deleting a pipeline reclaimed the Kafka *Connect* connector but never the
// Kafka *broker* resources the pipeline created, so every deleted pipeline
// stranded its topics and consumer groups forever. Measured on prod
// 2026-07-31: 398 topics / 61 consumer groups, including 25 schemahistory.cdc-*
// with no owning connector, accumulated from pipelines deleted weeks earlier.
// Per CDC pipeline the leak is 4 topics + 3 groups; per batch pipeline 1 topic
// + 1 group — plus one extra group per execution in streaming_only/never mode.
//
// Everything here is best-effort: the pipeline row is going away either way, so
// an unreachable broker must never block a user's delete.

// pipelineKafkaNames matches the topics and consumer groups owned by one
// pipeline. The names come from the producers:
//
//	topics  cdc-<id8>                      executor.go (Debezium topic.prefix == connector name)
//	        cdc-<id8>.<db>.<table>         Debezium per-table topic
//	        cdc-<id8>.<db>.<table>.dlq     kafka-sink-worker (srcTopic + ".dlq")
//	        schemahistory.cdc-<id8>        debezium connector.py
//	        pipeline.<id8>.data(+.dlq)     batch backfill
//	groups  sink-<id8>                     CDC streaming sink
//	        sink-<id8>-batch               batch backfill sink
//	        sink-<id8>-stream              CDC streaming_only/never (stable per pipeline)
//	        sink-<id8>-<exec8>             ditto, when CDC_STREAMING_SINK_GROUP_PER_EXECUTION is on
//	        cdc-schema-changes-<uuid>      cdcstats/schema_changes.go
//	        cdc-table-stats-<uuid>         cdcstats/agent.go
//
// Every one of these is matched in BOTH its bare and its namespace-qualified
// spelling. The producers now mint qualified names (kafkaclient.Topic/Group put
// them under KAFKA_TOPIC_PREFIX), but a pipeline created before that change has
// live resources under the bare name and the sweep has to reclaim both. Matching
// only one spelling does not fail loudly -- the delete succeeds and the sweep
// silently matches nothing, which is precisely the leak this file exists to
// close.
type pipelineKafkaNames struct {
	id8  string
	uuid string
}

// ownsTopic reports whether a topic belongs to this pipeline.
//
// Matching is anchored on the 8-char id with an explicit "." terminator, so
// "cdc-abd8a64d" can never swallow "cdc-abd8a64de" — a bare strings.HasPrefix
// on the id alone would delete a different pipeline's data.
func (n pipelineKafkaNames) ownsTopic(topic string) bool {
	for _, base := range []string{"cdc-" + n.id8, "schemahistory.cdc-" + n.id8} {
		if matchesEitherSpelling(topic, base, ".", kafkaclient.Topic) {
			return true
		}
	}
	return kafkaclient.InNamespace(topic, "pipeline."+n.id8+".")
}

// ownsGroup reports whether a consumer group belongs to this pipeline. Same
// anchoring rule as ownsTopic, with "-" as the sink-group separator.
func (n pipelineKafkaNames) ownsGroup(group string) bool {
	if matchesEitherSpelling(group, "sink-"+n.id8, "-", kafkaclient.Group) {
		return true
	}
	for _, base := range []string{"cdc-schema-changes-" + n.uuid, "cdc-table-stats-" + n.uuid} {
		if group == base || group == kafkaclient.Group(base) {
			return true
		}
	}
	return false
}

// matchesEitherSpelling reports whether name is base -- or base followed by sep
// -- in either the bare spelling or the one qualify() produces under the
// configured namespace.
//
// The sep terminator is what keeps "cdc-abd8a64d" from swallowing
// "cdc-abd8a64de": a bare HasPrefix on the 8-char id alone would sweep a
// DIFFERENT pipeline's data. Qualifying must not weaken that, so the terminator
// is applied to each spelling rather than to a stripped-off remainder.
func matchesEitherSpelling(name, base, sep string, qualify func(string) string) bool {
	for _, form := range [2]string{base, qualify(base)} {
		if name == form || strings.HasPrefix(name, form+sep) {
			return true
		}
	}
	return false
}

// id8IsUnique guards the sweep against the one case where prefix matching would
// hit the wrong pipeline: two pipelines whose UUIDs share the first 8 chars.
// Their topic and connector names already collide, so that pipeline pair is
// broken regardless — but deleting a LIVE pipeline's topics while tearing down a
// different one is far worse than leaking, so we skip the sweep instead.
//
// A DB error counts as "not unique" (fail closed): leaking a topic is
// recoverable, deleting someone else's is not.
//
// The `id <> $1` term is redundant on the normal post-delete path (the row is
// already gone) and is kept so the check is also correct if this is ever called
// while the pipeline still exists.
func id8IsUnique(ctx context.Context, db *sql.DB, pipelineID, id8 string) bool {
	if db == nil {
		return false
	}
	var others int
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipelines WHERE id::text <> $1 AND LEFT(id::text, 8) = $2`,
		pipelineID, id8).Scan(&others)
	if err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).
			Warn("kafka teardown: id8 uniqueness check failed; skipping topic/group sweep")
		return false
	}
	if others > 0 {
		log.WithFields(log.Fields{
			"pipeline_id": pipelineID,
			"id8":         id8,
			"others":      others,
		}).Warn("kafka teardown: another pipeline shares this id8 prefix; skipping sweep so its topics survive")
		return false
	}
	return true
}

// kafkaTeardownTimeout bounds the whole teardown. Deliberately short: the
// pipeline row is already gone, so this is reclamation, not correctness — a slow
// broker must not hold the user's delete request open.
const kafkaTeardownTimeout = 45 * time.Second

// pipelineWorkerStopper is the slice of the CDC table-stats agent this handler
// needs. Declared here (rather than importing internal/agents/cdcstats) to keep
// the handler package from depending on an agent package.
type pipelineWorkerStopper interface {
	StopPipeline(pipelineID string) bool
}

// KafkaTeardownRequest is the body of POST /api/v1/cdc/kafka-teardown.
type KafkaTeardownRequest struct {
	PipelineID string `json:"pipeline_id" binding:"required"`
}

// TeardownPipelineKafka reclaims the broker-side resources of a DELETED pipeline:
// its consumer groups and its topics.
//
// This runs AFTER the pipeline row is gone, which is what makes it correct.
// /cdc/cleanup runs BEFORE (it has to — cdc_resources.pipeline_id is ON DELETE
// SET NULL, so a post-delete read finds nothing and the replication slot leaks),
// but Kafka teardown has the opposite requirement: while the row still exists,
// the cdcstats syncLoop (30s tick) and the sink workers will happily recreate
// any consumer group we delete. Deleting the row first closes that window
// permanently, since every recreate path is keyed on a live pipelines row.
//
// Internal callers only. Post-delete there is no row left to authorize against,
// so a user principal cannot be checked and is refused outright — api-gateway
// has already applied its own workspace-role gate before calling.
func TeardownPipelineKafka(db *sql.DB, mcpManager *mcp.ServerManager, tm *kafka.TopologyManager, stats pipelineWorkerStopper) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req KafkaTeardownRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if _, internal := cdcPrincipal(c); !internal {
			c.JSON(http.StatusForbidden, gin.H{"error": "internal callers only"})
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), kafkaTeardownTimeout)
		defer cancel()

		// Lower-case, because every producer of these names derives them from
		// pipelines.id::text, which Postgres renders lower-case. The delete path
		// takes the id from a URL param instead (requireUUIDParam does not
		// normalize, and uuid.Parse accepts upper-case), so an upper-case UUID
		// would otherwise produce an id8 that matches nothing and silently
		// reclaim nothing at all.
		pipelineID := strings.ToLower(strings.TrimSpace(req.PipelineID))
		log.WithField("pipeline_id", pipelineID).Info("Tearing down pipeline Kafka resources")

		stopPipelineConsumers(ctx, mcpManager, tm, stats, pipelineID)
		errs := cleanupPipelineKafkaResources(ctx, db, tm, pipelineID)

		if len(errs) > 0 {
			log.WithFields(log.Fields{"pipeline_id": pipelineID, "errors": errs}).
				Warn("Kafka teardown completed with errors")
			c.JSON(http.StatusOK, gin.H{
				"success": false,
				"errors":  errs,
				"message": "Kafka teardown completed with errors",
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "Kafka resources torn down successfully",
		})
	}
}

// stopPipelineConsumers stops everything still joined to this pipeline's consumer
// groups, so the groups can actually be deleted (Kafka rejects DeleteGroup with
// NonEmptyGroup while a member is joined).
//
// Sink workers are discovered from the live consumer groups rather than derived
// from the executions table: a CDC pipeline in streaming_only/never mode gets one
// sink-<id8>-<exec8> group PER EXECUTION, so a long-lived pipeline would mean
// hundreds of stop_sink round-trips, nearly all for workers that exited long ago.
// The broker knows which ones are actually still there.
func stopPipelineConsumers(ctx context.Context, mcpManager *mcp.ServerManager, tm *kafka.TopologyManager, stats pipelineWorkerStopper, pipelineID string) {
	if strings.TrimSpace(pipelineID) == "" {
		return
	}

	// In-process CDC table-stats / schema-change consumers.
	if stats != nil {
		stats.StopPipeline(pipelineID)
	}

	if mcpManager == nil {
		return
	}

	names := pipelineKafkaNames{id8: utils.SafeID8(pipelineID), uuid: pipelineID}
	groups := discoverSinkGroups(ctx, tm, names)

	client := mcp.NewClient(mcpManager)
	for _, group := range groups {
		if stopResp, _ := client.ExecuteWithContext(ctx, mcp.ExecuteRequest{
			Connector: "kafka-mcp-sink",
			Operation: "stop_sink",
			Config:    map[string]string{},
			Params: map[string]interface{}{
				"config": map[string]interface{}{
					"consumer_group": group,
				},
			},
		}); stopResp != nil && !stopResp.Success {
			// A worker that already exited is the normal case here, not a failure:
			// only the CDC streaming worker is long-lived, while batch and
			// per-execution workers stop on their own when their run ends.
			log.WithFields(log.Fields{
				"pipeline_id":     pipelineID,
				"consumer_group":  group,
				"stop_sink_error": stopResp.Error,
			}).Debug("stop_sink on teardown returned failure (likely no worker running; continuing)")
		}
	}
}

// discoverSinkGroups returns this pipeline's sink consumer groups that currently
// exist on the broker. Falls back to the two long-lived well-known names when the
// broker cannot be listed, so a listing failure still stops the streaming worker.
func discoverSinkGroups(ctx context.Context, tm *kafka.TopologyManager, names pipelineKafkaNames) []string {
	fallback := []string{"sink-" + names.id8, "sink-" + names.id8 + "-batch"}
	if tm == nil {
		return fallback
	}

	all, err := tm.ListConsumerGroupNames(ctx)
	if err != nil {
		log.WithError(err).WithField("pipeline_id", names.uuid).
			Warn("kafka teardown: cannot list consumer groups; stopping well-known sink workers only")
		return fallback
	}

	var groups []string
	for _, g := range all {
		if strings.HasPrefix(g, "sink-") && names.ownsGroup(g) {
			groups = append(groups, g)
		}
	}
	return groups
}

// cleanupPipelineKafkaResources deletes the topics and consumer groups owned by
// a pipeline, returning human-readable errors for the caller to report without
// failing the delete.
//
// Callers must have stopped the pipeline's consumers first (see
// stopPipelineConsumers) and deleted its Debezium connector — otherwise Kafka
// refuses to delete a group with live members, and Connect immediately recreates
// the topics it is still streaming into.
func cleanupPipelineKafkaResources(ctx context.Context, db *sql.DB, tm *kafka.TopologyManager, pipelineID string) []string {
	pipelineID = strings.TrimSpace(pipelineID)
	if pipelineID == "" {
		return nil
	}
	if tm == nil {
		log.WithField("pipeline_id", pipelineID).
			Warn("kafka teardown: no topology manager; topics and consumer groups will be left behind")
		return []string{"kafka teardown skipped: topology manager unavailable"}
	}

	names := pipelineKafkaNames{id8: utils.SafeID8(pipelineID), uuid: pipelineID}
	if !id8IsUnique(ctx, db, pipelineID, names.id8) {
		return []string{"kafka teardown skipped: pipeline id8 prefix is not unique"}
	}

	var errs []string

	// Consumer groups first: deleting a topic out from under a live group leaves
	// the group behind with offsets pointing at a topic that no longer exists.
	if groups, err := tm.ListConsumerGroupNames(ctx); err != nil {
		errs = append(errs, "list consumer groups: "+err.Error())
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("kafka teardown: list consumer groups failed")
	} else {
		for _, g := range groups {
			if !names.ownsGroup(g) {
				continue
			}
			if err := tm.DeleteConsumerGroup(ctx, g); err != nil {
				errs = append(errs, fmt.Sprintf("delete consumer group %s: %s", g, err.Error()))
				log.WithError(err).WithFields(log.Fields{"pipeline_id": pipelineID, "group": g}).
					Warn("kafka teardown: consumer group delete failed")
			}
		}
	}

	if topics, err := tm.ListTopicNamesFresh(ctx); err != nil {
		errs = append(errs, "list topics: "+err.Error())
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("kafka teardown: list topics failed")
	} else {
		for _, t := range topics {
			if !names.ownsTopic(t) {
				continue
			}
			if err := tm.DeleteTopic(ctx, t); err != nil {
				// Already gone is the desired end state, not a failure.
				if strings.Contains(strings.ToLower(err.Error()), "unknown topic") {
					continue
				}
				errs = append(errs, fmt.Sprintf("delete topic %s: %s", t, err.Error()))
				log.WithError(err).WithFields(log.Fields{"pipeline_id": pipelineID, "topic": t}).
					Warn("kafka teardown: topic delete failed")
			}
		}
	}

	return errs
}
