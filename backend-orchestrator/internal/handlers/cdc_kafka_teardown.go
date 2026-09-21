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
//	        signals.<id8>                  executor.go incremental-snapshot signal channel
//	        signals.cdc-<id8>              debezium connector.py fallback for the same
//	        pipeline.<id8>.data(+.dlq)     batch backfill
//	groups  sink-<id8>                     CDC streaming sink
//	        sink-<id8>-batch               batch backfill sink
//	        sink-<id8>-stream              CDC streaming_only/never (stable per pipeline)
//	        sink-<id8>-<exec8>             ditto, when CDC_STREAMING_SINK_GROUP_PER_EXECUTION is on
//	        cdc-<id8>-signal               debezium connector.py signal.kafka.groupId
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
	// Signal topics carry no suffix, so they match exactly.
	for _, base := range []string{"signals." + n.id8, "signals.cdc-" + n.id8} {
		if topic == base || topic == kafkaclient.Topic(base) {
			return true
		}
	}
	return kafkaclient.InNamespace(topic, "pipeline."+n.id8+".")
}

// ownsSinkGroup reports whether a consumer group is one of this pipeline's
// kafka-mcp-sink workers, in either spelling. Only these have a worker to stop.
func (n pipelineKafkaNames) ownsSinkGroup(group string) bool {
	return matchesEitherSpelling(group, "sink-"+n.id8, "-", kafkaclient.Group)
}

// ownsGroup reports whether a consumer group belongs to this pipeline. Same
// anchoring rule as ownsTopic, with "-" as the sink-group separator.
func (n pipelineKafkaNames) ownsGroup(group string) bool {
	if n.ownsSinkGroup(group) {
		return true
	}
	for _, base := range []string{"cdc-schema-changes-" + n.uuid, "cdc-table-stats-" + n.uuid, "cdc-" + n.id8 + "-signal"} {
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
			Warn("pipeline teardown: id8 uniqueness check failed; skipping cleanup by derived name")
		return false
	}
	if others > 0 {
		log.WithFields(log.Fields{
			"pipeline_id": pipelineID,
			"id8":         id8,
			"others":      others,
		}).Warn("pipeline teardown: another pipeline shares this id8 prefix; skipping cleanup by derived name so its resources survive")
		return false
	}
	return true
}

// sinkStopExecutor is the one mcp.Client method the delete path uses to stop sink
// workers. It is an interface so handler tests can stand in for the sink service.
type sinkStopExecutor interface {
	ExecuteWithContext(ctx context.Context, req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error)
}

// newSinkStopExecutor returns a nil interface, not a typed nil pointer, when there is no
// MCP manager, so the callers' `sinks == nil` checks mean what they say.
func newSinkStopExecutor(mcpManager *mcp.ServerManager) sinkStopExecutor {
	if mcpManager == nil {
		return nil
	}
	return mcp.NewClient(mcpManager)
}

const (
	// sinkWorkerNotFound is kafka-mcp-sink's stop_sink answer when no worker holds the
	// group (connector.py stop_sink). The worker is already gone, which is what a delete
	// wants, so it counts as stopped.
	sinkWorkerNotFound = "Worker not found"

	// mcpStdioFallbackMarker is the text mcp.Client adds (mcp.stdioFallbackMarker) when
	// the call ran in a subprocess inside the orchestrator because the sink container was
	// unreachable. That subprocess has no workers at all, so its "Worker not found" says
	// nothing about the container where the real worker runs. A test ties the two.
	mcpStdioFallbackMarker = "mcp stdio fallback"
)

// stopSinkRequest is the stop_sink call for one consumer group.
func stopSinkRequest(group string) mcp.ExecuteRequest {
	return mcp.ExecuteRequest{
		Connector: "kafka-mcp-sink",
		Operation: "stop_sink",
		Config:    map[string]string{},
		Params: map[string]interface{}{
			"config": map[string]interface{}{
				"consumer_group": group,
			},
		},
	}
}

// sinkStopFailure explains why a stop_sink answer does not prove the worker is gone, or
// returns "" when it does.
func sinkStopFailure(resp *mcp.ExecuteResponse, err error) string {
	switch {
	case err != nil:
		return "the stop request failed: " + err.Error()
	case resp == nil:
		return "the sink service sent no answer"
	case resp.Success:
		return ""
	case strings.Contains(resp.Error, sinkWorkerNotFound) && !strings.Contains(resp.Error, mcpStdioFallbackMarker):
		return ""
	case strings.TrimSpace(resp.Error) == "":
		return "the sink service reported a failure without saying why"
	default:
		return "the stop request failed: " + resp.Error
	}
}

// stopSinkWorker asks the sink service to stop one group and returns "" once the worker
// is gone, otherwise why it may still be running.
//
// The call runs in a goroutine and the wait ends when ctx does. mcp.Client may spend up to
// a minute starting the sink container before it sends anything, and it does not watch ctx
// while it does, so waiting on the call itself would let one stop hold the whole delete.
// The channel is buffered so an abandoned call can still finish and exit.
func stopSinkWorker(ctx context.Context, sinks sinkStopExecutor, group string) string {
	if ctx.Err() != nil {
		return "it was not tried because the time allowed for stopping sink workers ran out"
	}
	type answer struct {
		resp *mcp.ExecuteResponse
		err  error
	}
	done := make(chan answer, 1)
	go func() {
		resp, err := sinks.ExecuteWithContext(ctx, stopSinkRequest(group))
		done <- answer{resp: resp, err: err}
	}()
	select {
	case a := <-done:
		return sinkStopFailure(a.resp, a.err)
	case <-ctx.Done():
		return "the sink service did not answer in the time allowed for stopping sink workers"
	}
}

// stopSinkWorkers stops each group in turn within ctx and returns one plain-words error
// per group whose worker may still be running. A failure used to be logged at Debug and
// dropped, so a delete reported success while a worker kept writing to the destination of
// a pipeline that no longer existed.
func stopSinkWorkers(ctx context.Context, sinks sinkStopExecutor, pipelineID string, groups []string) []string {
	if sinks == nil {
		return nil
	}
	var errs []string
	for _, group := range groups {
		reason := stopSinkWorker(ctx, sinks, group)
		if reason == "" {
			continue
		}
		log.WithFields(log.Fields{
			"pipeline_id":    pipelineID,
			"consumer_group": group,
			"reason":         reason,
		}).Warn("stop_sink did not stop the worker; it may still be writing to the destination")
		errs = append(errs, fmt.Sprintf("could not stop the sink worker for consumer group %s (%s); "+
			"it may still be writing to the destination. Stop it from the sink service or restart the kafka-mcp-sink container.",
			group, reason))
	}
	return errs
}

// kafkaTeardownBudgets gives each teardown phase its own time. Deliberately short: the
// pipeline row is already gone, so this is reclamation, not correctness — a slow broker
// must not hold the user's delete request open.
//
// The phases used to share one 45s context, so a sink stop that never answered spent
// the time the group and topic deletes needed. Separate budgets bound each phase alone;
// together they stay under the 45s api-gateway waits (runKafkaTeardownSync) with room for
// the reply, because an answer after that wait is dropped
// (TestDeleteBudgetsFitTheGatewayWaits).
type kafkaTeardownBudgets struct {
	sinkStop time.Duration // uniqueness check, group listing and every stop_sink
	cleanup  time.Duration // consumer group and topic deletes
}

var defaultKafkaTeardownBudgets = kafkaTeardownBudgets{
	sinkStop: 12 * time.Second,
	cleanup:  28 * time.Second,
}

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
	return teardownPipelineKafka(db, newSinkStopExecutor(mcpManager), tm, stats, cleanupPipelineKafkaResources, defaultKafkaTeardownBudgets)
}

// kafkaResourceCleanup deletes a pipeline's consumer groups and topics and returns one
// error per failure (cleanupPipelineKafkaResources).
type kafkaResourceCleanup func(ctx context.Context, db *sql.DB, tm *kafka.TopologyManager, pipelineID string) []string

// teardownPipelineKafka is TeardownPipelineKafka with the sink service, the group and
// topic cleanup and the phase budgets passed in, so tests can replace them.
func teardownPipelineKafka(db *sql.DB, sinks sinkStopExecutor, tm *kafka.TopologyManager, stats pipelineWorkerStopper, cleanupKafka kafkaResourceCleanup, budgets kafkaTeardownBudgets) gin.HandlerFunc {
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

		// Lower-case, because every producer of these names derives them from
		// pipelines.id::text, which Postgres renders lower-case. The delete path
		// takes the id from a URL param instead (requireUUIDParam does not
		// normalize, and uuid.Parse accepts upper-case), so an upper-case UUID
		// would otherwise produce an id8 that matches nothing and silently
		// reclaim nothing at all.
		pipelineID := strings.ToLower(strings.TrimSpace(req.PipelineID))
		log.WithField("pipeline_id", pipelineID).Info("Tearing down pipeline Kafka resources")

		stopCtx, cancelStop := context.WithTimeout(context.Background(), budgets.sinkStop)
		defer cancelStop()
		errs := stopPipelineConsumers(stopCtx, db, sinks, tm, stats, pipelineID)

		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), budgets.cleanup)
		defer cancelCleanup()
		errs = append(errs, cleanupKafka(cleanupCtx, db, tm, pipelineID)...)

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
//
// It returns one error per sink worker that may still be running. "Worker not found"
// is not one: only the CDC streaming worker is long-lived, while batch and
// per-execution workers stop on their own when their run ends.
//
// Every sink group here is derived from the 8-character id -- the manifest rows went
// with the pipeline row -- so none is stopped when another pipeline shares that id:
// the names could be that pipeline's live workers. The warning says so instead.
func stopPipelineConsumers(ctx context.Context, db *sql.DB, sinks sinkStopExecutor, tm *kafka.TopologyManager, stats pipelineWorkerStopper, pipelineID string) []string {
	if strings.TrimSpace(pipelineID) == "" {
		return nil
	}

	// In-process CDC table-stats / schema-change consumers.
	if stats != nil {
		stats.StopPipeline(pipelineID)
	}

	if sinks == nil {
		return nil
	}

	names := pipelineKafkaNames{id8: utils.SafeID8(pipelineID), uuid: pipelineID}
	if !id8IsUnique(ctx, db, pipelineID, names.id8) {
		return []string{sharedID8TeardownWarning}
	}
	return stopSinkWorkers(ctx, sinks, pipelineID, discoverSinkGroups(ctx, tm, names))
}

// sharedID8TeardownWarning is returned by the Kafka teardown when it stopped no sink
// worker because another pipeline's id starts with the same 8 characters.
const sharedID8TeardownWarning = "sink workers for this pipeline were not stopped: another pipeline's id starts " +
	"with the same 8 characters, so the worker names could belong to it. Check the sink service for a leftover worker."

// discoverSinkGroups returns this pipeline's sink consumer groups that currently
// exist on the broker. Falls back to the long-lived well-known names when the
// broker cannot be listed, so a listing failure still stops the streaming worker.
//
// Both the listing filter and the fallback accept the namespace-qualified
// spelling. They used to require a bare "sink-" prefix, which every group the
// executor mints since kafkaclient.Group ("rsync.sink-<pid8>") fails: the delete
// found no groups, stopped no worker, and the sink kept consuming a pipeline that
// no longer existed while the broker refused to delete its still-joined group.
// The fallback lists the bare spelling too, for a pipeline created before the
// namespace whose worker still runs under it.
func discoverSinkGroups(ctx context.Context, tm *kafka.TopologyManager, names pipelineKafkaNames) []string {
	fallback := derivedSinkGroups(names.uuid)
	if tm == nil {
		return fallback
	}

	all, err := tm.ListConsumerGroupNames(ctx)
	if err != nil {
		log.WithError(err).WithField("pipeline_id", names.uuid).
			Warn("kafka teardown: cannot list consumer groups; stopping well-known sink workers only")
		return fallback
	}

	return filterSinkGroups(all, names)
}

func filterSinkGroups(all []string, names pipelineKafkaNames) []string {
	var groups []string
	for _, g := range all {
		if names.ownsSinkGroup(g) {
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
		left := forEachOwned(ctx, groups, names.ownsGroup, func(g string) {
			if err := tm.DeleteConsumerGroup(ctx, g); err != nil {
				errs = append(errs, fmt.Sprintf("delete consumer group %s: %s", g, err.Error()))
				log.WithError(err).WithFields(log.Fields{"pipeline_id": pipelineID, "group": g}).
					Warn("kafka teardown: consumer group delete failed")
			}
		})
		if left > 0 {
			errs = append(errs, fmt.Sprintf(
				"kafka teardown ran out of time: %d consumer group(s) left behind", left))
			log.WithField("pipeline_id", pipelineID).WithField("remaining", left).
				Warn("kafka teardown: budget expired before consumer groups were deleted")
		}
	}

	if topics, err := tm.ListTopicNamesFresh(ctx); err != nil {
		errs = append(errs, "list topics: "+err.Error())
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("kafka teardown: list topics failed")
	} else {
		left := forEachOwned(ctx, topics, names.ownsTopic, func(t string) {
			if err := tm.DeleteTopic(ctx, t); err != nil {
				// Already gone is the desired end state, not a failure.
				if strings.Contains(strings.ToLower(err.Error()), "unknown topic") {
					return
				}
				errs = append(errs, fmt.Sprintf("delete topic %s: %s", t, err.Error()))
				log.WithError(err).WithFields(log.Fields{"pipeline_id": pipelineID, "topic": t}).
					Warn("kafka teardown: topic delete failed")
			}
		})
		if left > 0 {
			errs = append(errs, fmt.Sprintf(
				"kafka teardown ran out of time: %d topic(s) left behind", left))
			log.WithField("pipeline_id", pipelineID).WithField("remaining", left).
				Warn("kafka teardown: budget expired before topics were deleted")
		}
	}

	return errs
}

// forEachOwned runs fn over every entry of a cluster listing this pipeline owns,
// re-checking the caller's budget before each one, and returns how many owned
// entries it did NOT get to (0 = the whole list was processed).
//
// Two things here are load-bearing.
//
// The budget is re-checked BETWEEN items, not once up front: every fn is a
// broker round-trip, so a teardown given 5 seconds and 200 topics would
// otherwise run long past its deadline and report success for deletes that were
// refused. Each remaining item is then reported, not silently dropped.
//
// The remainder is counted from the LOOP INDEX, not from a count of owned items
// seen so far. The listing interleaves other pipelines' names, so those two
// numbers diverge, and counting from the owned-so-far counter re-counts entries
// already deleted — over-reporting what is left on the cluster. That number is
// what an operator uses to decide what to clean up by hand, so it has to be
// exact; on an all-owned listing the two agree, which is what makes the wrong
// one look right.
func forEachOwned(ctx context.Context, listed []string, owns func(string) bool, fn func(string)) int {
	for i, name := range listed {
		if !owns(name) {
			continue
		}
		if ctx.Err() != nil {
			return countOwned(listed[i:], owns)
		}
		fn(name)
	}
	return 0
}

func countOwned(listed []string, owns func(string) bool) int {
	n := 0
	for _, name := range listed {
		if owns(name) {
			n++
		}
	}
	return n
}
