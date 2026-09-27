package handlers

import (
	"database/sql"
	"net/http"
	"strings"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
)

// ConsumerTopicLag is one topic a consumer group reads, and how far behind it is.
type ConsumerTopicLag struct {
	Topic string `json:"topic"`
	// Table is the topic with the pipeline's Debezium prefix stripped
	// ("rsync.cdc-aa4c1a3c.public.orders" -> "public.orders"), or "" when the
	// topic is not shaped like one of this pipeline's table topics. Derived
	// server-side so the rule lives in one place.
	Table string `json:"table,omitempty"`
	// Lag carries NO omitempty: zero is the load-bearing value ("this topic is
	// drained"), and omitting it would leave a client unable to tell that from a
	// field the server did not send.
	Lag       int64 `json:"lag"`
	Committed int64 `json:"committed"`
}

// PipelineConsumer is one of the pipeline's Kafka consumer groups.
type PipelineConsumer struct {
	Group string `json:"group"`
	// Role: sink | sink_batch | sink_stream. What this consumer is for.
	Role string `json:"role"`
	// State from the broker: Stable, PreparingRebalance, CompletingRebalance,
	// Empty, Dead. ABSENT when the describe call could not answer, which the UI
	// must render as unknown and never as Stable. "Empty" beside a lag of 0 is
	// the reading lag alone cannot give: nobody is consuming.
	State   string `json:"state,omitempty"`
	Members *int   `json:"members,omitempty"`
	// TotalLag is the sum over this group's topics.
	TotalLag   int64              `json:"total_lag"`
	Topics     []ConsumerTopicLag `json:"topics"`
	MeasuredAt time.Time          `json:"measured_at"`
}

// GetPipelineConsumers lists the pipeline's Kafka consumer groups with their
// per-topic lag.
//
// GET /api/v1/pipelines/:id/consumers
//
// WHY THIS IS NOT THE ADMIN CONSUMER VIEW
//
// Admin -> Health reads sentinel_component_health, and it is the wrong data for
// this question twice over. The sentinel HealthMonitor writes it for the
// platform's own components (infrastructure and MCP connector containers) and
// writes no Kafka consumer rows at all — so it contains no pipeline data topic and
// no sink group. And that table has no workspace column, which is why its route is
// admin-only.
//
// The two views answer different questions for different people:
//
//	Admin -> Health        the PLATFORM's own workers      admin, infrastructure
//	this route             the PIPELINE's consumers        the pipeline's owners
//
// Keying the census on pipeline_id is what makes the second possible: the read is
// workspace-scoped through requirePipelineWorkspaceRole at the Viewer floor every
// other pipeline GET uses, with no feature flag — the same reasoning as
// GetPipelineAlerts. A consumer view gated on FEATURE_MONITORING_INFRA would be a
// pipeline surface hidden by an infrastructure switch, which is the mistake that
// route's own history records.
//
// SCOPE
//
// The rows are the pipeline's kafka-mcp-sink groups — the consumers that actually
// move rows to the destination. rsync's internal bookkeeping consumers
// (cdc-table-stats-*, cdc-schema-changes-*) are not written by the census: their
// lag is real but a user cannot act on it. The `role` column exists so they can be
// added later behind a toggle without a migration or an API change.
//
// An empty list is a valid answer and is NOT a 404: a pipeline that has not started
// its sink yet genuinely has no consumers, and the card says so. It is also what a
// deployment that has not run migration 117 reports, which is why the response
// carries `measured` — see below.
func GetPipelineConsumers(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	consumers, measured, err := loadPipelineConsumers(database, pipelineID)
	if err != nil {
		respondError(c, http.StatusInternalServerError, "pipeline_consumers_query_failed",
			"Failed to read this pipeline's consumers", err)
		return
	}

	// `measured` separates "the Sentinel has looked and this pipeline has no
	// consumers" from "nothing has looked yet". Without it the card cannot tell a
	// pipeline whose sink has not started from a deployment whose orchestrator is
	// not writing the census, and it would render the same confident empty state
	// for both — the same "absence reported as a finding" the health tiles were
	// just fixed for.
	c.JSON(http.StatusOK, gin.H{"consumers": consumers, "measured": measured})
}

// loadPipelineConsumers reads pipeline_consumer_lag and folds the (group, topic)
// rows into one entry per group.
//
// A missing table — a deployment that has not run migration 117 — is reported as
// "not measured", not as an error: the rest of the pipeline page is independently
// useful and must not 500 because one table is absent. A real query failure on an
// existing table is still an error, because answering 200 with no consumers would
// tell the user their pipeline has none.
func loadPipelineConsumers(database *sql.DB, pipelineID string) ([]PipelineConsumer, bool, error) {
	rows, err := database.Query(`
		SELECT consumer_group, topic, role, lag, committed, group_state, members, measured_at
		FROM pipeline_consumer_lag
		WHERE pipeline_id = $1::uuid
		ORDER BY consumer_group, topic
	`, pipelineID)
	if err != nil {
		if isUndefinedTable(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer rows.Close()

	// Insertion order is preserved so the response follows the query's ORDER BY
	// rather than Go's map iteration, which would reshuffle the card every poll.
	byGroup := map[string]*PipelineConsumer{}
	var order []string

	for rows.Next() {
		var group, topic, role string
		var lag, committed int64
		var state sql.NullString
		var members sql.NullInt64
		var measuredAt time.Time
		if err := rows.Scan(&group, &topic, &role, &lag, &committed, &state, &members, &measuredAt); err != nil {
			// A row we cannot read is a fault, not an absence. Returning the rows so
			// far would under-report the pipeline's consumers and their backlog.
			return nil, false, err
		}

		entry, seen := byGroup[group]
		if !seen {
			entry = &PipelineConsumer{Group: group, Role: role, MeasuredAt: measuredAt}
			if state.Valid {
				entry.State = state.String
			}
			if members.Valid {
				n := int(members.Int64)
				entry.Members = &n
			}
			byGroup[group] = entry
			order = append(order, group)
		}
		// The census writes one measured_at per tick, but a row could be left from
		// an earlier one if a write failed partway; the OLDEST is the honest answer
		// for the group, since that is how stale the group's picture actually is.
		if measuredAt.Before(entry.MeasuredAt) {
			entry.MeasuredAt = measuredAt
		}
		entry.TotalLag += lag
		entry.Topics = append(entry.Topics, ConsumerTopicLag{
			Topic:     topic,
			Table:     topicTableName(topic),
			Lag:       lag,
			Committed: committed,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	consumers := make([]PipelineConsumer, 0, len(order))
	for _, group := range order {
		consumers = append(consumers, *byGroup[group])
	}
	// Measured, including when the answer is "no consumers": the query succeeded
	// against an existing table, so the Sentinel has had its chance.
	return consumers, true, nil
}

// topicTableName reduces a Debezium CDC topic to the table it carries.
//
// Debezium's topic.prefix is the connector name, so a pipeline's table topics are
// "<kafka-prefix>cdc-<id8>.<db_or_schema>.<table>" (executor.go
// resolveCDCStreamTopic / debeziumTopicPrefixFor). Everything up to and including
// the connector segment is the same for every topic in the pipeline and carries no
// information for the reader, so the card shows what is left.
//
// Returns "" rather than a guess when the topic is not that shape — a signal,
// heartbeat, schema-history or batch data topic. The caller then shows the raw
// topic, which is always correct, instead of a mangled fragment that looks like a
// table name and is not one.
func topicTableName(topic string) string {
	// Find the "cdc-<something>" segment, wherever the configurable Kafka prefix
	// put it, and take what follows.
	for _, seg := range strings.Split(topic, ".") {
		if !strings.HasPrefix(seg, "cdc-") {
			continue
		}
		rest := topic[strings.Index(topic, seg)+len(seg):]
		rest = strings.TrimPrefix(rest, ".")
		// A bare "rsync.cdc-<id8>" (the pre-provisioned stream topic) has nothing
		// after the connector segment, and neither do the per-connector signal and
		// heartbeat topics, whose table half does not exist.
		if rest == "" || !strings.Contains(rest, ".") {
			return ""
		}
		return rest
	}
	return ""
}

// isUndefinedTable reports whether err is PostgreSQL's "relation does not exist"
// (SQLSTATE 42P01), i.e. a migration this deployment has not run yet.
//
// Matched on the message rather than on a driver error type because this package
// is driver-agnostic at the call site (lib/pq and pgx both reach it) and both
// spell the condition into the message. Narrow on purpose: any other failure stays
// an error, because a pipeline reported as having no consumers when the read
// simply broke is the "absence dressed as a finding" failure this area keeps
// producing.
func isUndefinedTable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "42p01") ||
		(strings.Contains(msg, "does not exist") && strings.Contains(msg, "relation"))
}
