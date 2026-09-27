package sentinel

import (
	"context"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/handlers"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

// Roles a pipeline's sink consumer group can have. They are a function of the
// group's suffix, which sinkConsumerGroup() derives from the pipeline's own mode
// (executor/sink_consumer_group.go documents all three shapes).
const (
	roleSink       = "sink"        // sink-<pid8>          — CDC streaming, the common case
	roleSinkBatch  = "sink_batch"  // sink-<pid8>-batch    — hybrid-CDC backfill
	roleSinkStream = "sink_stream" // sink-<pid8>-stream   — cdc_mode streaming_only | never
)

// sinkGroupRole labels a group by its suffix.
//
// Matched on the suffix rather than rebuilt from the pipeline's mode on purpose:
// the mode is what the executor used when it STARTED the sink, and the census
// reports what is running now. A pipeline switched from hybrid to streaming-only
// still has a -batch group draining its backfill, and labelling that by the
// pipeline's current mode would mislabel it.
func sinkGroupRole(group string) string {
	switch {
	case strings.HasSuffix(group, "-batch"):
		return roleSinkBatch
	case strings.HasSuffix(group, "-stream"):
		return roleSinkStream
	default:
		return roleSink
	}
}

// consumerReading is one group's drain plus its broker-side description.
type consumerReading struct {
	group      string
	role       string
	lagByTopic map[string]int64
	// committedByTopic is the group's committed position on each topic, so each row
	// carries its own topic's position rather than the group-wide sum.
	committedByTopic map[string]int64
	state            string
	members          int
	// described is false when DescribeConsumerGroups could not answer for this
	// group, so state/members must be stored NULL rather than as "" / 0. A group
	// the broker has never seen is also not described — "has not started" is a
	// different fact from "has lost its members".
	described bool
}

// recordConsumerCensus writes one row per (consumer group, topic) for a pipeline
// into pipeline_consumer_lag, so the pipeline page can show which consumer reads
// which topic and how far behind each one is.
//
// WHY THIS IS SEPARATE FROM THE ALARM
//
// checkSinkConsumerLag decides whether to raise cdc-sink-lag-<id>, and that
// decision is safety-critical and well-tested: one group (the manifest's newest
// row, via ResolveSinkConsumerGroup), one drain reading, one two-signal stall
// verdict. This function does not touch any of it. It is pure observation, added
// alongside, so a census that fails or finds nothing cannot change whether the
// alarm fires.
//
// It does, however, reuse the alarm's reading for the primary group instead of
// taking its own. fetchConsumerGroupOffsets builds its OffsetFetch request over
// EVERY topic and partition in the cluster (manager.go), so on a broker with a few
// hundred topics a second read of the same group would double the tick's cost for
// no new information. Extra reads happen only for the groups the alarm did not
// look at, which for the common single-sink pipeline is none.
//
// WHY THE MANIFEST AND NOT DERIVED NAMES
//
// RegisteredSinkConsumerGroups returns the groups the sink itself registered. A
// derived name for a shape this pipeline never ran would show up as a consumer
// with no data, which a user reads as a broken consumer rather than an absent one.
// When the manifest is empty (an older pipeline, or a failed upsert — that path
// only logs), the primary group is still reported, so the card never claims a
// running pipeline has no consumers.
//
// Best-effort throughout: every failure logs at Debug and leaves whatever rows the
// previous tick wrote, which the reader already has to treat as possibly stale
// (that is what measured_at is for). Nothing here may abort the tick.
func (s *CDCSentinel) recordConsumerCensus(
	ctx context.Context,
	pipelineID string,
	primaryGroup string,
	primaryDrain kafka.ConsumerGroupDrain,
) {
	if s.db == nil || s.kafkaManager == nil {
		return
	}

	groups := handlers.RegisteredSinkConsumerGroups(ctx, s.db, pipelineID)
	if len(groups) == 0 && strings.TrimSpace(primaryGroup) != "" {
		groups = []string{primaryGroup}
	}
	if len(groups) == 0 {
		return
	}

	readings := make([]consumerReading, 0, len(groups))
	for _, group := range groups {
		r := consumerReading{group: group, role: sinkGroupRole(group)}
		if group == primaryGroup {
			// Already measured by the alarm above — do not pay for it twice.
			r.lagByTopic = primaryDrain.LagByTopic
			r.committedByTopic = primaryDrain.CommittedByTopic
		} else {
			drain, err := s.kafkaManager.GetConsumerGroupDrain(group)
			if err != nil {
				// A group that does not exist yet answers with an error. Absence is
				// not a reading, so this group contributes no rows this tick.
				log.WithError(err).WithFields(log.Fields{
					"pipeline_id":    pipelineID,
					"consumer_group": group,
				}).Debug("🛡️ consumer census: could not read drain")
				continue
			}
			r.lagByTopic = drain.LagByTopic
			r.committedByTopic = drain.CommittedByTopic
		}
		readings = append(readings, r)
	}
	if len(readings) == 0 {
		return
	}

	// State and members in one round trip for every group, rather than per group.
	names := make([]string, 0, len(readings))
	for _, r := range readings {
		names = append(names, r.group)
	}
	if described, err := s.kafkaManager.DescribeConsumerGroups(names); err != nil {
		// Lag without state is still worth showing; state renders as unknown.
		log.WithError(err).WithField("pipeline_id", pipelineID).
			Debug("🛡️ consumer census: could not describe groups")
	} else {
		for i := range readings {
			if d, ok := described[readings[i].group]; ok {
				readings[i].state = d.State
				readings[i].members = d.Members
				readings[i].described = true
			}
		}
	}

	s.writeConsumerCensus(ctx, pipelineID, readings)
}

// writeConsumerCensus replaces the pipeline's census rows with this tick's.
//
// Written as delete-then-insert inside one transaction rather than as an upsert.
// The set of (group, topic) pairs SHRINKS in normal operation — a table is removed
// from the pipeline and its topic is reaped, a hybrid pipeline's -batch worker
// finishes and its group is eventually collected — and an upsert would leave those
// rows behind forever at their last value. A consumer that no longer exists,
// displayed with a stale lag, is the failure mode the Sentinel's former per-topic
// consumer check (removed with the agent command bus) was rewritten to fix (its
// rows kept "Consumer group closed" and a lag of 4500 indefinitely). One
// transaction, so a reader never sees the
// pipeline with no consumers at all.
func (s *CDCSentinel) writeConsumerCensus(ctx context.Context, pipelineID string, readings []consumerReading) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).
			Debug("🛡️ consumer census: could not begin transaction")
		return
	}
	// Rollback is the no-op path after a successful Commit.
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM pipeline_consumer_lag WHERE pipeline_id = $1`, pipelineID); err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).
			Debug("🛡️ consumer census: could not clear previous rows")
		return
	}

	for _, r := range readings {
		// Sorted so a row's position is stable between ticks, which makes a diff of
		// two censuses readable by a person.
		topics := make([]string, 0, len(r.lagByTopic))
		for topic := range r.lagByTopic {
			topics = append(topics, topic)
		}
		sort.Strings(topics)

		for _, topic := range topics {
			var state interface{}
			var members interface{}
			if r.described {
				state = r.state
				members = r.members
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO pipeline_consumer_lag (
					pipeline_id, consumer_group, topic, role,
					lag, committed, group_state, members, measured_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
			`, pipelineID, r.group, topic, r.role,
				r.lagByTopic[topic], r.committedByTopic[topic], state, members); err != nil {
				log.WithError(err).WithFields(log.Fields{
					"pipeline_id":    pipelineID,
					"consumer_group": r.group,
					"topic":          topic,
				}).Debug("🛡️ consumer census: row write failed")
				return
			}
		}
	}

	if err := tx.Commit(); err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).
			Debug("🛡️ consumer census: commit failed")
	}
}
