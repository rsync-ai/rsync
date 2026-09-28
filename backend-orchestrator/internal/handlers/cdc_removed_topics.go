package handlers

import (
	"context"
	"database/sql"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/cdcstats"
	"github.com/rsync-ai/backend-orchestrator/internal/cdcsnapshot"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

// RemovedTopicReaper deletes the Kafka data topic of a table removed from a CDC
// connector's include list (BUG #20). Removing a table used to leave its topic,
// its stats consumer and its sink subscription behind for good; re-adding the
// table later then resumed from a stale topic.
//
// A topic is deleted only when nothing can still need it, checked in this order
// on every poll:
//
//  1. the connector config captures the table again (re-added) → stop, keep it;
//  2. a running task still captures it (the old task has not restarted yet) → wait;
//  3. a live consumer-group member still reads it → wait (the stats consumer is
//     told to drop it first; the sink drops it on the restart the gateway asks
//     for after every table edit). A member that still reads a deleted topic
//     would re-create it on a broker with auto.create.topics.enable;
//  4. the topic still holds messages and no sink group of the pipeline has read
//     it to the end → wait: rows the destination has not received are never
//     thrown away.
//
// A job that is still waiting at the deadline gives up and KEEPS the topic, with
// a warning naming the reason. Jobs live in memory: an orchestrator restart
// drops them and the topic stays (the pre-fix behaviour, never data loss).
type RemovedTopicReaper struct {
	admin      removedTopicAdmin
	offsets    removedTopicOffsets
	connect    removedTopicConnect
	stats      topicExcluder
	sinkGroups func(ctx context.Context, pipelineID string) []string

	poll     time.Duration
	deadline time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	jobs   map[string]bool // pipelineID|topic
	wg     sync.WaitGroup
}

// removedTopicAdmin is the slice of *kafka.TopologyManager the reaper uses.
type removedTopicAdmin interface {
	ListTopicNamesFresh(ctx context.Context) ([]string, error)
	TopicConsumerGroups(ctx context.Context, topic string) ([]string, error)
	DeleteTopic(ctx context.Context, name string) error
}

// removedTopicOffsets is the slice of *kafka.Manager the reaper uses.
type removedTopicOffsets interface {
	GetConsumerGroupDrain(groupID string) (kafka.ConsumerGroupDrain, error)
	TopicMessageCount(topic string) (int64, error)
}

// removedTopicConnect reads the connector from Kafka Connect.
type removedTopicConnect interface {
	Config(ctx context.Context, name string) (map[string]interface{}, error)
	State(ctx context.Context, name string) (cdcsnapshot.ConnectorState, error)
}

// topicExcluder is the CDC stats agent's per-topic switch.
type topicExcluder interface {
	ExcludeTopic(topic string)
	IncludeTopic(topic string)
}

const (
	removedTopicPoll     = 15 * time.Second
	removedTopicDeadline = 30 * time.Minute
)

// NewRemovedTopicReaper returns nil when the topology admin is unavailable; a nil
// reaper does nothing, so removed topics stay (the pre-fix behaviour).
func NewRemovedTopicReaper(db *sql.DB, tm *kafka.TopologyManager, km *kafka.Manager, stats *cdcstats.Agent) *RemovedTopicReaper {
	if tm == nil || km == nil {
		return nil
	}
	var excl topicExcluder
	if stats != nil {
		excl = stats
	}
	return newRemovedTopicReaper(tm, km, cdcsnapshot.NewConnectClient(getKafkaConnectURL()), excl,
		func(ctx context.Context, pipelineID string) []string {
			return SinkConsumerGroupsToStop(ctx, db, pipelineID)
		},
		removedTopicPoll, removedTopicDeadline)
}

func newRemovedTopicReaper(admin removedTopicAdmin, offsets removedTopicOffsets, connect removedTopicConnect, stats topicExcluder,
	sinkGroups func(context.Context, string) []string, poll, deadline time.Duration) *RemovedTopicReaper {
	ctx, cancel := context.WithCancel(context.Background())
	return &RemovedTopicReaper{
		admin: admin, offsets: offsets, connect: connect, stats: stats, sinkGroups: sinkGroups,
		poll: poll, deadline: deadline, ctx: ctx, cancel: cancel, jobs: map[string]bool{},
	}
}

// Stop cancels every job (their topics are kept) and waits for them to return.
func (r *RemovedTopicReaper) Stop() {
	if r == nil {
		return
	}
	r.cancel()
	r.wg.Wait()
}

// plainCollection matches an include-list entry that names one table
// (schema.table or db.collection) rather than a pattern: only such a name maps
// to exactly one topic.
var plainCollection = regexp.MustCompile(`^[A-Za-z0-9_$-]+(\.[A-Za-z0-9_$-]+)*$`)

// Readd tells the stats agent to read the topics of tables that are captured
// again, straight away rather than when the removal job notices.
func (r *RemovedTopicReaper) Readd(connCfg map[string]interface{}, tables []string) {
	if r == nil || r.stats == nil {
		return
	}
	prefix := connCfgString(connCfg, "topic.prefix")
	for _, t := range cdcDataTopics(connCfg, prefix, tables) {
		r.stats.IncludeTopic(t)
	}
}

// Schedule starts one job per removed table that names a single topic. connCfg
// is the connector config the removal was written into (for topic.prefix).
func (r *RemovedTopicReaper) Schedule(pipelineID, connectorName string, connCfg map[string]interface{}, removed []string) {
	if r == nil || len(removed) == 0 {
		return
	}
	// Debezium requires topic.prefix; without it the topic name would be a guess,
	// and a guessed name is never deleted.
	prefix := connCfgString(connCfg, "topic.prefix")
	if prefix == "" {
		return
	}
	for _, table := range removed {
		if !plainCollection.MatchString(table) {
			continue
		}
		// The name Debezium actually writes (SQL Server adds a database segment);
		// an entry it cannot name for certain is skipped, never guessed.
		topic, ok := cdcDataTopic(connCfg, prefix, table)
		if !ok {
			continue
		}
		key := pipelineID + "|" + topic
		r.mu.Lock()
		if r.jobs[key] {
			r.mu.Unlock()
			continue
		}
		r.jobs[key] = true
		r.mu.Unlock()
		r.wg.Add(1)
		go r.run(key, pipelineID, connectorName, table, topic)
	}
}

func (r *RemovedTopicReaper) run(key, pipelineID, connectorName, table, topic string) {
	defer r.wg.Done()
	defer func() {
		r.mu.Lock()
		delete(r.jobs, key)
		r.mu.Unlock()
	}()
	fields := log.Fields{"pipeline_id": pipelineID, "connector": connectorName, "table": table, "topic": topic}
	excluded := false
	release := func() {
		if excluded && r.stats != nil {
			r.stats.IncludeTopic(topic)
		}
	}
	deadline := time.Now().Add(r.deadline)
	ticker := time.NewTicker(r.poll)
	defer ticker.Stop()
	reason := "not checked yet"
	for {
		verdict, why := r.step(pipelineID, connectorName, table, topic, &excluded)
		switch verdict {
		case reapDone:
			// The stats consumer keeps ignoring the name: a broker can still list a
			// topic for a while after deleting it, and subscribing to it then would
			// auto-create it again. Readd lifts this when the table comes back.
			log.WithFields(fields).Info("cdc removed table: deleted its Kafka topic")
			return
		case reapKeep:
			log.WithFields(fields).WithField("reason", why).Info("cdc removed table: keeping its Kafka topic")
			release()
			return
		}
		reason = why
		if time.Now().After(deadline) {
			log.WithFields(fields).WithField("reason", reason).
				Warn("cdc removed table: its Kafka topic was not deleted in time and is kept; delete it by hand once the sink has applied it")
			release()
			return
		}
		select {
		case <-r.ctx.Done():
			release()
			return
		case <-ticker.C:
		}
	}
}

type reapVerdict int

const (
	reapWait reapVerdict = iota
	reapDone
	reapKeep
)

// step runs one poll of the job. excluded records whether the stats consumer
// was told to drop the topic, so a job that ends without deleting undoes it.
func (r *RemovedTopicReaper) step(pipelineID, connectorName, table, topic string, excluded *bool) (reapVerdict, string) {
	ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
	defer cancel()

	if verdict, why := r.connectorLetsGo(ctx, connectorName, table); verdict != reapDone {
		return verdict, why
	}

	names, err := r.admin.ListTopicNamesFresh(ctx)
	if err != nil {
		return reapWait, "listing topics failed: " + err.Error()
	}
	if !slices.Contains(names, topic) {
		return reapKeep, "the topic does not exist"
	}

	if r.stats != nil && !*excluded {
		r.stats.ExcludeTopic(topic)
		*excluded = true
	}
	readers, err := r.admin.TopicConsumerGroups(ctx, topic)
	if err != nil {
		return reapWait, "describing consumer groups failed: " + err.Error()
	}
	if len(readers) > 0 {
		return reapWait, "consumer group(s) still read the topic: " + strings.Join(readers, ", ")
	}

	if applied, why := r.sinkFinished(ctx, pipelineID, topic); !applied {
		return reapWait, why
	}

	// Last look right before the delete: a table added back since the first
	// check keeps its topic.
	if verdict, why := r.connectorLetsGo(ctx, connectorName, table); verdict != reapDone {
		return verdict, why
	}
	if err := r.admin.DeleteTopic(ctx, topic); err != nil {
		return reapWait, "deleting the topic failed: " + err.Error()
	}
	return reapDone, ""
}

// connectorLetsGo is reapDone when neither the connector config nor any
// running task captures table.
func (r *RemovedTopicReaper) connectorLetsGo(ctx context.Context, connectorName, table string) (reapVerdict, string) {
	cfg, err := r.connect.Config(ctx, connectorName)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			// Pipeline deleted: its teardown owns the topics now.
			return reapKeep, "the connector no longer exists"
		}
		return reapWait, "reading the connector config failed: " + err.Error()
	}
	if cdcsnapshot.IncludeListCaptures(cdcsnapshot.ParseIncludeList(cfg), table) {
		return reapKeep, "the table was added back"
	}
	cs, err := r.connect.State(ctx, connectorName)
	if err != nil {
		return reapWait, "reading the connector state failed: " + err.Error()
	}
	if !cs.Found {
		return reapKeep, "the connector no longer exists"
	}
	if len(cs.TaskIncludes) == 0 {
		return reapWait, "the connector has no task configs yet"
	}
	for _, inc := range cs.TaskIncludes {
		if cdcsnapshot.IncludeListCaptures(inc, table) {
			return reapWait, "a connector task still captures the table"
		}
	}
	return reapDone, ""
}

// sinkFinished reports whether the destination has everything the topic holds:
// the topic is empty, or one of the pipeline's sink groups has read it to the
// end. One group is enough — after a snapshot the sink moves from its -batch
// group to its -stream group, and the group it left keeps its old, stale lag.
func (r *RemovedTopicReaper) sinkFinished(ctx context.Context, pipelineID, topic string) (bool, string) {
	count, err := r.offsets.TopicMessageCount(topic)
	if err != nil {
		return false, "reading the topic's offsets failed: " + err.Error()
	}
	if count == 0 {
		return true, ""
	}
	behind := ""
	for _, g := range r.sinkGroups(ctx, pipelineID) {
		drain, err := r.offsets.GetConsumerGroupDrain(g)
		if err != nil {
			continue
		}
		lag, committed := drain.LagByTopic[topic]
		if !committed {
			continue
		}
		if lag == 0 {
			return true, ""
		}
		behind = g
	}
	if behind != "" {
		return false, "the sink has not applied the whole topic yet (group " + behind + " is behind)"
	}
	return false, "no sink group has read the topic, and it still holds messages"
}
