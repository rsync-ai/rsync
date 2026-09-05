package kafka

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	"github.com/rsync-ai/shared/kafkaclient/saramaauth"
	log "github.com/sirupsen/logrus"
)

// TopicConfig represents configuration for creating a Kafka topic
type TopicConfig struct {
	Name              string            `json:"name"`
	Partitions        int32             `json:"partitions"`
	ReplicationFactor int16             `json:"replication_factor"`
	Config            map[string]string `json:"config,omitempty"`

	// KeepExistingPartitions leaves an already-existing topic exactly as it is
	// instead of growing it to Partitions.
	//
	// Growing partitions is safe for the control topics this package mints itself
	// (they are keyless, and the point of EnsureAgentControlTopics is that a
	// 1-partition auto-created topic starves every consumer but one). It is NOT
	// safe for a topic that carries KEYED data: Kafka hashes a key modulo the
	// partition count, so adding partitions silently re-routes a key to a
	// different partition and destroys the per-key ordering CDC depends on. The
	// pre-creation callers — the Debezium data topics and the incremental-snapshot
	// signal topic — set this, which also keeps them behaving exactly as they did
	// when they had their own creation path that could not repartition at all.
	//
	// Not serialized: this is an internal caller's decision, never something the
	// planner can ask for over POST /api/v1/topology/topics.
	KeepExistingPartitions bool `json:"-"`

	// NameIsAuthoritative marks a name that some OTHER component already decided
	// and is reading or writing under, so this package must not re-derive it.
	//
	// Debezium computes its own topic.prefix (connector.py _qualify_topic) and the
	// signal topic is minted through kafkaclient.Topic at the call site, so both
	// arrive here already namespaced. Qualifying such a name a second time is
	// harmless when the two sides agree and catastrophic when they do not: it
	// creates a topic under a name nobody produces to or consumes from, and the
	// pipeline reports running while streaming zero rows. So these names are
	// verified against the namespace and passed through, never rewritten.
	//
	// Not serialized, for the same reason as above: an HTTP caller must not be
	// able to opt out of namespace confinement.
	NameIsAuthoritative bool `json:"-"`
}

// TopicInfo represents information about an existing topic
type TopicInfo struct {
	Name              string            `json:"name"`
	Partitions        int               `json:"partitions"`
	ReplicationFactor int               `json:"replication_factor"`
	Config            map[string]string `json:"config,omitempty"`
	IsInternal        bool              `json:"is_internal"`
}

// TopologyManager manages Kafka topic topology
// This is the Go implementation for plan-time topic provisioning
type TopologyManager struct {
	client     sarama.Client
	admin      sarama.ClusterAdmin
	brokers    string
	mu         sync.RWMutex
	topicCache map[string]*TopicInfo
	cacheTime  time.Time
	cacheTTL   time.Duration
}

func defaultIfZeroI32(v int32, def int32) int32 {
	if v <= 0 {
		return def
	}
	return v
}

// normalizeTopicConfig applies creation-time defaults and reconciles the caller's
// stated durability intent with the cluster that actually exists.
//
// BOTH creation paths must run it. EnsureTopic is the internal one; CreateTopic is
// the planner-facing one behind POST /api/v1/topology/topics — and that is the path
// that actually carries an over-large request. The planner sends
// replication_factor=3 unconditionally together with min.insync.replicas=min(2,rf)
// (llm-service/src/agents/planner/strategies.py), which a 1- or 2-broker
// customer-managed cluster rejects outright with InvalidReplicationFactor. Clamping
// only inside EnsureTopic left that path unprotected.
func normalizeTopicConfig(cfg *TopicConfig, brokerCount int) error {
	// Every creation path runs this, which makes it the one place that can
	// guarantee the product never creates a topic outside its own namespace --
	// including via POST /api/v1/topology/topics, whose name comes from the
	// planner over HTTP.
	name, err := confineTopicName(cfg.Name, cfg.NameIsAuthoritative)
	if err != nil {
		return err
	}
	cfg.Name = name
	cfg.Partitions = defaultIfZeroI32(cfg.Partitions, 3)
	// Durability (RF + min.insync.replicas) is resolved in one place shared with
	// Manager.EnsureTopicExists, which is the CDC path and used to bypass all of it.
	cfg.applyReplicationPolicy(brokerCount, 1)
	return nil
}

// confineTopicName decides the name a topic is actually created under.
//
// There are two kinds of name here and conflating them is the bug this splits
// apart. A name this package DERIVES (agent.control.commands.planner,
// pipeline.<id8>.data, whatever the planner POSTs) is ours to place, so it gets
// qualified into the deployment's namespace -- that is what makes the topics this
// product creates on a customer's shared cluster identifiable, and what gives the
// confinement allowlist in the topology handlers something to defend.
//
// A name another component already OWNS is different. Debezium's topic.prefix is
// computed inside the connector and the incremental-snapshot signal topic is minted
// at the executor call site; both already carry the namespace. Re-qualifying such a
// name cannot help -- if the two sides agree, qualification is a no-op, and if they
// disagree, it creates a THIRD name that neither the producer writes to nor the sink
// reads from. That failure is invisible: creation succeeds, the pipeline reports
// running, and zero rows move. So an authoritative name is verified and returned
// unchanged, with the disagreement named in the log rather than papered over.
func confineTopicName(name string, authoritative bool) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("topic name is required")
	}
	if !authoritative {
		return kafkaclient.Topic(name), nil
	}
	// The warning is the diagnostic for a split-brain BYO-Kafka deployment: the
	// orchestrator and the Debezium connector read KAFKA_TOPIC_PREFIX from separate
	// containers, and nothing else in the system ever prints the two side by side.
	if prefix := kafkaclient.TopicPrefix(); prefix != "" && !strings.HasPrefix(name, prefix) {
		log.Warnf("⚠️ topic '%s' was named by another component and does not carry this "+
			"service's %s=%q — creating it under its own name, but the two components "+
			"disagree about the namespace and one of them is writing where the other is "+
			"not reading", name, kafkaclient.EnvTopicPrefix, prefix)
	}
	return name, nil
}

// brokerCount reports the live broker count, or 0 when it cannot be determined.
// 0 disables clamping: guessing at a cluster we cannot see would be worse than
// letting Kafka reject the request with its own, accurate error.
func (tm *TopologyManager) brokerCount() int {
	if tm.client == nil {
		return 0
	}
	return len(tm.client.Brokers())
}

// clampToCluster lowers a requested replication factor to what the cluster can
// actually satisfy, and then enforces the invariant min.insync.replicas <= RF.
//
// Callers state durability *intent*, not cluster facts: llm-service's planner
// asks for RF=3 unconditionally (strategies.py DEFAULT_REPLICATION) and pairs it
// with min.insync.replicas=min(2, rf). That is right for the bundled cluster and
// wrong for a customer's own Kafka, which is the deployment shape this has to
// survive. An over-large RF fails topic creation outright with
// InvalidReplicationFactor; the pipeline then dies much later at produce time
// with an error naming the topic rather than the cause.
//
// This is the only component holding a live broker view, so it is the only place
// intent can be reconciled with reality.
func (cfg *TopicConfig) clampToCluster(brokerCount int) {
	if brokerCount > 0 && int(cfg.ReplicationFactor) > brokerCount {
		log.Warnf("⚠️ topic '%s': replication factor %d exceeds broker count %d — clamping to %d",
			cfg.Name, cfg.ReplicationFactor, brokerCount, brokerCount)
		cfg.ReplicationFactor = int16(brokerCount)
	}
	cfg.clampMinInsyncReplicas()
}

// clampMinInsyncReplicas holds min.insync.replicas at or below the replication
// factor, whatever route the spec took to get here.
//
// This is the invariant, and it is deliberately NOT conditioned on the RF clamp
// above having fired — that gating was the bug. "RF exceeds the broker count" is
// merely the most common way to arrive at an unsatisfiable pair; a caller asking
// for a deliberately cheap RF=1 topic on a healthy 3-broker cluster reaches the
// same place without tripping it, and so does any path whose broker count is
// unknown. The result either way is a topic that is created successfully, appears
// in ListTopics, accepts a sink subscription, and then rejects every acks=all
// produce with NOT_ENOUGH_REPLICAS for the rest of its life — while the pipeline
// reports running and streams zero rows.
//
// A caller that never set min.insync.replicas does not acquire one here; that
// decision belongs to pinMinInsyncReplicas, which knows what the default should be.
func (cfg *TopicConfig) clampMinInsyncReplicas() {
	if cfg.ReplicationFactor < 1 || cfg.Config == nil {
		return
	}
	raw, ok := cfg.Config[minInsyncReplicasKey]
	if !ok {
		return
	}
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || v <= int(cfg.ReplicationFactor) {
		return
	}
	log.Warnf("⚠️ topic '%s': %s %d exceeds replication factor %d — clamping to %d "+
		"(a topic with %s above its RF is created successfully and is then permanently unwritable)",
		cfg.Name, minInsyncReplicasKey, v, cfg.ReplicationFactor, cfg.ReplicationFactor, minInsyncReplicasKey)
	cfg.Config[minInsyncReplicasKey] = strconv.Itoa(int(cfg.ReplicationFactor))
}

// NewTopologyManager creates a new topology manager
func NewTopologyManager(brokers string) (*TopologyManager, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_3_0_0

	// Resolve SASL/TLS from the environment. Note the caller only log.Warnf's
	// on failure, so without this a secured cluster leaves topology management
	// silently disabled rather than failing the boot.
	security, err := serviceSecurityConfig(brokers)
	if err != nil {
		return nil, err
	}

	// Create client. The broker list is split by ParseBrokers, which also
	// trims — the previous strings.Split kept the space in "b1:9092, b2:9092".
	client, err := saramaauth.NewClient(security, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	// Create admin client
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create Kafka admin: %w", err)
	}

	log.Info("✅ Kafka TopologyManager initialized")

	return &TopologyManager{
		client:     client,
		admin:      admin,
		brokers:    brokers,
		topicCache: make(map[string]*TopicInfo),
		cacheTTL:   30 * time.Second,
	}, nil
}

// EnsureTopic ensures a topic exists and has at least the requested partition count.
// - Creates the topic if missing
// - Increases partitions if topic exists but has fewer partitions than requested
// - Does not attempt to decrease partitions (Kafka doesn't support it)
func (tm *TopologyManager) EnsureTopic(ctx context.Context, cfg TopicConfig) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.ensureTopicLocked(ctx, cfg)
}

// ensureTopicLocked is the ONLY place in this service that asks Kafka to create a
// topic. Every other entry point -- TopologyManager.CreateTopic behind
// POST /api/v1/topology/topics, CreateTopicForPipeline, EnsureAgentControlTopics,
// and Manager.EnsureTopicExists on the CDC pre-creation path -- funnels through it.
//
// It is one function because the alternative was tried and failed silently: there
// were three hand-rolled sarama.TopicDetail builders, and a durability rule added to
// one of them was simply absent from the other two. The clamp's unit tests stayed
// green while the path that runs on every CDC start created RF=1 topics with no
// min.insync.replicas, which a broker defaulting to misr=2 turns into a topic that is
// created, listed, subscribable, and permanently unwritable. A single choke point is
// what makes "the policy applies" checkable rather than a claim about three copies,
// and topology_single_creator_test.go fails if a fourth ever appears.
//
// Caller must hold tm.mu.
func (tm *TopologyManager) ensureTopicLocked(ctx context.Context, cfg TopicConfig) error {
	if err := normalizeTopicConfig(&cfg, tm.brokerCount()); err != nil {
		return err
	}

	topics, err := tm.admin.ListTopics()
	if err != nil {
		return fmt.Errorf("failed to list topics: %w", err)
	}

	if detail, exists := topics[cfg.Name]; exists {
		if cfg.KeepExistingPartitions {
			log.Infof("📦 Topic '%s' already exists (%d partitions) — leaving it as it is",
				cfg.Name, detail.NumPartitions)
			return nil
		}
		// Ensure minimum partitions
		if cfg.Partitions > detail.NumPartitions {
			if err := tm.admin.CreatePartitions(cfg.Name, cfg.Partitions, nil, false); err != nil {
				return fmt.Errorf("failed to increase partitions for '%s': %w", cfg.Name, err)
			}
			log.Infof("📈 Increased topic '%s' partitions: %d -> %d", cfg.Name, detail.NumPartitions, cfg.Partitions)
			delete(tm.topicCache, cfg.Name)
		}
		return nil
	}

	// Topic missing: create it
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     cfg.Partitions,
		ReplicationFactor: cfg.ReplicationFactor,
	}
	if len(cfg.Config) > 0 {
		topicDetail.ConfigEntries = make(map[string]*string)
		for k, v := range cfg.Config {
			val := v
			topicDetail.ConfigEntries[k] = &val
		}
	}

	if err := tm.admin.CreateTopic(cfg.Name, topicDetail, false); err != nil {
		// Check if it's an "already exists" error (race condition)
		if strings.Contains(err.Error(), "already exists") {
			log.Infof("📦 Topic '%s' was created concurrently, skipping", cfg.Name)
			return nil
		}
		return fmt.Errorf("failed to create topic '%s': %w", cfg.Name, err)
	}

	log.Infof("✅ Created topic '%s' with %d partitions, replication=%d",
		cfg.Name, cfg.Partitions, cfg.ReplicationFactor)
	delete(tm.topicCache, cfg.Name)
	return nil
}

// EnsureAgentControlTopics provisions the orchestrator/Temporal control topics needed for
// agent command routing and workflow result signaling.
//
// This is required for horizontal scaling: if these topics are auto-created with 1 partition,
// only one consumer in the group will receive work.
func (tm *TopologyManager) EnsureAgentControlTopics(ctx context.Context, partitions int32) error {
	partitions = defaultIfZeroI32(partitions, 3)

	// Replication factor: whatever KAFKA_REPLICATION_FACTOR asks for, else derived
	// from the live broker count as before. EnsureTopic clamps it either way.
	rf := replicationDefaults().forCluster(len(tm.client.Brokers()))

	// Commands are transient; keep retention limited.
	commandsConfig := map[string]string{
		"cleanup.policy":   "delete",
		"retention.ms":     "86400000", // 1 day
		"compression.type": "snappy",
	}
	// Results must exist for the Temporal adapter (KafkaAdapter) to signal workflows.
	resultsConfig := map[string]string{
		"cleanup.policy":   "delete",
		"retention.ms":     "86400000", // 1 day
		"compression.type": "snappy",
	}

	topics := []TopicConfig{
		{Name: "agent.control.commands.intent", Partitions: partitions, ReplicationFactor: rf, Config: commandsConfig},
		{Name: "agent.control.commands.resolver", Partitions: partitions, ReplicationFactor: rf, Config: commandsConfig},
		{Name: "agent.control.commands.discovery", Partitions: partitions, ReplicationFactor: rf, Config: commandsConfig},
		{Name: "agent.control.commands.planner", Partitions: partitions, ReplicationFactor: rf, Config: commandsConfig},
		{Name: "agent.control.commands.validator", Partitions: partitions, ReplicationFactor: rf, Config: commandsConfig},
		{Name: "agent.control.commands.executor", Partitions: partitions, ReplicationFactor: rf, Config: commandsConfig},
		{Name: "agent.control.commands.cost_estimator", Partitions: partitions, ReplicationFactor: rf, Config: commandsConfig},
		{Name: "agent.control.commands.capability_resolver", Partitions: partitions, ReplicationFactor: rf, Config: commandsConfig},
		{Name: "agent.control.commands.connection_validator", Partitions: partitions, ReplicationFactor: rf, Config: commandsConfig},

		// Results topic used by V1 adapter path (and still useful for observability).
		{Name: "agent.control.results", Partitions: partitions, ReplicationFactor: rf, Config: resultsConfig},
		// DLQ for failed activities (Temporal adapter helper).
		{Name: "agent.failed.dlq", Partitions: partitions, ReplicationFactor: rf, Config: resultsConfig},
	}

	// The steady-state topics the orchestrator produces to but nothing ever created.
	//
	// They existed only because the broker auto-created them on first produce, which
	// is a setting this platform does not own on a customer-managed cluster: with
	// auto.create.topics.enable=false the produce is rejected, and with it on they are
	// born carrying the BROKER's defaults, including a min.insync.replicas that may
	// exceed their replication factor and make them permanently unwritable. Naming
	// them here is what lets the setting be turned off.
	//
	// KeepExistingPartitions on all three: they carry KEYED records (pipeline id,
	// task id), so widening a LIVE topic would re-hash keys onto other partitions
	// and let two consumers process one pipeline's events concurrently. Re-sizing
	// an existing topic is a separate decision from creating a new one, and this is
	// only the second.
	//
	// The width and retention below are NOT free choices -- two other provisioners
	// already create two of these topics, and neither ALTERs an existing one, so
	// whichever runs first wins permanently. On the deployment this whole change
	// targets (BYO Kafka, no kafka-init container) the orchestrator is the ONLY
	// creator, so a divergence here is not a race, it is a guarantee. The values
	// must therefore match, verbatim:
	//
	//	pipeline.domain.events    scripts/kafka-init-new-topics.sh:185  retention -1
	//	                          docker-compose.quickstart.yml:140     --partitions 3
	//	pipeline.agent.telemetry  scripts/kafka-init-new-topics.sh:189  retention 7d
	//	                          scripts/kafka-init-new-topics.sh:150  --partitions $PARTITIONS (3)
	//
	// pipeline.domain.events is the canonical event log the api-gateway projector
	// and websocket bridge rebuild read models from. Creating it with the 1-day
	// commands/results retention would silently discard that history after a day --
	// produce and consume both keep working and the loss surfaces only when a read
	// model is rebuilt and comes back empty.
	const (
		keyedPartitions  = 3 // matches kafka-init / quickstart; only applies at CREATE
		retentionForever = "-1"
		retentionSevenD  = "604800000"
	)
	eventLogConfig := map[string]string{
		"cleanup.policy":   "delete",
		"retention.ms":     retentionForever, // canonical event log -- never expire
		"compression.type": "snappy",
	}
	telemetryConfig := map[string]string{
		"cleanup.policy":   "delete",
		"retention.ms":     retentionSevenD, // 7 days, per kafka-init-new-topics.sh:188
		"compression.type": "snappy",
	}
	// agent.executor.responses has no second provisioner, so there is no value to
	// match -- but it was auto-created, which means it inherited the BROKER's
	// retention. 7 days is Kafka's own default, so naming it explicitly preserves
	// what a default broker already gave it rather than narrowing it to 1 day.
	// One partition, because auto-creation gave it one and its records are keyed.
	topics = append(topics,
		// Executor task results, consumed by the agent consumer registry.
		TopicConfig{Name: "agent.executor.responses", Partitions: 1,
			ReplicationFactor: rf, Config: telemetryConfig, KeepExistingPartitions: true},
		// Pipeline lifecycle events, projected into api-gateway read models.
		TopicConfig{Name: "pipeline.domain.events", Partitions: keyedPartitions,
			ReplicationFactor: rf, Config: eventLogConfig, KeepExistingPartitions: true},
		// Per-agent telemetry.
		TopicConfig{Name: "pipeline.agent.telemetry", Partitions: keyedPartitions,
			ReplicationFactor: rf, Config: telemetryConfig, KeepExistingPartitions: true},
	)

	// The notification and healer family, for the same reason as the three above:
	// nothing has ever created them, so they exist only where a broker auto-created
	// them on first produce. They are listed later than the rest because they were
	// the last to be noticed -- the scan in topology_produce_targets_test.go finds
	// produce targets written as LITERALS, and all but rsync.notifications are
	// produced through exported constants (healer.go:50-52, sentinel.go:23-24), so
	// they were invisible to the check that caught the others.
	//
	// This is the family a customer actually feels. rsync.notifications is every
	// Slack and email alert the platform sends; rsync.healer.{actions,results} is
	// the self-healing control loop. On a cluster with auto.create.topics.enable
	// off, the first produce to any of them is rejected and alerting is dead on
	// arrival. On a cluster with it on and the common MSK default of
	// min.insync.replicas=2 over an RF=1 topic, they are created, listed and
	// subscribable but permanently unwritable -- which is worse, because the
	// deployment looks healthy and the thing that has stopped working is the
	// mechanism that would have told somebody.
	//
	// Retention deliberately matches what a default broker already gives them
	// (7 days, Kafka's own default) rather than the 1 day the commands/results
	// topics use. Naming a value here makes it explicit without changing what any
	// existing deployment has, which is the same reasoning agent.executor.responses
	// carries above. KeepExistingPartitions for all of them: their records are
	// keyed (issue id, pipeline id, agent id), so widening a live topic would
	// re-hash keys onto other partitions and let two healers act on one issue.
	//
	// approved-changes and schema-changes are produced by OTHER services
	// (api-gateway's schema_evolution handler and the kafka sink worker), not by
	// this one, so the scan cannot see them at all. They are provisioned here
	// because this is the only service on a BYO-Kafka deployment that manages
	// topology -- there is no kafka-init container to fall back on.
	notificationConfig := telemetryConfig
	for _, name := range []string{
		"rsync.notifications",           // healer.go:1318, healthwatch/watchdog.go:333, sentinel/cdc_wal_watchdog.go:369
		"rsync.healer.actions",          // healer.go ActionTopic
		"rsync.healer.results",          // healer.go ResultsTopic
		"rsync.healer.approved-changes", // produced by api-gateway/internal/handlers/schema_evolution.go
		"rsync.healer.schema-changes",   // produced by the kafka-mcp-sink worker
		"rsync.agents.heartbeat",        // sentinel.go / common/heartbeat.go
		"rsync.sentinel.audit",          // sentinel.go AuditTopic
	} {
		topics = append(topics, TopicConfig{
			Name: name, Partitions: keyedPartitions, ReplicationFactor: rf,
			Config: notificationConfig, KeepExistingPartitions: true,
		})
	}

	// Cross-service steady-state topics: produced or consumed by api-gateway, the
	// Temporal adapter and llm-service, and provisioned here for the same reason as
	// the healer family above -- on a BYO-Kafka or Kubernetes deployment there is no
	// kafka-init container, so this is the only thing that creates a topic.
	//
	// Three of these values are NOT free choices. docker-compose.quickstart.yml:214-216
	// creates agent.planner.responses, pipeline.domain.events and pii.scan.response
	// with --partitions 3 and no --config, so they inherit the broker's default
	// retention. Neither creator ALTERs an existing topic, so whichever runs first on
	// a given deployment wins permanently, and a divergence here would mean the same
	// topic has different geometry depending on which path installed it. 3 partitions
	// and 7-day retention is what that file already produces on a default broker, so
	// these match it rather than restating it differently.
	//
	// pii.scan.request and pipeline.failed.dlq are created by NOTHING today, on any
	// path including quickstart -- they have only ever existed by auto-creation.
	//
	// pipeline.failed.dlq gets 7 days rather than the 1 day its sibling agent.failed.dlq
	// uses above. A dead-letter queue is read when somebody investigates, which is
	// rarely within a day of the failure, and 7 days is what a default broker already
	// gives this topic today -- so this preserves current behavior rather than
	// narrowing it. agent.failed.dlq is left alone deliberately: changing the retention
	// of a topic that already exists is a separate decision from naming a new one, and
	// this is only the second.
	for _, name := range []string{
		"agent.planner.responses", // llm-service planner -> api-gateway (main.go:455)
		"pii.scan.request",        // api-gateway (pii.go:300) -> llm-service PII scanner
		"pii.scan.response",       // llm-service PII scanner -> api-gateway (main.go:455)
		"pipeline.failed.dlq",     // backend-temporal-adapter (workflows/activities.go:169)
	} {
		topics = append(topics, TopicConfig{
			Name: name, Partitions: keyedPartitions, ReplicationFactor: rf,
			Config: telemetryConfig, KeepExistingPartitions: true,
		})
	}

	for _, t := range topics {
		if err := tm.EnsureTopic(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

// CreateTopic creates a Kafka topic with the specified configuration
// This is called by the Planner during plan-time topic provisioning
//
// It used to carry its own copy of the create-a-topic body — its own
// sarama.TopicDetail, its own already-exists handling — which is how the RF and
// min.insync.replicas policy came to apply on one route and not the others. It is now
// EnsureTopic with one difference, stated as configuration rather than as duplicated
// code: a create call that finds the topic already there leaves it alone.
func (tm *TopologyManager) CreateTopic(ctx context.Context, config TopicConfig) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// "Create" means create. Growing an existing topic's partitions is a separate,
	// destructive-for-keyed-data operation that a plan-time provisioning call has no
	// business performing implicitly; UpdatePartitions is the explicit route.
	config.KeepExistingPartitions = true
	return tm.ensureTopicLocked(ctx, config)
}

// CreateTopicForPipeline creates an optimally configured topic for a pipeline
// This is the main entry point for plan-time topic provisioning
func (tm *TopologyManager) CreateTopicForPipeline(ctx context.Context, pipelineID string, syncMode string, tableCount int, estimatedSizeGB float64) (*TopicInfo, error) {
	// Calculate optimal partitions
	partitions := tm.calculateOptimalPartitions(syncMode, tableCount, estimatedSizeGB)

	// Generate topic name
	topicName := tm.generateTopicName(pipelineID, syncMode)

	// Determine topic config based on sync mode
	topicConfig := tm.getTopicConfigForMode(syncMode)

	config := TopicConfig{
		Name:       topicName,
		Partitions: int32(partitions),
		// KAFKA_REPLICATION_FACTOR if the operator set one, else derived from the
		// live broker count as before. CreateTopic clamps it either way.
		ReplicationFactor: replicationDefaults().forCluster(len(tm.client.Brokers())),
		Config:            topicConfig,
	}

	// Create the topic
	if err := tm.CreateTopic(ctx, config); err != nil {
		return nil, err
	}

	return &TopicInfo{
		Name:              topicName,
		Partitions:        partitions,
		ReplicationFactor: int(config.ReplicationFactor),
		Config:            topicConfig,
	}, nil
}

// calculateOptimalPartitions calculates optimal partition count
// Based on the Kafka Topology Strategy document
func (tm *TopologyManager) calculateOptimalPartitions(syncMode string, tableCount int, estimatedSizeGB float64) int {
	const (
		MinPartitions  = 3
		MaxPartitions  = 50
		GBPerPartition = 2.0
	)

	var partitions int

	if syncMode == "cdc" || syncMode == "streaming" {
		// CDC: Partition by table count for ordering guarantee per table
		partitions = max(MinPartitions, tableCount)
	} else {
		// Batch: Partition by data size for parallelism
		partitions = max(MinPartitions, int(estimatedSizeGB/GBPerPartition))
	}

	// Apply bounds
	partitions = min(partitions, MaxPartitions)
	partitions = max(partitions, MinPartitions)

	log.Infof("📐 Calculated partitions: %d (mode=%s, tables=%d, size=%.1fGB)",
		partitions, syncMode, tableCount, estimatedSizeGB)

	return partitions
}

// generateTopicName generates a standardized topic name
func (tm *TopologyManager) generateTopicName(pipelineID string, syncMode string) string {
	shortID := pipelineID
	if len(pipelineID) > 8 {
		shortID = pipelineID[:8]
	}

	if syncMode == "cdc" || syncMode == "streaming" {
		return kafkaclient.Topic(fmt.Sprintf("cdc.%s", shortID))
	}
	return kafkaclient.Topic(fmt.Sprintf("pipeline.%s.data", shortID))
}

// getTopicConfigForMode returns topic configuration based on sync mode
func (tm *TopologyManager) getTopicConfigForMode(syncMode string) map[string]string {
	// KAFKA_MIN_INSYNC_REPLICAS if the operator set one, else the historical 1.
	// Whatever comes out is still clamped to the topic's final RF at creation.
	misr := strconv.Itoa(replicationDefaults().minInsyncReplicasOr(1))
	if syncMode == "cdc" || syncMode == "streaming" {
		return map[string]string{
			"cleanup.policy":     "compact", // Keep latest value per key
			"retention.ms":       "-1",      // Infinite retention for CDC
			minInsyncReplicasKey: misr,      // At least this many replicas must ack
			"compression.type":   "snappy",  // Good balance of speed/ratio
		}
	}
	return map[string]string{
		"cleanup.policy":     "delete",    // Delete old messages
		"retention.ms":       "604800000", // 7 days for batch
		minInsyncReplicasKey: misr,
		"compression.type":   "snappy",
	}
}

// ListTopics returns all topics with their info
func (tm *TopologyManager) ListTopics(ctx context.Context) (map[string]*TopicInfo, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Check cache
	if time.Since(tm.cacheTime) < tm.cacheTTL && len(tm.topicCache) > 0 {
		return tm.topicCache, nil
	}

	topics, err := tm.admin.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("failed to list topics: %w", err)
	}

	result := make(map[string]*TopicInfo)
	for name, detail := range topics {
		result[name] = &TopicInfo{
			Name:              name,
			Partitions:        int(detail.NumPartitions),
			ReplicationFactor: int(detail.ReplicationFactor),
			IsInternal:        strings.HasPrefix(name, "_"),
		}
	}

	// Update cache
	tm.topicCache = result
	tm.cacheTime = time.Now()

	return result, nil
}

// ListTopicNamesFresh returns every topic name straight from the broker,
// bypassing the 30s ListTopics cache.
//
// Teardown must not read the cache: a topic created moments ago (a new CDC
// table topic, say) would be absent from a stale entry and get left behind
// permanently, since nothing ever revisits a deleted pipeline.
func (tm *TopologyManager) ListTopicNamesFresh(ctx context.Context) ([]string, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	topics, err := tm.admin.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("failed to list topics: %w", err)
	}

	names := make([]string, 0, len(topics))
	for name := range topics {
		names = append(names, name)
	}
	return names, nil
}

// ListConsumerGroupNames returns every consumer group ID known to the cluster.
func (tm *TopologyManager) ListConsumerGroupNames(ctx context.Context) ([]string, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	groups, err := tm.admin.ListConsumerGroups()
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}

	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	return names, nil
}

// DeleteConsumerGroup deletes a consumer group and its committed offsets.
// Kafka rejects this with NonEmptyGroup while any member is still joined, so
// callers must stop the group's consumers first.
func (tm *TopologyManager) DeleteConsumerGroup(ctx context.Context, group string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if err := tm.admin.DeleteConsumerGroup(group); err != nil {
		return fmt.Errorf("failed to delete consumer group '%s': %w", group, err)
	}

	log.Infof("🗑️ Deleted consumer group '%s'", group)
	return nil
}

// GetTopic returns info about a specific topic
func (tm *TopologyManager) GetTopic(ctx context.Context, name string) (*TopicInfo, error) {
	topics, err := tm.ListTopics(ctx)
	if err != nil {
		return nil, err
	}

	info, exists := topics[name]
	if !exists {
		return nil, fmt.Errorf("topic '%s' not found", name)
	}

	return info, nil
}

// DeleteTopic deletes a topic
func (tm *TopologyManager) DeleteTopic(ctx context.Context, name string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	err := tm.admin.DeleteTopic(name)
	if err != nil {
		return fmt.Errorf("failed to delete topic '%s': %w", name, err)
	}

	log.Infof("🗑️ Deleted topic '%s'", name)

	// Invalidate cache
	delete(tm.topicCache, name)

	return nil
}

// UpdatePartitions increases the partition count for a topic
// Note: Kafka doesn't support decreasing partitions
func (tm *TopologyManager) UpdatePartitions(ctx context.Context, name string, count int32) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Get current partition count
	topics, err := tm.admin.ListTopics()
	if err != nil {
		return fmt.Errorf("failed to list topics: %w", err)
	}

	detail, exists := topics[name]
	if !exists {
		return fmt.Errorf("topic '%s' not found", name)
	}

	if count <= detail.NumPartitions {
		return fmt.Errorf("new partition count (%d) must be greater than current (%d)",
			count, detail.NumPartitions)
	}

	err = tm.admin.CreatePartitions(name, count, nil, false)
	if err != nil {
		return fmt.Errorf("failed to update partitions: %w", err)
	}

	log.Infof("📈 Updated topic '%s' partitions: %d -> %d", name, detail.NumPartitions, count)

	// Invalidate cache
	delete(tm.topicCache, name)

	return nil
}

// Close closes the topology manager
func (tm *TopologyManager) Close() error {
	if tm.admin != nil {
		tm.admin.Close()
	}
	if tm.client != nil {
		tm.client.Close()
	}
	log.Info("✅ TopologyManager closed")
	return nil
}

// Helper functions
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// SinkBootstrapServers is the bootstrap string handed to the kafka-mcp-sink
// connector over MCP when a sink is started or restarted.
//
// Both call sites used to pass the literal "kafka:29092" — the internal
// listener of the bundled broker. That is correct for the compose stack and
// wrong everywhere else: against a customer-managed cluster the sink dialed a
// hostname that does not exist, and because the sink is a separate process the
// failure surfaced as a stalled pipeline rather than a config error.
//
// Every shipped compose file sets KAFKA_BROKERS/KAFKA_BOOTSTRAP_SERVERS to
// kafka:29092, so this returns the same value it always did there.
//
// Deliberately FromEnv, not FromEnvForService: this resolves an ADDRESS and
// never opens a connection, so it has no client.id to declare. Naming a service
// here would imply an identity that never reaches a broker.
func SinkBootstrapServers() string {
	security, err := kafkaclient.FromEnv("kafka:29092")
	if err != nil || len(security.Brokers) == 0 {
		return "kafka:29092"
	}
	return strings.Join(security.Brokers, ",")
}
