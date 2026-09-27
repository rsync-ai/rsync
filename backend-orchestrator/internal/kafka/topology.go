package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/rsync-ai/backend-orchestrator/internal/config"
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
	// Growing partitions is safe only for a keyless topic, where a 1-partition
	// auto-created topic would starve every consumer in a group but one. It is NOT
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
// apart. A name this package DERIVES (pipeline.<id8>.data, whatever the planner
// POSTs) is ours to place, so it gets qualified into the deployment's namespace --
// that is what makes the topics this product creates on a customer's shared cluster
// identifiable, and what gives the confinement allowlist in the topology handlers
// something to defend.
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
// POST /api/v1/topology/topics, EnsurePlatformTopics,
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

// Platform topic geometry, shared by every topic EnsurePlatformTopics creates.
//
// The width and retention are NOT free choices: scripts/kafka-init-new-topics.sh,
// the Helm kafka-init job (deploy/helm/rsync-ai/templates/jobs/kafka-init.yaml) and
// docker-compose.quickstart.yml create the same topics, none of them ALTERs an
// existing one, so whichever runs first on a deployment wins permanently. On BYO
// Kafka there is no kafka-init container and this function is the ONLY creator, so a
// divergence would not be a race, it would be a guarantee.
// topology_retention_contract_test.go reads all three provisioners and fails if any
// of them disagrees with these values for pipeline.domain.events.
const (
	platformTopicPartitions = 3           // keyed records; only applies at CREATE
	platformTopicRetention  = "604800000" // 7 days
)

// platformTopicConfig is the config every platform topic is created with.
// pipeline.domain.events carries the api-gateway read-model projection; 7 days is
// the replay window the provisioners above agree on.
func platformTopicConfig() map[string]string {
	return map[string]string{
		"cleanup.policy":   "delete",
		"retention.ms":     platformTopicRetention,
		"compression.type": "snappy",
	}
}

// PlatformTopicNames lists the steady-state topics EnsurePlatformTopics creates,
// unqualified: the four every install has, plus the three rsync.healer.* topics of
// the schema-drift loop when config.SchemaDriftEnabled() reports it on.
//
//	pipeline.domain.events        pipeline lifecycle events, projected by api-gateway
//	rsync.notifications           every Slack and email alert the platform sends
//	pii.scan.request              api-gateway -> llm-service PII scanner
//	pii.scan.response             llm-service PII scanner -> api-gateway
//	rsync.healer.schema-changes   drift detected (sink worker, executor, cdcstats) -> healer
//	rsync.healer.approved-changes a user approved a DDL (api-gateway) -> healer
//	rsync.healer.results          healer outcome -> api-gateway notifier
//
// The healer topics are gated because their producers and consumers are: with the
// flag off nothing reads or writes them, and a topic created anyway is one an
// operator has to account for on a customer-managed cluster.
func PlatformTopicNames() []string {
	names := []string{
		"pipeline.domain.events",
		"rsync.notifications",
		"pii.scan.request",
		"pii.scan.response",
	}
	if config.SchemaDriftEnabled() {
		names = append(names,
			"rsync.healer.schema-changes",
			"rsync.healer.approved-changes",
			"rsync.healer.results",
		)
	}
	return names
}

// EnsurePlatformTopics creates the steady-state platform topics this and the sibling
// services produce to and consume from (PlatformTopicNames), each at
// platformTopicPartitions with platformTopicConfig.
//
// It exists because on a customer-managed cluster nothing else does: with
// auto.create.topics.enable=false the first produce is rejected, and with it on a
// topic is born carrying the BROKER's defaults, including a min.insync.replicas that
// may exceed its replication factor and make it permanently unwritable -- which for
// rsync.notifications means the mechanism that would have told somebody is the thing
// that stopped working.
//
// KeepExistingPartitions on every topic: their records are keyed (pipeline id, issue
// id), so widening a live topic would re-hash keys onto other partitions and let two
// consumers act on one pipeline concurrently. Re-sizing an existing topic is a
// separate decision from creating a new one, and this is only the second.
//
// A failed create does not stop the others: every topic is attempted and the
// failures come back together (errors.Join), so one topic the broker refuses cannot
// leave the rest of the platform uncreated. main.go calls this before any consumer of
// these topics starts, because a consumer-group subscription auto-creates the topic
// it joins at the broker's defaults.
func (tm *TopologyManager) EnsurePlatformTopics(ctx context.Context) error {
	// Replication factor: whatever KAFKA_REPLICATION_FACTOR asks for, else derived
	// from the live broker count. EnsureTopic clamps it either way.
	rf := replicationDefaults().forCluster(len(tm.client.Brokers()))

	var errs []error
	for _, name := range PlatformTopicNames() {
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("platform topic %s: %w", name, err))
			continue
		}
		cfg := TopicConfig{
			Name:                   name,
			Partitions:             platformTopicPartitions,
			ReplicationFactor:      rf,
			Config:                 platformTopicConfig(),
			KeepExistingPartitions: true,
		}
		if err := tm.EnsureTopic(ctx, cfg); err != nil {
			errs = append(errs, fmt.Errorf("platform topic %s: %w", name, err))
		}
	}
	return errors.Join(errs...)
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
	// Sarama's admin client is blocking and takes no context, so ctx can only
	// bind at the call boundary. Check it BEFORE taking the lock: a caller whose
	// deadline has already passed must not queue behind an in-flight admin call
	// and then spend another broker round-trip it no longer has budget for.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

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
	if err := ctx.Err(); err != nil {
		return nil, err
	}

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
	if err := ctx.Err(); err != nil {
		return err
	}

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
	if err := ctx.Err(); err != nil {
		return err
	}

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

// TopicConsumerGroups returns the consumer groups with a live member that is
// subscribed to, or assigned partitions of, topic. A topic that one of them still
// reads must not be deleted: the member's next metadata request would auto-create
// it again on a broker with auto.create.topics.enable, and a consumer may lose
// messages it has not applied yet.
func (tm *TopologyManager) TopicConsumerGroups(ctx context.Context, topic string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	tm.mu.RLock()
	defer tm.mu.RUnlock()

	listed, err := tm.admin.ListConsumerGroups()
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}
	names := make([]string, 0, len(listed))
	for name, protocolType := range listed {
		// Kafka Connect workers ("connect") and other non-consumer protocols carry
		// no topic subscription in the consumer wire format.
		if protocolType == "" || protocolType == "consumer" {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil, nil
	}
	described, err := tm.admin.DescribeConsumerGroups(names)
	if err != nil {
		return nil, fmt.Errorf("failed to describe consumer groups: %w", err)
	}
	return groupsReadingTopic(described, topic), nil
}

// groupsReadingTopic is TopicConsumerGroups' decision over described groups: a
// group reads topic when any member's subscription or assignment names it. A
// member whose metadata cannot be decoded counts as reading it — an unknown
// consumer keeps the topic rather than risk deleting what it reads.
func groupsReadingTopic(described []*sarama.GroupDescription, topic string) []string {
	var out []string
	for _, g := range described {
		if g == nil || (g.ProtocolType != "" && g.ProtocolType != "consumer") {
			continue
		}
		for _, m := range g.Members {
			if memberReadsTopic(m, topic) {
				out = append(out, g.GroupId)
				break
			}
		}
	}
	sort.Strings(out)
	return out
}

func memberReadsTopic(m *sarama.GroupMemberDescription, topic string) bool {
	if m == nil {
		return false
	}
	meta, merr := m.GetMemberMetadata()
	if merr == nil && meta != nil {
		for _, t := range meta.Topics {
			if t == topic {
				return true
			}
		}
	}
	asg, aerr := m.GetMemberAssignment()
	if aerr == nil && asg != nil {
		if _, ok := asg.Topics[topic]; ok {
			return true
		}
	}
	return merr != nil && aerr != nil
}

// SetTopicConfig sets per-topic configuration on an existing topic
// (IncrementalAlterConfigs SET), leaving every other override as it is.
func (tm *TopologyManager) SetTopicConfig(ctx context.Context, name string, entries map[string]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	alter := make(map[string]sarama.IncrementalAlterConfigsEntry, len(entries))
	for k, v := range entries {
		v := v
		alter[k] = sarama.IncrementalAlterConfigsEntry{Operation: sarama.IncrementalAlterConfigsOperationSet, Value: &v}
	}
	if err := tm.admin.IncrementalAlterConfig(sarama.TopicResource, name, alter, false); err != nil {
		return fmt.Errorf("failed to set config on topic '%s': %w", name, err)
	}
	delete(tm.topicCache, name)
	return nil
}
