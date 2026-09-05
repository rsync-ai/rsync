package kafka

import (
	"os"
	"strconv"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
)

// minInsyncReplicasKey is the Kafka topic config that decides whether an acks=all
// produce is allowed to succeed. It is spelled once because getting it wrong is
// invisible: an unrecognised topic config is accepted by the broker and ignored.
const minInsyncReplicasKey = "min.insync.replicas"

const (
	// EnvReplicationFactor is the replication factor topics created by this service
	// are born with.
	//
	// Nothing in this platform read a replication factor from anywhere before this:
	// every creation path either hardcoded 1 or derived min(3, brokers) from the live
	// broker list. A customer running a 5-broker cluster who wants RF=3 on their CDC
	// topics had no way to ask for it, and a customer on MSK — where a rolling patch
	// takes one broker down at a time — got RF=1 topics that go unavailable during
	// routine AWS maintenance.
	//
	// The value is a REQUEST, not a guarantee: it is still clamped down to the live
	// broker count, so a typo (RF=30) degrades to what the cluster can serve instead
	// of failing every topic creation with InvalidReplicationFactor.
	EnvReplicationFactor = "KAFKA_REPLICATION_FACTOR"

	// EnvMinInsyncReplicas is the companion durability floor.
	//
	// It has to travel with EnvReplicationFactor rather than being left to the broker
	// default, because the two are only meaningful together: min.insync.replicas > RF
	// yields a topic that is created, listed, and subscribable, and then rejects every
	// acks=all produce with NOT_ENOUGH_REPLICAS. Whatever an operator sets here is
	// clamped to the topic's final RF for exactly that reason.
	EnvMinInsyncReplicas = "KAFKA_MIN_INSYNC_REPLICAS"
)

// replicationPolicy is the operator's stated durability intent. A zero field means
// "not set" — the built-in default applies — rather than "zero replicas", which is
// not a thing a caller can ask for.
type replicationPolicy struct {
	replicationFactor int16
	minInsyncReplicas int
}

var (
	replicationPolicyOnce sync.Once
	replicationPolicyRead replicationPolicy
)

// replicationDefaults reads the policy from the environment once per process.
//
// Once, because these are deployment-level settings that cannot change under a
// running orchestrator, and because the warning about a malformed value should be
// logged once at first topic creation rather than on every CDC start.
func replicationDefaults() replicationPolicy {
	replicationPolicyOnce.Do(func() {
		replicationPolicyRead = readReplicationPolicyFromEnv()
	})
	return replicationPolicyRead
}

// readReplicationPolicyFromEnv is the env-reading half, kept out of the sync.Once so a
// test can prove the variable NAMES are wired without racing a process-wide latch that
// whichever test ran first has already tripped.
func readReplicationPolicyFromEnv() replicationPolicy {
	return parseReplicationPolicy(
		os.Getenv(EnvReplicationFactor),
		os.Getenv(EnvMinInsyncReplicas),
	)
}

// parseReplicationPolicy is the pure core of replicationDefaults, split out so the
// parsing rules are testable without mutating the process environment (and without
// fighting the sync.Once).
func parseReplicationPolicy(rf, misr string) replicationPolicy {
	return replicationPolicy{
		replicationFactor: int16(parsePositiveEnv(EnvReplicationFactor, rf)),
		minInsyncReplicas: parsePositiveEnv(EnvMinInsyncReplicas, misr),
	}
}

// parsePositiveEnv returns 0 for anything that is not a positive integer.
//
// A malformed value falls back to the built-in default rather than failing the boot:
// these variables tune durability, and refusing to start over a typo in one of them
// would take down a deployment that was working fine the release before.
func parsePositiveEnv(name, raw string) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 {
		log.Warnf("⚠️ %s=%q is not a positive integer — ignoring it and using the built-in default", name, raw)
		return 0
	}
	return v
}

// replicationFactorOr returns the operator's requested replication factor, or def
// when they did not state one.
func (p replicationPolicy) replicationFactorOr(def int16) int16 {
	if p.replicationFactor > 0 {
		return p.replicationFactor
	}
	return def
}

// minInsyncReplicasOr returns the operator's requested durability floor, or def when
// they did not state one.
func (p replicationPolicy) minInsyncReplicasOr(def int) int {
	if p.minInsyncReplicas > 0 {
		return p.minInsyncReplicas
	}
	return def
}

// forCluster is the replication factor for a topic this package mints itself, as
// opposed to one whose RF a caller stated.
//
// An explicit KAFKA_REPLICATION_FACTOR wins outright — that is the point of making
// it configurable, and clampToCluster still keeps a too-large value from wedging the
// cluster. With nothing set it reproduces the derivation these call sites already
// used: 1 on a single-broker cluster, min(3, brokers) beyond that.
func (p replicationPolicy) forCluster(brokerCount int) int16 {
	if p.replicationFactor > 0 {
		return p.replicationFactor
	}
	if brokerCount > 1 {
		return int16(min(3, brokerCount))
	}
	return 1
}

// pinMinInsyncReplicas writes an explicit min.insync.replicas onto a topic spec that
// does not carry one.
//
// Leaving it unset does not mean "no floor" — it means the BROKER's default applies,
// and that default is invisible from here. On MSK, and on most managed clusters, it
// is 2. A topic created RF=1 against such a broker inherits misr=2 and is born
// permanently unwritable: creation succeeds, ListTopics shows it, the sink subscribes
// to it, and every acks=all produce returns NOT_ENOUGH_REPLICAS with nothing in any
// log naming the replication factor as the cause. The pipeline reports running and
// streams zero rows.
//
// min(2, RF) is the value because it is what the planner already asks for
// (llm-service strategies.py pairs its RF with min.insync.replicas=min(2, rf)) and
// because it matches the common managed-cluster default, so pinning it is not a
// durability downgrade on a healthy cluster — it only ever removes the impossible
// case. An operator who wants a different floor sets EnvMinInsyncReplicas.
func (p replicationPolicy) pinMinInsyncReplicas(cfg *TopicConfig) {
	if cfg.ReplicationFactor < 1 {
		return
	}
	if _, ok := cfg.Config[minInsyncReplicasKey]; ok {
		return // caller stated one; clampMinInsyncReplicas has already bounded it
	}
	want := p.minInsyncReplicas
	if want <= 0 {
		want = 2
	}
	if want > int(cfg.ReplicationFactor) {
		want = int(cfg.ReplicationFactor)
	}
	if cfg.Config == nil {
		cfg.Config = make(map[string]string, 1)
	}
	cfg.Config[minInsyncReplicasKey] = strconv.Itoa(want)
}

// applyReplicationPolicy resolves a topic's durability settings against the cluster
// that actually exists. Every creation path in this package runs it — that is the
// whole point of it existing.
//
// EnsureTopicExists used to sit outside this entirely, creating topics with a
// hardcoded ReplicationFactor: 1 and no min.insync.replicas at all, which is the live
// path on every CDC start. The clamp's own tests were green throughout, so the RF/misr
// problem read as fixed while the path that runs in production was untouched.
//
// defaultRF is what to use when the caller stated no replication factor of their own;
// KAFKA_REPLICATION_FACTOR overrides it.
func (cfg *TopicConfig) applyReplicationPolicy(brokerCount int, defaultRF int16) {
	replicationDefaults().applyTo(cfg, brokerCount, defaultRF)
}

// applyTo is applyReplicationPolicy against an explicit policy rather than the
// process-wide one, so the ordering below can be tested with a policy a test chose.
//
// The order is load-bearing: default the RF, clamp it to the cluster, and only then
// decide the floor — pinning a floor before the RF settles would pin it against a
// replication factor that no longer exists by the time Kafka sees the request.
func (p replicationPolicy) applyTo(cfg *TopicConfig, brokerCount int, defaultRF int16) {
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = p.replicationFactorOr(defaultRF)
	}
	cfg.clampToCluster(brokerCount)
	p.pinMinInsyncReplicas(cfg)
}
