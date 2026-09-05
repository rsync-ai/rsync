package kafkaclient

import (
	"os"
	"strings"
)

// EnvTopicPrefix names the environment variable that decides the namespace every
// rsync-ai topic is created and consumed under.
const EnvTopicPrefix = "KAFKA_TOPIC_PREFIX"

// DefaultTopicPrefix is the namespace rsync-ai claims when the deployment does
// not say otherwise.
//
// The platform grew up owning its broker, so it named topics after the thing
// they carried: agent.planner.requests, pipeline.<id8>.data, cdc.<id8>,
// pii.scan.request. On a dedicated cluster those read well. On a customer's
// shared cluster — the BYO-Kafka shape the managed-Kubernetes deployment targets
// — they are anonymous: an operator listing topics cannot tell which ones this
// product created, which ones are safe to delete when they uninstall, or which
// ones to size retention for. Worse, "pipeline." and "agent." are generic enough
// to collide outright with another team's topics on the same cluster.
//
// A single owned prefix fixes all of that at once: every topic the product
// creates is greppable, and the confinement allowlist in the topology handlers
// gets a namespace it can actually defend.
const DefaultTopicPrefix = "rsync."

// Topic qualifies a logical topic name with the deployment's namespace.
//
// Every producer, consumer and admin call routes its topic name through here, so
// the prefix is decided once for the whole platform rather than at ~150 string
// literals. That matters more than the tidiness: a rename applied to producers
// but missed on consumers does not fail — the consumer simply subscribes to a
// topic nobody writes and blocks forever with no error. Funnelling both sides
// through one function makes that class of desync unrepresentable.
//
// The call sites keep spelling the logical name in full — Topic("agent.planner.
// requests"), not Topic(AgentPlannerRequests) — so the code still says what it
// talks to and a grep for the old name still lands on the right line.
//
// Qualification is idempotent. Topic names are persisted (pipelines.kafka_topic,
// the sink's subscription config, connector configs) and read back on the next
// run, so an already-qualified name arriving here a second time must not become
// rsync.rsync.cdc.abc12345.
func Topic(name string) string {
	name = strings.TrimSpace(name)
	prefix := TopicPrefix()
	if prefix == "" || name == "" || strings.HasPrefix(name, prefix) {
		return name
	}
	return prefix + name
}

// TopicPrefix resolves the configured namespace, normalized so it can be
// concatenated safely.
//
// Setting the variable to the empty string disables qualification entirely. That
// is the migration lever, not a supported end state: an existing deployment has
// live topics and committed consumer-group offsets under the unprefixed names,
// and renaming those is a coordinated, all-services-at-once deploy. Setting it
// empty lets such a deployment take this code first and rename deliberately,
// instead of having the two happen to coincide.
//
// The value is resolved per call rather than cached at init. Topic names are
// computed at wiring time, not per message, so the map lookup is free, and a
// cached value would have to be reset by hand in tests — the kind of shared
// mutable state that makes one test's prefix leak into the next.
func TopicPrefix() string {
	raw, ok := os.LookupEnv(EnvTopicPrefix)
	if !ok {
		return DefaultTopicPrefix
	}

	prefix := sanitizeTopicChars(strings.TrimSpace(raw))
	if prefix == "" {
		return ""
	}
	// Without a trailing separator the prefix runs into the name it qualifies
	// ("rsync" + "agent.x" = "rsyncagent.x"), which is a legal topic name and so
	// fails silently rather than loudly.
	if !strings.HasSuffix(prefix, ".") && !strings.HasSuffix(prefix, "-") && !strings.HasSuffix(prefix, "_") {
		prefix += "."
	}
	return prefix
}

// sanitizeTopicChars reduces s to the character set Kafka accepts in a topic
// name ([a-zA-Z0-9._-]). An operator-supplied prefix reaches this from the
// environment; a stray space or slash in it would make every topic the platform
// derives illegal at once, and the resulting InvalidTopicException names the
// derived topic rather than the prefix that poisoned it.
func sanitizeTopicChars(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '.', c == '_', c == '-':
			b.WriteByte(c)
		}
	}
	return b.String()
}

// Topics qualifies a list of topic names in place-safe fashion, for the many
// consumers that subscribe to a fixed set.
func Topics(names ...string) []string {
	out := make([]string, 0, len(names))
	for _, n := range names {
		out = append(out, Topic(n))
	}
	return out
}

// InNamespace reports whether topic belongs to the logical namespace ns,
// regardless of whether it carries the configured prefix.
//
// Guards that classify a topic by its namespace — "is this the batch-backfill
// topic rather than a Debezium stream?", "does this pipeline own this topic?" —
// must keep working across the rename. A deployment that adopts the prefix still
// has live topics minted under the bare names, and a guard that recognized only
// the qualified form would quietly misclassify every one of them: the teardown
// path would stop recognizing its own topics and orphan them on the customer's
// cluster, which is the opposite of what an owned namespace is for.
//
// Matching both forms is deliberate rather than transitional. The bare
// namespaces are ours either way, so there is no name this can claim that the
// platform did not create.
func InNamespace(topic, ns string) bool {
	topic = strings.TrimSpace(topic)
	if topic == "" || ns == "" {
		return false
	}
	return strings.HasPrefix(topic, ns) || strings.HasPrefix(topic, Topic(ns))
}
