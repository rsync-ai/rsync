package config

import "os"

// SchemaDriftEnabled reports the installation-wide schema-drift switch,
// RSYNC_SCHEMA_DRIFT_ENABLED. It is true only for the exact value "true", the
// same test api-gateway applies (api-gateway internal/config SchemaDriftEnabled);
// docker-compose passes one value to both services, so the two agree on it.
//
// The flag decides whether the three rsync.healer.* topics exist at all:
// rsync.healer.schema-changes, rsync.healer.approved-changes and
// rsync.healer.results are created (kafka.TopologyManager.EnsurePlatformTopics),
// produced and consumed only when it is on. Every orchestrator call site that
// subscribes to or produces one of them asks this function first, because a
// sarama consumer group subscription and a produce both auto-create the topic
// they touch on a broker that allows it: a producer left running with the flag
// off would re-create a topic the platform no longer provisions.
//
// It is read on every call rather than cached: the callers run once at wiring
// time or once per event, and tests flip it with t.Setenv.
func SchemaDriftEnabled() bool {
	return os.Getenv("RSYNC_SCHEMA_DRIFT_ENABLED") == "true"
}
