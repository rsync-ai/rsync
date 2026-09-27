package config

import "os"

// SchemaDriftEnabled reports the installation-wide schema-drift switch,
// RSYNC_SCHEMA_DRIFT_ENABLED. It is true only for the exact value "true", the
// same test the orchestrator applies (workers/executor.go, where it decides
// whether to start the healer's schema-change consumers, and
// agents/executor/schema_drift.go schemaDriftEnabled). docker-compose passes one
// value to both services, so the gateway and the orchestrator agree on it.
//
// The flag decides whether the three rsync.healer.* topics exist at all:
// rsync.healer.schema-changes, rsync.healer.approved-changes and
// rsync.healer.results are created, produced and consumed only when it is on.
// Every gateway call site that subscribes to or produces one of them asks this
// function first, because a kafka-go reader, a sarama consumer group and the
// UnifiedProducer all auto-create the topic they touch. A subscription left
// running with the flag off would re-create a topic the platform no longer
// provisions.
//
// It is read on every call rather than cached like FeatureFlags: the callers
// run once at wiring time or once per request, and tests flip it with
// t.Setenv.
func SchemaDriftEnabled() bool {
	return os.Getenv("RSYNC_SCHEMA_DRIFT_ENABLED") == "true"
}
