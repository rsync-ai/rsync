package executor

import (
	"context"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/pkg/llmscrub"
)

// A MongoDB CDC source must be a replica set or a sharded cluster: Debezium reads
// change streams, and a standalone mongod has none. Nothing notices that at connector
// creation. Kafka Connect accepts the connector (HTTP 201), the task reports RUNNING,
// and the worker then retries "$changeStream stage is only supported on replica sets"
// (server code 40573) with unlimited retries — so the pipeline shows Running forever
// and writes nothing.
//
// The mongodb connector already reads the server's topology in test_connection, from
// the hello reply (which needs no privileges). checkMongoDBCDCSource asks for that
// answer before start_sync and fails the run with the remediation instead.
//
// It blocks on proof only. is_replica_set=false on its own is NOT proof of a
// standalone: a mongos router has no setName and streams fine (Debezium 3.1 snapshots
// and streams through one — Atlas sharded clusters are this shape), and a connector
// image built before is_sharded_cluster existed reports false for every sharded
// cluster. So the run fails only when the result carries BOTH fields and both are
// false. A failed or timed-out call, a missing field, or a non-bool value all mean
// "unknown", and unknown starts exactly as it did before this check existed.

// mongoCDCTopologyCheckTimeout bounds the check. The connector's own server-selection
// timeout is 8s, so a reachable server answers well inside this; an unreachable one
// fails open when it expires.
const mongoCDCTopologyCheckTimeout = 20 * time.Second

// mongoStandaloneCDCError is the run error for a standalone source. It is a constant
// rather than built from the connector's reply, so no server-supplied text reaches the
// run error. diagnose.go keys MONGODB_NOT_REPLICA_SET (and ActionEscalate) on the
// phrase "not a replica set" — keep it.
const mongoStandaloneCDCError = "MongoDB CDC cannot start: the source is not a replica set or sharded cluster " +
	"(it is a standalone mongod), and change streams only run on a replica set or a sharded cluster. " +
	"Restart mongod with --replSet, run rs.initiate() once from mongosh, then re-run this pipeline."

// connectionResultTester has the shape of (*Agent).TestConnectionResult. The check
// takes it as a value so it can be exercised without an MCP client.
type connectionResultTester func(ctx context.Context, connectorType, connectorVersion string, config map[string]string) (bool, string, map[string]interface{})

// mongoResultIsStandalone reports whether a mongodb test_connection result proves a
// standalone deployment: both topology fields present, both bool, both false.
func mongoResultIsStandalone(result map[string]interface{}) bool {
	isReplicaSet, rsKnown := result["is_replica_set"].(bool)
	isSharded, shardedKnown := result["is_sharded_cluster"].(bool)
	return rsKnown && shardedKnown && !isReplicaSet && !isSharded
}

// checkMongoDBCDCSource runs the pre-start topology check for a CDC source. It returns
// a scrubbed, user-facing failure message when sourceType is mongodb and the
// connector reports a standalone deployment, and "" in every other case, including
// when the topology could not be determined.
func checkMongoDBCDCSource(ctx context.Context, test connectionResultTester, sourceType, connectorVersion string, config map[string]string) string {
	if test == nil || strings.ToLower(strings.TrimSpace(sourceType)) != "mongodb" {
		return ""
	}
	if !mongoConfigNamesAServer(config) {
		// Nothing to probe; the check would only spend the connector's server-selection
		// timeout on localhost. Debezium receives the same empty config and fails loudly.
		return ""
	}
	if strings.TrimSpace(connectorVersion) == "" {
		connectorVersion = "latest"
	}

	checkCtx, cancel := context.WithTimeout(ctx, mongoCDCTopologyCheckTimeout)
	defer cancel()
	ok, _, result := test(checkCtx, "mongodb", connectorVersion, config)
	if !ok {
		// TestConnectionResult has already logged the reason.
		log.Warn("MongoDB CDC topology check could not reach the source; starting CDC without it")
		return ""
	}
	if !mongoResultIsStandalone(result) {
		return ""
	}
	return llmscrub.Scrub(mongoStandaloneCDCError)
}

// mongoConfigNamesAServer reports whether config addresses a server the way the
// mongodb connector's _build_uri reads it: an explicit connection string, or a host.
func mongoConfigNamesAServer(config map[string]string) bool {
	for _, key := range []string{"connection_string", "mongodb_connection_string", "mongodb_uri", "uri", "host"} {
		if strings.TrimSpace(config[key]) != "" {
			return true
		}
	}
	return false
}

// sourceConnectorVersion is the source connector's version pin: the task's own
// Version, then the connection record's connector_version or version, else "latest" —
// the order the batch path in executor.go resolves it in.
func sourceConnectorVersion(src *ConnectorConfig) string {
	if src == nil {
		return "latest"
	}
	if v := strings.TrimSpace(src.Version); v != "" {
		return v
	}
	for _, key := range []string{"connector_version", "version"} {
		if v := strings.TrimSpace(src.Config[key]); v != "" {
			return v
		}
	}
	return "latest"
}
