// MongoDB pre-flight assessor.
//
// MongoDB has no SQL catalog to query from Go, so — like the generic
// ConnectorAssessor it replaces for this source type — it asks the mongodb MCP
// connector's own test_connection. For a CDC pipeline it also passes
// cdc_readiness=true, and the connector reports what Debezium will need:
//
//   - is_replica_set / is_sharded_cluster — change streams need one of the two
//     (a standalone mongod has none; Debezium then retries forever while the
//     task shows RUNNING);
//   - change_stream_access — the result of opening, and at once closing, a
//     change stream at the scope Debezium will watch;
//   - oplog_window_hours — how long a stopped pipeline can stay down and still
//     resume without a re-snapshot.
//
// A field the connector did not report (an older image, or a probe that could
// not run) yields no check: unknown is never a finding. Read-only throughout.
package assessor

import (
	"context"
	"fmt"
	"strings"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// mongoOplogWindowMinHours is the oplog window below which a pipeline stopped
// over a weekend or a long incident may no longer be able to resume.
const mongoOplogWindowMinHours = 24.0

// MongoDBAssessor implements SourceAssessor for MongoDB sources.
type MongoDBAssessor struct {
	connector *ConnectorAssessor
}

// NewMongoDBAssessor builds the MongoDB assessor. Pass the shared *mcp.Client.
func NewMongoDBAssessor(client connectorExecutor) *MongoDBAssessor {
	return &MongoDBAssessor{connector: NewConnectorAssessor(client)}
}

func (a *MongoDBAssessor) SourceType() string { return "mongodb" }

func (a *MongoDBAssessor) Assess(ctx context.Context, in Input) (*Result, error) {
	const connType = "mongodb"
	r := &Result{SourceType: connType, Checks: []Check{}}

	if a.connector == nil || a.connector.client == nil {
		r.Checks = append(r.Checks, unavailableCheck(connType, "assessment client not configured"))
		Summarize(r)
		return r, nil
	}
	version := resolveConnectorVersion(in)

	params := map[string]interface{}{}
	if in.IsCDC() {
		params["cdc_readiness"] = true
		params["collections"] = in.Tables
	}
	resp, err := a.connector.client.Execute(mcp.ExecuteRequest{
		Connector: connType,
		Version:   version,
		Operation: "test_connection",
		Config:    in.ConnectionConfig,
		Params:    params,
	})
	if err != nil {
		r.Checks = append(r.Checks, unavailableCheck(connType, err.Error()))
		Summarize(r)
		return r, nil
	}
	if resp == nil || !resp.Success {
		msg := ""
		if resp != nil {
			msg = strings.TrimSpace(resp.Error)
		}
		if msg == "" {
			msg = "the connector reported the connection is not ready"
		}
		r.Checks = append(r.Checks, classifyConnectorFailure(connType, msg))
		Summarize(r)
		return r, nil
	}

	r.Checks = append(r.Checks, Check{
		Code:     "CONNECTOR_CONNECTION",
		Severity: SeverityInfo,
		Passed:   true,
		Message:  "Connected and authenticated to mongodb successfully — required configuration and credentials are valid.",
	})
	if in.IsCDC() {
		r.Checks = append(r.Checks, mongoCDCChecks(resp.Result)...)
	}
	for _, table := range in.Tables {
		if t := strings.TrimSpace(table); t != "" {
			r.Checks = append(r.Checks, withObject(a.connector.probeTableRead(connType, version, in.ConnectionConfig, t), t))
		}
	}
	Summarize(r)
	return r, nil
}

// mongoCDCChecks turns the connector's CDC-readiness fields into checks.
func mongoCDCChecks(result map[string]interface{}) []Check {
	var checks []Check
	standalone := false
	if c, ok := mongoTopologyCheck(result); ok {
		checks = append(checks, c)
		standalone = !c.Passed
	}
	// On a standalone the change stream probe can only report "unsupported",
	// which the topology finding above already says with its fix.
	if c, ok := mongoChangeStreamCheck(result); ok && !standalone {
		checks = append(checks, c)
	}
	if c, ok := mongoOplogWindowCheck(result); ok {
		checks = append(checks, c)
	}
	return checks
}

// mongoTopologyCheck blocks on proof only, as the pre-start check in
// executor/mongodb_topology_check.go does: both fields present and false. A
// mongos has no setName, so is_replica_set=false alone proves nothing.
func mongoTopologyCheck(result map[string]interface{}) (Check, bool) {
	const code = "MONGODB_NOT_REPLICA_SET"
	isReplicaSet, rsKnown := result["is_replica_set"].(bool)
	isSharded, shardedKnown := result["is_sharded_cluster"].(bool)
	switch {
	case isReplicaSet || isSharded:
		return Check{
			Code: code, Severity: SeverityInfo, Passed: true,
			Message: "The source is a replica set or sharded cluster, so change streams are available.",
		}, true
	case rsKnown && shardedKnown:
		return Check{
			Code: code, Severity: SeverityError, Passed: false,
			Message: "The source is a standalone mongod. Change streams only run on a replica set or a sharded cluster, so CDC cannot start.",
			Remediation: &diagnose.Remediation{
				Steps: []string{
					"Restart mongod with --replSet <name> (or set replication.replSetName in mongod.conf)",
					"Run rs.initiate() once from mongosh",
					"Re-run the assessment",
				},
				CommandsToRun:    []string{"rs.initiate()"},
				DocURL:           diagnose.ErrorDocURL("mongodb-not-replica-set"),
				EstimatedMinutes: 15,
			},
		}, true
	default:
		return Check{}, false
	}
}

// mongoChangeStreamCheck reports whether the connection's user may open a
// change stream where Debezium will. Only a database-scoped denial blocks: the
// connector probes the deployment when collections span databases or are bare
// names, which is its best guess at the scope, not proof of it.
func mongoChangeStreamCheck(result map[string]interface{}) (Check, bool) {
	const code = "MONGODB_CHANGE_STREAM_UNAUTHORIZED"
	access, ok := result["change_stream_access"].(map[string]interface{})
	if !ok {
		return Check{}, false
	}
	status, _ := access["status"].(string)
	scope, _ := access["scope"].(string)
	database, _ := access["database"].(string)
	detail, _ := access["message"].(string)

	where := "the whole deployment"
	if scope == "database" && database != "" {
		where = fmt.Sprintf("database %q", database)
	}

	switch status {
	case "ok":
		return Check{
			Code: code, Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("Opened a change stream on %s.", where),
		}, true
	case "unauthorized":
		c := Check{
			Code: code, Passed: false,
			Message: fmt.Sprintf("The connection's user may not open a change stream on %s, which CDC reads from: %s", where, truncate(detail, 200)),
		}
		if scope == "database" && database != "" {
			c.Severity = SeverityError
			c.Remediation = &diagnose.Remediation{
				Steps: []string{
					fmt.Sprintf("Grant the user the built-in read role on %q — it carries the changeStream and find actions change streams need", database),
					"On Atlas: Database Access → edit the user → add the read role for this database",
					"Re-run the assessment",
				},
				CommandsToRun:    []string{fmt.Sprintf(`db.getSiblingDB("admin").grantRolesToUser("<user>", [{ role: "read", db: %q }])`, database)},
				DocURL:           diagnose.ErrorDocURL("mongodb-change-stream-access"),
				EstimatedMinutes: 5,
			}
			return c, true
		}
		c.Severity = SeverityWarning
		c.Remediation = &diagnose.Remediation{
			Steps: []string{
				"If the pipeline streams from more than one database, grant the user readAnyDatabase",
				"If every collection is in one database, select them as <database>.<collection> so CDC watches only that database",
				"Re-run the assessment",
			},
			CommandsToRun:    []string{`db.getSiblingDB("admin").grantRolesToUser("<user>", [{ role: "readAnyDatabase", db: "admin" }])`},
			DocURL:           diagnose.ErrorDocURL("mongodb-change-stream-access"),
			EstimatedMinutes: 5,
		}
		return c, true
	case "unsupported", "error":
		return Check{
			Code: "MONGODB_CHANGE_STREAM_UNVERIFIED", Severity: SeverityInfo, Passed: false,
			Message: fmt.Sprintf("Could not confirm a change stream opens on %s: %s", where, truncate(detail, 200)),
		}, true
	default:
		return Check{}, false
	}
}

// mongoOplogWindowCheck is advisory: a short window never blocks a start, it
// limits how long the pipeline may be stopped before it must re-snapshot.
func mongoOplogWindowCheck(result map[string]interface{}) (Check, bool) {
	const code = "MONGODB_OPLOG_WINDOW_SHORT"
	hours, ok := result["oplog_window_hours"].(float64)
	if !ok {
		return Check{}, false
	}
	if hours >= mongoOplogWindowMinHours {
		return Check{
			Code: code, Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("The oplog holds about %.1f hours of changes.", hours),
		}, true
	}
	return Check{
		Code: code, Severity: SeverityInfo, Passed: false,
		Message: fmt.Sprintf("The oplog holds only about %.1f hours of changes. A pipeline stopped or behind for longer than that cannot resume and must re-snapshot.", hours),
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"Keep at least 24 hours of oplog: on each replica set member run the command below (MongoDB 4.4+)",
				"On Atlas: edit the cluster → Additional Settings → set a minimum oplog window",
				"Re-run the assessment",
			},
			CommandsToRun:    []string{"db.adminCommand({ replSetResizeOplog: 1, minRetentionHours: 48 })"},
			DocURL:           diagnose.ErrorDocURL("mongodb-oplog-window-short"),
			EstimatedMinutes: 10,
		},
	}, true
}
