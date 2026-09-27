package cdc

import "strings"

// Historized Debezium engines.
//
// A HISTORIZED connector (Debezium's HistorizedRelationalDatabaseConnector) keeps a
// schema-history topic that it replays on every restart to rebuild table schemas,
// and, with include.schema.changes on, publishes each source DDL statement to the
// bare topic.prefix topic (rsync.cdc-<id8>). MySQL, MariaDB, SQL Server, Oracle and
// Db2 are historized. PostgreSQL decodes the WAL against the live catalog and
// MongoDB has no relational schema, so neither keeps a history topic nor emits DDL.
//
// Two orchestrator decisions hang off this, and both used to be made for every
// engine: the executor pre-created a schema-history topic for every CDC pipeline
// (one no PostgreSQL or MongoDB connector ever wrote), and the cdcstats agent
// subscribed a DDL consumer to the bare topic.prefix topic for every pipeline --
// which, because a sarama group subscription auto-creates its topic, minted an
// empty rsync.cdc-<id8> topic for every PostgreSQL and MongoDB pipeline.
//
// The Debezium connector makes the same split in connector.py (_HISTORIZED_ENGINES,
// which holds mysql/sqlserver/oracle after its _normalize_db_type folds mariadb and
// mssql onto them). That file has no Db2 class today; Db2 is listed here because
// Debezium's Db2 connector is historized, so a Db2 source added later is born with
// the right topics rather than inheriting PostgreSQL's. historized_test.go reads
// connector.py and fails if an engine it treats as historized is not historized here.
var historizedEngines = map[string]bool{
	"mysql":     true,
	"sqlserver": true,
	"oracle":    true,
	"db2":       true,
}

// normalizeHistorizedEngine folds a source type onto the engine names
// historizedEngines is keyed by, the way connector.py _normalize_db_type does:
// lower case, "-" to "_", and the aliases that name the same Debezium connector.
func normalizeHistorizedEngine(dbType string) string {
	v := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(dbType)), "-", "_")
	switch v {
	case "postgres":
		return "postgresql"
	case "mariadb":
		return "mysql"
	case "mssql", "ms_sql":
		return "sqlserver"
	default:
		return v
	}
}

// HistorizedEngine reports whether a CDC source of this type runs a historized
// Debezium connector, i.e. one that needs a schema-history topic and emits source
// DDL to the bare topic.prefix topic.
func HistorizedEngine(dbType string) bool {
	return historizedEngines[normalizeHistorizedEngine(dbType)]
}

// historizedConnectorPackages are the io.debezium.connector.<package> segments of
// the historized connector classes. mariadb is Debezium's own MariaDB connector
// package (io.debezium.connector.mariadb.MariaDbConnector); a MariaDB source run
// through the MySQL connector arrives here as "mysql".
var historizedConnectorPackages = map[string]bool{
	"mysql":     true,
	"mariadb":   true,
	"sqlserver": true,
	"oracle":    true,
	"db2":       true,
}

// HistorizedConnectorClass is HistorizedEngine for a running connector, keyed by its
// Kafka Connect connector.class (e.g. io.debezium.connector.mysql.MySqlConnector).
// Callers that read a live connector config use this instead of guessing the source
// type from the connector name.
func HistorizedConnectorClass(connectorClass string) bool {
	const ns = "io.debezium.connector."
	c := strings.ToLower(strings.TrimSpace(connectorClass))
	if !strings.HasPrefix(c, ns) {
		return false
	}
	pkg := strings.TrimPrefix(c, ns)
	if i := strings.IndexByte(pkg, '.'); i > 0 {
		pkg = pkg[:i]
	} else {
		return false
	}
	return historizedConnectorPackages[pkg]
}
