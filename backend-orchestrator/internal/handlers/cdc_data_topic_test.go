package handlers

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/rsync-ai/backend-orchestrator/internal/cdcsnapshot"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

// The bug class: an orchestrator path names a CDC data topic as
// "<topic.prefix>.<include-list entry>" while Debezium writes another name.
// For SQL Server the entry is "<schema>.<table>" and the topic carries the
// database too, so the removed-table reaper looked for a topic that never exists
// and kept the real one forever (an orphan topic), and the sink respawn after a
// table edit subscribed to — and auto-created — the phantom name.

const sqlServerPrefix = "rsync.cdc-5a1e0001"

func sqlServerConnector(include string) map[string]interface{} {
	return map[string]interface{}{
		"connector.class":    "io.debezium.connector.sqlserver.SqlServerConnector",
		"topic.prefix":       sqlServerPrefix,
		"database.names":     "inventory",
		"table.include.list": include,
	}
}

func TestCDCDataTopicPerSource(t *testing.T) {
	cases := []struct {
		name   string
		cfg    map[string]interface{}
		entry  string
		want   string
		wantOK bool
	}{
		{"postgres", map[string]interface{}{"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "database.dbname": "app"},
			"public.orders", "p.public.orders", true},
		{"postgres escaped regex entry", map[string]interface{}{"connector.class": "io.debezium.connector.postgresql.PostgresConnector"},
			`public\.orders`, "p.public.orders", true},
		{"mysql", map[string]interface{}{"connector.class": "io.debezium.connector.mysql.MySqlConnector", "database.include.list": "shop"},
			"shop.orders", "p.shop.orders", true},
		{"mongodb", map[string]interface{}{"connector.class": "io.debezium.connector.mongodb.MongoDbConnector", "collection.include.list": "app.users"},
			"app.users", "p.app.users", true},
		{"oracle", map[string]interface{}{"connector.class": "io.debezium.connector.oracle.OracleConnector"},
			"INVENTORY.ORDERS", "p.INVENTORY.ORDERS", true},
		{"sqlserver adds the database", sqlServerConnector("dbo.orders"),
			"dbo.orders", sqlServerPrefix + ".inventory.dbo.orders", true},
		{"sqlserver entry already database-qualified", sqlServerConnector("inventory.dbo.orders"),
			"inventory.dbo.orders", sqlServerPrefix + ".inventory.dbo.orders", true},
		{"already a full topic name", sqlServerConnector("dbo.orders"),
			sqlServerPrefix + ".inventory.dbo.orders", sqlServerPrefix + ".inventory.dbo.orders", true},
		{"sqlserver with two databases is not guessed", map[string]interface{}{
			"connector.class": "io.debezium.connector.sqlserver.SqlServerConnector", "database.names": "a,b"},
			"dbo.orders", "", false},
		{"sqlserver without database.names is not guessed", map[string]interface{}{
			"connector.class": "io.debezium.connector.sqlserver.SqlServerConnector"},
			"dbo.orders", "", false},
		{"no prefix", map[string]interface{}{}, "public.orders", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prefix := connCfgString(tc.cfg, "topic.prefix")
			if prefix == "" && tc.name != "no prefix" {
				prefix = "p"
			}
			got, ok := cdcDataTopic(tc.cfg, prefix, tc.entry)
			if got != tc.want || ok != tc.wantOK {
				t.Fatalf("cdcDataTopic(%q) = (%q, %v), want (%q, %v)", tc.entry, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

// TestReaperDeletesTheSQLServerTopicDebeziumWrites drives Schedule end to end: a
// table removed from a SQL Server connector must lead to the delete of the
// topic Debezium actually wrote.
func TestReaperDeletesTheSQLServerTopicDebeziumWrites(t *testing.T) {
	realTopic := sqlServerPrefix + ".inventory.dbo.orders"
	admin := &fakeReapAdmin{topics: []string{sqlServerPrefix + ".inventory.dbo.users", realTopic}}
	offsets := &fakeReapOffsets{count: 10, drains: map[string]kafka.ConsumerGroupDrain{
		"sink-5a1e0001-stream": {LagByTopic: map[string]int64{realTopic: 0}},
	}}
	after := sqlServerConnector(`dbo\.users`)
	connect := &fakeReapConnect{
		configs: []map[string]interface{}{after},
		state:   cdcsnapshot.ConnectorState{Found: true, State: "RUNNING", TaskIncludes: [][]string{{`dbo\.users`}}},
	}
	r := newRemovedTopicReaper(admin, offsets, connect, &fakeExcluder{},
		func(context.Context, string) []string { return []string{"sink-5a1e0001-stream"} },
		2*time.Millisecond, time.Minute)
	t.Cleanup(r.Stop)

	r.Schedule(reapPipeline, "cdc-5a1e0001", after, []string{"dbo.orders"})
	waitForJobs(t, r)

	if got := admin.deletedTopics(); !reflect.DeepEqual(got, []string{realTopic}) {
		t.Fatalf("deleted = %v, want [%s]: the removed SQL Server table's topic was kept as an orphan", got, realTopic)
	}
}

// The sink respawn after a table edit must subscribe to the topics Debezium
// writes, not to phantom names a broker with auto-create would create empty.
func TestDeriveCDCSinkTopics_SQLServerCarriesTheDatabase(t *testing.T) {
	cfg := sqlServerConnector("dbo.orders,dbo.users")
	got := deriveCDCSinkTopics(cfg, connectorConfigString(cfg, "topic.prefix"), connectorIncludeList(cfg))
	want := []string{sqlServerPrefix + ".inventory.dbo.orders", sqlServerPrefix + ".inventory.dbo.users"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("respawn topics = %v, want %v", got, want)
	}
}
