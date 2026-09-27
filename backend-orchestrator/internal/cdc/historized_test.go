package cdc

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/connectorpaths"
)

// TestHistorizedEngineSplitsTheEngines pins which source types keep a schema-history
// topic and a bare topic.prefix DDL topic. The PostgreSQL rows are the point: every
// PostgreSQL-family and MongoDB pipeline used to get both, and neither connector ever
// writes either.
func TestHistorizedEngineSplitsTheEngines(t *testing.T) {
	historized := []string{
		"mysql", "MySQL", "mariadb", "MariaDB",
		"sqlserver", "SQLServer", "mssql", "ms-sql", "ms_sql",
		"oracle", "Oracle", "db2", "DB2",
	}
	for _, s := range historized {
		if !HistorizedEngine(s) {
			t.Errorf("HistorizedEngine(%q) = false, want true: Debezium keeps a schema "+
				"history for this engine and restarts fail without the topic", s)
		}
	}

	notHistorized := []string{"", "  "}
	// Every PostgreSQL-family member and raw spelling the golden list knows.
	notHistorized = append(notHistorized, postgresFamilyFromGolden(t)...)
	notHistorized = append(notHistorized, "mongodb", "MongoDB", "snowflake", "bigquery")
	for _, s := range notHistorized {
		if HistorizedEngine(s) {
			t.Errorf("HistorizedEngine(%q) = true, want false: this connector keeps no "+
				"schema history and emits no DDL, so the orchestrator would create two "+
				"topics nothing writes", s)
		}
	}
}

func TestHistorizedConnectorClass(t *testing.T) {
	cases := map[string]bool{
		"io.debezium.connector.mysql.MySqlConnector":         true,
		"io.debezium.connector.mariadb.MariaDbConnector":     true,
		"io.debezium.connector.sqlserver.SqlServerConnector": true,
		"io.debezium.connector.oracle.OracleConnector":       true,
		"io.debezium.connector.db2.Db2Connector":             true,
		" IO.DEBEZIUM.CONNECTOR.MYSQL.MySqlConnector ":       true,

		"io.debezium.connector.postgresql.PostgresConnector": false,
		"io.debezium.connector.mongodb.MongoDbConnector":     false,
		"io.debezium.connector.mysql":                        false, // no class segment
		"com.example.mysql.MySqlConnector":                   false,
		"":                                                   false,
	}
	for class, want := range cases {
		if got := HistorizedConnectorClass(class); got != want {
			t.Errorf("HistorizedConnectorClass(%q) = %v, want %v", class, got, want)
		}
	}
}

// TestHistorizedEnginesCoverTheConnectors reads the Debezium connector's own
// _HISTORIZED_ENGINES and its engine -> connector.class map, from the CURRENT
// version directory, and fails if the orchestrator disagrees with it. A disagreement
// is not cosmetic: an engine the connector historizes but the orchestrator does not
// gets no pre-created history topic (the connector then fails on its first restart
// on a broker that does not auto-create), and the reverse creates topics nothing
// writes.
func TestHistorizedEnginesCoverTheConnectors(t *testing.T) {
	src := debeziumConnectorPy(t)

	m := regexp.MustCompile(`_HISTORIZED_ENGINES\s*=\s*frozenset\(\{([^}]*)\}\)`).FindStringSubmatch(src)
	if m == nil {
		t.Fatal("connector.py no longer defines _HISTORIZED_ENGINES as a frozenset literal; " +
			"re-point this guard rather than deleting it")
	}
	pyHistorized := map[string]bool{}
	for _, q := range regexp.MustCompile(`"([a-z0-9_]+)"`).FindAllStringSubmatch(m[1], -1) {
		pyHistorized[q[1]] = true
	}
	if len(pyHistorized) == 0 {
		t.Fatal("parsed an empty _HISTORIZED_ENGINES; the comparison below would be vacuous")
	}

	supported := regexp.MustCompile(`"([a-z0-9_]+)":\s*"(io\.debezium\.connector\.[A-Za-z0-9_.]+)"`).FindAllStringSubmatch(src, -1)
	if len(supported) < 3 {
		t.Fatalf("found %d engine -> connector.class entries in connector.py, want at least 3", len(supported))
	}
	for _, s := range supported {
		engine, class := s[1], s[2]
		want := pyHistorized[engine]
		if got := HistorizedEngine(engine); got != want {
			t.Errorf("connector.py historized(%q) = %v but HistorizedEngine = %v", engine, want, got)
		}
		if got := HistorizedConnectorClass(class); got != want {
			t.Errorf("connector.py historized(%q) = %v but HistorizedConnectorClass(%q) = %v",
				engine, want, class, got)
		}
	}
}

// postgresFamilyFromGolden returns every member, alias and raw spelling in
// shared/postgres_family_golden.json, the list the CDC provisioning order rule pins.
func postgresFamilyFromGolden(t *testing.T) []string {
	t.Helper()
	path := filepath.Join(repoRoot(t), "shared", "postgres_family_golden.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var g struct {
		Members    []string          `json:"members"`
		Aliases    map[string]string `json:"aliases"`
		RawMembers []string          `json:"raw_members"`
	}
	if err := json.Unmarshal(data, &g); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	out := append([]string{}, g.Members...)
	out = append(out, g.RawMembers...)
	for k := range g.Aliases {
		out = append(out, k)
	}
	if len(out) == 0 {
		t.Fatalf("%s lists no PostgreSQL-family types; the check would be vacuous", path)
	}
	return out
}

func repoRoot(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for d := cwd; ; {
		if info, err := os.Stat(filepath.Join(d, "shared", "mcp-connectors")); err == nil && info.IsDir() {
			return d
		}
		parent := filepath.Dir(d)
		if parent == d {
			t.Fatalf("no shared/mcp-connectors above %s", cwd)
		}
		d = parent
	}
}

func debeziumConnectorPy(t *testing.T) string {
	t.Helper()
	root := filepath.Join(repoRoot(t), "shared", "mcp-connectors", "internal", "debezium")
	cv, ok := connectorpaths.ResolveCurrentVersion(root)
	if !ok {
		t.Fatalf("cannot resolve current_version from %s/latest.json", root)
	}
	path := filepath.Join(root, "versions", cv, "connector.py")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !strings.Contains(string(data), "io.debezium.connector.") {
		t.Fatalf("%s names no Debezium connector class; wrong file?", path)
	}
	return string(data)
}
