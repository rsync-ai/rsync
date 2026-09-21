package executor

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestServerLevelSource(t *testing.T) {
	cases := []struct {
		name string
		typ  string
		cfg  map[string]string
		want bool
	}{
		{"mysql naming no database", "mysql", map[string]string{"host": "h"}, true},
		{"mysql naming a database", "mysql", map[string]string{"database": "shop"}, false},
		{"mongodb db_name", "mongodb", map[string]string{"db_name": "crm"}, false},
		{"mongodb db", "mongodb", map[string]string{"db": "crm"}, false},
		{"blank database is none", "mongodb", map[string]string{"database": "  "}, true},
		{"clickhouse naming no database", "clickhouse", map[string]string{}, true},
		// PostgreSQL tables live in schemas: a connection is always one database.
		{"postgresql is never server-level", "postgresql", map[string]string{}, false},
		{"object storage is never server-level", "gcs", map[string]string{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := serverLevelSource(tc.typ, tc.cfg); got != tc.want {
				t.Fatalf("serverLevelSource(%q, %v) = %v, want %v", tc.typ, tc.cfg, got, tc.want)
			}
		})
	}
}

func TestMirrorSourceNamespaces(t *testing.T) {
	server := map[string]string{"host": "h"}
	named := map[string]string{"database": "shop"}
	cases := []struct {
		name     string
		srcType  string
		srcCfg   map[string]string
		destType string
		ns       string
		mode     string
		want     bool
	}{
		{"server-level, no namespace", "mysql", server, "mysql", "", "", true},
		{"server-level, placeholder", "mysql", server, "mysql", "default", "", true},
		// What pipeline creation seeds (api-gateway seedDestinationNamespace) is not a
		// choice the user made; it must not collapse every source database into one.
		{"server-level, seeded public for a postgres destination", "mysql", server, "postgresql", "public", "", true},
		{"server-level, seeded connector name for a mongodb source", "mongodb", server, "mongodb", "mongodb", "", true},
		{"seeded name matches case-insensitively", "mongodb", server, "mongodb", "MongoDB", "", true},
		{"server-level, typed namespace sends everything there", "mysql", server, "postgresql", "analytics", "", false},
		{"named database is not mirrored", "mysql", named, "mysql", "", "", false},
		{"postgresql source is not server-level", "postgresql", map[string]string{}, "mysql", "", "", false},
		// The picker's explicit choice decides first: a typed engine-default name
		// ("public") is stored with flatten, and a blanked one with preserve.
		{"flatten override beats a seeded-looking name", "mysql", server, "postgresql", "public", "flatten", false},
		{"preserve override beats a typed name", "mysql", server, "postgresql", "analytics", "preserve", true},
		{"override cannot mirror a named database", "mysql", named, "mysql", "", "preserve", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := MirrorSourceNamespaces(tc.srcType, tc.srcCfg, tc.destType, tc.ns, tc.mode); got != tc.want {
				t.Fatalf("MirrorSourceNamespaces = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMirrorSourceNamespacesForNilConnectors(t *testing.T) {
	if (&Agent{}).mirrorSourceNamespacesFor(context.Background(), ExecutorTask{}, "") {
		t.Fatal("a task without source/destination must not mirror")
	}
}

func TestPreserveSourceSchemaLayoutServerLevelSource(t *testing.T) {
	// The batch run and the CDC sink must agree: a server-level source mirrors even a
	// single-database selection, which the multi-schema heuristic alone would flatten.
	a := &Agent{}
	ctx := context.Background()
	single := []interface{}{"shop.users", "shop.orders"}
	serverTask := ExecutorTask{PipelineID: "p1",
		Source:      &ConnectorConfig{Type: "mysql", Config: map[string]string{"host": "h"}},
		Destination: &ConnectorConfig{Type: "postgresql"}}
	if !a.preserveSourceSchemaLayout(ctx, serverTask, single, "") {
		t.Error("server-level source, no namespace: must PRESERVE")
	}
	if !a.preserveSourceSchemaLayout(ctx, serverTask, single, "public") {
		t.Error(`server-level source, seeded "public": must PRESERVE`)
	}
	if a.preserveSourceSchemaLayout(ctx, serverTask, single, "analytics") {
		t.Error("server-level source, typed namespace: must FLATTEN into it")
	}
	namedTask := serverTask
	namedTask.Source = &ConnectorConfig{Type: "mysql", Config: map[string]string{"database": "shop"}}
	if a.preserveSourceSchemaLayout(ctx, namedTask, single, "") {
		t.Error("named-database source, single schema: unchanged (FLATTEN)")
	}
}

func TestSchemaModeOverride(t *testing.T) {
	if got := SchemaModeOverride(context.Background(), nil, "p1"); got != "" {
		t.Fatalf("no db: got %q, want \"\"", got)
	}
	for stored, want := range map[string]string{"preserve": "preserve", "mirror": "preserve", "flatten": "flatten", "sideways": ""} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		mock.ExpectQuery("destination_schema_mode").WithArgs("p1").
			WillReturnRows(sqlmock.NewRows([]string{"mode"}).AddRow(stored))
		if got := SchemaModeOverride(context.Background(), db, "p1"); got != want {
			t.Errorf("stored %q: got %q, want %q", stored, got, want)
		}
		db.Close()
	}
}

func TestBatchAndCDCAgreeOnStoredSchemaMode(t *testing.T) {
	// The picker stores schema_mode "flatten" when the user types a name — even an
	// engine default like "public", which deliberateNamespace alone reads as seeded.
	// The batch layout and the CDC sink flag must both follow the stored choice.
	task := ExecutorTask{PipelineID: "p1",
		Source:      &ConnectorConfig{Type: "mysql", Config: map[string]string{"host": "h"}},
		Destination: &ConnectorConfig{Type: "postgresql"}}
	for stored, want := range map[string]bool{"flatten": false, "preserve": true} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		rows := func() *sqlmock.Rows { return sqlmock.NewRows([]string{"mode"}).AddRow(stored) }
		mock.ExpectQuery("destination_schema_mode").WithArgs("p1").WillReturnRows(rows())
		mock.ExpectQuery("destination_schema_mode").WithArgs("p1").WillReturnRows(rows())
		a := &Agent{db: db}
		batch := a.preserveSourceSchemaLayout(context.Background(), task, []interface{}{"shop.users"}, "public")
		cdc := a.mirrorSourceNamespacesFor(context.Background(), task, "public")
		if batch != want || cdc != want {
			t.Errorf("stored %q: batch preserve=%v, cdc mirror=%v, want both %v", stored, batch, cdc, want)
		}
		db.Close()
	}
}
