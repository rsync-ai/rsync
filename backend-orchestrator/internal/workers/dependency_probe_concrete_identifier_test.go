package workers

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// #56 prod retest: one dependency panel showed "mongodb@latest" beside
// "gcs@v1.0.0". dependency_manifest.go resolves the version for rows it writes
// now, but rows written before that fix kept "@latest" forever. The probe
// already resolves "@latest" every tick to find the server; these tests lock
// that it also writes the concrete identifier back, once.

type stubResolver struct {
	version string
	err     error
	calls   []string
}

func (s *stubResolver) ResolveConcreteVersion(name, version string) (string, error) {
	s.calls = append(s.calls, name+"@"+version)
	return s.version, s.err
}

func TestConcreteLatestIdentifier(t *testing.T) {
	cases := []struct {
		name, kind, identifier string
		resolver               *stubResolver
		want                   string
	}{
		{"latest source", "mcp_source", "mongodb@latest", &stubResolver{version: "v1.0.0"}, "mongodb@v1.0.0"},
		{"latest destination", "mcp_dest", "gcs@latest", &stubResolver{version: "v1.0.0"}, "gcs@v1.0.0"},
		{"empty version", "mcp_source", "mongodb@", &stubResolver{version: "v1.0.0"}, "mongodb@v1.0.0"},
		{"no version at all", "mcp_source", "mongodb", &stubResolver{version: "v1.0.0"}, "mongodb@v1.0.0"},
		{"already concrete", "mcp_dest", "gcs@v1.0.0", &stubResolver{version: "v2.0.0"}, ""},
		{"not an MCP kind", "debezium_task", "cdc-abd8a64d", &stubResolver{version: "v1.0.0"}, ""},
		{"resolution failed", "mcp_source", "mongodb@latest", &stubResolver{err: errors.New("no latest.json")}, ""},
		{"resolver echoed latest", "mcp_source", "mongodb@latest", &stubResolver{version: "latest"}, ""},
		{"malformed", "mcp_source", "@latest", &stubResolver{version: "v1.0.0"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := concreteLatestIdentifier(tc.resolver, tc.kind, tc.identifier); got != tc.want {
				t.Fatalf("concreteLatestIdentifier(%q, %q) = %q, want %q", tc.kind, tc.identifier, got, tc.want)
			}
		})
	}

	t.Run("asks for the latest version of the named connector", func(t *testing.T) {
		r := &stubResolver{version: "v1.0.0"}
		concreteLatestIdentifier(r, "mcp_source", " mongodb @latest")
		if len(r.calls) != 1 || r.calls[0] != "mongodb@latest" {
			t.Fatalf("resolver calls = %v, want [mongodb@latest]", r.calls)
		}
	})

	t.Run("a nil resolver leaves the row alone", func(t *testing.T) {
		if got := concreteLatestIdentifier(nil, "mcp_source", "mongodb@latest"); got != "" {
			t.Fatalf("got %q, want \"\"", got)
		}
	})
}

const concreteDepID = "8f0c2a4e-5d1b-4a8e-9c3f-2b7d6e1a0f94"

func concreteProbe(t *testing.T) (*DependencyProbe, sqlmock.Sqlmock) {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return &DependencyProbe{db: database}, mock
}

func TestPersistConcreteIdentifier(t *testing.T) {
	ctx := context.Background()

	t.Run("renames the row when no concrete twin exists", func(t *testing.T) {
		p, mock := concreteProbe(t)
		mock.ExpectExec(`UPDATE pipeline_dependencies d SET identifier = \$2\s+WHERE d.id = \$1 AND d.identifier = \$3\s+AND NOT EXISTS`).
			WithArgs(concreteDepID, "mongodb@v1.0.0", "mongodb@latest").
			WillReturnResult(sqlmock.NewResult(0, 1))

		if got := p.persistConcreteIdentifier(ctx, concreteDepID, "mongodb@latest", "mongodb@v1.0.0"); got != identifierRenamed {
			t.Fatalf("got %v, want identifierRenamed", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("drops the @latest row when a later run already wrote the concrete one", func(t *testing.T) {
		p, mock := concreteProbe(t)
		mock.ExpectExec(`UPDATE pipeline_dependencies d SET identifier`).
			WithArgs(concreteDepID, "mongodb@v1.0.0", "mongodb@latest").
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`DELETE FROM pipeline_dependencies d\s+WHERE d.id = \$1 AND d.identifier = \$3\s+AND EXISTS`).
			WithArgs(concreteDepID, "mongodb@v1.0.0", "mongodb@latest").
			WillReturnResult(sqlmock.NewResult(0, 1))

		if got := p.persistConcreteIdentifier(ctx, concreteDepID, "mongodb@latest", "mongodb@v1.0.0"); got != identifierMergedIntoTwin {
			t.Fatalf("got %v, want identifierMergedIntoTwin", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("leaves the row when another writer changed it first", func(t *testing.T) {
		p, mock := concreteProbe(t)
		mock.ExpectExec(`UPDATE pipeline_dependencies d SET identifier`).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`DELETE FROM pipeline_dependencies d`).
			WillReturnResult(sqlmock.NewResult(0, 0))

		if got := p.persistConcreteIdentifier(ctx, concreteDepID, "mongodb@latest", "mongodb@v1.0.0"); got != identifierUnchanged {
			t.Fatalf("got %v, want identifierUnchanged", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("a failed rename reports the row unchanged", func(t *testing.T) {
		p, mock := concreteProbe(t)
		mock.ExpectExec(`UPDATE pipeline_dependencies d SET identifier`).
			WillReturnError(sql.ErrConnDone)

		if got := p.persistConcreteIdentifier(ctx, concreteDepID, "mongodb@latest", "mongodb@v1.0.0"); got != identifierUnchanged {
			t.Fatalf("got %v, want identifierUnchanged", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}
