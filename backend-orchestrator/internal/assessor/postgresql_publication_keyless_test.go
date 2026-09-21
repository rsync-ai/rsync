package assessor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func keylessRows(tables ...string) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"nspname", "relname"})
	for _, t := range tables {
		s, n, _ := strings.Cut(t, ".")
		rows.AddRow(s, n)
	}
	return rows
}

func TestCheckPostgresKeylessTablesOutsidePipeline(t *testing.T) {
	t.Run("warns once per keyless table the pipeline does not copy", func(t *testing.T) {
		db, mock := newHealthMock(t)
		mock.ExpectQuery("relreplident").
			WillReturnRows(keylessRows("app.audit_log", "public.events", "public.orders_raw", "sales.tmp"))

		// public.events is selected bare (default schema), sales.tmp qualified and quoted.
		got := checkPostgresKeylessTablesOutsidePipeline(context.Background(), db,
			map[string]string{}, []string{"events", `"sales"."tmp"`, "public.customers"})

		var objects []string
		for _, c := range got {
			if c.Code != codePostgresUnselectedTableMissingPK || c.Severity != SeverityWarning || c.Passed {
				t.Fatalf("want a failed warning, got %+v", c)
			}
			if c.Remediation == nil || len(c.Remediation.SQLToRun) != 1 || !strings.Contains(c.Remediation.SQLToRun[0], "ADD PRIMARY KEY") {
				t.Fatalf("%s carries no ALTER TABLE: %+v", c.Object, c.Remediation)
			}
			objects = append(objects, c.Object)
		}
		if want := "app.audit_log,public.orders_raw"; strings.Join(objects, ",") != want {
			t.Fatalf("objects = %v, want %s (selected tables get REPLICA IDENTITY FULL and are not at risk)", objects, want)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("a connection schema is the default for bare selections", func(t *testing.T) {
		db, mock := newHealthMock(t)
		mock.ExpectQuery("relreplident").WillReturnRows(keylessRows("crm.leads", "public.leads"))

		got := checkPostgresKeylessTablesOutsidePipeline(context.Background(), db,
			map[string]string{"schema": "crm"}, []string{"leads"})

		if len(got) != 1 || got[0].Object != "public.leads" {
			t.Fatalf("got %+v, want only public.leads", got)
		}
	})

	t.Run("no keyless tables, no checks (control)", func(t *testing.T) {
		db, mock := newHealthMock(t)
		mock.ExpectQuery("relreplident").WillReturnRows(keylessRows())

		if got := checkPostgresKeylessTablesOutsidePipeline(context.Background(), db, map[string]string{}, []string{"public.a"}); len(got) != 0 {
			t.Fatalf("got %d checks, want none", len(got))
		}
	})

	t.Run("past the cap the rest are counted in one check with the listing query", func(t *testing.T) {
		db, mock := newHealthMock(t)
		var names []string
		for i := 0; i < maxKeylessTablesListed+5; i++ {
			names = append(names, fmt.Sprintf("public.t%03d", i))
		}
		mock.ExpectQuery("relreplident").WillReturnRows(keylessRows(names...))

		got := checkPostgresKeylessTablesOutsidePipeline(context.Background(), db, map[string]string{}, nil)

		if len(got) != maxKeylessTablesListed+1 {
			t.Fatalf("got %d checks, want %d per-table + 1 summary", len(got), maxKeylessTablesListed)
		}
		last := got[len(got)-1]
		if last.Object != "" || !strings.Contains(last.Message, "5 more tables") || !strings.Contains(last.Message, "105 in all") {
			t.Fatalf("summary check = %+v", last)
		}
		if last.Remediation == nil || !strings.Contains(last.Remediation.SQLToRun[0], "relreplident") {
			t.Fatal("summary check does not carry the listing query")
		}
	})

	t.Run("a failed query still warns, without blocking", func(t *testing.T) {
		db, mock := newHealthMock(t)
		mock.ExpectQuery("relreplident").WillReturnError(errors.New("permission denied for table pg_class"))

		got := checkPostgresKeylessTablesOutsidePipeline(context.Background(), db, map[string]string{}, nil)

		if len(got) != 1 || got[0].Severity != SeverityWarning || got[0].Passed {
			t.Fatalf("got %+v, want one failed warning", got)
		}
	})
}
