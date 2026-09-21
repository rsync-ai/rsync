// Keyless tables outside the pipeline (PostgreSQL CDC).
//
// rsync creates each pipeline's publication FOR ALL TABLES (cdc/postgresql.go
// ProvisionResources), and a table in a publication that publishes UPDATE and
// DELETE must have a replica identity. So once the pipeline starts, every
// table in the database without a primary key — including tables the pipeline
// does not copy — rejects the user's own UPDATE and DELETE with "cannot update
// table … because it does not have a replica identity and publishes updates".
// Selected tables are safe: provisioning sets REPLICA IDENTITY FULL on them.
//
// rsync does not work around this (user decision 2026-09-18): the assessment
// lists those tables with the ALTER TABLE to run, as a warning, and the user
// adds the keys. The scheduled recheck of running CDC pipelines reports a new
// keyless table the same way.
package assessor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

const (
	codePostgresUnselectedTableMissingPK = "POSTGRES_UNSELECTED_TABLE_MISSING_PRIMARY_KEY"

	// maxKeylessTablesListed caps the per-table checks; the rest are counted
	// in one extra check that carries the query listing them all.
	maxKeylessTablesListed = 100
)

// keylessPublishableTablesQuery lists the tables a FOR ALL TABLES publication
// covers (ordinary permanent user tables, as is_publishable_class decides)
// whose UPDATE and DELETE fail once they are published: replica identity
// NOTHING, DEFAULT without a primary key, or USING INDEX whose index is gone.
const keylessPublishableTablesQuery = `
SELECT n.nspname, c.relname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND c.relpersistence = 'p'
  AND c.oid >= 16384
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND (
        c.relreplident = 'n'
     OR (c.relreplident = 'd' AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary))
     OR (c.relreplident = 'i' AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisreplident))
  )
ORDER BY n.nspname, c.relname`

// checkPostgresKeylessTablesOutsidePipeline returns one warning per keyless
// table the pipeline does not select, or nothing when there are none.
func checkPostgresKeylessTablesOutsidePipeline(ctx context.Context, db *sql.DB, cfg map[string]string, selected []string) []Check {
	defaultSchema := strings.TrimSpace(cfg["schema"])
	if defaultSchema == "" {
		defaultSchema = "public"
	}
	inPipeline := make(map[string]bool, len(selected))
	for _, raw := range selected {
		t := strings.TrimSpace(raw)
		if t == "" {
			continue
		}
		schemaName, tableName := defaultSchema, strings.Trim(t, `"`)
		if idx := strings.IndexByte(t, '.'); idx > 0 {
			schemaName = strings.Trim(t[:idx], `"`)
			tableName = strings.Trim(t[idx+1:], `"`)
		}
		inPipeline[schemaName+"."+tableName] = true
	}

	rows, err := db.QueryContext(ctx, keylessPublishableTablesQuery)
	if err != nil {
		return []Check{{
			Code: codePostgresUnselectedTableMissingPK, Severity: SeverityWarning, Passed: false,
			Message: fmt.Sprintf("Could not list the tables without a primary key: %v. Once this pipeline starts, UPDATE and DELETE fail on any table in this database that has no primary key.", err),
		}}
	}
	defer rows.Close()
	type table struct{ schema, name string }
	var keyless []table
	for rows.Next() {
		var t table
		if err := rows.Scan(&t.schema, &t.name); err != nil {
			continue
		}
		if !inPipeline[t.schema+"."+t.name] {
			keyless = append(keyless, t)
		}
	}

	out := make([]Check, 0, len(keyless))
	for i, t := range keyless {
		if i == maxKeylessTablesListed {
			out = append(out, Check{
				Code: codePostgresUnselectedTableMissingPK, Severity: SeverityWarning, Passed: false,
				Message: fmt.Sprintf("%d more tables outside this pipeline have no primary key (%d in all). Once this pipeline starts, UPDATE and DELETE on them fail in your application until each has a primary key. Run the query below to list them all.", len(keyless)-maxKeylessTablesListed, len(keyless)),
				Remediation: &diagnose.Remediation{
					Steps:            []string{"List every table without a primary key", "Add a primary key to each one", "Re-run this assessment"},
					SQLToRun:         []string{strings.TrimSpace(keylessPublishableTablesQuery) + ";"},
					DocURL:           diagnose.ErrorDocURL("postgres-keyless-tables-outside-pipeline"),
					EstimatedMinutes: 30,
				},
			})
			break
		}
		name := t.schema + "." + t.name
		out = append(out, withObject(Check{
			Code: codePostgresUnselectedTableMissingPK, Severity: SeverityWarning, Passed: false,
			Message: fmt.Sprintf("%s is not in this pipeline and has no primary key. rsync's publication covers every table in the database, so once this pipeline starts, UPDATE and DELETE on %s fail in your application until it has a primary key.", name, name),
			Remediation: &diagnose.Remediation{
				Steps: []string{
					"Add a primary key to this table (replace id with the column(s) that identify a row)",
					"Re-run this assessment",
				},
				SQLToRun:         []string{fmt.Sprintf("ALTER TABLE %q.%q ADD PRIMARY KEY (id);", t.schema, t.name)},
				DocURL:           diagnose.ErrorDocURL("postgres-keyless-tables-outside-pipeline"),
				EstimatedMinutes: 5,
			},
		}, name))
	}
	return out
}
