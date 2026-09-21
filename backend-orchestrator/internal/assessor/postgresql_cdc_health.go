// PostgreSQL CDC health checks for the pre-migration Assessment tab.
//
// These run only for CDC pipelines, next to the wal_level / slot-count /
// REPLICATION checks in postgresql.go. One of them blocks (no free replication
// slot — slot creation is certain to fail); the rest are advisories: an info
// check that does not pass is shown on the Assessment tab with a fix, but it
// never gates a run (Summarize counts it as a warning, not a failure).
//
// Every query is read-only.
package assessor

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/rsync-ai/backend-orchestrator/internal/cdc"
	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// postgresCDCHealthChecks returns the CDC health checks that apply to this
// server. A setting the server does not have (older PostgreSQL) yields no check
// rather than a failure.
func postgresCDCHealthChecks(ctx context.Context, db *sql.DB, pipelineID string) []Check {
	out := []Check{checkPostgresSlotCapacity(ctx, db, pipelineID)}
	if c, ok := checkPostgresMaxSlotWALKeepSize(ctx, db); ok {
		out = append(out, c)
	}
	if c, ok := checkPostgresWALSenderTimeout(ctx, db); ok {
		out = append(out, c)
	}
	if c, ok := checkPostgresLogicalDecodingWorkMem(ctx, db); ok {
		out = append(out, c)
	}
	if c, ok := checkPostgresPublicationPrivilege(ctx, db, pipelineID); ok {
		out = append(out, c)
	}
	return out
}

// pgSetting reads one server setting from pg_settings. ok=false when the
// server has no such setting (it was added in a later PostgreSQL version).
func pgSetting(ctx context.Context, db *sql.DB, name string) (value string, ok bool, err error) {
	err = db.QueryRowContext(ctx, `SELECT setting FROM pg_settings WHERE name = $1`, name).Scan(&value)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

// checkPostgresSlotCapacity blocks when every replication slot is taken and
// this pipeline does not already own one: provisioning would then fail at
// pg_create_logical_replication_slot with "all replication slots are in use".
func checkPostgresSlotCapacity(ctx context.Context, db *sql.DB, pipelineID string) Check {
	const code = "POSTGRES_REPLICATION_SLOTS_EXHAUSTED"
	prefix := ""
	if strings.TrimSpace(pipelineID) != "" {
		prefix = cdc.PipelineResourcePrefix(pipelineID, "replication_slot")
	}
	var maxSlots, used, own int
	err := db.QueryRowContext(ctx, `
		SELECT current_setting('max_replication_slots')::int,
		       (SELECT count(*) FROM pg_replication_slots),
		       (SELECT count(*) FROM pg_replication_slots
		         WHERE $1::text <> '' AND left(slot_name, length($1::text)) = $1::text)`,
		prefix,
	).Scan(&maxSlots, &used, &own)
	if err != nil {
		return Check{
			Code: code, Severity: SeverityWarning, Passed: false,
			Message: fmt.Sprintf("Could not count replication slots: %v", err),
		}
	}
	if own > 0 {
		return Check{
			Code: code, Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("This pipeline already has its replication slot (%d of %d slots in use)", used, maxSlots),
		}
	}
	if used < maxSlots {
		return Check{
			Code: code, Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("%d of %d replication slots in use; one is free for this pipeline", used, maxSlots),
		}
	}
	return Check{
		Code: code, Severity: SeverityError, Passed: false,
		Message: fmt.Sprintf("All %d replication slots are in use, so this pipeline cannot create its slot. Free an unused slot or raise max_replication_slots.", maxSlots),
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"List the slots and find ones no longer used (active = false, and no pipeline or tool still needs them)",
				"Drop an unused slot, or raise max_replication_slots and restart PostgreSQL",
				"Re-run this assessment",
			},
			SQLToRun: []string{
				"SELECT slot_name, plugin, active, restart_lsn FROM pg_replication_slots ORDER BY active, slot_name;",
				"-- Only for a slot nothing needs any more (dropping it discards its unread changes):",
				"SELECT pg_drop_replication_slot('<slot_name>');",
			},
			DocURL:           diagnose.ErrorDocURL("postgres-max-replication-slots"),
			EstimatedMinutes: 10,
		},
	}
}

// checkPostgresMaxSlotWALKeepSize flags an unlimited max_slot_wal_keep_size
// (PostgreSQL 13+). Neither choice is free, so this is advisory: unlimited
// means a stalled pipeline's slot keeps WAL until the source disk fills; a
// limit protects the disk but invalidates a slot that falls further behind,
// and the pipeline must then re-snapshot.
func checkPostgresMaxSlotWALKeepSize(ctx context.Context, db *sql.DB) (Check, bool) {
	const code = "POSTGRES_MAX_SLOT_WAL_KEEP_SIZE_UNLIMITED"
	v, ok, err := pgSetting(ctx, db, "max_slot_wal_keep_size")
	if err != nil || !ok {
		return Check{}, false
	}
	if strings.TrimSpace(v) != "-1" {
		return Check{
			Code: code, Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("max_slot_wal_keep_size = %s MB, so a stalled slot cannot fill the source disk", v),
		}, true
	}
	return Check{
		Code: code, Severity: SeverityInfo, Passed: false,
		Message: "max_slot_wal_keep_size is unlimited (-1). If this pipeline stops reading, its replication slot keeps WAL without limit and can fill the source disk. " +
			"A limit protects the disk, but a slot that falls further behind than the limit is invalidated and the pipeline must re-snapshot — pick a size that covers your longest expected outage.",
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"Choose a limit that covers the longest pause you expect (WAL written per hour × hours)",
				"Self-managed PostgreSQL: run the SQL below (no restart needed)",
				"Cloud SQL / RDS / Azure: set the max_slot_wal_keep_size flag or parameter instead",
			},
			SQLToRun: []string{
				"ALTER SYSTEM SET max_slot_wal_keep_size = '50GB';",
				"SELECT pg_reload_conf();",
			},
			DocURL:           diagnose.ErrorDocURL("postgres-max-slot-wal-keep-size"),
			EstimatedMinutes: 10,
		},
	}, true
}

// checkPostgresWALSenderTimeout flags a wal_sender_timeout under 10s: the
// server then drops the CDC connection during a slow write or a long snapshot
// and the stream restarts. 0 disables the timeout and is fine.
func checkPostgresWALSenderTimeout(ctx context.Context, db *sql.DB) (Check, bool) {
	const code = "POSTGRES_WAL_SENDER_TIMEOUT_LOW"
	const minMillis = 10000
	v, ok, err := pgSetting(ctx, db, "wal_sender_timeout")
	if err != nil || !ok {
		return Check{}, false
	}
	ms, perr := strconv.Atoi(strings.TrimSpace(v))
	if perr != nil {
		return Check{}, false
	}
	if ms == 0 || ms >= minMillis {
		return Check{
			Code: code, Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("wal_sender_timeout = %d ms", ms),
		}, true
	}
	return Check{
		Code: code, Severity: SeverityInfo, Passed: false,
		Message: fmt.Sprintf("wal_sender_timeout is %d ms. Under 10 s the server can drop the CDC connection during a slow write or a long snapshot, which restarts the stream.", ms),
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"Self-managed PostgreSQL: run the SQL below (no restart needed)",
				"Cloud SQL / RDS / Azure: set the wal_sender_timeout flag or parameter instead",
			},
			SQLToRun: []string{
				"ALTER SYSTEM SET wal_sender_timeout = '60s';",
				"SELECT pg_reload_conf();",
			},
			DocURL:           diagnose.ErrorDocURL("postgres-wal-sender-timeout"),
			EstimatedMinutes: 5,
		},
	}, true
}

// checkPostgresLogicalDecodingWorkMem flags a logical_decoding_work_mem below
// the 64MB default (PostgreSQL 13+): large transactions then spill to disk on
// the source while being decoded, which slows CDC.
func checkPostgresLogicalDecodingWorkMem(ctx context.Context, db *sql.DB) (Check, bool) {
	const code = "POSTGRES_LOGICAL_DECODING_WORK_MEM_LOW"
	const minKB = 65536
	v, ok, err := pgSetting(ctx, db, "logical_decoding_work_mem")
	if err != nil || !ok {
		return Check{}, false
	}
	kb, perr := strconv.Atoi(strings.TrimSpace(v))
	if perr != nil {
		return Check{}, false
	}
	if kb >= minKB {
		return Check{
			Code: code, Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("logical_decoding_work_mem = %d kB", kb),
		}, true
	}
	return Check{
		Code: code, Severity: SeverityInfo, Passed: false,
		Message: fmt.Sprintf("logical_decoding_work_mem is %d kB, below the 64 MB default. Large transactions spill to disk on the source while being decoded, which slows CDC.", kb),
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"Self-managed PostgreSQL: run the SQL below (no restart needed)",
				"Cloud SQL / RDS / Azure: set the logical_decoding_work_mem flag or parameter instead",
			},
			SQLToRun: []string{
				"ALTER SYSTEM SET logical_decoding_work_mem = '64MB';",
				"SELECT pg_reload_conf();",
			},
			DocURL:           diagnose.ErrorDocURL("postgres-logical-decoding-work-mem"),
			EstimatedMinutes: 5,
		},
	}, true
}

// managedPostgresAdminRoles are the platform roles that may create a
// publication FOR ALL TABLES without being a true superuser.
var managedPostgresAdminRoles = []string{"rds_superuser", "cloudsqlsuperuser", "azure_pg_admin"}

// checkPostgresPublicationPrivilege warns when the connection user looks unable
// to create this pipeline's publication (CREATE PUBLICATION … FOR ALL TABLES
// needs a superuser or a platform admin role). Advisory only, never blocking:
// platforms grant this in ways the catalog does not always show, and a false
// block would stop a pipeline that works. It passes when the pipeline's
// publication already exists.
func checkPostgresPublicationPrivilege(ctx context.Context, db *sql.DB, pipelineID string) (Check, bool) {
	const code = "POSTGRES_PUBLICATION_PRIVILEGE"
	if strings.TrimSpace(pipelineID) != "" {
		prefix := cdc.PipelineResourcePrefix(pipelineID, "publication")
		var n int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM pg_publication WHERE left(pubname, length($1::text)) = $1::text`, prefix,
		).Scan(&n); err == nil && n > 0 {
			return Check{
				Code: code, Severity: SeverityInfo, Passed: true,
				Message: "This pipeline's publication already exists",
			}, true
		}
	}
	var user string
	var super, platformAdmin bool
	err := db.QueryRowContext(ctx, `
		SELECT current_user::text,
		       COALESCE((SELECT rolsuper FROM pg_roles WHERE rolname = current_user), false),
		       EXISTS (SELECT 1 FROM pg_roles r
		                WHERE r.rolname IN ('`+strings.Join(managedPostgresAdminRoles, "','")+`')
		                  AND pg_has_role(current_user, r.oid, 'MEMBER'))`,
	).Scan(&user, &super, &platformAdmin)
	if err != nil {
		return Check{}, false
	}
	if super || platformAdmin {
		return Check{
			Code: code, Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("User %q can create the publication rsync needs", user),
		}, true
	}
	return Check{
		Code: code, Severity: SeverityInfo, Passed: false,
		Message: fmt.Sprintf("User %q is not a superuser or a platform admin role member. rsync creates this pipeline's publication with CREATE PUBLICATION … FOR ALL TABLES, which may be refused. If the pipeline fails to start, grant the role below.", user),
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"Grant the connection user your platform's admin role (Cloud SQL: cloudsqlsuperuser, RDS: rds_superuser, Azure: azure_pg_admin), or make it a superuser on self-managed PostgreSQL",
				"Re-run this assessment",
			},
			SQLToRun: []string{
				fmt.Sprintf("GRANT cloudsqlsuperuser TO %q;  -- Cloud SQL", user),
				fmt.Sprintf("GRANT rds_superuser TO %q;  -- Amazon RDS / Aurora", user),
				fmt.Sprintf("ALTER USER %q WITH SUPERUSER;  -- self-managed", user),
			},
			DocURL:           diagnose.ErrorDocURL("postgres-publication-privilege"),
			EstimatedMinutes: 5,
		},
	}, true
}
