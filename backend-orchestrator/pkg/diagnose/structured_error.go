// Package diagnose — structured error envelope.
//
// StructuredError is the canonical user-facing failure shape. Every place
// that writes an error string to pipeline_run_events, pipeline_notifications,
// or surfaces an error to the API/UI should produce a StructuredError so the
// user sees a consistent {failure_type, code, user_message, remediation}
// envelope instead of a raw exception.
//
// Pattern stolen from Airbyte's AirbyteErrorTraceMessage:
//   - failure_type is a small 3-value enum so users can filter "is this on me
//     or on rsync?" at a glance.
//   - user_message contains the fix, not just the symptom (Fivetran pattern).
//   - internal_message preserves the raw exception for support tickets.
//   - remediation carries copy-pasteable commands + a doc URL for the fix.
package diagnose

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// FailureType — small, blunt taxonomy. Don't add a 4th value without a
// strong UX justification; users won't read more than 3.
type FailureType string

const (
	// FailureTypeConfigError — user (or their DBA) must change something
	// on their side. Examples: MySQL binlog_format=STATEMENT, missing PK,
	// expired API key, missing OAuth scope.
	FailureTypeConfigError FailureType = "config_error"

	// FailureTypeSystemError — rsync itself has a bug. Examples: connector
	// silent drop, ownership row missing, healer poison-pill loop. Routes
	// to developer audience, surfaces to user as "we're working on it".
	FailureTypeSystemError FailureType = "system_error"

	// FailureTypeTransientError — expected to recover with automated retry.
	// Examples: network blip, HTTP 429, slot temporarily locked. Should
	// rarely escape to the user unless retry budget exhausts.
	FailureTypeTransientError FailureType = "transient_error"
)

// Audience selects the notification routing. UI banner shows everything;
// email/Slack routing differs by audience.
type Audience string

const (
	AudienceUser      Audience = "user"      // end user / DBA
	AudienceOperator  Audience = "operator"  // ops on-call
	AudienceDeveloper Audience = "developer" // rsync engineer (paged via internal channel)
)

// Severity for UI rendering + alert routing thresholds.
type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
	SeverityInfo    Severity = "info"
)

// Remediation carries the actionable fix. All fields optional — populate
// what makes sense per error code; the UI renders only what's present.
type Remediation struct {
	// Steps are ordered human-readable actions ("1. SET GLOBAL binlog_format=ROW",
	// "2. Restart MySQL", "3. Re-run pre-flight assessment").
	Steps []string `json:"steps,omitempty"`

	// SQLToRun is copy-pasteable SQL the user can execute directly. Kept
	// separate from Steps so the UI can render a code-block with a copy
	// button.
	SQLToRun []string `json:"sql_to_run,omitempty"`

	// CommandsToRun is copy-pasteable non-SQL commands (mongosh, sqlplus,
	// shell) for engines whose remediation isn't portable SQL — e.g. MongoDB
	// rs.initiate(), Oracle "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA". Kept
	// separate from SQLToRun so the UI labels the code-block correctly.
	CommandsToRun []string `json:"commands_to_run,omitempty"`

	// DocURL points to the long-form runbook for this error code. Never
	// hand-write one: build it with ErrorDocURL so a single base URL (and the
	// RSYNC_DOCS_BASE_URL override) governs every payload we emit.
	// Convention: ErrorDocURL("{CODE_LOWER_KEBAB}").
	DocURL string `json:"doc_url,omitempty"`

	// EstimatedMinutes is the wall-clock fix time (best guess). Helps the
	// user decide whether to act now or schedule.
	EstimatedMinutes int `json:"estimated_minutes,omitempty"`
}

// DocsBaseURL is the root of the published documentation tree that every
// Remediation.DocURL is built from. It is the ONE place the docs host is
// named — no call site may hard-code a URL.
//
// It points at the in-repo error reference rather than a docs site because a
// docs site is a second thing to keep alive: the previous value,
// docs.rsync-ai.dev, went to NXDOMAIN while the URLs it produced were still
// being compiled into runtime error payloads (Remediation.DocURL is a JSON
// field returned to users, not just a doc link). A path inside the repo that
// serves the docs cannot rot independently of the docs.
const DocsBaseURL = "https://github.com/rsync-ai/rsync/blob/main/docs"

// DocsBaseURLEnv overrides DocsBaseURL at runtime so an operator running a
// self-hosted mirror can point remediation at their own copy. A trailing
// slash is tolerated.
const DocsBaseURLEnv = "RSYNC_DOCS_BASE_URL"

// docsBaseURL resolves the effective base: the env override if set and
// non-blank, otherwise DocsBaseURL.
func docsBaseURL() string {
	if v := strings.TrimSpace(os.Getenv(DocsBaseURLEnv)); v != "" {
		return strings.TrimRight(v, "/")
	}
	return DocsBaseURL
}

// ErrorDocURL builds the documentation URL for one error slug.
//
// slug is the lower-kebab form of the error Code and MUST match a "## <slug>"
// heading in docs/errors/README.md — the heading is what makes the fragment a
// working anchor. This is an anchor into a single page, not a per-code path:
// GitHub renders docs/errors/README.md, and "#postgres-wal-level" jumps to the
// section. A path-shaped URL (.../errors/postgres-wal-level) would 404.
func ErrorDocURL(slug string) string {
	return docsBaseURL() + "/errors/README.md#" + slug
}

// StructuredError is the canonical user-facing error envelope.
//
// This struct is serialized into:
//   - pipeline_run_events.payload (as JSON column "error")
//   - pipeline_notifications.payload (when failure escalates to a notification)
//   - the GET /v1/pipelines/{id}/status API response
//
// Once persisted, the Code field is the stable identifier — never reuse a
// code for a different meaning. If the meaning changes, mint a new code.
type StructuredError struct {
	// FailureType — required, drives UI grouping + notification routing.
	FailureType FailureType `json:"failure_type"`

	// Code — stable identifier, SCREAMING_SNAKE_CASE. Stable across
	// rsync versions. Examples:
	//   "MYSQL_BINLOG_FORMAT_NOT_ROW"
	//   "POSTGRES_PUBLICATION_DOES_NOT_EXIST"
	//   "POSTGRES_REPLICATION_SLOT_CONFLICT"
	//   "CDC_TABLE_MISSING_PRIMARY_KEY"
	//   "AUTH_TOKEN_EXPIRED"
	//   "AUTH_SCOPE_INSUFFICIENT"
	//   "RATE_LIMIT_EXCEEDED"
	//   "RSYNC_BUG_SILENT_DROP"
	//   "RSYNC_BUG_OWNERSHIP_ROW_MISSING"
	//   "NETWORK_TRANSIENT_FAILURE"
	//   "UNKNOWN_ERROR"
	Code string `json:"code"`

	// Severity — drives banner color + whether to page.
	Severity Severity `json:"severity"`

	// Audience — who is responsible for resolving this.
	Audience Audience `json:"audience"`

	// UserMessage — human-friendly, includes the FIX, not just the symptom.
	// Example: "Your MySQL needs binlog_format=ROW for CDC. Run
	// `SET GLOBAL binlog_format='ROW'` and restart MySQL."
	// NOT: "Failed to start Debezium connector"
	UserMessage string `json:"user_message"`

	// InternalMessage — raw exception / log line. Preserved for support
	// tickets and bug reports. Never shown to the user by default.
	InternalMessage string `json:"internal_message,omitempty"`

	// Remediation — optional structured fix actions.
	Remediation *Remediation `json:"remediation,omitempty"`

	// SourceDBType / DestinationType — populated when relevant so the UI
	// can render DB-specific iconography + so notifications can route by
	// DB type (different on-call rotations for PG vs MySQL specialists).
	SourceDBType    string `json:"source_db_type,omitempty"`
	DestinationType string `json:"destination_type,omitempty"`

	// Category — back-reference to the diagnose.Category that produced
	// this error. Useful for analytics ("how many config_errors per week").
	Category Category `json:"category,omitempty"`

	// PipelineID / ExecutionID — set when the error originated from a
	// specific pipeline run. Required for cross-referencing in events DB.
	PipelineID  string `json:"pipeline_id,omitempty"`
	ExecutionID string `json:"execution_id,omitempty"`

	// OccurredAt — ISO-8601 UTC. Set by NewStructuredError; callers
	// shouldn't need to populate this manually.
	OccurredAt string `json:"occurred_at"`

	// DedupSubject — optional, machine-built identity of the SPECIFIC thing this
	// error is about, within its Code. The notifier's dedup key is
	// (pipeline_id, code, action_url, dedup_subject), and the first three are
	// constant across a whole code family: every schema drift on a pipeline is
	// SCHEMA_DRIFT_DETECTED pointing at the same /schema-changes link, so without a
	// subject the first drift in the hour-long window notified and every later one —
	// a different table, a different column — was silently swallowed.
	//
	// Rules for producers:
	//   - Build it from STRUCTURED fields (change_type, table, column), never from
	//     rendered copy. LLM-authored UserMessage text varies between retries of the
	//     SAME event, which would defeat dedup entirely instead of narrowing it.
	//   - Leave it empty when an event genuinely has no sub-identity (a pipeline
	//     failed once; there is only one of it). Empty = the pre-existing behavior.
	//   - Never show it to a user. It is a key, not copy.
	DedupSubject string `json:"dedup_subject,omitempty"`
}

// NewStructuredError stamps OccurredAt and returns a pointer for
// composition convenience.
func NewStructuredError(code string, failureType FailureType, audience Audience, userMessage string) *StructuredError {
	return &StructuredError{
		FailureType: failureType,
		Code:        code,
		Severity:    SeverityError,
		Audience:    audience,
		UserMessage: userMessage,
		OccurredAt:  time.Now().UTC().Format(time.RFC3339),
	}
}

// FromDiagnosis builds a StructuredError from a Diagnosis + Signal.
//
// This is the translator that converts the diagnoser's internal
// classification (Category + Action) into the user-facing envelope.
// Edit this function when adding a new diagnose.Category — there must
// be a corresponding StructuredError code + user message.
func FromDiagnosis(d Diagnosis, s Signal) *StructuredError {
	se := &StructuredError{
		Category:        d.Category,
		InternalMessage: s.ErrorMessage,
		PipelineID:      s.PipelineID,
		ExecutionID:     s.ExecutionID,
		SourceDBType:    s.SourceType,
		DestinationType: s.DestinationType,
		Severity:        SeverityError,
		OccurredAt:      time.Now().UTC().Format(time.RFC3339),
	}

	switch d.Category {
	case CategoryAuthExpired:
		se.FailureType = FailureTypeConfigError
		se.Code = "AUTH_TOKEN_EXPIRED"
		se.Audience = AudienceUser
		se.UserMessage = "The credentials for this connection have expired. Re-authenticate to resume the pipeline."
		se.Remediation = &Remediation{
			Steps: []string{
				"Open the connection settings for this pipeline",
				"Click 'Re-authenticate' to refresh credentials",
				"Re-run the pipeline once the connection is green",
			},
			DocURL:           ErrorDocURL("auth-token-expired"),
			EstimatedMinutes: 2,
		}

	case CategoryAuthScope:
		se.FailureType = FailureTypeConfigError
		se.Code = "AUTH_SCOPE_INSUFFICIENT"
		se.Audience = AudienceUser
		se.UserMessage = "The connected credentials lack permission for one or more resources. Grant the required scopes and re-authenticate."
		se.Remediation = &Remediation{
			Steps: []string{
				"Review the missing scope in the error details",
				"In the source app (e.g. Shopify Admin, Stripe Dashboard), grant the required permission to the API key / OAuth app",
				"Click 'Re-authenticate' in the connection settings",
			},
			DocURL:           ErrorDocURL("auth-scope-insufficient"),
			EstimatedMinutes: 5,
		}

	case CategoryRateLimit:
		se.FailureType = FailureTypeTransientError
		se.Code = "RATE_LIMIT_EXCEEDED"
		se.Audience = AudienceUser
		se.Severity = SeverityWarning
		se.UserMessage = "The source API is rate-limiting this pipeline. rsync is automatically backing off and will retry."
		// No remediation needed — auto-handled. Surface only if retries exhaust.

	case CategorySchemaDrift:
		se.FailureType = FailureTypeConfigError
		se.Code = "SCHEMA_DRIFT_DETECTED"
		se.Audience = AudienceUser
		se.UserMessage = "The source schema has changed (column added/removed/retyped) and the destination is out of sync."
		se.Remediation = &Remediation{
			Steps: []string{
				"Open the pipeline's Schema tab to see the detected drift",
				"Approve the proposed schema migration, OR",
				"Run the pipeline in 'reload' mode to recreate destination tables",
			},
			DocURL:           ErrorDocURL("schema-drift"),
			EstimatedMinutes: 10,
		}

	case CategoryNetwork:
		se.FailureType = FailureTypeTransientError
		se.Code = "NETWORK_TRANSIENT_FAILURE"
		se.Audience = AudienceUser
		se.Severity = SeverityWarning
		se.UserMessage = "Transient network error talking to the source or destination. rsync will retry automatically."

	case CategoryConnectorBug:
		se.FailureType = FailureTypeSystemError
		se.Code = "RSYNC_BUG_SILENT_DROP"
		se.Audience = AudienceDeveloper
		se.UserMessage = "rsync detected a connector behaved incorrectly (read rows from source but wrote none to destination). Our team has been alerted."
		se.Remediation = &Remediation{
			Steps: []string{
				"No action required from you",
				"rsync engineering will investigate the connector logs",
				"You will be notified when a fix is deployed",
			},
			DocURL: ErrorDocURL("rsync-bug-silent-drop"),
		}

	case CategoryDestCapacity:
		se.FailureType = FailureTypeConfigError
		se.Code = "DESTINATION_CAPACITY_EXCEEDED"
		se.Audience = AudienceUser
		se.UserMessage = "The destination is out of disk/memory/quota. Add capacity or reduce the sync window."
		se.Remediation = &Remediation{
			Steps: []string{
				"Free space on the destination (drop unused tables, increase disk)",
				"Or reduce the pipeline's table set / time window",
				"Re-run the pipeline once capacity is available",
			},
			DocURL:           ErrorDocURL("dest-capacity"),
			EstimatedMinutes: 30,
		}

	case CategoryUserConfig:
		// Most precise mapping happens via FromErrorMessage's CDC
		// provisioning rules below. Default = generic config error.
		se.FailureType = FailureTypeConfigError
		se.Code = "USER_CONFIG_INVALID"
		se.Audience = AudienceUser
		se.UserMessage = "Pipeline configuration is incomplete or invalid. Review the connection and pipeline settings."
		se.Remediation = &Remediation{
			Steps: []string{
				"Open the pipeline configuration",
				"Review the field flagged in the error details",
				"Save changes and re-run",
			},
			DocURL:           ErrorDocURL("user-config-invalid"),
			EstimatedMinutes: 5,
		}
		// Refine based on specific known patterns.
		applyCDCProvisioningRefinement(se, s)

	case CategoryUnknown:
		fallthrough
	default:
		se.FailureType = FailureTypeSystemError
		se.Code = "UNKNOWN_ERROR"
		se.Audience = AudienceDeveloper
		se.UserMessage = "An unexpected error occurred. Our team has been notified."
	}

	return se
}

// applyCDCProvisioningRefinement maps specific CDC provisioning error
// patterns to more precise error codes + DB-specific remediation.
//
// This is the routine that turns a generic "user_config" diagnosis into
// "MYSQL_BINLOG_FORMAT_NOT_ROW with copy-pasteable SQL" — the difference
// between a useless alert and an actionable one.
func applyCDCProvisioningRefinement(se *StructuredError, s Signal) {
	low := strings.ToLower(s.ErrorMessage)

	switch {
	case strings.Contains(low, "publication does not exist"):
		se.Code = "POSTGRES_PUBLICATION_DOES_NOT_EXIST"
		se.UserMessage = "The PostgreSQL publication for this pipeline doesn't exist. rsync provisions it automatically; this usually means it was dropped manually."
		se.Remediation = &Remediation{
			Steps: []string{
				"Re-run the pipeline — rsync will recreate the publication",
				"If it fails again, ensure the database user has CREATE on the database",
			},
			DocURL:           ErrorDocURL("postgres-publication-missing"),
			EstimatedMinutes: 2,
		}

	case strings.Contains(low, "wal_level") && strings.Contains(low, "logical"):
		se.Code = "POSTGRES_WAL_LEVEL_NOT_LOGICAL"
		se.UserMessage = "PostgreSQL wal_level must be 'logical' for CDC. The current setting blocks replication slot creation."
		se.Remediation = &Remediation{
			Steps: []string{
				"As a PostgreSQL superuser, run the SQL below",
				"Restart PostgreSQL for the setting to take effect",
				"Re-run this pipeline",
			},
			SQLToRun: []string{
				"ALTER SYSTEM SET wal_level = 'logical';",
				"-- Then restart PostgreSQL (sudo systemctl restart postgresql, or your platform's equivalent)",
			},
			DocURL:           ErrorDocURL("postgres-wal-level"),
			EstimatedMinutes: 10,
		}

	case strings.Contains(low, "binlog_format"):
		se.Code = "MYSQL_BINLOG_FORMAT_NOT_ROW"
		se.UserMessage = "MySQL binlog_format must be 'ROW' for CDC. The current setting prevents Debezium from decoding row-level changes."
		se.Remediation = &Remediation{
			Steps: []string{
				"As a MySQL admin, run the SQL below",
				"For RDS/Aurora, set 'binlog_format' to 'ROW' in the DB parameter group + reboot",
				"Re-run this pipeline",
			},
			SQLToRun: []string{
				"SET GLOBAL binlog_format = 'ROW';",
				"-- For permanence across MySQL restart, also update my.cnf:",
				"-- [mysqld]",
				"-- binlog_format = ROW",
			},
			DocURL:           ErrorDocURL("mysql-binlog-format"),
			EstimatedMinutes: 10,
		}

	case strings.Contains(low, "binlog_row_image"):
		se.Code = "MYSQL_BINLOG_ROW_IMAGE_NOT_FULL"
		se.UserMessage = "MySQL binlog_row_image should be 'FULL' for CDC correctness. Current setting may produce incomplete UPDATE events."
		se.Severity = SeverityWarning
		se.Remediation = &Remediation{
			Steps: []string{
				"As a MySQL admin, run the SQL below",
				"Re-run this pipeline",
			},
			SQLToRun:         []string{"SET GLOBAL binlog_row_image = 'FULL';"},
			DocURL:           ErrorDocURL("mysql-binlog-row-image"),
			EstimatedMinutes: 5,
		}

	case strings.Contains(low, "service tier") || strings.Contains(low, "not supported in this") || strings.Contains(low, "requires a higher") || strings.Contains(low, "not available in this edition"):
		se.Code = "SQLSERVER_CDC_TIER_UNSUPPORTED"
		se.UserMessage = "This Azure SQL Database service tier does not support change data capture. CDC requires at least Standard S3 (DTU model) or a General Purpose vCore tier — Basic/S0/S1/S2 and the smallest vCore sizes are rejected."
		se.Remediation = &Remediation{
			Steps: []string{
				"Scale the Azure SQL Database up to a CDC-capable service tier (S3+ DTU, or any General Purpose / Business Critical vCore tier)",
				"Wait for the scale operation to finish",
				"Re-run this pipeline",
			},
			DocURL:           ErrorDocURL("sqlserver-cdc-tier-unsupported"),
			EstimatedMinutes: 10,
		}

	case strings.Contains(low, "sp_cdc_enable_db") || strings.Contains(low, "cannot enable cdc") || strings.Contains(low, "is_cdc_enabled") || strings.Contains(low, "needs sysadmin/db_owner"):
		se.Code = "SQLSERVER_CDC_NOT_ENABLED"
		se.UserMessage = "SQL Server change data capture is not enabled on this database. rsync enables it automatically, but that requires a sysadmin or db_owner login."
		se.Remediation = &Remediation{
			Steps: []string{
				"As a sysadmin/db_owner, run the SQL below against the source database",
				"Ensure the runtime login can SELECT from the cdc schema",
				"Re-run this pipeline",
			},
			SQLToRun: []string{
				"USE [your_database];",
				"EXEC sys.sp_cdc_enable_db;",
			},
			DocURL:           ErrorDocURL("sqlserver-cdc-not-enabled"),
			EstimatedMinutes: 5,
		}

	case strings.Contains(low, "sql server agent is not running") || strings.Contains(low, "agent is not running"):
		se.Code = "SQLSERVER_AGENT_NOT_RUNNING"
		se.UserMessage = "The SQL Server Agent is not running. CDC capture jobs run under the Agent; without it no change data is produced (fn_cdc_get_max_lsn stays NULL)."
		se.Remediation = &Remediation{
			Steps: []string{
				"On Azure SQL Database, CDC capture is auto-scheduled — there is no Agent to start; confirm CDC is enabled and give the internal scheduler a minute",
				"On Azure SQL Managed Instance the Agent is managed for you",
				"On box / on-prem SQL Server only: start the SQL Server Agent service on the source instance",
				"Re-run this pipeline once change data begins flowing",
			},
			DocURL:           ErrorDocURL("sqlserver-agent-not-running"),
			EstimatedMinutes: 5,
		}

	case strings.Contains(low, "capture instance"):
		se.Code = "SQLSERVER_CAPTURE_INSTANCE_ERROR"
		se.UserMessage = "A SQL Server capture instance could not be created or was not found. Each source table allows at most two capture instances."
		se.Remediation = &Remediation{
			Steps: []string{
				"List existing capture instances: SELECT capture_instance, source_schema, source_name FROM cdc.change_tables;",
				"If the table already has two capture instances, disable an unused one with sys.sp_cdc_disable_table",
				"Re-run this pipeline",
			},
			DocURL:           ErrorDocURL("sqlserver-capture-instance"),
			EstimatedMinutes: 10,
		}

	case strings.Contains(low, "not a replica set") || strings.Contains(low, "notprimarynosecondaryok") || strings.Contains(low, "is not a member of a replica set"):
		se.Code = "MONGODB_NOT_REPLICA_SET"
		se.UserMessage = "MongoDB change data capture requires a replica set (or sharded cluster) — Debezium reads the change stream, which a standalone mongod does not expose. Initialize a single-node replica set, or point rsync at your existing one."
		se.Remediation = &Remediation{
			Steps: []string{
				"Start mongod with --replSet rs0 (or set replication.replSetName in mongod.conf)",
				"Initialize the replica set once from a mongosh shell connected to the node (command below)",
				"Ensure the CDC user has the 'read' + 'changeStream' privileges and can read the 'admin' database",
				"Re-run this pipeline",
			},
			CommandsToRun: []string{
				"// In mongosh, connected to the mongod:",
				"rs.initiate()",
			},
			DocURL:           ErrorDocURL("mongodb-not-replica-set"),
			EstimatedMinutes: 10,
		}

	case strings.Contains(low, "resume token") || strings.Contains(low, "resume of change stream") || strings.Contains(low, "changestreamhistorylost") || strings.Contains(low, "change stream history lost") || strings.Contains(low, "resume point may no longer be in the oplog"):
		se.Code = "MONGODB_RESUME_TOKEN_INVALID"
		se.UserMessage = "The MongoDB change-stream resume token is no longer in the oplog, so streaming can't continue from where it left off. rsync re-snapshots the affected collections from a fresh position; no data is lost, but the snapshot re-reads them."
		se.Severity = SeverityWarning
		se.Remediation = &Remediation{
			Steps: []string{
				"No manual SQL is needed — rsync re-snapshots from a fresh change-stream position",
				"To prevent recurrence, size the oplog so tokens survive longer downtime (commands below)",
				"Re-run or resume this pipeline",
			},
			CommandsToRun: []string{
				"// Inspect the current oplog window (mongosh):",
				"db.getReplicationInfo()",
				"// Increase the oplog size (example: 16 GB):",
				"db.adminCommand({ replSetResizeOplog: 1, size: 16384 })",
			},
			DocURL:           ErrorDocURL("mongodb-resume-token-invalid"),
			EstimatedMinutes: 10,
		}

	case strings.Contains(low, "primary key") || strings.Contains(low, "missing pk"):
		se.Code = "CDC_TABLE_MISSING_PRIMARY_KEY"
		se.UserMessage = "One or more tables in this pipeline don't have a primary key. CDC to a database destination requires PKs to identify rows for updates and deletes."
		se.Remediation = &Remediation{
			Steps: []string{
				"Identify the table(s) listed in the error details",
				"Add a primary key (or unique not-null index) to each",
				"Re-run this pipeline",
			},
			SQLToRun: []string{
				"-- Example (replace schema/table/column):",
				"ALTER TABLE schema_name.table_name ADD PRIMARY KEY (id);",
			},
			DocURL:           ErrorDocURL("cdc-missing-pk"),
			EstimatedMinutes: 15,
		}

	case strings.Contains(low, "slot already exists") || strings.Contains(low, "replication slot"):
		se.Code = "POSTGRES_REPLICATION_SLOT_CONFLICT"
		se.UserMessage = "A PostgreSQL replication slot for this pipeline already exists from a previous run. rsync's healer will attempt to reuse or recreate it."
		se.Audience = AudienceOperator // auto-healable, only escalate if heal fails
		se.Severity = SeverityWarning
		se.Remediation = &Remediation{
			Steps: []string{
				"No action required — rsync's healer is attempting auto-recovery",
				"If this error persists after 5 minutes, contact support",
			},
			DocURL: ErrorDocURL("postgres-slot-conflict"),
		}

	case strings.Contains(low, "table not found") || strings.Contains(low, "no such table"):
		se.Code = "CDC_TABLE_NOT_FOUND_IN_SOURCE"
		se.UserMessage = fmt.Sprintf("A table selected for this pipeline doesn't exist in the source. Verify the table name and schema in your source %s.", se.SourceDBType)
		se.Remediation = &Remediation{
			Steps: []string{
				"Check the source database for the listed table",
				"If you renamed/dropped it, update this pipeline's table selection",
				"If you use a non-default schema, verify it's set on the connection",
			},
			DocURL:           ErrorDocURL("cdc-table-not-found"),
			EstimatedMinutes: 5,
		}
	}
}

// FromError is a convenience wrapper for places that have only a raw error
// (no Signal context). Prefer FromDiagnosis when you have a Signal.
func FromError(err error, code string, failureType FailureType, audience Audience, userMessage string) *StructuredError {
	se := NewStructuredError(code, failureType, audience, userMessage)
	if err != nil {
		se.InternalMessage = err.Error()
	}
	return se
}
