package diagnose

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Each test names the input class + asserts the user sees a fix, not
// just a symptom. The user_message is the contract; if a refactor
// turns "your MySQL needs binlog_format=ROW" back into
// "failed to start Debezium connector", these tests fail loudly.

func TestNewStructuredError_StampsTimestamp(t *testing.T) {
	se := NewStructuredError("TEST", FailureTypeSystemError, AudienceDeveloper, "msg")
	if se.OccurredAt == "" {
		t.Fatal("OccurredAt was empty")
	}
	if _, err := time.Parse(time.RFC3339, se.OccurredAt); err != nil {
		t.Fatalf("OccurredAt not RFC3339: %v", err)
	}
}

func TestFromDiagnosis_AuthExpiredHasRemediation(t *testing.T) {
	se := FromDiagnosis(
		Diagnosis{Category: CategoryAuthExpired, SuggestedAction: ActionRefreshAuth, Confidence: 0.95},
		Signal{ErrorMessage: "HTTP 401 invalid_token", SourceType: "stripe-rest"},
	)
	if se.FailureType != FailureTypeConfigError {
		t.Errorf("failure_type: want config_error, got %s", se.FailureType)
	}
	if se.Code != "AUTH_TOKEN_EXPIRED" {
		t.Errorf("code: want AUTH_TOKEN_EXPIRED, got %s", se.Code)
	}
	if se.Audience != AudienceUser {
		t.Errorf("audience: want user, got %s", se.Audience)
	}
	if se.Remediation == nil || len(se.Remediation.Steps) == 0 {
		t.Errorf("expected non-empty remediation steps")
	}
	if se.InternalMessage != "HTTP 401 invalid_token" {
		t.Errorf("internal_message should preserve raw error")
	}
}

func TestFromDiagnosis_RateLimitIsTransientWarning(t *testing.T) {
	se := FromDiagnosis(
		Diagnosis{Category: CategoryRateLimit, SuggestedAction: ActionBackoffRetry, Confidence: 0.9},
		Signal{ErrorMessage: "HTTP 429 too many requests"},
	)
	if se.FailureType != FailureTypeTransientError {
		t.Errorf("want transient_error, got %s", se.FailureType)
	}
	if se.Severity != SeverityWarning {
		t.Errorf("rate limit should be warning, got %s", se.Severity)
	}
}

func TestFromDiagnosis_ConnectorBugRoutesToDeveloper(t *testing.T) {
	se := FromDiagnosis(
		Diagnosis{Category: CategoryConnectorBug, SuggestedAction: ActionRegenerateConnector, Confidence: 0.9},
		Signal{ErrorMessage: "connector returned 0 rows but source has 5", SourceRowCount: 5, WrittenRows: 0},
	)
	if se.FailureType != FailureTypeSystemError {
		t.Errorf("want system_error, got %s", se.FailureType)
	}
	if se.Audience != AudienceDeveloper {
		t.Errorf("connector bug must route to developer, got %s", se.Audience)
	}
	if se.Remediation == nil || len(se.Remediation.Steps) == 0 {
		t.Fatal("connector bug should have remediation steps reassuring the user")
	}
	joined := strings.ToLower(strings.Join(se.Remediation.Steps, " "))
	if !strings.Contains(joined, "no action required") {
		t.Errorf("remediation steps should tell user no action needed; got %v", se.Remediation.Steps)
	}
}

func TestRefinement_MySQLBinlogFormatGivesCopyableSQL(t *testing.T) {
	se := FromDiagnosis(
		Diagnosis{Category: CategoryUserConfig, SuggestedAction: ActionEscalate, Confidence: 0.9},
		Signal{ErrorMessage: "MySQL binlog_format is 'STATEMENT' but must be 'ROW' for CDC", SourceType: "mysql"},
	)
	if se.Code != "MYSQL_BINLOG_FORMAT_NOT_ROW" {
		t.Errorf("code: want MYSQL_BINLOG_FORMAT_NOT_ROW, got %s", se.Code)
	}
	if se.Remediation == nil || len(se.Remediation.SQLToRun) == 0 {
		t.Fatal("expected copy-pasteable SQL")
	}
	found := false
	for _, sql := range se.Remediation.SQLToRun {
		if strings.Contains(sql, "SET GLOBAL binlog_format") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected SET GLOBAL binlog_format in SQLToRun; got %v", se.Remediation.SQLToRun)
	}
}

func TestRefinement_PostgresWALLevelGivesCopyableSQL(t *testing.T) {
	se := FromDiagnosis(
		Diagnosis{Category: CategoryUserConfig, SuggestedAction: ActionEscalate, Confidence: 0.9},
		Signal{ErrorMessage: "wal_level is 'replica', must be 'logical'", SourceType: "postgresql"},
	)
	if se.Code != "POSTGRES_WAL_LEVEL_NOT_LOGICAL" {
		t.Errorf("code: want POSTGRES_WAL_LEVEL_NOT_LOGICAL, got %s", se.Code)
	}
	if se.Remediation == nil || len(se.Remediation.SQLToRun) == 0 {
		t.Fatal("expected SQLToRun for wal_level fix")
	}
	if !strings.Contains(strings.Join(se.Remediation.SQLToRun, " "), "wal_level") {
		t.Errorf("expected wal_level in SQLToRun")
	}
}

func TestRefinement_SQLServerCDCNotEnabledGivesCopyableSQL(t *testing.T) {
	se := FromDiagnosis(
		Diagnosis{Category: CategoryUserConfig, SuggestedAction: ActionEscalate, Confidence: 0.9},
		Signal{ErrorMessage: "sp_cdc_enable_db failed (needs sysadmin/db_owner): permission denied", SourceType: "sqlserver"},
	)
	if se.Code != "SQLSERVER_CDC_NOT_ENABLED" {
		t.Errorf("code: want SQLSERVER_CDC_NOT_ENABLED, got %s", se.Code)
	}
	if se.Remediation == nil || len(se.Remediation.SQLToRun) == 0 {
		t.Fatal("expected SQLToRun for sp_cdc_enable_db fix")
	}
	if !strings.Contains(strings.Join(se.Remediation.SQLToRun, " "), "sp_cdc_enable_db") {
		t.Errorf("expected sp_cdc_enable_db in SQLToRun")
	}
}

// A tier-gated CDC-enable failure still wraps "sp_cdc_enable_db" (which also
// matches the CDC_NOT_ENABLED needle), so the tier case MUST be checked first
// and MUST NOT hand back a retry SQL — re-running EXEC sp_cdc_enable_db at the
// same service tier fails identically forever.
func TestRefinement_SQLServerCDCTierUnsupportedHasNoRetrySQL(t *testing.T) {
	se := FromDiagnosis(
		Diagnosis{Category: CategoryUserConfig, SuggestedAction: ActionEscalate, Confidence: 0.9},
		Signal{ErrorMessage: "sp_cdc_enable_db failed: change data capture is not supported in this service tier of Azure SQL Database", SourceType: "sqlserver"},
	)
	if se.Code != "SQLSERVER_CDC_TIER_UNSUPPORTED" {
		t.Errorf("code: want SQLSERVER_CDC_TIER_UNSUPPORTED, got %s", se.Code)
	}
	if se.Remediation == nil || len(se.Remediation.Steps) == 0 {
		t.Fatal("expected remediation steps to scale the tier up")
	}
	if len(se.Remediation.SQLToRun) != 0 {
		t.Errorf("tier-unsupported must not hand back a retry SQL; got %v", se.Remediation.SQLToRun)
	}
}

func TestRefinement_SQLServerAgentNotRunning(t *testing.T) {
	se := FromDiagnosis(
		Diagnosis{Category: CategoryUserConfig, SuggestedAction: ActionEscalate, Confidence: 0.9},
		Signal{ErrorMessage: "SQL Server Agent is not running but must be Running for CDC capture", SourceType: "sqlserver"},
	)
	if se.Code != "SQLSERVER_AGENT_NOT_RUNNING" {
		t.Errorf("code: want SQLSERVER_AGENT_NOT_RUNNING, got %s", se.Code)
	}
	if se.Remediation == nil || len(se.Remediation.Steps) == 0 {
		t.Fatal("expected remediation steps for agent-not-running")
	}
}

func TestRefinement_MongoDBNotReplicaSetGivesCommand(t *testing.T) {
	se := FromDiagnosis(
		Diagnosis{Category: CategoryUserConfig, SuggestedAction: ActionEscalate, Confidence: 0.9},
		Signal{ErrorMessage: "The $changeStream stage is only supported on replica sets: server is not a replica set", SourceType: "mongodb"},
	)
	if se.Code != "MONGODB_NOT_REPLICA_SET" {
		t.Errorf("code: want MONGODB_NOT_REPLICA_SET, got %s", se.Code)
	}
	// rs.initiate() is not SQL — it must land in CommandsToRun, not SQLToRun.
	if se.Remediation == nil || len(se.Remediation.CommandsToRun) == 0 {
		t.Fatal("expected CommandsToRun for rs.initiate()")
	}
	if !strings.Contains(strings.Join(se.Remediation.CommandsToRun, " "), "rs.initiate") {
		t.Errorf("expected rs.initiate() in CommandsToRun; got %v", se.Remediation.CommandsToRun)
	}
	if len(se.Remediation.SQLToRun) != 0 {
		t.Errorf("MongoDB remediation must not use SQLToRun; got %v", se.Remediation.SQLToRun)
	}
}

func TestRefinement_MongoDBResumeTokenInvalid(t *testing.T) {
	se := FromDiagnosis(
		Diagnosis{Category: CategoryUserConfig, SuggestedAction: ActionReSnapshot, Confidence: 0.8},
		Signal{ErrorMessage: "resume of change stream was not possible, as the resume point may no longer be in the oplog", SourceType: "mongodb"},
	)
	if se.Code != "MONGODB_RESUME_TOKEN_INVALID" {
		t.Errorf("code: want MONGODB_RESUME_TOKEN_INVALID, got %s", se.Code)
	}
	if se.Severity != SeverityWarning {
		t.Errorf("resume-token loss is recoverable via re-snapshot; want warning severity, got %s", se.Severity)
	}
	if se.Remediation == nil || len(se.Remediation.CommandsToRun) == 0 {
		t.Fatal("expected CommandsToRun (oplog sizing) for resume-token invalidation")
	}
}

func TestRefinement_MissingPKGivesActionableSteps(t *testing.T) {
	se := FromDiagnosis(
		Diagnosis{Category: CategoryUserConfig, SuggestedAction: ActionEscalate, Confidence: 0.9},
		Signal{ErrorMessage: "CDC requires PRIMARY KEY for DB destinations; missing PK on: public.events"},
	)
	if se.Code != "CDC_TABLE_MISSING_PRIMARY_KEY" {
		t.Errorf("code: want CDC_TABLE_MISSING_PRIMARY_KEY, got %s", se.Code)
	}
	if !strings.Contains(strings.ToLower(se.UserMessage), "primary key") {
		t.Errorf("user message should mention primary key")
	}
}

func TestRefinement_SlotConflictRoutesToOperator(t *testing.T) {
	se := FromDiagnosis(
		Diagnosis{Category: CategoryUserConfig, SuggestedAction: ActionEscalate, Confidence: 0.9},
		Signal{ErrorMessage: "replication slot already exists: rsync_pipeline_abc"},
	)
	if se.Code != "POSTGRES_REPLICATION_SLOT_CONFLICT" {
		t.Errorf("code: want POSTGRES_REPLICATION_SLOT_CONFLICT, got %s", se.Code)
	}
	if se.Audience != AudienceOperator {
		t.Errorf("slot conflict should escalate to operator, not user; got %s", se.Audience)
	}
}

func TestStructuredError_JSONSerializesCleanly(t *testing.T) {
	se := FromDiagnosis(
		Diagnosis{Category: CategoryAuthExpired, Confidence: 0.95},
		Signal{
			ErrorMessage: "HTTP 401",
			PipelineID:   "pipeline-123",
			ExecutionID:  "exec-456",
			SourceType:   "shopify-admin-graphql",
		},
	)
	b, err := json.Marshal(se)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	// Spot-check that key user-facing fields survive serialization.
	for _, want := range []string{"failure_type", "code", "user_message", "remediation", "occurred_at", "pipeline_id"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("serialized JSON missing field %q\nJSON: %s", want, string(b))
		}
	}
}

func TestUnknownCategoryRoutesToDeveloper(t *testing.T) {
	se := FromDiagnosis(
		Diagnosis{Category: CategoryUnknown, Confidence: 0.3},
		Signal{ErrorMessage: "something inscrutable"},
	)
	if se.FailureType != FailureTypeSystemError {
		t.Errorf("unknown should be system_error, got %s", se.FailureType)
	}
	if se.Audience != AudienceDeveloper {
		t.Errorf("unknown should route to developer, got %s", se.Audience)
	}
}

func TestFromError_PreservesRawError(t *testing.T) {
	se := FromError(
		errString("connection refused"),
		"NETWORK_TRANSIENT_FAILURE",
		FailureTypeTransientError,
		AudienceUser,
		"Network blip; retrying",
	)
	if se.InternalMessage != "connection refused" {
		t.Errorf("expected internal_message preserved, got %q", se.InternalMessage)
	}
	if se.Code != "NETWORK_TRANSIENT_FAILURE" {
		t.Errorf("code not set correctly")
	}
}

// tiny inline error helper so we don't pull in another package
type errString string

func (e errString) Error() string { return string(e) }
