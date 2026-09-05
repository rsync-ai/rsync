package diagnose

import (
	"strings"
	"testing"
)

// Each test states the signal it builds and the category it expects.
// Keeping them simple and explicit so reviewers can read the entire
// rule set as a series of named examples.

func TestSilentDropWithSourceRows_IsConnectorBug(t *testing.T) {
	d := New()
	got := d.Diagnose(Signal{
		ExecutorStatus: "silent_drop_detected",
		SourceRowCount: 5,
		WrittenRows:    0,
		ErrorMessage:   "connector returned 0 rows but source actually has 5 products rows",
	})
	if got.Category != CategoryConnectorBug {
		t.Errorf("category: want connector_bug, got %s", got.Category)
	}
	if got.SuggestedAction != ActionRegenerateConnector {
		t.Errorf("action: want regenerate_connector, got %s", got.SuggestedAction)
	}
	if got.Confidence < 0.5 {
		t.Errorf("confidence: should be ≥0.5, got %f", got.Confidence)
	}
}

func TestSilentDropWithoutClearCause_EscalatesAtLowConfidence(t *testing.T) {
	d := New()
	got := d.Diagnose(Signal{
		ExecutorStatus: "silent_drop_detected",
		// SourceRowCount unset (0), so the more specific rule above
		// doesn't fire.
	})
	if got.Category != CategoryConnectorBug {
		t.Errorf("category: want connector_bug, got %s", got.Category)
	}
	if got.SuggestedAction != ActionEscalate {
		t.Errorf("action: want escalate (insufficient signal), got %s", got.SuggestedAction)
	}
	if got.Confidence > 0.7 {
		t.Errorf("confidence: should be modest, got %f", got.Confidence)
	}
}

func TestOracleCDCProvisioningErrorsEscalate(t *testing.T) {
	d := New()
	cases := []string{
		"[ORACLE_ARCHIVELOG_DISABLED] Database LOG_MODE is \"NOARCHIVELOG\" but Oracle LogMiner CDC requires ARCHIVELOG mode",
		"Database-level minimal supplemental logging is disabled",
		"Capture user cannot read V$LOG — LogMiner privileges are missing",
		"failed to add supplemental logging for HR.ORDERS: ORA-01031: insufficient privileges",
		"table not found in source oracle: HR.ORDERS",
	}
	for _, msg := range cases {
		got := d.Diagnose(Signal{ErrorMessage: msg})
		if got.SuggestedAction != ActionEscalate {
			t.Errorf("oracle provisioning error %q: want escalate, got %s", msg, got.SuggestedAction)
		}
	}
}

func TestOracleScnAgedOutReSnapshots(t *testing.T) {
	d := New()
	for _, msg := range []string{
		"ORA-01555: snapshot too old: rollback segment number ... ",
		"the requested SCN is no longer available in the online or archived redo logs",
	} {
		got := d.Diagnose(Signal{ErrorMessage: msg})
		if got.SuggestedAction != ActionReSnapshot {
			t.Errorf("oracle SCN-aged-out %q: want re_snapshot, got %s", msg, got.SuggestedAction)
		}
	}
}

func TestPhase25CredentialReauthIsHighConfidenceAuthExpired(t *testing.T) {
	d := New()
	got := d.Diagnose(Signal{
		ExecutorStatus: "waiting_for_credential_reauth",
	})
	if got.Category != CategoryAuthExpired {
		t.Errorf("category: want auth_expired, got %s", got.Category)
	}
	if got.SuggestedAction != ActionRefreshAuth {
		t.Errorf("action: want refresh_auth, got %s", got.SuggestedAction)
	}
	if got.Confidence < 0.9 {
		t.Errorf("confidence: phase-2.5 signal should give >0.9, got %f", got.Confidence)
	}
}

func TestHTTP401InErrorIsAuthExpired(t *testing.T) {
	d := New()
	got := d.Diagnose(Signal{ErrorMessage: "request failed: HTTP 401 Unauthorized: invalid_token"})
	if got.Category != CategoryAuthExpired {
		t.Errorf("category: want auth_expired, got %s", got.Category)
	}
}

func TestHTTP403InErrorIsAuthScope(t *testing.T) {
	d := New()
	got := d.Diagnose(Signal{ErrorMessage: "HTTP 403 Forbidden: missing read_orders scope"})
	if got.Category != CategoryAuthScope {
		t.Errorf("category: want auth_scope, got %s", got.Category)
	}
}

func TestRateLimitClassifiesAsBackoff(t *testing.T) {
	d := New()
	cases := []string{
		"HTTP 429 Too Many Requests",
		"rate limit exceeded on /products",
		"throttled by upstream API",
	}
	for _, msg := range cases {
		got := d.Diagnose(Signal{ErrorMessage: msg})
		if got.Category != CategoryRateLimit {
			t.Errorf("msg=%q: want rate_limit, got %s", msg, got.Category)
		}
		if got.SuggestedAction != ActionBackoffRetry {
			t.Errorf("msg=%q: want backoff_retry, got %s", msg, got.SuggestedAction)
		}
	}
}

func TestSchemaDriftIncludesShopifyRelay(t *testing.T) {
	d := New()
	got := d.Diagnose(Signal{ErrorMessage: "graphql errors: first or last must be provided"})
	if got.Category != CategorySchemaDrift {
		t.Errorf("category: want schema_drift, got %s", got.Category)
	}
	if got.SuggestedAction != ActionRegenerateConnector {
		t.Errorf("action: want regenerate_connector, got %s", got.SuggestedAction)
	}
}

func TestNetworkClassifiesAsBackoff(t *testing.T) {
	d := New()
	cases := []string{
		"dial tcp: connection refused",
		"http: i/o timeout",
		"context deadline exceeded",
	}
	for _, msg := range cases {
		got := d.Diagnose(Signal{ErrorMessage: msg})
		if got.Category != CategoryNetwork {
			t.Errorf("msg=%q: want network, got %s", msg, got.Category)
		}
		if got.SuggestedAction != ActionBackoffRetry {
			t.Errorf("msg=%q: want backoff_retry, got %s", msg, got.SuggestedAction)
		}
	}
}

func TestDestCapacityEscalates(t *testing.T) {
	d := New()
	got := d.Diagnose(Signal{ErrorMessage: "ERROR: disk full on data partition"})
	if got.Category != CategoryDestCapacity {
		t.Errorf("category: want dest_capacity, got %s", got.Category)
	}
	if got.SuggestedAction != ActionEscalate {
		t.Errorf("action: want escalate, got %s", got.SuggestedAction)
	}
	if got.Confidence < 0.9 {
		t.Errorf("confidence: capacity issues are unambiguous, expected ≥0.9, got %f", got.Confidence)
	}
}

func TestUserConfigClassification(t *testing.T) {
	d := New()
	got := d.Diagnose(Signal{ErrorMessage: "config validation failed: missing required field 'shop'"})
	if got.Category != CategoryUserConfig {
		t.Errorf("category: want user_config, got %s", got.Category)
	}
	if got.SuggestedAction != ActionRequestUserConfig {
		t.Errorf("action: want request_user_config, got %s", got.SuggestedAction)
	}
}

func TestUnknownErrorEscalatesAtLowConfidence(t *testing.T) {
	d := New()
	got := d.Diagnose(Signal{ErrorMessage: "ICMP destination port unreachable on protocol 17"})
	if got.Category != CategoryUnknown {
		t.Errorf("category: want unknown, got %s", got.Category)
	}
	if got.SuggestedAction != ActionEscalate {
		t.Errorf("action: want escalate, got %s", got.SuggestedAction)
	}
	if got.Confidence >= 0.5 {
		t.Errorf("confidence: unknown should be low (<0.5), got %f", got.Confidence)
	}
}

func TestRationaleIsAlwaysPopulated(t *testing.T) {
	d := New()
	cases := []Signal{
		{ErrorMessage: "HTTP 401"},
		{ErrorMessage: "no such table users"},
		{ErrorMessage: "unrelated error"},
		{ExecutorStatus: "silent_drop_detected", SourceRowCount: 5},
	}
	for _, s := range cases {
		got := d.Diagnose(s)
		if strings.TrimSpace(got.Rationale) == "" {
			t.Errorf("rationale empty for signal: %+v", s)
		}
	}
}

func TestCDCProvisioningErrors_EscalateNotRetry(t *testing.T) {
	d := New()
	cases := []struct {
		msg  string
		desc string
	}{
		{"publication does not exist: public_users_pub", "publication missing"},
		{"CDC requires PRIMARY KEY for DB destinations; missing PK on: public.events", "PK validation"},
		{"table not found in source postgresql: public.driverb_big", "table not found"},
		{"debezium connector failed to start: topic.prefix mismatch", "debezium keyword"},
		{"failed to connect to postgresql for pk validation: connection refused", "pg connection for pk"},
		{"wal_level is 'replica', must be 'logical'", "WAL level wrong"},
		{"connector already exists: cdc-abc123-v1", "connector conflict"},
		{"table not found in source sqlserver: dbo.orders", "sqlserver table not found"},
		{"sp_cdc_enable_db failed (needs sysadmin/db_owner)", "sqlserver enable_db privs"},
		{"SQL Server Agent is not running but must be Running for CDC capture", "sqlserver agent down"},
		{"the specified '@capture_instance' does not exist: cdc_abc_123", "sqlserver capture instance"},
		{"sp_cdc_enable_db failed: change data capture is not supported in this service tier", "sqlserver tier gated"},
		{"The $changeStream stage is only supported on replica sets: server is not a replica set", "mongodb not a replica set"},
		{"NotPrimaryNoSecondaryOk: node is not primary and cannot open a change stream", "mongodb no primary"},
	}
	for _, c := range cases {
		got := d.Diagnose(Signal{ErrorMessage: c.msg})
		if got.SuggestedAction != ActionEscalate {
			t.Errorf("desc=%q msg=%q: want escalate (not retry), got %s", c.desc, c.msg, got.SuggestedAction)
		}
		if got.Confidence < 0.8 {
			t.Errorf("desc=%q: CDC provisioning failures are unambiguous; want confidence≥0.8, got %f", c.desc, got.Confidence)
		}
	}
}

// A MongoDB change-stream resume token that has aged out of the oplog is neither
// a transient retry nor an operator-config escalation: the position is gone, so
// the pipeline must re-snapshot. This case must route to ActionReSnapshot and
// must win over the "change stream" escalate needle (it is checked first).
func TestMongoDBResumeTokenLoss_ReSnapshot(t *testing.T) {
	d := New()
	cases := []string{
		"resume of change stream was not possible, as the resume point may no longer be in the oplog",
		"ChangeStreamHistoryLost: Resume of change stream was not possible",
		"cannot resume stream; the resume token was not found in the oplog",
	}
	for _, msg := range cases {
		got := d.Diagnose(Signal{ErrorMessage: msg})
		if got.SuggestedAction != ActionReSnapshot {
			t.Errorf("msg=%q: want re_snapshot, got %s", msg, got.SuggestedAction)
		}
	}
}

func TestConfidenceIsInRange(t *testing.T) {
	d := New()
	cases := []Signal{
		{ErrorMessage: ""},
		{ErrorMessage: "HTTP 401"},
		{ErrorMessage: "rate limit"},
		{ErrorMessage: "no such column"},
		{ExecutorStatus: "silent_drop_detected"},
	}
	for _, s := range cases {
		got := d.Diagnose(s)
		if got.Confidence < 0.0 || got.Confidence > 1.0 {
			t.Errorf("confidence out of [0,1] for signal %+v: %f", s, got.Confidence)
		}
	}
}
