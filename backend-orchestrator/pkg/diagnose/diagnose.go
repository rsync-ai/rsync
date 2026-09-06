// Package diagnose — Phase 3 of the strategic plan.
//
// DiagnoseAgent reads an execution's failure signals and produces a
// structured diagnosis the Healer (Phase 4) can act on. v1 is a pure
// rule-based classifier — fast, deterministic, easy to test. The
// interface is shaped so a v2 LLM-backed Diagnoser can be dropped in
// without changing callers.
//
// Inputs the diagnoser may use:
//
//	error_message  — the executor's terminal error string
//	stage          — which stage failed (executor, planner, sink, etc.)
//	source_type    — the source connector_type (when known)
//	dest_type      — the destination connector_type (when known)
//	last_events    — recent pipeline_run_events for the run
//
// Output:
//
//	Diagnosis{Category, SuggestedAction, Confidence, Rationale}
//
// Confidence is in [0.0, 1.0] and represents the diagnoser's belief
// that the chosen Category is correct. The Healer reads it to decide:
//   - >0.85: auto-execute the action
//   - 0.5-0.85: HITL approve ("I think X; OK?")
//   - <0.5: escalate to a human
package diagnose

import (
	"fmt"
	"strings"
)

// AutoExecuteBand is the single source of truth for the confidence threshold at
// or above which the Healer auto-executes an action (below it the diagnosis
// goes to HITL or escalation — see the confidence bands documented above). Both
// heal.AutoBand and this package's llmConfidenceCap derive from this constant so
// the "an LLM diagnosis can propose but never auto-execute" guarantee can never
// drift. It lives here (the lower-level package) because heal imports diagnose,
// not the other way round — a cycle otherwise.
const AutoExecuteBand = 0.85

// Category is the failure type a diagnoser maps an incident to. The
// enum is closed — every diagnoser must return one of these values so
// the Healer's action table can be exhaustive.
type Category string

const (
	CategoryAuthExpired  Category = "auth_expired"
	CategoryAuthScope    Category = "auth_scope"
	CategoryRateLimit    Category = "rate_limit"
	CategorySchemaDrift  Category = "schema_drift"
	CategoryNetwork      Category = "network"
	CategoryConnectorBug Category = "connector_bug"
	CategoryDestCapacity Category = "dest_capacity"
	CategoryUserConfig   Category = "user_config"
	// CategoryOrchestration covers failures of the run's control plane rather
	// than of the data path: the Temporal workflow is gone, or the execution
	// row was reaped by the healer's own zombie sweep. The connector, the
	// credentials and the schema are all fine — there is simply no live run
	// left. The remedy is always "start a fresh run", never a config change.
	CategoryOrchestration Category = "orchestration"
	CategoryUnknown       Category = "unknown"
)

// Action is a closed enum of things the Healer can do in response to a
// diagnosis. v1 keeps the set small; new actions are added as Healer
// gains capabilities.
type Action string

const (
	ActionRegenerateConnector Action = "regenerate_connector"
	ActionRefreshAuth         Action = "refresh_auth"
	ActionBackoffRetry        Action = "backoff_retry"
	ActionRequestUserConfig   Action = "request_user_config"
	ActionEscalate            Action = "escalate_to_human"
	ActionNoOp                Action = "no_op"
	// ActionReSnapshot re-provisions the stream from a fresh position when the
	// prior CDC position is irrecoverably gone (MongoDB change-stream resume
	// token no longer in the oplog; a future Oracle SCN aged out of retention).
	// Distinct from ActionBackoffRetry (the position will never return, so
	// retrying is futile) and ActionEscalate (there is nothing for an operator
	// to reconfigure — the fix is a re-snapshot the system can perform).
	ActionReSnapshot Action = "re_snapshot"
)

// Signal is the minimal bundle of facts a Diagnoser inspects. Keeping
// this a plain struct (no DB / HTTP handles) makes the diagnoser pure
// and trivially testable.
type Signal struct {
	// Execution identity — required by Healer executors that need to
	// write back to the DB or call the retry API. Zero-valued when the
	// Signal is constructed purely for classification (e.g. tests).
	PipelineID  string
	ExecutionID string

	ErrorMessage    string
	ExecutorStatus  string // e.g. "silent_drop_detected", "failed"
	Stage           string // "executor", "planner", "sink", etc.
	SourceType      string
	DestinationType string
	LastEvents      []string // human-readable summary lines
	WrittenRows     int64
	SourceRowCount  int64

	// Per-table shape of the run, which the two totals above cannot express.
	// TablesWithNoLandedRows counts tables that read rows from the source and
	// landed none of them; TablesObserved is how many tables reported stats at
	// all, and is the denominator without which the first number cannot be read.
	//
	// A run with one healthy table and two total losses sums to WrittenRows > 0,
	// exactly like a run that lost fifty rows spread thinly across every table.
	// These two counters are what tells those apart. Zero/zero means "no
	// per-table evidence", not "no tables were dropped" — the rules below treat
	// it that way.
	TablesWithNoLandedRows int64
	TablesObserved         int64
}

// Diagnosis is the structured output. Rationale is a one-line string
// the UI shows the operator alongside the chosen action.
type Diagnosis struct {
	Category        Category
	SuggestedAction Action
	Confidence      float64
	Rationale       string
}

// Diagnoser is the interface callers (Healer, tests) depend on. The
// v1 RuleBasedDiagnoser satisfies it; a future LLMDiagnoser swaps in
// without changes elsewhere.
type Diagnoser interface {
	Diagnose(signal Signal) Diagnosis
}

// RuleBasedDiagnoser — v1 classifier.
//
// The matching rules are intentionally narrow: false categorisation is
// worse than no categorisation (it sends the Healer down the wrong
// path). When in doubt we return CategoryUnknown with low confidence
// so the Healer escalates rather than guesses.
type RuleBasedDiagnoser struct{}

func New() *RuleBasedDiagnoser { return &RuleBasedDiagnoser{} }

// Diagnose classifies a signal by FIRST-MATCH over an ordered rule chain —
// the rules below are evaluated top-to-bottom and the first one that matches
// wins (this is a sequential if/return chain, NOT a highest-confidence sort).
// A message may contain keywords for several rules (e.g. a CDC-provisioning
// error whose underlying cause is "connection reset by peer"); source order
// alone decides the winner, so the ordering is load-bearing and safety-
// critical. In particular the higher-priority rules must stay above the
// lower-priority ones: silent-drop → auth → rate-limit → lost-stream-position
// (re-snapshot) → CDC provisioning (escalate) → mask-column (user-config) →
// schema-drift → workflow-gone (orchestration) → swept-zombie (orchestration)
// → non-retryable (escalate) → transient network (backoff-retry) →
// dest-capacity → user-config → unknown. Moving the transient/network rule
// above CDC provisioning would let a provisioning failure be retried forever
// instead of escalated; moving it above the non-retryable guard would let a
// failure the executor already declared deterministic be retried because it
// happens to contain the words "timed out" — see
// TestRulePrecedence_SafetyCriticalOrdering, which locks this order.
func (d *RuleBasedDiagnoser) Diagnose(signal Signal) Diagnosis {
	low := strings.ToLower(signal.ErrorMessage)

	// Phase 1's silent-drop status is a strong signal — high confidence
	// it's a connector bug or auth/scope misconfig. Drill further by
	// inspecting error_message and SourceRowCount.
	if signal.ExecutorStatus == "silent_drop_detected" || signal.ExecutorStatus == "silent_partial_drop_detected" {
		if signal.SourceRowCount > 0 && signal.WrittenRows == 0 {
			return Diagnosis{
				Category:        CategoryConnectorBug,
				SuggestedAction: ActionRegenerateConnector,
				Confidence:      0.7,
				Rationale:       "source has rows but destination wrote zero — connector likely doesn't emit them correctly",
			}
		}
		// Partial loss with a nameable shape. The totals say some rows landed, so
		// the predicate above is false — but if whole tables read rows and landed
		// none of them, the run did not lose rows diffusely, it lost specific
		// tables, and the operator's next move is to look at those.
		//
		// Escalates rather than regenerating: the connector demonstrably works for
		// the tables that did land, so the fault is more likely per-table (a
		// permission, a type the mapper refuses, a name collision at the
		// destination) than connector-wide, and no executor can settle which
		// without seeing the tables.
		if signal.TablesWithNoLandedRows > 0 {
			return Diagnosis{
				Category:        CategoryConnectorBug,
				SuggestedAction: ActionEscalate,
				Confidence:      0.65,
				Rationale: fmt.Sprintf(
					"%d of %d tables read rows from the source and landed none — the drop is "+
						"table-scoped, not pipeline-wide (%d rows read, %d landed overall)",
					signal.TablesWithNoLandedRows, signal.TablesObserved,
					signal.SourceRowCount, signal.WrittenRows),
			}
		}
		return Diagnosis{
			Category:        CategoryConnectorBug,
			SuggestedAction: ActionEscalate,
			Confidence:      0.55,
			Rationale:       "silent drop detected without clear cause; needs human inspection",
		}
	}

	// Auth-expired patterns. HTTP 401 in error_message is the strongest
	// signal. Pre-existing `waiting_for_credential_reauth` from Phase 2.5
	// is even stronger.
	if signal.ExecutorStatus == "waiting_for_credential_reauth" {
		return Diagnosis{
			Category:        CategoryAuthExpired,
			SuggestedAction: ActionRefreshAuth,
			Confidence:      0.95,
			Rationale:       "Phase 2.5 CredentialAgent classified failure as auth_expired",
		}
	}
	if matchesAny(low, []string{"http 401", "401 unauthorized", "invalid token", "token expired", "token has expired"}) {
		return Diagnosis{
			Category:        CategoryAuthExpired,
			SuggestedAction: ActionRefreshAuth,
			Confidence:      0.9,
			Rationale:       "error message indicates auth token is expired or invalid",
		}
	}

	// Auth-scope patterns.
	if signal.ExecutorStatus == "waiting_for_credential_scope" {
		return Diagnosis{
			Category:        CategoryAuthScope,
			SuggestedAction: ActionRequestUserConfig,
			Confidence:      0.95,
			Rationale:       "Phase 2.5 CredentialAgent classified failure as auth_scope",
		}
	}
	if matchesAny(low, []string{"http 403", "403 forbidden", "access denied", "accessdenied", "permission denied", "missing scope", "insufficient scope"}) {
		return Diagnosis{
			Category:        CategoryAuthScope,
			SuggestedAction: ActionRequestUserConfig,
			Confidence:      0.85,
			Rationale:       "permission/scope failure surfaced in error message",
		}
	}

	// Rate limits.
	if matchesAny(low, []string{"http 429", "rate limit", "too many requests", "throttled", "quota exceeded"}) {
		return Diagnosis{
			Category:        CategoryRateLimit,
			SuggestedAction: ActionBackoffRetry,
			Confidence:      0.9,
			Rationale:       "upstream API is rate-limiting us",
		}
	}

	// MongoDB change-stream resume-token loss — the token no longer exists in the
	// oplog (it rolled over past the token, or the stream was invalidated). The
	// stream position is GONE: a bare retry can never recover it, and there is
	// nothing for an operator to reconfigure, so the pipeline must re-snapshot
	// from a fresh position. Checked BEFORE the escalate block below so a
	// "resume of change stream was not possible" message routes here, not there.
	if matchesAny(low, []string{
		"resume token",
		"resume of change stream was not possible",
		"resume point may no longer be in the oplog",
		"changestreamhistorylost",
		"change stream history lost",
		// MongoDB server error code 260. Was spelled "invalidateresumetoken",
		// which matches nothing the server emits — and could not match the spaced
		// phrase "invalidate resume token" either, having no spaces itself. So an
		// InvalidResumeToken failure fell past this block to escalate/0.3.
		"invalidresumetoken",
		// Oracle: the starting SCN aged out of the available redo/archive logs
		// (ORA-01555 "snapshot too old"). Like a lost Mongo resume token, the
		// position is GONE — a retry can never recover it, so re-snapshot from a
		// fresh SCN. Checked BEFORE the escalate block so it routes here, not there.
		"ora-01555",
		"snapshot too old",
		"scn is no longer available",
		"scn no longer available",
	}) {
		return Diagnosis{
			Category:        CategoryUserConfig,
			SuggestedAction: ActionReSnapshot,
			Confidence:      0.8,
			Rationale:       "CDC stream position is no longer available (MongoDB resume token aged out of the oplog, or an Oracle SCN aged out of redo/archive retention) — re-snapshot from a fresh position (retry cannot recover a lost position)",
		}
	}

	// CDC provisioning hard-failures — these are operator-configuration errors,
	// not transient network blips. Auto-retry would spin forever without fixing
	// anything, so we escalate immediately so the operator is notified.
	if matchesAny(low, []string{
		// Debezium / replication-slot errors (PostgreSQL)
		"publication does not exist",
		"replication slot",
		"slot already exists",
		"logical replication",
		// PK validation failures (all source families — "table not found in
		// source <db>" is emitted verbatim by each provider's PK validator)
		"cdc requires primary key",
		"table not found in source postgresql",
		"table not found in source mysql",
		"table not found in source sqlserver",
		"failed to connect to postgresql for pk validation",
		"failed to connect to mysql for pk validation",
		"failed to connect to sql server for pk validation",
		// Debezium connector bootstrap
		"connector already exists",
		"connector not found",
		"debezium",
		// WAL / binlog misconfiguration (PostgreSQL / MySQL)
		"wal_level",
		"binlog_format",
		"binlog_row_image",
		// SQL Server CDC provisioning (sp_cdc_enable_db/table, Agent, capture
		// instances). fn_cdc_get_max_lsn stays NULL when the Agent is down, so
		// these are operator-config failures, never transient — escalate.
		"sp_cdc_enable_db",
		"sp_cdc_enable_table",
		"cannot enable cdc",
		"capture instance",
		"capture_instance",
		"is_cdc_enabled",
		"sql server agent is not running",
		"needs sysadmin/db_owner",
		// Azure SQL Database service-tier gating: sys.sp_cdc_enable_db is rejected
		// on Basic/S0–S2 and the smallest vCore sizes. Escalate (scale the tier
		// up) — retrying at the same tier fails identically forever.
		"service tier",
		"not supported in this",
		"requires a higher",
		// MongoDB CDC prerequisites — change streams require a replica set (or
		// sharded cluster) with a reachable primary; a standalone mongod or a
		// stepped-down node can't stream. Operator-config, not transient. (The
		// resume-token-lost case is handled earlier as ActionReSnapshot.)
		"not a replica set",
		"notprimarynosecondaryok",
		"is not a member of a replica set",
		"change stream",
		"changestream",
		// Oracle LogMiner CDC provisioning — ARCHIVELOG mode, DB/table supplemental
		// logging, and LogMiner capture-user grants are all DBA-provisioned (and
		// ARCHIVELOG needs an instance restart). None are transient, so escalate;
		// a retry loop can never fix them. Codes come from OracleManager /
		// structured_error.go. (SCN-aged-out is handled earlier as ActionReSnapshot.)
		"archivelog",
		"supplemental log",
		"supplemental_logging",
		"logminer",
		"oracle_archivelog_disabled",
		"oracle_supplemental_logging_disabled",
		"oracle_logminer_privs_missing",
		"table not found in source oracle",
		"failed to connect to oracle for pk validation",
		"ora-01031", // insufficient privileges (grant/ALTER)
		"ora-00942", // table or view does not exist (provisioning target missing)
		"ora-65040", // operation not allowed from within a pluggable database (PDB/CDB)
	}) {
		return Diagnosis{
			Category:        CategoryUserConfig,
			SuggestedAction: ActionEscalate,
			Confidence:      0.9,
			Rationale:       "CDC provisioning failure — requires operator investigation (replication config, slot/publication/capture-instance state, supplemental logging/ARCHIVELOG, or PK requirements)",
		}
	}

	// A transform names a column the source does not have. This looks like
	// schema drift and is NOT: the source schema is whatever it is, and the
	// pipeline's own masking/transform config is what is out of date. Sits
	// ABOVE schema-drift so it can't be answered with "regenerate the
	// connector", which would regenerate a connector that is already correct
	// and leave the stale column name in place.
	//
	// The live prod wording is the nl-transform planner's PII guard:
	//   "requested mask column(s) [ssn] not found in source schema — refusing
	//    to run so PII is never silently left unmasked"
	// Refusing to run is the correct behaviour; only a human can say whether
	// the column was renamed or the mask should be dropped.
	if matchesAny(low, []string{
		"requested mask column",
		"not found in source schema",
		"mask column(s)",
	}) {
		return Diagnosis{
			Category:        CategoryUserConfig,
			SuggestedAction: ActionRequestUserConfig,
			Confidence:      0.85,
			Rationale:       "a transform references a column the source schema does not have — the pipeline's masking/transform config needs updating",
		}
	}

	// Schema drift — column/table not found, type mismatch.
	if matchesAny(low, []string{
		"no such table", "no such column",
		"column does not exist", "relation does not exist",
		"unknown column", "unknown field",
		"undefined column", "undefined field",
		"schema mismatch", "schema drift",
		"first or last must be provided", // Shopify Relay pagination
	}) {
		return Diagnosis{
			Category:        CategorySchemaDrift,
			SuggestedAction: ActionRegenerateConnector,
			Confidence:      0.75,
			Rationale:       "source or destination schema doesn't match what the connector expects",
		}
	}

	// The run's control plane is gone — the Temporal workflow no longer exists,
	// so the execution can never make progress again no matter what is done to
	// it. This is the single most common failure text on production and it had
	// no rule at all: every occurrence fell through to the unknown/escalate
	// fallback at 0.3.
	//
	// Confidence is deliberately 0.8 — inside the HITL band (0.50) and BELOW
	// AutoExecuteBand (0.85). BackoffRetryExecutor is the one executor that is
	// NOT HITLSafe, so at this confidence the Healer records the recommendation
	// and its prompt WITHOUT running it (heal.go's HITLSafe branch). That is
	// the intended behaviour: the remedy is a real pipeline run with real side
	// effects, and nothing has authorised the healer to start runs unattended.
	// Raising this to >= 0.85 is a one-line change that turns the healer
	// autonomous for this class — do it only deliberately.
	if matchesAny(low, []string{
		"workflow not found",
		"no longer active (workflow not found)",
		"pipeline run is no longer active",
		"workflow execution already completed",
		"workflow execution not found",
	}) {
		return Diagnosis{
			Category:        CategoryOrchestration,
			SuggestedAction: ActionBackoffRetry,
			Confidence:      0.8,
			Rationale:       "the run's workflow no longer exists — nothing can advance this execution; a fresh run is the only remedy",
		}
	}

	// The healer's own zombie sweep closed this execution (it exceeded
	// ZombieTimeout with no end_time). The marker is written by
	// sweepZombiesQuery / ZombieExecutionSweeper.Sweep, so this rule is the
	// diagnoser reading its own handwriting. Same HITL-band reasoning as above,
	// one notch lower: "it hung once" is weaker evidence that re-running helps
	// than "the workflow is provably gone".
	//
	// Note the needle is the "execution timed out" phrasing, NOT the network
	// rule's "connection timed out" — a hung run and an unreachable peer are
	// different failures, and this rule sits above the network block so the
	// shared word "timed out" can never route a swept zombie into the
	// transient-retry path.
	if matchesAny(low, []string{
		"zombie: execution timed out",
		"healer_zombie_sweep",
		"healer cleanup",
	}) {
		return Diagnosis{
			Category:        CategoryOrchestration,
			SuggestedAction: ActionBackoffRetry,
			Confidence:      0.75,
			Rationale:       "the run stalled with no end_time and was reaped by the zombie sweep — it produced no diagnosis of its own; a fresh run is needed to learn anything more",
		}
	}

	// Explicitly non-retryable failures. This is a SAFETY NET, not a
	// classifier: it sits immediately above the transient-network block so
	// that anything the executor has already declared deterministic can never
	// reach ActionBackoffRetry by merely containing a word like "timed out" or
	// "connection reset". Every more specific rule (auth, CDC provisioning,
	// schema drift, mask-column) is ABOVE this one and still wins, so this
	// only catches deterministic failures nothing else recognised.
	//
	// Escalate at 0.5 rather than the 0.3 fallback: we know something real
	// (the executor marked it non-retryable), we just don't know what.
	if matchesAny(low, []string{
		"retryable: false",
		"[deterministic:",
		"deterministic:execution_failed",
		"non-retryable",
	}) {
		return Diagnosis{
			Category:        CategoryUnknown,
			SuggestedAction: ActionEscalate,
			Confidence:      0.5,
			Rationale:       "the executor marked this failure non-retryable — retrying cannot help; human inspection required",
		}
	}

	// Network / transport — genuinely transient failures that a bounded backoff
	// can recover (the peer/DB is briefly unreachable, overloaded, or the txn
	// lost a race). Evaluated AFTER the auth and CDC-provisioning blocks so a
	// provisioning or auth error that merely mentions one of these words still
	// routes to its (higher-priority) escalate/refresh rule. The Healer caps
	// backoff-retry at 3/24h, so a peer that never recovers eventually escalates.
	//
	// NOTE: TLS / x509 certificate failures are deliberately EXCLUDED here — an
	// expired cert, unknown CA, or hostname mismatch is a persistent config
	// fault that a retry loop can never fix, so it must fall through to escalate
	// (see TestCertificateErrors_NotAutoRetried).
	if matchesAny(low, []string{
		"connection refused", "no such host", "name or service not known",
		// DNS answered, but not with an address. "no such host" is only
		// NXDOMAIN; Go reports every other resolver failure in a wording of its
		// own, so naming that one text left a SERVFAIL or a truncated answer
		// escalating to a human over an outage that clears itself. Enumerated
		// from net/dnsclient_unix.go's error block plus the glibc getaddrinfo
		// wording, and kept in lockstep with destInfraFaultMarkers in the CDC
		// sink worker, which mirrors this rule.
		"server misbehaving", "no answer from dns server", "lame referral",
		"invalid dns response", "cannot unmarshal dns message",
		"cannot marshal dns message", "temporary failure in name resolution",
		"network is unreachable", "i/o timeout", "context deadline exceeded",
		// Peer dropped an established connection mid-stream — retryable.
		"connection reset", "broken pipe", "unexpected eof",
		"server closed the connection unexpectedly", "connection timed out",
		// Connection-pool exhaustion (source or destination) — slots free up.
		"too many connections", "remaining connection slots",
		// Concurrency races/waits that resolve on retry. Postgres wordings
		// (40P01 "deadlock detected", 40001 "could not serialize access") and
		// the distinct MySQL wordings (ER_LOCK_DEADLOCK "deadlock found when
		// trying to get lock", ER_LOCK_WAIT_TIMEOUT "lock wait timeout exceeded").
		"deadlock detected", "could not serialize access",
		"deadlock found when trying to get lock", "lock wait timeout exceeded",
	}) {
		return Diagnosis{
			Category:        CategoryNetwork,
			SuggestedAction: ActionBackoffRetry,
			Confidence:      0.85,
			Rationale:       "transient network/transport failure",
		}
	}

	// Destination capacity / quota.
	if matchesAny(low, []string{
		"out of memory", "disk full", "no space left",
		"quota exceeded for", "storage full",
	}) {
		return Diagnosis{
			Category:        CategoryDestCapacity,
			SuggestedAction: ActionEscalate,
			Confidence:      0.95,
			Rationale:       "destination is out of capacity — operator intervention required",
		}
	}

	// User config — missing required fields the connector flagged.
	if matchesAny(low, []string{
		"required field", "missing required",
		"invalid configuration", "config validation failed",
	}) {
		return Diagnosis{
			Category:        CategoryUserConfig,
			SuggestedAction: ActionRequestUserConfig,
			Confidence:      0.8,
			Rationale:       "connector reported invalid or incomplete configuration",
		}
	}

	// Nothing matched.
	return Diagnosis{
		Category:        CategoryUnknown,
		SuggestedAction: ActionEscalate,
		Confidence:      0.3,
		Rationale:       "no rule matched; human inspection required",
	}
}

func matchesAny(haystack string, needles []string) bool {
	for _, n := range needles {
		if strings.Contains(haystack, n) {
			return true
		}
	}
	return false
}
