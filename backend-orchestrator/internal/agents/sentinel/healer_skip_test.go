package sentinel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	logrus "github.com/sirupsen/logrus"
)

// `nil` meant two things in restartConsumer, and the caller could not tell them apart.
//
// The function recognises four new-architecture topics and returns nil for them, meaning
// "Kafka rebalancing has this". For everything else — MCP connectors included, despite the
// doc comment that used to promise an MCP branch — it logged "skipping restart" and returned
// nil too. ExecuteHealing grades `Success: err == nil`, so the no-op scored a success, and
// Agent.triggerHealing then logged "✅ Healing action successful" and deleted the issue from
// activeIssues. A component nobody could heal had its issue closed by the attempt not to
// heal it. KI-HEAL-RESTARTCONSUMER-SKIP-GRADED-SUCCESS.
func TestRestartConsumerDistinguishesASkipFromASuccess(t *testing.T) {
	cases := []struct {
		name        string
		componentID string
		wantSkipped bool
		wantAction  string
	}{
		{
			name:        "a new-architecture topic really is handled by Kafka rebalancing",
			componentID: "task.assignments",
			wantSkipped: false,
			wantAction:  "kafka_auto_rebalance",
		},
		{
			name:        "another new-architecture topic, so the pass arm is not a single fixture",
			componentID: "pipeline.domain.events",
			wantSkipped: false,
			wantAction:  "kafka_auto_rebalance",
		},
		{
			name:        "an MCP connector has no restart path at all",
			componentID: "mcp_connector:rsync-ai-postgresql-v1-0-0-mcp",
			wantSkipped: true,
			wantAction:  "skipped",
		},
		{
			name:        "nor does an infrastructure component",
			componentID: "infrastructure:postgresql",
			wantSkipped: true,
			wantAction:  "skipped",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHealer(nil, nil, DefaultSentinelConfig(), nil, nil)

			err, details := h.restartConsumer(context.Background(), tc.componentID)

			if got := errors.Is(err, ErrHealingSkipped); got != tc.wantSkipped {
				t.Fatalf("errors.Is(err, ErrHealingSkipped) = %v, want %v (err = %v)", got, tc.wantSkipped, err)
			}
			if details["action"] != tc.wantAction {
				t.Errorf("details[action] = %v, want %q", details["action"], tc.wantAction)
			}
			if tc.wantSkipped && !strings.Contains(err.Error(), tc.componentID) {
				t.Errorf("skip error %q does not name the component it declined", err)
			}
			if !tc.wantSkipped && err != nil {
				t.Errorf("a handled topic returned err = %v, want nil", err)
			}
		})
	}
}

// The grading, one level up. Success and Skipped are separate answers, and a skip must not
// touch the attempt bookkeeping in either direction: recording it as a success would clear
// the circuit breaker and the backoff for a component nothing was done to; recording it as
// a failure would count a no-op toward MaxRestartAttempts and eventually trip the breaker
// on a component that was never touched.
func TestExecuteHealingDoesNotGradeASkipAsSuccess(t *testing.T) {
	cases := []struct {
		name        string
		componentID string
		wantSuccess bool
		wantSkipped bool
	}{
		{
			name:        "a real restart path grades as a success",
			componentID: "task.assignments",
			wantSuccess: true,
			wantSkipped: false,
		},
		{
			name:        "a component with no restart path grades as neither success nor failure",
			componentID: "mcp_connector:rsync-ai-postgresql-v1-0-0-mcp",
			wantSuccess: false,
			wantSkipped: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHealer(nil, nil, DefaultSentinelConfig(), nil, nil)
			// Prior failures, below MaxRestartAttempts (3) so the action still runs, and an
			// untripped breaker so isCircuitBroken lets it through. A skip must leave both
			// exactly as it found them — recordAttempt(_, true) deletes both entries.
			h.healingAttempts[tc.componentID] = 2
			h.circuitBreakers[tc.componentID] = &CircuitBreaker{FailureCount: 2}

			issue := &Issue{
				ID:            generateIssueID(tc.componentID, IssueTypeConnectorDown),
				Type:          IssueTypeConnectorDown,
				ComponentID:   tc.componentID,
				ComponentType: ComponentTypeMCPConnector,
			}
			result := h.ExecuteHealing(context.Background(), issue, HealingActionRestartConsumer)

			if result.Success != tc.wantSuccess {
				t.Errorf("Success = %v, want %v (error: %q)", result.Success, tc.wantSuccess, result.Error)
			}
			if result.Skipped != tc.wantSkipped {
				t.Errorf("Skipped = %v, want %v", result.Skipped, tc.wantSkipped)
			}

			if !tc.wantSkipped {
				return
			}
			if result.Error == "" {
				t.Error("a skipped result carries no reason; the log line and the DB row would say only that it was not a success")
			}
			if got := h.healingAttempts[tc.componentID]; got != 2 {
				t.Errorf("healingAttempts = %d after a skip, want 2 unchanged", got)
			}
			if _, exists := h.circuitBreakers[tc.componentID]; !exists {
				t.Error("the skip cleared the circuit breaker, which is exactly what recording it as a success does")
			}
			if _, exists := h.lastAttempt[tc.componentID]; exists {
				t.Error("the skip was recorded as an attempt; nothing was attempted")
			}
		})
	}
}

// The consequence the KI is actually about: whether the issue survives, and what the audit
// table says happened.
//
// Two-sided on purpose. A triggerHealing that never deleted anything would satisfy the skip
// row on its own, so the success row pins the deletion path as still working — otherwise a
// regression that simply stopped resolving issues would look identical to the fix.
func TestOnlyARealHealingSuccessClosesTheIssue(t *testing.T) {
	cases := []struct {
		name         string
		componentID  string
		wantResolved bool
	}{
		{
			name:         "a real restart resolves the issue",
			componentID:  "task.assignments",
			wantResolved: true,
		},
		{
			name:         "a skipped action leaves the issue open",
			componentID:  "mcp_connector:rsync-ai-postgresql-v1-0-0-mcp",
			wantResolved: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()

			// The audit row an operator reads back. `success` is the fifth argument in
			// storeHealingResultInDatabase (logger.go) and must agree with the verdict the
			// agent acted on — a skip stored as success=true is the same lie one table over.
			mock.ExpectExec("INSERT INTO sentinel_healing_results").
				WithArgs(
					sqlmock.AnyArg(), // issue_id
					sqlmock.AnyArg(), // action
					sqlmock.AnyArg(), // component_id
					sqlmock.AnyArg(), // component_type
					tc.wantResolved,  // success
					sqlmock.AnyArg(), // error_message
					sqlmock.AnyArg(), // duration_ms
					sqlmock.AnyArg(), // details
					sqlmock.AnyArg(), // timestamp
				).
				WillReturnResult(sqlmock.NewResult(0, 1))

			config := DefaultSentinelConfig()
			issue := &Issue{
				ID:            generateIssueID(tc.componentID, IssueTypeConnectorDown),
				Type:          IssueTypeConnectorDown,
				ComponentID:   tc.componentID,
				ComponentType: ComponentTypeMCPConnector,
			}
			a := &Agent{
				config:       config,
				components:   make(map[string]*ComponentHealth),
				activeIssues: map[string]*Issue{issue.ID: issue},
				healer:       NewHealer(nil, nil, config, nil, nil),
				logger:       NewAuditLogger(nil, db, config),
				ctx:          context.Background(),
			}

			a.triggerHealing(issue)

			_, stillOpen := a.activeIssues[issue.ID]
			if stillOpen == tc.wantResolved {
				t.Errorf("issue %s still in activeIssues = %v, want %v", issue.ID, stillOpen, !tc.wantResolved)
			}
			wantResolvedCount := int64(0)
			if tc.wantResolved {
				wantResolvedCount = 1
			}
			if a.issuesResolved != wantResolvedCount {
				t.Errorf("issuesResolved = %d, want %d", a.issuesResolved, wantResolvedCount)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("healing result not recorded as expected: %v", err)
			}
		})
	}
}

// The doc comment is half the defect: it promised an MCP branch the body has never had, so
// a reader concludes MCP healing is covered. Pin the body instead of the prose — if an MCP
// branch is ever added for real, this fails and the person adding it updates the KI rather
// than leaving a stale "skipped forever" claim behind.
//
// The floor matters: a matcher that found nothing would let both assertions pass vacuously.
func TestRestartConsumerStillHasNoMCPBranch(t *testing.T) {
	h := NewHealer(nil, nil, DefaultSentinelConfig(), nil, nil)

	// Every id shape the MCP health probe can produce (health_monitor.go writes
	// "mcp_connector:<name>"), plus the bare container name, plus the infrastructure ids
	// recordInfraHealth writes. None of them has a restart path today.
	ids := []string{
		"mcp_connector:rsync-ai-postgresql-v1-0-0-mcp",
		"mcp_connector:rsync-ai-stripe-v1-0-0-mcp",
		"rsync-ai-shopify-v1-0-0-mcp",
		"infrastructure:kafka",
		"infrastructure:kafka-connect",
		"infrastructure:postgresql",
	}
	if len(ids) == 0 {
		t.Fatal("vacuity floor: no component ids under test")
	}

	skipped := 0
	for _, id := range ids {
		err, details := h.restartConsumer(context.Background(), id)
		if !errors.Is(err, ErrHealingSkipped) {
			t.Errorf("restartConsumer(%q) = %v, want ErrHealingSkipped — a real branch appeared, update KI-HEAL-RESTARTCONSUMER-SKIP-GRADED-SUCCESS", id, err)
			continue
		}
		if details["reason"] != "unknown_component" {
			t.Errorf("restartConsumer(%q) details[reason] = %v, want %q", id, details["reason"], "unknown_component")
		}
		skipped++
	}
	if skipped != len(ids) {
		t.Fatalf("only %d of %d ids were skipped; the assertion below would be measuring nothing", skipped, len(ids))
	}
}

// The metric half of the same claim. The SigNoz counters are OpenTelemetry instruments;
// asserting on the points they emit needs go.opentelemetry.io/otel/sdk/metric, which this
// module does not require, so the branch is pinned where it is decided instead:
// LogHealingResult reads `outcome := classifyHealingResult(result)` ONCE and switches on
// that same value for both the audit level and the counters. Flip a case here and the
// level test and the counter arm move together — which is the point of routing them
// through one function rather than two copies of `if !result.Success`.
func TestClassifyHealingResultSeparatesADeclineFromAFailure(t *testing.T) {
	cases := []struct {
		name    string
		result  HealingResult
		want    healingOutcome
		counter string // the SigNoz counter this outcome increments, "" for none
	}{
		{
			name:    "declined: neither counter moves",
			result:  HealingResult{Skipped: true, Success: false},
			want:    healingOutcomeSkipped,
			counter: "",
		},
		{
			name:    "repaired",
			result:  HealingResult{Skipped: false, Success: true},
			want:    healingOutcomeSuccess,
			counter: "sentinel.healing.success",
		},
		{
			name:    "attempted and failed",
			result:  HealingResult{Skipped: false, Success: false},
			want:    healingOutcomeFailure,
			counter: "sentinel.healing.failures",
		},
		{
			// Not reachable from ExecuteHealing (a skip sets Success=false), asserted so
			// the classifier has a defined answer if some future caller sets both.
			name:    "contradictory: a decline is still a decline",
			result:  HealingResult{Skipped: true, Success: true},
			want:    healingOutcomeSkipped,
			counter: "",
		},
	}

	if len(cases) == 0 {
		t.Fatal("vacuity floor: no results under test")
	}
	seen := map[healingOutcome]bool{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.result
			if got := classifyHealingResult(&r); got != tc.want {
				t.Errorf("classifyHealingResult(%+v) = %q, want %q (counter: %q)", r, got, tc.want, tc.counter)
			}
		})
		seen[tc.want] = true
	}
	// All three states exercised, so a classifier collapsed to a constant fails here
	// rather than passing on a table that only ever asked about one of them.
	if len(seen) != 3 {
		t.Fatalf("vacuity floor: only %d of the 3 outcomes are covered", len(seen))
	}
}

// logLine is one captured logrus record, decoded rather than substring-matched. What
// makes a record a "failure" is its LEVEL and its fields, not any English in the message,
// so the assertions have to read those.
type logLine struct {
	level  string
	msg    string
	fields map[string]interface{}
}

// captureSentinelLogJSON runs fn with the standard logger swapped for a JSON formatter
// writing into a buffer, and returns the decoded records. Restores the logger afterwards.
func captureSentinelLogJSON(t *testing.T, fn func()) []logLine {
	t.Helper()

	var buf bytes.Buffer
	std := logrus.StandardLogger()
	prevOut, prevLevel, prevFormatter := std.Out, std.GetLevel(), std.Formatter
	std.SetOutput(&buf)
	std.SetLevel(logrus.DebugLevel)
	std.SetFormatter(&logrus.JSONFormatter{})
	defer func() {
		std.SetOutput(prevOut)
		std.SetLevel(prevLevel)
		std.SetFormatter(prevFormatter)
	}()

	fn()

	var lines []logLine
	for _, raw := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if raw == "" {
			continue
		}
		fields := map[string]interface{}{}
		if err := json.Unmarshal([]byte(raw), &fields); err != nil {
			t.Fatalf("captured log line is not JSON (%v): %s", err, raw)
		}
		level, _ := fields["level"].(string)
		msg, _ := fields["msg"].(string)
		lines = append(lines, logLine{level: level, msg: msg, fields: fields})
	}
	return lines
}

// The operator-facing half, and the one place the patch's central claim — "a skip is
// neither a success nor a failure" — is kept or broken END TO END.
//
// The first version of this test asserted only that the captured output did not contain
// the string "Healing action failed". That is a test that cannot fail on the defect it
// was written for: AuditLogger.LogHealingResult never writes that literal under ANY
// circumstance — it writes the raw error text, at level "error", with success=false.
// So the negative was satisfied while a failure record for the declined action was being
// emitted into the same buffer the test was reading.
//
// This version asserts the positive, on the two things that actually encode "failure":
// the record's LEVEL and its fields. Three arms, because a one-armed version passes for
// a logger that files everything as a warning:
//
//	skip    -> level=warning, skipped=true,  skip_reason names the component
//	success -> level=info,    no skipped field, success=true
//	failure -> level=error,   no skipped field  (the control: real failures still shout)
func TestTriggerHealingLogsASkipAsASkipNotAFailure(t *testing.T) {
	cases := []struct {
		name            string
		componentID     string
		priorAttempts   int
		wantAuditLevel  string
		wantAuditAction string
		wantSkippedFld  bool
		wantSuccessFld  bool
		wantAgentMsg    string
		wantAgentLevel  string
		bannedLevels    []string
		bannedMsgs      []string
	}{
		{
			name:            "a component with no restart path is a warning, not a failure",
			componentID:     "mcp_connector:rsync-ai-postgresql-v1-0-0-mcp",
			wantAuditLevel:  "warning",
			wantAuditAction: string(HealingActionRestartConsumer),
			wantSkippedFld:  true,
			wantSuccessFld:  false,
			wantAgentMsg:    "Healing action skipped",
			wantAgentLevel:  "warning",
			// Nothing in a declined action is an error. This is the assertion that goes
			// red if LogHealingResult goes back to branching on !Success alone.
			bannedLevels: []string{"error"},
			bannedMsgs:   []string{"Healing action successful", "Healing action failed"},
		},
		{
			name:            "a component Kafka rebalancing really does heal",
			componentID:     "task.assignments",
			wantAuditLevel:  "info",
			wantAuditAction: string(HealingActionRestartConsumer),
			wantSkippedFld:  false,
			wantSuccessFld:  true,
			wantAgentMsg:    "Healing action successful",
			wantAgentLevel:  "info",
			bannedLevels:    []string{"error", "warning"},
			bannedMsgs:      []string{"Healing action skipped", "Healing action failed"},
		},
		{
			// The control. Without it, a logger that graded EVERY result "warn" would
			// satisfy the skip arm, and "a skip is not a failure" would be true only
			// because nothing is a failure any more.
			name:            "a genuine failure is still filed as a failure",
			componentID:     "task.assignments",
			priorAttempts:   DefaultSentinelConfig().MaxRestartAttempts,
			wantAuditLevel:  "error",
			wantAuditAction: string(HealingActionCircuitBreak),
			wantSkippedFld:  false,
			wantSuccessFld:  false,
			wantAgentMsg:    "Healing action failed",
			wantAgentLevel:  "error",
			bannedMsgs:      []string{"Healing action successful", "Healing action skipped"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()
			mock.ExpectExec("INSERT INTO sentinel_healing_results").
				WillReturnResult(sqlmock.NewResult(0, 1))

			config := DefaultSentinelConfig()
			issue := &Issue{
				ID:            generateIssueID(tc.componentID, IssueTypeConnectorDown),
				Type:          IssueTypeConnectorDown,
				ComponentID:   tc.componentID,
				ComponentType: ComponentTypeMCPConnector,
			}
			healer := NewHealer(nil, nil, config, nil, nil)
			if tc.priorAttempts > 0 {
				healer.healingAttempts[tc.componentID] = tc.priorAttempts
			}
			a := &Agent{
				config:       config,
				components:   make(map[string]*ComponentHealth),
				activeIssues: map[string]*Issue{issue.ID: issue},
				healer:       healer,
				logger:       NewAuditLogger(nil, db, config),
				ctx:          context.Background(),
			}

			lines := captureSentinelLogJSON(t, func() { a.triggerHealing(issue) })

			if len(lines) == 0 {
				t.Fatal("vacuity floor: nothing was logged at all, so every assertion below would pass on an empty set")
			}

			// The audit record is the one AuditLogger.LogHealingResult writes: it is the
			// only line in this path carrying component=sentinel. Exactly one is expected,
			// asserted as a positive denominator before anything is read out of it.
			var audits []logLine
			for _, l := range lines {
				if l.fields["component"] == "sentinel" {
					audits = append(audits, l)
				}
			}
			if len(audits) != 1 {
				t.Fatalf("want exactly 1 audit record (component=sentinel), got %d of %d lines: %v", len(audits), len(lines), lines)
			}
			audit := audits[0]

			if audit.level != tc.wantAuditLevel {
				t.Errorf("audit record level = %q, want %q (fields: %v)", audit.level, tc.wantAuditLevel, audit.fields)
			}
			if got := audit.fields["action"]; got != tc.wantAuditAction {
				t.Errorf("audit record action = %v, want %q", got, tc.wantAuditAction)
			}
			if got := audit.fields["success"]; got != tc.wantSuccessFld {
				t.Errorf("audit record success = %v, want %v", got, tc.wantSuccessFld)
			}

			// success=false is true of a decline AND of a failure, so it cannot be the
			// thing that tells them apart. `skipped` is.
			skippedFld, present := audit.fields["skipped"]
			if tc.wantSkippedFld {
				if !present || skippedFld != true {
					t.Errorf("audit record has no skipped=true field (got %v, present=%v); a declined action is indistinguishable from a failed one without it. fields: %v", skippedFld, present, audit.fields)
				}
				reason, _ := audit.fields["skip_reason"].(string)
				if !strings.Contains(reason, tc.componentID) {
					t.Errorf("audit record skip_reason = %q, want it to name the declined component %q", reason, tc.componentID)
				}
			} else if present {
				t.Errorf("audit record carries skipped=%v for a result that was not skipped", skippedFld)
			}

			// The agent's own line, one layer up.
			var agentLine *logLine
			for i := range lines {
				if strings.Contains(lines[i].msg, tc.wantAgentMsg) {
					agentLine = &lines[i]
					break
				}
			}
			if agentLine == nil {
				t.Fatalf("no log record mentions %q; got: %v", tc.wantAgentMsg, lines)
			}
			if agentLine.level != tc.wantAgentLevel {
				t.Errorf("%q logged at level %q, want %q", tc.wantAgentMsg, agentLine.level, tc.wantAgentLevel)
			}

			for _, banned := range tc.bannedLevels {
				for _, l := range lines {
					if l.level == banned {
						t.Errorf("a %s-level record was emitted: msg=%q fields=%v", banned, l.msg, l.fields)
					}
				}
			}
			for _, banned := range tc.bannedMsgs {
				for _, l := range lines {
					if strings.Contains(l.msg, banned) {
						t.Errorf("log contains %q, which misdescribes what happened; got: %v", banned, lines)
					}
				}
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("healing result not recorded as expected: %v", err)
			}
		})
	}
}
