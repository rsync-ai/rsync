package sentinel

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Tests for the sentinel → user-alert bridge (notify.go).
//
// The sentinels hold a concrete *kafka.Manager, which is why nothing in this
// package unit-tests a produce; these exercise the two halves that actually
// decide whether a person hears about an outage — the policy
// (sentinelAlertFor) and the payload (buildSentinelNotification) — the same
// split cdc_sink_lag_test.go takes.

// codeOf digs the catalog code out of a built payload. It reads the structured
// error the way the api-gateway does (notifier.go structuredErrorView) rather
// than the private "_code" hint, so a payload that fails to carry its code on
// the wire fails here too.
func codeOf(t *testing.T, n map[string]interface{}) string {
	t.Helper()
	raw, ok := n["error"]
	if !ok {
		return ""
	}
	b, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal structured error: %v", err)
	}
	var se struct {
		Code     string `json:"code"`
		Audience string `json:"audience"`
	}
	if err := json.Unmarshal(b, &se); err != nil {
		t.Fatalf("unmarshal structured error: %v", err)
	}
	return se.Code
}

// Every failure class that means "the data stopped" must reach the bell, and
// must carry the catalog code its copy hangs off. A missing code is not a
// cosmetic problem: the api-gateway falls back to a humanized event type and
// the alert loses its category, so it can no longer be muted on its own.
func TestEveryStoppedDataIssueReachesTheUser(t *testing.T) {
	cases := []struct {
		name      string
		issueType IssueType
		severity  IssueSeverity
		alertType string
		wantCode  string
	}{
		{"connector down", IssueTypeConnectorDown, IssueSeverityCritical, "connector_down", "CDC_CONNECTOR_DOWN"},
		{"source stream stalled", IssueTypeSourceStreamStalled, IssueSeverityCritical, "source_stream_stalled", "CDC_SOURCE_STREAM_STALLED"},
		{"sink worker absent", IssueTypeSinkWorkerAbsent, IssueSeverityCritical, "sink_worker_absent", "SINK_WORKER_ABSENT"},
		{"sink write rejected", IssueTypeSinkWriteRejected, IssueSeverityCritical, "sink_write_rejected", "SINK_WRITE_REJECTED"},
		// The terminal wedge rides in through emitLagIssue as a WARNING with
		// type high_lag (cdc_sink_autorestart.go:190). Severity alone would
		// never surface it, which is the whole reason the policy looks at the
		// alert type first.
		{"sink wedged escalation", IssueTypeHighLag, IssueSeverityWarning, "sink_wedged_escalation", "CDC_SINK_WEDGED"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n, ok := buildSentinelNotification(tc.issueType, tc.severity, tc.alertType, "pipe-1", "engineer text", nil)
			if !ok {
				t.Fatalf("%s produced no user alert — this failure is invisible to the pipeline's owner", tc.name)
			}
			if got := codeOf(t, n); got != tc.wantCode {
				t.Fatalf("code = %q, want %q", got, tc.wantCode)
			}
			if n["pipeline_id"] != "pipe-1" {
				t.Fatalf("pipeline_id = %v, want pipe-1", n["pipeline_id"])
			}
			if n["action_url"] != "/pipelines/pipe-1" {
				t.Fatalf("action_url = %v", n["action_url"])
			}
			if n["severity"] != "critical" {
				t.Fatalf("severity = %v, want critical", n["severity"])
			}
			if msg, _ := n["message"].(string); strings.TrimSpace(msg) == "" {
				t.Fatal("message is empty — the bell would render a headline with no detail")
			}
		})
	}
}

// The ordinary warnings stay out of the bell. They re-fire every poll, they
// usually resolve themselves, and they already paint the pipeline overview.
// Pushing them is how an alert channel becomes noise people filter away.
func TestAnOrdinaryWarningIsNotPushed(t *testing.T) {
	for _, tc := range []struct {
		name      string
		issueType IssueType
		alertType string
	}{
		{"high lag", IssueTypeHighLag, "sink_lag"},
		{"no row movement", IssueTypeNoRowMovement, string(IssueTypeNoRowMovement)},
		{"stalled run", IssueTypeStalledRun, string(IssueTypeStalledRun)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := buildSentinelNotification(tc.issueType, IssueSeverityWarning, tc.alertType, "pipe-1", "d", nil); ok {
				t.Fatalf("%s was pushed to the bell; warnings belong on the overview", tc.name)
			}
		})
	}
}

// A critical nobody wrote copy for still alerts. Silence is the bug this file
// exists to fix, so an unmapped type degrades to a humanized headline rather
// than to nothing — the same bias categories.go takes for an uncategorized code.
func TestAnUncuratedCriticalStillAlerts(t *testing.T) {
	n, ok := buildSentinelNotification(IssueType("destination_disk_full"), IssueSeverityCritical, "destination_disk_full", "pipe-1", "the description", nil)
	if !ok {
		t.Fatal("an uncurated critical was dropped — a detector added later would be silent")
	}
	if code := codeOf(t, n); code != "" {
		t.Fatalf("code = %q, want empty (no catalog entry exists to render)", code)
	}
	if n["type"] != "destination_disk_full" {
		t.Fatalf("type = %v — the api-gateway humanizes this into the headline", n["type"])
	}
	if n["message"] != "the description" {
		t.Fatalf("message = %v, want the issue description as the fallback", n["message"])
	}
}

// The user-facing message is built from metadata, never from the issue's
// description. sink_write_rejected's description embeds the destination's raw
// error, which can quote the row that was rejected — and this payload is what
// Slack and email send. The engineer text stays in sentinel_active_issues and
// on the domain event, where whoever is fixing it can read it.
func TestTheUserMessageLeavesTheRawSinkErrorBehind(t *testing.T) {
	description := `Sink rejected 3 batch(es) for table "orders" while the run is still in flight — ` +
		`rows are being read but NOT written to the destination. Sink error: duplicate key value ` +
		`violates unique constraint "users_email_key" Key (email)=(ada@example.com) already exists`

	n, ok := buildSentinelNotification(IssueTypeSinkWriteRejected, IssueSeverityCritical, "sink_write_rejected",
		"pipe-1", description, map[string]interface{}{
			"table_name":       "orders",
			"rejected_batches": 3,
			"sink_error":       `Key (email)=(ada@example.com)`,
		})
	if !ok {
		t.Fatal("no alert built")
	}
	msg, _ := n["message"].(string)
	if strings.Contains(msg, "ada@example.com") {
		t.Fatalf("the destination's raw error reached the user message, and from there Slack and email: %q", msg)
	}
	if strings.Contains(msg, "users_email_key") {
		t.Fatalf("internal constraint name leaked into the user message: %q", msg)
	}
	// It still has to say something specific, or the alert is unactionable.
	if !strings.Contains(msg, "orders") || !strings.Contains(msg, "3") {
		t.Fatalf("message lost the facts that make it actionable: %q", msg)
	}
}

// The api-gateway hides audience=developer rows from the bell
// (handlers/notifications.go). An alert addressed to the wrong audience is
// stored and never shown — silent in exactly the way this whole change is
// meant to stop.
func TestTheAlertIsAddressedToTheOwnerNotToUs(t *testing.T) {
	n, ok := buildSentinelNotification(IssueTypeConnectorDown, IssueSeverityCritical, "connector_down", "pipe-1", "d", nil)
	if !ok {
		t.Fatal("no alert built")
	}
	b, err := json.Marshal(n["error"])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var se struct {
		Audience string `json:"audience"`
	}
	if err := json.Unmarshal(b, &se); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if se.Audience != "user" {
		t.Fatalf("audience = %q; the bell filters anything addressed to developer", se.Audience)
	}
}

// A stall is reported in the roughest unit that is still true.
func TestStallDurationReadsAsEnglish(t *testing.T) {
	cases := map[int64]string{0: "", 45: "45 seconds", 600: "10 minutes", 7200: "2 hours"}
	for secs, want := range cases {
		if got := humanDuration(secs); got != want {
			t.Fatalf("humanDuration(%d) = %q, want %q", secs, got, want)
		}
	}

	n, ok := buildSentinelNotification(IssueTypeSourceStreamStalled, IssueSeverityCritical, "source_stream_stalled",
		"pipe-1", "d", map[string]interface{}{"stalled_for_seconds": int64(7200)})
	if !ok {
		t.Fatal("no alert built")
	}
	if msg, _ := n["message"].(string); !strings.Contains(msg, "2 hours") {
		t.Fatalf("the stall duration never reached the user: %q", msg)
	}
}

// TestEveryCuratedCodeHasCopyInTheGateway closes a service boundary that nothing
// else checks.
//
// The code in this file is only half of an alert: the api-gateway renders the
// headline, the impact line and the mute category from its own catalog.go, keyed
// by this string. Nothing couples the two — no golden file, no shared package,
// and they are separate Go modules, so a code invented here and never added there
// compiles, ships, publishes, and arrives as a machine-shaped
// "Consumer Group Closed" with no category and therefore no way to mute it. The
// same gap in the other direction already left one catalog entry with no producer
// at all.
//
// Read from disk rather than imported for exactly that reason, the way
// TestEveryGeneratedConnectorContainerIsDiscoverable reads the generated compose
// file. Both directions are NOT checked: a catalog entry without a producer here
// is dead copy, not a broken alert, and other services publish codes too.
func TestEveryCuratedCodeHasCopyInTheGateway(t *testing.T) {
	catalogPath := filepath.Join(repoRoot(t), "api-gateway", "internal", "notifier", "catalog.go")
	b, err := os.ReadFile(catalogPath)
	if err != nil {
		t.Fatalf("read %s: %v", catalogPath, err)
	}
	catalog := string(b)

	codes := make([]string, 0, len(notifiableIssueTypes)+len(notifiableAlertTypes))
	for _, a := range notifiableIssueTypes {
		codes = append(codes, a.code)
	}
	for _, a := range notifiableAlertTypes {
		codes = append(codes, a.code)
	}
	// An empty set would make this pass against a file that curates nothing.
	if len(codes) < 5 {
		t.Fatalf("only %d curated codes found; the test, not the code, is broken", len(codes))
	}

	for _, code := range codes {
		if code == "" {
			continue
		}
		// The catalog key, not a mention in a comment: `"CODE": {`.
		if !strings.Contains(catalog, `"`+code+`": {`) {
			t.Errorf("code %q is published by the sentinel but has no entry in api-gateway catalog.go; "+
				"the bell will show a humanized event type with no category", code)
		}
	}
}

// TestComponentIssuesReachTheInstanceBell covers the lane
// KI-COMPONENT-HEALTH-ALERTS-NOBODY was filed for: issues about a COMPONENT, which
// have no pipeline to hang off and so were never eligible for any of the
// pipeline-scoped publishers.
func TestComponentIssuesReachTheInstanceBell(t *testing.T) {
	cases := []struct {
		name      string
		issue     *Issue
		wantCode  string
		wantInMsg string
	}{
		{
			name: "infrastructure down",
			issue: &Issue{
				Type: IssueTypeInfrastructureDown, Severity: IssueSeverityCritical,
				ComponentID: "infrastructure:postgres", ComponentType: ComponentTypeInfrastructure,
				Description: "Component is unhealthy",
			},
			wantCode: "INFRASTRUCTURE_DOWN", wantInMsg: "postgres",
		},
		{
			name: "worker stopped reporting",
			issue: &Issue{
				Type: IssueTypeMissingHeartbeat, Severity: IssueSeverityCritical,
				ComponentID: "agent:temporal-worker", ComponentType: ComponentTypeAgent,
				Description:    "Component has not sent heartbeat for 5m0s",
				Metadata:       map[string]interface{}{"time_since_heartbeat": 300.0},
				DetectedAt:     time.Time{},
				LastOccurrence: time.Time{},
			},
			wantCode: "WORKER_HEARTBEAT_LOST", wantInMsg: "temporal-worker",
		},
		{
			name: "queue reader closed",
			issue: &Issue{
				Type: IssueTypeConsumerGroupClosed, Severity: IssueSeverityCritical,
				ComponentID: "kafka_consumer:rsync.cdc-orders", ComponentType: ComponentTypeKafkaConsumer,
				Description: "Consumer group for kafka_consumer:rsync.cdc-orders is closed",
			},
			wantCode: "CONSUMER_GROUP_CLOSED", wantInMsg: "rsync.cdc-orders",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			metadata := map[string]interface{}{}
			for k, v := range c.issue.Metadata {
				metadata[k] = v
			}
			metadata["component_id"] = c.issue.ComponentID
			metadata["component_type"] = string(c.issue.ComponentType)

			n, ok := buildSentinelNotification(
				c.issue.Type, c.issue.Severity, "", instanceScopeID, c.issue.Description, metadata)
			if !ok {
				t.Fatalf("%s is not publishable; it would reach nobody, which is the bug", c.issue.Type)
			}
			if got := codeOf(t, n); got != c.wantCode {
				t.Errorf("code = %q, want %q", got, c.wantCode)
			}
			msg, _ := n["message"].(string)
			if !strings.Contains(msg, c.wantInMsg) {
				t.Errorf("message does not name the component (%q): %q", c.wantInMsg, msg)
			}
			// Instance scope: the row stores with a NULL pipeline_id and fans out to
			// every admin. A uuid check on the gateway side rejects anything else.
			if n["pipeline_id"] != instanceScopeID {
				t.Errorf("pipeline_id = %v, want %q", n["pipeline_id"], instanceScopeID)
			}
		})
	}
}

// TestAnInstanceAlertDoesNotLinkIntoPipelines pins the destination. The generic
// builder renders "/pipelines/" + pipelineID, which for the instance sentinel is
// "/pipelines/instance" — a 404 on the one alert class with no pipeline, and the
// exact bug #1159 fixed for healthwatch's per-version deep link.
func TestAnInstanceAlertDoesNotLinkIntoPipelines(t *testing.T) {
	n, ok := buildSentinelNotification(IssueTypeInfrastructureDown, IssueSeverityCritical, "",
		instanceScopeID, "Component is unhealthy",
		map[string]interface{}{"component_id": "infrastructure:kafka"})
	if !ok {
		t.Fatal("infrastructure_down is not publishable")
	}
	if got := n["action_url"]; got != "/pipelines/instance" {
		t.Fatalf("the builder's default action_url changed to %v; this test's premise is stale", got)
	}
	// publishInstanceAlert overrides it; the constant is what it overrides it with.
	if instanceActionURL != "/admin/health" {
		t.Errorf("instance alerts point at %q, not the component health page", instanceActionURL)
	}
	if strings.HasPrefix(instanceActionURL, "/pipelines") {
		t.Error("an instance alert must not deep-link into a pipeline it does not have")
	}
}

// TestAComponentAlertNeverQuotesItsLastError is a privacy test.
//
// detectUnhealthyStatus copies component.LastError into the issue metadata, and a
// component's last error is whatever its probe got back — for an infrastructure
// component, frequently a connection string with the password in it. It belongs in
// the issues row and the log. The bell goes to every admin and on to email.
func TestAComponentAlertNeverQuotesItsLastError(t *testing.T) {
	const secret = "postgres://rsync:sup3rs3cret@db:5432/pipeline_db"

	for _, it := range []IssueType{
		IssueTypeInfrastructureDown, IssueTypeMissingHeartbeat, IssueTypeConsumerGroupClosed,
	} {
		n, ok := buildSentinelNotification(it, IssueSeverityCritical, "", instanceScopeID,
			"Component is unhealthy: "+secret,
			map[string]interface{}{
				"component_id": "infrastructure:postgres",
				"last_error":   "dial error: " + secret,
			})
		if !ok {
			t.Fatalf("%s is not publishable", it)
		}
		b, err := json.Marshal(n)
		if err != nil {
			t.Fatal(err)
		}
		// The whole payload, not just the message: the structured error carries the
		// user message too, and the description is passed in as a fallback.
		if strings.Contains(string(b), "sup3rs3cret") {
			t.Errorf("%s alert carries the component's raw error into a user notification: %s", it, b)
		}
	}
}
