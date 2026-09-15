package workflows

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/rsync-ai/shared/kafkaclient"
)

// A terminal run failure is the one event a team has to hear about, and every part
// of the path from here to Slack is silent when it breaks: an unqualified topic
// publishes where nobody subscribes, an envelope with a blank code is ignored by the
// consumer, and a missing dedup subject collapses every future failure of the same
// pipeline into the first alert. None of those raise an error anywhere. These tests
// pin each one.

var notifyFixedClock = time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)

// capturingProducer records what would have gone to the broker.
type capturingProducer struct {
	sent []*sarama.ProducerMessage
	err  error
}

func (p *capturingProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	p.sent = append(p.sent, msg)
	return 0, 0, p.err
}

// withProducer installs a capturing producer into the package-global activity
// context for one test and restores whatever was there.
func withProducer(t *testing.T) *capturingProducer {
	t.Helper()
	prev := activityCtx
	p := &capturingProducer{}
	activityCtx = &ActivityContext{KafkaProducer: p}
	t.Cleanup(func() { activityCtx = prev })
	return p
}

func decodeSent(t *testing.T, msg *sarama.ProducerMessage) terminalNotification {
	t.Helper()
	raw, err := msg.Value.Encode()
	if err != nil {
		t.Fatalf("encoding the produced value failed: %v", err)
	}
	var got terminalNotification
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("produced value is not the notification shape: %v (%s)", err, raw)
	}
	return got
}

func TestOnlyATerminalNonSuccessOutcomeAlerts(t *testing.T) {
	cases := []struct {
		status   string
		wantSend bool
		wantCode string
	}{
		{"failed", true, codeRunFailed},
		{"stopped", true, codeRunStopped},
		// The negative controls. A success alert on the same channel is how a team
		// learns to ignore the failure alert.
		{"completed", false, ""},
		{"running", false, ""},
		{"streaming_active", false, ""},
		{"", false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.status+"/"+map[bool]string{true: "alerts", false: "silent"}[tc.wantSend], func(t *testing.T) {
			got, ok := buildTerminalNotification("pipe-1", "exec-1", tc.status, "boom", false, notifyFixedClock)
			if ok != tc.wantSend {
				t.Fatalf("status %q: alert=%v, want %v", tc.status, ok, tc.wantSend)
			}
			if !ok {
				return
			}
			if got.Error.Code != tc.wantCode {
				t.Fatalf("status %q: code = %q, want %q", tc.status, got.Error.Code, tc.wantCode)
			}
		})
	}
}

// The notifier activates the structured-error path ONLY when error.code is
// non-empty (notifier.go handleMessage: `parsed.Code != ""`). A blank code is not an
// error at either end — the alert just silently loses its severity, its curated copy
// and its remediation, and renders through the legacy fallback instead.
func TestEveryAlertCarriesANonEmptyCodeAndSeverity(t *testing.T) {
	for _, status := range []string{"failed", "stopped"} {
		t.Run(status, func(t *testing.T) {
			got, ok := buildTerminalNotification("pipe-1", "exec-1", status, "", false, notifyFixedClock)
			if !ok {
				t.Fatalf("status %q raised no alert", status)
			}
			if strings.TrimSpace(got.Error.Code) == "" {
				t.Fatal("error.code is empty; the consumer ignores the envelope entirely")
			}
			if strings.TrimSpace(got.Error.Severity) == "" {
				t.Fatal("error.severity is empty")
			}
			if strings.TrimSpace(got.Error.UserMessage) == "" {
				t.Fatal("error.user_message is empty; a blank body reaches Slack and email")
			}
			if got.Type != "structured_error_notification" {
				t.Fatalf("type = %q, want the carrier type the healer also uses", got.Type)
			}
			if got.ActionURL != "/pipelines/pipe-1" {
				t.Fatalf("action_url = %q; the deep link is what makes the alert actionable", got.ActionURL)
			}
		})
	}
}

// dedup_subject is what stops the notifier's 60-minute window from swallowing every
// failure after the first. A pipeline scheduled every 15 minutes that starts failing
// would otherwise alert once and then go quiet while it kept failing.
func TestDedupSubjectScopesTheWindowToOneRun(t *testing.T) {
	first, ok := buildTerminalNotification("pipe-1", "exec-1", "failed", "boom", false, notifyFixedClock)
	if !ok {
		t.Fatal("no alert for a failed run")
	}
	second, ok := buildTerminalNotification("pipe-1", "exec-2", "failed", "boom", false, notifyFixedClock)
	if !ok {
		t.Fatal("no alert for the second failed run")
	}
	if first.Error.DedupSubject == "" {
		t.Fatal("dedup_subject is empty; the notifier would collapse every later failure " +
			"of this pipeline into the first alert for a full hour")
	}
	if first.Error.DedupSubject == second.Error.DedupSubject {
		t.Fatalf("two different executions share dedup_subject %q; the second failure "+
			"would be dropped as a duplicate", first.Error.DedupSubject)
	}
	if first.Error.DedupSubject != "exec-1" {
		t.Fatalf("dedup_subject = %q, want the execution id", first.Error.DedupSubject)
	}
}

// The postflight guard's downgrade is the "no data loss" case: the run reported
// success and the destination disagrees. It must not read as an ordinary failure.
func TestASilentDropReadsDifferentlyFromAnOrdinaryFailure(t *testing.T) {
	drop, ok := buildTerminalNotification("pipe-1", "exec-1", "failed",
		"silent_drop_detected: orders: read=1000 landed=0", true, notifyFixedClock)
	if !ok {
		t.Fatal("no alert for a silent drop")
	}
	if drop.Error.Code != codeSilentDrop {
		t.Fatalf("code = %q, want %q so the alert says rows may be missing", drop.Error.Code, codeSilentDrop)
	}

	ordinary, _ := buildTerminalNotification("pipe-1", "exec-1", "failed", "boom", false, notifyFixedClock)
	if ordinary.Error.Code == drop.Error.Code {
		t.Fatal("an ordinary failure and a silent drop resolve to the same code; the " +
			"silentDrop flag is not reaching the payload")
	}

	// The reason the guard built must survive to the body a human reads. Without it
	// the alert says a run failed and names no table.
	if !strings.Contains(drop.Message, "read=1000 landed=0") {
		t.Fatalf("message = %q, want it to carry the guard's reason", drop.Message)
	}
	if !strings.Contains(drop.Error.InternalMessage, "silent_drop_detected") {
		t.Fatalf("internal_message = %q, want the raw reason kept for support", drop.Error.InternalMessage)
	}
}

// A payload with no pipeline id is dropped by the consumer before it can resolve an
// owner, so producing one is pure topic noise.
func TestAnAlertWithNoPipelineIsNotProduced(t *testing.T) {
	if _, ok := buildTerminalNotification("   ", "exec-1", "failed", "boom", false, notifyFixedClock); ok {
		t.Fatal("built an alert with a blank pipeline id; the notifier drops it unread")
	}
}

// KAFKA_TOPIC_PREFIX is applied at every other producer's chokepoint and by the
// notifier at subscribe time. This package talks to sarama directly, so the
// qualification has to happen here — and a topic nobody produces to raises nothing
// at either end, which is how a silent alerting outage starts.
func TestTheTopicIsPrefixQualified(t *testing.T) {
	const prefix = "acme."
	t.Setenv("KAFKA_TOPIC_PREFIX", prefix)

	// Positive control: prove the prefix is actually in effect for this process
	// before reading anything into what the producer emitted. kafkaclient resolves
	// the prefix per call for exactly this reason; if that ever changes to a cached
	// value, the comparison below would pass by coincidence.
	if qualified := kafkaclient.Topic(notificationsTopic); !strings.HasPrefix(qualified, prefix) {
		t.Fatalf("kafkaclient.Topic(%q) = %q, which does not carry the prefix set for this "+
			"test — the assertion below would compare two identical unprefixed strings and "+
			"prove nothing", notificationsTopic, qualified)
	}

	p := withProducer(t)
	emitTerminalRunNotification("pipe-1", "exec-1", "failed", "boom", false)
	if len(p.sent) != 1 {
		t.Fatalf("produced %d messages, want 1", len(p.sent))
	}
	want := kafkaclient.Topic(notificationsTopic)
	if p.sent[0].Topic != want {
		t.Fatalf("topic = %q, want %q — an unqualified topic publishes where the "+
			"notifier is not subscribed, and Kafka reports nothing", p.sent[0].Topic, want)
	}
}

func TestTheProducedMessageIsKeyedByPipeline(t *testing.T) {
	p := withProducer(t)
	emitTerminalRunNotification("pipe-1", "exec-1", "failed", "boom", false)
	if len(p.sent) != 1 {
		t.Fatalf("produced %d messages, want 1", len(p.sent))
	}
	key, err := p.sent[0].Key.Encode()
	if err != nil {
		t.Fatalf("key encode: %v", err)
	}
	if string(key) != "pipe-1" {
		t.Fatalf("key = %q, want the pipeline id so one pipeline's alerts stay ordered", key)
	}
	got := decodeSent(t, p.sent[0])
	if got.PipelineID != "pipe-1" || got.Error.Code != codeRunFailed {
		t.Fatalf("decoded payload = %+v", got)
	}
}

// A broker outage must never turn a durably recorded failure into a retried
// activity: the emit happens after the terminal transaction has committed.
func TestAPublishFailureIsSwallowed(t *testing.T) {
	prev := activityCtx
	t.Cleanup(func() { activityCtx = prev })
	activityCtx = &ActivityContext{KafkaProducer: &capturingProducer{err: sarama.ErrOutOfBrokers}}

	// The assertion is that this returns at all rather than panicking or escalating.
	emitTerminalRunNotification("pipe-1", "exec-1", "failed", "boom", false)

	// ...and that a worker with no producer wired is equally harmless.
	activityCtx = &ActivityContext{}
	emitTerminalRunNotification("pipe-1", "exec-1", "failed", "boom", false)
	activityCtx = nil
	emitTerminalRunNotification("pipe-1", "exec-1", "failed", "boom", false)
}

// Lockstep with the consumer that renders these alerts. api-gateway is a separate Go
// module, so the check is made against its source on disk — the same reason the
// mongodb CDC-config test reads the debezium connector rather than a fixture.
//
// A code with no catalog entry is not fatal: resolve() falls back to a neutral
// sentence and never leaks a raw code into a headline. What it loses is the curated
// "is my data still moving" line, which is the entire reason the alert is worth
// sending.
func TestEveryEmittedCodeHasCatalogCopy(t *testing.T) {
	path := filepath.Join("..", "..", "..", "api-gateway", "internal", "notifier", "catalog.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("api-gateway catalog not present in this tree: %v", err)
	}
	src := string(raw)

	// Vacuity floor: a moved or truncated file would make every `Contains` below
	// fail for the wrong reason, or — with a negated assertion — pass trivially.
	if len(src) < 5_000 {
		t.Fatalf("%s read back as %d bytes; that is not the catalog", path, len(src))
	}

	// Positive control: a code that is definitely in the catalog must be found by
	// the same search the assertions use.
	if !strings.Contains(src, `"SCHEMA_DRIFT_DETECTED": {`) {
		t.Fatal("the search pattern does not find a code known to be in the catalog; " +
			"a miss below would prove nothing")
	}

	for _, code := range []string{codeRunFailed, codeRunStopped, codeSilentDrop} {
		if !strings.Contains(src, `"`+code+`": {`) {
			t.Errorf("this activity emits %s but api-gateway's notifier catalog has no entry "+
				"for it, so the alert renders generic copy instead of what it means", code)
		}
	}
}

// Everything above tests the payload. None of it tests that anything ever CALLS the
// emitter — deleting the one line in UpdatePipelineStatusActivity removes alerting
// entirely and leaves this whole package green (verified by mutation). That is the
// same shape as a guard whose CI job never runs: the tests pass and the feature is
// gone.
//
// A behavioural test can't reach the call site — UpdatePipelineStatusActivity needs a
// live DB and a Temporal activity context — so the wiring is pinned against the source,
// the way TestStageCompletedNeverCarriesRunTerminalStatus already does in this package.
func TestTheTerminalWriteSiteStillRaisesTheAlert(t *testing.T) {
	const path = "pipeline_status_activity.go"
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("cannot read %s, which is the site under test: %v", path, err)
	}
	src := string(raw)

	// Vacuity floor: a truncated read would make every Contains below fail for the
	// wrong reason and the ordering check pass on two -1 indexes.
	if len(src) < 10_000 {
		t.Fatalf("%s read back as %d bytes; that is not the activity", path, len(src))
	}

	// Positive controls: the anchors the assertions are expressed in terms of must
	// themselves be findable, or a miss below proves nothing.
	fnIdx := strings.Index(src, "func UpdatePipelineStatusActivity(")
	if fnIdx < 0 {
		t.Fatal("cannot find UpdatePipelineStatusActivity; this guard is anchored to a " +
			"function that no longer exists under that name and is now inert")
	}
	commitIdx := strings.Index(src[fnIdx:], "tx.Commit()")
	if commitIdx < 0 {
		t.Fatal("cannot find the terminal commit; the ordering assertion below would be vacuous")
	}
	commitIdx += fnIdx

	emitIdx := strings.Index(src, "emitTerminalRunNotification(")
	if emitIdx < 0 {
		t.Fatal("UpdatePipelineStatusActivity no longer calls emitTerminalRunNotification: " +
			"every run can now fail with no Slack or email alert, and no other test notices")
	}

	// The emit has to sit inside this function. Anywhere else and it no longer sees
	// every terminal outcome, which is the entire argument for this site.
	fnEnd := strings.Index(src[fnIdx+1:], "\nfunc ")
	if fnEnd < 0 {
		t.Fatal("cannot delimit the function body")
	}
	fnEnd += fnIdx + 1
	if emitIdx > fnEnd {
		t.Fatal("the emit moved out of UpdatePipelineStatusActivity; outside this function " +
			"nothing sees all three executor dispatch paths")
	}

	// After the commit, not inside the transaction. Before it, a failed commit would
	// alert about a failure the DB never recorded, and a slow broker would hold a
	// write transaction open.
	if emitIdx < commitIdx {
		t.Fatal("the alert is raised before the terminal transaction commits; a rolled-back " +
			"transaction would still have alerted")
	}

	// The tracked flag, not a literal. `false` here compiles, passes every payload test
	// above, and silently turns every silent-drop alert into an ordinary failure alert
	// — losing exactly the "rows are missing" wording the no-data-loss case needs.
	call := src[emitIdx:]
	if end := strings.Index(call, ")"); end >= 0 {
		call = call[:end]
	}
	if !strings.Contains(call, "silentDrop") {
		t.Fatalf("the emit call %q does not pass the tracked silentDrop flag", call)
	}
	if !strings.Contains(src, "silentDrop = true") {
		t.Fatal("nothing sets silentDrop = true; the postflight guard's downgrade would " +
			"alert as an ordinary failure and never say rows may be missing")
	}
}
