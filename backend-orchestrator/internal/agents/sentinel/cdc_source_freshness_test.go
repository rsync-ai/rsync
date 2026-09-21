package sentinel

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// The bug this file guards (KI-CDC-MONGO-RESUME-TOKEN-SILENT-STALL): a MongoDB source
// connector whose resume token had aged out of the oplog retried the same permanent
// error roughly ten times a minute for three days, never left RUNNING, and so reported a
// fully green /status the entire time. Nothing in rsync noticed, because everything that
// looks for trouble was looking at the connector's STATE.
//
// The freshness alarm looks at the connector's committed source POSITION instead, which
// is the one thing a looping connector cannot fake. Two properties make that sound, and
// both are easy to break by accident, so both are pinned here:
//
//   - It must not alarm on a merely quiet source. That is what the heartbeat gate is
//     for, and why the first reading only ever starts the clock.
//   - It must alarm on a frozen one, and keep alarming — a watchdog that fires once and
//     then forgets is indistinguishable from one that never fired.

// freshTick is one sentinel poll as the freshness alarm sees it.
type freshTick struct {
	at          time.Duration // since the first tick
	fingerprint string
}

// runFreshnessTicks feeds ticks through observeSourceFreshness the way
// checkSourceFreshness does, and returns each tick's alarm verdict.
func runFreshnessTicks(t *testing.T, ticks []freshTick) []bool {
	t.Helper()
	return runFreshnessTicksFor(t, "conn-1", ticks)
}

func runFreshnessTicksFor(t *testing.T, connector string, ticks []freshTick) []bool {
	t.Helper()
	t.Setenv("CDC_SOURCE_FRESHNESS_STALL_AFTER", "20m")
	s := &CDCSentinel{} // no map on purpose: a bare literal must not panic
	got := make([]bool, len(ticks))
	for i, tk := range ticks {
		got[i], _ = s.observeSourceFreshness(connector, tk.fingerprint, freshnessStart.Add(tk.at))
	}
	return got
}

var freshnessStart = time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)

func assertFreshnessAlarms(t *testing.T, got, want []bool) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d verdicts, want %d (got %v, want %v)", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("tick %d: alarm=%v, want %v (all: got %v, want %v)", i, got[i], want[i], got, want)
		}
	}
}

// A heartbeating connector advances its position on a timer whether or not the source is
// writing, so a position that keeps changing is proof of life no matter how quiet the
// database is.
func TestSourceFreshness_AdvancingPositionNeverAlarms(t *testing.T) {
	var ticks []freshTick
	for m := 0; m <= 90; m += 5 {
		ticks = append(ticks, freshTick{
			at:          time.Duration(m) * time.Minute,
			fingerprint: "token-" + strings.Repeat("a", m),
		})
	}
	got := runFreshnessTicks(t, ticks)
	assertFreshnessAlarms(t, got, make([]bool, len(ticks)))
}

// The live shape of the bug: Connect says RUNNING, the position never moves. The alarm
// must fire once the window has fully passed and must keep firing — this connector was
// stuck for three days, and an alarm that cleared itself after one tick would have been
// as useless as no alarm at all.
func TestSourceFreshness_FrozenPositionAlarmsAfterTheWindowAndStays(t *testing.T) {
	got := runFreshnessTicks(t, []freshTick{
		{at: 0, fingerprint: "tok-A"},                  // first reading: starts the clock
		{at: 5 * time.Minute, fingerprint: "tok-A"},    // 5m frozen
		{at: 19 * time.Minute, fingerprint: "tok-A"},   // 19m frozen: not yet
		{at: 20 * time.Minute, fingerprint: "tok-A"},   // 20m frozen: alarm (>= is deliberate)
		{at: 3 * 24 * time.Hour, fingerprint: "tok-A"}, // three days later: still alarming
	})
	assertFreshnessAlarms(t, got, []bool{false, false, false, true, true})
}

// Recovery has to be believed the moment the position moves, and a later freeze has to
// serve a full fresh window before it alarms again.
func TestSourceFreshness_MovingAgainClearsAndRestartsTheClock(t *testing.T) {
	got := runFreshnessTicks(t, []freshTick{
		{at: 0, fingerprint: "tok-A"},
		{at: 25 * time.Minute, fingerprint: "tok-A"}, // frozen: alarm
		{at: 26 * time.Minute, fingerprint: "tok-B"}, // moved: clear
		{at: 45 * time.Minute, fingerprint: "tok-B"}, // 19m frozen: not yet
		{at: 46 * time.Minute, fingerprint: "tok-B"}, // 20m frozen: alarm
	})
	assertFreshnessAlarms(t, got, []bool{false, true, false, false, true})
}

// The first reading after an orchestrator restart says nothing about how long the
// position has been frozen — the connector may have been streaming happily a second ago.
// Treating it as the start of a stall would alarm on every restart.
func TestSourceFreshness_FirstReadingOnlyStartsTheClock(t *testing.T) {
	// A single tick a week after the epoch: the elapsed time is enormous, and it still
	// must not alarm, because nothing was observed before it.
	got := runFreshnessTicks(t, []freshTick{{at: 7 * 24 * time.Hour, fingerprint: "tok-A"}})
	assertFreshnessAlarms(t, got, []bool{false})
}

// decideSourceFreshnessAlarm's `seen` flag, exercised directly.
//
// Through observeSourceFreshness the flag looks redundant — a first reading has a zero
// prev.fingerprint, so the "position changed" branch resets the clock anyway. It stops
// being redundant the moment a caller passes an empty fingerprint, where without `seen`
// the zero movingAt would be read as "frozen since the zero time" and alarm instantly.
// sourceOffsetFingerprint never returns an empty string today; this pins the function's
// behaviour so that it stays correct if one ever does.
func TestDecideSourceFreshnessAlarm_UnseenConnectorNeverAlarms(t *testing.T) {
	var zero sourceFreshnessState
	next, alarm := decideSourceFreshnessAlarm(zero, false, "", freshnessStart, 20*time.Minute)
	if alarm {
		t.Fatal("a connector observed for the first time alarmed")
	}
	if !next.movingAt.Equal(freshnessStart) {
		t.Fatalf("movingAt = %s, want the current tick %s — the clock must start now, not at the zero time",
			next.movingAt, freshnessStart)
	}
}

// Two connectors are tracked independently: one stuck pipeline must not implicate a
// healthy one, and a healthy one must not mask a stuck one.
func TestSourceFreshness_ConnectorsAreIndependent(t *testing.T) {
	t.Setenv("CDC_SOURCE_FRESHNESS_STALL_AFTER", "20m")
	s := &CDCSentinel{}

	type step struct {
		conn      string
		at        time.Duration
		fp        string
		wantAlarm bool
	}
	for i, st := range []step{
		{"stuck", 0, "frozen", false},
		{"healthy", 0, "moving-0", false},
		{"stuck", 25 * time.Minute, "frozen", true},
		{"healthy", 25 * time.Minute, "moving-1", false},
		{"stuck", 30 * time.Minute, "frozen", true},
		{"healthy", 30 * time.Minute, "moving-2", false},
	} {
		alarm, _ := s.observeSourceFreshness(st.conn, st.fp, freshnessStart.Add(st.at))
		if alarm != st.wantAlarm {
			t.Fatalf("step %d (%s @ %s): alarm=%v, want %v", i, st.conn, st.at, alarm, st.wantAlarm)
		}
	}
}

// The reported duration is what lands in the issue description and metadata, so it has to
// be the age of the freeze, not the age of the connector.
func TestSourceFreshness_ReportsHowLongThePositionHasBeenFrozen(t *testing.T) {
	t.Setenv("CDC_SOURCE_FRESHNESS_STALL_AFTER", "20m")
	s := &CDCSentinel{}

	s.observeSourceFreshness("c", "tok-A", freshnessStart)
	s.observeSourceFreshness("c", "tok-B", freshnessStart.Add(time.Hour)) // moved at T+1h
	_, frozenFor := s.observeSourceFreshness("c", "tok-B", freshnessStart.Add(90*time.Minute))

	if frozenFor != 30*time.Minute {
		t.Fatalf("frozen for %s, want 30m — the clock must run from the last MOVE, not from the first sighting", frozenFor)
	}
}

// A connector deleted and recreated under the same name (which is what "delete the
// pipeline and create it again" does) must not inherit the dead one's frozen-since
// timestamp and alarm immediately.
func TestSourceFreshness_ForgettingAConnectorResetsItsClock(t *testing.T) {
	t.Setenv("CDC_SOURCE_FRESHNESS_STALL_AFTER", "20m")
	s := &CDCSentinel{}

	s.observeSourceFreshness("gone", "tok-A", freshnessStart)
	s.observeSourceFreshness("kept", "tok-A", freshnessStart)

	s.forgetSourceFreshness(map[string]bool{"kept": true})

	if _, ok := s.sourceFreshness["gone"]; ok {
		t.Fatal("forgetSourceFreshness kept a connector that is no longer running")
	}
	if _, ok := s.sourceFreshness["kept"]; !ok {
		t.Fatal("forgetSourceFreshness dropped a connector that is still running")
	}

	// The recreated connector reports the same position it had before (a fresh snapshot
	// can legitimately produce one), 25 minutes later. Because its history was dropped,
	// this is a first reading and must only start the clock.
	alarm, _ := s.observeSourceFreshness("gone", "tok-A", freshnessStart.Add(25*time.Minute))
	if alarm {
		t.Fatal("a recreated connector alarmed on its first reading — it inherited the deleted one's clock")
	}
}

// forgetSourceFreshness runs on a Sentinel that has never observed anything (the startup
// window, and every unit test that does not touch freshness).
func TestSourceFreshness_ForgetOnAnEmptySentinelDoesNotPanic(t *testing.T) {
	s := &CDCSentinel{}
	s.forgetSourceFreshness(map[string]bool{"anything": true})
}

func TestSourceOffsetFingerprint(t *testing.T) {
	decode := func(t *testing.T, raw string) map[string]interface{} {
		t.Helper()
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatalf("fixture is not valid JSON: %v", err)
		}
		return m
	}

	// A connector that has committed nothing yet is starting up, not stalled. Every one
	// of these must be a skip, because alarming on them would fire on every pipeline
	// launch.
	for name, raw := range map[string]string{
		"missing offsets key": `{}`,
		"null offsets":        `{"offsets":null}`,
		"empty offsets":       `{"offsets":[]}`,
		"offsets not a list":  `{"offsets":{"resume_token":"x"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := sourceOffsetFingerprint(decode(t, raw)); ok {
				t.Fatalf("%s produced a usable fingerprint; want a skip", name)
			}
		})
	}

	t.Run("nil payload", func(t *testing.T) {
		if _, ok := sourceOffsetFingerprint(nil); ok {
			t.Fatal("a nil payload produced a usable fingerprint; want a skip")
		}
	})

	// The real KIP-875 shape from the stalled pipeline on the demo VM.
	live := `{"offsets":[{"partition":{"server_id":"cdc-67b8ac8b"},` +
		`"offset":{"resume_token":"826D0A1F000000012B0429","sec":-1,"ord":-1}}]}`

	t.Run("a committed position fingerprints", func(t *testing.T) {
		fp, ok := sourceOffsetFingerprint(decode(t, live))
		if !ok {
			t.Fatal("a committed position did not produce a fingerprint")
		}
		if !strings.Contains(fp, "826D0A1F000000012B0429") {
			t.Fatalf("fingerprint %q does not carry the committed position", fp)
		}
	})

	// The alarm compares fingerprints across ticks, so the encoding must not depend on
	// map iteration order — otherwise a frozen connector would look like it is moving on
	// roughly every tick and the alarm would never fire. encoding/json sorts map keys;
	// this pins that we still rely on it.
	t.Run("stable across decodes", func(t *testing.T) {
		reordered := `{"offsets":[{"offset":{"ord":-1,"sec":-1,"resume_token":"826D0A1F000000012B0429"},` +
			`"partition":{"server_id":"cdc-67b8ac8b"}}]}`
		a, okA := sourceOffsetFingerprint(decode(t, live))
		b, okB := sourceOffsetFingerprint(decode(t, reordered))
		if !okA || !okB {
			t.Fatal("expected both payloads to fingerprint")
		}
		if a != b {
			t.Fatalf("the same position fingerprinted differently:\n  %s\n  %s", a, b)
		}
	})

	// And it must still change when the position actually changes.
	t.Run("changes when the position moves", func(t *testing.T) {
		moved := strings.Replace(live, "826D0A1F000000012B0429", "826D0A20000000012B0429", 1)
		a, _ := sourceOffsetFingerprint(decode(t, live))
		b, _ := sourceOffsetFingerprint(decode(t, moved))
		if a == b {
			t.Fatal("two different committed positions produced the same fingerprint")
		}
	})
}

// The heartbeat gate is what separates "idle" from "dead". If it ever returns true for a
// connector without heartbeats, every quiet source in every deployment starts alarming.
func TestSourceHeartbeatEnabled(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config map[string]interface{}
		want   bool
	}{
		{"nil config", nil, false},
		{"key absent", map[string]interface{}{"connector.class": "io.debezium.connector.mongodb.MongoDbConnector"}, false},
		{"empty string", map[string]interface{}{"heartbeat.interval.ms": ""}, false},
		{"zero disables heartbeats", map[string]interface{}{"heartbeat.interval.ms": "0"}, false},
		{"negative", map[string]interface{}{"heartbeat.interval.ms": "-1"}, false},
		{"not a number", map[string]interface{}{"heartbeat.interval.ms": "5m"}, false},
		{"nil value", map[string]interface{}{"heartbeat.interval.ms": nil}, false},
		// Connect returns config values as strings; a hand-built config or a test may
		// carry a number. Both mean the same thing.
		{"the shipped default, as a string", map[string]interface{}{"heartbeat.interval.ms": "300000"}, true},
		{"padded", map[string]interface{}{"heartbeat.interval.ms": " 300000 "}, true},
		{"as an int", map[string]interface{}{"heartbeat.interval.ms": 300000}, true},
		// JSON decodes numbers as float64. fmt.Sprint renders this one as "300000"
		// (no exponent at this magnitude), so it still reads as enabled.
		{"as a JSON number", map[string]interface{}{"heartbeat.interval.ms": float64(300000)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sourceHeartbeatEnabled(tc.config); got != tc.want {
				t.Fatalf("sourceHeartbeatEnabled(%v) = %v, want %v", tc.config, got, tc.want)
			}
		})
	}
}

// The issue id must be its own class. Colliding with cdc-connector-down-* would let the
// connector-down resolver clear a stall that Connect never admitted to, which is exactly
// the blindness this alarm exists to fix.
func TestSourceStalledIssueIDIsItsOwnClass(t *testing.T) {
	id := sourceStalledIssueID("67b8ac8b")
	if !strings.Contains(id, "67b8ac8b") {
		t.Fatalf("issue id %q does not identify the pipeline", id)
	}
	if id == connectorIssueID("67b8ac8b") {
		t.Fatal("the stall issue id collides with the connector-down issue id")
	}
	if a, b := sourceStalledIssueID("aaa"), sourceStalledIssueID("bbb"); a == b {
		t.Fatal("two pipelines share one stall issue id")
	}
}

// diagnosableErrorText is the §3A half of the fix: the Healer builds its diagnose.Signal
// from the issue DESCRIPTION and never reads metadata (heal/issue_sweep.go), so an error
// that lives only in metadata is an error rsync cannot classify. These tests pin the two
// things that makes it useful — the cause survives, and nothing secret does.
func TestDiagnosableErrorText(t *testing.T) {
	t.Run("keeps the first line and every cause, drops the frames", func(t *testing.T) {
		trace := strings.Join([]string{
			"org.apache.kafka.connect.errors.ConnectException: An exception occurred in the change event producer",
			"\tat io.debezium.pipeline.ErrorHandler.setProducerThrowable(ErrorHandler.java:52)",
			"\tat io.debezium.connector.mongodb.MongoDbStreamingChangeEventSource.lambda$0(MongoDbStreamingChangeEventSource.java:171)",
			"Caused by: com.mongodb.MongoCommandException: Command failed with error 286 (ChangeStreamHistoryLost): 'Resume of change stream was not possible, as the resume point may no longer be in the oplog.'",
			"\tat com.mongodb.internal.connection.ProtocolHelper.getCommandFailureException(ProtocolHelper.java:205)",
			"Caused by: java.lang.IllegalStateException: change stream was closed",
			"\tat java.base/java.lang.Thread.run(Thread.java:840)",
		}, "\n")

		got := diagnosableErrorText(trace, maxIssueErrorText)

		for _, want := range []string{
			"ConnectException",
			"ChangeStreamHistoryLost",
			"change stream was closed",
		} {
			if !strings.Contains(got, want) {
				t.Fatalf("the diagnosable text lost %q:\n%s", want, got)
			}
		}

		// Known and accepted: llmscrub redacts single-quoted literals, because in a
		// Postgres or MySQL error that is where a row value lives. MongoDB puts its
		// human-readable message there too ('Resume of change stream was not possible…'),
		// so that sentence does not survive. It is not worth widening the scrubber for:
		// the unquoted error NAME is what diagnose.go matches on, and it does survive.
		// This assertion exists so that a future change to llmscrub that starts eating
		// the error name is caught here rather than in production.
		if strings.Contains(got, "resume point may no longer be in the oplog") {
			t.Log("note: llmscrub no longer redacts quoted literals; the assertion above can be tightened")
		}
		if strings.Contains(got, "ErrorHandler.java") || strings.Contains(got, "Thread.java") {
			t.Fatalf("stack frames survived and will crowd out the cause:\n%s", got)
		}
		if n := strings.Count(got, " | "); n != 2 {
			t.Fatalf("expected 3 joined lines (1 header + 2 causes), got %d separators:\n%s", n, got)
		}
	})

	// This is the whole point: what lands in the description must match the rule that
	// already knows what to do about it. Without this the description says only that a
	// connector failed, which matches nothing.
	t.Run("the result is what the re-snapshot rule matches on", func(t *testing.T) {
		got := strings.ToLower(diagnosableErrorText(
			"Caused by: com.mongodb.MongoCommandException: Command failed with error 286 (ChangeStreamHistoryLost)",
			maxIssueErrorText))
		if !strings.Contains(got, "changestreamhistorylost") {
			t.Fatalf("diagnose.go's re-snapshot rule will not match this text:\n%s", got)
		}
	})

	t.Run("credentials never survive", func(t *testing.T) {
		got := diagnosableErrorText(
			"ConnectException: cannot reach mongodb://rsync_cdc:hunter2@cluster0.abcd.mongodb.net:27017/?replicaSet=rs0",
			maxIssueErrorText)
		if strings.Contains(got, "hunter2") {
			t.Fatalf("a password reached an issue description that the chat diagnoser reads back:\n%s", got)
		}
		if !strings.Contains(got, "[redacted]@") {
			t.Fatalf("expected the connection URI's userinfo to be masked:\n%s", got)
		}
	})

	t.Run("bounded", func(t *testing.T) {
		// Prose, not a long alphanumeric run: llmscrub masks the latter as suspected
		// base64 and the input would never reach the cap, making this test vacuous.
		got := diagnosableErrorText(
			"ConnectException: "+strings.Repeat("the connector could not reach the source. ", 60), 100)
		if n := len([]rune(got)); n > 101 { // 100 runes + the ellipsis ScrubMax appends
			t.Fatalf("length %d exceeds the cap", n)
		}
		if !strings.HasSuffix(got, "…") {
			t.Fatalf("a truncated description should say it was truncated: %q", got)
		}
	})

	t.Run("empty and whitespace-only traces add nothing", func(t *testing.T) {
		for _, in := range []string{"", "   ", "\n\n\t\n"} {
			if got := diagnosableErrorText(in, maxIssueErrorText); got != "" {
				t.Fatalf("diagnosableErrorText(%q) = %q, want \"\" — a trailing \": \" in the description helps nobody", in, got)
			}
		}
	})
}

// getDebeziumStatus stores the harvested trace in a map; a connector state that carries
// no trace (the live shape of this bug — Connect exposed none) must degrade to "no extra
// text", never to a panic or a stringified map.
func TestTraceFromConnectorState(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state interface{}
		want  string
	}{
		{"nil", nil, ""},
		{"not a map", "RUNNING", ""},
		{"map without a trace", map[string]interface{}{"state": "RUNNING"}, ""},
		{"trace is not a string", map[string]interface{}{"trace": 42}, ""},
		{"trace present", map[string]interface{}{"state": "RUNNING", "trace": "boom"}, "boom"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := traceFromConnectorState(tc.state); got != tc.want {
				t.Fatalf("traceFromConnectorState(%v) = %q, want %q", tc.state, got, tc.want)
			}
		})
	}
}

// The alarm's description must NOT contain the phrases diagnose.go's re-snapshot rule
// matches. A frozen position has several possible causes (unreachable source, a permanent
// error retried forever, a lost resume token); naming one of them in the boilerplate
// would make the diagnoser confidently pick that one every single time, including for a
// network partition. Only the connector's own error text, appended after it, may carry
// those phrases.
func TestSourceStalledDescriptionDoesNotPreJudgeTheCause(t *testing.T) {
	// Lifted from pkg/diagnose/diagnose.go's re-snapshot rule.
	triggers := []string{
		"resume token",
		"resume of change stream was not possible",
		"resume point may no longer be in the oplog",
		"changestreamhistorylost",
		"change stream history lost",
		"invalidresumetoken",
	}
	raw, err := os.ReadFile("cdc_source_freshness.go")
	if err != nil {
		t.Fatalf("could not read the source under guard: %v", err)
	}
	src := string(raw)
	// The description is the one fmt.Sprintf in checkSourceFreshness.
	start := strings.Index(src, "description := fmt.Sprintf(")
	if start < 0 {
		t.Fatal("could not find the stall description — this guard is vacuous, fix the anchor")
	}
	end := strings.Index(src[start:], "connectorName, stalledFor")
	if end < 0 {
		t.Fatal("could not find the end of the stall description — this guard is vacuous, fix the anchor")
	}
	description := strings.ToLower(src[start : start+end])
	for _, trig := range triggers {
		if strings.Contains(description, trig) {
			t.Fatalf("the stall description contains %q, which makes diagnose.go's re-snapshot rule "+
				"fire for every frozen position — including an unreachable source", trig)
		}
	}
	// Non-vacuity: the anchor really did capture the sentence.
	if !strings.Contains(description, "has not advanced its position") {
		t.Fatalf("the captured text is not the stall description:\n%s", description)
	}
}
