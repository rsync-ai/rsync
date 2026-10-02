package cdcsnapshot

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// creditSlack absorbs clock skew between the Kafka Connect worker (which stamps
// the envelope ts_ms) and this process (which stamps sent_at).
const creditSlack = 5 * time.Second

// Timing holds the dispatcher's clocks. Defaults: DefaultTiming.
type Timing struct {
	// ReadyStable is how long the connector must stay RUNNING with every table in
	// its task configs before the signal is sent: a config PUT restarts the tasks,
	// and the new task configs can be listed a moment before the old task stops.
	ReadyStable time.Duration
	// FailAfter: a connector that is missing, FAILED, or never captures the
	// tables this long after the request fails it.
	FailAfter time.Duration
	// SendTimeout: sent, no snapshot row seen → send again.
	SendTimeout time.Duration
	// StallTimeout: a started BLOCKING snapshot with no new row → send again (a
	// restart mid-snapshot drops a blocking snapshot).
	StallTimeout time.Duration
	// IdleComplete: a started INCREMENTAL snapshot with no new row is finished.
	// Debezium marks no end for it.
	IdleComplete time.Duration
	// UntrackedTimeout: with the stats consumer off nothing can confirm rows, so
	// a sent request is closed as unconfirmed after this long.
	UntrackedTimeout time.Duration
	MaxAttempts      int
	// InitialStall: a started BLOCKING initial load with no new row is closed as
	// unconfirmed. Nothing can send it again, so it only waits longer than a
	// Re-snapshot does before saying so.
	InitialStall time.Duration
}

var DefaultTiming = Timing{
	ReadyStable:      10 * time.Second,
	FailAfter:        10 * time.Minute,
	SendTimeout:      5 * time.Minute,
	StallTimeout:     10 * time.Minute,
	IdleComplete:     2 * time.Minute,
	UntrackedTimeout: 10 * time.Minute,
	MaxAttempts:      3,
	InitialStall:     time.Hour,
}

// UntrackedMessage is last_error for a request nothing could confirm.
const UntrackedMessage = "the snapshot signal was sent, but snapshot progress is not tracked on this deployment (ENABLE_CDC_TABLE_STATS is off)"

// tableMatches compares a data-collection with a stats table name: equal
// ignoring case, or one is the other's dot-suffix ("users" vs "public.users").
func tableMatches(a, b string) bool {
	a, b = strings.ToLower(strings.TrimSpace(a)), strings.ToLower(strings.TrimSpace(b))
	if a == "" || b == "" {
		return false
	}
	return a == b || strings.HasSuffix(a, "."+b) || strings.HasSuffix(b, "."+a)
}

func (r *Request) match(table string) (string, bool) {
	for _, t := range r.Tables {
		if tableMatches(t, table) {
			return t, true
		}
	}
	return "", false
}

func containsFold(list []string, v string) bool {
	for _, x := range list {
		if strings.EqualFold(x, v) {
			return true
		}
	}
	return false
}

// ApplyObservations folds one flush window's snapshot observations into r and
// reports whether r changed. Only rows the connector read after r was first
// sent count, so the tail of an older snapshot of the same table does not
// start (or finish) this one.
//
// An initial load takes every table's rows (its table list is the connector's
// include list, or what was seen, and a regex list or a table added since
// names more) and ends only on Debezium's "last": every table SEEN being done
// says nothing about a table whose rows have not arrived yet.
func ApplyObservations(r *Request, obs []Observation, now time.Time) bool {
	if (r.Status != StatusSent && r.Status != StatusStarted) || r.SentAt == nil {
		return false
	}
	initial := r.Source == SourceInitial
	credit := r.SentAt.Add(-creditSlack)
	changed, allDone := false, false
	for _, o := range obs {
		if o.LastSeen.Before(credit) {
			continue
		}
		if o.Rows <= 0 && !o.TableDone && !o.AllDone {
			continue
		}
		name, ok := r.match(o.Table)
		if !ok && initial && strings.TrimSpace(o.Table) != "" {
			name, ok = o.Table, true
			r.Tables = append(r.Tables, name)
		}
		if !ok {
			continue
		}
		t := now
		if r.Status == StatusSent {
			r.Status = StatusStarted
			r.StartedAt = &t
		}
		r.LastProgressAt = &t
		changed = true
		if o.TableDone && !containsFold(r.CompletedTables, name) {
			r.CompletedTables = append(r.CompletedTables, name)
		}
		if o.AllDone {
			allDone = true
		}
	}
	if changed && (allDone || (!initial && r.allTablesDone())) {
		t := now
		r.Status = StatusCompleted
		r.CompletedAt = &t
	}
	return changed
}

func (r *Request) allTablesDone() bool {
	if len(r.Tables) == 0 {
		return false
	}
	for _, t := range r.Tables {
		if !containsFold(r.CompletedTables, t) {
			return false
		}
	}
	return true
}

// Action is the watchdog's decision for an in-flight request.
type Action int

const (
	ActNone Action = iota
	ActResend
	ActComplete
	ActUnconfirmed
	// ActFail closes a request that is known not to have finished: a blocking
	// snapshot that started, then stopped sending rows on every attempt.
	ActFail
)

// ClampToStart returns r with its send and progress clocks moved up to start
// (the dispatcher's first tick) where they are older. While the orchestrator was
// down nothing recorded snapshot rows, so that time is not evidence of a stalled
// or ignored snapshot: without this, a blocking snapshot still running across a
// restart was sent again, re-arming its object-storage clean markers mid-load.
func ClampToStart(r Request, start time.Time) Request {
	if start.IsZero() {
		return r
	}
	for _, p := range []**time.Time{&r.SentAt, &r.LastSentAt, &r.LastProgressAt} {
		if *p != nil && (*p).Before(start) {
			t := start
			*p = &t
		}
	}
	return r
}

// InitialStalledMessage is last_error for a blocking initial load that went
// quiet before Debezium marked it finished.
const InitialStalledMessage = "the full load stopped sending rows before Debezium marked it finished; " +
	"check the CDC connector, then Re-snapshot the tables not marked done"

// Watch decides what to do with a sent or started request. tracked reports
// whether the CDC stats consumer runs (and so can confirm rows at all).
func Watch(r Request, now time.Time, tracked bool, t Timing) (Action, string) {
	if r.Source == SourceInitial {
		return watchInitial(r, now, t)
	}
	switch r.Status {
	case StatusSent:
		base := r.LastSentAt
		if base == nil {
			base = r.SentAt
		}
		if base == nil {
			return ActNone, ""
		}
		waited := now.Sub(*base)
		if !tracked {
			if waited >= t.UntrackedTimeout {
				return ActUnconfirmed, UntrackedMessage
			}
			return ActNone, ""
		}
		if waited < t.SendTimeout {
			return ActNone, ""
		}
		if r.Attempts < t.MaxAttempts {
			return ActResend, ""
		}
		return ActUnconfirmed, fmt.Sprintf("the snapshot signal was sent %d times but no snapshot rows arrived: "+
			"the table may be empty, or the connector ignored the signal", r.Attempts)
	case StatusStarted:
		if !tracked {
			return ActUnconfirmed, UntrackedMessage
		}
		if r.LastProgressAt == nil {
			return ActNone, ""
		}
		idle := now.Sub(*r.LastProgressAt)
		if r.Mode == "incremental" {
			if idle >= t.IdleComplete {
				return ActComplete, ""
			}
			return ActNone, ""
		}
		if idle < t.StallTimeout {
			return ActNone, ""
		}
		if r.Attempts < t.MaxAttempts {
			return ActResend, ""
		}
		// Rows arrived, then stopped short of Debezium's end marker on every
		// attempt: the snapshot aborted (the connector logs "Snapshot was not
		// completed successfully" and goes back to streaming). Each attempt
		// emptied the object-storage folder first, so the destination holds only
		// the last attempt's partial rows — that is a failure, not "unconfirmed".
		return ActFail, fmt.Sprintf("the blocking snapshot stopped before it finished on all %d attempts, "+
			"so the destination may be missing rows for the tables not marked done; "+
			"check the CDC connector's log, then re-run Re-snapshot for those tables", r.Attempts)
	}
	return ActNone, ""
}

// watchInitial never re-sends: nothing sent an initial load. Rows recorded it
// (so tracking was on), or the hybrid executor did, which runs the load itself
// and closes the row — it has no progress clock, and waits for the executor.
func watchInitial(r Request, now time.Time, t Timing) (Action, string) {
	if r.Status != StatusStarted || r.LastProgressAt == nil {
		return ActNone, ""
	}
	idle := now.Sub(*r.LastProgressAt)
	if r.Mode == "incremental" {
		if idle >= t.IdleComplete {
			return ActComplete, ""
		}
		return ActNone, ""
	}
	if idle >= t.InitialStall {
		return ActUnconfirmed, InitialStalledMessage
	}
	return ActNone, ""
}

// PlainIncludeTables returns the tables an include list names, unescaped
// ("public\.users" → "public.users"), or nil when any entry is a pattern:
// "public\..*" names no table, and a load of it would read as one table.
func PlainIncludeTables(entries []string) []string {
	if len(entries) == 0 {
		return nil
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		e = strings.TrimSpace(strings.ReplaceAll(e, `\`, ""))
		if e == "" || strings.ContainsAny(e, "*+?[](){}|^$") {
			return nil
		}
		if !containsFold(out, e) {
			out = append(out, e)
		}
	}
	return out
}

// ConnectorState is what Kafka Connect reports for a connector.
type ConnectorState struct {
	Found      bool
	State      string
	TaskStates []string
	// TaskIncludes holds each task's include-list entries; a nil entry means the
	// task config has no include list (it captures everything).
	TaskIncludes [][]string
}

// Readiness is whether a queued request can be sent now.
type Readiness int

const (
	// NotReady: the connector is (re)starting or its tasks do not capture every
	// table yet — wait.
	NotReady Readiness = iota
	// Ready: send.
	Ready
	// Paused: the pipeline is paused — wait without a deadline.
	Paused
	// Broken: missing or FAILED — wait up to Timing.FailAfter, then fail.
	Broken
)

// Assess reports whether a snapshot of tables can be sent to this connector.
func Assess(cs ConnectorState, tables []string) (Readiness, string) {
	if !cs.Found {
		return Broken, "the CDC connector does not exist"
	}
	state := strings.ToUpper(strings.TrimSpace(cs.State))
	if state == "FAILED" {
		return Broken, "the CDC connector is FAILED"
	}
	for _, ts := range cs.TaskStates {
		if strings.EqualFold(ts, "FAILED") {
			return Broken, "a CDC connector task is FAILED"
		}
	}
	if state == "PAUSED" || state == "STOPPED" {
		return Paused, "the CDC connector is " + state
	}
	for _, ts := range cs.TaskStates {
		if strings.EqualFold(ts, "PAUSED") {
			return Paused, "a CDC connector task is PAUSED"
		}
	}
	if state != "RUNNING" || len(cs.TaskStates) == 0 {
		return NotReady, "the CDC connector is starting"
	}
	for _, ts := range cs.TaskStates {
		if !strings.EqualFold(ts, "RUNNING") {
			return NotReady, "a CDC connector task is starting"
		}
	}
	if len(cs.TaskIncludes) == 0 {
		return NotReady, "the CDC connector's task configs are not available yet"
	}
	for _, t := range tables {
		for _, inc := range cs.TaskIncludes {
			if inc != nil && !includeListCaptures(inc, t) {
				return NotReady, fmt.Sprintf("the running CDC connector does not capture %s yet", t)
			}
		}
	}
	return Ready, ""
}

// includeKeys are the task-config keys that name what a Debezium task captures,
// newest spelling first.
var includeKeys = []string{"table.include.list", "collection.include.list", "table.whitelist", "collection.whitelist"}

// ParseIncludeList returns a task config's include-list entries, or nil when it
// has none (the task captures every table).
func ParseIncludeList(cfg map[string]interface{}) []string {
	for _, k := range includeKeys {
		v, ok := cfg[k]
		if !ok || v == nil {
			continue
		}
		raw := strings.TrimSpace(fmt.Sprint(v))
		if raw == "" {
			continue
		}
		out := []string{}
		for _, e := range strings.Split(raw, ",") {
			if e = strings.TrimSpace(e); e != "" {
				out = append(out, e)
			}
		}
		return out
	}
	return nil
}

// includeListCaptures matches a data-collection against include-list entries
// the way Debezium does: each entry is a regular expression over the whole
// identifier, case-insensitive. rsync.ai writes plain names with escaped dots
// ("public\.users"), which the fast path compares directly.
func includeListCaptures(entries []string, table string) bool {
	for _, e := range entries {
		if strings.EqualFold(strings.ReplaceAll(e, `\`, ""), table) {
			return true
		}
		re, err := regexp.Compile(`(?i)^(?:` + e + `)$`)
		if err == nil && re.MatchString(table) {
			return true
		}
	}
	return false
}

// IncludeListCaptures reports whether include-list entries capture table, the
// way Debezium matches them. nil entries (no include list) capture everything.
func IncludeListCaptures(entries []string, table string) bool {
	if entries == nil {
		return true
	}
	return includeListCaptures(entries, table)
}

// ReloadTopics are the Kafka data topics of tables, the key the sink's
// object-storage clean markers use: topic.prefix + "." + data-collection.
func ReloadTopics(topicPrefix string, tables []string) []string {
	topicPrefix = strings.TrimSpace(topicPrefix)
	if topicPrefix == "" {
		return nil
	}
	out := make([]string, 0, len(tables))
	for _, t := range tables {
		if t = strings.TrimSpace(strings.ReplaceAll(t, `\`, "")); t != "" {
			out = append(out, topicPrefix+"."+t)
		}
	}
	return out
}
