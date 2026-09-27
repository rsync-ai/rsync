package cdcsnapshot

import (
	"encoding/json"
	"testing"
	"time"
)

func tp(t time.Time) *time.Time { return &t }

var t0 = time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)

func sentRequest(mode string, tables ...string) Request {
	return Request{
		ID: "r1", PipelineID: "p1", Mode: mode, Tables: tables, Status: StatusSent,
		Attempts: 1, SentAt: tp(t0), LastSentAt: tp(t0), RequestedAt: t0.Add(-time.Minute),
	}
}

func TestApplyObservations_IgnoresRowsReadBeforeTheSend(t *testing.T) {
	r := sentRequest("blocking", "public.users")
	// The tail of an OLDER snapshot of the same table, read before this send.
	old := []Observation{{Table: "public.users", Rows: 50, FirstSeen: t0.Add(-time.Minute), LastSeen: t0.Add(-10 * time.Second), TableDone: true}}
	if ApplyObservations(&r, old, t0.Add(time.Second)) {
		t.Fatalf("rows read before the send must not move the request: %+v", r)
	}
	if r.Status != StatusSent {
		t.Fatalf("status = %s, want sent", r.Status)
	}
	// Within the clock-skew slack counts.
	skew := []Observation{{Table: "public.users", Rows: 1, LastSeen: t0.Add(-2 * time.Second)}}
	if !ApplyObservations(&r, skew, t0.Add(time.Second)) || r.Status != StatusStarted {
		t.Fatalf("a row inside the slack must start the request: %+v", r)
	}
}

func TestApplyObservations_StartsThenCompletesPerTable(t *testing.T) {
	r := sentRequest("blocking", "public.users", "public.orders")
	now := t0.Add(30 * time.Second)
	obs := []Observation{{Table: "public.users", Rows: 100, FirstSeen: t0.Add(5 * time.Second), LastSeen: t0.Add(20 * time.Second), TableDone: true}}
	if !ApplyObservations(&r, obs, now) {
		t.Fatal("expected a change")
	}
	if r.Status != StatusStarted || r.StartedAt == nil || !r.LastProgressAt.Equal(now) {
		t.Fatalf("want started with progress at now: %+v", r)
	}
	if len(r.CompletedTables) != 1 || r.CompletedTables[0] != "public.users" {
		t.Fatalf("completed tables = %v", r.CompletedTables)
	}
	// Second table done (named without the schema, as a Mongo topic suffix
	// would be) → every table done → completed.
	obs = []Observation{{Table: "ORDERS", Rows: 3, LastSeen: t0.Add(40 * time.Second), TableDone: true}}
	ApplyObservations(&r, obs, now.Add(10*time.Second))
	if r.Status != StatusCompleted || r.CompletedAt == nil {
		t.Fatalf("want completed: %+v", r)
	}
	if got := r.CompletedTables; len(got) != 2 || got[1] != "public.orders" {
		t.Fatalf("completed tables must use the request's names: %v", got)
	}
}

func TestApplyObservations_AllDoneMarkerCompletes(t *testing.T) {
	r := sentRequest("blocking", "db.a", "db.b")
	obs := []Observation{{Table: "db.a", Rows: 1, LastSeen: t0.Add(time.Second), AllDone: true, TableDone: true}}
	ApplyObservations(&r, obs, t0.Add(2*time.Second))
	if r.Status != StatusCompleted {
		t.Fatalf("source.snapshot=last must complete the request: %+v", r)
	}
}

func TestApplyObservations_OtherTablesAndClosedRequestsIgnored(t *testing.T) {
	r := sentRequest("incremental", "public.users")
	if ApplyObservations(&r, []Observation{{Table: "public.orders", Rows: 5, LastSeen: t0.Add(time.Second)}}, t0.Add(2*time.Second)) {
		t.Fatal("another table's rows must not move the request")
	}
	// "users" must not match "public.superusers" (suffix match is per segment).
	if ApplyObservations(&r, []Observation{{Table: "public.superusers", Rows: 5, LastSeen: t0.Add(time.Second)}}, t0.Add(2*time.Second)) {
		t.Fatal("a table whose name merely ends with the same letters must not match")
	}
	q := r
	q.Status, q.SentAt = StatusQueued, nil
	if ApplyObservations(&q, []Observation{{Table: "public.users", Rows: 5, LastSeen: t0.Add(time.Second)}}, t0) {
		t.Fatal("a queued request cannot be started by rows")
	}
}

func TestWatch(t *testing.T) {
	tm := DefaultTiming
	cases := []struct {
		name    string
		r       Request
		now     time.Time
		tracked bool
		want    Action
	}{
		{"sent, waiting", sentRequest("blocking", "t"), t0.Add(4 * time.Minute), true, ActNone},
		{"sent, timed out → resend", sentRequest("blocking", "t"), t0.Add(5 * time.Minute), true, ActResend},
		{"sent, out of attempts", func() Request { r := sentRequest("blocking", "t"); r.Attempts = 3; return r }(), t0.Add(5 * time.Minute), true, ActUnconfirmed},
		{"sent, untracked, waiting", sentRequest("blocking", "t"), t0.Add(9 * time.Minute), false, ActNone},
		{"sent, untracked, closed", sentRequest("blocking", "t"), t0.Add(10 * time.Minute), false, ActUnconfirmed},
		{"sent, resend clock is the LAST send", func() Request {
			r := sentRequest("blocking", "t")
			r.LastSentAt = tp(t0.Add(4 * time.Minute))
			return r
		}(), t0.Add(6 * time.Minute), true, ActNone},
		{"started incremental idle → complete", func() Request {
			r := sentRequest("incremental", "t")
			r.Status, r.LastProgressAt = StatusStarted, tp(t0)
			return r
		}(), t0.Add(2 * time.Minute), true, ActComplete},
		{"started blocking idle 2m → keep waiting", func() Request {
			r := sentRequest("blocking", "t")
			r.Status, r.LastProgressAt = StatusStarted, tp(t0)
			return r
		}(), t0.Add(2 * time.Minute), true, ActNone},
		{"started blocking stalled → resend", func() Request {
			r := sentRequest("blocking", "t")
			r.Status, r.LastProgressAt = StatusStarted, tp(t0)
			return r
		}(), t0.Add(10 * time.Minute), true, ActResend},
		{"started blocking stalled, out of attempts", func() Request {
			r := sentRequest("blocking", "t")
			r.Status, r.LastProgressAt, r.Attempts = StatusStarted, tp(t0), 3
			return r
		}(), t0.Add(10 * time.Minute), true, ActFail},
		{"completed is left alone", func() Request { r := sentRequest("blocking", "t"); r.Status = StatusCompleted; return r }(), t0.Add(time.Hour), true, ActNone},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, msg := Watch(c.r, c.now, c.tracked, tm)
			if got != c.want {
				t.Fatalf("Watch = %v (%q), want %v", got, msg, c.want)
			}
			if (got == ActUnconfirmed || got == ActFail) && msg == "" {
				t.Fatal("an unconfirmed or failed request needs a reason")
			}
		})
	}
}

func running(includes ...[]string) ConnectorState {
	cs := ConnectorState{Found: true, State: "RUNNING"}
	for range includes {
		cs.TaskStates = append(cs.TaskStates, "RUNNING")
	}
	cs.TaskIncludes = includes
	return cs
}

func TestAssess(t *testing.T) {
	tables := []string{"public.users", "public.orders"}
	cases := []struct {
		name string
		cs   ConnectorState
		want Readiness
	}{
		{"missing", ConnectorState{}, Broken},
		{"connector failed", ConnectorState{Found: true, State: "FAILED"}, Broken},
		{"task failed", ConnectorState{Found: true, State: "RUNNING", TaskStates: []string{"FAILED"}, TaskIncludes: [][]string{nil}}, Broken},
		{"paused", ConnectorState{Found: true, State: "PAUSED", TaskStates: []string{"PAUSED"}}, Paused},
		{"stopped", ConnectorState{Found: true, State: "STOPPED"}, Paused},
		{"restarting", ConnectorState{Found: true, State: "RESTARTING", TaskStates: []string{"RUNNING"}}, NotReady},
		{"no tasks yet", ConnectorState{Found: true, State: "RUNNING"}, NotReady},
		{"task unassigned", ConnectorState{Found: true, State: "RUNNING", TaskStates: []string{"UNASSIGNED"}, TaskIncludes: [][]string{nil}}, NotReady},
		// The old task still runs with the old list: the whole point of the queue.
		{"old task lacks a table", running([]string{`public\.users`}), NotReady},
		{"new task captures both", running([]string{`public\.users`, `public\.orders`}), Ready},
		{"regex include", running([]string{`public\..*`}), Ready},
		{"no include list = everything", running(nil), Ready},
		{"one of two tasks lags", running([]string{`public\.users`, `public\.orders`}, []string{`public\.users`}), NotReady},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got, why := Assess(c.cs, tables); got != c.want {
				t.Fatalf("Assess = %v (%s), want %v", got, why, c.want)
			}
		})
	}
}

func TestParseIncludeList(t *testing.T) {
	var cfg map[string]interface{}
	_ = json.Unmarshal([]byte(`{"table.include.list":"public\\.users, public\\.orders","connector.class":"x"}`), &cfg)
	got := ParseIncludeList(cfg)
	if len(got) != 2 || got[0] != `public\.users` || got[1] != `public\.orders` {
		t.Fatalf("ParseIncludeList = %q", got)
	}
	if got := ParseIncludeList(map[string]interface{}{"collection.include.list": "shop.orders"}); len(got) != 1 || got[0] != "shop.orders" {
		t.Fatalf("mongo include list = %q", got)
	}
	if got := ParseIncludeList(map[string]interface{}{"connector.class": "x"}); got != nil {
		t.Fatalf("no include list must read as nil (captures everything), got %q", got)
	}
}

func TestIncludeListCaptures(t *testing.T) {
	cases := []struct {
		entries []string
		table   string
		want    bool
	}{
		{[]string{`public\.users`}, "public.users", true},
		{[]string{`public\.users`}, "PUBLIC.USERS", true},
		{[]string{`public\.users`}, "public.users2", false},
		{[]string{`public\.users`}, "publicXusers", false},
		{[]string{`shop.orders`}, "shop.orders", true},
		{[]string{`public\.(users|orders)`}, "public.orders", true},
		{[]string{`[`}, "[", true}, // invalid regex still matches literally
	}
	for _, c := range cases {
		if got := includeListCaptures(c.entries, c.table); got != c.want {
			t.Errorf("includeListCaptures(%q, %q) = %v, want %v", c.entries, c.table, got, c.want)
		}
	}
}

func TestReloadTopics(t *testing.T) {
	got := ReloadTopics("rsync_cdc_abc", []string{`public\.users`, " public.orders ", ""})
	want := []string{"rsync_cdc_abc.public.users", "rsync_cdc_abc.public.orders"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("ReloadTopics = %q, want %q", got, want)
	}
	if ReloadTopics("", []string{"a"}) != nil {
		t.Fatal("no prefix, no topics")
	}
}

func TestBuildExecuteSnapshotSignal(t *testing.T) {
	b, err := BuildExecuteSnapshotSignal("blocking", []string{"public.users"})
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Type string `json:"type"`
		Data struct {
			Type            string   `json:"type"`
			DataCollections []string `json:"data-collections"`
		} `json:"data"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	if v.Type != "execute-snapshot" || v.Data.Type != "BLOCKING" || len(v.Data.DataCollections) != 1 {
		t.Fatalf("signal = %s", b)
	}
	if _, err := BuildExecuteSnapshotSignal("incremental", nil); err == nil {
		t.Fatal("an empty collection list must be refused")
	}
}
