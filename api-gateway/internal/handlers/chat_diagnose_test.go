package handlers

import "testing"

// TestDiagnosisTriggers locks in the Phase-1 fix: the natural self-serve
// phrasings a real user types ("check issues", "what's wrong", "not working",
// "0 rows", "stuck") now route to pipeline diagnosis, while unrelated intents
// (create/list/connectors) are NOT hijacked. Mirrors the trigger decision in
// maybeHandleDiagnoseCommand.
func TestDiagnosisTriggers(t *testing.T) {
	triggers := func(msg string) bool {
		if m := reDiagWithUUID.FindStringSubmatch(msg); len(m) == 2 {
			return true
		}
		return reDiagLast.MatchString(msg) || reDiagGeneric.MatchString(msg) || reDiagBroad.MatchString(msg)
	}
	cases := []struct {
		msg  string
		want bool
	}{
		// The gap this fix closes — natural phrasings that previously fell
		// through to generic chat with no pipeline context:
		{"check issues", true},
		{"check the issues on my pipeline", true},
		{"what went wrong", true},
		{"what's wrong with my sync", true},
		{"troubleshoot my pipeline", true},
		{"my pipeline is not working", true},
		{"the sync isn't working", true},
		{"no data is showing up", true},
		{"0 rows landed", true},
		{"it's stuck", true},
		{"the pipeline is broken", true},
		// Already worked before this fix — must keep working:
		{"diagnose", true},
		{"why did my pipeline fail", true},
		{"why did my last run fail", true},
		{"fix my pipeline", true},
		{"diagnose execution 3a7e63e5-1111-2222-3333-444455556666", true},
		// Must NOT hijack unrelated intents:
		{"create a pipeline from mysql to postgres", false},
		{"list my connections", false},
		{"show me my pipelines", false},
		{"what connectors do you support", false},
		{"check my network settings", false}, // "network" must not trip "not work"
		{"schedule my pipeline for 9am", false},
	}
	for _, tc := range cases {
		if got := triggers(tc.msg); got != tc.want {
			t.Errorf("triggers(%q) = %v, want %v", tc.msg, got, tc.want)
		}
	}
}

// TestEvidenceStr covers the defensive nested-map reader used to pull pipeline
// fields out of the diagnosis evidence map.
func TestEvidenceStr(t *testing.T) {
	ev := map[string]interface{}{
		"pipeline": map[string]interface{}{"name": "p1", "status": "failed"},
		"progress": map[string]interface{}{"blocking_reason": ""},
	}
	if got := evidenceStr(ev, "pipeline", "name"); got != "p1" {
		t.Errorf("name = %q, want p1", got)
	}
	if got := evidenceStr(ev, "pipeline", "status"); got != "failed" {
		t.Errorf("status = %q, want failed", got)
	}
	if got := evidenceStr(ev, "pipeline", "missing"); got != "" {
		t.Errorf("missing key = %q, want empty", got)
	}
	if got := evidenceStr(ev, "absent", "x"); got != "" {
		t.Errorf("absent outer = %q, want empty", got)
	}
}
