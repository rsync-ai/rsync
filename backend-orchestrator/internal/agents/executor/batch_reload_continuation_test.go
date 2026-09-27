package executor

import "testing"

// A batch reload bigger than one chunk restarted at offset 0 on every
// continuation (local e2e 2026-09-26: 599 "cleaning destination scope" lines in a
// minute, no batch past offset 0). Only this reload's own checkpoint resumes it.
func TestReloadContinuesTable(t *testing.T) {
	const exec = "3662528f-0000-4000-8000-000000000001"
	cases := []struct {
		name     string
		position map[string]interface{}
		execID   string
		want     bool
	}{
		{"this reload's checkpoint", map[string]interface{}{"execution_id": exec, "offset": 1000.0}, exec, true},
		{"an earlier run's checkpoint", map[string]interface{}{"execution_id": "b1c6a413-0000-4000-8000-000000000002"}, exec, false},
		{"written before execution_id existed", map[string]interface{}{"offset": 1000.0}, exec, false},
		{"no execution id on the task", map[string]interface{}{"execution_id": ""}, "", false},
		{"no position", nil, exec, false},
	}
	for _, c := range cases {
		if got := reloadContinuesTable(c.position, c.execID); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

// A multi-table reload re-read a table it had finished on every continuation.
func TestReloadFinishedTable(t *testing.T) {
	const exec = "3662528f-0000-4000-8000-000000000001"
	cases := []struct {
		name     string
		position map[string]interface{}
		want     bool
	}{
		{"this reload finished it", map[string]interface{}{"execution_id": exec, "table_complete": true}, true},
		{"this reload is mid-table", map[string]interface{}{"execution_id": exec, "table_complete": false}, false},
		{"an earlier run finished it", map[string]interface{}{"execution_id": "b1c6a413-0000-4000-8000-000000000002", "table_complete": true}, false},
		{"no table_complete field", map[string]interface{}{"execution_id": exec}, false},
	}
	for _, c := range cases {
		if got := reloadFinishedTable(c.position, exec); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
