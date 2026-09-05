package executor

import "testing"

// TestEvaluateCapabilityGate is the core §2 DoD matrix: a move modality must be in
// the destination's declared modalities, else CAPABILITY_MISMATCH.
func TestEvaluateCapabilityGate(t *testing.T) {
	cases := []struct {
		name      string
		move      string
		destMods  []string
		wantAllow bool
	}{
		{"blob into blob-capable dest -> allow", ModalityBlob, []string{"structured", "blob"}, true},
		{"blob into structured-only dest -> reject", ModalityBlob, []string{"structured"}, false},
		{"structured into structured-only dest -> allow", ModalityStructured, []string{"structured"}, true},
		{"structured into blob-capable dest -> allow", ModalityStructured, []string{"structured", "blob"}, true},
		{"blob into empty modalities -> reject", ModalityBlob, []string{}, false},
		{"blob into nil modalities -> reject", ModalityBlob, nil, false},
		{"case-insensitive blob match -> allow", "Blob", []string{"structured", "BLOB"}, true},
		{"empty move treated as structured -> allow", "", []string{"structured"}, true},
		{"whitespace-padded dest token matches -> allow", ModalityBlob, []string{" blob "}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := evaluateCapabilityGate(tc.move, tc.destMods)
			if tc.wantAllow && got != nil {
				t.Fatalf("expected allow, got mismatch: %v", got.Error())
			}
			if !tc.wantAllow && got == nil {
				t.Fatalf("expected CAPABILITY_MISMATCH, got allow")
			}
			if got != nil {
				// rejection must carry the move modality + dest modalities for the message
				if got.MoveModality == "" {
					t.Errorf("mismatch missing MoveModality")
				}
				if msg := got.Error(); msg == "" || msg[:len(CapabilityMismatchCode)] != CapabilityMismatchCode {
					t.Errorf("mismatch message should start with %q, got %q", CapabilityMismatchCode, msg)
				}
			}
		})
	}
}

// TestDeriveMoveModality: blob is strictly opt-in; everything else is structured
// (the default that keeps existing pipelines untouched).
func TestDeriveMoveModality(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]interface{}
		want   string
	}{
		{"nil params -> structured", nil, ModalityStructured},
		{"empty params -> structured", map[string]interface{}{}, ModalityStructured},
		{"unrelated params -> structured", map[string]interface{}{"tables": []string{"a"}, "sync_mode": "batch"}, ModalityStructured},
		{"copy_raw_files bool true -> blob", map[string]interface{}{"copy_raw_files": true}, ModalityBlob},
		{"copy_raw_files string true -> blob", map[string]interface{}{"copy_raw_files": "true"}, ModalityBlob},
		{"copy_raw_files false -> structured", map[string]interface{}{"copy_raw_files": false}, ModalityStructured},
		{"passthrough yes -> blob", map[string]interface{}{"passthrough": "yes"}, ModalityBlob},
		{"raw_files on -> blob", map[string]interface{}{"raw_files": "on"}, ModalityBlob},
		{"move_modality blob -> blob", map[string]interface{}{"move_modality": "blob"}, ModalityBlob},
		{"move_modality structured -> structured", map[string]interface{}{"move_modality": "structured"}, ModalityStructured},
		{"delivery_method copy_raw_files -> blob", map[string]interface{}{"delivery_method": "copy_raw_files"}, ModalityBlob},
		{"delivery_method raw -> blob", map[string]interface{}{"delivery_method": "RAW"}, ModalityBlob},
		{"delivery_method replicate_records -> structured", map[string]interface{}{"delivery_method": "replicate_records"}, ModalityStructured},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := deriveMoveModality(tc.params); got != tc.want {
				t.Fatalf("deriveMoveModality = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestModalitiesFromCapabilities: parse supported_modalities out of a connector's
// metadata capabilities block, defaulting to ["structured"] on any miss.
func TestModalitiesFromCapabilities(t *testing.T) {
	eq := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	cases := []struct {
		name string
		caps interface{}
		want []string
	}{
		{
			"object with structured+blob",
			map[string]interface{}{"supported_modalities": []interface{}{"structured", "blob"}},
			[]string{"structured", "blob"},
		},
		{"object without the key -> default", map[string]interface{}{"max_batch_size": 10000}, []string{"structured"}},
		{"nil caps -> default", nil, []string{"structured"}},
		{"array-form capabilities -> default", []interface{}{"export", "import"}, []string{"structured"}},
		{"empty modalities array -> default", map[string]interface{}{"supported_modalities": []interface{}{}}, []string{"structured"}},
		{
			"junk entries filtered, strings kept",
			map[string]interface{}{"supported_modalities": []interface{}{"structured", 42, "", "blob"}},
			[]string{"structured", "blob"},
		},
		{
			"non-array value -> default",
			map[string]interface{}{"supported_modalities": "structured"},
			[]string{"structured"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := modalitiesFromCapabilities(tc.caps); !eq(got, tc.want) {
				t.Fatalf("modalitiesFromCapabilities = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestEnforceCapabilityGate_Wiring exercises the wired entry point end to end with a
// bare Agent (nil mcpManager). It proves:
//   - structured moves are a true no-op (returns nil, never reads metadata),
//   - a blob move with unverifiable destination metadata FAILS CLOSED (rejected with
//     a well-formed capability_mismatch response).
func TestEnforceCapabilityGate_Wiring(t *testing.T) {
	a := &Agent{} // mcpManager is nil -> destSupportedModalities falls back to ["structured"]

	t.Run("structured move is a no-op", func(t *testing.T) {
		task := &ExecutorTask{
			TaskID:      "t1",
			PipelineID:  "p1",
			Source:      &ConnectorConfig{Type: "postgresql"},
			Destination: &ConnectorConfig{Type: "aws-s3"},
			Params:      map[string]interface{}{"sync_mode": "batch"},
		}
		if resp := a.enforceCapabilityGate(task); resp != nil {
			t.Fatalf("structured move should pass the gate, got rejection: %+v", resp)
		}
	})

	t.Run("blob move into structured-only/unverifiable dest fails closed", func(t *testing.T) {
		task := &ExecutorTask{
			TaskID:      "t2",
			PipelineID:  "p2",
			Source:      &ConnectorConfig{Type: "aws-s3"},
			Destination: &ConnectorConfig{Type: "postgresql"},
			Params:      map[string]interface{}{"copy_raw_files": true},
		}
		resp := a.enforceCapabilityGate(task)
		if resp == nil {
			t.Fatalf("blob move into structured-only dest should be rejected")
		}
		if resp.Status != "failed" {
			t.Errorf("expected Status=failed, got %q", resp.Status)
		}
		if resp.Result == nil || resp.Result["error_code"] != CapabilityMismatchCode {
			t.Errorf("expected error_code=%q in Result, got %v", CapabilityMismatchCode, resp.Result)
		}
		if resp.PipelineID != "p2" || resp.TaskID != "t2" {
			t.Errorf("response should echo task identity, got task=%q pipeline=%q", resp.TaskID, resp.PipelineID)
		}
	})
}
