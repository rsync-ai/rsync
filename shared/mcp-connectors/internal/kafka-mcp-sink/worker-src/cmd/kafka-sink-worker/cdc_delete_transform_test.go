package main

import (
	"context"
	"testing"

	"github.com/rsync-ai/shared/transforms"
)

// The chain a privacy-conscious CDC pipeline actually ships: mask the PII column,
// rename it to the destination's spelling, and keep only live rows.
func deletePipelineChain(t *testing.T) []transforms.CanonicalTransform {
	t.Helper()
	raw := []map[string]any{
		{"type": "mask_pii", "config": map[string]any{"column": "email", "mask_type": "hash"}, "order": 1},
		{"type": "rename_columns", "config": map[string]any{"mappings": map[string]any{"email": "contact_email"}}, "order": 2},
		{"type": "filter", "config": map[string]any{"condition": "status = 'active'"}, "order": 3},
	}
	canonical, _, err := transforms.NormalizeAndValidate(raw, "customers", transforms.NormalizeModeCDC)
	if err != nil {
		t.Fatalf("fixture chain did not validate: %v", err)
	}
	if len(canonical) != 3 {
		t.Fatalf("fixture chain normalized to %d transforms, want 3", len(canonical))
	}
	return canonical
}

// applyChain runs a canonical chain the way applyConsumerTransforms runs it,
// minus the DB-backed audit logging.
func applyChain(t *testing.T, chain []transforms.CanonicalTransform, rows []transforms.Row) []transforms.Row {
	t.Helper()
	coordinator := transforms.NewTransformCoordinator(
		transforms.NewSimpleTransformEngine(), transforms.NewDuckDBTransformEngine())
	out := rows
	for _, ct := range chain {
		next, err := coordinator.Apply(context.Background(), out, []transforms.Transform{ct.EngineTransform()})
		if err != nil {
			t.Fatalf("apply %s: %v", ct.Type, err)
		}
		out = next
	}
	return out
}

// A delete's before image is a row like any other and must be masked and
// renamed - but the rule that would DROP it has to be left out, or the tombstone
// never reaches the destination.
func TestCDCApplicableTransforms_DeleteKeepsShapingRulesAndDropsReducers(t *testing.T) {
	chain := deletePipelineChain(t)

	kept := cdcApplicableTransforms(chain, true, "customers")

	if len(kept) != 2 {
		t.Fatalf("delete kept %d rules, want 2 (mask_pii, rename_columns)", len(kept))
	}
	for _, ct := range kept {
		if transforms.IsRowReducing(ct.EngineTransform()) {
			t.Errorf("row-reducing rule %q survived the delete filter", ct.Type)
		}
	}
	if kept[0].Type != "mask_pii" || kept[1].Type != "rename_columns" {
		t.Errorf("order was not preserved: %s, %s", kept[0].Type, kept[1].Type)
	}
}

func TestCDCApplicableTransforms_NonDeleteRunsTheWholeChain(t *testing.T) {
	chain := deletePipelineChain(t)
	if got := cdcApplicableTransforms(chain, false, "customers"); len(got) != len(chain) {
		t.Fatalf("insert/update ran %d of %d rules; only a delete skips any", len(got), len(chain))
	}
}

// The composition that matters: run the delete's before image through the rules
// that survive and check it comes out masked, renamed, and STILL THERE. Before
// this fix the whole chain was skipped and the raw row went to the destination.
func TestCDCDelete_BeforeImageIsMaskedRenamedAndNotDropped(t *testing.T) {
	before := map[string]interface{}{"id": 42, "email": "gone@example.com", "status": "deleted"}

	kept := cdcApplicableTransforms(deletePipelineChain(t), true, "customers")
	out := applyChain(t, kept, []transforms.Row{transforms.Row(before)})

	if len(out) != 1 {
		t.Fatalf("the delete's before image was dropped (%d rows out); the tombstone would never reach the destination", len(out))
	}
	row := out[0]
	if _, raw := row["email"]; raw {
		t.Errorf("column was not renamed; row still carries source column `email`: %v", row)
	}
	masked, ok := row["contact_email"]
	if !ok {
		t.Fatalf("renamed column missing from the transformed before image: %v", row)
	}
	if masked == "gone@example.com" {
		t.Errorf("the deleted row's email reached the destination unmasked: %v", masked)
	}
	if row["id"] != 42 {
		t.Errorf("untouched column was lost: %v", row)
	}

	// status="deleted" does not satisfy the filter, so leaving the filter in would
	// have emptied this batch - which is exactly the control this test needs.
	if dropped := applyChain(t, deletePipelineChain(t), []transforms.Row{transforms.Row(before)}); len(dropped) != 0 {
		t.Fatalf("control is inert: the full chain kept %d rows, so skipping the filter proved nothing", len(dropped))
	}
}

// sm.KeyFields and sm.PK travel to the destination as key_fields / key_data
// while the rows travel renamed. They have to be renamed by the same table.
func TestApplyRenamesToCDCKeys_FollowsARenameIntoKeyFieldsAndPK(t *testing.T) {
	sm := &SinkMessage{
		Table:     "customers",
		KeyFields: []string{"tenant_id", "email"},
		PK:        map[string]interface{}{"tenant_id": "t1", "email": "gone@example.com"},
	}

	applyRenamesToCDCKeys(sm, deletePipelineChain(t))

	if len(sm.KeyFields) != 2 || sm.KeyFields[0] != "tenant_id" || sm.KeyFields[1] != "contact_email" {
		t.Errorf("key fields = %v, want [tenant_id contact_email]", sm.KeyFields)
	}
	if _, stale := sm.PK["email"]; stale {
		t.Errorf("PK still keyed on the source column name: %v", sm.PK)
	}
	if sm.PK["contact_email"] != "gone@example.com" {
		t.Errorf("PK value did not survive the rename: %v", sm.PK)
	}
	if sm.PK["tenant_id"] != "t1" {
		t.Errorf("unrenamed key column was lost: %v", sm.PK)
	}
}

func TestApplyRenamesToCDCKeys_NoRenameLeavesKeysAlone(t *testing.T) {
	raw := []map[string]any{
		{"type": "mask_pii", "config": map[string]any{"column": "email", "mask_type": "hash"}, "order": 1},
	}
	canonical, _, err := transforms.NormalizeAndValidate(raw, "customers", transforms.NormalizeModeCDC)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}

	sm := &SinkMessage{Table: "customers", KeyFields: []string{"id"}, PK: map[string]interface{}{"id": 1}}
	applyRenamesToCDCKeys(sm, canonical)

	if len(sm.KeyFields) != 1 || sm.KeyFields[0] != "id" {
		t.Errorf("key fields changed with no rename in the chain: %v", sm.KeyFields)
	}
	if sm.PK["id"] != 1 {
		t.Errorf("PK changed with no rename in the chain: %v", sm.PK)
	}
}

func TestApplyRenamesToCDCKeys_EmptyChainAndNilMessageAreSafe(t *testing.T) {
	applyRenamesToCDCKeys(nil, deletePipelineChain(t))
	sm := &SinkMessage{KeyFields: []string{"id"}}
	applyRenamesToCDCKeys(sm, nil)
	if len(sm.KeyFields) != 1 {
		t.Fatalf("empty chain altered the key fields: %v", sm.KeyFields)
	}
}
