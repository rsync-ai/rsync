package transforms

import (
	"context"
	"reflect"
	"sort"
	"testing"
)

// The CDC delete path routes on IsRowReducing, so a wrong answer here is either
// a delete that never reaches the destination or a tombstone carrying unmasked
// PII. These tests check the classification against the ENGINE's actual
// behaviour rather than against a second copy of the same table.

// A config that makes each type do real work on the fixture row below.
func exercisingConfig(typ string) map[string]interface{} {
	switch typ {
	case "filter":
		return map[string]interface{}{"condition": "status = 'gone'"}
	case "validate":
		return map[string]interface{}{"required_columns": []interface{}{"status"}}
	case "null_handle":
		return map[string]interface{}{"column": "status", "strategy": "drop_row"}
	case "mask_pii":
		return map[string]interface{}{"column": "email"}
	case "select_columns":
		return map[string]interface{}{"columns": []interface{}{"id", "email"}}
	case "exclude_columns":
		return map[string]interface{}{"columns": []interface{}{"email"}}
	case "rename_columns":
		return map[string]interface{}{"mappings": map[string]interface{}{"email": "email_address"}}
	case "truncate":
		return map[string]interface{}{"column": "email", "max_length": 3}
	case "type_convert":
		return map[string]interface{}{"column": "id", "to": "string"}
	case "json_flatten":
		return map[string]interface{}{"column": "doc"}
	case "array_expand":
		return map[string]interface{}{"column": "tags"}
	}
	return nil
}

func reducingFixture() []Row {
	return []Row{
		{"id": 1, "status": "active", "email": "a@b.com", "doc": map[string]interface{}{"k": "v"}, "tags": []interface{}{"x", "y"}},
		{"id": 2, "status": "active", "email": "c@d.com", "doc": map[string]interface{}{"k": "w"}, "tags": []interface{}{"z"}},
	}
}

// The golden guard: every type the engine can run must be classified on
// purpose. Adding a transform type without answering "can this drop a row?"
// fails here rather than in a CDC pipeline.
func TestIsRowReducing_ClassifiesEverySupportedType(t *testing.T) {
	expected := map[string]bool{
		"filter":          true,
		"validate":        true,
		"null_handle":     true, // with strategy=drop_row; see the strategy test
		"mask_pii":        false,
		"select_columns":  false,
		"exclude_columns": false,
		"rename_columns":  false,
		"truncate":        false,
		"type_convert":    false,
		"json_flatten":    false,
		"array_expand":    false,
	}

	for _, typ := range SupportedTransformTypes() {
		want, classified := expected[typ]
		if !classified {
			t.Fatalf("transform type %q is runnable but unclassified: decide whether it can drop a row, then add it here and to IsRowReducing", typ)
		}
		got := IsRowReducing(Transform{Type: typ, Config: exercisingConfig(typ)})
		if got != want {
			t.Errorf("IsRowReducing(%s) = %v, want %v", typ, got, want)
		}
	}

	for typ := range expected {
		if !NewSimpleTransformEngine().CanHandle(typ) {
			t.Errorf("%q is classified but the engine cannot run it; the two lists have drifted", typ)
		}
	}
}

// The classification is a claim about what the engine does, so check it against
// the engine: every type called row-preserving must return exactly the rows it
// was given, and every type called row-reducing must be able to return fewer.
func TestIsRowReducing_AgreesWithWhatTheEngineActuallyDoes(t *testing.T) {
	for _, typ := range SupportedTransformTypes() {
		cfg := exercisingConfig(typ)
		in := reducingFixture()
		out, err := applyRaw(in, typ, cfg)
		if err != nil {
			t.Fatalf("%s: fixture did not exercise the transform: %v", typ, err)
		}

		if IsRowReducing(Transform{Type: typ, Config: cfg}) {
			continue
		}
		if len(out) != len(in) {
			t.Errorf("%s is classified row-preserving but returned %d of %d rows", typ, len(out), len(in))
		}
	}
}

// Both row-reducing types must actually be able to remove a row, or the
// classification is protecting against nothing.
func TestIsRowReducing_ReducersReallyRemoveRows(t *testing.T) {
	cases := []struct {
		typ  string
		cfg  map[string]interface{}
		data []Row
	}{
		{"filter", map[string]interface{}{"condition": "status = 'gone'"}, reducingFixture()},
		{"validate", map[string]interface{}{"required_columns": []interface{}{"status"}},
			[]Row{{"status": "active"}, {"status": nil}}},
		{"null_handle", map[string]interface{}{"column": "status", "strategy": "drop_row"},
			[]Row{{"status": "active"}, {"status": nil}}},
	}
	for _, c := range cases {
		out, err := applyRaw(c.data, c.typ, c.cfg)
		if err != nil {
			t.Fatalf("%s: %v", c.typ, err)
		}
		if len(out) >= len(c.data) {
			t.Fatalf("%s did not remove any row (%d -> %d); the reducing control is inert", c.typ, len(c.data), len(out))
		}
		if !IsRowReducing(Transform{Type: c.typ, Config: c.cfg}) {
			t.Fatalf("%s removed a row but is classified row-preserving", c.typ)
		}
	}
}

// null_handle is the one type whose answer depends on its config.
func TestIsRowReducing_NullHandleDependsOnStrategy(t *testing.T) {
	drop := Transform{Type: "null_handle", Config: map[string]interface{}{"column": "a", "strategy": "drop_row"}}
	if !IsRowReducing(drop) {
		t.Error("null_handle strategy=drop_row must be row-reducing")
	}

	for _, cfg := range []map[string]interface{}{
		{"column": "a", "strategy": "default", "default_value": 0},
		{"column": "a", "default_value": 0},                    // strategy omitted -> default
		{"column": "a", "strategy": "   ", "default_value": 0}, // blank -> default
		{"column": "a", "strategy": "DROP_ROW"},                // case is normalized
	} {
		want := nullHandleStrategy(cfg) == "drop_row"
		if got := IsRowReducing(Transform{Type: "null_handle", Config: cfg}); got != want {
			t.Errorf("config %v: IsRowReducing = %v, want %v", cfg, got, want)
		}
	}
}

func TestIsRowReducing_UnknownTypeIsTreatedAsReducing(t *testing.T) {
	if !IsRowReducing(Transform{Type: "aggregate"}) {
		t.Fatal("an unrecognized type must be assumed row-reducing so a delete is never silently dropped by it")
	}
}

// ------------------------------------------------- rename mapping reuse

func TestRenameMappings_ReadsEveryConfigShapeAndOnlyRenames(t *testing.T) {
	shapes := []map[string]interface{}{
		{"mappings": map[string]interface{}{"users.email": "email_address"}},
		{"mappings": map[string]string{"email": "email_address"}},
		{"mappings": []interface{}{map[string]interface{}{"from": "email", "to": "email_address"}}},
		{"from": "email", "to": "email_address"},
	}
	for i, cfg := range shapes {
		got := RenameMappings(Transform{Type: "rename_columns", Config: cfg})
		if !reflect.DeepEqual(got, map[string]string{"email": "email_address"}) {
			t.Errorf("shape %d: got %v", i, got)
		}
	}

	if got := RenameMappings(Transform{Type: "mask_pii", Config: map[string]interface{}{"column": "email"}}); got != nil {
		t.Errorf("only rename_columns has mappings, got %v", got)
	}
	if got := RenameMappings(Transform{Type: "rename_columns", Config: map[string]interface{}{}}); got != nil {
		t.Errorf("an empty rename must report no mappings, got %v", got)
	}
}

// The mapping that renames the rows must be the mapping that renames the key
// fields — same table, same parser. A composite key also has to keep its order.
func TestRemapColumnNames_FollowsARenameIntoKeyFields(t *testing.T) {
	cfg := map[string]interface{}{"mappings": map[string]interface{}{"email": "email_address"}}
	m := RenameMappings(Transform{Type: "rename_columns", Config: cfg})

	keys := []string{"tenant_id", "email", "region"}
	got := RemapColumnNames(keys, m)
	want := []string{"tenant_id", "email_address", "region"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if keys[1] != "email" {
		t.Fatal("RemapColumnNames mutated the caller's slice")
	}

	// And the renamed key must be the column the rows now carry.
	rows, err := applyRaw([]Row{{"tenant_id": 1, "email": "a@b.com", "region": "eu"}}, "rename_columns", cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range got {
		if _, ok := rows[0][k]; !ok {
			t.Fatalf("key field %q is not a column of the transformed row %v", k, rows[0])
		}
	}
}

func TestRemapColumnNames_NoMappingsIsANoOp(t *testing.T) {
	keys := []string{"id"}
	if got := RemapColumnNames(keys, nil); !reflect.DeepEqual(got, keys) {
		t.Fatalf("got %v", got)
	}
}

func renameStep(mappings map[string]interface{}) Transform {
	return Transform{Type: "rename_columns", Config: map[string]interface{}{"mappings": mappings}}
}

// A chain that moves a column twice has to answer where it ENDED UP. Remapping
// key metadata against only the first step aims the destination at an
// intermediate name no row ever carries out of the chain.
func TestAccumulateRenameMappings_FollowsAColumnThroughTheWholeChain(t *testing.T) {
	chain := []Transform{
		renameStep(map[string]interface{}{"email": "contact"}),
		{Type: "mask_pii", Config: map[string]interface{}{"columns": []interface{}{"contact"}}},
		renameStep(map[string]interface{}{"contact": "contact_email", "id": "customer_id"}),
	}

	got := AccumulateRenameMappings(chain)

	want := map[string]string{"email": "contact_email", "id": "customer_id"}
	if len(got) != len(want) {
		t.Fatalf("accumulated %d mappings, want %d: %v", len(got), len(want), got)
	}
	for from, to := range want {
		if got[from] != to {
			t.Errorf("mapping %q: got %q, want %q (full: %v)", from, got[from], to, got)
		}
	}

	// And the whole point: the key fields follow the rows to the final spelling.
	if keys := RemapColumnNames([]string{"id", "email"}, got); keys[0] != "customer_id" || keys[1] != "contact_email" {
		t.Errorf("key fields after the chain = %v, want [customer_id contact_email]", keys)
	}
}

func TestAccumulateRenameMappings_NoRenamesIsNil(t *testing.T) {
	chain := []Transform{
		{Type: "mask_pii", Config: map[string]interface{}{"columns": []interface{}{"email"}}},
		{Type: "filter", Config: map[string]interface{}{"condition": "id > 0"}},
	}
	if got := AccumulateRenameMappings(chain); len(got) != 0 {
		t.Fatalf("a chain with no rename_columns accumulated %v, want none", got)
	}
}

// The accumulated table is only trustworthy if it agrees with what the engine
// actually did to the rows. Run the chain and compare the column names.
func TestAccumulateRenameMappings_AgreesWithTheColumnsTheEngineProduces(t *testing.T) {
	chain := []Transform{
		renameStep(map[string]interface{}{"email": "contact"}),
		renameStep(map[string]interface{}{"contact": "contact_email"}),
	}

	engine := NewSimpleTransformEngine()
	out := []Row{{"id": 1, "email": "a@b.c"}}
	for _, step := range chain {
		var err error
		if out, err = engine.Apply(context.Background(), out, step); err != nil {
			t.Fatalf("apply %s: %v", step.Type, err)
		}
	}
	if len(out) != 1 {
		t.Fatalf("got %d rows, want 1", len(out))
	}

	mappings := AccumulateRenameMappings(chain)
	for _, src := range []string{"email"} {
		final := RemapColumnNames([]string{src}, mappings)[0]
		if _, ok := out[0][final]; !ok {
			t.Errorf("mappings say %q ends up as %q, but the transformed row has columns %v",
				src, final, columnNames(out[0]))
		}
	}
}

func columnNames(r Row) []string {
	names := make([]string, 0, len(r))
	for k := range r {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// sm.PK is the parsed Kafka record key. Its KEYS are what an upsert destination
// is told to match on, so a rename has to reach them too - and it must not
// rewrite the caller's map, which a redelivery of the same offset would reuse.
func TestRemapMapKeys_RenamesKeysAndCopiesTheMap(t *testing.T) {
	pk := map[string]interface{}{"id": 7, "email": "a@b.c"}
	mappings := map[string]string{"email": "contact_email"}

	got := RemapMapKeys(pk, mappings)

	if got["contact_email"] != "a@b.c" {
		t.Errorf("remapped PK = %v, want contact_email carried over", got)
	}
	if _, stale := got["email"]; stale {
		t.Errorf("remapped PK still carries the old key: %v", got)
	}
	if got["id"] != 7 {
		t.Errorf("unmapped key was not carried through: %v", got)
	}
	if _, ok := pk["email"]; !ok {
		t.Errorf("caller's PK map was mutated: %v", pk)
	}
}

func TestRemapMapKeys_NoMappingsIsANoOp(t *testing.T) {
	pk := map[string]interface{}{"id": 7}
	if got := RemapMapKeys(pk, nil); len(got) != 1 || got["id"] != 7 {
		t.Fatalf("RemapMapKeys with no mappings changed the map: %v", got)
	}
}
