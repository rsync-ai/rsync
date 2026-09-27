package transforms

import (
	"context"
	"strings"
	"testing"
)

// The Transform Builder stores a rule as {transform_config: {operation, ...}},
// in ITS key names. Every server-side reader — the save validator
// (api-gateway transforms.go), the batch executor and the CDC sink — starts
// from that stored row and goes through NormalizeAndValidate.
//
// These tests drive that exact shape and then RUN the engine on the result,
// because the failures this family produced were not all validation failures:
// two of them saved cleanly and then did the wrong thing at run time. Asserting
// only that normalization succeeds would re-admit exactly those.

// storedRule builds the row shape the builder writes (transformPlan.ts
// toDefinitions): operation inside transform_config, alongside its own fields.
func storedRule(operation string, cfg map[string]any) []map[string]any {
	tc := map[string]any{"operation": operation}
	for k, v := range cfg {
		tc[k] = v
	}
	return []map[string]any{{"transform_order": 0, "enabled": true, "transform_config": tc}}
}

// canonicalOne normalizes a single stored rule in execution mode and fails the
// test if it is rejected.
func canonicalOne(t *testing.T, operation string, cfg map[string]any) CanonicalTransform {
	t.Helper()
	out, warnings, err := NormalizeAndValidate(storedRule(operation, cfg), "", NormalizeModeExecution)
	if err != nil {
		t.Fatalf("operation %q was rejected: %v (warnings=%v)", operation, err, warnings)
	}
	if len(out) != 1 {
		t.Fatalf("operation %q: expected 1 canonical transform, got %d", operation, len(out))
	}
	return out[0]
}

func applyOne(t *testing.T, ct CanonicalTransform, rows []Row) []Row {
	t.Helper()
	out, err := NewSimpleTransformEngine().Apply(context.Background(), rows, ct.EngineTransform())
	if err != nil {
		t.Fatalf("engine refused %q: %v", ct.Type, err)
	}
	return out
}

// The Hash Column card stores operation "hash" with no mask_type. Before this,
// normalizeType had no `hash` case, so save failed with
// `unknown transform type "hash"` — the card could not be used at all.
func TestBuilderVocabulary_HashColumnHashes(t *testing.T) {
	ct := canonicalOne(t, "hash", map[string]any{"column": "email", "hash_function": "sha256"})

	if ct.Type != "mask_pii" {
		t.Fatalf("expected hash to fold to mask_pii, got %q", ct.Type)
	}
	// The mask_type matters more than the fold: applyMask's default branch
	// redacts to "***", so a `hash`->`mask_pii` alias WITHOUT this would store a
	// rule that quietly writes a constant where a digest was promised.
	if mt, _ := ct.Config["mask_type"].(string); mt != "hash" {
		t.Fatalf("expected mask_type=hash to be supplied, got %q", mt)
	}

	out := applyOne(t, ct, []Row{{"email": "a@b.com", "id": 1}})
	got, _ := out[0]["email"].(string)
	if got == "***" {
		t.Fatalf("email was redacted, not hashed: %q", got)
	}
	if !strings.HasPrefix(got, "sha256:") {
		t.Fatalf("expected a sha256: digest, got %q", got)
	}
	if got == "a@b.com" {
		t.Fatalf("email reached the destination unmasked")
	}
}

// An explicit mask_type on a hash rule is not overwritten.
func TestBuilderVocabulary_HashRespectsExplicitMaskType(t *testing.T) {
	ct := canonicalOne(t, "hash", map[string]any{"column": "email", "mask_type": "redact"})
	if mt, _ := ct.Config["mask_type"].(string); mt != "redact" {
		t.Fatalf("explicit mask_type was overwritten: %q", mt)
	}
}

// The Rename card stores "old:new, old2:new2". Both validateConfig
// (asStringStringMap) and applyRenameColumns read a map and ignore a string, so
// this used to be rejected on save.
func TestBuilderVocabulary_RenameParsesMappingString(t *testing.T) {
	ct := canonicalOne(t, "rename", map[string]any{"mappings": "email:user_email, name:full_name"})

	if ct.Type != "rename_columns" {
		t.Fatalf("expected rename_columns, got %q", ct.Type)
	}
	out := applyOne(t, ct, []Row{{"email": "a@b.com", "name": "Ada", "id": 1}})
	if _, still := out[0]["email"]; still {
		t.Fatalf("email was not renamed: %v", out[0])
	}
	if v, _ := out[0]["user_email"].(string); v != "a@b.com" {
		t.Fatalf("expected user_email=a@b.com, got %v", out[0])
	}
	if v, _ := out[0]["full_name"].(string); v != "Ada" {
		t.Fatalf("expected full_name=Ada, got %v", out[0])
	}
}

// A mapping string that parses to nothing must still be refused, rather than
// reaching the engine as a rename that renames nothing.
func TestBuilderVocabulary_RenameRejectsUnparseableMappings(t *testing.T) {
	_, _, err := NormalizeAndValidate(
		storedRule("rename", map[string]any{"mappings": "email, name"}),
		"", NormalizeModeExecution,
	)
	if err == nil {
		t.Fatal("expected an unparseable mapping string to be rejected")
	}
}

// The Type Conversion card stores to_type; the engine reads to.
func TestBuilderVocabulary_TypeConvertReadsToType(t *testing.T) {
	ct := canonicalOne(t, "type_convert", map[string]any{"column": "age", "to_type": "integer"})

	if to, _ := ct.Config["to"].(string); to != "integer" {
		t.Fatalf("expected to=integer, got %q", to)
	}
	if _, leftover := ct.Config["to_type"]; leftover {
		t.Fatal("to_type should be consumed, not passed through alongside to")
	}
	out := applyOne(t, ct, []Row{{"age": "42"}})
	if v, ok := out[0]["age"].(int); !ok || v != 42 {
		t.Fatalf("expected int 42, got %#v", out[0]["age"])
	}
}

// The Handle Nulls card stores action ("default" | "skip"); the engine reads
// strategy. This is the one that SAVED CLEANLY and then did the opposite: with
// action untranslated, strategy fell back to "default" and the rule FILLED the
// rows the operator asked it to drop.
func TestBuilderVocabulary_NullHandleSkipDropsRows(t *testing.T) {
	ct := canonicalOne(t, "null_handle", map[string]any{
		"column": "email", "action": "skip", "default_value": "n/a",
	})

	if s, _ := ct.Config["strategy"].(string); s != "drop_row" {
		t.Fatalf("expected strategy=drop_row for action=skip, got %q", s)
	}
	// {"id": 2} does not carry an email column at all, which is not a null email:
	// only the explicit null is dropped
	// (KI-NULL-HANDLE-MISSING-COLUMN-DROPS-EVERY-ROW).
	out := applyOne(t, ct, []Row{{"email": "a@b.com"}, {"email": nil}, {"id": 2}})
	if len(out) != 2 {
		t.Fatalf("expected the null row dropped and the other 2 kept, got %d rows: %v", len(out), out)
	}
	if v, _ := out[0]["email"].(string); v != "a@b.com" {
		t.Fatalf("expected the non-null row kept first, got %#v", out[0])
	}
	if out[1]["id"] != 2 {
		t.Fatalf("expected the row without an email column kept, got %#v", out[1])
	}
	for _, r := range out {
		if v, _ := r["email"].(string); v == "n/a" {
			t.Fatal("row was filled with the default value instead of dropped")
		}
	}
}

func TestBuilderVocabulary_NullHandleDefaultStillFills(t *testing.T) {
	ct := canonicalOne(t, "null_handle", map[string]any{
		"column": "email", "action": "default", "default_value": "n/a",
	})
	if s, _ := ct.Config["strategy"].(string); s != "default" {
		t.Fatalf("expected strategy=default, got %q", s)
	}
	out := applyOne(t, ct, []Row{{"email": nil}})
	if v, _ := out[0]["email"].(string); v != "n/a" {
		t.Fatalf("expected the default value to be filled in, got %#v", out[0]["email"])
	}
}

// The Select card stores "id, name, email". validateConfig always accepted the
// string (asStringSlice splits it), but applySelect rejected anything that was
// not a slice — so this saved 200 and then failed the run.
func TestBuilderVocabulary_SelectSplitsColumnString(t *testing.T) {
	ct := canonicalOne(t, "select", map[string]any{"columns": "id, email"})

	if _, isString := ct.Config["columns"].(string); isString {
		t.Fatal("columns reached the engine as a string")
	}
	out := applyOne(t, ct, []Row{{"id": 1, "email": "a@b.com", "secret": "x"}})
	if _, leaked := out[0]["secret"]; leaked {
		t.Fatalf("select kept a column it was not given: %v", out[0])
	}
	if len(out[0]) != 2 {
		t.Fatalf("expected 2 columns, got %v", out[0])
	}
}

func TestBuilderVocabulary_ExcludeSplitsColumnString(t *testing.T) {
	ct := canonicalOne(t, "exclude", map[string]any{"columns": "password, secret_key"})
	out := applyOne(t, ct, []Row{{"id": 1, "password": "p", "secret_key": "s"}})
	if _, leaked := out[0]["password"]; leaked {
		t.Fatalf("password survived an exclude: %v", out[0])
	}
	if _, leaked := out[0]["secret_key"]; leaked {
		t.Fatalf("secret_key survived an exclude: %v", out[0])
	}
}

// The builder still must not be able to save what no engine runs — the #1160
// guarantee this change must not weaken while it adds a new accepted verb.
func TestBuilderVocabulary_UnrunnableOperationsStillRejected(t *testing.T) {
	for _, op := range []string{"aggregate", "join", "enrich", "deduplicate", "sort", "limit", "sql", "python_udf"} {
		if _, _, err := NormalizeAndValidate(storedRule(op, map[string]any{"columns": "id"}), "", NormalizeModeExecution); err == nil {
			t.Fatalf("operation %q was accepted; no engine can run it", op)
		}
	}
}
