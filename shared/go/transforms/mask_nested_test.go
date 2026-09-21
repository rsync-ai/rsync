package transforms

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

var sha256MaskRe = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// legacyApplyMask is a verbatim copy of SimpleTransformEngine.applyMask as it was
// before nested targeting (origin/main e293a9e6, engine.go:168-250). The
// back-compat test pins the new implementation to it for every column-only
// config a current caller can send.
func legacyApplyMask(data []Row, config map[string]interface{}) ([]Row, error) {
	parseCols := func(v interface{}) []string {
		out := make([]string, 0, 4)
		switch t := v.(type) {
		case string:
			s := strings.TrimSpace(t)
			if s != "" {
				out = append(out, s)
			}
		case []string:
			for _, it := range t {
				s := strings.TrimSpace(it)
				if s != "" {
					out = append(out, s)
				}
			}
		case []interface{}:
			for _, it := range t {
				s := strings.TrimSpace(fmt.Sprint(it))
				if s != "" {
					out = append(out, s)
				}
			}
		}
		return out
	}
	cols := parseCols(config["columns"])
	if len(cols) == 0 {
		cols = parseCols(config["column"])
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("mask_pii requires 'column' or 'columns' config")
	}
	colSet := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		cc := c
		if parts := strings.Split(cc, "."); len(parts) > 1 {
			cc = parts[len(parts)-1]
		}
		cc = strings.TrimSpace(cc)
		if cc != "" {
			colSet[cc] = struct{}{}
		}
	}
	if len(colSet) == 0 {
		return nil, fmt.Errorf("mask_pii requires non-empty column names")
	}
	maskType := "hash"
	if mt, ok := config["mask_type"].(string); ok {
		maskType = mt
	}
	hashFunc := "sha256"
	if hf, ok := config["hash_function"].(string); ok && strings.TrimSpace(hf) != "" {
		hashFunc = strings.ToLower(strings.TrimSpace(hf))
	}
	result := make([]Row, len(data))
	for i, row := range data {
		newRow := make(Row)
		for k, v := range row {
			if _, ok := colSet[k]; ok {
				newRow[k] = applyMask(v, maskType, hashFunc)
			} else {
				newRow[k] = v
			}
		}
		result[i] = newRow
	}
	return result, nil
}

func maskFixtureRows() []Row {
	return []Row{
		{"id": 1, "email": "alice@example.com", "Email": "upper@example.com", "phone": "5551234567", "n": nil},
		{"id": 2, "email": "", "users": map[string]interface{}{"email": "nested@example.com"}},
		{"id": 3, "document": map[string]interface{}{"email": "packed@example.com"}, "ssn": 123456789},
		{"id": 4, "tags": []interface{}{"a", map[string]interface{}{"email": "arr@example.com"}}},
	}
}

func TestMaskPII_TopLevelBackCompat_ByteIdentical(t *testing.T) {
	e := NewSimpleTransformEngine()
	configs := []map[string]interface{}{
		{"column": "email"},
		{"column": "users.email"},
		{"columns": []interface{}{"email", "phone", "missing"}},
		{"columns": []string{"public.users.email", "ssn"}},
		{"column": "email", "mask_type": "redact"},
		{"column": "phone", "mask_type": "partial"},
		{"column": "email", "hash_function": "md5"},
		{"column": "email", "hash_function": "HMAC_SHA256"},
		{"column": "Email"},
		{"column": "document"},
		{"columns": []interface{}{}, "column": "ssn"},
		// error shapes
		{},
		{"column": "  "},
		{"column": "users."},
	}
	for i, cfg := range configs {
		cfg := cfg
		t.Run(fmt.Sprintf("cfg%d", i), func(t *testing.T) {
			want, wantErr := legacyApplyMask(maskFixtureRows(), cfg)
			got, gotErr := e.Apply(context.Background(), maskFixtureRows(), Transform{Type: "mask_pii", Config: cfg})
			if fmt.Sprint(wantErr) != fmt.Sprint(gotErr) {
				t.Fatalf("error changed: legacy=%v new=%v", wantErr, gotErr)
			}
			wb, _ := json.Marshal(want)
			gb, _ := json.Marshal(got)
			if string(wb) != string(gb) {
				t.Fatalf("output changed for %v:\nlegacy=%s\nnew   =%s", cfg, wb, gb)
			}
		})
	}

	// The top-level path still shares untouched nested values by reference, as
	// before (no new copies for existing callers).
	in := maskFixtureRows()
	out, err := e.Apply(context.Background(), in, Transform{Type: "mask_pii", Config: map[string]interface{}{"column": "email"}})
	if err != nil {
		t.Fatal(err)
	}
	if reflect.ValueOf(out[2]["document"]).Pointer() != reflect.ValueOf(in[2]["document"]).Pointer() {
		t.Fatalf("top-level mask must not copy untouched nested maps")
	}
}

func TestMaskPII_DeepMasksNestedDocumentKey(t *testing.T) {
	e := NewSimpleTransformEngine()
	in := []Row{{
		"_id": "65f0c0ffee",
		"document": map[string]interface{}{
			"email":   "alice@example.com",
			"name":    "Alice",
			"profile": map[string]interface{}{"EMAIL": "alice.alt@example.com"},
			"contacts": []interface{}{
				map[string]interface{}{"email": "bob@example.com"},
				"plain",
			},
		},
	}}
	out, stats, err := e.ApplyMaskWithStats(context.Background(), in, map[string]interface{}{"column": "email", "deep": true})
	if err != nil {
		t.Fatal(err)
	}
	doc := out[0]["document"].(map[string]interface{})
	for name, v := range map[string]interface{}{
		"document.email":             doc["email"],
		"document.profile.EMAIL":     doc["profile"].(map[string]interface{})["EMAIL"],
		"document.contacts[0].email": doc["contacts"].([]interface{})[0].(map[string]interface{})["email"],
	} {
		s, _ := v.(string)
		if !sha256MaskRe.MatchString(s) {
			t.Fatalf("%s not hashed: got a %T that does not match sha256:<64 hex>", name, v)
		}
	}
	if doc["name"] != "Alice" || doc["contacts"].([]interface{})[1] != "plain" || out[0]["_id"] != "65f0c0ffee" {
		t.Fatalf("unmatched values must pass through unchanged")
	}
	if len(stats.Targets) != 1 || stats.Targets[0].Kind != "deep" || stats.Targets[0].Matched != 3 {
		t.Fatalf("stats = %+v, want one deep target matched=3", stats)
	}
}

func TestMaskPII_DeepIsCopyOnWrite(t *testing.T) {
	e := NewSimpleTransformEngine()
	shared := map[string]interface{}{"city": "Paris"}
	in := []Row{{
		"document": map[string]interface{}{
			"email":   "alice@example.com",
			"address": shared,
			"list":    []interface{}{map[string]interface{}{"email": "bob@example.com"}},
		},
	}}
	before, _ := json.Marshal(in)
	for _, cfg := range []map[string]interface{}{
		{"column": "email", "deep": true},
		{"path": "document.email"},
		{"path": "document.list.email"},
	} {
		if _, err := e.Apply(context.Background(), in, Transform{Type: "mask_pii", Config: cfg}); err != nil {
			t.Fatal(err)
		}
		after, _ := json.Marshal(in)
		if string(before) != string(after) {
			t.Fatalf("mask_pii %v mutated its input rows in place", cfg)
		}
	}
	out, _ := e.Apply(context.Background(), in, Transform{Type: "mask_pii", Config: map[string]interface{}{"column": "email", "deep": true}})
	if reflect.ValueOf(out[0]["document"]).Pointer() == reflect.ValueOf(in[0]["document"]).Pointer() {
		t.Fatalf("a changed nested map must be a copy")
	}
	outAddr := out[0]["document"].(map[string]interface{})["address"]
	if reflect.ValueOf(outAddr).Pointer() != reflect.ValueOf(shared).Pointer() {
		t.Fatalf("an unchanged nested map should be shared, not copied")
	}
}

func TestMaskPII_PathMasksOnlyThatPath(t *testing.T) {
	e := NewSimpleTransformEngine()
	in := []Row{{
		"email": "top@example.com",
		"document": map[string]interface{}{
			"email":   "doc@example.com",
			"profile": map[string]interface{}{"email": "profile@example.com"},
			"items":   []interface{}{map[string]interface{}{"email": "i0@example.com"}, map[string]interface{}{"email": "i1@example.com"}},
		},
	}}
	out, stats, err := e.ApplyMaskWithStats(context.Background(), in, map[string]interface{}{
		"paths":     []interface{}{"document.profile.email", "document.items.email", "document.nope"},
		"mask_type": "redact",
	})
	if err != nil {
		t.Fatal(err)
	}
	doc := out[0]["document"].(map[string]interface{})
	if out[0]["email"] != "top@example.com" || doc["email"] != "doc@example.com" {
		t.Fatalf("path mask touched a key outside its path")
	}
	if doc["profile"].(map[string]interface{})["email"] != "***" {
		t.Fatalf("document.profile.email not masked")
	}
	for i, it := range doc["items"].([]interface{}) {
		if it.(map[string]interface{})["email"] != "***" {
			t.Fatalf("document.items[%d].email not masked", i)
		}
	}
	want := []MaskTargetStats{
		{Target: "document.profile.email", Kind: "path", Matched: 1},
		{Target: "document.items.email", Kind: "path", Matched: 2},
		{Target: "document.nope", Kind: "path", Matched: 0},
	}
	if !reflect.DeepEqual(stats.Targets, want) {
		t.Fatalf("stats = %+v, want %+v", stats.Targets, want)
	}
	if got := stats.Unmatched(); !reflect.DeepEqual(got, []string{"document.nope"}) {
		t.Fatalf("Unmatched = %v", got)
	}
}

func TestMaskPII_QualifierVsPathNotConfused(t *testing.T) {
	e := NewSimpleTransformEngine()
	rows := func() []Row {
		return []Row{{
			"email": "top@example.com",
			"users": map[string]interface{}{"email": "nested@example.com"},
		}}
	}

	// column "users.email": "users" is a TABLE qualifier -> top-level email only.
	out, err := e.Apply(context.Background(), rows(), Transform{Type: "mask_pii", Config: map[string]interface{}{"column": "users.email", "mask_type": "redact"}})
	if err != nil {
		t.Fatal(err)
	}
	if out[0]["email"] != "***" {
		t.Fatalf("qualified column must mask the top-level key")
	}
	if out[0]["users"].(map[string]interface{})["email"] != "nested@example.com" {
		t.Fatalf("qualified column must NOT be read as a nested path")
	}

	// path "users.email": a nested path -> nested email only.
	out, err = e.Apply(context.Background(), rows(), Transform{Type: "mask_pii", Config: map[string]interface{}{"path": "users.email", "mask_type": "redact"}})
	if err != nil {
		t.Fatal(err)
	}
	if out[0]["email"] != "top@example.com" {
		t.Fatalf("path must NOT be read as a table qualifier")
	}
	if out[0]["users"].(map[string]interface{})["email"] != "***" {
		t.Fatalf("path must mask the nested key")
	}
}

func TestMaskPII_StatsReportZeroMatches(t *testing.T) {
	e := NewSimpleTransformEngine()
	in := []Row{{"_id": "x", "document": map[string]interface{}{"email": "packed@example.com"}}}
	out, stats, err := e.ApplyMaskWithStats(context.Background(), in, map[string]interface{}{"columns": []interface{}{"users.email", "phone"}})
	if err != nil {
		t.Fatalf("an unmatched target stays a non-error in the library: %v", err)
	}
	if stats.Rows != 1 {
		t.Fatalf("Rows = %d", stats.Rows)
	}
	want := []MaskTargetStats{{Target: "email", Kind: "column", Matched: 0}, {Target: "phone", Kind: "column", Matched: 0}}
	if !reflect.DeepEqual(stats.Targets, want) {
		t.Fatalf("stats = %+v, want %+v", stats.Targets, want)
	}
	if got := stats.Unmatched(); !reflect.DeepEqual(got, []string{"email", "phone"}) {
		t.Fatalf("Unmatched = %v", got)
	}
	if out[0]["document"].(map[string]interface{})["email"] != "packed@example.com" {
		t.Fatalf("top-level mask must not reach into the packed document")
	}

	// A matching top-level key is counted once per row.
	_, stats, err = e.ApplyMaskWithStats(context.Background(), []Row{{"email": "a@example.com"}, {"email": nil}, {"id": 3}}, map[string]interface{}{"column": "email"})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Targets[0].Matched != 2 {
		t.Fatalf("Matched = %d, want 2 (present keys, including null)", stats.Targets[0].Matched)
	}
}

func TestMaskPII_ConfigErrors(t *testing.T) {
	e := NewSimpleTransformEngine()
	cases := []map[string]interface{}{
		{"column": "email", "deep": "yes"},
		{"path": "document..email"},
	}
	for _, cfg := range cases {
		if _, err := e.Apply(context.Background(), []Row{{"email": "a@example.com"}}, Transform{Type: "mask_pii", Config: cfg}); err == nil {
			t.Fatalf("config %v: expected an error", cfg)
		}
	}
	// path alone is a valid mask (no column required).
	if _, err := e.Apply(context.Background(), []Row{{"a": map[string]interface{}{"b": "x"}}}, Transform{Type: "mask_pii", Config: map[string]interface{}{"path": "a.b"}}); err != nil {
		t.Fatalf("path-only mask rejected: %v", err)
	}
}

func TestNormalizeAndValidate_MaskPIIDeepAndPath(t *testing.T) {
	ok := []map[string]any{
		{"type": "mask_pii", "config": map[string]any{"column": "email", "deep": true}},
		{"type": "mask_pii", "config": map[string]any{"path": "document.email"}},
		{"type": "mask_pii", "config": map[string]any{"paths": []any{"document.email", "document.phone"}}},
	}
	if _, _, err := NormalizeAndValidate(ok, "", NormalizeModeCDC); err != nil {
		t.Fatalf("valid nested mask configs rejected: %v", err)
	}
	for _, bad := range []map[string]any{
		{"type": "mask_pii", "config": map[string]any{"column": "email", "deep": "true"}},
		{"type": "mask_pii", "config": map[string]any{"path": "document."}},
		{"type": "mask_pii", "config": map[string]any{"deep": true}},
	} {
		if _, _, err := NormalizeAndValidate([]map[string]any{bad}, "", NormalizeModeCDC); err == nil {
			t.Fatalf("invalid mask config accepted: %v", bad)
		}
	}
}

func TestMaskTargets_ListsEnabledMasksOnly(t *testing.T) {
	got, err := MaskTargets([]CanonicalTransform{
		{Type: "mask_pii", Enabled: true, Config: map[string]any{"columns": []any{"users.email", "phone"}}},
		{Type: "mask_pii", Enabled: true, Config: map[string]any{"column": "ssn", "deep": true, "path": "document.card.number"}},
		{Type: "mask_pii", Enabled: false, Config: map[string]any{"column": "disabled"}},
		{Type: "rename_columns", Enabled: true, Config: map[string]any{"from": "a", "to": "b"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []MaskTarget{
		{Column: "email"},
		{Column: "phone"},
		{Column: "ssn", Deep: true},
		{Path: "document.card.number"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MaskTargets = %+v, want %+v", got, want)
	}
	if got[3].Leaf() != "number" || got[0].Leaf() != "email" {
		t.Fatalf("Leaf() wrong: %q %q", got[3].Leaf(), got[0].Leaf())
	}
	if _, err := MaskTargets([]CanonicalTransform{{Type: "mask_pii", Enabled: true, Config: map[string]any{}}}); err == nil {
		t.Fatalf("an unparseable mask must be an error, not a dropped target")
	}
}
