package transforms

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

// The transforms in this file all had the same shape of defect: the step wrote a
// column that already held something, and the old value was gone with no error,
// no warning and no change in row count. Each test below fails on the unfixed
// engine — most of them by asserting the destroyed value is still there.

// applyRaw runs one transform and returns rows + error, so a test can assert on
// the refusal itself rather than only on the output.
func applyRaw(data []Row, typ string, cfg map[string]interface{}) ([]Row, error) {
	return NewSimpleTransformEngine().Apply(context.Background(), data, Transform{Type: typ, Config: cfg})
}

// ---------------------------------------------------------------- T4 rename

// The rename walked its mapping table with a bare map range, so the result was a
// function of Go's per-process hash seed rather than of the input. 200 rows in
// one call exercise 200 independent map walks; before the fix this test fails
// within the first handful.
func TestRenameColumns_IsDeterministicAcrossChainedMappings(t *testing.T) {
	const n = 200
	data := make([]Row, n)
	for i := range data {
		data[i] = Row{"a": 1, "b": 2}
	}

	out, err := applyRaw(data, "rename_columns", map[string]interface{}{
		"mappings": map[string]interface{}{"a": "b", "b": "c"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Simultaneous rename: a's value lands in b, b's value lands in c, and no
	// value is consumed by the other mapping on the way.
	for i, row := range out {
		if len(row) != 2 || row["b"] != 1 || row["c"] != 2 {
			t.Fatalf("row %d = %#v, want map[b:1 c:2] — the rename is order-dependent", i, row)
		}
	}
}

// Renaming onto a column that is not itself renamed away destroys that column.
// There is no result that keeps both, so the chain is refused instead of silently
// picking one.
func TestRenameColumns_RefusesToOverwriteAnOccupiedColumn(t *testing.T) {
	_, err := applyRaw([]Row{{"a": 1, "b": 2}}, "rename_columns", map[string]interface{}{
		"mappings": map[string]interface{}{"a": "b"},
	})
	if err == nil {
		t.Fatal("renaming a onto an existing b succeeded; the original b was destroyed silently")
	}
	for _, want := range []string{`"a"`, `"b"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %s", err, want)
		}
	}
}

// Two sources, one destination: same refusal, and the message must not depend on
// which one the row walk happened to reach first.
func TestRenameColumns_CollisionMessageIsStable(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		_, err := applyRaw([]Row{{"a": 1, "b": 2}}, "rename_columns", map[string]interface{}{
			"mappings": map[string]interface{}{"a": "z", "b": "z"},
		})
		if err == nil {
			t.Fatal("two columns renamed onto z succeeded; one value was destroyed")
		}
		seen[err.Error()] = true
	}
	if len(seen) != 1 {
		t.Fatalf("collision message varies between runs: %d distinct messages", len(seen))
	}
}

// A rename with nothing in its way still works — the guard must not have turned
// every rename into an error.
func TestRenameColumns_PlainRenameStillWorks(t *testing.T) {
	out := applyStep(t, Transform{Type: "rename_columns", Config: map[string]interface{}{
		"mappings": map[string]interface{}{"a": "b"},
	}}, []Row{{"a": 1, "keep": "x"}})
	if len(out) != 1 || out[0]["b"] != 1 || out[0]["keep"] != "x" {
		t.Fatalf("out = %#v, want map[b:1 keep:x]", out[0])
	}
	if _, stale := out[0]["a"]; stale {
		t.Fatalf("source column survived the rename: %#v", out[0])
	}
}

// ---------------------------------------------------------------- T5 flatten

// The documented default prefix was "", which lifts every nested key to the top
// level — so flattening a metadata column overwrote the row's own primary key.
func TestJSONFlatten_DefaultPrefixDoesNotOverwriteTheTopLevel(t *testing.T) {
	out := applyStep(t, Transform{Type: "json_flatten", Config: map[string]interface{}{
		"column": "meta",
	}}, []Row{{"id": 7, "meta": map[string]interface{}{"id": 99}}})

	if got := out[0]["id"]; got != 7 {
		t.Fatalf("id = %#v, want 7 — the flatten overwrote the primary key", got)
	}
	if got := out[0]["meta_id"]; got != 99 {
		t.Fatalf("meta_id = %#v, want 99", got)
	}
}

// An explicit "" is a deliberate request for the flat namespace and is still
// honoured — but it can no longer destroy a column on the way.
func TestJSONFlatten_ExplicitFlatPrefixRefusesACollision(t *testing.T) {
	rows := []Row{{"id": 7, "meta": map[string]interface{}{"id": 99}}}

	_, err := applyRaw(rows, "json_flatten", map[string]interface{}{
		"column": "meta", "prefix": "",
	})
	if err == nil {
		t.Fatal("flattening onto an existing id succeeded; the row's own id was destroyed")
	}
	if !strings.Contains(err.Error(), "id") {
		t.Fatalf("error %q does not name the overwritten column", err)
	}

	// …and with no collision the flat namespace still works as asked.
	out := applyStep(t, Transform{Type: "json_flatten", Config: map[string]interface{}{
		"column": "meta", "prefix": "",
	}}, []Row{{"id": 7, "meta": map[string]interface{}{"kind": "x"}}})
	if out[0]["kind"] != "x" || out[0]["id"] != 7 {
		t.Fatalf("out = %#v, want map[id:7 kind:x]", out[0])
	}
}

// ------------------------------------------------------------ T6 arrayexpand

func TestArrayExpand_RefusesToOverwriteAnExistingIndexedColumn(t *testing.T) {
	_, err := applyRaw([]Row{{"tags": []interface{}{"a", "b"}, "tags_0": "keep"}},
		"array_expand", map[string]interface{}{"column": "tags"})
	if err == nil {
		t.Fatal("expanding over an existing tags_0 succeeded; that column was destroyed")
	}
	if !strings.Contains(err.Error(), "tags_0") {
		t.Fatalf("error %q does not name the overwritten column", err)
	}
}

func TestArrayExpand_PlainExpansionStillWorks(t *testing.T) {
	out := applyStep(t, Transform{Type: "array_expand", Config: map[string]interface{}{
		"column": "tags",
	}}, []Row{{"id": 1, "tags": []interface{}{"a", "b"}}})
	if out[0]["tags_0"] != "a" || out[0]["tags_1"] != "b" || out[0]["id"] != 1 {
		t.Fatalf("out = %#v", out[0])
	}
}

// ------------------------------------------------------------ T9 null_handle

// strategy=default filled a column that was absent from every row — which does
// not fill anything, it invents a column across the whole table and ships it to
// the destination. One typo in a column name became a schema change.
func TestNullHandleDefault_DoesNotFabricateAColumnNoRowHas(t *testing.T) {
	rows := []Row{{"id": 1, "status": "live"}, {"id": 2, "status": "live"}}
	tr := Transform{Type: "null_handle", Config: map[string]interface{}{
		"column": "stattus", "strategy": "default", "default_value": "unknown",
	}}

	out := applyStep(t, tr, rows)
	for i, row := range out {
		if _, invented := row["stattus"]; invented {
			t.Fatalf("row %d = %#v — the typo created a column on every row", i, row)
		}
	}

	// And the operator is told, rather than left with a rule that quietly did
	// nothing. The wording only becomes true once the branch above stops acting.
	warnings := MissingColumnWarnings(tr, rows)
	if len(warnings) != 1 || !strings.Contains(warnings[0], "stattus") {
		t.Fatalf("warnings = %v, want one naming stattus", warnings)
	}
	if !strings.Contains(warnings[0], "no effect") {
		t.Fatalf("warning %q no longer matches what the rule does", warnings[0])
	}
}

// The control: a column present on SOME rows is an ordinary sparse document, and
// filling the default on the rows that lack it is the entire point of the rule.
func TestNullHandleDefault_StillFillsASparseColumn(t *testing.T) {
	out := applyStep(t, Transform{Type: "null_handle", Config: map[string]interface{}{
		"column": "status", "strategy": "default", "default_value": "unknown",
	}}, []Row{{"id": 1, "status": "live"}, {"id": 2}, {"id": 3, "status": nil}})

	if out[0]["status"] != "live" {
		t.Fatalf("row 0 status = %#v, want live", out[0]["status"])
	}
	for _, i := range []int{1, 2} {
		if out[i]["status"] != "unknown" {
			t.Fatalf("row %d status = %#v, want unknown", i, out[i]["status"])
		}
	}
}

// ------------------------------------------------------------------ T10 mask

// Partial masking sliced the value by BYTE, cutting multi-byte characters in
// half and emitting the halves as replacement characters.
func TestPartialMask_SlicesRunesNotBytes(t *testing.T) {
	for _, in := range []string{"日本語テスト", "naïve-café-user", "🙂🙃😀😃😄😁"} {
		got, _ := applyMask(in, "partial", "").(string)
		if !utf8.ValidString(got) {
			t.Fatalf("mask(%q) = %q, which is not valid UTF-8", in, got)
		}
		if strings.ContainsRune(got, utf8.RuneError) {
			t.Fatalf("mask(%q) = %q — a character was cut in half", in, got)
		}
		r := []rune(in)
		want := string(r[:2]) + "***" + string(r[len(r)-2:])
		if got != want {
			t.Fatalf("mask(%q) = %q, want %q", in, got, want)
		}
	}
}

// The byte-length guard also mis-classified short non-ASCII values: two
// characters of a 3-byte script are 6 bytes, so "len > 4" sent them down the
// slice path, where the last two bytes are the tail of a single character.
func TestPartialMask_ShortNonASCIIValueIsFullyRedacted(t *testing.T) {
	got, _ := applyMask("日本", "partial", "").(string)
	if got != "***" {
		t.Fatalf("mask(日本) = %q, want *** — a 2-character value has no prefix to keep", got)
	}
}

// --------------------------------------------------------- T13 type_convert

// to=string used fmt.Sprintf("%v"), which prints GO syntax for objects and
// arrays: "map[a:1 b:x]" is not JSON and nothing downstream can read it back.
func TestTypeConvertToString_EmitsJSONNotGoSyntax(t *testing.T) {
	out := applyStep(t, Transform{Type: "type_convert", Config: map[string]interface{}{
		"column": "payload", "to": "string",
	}}, []Row{{"payload": map[string]interface{}{"a": float64(1), "b": "x"}}})

	s, ok := out[0]["payload"].(string)
	if !ok {
		t.Fatalf("payload = %#v, want a string", out[0]["payload"])
	}
	if strings.HasPrefix(s, "map[") {
		t.Fatalf("payload = %q — that is Go syntax, not JSON", s)
	}
	var back map[string]interface{}
	if err := json.Unmarshal([]byte(s), &back); err != nil {
		t.Fatalf("payload %q does not parse as JSON: %v", s, err)
	}
	if back["a"] != float64(1) || back["b"] != "x" {
		t.Fatalf("round-tripped to %#v", back)
	}
}

func TestTypeConvertToString_ScalarsAndBytesAreUnsurprising(t *testing.T) {
	cases := []struct {
		in   interface{}
		want string
	}{
		{42, "42"},
		{true, "true"},
		{"already", "already"},
		{[]byte("hello"), "hello"},
		{[]interface{}{float64(1), "x"}, `[1,"x"]`},
	}
	for _, c := range cases {
		out := applyStep(t, Transform{Type: "type_convert", Config: map[string]interface{}{
			"column": "v", "to": "string",
		}}, []Row{{"v": c.in}})
		if got := out[0]["v"]; got != c.want {
			t.Fatalf("convert(%#v) = %#v, want %q", c.in, got, c.want)
		}
	}
}

// A chain is the shape these actually ship in: flatten a payload, then rename one
// of the flattened columns onto a name the row already uses. The rename must
// refuse rather than quietly drop the original.
func TestChain_FlattenThenRenameOntoAnOccupiedColumnIsRefused(t *testing.T) {
	coord := NewTransformCoordinator(NewSimpleTransformEngine(), nil)
	_, _, err := coord.ApplyWithWarnings(context.Background(),
		[]Row{{"id": 7, "meta": map[string]interface{}{"id": 99}}},
		[]Transform{
			{Type: "json_flatten", Config: map[string]interface{}{"column": "meta"}},
			{Type: "rename_columns", Config: map[string]interface{}{
				"mappings": map[string]interface{}{"meta_id": "id"},
			}},
		})
	if err == nil {
		t.Fatal("the chain silently replaced the primary key with the nested one")
	}
	if !strings.Contains(err.Error(), "rename_columns") {
		t.Fatalf("error %q does not identify the failing step", err)
	}
}
