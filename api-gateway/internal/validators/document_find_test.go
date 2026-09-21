package validators

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func rawJSON(s string) json.RawMessage { return json.RawMessage(s) }

func TestValidateDocumentFind_Accepts(t *testing.T) {
	spec, ferr := ValidateDocumentFind(DocumentFindRequest{
		Database:   " crm ",
		Collection: "  orders ",
		Filter: rawJSON(`{"status":"paid","n":{"$gte":9007199254740993},"$or":[{"a":1},{"tags":{"$all":["x"]}}],
			"_id":{"$oid":"65f000000000000000000001"},"created":{"$gte":{"$date":"2026-01-01T00:00:00Z"}},
			"customer.email":{"$regex":"^u","$options":"i"},"price$usd":1}`),
		Projection: rawJSON(`{"n":1,"status":true,"_id":0}`),
		Sort:       rawJSON(`{"customer.tier":1,"n":-1}`),
		Limit:      9999,
		Skip:       20,
	})
	if ferr != nil {
		t.Fatalf("unexpected error: %+v", ferr)
	}
	if spec.Collection != "orders" {
		t.Errorf("collection not trimmed: %q", spec.Collection)
	}
	if spec.Database != "crm" {
		t.Errorf("database not trimmed: %q", spec.Database)
	}
	if spec.Limit != DocumentFindMaxLimit {
		t.Errorf("limit not clamped: %d", spec.Limit)
	}
	if !strings.Contains(string(spec.Filter), "9007199254740993") {
		t.Errorf("64-bit integer lost precision: %s", spec.Filter)
	}
	if spec.Projection["status"] != 1 || spec.Projection["_id"] != 0 {
		t.Errorf("projection not normalized: %v", spec.Projection)
	}
	got, _ := json.Marshal(spec.Sort)
	if string(got) != `[["customer.tier",1],["n",-1]]` {
		t.Errorf("sort order not preserved: %s", got)
	}
}

func TestValidateDocumentFind_Defaults(t *testing.T) {
	spec, ferr := ValidateDocumentFind(DocumentFindRequest{Collection: "c", Filter: rawJSON(`null`)})
	if ferr != nil {
		t.Fatalf("unexpected error: %+v", ferr)
	}
	if spec.Limit != DocumentFindDefaultLimit || spec.Filter != nil || spec.Sort != nil || spec.Projection != nil {
		t.Errorf("unexpected defaults: %+v", spec)
	}
	body, _ := json.Marshal(spec)
	if string(body) != `{"collection":"c","limit":50}` {
		t.Errorf("wire shape: %s", body)
	}

	// The list sort form is kept, and an _id-only sort is keyset (cursor allowed).
	spec, ferr = ValidateDocumentFind(DocumentFindRequest{Collection: "c", Sort: rawJSON(`[["_id",-1]]`), Cursor: `{"_id":{"$oid":"65f000000000000000000001"}}`})
	if ferr != nil {
		t.Fatalf("unexpected error: %+v", ferr)
	}
	if spec.Sort[0][0] != "_id" || spec.Sort[0][1] != -1 {
		t.Errorf("list sort: %v", spec.Sort)
	}
}

func TestValidateDocumentFind_Rejects(t *testing.T) {
	deep := strings.Repeat(`{"a":`, 21) + "1" + strings.Repeat("}", 21)
	cases := []struct {
		name     string
		req      DocumentFindRequest
		wantCode string
		wantPath string
	}{
		{"missing collection", DocumentFindRequest{Collection: "  "}, "invalid_collection", "collection"},
		{"dollar collection", DocumentFindRequest{Collection: "a$b"}, "invalid_collection", "collection"},
		{"system collection", DocumentFindRequest{Collection: "system.users"}, "invalid_collection", "collection"},
		{"long collection", DocumentFindRequest{Collection: strings.Repeat("x", 121)}, "invalid_collection", "collection"},
		{"$where top level", DocumentFindRequest{Collection: "c", Filter: rawJSON(`{"$where":"sleep(1)"}`)}, "operator_not_allowed", "filter.$where"},
		{"$expr top level", DocumentFindRequest{Collection: "c", Filter: rawJSON(`{"$expr":{"$function":{}}}`)}, "operator_not_allowed", "filter.$expr"},
		{"$where nested in $or", DocumentFindRequest{Collection: "c", Filter: rawJSON(`{"$or":[{"n":1},{"$where":"x"}]}`)}, "operator_not_allowed", "filter.$or[1].$where"},
		{"$function under field", DocumentFindRequest{Collection: "c", Filter: rawJSON(`{"n":{"$function":{}}}`)}, "operator_not_allowed", "filter.n.$function"},
		{"$oid top level", DocumentFindRequest{Collection: "c", Filter: rawJSON(`{"$oid":"x"}`)}, "operator_not_allowed", "filter.$oid"},
		{"wrapper with sibling", DocumentFindRequest{Collection: "c", Filter: rawJSON(`{"_id":{"$oid":"x","$where":"y"}}`)}, "invalid_filter", "filter._id.$oid"},
		{"filter array", DocumentFindRequest{Collection: "c", Filter: rawJSON(`[1]`)}, "invalid_filter", "filter"},
		{"filter bad json", DocumentFindRequest{Collection: "c", Filter: rawJSON(`{"a":`)}, "invalid_filter", "filter"},
		{"filter too deep", DocumentFindRequest{Collection: "c", Filter: rawJSON(deep)}, "filter_too_deep", ""},
		{"filter too large", DocumentFindRequest{Collection: "c", Filter: rawJSON(`{"a":"` + strings.Repeat("x", DocumentFindMaxFilterBytes) + `"}`)}, "filter_too_large", "filter"},
		{"projection mix", DocumentFindRequest{Collection: "c", Projection: rawJSON(`{"a":1,"b":0}`)}, "invalid_projection", "projection"},
		{"projection value 2", DocumentFindRequest{Collection: "c", Projection: rawJSON(`{"a":2}`)}, "invalid_projection", "projection.a"},
		{"projection float", DocumentFindRequest{Collection: "c", Projection: rawJSON(`{"a":1.0}`)}, "invalid_projection", "projection.a"},
		{"projection $ key", DocumentFindRequest{Collection: "c", Projection: rawJSON(`{"a.$":1}`)}, "invalid_projection", "projection.a.$"},
		{"projection array", DocumentFindRequest{Collection: "c", Projection: rawJSON(`[1]`)}, "invalid_projection", "projection"},
		{"sort too many", DocumentFindRequest{Collection: "c", Sort: rawJSON(`{"a":1,"b":1,"c":1,"d":1,"e":1,"f":1}`)}, "invalid_sort", "sort"},
		{"sort bool dir", DocumentFindRequest{Collection: "c", Sort: rawJSON(`{"a":true}`)}, "invalid_sort", "sort.a"},
		{"sort dir 2", DocumentFindRequest{Collection: "c", Sort: rawJSON(`[["a",2]]`)}, "invalid_sort", "sort.a"},
		{"sort dup", DocumentFindRequest{Collection: "c", Sort: rawJSON(`[["a",1],["a",-1]]`)}, "invalid_sort", "sort.a"},
		{"sort $ key", DocumentFindRequest{Collection: "c", Sort: rawJSON(`{"$natural":1}`)}, "invalid_sort", "sort.$natural"},
		{"sort bad pair", DocumentFindRequest{Collection: "c", Sort: rawJSON(`[["a"]]`)}, "invalid_sort", "sort[0]"},
		{"sort scalar", DocumentFindRequest{Collection: "c", Sort: rawJSON(`1`)}, "invalid_sort", "sort"},
		{"skip negative", DocumentFindRequest{Collection: "c", Sort: rawJSON(`{"a":1}`), Skip: -1}, "invalid_skip", "skip"},
		{"skip too big", DocumentFindRequest{Collection: "c", Sort: rawJSON(`{"a":1}`), Skip: DocumentFindMaxSkip + 1}, "invalid_skip", "skip"},
		{"skip with keyset", DocumentFindRequest{Collection: "c", Skip: 10}, "invalid_skip", "skip"},
		{"cursor with custom sort", DocumentFindRequest{Collection: "c", Sort: rawJSON(`{"a":1}`), Cursor: "x"}, "invalid_cursor", "cursor"},
		{"cursor too long", DocumentFindRequest{Collection: "c", Cursor: strings.Repeat("x", DocumentFindMaxCursorChars+1)}, "invalid_cursor", "cursor"},
		{"database too long", DocumentFindRequest{Database: strings.Repeat("d", DocumentFindMaxDatabaseBytes+1), Collection: "c"}, "invalid_database", "database"},
		{"database with a dot", DocumentFindRequest{Database: "a.b", Collection: "c"}, "invalid_database", "database"},
		{"database with $", DocumentFindRequest{Database: "$x", Collection: "c"}, "invalid_database", "database"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, ferr := ValidateDocumentFind(tc.req)
			if ferr == nil {
				t.Fatalf("expected %s, got nil", tc.wantCode)
			}
			if ferr.Code != tc.wantCode {
				t.Errorf("code = %s (%s), want %s", ferr.Code, ferr.Message, tc.wantCode)
			}
			if tc.wantPath != "" && ferr.Path != tc.wantPath {
				t.Errorf("path = %q, want %q", ferr.Path, tc.wantPath)
			}
		})
	}
}

// Error text names paths and operators, never a filter value.
func TestValidateDocumentFind_NoValueEcho(t *testing.T) {
	for _, f := range []string{
		`{"$where":"secretvalue"}`,
		`{"$or":[{"n":"secretvalue"},{"$where":"secretvalue"}]}`,
		`{"_id":{"$oid":"secretvalue","x":"secretvalue"}}`,
	} {
		_, ferr := ValidateDocumentFind(DocumentFindRequest{Collection: "c", Filter: rawJSON(f)})
		if ferr == nil {
			t.Fatalf("expected rejection for %s", f)
		}
		if strings.Contains(fmt.Sprintf("%s %s", ferr.Message, ferr.Path), "secretvalue") {
			t.Errorf("value echoed: %+v", ferr)
		}
	}
}

// Every operator the connector allows is allowed here, and nothing else — the
// lockstep with connector.py FIND_QUERY_OPERATORS.
func TestDocumentFindOperatorsMatchConnector(t *testing.T) {
	want := []string{"$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin", "$and", "$or", "$nor", "$not",
		"$exists", "$type", "$elemMatch", "$size", "$all", "$regex", "$options", "$mod"}
	if len(DocumentFindQueryOperators) != len(want) {
		t.Fatalf("operator count = %d, want %d", len(DocumentFindQueryOperators), len(want))
	}
	for _, op := range want {
		if !DocumentFindQueryOperators[op] {
			t.Errorf("missing %s", op)
		}
	}
}
