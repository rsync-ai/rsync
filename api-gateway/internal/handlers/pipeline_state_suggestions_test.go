package handlers

import (
	"encoding/json"
	"testing"
)

func TestTableSuggestIsTableSelectionType(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"canonical", "table_selection", true},
		{"needs_tables alias", "needs_tables", true},
		{"needs_table_selection alias", "needs_table_selection", true},
		{"connection type", "needs_connections", false},
		{"empty", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTableSelectionType(tc.in); got != tc.want {
				t.Errorf("isTableSelectionType(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestTableSuggestBuildTableMetadataFromAvailable(t *testing.T) {
	// The executor parks with entries shaped {name, schema, row_count,
	// columns:<count>}; after the jsonb round-trip numbers arrive as float64.
	cases := []struct {
		name      string
		in        interface{}
		wantLen   int
		wantFirst TableMetadata
	}{
		{
			name: "executor park shape",
			in: []interface{}{
				map[string]interface{}{"name": "users", "schema": "public", "row_count": float64(100), "columns": float64(10)},
				map[string]interface{}{"name": "orders", "schema": "public", "row_count": float64(500), "columns": float64(18)},
			},
			wantLen:   2,
			wantFirst: TableMetadata{Name: "users", Schema: "public", RowCount: 100},
		},
		{
			name: "table alias only (normalizeAvailableTables output)",
			in: []interface{}{
				map[string]interface{}{"table": "events"},
			},
			wantLen:   1,
			wantFirst: TableMetadata{Name: "events"},
		},
		{
			name: "skips nameless and non-map entries",
			in: []interface{}{
				map[string]interface{}{"schema": "public", "row_count": float64(5)},
				"not-a-map",
				nil,
				map[string]interface{}{"name": "  ", "row_count": float64(5)},
				map[string]interface{}{"name": "kept"},
			},
			wantLen:   1,
			wantFirst: TableMetadata{Name: "kept"},
		},
		{
			name: "drops rsync-internal bookkeeping/staging tables (never ranked)",
			in: []interface{}{
				map[string]interface{}{"name": "_rsync_cdc_offsets", "schema": "pipeline_test"},
				map[string]interface{}{"name": "_rsync_pipelines", "schema": "pipeline_test"},
				map[string]interface{}{"name": "flat_mysql_1720000000", "schema": "public"},
				map[string]interface{}{"name": "demo_products", "schema": "pipeline_test", "row_count": float64(42)},
			},
			wantLen:   1,
			wantFirst: TableMetadata{Name: "demo_products", Schema: "pipeline_test", RowCount: 42},
		},
		{
			name: "all-internal input → empty (nothing to rank)",
			in: []interface{}{
				map[string]interface{}{"name": "_rsync_row_hash", "schema": "public"},
				map[string]interface{}{"name": "flat_pg_42"},
			},
			wantLen: 0,
		},
		{name: "nil input", in: nil, wantLen: 0},
		{name: "wrong type", in: "nope", wantLen: 0},
		{name: "empty array", in: []interface{}{}, wantLen: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildTableMetadataFromAvailable(tc.in)
			if len(got) != tc.wantLen {
				t.Fatalf("len = %d, want %d (got %+v)", len(got), tc.wantLen, got)
			}
			if tc.wantLen == 0 {
				return
			}
			first := got[0]
			if first.Name != tc.wantFirst.Name || first.Schema != tc.wantFirst.Schema || first.RowCount != tc.wantFirst.RowCount {
				t.Errorf("first = %+v, want %+v", first, tc.wantFirst)
			}
			if len(first.Columns) != 0 {
				t.Errorf("Columns should stay empty (only a count is available at park time), got %+v", first.Columns)
			}
		})
	}
}

func TestTableSuggestAsInt64(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want int64
	}{
		{"float64 (jsonb)", float64(42), 42},
		{"int64", int64(7), 7},
		{"int", 3, 3},
		{"json.Number", json.Number("99"), 99},
		{"json.Number non-int", json.Number("nan"), 0},
		{"string", "10", 0},
		{"nil", nil, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := asInt64(tc.in); got != tc.want {
				t.Errorf("asInt64(%v) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestTableSuggestSuggestedEntriesFromRecommendations(t *testing.T) {
	mkRecs := func(n int) []TableRecommendation {
		recs := make([]TableRecommendation, 0, n)
		for i := 0; i < n; i++ {
			recs = append(recs, TableRecommendation{
				Name:       "t" + string(rune('a'+i)),
				Schema:     "public",
				Reason:     "matches intent",
				Confidence: 0.9,
			})
		}
		return recs
	}

	t.Run("shape is {name, schema, reason, confidence}", func(t *testing.T) {
		got := suggestedEntriesFromRecommendations([]TableRecommendation{
			{Name: "users", Schema: "public", Reason: "user data", Confidence: 0.92, RowCount: 100, Category: "user_data", HasPII: true},
		}, maxSuggestedTables)
		if len(got) != 1 {
			t.Fatalf("len = %d, want 1", len(got))
		}
		e := got[0]
		if e["name"] != "users" || e["schema"] != "public" || e["reason"] != "user data" || e["confidence"] != 0.92 {
			t.Errorf("entry = %+v", e)
		}
		for _, extra := range []string{"row_count", "category", "has_pii", "key_columns"} {
			if _, ok := e[extra]; ok {
				t.Errorf("entry leaked extra field %q: %+v", extra, e)
			}
		}
	})

	t.Run("schema omitted when empty", func(t *testing.T) {
		got := suggestedEntriesFromRecommendations([]TableRecommendation{{Name: "events", Confidence: 0.5}}, maxSuggestedTables)
		if len(got) != 1 {
			t.Fatalf("len = %d, want 1", len(got))
		}
		if _, ok := got[0]["schema"]; ok {
			t.Errorf("empty schema should be omitted: %+v", got[0])
		}
	})

	t.Run("caps at limit", func(t *testing.T) {
		got := suggestedEntriesFromRecommendations(mkRecs(15), maxSuggestedTables)
		if len(got) != maxSuggestedTables {
			t.Errorf("len = %d, want %d", len(got), maxSuggestedTables)
		}
	})

	t.Run("skips nameless, nil for empty/zero-limit", func(t *testing.T) {
		if got := suggestedEntriesFromRecommendations([]TableRecommendation{{Name: "  "}}, 10); len(got) != 0 {
			t.Errorf("nameless rec should be skipped, got %+v", got)
		}
		if got := suggestedEntriesFromRecommendations(nil, 10); got != nil {
			t.Errorf("nil recs → nil, got %+v", got)
		}
		if got := suggestedEntriesFromRecommendations(mkRecs(2), 0); got != nil {
			t.Errorf("limit 0 → nil, got %+v", got)
		}
	})
}

func TestTableSuggestParsePersistedSuggestions(t *testing.T) {
	// Values arrive via a jsonb round-trip: maps and []interface{}.
	roundTrip := func(t *testing.T, v map[string]interface{}) map[string]interface{} {
		t.Helper()
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var out map[string]interface{}
		if err := json.Unmarshal(b, &out); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		return out
	}

	t.Run("absent key → not claimed", func(t *testing.T) {
		entries, status, claimed := parsePersistedSuggestions(map[string]interface{}{"other": 1})
		if claimed || status != "" || entries != nil {
			t.Errorf("got entries=%v status=%q claimed=%v", entries, status, claimed)
		}
	})

	t.Run("pending claim → claimed, no entries", func(t *testing.T) {
		meta := roundTrip(t, map[string]interface{}{
			aiSuggestionsMetaKey: map[string]interface{}{"status": "pending", "claimed_at": "2026-07-02T00:00:00Z"},
		})
		entries, status, claimed := parsePersistedSuggestions(meta)
		if !claimed || status != "pending" || len(entries) != 0 {
			t.Errorf("got entries=%v status=%q claimed=%v", entries, status, claimed)
		}
	})

	t.Run("failed result → claimed, no entries", func(t *testing.T) {
		meta := roundTrip(t, map[string]interface{}{
			aiSuggestionsMetaKey: map[string]interface{}{"status": "failed"},
		})
		entries, status, claimed := parsePersistedSuggestions(meta)
		if !claimed || status != "failed" || len(entries) != 0 {
			t.Errorf("got entries=%v status=%q claimed=%v", entries, status, claimed)
		}
	})

	t.Run("ready result → entries returned", func(t *testing.T) {
		meta := roundTrip(t, map[string]interface{}{
			aiSuggestionsMetaKey: map[string]interface{}{
				"status": "ready",
				"source": "llm",
				"suggestions": []interface{}{
					map[string]interface{}{"name": "users", "schema": "public", "reason": "r", "confidence": 0.9},
					map[string]interface{}{"name": "orders", "reason": "r2", "confidence": 0.8},
				},
			},
		})
		entries, status, claimed := parsePersistedSuggestions(meta)
		if !claimed || status != "ready" {
			t.Fatalf("status=%q claimed=%v", status, claimed)
		}
		if len(entries) != 2 || entries[0]["name"] != "users" || entries[1]["name"] != "orders" {
			t.Errorf("entries = %+v", entries)
		}
	})

	t.Run("ready result caps at maxSuggestedTables", func(t *testing.T) {
		suggestions := make([]interface{}, 0, maxSuggestedTables+5)
		for i := 0; i < maxSuggestedTables+5; i++ {
			suggestions = append(suggestions, map[string]interface{}{"name": "t", "confidence": 0.5})
		}
		meta := map[string]interface{}{
			aiSuggestionsMetaKey: map[string]interface{}{"status": "ready", "suggestions": suggestions},
		}
		entries, _, _ := parsePersistedSuggestions(meta)
		if len(entries) != maxSuggestedTables {
			t.Errorf("len = %d, want %d", len(entries), maxSuggestedTables)
		}
	})

	t.Run("malformed claim → claimed (never recompute in a loop)", func(t *testing.T) {
		for _, malformed := range []interface{}{"garbage", []interface{}{1}, 42} {
			entries, status, claimed := parsePersistedSuggestions(map[string]interface{}{aiSuggestionsMetaKey: malformed})
			if !claimed || status != "" || len(entries) != 0 {
				t.Errorf("malformed %v: got entries=%v status=%q claimed=%v", malformed, entries, status, claimed)
			}
		}
	})
}

func TestTableSuggestExtractIntentText(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]interface{}
		want string
	}{
		{"nil map", nil, ""},
		{"empty map", map[string]interface{}{}, ""},
		{"intent string", map[string]interface{}{"intent": "sync customer orders"}, "sync customer orders"},
		{"user_request", map[string]interface{}{"user_request": " move users to warehouse "}, "move users to warehouse"},
		{"prefers intent over description", map[string]interface{}{"intent": "a", "description": "b"}, "a"},
		{"whitespace-only skipped", map[string]interface{}{"intent": "  ", "description": "fallback"}, "fallback"},
		{
			"nested CachedIntent shape",
			map[string]interface{}{"intent": map[string]interface{}{"user_request": "replicate billing tables"}},
			"replicate billing tables",
		},
		{"non-string values ignored", map[string]interface{}{"intent": 42, "description": true}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := extractIntentText(tc.in); got != tc.want {
				t.Errorf("extractIntentText(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestTableSuggestAttachTableSuggestionsReusesPersistedResult(t *testing.T) {
	// A ready result persisted by an earlier poll must be surfaced verbatim
	// (as details.suggested_tables + suggestions_source) with the internal
	// bookkeeping key removed — no DB, no LLM involved on this path.
	details := map[string]interface{}{
		"available_tables": []interface{}{
			map[string]interface{}{"name": "users", "schema": "public", "row_count": float64(100)},
		},
		aiSuggestionsMetaKey: map[string]interface{}{
			"status": "ready",
			"source": "llm",
			"suggestions": []interface{}{
				map[string]interface{}{"name": "users", "schema": "public", "reason": "user data", "confidence": 0.92},
			},
		},
	}
	br := &BlockingReason{Type: "table_selection", Details: details}

	// database=nil is fine: the reuse branch returns before any DB access
	// would otherwise happen, and the nil guard covers the rest.
	attachTableSuggestions(nil, nil, "p1", br)

	if _, ok := details[aiSuggestionsMetaKey]; ok {
		t.Errorf("internal %q key should be stripped from client details", aiSuggestionsMetaKey)
	}
	if details["suggestions_source"] != "llm" {
		t.Errorf("suggestions_source = %v, want llm", details["suggestions_source"])
	}
	suggested, ok := details["suggested_tables"].([]map[string]interface{})
	if !ok || len(suggested) != 1 || suggested[0]["name"] != "users" {
		t.Errorf("suggested_tables = %+v", details["suggested_tables"])
	}
}

func TestTableSuggestAttachTableSuggestionsFailSoft(t *testing.T) {
	t.Run("pending claim → no suggestions injected", func(t *testing.T) {
		details := map[string]interface{}{
			aiSuggestionsMetaKey: map[string]interface{}{"status": "pending"},
		}
		attachTableSuggestions(nil, nil, "p1", &BlockingReason{Type: "table_selection", Details: details})
		if _, ok := details["suggested_tables"]; ok {
			t.Errorf("pending claim must not inject suggestions: %+v", details)
		}
		if _, ok := details[aiSuggestionsMetaKey]; ok {
			t.Errorf("internal key should still be stripped: %+v", details)
		}
	})

	t.Run("nil guards leave details untouched", func(t *testing.T) {
		attachTableSuggestions(nil, nil, "p1", nil)
		attachTableSuggestions(nil, nil, "p1", &BlockingReason{Type: "table_selection"})

		details := map[string]interface{}{"available_tables": []interface{}{}}
		attachTableSuggestions(nil, nil, "p1", &BlockingReason{Type: "table_selection", Details: details})
		if _, ok := details["suggested_tables"]; ok {
			t.Errorf("no tables → no suggestions: %+v", details)
		}
	})
}
