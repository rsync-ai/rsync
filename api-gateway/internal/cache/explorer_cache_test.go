package cache

import "testing"

func TestComputeSchemaHash_DeterministicOrdering(t *testing.T) {
	tablesA := []ExplorerTableIndex{
		{
			Name:   "users",
			Schema: "public",
			Columns: []ExplorerColumnIndex{
				{Name: "id", Type: "int", IsPrimaryKey: true},
				{Name: "email", Type: "text"},
			},
		},
		{
			Name:   "orders",
			Schema: "public",
			Columns: []ExplorerColumnIndex{
				{Name: "id", Type: "int", IsPrimaryKey: true},
				{Name: "user_id", Type: "int"},
			},
		},
	}

	// Same schema, different order (tables + columns)
	tablesB := []ExplorerTableIndex{
		{
			Name:   "orders",
			Schema: "public",
			Columns: []ExplorerColumnIndex{
				{Name: "user_id", Type: "int"},
				{Name: "id", Type: "int", IsPrimaryKey: true},
			},
		},
		{
			Name:   "users",
			Schema: "public",
			Columns: []ExplorerColumnIndex{
				{Name: "email", Type: "text"},
				{Name: "id", Type: "int", IsPrimaryKey: true},
			},
		},
	}

	h1 := ComputeSchemaHash(tablesA, nil)
	h2 := ComputeSchemaHash(tablesB, nil)
	if h1 != h2 {
		t.Fatalf("expected schema hash to be deterministic; got %q != %q", h1, h2)
	}
}

func TestComputeSchemaHash_ChangesOnTypeChange(t *testing.T) {
	base := []ExplorerTableIndex{
		{
			Name:   "users",
			Schema: "public",
			Columns: []ExplorerColumnIndex{
				{Name: "id", Type: "int", IsPrimaryKey: true},
				{Name: "email", Type: "text"},
			},
		},
	}
	changed := []ExplorerTableIndex{
		{
			Name:   "users",
			Schema: "public",
			Columns: []ExplorerColumnIndex{
				{Name: "id", Type: "int", IsPrimaryKey: true},
				{Name: "email", Type: "varchar"}, // type changed
			},
		},
	}

	h1 := ComputeSchemaHash(base, nil)
	h2 := ComputeSchemaHash(changed, nil)
	if h1 == h2 {
		t.Fatalf("expected schema hash to change when column type changes")
	}
}

func TestInferForeignKeysFromHeuristics_BasicCustomerID(t *testing.T) {
	tables := []ExplorerTableIndex{
		{
			Name:   "orders",
			Schema: "public",
			Columns: []ExplorerColumnIndex{
				{Name: "id", Type: "int", IsPrimaryKey: true},
				{Name: "customer_id", Type: "int"},
			},
		},
		{
			Name:   "customers",
			Schema: "public",
			Columns: []ExplorerColumnIndex{
				{Name: "id", Type: "int", IsPrimaryKey: true},
				{Name: "name", Type: "text"},
			},
		},
	}
	inferred := InferForeignKeysFromHeuristics(tables, nil)
	if len(inferred) == 0 {
		t.Fatalf("expected inferred foreign keys, got none")
	}
	// Expect an orders.customer_id -> customers.id edge.
	found := false
	for _, fk := range inferred {
		if fk.FromTable == "orders" && fk.FromColumn == "customer_id" && fk.ToTable == "customers" && fk.ToColumn == "id" {
			found = true
			if fk.Confidence >= 1.0 {
				t.Fatalf("expected inferred confidence < 1.0, got %.2f", fk.Confidence)
			}
		}
	}
	if !found {
		t.Fatalf("expected to find inferred orders.customer_id -> customers.id, got %#v", inferred)
	}
}

func TestInferForeignKeysFromHeuristics_DoesNotDuplicateExplicit(t *testing.T) {
	tables := []ExplorerTableIndex{
		{
			Name:   "orders",
			Schema: "public",
			Columns: []ExplorerColumnIndex{
				{Name: "id", Type: "int", IsPrimaryKey: true},
				{Name: "customer_id", Type: "int"},
			},
		},
		{
			Name:   "customers",
			Schema: "public",
			Columns: []ExplorerColumnIndex{
				{Name: "id", Type: "int", IsPrimaryKey: true},
			},
		},
	}
	explicit := []ExplorerForeignKeyIndex{
		{FromSchema: "public", FromTable: "orders", FromColumn: "customer_id", ToSchema: "public", ToTable: "customers", ToColumn: "id", Confidence: 1.0},
	}
	inferred := InferForeignKeysFromHeuristics(tables, explicit)
	for _, fk := range inferred {
		if fk.FromTable == "orders" && fk.FromColumn == "customer_id" && fk.ToTable == "customers" && fk.ToColumn == "id" {
			t.Fatalf("expected inferred to skip explicit FK duplicate, but found %#v", fk)
		}
	}
}

func TestExtractSearchTerms_StopWordsFiltered(t *testing.T) {
	terms := ExtractSearchTerms("Show me the total revenue by month for 2024")
	// Expect we keep signal words
	wantContains := map[string]bool{
		"total":   true,
		"revenue": true,
		"month":   true,
		"2024":    true,
	}

	found := make(map[string]bool)
	for _, tkn := range terms {
		found[tkn] = true
	}
	for w := range wantContains {
		if !found[w] {
			t.Fatalf("expected search terms to include %q; got %v", w, terms)
		}
	}

	// Expect we drop common stopwords
	if found["show"] || found["me"] || found["the"] || found["for"] || found["by"] {
		t.Fatalf("expected stopwords to be filtered; got %v", terms)
	}
}

func TestRetrieveTopTables_BasicScoring(t *testing.T) {
	index := &ExplorerSchemaIndex{
		ConnectionID: "c1",
		TableCount:   3,
		Tables: []ExplorerTableIndex{
			{
				Name:         "users",
				Schema:       "public",
				Columns:      []ExplorerColumnIndex{{Name: "id", Type: "int"}, {Name: "email", Type: "text"}},
				SearchTokens: "users user id email",
			},
			{
				Name:         "orders",
				Schema:       "public",
				Columns:      []ExplorerColumnIndex{{Name: "id", Type: "int"}, {Name: "total", Type: "decimal"}},
				SearchTokens: "orders order id total",
			},
			{
				Name:         "events",
				Schema:       "public",
				Columns:      []ExplorerColumnIndex{{Name: "id", Type: "int"}, {Name: "payload", Type: "json"}},
				SearchTokens: "events event payload",
			},
		},
	}

	// Include a shared term ("id") so we get multiple non-zero matches,
	// but "orders" should still be the strongest.
	top := RetrieveTopTables(index, []string{"orders", "id"}, 2)
	if len(top) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(top))
	}
	if top[0].Name != "orders" {
		t.Fatalf("expected 'orders' to be best match, got %q", top[0].Name)
	}
}

