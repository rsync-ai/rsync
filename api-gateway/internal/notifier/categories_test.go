package notifier

import "testing"

// A catalog code nobody categorized falls into "other". It is still delivered,
// but it can no longer be muted on its own, and the admin who muted "Health &
// capacity" keeps getting it. Adding a code without a category must fail here.
func TestEveryCatalogCodeHasACategory(t *testing.T) {
	if len(catalog) == 0 {
		t.Fatal("catalog is empty; this test would pass vacuously")
	}
	for code := range catalog {
		got := CategoryFor(code, "")
		if placeholderCodes[code] {
			continue // the classifier gave up; "other" is the honest answer
		}
		if got == CategoryOther {
			t.Errorf("catalog code %s has no category; add it to codeCategory in categories.go", code)
		}
	}
}

func TestCategoryMapHasNoStaleCodes(t *testing.T) {
	for code, cat := range codeCategory {
		if _, ok := catalog[code]; !ok && code != codeSchemaChangeApplied {
			t.Errorf("codeCategory has %s, which is not a catalog code", code)
		}
		if !IsCategory(cat) {
			t.Errorf("codeCategory maps %s to unknown category %q", code, cat)
		}
	}
	for typ, cat := range typeCategory {
		if !IsCategory(cat) {
			t.Errorf("typeCategory maps %s to unknown category %q", typ, cat)
		}
	}
}

func TestCategoryFor(t *testing.T) {
	cases := []struct {
		code, eventType, want string
	}{
		{"RSYNC_BUG_SILENT_DROP", "", CategoryDataLoss},
		{"PIPELINE_RUN_FAILED", "cdc_wal_pressure", CategoryRunStatus}, // code wins
		{"", "CDC_WAL_PRESSURE", CategoryDataLoss},                     // type is case-insensitive
		{" SCHEMA_DRIFT_DETECTED ", "", CategorySchemaDrift},
		{codeSchemaChangeApplied, "", CategorySchemaDrift},
		{"NOT_A_CODE", "not_a_type", CategoryOther},
		{"", "", CategoryOther},
	}
	for _, tc := range cases {
		if got := CategoryFor(tc.code, tc.eventType); got != tc.want {
			t.Errorf("CategoryFor(%q, %q) = %q, want %q", tc.code, tc.eventType, got, tc.want)
		}
	}
}

func TestCategoriesAreUniqueAndReturnedByCopy(t *testing.T) {
	seen := map[string]bool{}
	for _, c := range Categories() {
		if seen[c.ID] {
			t.Errorf("duplicate category %s", c.ID)
		}
		seen[c.ID] = true
		if c.Label == "" || c.Description == "" {
			t.Errorf("category %s needs a label and description for the settings UI", c.ID)
		}
	}
	got := Categories()
	got[0].ID = "mutated"
	if Categories()[0].ID == "mutated" {
		t.Error("Categories() exposes the package slice")
	}
}

func TestMutedSetDropsUnknownIDs(t *testing.T) {
	m := mutedSet([]string{CategoryHealth, "retired_category", ""})
	if !m[CategoryHealth] || len(m) != 1 {
		t.Errorf("mutedSet = %v, want only %s", m, CategoryHealth)
	}
}
