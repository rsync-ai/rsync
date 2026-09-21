package executor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestPostgresFamilyMatchesSharedGolden pins postgresFamilyTypes to
// shared/postgres_family_golden.json, which llm-service's
// CDCConfigGenerator.POSTGRES_FAMILY is pinned to as well. The executor forces
// publication-before-slot for a family member; the planner forces Debezium's
// publication.autocreate.mode to "disabled". A derivative added to one list and
// not the other gets one guard and not the second, and loses rows silently.
func TestPostgresFamilyMatchesSharedGolden(t *testing.T) {
	path := filepath.Join("..", "..", "..", "..", "shared", "postgres_family_golden.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	var golden struct {
		Members    []string          `json:"members"`
		Aliases    map[string]string `json:"aliases"`
		RawMembers []string          `json:"raw_members"`
		NonMembers []string          `json:"non_members"`
	}
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(golden.Members) == 0 || len(golden.NonMembers) == 0 || len(golden.RawMembers) == 0 {
		t.Fatal("golden fixture has an empty group")
	}

	var got []string
	for k := range postgresFamilyTypes {
		got = append(got, k)
	}
	want := append([]string{}, golden.Members...)
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("postgresFamilyTypes drifted from the shared golden\n got: %v\nwant: %v", got, want)
	}

	for alias, member := range golden.Aliases {
		if n := normalizeDBType(alias); n != member {
			t.Errorf("normalizeDBType(%q) = %q, want %q", alias, n, member)
		}
	}
	for _, s := range append(append([]string{}, golden.Members...), golden.RawMembers...) {
		if !isPostgresFamily(s) {
			t.Errorf("isPostgresFamily(%q) = false, want true", s)
		}
	}
	for _, s := range golden.NonMembers {
		if isPostgresFamily(s) {
			t.Errorf("isPostgresFamily(%q) = true, want false", s)
		}
	}
}
