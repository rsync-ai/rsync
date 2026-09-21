package executor

import "testing"

// The sink labels TABLE_STATS with "source_table" when present. A MongoDB
// collection is bare, so it must be qualified with the source database: the
// destination identifier ("demo.matches") is what the stats row showed before.
func TestSourceTableForStats(t *testing.T) {
	cases := []struct{ table, db, want string }{
		{"matches", "datingapp", "datingapp.matches"},
		{"public.customers", "shop", "public.customers"},
		{"matches", "", "matches"},
		{" matches ", " datingapp ", "datingapp.matches"},
		{"", "datingapp", ""},
	}
	for _, tc := range cases {
		if got := sourceTableForStats(tc.table, tc.db); got != tc.want {
			t.Errorf("sourceTableForStats(%q, %q) = %q, want %q", tc.table, tc.db, got, tc.want)
		}
	}
	// And it is never the destination identifier.
	if dest := resolveDestTableName("matches", "demo", true); dest == sourceTableForStats("matches", "datingapp") {
		t.Fatalf("source label equals destination identifier %q", dest)
	}
}
