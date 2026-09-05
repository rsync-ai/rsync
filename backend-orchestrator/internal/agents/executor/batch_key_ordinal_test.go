package executor

import (
	"fmt"
	"testing"
)

// partKeyName mirrors the sink's object-name formatter
// (shared/mcp-connectors/internal/kafka-mcp-sink/worker-src/cmd/kafka-sink-worker/main.go
// partKey → fmt.Sprintf("part-%06d", offset), fed from sm.BatchOffset). It is
// duplicated here — rather than asserting on the ordinals alone — because the
// ordinals only matter insofar as they produce distinct destination objects,
// and that is the property the bug violated.
func partKeyName(keyOrdinal int) string {
	return fmt.Sprintf("part-%06d", keyOrdinal)
}

// runPages walks the export loop's paging arithmetic for a run of pages,
// returning the value published as `batch_offset` for each page (the emit
// happens BEFORE the advance, so the pre-advance ordinal is what ships).
// pageRows[i] < exportBatchSize terminates the sweep, exactly as the loop's
// short-page break does.
func runPages(startOffset, startKeyOrdinal int, keysetPaged bool, pageRows []int) (published []int, endOffset, endKeyOrdinal int) {
	offset, keyOrdinal := startOffset, startKeyOrdinal
	for _, n := range pageRows {
		published = append(published, keyOrdinal)
		offset, keyOrdinal = advancePage(offset, keyOrdinal, n, keysetPaged)
	}
	return published, offset, keyOrdinal
}

// A keyset-paged export publishes one batch_offset per page, and the sink turns
// that number straight into the destination object key. Before the fix `offset`
// served as both the source-query position and the batch identity; keyset
// paging pins the source position to 0, so every page shipped batch_offset=0
// and wrote part-000000 over its predecessor — while the run still reported
// `completed`. N pages must yield N distinct keys.
func TestKeysetPagingPublishesDistinctKeyOrdinals(t *testing.T) {
	pages := []int{1000, 1000, 1000, 1000, 250}
	published, _, _ := runPages(0, 0, true, pages)

	if len(published) != len(pages) {
		t.Fatalf("published %d batch_offsets for %d pages", len(published), len(pages))
	}

	seen := map[string]int{}
	for i, ord := range published {
		key := partKeyName(ord)
		if prev, dup := seen[key]; dup {
			t.Fatalf("page %d reuses object key %s already written by page %d (published=%v) — "+
				"this is the silent-overwrite bug: the later page destroys the earlier one's rows",
				i, key, prev, published)
		}
		seen[key] = i
		if i > 0 && ord <= published[i-1] {
			t.Fatalf("batch_offset must be strictly increasing: page %d got %d, page %d got %d (published=%v)",
				i, ord, i-1, published[i-1], published)
		}
	}

	// The ordinal is the row ordinal of the page's first row, so the values are
	// the running row totals — not merely "some distinct numbers".
	want := []int{0, 1000, 2000, 3000, 4000}
	for i := range want {
		if published[i] != want[i] {
			t.Fatalf("page %d published batch_offset=%d, want %d (published=%v)", i, published[i], want[i], published)
		}
	}
}

// The other half of the proof: offset-paged exports must be untouched. The two
// numbers share a seed and an increment there, so every published value — and
// therefore every object key already sitting in every customer's bucket — is
// byte-identical to what the pre-fix code emitted. A fix that renumbered these
// would orphan existing data.
func TestOffsetPagingPublishesUnchangedValues(t *testing.T) {
	pages := []int{1000, 1000, 400}

	published, endOffset, endKeyOrdinal := runPages(0, 0, false, pages)

	// What the pre-fix code published: `offset`, advanced the same way.
	legacy := []int{}
	off := 0
	for _, n := range pages {
		legacy = append(legacy, off)
		off += n
	}

	for i := range legacy {
		if published[i] != legacy[i] {
			t.Fatalf("offset paging changed published batch_offset at page %d: got %d, pre-fix emitted %d",
				i, published[i], legacy[i])
		}
	}
	if endOffset != endKeyOrdinal {
		t.Fatalf("under offset paging the two positions must stay in lockstep: offset=%d key_ordinal=%d",
			endOffset, endKeyOrdinal)
	}
}

// Under keyset paging the source-query offset must STAY pinned at 0 — the
// cursor positions the query, and a non-zero offset would skip rows. The fix
// must not accidentally resurrect it.
func TestKeysetPagingLeavesSourceOffsetPinned(t *testing.T) {
	_, endOffset, endKeyOrdinal := runPages(0, 0, true, []int{500, 500, 500})
	if endOffset != 0 {
		t.Fatalf("keyset paging must leave the source offset at 0, got %d", endOffset)
	}
	if endKeyOrdinal != 1500 {
		t.Fatalf("key_ordinal should have advanced to 1500, got %d", endKeyOrdinal)
	}
}

// A page whose rows are all dropped by transforms publishes nothing, but the
// rows were still consumed from the source. If the ordinal did not advance
// there, the NEXT page would ship the ordinal the filtered page would have
// used — harmless on its own, but it breaks the "ordinal = row offset"
// invariant the checkpoint and the ack ledger read.
func TestFullyFilteredPageStillAdvancesOrdinal(t *testing.T) {
	offset, keyOrdinal := 0, 0

	// Page 1 publishes at 0, then advances.
	offset, keyOrdinal = advancePage(offset, keyOrdinal, 1000, true)
	// Page 2: 1000 source rows, all filtered out — nothing published.
	offset, keyOrdinal = advancePage(offset, keyOrdinal, 1000, true)

	if keyOrdinal != 2000 {
		t.Fatalf("filtered page must still consume its rows: key_ordinal=%d, want 2000", keyOrdinal)
	}
	if offset != 0 {
		t.Fatalf("keyset source offset must stay pinned, got %d", offset)
	}
}

// Checkpoint resume. The `float64` types are not incidental: Position round-trips
// through JSON, so every number comes back as float64 — a `.(int)` assertion
// would silently fall through to the zero value and restart numbering at 0.
func TestSeedKeyOrdinal(t *testing.T) {
	cases := []struct {
		name        string
		position    map[string]interface{}
		startOffset int
		want        int
	}{
		{
			// Written by the fixed code.
			name:     "key_ordinal present wins",
			position: map[string]interface{}{"offset": float64(0), "key_ordinal": float64(4000)},
			want:     4000,
		},
		{
			// Pre-fix checkpoint from an OFFSET-paged table: the two were equal
			// by construction, so adopting offset resumes exactly where it left off.
			name:        "legacy offset-paged checkpoint adopts offset",
			position:    map[string]interface{}{"offset": float64(3000)},
			startOffset: 3000,
			want:        3000,
		},
		{
			// Pre-fix checkpoint from a KEYSET-paged table: the stored offset is
			// the frozen 0 that is the bug. Restarting numbering at 0 is the
			// documented one-time cost of the upgrade; it is monotonic after.
			name:     "legacy keyset checkpoint restarts from frozen zero",
			position: map[string]interface{}{"offset": float64(0)},
			want:     0,
		},
		{
			// A stored 0 is indistinguishable from absent, so it must not beat
			// a non-zero offset — matching how batch_idx/offset are seeded.
			name:        "zero key_ordinal falls back to offset",
			position:    map[string]interface{}{"offset": float64(2000), "key_ordinal": float64(0)},
			startOffset: 2000,
			want:        2000,
		},
		{
			name:     "empty position seeds zero",
			position: map[string]interface{}{},
			want:     0,
		},
		{
			// Defensive: a hand-edited or differently-typed checkpoint must not
			// panic the export.
			name:        "non-numeric key_ordinal ignored",
			position:    map[string]interface{}{"key_ordinal": "4000"},
			startOffset: 1000,
			want:        1000,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := seedKeyOrdinal(tc.position, tc.startOffset); got != tc.want {
				t.Fatalf("seedKeyOrdinal(%v, %d) = %d, want %d", tc.position, tc.startOffset, got, tc.want)
			}
		})
	}
}

// The second collision axis. `dt` in the object key is day-resolution and the
// table prefix carries no execution id, so a re-run on the same day lands in
// the same directory as the previous run. batch_idx and offset reset when a
// sweep completes (they are positions within a sweep); key_ordinal must NOT,
// because it is an identity. If it reset, the next incremental sweep's first
// page would reuse part-000000 and overwrite the previous sweep's — the same
// silent overwrite, one axis over.
func TestSecondSweepSameDayDoesNotReuseKeys(t *testing.T) {
	firstRun, _, endKeyOrdinal := runPages(0, 0, true, []int{1000, 1000, 400})

	// Sweep finished (short page). The executor resets batch_idx/offset/rows_so_far
	// on `table_complete` but deliberately carries key_ordinal forward via the
	// checkpoint — which is what seedKeyOrdinal reads on the next run.
	resumed := seedKeyOrdinal(map[string]interface{}{
		"offset":         float64(0), // reset by the table_complete branch
		"key_ordinal":    float64(endKeyOrdinal),
		"table_complete": true,
	}, 0)

	secondRun, _, _ := runPages(0, resumed, true, []int{1000, 300})

	seen := map[string]int{}
	for _, ord := range firstRun {
		seen[partKeyName(ord)] = 1
	}
	for i, ord := range secondRun {
		if key := partKeyName(ord); seen[key] == 1 {
			t.Fatalf("second sweep page %d reuses object key %s from the first sweep "+
				"(first=%v second=%v) — same-day re-run would overwrite the earlier run's data",
				i, key, firstRun, secondRun)
		}
	}
}
