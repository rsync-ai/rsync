package executor

import (
	"math"
	"strings"
)

// Connector-agnostic reading of an export page's paging fields. Every source
// connector reports its paging through the same few keys (`paging_mode`,
// `next_cursor`, `cursor_column`), but not every connector gives them the same
// meaning, and the table loop used to assume the keyset one for all of them.

// pageCursor is what one export page says about where the next page starts.
type pageCursor struct {
	// next is the keyset cursor to send back as `cursor`; nil when there is none.
	next interface{}
	// offsetPaged: the connector paged by OFFSET. Its `next_cursor` is the next
	// offset, not a key — sending it back as `cursor` pins `offset` to 0, so
	// every page re-read page one until the stuck-cursor guard ended the table
	// after a single page and the run still reported completed.
	offsetPaged bool
	// highWater is the largest key this page reached, for the PK high-water.
	// It is the keyset cursor, or the last row's `cursor_column` value when a
	// connector names its key column but leaves `next_cursor` off its final
	// (short) page — without it the next Resume's `since_cursor` stops one page
	// early and re-copies that page.
	highWater interface{}
}

func readPageCursor(res map[string]interface{}, rows []map[string]interface{}) pageCursor {
	mode, _ := res["paging_mode"].(string)
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "offset" {
		return pageCursor{offsetPaged: true}
	}
	pc := pageCursor{}
	if nc, ok := res["next_cursor"]; ok && nc != nil {
		pc.next = nc
		pc.highWater = nc
		return pc
	}
	if mode == "keyset" && len(rows) > 0 {
		if col, _ := res["cursor_column"].(string); strings.TrimSpace(col) != "" {
			if v, ok := rows[len(rows)-1][col]; ok && v != nil {
				pc.highWater = v
			}
		}
	}
	return pc
}

// maxExactJSONInt is the largest integer a float64 (how encoding/json decodes a
// JSON number) holds exactly. Past it two different keys can decode equal.
const maxExactJSONInt = 1 << 53

// dropRowsAtOrBelowSinceCursor removes rows whose key is at or below the
// previous sweep's PK high-water, for a connector that pages by key but ignores
// `since_cursor` and so re-exports every row on each Resume (duplicates in an
// object-store destination).
//
// Deliberately narrow, so it can only ever remove a row that a connector
// honoring `since_cursor` would not have returned either:
//   - only a keyset page that names its `cursor_column`;
//   - only for a pure PK delta: with an incremental `since` the delta is
//     `updated > since OR pk > since_cursor`, and an UPDATED old row legitimately
//     comes back below the high-water;
//   - only numeric keys, compared exactly. A string key's order is the source's
//     collation, which the executor cannot reproduce; any row it cannot compare
//     is kept (a duplicate is recoverable, a skipped row is not).
//
// For a connector that honors `since_cursor` it removes nothing.
func dropRowsAtOrBelowSinceCursor(rows []map[string]interface{}, res map[string]interface{}, since interface{}, incrementalSince string) ([]map[string]interface{}, int) {
	if since == nil || incrementalSince != "" || len(rows) == 0 {
		return rows, 0
	}
	if mode, _ := res["paging_mode"].(string); strings.ToLower(strings.TrimSpace(mode)) != "keyset" {
		return rows, 0
	}
	col, _ := res["cursor_column"].(string)
	if strings.TrimSpace(col) == "" {
		return rows, 0
	}
	floor, ok := exactJSONInt(since)
	if !ok {
		return rows, 0
	}
	kept := rows[:0:0]
	dropped := 0
	for _, r := range rows {
		if v, ok := exactJSONInt(r[col]); ok && v <= floor {
			dropped++
			continue
		}
		kept = append(kept, r)
	}
	if dropped == 0 {
		return rows, 0
	}
	return kept, dropped
}

// exactJSONInt reports a JSON-decoded number as an exact integer. Fractions and
// magnitudes a float64 cannot hold exactly report false.
func exactJSONInt(v interface{}) (int64, bool) {
	f, ok := cursorAsFloat(v)
	if !ok || math.IsNaN(f) || math.IsInf(f, 0) || f != math.Trunc(f) || math.Abs(f) >= maxExactJSONInt {
		return 0, false
	}
	return int64(f), true
}
