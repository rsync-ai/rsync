package handlers

import (
	"encoding/json"
	"sort"
	"strings"
)

// IntentResolver — Phase 5 of the strategic plan.
//
// Maps a user's natural-language pipeline request to a concrete
// connector_type for source and/or destination. v1 is a deterministic
// keyword/alias index over the connectors installed on disk; v2 will
// swap in an LLM-backed resolver behind the same Intent interface.
//
// Why pattern-matching first:
//   - The keyword catalogue is the same data the connector's metadata
//     already declares (`aliases`, `display_name`, `id`).
//   - Pattern hit rates on well-known SaaS terms are ~95% without LLM.
//   - Falling back to LLM costs latency + $ and shouldn't run when a
//     deterministic rule already works.
//   - When pattern-matching DOES miss, we return a structured response
//     listing close candidates so the UI can render a picker.

// IntentMatch is the resolver's output for one side (source or
// destination). When MatchedConnectorType is empty, the caller treats
// it as ambiguous and surfaces Candidates as a picker.
type IntentMatch struct {
	MatchedConnectorType string
	Confidence           float64 // 0.0–1.0
	// Candidates is populated when MatchedConnectorType is empty OR when
	// confidence is below the auto-resolve threshold. Sorted by match
	// quality.
	Candidates []string
	// Rationale is a one-line explanation surfaced in HITL prompts.
	Rationale string
}

// ConnectorIndex provides the resolver with a list of connectors it
// can match against. Implementations: a real on-disk index, or a fake
// for tests. Returns one entry per installed connector.
type ConnectorIndex interface {
	List() []ConnectorEntry
}

// ConnectorEntry is the minimal metadata the resolver needs about an
// installed connector — its canonical id plus any aliases declared in
// metadata.json.
type ConnectorEntry struct {
	ID          string
	Aliases     []string
	DisplayName string
	Category    string
}

// IntentResolver is the v1 resolver. Construct via NewIntentResolver.
type IntentResolver struct {
	Index ConnectorIndex
}

// NewIntentResolver returns a resolver backed by the given index.
func NewIntentResolver(index ConnectorIndex) *IntentResolver {
	return &IntentResolver{Index: index}
}

// ResolveSource matches the request text to a source connector_type.
// "From X" / "sync X" / standalone "X" all hit the same matcher; v1
// doesn't try to be clever about ordering (Phase 5.5 will), it just
// scans the whole request text.
func (r *IntentResolver) ResolveSource(request string) IntentMatch {
	return r.resolve(request, true /*sourcePreference*/)
}

// ResolveDestination matches the request text to a destination
// connector. Heuristic: "to X" / "into X" / "destination X". When
// nothing matches the directional prepositions, falls back to a full
// scan and surfaces multiple candidates.
func (r *IntentResolver) ResolveDestination(request string) IntentMatch {
	return r.resolve(request, false)
}

// resolve does the actual matching. sourcePreference biases scoring
// when the same keyword appears both before "to" (source) and after.
func (r *IntentResolver) resolve(request string, sourcePreference bool) IntentMatch {
	if r == nil || r.Index == nil {
		return IntentMatch{Rationale: "no connector index configured"}
	}
	req := strings.ToLower(request)
	if strings.TrimSpace(req) == "" {
		return IntentMatch{Rationale: "empty request"}
	}

	entries := r.Index.List()
	type scored struct {
		entry ConnectorEntry
		score float64
	}
	var hits []scored

	// Split the request at directional markers. When a marker exists,
	// scope each side's match strictly to its own half — sources can
	// only match the pre-"to" segment, destinations only the post-"to"
	// segment. No marker → fall back to scanning the whole request.
	preTo, postTo := splitOnDirection(req)
	var haystack string
	if postTo == "" {
		haystack = req
	} else if sourcePreference {
		haystack = preTo
	} else {
		haystack = postTo
	}

	for _, e := range entries {
		keys := append([]string{e.ID}, e.Aliases...)
		if e.DisplayName != "" {
			keys = append(keys, e.DisplayName)
		}
		best := 0.0
		for _, key := range keys {
			k := strings.ToLower(strings.TrimSpace(key))
			if k == "" {
				continue
			}
			// Word-boundary match for short keys (≤4 chars) to avoid
			// matching "s3" inside "stripes3stuff"; substring for
			// longer keys.
			var s float64
			if len(k) <= 4 {
				if intentContainsWord(haystack, k) {
					s = 1.0
				}
			} else if strings.Contains(haystack, k) {
				s = 0.9
			}
			if s > best {
				best = s
			}
		}
		if best > 0 {
			hits = append(hits, scored{entry: e, score: best})
		}
	}
	if len(hits) == 0 {
		return IntentMatch{Rationale: "no installed connector matched the request"}
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].score > hits[j].score })

	// Auto-resolve when the top hit is a clear winner (score gap ≥ 0.2)
	// AND its absolute score is ≥ 0.8.
	top := hits[0]
	gap := 0.0
	if len(hits) > 1 {
		gap = top.score - hits[1].score
	} else {
		gap = 1.0
	}
	if top.score >= 0.8 && gap >= 0.2 {
		return IntentMatch{
			MatchedConnectorType: top.entry.ID,
			Confidence:           top.score,
			Rationale:            "exact alias/keyword match",
		}
	}
	// Otherwise return the candidate list.
	candidates := make([]string, 0, len(hits))
	for _, h := range hits {
		candidates = append(candidates, h.entry.ID)
		if len(candidates) >= 5 {
			break
		}
	}
	return IntentMatch{
		Candidates: candidates,
		Confidence: top.score,
		Rationale:  "multiple plausible connectors — pick one",
	}
}

// splitOnDirection splits a request at common directional prepositions
// ("to", "into", "→"). Returns (preTo, postTo). When no marker is
// found, returns (full, "").
func splitOnDirection(req string) (string, string) {
	for _, marker := range []string{" to ", " into ", " → ", " => ", " -> "} {
		if i := strings.Index(req, marker); i >= 0 {
			return req[:i], req[i+len(marker):]
		}
	}
	return req, ""
}

// intentContainsWord checks whether `key` appears as a word in `haystack`,
// where a word boundary is start/end of string OR a non-alphanumeric
// character. Avoids matching "s3" inside "stripe-s3-bridge".
func intentContainsWord(haystack, key string) bool {
	i := 0
	for {
		j := strings.Index(haystack[i:], key)
		if j < 0 {
			return false
		}
		idx := i + j
		// Boundary check before
		if idx > 0 {
			c := haystack[idx-1]
			if isAlphaNum(c) {
				i = idx + 1
				continue
			}
		}
		// Boundary check after
		end := idx + len(key)
		if end < len(haystack) {
			c := haystack[end]
			if isAlphaNum(c) {
				i = idx + 1
				continue
			}
		}
		return true
	}
}

func isAlphaNum(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// ── DiskConnectorIndex ────────────────────────────────────────────────────
// ConnectorIndex implementation backed by the on-disk MCP connector
// directory. Delegates to getConnectorIndex (TTL-cached) so repeated
// calls within a request cycle are cheap.

type diskConnectorIndex struct {
	root string
}

func newDiskConnectorIndex() *diskConnectorIndex {
	return &diskConnectorIndex{root: connectorsRoot()}
}

func (d *diskConnectorIndex) List() []ConnectorEntry {
	idx := getConnectorIndex(d.root)
	entries := make([]ConnectorEntry, 0, len(idx.scanned))
	for _, sc := range idx.scanned {
		e := parseConnectorEntry(sc)
		if e.ID != "" {
			entries = append(entries, e)
		}
	}
	return entries
}

// parseConnectorEntry extracts IntentResolver metadata from a scannedConnector.
// Falls back to the dir name when metadata.json has no aliases.
func parseConnectorEntry(sc scannedConnector) ConnectorEntry {
	type meta struct {
		ID          string   `json:"id"`
		Name        string   `json:"name"`
		DisplayName string   `json:"display_name"`
		Aliases     []string `json:"aliases"`
		Category    string   `json:"category"`
	}
	var m meta
	if len(sc.Metadata) > 0 {
		_ = json.Unmarshal(sc.Metadata, &m)
	}
	id := m.ID
	if id == "" {
		id = sc.DirName
	}
	aliases := m.Aliases
	if len(aliases) == 0 && m.Name != "" {
		aliases = []string{m.Name}
	}
	display := m.DisplayName
	if display == "" {
		display = m.Name
	}
	return ConnectorEntry{
		ID:          id,
		Aliases:     aliases,
		DisplayName: display,
		Category:    m.Category,
	}
}

// newDiskIntentResolver returns an IntentResolver backed by the on-disk
// connector catalogue. Cheap to call — the underlying index is TTL-cached.
func newDiskIntentResolver() *IntentResolver {
	return NewIntentResolver(newDiskConnectorIndex())
}
