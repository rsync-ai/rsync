package transforms

import (
	"context"
	"fmt"
	"strings"
)

// mask_pii targeting
//
// A mask_pii transform names what to mask in one of three ways:
//
//   - column / columns (default): TOP-LEVEL keys only, exact case. A dot in a
//     column name is a TABLE QUALIFIER ("users.email" masks the top-level key
//     "email"), never a nested path. This is the historical behavior and it is
//     unchanged byte for byte for every existing caller.
//   - column / columns + deep: true: the same leaf names, matched
//     case-insensitively at ANY depth (map values, and array elements). Built for
//     MongoDB CDC rows, which arrive packed as {_id, document:{...}}, where a
//     top-level mask never matches.
//   - path / paths: an explicit dotted path from the row root
//     ("document.profile.email"), exact case per segment. Arrays met along the
//     path are traversed element by element. Dots here are path separators; the
//     separate key is what keeps them from being read as a table qualifier.
//
// Nested masking is copy-on-write: only the maps and slices on the way to a
// masked value are copied. The caller's input rows are never mutated, which is
// what lets VerifyNoResidualPlaintext compare "before" with "after" honestly.

// MaskTargetStats is the match count for one mask target. It carries names and
// counts only, never row values.
type MaskTargetStats struct {
	// Target is the column leaf (after the table qualifier is stripped) or the
	// dotted path.
	Target string `json:"target"`
	// Kind is "column" (top-level), "deep" (any depth) or "path".
	Kind string `json:"kind"`
	// Matched is the number of values masked for this target across all rows.
	Matched int `json:"matched"`
}

// MaskStats reports how many values each target of one mask_pii transform
// matched, so a caller can see (and log) a mask that matched nothing.
type MaskStats struct {
	Rows    int               `json:"rows"`
	Targets []MaskTargetStats `json:"targets"`
}

// Unmatched returns the targets that matched zero values, in config order.
func (s MaskStats) Unmatched() []string {
	out := make([]string, 0)
	for _, t := range s.Targets {
		if t.Matched == 0 {
			out = append(out, t.Target)
		}
	}
	return out
}

type maskSpec struct {
	// leaves are the column leaves in config order, deduped.
	leaves []string
	// colIdx maps an exact leaf to its stats index (top-level mode).
	colIdx map[string]int
	// deep switches column targets to case-insensitive any-depth matching.
	deep bool
	// deepIdx maps a lower-cased leaf to its stats index (deep mode).
	deepIdx map[string]int
	// paths are the split `path`/`paths` targets; pathNames the dotted form.
	paths     [][]string
	pathNames []string
	maskType  string
	hashFunc  string
}

func parseMaskStrings(v interface{}) []string {
	out := make([]string, 0, 4)
	switch t := v.(type) {
	case string:
		s := strings.TrimSpace(t)
		if s != "" {
			out = append(out, s)
		}
	case []string:
		for _, it := range t {
			s := strings.TrimSpace(it)
			if s != "" {
				out = append(out, s)
			}
		}
	case []interface{}:
		for _, it := range t {
			s := strings.TrimSpace(fmt.Sprint(it))
			if s != "" {
				out = append(out, s)
			}
		}
	default:
		// ignore
	}
	return out
}

// parseMaskSpec reads a mask_pii config. Error texts for the historical
// column-only shapes are kept verbatim.
func parseMaskSpec(config map[string]interface{}) (*maskSpec, error) {
	// Accept either:
	// - config.column: "email"
	// - config.columns: ["email","phone"]
	cols := parseMaskStrings(config["columns"])
	if len(cols) == 0 {
		cols = parseMaskStrings(config["column"])
	}

	rawPaths := parseMaskStrings(config["paths"])
	if len(rawPaths) == 0 {
		rawPaths = parseMaskStrings(config["path"])
	}

	if len(cols) == 0 && len(rawPaths) == 0 {
		return nil, fmt.Errorf("mask_pii requires 'column' or 'columns' config")
	}

	spec := &maskSpec{
		colIdx:  map[string]int{},
		deepIdx: map[string]int{},
	}

	if raw, present := config["deep"]; present && raw != nil {
		b, ok := raw.(bool)
		if !ok {
			return nil, fmt.Errorf("mask_pii config.deep must be a boolean")
		}
		spec.deep = b
	}

	// Normalize: allow table-qualified column names (e.g. "users.email").
	for _, c := range cols {
		cc := c
		if parts := strings.Split(cc, "."); len(parts) > 1 {
			cc = parts[len(parts)-1]
		}
		cc = strings.TrimSpace(cc)
		if cc == "" {
			continue
		}
		if spec.deep {
			if _, dup := spec.deepIdx[strings.ToLower(cc)]; dup {
				continue
			}
			spec.deepIdx[strings.ToLower(cc)] = len(spec.leaves)
		} else {
			if _, dup := spec.colIdx[cc]; dup {
				continue
			}
			spec.colIdx[cc] = len(spec.leaves)
		}
		spec.leaves = append(spec.leaves, cc)
	}
	if len(cols) > 0 && len(spec.leaves) == 0 {
		return nil, fmt.Errorf("mask_pii requires non-empty column names")
	}

	seenPath := map[string]struct{}{}
	for _, p := range rawPaths {
		segs := strings.Split(p, ".")
		for i := range segs {
			segs[i] = strings.TrimSpace(segs[i])
			if segs[i] == "" {
				return nil, fmt.Errorf("mask_pii path %q has an empty segment", p)
			}
		}
		name := strings.Join(segs, ".")
		if _, dup := seenPath[name]; dup {
			continue
		}
		seenPath[name] = struct{}{}
		spec.paths = append(spec.paths, segs)
		spec.pathNames = append(spec.pathNames, name)
	}

	spec.maskType = "hash"
	if mt, ok := config["mask_type"].(string); ok {
		spec.maskType = mt
	}

	// hash_function selects the digest used when mask_type=hash.
	// Defaults to sha256 for backward compatibility.
	spec.hashFunc = "sha256"
	if hf, ok := config["hash_function"].(string); ok && strings.TrimSpace(hf) != "" {
		spec.hashFunc = strings.ToLower(strings.TrimSpace(hf))
	}
	return spec, nil
}

// ApplyMaskWithStats applies one mask_pii config and reports how many values
// each target matched. Apply(..., "mask_pii") returns the same rows and drops
// the stats.
func (e *SimpleTransformEngine) ApplyMaskWithStats(ctx context.Context, data []Row, config map[string]interface{}) ([]Row, MaskStats, error) {
	spec, err := parseMaskSpec(config)
	if err != nil {
		return nil, MaskStats{}, err
	}

	colKind := "column"
	if spec.deep {
		colKind = "deep"
	}
	counts := make([]int, len(spec.leaves)+len(spec.paths))

	result := make([]Row, len(data))
	for i, row := range data {
		newRow := make(Row)
		switch {
		case len(spec.leaves) == 0:
			for k, v := range row {
				newRow[k] = v
			}
		case !spec.deep:
			// Historical top-level path: exact key match, nested values shared.
			for k, v := range row {
				if idx, ok := spec.colIdx[k]; ok {
					newRow[k] = applyMask(v, spec.maskType, spec.hashFunc)
					counts[idx]++
				} else {
					newRow[k] = v
				}
			}
		default:
			for k, v := range row {
				if idx, ok := spec.deepIdx[strings.ToLower(k)]; ok {
					newRow[k] = applyMask(v, spec.maskType, spec.hashFunc)
					counts[idx]++
					continue
				}
				nv, _ := spec.maskDeepValue(v, counts)
				newRow[k] = nv
			}
		}

		for pi, segs := range spec.paths {
			if nv, n := spec.maskAtPath(map[string]interface{}(newRow), segs); n > 0 {
				newRow = Row(nv.(map[string]interface{}))
				counts[len(spec.leaves)+pi] += n
			}
		}
		result[i] = newRow
	}

	stats := MaskStats{Rows: len(data), Targets: make([]MaskTargetStats, 0, len(counts))}
	for i, leaf := range spec.leaves {
		stats.Targets = append(stats.Targets, MaskTargetStats{Target: leaf, Kind: colKind, Matched: counts[i]})
	}
	for pi, name := range spec.pathNames {
		stats.Targets = append(stats.Targets, MaskTargetStats{Target: name, Kind: "path", Matched: counts[len(spec.leaves)+pi]})
	}
	return result, stats, nil
}

// maskDeepValue masks every key matching a deep leaf beneath v. It returns v
// itself when nothing matched, and a copy (of only the changed containers)
// otherwise.
func (s *maskSpec) maskDeepValue(v interface{}, counts []int) (interface{}, bool) {
	switch t := v.(type) {
	case map[string]interface{}:
		return s.maskDeepMap(t, counts)
	case Row:
		nm, changed := s.maskDeepMap(map[string]interface{}(t), counts)
		if !changed {
			return t, false
		}
		return Row(nm), true
	case []interface{}:
		var cp []interface{}
		for i, el := range t {
			nv, changed := s.maskDeepValue(el, counts)
			if !changed {
				continue
			}
			if cp == nil {
				cp = make([]interface{}, len(t))
				copy(cp, t)
			}
			cp[i] = nv
		}
		if cp == nil {
			return t, false
		}
		return cp, true
	case []map[string]interface{}:
		var cp []map[string]interface{}
		for i, el := range t {
			nm, changed := s.maskDeepMap(el, counts)
			if !changed {
				continue
			}
			if cp == nil {
				cp = make([]map[string]interface{}, len(t))
				copy(cp, t)
			}
			cp[i] = nm
		}
		if cp == nil {
			return t, false
		}
		return cp, true
	}
	return v, false
}

func (s *maskSpec) maskDeepMap(src map[string]interface{}, counts []int) (map[string]interface{}, bool) {
	var cp map[string]interface{}
	for k, val := range src {
		var nv interface{}
		changed := false
		if idx, ok := s.deepIdx[strings.ToLower(k)]; ok {
			nv = applyMask(val, s.maskType, s.hashFunc)
			counts[idx]++
			changed = true
		} else {
			nv, changed = s.maskDeepValue(val, counts)
		}
		if !changed {
			continue
		}
		if cp == nil {
			cp = make(map[string]interface{}, len(src))
			for k2, v2 := range src {
				cp[k2] = v2
			}
		}
		cp[k] = nv
	}
	if cp == nil {
		return src, false
	}
	return cp, true
}

// maskAtPath masks the value at segs beneath v (copy-on-write) and returns the
// new value plus how many values were masked (more than one when the path
// crosses an array).
func (s *maskSpec) maskAtPath(v interface{}, segs []string) (interface{}, int) {
	switch t := v.(type) {
	case map[string]interface{}:
		nm, n := s.maskMapAtPath(t, segs)
		return nm, n
	case Row:
		nm, n := s.maskMapAtPath(map[string]interface{}(t), segs)
		if n == 0 {
			return t, 0
		}
		return Row(nm), n
	case []interface{}:
		var cp []interface{}
		total := 0
		for i, el := range t {
			nv, n := s.maskAtPath(el, segs)
			if n == 0 {
				continue
			}
			if cp == nil {
				cp = make([]interface{}, len(t))
				copy(cp, t)
			}
			cp[i] = nv
			total += n
		}
		if cp == nil {
			return t, 0
		}
		return cp, total
	case []map[string]interface{}:
		var cp []map[string]interface{}
		total := 0
		for i, el := range t {
			nm, n := s.maskMapAtPath(el, segs)
			if n == 0 {
				continue
			}
			if cp == nil {
				cp = make([]map[string]interface{}, len(t))
				copy(cp, t)
			}
			cp[i] = nm
			total += n
		}
		if cp == nil {
			return t, 0
		}
		return cp, total
	}
	return v, 0
}

func (s *maskSpec) maskMapAtPath(src map[string]interface{}, segs []string) (map[string]interface{}, int) {
	child, ok := src[segs[0]]
	if !ok {
		return src, 0
	}
	var nv interface{}
	n := 0
	if len(segs) == 1 {
		nv = applyMask(child, s.maskType, s.hashFunc)
		n = 1
	} else {
		nv, n = s.maskAtPath(child, segs[1:])
	}
	if n == 0 {
		return src, 0
	}
	cp := make(map[string]interface{}, len(src))
	for k, v := range src {
		cp[k] = v
	}
	cp[segs[0]] = nv
	return cp, n
}
