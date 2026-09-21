package transforms

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Residual-plaintext guard
//
// A mask that matches nothing is indistinguishable from a mask that worked: the
// engine's contract is "mask if present". The guard closes that gap for a
// caller that must be sure the privacy guarantee took effect on the row shape
// it is about to write:
//
//	snap, err := SnapshotPlaintext(rows, canonical)  // BEFORE the chain runs
//	out, err := coordinator.Apply(ctx, rows, ...)
//	violations, err := VerifyNoResidualPlaintext(snap, out)
//
// The snapshot copies (in memory only) the scalars a mask target covers. For a
// column target (with or without deep) that is every scalar under a key named
// like the column, at any depth and in any case, so a top-level mask that
// no-ops on a packed row is still caught. For a path target it is only the
// scalars at that exact path (arrays on the way walked element by element, as
// the mask does), so the same key name elsewhere in the document, which the
// user did not ask to mask, is not collected. The verify step walks the
// output at any depth and reports each output PATH that still holds one of
// those values, including values moved by rename_columns or json_flatten.
// Neither function ever returns or formats a row value: violations and errors
// carry key paths and counts only.
//
// The snapshot is taken before the chain because a chain that mutates nested
// maps in place would mutate the "input" too, and a before/after comparison
// made afterwards would find nothing and pass vacuously.
//
// Floor (decision D-T7): strings shorter than 4 characters and numbers with
// fewer than 6 digits are not collected; booleans and nulls never are. That
// avoids false halts on enums, flags and small ids.
//
// Limits a caller must cover when wiring this in: the mask and the guard walk
// only map[string]interface{}, Row, []interface{} and []map[string]interface{};
// a value nested in any other container type (e.g. map[string]string) is
// neither masked nor snapshotted, so check MaskStats.Unmatched() as well.
// Violation paths are built from output map keys, and a document that uses
// data as keys would put that data in the path, so scrub paths before they
// reach a log or an LLM.

const (
	residualMinStringRunes  = 4
	residualMinNumberDigits = 6
	residualMaxListedPaths  = 20
)

// ErrResidualPlaintext is wrapped by VerifyNoResidualPlaintext when an output
// still holds a value that a mask was meant to hide.
var ErrResidualPlaintext = errors.New("residual plaintext under a masked field")

// MaskTarget is one thing an enabled mask_pii transform asks to mask.
type MaskTarget struct {
	// Column is the column leaf after the table qualifier is stripped; empty for
	// a path target.
	Column string `json:"column,omitempty"`
	// Path is the dotted nested path from config.path/paths; empty for a column
	// target.
	Path string `json:"path,omitempty"`
	// Deep is true when the column is matched at any depth.
	Deep bool `json:"deep,omitempty"`
}

// Leaf returns the key name this target masks: the column, or the last path
// segment.
func (t MaskTarget) Leaf() string {
	if t.Path != "" {
		return lastIdent(t.Path)
	}
	return t.Column
}

// MaskTargets lists the targets of every enabled mask_pii transform. A mask
// config that cannot be parsed is an error, never a silently dropped target.
func MaskTargets(canonical []CanonicalTransform) ([]MaskTarget, error) {
	out := make([]MaskTarget, 0)
	for i, ct := range canonical {
		if !ct.Enabled || normalizeType(ct.Type) != "mask_pii" {
			continue
		}
		cfg := ct.Config
		if cfg == nil {
			cfg = map[string]interface{}{}
		}
		spec, err := parseMaskSpec(cfg)
		if err != nil {
			return nil, fmt.Errorf("transform[%d] (mask_pii): %w", i, err)
		}
		for _, leaf := range spec.leaves {
			out = append(out, MaskTarget{Column: leaf, Deep: spec.deep})
		}
		for _, p := range spec.pathNames {
			out = append(out, MaskTarget{Path: p})
		}
	}
	return out, nil
}

// PlaintextSnapshot holds, in memory only, the input values a caller's masks
// must not leave in the output. Build it with SnapshotPlaintext.
type PlaintextSnapshot struct {
	// leaves are the lower-cased column-target names, collected at any depth.
	leaves map[string]struct{}
	// paths are the split path targets, collected only at that exact path.
	paths   [][]string
	values  map[string]struct{}
	targets int
}

// Targets is the number of mask targets the snapshot was built from.
func (s *PlaintextSnapshot) Targets() int { return s.targets }

// Values is the number of distinct values collected (a count, never the values).
func (s *PlaintextSnapshot) Values() int { return len(s.values) }

// SnapshotPlaintext collects the values under mask-target keys in the input
// rows. Call it BEFORE the transform chain runs. The collected values are
// copies, so later in-place mutation of the rows cannot empty the snapshot.
func SnapshotPlaintext(in []Row, canonical []CanonicalTransform) (*PlaintextSnapshot, error) {
	targets, err := MaskTargets(canonical)
	if err != nil {
		return nil, err
	}
	snap := &PlaintextSnapshot{
		leaves:  make(map[string]struct{}, len(targets)),
		values:  map[string]struct{}{},
		targets: len(targets),
	}
	for _, t := range targets {
		if t.Path != "" {
			snap.paths = append(snap.paths, strings.Split(t.Path, "."))
			continue
		}
		if leaf := strings.ToLower(t.Column); leaf != "" {
			snap.leaves[leaf] = struct{}{}
		}
	}
	if len(snap.leaves) == 0 && len(snap.paths) == 0 {
		return snap, nil
	}
	for _, row := range in {
		m := map[string]interface{}(row)
		if len(snap.leaves) > 0 {
			snap.collectMap(m)
		}
		for _, segs := range snap.paths {
			snap.collectAtPath(m, segs)
		}
	}
	return snap, nil
}

// collectAtPath records the scalars at segs beneath v, matching each segment
// exactly and walking arrays element by element, the way maskAtPath masks.
func (s *PlaintextSnapshot) collectAtPath(v interface{}, segs []string) {
	switch t := v.(type) {
	case map[string]interface{}:
		s.collectMapAtPath(t, segs)
	case Row:
		s.collectMapAtPath(map[string]interface{}(t), segs)
	case []interface{}:
		for _, el := range t {
			s.collectAtPath(el, segs)
		}
	case []map[string]interface{}:
		for _, el := range t {
			s.collectMapAtPath(el, segs)
		}
	}
}

func (s *PlaintextSnapshot) collectMapAtPath(m map[string]interface{}, segs []string) {
	child, ok := m[segs[0]]
	if !ok {
		return
	}
	if len(segs) == 1 {
		s.collectScalars(child)
		return
	}
	s.collectAtPath(child, segs[1:])
}

func (s *PlaintextSnapshot) collect(v interface{}) {
	switch t := v.(type) {
	case map[string]interface{}:
		s.collectMap(t)
	case Row:
		s.collectMap(map[string]interface{}(t))
	case []interface{}:
		for _, el := range t {
			s.collect(el)
		}
	case []map[string]interface{}:
		for _, el := range t {
			s.collectMap(el)
		}
	}
}

func (s *PlaintextSnapshot) collectMap(m map[string]interface{}) {
	for k, v := range m {
		if _, ok := s.leaves[strings.ToLower(k)]; ok {
			s.collectScalars(v)
			continue
		}
		s.collect(v)
	}
}

// collectScalars records every scalar in v's subtree that passes the floor.
func (s *PlaintextSnapshot) collectScalars(v interface{}) {
	switch t := v.(type) {
	case map[string]interface{}:
		for _, el := range t {
			s.collectScalars(el)
		}
	case Row:
		for _, el := range t {
			s.collectScalars(el)
		}
	case []interface{}:
		for _, el := range t {
			s.collectScalars(el)
		}
	case []map[string]interface{}:
		for _, el := range t {
			s.collectScalars(el)
		}
	default:
		if key, ok := residualKey(v, true); ok {
			s.values[key] = struct{}{}
		}
	}
}

// VerifyNoResidualPlaintext walks the output rows at any depth and returns the
// sorted, deduped PATHS (e.g. "document.email", "items[].email") whose value
// equals a snapshotted input value. A non-empty result also returns an error
// wrapping ErrResidualPlaintext. A nil snapshot is an error: the guard fails
// closed rather than passing a check it never ran.
func VerifyNoResidualPlaintext(snap *PlaintextSnapshot, out []Row) ([]string, error) {
	if snap == nil {
		return nil, errors.New("residual plaintext guard: nil snapshot (SnapshotPlaintext must run before the transform chain)")
	}
	if len(snap.values) == 0 {
		return nil, nil
	}
	found := map[string]struct{}{}
	for _, row := range out {
		snap.verifyMap(map[string]interface{}(row), "", found)
	}
	if len(found) == 0 {
		return nil, nil
	}
	paths := make([]string, 0, len(found))
	for p := range found {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	listed := paths
	suffix := ""
	if len(listed) > residualMaxListedPaths {
		listed = listed[:residualMaxListedPaths]
		suffix = fmt.Sprintf(" (+%d more)", len(paths)-residualMaxListedPaths)
	}
	return paths, fmt.Errorf("%w: %d output path(s): %s%s",
		ErrResidualPlaintext, len(paths), strings.Join(listed, ", "), suffix)
}

func (s *PlaintextSnapshot) verifyMap(m map[string]interface{}, prefix string, found map[string]struct{}) {
	for k, v := range m {
		s.verifyValue(v, prefix, k, found)
	}
}

// verifyValue checks v, which sits at prefix + "." + key. The path string is
// built only for containers and hits, so clean scalars cost no allocation.
func (s *PlaintextSnapshot) verifyValue(v interface{}, prefix, key string, found map[string]struct{}) {
	switch t := v.(type) {
	case map[string]interface{}:
		s.verifyMap(t, joinResidualPath(prefix, key), found)
	case Row:
		s.verifyMap(map[string]interface{}(t), joinResidualPath(prefix, key), found)
	case []interface{}:
		elemKey := key + "[]"
		for _, el := range t {
			s.verifyValue(el, prefix, elemKey, found)
		}
	case []map[string]interface{}:
		p := joinResidualPath(prefix, key+"[]")
		for _, el := range t {
			s.verifyMap(el, p, found)
		}
	default:
		k, ok := residualKey(v, false)
		if !ok {
			return
		}
		if _, hit := s.values[k]; hit {
			found[joinResidualPath(prefix, key)] = struct{}{}
		}
	}
}

func joinResidualPath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}

// residualKey returns the comparison form of a scalar: the string itself, or a
// number's decimal form (so 123456 and "123456" compare equal). Booleans, nil
// and other types are never compared. applyFloor enforces D-T7 on collection.
func residualKey(v interface{}, applyFloor bool) (string, bool) {
	var s string
	isNumber := false
	switch t := v.(type) {
	case string:
		s = t
	case []byte:
		s = string(t)
	case json.Number:
		if i, err := t.Int64(); err == nil {
			s = strconv.FormatInt(i, 10)
		} else if f, err := t.Float64(); err == nil {
			s = strconv.FormatFloat(f, 'f', -1, 64)
		} else {
			s = t.String()
		}
		isNumber = true
	case int:
		s, isNumber = strconv.FormatInt(int64(t), 10), true
	case int8:
		s, isNumber = strconv.FormatInt(int64(t), 10), true
	case int16:
		s, isNumber = strconv.FormatInt(int64(t), 10), true
	case int32:
		s, isNumber = strconv.FormatInt(int64(t), 10), true
	case int64:
		s, isNumber = strconv.FormatInt(t, 10), true
	case uint:
		s, isNumber = strconv.FormatUint(uint64(t), 10), true
	case uint8:
		s, isNumber = strconv.FormatUint(uint64(t), 10), true
	case uint16:
		s, isNumber = strconv.FormatUint(uint64(t), 10), true
	case uint32:
		s, isNumber = strconv.FormatUint(uint64(t), 10), true
	case uint64:
		s, isNumber = strconv.FormatUint(t, 10), true
	case float32:
		s, isNumber = strconv.FormatFloat(float64(t), 'f', -1, 32), true
	case float64:
		s, isNumber = strconv.FormatFloat(t, 'f', -1, 64), true
	default:
		return "", false
	}
	if s == "" {
		return "", false
	}
	if !applyFloor {
		return s, true
	}
	if isNumber {
		digits := 0
		for i := 0; i < len(s); i++ {
			if s[i] >= '0' && s[i] <= '9' {
				digits++
			}
		}
		return s, digits >= residualMinNumberDigits
	}
	return s, utf8.RuneCountInString(s) >= residualMinStringRunes
}
