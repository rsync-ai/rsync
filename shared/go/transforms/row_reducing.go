package transforms

import "strings"

// SupportedTransformTypes is the canonical list of types the simple engine can
// run. CanHandle answers from it, and TestIsRowReducing_ClassifiesEverySupportedType
// iterates it, so a new transform type cannot be added without someone deciding
// whether it removes rows — which is the question the CDC delete path routes on.
func SupportedTransformTypes() []string {
	return []string{
		"filter",
		"mask_pii",
		"select_columns",
		"validate",
		"rename_columns",
		"exclude_columns",
		"null_handle",
		"truncate",
		"type_convert",
		"json_flatten",
		"array_expand",
	}
}

// IsRowReducing reports whether t can remove rows from the data it is given.
//
// The CDC delete path routes on this. A delete's Before image still has to be
// masked, renamed and reshaped like every other row that reaches the
// destination, but it must never be DROPPED: an upsert destination would keep
// the deleted row forever, and an append destination would lose the tombstone
// that says it ever went away. Neither leaves a trace.
//
// An unrecognized type answers true. It cannot occur on the CDC path — the
// sink's NormalizeAndValidate rejects any type no engine can run before this is
// consulted — but if it ever did, skipping the rule leaves the tombstone in its
// source shape, which is visible at the destination and fixable by replay,
// while applying a rule that turns out to drop rows destroys the delete with
// nothing to find afterwards.
func IsRowReducing(t Transform) bool {
	switch t.Type {
	case "filter", "validate":
		// filter selects rows; validate drops every row missing a required column.
		return true
	case "null_handle":
		return nullHandleStrategy(t.Config) == "drop_row"
	case "mask_pii", "select_columns", "exclude_columns", "rename_columns",
		"truncate", "type_convert", "json_flatten", "array_expand":
		// Column-shaping only: each rewrites, adds or removes COLUMNS and returns
		// exactly the rows it was given.
		return false
	default:
		return true
	}
}

// nullHandleStrategy resolves null_handle's strategy the one way applyNullHandle
// resolves it. Kept as a single function because IsRowReducing and the engine
// disagreeing about what "drop_row" means is exactly the drift that would make a
// delete vanish.
func nullHandleStrategy(config map[string]interface{}) string {
	if s, ok := config["strategy"].(string); ok && strings.TrimSpace(s) != "" {
		return strings.ToLower(strings.TrimSpace(s))
	}
	return "default"
}

// RenameMappings returns the source -> destination column mappings a
// rename_columns transform will apply, or nil for any other type.
//
// This exists so a caller can follow a rename into metadata that travels
// ALONGSIDE the rows — primary-key field names, most of all. The rows and the
// key fields have to be renamed by the same table or the destination is told to
// key on a column the rows no longer carry.
func RenameMappings(t Transform) map[string]string {
	if t.Type != "rename_columns" {
		return nil
	}
	m := parseRenameMappings(t.Config)
	if len(m) == 0 {
		return nil
	}
	return m
}

// RemapColumnNames returns names with each entry replaced by its mapping, if it
// has one. Order is preserved (key-field order is part of a composite key's
// identity) and the input slice is never mutated. A name with no mapping is
// returned unchanged, so a chain that renames one key column of three leaves
// the other two alone.
func RemapColumnNames(names []string, mappings map[string]string) []string {
	if len(names) == 0 || len(mappings) == 0 {
		return names
	}
	out := make([]string, len(names))
	for i, n := range names {
		if to, ok := mappings[lastIdent(strings.TrimSpace(n))]; ok {
			out[i] = to
			continue
		}
		out[i] = n
	}
	return out
}

// RemapMapKeys returns m with every key that has a mapping replaced by it. The
// map is copied rather than rewritten in place: a CDC message's PK map is the
// parsed Kafka record key, and mutating it would change what a redelivery of the
// same offset sees. A key with no mapping is carried through unchanged.
func RemapMapKeys(m map[string]interface{}, mappings map[string]string) map[string]interface{} {
	if len(m) == 0 || len(mappings) == 0 {
		return m
	}
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		if to, ok := mappings[lastIdent(strings.TrimSpace(k))]; ok {
			out[to] = v
			continue
		}
		out[k] = v
	}
	return out
}

// AccumulateRenameMappings folds every rename_columns step in a chain into a
// single source-name -> final-name table.
//
// A chain that renames a -> b and then b -> c has to answer a -> c. Metadata
// that travels alongside the rows (primary-key field names, the PK map's keys)
// is remapped once, after the whole chain has run, so it must land where the
// rows landed and not where the first step left them.
func AccumulateRenameMappings(chain []Transform) map[string]string {
	var acc map[string]string
	for _, t := range chain {
		step := RenameMappings(t)
		if len(step) == 0 {
			continue
		}
		if acc == nil {
			acc = make(map[string]string, len(step))
		}
		// Names an earlier step already moved: follow them on to their new name.
		// produced records what those earlier steps OUTPUT, so the second loop can
		// tell "a source column this step renames" from "a name an earlier step
		// created", which is no longer present under its original spelling.
		produced := make(map[string]bool, len(acc))
		for from, to := range acc {
			produced[to] = true
			if next, ok := step[to]; ok {
				acc[from] = next
			}
		}
		// Names this step is the first to touch.
		for from, to := range step {
			if _, seen := acc[from]; seen || produced[from] {
				continue
			}
			acc[from] = to
		}
	}
	return acc
}
