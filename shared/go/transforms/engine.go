package transforms

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Row represents a single data row (map of column name to value)
type Row map[string]interface{}

// Transform represents a transform configuration
type Transform struct {
	Type   string                 `json:"type"`
	Config map[string]interface{} `json:"config"`
}

// TransformEngine applies transforms to data
type TransformEngine interface {
	Apply(ctx context.Context, data []Row, transform Transform) ([]Row, error)
	CanHandle(transformType string) bool
}

// TransformCoordinator routes transforms to the appropriate engine
type TransformCoordinator struct {
	tier1Engine TransformEngine
	tier2Engine TransformEngine // For future DuckDB engine
}

// NewTransformCoordinator creates a new coordinator
func NewTransformCoordinator(tier1 TransformEngine, tier2 TransformEngine) *TransformCoordinator {
	return &TransformCoordinator{
		tier1Engine: tier1,
		tier2Engine: tier2,
	}
}

// Apply applies a list of transforms sequentially, discarding the non-fatal
// warnings. Callers that can show a warning to a user should call
// ApplyWithWarnings instead.
func (c *TransformCoordinator) Apply(ctx context.Context, data []Row, transforms []Transform) ([]Row, error) {
	result, _, err := c.ApplyWithWarnings(ctx, data, transforms)
	return result, err
}

// ApplyWithWarnings applies a list of transforms sequentially and returns the
// non-fatal warnings raised along the way.
//
// The column check runs against the rows ENTERING each step, so a column that an
// earlier rename_columns / select_columns / exclude_columns removed is caught
// exactly, with no need to model those transforms' semantics here.
func (c *TransformCoordinator) ApplyWithWarnings(ctx context.Context, data []Row, transforms []Transform) ([]Row, []string, error) {
	result := data
	warnings := []string{}

	for i, transform := range transforms {
		for _, w := range MissingColumnWarnings(transform, result) {
			warnings = append(warnings, fmt.Sprintf("transform %d (%s): %s", i, transform.Type, w))
		}

		select {
		case <-ctx.Done():
			return nil, warnings, fmt.Errorf("transform cancelled at step %d: %w", i, ctx.Err())
		default:
		}

		var engine TransformEngine
		var err error

		// Route to appropriate engine
		if c.tier1Engine != nil && c.tier1Engine.CanHandle(transform.Type) {
			engine = c.tier1Engine
		} else if c.tier2Engine != nil && c.tier2Engine.CanHandle(transform.Type) {
			engine = c.tier2Engine
		} else {
			return nil, warnings, fmt.Errorf("no engine available for transform type: %s", transform.Type)
		}

		result, err = engine.Apply(ctx, result, transform)
		if err != nil {
			return nil, warnings, fmt.Errorf("transform %d (%s) failed: %w", i, transform.Type, err)
		}
	}

	return result, warnings, nil
}

// MissingColumnWarnings reports each column a transform names that is absent
// from every row given it. Such a rule cannot do what the operator asked.
//
// This used to be gated to null_handle, because that is the rule whose missing
// column was found deleting whole datasets
// (KI-NULL-HANDLE-MISSING-COLUMN-DROPS-EVERY-ROW). The gate was the bug, not the
// check: filter, validate and select_columns had the same defect and the same
// symptom, and the warning that would have named it was sitting three lines
// away, switched off for them. Every column-naming rule is covered now.
//
// Pass the rows ENTERING the rule, not the original input: that is what catches
// a column an earlier rename_columns / select_columns / exclude_columns removed,
// with no need to model those transforms' semantics. An empty input is not
// evidence of anything, so it raises nothing, and neither does a column that at
// least one row carries — a sparse document is normal.
//
// The message carries no position, because only the caller knows one: a chain
// passed whole to ApplyWithWarnings has step indices, while the batch executor
// and the CDC sink feed rules in one at a time and number them by Order.
func MissingColumnWarnings(transform Transform, data []Row) []string {
	if len(data) == 0 {
		return nil
	}
	var out []string
	for _, col := range columnsReferenced(transform) {
		present := false
		for _, row := range data {
			if _, ok := row[col]; ok {
				present = true
				break
			}
		}
		if present {
			continue
		}
		out = append(out, fmt.Sprintf(
			"column %q is not present in any input row, so the rule has no effect. Check for a rename_columns, select_columns or exclude_columns earlier in the chain that renamed or removed it.",
			col))
	}
	return out
}

// columnsReferenced names the columns a transform's config reads, deduplicated
// and in config order.
//
// mask_pii is deliberately absent: its targets can be nested paths and `deep`
// wildcards, so a top-level name missing from the rows is not evidence of a
// mismatch. Its own accounting lives in mask_guard.go, which is the thing that
// must not go quiet — an unmatched mask is a PII leak, not a no-op.
func columnsReferenced(t Transform) []string {
	var raw []string
	switch t.Type {
	case "filter":
		cond, _ := t.Config["condition"].(string)
		raw = conditionColumns(cond)
	case "validate":
		raw = configStrings(t.Config["required_columns"])
	case "select_columns", "exclude_columns":
		raw = configStrings(t.Config["columns"])
	case "rename_columns":
		raw = renameSources(t.Config)
	case "null_handle", "truncate", "type_convert", "json_flatten", "array_expand":
		if col, ok := t.Config["column"].(string); ok {
			raw = []string{col}
		}
	default:
		return nil
	}

	seen := map[string]bool{}
	out := []string{}
	for _, c := range raw {
		c = lastIdent(strings.TrimSpace(c))
		if c == "" || seen[c] {
			continue
		}
		seen[c] = true
		out = append(out, c)
	}
	return out
}

// renameSources lists the columns a rename_columns rule reads, from either
// config shape (a mappings map, or a single from/to pair).
func renameSources(config map[string]interface{}) []string {
	out := []string{}
	switch v := config["mappings"].(type) {
	case map[string]string:
		for from := range v {
			out = append(out, from)
		}
	case map[string]interface{}:
		for from := range v {
			out = append(out, from)
		}
	}
	if from, ok := config["from"].(string); ok && strings.TrimSpace(from) != "" {
		out = append(out, from)
	}
	// Map iteration order is random, so a warning list built from one would come
	// out shuffled between runs and read as flapping in a log.
	sort.Strings(out)
	return out
}

// configStrings reads a list of column names from a config value, accepting both
// []string and the []interface{} that JSON decoding yields.
func configStrings(v interface{}) []string {
	switch list := v.(type) {
	case []string:
		return list
	case []interface{}:
		out := make([]string, 0, len(list))
		for _, item := range list {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// presentColumns reports which of cols at least one row carries.
func presentColumns(cols []string, data []Row) map[string]bool {
	present := map[string]bool{}
	for _, row := range data {
		for _, c := range cols {
			if _, ok := row[c]; ok {
				present[c] = true
			}
		}
	}
	return present
}

// SimpleTransformEngine implements Tier 1 transforms (filter, mask_pii, select_columns, validate)
type SimpleTransformEngine struct{}

// NewSimpleTransformEngine creates a new simple transform engine
func NewSimpleTransformEngine() *SimpleTransformEngine {
	return &SimpleTransformEngine{}
}

// CanHandle returns true if this engine can handle the transform type
func (e *SimpleTransformEngine) CanHandle(transformType string) bool {
	for _, t := range SupportedTransformTypes() {
		if t == transformType {
			return true
		}
	}
	return false
}

// Apply applies a simple transform
func (e *SimpleTransformEngine) Apply(ctx context.Context, data []Row, transform Transform) ([]Row, error) {
	switch transform.Type {
	case "filter":
		return e.applyFilter(ctx, data, transform.Config)
	case "mask_pii":
		return e.applyMask(ctx, data, transform.Config)
	case "select_columns":
		return e.applySelect(ctx, data, transform.Config)
	case "validate":
		return e.applyValidate(ctx, data, transform.Config)
	case "rename_columns":
		return e.applyRenameColumns(ctx, data, transform.Config)
	case "exclude_columns":
		return e.applyExcludeColumns(ctx, data, transform.Config)
	case "null_handle":
		return e.applyNullHandle(ctx, data, transform.Config)
	case "truncate":
		return e.applyTruncate(ctx, data, transform.Config)
	case "type_convert":
		return e.applyTypeConvert(ctx, data, transform.Config)
	case "json_flatten":
		return e.applyJSONFlatten(ctx, data, transform.Config)
	case "array_expand":
		return e.applyArrayExpand(ctx, data, transform.Config)
	default:
		return nil, fmt.Errorf("unsupported transform type: %s", transform.Type)
	}
}

// applyFilter filters rows based on a condition
func (e *SimpleTransformEngine) applyFilter(ctx context.Context, data []Row, config map[string]interface{}) ([]Row, error) {
	condition, ok := config["condition"].(string)
	if !ok || condition == "" {
		return data, nil // No filter condition, return all data
	}

	// A column that NO row carries cannot produce a filter result, only an
	// unsatisfiable comparison. `status = 'active'` after an earlier step renamed
	// or dropped `status` used to match zero rows and empty the batch — HTTP 200,
	// no error, no warning, destination silently truncated. It now passes its
	// rows through untouched, which is what every other rule in this engine does
	// with a missing column (applyNullHandle, applyTruncate, applyTypeConvert,
	// applyJSONFlatten), and MissingColumnWarnings names the column.
	//
	// The test is deliberately "absent from every row", not "absent from this
	// row": a column missing from SOME rows is an ordinary sparse document, and
	// a non-match there is correct SQL semantics, kept in evaluateLeaf.
	//
	// Conservative on purpose. `a = 1 OR b = 2` with `b` gone would still filter
	// correctly on `a`, and passing every row through is looser than that — but
	// the alternative direction deletes data, and a filter that lets too much
	// through is visible at the destination and recoverable by re-running.
	if cols := conditionColumns(condition); len(cols) > 0 {
		present := presentColumns(cols, data)
		if len(present) < len(cols) && len(data) > 0 {
			return data, nil
		}
	}

	result := make([]Row, 0, len(data))

	// A condition that cannot be evaluated (unknown operator/format) is a hard
	// error, never a silent drop: an unparseable condition errors on every row,
	// and swallowing that error would discard the whole dataset while reporting
	// success. Fail loudly so the caller (executor / sink worker) surfaces it.
	for _, row := range data {
		matches, err := evaluateCondition(row, condition)
		if err != nil {
			return nil, fmt.Errorf("filter condition %q could not be evaluated: %w", condition, err)
		}
		if matches {
			result = append(result, row)
		}
	}

	return result, nil
}

// applyMask masks PII fields. The implementation, including nested (`deep` /
// `path`) targeting and per-target match accounting, lives in mask_nested.go.
func (e *SimpleTransformEngine) applyMask(ctx context.Context, data []Row, config map[string]interface{}) ([]Row, error) {
	out, _, err := e.ApplyMaskWithStats(ctx, data, config)
	return out, err
}

// applySelect selects specific columns
func (e *SimpleTransformEngine) applySelect(ctx context.Context, data []Row, config map[string]interface{}) ([]Row, error) {
	columnsInterface, ok := config["columns"]
	if !ok {
		return data, nil // No column selection, return all data
	}

	columns := make([]string, 0)
	switch v := columnsInterface.(type) {
	case []string:
		columns = v
	case []interface{}:
		for _, col := range v {
			if colStr, ok := col.(string); ok {
				columns = append(columns, colStr)
			}
		}
	case string:
		// "id, name, email" — the shape the Transform Builder stores.
		// normalizeConfigAliases (validate.go) splits it before the engine ever
		// sees it, but applyExcludeColumns has always accepted the string form
		// directly and the asymmetry was itself the bug: a select saved cleanly
		// (validateConfig splits a string too) and then failed the run here.
		for _, part := range strings.Split(v, ",") {
			if s := strings.TrimSpace(part); s != "" {
				columns = append(columns, s)
			}
		}
	default:
		return nil, fmt.Errorf("invalid columns config type")
	}

	if len(columns) == 0 {
		return data, nil
	}

	// A selection where NOT ONE requested column exists emits {} for every row.
	// The row COUNT is preserved, so every row-count invariant in the executor,
	// the sink and the UI reads healthy while the batch carries no data at all —
	// which is how a single typo in a column name became the quietest failure in
	// the engine. Pass the rows through instead and let MissingColumnWarnings
	// name the columns.
	//
	// A selection where SOME requested columns exist is left alone: that is a
	// sparse document, and dropping the absent names from the projection is the
	// right answer.
	if len(data) > 0 {
		keys := make([]string, 0, len(columns))
		for _, col := range columns {
			keys = append(keys, lastIdent(col))
		}
		if len(presentColumns(keys, data)) == 0 {
			return data, nil
		}
	}

	result := make([]Row, len(data))
	for i, row := range data {
		newRow := make(Row)
		for _, col := range columns {
			// Allow table-qualified column names (e.g. "users.email")
			key := col
			if parts := strings.Split(col, "."); len(parts) > 1 {
				key = parts[len(parts)-1]
			}
			if val, exists := row[key]; exists {
				// Preserve the original row key (usually unqualified).
				newRow[key] = val
			}
		}
		result[i] = newRow
	}

	return result, nil
}

// applyValidate validates rows and filters out invalid ones
func (e *SimpleTransformEngine) applyValidate(ctx context.Context, data []Row, config map[string]interface{}) ([]Row, error) {
	// Accept both []string and []interface{} (JSON decoding commonly yields []interface{}).
	requiredColumns := make([]string, 0)
	switch v := config["required_columns"].(type) {
	case []string:
		requiredColumns = v
	case []interface{}:
		for _, c := range v {
			if s, ok := c.(string); ok && s != "" {
				requiredColumns = append(requiredColumns, s)
			}
		}
	}
	if len(requiredColumns) == 0 {
		return data, nil
	}

	// A required column absent from EVERY row is a configuration mismatch, not a
	// dataset where every row is invalid. Enforcing it drops the whole batch
	// silently — the same shape as the filter and select_columns defects above.
	// The requirement is skipped and MissingColumnWarnings names it; the other
	// required columns are still enforced, so one stale name in a list of five
	// no longer disables the other four AND no longer empties the table.
	if len(data) > 0 {
		keys := make([]string, 0, len(requiredColumns))
		for _, col := range requiredColumns {
			keys = append(keys, lastIdent(col))
		}
		present := presentColumns(keys, data)
		kept := make([]string, 0, len(requiredColumns))
		for _, col := range requiredColumns {
			if present[lastIdent(col)] {
				kept = append(kept, col)
			}
		}
		requiredColumns = kept
		if len(requiredColumns) == 0 {
			return data, nil
		}
	}

	result := make([]Row, 0)
	for _, row := range data {
		valid := true
		for _, col := range requiredColumns {
			// Allow table-qualified column names (e.g. "users.email")
			key := col
			if parts := strings.Split(col, "."); len(parts) > 1 {
				key = parts[len(parts)-1]
			}
			val, exists := row[key]
			if !exists || val == nil {
				valid = false
				break
			}
		}
		if valid {
			result = append(result, row)
		}
	}

	return result, nil
}

// parseRenameMappings extracts the source -> destination column table from a
// rename_columns config, accepting every shape the API and the UI have emitted:
// a mappings object (typed or JSON-decoded), a mappings array of {from,to}, or a
// bare from/to pair. Identity and half-empty entries are dropped, and names are
// unqualified, so "users.email" and "email" mean the same column.
//
// Split out of applyRenameColumns so the CDC path can rename a message's KEY
// FIELDS through exactly the same table that renames its rows. Two parsers would
// eventually disagree, and the way that shows up is a destination told to key on
// a column its rows no longer carry.
func parseRenameMappings(config map[string]interface{}) map[string]string {
	mappings := map[string]string{}

	// Preferred: mappings map
	if raw, ok := config["mappings"]; ok {
		switch v := raw.(type) {
		case map[string]string:
			for k, to := range v {
				from := strings.TrimSpace(k)
				to = strings.TrimSpace(to)
				if from == "" || to == "" || from == to {
					continue
				}
				mappings[lastIdent(from)] = lastIdent(to)
			}
		case map[string]interface{}:
			for k, it := range v {
				from := strings.TrimSpace(k)
				to, _ := it.(string)
				to = strings.TrimSpace(to)
				if from == "" || to == "" || from == to {
					continue
				}
				mappings[lastIdent(from)] = lastIdent(to)
			}
		case []interface{}:
			for _, it := range v {
				m, ok := it.(map[string]interface{})
				if !ok || m == nil {
					continue
				}
				from, _ := m["from"].(string)
				to, _ := m["to"].(string)
				from = strings.TrimSpace(from)
				to = strings.TrimSpace(to)
				if from == "" || to == "" || from == to {
					continue
				}
				mappings[lastIdent(from)] = lastIdent(to)
			}
		}
	}

	// Fallback: from/to
	if len(mappings) == 0 {
		from, _ := config["from"].(string)
		to, _ := config["to"].(string)
		from = strings.TrimSpace(from)
		to = strings.TrimSpace(to)
		if from != "" && to != "" && from != to {
			mappings[lastIdent(from)] = lastIdent(to)
		}
	}

	return mappings
}

func (e *SimpleTransformEngine) applyRenameColumns(ctx context.Context, data []Row, config map[string]interface{}) ([]Row, error) {
	mappings := parseRenameMappings(config)
	if len(mappings) == 0 {
		return data, nil
	}

	// SIMULTANEOUS rename, computed from the SOURCE row.
	//
	// This used to copy the row and then apply each mapping to the copy, walking
	// `mappings` with a bare map range — so the result depended on Go's randomized
	// map order, redrawn for every row. With {a->b, b->c} on {a:1, b:2}, the order
	// (a->b, b->c) destroys b's value and yields {c:1}, while (b->c, a->b) yields
	// {b:1, c:2}. Six runs in one process gave {c:1} four times and {b:1,c:2} twice:
	// the same rule, the same input, two different destination tables, chosen by a
	// hash seed. A chain that reshapes a primary key this way splits the partition
	// the same way #1155 did.
	//
	// Reading each source column once and writing it to its destination name makes
	// the mapping a function of the input alone: {a->b, b->c} is always {b:1, c:2},
	// which is also what SQL and every dataframe library mean by a rename.
	result := make([]Row, len(data))
	for i, row := range data {
		newRow := make(Row, len(row))
		// dest column -> the source column that already claimed it.
		claimed := make(map[string]string, len(row))
		for k, v := range row {
			dst := k
			if to, ok := mappings[k]; ok {
				dst = to
			}
			if prev, taken := claimed[dst]; taken {
				// Two source columns want one destination name. Silently letting
				// the last writer win is how a rename deleted a column nobody
				// asked to lose; there is no answer here that keeps both, so the
				// chain is refused instead of guessing. Sorted so the message is
				// the same every run even though the row walk is not.
				a, b := prev, k
				if b < a {
					a, b = b, a
				}
				return nil, fmt.Errorf(
					"rename_columns: columns %q and %q would both become %q, which would destroy one of them; exclude or rename the other column first",
					a, b, dst)
			}
			claimed[dst] = k
			newRow[dst] = v
		}
		result[i] = newRow
	}
	return result, nil
}

func (e *SimpleTransformEngine) applyExcludeColumns(ctx context.Context, data []Row, config map[string]interface{}) ([]Row, error) {
	cols := make([]string, 0)
	switch v := config["columns"].(type) {
	case []string:
		cols = v
	case []interface{}:
		for _, it := range v {
			if s, ok := it.(string); ok && strings.TrimSpace(s) != "" {
				cols = append(cols, s)
			}
		}
	case string:
		parts := strings.Split(v, ",")
		for _, p := range parts {
			if s := strings.TrimSpace(p); s != "" {
				cols = append(cols, s)
			}
		}
	}
	if len(cols) == 0 {
		return data, nil
	}

	drop := map[string]struct{}{}
	for _, c := range cols {
		cc := lastIdent(strings.TrimSpace(c))
		if cc != "" {
			drop[cc] = struct{}{}
		}
	}
	if len(drop) == 0 {
		return data, nil
	}

	result := make([]Row, len(data))
	for i, row := range data {
		newRow := make(Row, 0)
		for k, v := range row {
			if _, shouldDrop := drop[k]; shouldDrop {
				continue
			}
			newRow[k] = v
		}
		result[i] = newRow
	}
	return result, nil
}

func (e *SimpleTransformEngine) applyNullHandle(ctx context.Context, data []Row, config map[string]interface{}) ([]Row, error) {
	col, _ := config["column"].(string)
	col = lastIdent(strings.TrimSpace(col))
	if col == "" {
		return nil, fmt.Errorf("null_handle requires 'column' config")
	}

	strategy := nullHandleStrategy(config)

	switch strategy {
	case "default", "fill", "fill_default":
		defaultValue, ok := config["default_value"]
		if !ok {
			return nil, fmt.Errorf("null_handle strategy=default requires 'default_value'")
		}
		// A column absent from EVERY row is a configuration mismatch, and filling
		// it here does not fill anything: it FABRICATES a new column, carrying the
		// default value, on every row of the table, and ships it to the
		// destination. One typo in a column name became a schema change.
		// MissingColumnWarnings names the column, and its wording — "the rule has
		// no effect" — is only true once this branch stops having one.
		//
		// A column absent from SOME rows is an ordinary sparse document, and
		// filling the default there is the entire point of the rule, so that case
		// is untouched.
		if len(data) > 0 && len(presentColumns([]string{col}, data)) == 0 {
			return data, nil
		}
		result := make([]Row, len(data))
		for i, row := range data {
			newRow := make(Row, len(row))
			for k, v := range row {
				newRow[k] = v
			}
			if isNullish(newRow[col]) {
				newRow[col] = defaultValue
			}
			result[i] = newRow
		}
		return result, nil
	case "drop_row":
		out := make([]Row, 0, len(data))
		for _, row := range data {
			// A column that is ABSENT from the row is not a null value. Treating
			// it as one let a single rule naming a column no row carries delete
			// the whole dataset, silently (HTTP 200, no warning, zero rows) —
			// KI-NULL-HANDLE-MISSING-COLUMN-DROPS-EVERY-ROW. Every other branch
			// of this engine fails open on a missing column (applyTruncate,
			// applyTypeConvert, applyJSONFlatten); drop_row now does too, and
			// TransformCoordinator.ApplyWithWarnings surfaces the mismatch
			// instead of consuming it.
			if val, ok := row[col]; ok && isNullish(val) {
				continue
			}
			out = append(out, row)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported null_handle strategy: %s", strategy)
	}
}

func (e *SimpleTransformEngine) applyTruncate(ctx context.Context, data []Row, config map[string]interface{}) ([]Row, error) {
	col, _ := config["column"].(string)
	col = lastIdent(strings.TrimSpace(col))
	if col == "" {
		return nil, fmt.Errorf("truncate requires 'column' config")
	}

	maxLen := 0
	switch v := config["max_length"].(type) {
	case int:
		maxLen = v
	case float64:
		maxLen = int(v)
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			maxLen = n
		}
	}
	if maxLen <= 0 {
		return nil, fmt.Errorf("truncate requires positive 'max_length'")
	}

	result := make([]Row, len(data))
	for i, row := range data {
		newRow := make(Row, len(row))
		for k, v := range row {
			if k == col {
				if s, ok := v.(string); ok && len(s) > 0 {
					newRow[k] = truncateRunes(s, maxLen)
				} else {
					newRow[k] = v
				}
			} else {
				newRow[k] = v
			}
		}
		result[i] = newRow
	}
	return result, nil
}

func (e *SimpleTransformEngine) applyTypeConvert(ctx context.Context, data []Row, config map[string]interface{}) ([]Row, error) {
	col, _ := config["column"].(string)
	col = lastIdent(strings.TrimSpace(col))
	if col == "" {
		return nil, fmt.Errorf("type_convert requires 'column' config")
	}

	to, _ := config["to"].(string)
	to = strings.ToLower(strings.TrimSpace(to))
	if to == "" {
		return nil, fmt.Errorf("type_convert requires 'to' config")
	}

	// Default to "skip" (keep the original value): a failed conversion must
	// never silently destroy data. Callers opt in to "null" (coerce to NULL) or
	// "error" (fail the run) explicitly.
	onErr := "skip"
	if s, ok := config["on_error"].(string); ok && strings.TrimSpace(s) != "" {
		onErr = strings.ToLower(strings.TrimSpace(s))
	}
	if onErr != "null" && onErr != "skip" && onErr != "error" {
		onErr = "skip"
	}

	result := make([]Row, len(data))
	for i, row := range data {
		newRow := make(Row, len(row))
		for k, v := range row {
			newRow[k] = v
		}

		val, exists := newRow[col]
		if !exists || val == nil {
			result[i] = newRow
			continue
		}

		converted, err := convertValue(val, to)
		if err != nil {
			switch onErr {
			case "skip":
				// keep original
			case "error":
				return nil, fmt.Errorf("type_convert %s -> %s: %w", col, to, err)
			default: // "null"
				newRow[col] = nil
			}
		} else {
			newRow[col] = converted
		}

		result[i] = newRow
	}
	return result, nil
}

// applyJSONFlatten expands a JSON/object column into multiple scalar columns.
// WIDE FORMAT: one input row -> one output row (more columns). Never expands rows,
// preserving the ACK-ledger row-count invariants.
//
// Config:
//   - column     (required) the source column holding a JSON object or string
//   - prefix     (default "") prepended to every emitted column name
//   - separator  (default "_") joins nested key paths
//   - max_depth  (default 0 = unlimited) how many object levels to descend
//
// Non-object, non-JSON, or nil values leave the row unchanged (fail-open).
func (e *SimpleTransformEngine) applyJSONFlatten(ctx context.Context, data []Row, config map[string]interface{}) ([]Row, error) {
	col, _ := config["column"].(string)
	col = lastIdent(strings.TrimSpace(col))
	if col == "" {
		return nil, fmt.Errorf("json_flatten requires 'column' config")
	}

	sep := "_"
	if s, ok := config["separator"].(string); ok && s != "" {
		sep = s
	}

	// The default prefix namespaces the flattened keys under the source column.
	// It used to default to "", which lifts the nested keys straight into the top
	// level: flattening meta on {id:7, meta:{id:99}} wrote id=99 over the row's
	// own primary key and returned HTTP 200.
	//
	// col+sep is not a new convention — it is the one the rest of the product
	// already stated. applyArrayExpand defaults to col+"_", the suggestion engine
	// emits prefix=<col>_ on every json_flatten it proposes
	// (llm-service/src/agents/suggestions/service.py), and the UI RENDERS an unset
	// prefix as `${column}_` (frontend/src/lib/transform-display.ts). The engine
	// was the only component that believed the default was flat, and it was the
	// one holding the data.
	//
	// An EXPLICIT "" is still honoured — the key is present in config, so it is a
	// deliberate request for the flat namespace, and the collision check below
	// makes it safe rather than silent.
	prefix := col + sep
	if p, ok := config["prefix"].(string); ok {
		prefix = p
	}

	maxDepth := 0
	if n, ok := asInt(config["max_depth"]); ok && n > 0 {
		maxDepth = n
	}

	result := make([]Row, len(data))
	for i, row := range data {
		newRow := make(Row, len(row))
		for k, v := range row {
			newRow[k] = v
		}

		raw, exists := newRow[col]
		if !exists {
			result[i] = newRow
			continue
		}

		m, isObj := toStringMap(raw)
		if !isObj {
			// Not a flattenable object — leave the row untouched.
			result[i] = newRow
			continue
		}

		delete(newRow, col)

		// Flatten into a side map first so a key that would land on an existing
		// column is caught BEFORE it overwrites one. Writing straight into newRow
		// made the overwrite unobservable: the column count looked right and the
		// old value was simply gone.
		flat := make(Row, len(m))
		for k, v := range m {
			flattenValue(flat, prefix+k, v, sep, 1, maxDepth)
		}
		var clashes []string
		for k := range flat {
			if _, taken := newRow[k]; taken {
				clashes = append(clashes, k)
			}
		}
		if len(clashes) > 0 {
			sort.Strings(clashes) // map order would otherwise reshuffle the message
			return nil, fmt.Errorf(
				"json_flatten: flattening %q would overwrite existing column(s) %s; set a distinct 'prefix' or exclude the column(s) first",
				col, strings.Join(clashes, ", "))
		}
		for k, v := range flat {
			newRow[k] = v
		}
		result[i] = newRow
	}

	return result, nil
}

// flattenValue recursively writes scalar leaves into dst. When it stops descending
// (max depth reached, or value is not an object), the value is scalarized so the
// destination always receives a clean scalar/JSON-string rather than a Go map.
func flattenValue(dst Row, key string, val interface{}, sep string, depth, maxDepth int) {
	if m, ok := toStringMap(val); ok && (maxDepth <= 0 || depth < maxDepth) {
		for k, v := range m {
			flattenValue(dst, key+sep+k, v, sep, depth+1, maxDepth)
		}
		return
	}
	dst[key] = scalarize(val)
}

// applyArrayExpand expands an array column into indexed scalar columns.
// WIDE FORMAT: one input row -> one output row (more columns). Never expands rows.
//
// Config:
//   - column        (required) the source column holding an array or JSON array string
//   - output_prefix (default column+"_") prepended to each indexed column name
//   - max_elements  (default 10) cap on how many elements are emitted
//
// Non-array or nil values leave the row unchanged (fail-open).
func (e *SimpleTransformEngine) applyArrayExpand(ctx context.Context, data []Row, config map[string]interface{}) ([]Row, error) {
	col, _ := config["column"].(string)
	col = lastIdent(strings.TrimSpace(col))
	if col == "" {
		return nil, fmt.Errorf("array_expand requires 'column' config")
	}

	prefix := col + "_"
	if p, ok := config["output_prefix"].(string); ok && p != "" {
		prefix = p
	}

	maxElems := 10
	if n, ok := asInt(config["max_elements"]); ok && n > 0 {
		maxElems = n
	}

	result := make([]Row, len(data))
	for i, row := range data {
		newRow := make(Row, len(row))
		for k, v := range row {
			newRow[k] = v
		}

		raw, exists := newRow[col]
		if !exists {
			result[i] = newRow
			continue
		}

		arr, isArr := toSlice(raw)
		if !isArr {
			// Not an array — leave the row untouched.
			result[i] = newRow
			continue
		}

		delete(newRow, col)
		limit := len(arr)
		if limit > maxElems {
			limit = maxElems
		}
		for j := 0; j < limit; j++ {
			// A row that already carries <prefix><j> — a legitimate column named
			// tags_0 alongside an array column tags — had it silently replaced by
			// the expansion. Refuse rather than overwrite; the operator picks a
			// different output_prefix.
			key := prefix + strconv.Itoa(j)
			if _, taken := newRow[key]; taken {
				return nil, fmt.Errorf(
					"array_expand: expanding %q would overwrite existing column %q; set a distinct 'output_prefix'",
					col, key)
			}
			newRow[key] = scalarize(arr[j])
		}
		result[i] = newRow
	}

	return result, nil
}

// toStringMap coerces a value into a JSON object. It accepts an already-decoded
// map, or a JSON object string / []byte. Returns false for anything else.
func toStringMap(v interface{}) (map[string]interface{}, bool) {
	switch t := v.(type) {
	case map[string]interface{}:
		return t, true
	case string:
		s := strings.TrimSpace(t)
		if len(s) == 0 || s[0] != '{' {
			return nil, false
		}
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(s), &m); err == nil {
			return m, true
		}
		return nil, false
	case []byte:
		s := strings.TrimSpace(string(t))
		if len(s) == 0 || s[0] != '{' {
			return nil, false
		}
		var m map[string]interface{}
		if err := json.Unmarshal(t, &m); err == nil {
			return m, true
		}
		return nil, false
	default:
		return nil, false
	}
}

// toSlice coerces a value into a slice. It accepts an already-decoded slice, or a
// JSON array string / []byte. Returns false for anything else.
func toSlice(v interface{}) ([]interface{}, bool) {
	switch t := v.(type) {
	case []interface{}:
		return t, true
	case string:
		s := strings.TrimSpace(t)
		if len(s) == 0 || s[0] != '[' {
			return nil, false
		}
		var arr []interface{}
		if err := json.Unmarshal([]byte(s), &arr); err == nil {
			return arr, true
		}
		return nil, false
	case []byte:
		s := strings.TrimSpace(string(t))
		if len(s) == 0 || s[0] != '[' {
			return nil, false
		}
		var arr []interface{}
		if err := json.Unmarshal(t, &arr); err == nil {
			return arr, true
		}
		return nil, false
	default:
		return nil, false
	}
}

// scalarize ensures a value is a destination-safe scalar. Nested objects/arrays
// are JSON-encoded to a string; scalars pass through unchanged.
func scalarize(v interface{}) interface{} {
	switch v.(type) {
	case map[string]interface{}, []interface{}:
		if b, err := json.Marshal(v); err == nil {
			return string(b)
		}
		return fmt.Sprintf("%v", v)
	default:
		return v
	}
}

// Helper functions

// condRe captures "column <op> value". Multi-char operators precede their
// single-char prefixes so ">=" matches before ">", "<>" before "<", etc.
var condRe = regexp.MustCompile(`^(\w+)\s*(>=|<=|!=|<>|=|>|<)\s*(.+)$`)

// likeRe captures "column LIKE 'pattern'" (LIKE keyword is case-insensitive).
var likeRe = regexp.MustCompile(`^(\w+)\s+(?i:LIKE)\s+['"](.*)['"]\s*$`)

// evaluateCondition evaluates a filter condition against one row.
//
// A condition is an OR of AND-groups of leaf comparisons, with SQL's precedence
// (AND binds tighter than OR) and no parentheses; parseCondition in condition.go
// owns the splitting and explains why. Each leaf is "column <op> value" with
// =, != (or <>), >, >=, <, <= or LIKE.
//
// A missing column is a non-match (not an error): that is correct per-row SQL
// semantics for a sparse document, and applyFilter separately refuses to let a
// column missing from EVERY row masquerade as a filter result.
//
// An unrecognized leaf returns an error so the caller can fail loudly instead of
// silently discarding every row.
func evaluateCondition(row Row, condition string) (bool, error) {
	expr, err := parseCondition(condition)
	if err != nil {
		return false, err
	}
	if len(expr.orGroups) == 0 {
		return true, nil
	}
	for _, group := range expr.orGroups {
		groupMatches := true
		for _, leaf := range group {
			ok, err := evaluateLeaf(row, leaf)
			if err != nil {
				return false, err
			}
			if !ok {
				groupMatches = false
				break
			}
		}
		if groupMatches {
			return true, nil
		}
	}
	return false, nil
}

// evaluateLeaf evaluates one "column <op> value" comparison. A comparison is
// integer when both operands are whole numbers, numeric when both parse as
// numbers, and lexicographic otherwise.
func evaluateLeaf(row Row, condition string) (bool, error) {
	condition = strings.TrimSpace(condition)
	if condition == "" {
		return true, nil
	}

	// LIKE is checked first; the comparison-operator regex does not match it.
	if m := likeRe.FindStringSubmatch(condition); len(m) == 3 {
		val, exists := row[m[1]]
		if !exists {
			return false, nil
		}
		matched, err := regexp.MatchString(likeToRegex(m[2]), fmt.Sprintf("%v", val))
		if err != nil {
			return false, fmt.Errorf("invalid LIKE pattern %q: %w", m[2], err)
		}
		return matched, nil
	}

	m := condRe.FindStringSubmatch(condition)
	if m == nil {
		return false, fmt.Errorf("unsupported condition format: %q", condition)
	}
	column, op := m[1], m[2]
	rhs := strings.Trim(m[3], "'\" ")

	val, exists := row[column]
	if !exists {
		return false, nil
	}
	return compareValue(val, op, rhs)
}

// compareValue applies op between a row value and a right-hand string literal.
//
// Whole numbers are compared as int64 before the float64 path is considered,
// because float64 is exact only below 2^53 -- see integerValue.
func compareValue(val interface{}, op, rhs string) (bool, error) {
	if li, lok := integerValue(val); lok {
		if ri, rok := integerString(rhs); rok {
			return compareOrdered(li, ri, op)
		}
	}

	lf, lok := numericValue(val)
	rf, rok := numericFloat(rhs)
	numeric := lok && rok

	switch op {
	case "=":
		if numeric {
			return lf == rf, nil
		}
		return fmt.Sprintf("%v", val) == rhs, nil
	case "!=", "<>":
		if numeric {
			return lf != rf, nil
		}
		return fmt.Sprintf("%v", val) != rhs, nil
	case ">", ">=", "<", "<=":
		cmp := 0
		if numeric {
			switch {
			case lf < rf:
				cmp = -1
			case lf > rf:
				cmp = 1
			}
		} else {
			cmp = strings.Compare(fmt.Sprintf("%v", val), rhs)
		}
		switch op {
		case ">":
			return cmp > 0, nil
		case ">=":
			return cmp >= 0, nil
		case "<":
			return cmp < 0, nil
		default: // "<="
			return cmp <= 0, nil
		}
	}
	return false, fmt.Errorf("unsupported operator: %q", op)
}

// compareOrdered applies op to two already-ordered operands.
func compareOrdered(l, r int64, op string) (bool, error) {
	switch op {
	case "=":
		return l == r, nil
	case "!=", "<>":
		return l != r, nil
	case ">":
		return l > r, nil
	case ">=":
		return l >= r, nil
	case "<":
		return l < r, nil
	case "<=":
		return l <= r, nil
	}
	return false, fmt.Errorf("unsupported operator: %q", op)
}

// applyMask applies masking to a value.
// hashFunc selects the digest used when maskType=hash ("sha256" default,
// "hmac_sha256" keyed by RSYNC_PII_HASH_SALT, or "md5" for legacy systems).
func applyMask(value interface{}, maskType string, hashFunc string) interface{} {
	if value == nil {
		return nil
	}

	str := fmt.Sprintf("%v", value)
	mt := strings.ToLower(strings.TrimSpace(maskType))

	switch mt {
	case "hash":
		if str == "" {
			return ""
		}
		return hashValue(str, hashFunc)
	case "redact":
		return "***"
	case "partial", "mask", "partial_mask":
		// Runes, not bytes. str[:2] cuts a multi-byte character in half and emits
		// the fragments as replacement characters, so masking a non-ASCII value
		// produced mojibake instead of a readable prefix — and on a 3-byte-per-
		// character script the "len > 4" guard let a 2-character value through the
		// slice path at all. truncateRunes in this file has always done it right.
		r := []rune(str)
		if len(r) > 4 {
			return string(r[:2]) + "***" + string(r[len(r)-2:])
		}
		return "***"
	default:
		return "***"
	}
}

// hashValue produces a deterministic, salted digest of str.
// Output is prefixed with the algorithm so downstream consumers can tell
// masked values apart. Unknown hashFunc falls back to sha256.
func hashValue(str string, hashFunc string) string {
	salt := getPIIHashSalt()
	switch strings.ToLower(strings.TrimSpace(hashFunc)) {
	case "hmac_sha256", "hmac-sha256", "hmac256":
		// Keyed hash. The salt is the HMAC key; an empty key still produces a
		// valid (though unkeyed-equivalent) HMAC so masking never leaks raw values.
		mac := hmac.New(sha256.New, []byte(salt))
		mac.Write([]byte(str))
		return "hmac256:" + hex.EncodeToString(mac.Sum(nil))
	case "md5":
		sum := md5.Sum([]byte(salt + "\x00" + str))
		return "md5:" + hex.EncodeToString(sum[:])
	default: // "sha256" and anything unrecognized
		sum := sha256.Sum256([]byte(salt + "\x00" + str))
		return "sha256:" + hex.EncodeToString(sum[:])
	}
}

var (
	_piiHashSaltOnce sync.Once
	_piiHashSalt     string
)

func getPIIHashSalt() string {
	_piiHashSaltOnce.Do(func() {
		// Optional salt. Keep stable per environment for deterministic masking.
		// If unset, we still hash (unsalted) to avoid leaking raw values.
		s := strings.TrimSpace(os.Getenv("RSYNC_PII_HASH_SALT"))
		if s == "" {
			s = strings.TrimSpace(os.Getenv("PII_HASH_SALT"))
		}
		_piiHashSalt = s
	})
	return _piiHashSalt
}

// numericValue reports whether v represents a number and, if so, its float64
// value. It accepts the numeric Go kinds, json.Number, and numeric strings.
func numericValue(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		return numericFloat(n)
	}
	return 0, false
}

// numericFloat parses s as a float64, reporting ok=false for non-numeric text.
func numericFloat(s string) (float64, bool) {
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return f, err == nil
}

func lastIdent(s string) string {
	if parts := strings.Split(s, "."); len(parts) > 1 {
		return parts[len(parts)-1]
	}
	return s
}

func isNullish(v interface{}) bool {
	if v == nil {
		return true
	}
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s) == ""
	}
	return false
}

func truncateRunes(s string, maxLen int) string {
	if maxLen <= 0 {
		return s
	}
	r := []rune(s)
	if len(r) <= maxLen {
		return s
	}
	return string(r[:maxLen])
}

func convertValue(v interface{}, to string) (interface{}, error) {
	switch to {
	case "string":
		// fmt.Sprintf("%v") on an object or array emits GO syntax —
		// "map[a:1 b:x]", "[1 2 3]" — which is not JSON, round-trips through
		// nothing, and is what landed in the destination whenever a nested column
		// was converted to string. scalarize (this file) is the encoder the wide
		// transforms already use for the same job.
		switch t := v.(type) {
		case []byte:
			return string(t), nil
		}
		if s, ok := scalarize(v).(string); ok {
			return s, nil
		}
		return fmt.Sprintf("%v", v), nil
	case "int", "integer":
		switch n := v.(type) {
		case int:
			return n, nil
		case int64:
			return int(n), nil
		case float64:
			return int(n), nil
		case float32:
			return int(n), nil
		case string:
			s := strings.TrimSpace(n)
			if s == "" {
				return nil, fmt.Errorf("empty string")
			}
			if i, err := strconv.ParseInt(s, 10, 64); err == nil {
				return int(i), nil
			}
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return int(f), nil
			}
			return nil, fmt.Errorf("invalid int: %q", n)
		default:
			return nil, fmt.Errorf("cannot convert %T to int", v)
		}
	case "float", "double":
		switch n := v.(type) {
		case float64:
			return n, nil
		case float32:
			return float64(n), nil
		case int:
			return float64(n), nil
		case int64:
			return float64(n), nil
		case string:
			s := strings.TrimSpace(n)
			if s == "" {
				return nil, fmt.Errorf("empty string")
			}
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid float: %q", n)
			}
			return f, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to float", v)
		}
	case "bool", "boolean":
		switch b := v.(type) {
		case bool:
			return b, nil
		case int:
			return b != 0, nil
		case int64:
			return b != 0, nil
		case float64:
			return b != 0, nil
		case string:
			s := strings.ToLower(strings.TrimSpace(b))
			switch s {
			case "true", "t", "yes", "y", "1":
				return true, nil
			case "false", "f", "no", "n", "0":
				return false, nil
			default:
				return nil, fmt.Errorf("invalid bool: %q", b)
			}
		default:
			return nil, fmt.Errorf("cannot convert %T to bool", v)
		}
	default:
		return nil, fmt.Errorf("unsupported target type: %s", to)
	}
}

// DuckDBTransformEngine is a stub for Phase 2
type DuckDBTransformEngine struct{}

// NewDuckDBTransformEngine creates a new DuckDB transform engine (stub for Phase 2)
func NewDuckDBTransformEngine() *DuckDBTransformEngine {
	return &DuckDBTransformEngine{}
}

// CanHandle returns true for SQL transforms (Phase 2)
func (e *DuckDBTransformEngine) CanHandle(transformType string) bool {
	return transformType == "sql"
}

// Apply is stubbed for Phase 2
func (e *DuckDBTransformEngine) Apply(ctx context.Context, data []Row, transform Transform) ([]Row, error) {
	return nil, fmt.Errorf("DuckDB engine not implemented yet (Phase 2)")
}

// ErrPreviewTimeout reports that a preview run hit its deadline before the
// transform chain finished. Callers need to tell this apart from a chain that
// legitimately produced no rows, so it is a sentinel rather than a free-text
// error: errors.Is(err, ErrPreviewTimeout).
var ErrPreviewTimeout = errors.New("transform preview timed out")

// PreviewExecutor runs transforms with timeouts for preview
type PreviewExecutor struct {
	coordinator *TransformCoordinator
}

// NewPreviewExecutor creates a new preview executor
func NewPreviewExecutor(coordinator *TransformCoordinator) *PreviewExecutor {
	return &PreviewExecutor{
		coordinator: coordinator,
	}
}

// Preview executes transforms with a timeout and returns warnings
func (e *PreviewExecutor) Preview(sampleData []Row, transforms []Transform, timeout time.Duration) ([]Row, []string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	warnings := []string{}

	result, stepWarnings, err := e.coordinator.ApplyWithWarnings(ctx, sampleData, transforms)
	warnings = append(warnings, stepWarnings...)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			// ApplyWithWarnings returns nil rows on error, so falling through to a
			// nil error here answered a timed-out preview with an empty result set
			// and no error at all. The UI renders that as "0 rows", which reads as
			// "your transforms dropped every row" - the one conclusion a run that
			// never finished cannot support. Report the timeout instead.
			return nil, warnings, fmt.Errorf("%w after %s", ErrPreviewTimeout, timeout)
		}
		return nil, warnings, err
	}

	return result, warnings, nil
}
