package transforms

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
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

// Apply applies a list of transforms sequentially
func (c *TransformCoordinator) Apply(ctx context.Context, data []Row, transforms []Transform) ([]Row, error) {
	result := data

	for i, transform := range transforms {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("transform cancelled at step %d: %w", i, ctx.Err())
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
			return nil, fmt.Errorf("no engine available for transform type: %s", transform.Type)
		}

		result, err = engine.Apply(ctx, result, transform)
		if err != nil {
			return nil, fmt.Errorf("transform %d (%s) failed: %w", i, transform.Type, err)
		}
	}

	return result, nil
}

// SimpleTransformEngine implements Tier 1 transforms (filter, mask_pii, select_columns, validate)
type SimpleTransformEngine struct{}

// NewSimpleTransformEngine creates a new simple transform engine
func NewSimpleTransformEngine() *SimpleTransformEngine {
	return &SimpleTransformEngine{}
}

// CanHandle returns true if this engine can handle the transform type
func (e *SimpleTransformEngine) CanHandle(transformType string) bool {
	supportedTypes := []string{
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
	for _, t := range supportedTypes {
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

// applyMask masks PII fields
func (e *SimpleTransformEngine) applyMask(ctx context.Context, data []Row, config map[string]interface{}) ([]Row, error) {
	// Accept either:
	// - config.column: "email"
	// - config.columns: ["email","phone"]
	parseCols := func(v interface{}) []string {
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

	cols := parseCols(config["columns"])
	if len(cols) == 0 {
		cols = parseCols(config["column"])
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("mask_pii requires 'column' or 'columns' config")
	}

	// Normalize: allow table-qualified column names (e.g. "users.email").
	colSet := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		cc := c
		if parts := strings.Split(cc, "."); len(parts) > 1 {
			cc = parts[len(parts)-1]
		}
		cc = strings.TrimSpace(cc)
		if cc != "" {
			colSet[cc] = struct{}{}
		}
	}
	if len(colSet) == 0 {
		return nil, fmt.Errorf("mask_pii requires non-empty column names")
	}

	maskType := "hash"
	if mt, ok := config["mask_type"].(string); ok {
		maskType = mt
	}

	// hash_function selects the digest used when mask_type=hash.
	// Defaults to sha256 for backward compatibility.
	hashFunc := "sha256"
	if hf, ok := config["hash_function"].(string); ok && strings.TrimSpace(hf) != "" {
		hashFunc = strings.ToLower(strings.TrimSpace(hf))
	}

	result := make([]Row, len(data))
	for i, row := range data {
		newRow := make(Row)
		for k, v := range row {
			if _, ok := colSet[k]; ok {
				newRow[k] = applyMask(v, maskType, hashFunc)
			} else {
				newRow[k] = v
			}
		}
		result[i] = newRow
	}

	return result, nil
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
	default:
		return nil, fmt.Errorf("invalid columns config type")
	}

	if len(columns) == 0 {
		return data, nil
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

func (e *SimpleTransformEngine) applyRenameColumns(ctx context.Context, data []Row, config map[string]interface{}) ([]Row, error) {
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

	if len(mappings) == 0 {
		return data, nil
	}

	result := make([]Row, len(data))
	for i, row := range data {
		newRow := make(Row, len(row))
		for k, v := range row {
			newRow[k] = v
		}
		for from, to := range mappings {
			if from == "" || to == "" || from == to {
				continue
			}
			if val, ok := newRow[from]; ok {
				newRow[to] = val
				delete(newRow, from)
			}
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

	strategy := "default"
	if s, ok := config["strategy"].(string); ok && strings.TrimSpace(s) != "" {
		strategy = strings.ToLower(strings.TrimSpace(s))
	}

	switch strategy {
	case "default", "fill", "fill_default":
		defaultValue, ok := config["default_value"]
		if !ok {
			return nil, fmt.Errorf("null_handle strategy=default requires 'default_value'")
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
			val, ok := row[col]
			if !ok || isNullish(val) {
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

	prefix := ""
	if p, ok := config["prefix"].(string); ok {
		prefix = p
	}

	sep := "_"
	if s, ok := config["separator"].(string); ok && s != "" {
		sep = s
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
		for k, v := range m {
			flattenValue(newRow, prefix+k, v, sep, 1, maxDepth)
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
			newRow[prefix+strconv.Itoa(j)] = scalarize(arr[j])
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

// evaluateCondition evaluates a simple "column <op> value" condition against a
// row. Supported operators: =, != (or <>), >, >=, <, <=, and LIKE. A comparison
// is numeric when BOTH operands parse as numbers (so numeric values arriving as
// strings — e.g. decimals from DB drivers — compare correctly); otherwise it is
// a lexicographic string comparison. A missing column is a non-match (not an
// error). An unrecognized condition format returns an error so the caller can
// fail loudly instead of silently discarding every row.
func evaluateCondition(row Row, condition string) (bool, error) {
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
		// Naive SQL-LIKE → regex: % => any run, _ => any single char.
		pattern := strings.Replace(m[2], "%", ".*", -1)
		pattern = strings.Replace(pattern, "_", ".", -1)
		matched, err := regexp.MatchString("^"+pattern+"$", fmt.Sprintf("%v", val))
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
func compareValue(val interface{}, op, rhs string) (bool, error) {
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
		if len(str) > 4 {
			return str[:2] + "***" + str[len(str)-2:]
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

	result, err := e.coordinator.Apply(ctx, sampleData, transforms)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			warnings = append(warnings, "Preview timed out - results may be incomplete")
		} else {
			return nil, warnings, err
		}
	}

	return result, warnings, nil
}
