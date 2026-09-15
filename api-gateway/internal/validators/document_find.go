package validators

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Document-mode `find` validation for the Data Explorer (MongoDB).
//
// The api-gateway validates a user-authored filter / projection / sort BEFORE any
// network hop; the MongoDB connector enforces the same rules again so it is safe when
// called directly. The two copies MUST stay in lockstep:
//
//	shared/mcp-connectors/public/database/mongodb/versions/v1.0.0/connector.py
//	  FIND_QUERY_OPERATORS · FIND_TOP_LEVEL_OPERATORS · FIND_EXTJSON_WRAPPERS · FIND_MAX_*
//
// An allowlist, never a denylist: an operator MongoDB adds later stays rejected until
// someone decides it is safe. Server-side JavaScript and arbitrary expressions
// ($where, $function, $accumulator, $expr) are why this exists.
//
// Error messages name paths and operators, never a filter VALUE: values are user data
// and must not be echoed into a string that may be logged.

// DocumentFindQueryOperators mirrors connector.py FIND_QUERY_OPERATORS.
var DocumentFindQueryOperators = map[string]bool{
	"$eq": true, "$ne": true, "$gt": true, "$gte": true, "$lt": true, "$lte": true, "$in": true, "$nin": true,
	"$and": true, "$or": true, "$nor": true, "$not": true,
	"$exists": true, "$type": true, "$elemMatch": true, "$size": true, "$all": true,
	"$regex": true, "$options": true, "$mod": true,
}

// DocumentFindTopLevelOperators mirrors connector.py FIND_TOP_LEVEL_OPERATORS.
var DocumentFindTopLevelOperators = map[string]bool{"$and": true, "$or": true, "$nor": true}

// DocumentFindExtJSONWrappers mirrors connector.py FIND_EXTJSON_WRAPPERS. Each must be
// the only key of its object; the connector decodes the wrapped value.
var DocumentFindExtJSONWrappers = map[string]bool{"$oid": true, "$date": true, "$numberLong": true, "$numberDecimal": true}

// Limits, mirroring connector.py FIND_*.
const (
	DocumentFindDefaultLimit        = 50
	DocumentFindMaxLimit            = 500
	DocumentFindMaxSkip             = 10000
	DocumentFindMaxFilterDepth      = 20
	DocumentFindMaxFilterBytes      = 64 * 1024
	DocumentFindMaxSortKeys         = 5
	DocumentFindMaxProjectionKeys   = 100
	DocumentFindMaxCollectionBytes  = 120
	DocumentFindMaxCursorChars      = 4096
	documentFindPathKeyMaxRunes     = 64
	documentFindMaxOperatorEchoRune = 64
)

// DocumentFindError is a refused find request. Code matches the connector's
// error_code vocabulary (operator_not_allowed, invalid_filter, filter_too_deep, ...).
type DocumentFindError struct {
	Code    string
	Path    string
	Message string
}

func (e *DocumentFindError) Error() string { return e.Message }

func findErr(code, path, format string, args ...interface{}) *DocumentFindError {
	return &DocumentFindError{Code: code, Path: path, Message: fmt.Sprintf(format, args...)}
}

// DocumentFindRequest is the raw, user-supplied part of a document-mode find.
// Filter/Projection/Sort stay raw JSON so key order (sort) and exact number text
// (a 64-bit integer in a filter) survive to the connector.
type DocumentFindRequest struct {
	Collection string
	Filter     json.RawMessage
	Projection json.RawMessage
	Sort       json.RawMessage
	Limit      int
	Cursor     string
	Skip       int
}

// DocumentFindSpec is a validated find, ready to forward. Sort is always the ordered
// [field, direction] pair list: a Go map cannot carry sort order.
type DocumentFindSpec struct {
	Collection string           `json:"collection"`
	Filter     json.RawMessage  `json:"filter,omitempty"`
	Projection map[string]int   `json:"projection,omitempty"`
	Sort       [][2]interface{} `json:"sort,omitempty"`
	Limit      int              `json:"limit"`
	Cursor     string           `json:"cursor,omitempty"`
	Skip       int              `json:"skip,omitempty"`
}

// ValidateDocumentFind checks a document-mode find against the operator allowlist and
// limits and returns the normalized spec, or a *DocumentFindError.
func ValidateDocumentFind(req DocumentFindRequest) (*DocumentFindSpec, *DocumentFindError) {
	collection, ferr := validateFindCollection(req.Collection)
	if ferr != nil {
		return nil, ferr
	}
	filter, ferr := validateFindFilter(req.Filter)
	if ferr != nil {
		return nil, ferr
	}
	projection, ferr := validateFindProjection(req.Projection)
	if ferr != nil {
		return nil, ferr
	}
	sort, ferr := validateFindSort(req.Sort)
	if ferr != nil {
		return nil, ferr
	}

	limit := req.Limit
	if limit <= 0 {
		limit = DocumentFindDefaultLimit
	}
	if limit > DocumentFindMaxLimit {
		limit = DocumentFindMaxLimit
	}
	if req.Skip < 0 || req.Skip > DocumentFindMaxSkip {
		return nil, findErr("invalid_skip", "skip", "skip must be between 0 and %d", DocumentFindMaxSkip)
	}
	if utf8.RuneCountInString(req.Cursor) > DocumentFindMaxCursorChars {
		return nil, findErr("invalid_cursor", "cursor", "cursor is malformed")
	}

	// Keyset paging on _id when there is no sort or the sort is _id alone; skip paging
	// for any other sort. Each token only makes sense in its own mode.
	keyset := len(sort) == 0 || (len(sort) == 1 && sort[0][0] == "_id")
	if keyset && req.Skip > 0 {
		return nil, findErr("invalid_skip", "skip", "skip applies only to a custom sort; page with cursor instead")
	}
	if !keyset && req.Cursor != "" {
		return nil, findErr("invalid_cursor", "cursor", "cursor applies only to the default _id sort; page with skip instead")
	}

	return &DocumentFindSpec{
		Collection: collection,
		Filter:     filter,
		Projection: projection,
		Sort:       sort,
		Limit:      limit,
		Cursor:     req.Cursor,
		Skip:       req.Skip,
	}, nil
}

func findPath(parent, key string) string {
	if utf8.RuneCountInString(key) > documentFindPathKeyMaxRunes {
		key = string([]rune(key)[:documentFindPathKeyMaxRunes])
	}
	return parent + "." + key
}

func validateFindCollection(raw string) (string, *DocumentFindError) {
	name := strings.TrimSpace(raw)
	switch {
	case name == "":
		return "", findErr("invalid_collection", "collection", "Missing 'collection' parameter")
	case len(name) > DocumentFindMaxCollectionBytes:
		return "", findErr("invalid_collection", "collection", "collection name exceeds %d bytes", DocumentFindMaxCollectionBytes)
	case strings.ContainsAny(name, "$\x00"):
		return "", findErr("invalid_collection", "collection", "collection name must not contain '$' or NUL")
	case strings.HasPrefix(name, "system."):
		return "", findErr("invalid_collection", "collection", "system collections cannot be browsed")
	}
	return name, nil
}

// isJSONNull reports an absent or literal-null raw value.
func isJSONNull(raw json.RawMessage) bool {
	t := bytes.TrimSpace(raw)
	return len(t) == 0 || bytes.Equal(t, []byte("null"))
}

func decodeFindJSON(raw json.RawMessage) (interface{}, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v interface{}
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, fmt.Errorf("trailing data")
	}
	return v, nil
}

func validateFindFilter(raw json.RawMessage) (json.RawMessage, *DocumentFindError) {
	if isJSONNull(raw) {
		return nil, nil
	}
	if len(raw) > 4*DocumentFindMaxFilterBytes {
		// Refuse before decoding something absurd; the exact check is below.
		return nil, findErr("filter_too_large", "filter", "filter exceeds %d KB", DocumentFindMaxFilterBytes/1024)
	}
	v, err := decodeFindJSON(raw)
	if err != nil {
		return nil, findErr("invalid_filter", "filter", "filter must be a JSON object")
	}
	obj, ok := v.(map[string]interface{})
	if !ok {
		return nil, findErr("invalid_filter", "filter", "filter must be a JSON object")
	}
	for key := range obj {
		if strings.HasPrefix(key, "$") && !DocumentFindTopLevelOperators[key] {
			return nil, findErr("operator_not_allowed", findPath("filter", key),
				"%s is not allowed at the top level of a filter", truncateRunes(key, documentFindMaxOperatorEchoRune))
		}
	}
	if ferr := walkFindFilter(obj, "filter", 1); ferr != nil {
		return nil, ferr
	}
	compact, err := json.Marshal(obj)
	if err != nil {
		return nil, findErr("invalid_filter", "filter", "filter must be plain JSON")
	}
	// json.Marshal writes UTF-8 where the connector's json.dumps writes \u escapes, so
	// a non-ASCII filter near the limit can pass here and still be refused there.
	if len(compact) > DocumentFindMaxFilterBytes {
		return nil, findErr("filter_too_large", "filter", "filter exceeds %d KB", DocumentFindMaxFilterBytes/1024)
	}
	return compact, nil
}

func walkFindFilter(node interface{}, path string, depth int) *DocumentFindError {
	switch n := node.(type) {
	case []interface{}:
		if depth > DocumentFindMaxFilterDepth {
			return findErr("filter_too_deep", path, "filter nesting exceeds %d levels", DocumentFindMaxFilterDepth)
		}
		for i, item := range n {
			if ferr := walkFindFilter(item, fmt.Sprintf("%s[%d]", path, i), depth+1); ferr != nil {
				return ferr
			}
		}
		return nil
	case map[string]interface{}:
		if depth > DocumentFindMaxFilterDepth {
			return findErr("filter_too_deep", path, "filter nesting exceeds %d levels", DocumentFindMaxFilterDepth)
		}
		for key := range n {
			if DocumentFindExtJSONWrappers[key] {
				if len(n) != 1 {
					return findErr("invalid_filter", findPath(path, key), "%s must be the only key in its object", key)
				}
				return nil // the wrapped value's shape is checked by the connector when decoded
			}
		}
		for key, value := range n {
			if strings.ContainsRune(key, 0) {
				return findErr("invalid_filter", path, "field names must be strings without NUL")
			}
			child := findPath(path, key)
			if strings.HasPrefix(key, "$") && !DocumentFindQueryOperators[key] {
				return findErr("operator_not_allowed", child, "operator %s is not allowed", truncateRunes(key, documentFindMaxOperatorEchoRune))
			}
			if ferr := walkFindFilter(value, child, depth+1); ferr != nil {
				return ferr
			}
		}
	}
	return nil
}

func truncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n])
}

// findIntValue accepts a JSON integer (or a bool, which the connector treats as 0/1
// for projections only — the caller decides) and rejects floats like 1.0, which the
// connector's isinstance(value, int) check also rejects.
func findIntValue(v interface{}) (int64, bool) {
	num, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	if strings.ContainsAny(string(num), ".eE") {
		return 0, false
	}
	i, err := strconv.ParseInt(string(num), 10, 64)
	return i, err == nil
}

func validFindFieldName(key string) bool {
	return key != "" && !strings.ContainsAny(key, "$\x00")
}

func validateFindProjection(raw json.RawMessage) (map[string]int, *DocumentFindError) {
	if isJSONNull(raw) {
		return nil, nil
	}
	v, err := decodeFindJSON(raw)
	if err != nil {
		return nil, findErr("invalid_projection", "projection", "projection must be a JSON object")
	}
	obj, ok := v.(map[string]interface{})
	if !ok {
		return nil, findErr("invalid_projection", "projection", "projection must be a JSON object")
	}
	if len(obj) == 0 {
		return nil, nil
	}
	if len(obj) > DocumentFindMaxProjectionKeys {
		return nil, findErr("invalid_projection", "projection", "projection accepts at most %d fields", DocumentFindMaxProjectionKeys)
	}
	out := make(map[string]int, len(obj))
	for key, value := range obj {
		path := findPath("projection", key)
		if !validFindFieldName(key) {
			return nil, findErr("invalid_projection", path, "projection field names must be non-empty and contain no '$'")
		}
		var n int64
		switch b := value.(type) {
		case bool:
			if b {
				n = 1
			}
		default:
			i, ok := findIntValue(value)
			if !ok || (i != 0 && i != 1) {
				return nil, findErr("invalid_projection", path, "projection values must be 0 or 1")
			}
			n = i
		}
		out[key] = int(n)
	}
	seen := map[int]bool{}
	for key, value := range out {
		if key != "_id" {
			seen[value] = true
		}
	}
	if len(seen) > 1 {
		return nil, findErr("invalid_projection", "projection", "projection cannot mix inclusion and exclusion (except _id)")
	}
	return out, nil
}

// orderedPairs reads a JSON object's key/value pairs in document order. A Go map
// would lose the order, and for a sort the order IS the meaning.
func orderedPairs(raw json.RawMessage) ([][2]interface{}, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, fmt.Errorf("not an object")
	}
	var pairs [][2]interface{}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, fmt.Errorf("non-string key")
		}
		var value interface{}
		if err := dec.Decode(&value); err != nil {
			return nil, err
		}
		pairs = append(pairs, [2]interface{}{key, value})
	}
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	return pairs, nil
}

func validateFindSort(raw json.RawMessage) ([][2]interface{}, *DocumentFindError) {
	if isJSONNull(raw) {
		return nil, nil
	}
	trimmed := bytes.TrimSpace(raw)
	var items [][2]interface{}
	switch trimmed[0] {
	case '{':
		pairs, err := orderedPairs(trimmed)
		if err != nil {
			return nil, findErr("invalid_sort", "sort", "sort must be an object or a list of [field, direction] pairs")
		}
		items = pairs
	case '[':
		v, err := decodeFindJSON(trimmed)
		if err != nil {
			return nil, findErr("invalid_sort", "sort", "sort must be an object or a list of [field, direction] pairs")
		}
		for i, entry := range v.([]interface{}) {
			pair, ok := entry.([]interface{})
			if !ok || len(pair) != 2 {
				return nil, findErr("invalid_sort", fmt.Sprintf("sort[%d]", i), "sort list entries must be [field, 1|-1] pairs")
			}
			items = append(items, [2]interface{}{pair[0], pair[1]})
		}
	default:
		return nil, findErr("invalid_sort", "sort", "sort must be an object or a list of [field, direction] pairs")
	}
	if len(items) == 0 {
		return nil, nil
	}
	if len(items) > DocumentFindMaxSortKeys {
		return nil, findErr("invalid_sort", "sort", "sort accepts at most %d keys", DocumentFindMaxSortKeys)
	}
	out := make([][2]interface{}, 0, len(items))
	seen := map[string]bool{}
	for _, item := range items {
		key, isString := item[0].(string)
		path := findPath("sort", fmt.Sprintf("%v", item[0]))
		if !isString || !validFindFieldName(key) || seen[key] {
			return nil, findErr("invalid_sort", path, "sort field names must be unique, non-empty and contain no '$'")
		}
		dir, ok := findIntValue(item[1])
		if !ok || (dir != 1 && dir != -1) {
			return nil, findErr("invalid_sort", path, "sort direction must be 1 or -1")
		}
		seen[key] = true
		out = append(out, [2]interface{}{key, int(dir)})
	}
	return out, nil
}
