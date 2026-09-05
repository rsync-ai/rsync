package transforms

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type NormalizeMode string

const (
	NormalizeModePreview   NormalizeMode = "preview"
	NormalizeModeExecution NormalizeMode = "execution"
	NormalizeModeCDC       NormalizeMode = "cdc"
)

// CanonicalTransform is the normalized, validated transform shape used across
// preview, batch execution, and CDC execution paths.
type CanonicalTransform struct {
	ID                  string         `json:"id,omitempty"`
	Order               int            `json:"order"`
	Type                string         `json:"type"`
	Enabled             bool           `json:"enabled"`
	Scope               TransformScope `json:"scope,omitempty"`
	Config              map[string]any `json:"config"`
	RequiresFullDataset bool           `json:"requires_full_dataset,omitempty"`
	Origin              string         `json:"origin,omitempty"`

	_rawIndex int
}

type TransformScope struct {
	Table string `json:"table,omitempty"`
}

func (t CanonicalTransform) EngineTransform() Transform {
	return Transform{Type: t.Type, Config: t.Config}
}

// NormalizeAndValidate converts heterogeneous transform shapes to a canonical list.
//
// Behavior:
// - Sorts by order (stable).
// - Filters out disabled transforms.
// - Applies scope.table filtering if currentTable is provided.
// - Blocks requires_full_dataset in execution + CDC modes (MVP).
// - In preview mode, invalid transforms are skipped and returned as warnings (best-effort).
// - In execution/CDC modes, invalid transforms return an error.
func NormalizeAndValidate(raw []map[string]any, currentTable string, mode NormalizeMode) ([]CanonicalTransform, []string, error) {
	warnings := make([]string, 0)
	out := make([]CanonicalTransform, 0, len(raw))

	for idx, item := range raw {
		if item == nil {
			warnings = append(warnings, fmt.Sprintf("transform[%d]: empty transform", idx))
			continue
		}

		ct, w, err := normalizeOne(item, idx)
		warnings = append(warnings, w...)
		if err != nil {
			if mode == NormalizeModePreview {
				warnings = append(warnings, fmt.Sprintf("transform[%d]: %v", idx, err))
				continue
			}
			return nil, warnings, fmt.Errorf("transform[%d]: %w", idx, err)
		}

		if !ct.Enabled {
			continue
		}

		if strings.TrimSpace(ct.Scope.Table) != "" && strings.TrimSpace(currentTable) != "" {
			if !tableMatches(currentTable, ct.Scope.Table) {
				continue
			}
		}

		if ct.RequiresFullDataset {
			switch mode {
			case NormalizeModePreview:
				warnings = append(warnings, fmt.Sprintf("transform[%d] (%s): requires_full_dataset is not supported in MVP (skipped)", idx, ct.Type))
				continue
			case NormalizeModeExecution, NormalizeModeCDC:
				return nil, warnings, fmt.Errorf("transform type %q is marked requires_full_dataset and is blocked in MVP", ct.Type)
			}
		}

		if err := validateConfig(ct); err != nil {
			if mode == NormalizeModePreview {
				warnings = append(warnings, fmt.Sprintf("transform[%d] (%s): %v", idx, ct.Type, err))
				continue
			}
			return nil, warnings, fmt.Errorf("transform type %q: %w", ct.Type, err)
		}

		out = append(out, ct)
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Order == out[j].Order {
			return out[i]._rawIndex < out[j]._rawIndex
		}
		return out[i].Order < out[j].Order
	})

	return out, warnings, nil
}

func normalizeOne(item map[string]any, idx int) (CanonicalTransform, []string, error) {
	warnings := make([]string, 0)

	enabled := true
	if v, ok := item["enabled"]; ok {
		if b, ok := v.(bool); ok {
			enabled = b
		}
	}

	order := idx
	if v, ok := item["order"]; ok {
		if n, ok := asInt(v); ok {
			order = n
		}
	} else if v, ok := item["transform_order"]; ok {
		if n, ok := asInt(v); ok {
			order = n
		}
	} else if v, ok := item["transformOrder"]; ok {
		if n, ok := asInt(v); ok {
			order = n
		}
	}

	id, _ := item["id"].(string)
	id = strings.TrimSpace(id)

	origin, _ := item["origin"].(string)
	origin = strings.TrimSpace(origin)

	requiresFullDataset := false
	if v, ok := item["requires_full_dataset"]; ok {
		if b, ok := v.(bool); ok {
			requiresFullDataset = b
		}
	}

	scope := TransformScope{}
	if rawScope, ok := item["scope"].(map[string]any); ok && rawScope != nil {
		if s, ok := rawScope["table"].(string); ok {
			scope.Table = strings.TrimSpace(s)
		}
	}

	var rawType string
	if s, ok := item["type"].(string); ok {
		rawType = s
	}

	// Stored shape: { transform_config: { operation: "..." , ... } }
	var config map[string]any
	if tc, ok := item["transform_config"].(map[string]any); ok && tc != nil {
		if rawType == "" {
			if op, ok := tc["operation"].(string); ok {
				rawType = op
			}
		}
		config = tc
	} else if c0, ok := item["config"].(map[string]any); ok && c0 != nil {
		config = c0
	} else {
		// Flat shape: all keys except known metadata are config.
		config = map[string]any{}
		for k, v := range item {
			switch k {
			case "id", "type", "order", "enabled", "origin", "scope", "requires_full_dataset", "summary", "reason":
				continue
			default:
				config[k] = v
			}
		}
	}

	engineType := normalizeType(rawType)
	if engineType == "" {
		return CanonicalTransform{}, warnings, fmt.Errorf("missing transform type")
	}

	// Back-compat: allow table scoping in legacy locations and move into scope.table.
	if scope.Table == "" {
		if s, ok := item["table"].(string); ok && strings.TrimSpace(s) != "" {
			scope.Table = strings.TrimSpace(s)
		} else if s, ok := config["table"].(string); ok && strings.TrimSpace(s) != "" {
			scope.Table = strings.TrimSpace(s)
		}
	}
	delete(config, "table")

	// Back-compat: requires_full_dataset inside config.
	if !requiresFullDataset {
		if b, ok := config["requires_full_dataset"].(bool); ok {
			requiresFullDataset = b
		}
	}
	delete(config, "requires_full_dataset")

	// Normalize common config aliases.
	switch engineType {
	case "mask_pii":
		if _, ok := config["column"]; !ok {
			if v, ok := config["field"]; ok {
				config["column"] = v
			}
		}
	case "select_columns":
		if _, ok := config["columns"]; !ok {
			if v, ok := config["columns_to_keep"]; ok {
				config["columns"] = v
			} else if v, ok := config["keep_columns"]; ok {
				config["columns"] = v
			}
		}
	case "filter":
		if _, ok := config["condition"]; !ok {
			if v, ok := config["where"]; ok {
				config["condition"] = v
			}
		}
	}

	// Stored transform_config includes operation; strip it after mapping.
	// delete is a no-op when the key is absent, so no existence guard is needed.
	delete(config, "operation")

	return CanonicalTransform{
		ID:                  id,
		Order:               order,
		Type:                engineType,
		Enabled:             enabled,
		Scope:               scope,
		Config:              config,
		RequiresFullDataset: requiresFullDataset,
		Origin:              origin,
		_rawIndex:           idx,
	}, warnings, nil
}

func normalizeType(t string) string {
	tt := strings.ToLower(strings.TrimSpace(t))
	switch tt {
	// Canonical engine types
	case "filter", "mask_pii", "select_columns", "validate",
		"rename_columns", "exclude_columns", "null_handle", "truncate", "type_convert",
		"json_flatten", "array_expand", "sql":
		return tt
	// Common aliases
	case "mask":
		return "mask_pii"
	case "select":
		return "select_columns"
	case "exclude", "drop_columns", "drop":
		return "exclude_columns"
	case "rename":
		return "rename_columns"
	case "null_handling", "nulls":
		return "null_handle"
	case "cast", "type_cast":
		return "type_convert"
	case "flatten", "json_flatten_columns", "flatten_json":
		return "json_flatten"
	case "array_explode", "explode", "expand_array", "unnest":
		return "array_expand"
	default:
		return tt
	}
}

func validateConfig(t CanonicalTransform) error {
	cfg := t.Config
	if cfg == nil {
		cfg = map[string]any{}
	}

	switch t.Type {
	case "filter":
		// condition is optional (no-op).
		if v, ok := cfg["condition"]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) == "" {
				return fmt.Errorf("condition cannot be empty")
			}
		}
		return nil
	case "mask_pii":
		col, _ := cfg["column"].(string)
		if strings.TrimSpace(col) == "" {
			cols, ok := asStringSlice(cfg["columns"])
			if !ok || len(cols) == 0 {
				return fmt.Errorf("mask_pii requires config.column or config.columns (non-empty)")
			}
		}
		return nil
	case "select_columns":
		cols, ok := asStringSlice(cfg["columns"])
		if !ok || len(cols) == 0 {
			return fmt.Errorf("select_columns requires config.columns (non-empty)")
		}
		return nil
	case "validate":
		cols, ok := asStringSlice(cfg["required_columns"])
		if !ok || len(cols) == 0 {
			return fmt.Errorf("validate requires config.required_columns (non-empty)")
		}
		return nil
	case "rename_columns":
		_, ok := asStringStringMap(cfg["mappings"])
		if !ok {
			// Accept single mapping shape.
			from, _ := cfg["from"].(string)
			to, _ := cfg["to"].(string)
			if strings.TrimSpace(from) == "" || strings.TrimSpace(to) == "" {
				return fmt.Errorf("rename_columns requires config.mappings (map) or config.from/config.to")
			}
		}
		return nil
	case "exclude_columns":
		cols, ok := asStringSlice(cfg["columns"])
		if !ok || len(cols) == 0 {
			return fmt.Errorf("exclude_columns requires config.columns (non-empty)")
		}
		return nil
	case "null_handle":
		col, _ := cfg["column"].(string)
		if strings.TrimSpace(col) == "" {
			return fmt.Errorf("null_handle requires config.column")
		}
		strategy := "default"
		if s, ok := cfg["strategy"].(string); ok && strings.TrimSpace(s) != "" {
			strategy = strings.ToLower(strings.TrimSpace(s))
		}
		switch strategy {
		case "default", "fill", "fill_default":
			if _, ok := cfg["default_value"]; !ok {
				return fmt.Errorf("null_handle strategy=default requires config.default_value")
			}
			return nil
		case "drop_row":
			return nil
		default:
			return fmt.Errorf("null_handle has unsupported strategy %q", strategy)
		}
	case "truncate":
		col, _ := cfg["column"].(string)
		if strings.TrimSpace(col) == "" {
			return fmt.Errorf("truncate requires config.column")
		}
		maxLen, ok := asInt(cfg["max_length"])
		if !ok || maxLen <= 0 {
			return fmt.Errorf("truncate requires config.max_length > 0")
		}
		return nil
	case "type_convert":
		col, _ := cfg["column"].(string)
		if strings.TrimSpace(col) == "" {
			return fmt.Errorf("type_convert requires config.column")
		}
		to, _ := cfg["to"].(string)
		to = strings.ToLower(strings.TrimSpace(to))
		if to == "" {
			return fmt.Errorf("type_convert requires config.to")
		}
		switch to {
		case "string", "int", "integer", "float", "double", "bool", "boolean":
			return nil
		default:
			return fmt.Errorf("type_convert has unsupported target type %q", to)
		}
	case "json_flatten":
		col, _ := cfg["column"].(string)
		if strings.TrimSpace(col) == "" {
			return fmt.Errorf("json_flatten requires config.column")
		}
		// max_depth is optional; if present it must be a non-negative integer (0 = unlimited).
		if v, ok := cfg["max_depth"]; ok {
			n, parsed := asInt(v)
			if !parsed || n < 0 {
				return fmt.Errorf("json_flatten config.max_depth must be a non-negative integer")
			}
		}
		return nil
	case "array_expand":
		col, _ := cfg["column"].(string)
		if strings.TrimSpace(col) == "" {
			return fmt.Errorf("array_expand requires config.column")
		}
		// max_elements is optional; if present it must be a positive integer.
		if v, ok := cfg["max_elements"]; ok {
			n, parsed := asInt(v)
			if !parsed || n <= 0 {
				return fmt.Errorf("array_expand config.max_elements must be a positive integer")
			}
		}
		return nil
	case "sql":
		// The Tier-2 SQL engine is a stub (engine.go DuckDBTransformEngine), so a
		// sql transform cannot execute. Reject it here so it fails at author time
		// (preview → warning; execution/CDC → clear error) instead of saving
		// cleanly and then hard-failing mid-run with an opaque stub error.
		return fmt.Errorf("transform type \"sql\" is not supported yet (SQL/Tier-2 engine not implemented)")
	default:
		return fmt.Errorf("unknown transform type %q", t.Type)
	}
}

func asInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	case float32:
		return int(n), true
	case jsonNumber:
		i64, err := n.Int64()
		if err != nil {
			return 0, false
		}
		return int(i64), true
	case string:
		i, err := strconv.Atoi(strings.TrimSpace(n))
		if err != nil {
			return 0, false
		}
		return i, true
	default:
		return 0, false
	}
}

// jsonNumber is a tiny interface compatible with encoding/json.Number without importing encoding/json here.
type jsonNumber interface {
	Int64() (int64, error)
	String() string
}

func asStringSlice(v any) ([]string, bool) {
	switch vv := v.(type) {
	case []string:
		out := make([]string, 0, len(vv))
		for _, s := range vv {
			ss := strings.TrimSpace(s)
			if ss != "" {
				out = append(out, ss)
			}
		}
		return out, true
	case []any:
		out := make([]string, 0, len(vv))
		for _, it := range vv {
			if s, ok := it.(string); ok {
				ss := strings.TrimSpace(s)
				if ss != "" {
					out = append(out, ss)
				}
			}
		}
		return out, true
	case string:
		parts := strings.Split(vv, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			ss := strings.TrimSpace(p)
			if ss != "" {
				out = append(out, ss)
			}
		}
		return out, len(out) > 0
	default:
		return nil, false
	}
}

func asStringStringMap(v any) (map[string]string, bool) {
	switch vv := v.(type) {
	case map[string]string:
		out := map[string]string{}
		for k, val := range vv {
			kk := strings.TrimSpace(k)
			vv2 := strings.TrimSpace(val)
			if kk != "" && vv2 != "" {
				out[kk] = vv2
			}
		}
		return out, len(out) > 0
	case map[string]any:
		out := map[string]string{}
		for k, it := range vv {
			kk := strings.TrimSpace(k)
			if kk == "" {
				continue
			}
			if s, ok := it.(string); ok {
				ss := strings.TrimSpace(s)
				if ss != "" {
					out[kk] = ss
				}
			}
		}
		return out, len(out) > 0
	case []any:
		// Accept [{"from":"a","to":"b"}, ...]
		out := map[string]string{}
		for _, it := range vv {
			m, ok := it.(map[string]any)
			if !ok || m == nil {
				continue
			}
			from, _ := m["from"].(string)
			to, _ := m["to"].(string)
			from = strings.TrimSpace(from)
			to = strings.TrimSpace(to)
			if from != "" && to != "" {
				out[from] = to
			}
		}
		return out, len(out) > 0
	default:
		return nil, false
	}
}

func tableMatches(current, configured string) bool {
	current = strings.TrimSpace(current)
	configured = strings.TrimSpace(configured)
	if current == "" || configured == "" {
		return false
	}
	if current == configured {
		return true
	}

	// Unqualified match (e.g. "users" matches "public.users" or "db.users").
	currentParts := strings.Split(current, ".")
	configuredParts := strings.Split(configured, ".")

	currentName := strings.TrimSpace(currentParts[len(currentParts)-1])
	configuredName := strings.TrimSpace(configuredParts[len(configuredParts)-1])
	if currentName == "" || configuredName == "" {
		return false
	}
	return strings.EqualFold(currentName, configuredName)
}
