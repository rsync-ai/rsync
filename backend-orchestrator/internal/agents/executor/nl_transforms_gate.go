package executor

// nl_transforms_gate.go — the path-independent backend gate that turns an
// NL-requested masking / type-conversion (persisted on
// pipelines.config.nl_transforms by the api-gateway chat handler) into
// transform_definitions rows BEFORE any data moves.
//
// Why this exists:
//   Masking/type-conversion suggestions were only ever written by the FRONTEND
//   SuggestionsReviewDialog, which is bolted onto the batch table-selection HITL.
//   The CDC chat path bypasses that dialog entirely, so transform_definitions
//   stayed empty and the CDC sink applied ZERO transforms → plaintext PII landed.
//   By writing the transforms here (once, at the top of executeDataTransfer,
//   before the batch export loader / CDC sink consume them), a pure-NL "mask
//   email" / "convert string columns" request applies for BOTH modes with no
//   dependency on the browser dialog.
//
// Rows are written as BOTH `producer` and `consumer` (mirroring the dialog):
// the batch export loader reads `producer`, the CDC sink reads `consumer`.
//
// Safety:
//   - fail-closed: once a masking intent is present, any error resolving it
//     aborts the transfer rather than moving data unmasked.
//   - idempotent: if transform_definitions already has rows for the pipeline
//     (e.g. the dialog wrote them, or a prior run of this gate did), it no-ops.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

// nlTransformIntent mirrors the pipelines.config.nl_transforms object the chat
// handler writes when the NL request asked for masking / type conversion.
type nlTransformIntent struct {
	MaskColumns []string `json:"mask_columns"` // explicitly-named columns ("mask email")
	MaskPII     bool     `json:"mask_pii"`     // generic "mask PII" — detect by column name
	TypeConvert bool     `json:"type_convert"` // "convert string columns to recommended types"
}

func (n *nlTransformIntent) empty() bool {
	return n == nil || (len(n.MaskColumns) == 0 && !n.MaskPII && !n.TypeConvert)
}

// planNLTransforms is the gate. See file header. Safe to call on every transfer;
// a no-op unless the pipeline carries an nl_transforms intent.
func (a *Agent) planNLTransforms(ctx context.Context, task ExecutorTask) error {
	if a.db == nil {
		return nil
	}
	pipelineID := strings.TrimSpace(task.PipelineID)
	if !looksLikeUUID(pipelineID) {
		return nil
	}

	intent, err := a.loadNLTransformIntent(ctx, pipelineID)
	if err != nil {
		// Fail-closed: we cannot tell whether masking was requested, so refuse to
		// move data rather than risk landing unmasked PII.
		return fmt.Errorf("load nl_transforms intent: %w", err)
	}
	if intent.empty() {
		return nil
	}

	// Idempotent: never clobber transforms already written (the dialog, or a prior
	// run of this gate). DELETE-then-INSERT here would race the dialog and drop a
	// user's reviewed choices.
	existing, err := a.countTransformDefinitions(ctx, pipelineID)
	if err != nil {
		return fmt.Errorf("count existing transform_definitions: %w", err)
	}
	if existing > 0 {
		log.WithField("pipeline_id", pipelineID).Infof(
			"planNLTransforms: %d transform(s) already present — skipping NL gate", existing)
		return nil
	}

	// Resolve the source schema when we need it: generic PII detection and type
	// conversion require the column list, and explicit mask columns benefit from
	// canonicalization (the engine matches column keys EXACTLY, so a casing/spelling
	// mismatch would silently skip masking — a fail-open PII risk).
	var cols []ColumnMetadata
	if intent.MaskPII || intent.TypeConvert || len(intent.MaskColumns) > 0 {
		cols, err = a.discoverSourceColumns(ctx, task)
		if err != nil {
			if intent.MaskPII || len(intent.MaskColumns) > 0 {
				// Fail-closed: without the column list we cannot verify the
				// requested mask columns (or enumerate PII), so masking could
				// silently no-op and land unmasked PII. Masking is a privacy
				// guarantee — never degrade it to literal names on discovery
				// failure (that was the F6-GAP fail-open, KI-NLCHAT-MASK-SILENT-NOOP).
				return fmt.Errorf("discover source columns for masking: %w", err)
			}
			// Only type conversion remains (not a privacy guarantee) — degrade to
			// skipping it rather than aborting the transfer.
			log.WithField("pipeline_id", pipelineID).Warnf(
				"planNLTransforms: schema discovery failed, skipping type conversion: %v", err)
			cols = nil
		}
	}

	if err := maskGuardError(intent, cols); err != nil {
		return err
	}

	configs := buildNLTransforms(intent, cols)
	if len(configs) == 0 {
		log.WithField("pipeline_id", pipelineID).Warnf(
			"planNLTransforms: intent present but no transforms resolved (mask_columns=%v mask_pii=%v type_convert=%v cols=%d)",
			intent.MaskColumns, intent.MaskPII, intent.TypeConvert, len(cols))
		return nil
	}

	if err := a.insertNLTransformDefinitions(ctx, pipelineID, configs); err != nil {
		return fmt.Errorf("persist nl_transforms: %w", err)
	}
	log.WithField("pipeline_id", pipelineID).Infof(
		"planNLTransforms: wrote %d transform(s) as producer+consumer (mask_columns=%v mask_pii=%v type_convert=%v)",
		len(configs), intent.MaskColumns, intent.MaskPII, intent.TypeConvert)
	return nil
}

// loadNLTransformIntent reads pipelines.config->'nl_transforms'. Returns a
// non-nil (possibly empty) intent so callers can use intent.empty().
func (a *Agent) loadNLTransformIntent(ctx context.Context, pipelineID string) (*nlTransformIntent, error) {
	var raw []byte
	err := a.db.QueryRowContext(ctx,
		`SELECT config->'nl_transforms' FROM pipelines WHERE id = $1`, pipelineID).Scan(&raw)
	if err == sql.ErrNoRows {
		return &nlTransformIntent{}, nil
	}
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || string(raw) == "null" {
		return &nlTransformIntent{}, nil
	}
	var intent nlTransformIntent
	if err := json.Unmarshal(raw, &intent); err != nil {
		return nil, fmt.Errorf("malformed nl_transforms JSON: %w", err)
	}
	return &intent, nil
}

func (a *Agent) countTransformDefinitions(ctx context.Context, pipelineID string) (int, error) {
	var n int
	err := a.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM transform_definitions WHERE pipeline_id = $1`, pipelineID).Scan(&n)
	return n, err
}

// discoverSourceColumns returns the DISTINCT columns (by name) across the
// source's discovered tables. Transform configs are name-scoped, so a mask/type
// on a column applies wherever that column appears.
func (a *Agent) discoverSourceColumns(ctx context.Context, task ExecutorTask) ([]ColumnMetadata, error) {
	if task.Source == nil {
		return nil, fmt.Errorf("source not specified")
	}
	// DiscoverSchema takes map[string]interface{}; task.Source.Config is
	// map[string]string (mirrors the conversion used elsewhere in this file).
	srcCfg := make(map[string]interface{}, len(task.Source.Config))
	for k, v := range task.Source.Config {
		srcCfg[k] = v
	}
	tables, err := a.DiscoverSchema(ctx, task.Source.Type, srcCfg)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []ColumnMetadata
	for _, tbl := range tables {
		for _, col := range tbl.Columns {
			name := strings.TrimSpace(col.Name)
			if name == "" || seen[strings.ToLower(name)] {
				continue
			}
			seen[strings.ToLower(name)] = true
			out = append(out, col)
		}
	}
	return out, nil
}

// insertNLTransformDefinitions writes each config as BOTH a producer and a
// consumer row (batch export reads producer, CDC sink reads consumer). id is
// left to the table default (uuid_generate_v4()).
func (a *Agent) insertNLTransformDefinitions(ctx context.Context, pipelineID string, configs []map[string]any) error {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	order := 0
	for _, cfg := range configs {
		raw, mErr := json.Marshal(cfg)
		if mErr != nil {
			return fmt.Errorf("marshal transform config: %w", mErr)
		}
		for _, ttype := range []string{"producer", "consumer"} {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO transform_definitions
					(pipeline_id, transform_type, transform_order, transform_config, enabled)
				VALUES ($1, $2, $3, $4, TRUE)
			`, pipelineID, ttype, order, string(raw)); err != nil {
				return err
			}
		}
		order++
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

// ---------------------------------------------------------------------------
// Pure helpers (unit-tested in nl_transforms_gate_test.go). Kept side-effect-free
// so masking/type heuristics can be exercised without a DB or a live connector.
// ---------------------------------------------------------------------------

// buildNLTransforms converts an intent (+ optional discovered columns) into the
// list of stored transform_config maps. Each map carries an `operation` key,
// matching the canonical stored shape that transforms.NormalizeAndValidate
// parses (see shared/go/transforms/validate.go).
// maskGuardError enforces KI-NLCHAT-MASK-SILENT-NOOP. Masking is a privacy
// guarantee, so any masking request we cannot verify against the discovered
// source schema must fail closed rather than silently no-op and land unmasked
// PII. It returns a non-nil error when masking was requested and either:
//   - discovery yielded NO columns (discoverSourceColumns can legitimately return
//     (nil,nil) on a successful-but-empty discovery — the F6-GAP prod case), so we
//     cannot verify ANY mask target exists; or
//   - an explicitly-named mask column is ABSENT from the discovered schema (the
//     engine's exact-key match would no-op → that column lands UNMASKED).
//
// Returns nil when no masking was requested (type-conversion-only intents are
// unaffected) or when every requested mask column is present. Pure + unit-testable.
func maskGuardError(intent *nlTransformIntent, cols []ColumnMetadata) error {
	if intent == nil || (!intent.MaskPII && len(intent.MaskColumns) == 0) {
		return nil
	}
	if len(cols) == 0 {
		return fmt.Errorf("cannot verify requested masking against the source schema "+
			"(discovery returned no columns; mask_columns=%v mask_pii=%v) — refusing to run so "+
			"PII is never silently left unmasked (KI-NLCHAT-MASK-SILENT-NOOP)",
			intent.MaskColumns, intent.MaskPII)
	}
	if missing := missingMaskColumns(intent.MaskColumns, cols); len(missing) > 0 {
		return fmt.Errorf("requested mask column(s) %v not found in source schema — "+
			"refusing to run so PII is never silently left unmasked (KI-NLCHAT-MASK-SILENT-NOOP)", missing)
	}
	return nil
}

// missingMaskColumns returns the explicitly-requested mask column names that are
// NOT present in the discovered source schema (case-insensitive). maskGuardError
// uses it to fail-closed rather than silently no-op a mask on a mistyped/absent
// column (KI-NLCHAT-MASK-SILENT-NOOP). Pure + unit-testable.
func missingMaskColumns(maskColumns []string, cols []ColumnMetadata) []string {
	present := make(map[string]bool, len(cols))
	for _, col := range cols {
		if n := strings.TrimSpace(col.Name); n != "" {
			present[strings.ToLower(n)] = true
		}
	}
	var missing []string
	for _, c := range maskColumns {
		name := strings.TrimSpace(c)
		if name != "" && !present[strings.ToLower(name)] {
			missing = append(missing, name)
		}
	}
	return missing
}

func buildNLTransforms(intent *nlTransformIntent, cols []ColumnMetadata) []map[string]any {
	if intent.empty() {
		return nil
	}
	var out []map[string]any
	masked := map[string]bool{} // lower-cased column name → already has a mask

	// Map lower-cased → canonical column name from the discovered schema (empty
	// when schema was unavailable). Used to fix the casing/spelling of explicit
	// mask columns so the engine's exact-key match actually fires.
	canon := map[string]string{}
	for _, col := range cols {
		if n := strings.TrimSpace(col.Name); n != "" {
			canon[strings.ToLower(n)] = n
		}
	}

	// 1. Explicitly-named mask columns ("mask email"), canonicalized to the real
	// column name when the schema is known.
	for _, c := range intent.MaskColumns {
		name := strings.TrimSpace(c)
		if name == "" {
			continue
		}
		if cn, ok := canon[strings.ToLower(name)]; ok {
			name = cn
		}
		key := strings.ToLower(name)
		if masked[key] {
			continue
		}
		masked[key] = true
		out = append(out, maskConfig(name, piiTypeForColumn(name)))
	}

	// 2. Generic PII masking — detect by column name across the discovered schema.
	if intent.MaskPII {
		for _, col := range cols {
			name := strings.TrimSpace(col.Name)
			if name == "" || masked[strings.ToLower(name)] {
				continue
			}
			if pii := piiTypeForColumn(name); pii != "" {
				masked[strings.ToLower(name)] = true
				out = append(out, maskConfig(name, pii))
			}
		}
	}

	// 3. Type conversion — recommend a numeric/bool target for string-declared
	// columns whose name signals a non-string type. Never convert a column that
	// is already being masked (the mask output is a string digest).
	if intent.TypeConvert {
		for _, col := range cols {
			name := strings.TrimSpace(col.Name)
			if name == "" || masked[strings.ToLower(name)] {
				continue
			}
			if !isStringType(col.Type) {
				continue
			}
			if to := recommendType(name); to != "" {
				out = append(out, typeConvertConfig(name, to))
			}
		}
	}
	return out
}

// maskConfig builds a mask transform_config. Mirrors the shape written by
// api-gateway parseTransformRequest / SuggestionsReviewDialog (operation=mask,
// SHA-256 hash). piiType may be "" (unknown) — the engine masks regardless.
func maskConfig(column, piiType string) map[string]any {
	cfg := map[string]any{
		"operation":     "mask",
		"column":        column,
		"mask_type":     "hash",
		"hash_function": "sha256",
	}
	if piiType != "" {
		cfg["pii_type"] = piiType
	}
	return cfg
}

// typeConvertConfig builds a type_convert transform_config. `to` must be one of
// the engine's supported targets (validate.go): string|int|integer|float|
// double|bool|boolean. on_error=skip keeps the original value when a cell can't
// be converted, so an auto-suggested conversion never silently drops data — a
// genuinely wrong target surfaces at the destination instead of being nulled.
func typeConvertConfig(column, to string) map[string]any {
	return map[string]any{
		"operation": "type_convert",
		"column":    column,
		"to":        to,
		"on_error":  "skip",
	}
}

// piiTypeForColumn classifies a column NAME as PII (high-confidence set only).
// Returns "" when the name is not a recognized PII field. Deliberately excludes
// person names (masking them would break joins and isn't unambiguously PII by
// name alone) — those are masked only when the user names them explicitly.
func piiTypeForColumn(name string) string {
	n := strings.ToLower(strings.TrimSpace(name))
	switch {
	case strings.Contains(n, "email"):
		return "email"
	case strings.Contains(n, "phone") || strings.Contains(n, "mobile") || strings.Contains(n, "fax"):
		return "phone"
	case n == "ssn" || strings.Contains(n, "social_security"):
		return "ssn"
	case strings.Contains(n, "credit_card") || strings.Contains(n, "card_number") || strings.Contains(n, "cardnum"):
		return "credit_card"
	case strings.Contains(n, "passport"):
		return "passport"
	case strings.Contains(n, "national_id") || strings.Contains(n, "tax_id"):
		return "national_id"
	}
	return ""
}

// isStringType reports whether a declared column type is textual (the only kind
// worth converting to a numeric/bool). Substring match handles vendor variants
// (varchar(255), character varying, nvarchar, citext, …).
func isStringType(declared string) bool {
	t := strings.ToLower(strings.TrimSpace(declared))
	if t == "" {
		return false
	}
	for _, s := range []string{"varchar", "text", "char", "string", "citext", "clob", "character"} {
		if strings.Contains(t, s) {
			return true
		}
	}
	return false
}

// textSignalTokens mark a column as textual; when any appears as a whole token
// we never recommend a numeric/bool conversion. This kills substring
// false-positives like "corporate_name"→float (via "rate") or "total_notes"→
// float (via "total") that silently nulled real string columns.
var textSignalTokens = map[string]bool{
	"name": true, "description": true, "desc": true, "note": true, "notes": true,
	"comment": true, "comments": true, "label": true, "title": true, "text": true,
	"status": true, "type": true, "url": true, "uri": true, "slug": true,
	"address": true, "message": true, "summary": true, "reason": true, "code": true,
}

// floatNameTokens / intNameTokens are matched as WHOLE tokens (not substrings).
var floatNameTokens = map[string]bool{
	"price": true, "amount": true, "cost": true, "total": true, "subtotal": true,
	"balance": true, "fee": true, "rate": true, "salary": true, "revenue": true,
	"discount": true, "tax": true,
}
var intNameTokens = map[string]bool{
	"age": true, "year": true, "quantity": true, "qty": true, "count": true,
}

// nameTokens splits a lowercased column name into alphanumeric tokens
// (snake_case, kebab-case, or other non-alphanumeric separators).
func nameTokens(n string) []string {
	return strings.FieldsFunc(n, func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'))
	})
}

// recommendType maps a column NAME to a recommended numeric/bool target, or ""
// to leave it as a string. Conservative by design: it matches whole name TOKENS
// (never substrings) and bails to "" the moment the name carries any textual
// signal, so "name"/"description"/"corporate_name"/"total_notes" stay strings.
// Excludes *_id (often a natural/opaque key that must stay textual).
func recommendType(name string) string {
	n := strings.ToLower(strings.TrimSpace(name))
	if n == "" || strings.HasSuffix(n, "_id") || n == "id" {
		return ""
	}
	tokens := nameTokens(n)
	for _, t := range tokens {
		if textSignalTokens[t] {
			return ""
		}
	}
	// Boolean flags.
	if strings.HasPrefix(n, "is_") || strings.HasPrefix(n, "has_") ||
		strings.HasSuffix(n, "_flag") || n == "enabled" || n == "active" || n == "deleted" {
		return "bool"
	}
	// Floating-point money / measures (whole-token match).
	for _, t := range tokens {
		if floatNameTokens[t] {
			return "float"
		}
	}
	// Integer counts / whole numbers (whole-token match + explicit suffixes).
	for _, t := range tokens {
		if intNameTokens[t] {
			return "int"
		}
	}
	if strings.HasSuffix(n, "_count") || strings.HasSuffix(n, "_qty") || strings.HasSuffix(n, "_quantity") {
		return "int"
	}
	return ""
}
