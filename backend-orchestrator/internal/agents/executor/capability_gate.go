package executor

// Capability gate for the universal-blob-passthrough feature.
//
// A "move modality" describes WHAT KIND of data a pipeline moves, independent of
// serialization format:
//   - "structured": row records (typed columns) — the existing, universal lane.
//   - "blob":       opaque bytes copied byte-identical (raw object → object copy).
//
// The gate keys off a connector's declared capabilities.supported_modalities. It
// rejects — fail-loud, at planning time, before any object is staged — a move
// whose modality the DESTINATION connector does not accept (e.g. a raw-file copy
// into a structured-only destination such as a relational table). This makes the
// user's "capability issue" explicit instead of a silent drop.
//
// Phase 1 ships the guardrail only: blob is strictly OPT-IN (deriveMoveModality),
// and nothing sets the opt-in yet, so the gate is a true no-op for every existing
// pipeline. Phase 2/3 add the source export + sink write that the gate protects.

import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	// ModalityStructured is the universal default: every connector can move row
	// records. A connector that declares no supported_modalities is treated as
	// structured-only.
	ModalityStructured = "structured"
	// ModalityBlob is opaque-bytes passthrough (raw object copy). Declared only by
	// connectors that can read/write raw bytes with a content-type (object storage).
	ModalityBlob = "blob"

	// CapabilityMismatchCode is the stable error_code surfaced to the pipeline
	// status when a move modality is not accepted by the destination connector.
	CapabilityMismatchCode = "capability_mismatch"
)

// CapabilityMismatch is the typed, fail-loud rejection produced by the gate.
// It implements error so callers can surface a clear human message.
type CapabilityMismatch struct {
	SourceType     string
	DestType       string
	MoveModality   string
	DestModalities []string
}

func (e *CapabilityMismatch) Error() string {
	src := e.SourceType
	if src == "" {
		src = "source"
	}
	dst := e.DestType
	if dst == "" {
		dst = "destination"
	}
	return fmt.Sprintf(
		"capability_mismatch: source %q emits %s data; destination %q accepts {%s} only — cannot move",
		src, e.MoveModality, dst, strings.Join(e.DestModalities, ", "),
	)
}

// deriveMoveModality determines the pipeline's move modality from explicit intent.
// Blob (raw passthrough) is OPT-IN — a user must ask to copy files as-is — so the
// default is "structured" and every existing pipeline is unaffected. Recognized
// opt-in signals (any one is enough):
//   - params["copy_raw_files"] / ["passthrough"] / ["raw_files"] truthy
//   - params["move_modality"] == "blob"
//   - params["delivery_method"] in {copy_raw_files, raw, raw_files, blob}
//
// Matches Airbyte's "Copy Raw Files" delivery-method opt-in (plan §0.5).
func deriveMoveModality(params map[string]interface{}) string {
	if params == nil {
		return ModalityStructured
	}
	if paramTruthy(params["copy_raw_files"]) || paramTruthy(params["passthrough"]) || paramTruthy(params["raw_files"]) {
		return ModalityBlob
	}
	switch strings.ToLower(strings.TrimSpace(paramString(params["move_modality"]))) {
	case ModalityBlob:
		return ModalityBlob
	case ModalityStructured:
		return ModalityStructured
	}
	switch strings.ToLower(strings.TrimSpace(paramString(params["delivery_method"]))) {
	case "copy_raw_files", "raw", "raw_files", ModalityBlob:
		return ModalityBlob
	}
	return ModalityStructured
}

// evaluateCapabilityGate enforces the §2 rule: the move modality must be present
// in the destination's declared modalities. Returns nil when allowed, a populated
// *CapabilityMismatch (with MoveModality + DestModalities) when rejected. Pure and
// side-effect-free — the caller fills SourceType/DestType and logs/wraps it.
//
// destModalities must already be defaulted to ["structured"] when the destination
// connector declares none (see modalitiesFromCapabilities / destSupportedModalities).
func evaluateCapabilityGate(moveModality string, destModalities []string) *CapabilityMismatch {
	move := strings.TrimSpace(moveModality)
	if move == "" {
		move = ModalityStructured
	}
	for _, m := range destModalities {
		if strings.EqualFold(strings.TrimSpace(m), move) {
			return nil
		}
	}
	return &CapabilityMismatch{MoveModality: move, DestModalities: destModalities}
}

// modalitiesFromCapabilities extracts capabilities.supported_modalities from a
// connector's parsed metadata block (mcp.ConnectorMetadata.Capabilities is an
// interface{} — a JSON object becomes map[string]interface{}). Defaults to
// ["structured"] when the field is absent, empty, the wrong shape, or the
// capabilities block is array-form — every connector is structured-capable.
func modalitiesFromCapabilities(caps interface{}) []string {
	def := []string{ModalityStructured}
	m, ok := caps.(map[string]interface{})
	if !ok {
		return def
	}
	raw, ok := m["supported_modalities"]
	if !ok {
		return def
	}
	arr, ok := raw.([]interface{})
	if !ok {
		return def
	}
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		if s, ok := v.(string); ok {
			if s = strings.TrimSpace(s); s != "" {
				out = append(out, s)
			}
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

// destSupportedModalities returns the destination connector's declared move
// modalities, read from its metadata.json via the MCP server manager.
//
// FAIL-CLOSED: on any metadata-read error (or no manager) we return only the
// default ["structured"], so a blob move into an unverifiable destination is
// REJECTED by the gate rather than silently attempted — matching the plan's
// fail-loud guardrail (§5). This is only ever reached for non-structured moves.
func (a *Agent) destSupportedModalities(destType, destVersion string) []string {
	def := []string{ModalityStructured}
	if a == nil || a.mcpManager == nil || strings.TrimSpace(destType) == "" {
		return def
	}
	meta, err := a.mcpManager.GetConnectorMetadata(destType, destVersion)
	if err != nil || meta == nil {
		log.Warnf("⚠️  capability gate: could not read metadata for destination %q (%v); treating as structured-only", destType, err)
		return def
	}
	return modalitiesFromCapabilities(meta.Capabilities)
}

// enforceCapabilityGate is the wired entry point called from executeDataTransfer.
// Returns a non-nil *ExecutorResponse (Status=failed, error_code=capability_mismatch)
// when the move is rejected; nil when allowed.
//
// Structured moves are allowed unconditionally — every connector is
// structured-capable — so this is a TRUE no-op for all existing pipelines and never
// reads destination metadata on that hot path. Only an explicit blob opt-in
// triggers a destination-capability lookup.
func (a *Agent) enforceCapabilityGate(task *ExecutorTask) *ExecutorResponse {
	moveModality := deriveMoveModality(task.Params)
	if moveModality == ModalityStructured {
		return nil
	}

	destType, destVersion := "", ""
	if task.Destination != nil {
		destType = task.Destination.Type
		destVersion = task.Destination.Version
	}
	destMods := a.destSupportedModalities(destType, destVersion)

	mismatch := evaluateCapabilityGate(moveModality, destMods)
	if mismatch == nil {
		return nil
	}

	srcType := ""
	if task.Source != nil {
		srcType = task.Source.Type
	}
	mismatch.SourceType = srcType
	mismatch.DestType = destType

	log.WithFields(log.Fields{
		"pipeline_id":     task.PipelineID,
		"source_type":     srcType,
		"dest_type":       destType,
		"move_modality":   moveModality,
		"dest_modalities": destMods,
	}).Error("❌ capability_mismatch: rejecting pipeline before any data is staged")

	return &ExecutorResponse{
		TaskID:     task.TaskID,
		PipelineID: task.PipelineID,
		Status:     "failed",
		Error:      mismatch.Error(),
		Result: map[string]interface{}{
			"error_code":      CapabilityMismatchCode,
			"source_type":     srcType,
			"dest_type":       destType,
			"move_modality":   moveModality,
			"dest_modalities": destMods,
			"message":         mismatch.Error(),
		},
	}
}

// paramTruthy interprets a loosely-typed pipeline param as a boolean opt-in.
// Accepts real bools and the strings "true"/"1"/"yes"/"on" (case-insensitive).
func paramTruthy(v interface{}) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "true", "1", "yes", "on":
			return true
		}
	}
	return false
}

// paramString returns a string param value, or "" for nil/non-string.
func paramString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
