package handlers

import (
	"encoding/json"
	"testing"
)

// TestValidateConnectorResponse_NearDuplicatePassthrough guards the DTO boundary
// bug that staging E2E caught: the gateway's ValidateConnector forwards to the
// tool-generator's /v1/validate, unmarshals the body into ValidateConnectorResponse,
// then re-marshals it to the client (connector_generator.go). If the struct lacks a
// field, that field is silently dropped here and never reaches the UI — which is what
// happened to near_duplicate_connectors. This asserts it survives the round-trip.
func TestValidateConnectorResponse_NearDuplicatePassthrough(t *testing.T) {
	// Shape emitted by the tool-generator's /v1/validate for a near-duplicate id.
	upstream := []byte(`{
		"valid": true,
		"connector_name": "stripe-demo",
		"normalized_name": "stripe-demo",
		"is_known_api": false,
		"has_documentation": false,
		"similar_connectors": [],
		"suggestions": [],
		"confidence": 0.5,
		"can_generate": true,
		"near_duplicate_connectors": ["Stripe (stripe)"]
	}`)

	var resp ValidateConnectorResponse
	if err := json.Unmarshal(upstream, &resp); err != nil {
		t.Fatalf("unmarshal upstream body: %v", err)
	}

	if len(resp.NearDuplicateConnectors) != 1 || resp.NearDuplicateConnectors[0] != "Stripe (stripe)" {
		t.Fatalf("near_duplicate_connectors not captured from upstream: %+v", resp.NearDuplicateConnectors)
	}
	if !resp.CanGenerate {
		t.Fatalf("advisory must not block generation: can_generate=%v", resp.CanGenerate)
	}

	// Re-marshal (what c.JSON does) and confirm the field reaches the client.
	out, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	var roundTrip map[string]any
	if err := json.Unmarshal(out, &roundTrip); err != nil {
		t.Fatalf("unmarshal round-trip: %v", err)
	}
	got, ok := roundTrip["near_duplicate_connectors"].([]any)
	if !ok || len(got) != 1 || got[0] != "Stripe (stripe)" {
		t.Fatalf("near_duplicate_connectors dropped on re-marshal: %v", roundTrip["near_duplicate_connectors"])
	}
}

// TestValidateConnectorResponse_NearDuplicateOmitEmpty confirms the advisory field is
// omitted (not null/[]) when empty, so ordinary validations don't carry noise.
func TestValidateConnectorResponse_NearDuplicateOmitEmpty(t *testing.T) {
	out, err := json.Marshal(ValidateConnectorResponse{Valid: true, CanGenerate: true})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, present := m["near_duplicate_connectors"]; present {
		t.Fatalf("expected near_duplicate_connectors to be omitted when empty, got: %s", out)
	}
}
