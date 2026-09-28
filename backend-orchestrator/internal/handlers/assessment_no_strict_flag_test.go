package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/rsync-ai/backend-orchestrator/internal/assessor"
)

// STRICT_PREFLIGHT was a flag the assessment API advertised (strict_preflight
// on /assess/supported-types, strict_preflight_on on every assessment row)
// while nothing enforced it: IsPipelineBlockedByPreflight, the only reader,
// had no caller. The run gate that does block a failed assessment lives in the
// api-gateway (RunPipeline → evaluateAssessmentGate) and is unconditional, so
// the flag only told an operator a policy was on or off when it was neither.
// Setting it must not resurrect the claim.
func TestAssessmentAPIDoesNotAdvertiseAStrictPreflightFlag(t *testing.T) {
	t.Setenv("STRICT_PREFLIGHT", "true")
	gin.SetMode(gin.TestMode)

	h := NewAssessmentHandler(nil, nil, assessor.NewRegistry())
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/assess/supported-types", nil)
	h.SupportedTypes(c)

	if w.Code != http.StatusOK {
		t.Fatalf("supported-types status = %d", w.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := body["supported"]; !ok {
		t.Fatalf("supported-types lost its `supported` list: %s", w.Body.String())
	}
	if v, ok := body["strict_preflight"]; ok {
		t.Errorf("supported-types advertises strict_preflight=%v, a policy nothing enforces", v)
	}

	raw, err := json.Marshal(assessmentRow{BlocksStart: true})
	if err != nil {
		t.Fatal(err)
	}
	var row map[string]any
	if err := json.Unmarshal(raw, &row); err != nil {
		t.Fatal(err)
	}
	if v, ok := row["strict_preflight_on"]; ok {
		t.Errorf("assessment rows carry strict_preflight_on=%v, a policy nothing enforces", v)
	}
	if row["blocks_start"] != true {
		t.Error("assessment rows lost blocks_start, which is persisted and still meaningful")
	}
}
