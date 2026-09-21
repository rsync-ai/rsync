package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// llm-service answers 503 {"error":"llm_not_configured","message":"Set up an LLM
// first: ..."} when a feature needs a model and none is set up
// (llm-service/src/utils/llm_gate.py). A route without the handler registered
// nests the same payload under "detail". Relaying it as-is lets the UI tell the
// operator what to do instead of showing a generic upstream failure.
const llmNotConfiguredCode = "llm_not_configured"

const llmNotConfiguredFallbackMessage = "Set up an LLM first: add OPENAI_API_KEY (or another provider's key) to .env, " +
	"or set LLM_PROVIDER=ollama for a local model, then restart rsync."

// llmNotConfiguredBody returns the body to relay when an llm-service response
// says no LLM is set up. ok is false for every other response, including a plain
// 503 from a busy or starting service.
func llmNotConfiguredBody(status int, body []byte) (gin.H, bool) {
	if status != http.StatusServiceUnavailable {
		return nil, false
	}
	type gate struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	var flat struct {
		gate
		Detail json.RawMessage `json:"detail"`
	}
	if err := json.Unmarshal(body, &flat); err != nil {
		return nil, false
	}
	found := flat.gate
	if found.Error != llmNotConfiguredCode && len(flat.Detail) > 0 {
		var nested gate
		if json.Unmarshal(flat.Detail, &nested) == nil {
			found = nested
		}
	}
	if found.Error != llmNotConfiguredCode {
		return nil, false
	}
	msg := strings.TrimSpace(found.Message)
	if msg == "" {
		msg = llmNotConfiguredFallbackMessage
	}
	return gin.H{"error": llmNotConfiguredCode, "message": msg}, true
}

// relayLLMNotConfigured writes the llm_not_configured answer and reports true
// when the upstream response is one; otherwise it writes nothing.
func relayLLMNotConfigured(c *gin.Context, status int, body []byte) bool {
	h, ok := llmNotConfiguredBody(status, body)
	if !ok {
		return false
	}
	c.JSON(http.StatusServiceUnavailable, h)
	return true
}

// llmNotConfiguredError carries the relayable answer out of a helper that
// returns an error rather than writing the response itself.
type llmNotConfiguredError struct {
	body gin.H
}

func (e *llmNotConfiguredError) Error() string {
	return llmNotConfiguredCode + ": " + e.body["message"].(string)
}

// Body returns a copy the caller may add fields to.
func (e *llmNotConfiguredError) Body() gin.H {
	out := gin.H{}
	for k, v := range e.body {
		out[k] = v
	}
	return out
}
