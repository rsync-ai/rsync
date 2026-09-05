package handlers

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"api-gateway/internal/telemetry"

	"github.com/gin-gonic/gin"
)

// GenerateSuggestions proxies AI-powered data suggestions (PII + transforms + optimizations)
// to the internal llm-service.
//
// Request body (pass-through):
//
//	{
//	  "schema": { ... },
//	  "intent": { ... },
//	  "discovery_result": { ... } // optional
//	}
//
// Response body: passthrough of llm-service SuggestionsResponse.
func GenerateSuggestions(c *gin.Context) {
	var payload map[string]interface{}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request", "message": err.Error()})
		return
	}

	llmURL := strings.TrimSpace(os.Getenv("LLM_SERVICE_URL"))
	if llmURL == "" {
		// docker-compose default
		llmURL = "http://llm-service:5000"
	}
	llmURL = strings.TrimRight(llmURL, "/")

	body, err := json.Marshal(payload)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request", "message": "failed to encode request"})
		return
	}

	req, err := http.NewRequestWithContext(
		c.Request.Context(),
		http.MethodPost,
		llmURL+"/api/v1/agents/suggestions/generate",
		bytes.NewReader(body),
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "request_build_failed"})
		return
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range telemetry.InjectTraceToHeaders(c.Request.Context()) {
		req.Header.Set(k, v)
	}

	// PII detection inside the LLM service walks every column with
	// Presidio + regex fallbacks and can take 15-25s on a wide schema
	// (Shopify orders has 124 columns). A 30s ceiling tripped the
	// "Failed to generate suggestions" UI banner whenever a retry or
	// a parallel call hit the slower path. 120s is the practical
	// upper bound observed during the round-3 testing — large enough
	// to absorb the worst case while still failing fast on a hung
	// dependency.
	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		respondError(c, http.StatusBadGateway, "llm_service_unreachable", "LLM service is unreachable", err)
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	c.Data(resp.StatusCode, "application/json", respBody)
}
