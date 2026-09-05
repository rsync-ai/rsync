package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// SchemaRegistryHandler handles Schema Registry operations
type SchemaRegistryHandler struct {
	registryURL string
	client      *http.Client
}

// SchemaInfo represents a schema from the registry
type SchemaInfo struct {
	Subject    string `json:"subject"`
	Version    int    `json:"version"`
	ID         int    `json:"id"`
	SchemaType string `json:"schemaType,omitempty"`
	Schema     string `json:"schema"`
}

// NewSchemaRegistryHandler creates a new Schema Registry handler
func NewSchemaRegistryHandler() *SchemaRegistryHandler {
	registryURL := os.Getenv("SCHEMA_REGISTRY_URL")
	if registryURL == "" {
		registryURL = "http://schema-registry:8080"
	}

	return &SchemaRegistryHandler{
		registryURL: registryURL,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// GetRegistryInfo returns Schema Registry info
func (h *SchemaRegistryHandler) GetRegistryInfo(c *gin.Context) {
	resp, err := h.client.Get(h.registryURL)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":        "registry_unavailable",
			"message":      "Schema Registry is not available",
			"details":      err.Error(),
			"registry_url": h.registryURL,
		})
		return
	}
	defer resp.Body.Close()

	body, rerr := io.ReadAll(resp.Body)
	if rerr != nil {
		respondError(c, http.StatusBadGateway, "registry_read_failed", "Failed to read Schema Registry response", rerr)
		return
	}

	var info map[string]interface{}
	if err := json.Unmarshal(body, &info); err != nil {
		respondError(c, http.StatusBadGateway, "registry_parse_failed", "Failed to parse Schema Registry response", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":       "available",
		"registry_url": h.registryURL,
		"info":         info,
	})
}

// ListSubjects returns all subjects (schemas) in the registry
func (h *SchemaRegistryHandler) ListSubjects(c *gin.Context) {
	resp, err := h.client.Get(h.registryURL + "/subjects")
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   "registry_unavailable",
			"message": err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	body, rerr := io.ReadAll(resp.Body)
	if rerr != nil {
		respondError(c, http.StatusBadGateway, "registry_read_failed", "Failed to read Schema Registry response", rerr)
		return
	}

	var subjects []string
	if err := json.Unmarshal(body, &subjects); err != nil {
		respondError(c, http.StatusBadGateway, "subjects_parse_failed", "Failed to parse subjects", err)
		return
	}

	// Filter to CDC-related subjects if requested
	filterCDC := c.Query("cdc_only") == "true"
	if filterCDC {
		filtered := make([]string, 0)
		for _, s := range subjects {
			if containsCDCKeyword(s) {
				filtered = append(filtered, s)
			}
		}
		subjects = filtered
	}

	c.JSON(http.StatusOK, gin.H{
		"subjects": subjects,
		"count":    len(subjects),
	})
}

// GetSubjectVersions returns all versions of a subject
func (h *SchemaRegistryHandler) GetSubjectVersions(c *gin.Context) {
	subject := c.Param("subject")

	resp, err := h.client.Get(fmt.Sprintf("%s/subjects/%s/versions", h.registryURL, subject))
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   "registry_unavailable",
			"message": err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		c.JSON(http.StatusNotFound, gin.H{
			"error":   "subject_not_found",
			"message": fmt.Sprintf("Subject '%s' not found", subject),
		})
		return
	}

	body, rerr := io.ReadAll(resp.Body)
	if rerr != nil {
		respondError(c, http.StatusBadGateway, "registry_read_failed", "Failed to read Schema Registry response", rerr)
		return
	}

	var versions []int
	if err := json.Unmarshal(body, &versions); err != nil {
		respondError(c, http.StatusBadGateway, "versions_parse_failed", "Failed to parse subject versions", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"subject":  subject,
		"versions": versions,
		"count":    len(versions),
	})
}

// GetSchema returns a specific schema version
func (h *SchemaRegistryHandler) GetSchema(c *gin.Context) {
	subject := c.Param("subject")
	version := c.Param("version")

	if version == "" {
		version = "latest"
	}

	resp, err := h.client.Get(fmt.Sprintf("%s/subjects/%s/versions/%s", h.registryURL, subject, version))
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   "registry_unavailable",
			"message": err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		c.JSON(http.StatusNotFound, gin.H{
			"error":   "schema_not_found",
			"message": fmt.Sprintf("Schema '%s' version '%s' not found", subject, version),
		})
		return
	}

	body, rerr := io.ReadAll(resp.Body)
	if rerr != nil {
		respondError(c, http.StatusBadGateway, "registry_read_failed", "Failed to read Schema Registry response", rerr)
		return
	}

	var schema SchemaInfo
	if err := json.Unmarshal(body, &schema); err != nil {
		respondError(c, http.StatusBadGateway, "schema_parse_failed", "Failed to parse schema response", err)
		return
	}

	// Parse the schema JSON string for better display
	var parsedSchema interface{}
	if schema.Schema != "" {
		// Best-effort: schema.Schema is a JSON string containing the actual schema.
		_ = json.Unmarshal([]byte(schema.Schema), &parsedSchema)
	}

	c.JSON(http.StatusOK, gin.H{
		"subject":       schema.Subject,
		"version":       schema.Version,
		"id":            schema.ID,
		"schema_type":   schema.SchemaType,
		"schema_raw":    schema.Schema,
		"schema_parsed": parsedSchema,
	})
}

// RegisterSchemaRequest for registering new schemas
type RegisterSchemaRequest struct {
	Schema     string `json:"schema" binding:"required"`
	SchemaType string `json:"schemaType,omitempty"` // AVRO, JSON, PROTOBUF
}

// RegisterSchema registers a new schema
func (h *SchemaRegistryHandler) RegisterSchema(c *gin.Context) {
	subject := c.Param("subject")

	var req RegisterSchemaRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "invalid_request",
			"message": err.Error(),
		})
		return
	}

	// Prepare request body
	body := map[string]string{
		"schema": req.Schema,
	}
	if req.SchemaType != "" {
		body["schemaType"] = req.SchemaType
	}

	bodyBytes, mErr := json.Marshal(body)
	if mErr != nil {
		respondError(c, http.StatusBadRequest, "invalid_schema", "Invalid schema payload", mErr)
		return
	}

	httpReq, err := http.NewRequest("POST",
		fmt.Sprintf("%s/subjects/%s/versions", h.registryURL, subject),
		bytes.NewReader(bodyBytes))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "request_failed",
			"message": err.Error(),
		})
		return
	}
	httpReq.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := h.client.Do(httpReq)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   "registry_unavailable",
			"message": err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	respBody, rerr := io.ReadAll(resp.Body)
	if rerr != nil {
		respondError(c, http.StatusBadGateway, "registry_read_failed", "Failed to read Schema Registry response", rerr)
		return
	}

	if resp.StatusCode != http.StatusOK {
		c.JSON(resp.StatusCode, gin.H{
			"error":   "registration_failed",
			"message": string(respBody),
		})
		return
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		respondError(c, http.StatusBadGateway, "registration_parse_failed", "Failed to parse schema registration response", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"subject": subject,
		"id":      result["id"],
	})
}

// CheckCompatibility checks schema compatibility
func (h *SchemaRegistryHandler) CheckCompatibility(c *gin.Context) {
	subject := c.Param("subject")
	version := c.DefaultQuery("version", "latest")

	var req RegisterSchemaRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "invalid_request",
			"message": err.Error(),
		})
		return
	}

	body := map[string]string{
		"schema": req.Schema,
	}
	bodyBytes, mErr := json.Marshal(body)
	if mErr != nil {
		respondError(c, http.StatusBadRequest, "invalid_schema", "Invalid schema payload", mErr)
		return
	}

	httpReq, reqErr := http.NewRequest("POST",
		fmt.Sprintf("%s/compatibility/subjects/%s/versions/%s", h.registryURL, subject, version),
		bytes.NewReader(bodyBytes))
	if reqErr != nil {
		respondError(c, http.StatusInternalServerError, "request_failed", "Failed to build request", reqErr)
		return
	}
	httpReq.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := h.client.Do(httpReq)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   "registry_unavailable",
			"message": err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	respBody, rerr := io.ReadAll(resp.Body)
	if rerr != nil {
		respondError(c, http.StatusBadGateway, "registry_read_failed", "Failed to read Schema Registry response", rerr)
		return
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		respondError(c, http.StatusBadGateway, "compatibility_parse_failed", "Failed to parse compatibility response", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"subject":       subject,
		"version":       version,
		"is_compatible": result["is_compatible"],
	})
}

// GetConfig returns global or subject-level config
func (h *SchemaRegistryHandler) GetConfig(c *gin.Context) {
	subject := c.Param("subject")

	var url string
	if subject != "" {
		url = fmt.Sprintf("%s/config/%s", h.registryURL, subject)
	} else {
		url = fmt.Sprintf("%s/config", h.registryURL)
	}

	resp, err := h.client.Get(url)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   "registry_unavailable",
			"message": err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	body, rerr := io.ReadAll(resp.Body)
	if rerr != nil {
		respondError(c, http.StatusBadGateway, "registry_read_failed", "Failed to read Schema Registry response", rerr)
		return
	}

	var config map[string]interface{}
	if err := json.Unmarshal(body, &config); err != nil {
		respondError(c, http.StatusBadGateway, "config_parse_failed", "Failed to parse Schema Registry config", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"subject": subject,
		"config":  config,
	})
}

// SetConfigRequest for setting compatibility level
type SetConfigRequest struct {
	Compatibility string `json:"compatibility" binding:"required"`
}

// SetConfig sets compatibility level
func (h *SchemaRegistryHandler) SetConfig(c *gin.Context) {
	subject := c.Param("subject")

	var req SetConfigRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "invalid_request",
			"message": err.Error(),
			"hint":    "compatibility must be one of: BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE, NONE",
		})
		return
	}

	var url string
	if subject != "" {
		url = fmt.Sprintf("%s/config/%s", h.registryURL, subject)
	} else {
		url = fmt.Sprintf("%s/config", h.registryURL)
	}

	bodyBytes, mErr := json.Marshal(map[string]string{
		"compatibility": req.Compatibility,
	})
	if mErr != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid compatibility payload", mErr)
		return
	}

	httpReq, reqErr := http.NewRequest("PUT", url, bytes.NewReader(bodyBytes))
	if reqErr != nil {
		respondError(c, http.StatusInternalServerError, "request_failed", "Failed to build request", reqErr)
		return
	}
	httpReq.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := h.client.Do(httpReq)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   "registry_unavailable",
			"message": err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	respBody, rerr := io.ReadAll(resp.Body)
	if rerr != nil {
		respondError(c, http.StatusBadGateway, "registry_read_failed", "Failed to read Schema Registry response", rerr)
		return
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		respondError(c, http.StatusBadGateway, "config_set_parse_failed", "Failed to parse config update response", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success":       true,
		"subject":       subject,
		"compatibility": req.Compatibility,
	})
}

// DeleteSubject deletes a subject and all its versions
func (h *SchemaRegistryHandler) DeleteSubject(c *gin.Context) {
	subject := c.Param("subject")
	permanent := c.Query("permanent") == "true"

	url := fmt.Sprintf("%s/subjects/%s", h.registryURL, subject)
	if permanent {
		url += "?permanent=true"
	}

	req, _ := http.NewRequest("DELETE", url, nil)
	resp, err := h.client.Do(req)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   "registry_unavailable",
			"message": err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		c.JSON(http.StatusNotFound, gin.H{
			"error":   "subject_not_found",
			"message": fmt.Sprintf("Subject '%s' not found", subject),
		})
		return
	}

	body, rerr := io.ReadAll(resp.Body)
	if rerr != nil {
		respondError(c, http.StatusBadGateway, "registry_read_failed", "Failed to read Schema Registry response", rerr)
		return
	}

	var versions []int
	if err := json.Unmarshal(body, &versions); err != nil {
		respondError(c, http.StatusBadGateway, "versions_parse_failed", "Failed to parse deleted subject versions", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success":          true,
		"subject":          subject,
		"deleted_versions": versions,
		"permanent":        permanent,
	})
}

// Helper function to check for CDC keywords
func containsCDCKeyword(s string) bool {
	lower := strings.ToLower(s)
	keywords := []string{"cdc", "debezium", "connect", "schema-changes"}
	for _, kw := range keywords {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}
