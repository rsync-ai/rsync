package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
)

// ValidateConnector used to answer valid:true, can_generate:true whenever the
// tool-generator could not be reached or its answer could not be read, and to
// decode error bodies as if they were answers. A connector must never be
// reported valid because the check did not happen.

const validateTestName = "acme-orders-api"

const validateGatedSentence = "Set up an LLM first: add a provider key to .env, then restart rsync."

// validateUpstream starts a fake tool-generator that counts its calls.
func validateUpstream(t *testing.T, handler http.HandlerFunc) (*int64, string) {
	t.Helper()
	var calls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&calls, 1)
		if r.URL.Path != "/v1/validate" {
			t.Errorf("unexpected upstream path %q", r.URL.Path)
		}
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	return &calls, srv.URL
}

func validateFixedAnswer(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

func callValidate(t *testing.T, toolGeneratorURL string) (*httptest.ResponseRecorder, map[string]interface{}) {
	t.Helper()
	return callValidateNamed(t, toolGeneratorURL, validateTestName)
}

// callValidateNamed sends connectorName exactly as typed.
func callValidateNamed(t *testing.T, toolGeneratorURL, connectorName string) (*httptest.ResponseRecorder, map[string]interface{}) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	// No connector exists locally, so the handler must ask the tool-generator.
	t.Setenv("MCP_CONNECTORS_PATH", t.TempDir())
	t.Setenv("TOOL_GENERATOR_URL", toolGeneratorURL)

	reqBody, _ := json.Marshal(map[string]string{"connector_name": connectorName})
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/connectors/validate", bytes.NewReader(reqBody))
	c.Request.Header.Set("Content-Type", "application/json")

	ValidateConnector(c)
	c.Writer.WriteHeaderNow()

	var out map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("response is not JSON: %v (%s)", err, w.Body.String())
	}
	return w, out
}

// assertValidateCheckFailed checks what the wizard needs to keep Discover disabled and show
// why: a 2xx answer (it reads the body only then), valid and can_generate
// false, and a plain warning.
func assertValidateCheckFailed(t *testing.T, w *httptest.ResponseRecorder, got map[string]interface{}, wantInWarning string) {
	t.Helper()
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 so the wizard reads the body (%s)", w.Code, w.Body.String())
	}
	if got["valid"] != false {
		t.Errorf("valid = %v, want false: a failed check must never report the connector valid", got["valid"])
	}
	if got["can_generate"] != false {
		t.Errorf("can_generate = %v, want false", got["can_generate"])
	}
	if got["validation_unavailable"] != true {
		t.Errorf("validation_unavailable = %v, want true", got["validation_unavailable"])
	}
	warning, _ := got["warning"].(string)
	if !strings.HasPrefix(warning, "Could not check this connector name") {
		t.Errorf("warning = %q, want a plain sentence saying the check failed", warning)
	}
	if wantInWarning != "" && !strings.Contains(warning, wantInWarning) {
		t.Errorf("warning = %q, want it to mention %q", warning, wantInWarning)
	}
	if got["connector_name"] != validateTestName {
		t.Errorf("connector_name = %v", got["connector_name"])
	}
	if got["normalized_name"] != validateTestName {
		t.Errorf("normalized_name = %v", got["normalized_name"])
	}
}

func TestValidateConnectorUnreachableIsNeverValid(t *testing.T) {
	// A server that is started and closed gives an address that refuses.
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()

	w, got := callValidate(t, deadURL)
	assertValidateCheckFailed(t, w, got, "did not answer")
}

func TestValidateConnectorBadGeneratorAddressIsNeverValid(t *testing.T) {
	w, got := callValidate(t, "http://bad host")
	assertValidateCheckFailed(t, w, got, "TOOL_GENERATOR_URL")
}

func TestValidateConnectorNon2xxIsNeverValid(t *testing.T) {
	for name, tc := range map[string]struct {
		status int
		body   string
	}{
		"404 route missing": {http.StatusNotFound, `{"detail":"Not Found"}`},
		"500":               {http.StatusInternalServerError, `{"detail":"boom"}`},
		"busy 503":          {http.StatusServiceUnavailable, `{"detail":"busy"}`},
		"502 not json":      {http.StatusBadGateway, `upstream connect error`},
		// An error body that happens to carry valid:true must still not count.
		"500 claiming valid": {http.StatusInternalServerError, `{"valid":true,"can_generate":true}`},
	} {
		t.Run(name, func(t *testing.T) {
			calls, url := validateUpstream(t, validateFixedAnswer(tc.status, tc.body))
			w, got := callValidate(t, url)
			if atomic.LoadInt64(calls) == 0 {
				t.Fatal("tool-generator was never called")
			}
			assertValidateCheckFailed(t, w, got, "status "+strconv.Itoa(tc.status))
		})
	}
}

func TestValidateConnectorUnreadableAnswerIsNeverValid(t *testing.T) {
	t.Run("200 not json", func(t *testing.T) {
		calls, url := validateUpstream(t, validateFixedAnswer(http.StatusOK, `<html>proxy page</html>`))
		w, got := callValidate(t, url)
		if atomic.LoadInt64(calls) == 0 {
			t.Fatal("tool-generator was never called")
		}
		assertValidateCheckFailed(t, w, got, "could not be read")
	})
	t.Run("body cut short", func(t *testing.T) {
		calls, url := validateUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Length", "500")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"valid":true`))
		})
		w, got := callValidate(t, url)
		if atomic.LoadInt64(calls) == 0 {
			t.Fatal("tool-generator was never called")
		}
		assertValidateCheckFailed(t, w, got, "could not be read")
	})
}

func TestValidateConnectorRelaysLLMNotConfigured(t *testing.T) {
	for name, body := range map[string]string{
		"flat":   `{"error":"llm_not_configured","message":"` + validateGatedSentence + `"}`,
		"nested": `{"detail":{"error":"llm_not_configured","message":"` + validateGatedSentence + `"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			calls, url := validateUpstream(t, validateFixedAnswer(http.StatusServiceUnavailable, body))
			// Typed with spaces, so what the user typed and the normalized id differ.
			typed := "  " + validateTestName + " "
			w, got := callValidateNamed(t, url, typed)
			if atomic.LoadInt64(calls) == 0 {
				t.Fatal("tool-generator was never called")
			}
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503 (%s)", w.Code, w.Body.String())
			}
			if got["error"] != llmNotConfiguredCode || got["message"] != validateGatedSentence {
				t.Fatalf("body = %v, want the standard not-configured body", got)
			}
			if got["valid"] != false || got["can_generate"] != false {
				t.Fatalf("valid=%v can_generate=%v, want both false", got["valid"], got["can_generate"])
			}
			// A caller that shows the name check's warning shows the setup sentence.
			if got["warning"] != validateGatedSentence {
				t.Errorf("warning = %v, want the service's sentence", got["warning"])
			}
			if got["connector_name"] != typed {
				t.Errorf("connector_name = %q, want the name as typed %q", got["connector_name"], typed)
			}
			if got["normalized_name"] != validateTestName {
				t.Errorf("normalized_name = %v, want %q", got["normalized_name"], validateTestName)
			}
		})
	}
}

// A real answer still passes through unchanged, for any 2xx status.
func TestValidateConnectorForwardsARealAnswer(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusCreated} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var sent map[string]string
			calls, url := validateUpstream(t, func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewDecoder(r.Body).Decode(&sent)
				validateFixedAnswer(status, `{"valid":true,"connector_name":"acme-orders-api","normalized_name":"acme-orders-api",`+
					`"is_known_api":false,"has_documentation":false,"similar_connectors":[],"suggestions":[],`+
					`"confidence":0.5,"can_generate":true,"near_duplicate_connectors":["Acme (acme)"]}`)(w, r)
			})
			w, got := callValidate(t, url)
			if atomic.LoadInt64(calls) != 1 {
				t.Fatalf("tool-generator calls = %d, want 1", atomic.LoadInt64(calls))
			}
			if sent["connector_name"] != validateTestName {
				t.Fatalf("sent connector_name = %q", sent["connector_name"])
			}
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d (%s)", w.Code, w.Body.String())
			}
			if got["valid"] != true || got["can_generate"] != true {
				t.Fatalf("a real valid answer was not forwarded: %v", got)
			}
			if _, has := got["validation_unavailable"]; has {
				t.Fatalf("a real answer must not be marked unavailable: %v", got)
			}
			dups, _ := got["near_duplicate_connectors"].([]interface{})
			if len(dups) != 1 {
				t.Fatalf("near_duplicate_connectors = %v", got["near_duplicate_connectors"])
			}
		})
	}
}

// The answer keeps the name as typed and adds the canonical id, whether the
// check failed or the tool-generator answered without a normalized name. Only a
// kebab-case name reaches the tool-generator, so an alias is the one case where
// the two differ.
func TestValidateConnectorKeepsTypedAndCanonicalNames(t *testing.T) {
	const typed, canonical = "postgres", "postgresql"

	t.Run("check failed", func(t *testing.T) {
		calls, url := validateUpstream(t, validateFixedAnswer(http.StatusInternalServerError, `{"detail":"boom"}`))
		_, got := callValidateNamed(t, url, typed)
		if atomic.LoadInt64(calls) != 1 {
			t.Fatalf("tool-generator calls = %d, want 1", atomic.LoadInt64(calls))
		}
		if got["validation_unavailable"] != true {
			t.Fatalf("validation_unavailable = %v, want true", got["validation_unavailable"])
		}
		if got["connector_name"] != typed {
			t.Errorf("connector_name = %v, want %q as typed", got["connector_name"], typed)
		}
		if got["normalized_name"] != canonical {
			t.Errorf("normalized_name = %v, want %q", got["normalized_name"], canonical)
		}
	})

	t.Run("answer without normalized_name", func(t *testing.T) {
		var sent map[string]string
		calls, url := validateUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&sent)
			validateFixedAnswer(http.StatusOK, `{"valid":true,"connector_name":"postgresql","can_generate":true}`)(w, r)
		})
		w, got := callValidateNamed(t, url, typed)
		if atomic.LoadInt64(calls) != 1 {
			t.Fatalf("tool-generator calls = %d, want 1", atomic.LoadInt64(calls))
		}
		if sent["connector_name"] != canonical {
			t.Fatalf("sent connector_name = %q, want %q", sent["connector_name"], canonical)
		}
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d (%s)", w.Code, w.Body.String())
		}
		if got["connector_name"] != typed {
			t.Errorf("connector_name = %v, want %q as typed", got["connector_name"], typed)
		}
		if got["normalized_name"] != canonical {
			t.Errorf("normalized_name = %v, want %q filled in", got["normalized_name"], canonical)
		}
	})
}
