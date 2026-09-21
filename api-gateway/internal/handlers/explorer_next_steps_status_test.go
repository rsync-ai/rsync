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
	log "github.com/sirupsen/logrus"
)

// GetExplorerNextSteps used to decode whatever llm-service sent, whatever the
// status, and answer 200. An error body has no "suggestions", so a failure
// looked like "nothing to suggest". These tests pin the status handling.

const nextStepsGatedSentence = "Set up an LLM first: add a provider key to .env, then restart rsync."

// The shape llm-service's rules fallback (_mock_next_steps) returns with 200
// when no LLM is set up and row_count > 10.
const nextStepsRulesBody = `{"suggestions":[` +
	`{"action_type":"metabase","title":"Create Dashboard","description":"Visualize these results","confidence":0.9,"required_inputs":["dashboard_name"],"cta":"Create Dashboard"},` +
	`{"action_type":"download_csv","title":"Download CSV","description":"Export 42 rows to CSV","confidence":0.8,"required_inputs":[],"cta":"Download"},` +
	`{"action_type":"slack","title":"Share to Slack","description":"Send results to a channel","confidence":0.6,"required_inputs":["channel"],"cta":"Share"}]}`

// callNextSteps starts a fake llm-service that answers with a fixed status and
// body, calls the handler, and returns the answer and the upstream call count.
func callNextSteps(t *testing.T, upstreamStatus int, upstreamBody string) (*httptest.ResponseRecorder, int64) {
	t.Helper()
	var calls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&calls, 1)
		if r.URL.Path != "/api/v1/explorer/nl/next-steps" {
			t.Errorf("unexpected upstream path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(upstreamStatus)
		_, _ = w.Write([]byte(upstreamBody))
	}))
	t.Cleanup(srv.Close)
	return callNextStepsAt(t, srv.URL), atomic.LoadInt64(&calls)
}

// callNextStepsAt calls the handler with llm-service at llmServiceURL.
func callNextStepsAt(t *testing.T, llmServiceURL string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	t.Setenv("LLM_SERVICE_URL", llmServiceURL)

	reqBody, _ := json.Marshal(map[string]interface{}{
		"question":       "orders per day",
		"sql":            "SELECT day, count(*) FROM orders GROUP BY day",
		"result_profile": map[string]interface{}{"row_count": 42, "column_count": 2},
	})
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("user_id", "u1")
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/explorer/nl/next-steps", bytes.NewReader(reqBody))
	c.Request.Header.Set("Content-Type", "application/json")

	GetExplorerNextSteps(c)
	c.Writer.WriteHeaderNow()
	return w
}

func decodeNextSteps(t *testing.T, w *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()
	var out map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("response is not JSON: %v (%s)", err, w.Body.String())
	}
	return out
}

// nextStepsActionTypes decodes a success answer and returns its action types in order.
func nextStepsActionTypes(t *testing.T, w *httptest.ResponseRecorder) []string {
	t.Helper()
	var got GetNextStepsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v (%s)", err, w.Body.String())
	}
	out := make([]string, 0, len(got.Suggestions))
	for _, s := range got.Suggestions {
		out = append(out, s.ActionType)
	}
	return out
}

// captureGatewayLogs sends the standard logger's output to a buffer for the
// rest of the test.
func captureGatewayLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	logger := log.StandardLogger()
	prevOut, prevLevel := logger.Out, logger.Level
	var buf bytes.Buffer
	logger.SetOutput(&buf)
	logger.SetLevel(log.InfoLevel)
	t.Cleanup(func() {
		logger.SetOutput(prevOut)
		logger.SetLevel(prevLevel)
	})
	return &buf
}

func TestNextStepsRelaysLLMNotConfigured(t *testing.T) {
	for name, body := range map[string]string{
		"flat":   `{"error":"llm_not_configured","message":"` + nextStepsGatedSentence + `"}`,
		"nested": `{"detail":{"error":"llm_not_configured","message":"` + nextStepsGatedSentence + `"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			w, calls := callNextSteps(t, http.StatusServiceUnavailable, body)
			if calls == 0 {
				t.Fatal("llm-service was never called")
			}
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503 (%s)", w.Code, w.Body.String())
			}
			got := decodeNextSteps(t, w)
			if got["error"] != llmNotConfiguredCode || got["message"] != nextStepsGatedSentence {
				t.Fatalf("body = %v, want the standard not-configured body", got)
			}
			if _, has := got["suggestions"]; has {
				t.Fatalf("a not-configured answer must not carry suggestions: %v", got)
			}
		})
	}
}

func TestNextStepsNon2xxIsAnErrorNotAnEmptySuccess(t *testing.T) {
	// A validation error from llm-service can echo the request, and the request
	// carries the result profile. Neither the answer nor the logs may repeat it.
	const echoed = "row-value-that-must-not-leak"
	for name, tc := range map[string]struct {
		status int
		body   string
		// retryHelps: a 5xx may pass; a 4xx gives the same answer every time.
		retryHelps bool
	}{
		"busy 503":          {http.StatusServiceUnavailable, `{"detail":"busy"}`, true},
		"500":               {http.StatusInternalServerError, `{"detail":"boom","input":"` + echoed + `"}`, true},
		"502 not json":      {http.StatusBadGateway, `upstream connect error ` + echoed, true},
		"400":               {http.StatusBadRequest, `{"detail":"bad","input":"` + echoed + `"}`, false},
		"422 echoing input": {http.StatusUnprocessableEntity, `{"detail":[{"loc":["body","result_profile"],"input":"` + echoed + `"}]}`, false},
		"404":               {http.StatusNotFound, `{"detail":"Not Found"}`, false},
		"499":               {499, `{"detail":"closed"}`, false},
	} {
		t.Run(name, func(t *testing.T) {
			logs := captureGatewayLogs(t)
			w, calls := callNextSteps(t, tc.status, tc.body)
			if calls == 0 {
				t.Fatal("llm-service was never called")
			}
			if w.Code != http.StatusBadGateway {
				t.Fatalf("status = %d for an upstream %d, want 502: a failure must not look like success "+
					"and 503 would read as not-configured (%s)", w.Code, tc.status, w.Body.String())
			}
			got := decodeNextSteps(t, w)
			if _, has := got["suggestions"]; has {
				t.Fatalf("error answer carries suggestions: %v", got)
			}
			msg, _ := got["error"].(string)
			if strings.TrimSpace(msg) == "" || msg == llmNotConfiguredCode {
				t.Fatalf("want a plain error sentence, got %v", got)
			}
			if !strings.Contains(msg, "status "+strconv.Itoa(tc.status)) {
				t.Errorf("error does not name the upstream status %d: %q", tc.status, msg)
			}
			if got["upstream_status"] != float64(tc.status) {
				t.Errorf("upstream_status = %v, want %d", got["upstream_status"], tc.status)
			}
			if tc.retryHelps {
				if !strings.Contains(msg, "Try again in a moment") {
					t.Errorf("a %d may pass on retry, so the error should say to try again: %q", tc.status, msg)
				}
			} else {
				if strings.Contains(msg, "Try again in a moment") || !strings.Contains(msg, "Trying again will not help") {
					t.Errorf("a %d gives the same answer on retry, so the error must not suggest retrying: %q", tc.status, msg)
				}
			}
			if strings.Contains(w.Body.String(), echoed) {
				t.Fatalf("upstream body leaked into the answer: %s", w.Body.String())
			}
			// Control: the failure was logged, so an empty buffer cannot pass the leak check.
			if !strings.Contains(logs.String(), "returned status "+strconv.Itoa(tc.status)) {
				t.Fatalf("the failure was not logged; the leak check below would prove nothing: %q", logs.String())
			}
			if strings.Contains(logs.String(), echoed) {
				t.Fatalf("upstream body leaked into the logs: %q", logs.String())
			}
		})
	}
}

// Any 2xx is an answer: the llm-service rules fallback answers 200 without an
// LLM, and it must reach the caller unchanged.
func TestNextStepsForwardsTheRulesFallback(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusCreated} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			w, calls := callNextSteps(t, status, nextStepsRulesBody)
			if calls == 0 {
				t.Fatal("llm-service was never called")
			}
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 (%s)", w.Code, w.Body.String())
			}
			got := nextStepsActionTypes(t, w)
			want := []string{"metabase", "download_csv", "slack"}
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("suggestions = %v, want %v", got, want)
			}
		})
	}
}

// Two failures keep their old answer on purpose: a 200 with default
// suggestions, so the explorer still offers something to do next.
func TestNextStepsKeepsDefaultSuggestionsWhenTheAnswerIsUnusable(t *testing.T) {
	t.Run("llm-service unreachable", func(t *testing.T) {
		dead := httptest.NewServer(http.NotFoundHandler())
		deadURL := dead.URL
		dead.Close()

		w := callNextStepsAt(t, deadURL)
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 with default suggestions (%s)", w.Code, w.Body.String())
		}
		if got := nextStepsActionTypes(t, w); strings.Join(got, ",") != "metabase,download_csv" {
			t.Fatalf("suggestions = %v, want the two defaults", got)
		}
	})
	t.Run("200 not json", func(t *testing.T) {
		w, calls := callNextSteps(t, http.StatusOK, `<html>proxy page</html>`)
		if calls == 0 {
			t.Fatal("llm-service was never called")
		}
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 with the default suggestion (%s)", w.Code, w.Body.String())
		}
		if got := nextStepsActionTypes(t, w); strings.Join(got, ",") != "metabase" {
			t.Fatalf("suggestions = %v, want the one default", got)
		}
	})
}
