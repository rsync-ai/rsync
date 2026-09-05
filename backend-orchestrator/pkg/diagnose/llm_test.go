package diagnose

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
)

// unknownSignal is an error no rule matches — the only class of signal
// that may reach the LLM.
func unknownSignal() Signal {
	return Signal{
		PipelineID:      "pl-1",
		ExecutionID:     "ex-1",
		ErrorMessage:    "ICMP destination port unreachable on protocol 17",
		Stage:           "executor",
		SourceType:      "shopify",
		DestinationType: "postgresql",
	}
}

// newLLMServer returns an httptest server that counts requests and
// replies with the given status + OpenAI-envelope content.
func newLLMServer(t *testing.T, status int, content string, calls *int32, lastBody *string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(calls, 1)
		if lastBody != nil {
			b, _ := io.ReadAll(r.Body)
			*lastBody = string(b)
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		resp := map[string]interface{}{
			"choices": []map[string]interface{}{
				{"message": map[string]string{"content": content}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
}

func chainWith(url string) *ChainDiagnoser {
	return &ChainDiagnoser{
		Rules: New(),
		LLM: &LLMDiagnoser{
			BaseURL:    url,
			Model:      "test-model",
			HTTPClient: &http.Client{},
		},
	}
}

func llmContent(category, action string, confidence float64, rationale string) string {
	return fmt.Sprintf(`{"category":%q,"action":%q,"confidence":%g,"rationale":%q}`,
		category, action, confidence, rationale)
}

func TestNewFromEnv_DisabledByDefault_NoHTTPRequest(t *testing.T) {
	var calls int32
	srv := newLLMServer(t, http.StatusOK, llmContent("network", "backoff_retry", 0.7, "x"), &calls, nil)
	defer srv.Close()

	t.Setenv("LLM_SERVICE_URL", srv.URL)
	t.Setenv("RSYNC_LLM_DIAGNOSER_ENABLED", "")

	d := NewFromEnv()
	if _, ok := d.(*RuleBasedDiagnoser); !ok {
		t.Fatalf("flag off: want *RuleBasedDiagnoser, got %T", d)
	}
	got := d.Diagnose(unknownSignal())
	if got.Category != CategoryUnknown || got.SuggestedAction != ActionEscalate {
		t.Errorf("flag off: want rules unknown/escalate, got %s/%s", got.Category, got.SuggestedAction)
	}
	if n := atomic.LoadInt32(&calls); n != 0 {
		t.Errorf("flag off: LLM server received %d request(s), want 0", n)
	}
}

func TestNewFromEnv_Enabled_ReturnsChain(t *testing.T) {
	t.Setenv("RSYNC_LLM_DIAGNOSER_ENABLED", "true")
	t.Setenv("LLM_SERVICE_URL", "http://example.invalid")
	d := NewFromEnv()
	if _, ok := d.(*ChainDiagnoser); !ok {
		t.Fatalf("flag on: want *ChainDiagnoser, got %T", d)
	}
}

func TestChain_RulesClassifiedCDCError_NeverCallsLLM(t *testing.T) {
	var calls int32
	srv := newLLMServer(t, http.StatusOK, llmContent("network", "backoff_retry", 0.8, "retry it"), &calls, nil)
	defer srv.Close()

	c := chainWith(srv.URL)
	cdcMsgs := []string{
		"publication does not exist: public_users_pub",
		"CDC requires PRIMARY KEY for DB destinations; missing PK on: public.events",
		"wal_level is 'replica', must be 'logical'",
	}
	for _, msg := range cdcMsgs {
		got := c.Diagnose(Signal{ErrorMessage: msg})
		if got.SuggestedAction != ActionEscalate {
			t.Errorf("msg=%q: CDC provisioning must escalate, got %s", msg, got.SuggestedAction)
		}
		if strings.HasPrefix(got.Rationale, "llm:") {
			t.Errorf("msg=%q: LLM rationale returned; rules result expected", msg)
		}
		if got.Confidence < 0.9 {
			t.Errorf("msg=%q: want rules confidence ≥0.9, got %f", msg, got.Confidence)
		}
	}
	if n := atomic.LoadInt32(&calls); n != 0 {
		t.Errorf("CDC errors are rules-classified; LLM received %d request(s), want 0", n)
	}
}

func TestChain_RulesClassifiedAuthError_NeverCallsLLM(t *testing.T) {
	var calls int32
	srv := newLLMServer(t, http.StatusOK, llmContent("network", "backoff_retry", 0.8, "x"), &calls, nil)
	defer srv.Close()

	c := chainWith(srv.URL)
	got := c.Diagnose(Signal{ErrorMessage: "request failed: HTTP 401 Unauthorized"})
	if got.Category != CategoryAuthExpired {
		t.Errorf("want rules auth_expired, got %s", got.Category)
	}
	if n := atomic.LoadInt32(&calls); n != 0 {
		t.Errorf("rules-classified error reached the LLM: %d request(s)", n)
	}
}

func TestChain_UnknownError_UsesLLMResult(t *testing.T) {
	var calls int32
	srv := newLLMServer(t, http.StatusOK,
		llmContent("rate_limit", "backoff_retry", 0.7, "looks like upstream throttling"), &calls, nil)
	defer srv.Close()

	got := chainWith(srv.URL).Diagnose(unknownSignal())
	if got.Category != CategoryRateLimit {
		t.Errorf("category: want rate_limit from LLM, got %s", got.Category)
	}
	if got.SuggestedAction != ActionBackoffRetry {
		t.Errorf("action: want backoff_retry from LLM, got %s", got.SuggestedAction)
	}
	if got.Confidence != 0.7 {
		t.Errorf("confidence: want 0.7 passed through, got %f", got.Confidence)
	}
	if !strings.HasPrefix(got.Rationale, "llm: ") {
		t.Errorf("rationale: want 'llm: ' prefix, got %q", got.Rationale)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Errorf("want exactly 1 LLM request, got %d", n)
	}
}

func TestChain_ConfidenceClampedBelowAutoBand(t *testing.T) {
	var calls int32
	srv := newLLMServer(t, http.StatusOK,
		llmContent("network", "backoff_retry", 0.99, "certain"), &calls, nil)
	defer srv.Close()

	got := chainWith(srv.URL).Diagnose(unknownSignal())
	if got.Confidence != 0.84 {
		t.Errorf("confidence: want clamp to 0.84 (below AutoBand 0.85), got %f", got.Confidence)
	}
}

func TestChain_NegativeConfidenceClampedToZero(t *testing.T) {
	var calls int32
	srv := newLLMServer(t, http.StatusOK,
		llmContent("network", "backoff_retry", -0.4, "x"), &calls, nil)
	defer srv.Close()

	got := chainWith(srv.URL).Diagnose(unknownSignal())
	if got.Confidence != 0 {
		t.Errorf("confidence: want clamp to 0, got %f", got.Confidence)
	}
}

func TestChain_LLMFailures_FallBackToRules(t *testing.T) {
	rulesFallback := New().Diagnose(unknownSignal())

	cases := []struct {
		name    string
		status  int
		content string
	}{
		{"http 500", http.StatusInternalServerError, ""},
		{"garbage json content", http.StatusOK, "not json at all"},
		{"invalid category", http.StatusOK, llmContent("cosmic_rays", "backoff_retry", 0.7, "x")},
		{"invalid action", http.StatusOK, llmContent("network", "reboot_universe", 0.7, "x")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls int32
			srv := newLLMServer(t, tc.status, tc.content, &calls, nil)
			defer srv.Close()

			got := chainWith(srv.URL).Diagnose(unknownSignal())
			if got != rulesFallback {
				t.Errorf("want rules fallback %+v, got %+v", rulesFallback, got)
			}
			if n := atomic.LoadInt32(&calls); n != 1 {
				t.Errorf("want 1 attempted LLM request, got %d", n)
			}
		})
	}
}

func TestChain_LLMUnreachable_FallsBackToRules(t *testing.T) {
	rulesFallback := New().Diagnose(unknownSignal())
	// Closed server → connection refused.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()

	got := chainWith(url).Diagnose(unknownSignal())
	if got != rulesFallback {
		t.Errorf("want rules fallback %+v, got %+v", rulesFallback, got)
	}
}

func TestChain_PromptIsScrubbed(t *testing.T) {
	var calls int32
	var lastBody string
	srv := newLLMServer(t, http.StatusOK,
		llmContent("user_config", "request_user_config", 0.6, "x"), &calls, &lastBody)
	defer srv.Close()

	sig := unknownSignal()
	sig.ErrorMessage = "weird failure for user bob@example.com with password=hunter2 value 'PII-ROW-DATA'"
	_ = chainWith(srv.URL).Diagnose(sig)

	if lastBody == "" {
		t.Fatal("LLM server saw no request body")
	}
	for _, leak := range []string{"bob@example.com", "hunter2", "PII-ROW-DATA"} {
		if strings.Contains(lastBody, leak) {
			t.Errorf("prompt leaked %q — llmscrub not applied", leak)
		}
	}
	if !strings.Contains(lastBody, "[email-redacted]") {
		t.Errorf("expected scrub marker [email-redacted] in prompt, body=%s", lastBody)
	}
}

func TestLLMDiagnoser_ErrorMessageTruncated(t *testing.T) {
	sig := unknownSignal()
	sig.ErrorMessage = strings.Repeat("z", 5000)
	prompt := buildLLMDiagnosePrompt(sig)
	if len(prompt) > 2200 { // 1500-rune cap + template overhead
		t.Errorf("prompt too long (%d chars) — error message not truncated", len(prompt))
	}
}

// resetDiagnoserModeLog clears the process-global sync.Once that gates
// NewFromEnv's mode log. Without this, whichever test in this package calls
// NewFromEnv first consumes the Once and every later assertion on that log
// observes zero entries and passes vacuously.
func resetDiagnoserModeLog(t *testing.T) *test.Hook {
	t.Helper()
	diagnoserModeLogOnce = sync.Once{}
	hook := test.NewGlobal()
	t.Cleanup(func() {
		hook.Reset()
		logrus.StandardLogger().ReplaceHooks(make(logrus.LevelHooks))
	})
	return hook
}

func TestNewFromEnv_LogsConstructedMode(t *testing.T) {
	// Two-sided on purpose: a test that only asserted the off-case would pass
	// just as well if the log were deleted from the enabled branch.
	cases := []struct {
		name     string
		flag     string
		wantMode string
		wantOn   bool
	}{
		{"flag unset", "", "rules-only", false},
		{"flag true", "true", "rules+llm", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RSYNC_LLM_DIAGNOSER_ENABLED", tc.flag)
			t.Setenv("LLM_SERVICE_URL", "http://example.invalid")
			hook := resetDiagnoserModeLog(t)

			NewFromEnv()

			entries := hook.AllEntries()
			if len(entries) != 1 {
				t.Fatalf("want exactly 1 log entry, got %d: %+v", len(entries), entries)
			}
			e := entries[0]
			if e.Level != logrus.InfoLevel {
				t.Errorf("want Info level, got %s", e.Level)
			}
			if got := e.Data["diagnoser"]; got != tc.wantMode {
				t.Errorf("diagnoser field: want %q, got %v", tc.wantMode, got)
			}
			if got := e.Data["enabled"]; got != tc.wantOn {
				t.Errorf("enabled field: want %v, got %v", tc.wantOn, got)
			}
			if got := e.Data["flag"]; got != "RSYNC_LLM_DIAGNOSER_ENABLED" {
				t.Errorf("flag field: want the env var name, got %v", got)
			}
		})
	}
}

func TestNewFromEnv_LogsOnlyOnce(t *testing.T) {
	// Both production call sites construct a Diagnoser per sweep, so a log
	// that is not Once-guarded would fire on every sweep tick forever.
	t.Setenv("RSYNC_LLM_DIAGNOSER_ENABLED", "")
	hook := resetDiagnoserModeLog(t)

	NewFromEnv()
	NewFromEnv()
	NewFromEnv()

	if n := len(hook.AllEntries()); n != 1 {
		t.Errorf("3 constructions produced %d log entries, want 1", n)
	}
}

func TestNewFromEnv_DiagnoserURLOverridesSharedName(t *testing.T) {
	// LLM_SERVICE_URL is shared with internal/workers/intent.go, which reads it
	// with a different fallback. The diagnoser-specific name must win so the
	// two consumers can be steered independently.
	t.Setenv("RSYNC_LLM_DIAGNOSER_ENABLED", "true")
	t.Setenv("LLM_SERVICE_URL", "http://planner.invalid:5011")
	t.Setenv("RSYNC_LLM_DIAGNOSER_URL", "http://diagnoser.invalid:5000")
	resetDiagnoserModeLog(t)

	d, ok := NewFromEnv().(*ChainDiagnoser)
	if !ok {
		t.Fatalf("flag on: want *ChainDiagnoser")
	}
	if d.LLM.BaseURL != "http://diagnoser.invalid:5000" {
		t.Errorf("want RSYNC_LLM_DIAGNOSER_URL to win, got %q", d.LLM.BaseURL)
	}

	// Unset, the shared name still applies — behaviour is unchanged for anyone
	// who was relying on LLM_SERVICE_URL.
	t.Setenv("RSYNC_LLM_DIAGNOSER_URL", "")
	d2, ok := NewFromEnv().(*ChainDiagnoser)
	if !ok {
		t.Fatalf("flag on: want *ChainDiagnoser")
	}
	if d2.LLM.BaseURL != "http://planner.invalid:5011" {
		t.Errorf("override unset: want LLM_SERVICE_URL, got %q", d2.LLM.BaseURL)
	}
}

func TestPromptMetadataFieldsAreScrubbed(t *testing.T) {
	// The prompt's privacy claim covers every free-text variable, not just the
	// error message. These four are closed-set today; this pins that the claim
	// survives a future caller that puts something else in them.
	sig := unknownSignal()
	sig.ErrorMessage = "benign"
	sig.Stage = "executor bob@example.com"
	sig.ExecutorStatus = "failed password=hunter2"
	sig.SourceType = "shopify bob@example.com"
	sig.DestinationType = "postgresql password=hunter2"

	prompt := buildLLMDiagnosePrompt(sig)

	for _, leak := range []string{"bob@example.com", "hunter2"} {
		if strings.Contains(prompt, leak) {
			t.Errorf("prompt leaked %q from a metadata field — llmscrub not applied", leak)
		}
	}
	if !strings.Contains(prompt, "[email-redacted]") {
		t.Errorf("expected scrub marker [email-redacted] in prompt, got:\n%s", prompt)
	}
}
