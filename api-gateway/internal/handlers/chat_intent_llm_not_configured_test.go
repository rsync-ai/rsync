package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"api-gateway/internal/chat"
	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// Without an LLM, a chat message the regex fast paths cannot read reaches
// parseIntent, and llm-service answers 503 llm_not_configured. The reply must
// say an LLM is needed (in the service's own words) and offer one phrasing that
// works without an LLM. These tests also prove that phrasing really is read
// without a model, so the hint cannot go stale.

const chatGatedSentence = "Set up an LLM first: add a provider key to .env, then restart rsync."

// A message that misses every regex fast path under the no-catalog fallback
// (TestNonCanonicalPhrasingsMissEveryRegexFastPath pins the same phrasing).
const chatNeedsLLMMessage = "can you move everything from our orders database over to the lake"

// completionStub serves /v1/completion with a fixed status and body and counts
// the calls.
func completionStub(t *testing.T, status int, body string) *int64 {
	t.Helper()
	var calls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&calls, 1)
		if r.URL.Path != "/v1/completion" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("LLM_SERVICE_URL", srv.URL)
	return &calls
}

func sendNewChatMessage(t *testing.T, msg string) ChatMessageResponse {
	t.Helper()
	return (&ChatHandler{}).handleNewIntent(context.Background(), newIntentTestContext(t),
		chat.NewConversationContext("u1", "s1"), msg, "trace", "s1", "u1")
}

func TestChatIntentWithoutLLMSaysSoAndOffersAWorkingPhrase(t *testing.T) {
	pinNoCatalogDB(t)

	bodies := map[string]string{
		"flat":   `{"error":"llm_not_configured","message":"` + chatGatedSentence + `"}`,
		"nested": `{"detail":{"error":"llm_not_configured","message":"` + chatGatedSentence + `"}}`,
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			calls := completionStub(t, http.StatusServiceUnavailable, body)

			resp := sendNewChatMessage(t, chatNeedsLLMMessage)

			if atomic.LoadInt64(calls) == 0 {
				t.Fatal("llm-service was never called: a fast path answered, so this test proves nothing")
			}
			if strings.HasPrefix(resp.Message, cannedIntentFallbackPrefix) {
				t.Fatalf("got the generic examples instead of the no-LLM answer: %q", resp.Message)
			}
			if !strings.Contains(resp.Message, chatGatedSentence) {
				t.Errorf("reply does not carry the service's sentence: %q", resp.Message)
			}
			if !strings.Contains(resp.Message, "need an LLM") {
				t.Errorf("reply does not say the request needs an LLM: %q", resp.Message)
			}
			if strings.Contains(resp.Message, llmNotConfiguredCode) {
				t.Errorf("reply shows the internal code: %q", resp.Message)
			}
			if len(resp.Suggestions) != 1 {
				t.Fatalf("suggestions = %v, want exactly the no-LLM example", resp.Suggestions)
			}
			example := resp.Suggestions[0]
			if example == "" || !strings.Contains(resp.Message, `"`+example+`"`) {
				t.Errorf("reply text does not show the suggested example %q: %q", example, resp.Message)
			}
			// The chat UI hides suggestion chips on a "confirmation" reply
			// (ChatMessageItem.tsx), which would remove the one clickable phrase
			// that works without an LLM.
			if resp.Type != "text" {
				t.Errorf("type = %q, want \"text\" so the example shows as a chip", resp.Type)
			}
			if resp.Data["needs_llm"] != true {
				t.Errorf("data.needs_llm = %v, want true", resp.Data["needs_llm"])
			}
			if resp.Data["example"] != example {
				t.Errorf("data.example = %v, want the suggested example %q", resp.Data["example"], example)
			}
		})
	}
}

// A gated error whose message is missing or blank still tells the user how to
// set up an LLM, using the standard sentence.
func TestChatNoLLMReplyWithoutAServiceSentenceUsesTheStandardOne(t *testing.T) {
	for name, body := range map[string]gin.H{
		"message missing": {"error": llmNotConfiguredCode},
		"message blank":   {"error": llmNotConfiguredCode, "message": "   "},
	} {
		t.Run(name, func(t *testing.T) {
			resp := llmNotConfiguredChatReply(&llmNotConfiguredError{body: body}, "trace")
			if !strings.Contains(resp.Message, llmNotConfiguredFallbackMessage) {
				t.Fatalf("reply does not say how to set up an LLM: %q", resp.Message)
			}
			if !strings.Contains(resp.Message, `"`+chatNoLLMExample+`"`) {
				t.Fatalf("reply lost the no-LLM example: %q", resp.Message)
			}
		})
	}
}

// The example the reply offers must be read without calling the LLM. The
// phrase is taken from the reply itself, not from the constant, and sent back
// through the real entry point with an LLM that is not set up.
func TestChatNoLLMExampleIsReadWithoutTheLLM(t *testing.T) {
	pinNoCatalogDB(t)
	gated := `{"error":"llm_not_configured","message":"` + chatGatedSentence + `"}`

	calls := completionStub(t, http.StatusServiceUnavailable, gated)
	first := sendNewChatMessage(t, chatNeedsLLMMessage)
	if atomic.LoadInt64(calls) == 0 || len(first.Suggestions) == 0 {
		t.Fatalf("setup: expected the no-LLM reply with an example (calls=%d, suggestions=%v)",
			atomic.LoadInt64(calls), first.Suggestions)
	}
	example := first.Suggestions[0]

	before := atomic.LoadInt64(calls)
	resp := sendNewChatMessage(t, example)
	if got := atomic.LoadInt64(calls) - before; got != 0 {
		t.Fatalf("the example %q called the LLM %d time(s); it must work without one", example, got)
	}
	if resp.Type != "confirmation" {
		t.Fatalf("the example %q did not reach the pipeline confirmation: type=%q message=%q",
			example, resp.Type, resp.Message)
	}
	if resp.Data["source_type"] != "mongodb" || resp.Data["destination_type"] != "google-cloud-storage" {
		t.Fatalf("the example %q parsed as %v -> %v", example, resp.Data["source_type"], resp.Data["destination_type"])
	}
}

// With a real catalog the fast path asks connector_catalog whether each side is
// active. Both sides of the example are seeded active (migrations 050 and 040).
// Every expected query must run, so the fail-open branch (any catalog error
// accepts the name) cannot make this pass.
func TestChatNoLLMExampleParsesAgainstTheCatalog(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	prev := db.DB
	db.DB = sqlDB
	t.Cleanup(func() {
		db.DB = prev
		sqlDB.Close()
	})

	for _, name := range []string{"mongodb", "google-cloud-storage"} {
		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM connector_catalog\s+WHERE name = \$1 AND status = 'active'`).
			WithArgs(name).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	}

	intent := (&ChatHandler{}).quickParseDataSyncIntent(chatNoLLMExample)
	if intent == nil {
		t.Fatalf("%q is not read by the fast path with the catalog", chatNoLLMExample)
	}
	if intent.SourceType != "mongodb" || intent.DestinationType != "google-cloud-storage" {
		t.Fatalf("%q parsed as %q -> %q", chatNoLLMExample, intent.SourceType, intent.DestinationType)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("catalog was not consulted for both sides: %v", err)
	}
}

// Every other failure keeps today's generic examples.
func TestChatIntentOtherFailuresKeepTheGenericReply(t *testing.T) {
	pinNoCatalogDB(t)
	for name, tc := range map[string]struct {
		status int
		body   string
	}{
		"busy 503":           {http.StatusServiceUnavailable, `{"detail":"busy"}`},
		"other error code":   {http.StatusServiceUnavailable, `{"error":"rate_limited","message":"slow down"}`},
		"500 with gate body": {http.StatusInternalServerError, `{"error":"llm_not_configured","message":"x"}`},
	} {
		t.Run(name, func(t *testing.T) {
			calls := completionStub(t, tc.status, tc.body)
			resp := sendNewChatMessage(t, chatNeedsLLMMessage)
			if atomic.LoadInt64(calls) == 0 {
				t.Fatal("llm-service was never called")
			}
			if !strings.HasPrefix(resp.Message, cannedIntentFallbackPrefix) {
				t.Fatalf("want the generic examples, got %q", resp.Message)
			}
		})
	}
}
