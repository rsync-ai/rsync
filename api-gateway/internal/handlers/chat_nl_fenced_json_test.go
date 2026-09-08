package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"api-gateway/internal/chat"
)

// These guard the three chat calls into llm-service against a model that fences
// its JSON.
//
// llm-service hands back {"content": "<the model's raw text>"}, and all three
// call sites used to feed that raw text straight to json.Unmarshal. Every
// OpenAI-compatible backend this repo can be pointed at fences by habit —
// confirmed live against Vertex AI's Gemini on 2026-09-08, whose reply began
// "```json\n{" — so json.Unmarshal failed with
// `invalid character '`' looking for beginning of value` and the handler fell
// back to a canned reply. Nothing in the response said the LLM path was dead.
//
// The regex fast path answered canonical phrasings without ever calling
// llm-service, which is why every smoke test stayed green through this. These
// tests hit the LLM path directly so they cannot be fooled the same way.
// Broker-free and DB-free: parseIntent, callHelpResponseLLM and
// callSlotFillingLLM read no ChatHandler fields.

// llmStub serves one llm-service /v1/completion reply with the given raw model
// text in the content envelope.
func llmStub(t *testing.T, content string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/completion" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"content": content})
	}))
	t.Cleanup(srv.Close)
	t.Setenv("LLM_SERVICE_URL", srv.URL)
	return srv
}

func TestParseIntentAcceptsFencedJSON(t *testing.T) {
	const bare = `{"intent":"create_pipeline","requires_execution":true,` +
		`"parameters":{"source":"postgresql","destination":"bigquery",` +
		`"tables":["orders"],"sync_mode":"cdc"}}`

	cases := []struct {
		name    string
		content string
	}{
		{"bare object (was already working)", bare},
		{"json-tagged fence (the Vertex/Gemini repro)", "```json\n" + bare + "\n```"},
		{"untagged fence", "```\n" + bare + "\n```"},
		{"prose either side of the fence", "Sure thing:\n```json\n" + bare + "\n```\nLet me know."},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			llmStub(t, c.content)
			h := &ChatHandler{}

			got, err := h.parseIntent(context.Background(), "move my orders to bigquery")
			if err != nil {
				t.Fatalf("parseIntent returned error: %v", err)
			}
			if got.IntentName != "create_pipeline" {
				t.Errorf("IntentName = %q, want create_pipeline", got.IntentName)
			}
			if got.SourceType != "postgresql" || got.DestinationType != "bigquery" {
				t.Errorf("source/destination = %q/%q, want postgresql/bigquery",
					got.SourceType, got.DestinationType)
			}
			if !got.RequiresExecution {
				t.Error("RequiresExecution = false, want true")
			}
			if len(got.Tables) != 1 || got.Tables[0] != "orders" {
				t.Errorf("Tables = %v, want [orders]", got.Tables)
			}
			if got.SyncMode != "cdc" {
				t.Errorf("SyncMode = %q, want cdc", got.SyncMode)
			}
		})
	}
}

func TestCallHelpResponseLLMAcceptsFencedJSON(t *testing.T) {
	bare := `{"message":"rsync moves data between systems.","suggestions":["Sync MySQL to BigQuery"]}`
	llmStub(t, "```json\n"+bare+"\n```")
	h := &ChatHandler{}

	msg, sugs, err := h.callHelpResponseLLM(context.Background(), "what does rsync do?")
	if err != nil {
		t.Fatalf("callHelpResponseLLM returned error: %v", err)
	}
	if msg != "rsync moves data between systems." {
		t.Errorf("message = %q", msg)
	}
	if len(sugs) != 1 || sugs[0] != "Sync MySQL to BigQuery" {
		t.Errorf("suggestions = %v", sugs)
	}
}

func TestCallSlotFillingLLMAcceptsFencedJSON(t *testing.T) {
	bare := `{"answering_previous":true,"extracted_slot":"destination",` +
		`"extracted_value":"bigquery","confidence":0.9}`
	llmStub(t, "```json\n"+bare+"\n```")
	h := &ChatHandler{}
	conv := chat.NewConversationContext("u1", "s1")

	got, err := h.callSlotFillingLLM(context.Background(), conv, "bigquery please")
	if err != nil {
		t.Fatalf("callSlotFillingLLM returned error: %v", err)
	}
	if !got.IsAnsweringPrevious {
		t.Error("IsAnsweringPrevious = false, want true")
	}
	if got.ExtractedSlot != "destination" || got.ExtractedValue != "bigquery" {
		t.Errorf("slot/value = %q/%q, want destination/bigquery", got.ExtractedSlot, got.ExtractedValue)
	}
}

// A reply with no JSON object in it must still surface a parse error rather than
// being silently turned into a zero-valued Intent — the extractor returns the
// trimmed input so the caller's json.Unmarshal still fails.
func TestParseIntentStillErrorsOnNonJSON(t *testing.T) {
	llmStub(t, "I'm sorry, I can't help with that.")
	h := &ChatHandler{}

	if _, err := h.parseIntent(context.Background(), "hello"); err == nil {
		t.Fatal("parseIntent accepted a reply containing no JSON object")
	}
}

// llmServiceTimeout replaces four hard-coded 10s literals. The default must stay
// 10s (the cloud behaviour); only docker-compose.quickstart.yml raises it, where
// inference runs on CPU Ollama.
func TestLLMServiceTimeoutDefaultsToTenSecondsAndIsOverridable(t *testing.T) {
	t.Setenv("LLM_SERVICE_TIMEOUT_SECONDS", "")
	if got := llmServiceTimeout(); got != 10*time.Second {
		t.Errorf("default timeout = %v, want 10s", got)
	}

	t.Setenv("LLM_SERVICE_TIMEOUT_SECONDS", "180")
	if got := llmServiceTimeout(); got != 180*time.Second {
		t.Errorf("overridden timeout = %v, want 180s", got)
	}
}
