package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A question the classifier reads as general knowledge goes on to a second
// model call, the help answer. When that call failed, the chat answered "To
// create a pipeline, tell me your source and destination" with four example
// pipelines: the same ignored-question symptom as a failed classification. It
// must say the model did not answer instead.

// The generic examples the help path used to fall back to.
const cannedHelpFallbackPrefix = "To create a pipeline, tell me **your source** and **destination**."

// helpStub serves /v1/completion: the classification prompt gets
// intentContent after intentDelay, the help prompt gets help(w, r). It records
// the prompts asked for, so a test can prove both calls were made.
func helpStub(t *testing.T, intentDelay time.Duration, intentContent string,
	help func(w http.ResponseWriter, r *http.Request)) *promptLog {
	t.Helper()
	called := &promptLog{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			PromptName string `json:"prompt_name"`
		}
		_ = json.Unmarshal(body, &req)
		called.add(req.PromptName)
		if req.PromptName == "chat/help_response" {
			help(w, r)
			return
		}
		waitOrGiveUp(r, intentDelay)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"content": intentContent})
	}))
	t.Cleanup(srv.Close)
	t.Setenv("LLM_SERVICE_URL", srv.URL)
	return called
}

// waitOrGiveUp sleeps d, returning early when the gateway drops the request so
// srv.Close does not wait out the full delay.
func waitOrGiveUp(r *http.Request, d time.Duration) {
	select {
	case <-time.After(d):
	case <-r.Context().Done():
	}
}

func helpReplies(status int, body string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

func helpContent(content string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"content": content})
	}
}

// The two places handleNewIntent asks for a help answer: a general-knowledge
// question, and a how-to question about an action other than data_sync.
var helpRoutes = map[string]struct {
	message string
	intent  string
}{
	"general knowledge": {
		message: "In my pipeline, these stages are taking unusually long: Executing Pipeline. Why might that be, and what should I check?",
		intent:  fencedIntent,
	},
	"how-to, not a data sync": {
		message: "how do I dedupe rows while copying our orders data over to the lake",
		intent: `{"intent":"transform","requires_execution":true,` +
			`"parameters":{"source":"mysql","destination":"bigquery"}}`,
	},
}

func TestFailedHelpAnswerSaysTheModelDidNotAnswer(t *testing.T) {
	pinNoCatalogDB(t)
	failures := map[string]struct {
		help   func(http.ResponseWriter, *http.Request)
		reason string
		leak   string
	}{
		"500":           {helpReplies(http.StatusInternalServerError, `{"detail":"upstream boom"}`), "error", "upstream boom"},
		"prose":         {helpContent("Stages are slow when the source is busy."), "error", "source is busy"},
		"blank message": {helpContent(`{"message":"  ","suggestions":["mysql to bigquery"]}`), "error", "mysql to bigquery"},
	}
	for route, rt := range helpRoutes {
		for name, f := range failures {
			t.Run(route+"/"+name, func(t *testing.T) {
				called := helpStub(t, 0, rt.intent, f.help)
				resp := sendNewChatMessage(t, rt.message)

				if got := called.snapshot(); len(got) != 2 || got[1] != "chat/help_response" {
					t.Fatalf("prompts called = %v, want classification then the help answer", got)
				}
				if strings.HasPrefix(resp.Message, cannedHelpFallbackPrefix) {
					t.Fatalf("got the generic pipeline examples: %q", resp.Message)
				}
				if !isLLMUnavailableReply(resp) || llmUnavailableReason(resp) != f.reason {
					t.Fatalf("want the model-did-not-answer reply (reason %q), got %q %v",
						f.reason, resp.Message, resp.Data)
				}
				if !strings.Contains(resp.Message, "couldn't answer your question") {
					t.Errorf("reply does not say the question went unanswered: %q", resp.Message)
				}
				if !strings.Contains(resp.Message, "`trace`") {
					t.Errorf("reply does not carry the trace id: %q", resp.Message)
				}
				if strings.Contains(resp.Message, f.leak) {
					t.Errorf("reply leaks llm-service's own text %q: %q", f.leak, resp.Message)
				}
			})
		}
	}
}

// A help call that says no model is set up gets the no-LLM reply, the same as a
// classification call that says so.
func TestHelpAnswerWithoutAnLLMSaysSo(t *testing.T) {
	pinNoCatalogDB(t)
	gated := `{"error":"llm_not_configured","message":"` + chatGatedSentence + `"}`
	called := helpStub(t, 0, fencedIntent, helpReplies(http.StatusServiceUnavailable, gated))

	resp := sendNewChatMessage(t, helpRoutes["general knowledge"].message)

	if len(called.snapshot()) != 2 {
		t.Fatalf("prompts called = %v, want classification then the help answer", called.snapshot())
	}
	if resp.Data["needs_llm"] != true || !strings.Contains(resp.Message, chatGatedSentence) {
		t.Fatalf("want the no-LLM reply with the service's sentence, got %q %v", resp.Message, resp.Data)
	}
}

// Classification and the help answer share one deadline. Each call here takes
// 0.7s, under the 1s deadline on its own, 1.4s together. With a deadline per
// call the help answer arrives; with the shared one it times out. A turn with a
// deadline per call could run for twice llmServiceTimeout(), past Cloudflare's
// 100s proxy timeout at the 60s default.
func TestClassificationAndHelpAnswerShareOneDeadline(t *testing.T) {
	pinNoCatalogDB(t)
	t.Setenv("LLM_SERVICE_TIMEOUT_SECONDS", "1")
	const perCall = 700 * time.Millisecond
	called := helpStub(t, perCall, fencedIntent, func(w http.ResponseWriter, r *http.Request) {
		waitOrGiveUp(r, perCall)
		helpContent(fencedHelp)(w, r)
	})

	start := time.Now()
	resp := sendNewChatMessage(t, helpRoutes["general knowledge"].message)
	took := time.Since(start)

	if got := called.snapshot(); len(got) != 2 || got[1] != "chat/help_response" {
		t.Fatalf("prompts called = %v, want classification then the help answer", got)
	}
	if resp.Message == realHelpAnswer {
		t.Fatalf("the help answer arrived after %v against a 1s deadline: each call got its own deadline", took)
	}
	if !isLLMUnavailableReply(resp) || llmUnavailableReason(resp) != "timeout" {
		t.Fatalf("want the timeout reply, got %q %v", resp.Message, resp.Data)
	}
	if took > 1300*time.Millisecond {
		t.Errorf("the turn took %v, past its 1s deadline", took)
	}
}

// Control for the test above: the same two 0.7s calls fit a 2s deadline, so the
// timeout there comes from sharing it, not from the stub.
func TestClassificationAndHelpAnswerFitASharedDeadlineLongEnoughForBoth(t *testing.T) {
	pinNoCatalogDB(t)
	t.Setenv("LLM_SERVICE_TIMEOUT_SECONDS", "2")
	const perCall = 700 * time.Millisecond
	helpStub(t, perCall, fencedIntent, func(w http.ResponseWriter, r *http.Request) {
		waitOrGiveUp(r, perCall)
		helpContent(fencedHelp)(w, r)
	})

	resp := sendNewChatMessage(t, helpRoutes["general knowledge"].message)

	if resp.Message != realHelpAnswer {
		t.Fatalf("reply = %q, want the model's answer", resp.Message)
	}
}
