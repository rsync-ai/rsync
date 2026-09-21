package handlers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"api-gateway/internal/chat"
	"api-gateway/internal/db"

	"github.com/gin-gonic/gin"
)

// Defect F survived every smoke test in this repo's history for one reason: the
// only messages anyone ever sent were canonical "<connector> to <connector>"
// phrasings, and those are answered by quickParseDataSyncIntent without ever
// calling a model. Both halves of the bug -- a deadline below CPU inference time
// and a fenced-JSON reply the parser rejected -- live strictly beyond that gate.
//
// chat_nl_fenced_json_test.go calls parseIntent/callHelpResponseLLM directly, so
// it proves the parser strips fences but says nothing about whether the LLM path
// is reached at all, nor whether the configured deadline reaches the real
// request. Re-hard-coding both timeout literals in parseIntent back to
// 10*time.Second leaves that file, and the whole handlers package, green.
//
// These tests go through handleNewIntent -- the real entry point -- with a
// non-canonical phrasing and a stub llm-service. No model, no DB, no broker:
// isKnownConnector and listKnownConnectors both degrade to a static set when
// db.GetDB() is nil, and maybeHandleDiagnoseCommand returns early for the same
// reason.

// isKnownConnector fails OPEN when its catalog query errors -- every word
// becomes a connector, and reToPair then matches phrasings like "...best WAY TO
// KEEP our analytics tables fresh?", swallowing them into the regex fast path
// these tests exist to bypass. A sibling test in this package assigning
// db.DB = <mock> beside a defer that closes it leaves the global pointing at a
// closed handle, which is exactly that error. Pin it to nil so the deterministic
// common-connector fallback runs instead. Production always has a healthy
// catalog DB. Same defence, same reason, as chat_nl_pipeline_pairparse_test.go.
func pinNoCatalogDB(t *testing.T) {
	t.Helper()
	prev := db.DB
	db.DB = nil
	t.Cleanup(func() { db.DB = prev })
}

// promptLog records the prompt names the stub was asked for. The stub's handler
// runs on net/http's goroutine while the assertions run on the test's, so this
// needs a mutex and not a bare slice. CI drops -race unconditionally
// (.github/workflows/ci.yml, macOS/arm64 LC_UUID), which is precisely why an
// unsynchronised append here would never turn a job red.
type promptLog struct {
	mu    sync.Mutex
	names []string
}

func (p *promptLog) add(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.names = append(p.names, name)
}

func (p *promptLog) snapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.names...)
}

// chatStub serves llm-service's /v1/completion for both chat prompts, after an
// artificial delay that stands in for CPU inference. It records which prompts
// were asked for, so a test can prove the LLM path was actually entered rather
// than short-circuited by a regex.
func chatStub(t *testing.T, delay time.Duration, intentContent, helpContent string) *promptLog {
	t.Helper()
	called := &promptLog{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/completion" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		var req struct {
			PromptName string `json:"prompt_name"`
		}
		_ = json.Unmarshal(body, &req)
		called.add(req.PromptName)

		time.Sleep(delay)

		content := intentContent
		if req.PromptName == "chat/help_response" {
			content = helpContent
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"content": content})
	}))
	t.Cleanup(srv.Close)
	t.Setenv("LLM_SERVICE_URL", srv.URL)
	return called
}

// The canned fallback handleNewIntent returns when parseIntent fails. This
// sentence is the entire user-visible symptom of defect F, and before this file
// no test in either repo mentioned it.
const cannedIntentFallbackPrefix = "I can help you move data between systems."

func newIntentTestContext(t *testing.T) *gin.Context {
	t.Helper()
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/chat/message", nil)
	return c
}

const (
	fencedIntent = "```json\n" +
		`{"intent":"general_knowledge","requires_execution":false,"parameters":{}}` +
		"\n```"
	fencedHelp = "```json\n" +
		`{"message":"Point rsync at a source and a destination and it moves the data.",` +
		`"suggestions":["mysql to bigquery"]}` +
		"\n```"
	realHelpAnswer = "Point rsync at a source and a destination and it moves the data."
)

// Vacuity floor. If a future widening of the regex fast paths swallows the
// phrasings below, the two tests after this one would stop exercising the LLM
// path and silently pass forever. This asserts the gate's shape in both
// directions -- a canonical control that MUST be caught by the fast path, and
// the non-canonical cases that MUST NOT be -- so that widening breaks here,
// loudly, instead of hollowing out the guard.
func TestNonCanonicalPhrasingsMissEveryRegexFastPath(t *testing.T) {
	pinNoCatalogDB(t)
	h := &ChatHandler{}

	reachesLLM := func(msg string) bool {
		if h.quickParseDataSyncIntent(msg) != nil {
			return false
		}
		if looksLikeHelpRequest(msg) {
			return true // routed to the help LLM, still a model call
		}
		scIntent, ambiguous := h.quickParseSingleConnectorIntent(msg)
		return scIntent == nil && ambiguous == ""
	}

	// Control: the phrasing every existing test and e2e spec sends. It must be
	// answered by the regex, never by a model -- that is what made defect F
	// invisible, and it is also the behaviour we want to keep.
	for _, canonical := range []string{
		"sync mysql to postgresql",
		"mysql  to  aws  s3",
		"postgresql to bigquery",
	} {
		if reachesLLM(canonical) {
			t.Errorf("canonical %q now reaches the LLM; the fast path regressed", canonical)
		}
	}

	for _, nonCanonical := range []string{
		"what's the best way to keep our analytics tables fresh?",
		"can you move everything from our orders database over to the lake",
		"set up a nightly copy of the billing data",
	} {
		if !reachesLLM(nonCanonical) {
			t.Errorf("non-canonical %q no longer reaches the LLM: a fast path widened "+
				"and the end-to-end tests below have gone vacuous", nonCanonical)
		}
	}
}

// The configured deadline must reach the actual outbound request, not merely be
// returned by llmServiceTimeout(). Proven the only way that does not need a
// 60-100s CPU inference: a stub slower than a deliberately short deadline must
// produce the canned fallback, and the same stub under the default deadline must
// produce the model's real answer.
//
// Re-hard-coding either literal in parseIntent back to 10*time.Second makes the
// first subtest fail -- the 1s deadline stops biting.
func TestConfiguredDeadlineReachesTheRequestForANonCanonicalMessage(t *testing.T) {
	pinNoCatalogDB(t)
	const msg = "can you move everything from our orders database over to the lake"
	const stubDelay = 1500 * time.Millisecond

	t.Run("a deadline shorter than inference yields the canned fallback", func(t *testing.T) {
		t.Setenv("LLM_SERVICE_TIMEOUT_SECONDS", "1")
		called := chatStub(t, stubDelay, fencedIntent, fencedHelp)

		resp := (&ChatHandler{}).handleNewIntent(context.Background(), newIntentTestContext(t),
			chat.NewConversationContext("u1", "s1"), msg, "trace-short", "s1", "u1")

		if len(called.snapshot()) == 0 {
			t.Fatal("llm-service was never called: a regex fast path swallowed the message")
		}
		if !strings.HasPrefix(resp.Message, cannedIntentFallbackPrefix) {
			t.Errorf("a 1s deadline against a %v reply did not time out; the configured\n"+
				"deadline is not reaching the request (a hard-coded literal is back).\n"+
				"got reply: %q", stubDelay, resp.Message)
		}
	})

	t.Run("the default deadline is long enough for the same reply", func(t *testing.T) {
		t.Setenv("LLM_SERVICE_TIMEOUT_SECONDS", "")
		called := chatStub(t, stubDelay, fencedIntent, fencedHelp)

		resp := (&ChatHandler{}).handleNewIntent(context.Background(), newIntentTestContext(t),
			chat.NewConversationContext("u1", "s1"), msg, "trace-default", "s1", "u1")

		if len(called.snapshot()) == 0 {
			t.Fatal("llm-service was never called: a regex fast path swallowed the message")
		}
		if strings.HasPrefix(resp.Message, cannedIntentFallbackPrefix) {
			t.Fatalf("the default deadline produced the canned fallback for a %v reply", stubDelay)
		}
	})
}

// The fenced reply must survive the whole route, not just parseIntent in
// isolation: a non-canonical message must come back as the model's own answer.
// Both chat prompts on this route fence their JSON here, so dropping
// llmjson.ExtractObject at either the parseIntent or the callHelpResponseLLM
// site turns this into the canned/generic reply.
func TestNonCanonicalMessageGetsTheModelsAnswerThroughTheWholeRoute(t *testing.T) {
	pinNoCatalogDB(t)
	const msg = "can you move everything from our orders database over to the lake"
	called := chatStub(t, 0, fencedIntent, fencedHelp)

	resp := (&ChatHandler{}).handleNewIntent(context.Background(), newIntentTestContext(t),
		chat.NewConversationContext("u1", "s1"), msg, "trace-fenced", "s1", "u1")

	// Both prompts must have been reached -- the intent classifier, then the help
	// responder it routes to. Asserting the pair keeps a future short-circuit
	// from quietly reducing this to a one-call test.
	names := called.snapshot()
	if len(names) != 2 ||
		names[0] != "chat/intent_classification" ||
		names[1] != "chat/help_response" {
		t.Fatalf("prompts called = %v, want [chat/intent_classification chat/help_response]", names)
	}

	if strings.HasPrefix(resp.Message, cannedIntentFallbackPrefix) {
		t.Fatalf("fenced intent reply produced the canned fallback: %q", resp.Message)
	}
	if resp.Message != realHelpAnswer {
		t.Errorf("reply = %q, want the model's own answer %q", resp.Message, realHelpAnswer)
	}
}
