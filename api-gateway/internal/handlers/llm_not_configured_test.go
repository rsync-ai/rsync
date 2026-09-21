package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// An install without an LLM is supported. llm-service answers the LLM-only
// routes with 503 {"error":"llm_not_configured","message":"Set up an LLM first: ..."};
// the gateway must hand that to the UI instead of "SQL generation failed".

const gatedMessage = "Set up an LLM first: add OPENAI_API_KEY to .env, then restart rsync."

var (
	flatGated   = `{"error":"llm_not_configured","message":"` + gatedMessage + `"}`
	nestedGated = `{"detail":{"error":"llm_not_configured","message":"` + gatedMessage + `"}}`
	plainBusy   = `{"detail":"busy"}`
)

func TestLLMNotConfiguredBody(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		body    string
		wantOK  bool
		wantMsg string
	}{
		{"flat", http.StatusServiceUnavailable, flatGated, true, gatedMessage},
		{"nested under detail", http.StatusServiceUnavailable, nestedGated, true, gatedMessage},
		{"missing message uses fallback", http.StatusServiceUnavailable, `{"error":"llm_not_configured"}`, true, llmNotConfiguredFallbackMessage},
		{"plain 503 is not it", http.StatusServiceUnavailable, plainBusy, false, ""},
		{"other error code", http.StatusServiceUnavailable, `{"error":"rate_limited","message":"x"}`, false, ""},
		{"right body, wrong status", http.StatusInternalServerError, flatGated, false, ""},
		{"not json", http.StatusServiceUnavailable, `upstream connect error`, false, ""},
		{"empty", http.StatusServiceUnavailable, ``, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, ok := llmNotConfiguredBody(tc.status, []byte(tc.body))
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (body %s)", ok, tc.wantOK, tc.body)
			}
			if !ok {
				return
			}
			if h["error"] != llmNotConfiguredCode {
				t.Fatalf("error = %v", h["error"])
			}
			if h["message"] != tc.wantMsg {
				t.Fatalf("message = %q, want %q", h["message"], tc.wantMsg)
			}
		})
	}
	if !strings.HasPrefix(llmNotConfiguredFallbackMessage, "Set up an LLM first") {
		t.Fatalf("fallback must lead with what to do: %q", llmNotConfiguredFallbackMessage)
	}
}

func upstream(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func decode(t *testing.T, w *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()
	var out map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("response is not JSON: %v (%s)", err, w.Body.String())
	}
	return out
}

func callGenerateSQL(t *testing.T) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/sql/generate",
		strings.NewReader(`{"question":"how many users signed up this week","schema":"users(id, created_at)"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	GenerateSQL(c)
	return w
}

func TestGenerateSQL_RelaysLLMNotConfigured(t *testing.T) {
	for name, body := range map[string]string{"flat": flatGated, "nested": nestedGated} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TEXT2SQL_ENDPOINT", upstream(t, http.StatusServiceUnavailable, body).URL)
			w := callGenerateSQL(t)
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503 (%s)", w.Code, w.Body.String())
			}
			got := decode(t, w)
			if got["error"] != llmNotConfiguredCode || got["message"] != gatedMessage {
				t.Fatalf("body = %v", got)
			}
		})
	}
}

func TestGenerateSQL_PlainUpstreamFailureStaysGeneric(t *testing.T) {
	// Control: a busy llm-service is not "no LLM"; the UI must not tell the
	// operator to configure something that is already configured.
	t.Setenv("TEXT2SQL_ENDPOINT", upstream(t, http.StatusServiceUnavailable, plainBusy).URL)
	w := callGenerateSQL(t)
	got := decode(t, w)
	if got["error"] != "SQL generation failed" {
		t.Fatalf("body = %v", got)
	}
}

func TestCallDiagnoseLLM_ReturnsLLMNotConfigured(t *testing.T) {
	t.Setenv("LLM_SERVICE_URL", upstream(t, http.StatusServiceUnavailable, flatGated).URL)
	_, _, err := callDiagnoseLLM(context.Background(), "p1", map[string]interface{}{"status": "failed"})
	var notSetUp *llmNotConfiguredError
	if !errors.As(err, &notSetUp) {
		t.Fatalf("err = %v, want llmNotConfiguredError", err)
	}
	body := notSetUp.Body()
	body["evidence"] = "added by caller"
	if _, leaked := notSetUp.Body()["evidence"]; leaked {
		t.Fatal("Body() must return a copy")
	}
	if body["message"] != gatedMessage {
		t.Fatalf("message = %v", body["message"])
	}
}

func TestCallDiagnoseLLM_PlainFailureIsNotLLMNotConfigured(t *testing.T) {
	t.Setenv("LLM_SERVICE_URL", upstream(t, http.StatusServiceUnavailable, plainBusy).URL)
	_, _, err := callDiagnoseLLM(context.Background(), "p1", map[string]interface{}{})
	var notSetUp *llmNotConfiguredError
	if err == nil || errors.As(err, &notSetUp) {
		t.Fatalf("err = %v, want a plain upstream error", err)
	}
}

func TestGenerateConnector_SaysSetUpAnLLMFirst(t *testing.T) {
	t.Setenv("TOOL_GENERATOR_URL", upstream(t, http.StatusServiceUnavailable, flatGated).URL)
	w := callGenerateConnector(`{"api_name":"mockwidgets-nollm","force_regenerate":true,"save_artifacts":false}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d (%s)", w.Code, w.Body.String())
	}
	got := decode(t, w)
	// The wizard reads error_message; it must say what to do, not the code.
	if got["error_message"] != gatedMessage || got["error"] != llmNotConfiguredCode {
		t.Fatalf("body = %v", got)
	}
}

func TestGenerateConnector_PlainFailureKeepsItsMessage(t *testing.T) {
	t.Setenv("TOOL_GENERATOR_URL", upstream(t, http.StatusServiceUnavailable, `{"success":false,"error":"docs unreachable"}`).URL)
	w := callGenerateConnector(`{"api_name":"mockwidgets-nollm","force_regenerate":true,"save_artifacts":false}`)
	got := decode(t, w)
	if got["error_message"] != "docs unreachable" || got["error_stage"] == llmNotConfiguredCode {
		t.Fatalf("body = %v", got)
	}
}

// The resolve and diagnose handlers need a live schema index or pipeline rows
// to reach llm-service, so check the wiring in the source: each must handle
// llm_not_configured before its generic failure branch.
func TestHandlersRelayBeforeGenericFailure(t *testing.T) {
	fset := token.NewFileSet()
	parsed := map[string]*ast.File{}
	for _, name := range []string{"explorer.go", "diagnose.go"} {
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		parsed[name] = f
	}
	// firstUse finds, inside fn, the first mention of ident and the first
	// string literal equal to failure.
	firstUse := func(file, fn, ident, failure string) (use, generic token.Pos, found bool) {
		for _, decl := range parsed[file].Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Name.Name != fn {
				continue
			}
			found = true
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.Ident:
					if x.Name == ident && use == token.NoPos {
						use = x.Pos()
					}
				case *ast.BasicLit:
					if x.Value == strconv.Quote(failure) && generic == token.NoPos {
						generic = x.Pos()
					}
				}
				return true
			})
		}
		return
	}
	for _, tc := range []struct{ file, fn, ident, failure string }{
		{"explorer.go", "GenerateSQL", "relayLLMNotConfigured", "SQL generation failed"},
		{"explorer.go", "ResolveExplorerTables", "relayLLMNotConfigured", "Table resolution failed"},
		{"explorer.go", "ResolveExplorerColumns", "relayLLMNotConfigured", "Column resolution failed"},
		{"diagnose.go", "DiagnosePipeline", "llmNotConfiguredError", "diagnose_unavailable"},
	} {
		use, generic, found := firstUse(tc.file, tc.fn, tc.ident, tc.failure)
		if !found {
			t.Fatalf("%s not found in %s", tc.fn, tc.file)
		}
		if generic == token.NoPos {
			t.Fatalf("%s: generic %q branch not found; update this test", tc.fn, tc.failure)
		}
		if use == token.NoPos || use > generic {
			t.Errorf("%s must handle %s before its %q branch", tc.fn, tc.ident, tc.failure)
		}
	}
	// Control: the finder does not see a relay where there is none.
	if use, _, found := firstUse("explorer.go", "ExecuteExplorerQuery", "relayLLMNotConfigured", ""); !found || use != token.NoPos {
		t.Fatalf("control: ExecuteExplorerQuery found=%v use=%v", found, use)
	}
}
