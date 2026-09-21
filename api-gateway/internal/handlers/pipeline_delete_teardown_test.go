package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestTeardownOutcome: both orchestrator teardown endpoints answer 200 with
// success:false when a step fails, so reading only the status code reported a
// leaked replication slot or sink worker as "completed".
func TestTeardownOutcome(t *testing.T) {
	cases := []struct {
		name        string
		status      int
		body        string
		wantSummary string
		wantDetail  string
	}{
		{"clean", 200, `{"success":true,"message":"ok"}`, "", ""},
		{"200 with errors", 200, `{"success":false,"errors":["connector delete: HTTP 500","postgresql cleanup: slot active"]}`,
			"2 error(s)", "connector delete: HTTP 500; postgresql cleanup: slot active"},
		{"unauthorized", 401, `{"error":"missing internal secret"}`, "HTTP 401", "missing internal secret"},
		{"forbidden, not json", 403, `nope`, "HTTP 403", "HTTP 403"},
		{"server error with errors list", 500, `{"success":false,"errors":["db down"]}`, "HTTP 500", "db down"},
		{"2xx without a success field", 204, ``, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			summary, detail := teardownOutcome(tc.status, []byte(tc.body))
			if summary != tc.wantSummary {
				t.Fatalf("summary = %q, want %q", summary, tc.wantSummary)
			}
			if !strings.Contains(detail, tc.wantDetail) {
				t.Fatalf("detail = %q, want it to contain %q", detail, tc.wantDetail)
			}
		})
	}
}

func TestPostOrchestratorTeardownReturnsWarningOnlyOnFailure(t *testing.T) {
	respond := func(status int, body string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/v1/cdc/cleanup" {
				t.Errorf("path = %q", r.URL.Path)
			}
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}))
	}
	call := func(url string) string {
		t.Setenv("ORCHESTRATOR_URL", url)
		return postOrchestratorTeardown(context.Background(), "/api/v1/cdc/cleanup",
			"abd8a64d-0000-4000-8000-000000000001", 5*time.Second, "CDC cleanup", "slot may leak")
	}

	ok := respond(200, `{"success":true}`)
	defer ok.Close()
	if w := call(ok.URL); w != "" {
		t.Fatalf("clean teardown produced warning %q", w)
	}

	partial := respond(200, `{"success":false,"errors":["password=hunter2 rejected"]}`)
	defer partial.Close()
	w := call(partial.URL)
	if !strings.Contains(w, "CDC cleanup") || !strings.Contains(w, "1 error(s)") || !strings.Contains(w, "slot may leak") {
		t.Fatalf("warning = %q, want label, error count and blast radius", w)
	}
	if strings.Contains(w, "hunter2") {
		t.Fatalf("warning leaked orchestrator error text into the API body: %q", w)
	}

	closed := respond(200, ``)
	closed.Close()
	if w := call(closed.URL); !strings.Contains(w, "unreachable") {
		t.Fatalf("unreachable orchestrator warning = %q", w)
	}
}

func TestDeletePipelineResponseOmitsEmptyWarnings(t *testing.T) {
	if _, ok := deletePipelineResponse(nil)["warnings"]; ok {
		t.Fatal("a clean delete must not carry a warnings key")
	}
	if got := deletePipelineResponse([]string{"x"})["warnings"]; got == nil {
		t.Fatal("warnings dropped from the delete response")
	}
}
