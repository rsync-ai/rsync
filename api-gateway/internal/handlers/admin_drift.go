package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// AdminDriftCheck hits the /version endpoint of every Go/Python service in
// the docker-compose network and reports back. The point is to catch
// "we shipped a fix but the running container is still on the old commit"
// — historically the highest-impact bug class in this repo. If the operator
// sees mixed commits across services, the deployment is broken regardless
// of what any other dashboard says.
//
// GET /api/v1/admin/drift
//
// Reads no auth/role beyond the existing admin middleware.
type DriftServiceResult struct {
	Service    string `json:"service"`
	URL        string `json:"url"`
	OK         bool   `json:"ok"`
	StatusCode int    `json:"status_code,omitempty"`
	Commit     string `json:"commit,omitempty"`
	BuiltAt    string `json:"built_at,omitempty"`
	StartedAt  string `json:"started_at,omitempty"`
	UptimeSecs int64  `json:"uptime_secs,omitempty"`
	Error      string `json:"error,omitempty"`
	LatencyMs  int64  `json:"latency_ms"`
}

type DriftReport struct {
	GatheredAt    string                `json:"gathered_at"`
	AllAgree      bool                  `json:"all_agree"`
	UniqueCommits []string              `json:"unique_commits"`
	Suspicions    []string              `json:"suspicions"`
	Services      []DriftServiceResult  `json:"services"`
}

// driftTargets is the canonical list of "what should agree on a commit."
// Mirrors config/services.yaml. Hardcoded here intentionally — services
// that need a config-file loader can read the YAML; this endpoint is
// itself part of the deployment-drift detector and must work even if
// the YAML is wrong.
var driftTargets = []struct {
	Name string
	URL  string
}{
	{"api-gateway", "http://api-gateway:8080/version"},
	{"backend-orchestrator", "http://orchestrator:8080/version"},
	{"backend-temporal-adapter", "http://temporal-adapter:8080/version"},
	{"llm-service", "http://llm-service:5000/version"},
}

func AdminDriftCheck(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	results := make([]DriftServiceResult, len(driftTargets))
	var wg sync.WaitGroup
	for i, t := range driftTargets {
		wg.Add(1)
		go func(i int, name, url string) {
			defer wg.Done()
			results[i] = probeVersion(ctx, name, url)
		}(i, t.Name, t.URL)
	}
	wg.Wait()

	// Compute drift summary. We only count commits that the service
	// actually returned (skip "dev" placeholders + error rows) when
	// deciding whether they agree, because dev environments often run
	// some services in mixed modes.
	uniqueCommits := map[string]struct{}{}
	for _, r := range results {
		if !r.OK || r.Commit == "" || r.Commit == "dev" {
			continue
		}
		uniqueCommits[r.Commit] = struct{}{}
	}
	commits := []string{}
	for c := range uniqueCommits {
		commits = append(commits, c)
	}
	suspicions := []string{}
	for _, r := range results {
		if !r.OK {
			suspicions = append(suspicions, r.Service+": unreachable ("+r.Error+")")
		} else if r.Commit == "dev" || r.Commit == "" {
			suspicions = append(suspicions, r.Service+": commit unknown — image was not built with GIT_COMMIT")
		}
	}
	if len(commits) > 1 {
		suspicions = append(suspicions, "services are running mixed commits — at least one container is stale")
	}

	c.JSON(http.StatusOK, DriftReport{
		GatheredAt:    time.Now().UTC().Format(time.RFC3339),
		AllAgree:      len(commits) <= 1 && len(suspicions) == 0,
		UniqueCommits: commits,
		Suspicions:    suspicions,
		Services:      results,
	})
}

func probeVersion(ctx context.Context, name, url string) DriftServiceResult {
	out := DriftServiceResult{Service: name, URL: url}
	t0 := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		out.Error = err.Error()
		out.LatencyMs = time.Since(t0).Milliseconds()
		return out
	}
	client := &http.Client{Timeout: 4 * time.Second}
	resp, err := client.Do(req)
	out.LatencyMs = time.Since(t0).Milliseconds()
	if err != nil {
		out.Error = err.Error()
		return out
	}
	defer resp.Body.Close()
	out.StatusCode = resp.StatusCode
	if resp.StatusCode >= 400 {
		out.Error = "non-2xx status"
		return out
	}
	var v map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		out.Error = "decode failed: " + err.Error()
		return out
	}
	out.OK = true
	out.Commit = stringField(v, "commit")
	out.BuiltAt = stringField(v, "built_at")
	out.StartedAt = stringField(v, "started_at")
	if u, ok := v["uptime_secs"].(float64); ok {
		out.UptimeSecs = int64(u)
	}
	log.Debugf("drift: %s ok commit=%s", name, out.Commit)
	return out
}

func stringField(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}
