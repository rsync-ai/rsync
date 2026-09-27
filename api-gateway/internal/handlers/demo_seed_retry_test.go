package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// The first "Try the demo" click on a fresh install deploys sample-data on demand, and
// the create that triggers the deploy answers 503 connector_deploying. These pin that
// the seed waits that out instead of surfacing it as demo_seed_failed.

func withDemoReplay(t *testing.T, window, interval time.Duration, fn func(CreateConnectionRequest) (int, map[string]interface{}, error)) *int {
	t.Helper()
	calls := 0
	origReplay, origWindow, origInterval := demoReplayCreate, demoDeployRetryWindow, demoDeployRetryInterval
	demoReplayCreate = func(_ *gin.Context, p CreateConnectionRequest) (int, map[string]interface{}, error) {
		calls++
		return fn(p)
	}
	demoDeployRetryWindow, demoDeployRetryInterval = window, interval
	t.Cleanup(func() {
		demoReplayCreate, demoDeployRetryWindow, demoDeployRetryInterval = origReplay, origWindow, origInterval
	})
	return &calls
}

func demoTestContext() *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/demo/seed", nil)
	return c
}

var deployingBody = map[string]interface{}{"error": "connector_deploying", "status": "connector_deploying", "retryable": true}

func TestDemoSeedRetriesWhileConnectorDeploys(t *testing.T) {
	n := 0
	calls := withDemoReplay(t, time.Second, time.Millisecond, func(CreateConnectionRequest) (int, map[string]interface{}, error) {
		n++
		if n < 3 {
			return http.StatusServiceUnavailable, deployingBody, nil
		}
		return http.StatusCreated, map[string]interface{}{"id": "conn-1"}, nil
	})

	id, err := ensureDemoConnection(demoTestContext(), "ws-1", time.Now().Add(time.Second), CreateConnectionRequest{ConnectorType: "sample-data"})
	if err != nil {
		t.Fatalf("expected success after the connector came up, got %v", err)
	}
	if id != "conn-1" || *calls != 3 {
		t.Fatalf("got id=%q after %d calls, want conn-1 after 3", id, *calls)
	}
}

func TestDemoSeedGivesUpWhenTheWindowCloses(t *testing.T) {
	calls := withDemoReplay(t, 0, 5*time.Millisecond, func(CreateConnectionRequest) (int, map[string]interface{}, error) {
		return http.StatusServiceUnavailable, deployingBody, nil
	})

	_, err := ensureDemoConnection(demoTestContext(), "ws-1", time.Now().Add(30*time.Millisecond), CreateConnectionRequest{})
	var seedErr demoSeedError
	if !asDemoSeedError(err, &seedErr) || seedErr.status != http.StatusServiceUnavailable {
		t.Fatalf("want the last 503 forwarded as a demoSeedError, got %v", err)
	}
	// A 30ms window at a 5ms interval allows a handful of attempts, never an unbounded loop.
	if *calls < 2 || *calls > 7 {
		t.Fatalf("made %d attempts inside a 30ms window at 5ms spacing", *calls)
	}
}

// Only connector_deploying is retryable. A 503 for any other reason, and every
// credential failure, must come back on the first attempt.
func TestDemoSeedDoesNotRetryOtherFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   map[string]interface{}
	}{
		{"other 503", http.StatusServiceUnavailable, map[string]interface{}{"error": "database_unavailable"}},
		{"bad credentials", http.StatusUnprocessableEntity, map[string]interface{}{"error": "connection_test_failed"}},
		{"forbidden", http.StatusForbidden, map[string]interface{}{"error": "forbidden"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := withDemoReplay(t, time.Second, time.Millisecond, func(CreateConnectionRequest) (int, map[string]interface{}, error) {
				return tc.status, tc.body, nil
			})
			_, err := ensureDemoConnection(demoTestContext(), "ws-1", time.Now().Add(time.Second), CreateConnectionRequest{})
			if err == nil || *calls != 1 {
				t.Fatalf("want one attempt and an error, got %d attempts, err=%v", *calls, err)
			}
		})
	}
}
