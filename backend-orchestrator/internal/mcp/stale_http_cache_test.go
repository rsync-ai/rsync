package mcp

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// KI-A-STOPPED-MCP-CONTAINER-IS-NEVER-REDEPLOYED: StartServer returned any cached HTTP
// entry marked "running" without asking whether it still answered, and nothing removed
// such an entry. After `docker stop` or `docker rm` of a connector container, the
// per-run preflight, the infra sweep and dependency_probe's recovery all got the dead
// entry back, so none of them asked tool-generator to redeploy it until the
// orchestrator restarted.

// countingToolGenerator answers /v1/deploy like tool-generator does when it has started
// a container, and counts the calls.
func countingToolGenerator(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var deploys atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/deploy" {
			deploys.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"building":false}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &deploys
}

func hostPort(t *testing.T, rawURL string) (string, int) {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}
	return host, port
}

// deadAddress is a host:port that refuses connections, like a stopped container's.
func deadAddress(t *testing.T) (string, int) {
	t.Helper()
	srv := httptest.NewServer(http.NotFoundHandler())
	host, port := hostPort(t, srv.URL)
	srv.Close()
	return host, port
}

func cacheHTTPServer(sm *ServerManager, id, host string, port int) *ServerInfo {
	entry := &ServerInfo{Name: id, Status: "running", ConnType: "http", Host: host, Port: port, StartedAt: time.Now()}
	sm.mu.Lock()
	sm.servers[makeServerKey(id, "v1.0.0")] = entry
	sm.mu.Unlock()
	return entry
}

func cachedEntry(sm *ServerManager, id string) *ServerInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.servers[makeServerKey(id, "v1.0.0")]
}

func TestStartServerRedeploysACachedHTTPServerThatNoLongerAnswers(t *testing.T) {
	const id = "zzfakestaleconn"
	tools := writeFakeConnector(t, id, "Fake DB")

	for _, tc := range []struct {
		name    string
		cfg     ServerConfig
		wantErr string
	}{
		// infra_preflight.go checkMCPUser for a batch destination: stdio stays allowed,
		// so with no container coming up the fake connector's stdio path fails.
		{"batch preflight", ServerConfig{Name: id, Version: "latest", DeployWaitTimeout: 50 * time.Millisecond}, "connector script not found"},
		// dependency_probe maybeRecover, the CDC preflight and the infra sweep.
		{"require HTTP", ServerConfig{Name: id, Version: "latest", RequireHTTP: true, DeployWaitTimeout: 50 * time.Millisecond}, "requires Docker HTTP transport"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tg, deploys := countingToolGenerator(t)
			t.Setenv("TOOL_GENERATOR_URL", tg.URL)

			sm := NewServerManager(tools)
			host, port := deadAddress(t)
			stale := cacheHTTPServer(sm, id, host, port)

			got, err := sm.StartServer(tc.cfg)
			if got == stale {
				t.Fatalf("StartServer returned the cached entry for %s:%d, which refuses connections", host, port)
			}
			if n := deploys.Load(); n != 1 {
				t.Errorf("tool-generator /v1/deploy calls = %d, want 1", n)
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("err = %v, want one containing %q", err, tc.wantErr)
			}
			if cachedEntry(sm, id) == stale {
				t.Error("the dead entry is still cached")
			}
		})
	}
}

// Control for the test above: a cached server that still answers /health is returned
// as it is, with no deploy call, and a second call inside the verify interval does not
// ask again. Without it, a StartServer that always redeployed would pass the test above.
func TestStartServerKeepsACachedHTTPServerThatAnswers(t *testing.T) {
	const id = "zzfakestaleconn"
	tools := writeFakeConnector(t, id, "Fake DB")
	tg, deploys := countingToolGenerator(t)
	t.Setenv("TOOL_GENERATOR_URL", tg.URL)

	var healthChecks atomic.Int32
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			healthChecks.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(live.Close)

	sm := NewServerManager(tools)
	host, port := hostPort(t, live.URL)
	entry := cacheHTTPServer(sm, id, host, port)

	for i := 0; i < 2; i++ {
		got, err := sm.StartServer(ServerConfig{Name: id, Version: "latest", RequireHTTP: true, DeployWaitTimeout: 50 * time.Millisecond})
		if err != nil || got != entry {
			t.Fatalf("call %d: StartServer = (%p, %v), want the cached entry %p", i+1, got, err, entry)
		}
	}
	if n := deploys.Load(); n != 0 {
		t.Errorf("tool-generator /v1/deploy calls = %d, want 0", n)
	}
	if n := healthChecks.Load(); n != 1 {
		t.Errorf("/health checks = %d, want 1 (the second call is inside the verify interval)", n)
	}
}

func TestExecuteViaHTTPForgetsAServerThatIsGone(t *testing.T) {
	const id = "zzfakestaleconn"
	req := JSONRPCRequest{JSONRPC: "2.0", ID: 1, Method: "tools/call"}

	t.Run("connection refused", func(t *testing.T) {
		sm := NewServerManager(t.TempDir())
		host, port := deadAddress(t)
		entry := cacheHTTPServer(sm, id, host, port)

		resp, _ := NewClient(sm).executeViaHTTP(context.Background(), entry, req)
		if resp == nil || resp.Success {
			t.Fatalf("expected a failed response, got %+v", resp)
		}
		if cachedEntry(sm, id) != nil {
			t.Error("a server that refuses connections is still cached")
		}
	})

	t.Run("no such host", func(t *testing.T) {
		sm := NewServerManager(t.TempDir())
		// .invalid never resolves (RFC 2606), like a removed container's name.
		entry := cacheHTTPServer(sm, id, "zzfakestaleconn-v1-0-0-mcp.invalid", 8000)

		resp, _ := NewClient(sm).executeViaHTTP(context.Background(), entry, req)
		if resp == nil || resp.Success {
			t.Fatalf("expected a failed response, got %+v", resp)
		}
		if cachedEntry(sm, id) != nil {
			t.Error("a server whose name does not resolve is still cached")
		}
	})

	// Control: a connector that is only slow is still there. A timeout, or the caller
	// giving up, must not throw its entry away.
	t.Run("timeout keeps the entry", func(t *testing.T) {
		release := make(chan struct{})
		slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}))
		t.Cleanup(func() { close(release); slow.Close() })

		sm := NewServerManager(t.TempDir())
		host, port := hostPort(t, slow.URL)
		entry := cacheHTTPServer(sm, id, host, port)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		resp, _ := NewClient(sm).executeViaHTTP(ctx, entry, req)
		if resp == nil || resp.Success {
			t.Fatalf("expected a failed response, got %+v", resp)
		}
		if cachedEntry(sm, id) != entry {
			t.Error("a slow server's entry was dropped")
		}
	})
}
