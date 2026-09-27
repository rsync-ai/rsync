package workers

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// Locks KI-PROBE-SOURCE-CACHE-AFTER-RESTART.
//
// The MCP registry is in-memory, so an orchestrator restart empties it. A CDC source
// connector is read by Debezium, never through the orchestrator, so nothing registers
// it again, and probeOne asked only the registry: on prod the postgresql container was
// up and healthy while both of its mcp_source rows read "no MCP server registered with
// orchestrator", which /runtime and the pipeline list roll up into a Failed pipeline.
// The rows recovered only when something unrelated called the connector (a new
// pipeline's first run, or the gateway's 6h assessment re-check).
//
// RED against the pre-fix source: probeOne never looked past GetServer, so every
// case in TestMCPProbeFindsUnregisteredRunningContainer reports unhealthy and the
// lookup is never asked.

// emptyRegistryProbe is a probe whose MCP registry is as empty as it is right after
// an orchestrator restart.
func emptyRegistryProbe(t *testing.T) *DependencyProbe {
	t.Helper()
	return &DependencyProbe{
		mcpManager: mcp.NewServerManager(t.TempDir()),
		httpClient: &http.Client{Timeout: 2 * time.Second},
		tickEvery:  15 * time.Second,
		stopCh:     make(chan struct{}),
	}
}

// stubFindRunning replaces the container lookup and records what it was asked for.
func stubFindRunning(t *testing.T, server *mcp.ServerInfo) *[]string {
	t.Helper()
	orig := findRunningMCPServer
	t.Cleanup(func() { findRunningMCPServer = orig })
	asked := []string{}
	findRunningMCPServer = func(_ *mcp.ServerManager, name, version string) (*mcp.ServerInfo, bool) {
		asked = append(asked, name+"@"+version)
		return server, server != nil
	}
	return &asked
}

// runningConnector is a container that answers /health the way a live connector does.
func runningConnector(t *testing.T) *mcp.ServerInfo {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse %s: %v", srv.URL, err)
	}
	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatalf("split %s: %v", u.Host, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("port %s: %v", portStr, err)
	}
	return &mcp.ServerInfo{Name: "postgresql", Status: "running", ConnType: "http", Host: host, Port: port}
}

func TestMCPProbeFindsUnregisteredRunningContainer(t *testing.T) {
	cases := []struct{ name, kind, identifier string }{
		{"source, as prod stores it", "mcp_source", "postgresql@v1.0.0"},
		// A dependency hydrated from a connection can carry a bare version. The lookup
		// still gets the v-prefixed one, so a discovered container is registered under
		// the key StartServer uses and not under a second, bare one.
		{"source, bare version", "mcp_source", "postgresql@1.0.0"},
		{"destination", "mcp_dest", "postgresql@v1.0.0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := emptyRegistryProbe(t)
			asked := stubFindRunning(t, runningConnector(t))

			status, lastError, details := p.probeOne(context.Background(), "637a0df2-eb7d-45ea-8d0b-4e9ca954d1f8", tc.kind, tc.identifier, nil)

			if status != "healthy" || lastError != "" {
				t.Fatalf("got (%q, %q), want a running, answering container to read healthy; asked %v", status, lastError, *asked)
			}
			if want := []string{"postgresql@v1.0.0"}; !reflect.DeepEqual(*asked, want) {
				t.Fatalf("lookup asked %v, want %v", *asked, want)
			}
			if details["http_status"] != http.StatusOK {
				t.Fatalf("details %v: the discovered container's /health was not probed", details)
			}
		})
	}
}

// Control: with no container to find, the verdict is still the unhealthy one, so the
// lookup cannot turn a dead connector green.
func TestMCPProbeNoContainerStaysUnhealthy(t *testing.T) {
	p := emptyRegistryProbe(t)
	asked := stubFindRunning(t, nil)

	status, lastError, _ := p.probeOne(context.Background(), "637a0df2-eb7d-45ea-8d0b-4e9ca954d1f8", "mcp_source", "postgresql@v1.0.0", nil)

	if status != "unhealthy" || lastError != "no MCP server registered with orchestrator" {
		t.Fatalf("got (%q, %q), want unhealthy / no MCP server registered", status, lastError)
	}
	if len(*asked) == 0 {
		t.Fatal("the lookup was never asked, so this control proves nothing")
	}
}
