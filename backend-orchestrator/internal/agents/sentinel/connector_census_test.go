package sentinel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// censusFixture stands up the smallest sweep that can be wrong: one connector in the
// tree, one sqlmock DB, one stubbed probe. The connector name is derived through
// mcp.MCPContainerName rather than written out, because the census keys ARE container
// names and a census that keys on anything else answers "not deployed" for everything.
type censusFixture struct {
	monitor     *HealthMonitor
	mock        sqlmock.Sqlmock
	container   string
	componentID string
	evicted     [][]string
}

func newCensusFixture(t *testing.T, probe stubTransport) *censusFixture {
	t.Helper()

	tree := t.TempDir()
	writeTestConnector(t, filepath.Join(tree, "public", "storage", "acme-blob"), "v2.1.0",
		`{"id":"acme-blob"}`, true)
	t.Setenv("MCP_CONNECTORS_PATH", tree)
	t.Setenv("STACK_PREFIX", "rsync-test")

	dbConn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { dbConn.Close() })

	f := &censusFixture{mock: mock}
	f.container = mcp.MCPContainerName("acme-blob", "v2.1.0")
	f.componentID = "mcp_connector:" + f.container

	f.monitor = NewHealthMonitor(nil, dbConn, &SentinelConfig{}, nil)
	f.monitor.ctx = context.Background()
	f.monitor.httpClient.Transport = probe
	f.monitor.onComponentsEvicted = func(ids []string) {
		f.evicted = append(f.evicted, ids)
	}
	return f
}

// serveCensus points the monitor at an httptest stand-in for the socket proxy, so the
// real dockerContainerCensus does the fetching, decoding and name normalisation. The
// seam (monitor.containerCensus) is deliberately NOT stubbed here: a test that injects
// the map cannot catch the two mistakes that matter — asking the wrong URL, and keying
// the map on the API's "/name" form while the sweep looks up "name".
func (f *censusFixture) serveCensus(t *testing.T, states map[string]string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("census used %s; the proxy runs with POST=0 so anything but GET is a 403 in production", r.Method)
		}
		if r.URL.Path != "/containers/json" {
			t.Errorf("census asked for %q, want /containers/json", r.URL.Path)
		}
		if r.URL.Query().Get("all") != "true" {
			t.Errorf("census asked without all=true; without it a stopped container reads as never-deployed, which is the bug")
		}
		out := make([]map[string]interface{}, 0, len(states))
		for name, state := range states {
			// The API reports names slash-prefixed. Hard-coding the prefix here is the
			// point: it is what the sweep has to strip to find a name it built itself.
			out = append(out, map[string]interface{}{
				"Names": []string{"/" + name},
				"State": state,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("MCP_DOCKER_API_URL", srv.URL)
	return srv
}

// TestTheCensusDecidesAbsenceNotTheProbe is the discriminating test for this fix.
//
// Both halves give the probe the SAME failing answer. The only thing that differs is
// whether the container list names this connector, and the verdicts must be opposite:
// listed is a fault to report, unlisted is a component to forget. Before the census the
// probe's answer decided both, which is why a host that had never deployed a connector
// still filed it as broken.
func TestTheCensusDecidesAbsenceNotTheProbe(t *testing.T) {
	t.Run("listed and not answering is a fault", func(t *testing.T) {
		f := newCensusFixture(t, stubTransport{status: http.StatusServiceUnavailable})
		f.serveCensus(t, map[string]string{f.container: "running"})

		f.mock.ExpectExec("INSERT INTO sentinel_component_health").
			WillReturnResult(sqlmock.NewResult(1, 1))

		f.monitor.checkMCPConnectorHealth()

		if err := f.mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("a deployed connector that did not answer was not written down as unhealthy: %v", err)
		}
		got := f.monitor.componentHealth[f.componentID]
		if got == nil || got.Status != HealthStatusUnhealthy {
			t.Fatalf("component status = %v, want unhealthy", got)
		}
		if len(f.evicted) != 0 {
			t.Fatalf("a deployed connector was evicted: %v", f.evicted)
		}
	})

	t.Run("unlisted is forgotten", func(t *testing.T) {
		f := newCensusFixture(t, stubTransport{status: http.StatusServiceUnavailable})
		// A host running something, just not this. An empty list is refused as
		// non-authoritative, and rightly — see TestTheCensusRefusesAnEmptyList.
		f.serveCensus(t, map[string]string{"rsync-test-something-else-v1-0-0-mcp": "running"})

		// Both tables, in this order. The issue row is the half that had no deletion
		// path at all for this lane, which is why prod accumulated 18 connector_down
		// issues against connectors it had never deployed.
		f.mock.ExpectExec("DELETE FROM sentinel_component_health").
			WithArgs(f.componentID).WillReturnResult(sqlmock.NewResult(0, 1))
		f.mock.ExpectExec("DELETE FROM sentinel_active_issues").
			WithArgs(f.componentID).WillReturnResult(sqlmock.NewResult(0, 1))

		f.monitor.checkMCPConnectorHealth()

		// No INSERT is expected, so writing the row would fail here as an unexpected
		// Exec — the regression itself.
		if err := f.mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("an undeployed connector was not forgotten from both tables: %v", err)
		}
		if got := f.monitor.componentHealth[f.componentID]; got != nil {
			t.Fatalf("undeployed connector left a component behind: %+v", got)
		}
		// Without this the Agent keeps the issue, handleDetectedIssue takes its
		// "already active" early return next time, and the deleted row never returns.
		if len(f.evicted) != 1 || len(f.evicted[0]) != 1 || f.evicted[0][0] != f.componentID {
			t.Fatalf("eviction hook got %v, want one call with [%s]", f.evicted, f.componentID)
		}
	})
}

// TestAStoppedContainerIsAFaultNotAnAbsence covers the case name resolution can never
// get right. Docker withdraws a stopped container's DNS record, so the name stops
// resolving while the container is still there — indistinguishable, to a resolver, from
// a connector that was never deployed. The probe here returns exactly that NXDOMAIN;
// the census overrules it, because `all=true` can see the container.
func TestAStoppedContainerIsAFaultNotAnAbsence(t *testing.T) {
	f := newCensusFixture(t, stubTransport{err: nxdomain("rsync-test-acme-blob-v2-1-0-mcp")})
	f.serveCensus(t, map[string]string{f.container: "exited"})

	f.mock.ExpectExec("INSERT INTO sentinel_component_health").
		WillReturnResult(sqlmock.NewResult(1, 1))

	f.monitor.checkMCPConnectorHealth()

	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("a stopped connector container was not reported as a fault: %v", err)
	}
	if got := f.monitor.componentHealth[f.componentID]; got == nil || got.Status != HealthStatusUnhealthy {
		t.Fatalf("component status = %v, want unhealthy", got)
	}
	if len(f.evicted) != 0 {
		t.Fatalf("a deployed-but-stopped connector was evicted: %v", f.evicted)
	}
}

// TestAnUnansweredResolverWritesNothing is the prod regression, reproduced.
//
// With no census — which is every deployment running no socket proxy, including the
// quickstart compose — the sweep is back on the resolver, and the resolver on prod's
// host does not answer conclusively for a name that does not exist: that VM's
// /etc/resolv.conf carries `search ....internal` with ndots:0, Docker's embedded
// resolver forwards the search-domain variants upstream, and a non-NXDOMAIN answer
// from any of them leaves *net.DNSError.IsNotFound false. The old code read that as a
// fault and wrote a row. There is nothing here to justify a row either way, so the
// sweep must write nothing at all — no INSERT, and no DELETE either, because "the
// resolver said nothing" is not evidence that an existing row is wrong.
func TestAnUnansweredResolverWritesNothing(t *testing.T) {
	servfail := stubTransport{err: &net.DNSError{Err: "server misbehaving", Name: "x"}}

	t.Run("nothing conclusive, nothing written", func(t *testing.T) {
		f := newCensusFixture(t, servfail)
		// No MCP_DOCKER_API_URL: errNoDockerAPI, the resolver fallback.

		f.monitor.checkMCPConnectorHealth()

		// Zero expectations are set, so ANY statement is an unexpected Exec.
		if err := f.mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("an inconclusive probe reached the database: %v", err)
		}
		if n := len(f.monitor.componentHealth); n != 0 {
			t.Fatalf("componentHealth has %d entries, want 0: %v", n, componentIDs(f.monitor.componentHealth))
		}
		// Not recorded as absent either: an unknown must not consume the
		// once-per-transition eviction that a real absence is entitled to.
		if n := len(f.monitor.absentConnectors); n != 0 {
			t.Fatalf("absentConnectors has %d entries, want 0", n)
		}
		if len(f.evicted) != 0 {
			t.Fatalf("an inconclusive probe evicted a component: %v", f.evicted)
		}
	})

	// The control. Same missing census, same fallback path, and a failure the probe CAN
	// interpret — so a row is written. Without this, the assertions above would also
	// pass if the fallback stopped reporting faults altogether.
	t.Run("control: a conclusive failure still writes", func(t *testing.T) {
		f := newCensusFixture(t, stubTransport{status: http.StatusBadGateway})

		f.mock.ExpectExec("INSERT INTO sentinel_component_health").
			WillReturnResult(sqlmock.NewResult(1, 1))

		f.monitor.checkMCPConnectorHealth()

		if err := f.mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("the resolver fallback stopped reporting real faults: %v", err)
		}
	})
}

// TestDockerContainerCensusReadsTheProxy covers the census fetch on its own: the shapes
// the proxy can answer with, and the two that must NOT be mistaken for "this host runs
// nothing" — because that mistake evicts every component in one sweep.
func TestDockerContainerCensusReadsTheProxy(t *testing.T) {
	h := NewHealthMonitor(nil, nil, &SentinelConfig{}, nil)

	t.Run("names are normalised and state is kept", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// A container can carry several names; each one has to answer.
			fmt.Fprint(w, `[
				{"Names":["/rsync-ai-postgresql-v1-0-0-mcp"],"State":"running"},
				{"Names":["/rsync-ai-mongodb-v1-2-0-mcp","/alias-for-mongodb"],"State":"exited"}
			]`)
		}))
		defer srv.Close()
		t.Setenv("MCP_DOCKER_API_URL", srv.URL+"/") // a trailing slash must not double up

		census, err := h.dockerContainerCensus(context.Background())
		if err != nil {
			t.Fatalf("census: %v", err)
		}
		want := containerCensus{
			"rsync-ai-postgresql-v1-0-0-mcp": "running",
			"rsync-ai-mongodb-v1-2-0-mcp":    "exited",
			"alias-for-mongodb":              "exited",
		}
		if len(census) != len(want) {
			t.Fatalf("census = %v, want %v", census, want)
		}
		for name, state := range want {
			if census[name] != state {
				t.Errorf("census[%q] = %q, want %q", name, census[name], state)
			}
		}
	})

	t.Run("an unset URL is not a fault", func(t *testing.T) {
		t.Setenv("MCP_DOCKER_API_URL", "")
		_, err := h.dockerContainerCensus(context.Background())
		// errors.Is, because the sweep logs this one at Debug and every other census
		// error at Warn: a deployment that runs no socket proxy is not broken.
		if !errors.Is(err, errNoDockerAPI) {
			t.Fatalf("err = %v, want errNoDockerAPI", err)
		}
	})

	t.Run("a 403 is an error, not an empty host", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// What the proxy answers when CONTAINERS is not in its allowlist.
			w.WriteHeader(http.StatusForbidden)
		}))
		defer srv.Close()
		t.Setenv("MCP_DOCKER_API_URL", srv.URL)

		census, err := h.dockerContainerCensus(context.Background())
		if err == nil {
			t.Fatalf("a 403 produced census %v and no error; the sweep would read it as a host running nothing", census)
		}
	})

	t.Run("an empty list is refused", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `[]`)
		}))
		defer srv.Close()
		t.Setenv("MCP_DOCKER_API_URL", srv.URL)

		// A host with no containers at all cannot be the host running this process, so
		// the answer is not authoritative. Accepting it would evict every component.
		if _, err := h.dockerContainerCensus(context.Background()); err == nil {
			t.Fatal("an empty container list was accepted as authoritative")
		}
	})

	t.Run("a body that is not a container list is an error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `{"message":"page not found"}`)
		}))
		defer srv.Close()
		t.Setenv("MCP_DOCKER_API_URL", srv.URL)

		if _, err := h.dockerContainerCensus(context.Background()); err == nil {
			t.Fatal("a non-list 200 body was accepted as a census")
		}
	})
}

// TestForgetIssuesForComponentsIsScoped guards the other half of the eviction: the
// Agent's in-memory issue map. handleDetectedIssue returns early for an issue it is
// already holding, so an entry left behind after the row is deleted suppresses the
// re-insert permanently — a later real fault on that component would never reappear in
// the table the UI reads. Equally, it must drop ONLY the evicted component's issues.
func TestForgetIssuesForComponentsIsScoped(t *testing.T) {
	a := &Agent{activeIssues: map[string]*Issue{
		// connector_down is the type prod actually accumulated 18 of.
		"issue-1": {ID: "issue-1", ComponentID: "mcp_connector:gone", Type: IssueTypeConnectorDown},
		"issue-2": {ID: "issue-2", ComponentID: "mcp_connector:gone", Type: IssueTypeMissingHeartbeat},
		"issue-3": {ID: "issue-3", ComponentID: "mcp_connector:still-here", Type: IssueTypeConnectorDown},
	}}

	a.forgetIssuesForComponents([]string{"mcp_connector:gone"})

	if _, ok := a.activeIssues["issue-3"]; !ok {
		t.Error("an issue against a component that still exists was dropped")
	}
	for _, id := range []string{"issue-1", "issue-2"} {
		if _, ok := a.activeIssues[id]; ok {
			t.Errorf("%s survived its component's eviction; its row is gone but the map still suppresses a re-insert", id)
		}
	}
}
