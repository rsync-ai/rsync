package sentinel

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/rsync-ai/backend-orchestrator/internal/connectorpaths"
	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// repoRoot walks up from this package to the repository root.
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	// internal/agents/sentinel -> internal/agents -> internal -> backend-orchestrator -> repo
	root := filepath.Join(wd, "..", "..", "..", "..")
	abs, err := filepath.Abs(root)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	return abs
}

// TestEveryGeneratedConnectorContainerIsDiscoverable is the load-bearing test.
//
// docker-compose.mcp.yml is generated from the connector tree by
// scripts/mcp_generate_compose.py, so it is a checked-in, independently-produced
// answer to "which MCP containers exist". The health check has to enumerate that
// same set from the same tree. Comparing the two turns the generated file into a
// free detector: break the tree walk the way #807 broke the registry's, and the
// container names simply stop being found here.
//
// Direction matters. Every generated container MUST be discoverable — a name the
// walker misses is a container the sentinel is blind to. The reverse is allowed:
// a connector added to the tree but not yet regenerated into the compose file is
// a stale compose file, not a broken walker.
func TestEveryGeneratedConnectorContainerIsDiscoverable(t *testing.T) {
	root := repoRoot(t)
	composePath := filepath.Join(root, "docker-compose.mcp.yml")
	f, err := os.Open(composePath)
	if err != nil {
		t.Fatalf("open %s: %v", composePath, err)
	}
	defer f.Close()

	generated := map[string]bool{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if name, ok := strings.CutPrefix(line, "container_name:"); ok {
			generated[strings.TrimSpace(name)] = true
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan compose: %v", err)
	}
	// An empty expectation set would make this test pass against a walker that
	// finds nothing at all.
	if len(generated) == 0 {
		t.Fatalf("parsed 0 container_name entries from %s — the test, not the code, is broken", composePath)
	}

	tree := filepath.Join(root, "shared", "mcp-connectors")
	roots := connectorpaths.IterConnectorRoots(tree)
	if len(roots) == 0 {
		t.Fatalf("IterConnectorRoots(%s) found 0 connectors", tree)
	}

	discovered := map[string]bool{}
	for _, cr := range roots {
		if cr.Internal || !cr.HasDockerfile {
			continue
		}
		if name := mcp.MCPContainerName(cr.ID, cr.CurrentVersion); name != "" {
			discovered[name] = true
		}
	}

	var missing []string
	for name := range generated {
		if !discovered[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("%d generated MCP containers are invisible to the connector health check: %v",
			len(missing), missing)
	}
	t.Logf("all %d generated MCP containers are discoverable from the tree", len(generated))
}

// stubTransport answers every request with a canned response or error, so the
// probe's real code — URL construction, the client call, and the error
// classification — runs without a Docker network to resolve names on.
type stubTransport struct {
	status int
	err    error
}

func (s stubTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &http.Response{
		StatusCode: s.status,
		Body:       http.NoBody,
		Request:    req,
	}, nil
}

// nxdomain is what a resolver returns for a container that is not running: the
// Docker embedded DNS has no record for the name. IsNotFound is the only field
// that distinguishes it from a resolver that failed to answer.
func nxdomain(name string) error {
	return &net.DNSError{Err: "no such host", Name: name, IsNotFound: true}
}

// TestCheckConnectorReachabilityClassifiesTheFailure is the discriminating test.
//
// The probe used to return a bool, so "this host never deployed this connector"
// and "this connector is deployed and broken" came back identical and were both
// written down as unhealthy — the whole of
// KI-HEALTH-COUNTS-UNDEPLOYED-CONNECTORS. Splitting them is only worth anything
// if the split is drawn in the right place, so each row below is a failure that
// must land on a specific side of it. The three that matter most are the DNS ones,
// which all arrive as the same error type and land on three different sides.
//
// The timeout row asserted connectorFailing until 2026-09-23, on the reasoning that
// a component "keeps its row and stays a fault" when the resolver does not answer.
// Prod disproved it. That host's containers inherit the VM's resolv.conf —
// `search us-central1-a.c.<project>.internal c.<project>.internal google.internal`,
// `ndots:0` — and Docker's resolver forwards those variants upstream, where an
// unknown name does not come back as a clean NXDOMAIN. So IsNotFound was false for
// every connector the host had never deployed, and "stays a fault" produced 18
// faults for containers that did not exist. A probe that cannot tell absence from
// fault has to say so; connectorUnknown is that answer, and the sweep writes nothing
// down for it.
func TestCheckConnectorReachabilityClassifiesTheFailure(t *testing.T) {
	cases := []struct {
		name      string
		transport stubTransport
		port      int
		want      connectorReachability
	}{
		{"200 is reachable", stubTransport{status: http.StatusOK}, 8000, connectorReachable},
		{"503 is a fault, not an absence", stubTransport{status: http.StatusServiceUnavailable}, 8000, connectorFailing},
		{"NXDOMAIN is not deployed", stubTransport{err: nxdomain("rsync-ai-acme-v1-0-0-mcp")}, 8000, connectorAbsent},
		{
			// Same error type as the row above, and neither of its verdicts: a resolver
			// that did not answer says nothing about whether the container exists, so
			// this is not an absence and it is not a fault either.
			"a DNS timeout is neither",
			stubTransport{err: &net.DNSError{Err: "i/o timeout", Name: "x", IsTimeout: true}},
			8000, connectorUnknown,
		},
		{
			// The shape prod actually produced: a search-domain traversal that ends in
			// SERVFAIL rather than NXDOMAIN. IsNotFound is false and IsTimeout is false,
			// so the old code had no branch for it at all and it fell through to a fault.
			"a SERVFAIL is neither",
			stubTransport{err: &net.DNSError{Err: "server misbehaving", Name: "x"}},
			8000, connectorUnknown,
		},
		{
			// Something accepted the name and refused the connection: it is there.
			// This also proves the errors.As walk does not match any *net.OpError.
			"connection refused is a fault",
			stubTransport{err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}},
			8000, connectorFailing,
		},
		{"port 0 is a fault", stubTransport{status: http.StatusOK}, 0, connectorFailing},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := NewHealthMonitor(nil, nil, &SentinelConfig{}, nil)
			h.httpClient.Transport = c.transport
			if got := h.checkConnectorReachability(context.Background(), "acme", c.port); got != c.want {
				t.Errorf("reachability = %v, want %v", got, c.want)
			}
		})
	}
}

// TestCheckMCPConnectorHealthReadsTheTreeNotTheDatabase is a provenance test.
//
// The bug it guards was not a wrong value: it was a wrong SOURCE. The check
// selected from connector_instances, a table with four indexes, this one reader
// and no writer, so it iterated zero rows on every tick and reported nothing,
// silently. sqlmock has no expectation for a SELECT, so reintroducing the query
// makes it error; the original code returned early on that error, which leaves
// the one Exec expectation unmet and fails here. Verified by mutation, not
// assumed: restoring the connector_instances query fails this test.
//
// The verdict it asserts changed with KI-HEALTH-COUNTS-UNDEPLOYED-CONNECTORS.
// This connector's container does not resolve, so it is not deployed here, and
// the component is dropped rather than written down as unhealthy — but it is
// still the tree that has to produce the name being dropped.
func TestCheckMCPConnectorHealthReadsTheTreeNotTheDatabase(t *testing.T) {
	tree := t.TempDir()
	writeTestConnector(t, filepath.Join(tree, "public", "storage", "acme-blob"), "v2.1.0",
		`{"id":"acme-blob"}`, true)
	// Internal plumbing and Dockerfile-less roots never become containers, so
	// probing them would manufacture a permanently-unhealthy component.
	writeTestConnector(t, filepath.Join(tree, "internal", "debezium"), "v1.0.0",
		`{"id":"debezium","internal":true}`, true)
	writeTestConnector(t, filepath.Join(tree, "public", "no-image"), "v1.0.0",
		`{"id":"no-image"}`, false)

	t.Setenv("MCP_CONNECTORS_PATH", tree)
	t.Setenv("STACK_PREFIX", "rsync-test")

	dbConn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer dbConn.Close()

	// A row this host cannot justify is deleted, not updated. Prod carried 18 of
	// these, refreshed every 30 seconds, each with error_count = 0 and no
	// last_error to explain itself.
	mock.ExpectExec("DELETE FROM sentinel_component_health").
		WithArgs("mcp_connector:rsync-test-acme-blob-v2-1-0-mcp").
		WillReturnResult(sqlmock.NewResult(0, 1))

	h := NewHealthMonitor(nil, dbConn, &SentinelConfig{}, nil)
	h.ctx = context.Background()
	h.httpClient.Transport = stubTransport{err: nxdomain("rsync-test-acme-blob-v2-1-0-mcp")}

	h.checkMCPConnectorHealth()

	// No INSERT is expected, so an unhealthy write would fail here as an
	// unexpected Exec — the regression this KI was filed for.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("connector health did not evict exactly the tree-derived component: %v", err)
	}

	if len(h.componentHealth) != 0 {
		t.Errorf("undeployed connectors were recorded as components: %v", componentIDs(h.componentHealth))
	}
	for id := range h.componentHealth {
		if strings.Contains(id, "debezium") {
			t.Errorf("internal connector %s was probed; it has no generated container", id)
		}
		if strings.Contains(id, "no-image") {
			t.Errorf("Dockerfile-less root %s was probed; it has no generated container", id)
		}
	}
}

// TestCheckMCPConnectorHealthPersistsADeployedConnectorsVerdict covers the two
// verdicts a deployed connector can have. Without it the eviction test above
// would pass against a check that evicted everything, which is the same blindness
// as the connector_instances query in a new costume.
func TestCheckMCPConnectorHealthPersistsADeployedConnectorsVerdict(t *testing.T) {
	const componentID = "mcp_connector:rsync-test-acme-blob-v2-1-0-mcp"

	cases := []struct {
		name       string
		transport  stubTransport
		wantStatus HealthStatus
	}{
		{"answers 200", stubTransport{status: http.StatusOK}, HealthStatusHealthy},
		{
			// There is a container there; it is broken. This is the verdict the old
			// bool probe gave to all 21 candidate connectors, deployed or not.
			"deployed but refusing connections",
			stubTransport{err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}},
			HealthStatusUnhealthy,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := t.TempDir()
			writeTestConnector(t, filepath.Join(tree, "public", "storage", "acme-blob"), "v2.1.0",
				`{"id":"acme-blob"}`, true)
			t.Setenv("MCP_CONNECTORS_PATH", tree)
			t.Setenv("STACK_PREFIX", "rsync-test")

			dbConn, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer dbConn.Close()

			// An unpersisted verdict and an unmade check are the same empty table.
			mock.ExpectExec("INSERT INTO sentinel_component_health").
				WithArgs(
					componentID,
					ComponentTypeMCPConnector,
					c.wantStatus,
					sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
					sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
				).
				WillReturnResult(sqlmock.NewResult(0, 1))

			h := NewHealthMonitor(nil, dbConn, &SentinelConfig{}, nil)
			h.ctx = context.Background()
			h.httpClient.Transport = c.transport

			h.checkMCPConnectorHealth()

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("connector health did not persist exactly one %s verdict: %v", c.wantStatus, err)
			}

			got, ok := h.componentHealth[componentID]
			if !ok {
				t.Fatalf("expected the tree-derived connector in componentHealth, got %v",
					componentIDs(h.componentHealth))
			}
			if got.Status != c.wantStatus {
				t.Errorf("in-memory status = %v, want %v", got.Status, c.wantStatus)
			}
			// The old query selected a `port` column from a table nobody wrote, so even
			// a row it found would have been probed on port 0. There is nothing to look
			// up: the generator gives every connector PORT=8000.
			// The literal, not the constant: comparing the constant to itself would let
			// a mutation of the constant pass unnoticed. 8000 is the number the compose
			// generator writes into every service.
			if got.Metadata["port"] != 8000 {
				t.Errorf("probed port recorded as %v, want 8000", got.Metadata["port"])
			}
			if got.Metadata["connector_id"] != "acme-blob" || got.Metadata["connector_version"] != "v2.1.0" {
				t.Errorf("connector identity not recorded: %v", got.Metadata)
			}
			if got.LastHeartbeat.IsZero() {
				t.Error("last_heartbeat left zero; the column is NOT NULL and this check is the heartbeat")
			}
		})
	}
}

// TestUndeployedConnectorsAreEvictedOncePerName guards the cost of the fix. The
// sweep runs every 30 seconds, and a host running 7 of 21 connectors has 14
// names that will never resolve; issuing a DELETE for each of them on every tick
// would be 40,000 pointless statements a day.
//
// Asserted on forgetConnector's return value rather than on a sqlmock call count,
// because deleteHealthFromDB logs its errors and moves on: an unexpected Exec
// against the mock is swallowed, so a DELETE-per-tick regression is invisible from
// there. Confirmed by mutation — inverting the guard below fails this test and
// passes a call-count version of it.
func TestUndeployedConnectorsAreEvictedOncePerName(t *testing.T) {
	const componentID = "mcp_connector:rsync-test-acme-blob-v2-1-0-mcp"

	h := NewHealthMonitor(nil, nil, &SentinelConfig{}, nil)
	h.ctx = context.Background()

	// First sighting: there is a row out there to delete. This has to hold even
	// though this process has no entry for the component — the rows outlive the
	// process, so a restart is what clears rows an older build wrote.
	if got := h.forgetConnector(componentID); got != componentID {
		t.Fatalf("first eviction returned %q, want %q — a stale row would never be deleted", got, componentID)
	}
	if got := h.forgetConnector(componentID); got != "" {
		t.Errorf("second eviction returned %q, want \"\" — one DELETE per 30s tick, forever", got)
	}
	if got := h.forgetConnector(componentID); got != "" {
		t.Errorf("third eviction returned %q, want \"\"", got)
	}

	// Deployed on demand: when the container finally appears, the component must be
	// recordable again, and a later disappearance must be evictable again.
	h.recordConnectorVerdict(componentID, "acme-blob", "v2.1.0", true)
	if _, stillAbsent := h.absentConnectors[componentID]; stillAbsent {
		t.Error("a connector that answered is still marked absent; it would never be evicted again")
	}
	if got := h.forgetConnector(componentID); got != componentID {
		t.Errorf("re-eviction after the connector came back returned %q, want %q", got, componentID)
	}
}

// TestASweepOfUndeployedConnectorsLeavesNoComponents ties the eviction contract
// above back to the sweep, which is the only caller of it.
func TestASweepOfUndeployedConnectorsLeavesNoComponents(t *testing.T) {
	const componentID = "mcp_connector:rsync-test-acme-blob-v2-1-0-mcp"

	tree := t.TempDir()
	writeTestConnector(t, filepath.Join(tree, "public", "storage", "acme-blob"), "v2.1.0",
		`{"id":"acme-blob"}`, true)
	t.Setenv("MCP_CONNECTORS_PATH", tree)
	t.Setenv("STACK_PREFIX", "rsync-test")

	h := NewHealthMonitor(nil, nil, &SentinelConfig{}, nil)
	h.ctx = context.Background()
	h.httpClient.Transport = stubTransport{err: nxdomain("rsync-test-acme-blob-v2-1-0-mcp")}

	// Seed the component the way a previous build did, so this exercises the
	// transition that clears prod's 18 rows rather than just never adding one.
	h.recordConnectorVerdict(componentID, "acme-blob", "v2.1.0", false)

	h.checkMCPConnectorHealth()
	h.checkMCPConnectorHealth()

	if len(h.componentHealth) != 0 {
		t.Errorf("sweep left undeployed connectors as components: %v", componentIDs(h.componentHealth))
	}
	if _, marked := h.absentConnectors[componentID]; !marked {
		t.Errorf("sweep did not reach the eviction path for %s", componentID)
	}
}

// TestCheckConnectorReachabilityAcceptsALiveConnector runs the probe over a real
// socket and the real transport. Every other test here stubs the transport, so
// without this one they could all be agreeing about a client that never works.
func TestCheckConnectorReachabilityAcceptsALiveConnector(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}

	h := NewHealthMonitor(nil, nil, &SentinelConfig{}, nil)
	h.ctx = context.Background()

	if got := h.checkConnectorReachability(context.Background(), u.Hostname(), port); got != connectorReachable {
		t.Fatalf("a connector serving 200 on /health was reported %v", got)
	}
	if got := h.checkConnectorReachability(context.Background(), u.Hostname(), 0); got == connectorReachable {
		t.Error("port 0 must not be reported reachable")
	}
}

// TestMCPContainerNameMatchesTheGenerator pins the one name format the compose
// generator emits. A drift here is silent: every probe would 404 into
// "unhealthy" for connectors that are running fine.
func TestMCPContainerNameMatchesTheGenerator(t *testing.T) {
	t.Setenv("STACK_PREFIX", "")
	cases := []struct{ id, version, want string }{
		{"postgresql", "v1.0.0", "rsync-ai-postgresql-v1-0-0-mcp"},
		{"aws_s3", "v1.2.3", "rsync-ai-aws-s3-v1-2-3-mcp"},
		{"petstore", "1.0.3", "rsync-ai-petstore-v1-0-3-mcp"},
		{"", "v1.0.0", ""},
		{"postgresql", "latest", ""},
		{"postgresql", "", ""},
	}
	for _, c := range cases {
		if got := mcp.MCPContainerName(c.id, c.version); got != c.want {
			t.Errorf("MCPContainerName(%q, %q) = %q, want %q", c.id, c.version, got, c.want)
		}
	}

	t.Setenv("STACK_PREFIX", "rsync-ci")
	if got := mcp.MCPContainerName("postgresql", "v1.0.0"); got != "rsync-ci-postgresql-v1-0-0-mcp" {
		t.Errorf("STACK_PREFIX ignored: got %q", got)
	}
}

func writeTestConnector(t *testing.T, root, cv, metadata string, withDockerfile bool) {
	t.Helper()
	verDir := filepath.Join(root, "versions", cv)
	if err := os.MkdirAll(verDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "latest.json"), []byte(`{"current_version":"`+cv+`"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(verDir, "metadata.json"), []byte(metadata), 0o644); err != nil {
		t.Fatal(err)
	}
	if withDockerfile {
		if err := os.WriteFile(filepath.Join(verDir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func componentIDs(m map[string]*ComponentHealth) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
