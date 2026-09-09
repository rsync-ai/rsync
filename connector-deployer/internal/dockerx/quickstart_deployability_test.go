package dockerx

// Cross-file guard: every connector this package refuses to build must be started
// by SOMEBODY on the stack that ships to self-hosters.
//
// composeManagedConnectors is a claim about a different file: "docker compose
// already runs these six, so never build them here." That claim was true only of
// docker-compose.mcp.yml, the dev/cloud compose. docker-compose.quickstart.yml —
// the entire self-host install — pre-starts three of the six. postgresql, mysql
// and aws-s3 were therefore refused by the deployer AND started by nobody:
// undeployable by construction, behind a 409 whose remedy named a compose service
// that does not exist. api-gateway pins postgresql as the bundled demo's
// destination, so POST /demo/seed 502'd after the orchestrator's 60 s connector
// wait, on a stack whose own onboarding card advertises that demo.
//
// Neither file can see the other, and no compiler links them, so this test does.
// It derives its subjects from the real files rather than a hand-kept list: add a
// connector to composeManagedConnectors without a compose service and it lands in
// the offender set on its own.

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"
)

// repoRoot is connector-deployer/../ — this package sits at
// connector-deployer/internal/dockerx.
const repoRoot = "../../.."

var (
	// docker-compose.*.yml pre-starts a connector as
	// `container_name: rsync-ai-<id>-vX-Y-Z-mcp` (MCPContainerName()).
	composeConnectorContainer = regexp.MustCompile(`(?m)^\s*container_name:\s*rsync-ai-([a-z0-9-]+)-v\d+-\d+-\d+-mcp\s*$`)
	// api-gateway/internal/handlers/demo.go pins the demo's two halves.
	demoDestPin   = regexp.MustCompile(`demoDestinationConnector\s*=\s*"([a-z0-9-]+)"`)
	demoSourcePin = regexp.MustCompile(`demoSourceConnector\s*=\s*"([a-z0-9-]+)"`)
)

// connectorsStartedBy returns the connector ids a compose file pre-starts.
func connectorsStartedBy(t *testing.T, rel string) map[string]struct{} {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(repoRoot, rel))
	if err != nil {
		t.Fatalf("read %s: %v — this guard is about that file; do not skip it", rel, err)
	}
	out := map[string]struct{}{}
	for _, m := range composeConnectorContainer.FindAllStringSubmatch(string(body), -1) {
		out[m[1]] = struct{}{}
	}
	return out
}

// deployMissing runs Deploy for a connector whose container does not exist,
// returning the error (nil = it was built on demand).
func deployMissing(t *testing.T, connectorID string) error {
	t.Helper()
	tools := writeToolsWithLatest(t, connectorID, "v1.0.0")
	fb := &fakeBackend{snap: nil, createID: "deadbeefcafe00"}
	d := NewDeployer(fb, tools)

	req := validReq()
	req.Name = "rsync-ai-" + connectorID + "-v1-0-0-mcp" // current version → protected
	_, err := d.Deploy(context.Background(), req, testDcfg, baseOpts())
	return err
}

// The corpus this guard reads must be the real one, checked before any verdict.
func TestQuickstartCorpusIsReal(t *testing.T) {
	started := connectorsStartedBy(t, "docker-compose.quickstart.yml")
	// Four named services that are in the file today. If a rename or a rewrite of
	// the compose file breaks the parse, these fail rather than silently emptying
	// the set and turning every assertion below into a different test.
	for _, want := range []string{"minio", "debezium", "kafka-mcp-sink", "sample-data"} {
		if _, ok := started[want]; !ok {
			t.Errorf("quickstart parse found no %q container — parse is broken, got %v", want, keysOf(started))
		}
	}
	// The dev/cloud compose is the one that really does start all six; if this
	// stops being true the premise of composeManagedConnectors has moved.
	mcp := connectorsStartedBy(t, "docker-compose.mcp.yml")
	if len(mcp) < 10 {
		t.Errorf("docker-compose.mcp.yml parse found %d connectors, expected >10", len(mcp))
	}
	if len(composeManagedConnectors) == 0 {
		t.Fatal("composeManagedConnectors is empty — the offender loop below would be vacuous")
	}
}

// Every compose-managed connector the self-host compose does NOT start must be
// buildable on demand, or it can never run there at all.
func TestComposeManagedConnectorsUnstartedByQuickstartAreJITDeployable(t *testing.T) {
	started := connectorsStartedBy(t, "docker-compose.quickstart.yml")

	var unstarted []string
	for id := range composeManagedConnectors {
		if _, ok := started[id]; !ok {
			unstarted = append(unstarted, id)
		}
	}
	sort.Strings(unstarted)
	t.Logf("compose-managed but not started by quickstart: %v", unstarted)

	for _, id := range unstarted {
		if err := deployMissing(t, id); err != nil {
			t.Errorf("%s is protected from the JIT path but no quickstart service starts it, "+
				"so a self-host stack can never run it; Deploy returned %v (%v)", id, KindOf(err), err)
		}
	}
}

// The bundled demo names two connectors. Both have to be runnable on the stack
// that offers the demo, whichever mechanism gets them running.
func TestDemoPinnedConnectorsAreRunnableOnQuickstart(t *testing.T) {
	body, err := os.ReadFile(filepath.Join(repoRoot, "api-gateway/internal/handlers/demo.go"))
	if err != nil {
		t.Fatalf("read demo.go: %v — this guard is about that file; do not skip it", err)
	}
	pins := map[string]string{}
	for label, re := range map[string]*regexp.Regexp{"destination": demoDestPin, "source": demoSourcePin} {
		m := re.FindStringSubmatch(string(body))
		if m == nil {
			t.Fatalf("could not find the demo %s connector pin in demo.go — parse broken", label)
		}
		pins[label] = m[1]
	}
	t.Logf("demo pins: source=%q destination=%q", pins["source"], pins["destination"])

	started := connectorsStartedBy(t, "docker-compose.quickstart.yml")
	for label, id := range pins {
		if _, ok := started[id]; ok {
			continue // compose starts it — nothing for the deployer to do
		}
		if err := deployMissing(t, id); err != nil {
			t.Errorf("the demo's %s connector %q is neither started by docker-compose.quickstart.yml "+
				"nor deployable on demand (%v: %v) — POST /demo/seed cannot succeed on a self-host stack",
				label, id, KindOf(err), err)
		}
	}
}

func keysOf(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
