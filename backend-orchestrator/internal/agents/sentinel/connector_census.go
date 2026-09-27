package sentinel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// containerCensus maps a container name on this host to Docker's State string
// ("running", "exited", "created", …). A name that is absent from the map is a
// container that does not exist here at all — which is the distinction the whole
// of checkMCPConnectorHealth turns on.
type containerCensus map[string]string

// errNoDockerAPI is not a failure: it is this deployment declining to expose the
// Docker API, which the quickstart/self-host compose does by simply not running a
// socket proxy. The sweep falls back to the resolver for it rather than logging a
// fault every 30 seconds.
var errNoDockerAPI = errors.New("sentinel: MCP_DOCKER_API_URL not set; no container census available")

// dockerCensusTimeout is deliberately longer than the 3s per-connector health probe.
// One census serves a whole sweep — 21 connectors on this repo's tree — so paying up
// to 5s once is cheaper than the 21 probes it replaces, and a census that times out
// costs the sweep its authoritative answer.
const dockerCensusTimeout = 5 * time.Second

// dockerContainerCensus asks the read-only docker-socket-proxy which containers
// exist on this host.
//
// WHY THIS REPLACED A DNS PROBE. The absence test used to be "the container name
// does not resolve", classified from *net.DNSError.IsNotFound — a clean NXDOMAIN.
// That inference is only sound on a resolver that answers an unknown single-label
// name with NXDOMAIN and nothing else, and it is not sound on this repo's own
// production host: a GCP VM's /etc/resolv.conf carries
//
//	search us-central1-a.c.<project>.internal c.<project>.internal google.internal
//	options ndots:0
//
// and Docker's embedded resolver forwards the search-domain variants to the host
// resolver. One variant answering with anything other than NXDOMAIN — SERVFAIL, a
// timeout, a refusal — is enough to leave IsNotFound false, and the old code read
// that as "deployed and broken". The result was every connector this repo can build
// but the host never deployed being written down as a fault: 18 of them on prod,
// each one also filed as a connector_down issue once the detector lane was wired.
//
// The Docker API needs no resolver semantics. A name is either in the list of
// containers on this host or it is not, and `all=true` additionally distinguishes
// "exists but is stopped" — a genuine fault — from "was never deployed here", which
// the resolver could not do at all, because Docker withdraws a stopped container's
// DNS record.
//
// Read-only by construction: the proxy is started with CONTAINERS=1 POST=0, so this
// process can GET /containers/json and nothing else. Verified against the live proxy
// — /containers/json?all=true answers 200, /images/json and POST /containers/create
// both answer 403 — so the orchestrator gains container visibility without a
// docker.sock mount and without root.
func (h *HealthMonitor) dockerContainerCensus(ctx context.Context) (containerCensus, error) {
	base := strings.TrimSpace(os.Getenv("MCP_DOCKER_API_URL"))
	if base == "" {
		return nil, errNoDockerAPI
	}
	base = strings.TrimRight(base, "/")

	ctx, cancel := context.WithTimeout(ctx, dockerCensusTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", base+"/containers/json?all=true", nil)
	if err != nil {
		return nil, err
	}

	resp, err := h.dockerClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// A 403 here is the proxy's allowlist, not a transient fault: it means
		// CONTAINERS is not set on the proxy this URL points at.
		return nil, fmt.Errorf("docker API GET /containers/json: status=%d", resp.StatusCode)
	}

	// Only the two fields the sweep reads. Decoding the whole container object would
	// pull in ~80KB per sweep of ports, mounts and labels nothing here looks at.
	var containers []struct {
		Names []string `json:"Names"`
		State string   `json:"State"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&containers); err != nil {
		return nil, err
	}

	// An empty list is a valid answer in principle, but not one this can act on: a
	// host with no containers at all cannot be the host running this process. Treating
	// it as authoritative would evict every component on the next sweep, so it is a
	// fault and the sweep keeps what it has.
	if len(containers) == 0 {
		return nil, errors.New("docker API returned no containers; refusing to treat as authoritative")
	}

	census := make(containerCensus, len(containers))
	for _, c := range containers {
		for _, n := range c.Names {
			// The API reports names with a leading slash ("/rsync-ai-…"), while
			// mcp.MCPContainerName builds them without one.
			census[strings.TrimPrefix(n, "/")] = c.State
		}
	}
	return census, nil
}
