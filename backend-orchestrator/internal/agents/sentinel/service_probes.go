package sentinel

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

// serviceFailureThreshold is how many consecutive failed probes it takes before a core
// service is recorded unhealthy — the status the IssueDetector turns into an
// INFRASTRUCTURE_DOWN alert for every admin. At the 30s infrastructure tick that is
// about 90 seconds of an outage. One refused dial during a container restart or a
// rolling deploy is not an outage, and paging every admin for it teaches them to ignore
// the page. The misses before the threshold are recorded degraded, so /admin/health
// still shows the service wobbling while nobody is paged.
const serviceFailureThreshold = 3

// serviceProbeTimeout bounds one probe. The loop runs every probe in turn on a single
// goroutine, so a hung dependency must not delay the others past the next tick.
const serviceProbeTimeout = 5 * time.Second

// serviceProbe checks one core rsync service this process depends on but does not
// own: the services whose outage stops pipelines without any pipeline-scoped sentinel
// noticing, because every pipeline just goes quiet at once.
//
// failures is touched only by checkServiceProbes, which runs on the single
// monitorInfrastructure goroutine, so it needs no lock.
type serviceProbe struct {
	componentID string
	// targetKey/target are recorded as the component's metadata so /admin/health can
	// say where the probe looked. target never carries credentials: URLs have their
	// userinfo stripped, and the Redis password is a separate setting.
	targetKey string
	target    string
	check     func(ctx context.Context) error
	close     func() error
	failures  int
}

// serviceProbesFromEnv builds the probes this deployment can run. Each one exists only
// when the address it needs is configured — except Redis, which the orchestrator cannot
// start without (main.go exits when the correlation client cannot connect), so it is
// always probed at the address the correlation client uses.
//
// The rest are opt-in by address on purpose. The probed services are not in every
// deployment shape — the Helm chart renders no connector-deployer and no Service in
// front of the temporal-adapter — and a probe that falls back to a compose service name
// on a host that has no such service would page every admin about a component that was
// never there. That is the trap infrastructure:kafka-connect is already in on a Helm
// install with CDC disabled.
func serviceProbesFromEnv(getenv func(string) string) []*serviceProbe {
	httpClient := &http.Client{Timeout: serviceProbeTimeout}
	probes := []*serviceProbe{redisServiceProbe(getenv)}

	if addr := strings.TrimSpace(getenv("TEMPORAL_ADDRESS")); addr != "" {
		probes = append(probes, tcpServiceProbe("infrastructure:temporal", addr))
	}
	// The temporal-adapter has no /health; its ops server answers GET /version
	// whenever the process is up, which is the question being asked.
	if base := strings.TrimSpace(getenv("SENTINEL_PROBE_TEMPORAL_ADAPTER_URL")); base != "" {
		probes = append(probes, httpServiceProbe(httpClient, "infrastructure:temporal-adapter", base, "/version"))
	}
	// /healthz answers 503 when the deployer cannot reach the Docker daemon. That is a
	// real outage — it cannot start a connector — so a 503 counts as down.
	if base := strings.TrimSpace(getenv("SENTINEL_PROBE_CONNECTOR_DEPLOYER_URL")); base != "" {
		probes = append(probes, httpServiceProbe(httpClient, "infrastructure:connector-deployer", base, "/healthz"))
	}
	// The address the orchestrator already uses to (re)deploy connectors
	// (server_manager.go tryDeployConnectorContainer), so what is probed is what a
	// connector restart would call.
	if base := strings.TrimSpace(getenv("TOOL_GENERATOR_URL")); base != "" {
		probes = append(probes, httpServiceProbe(httpClient, "infrastructure:tool-generator", base, "/health"))
	}
	if base := strings.TrimSpace(getenv("SENTINEL_PROBE_LLM_SERVICE_URL")); base != "" {
		probes = append(probes, httpServiceProbe(httpClient, "infrastructure:llm-service", base, "/health"))
	}
	return probes
}

// redisServiceProbe pings Redis at the address the orchestrator's correlation client
// resolves (workers/correlation_router.go InitCorrelationClient): REDIS_ADDRESS, then
// REDIS_ADDR, then the compose default. Probing any other address would report on a
// Redis the workers do not use.
func redisServiceProbe(getenv func(string) string) *serviceProbe {
	addr := strings.TrimSpace(getenv("REDIS_ADDRESS"))
	if addr == "" {
		addr = strings.TrimSpace(getenv("REDIS_ADDR"))
	}
	if addr == "" {
		addr = "redis:6379"
	}
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     getenv("REDIS_PASSWORD"),
		DialTimeout:  serviceProbeTimeout,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		// One connection is all a 30s ping needs; the default pool is sized for a
		// request path.
		PoolSize: 1,
	})
	return &serviceProbe{
		componentID: "infrastructure:redis",
		targetKey:   "address",
		target:      addr,
		check: func(ctx context.Context) error {
			return client.Ping(ctx).Err()
		},
		close: client.Close,
	}
}

func tcpServiceProbe(componentID, addr string) *serviceProbe {
	return &serviceProbe{
		componentID: componentID,
		targetKey:   "address",
		target:      addr,
		check: func(ctx context.Context) error {
			var d net.Dialer
			conn, err := d.DialContext(ctx, "tcp", addr)
			if err != nil {
				return err
			}
			return conn.Close()
		},
	}
}

func httpServiceProbe(client *http.Client, componentID, base, path string) *serviceProbe {
	probeURL := strings.TrimRight(base, "/") + path
	return &serviceProbe{
		componentID: componentID,
		targetKey:   "url",
		target:      redactURLUserinfo(probeURL),
		check: func(ctx context.Context) error {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
			if err != nil {
				return err
			}
			resp, err := client.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode > 299 {
				return fmt.Errorf("unexpected status: %s", resp.Status)
			}
			return nil
		},
	}
}

// redactURLUserinfo drops any user:password@ from a URL before it is stored where the
// admin UI and the monitoring API can read it.
func redactURLUserinfo(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		// Unparseable: store nothing that might be a secret rather than the raw text.
		return ""
	}
	u.User = nil
	return u.String()
}

// checkServiceProbes runs every configured service probe once and records each verdict.
func (h *HealthMonitor) checkServiceProbes(ctx context.Context) {
	for _, p := range h.serviceProbes {
		h.runServiceProbe(ctx, p)
	}
}

// runServiceProbe records one probe's verdict through recordInfraHealth, debounced by
// serviceFailureThreshold: healthy on success (and the count resets), degraded for the
// misses before the threshold, unhealthy from the threshold on.
func (h *HealthMonitor) runServiceProbe(ctx context.Context, p *serviceProbe) {
	probeCtx, cancel := context.WithTimeout(ctx, serviceProbeTimeout)
	err := p.check(probeCtx)
	cancel()

	metadata := map[string]interface{}{p.targetKey: p.target}
	if err == nil {
		p.failures = 0
		h.recordInfraHealth(p.componentID, HealthStatusHealthy, "", metadata)
		return
	}

	p.failures++
	if p.failures < serviceFailureThreshold {
		h.recordInfraHealth(p.componentID, HealthStatusDegraded,
			fmt.Sprintf("%s (failed %d of %d checks before it is reported down)", err, p.failures, serviceFailureThreshold),
			metadata)
		return
	}

	log.WithFields(log.Fields{
		"component_id": p.componentID,
		"failures":     p.failures,
		"error":        err.Error(),
	}).Error("Core service health check failed")
	h.recordInfraHealth(p.componentID, HealthStatusUnhealthy, err.Error(), metadata)
}

// closeServiceProbes releases whatever the probes hold open (the Redis client).
func (h *HealthMonitor) closeServiceProbes() {
	for _, p := range h.serviceProbes {
		if p.close != nil {
			_ = p.close()
		}
	}
}
