// Command deployer is the connector-deployer HTTP service: the small, trusted,
// no-LLM microservice that holds /var/run/docker.sock and builds+runs JIT connector
// containers on behalf of tool-generator (which has dropped the socket). The security
// property lives in internal/spec (allow-by-construction HostConfig); this binary
// wires config → docker client → HTTP server. See CONTRACT.md.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/docker/docker/client"

	"github.com/rsync-ai/connector-deployer/internal/config"
	"github.com/rsync-ai/connector-deployer/internal/dockerx"
	"github.com/rsync-ai/connector-deployer/internal/server"
)

func main() {
	cfg := config.Load()
	log := newLogger(cfg)

	cli, err := newDockerClient(cfg)
	if err != nil {
		log.Error("failed to construct docker client", "err", err.Error())
		os.Exit(1)
	}
	defer func() { _ = cli.Close() }()

	deployer := dockerx.NewDeployer(dockerx.NewDockerBackend(cli, dockerx.BuildConfig{
		Host:         cfg.BuildKitHost,
		RegistryPush: cfg.RegistryPush,
		RegistryPull: cfg.RegistryPull,
	}), cfg.ToolsDir)
	srv := server.New(cfg, deployer, log)

	httpSrv := &http.Server{
		Addr:    ":" + strconv.Itoa(cfg.Port),
		Handler: srv.Handler(),
		// A JIT image build can take up to ~900s (see dockerx.buildTimeout), so we do
		// NOT bound WriteTimeout; the build has its own context deadline. ReadHeaderTimeout
		// mitigates slowloris without capping a legitimately slow build.
		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Warn early if the daemon is unreachable, but do not block startup — /healthz
	// reports live status and the daemon may come up alongside us.
	{
		pctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if perr := deployer.Ping(pctx); perr != nil {
			log.Warn("docker daemon not reachable at startup", "docker_host", cfg.DockerHost, "err", perr.Error())
		} else {
			log.Info("docker daemon reachable")
		}
		cancel()
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Info("connector-deployer listening",
			"addr", httpSrv.Addr, "environment", cfg.Environment,
			"network", cfg.Network, "tools_dir", cfg.ToolsDir,
			"auth_enforced", cfg.InternalSecret != "" || cfg.IsProd(),
			"build_backend", buildBackendLabel(cfg))
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server error", "err", err.Error())
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received, draining")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "err", err.Error())
		os.Exit(1)
	}
	log.Info("shutdown complete")
}

// buildBackendLabel reports which BUILD path is active for the startup log: the rootless
// buildkitd sidecar (SEC-H-02 increment 2) when BUILDKIT_HOST is set, else the legacy
// in-daemon `docker build`.
func buildBackendLabel(cfg *config.Config) string {
	if cfg.BuildKitHost != "" {
		return "rootless-buildkit(" + cfg.BuildKitHost + ")"
	}
	return "legacy-docker-build"
}

// newDockerClient builds a docker SDK client from the environment (DOCKER_HOST etc.),
// with API-version negotiation so it works against a range of daemon versions.
func newDockerClient(cfg *config.Config) (*client.Client, error) {
	opts := []client.Opt{client.FromEnv, client.WithAPIVersionNegotiation()}
	if cfg.DockerHost != "" {
		opts = append(opts, client.WithHost(cfg.DockerHost))
	}
	return client.NewClientWithOpts(opts...)
}

// newLogger builds a slog logger honoring LOG_FORMAT (json|text) and LOG_LEVEL, the
// same knobs the other services use.
func newLogger(cfg *config.Config) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(strings.TrimSpace(cfg.LogLevel)) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if strings.EqualFold(strings.TrimSpace(cfg.LogFormat), "text") {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	logger := slog.New(h).With("service", "connector-deployer")
	slog.SetDefault(logger)
	return logger
}
