// Package server exposes the connector-deployer's authenticated internal RPC:
// POST /v1/deploy, POST /v1/undeploy, GET /v1/status, GET /healthz. It validates the
// untrusted request body (DATA only), maps the connector_id+version to the DERIVED
// image ref (never taken from the request), and delegates the daemon work to a
// Deployer. See CONTRACT.md for the authoritative behavior.
package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"regexp"
	"strings"

	"github.com/rsync-ai/connector-deployer/internal/config"
	"github.com/rsync-ai/connector-deployer/internal/dockerx"
	"github.com/rsync-ai/connector-deployer/internal/spec"
)

// Deployer is the daemon-facing dependency the handlers use. *dockerx.Deployer
// implements it; tests inject a fake so no live daemon is required.
type Deployer interface {
	Deploy(ctx context.Context, req spec.DeployRequest, dcfg spec.DeployerConfig, opts dockerx.DeployOptions) (dockerx.DeployResult, error)
	Undeploy(ctx context.Context, name string) error
	Status(ctx context.Context, name string) (dockerx.StatusResult, error)
	Ping(ctx context.Context) error
}

// Request-field validators for the fields spec.BuildContainerSpec does not cover.
var (
	connectorIDRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	versionRe     = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
)

// Server holds the deployer's runtime dependencies.
type Server struct {
	cfg      *config.Config
	deployer Deployer
	log      *slog.Logger
}

// New constructs a Server.
func New(cfg *config.Config, deployer Deployer, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{cfg: cfg, deployer: deployer, log: log}
}

// Handler returns the fully-wired HTTP mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("POST /v1/deploy", s.requireInternalSecret(s.handleDeploy))
	mux.HandleFunc("POST /v1/undeploy", s.requireInternalSecret(s.handleUndeploy))
	mux.HandleFunc("GET /v1/status", s.requireInternalSecret(s.handleStatus))
	return mux
}

// deployBody is the untrusted /v1/deploy request — DATA only, no HostConfig-adjacent
// fields (see spec.DeployRequest for why).
type deployBody struct {
	ConnectorID   string            `json:"connector_id"`
	Version       string            `json:"version"`
	ContextSubdir string            `json:"context_subdir"`
	ContainerName string            `json:"container_name"`
	Aliases       []string          `json:"aliases"`
	Env           map[string]string `json:"env"`
	Port          int               `json:"port"`
	Recreate      bool              `json:"recreate"`
	BuildArgs     map[string]string `json:"build_args"`
}

type undeployBody struct {
	ContainerName string `json:"container_name"`
}

func (s *Server) handleDeploy(w http.ResponseWriter, r *http.Request) {
	var body deployBody
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}

	// Validate the fields the spec layer doesn't (connector_id, version, context_subdir).
	if !connectorIDRe.MatchString(body.ConnectorID) {
		writeError(w, http.StatusBadRequest, "invalid_connector_id")
		return
	}
	if !versionRe.MatchString(body.Version) {
		writeError(w, http.StatusBadRequest, "invalid_version")
		return
	}
	contextDir, err := dockerx.ResolveContextDir(s.cfg.ToolsDir, body.ContextSubdir)
	if err != nil {
		s.log.Warn("rejected context_subdir", "subdir", body.ContextSubdir, "err", err.Error())
		writeError(w, http.StatusBadRequest, "invalid_context_subdir")
		return
	}

	// The image ref is DERIVED here — never taken from the request.
	imageRef := "mcp-" + body.ConnectorID + ":" + body.Version

	dcfg := s.cfg.DeployerConfig()
	req := spec.DeployRequest{
		Image:   imageRef,
		Name:    body.ContainerName,
		Aliases: body.Aliases,
		Env:     body.Env,
		Port:    body.Port,
	}

	// Pre-validate the DATA (container name / aliases / env / port) up front so a
	// malformed request is a clean 400 before we touch the daemon. Deploy re-runs this
	// plus ValidateHostConfigSafe as its own defense-in-depth gate.
	if _, _, _, verr := spec.BuildContainerSpec(req, dcfg); verr != nil {
		s.log.Warn("rejected deploy request", "err", verr.Error())
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}

	opts := dockerx.DeployOptions{
		Recreate:         body.Recreate,
		ContextDir:       contextDir,
		ConnectorID:      body.ConnectorID,
		Version:          body.Version,
		BuildArgs:        body.BuildArgs,
		MCPSharedNetwork: s.cfg.MCPSharedNetwork,
	}

	res, err := s.deployer.Deploy(r.Context(), req, dcfg, opts)
	if err != nil {
		s.log.Error("deploy failed", "container", body.ContainerName, "image", imageRef, "err", err.Error())
		writeError(w, statusForKind(dockerx.KindOf(err)), err.Error())
		return
	}

	s.log.Info("deployed", "container", body.ContainerName, "image", imageRef, "built", res.Built, "id", res.ContainerID)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":           true,
		"container_id": res.ContainerID,
		"status":       "running",
		"image":        imageRef,
		"built":        res.Built,
	})
}

func (s *Server) handleUndeploy(w http.ResponseWriter, r *http.Request) {
	var body undeployBody
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	if strings.TrimSpace(body.ContainerName) == "" {
		writeError(w, http.StatusBadRequest, "missing_container_name")
		return
	}
	if err := s.deployer.Undeploy(r.Context(), body.ContainerName); err != nil {
		s.log.Error("undeploy failed", "container", body.ContainerName, "err", err.Error())
		writeError(w, statusForKind(dockerx.KindOf(err)), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		writeError(w, http.StatusBadRequest, "missing_name")
		return
	}
	res, err := s.deployer.Status(r.Context(), name)
	if err != nil {
		writeError(w, statusForKind(dockerx.KindOf(err)), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"exists":       res.Exists,
		"status":       res.Status,
		"container_id": res.ContainerID,
	})
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if err := s.deployer.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "daemon_unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// requireInternalSecret mirrors tool_generator routes.py::require_internal_secret:
// secret set + header matches ⇒ allow; mismatch/absent ⇒ 401; secret unset ⇒ 503 in
// prod (fail-closed), allow+warn in dev.
func (s *Server) requireInternalSecret(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		secret := s.cfg.InternalSecret // already trimmed in config.Load
		if secret == "" {
			if s.cfg.IsProd() {
				writeError(w, http.StatusServiceUnavailable, "internal_secret_not_configured")
				return
			}
			s.log.Warn("INTERNAL_SERVICE_SECRET unset — allowing request (non-production only)", "path", r.URL.Path)
			next(w, r)
			return
		}
		if strings.TrimSpace(r.Header.Get("X-Internal-Secret")) != secret {
			writeError(w, http.StatusUnauthorized, "invalid_internal_secret")
			return
		}
		next(w, r)
	}
}

// statusForKind maps a deployer error kind to its HTTP status (CONTRACT.md).
func statusForKind(k dockerx.ErrorKind) int {
	switch k {
	case dockerx.KindProtectedMissing:
		return http.StatusConflict // 409
	case dockerx.KindDaemon:
		return http.StatusServiceUnavailable // 503
	case dockerx.KindBuildFailed, dockerx.KindCreateFailed, dockerx.KindValidationRefused, dockerx.KindInternal:
		return http.StatusInternalServerError // 500
	default:
		return http.StatusInternalServerError
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, code int, detail string) {
	writeJSON(w, code, map[string]any{"ok": false, "error": detail})
}
