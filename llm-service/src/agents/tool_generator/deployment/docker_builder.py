"""
Docker Builder

Builds Docker images from generated connector artifacts.

Features:
- Builds from generated Dockerfile
- Supports tagging and pushing to registry
- Health check verification
- Build log streaming

Uses Docker Python SDK for container management.

VERSION: 1.1.0
""" 

import os
import re
import sys
import json
import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from enum import Enum

logger = logging.getLogger(__name__)

# Connectors that docker-compose always pre-starts (internal infra + tier-1 CDC
# destinations). Only their CURRENT (compose-managed) version is protected from the
# JIT deploy path; OLDER pinned versions are JIT-owned (compose only runs
# current_version). See DockerBuilder._is_protected_compose_container / start_container.
_COMPOSE_MANAGED_CONNECTORS = {
    "debezium",
    "kafka-mcp-sink",
    "minio",
    "postgresql",
    "mysql",
    "aws-s3",
}

# Try to import Docker SDK
try:
    import docker
    from docker.errors import DockerException, BuildError, ContainerError, ImageNotFound, NotFound
    DOCKER_SDK_AVAILABLE = True
except ImportError:
    DOCKER_SDK_AVAILABLE = False
    logger.warning("Docker SDK not available - container operations will be skipped")


class BuildStatus(str, Enum):
    """Docker build status"""
    PENDING = "pending"
    BUILDING = "building"
    PUSHING = "pushing"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class DockerBuildResult:
    """Result of a Docker build operation"""
    success: bool
    image_name: str = ""
    image_tag: str = ""
    image_id: str = ""
    
    # Build info
    build_time_seconds: float = 0
    image_size_mb: float = 0
    
    # Registry info
    pushed: bool = False
    registry_url: str = ""
    
    # Error info
    error_message: str = ""
    build_logs: List[str] = field(default_factory=list)
    
    @property
    def full_image_name(self) -> str:
        """Get full image name with tag"""
        if self.registry_url:
            return f"{self.registry_url}/{self.image_name}:{self.image_tag}"
        return f"{self.image_name}:{self.image_tag}"


class DockerBuilder:
    """
    Builds Docker images from generated connector artifacts.
    
    Uses Docker Python SDK for all operations.
    
    Usage:
        builder = DockerBuilder()
        result = await builder.build(connector_dir, "my-connector", "1.0.0")
        
        if result.success:
            logger.info(f"Built: {result.full_image_name}")
    """
    
    # Configuration
    DEFAULT_REGISTRY = os.getenv("DOCKER_REGISTRY", "")
    BASE_IMAGE = os.getenv("MCP_BASE_IMAGE", "rsync-ai/mcp-base:python-3.11")
    BUILD_TIMEOUT = 300  # 5 minutes
    
    def __init__(self, registry_url: str = None):
        """
        Initialize Docker builder.

        Args:
            registry_url: Docker registry URL (e.g., 'docker.io/myorg')
        """
        self.registry_url = registry_url or self.DEFAULT_REGISTRY
        self.client = None

        # Feature flag (SEC-H-02): when DEPLOYER_URL is set, tool-generator NEVER
        # touches the Docker socket. It delegates build/start/undeploy to the
        # trusted `connector-deployer` service over an authenticated RPC. In that
        # mode we deliberately do NOT create docker.from_env() and do NOT fail if
        # the socket is absent — dropping the socket is the whole point. When
        # DEPLOYER_URL is unset (default), behavior is byte-for-byte the legacy
        # in-process Docker-SDK path (the one-release rollback path).
        self.deployer_url = (os.getenv("DEPLOYER_URL") or "").strip()
        self.use_deployer = bool(self.deployer_url)
        self._deployer = None
        if self.use_deployer:
            logger.info(
                "DockerBuilder in DEPLOYER mode (DEPLOYER_URL=%s) — Docker socket NOT used",
                self.deployer_url,
            )
        else:
            self._init_docker_client()

    @property
    def deployer(self):
        """Lazy-load the deployer RPC client (only ever used in deployer mode)."""
        if self._deployer is None:
            from .deployer_client import DeployerClient
            self._deployer = DeployerClient()
        return self._deployer

    def _init_docker_client(self) -> bool:
        """Initialize Docker client"""
        if not DOCKER_SDK_AVAILABLE:
            logger.warning("Docker SDK not available")
            return False
        
        try:
            # Try to connect to Docker daemon
            self.client = docker.from_env()
            version = self.client.version()
            logger.info(f"Docker version: {version.get('Version', 'unknown')}")
            return True
        except DockerException as e:
            logger.warning(f"Docker not available: {e}")
            self.client = None
            return False
        except Exception as e:
            logger.warning(f"Failed to initialize Docker client: {e}")
            self.client = None
            return False
    
    def _check_docker_available(self) -> bool:
        """Check if Docker is available"""
        if not self.client:
            return self._init_docker_client()
        try:
            self.client.ping()
            return True
        except Exception:
            return self._init_docker_client()

    @staticmethod
    def _infer_connector_id_from_container_name(container_name: str) -> str:
        """
        Best-effort inference of connector id from container name.

        Supports both:
        - Versioned runtime standard: rsync-ai-<id>-vX-Y-Z-mcp
        - Legacy stable:           rsync-ai-<id>-mcp
        """
        if not container_name:
            return ""

        # Only attempt inference for our naming convention.
        if not (container_name.startswith("rsync-ai-") and container_name.endswith("-mcp")):
            return container_name

        core = container_name[len("rsync-ai-"):-len("-mcp")]
        if not core:
            return ""

        # If core ends with "-v<digits...>", strip the version suffix.
        # Example: "aws-s3-v1-0-2" -> "aws-s3"
        if "-v" in core:
            left, right = core.rsplit("-v", 1)
            if left and right and right[0].isdigit():
                # Allow semver-ish tags like 1-0-2, 1-0-2-rc1, etc.
                # We only require it to start with a digit to avoid stripping ids like "inventory-vault".
                return left

        return core
        try:
            self.client.ping()
            return True
        except Exception:
            return self._init_docker_client()
    
    async def build(
        self,
        connector_dir: Path,
        name: str,
        version: str = "latest",
        push: bool = False,
        build_args: Dict[str, str] = None,
    ) -> DockerBuildResult:
        """
        Build a Docker image from a connector directory.
        
        Args:
            connector_dir: Path to connector directory (with Dockerfile)
            name: Image name (e.g., 'mcp-hubspot')
            version: Image tag/version (e.g., '1.0.0')
            push: Whether to push to registry after building
            build_args: Additional build arguments
        
        Returns:
            DockerBuildResult with build status and info
        """
        start_time = datetime.now()
        result = DockerBuildResult(
            success=False,
            image_name=f"mcp-{name}",
            image_tag=version,
        )
        
        connector_dir = Path(connector_dir)

        # Validate directory
        if not connector_dir.exists():
            result.error_message = f"Connector directory not found: {connector_dir}"
            return result

        # DEPLOYER mode: do NOT build locally. The image is built server-side inside
        # POST /v1/deploy (called from start_container). Here we only validate that a
        # connector context dir + Dockerfile are resolvable and hand back the DERIVED
        # image name (mcp-<name>:<version>, same ref the deployer derives). We never
        # touch the Docker socket. build() success gates start_container in both call
        # sites (service.py / routes.py _build_and_start_in_background), so we stay
        # lenient: the deployer independently re-resolves + validates the context by
        # connector_id/version, so a path-shape mismatch here must not abort the deploy.
        if self.use_deployer:
            dockerfile_dir = self._resolve_dockerfile_dir(connector_dir, version)
            if dockerfile_dir is None:
                logger.warning(
                    "Deployer mode: no Dockerfile resolvable under %s (name=%s, version=%s); "
                    "deferring context resolution to the deployer",
                    connector_dir, name, version,
                )
            else:
                logger.info(
                    "🐳 Deployer mode: build deferred to connector-deployer for %s:%s (context=%s)",
                    result.image_name, result.image_tag, dockerfile_dir,
                )
            result.success = True
            result.build_time_seconds = (datetime.now() - start_time).total_seconds()
            return result

        dockerfile = connector_dir / "Dockerfile"
        if not dockerfile.exists():
            result.error_message = f"Dockerfile not found in {connector_dir}"
            return result

        # Check Docker availability
        if not self._check_docker_available():
            result.error_message = "Docker not available"
            return result
        
        logger.info(f"🐳 Building Docker image: {result.image_name}:{result.image_tag}")
        
        try:
            # Build the image using Docker SDK
            build_result = await self._run_docker_build(
                connector_dir, result.image_name, result.image_tag, build_args
            )
            
            if not build_result[0]:
                result.error_message = build_result[1]
                result.build_logs = build_result[2]
                return result
            
            result.image_id = build_result[1]
            result.build_logs = build_result[2]
            
            # Get image size
            result.image_size_mb = self._get_image_size(f"{result.image_name}:{result.image_tag}")
            
            # Push if requested
            if push and self.registry_url:
                result.registry_url = self.registry_url
                push_success = await self._push_image(result.full_image_name)
                result.pushed = push_success
                if not push_success:
                    logger.warning(f"Failed to push image to registry")
            
            result.success = True
            result.build_time_seconds = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"✅ Docker build complete: {result.full_image_name} ({result.image_size_mb:.1f}MB)")
            
        except Exception as e:
            result.error_message = str(e)
            logger.error(f"❌ Docker build failed: {e}")
        
        return result
    
    async def _run_docker_build(
        self,
        context_dir: Path,
        image_name: str,
        tag: str,
        build_args: Dict[str, str] = None,
    ) -> Tuple[bool, str, List[str]]:
        """Run docker build using Docker SDK"""
        
        if not self.client:
            return False, "Docker client not initialized", []
        
        logs = []
        
        try:
            # Build using Docker SDK
            logger.debug(f"Building image {image_name}:{tag} from {context_dir}")
            
            # Run build in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            
            def do_build():
                # Extract connector name from image name (mcp-{name} -> name)
                connector_name = image_name.replace("mcp-", "")

                # Generated connector Dockerfiles pull shared libs via the `shared`
                # BuildKit named context (COPY --from=shared rsync_protocol ...),
                # declared as additional_contexts (shared: ./shared/mcp-connectors/
                # public) in docker-compose.mcp.yml. The high-level Docker SDK uses
                # the LEGACY builder, which cannot supply named contexts, so such a
                # JIT build dies with "invalid from flag value shared: pull access
                # denied for shared" — meaning a freshly generated connector can't be
                # deployed on demand. Detect the named-context use and build via the
                # docker CLI (BuildKit) with --build-context, mirroring the compose
                # build exactly.
                try:
                    dockerfile_text = (Path(context_dir) / "Dockerfile").read_text(errors="ignore")
                except Exception:
                    dockerfile_text = ""
                if "--from=shared" in dockerfile_text:
                    return self._cli_build_with_shared_context(
                        Path(context_dir), image_name, tag, connector_name, build_args
                    )

                image, build_logs = self.client.images.build(
                    path=str(context_dir),
                    tag=f"{image_name}:{tag}",
                    rm=True,
                    pull=False,
                    buildargs=build_args or {},
                    labels={
                        # Docker Compose project labels - separate rsync-ai-mcp group
                        "com.docker.compose.project": "rsync-ai-mcp",
                        "com.docker.compose.service": connector_name,
                        # MCP-specific labels
                        "mcp.connector.version": tag,
                        "mcp.connector.name": connector_name,
                        "mcp.managed": "true",
                        "mcp.auto.generated": "true",
                    },
                )
                return image, list(build_logs)
            
            image, build_logs = await loop.run_in_executor(None, do_build)
            
            # Parse logs
            for log_entry in build_logs:
                if 'stream' in log_entry:
                    line = log_entry['stream'].strip()
                    if line:
                        logs.append(line)
                        logger.debug(f"[docker] {line}")
                elif 'error' in log_entry:
                    logs.append(f"ERROR: {log_entry['error']}")
            
            # Get image ID
            image_id = image.short_id.replace('sha256:', '')[:12]
            
            return True, image_id, logs
                
        except BuildError as e:
            error_logs = [log.get('stream', '') for log in e.build_log if 'stream' in log]
            return False, str(e), error_logs
        except Exception as e:
            return False, str(e), logs

    def _cli_build_with_shared_context(self, context_dir, image_name, tag, connector_name, build_args=None):
        """Build via the docker CLI (BuildKit) supplying the `shared` named context.

        The high-level Docker SDK (images.build) uses the legacy builder and cannot
        provide BuildKit named contexts. Generated connector Dockerfiles do
        ``COPY --from=shared rsync_protocol ...`` where `shared` is the public/ root
        (declared as additional_contexts in docker-compose.mcp.yml). We resolve the
        public/ ancestor of the build context and pass it via --build-context — the
        same mechanism the compose build uses. Returns (image, logs) like the SDK
        path; raises on failure (handled by _run_docker_build's except).
        """
        import subprocess

        shared_dir = None
        for parent in Path(context_dir).resolve().parents:
            if parent.name == "public":
                shared_dir = parent
                break
        if shared_dir is None:
            raise RuntimeError(
                f"cannot resolve `shared` (public/) build context from {context_dir}"
            )

        cmd = [
            "docker", "build",
            "--build-context", f"shared={shared_dir}",
            "-t", f"{image_name}:{tag}",
            "--label", "com.docker.compose.project=rsync-ai-mcp",
            "--label", f"com.docker.compose.service={connector_name}",
            "--label", f"mcp.connector.version={tag}",
            "--label", f"mcp.connector.name={connector_name}",
            "--label", "mcp.managed=true",
            "--label", "mcp.auto.generated=true",
        ]
        for k, v in (build_args or {}).items():
            cmd += ["--build-arg", f"{k}={v}"]
        cmd.append(str(context_dir))

        env = {**os.environ, "DOCKER_BUILDKIT": "1"}
        logger.info(
            "🐳 JIT build via docker CLI with shared context: %s (shared=%s)",
            f"{image_name}:{tag}", shared_dir,
        )
        proc = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=900)
        logs = [{"stream": ln} for ln in ((proc.stdout or "") + "\n" + (proc.stderr or "")).splitlines() if ln.strip()]
        if proc.returncode != 0:
            tail = (proc.stderr or proc.stdout or "")[-1500:]
            raise RuntimeError(f"docker CLI build (shared context) failed: {tail}")
        image = self.client.images.get(f"{image_name}:{tag}")
        return image, logs

    @staticmethod
    def _resolve_dockerfile_dir(connector_dir: Path, version: str = "latest") -> Optional[Path]:
        """Best-effort locate the dir holding the connector's Dockerfile.

        Handles both shapes the two call sites pass to build():
        - routes.py passes the concrete ``versions/<v>/`` dir (Dockerfile lives there);
        - service.py passes the connector ROOT (no root copies in the versioned
          layout — the Dockerfile is under ``versions/<current>/``).
        Returns the containing dir, or ``None`` if nothing is found.
        """
        connector_dir = Path(connector_dir)
        candidates: List[Path] = [connector_dir]
        # Explicit version dir (with and without leading "v").
        if version and version.lower() != "latest":
            for v in {version, f"v{version}" if not version.startswith("v") else version}:
                candidates.append(connector_dir / "versions" / v)
        # latest.json → versions/<current>/ (shared resolver).
        try:
            from src.utils.connector_paths import resolve_current_dir
            candidates.append(resolve_current_dir(connector_dir))
        except Exception:
            pass
        for cand in candidates:
            try:
                if (cand / "Dockerfile").exists():
                    return cand
            except OSError:
                continue
        return None

    def _get_image_size(self, image_name: str) -> float:
        """Get image size in MB using Docker SDK"""
        if not self.client:
            return 0
        
        try:
            image = self.client.images.get(image_name)
            size_bytes = image.attrs.get('Size', 0)
            return size_bytes / (1024 * 1024)  # Convert to MB
        except Exception:
            return 0
    
    async def _push_image(self, image_name: str) -> bool:
        """Push image to registry using Docker SDK"""
        if not self.client:
            return False
        
        logger.info(f"📤 Pushing image: {image_name}")
        
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.client.images.push(image_name)
            )
            return True
        except Exception as e:
            logger.error(f"Push failed: {e}")
            return False
    
    @staticmethod
    def _parse_container_name(container_name: str) -> Optional[Tuple[str, str]]:
        """Parse 'rsync-ai-<id>-v<X-Y-Z>-mcp' → (connector_id, 'X-Y-Z').

        Returns None if the name doesn't match the versioned runtime naming scheme.
        """
        name = (container_name or "").strip()
        if not (name.startswith("rsync-ai-") and name.endswith("-mcp")):
            return None
        middle = name[len("rsync-ai-"):-len("-mcp")]
        m = re.match(r"^(.*)-v(\d+-\d+-\d+)$", middle)
        if not m:
            return None
        return m.group(1), m.group(2)

    def _resolve_current_version(self, connector_id: str) -> Optional[str]:
        """Read the connector's latest.json current_version → 'X-Y-Z' (hyphenated, no
        leading 'v'). Returns None if it can't be resolved."""
        tools_dir = os.getenv("TOOLS_DIR", "/app/shared/mcp-connectors")
        base = Path(tools_dir)
        if base.name in ("public", "internal"):
            base = base.parent
        cands = {connector_id, connector_id.replace("-", "_"), connector_id.replace("_", "-")}
        search_dirs: List[Path] = []
        for cand in cands:
            search_dirs.append(base / "internal" / cand)
            search_dirs.append(base / "public" / cand)
            search_dirs.append(base / cand)
        public_root = base / "public"
        if public_root.is_dir():
            try:
                for cat in public_root.iterdir():
                    if cat.is_dir():
                        for cand in cands:
                            search_dirs.append(cat / cand)
            except Exception:
                pass
        for d in search_dirs:
            lp = d / "latest.json"
            if lp.is_file():
                try:
                    cv = (json.loads(lp.read_text()).get("current_version") or "").strip()
                except Exception:
                    return None
                if cv:
                    return cv.lstrip("v").replace(".", "-")
        return None

    def _is_protected_compose_container(self, container_name: str) -> bool:
        """True only for the CURRENT compose-managed version of a compose-managed
        connector. Older pinned versions return False so the JIT build+start path owns
        them. If current_version can't be resolved, conservatively protect the legacy
        v1.0.0 container (preserves prior behavior)."""
        parsed = self._parse_container_name(container_name)
        if not parsed:
            return False
        connector_id, version_part = parsed
        if connector_id not in _COMPOSE_MANAGED_CONNECTORS:
            return False
        current = self._resolve_current_version(connector_id)
        if current is None:
            return version_part == "1-0-0"
        return version_part == current

    async def start_container(
        self,
        image_name: str,
        container_name: str,
        port: int = None,
        # Default to the dedicated user-connector network (SEC-M-06 isolation). Callers
        # pass DOCKER_NETWORK from env (deployment/service.py); this default is the
        # fallback when unset. When `network` already IS rsync-ai-mcp the "ALSO join
        # rsync-ai-mcp" block below is a no-op (guarded by `mcp_network != network`).
        network: str = "rsync-ai-mcp",
        env_vars: Dict[str, str] = None,
        aliases: List[str] = None,
        recreate: bool = False,
    ) -> Tuple[bool, str]:
        """
        Start a container from an image using Docker SDK.
        
        Containers are labeled to appear in the rsync-ai Docker Compose group.
        
        Args:
            image_name: Docker image name (e.g., 'mcp-hubspot:1.0.0')
            container_name: Name for the container
            port: External port to bind (internal is always 8000)
            network: Docker network to join
            env_vars: Environment variables to set
        
        Returns:
            Tuple of (success, container_id or error_message)
        """
        # DEPLOYER mode: delegate the entire build-if-missing + run to the trusted
        # connector-deployer over RPC. This branch is taken BEFORE any Docker-socket
        # access (_check_docker_available) so tool-generator never touches the socket.
        # We translate the deployer's JSON response back to the SAME (success,
        # container_id_or_msg) shape the legacy path returns, so service.py/routes.py
        # need no change.
        if self.use_deployer:
            return await self._start_via_deployer(
                image_name=image_name,
                container_name=container_name,
                port=port,
                env_vars=env_vars,
                aliases=aliases,
                recreate=recreate,
            )

        if not self._check_docker_available():
            return False, "Docker not available"

        logger.info(f"🚀 Starting container: {container_name} from {image_name}")

        # Compose-managed connectors (internal infra + tier-1 CDC destinations) are
        # pre-started by docker-compose. We must NEVER tear down the container for the
        # connector's CURRENT (compose-managed) version: it may be actively serving a CDC
        # stream, and its image name (rsync-ai-*-mcp) differs from the mcp-{id}:{version}
        # scheme so a remove+recreate would fail with ImageNotFound. If that container is
        # down, surface it (fail loud) rather than silently spawning a replacement.
        #
        # OLDER pinned versions of these connectors are NOT compose-managed (compose only
        # runs current_version), so they fall through to the JIT build+start path below —
        # this is what lets a pipeline pinned to an old version get its container built on
        # demand.
        if self._is_protected_compose_container(container_name):
            try:
                existing = self.client.containers.get(container_name)
                if existing.status == "running":
                    logger.info(f"⏭️  {container_name} is compose-managed and already running — skipping deploy")
                    return True, existing.id
                # Stopped/paused — restart without touching the image (compose owns it)
                logger.info(f"🔄 {container_name} is compose-managed but stopped (status={existing.status}) — restarting")
                existing.start()
                return True, existing.id
            except NotFound:
                svc = container_name.removeprefix("rsync-ai-").removesuffix("-mcp")
                return False, (
                    f"Compose-managed container {container_name} not found; "
                    f"run: docker compose up -d {svc}"
                )
            except Exception as e:
                return False, f"Failed to restart compose-managed container {container_name}: {e}"

        # SAFETY: never tear down a RUNNING container — it may be serving a pinned pipeline
        # (or an old-version CDC stream). If one is already up under this exact name, reuse
        # it. Only remove a stale STOPPED/dead container before (re)creating.
        try:
            existing = self.client.containers.get(container_name)
            if existing.status == "running" and not recreate:
                logger.info(f"⏭️  {container_name} already running — reusing (no rebuild)")
                return True, existing.id
            existing.remove(force=True)
            logger.info(f"Removed (status={existing.status}, recreate={recreate}) container: {container_name}")
        except NotFound:
            pass
        except Exception:
            pass
        
        # Prepare environment
        environment = env_vars or {}
        
        # Optional: mount shared OAuth token storage so connectors can use TokenManager.
        # This enables a Fivetran-style "Connect" flow where tool-generator exchanges the
        # code and stores tokens by connection_id, and runtime containers read them later.
        oauth_tokens_volume = os.getenv("OAUTH_TOKENS_VOLUME_NAME", "rsync-ai-oauth-tokens").strip()
        volumes = None
        if oauth_tokens_volume:
            volumes = {
                oauth_tokens_volume: {
                    "bind": "/root/.rsync-ai",
                    "mode": "rw",
                }
            }
        
        # Prefer explicit connector id passed by caller (stable, canonical).
        # This avoids incorrect parsing for versioned container names.
        connector_id = (environment.get("MCP_CONNECTOR_NAME") or "").strip()
        if not connector_id:
            connector_id = self._infer_connector_id_from_container_name(container_name).strip()
        
        # Labels to make container appear in separate rsync-ai-mcp compose group
        labels = {
            # Docker Compose project labels - creates separate "rsync-ai-mcp" group
            "com.docker.compose.project": "rsync-ai-mcp",
            "com.docker.compose.project.working_dir": "/app/shared/mcp-connectors",
            "com.docker.compose.service": connector_id,
            "com.docker.compose.container-number": "1",
            "com.docker.compose.oneoff": "False",
            # MCP-specific labels
            "mcp.connector.name": connector_id,
            "mcp.connector.type": "generated",
            "mcp.managed": "true",
            "mcp.auto.generated": "true",
            # rsync-ai discovery labels (used by api-gateway)
            "rsync-ai.mcp": "true",
            "rsync-ai.connector": connector_id,
        }
        
        # Retry loop for port conflicts
        max_retries = 3
        current_port = port
        
        for attempt in range(max_retries):
            try:
                # Prepare port bindings
                ports = None
                if current_port:
                    ports = {'8000/tcp': current_port}
                
                # Create and start container
                container = self.client.containers.run(
                    image_name,
                    name=container_name,
                    detach=True,
                    network=network,
                    ports=ports,
                    environment=environment,
                    restart_policy={"Name": "unless-stopped"},
                    labels=labels,
                    volumes=volumes,
                )
                
                container_id = container.short_id

                # Attach the same network aliases that docker-compose assigns
                # (canonical map: scripts/mcp_generate_compose.py
                # NETWORK_ALIASES_BY_CONNECTOR_ID). Without these, a JIT-deployed
                # destination connector can't be resolved by kafka-mcp-sink's
                # unversioned hostname (rsync-ai-{conn}-mcp), the way compose-started
                # connectors can. Non-fatal: the container still works by its
                # versioned name even if aliasing fails.
                if aliases:
                    try:
                        net = self.client.networks.get(network)
                        net.disconnect(container, force=True)
                        net.connect(container, aliases=aliases)
                        logger.info(f"🔗 Attached aliases {aliases} to {container_name} on {network}")
                    except Exception as alias_err:
                        logger.warning(f"Could not attach aliases {aliases} to {container_name}: {alias_err}")

                # ALSO join the shared MCP network so kafka-mcp-sink (which bridges
                # into rsync-ai-mcp) can resolve this connector by name/alias. In the
                # normal case `network` IS rsync-ai-mcp (SEC-M-06 default, set via
                # DOCKER_NETWORK), so this block is a NO-OP — skipped by the
                # `mcp_network != network` guard below, because the container already
                # joined rsync-ai-mcp as its primary network above (with aliases).
                # It only does real work when a caller overrides `network` to some
                # OTHER network (e.g. legacy rsync-ai_default): a JIT-deployed
                # DESTINATION connector that never joins rsync-ai-mcp is invisible to
                # the sink, so every write row is DLQ'd. Non-fatal: the container
                # still serves the source/read path on `network` if this fails.
                mcp_network = os.getenv("MCP_SHARED_NETWORK", "rsync-ai-mcp")
                if mcp_network and mcp_network != network:
                    try:
                        mcp_net = self.client.networks.get(mcp_network)
                        try:
                            mcp_net.disconnect(container, force=True)
                        except Exception:
                            pass  # not previously attached — expected on first spawn
                        mcp_net.connect(container, aliases=aliases or None)
                        logger.info(
                            f"🔗 Attached {container_name} to {mcp_network} "
                            f"(aliases={aliases}) for kafka-mcp-sink reachability")
                    except Exception as mcp_err:
                        logger.warning(
                            f"Could not attach {container_name} to {mcp_network} "
                            f"(kafka-mcp-sink may not reach this destination connector): {mcp_err}")

                if current_port:
                    logger.info(f"✅ Container started: {container_name} ({container_id}) on port {current_port}")
                else:
                    logger.info(f"✅ Container started: {container_name} ({container_id}) - internal access only")
                return True, container_id
                
            except ImageNotFound:
                return False, f"Image not found: {image_name}"
            except ContainerError as e:
                return False, f"Container error: {e}"
            except Exception as e:
                error_str = str(e)
                # Check if port conflict - retry with different port
                if "port is already allocated" in error_str or "Bind for" in error_str:
                    logger.warning(f"Port {current_port} in use, trying next port...")
                    # Remove the failed container attempt
                    try:
                        failed_container = self.client.containers.get(container_name)
                        failed_container.remove(force=True)
                    except Exception:
                        pass
                    # Get next available port
                    current_port = await self.get_next_available_port(current_port + 1)
                    continue
                return False, error_str
        
        return False, f"Failed to start container after {max_retries} attempts"

    @staticmethod
    def _version_from_image_name(image_name: str) -> str:
        """'mcp-postgresql:v1.0.0' -> 'v1.0.0'. '' if no tag present."""
        if image_name and ":" in image_name:
            return image_name.rsplit(":", 1)[1].strip()
        return ""

    async def _start_via_deployer(
        self,
        *,
        image_name: str,
        container_name: str,
        port: Optional[int],
        env_vars: Optional[Dict[str, str]],
        aliases: Optional[List[str]],
        recreate: bool,
    ) -> Tuple[bool, str]:
        """Deployer-mode implementation of start_container.

        Derives connector_id/version the SAME way the legacy path does (caller's
        MCP_CONNECTOR_NAME/MCP_CONNECTOR_VERSION env, falling back to the container
        name / image tag), then POSTs /v1/deploy. The deployer resolves
        context_subdir under its read-only TOOLS_DIR mount, builds the image if
        missing, and runs the container — mirroring start_container's semantics.
        """
        env = env_vars or {}
        connector_id = (env.get("MCP_CONNECTOR_NAME") or "").strip()
        if not connector_id:
            connector_id = self._infer_connector_id_from_container_name(container_name).strip()
        version = (env.get("MCP_CONNECTOR_VERSION") or "").strip()
        if not version:
            version = self._version_from_image_name(image_name)

        logger.info(
            "🚀 Deployer mode: POST /v1/deploy for %s (connector_id=%s version=%s recreate=%s)",
            container_name, connector_id, version, recreate,
        )

        loop = asyncio.get_event_loop()
        try:
            resp = await loop.run_in_executor(
                None,
                lambda: self.deployer.deploy(
                    connector_id=connector_id,
                    version=version,
                    container_name=container_name,
                    aliases=aliases or [],
                    env=env,
                    port=int(port or 0),
                    recreate=bool(recreate),
                    build_args={},
                ),
            )
        except Exception as e:
            logger.error("❌ Deployer /v1/deploy failed for %s: %s", container_name, e)
            return False, f"deployer deploy failed for {container_name}: {e}"

        if resp.get("ok"):
            container_id = resp.get("container_id") or resp.get("status") or "started"
            logger.info(
                "✅ Deployer started %s (%s, built=%s)",
                container_name, container_id, resp.get("built"),
            )
            return True, container_id
        return False, str(resp.get("error") or resp.get("detail") or resp)

    async def stop_container(self, container_name: str) -> bool:
        """Stop a running container.

        Deployer mode: /v1/undeploy performs stop+remove atomically, so the actual
        teardown is deferred to remove_container (called immediately after in the
        undeploy loop). Returning True here keeps the paired stop→remove flow intact
        without a redundant RPC. Legacy: unchanged (Docker SDK)."""
        if self.use_deployer:
            return True
        if not self.client:
            return False

        try:
            container = self.client.containers.get(container_name)
            container.stop(timeout=10)
            return True
        except Exception:
            return False

    async def remove_container(self, container_name: str, force: bool = True) -> bool:
        """Remove a container.

        Deployer mode: delegate to POST /v1/undeploy (stop+remove, idempotent —
        a missing container is a 200). Legacy: unchanged (Docker SDK)."""
        if self.use_deployer:
            loop = asyncio.get_event_loop()
            try:
                await loop.run_in_executor(
                    None, lambda: self.deployer.undeploy(container_name)
                )
                return True
            except Exception as e:
                logger.warning("Deployer /v1/undeploy failed for %s: %s", container_name, e)
                return False
        if not self.client:
            return False

        try:
            container = self.client.containers.get(container_name)
            container.remove(force=force)
            return True
        except Exception:
            return False

    def list_managed_container_names(self) -> List[str]:
        """Return the names of all containers the Docker daemon knows about.

        Used by DeploymentService.undeploy to discover versioned connector
        container names to tear down. Legacy: enumerates via the Docker SDK.
        Deployer mode: the RPC contract has no list endpoint (by design — the
        deployer is a minimal, allow-by-construction surface), so this returns
        an empty list; undeploy then relies on the deterministic legacy/versioned
        name teardown, which routes through /v1/undeploy per name."""
        if self.use_deployer or not self.client:
            return []
        try:
            return [
                getattr(c, "name", "") or ""
                for c in self.client.containers.list(all=True)
            ]
        except Exception:
            return []
    
    async def get_next_available_port(self, start_port: int = 8090) -> int:
        """Find the next available port for a new connector"""
        import socket
        
        # Also check what ports are used by existing containers
        used_ports = set()
        if self.client:
            try:
                containers = self.client.containers.list(all=True)
                for c in containers:
                    ports = c.attrs.get('NetworkSettings', {}).get('Ports', {}) or {}
                    for port_info in ports.values():
                        if port_info:
                            for binding in port_info:
                                if binding and binding.get('HostPort'):
                                    used_ports.add(int(binding['HostPort']))
            except Exception:
                pass
        
        for port in range(start_port, start_port + 100):
            # Skip if already used by a container
            if port in used_ports:
                continue
            
            # Check if port is available on host
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    s.bind(('', port))
                    return port
            except OSError:
                continue
        
        # Fallback: return a random high port
        import random
        return random.randint(9000, 9999)
    
    async def verify_image(self, image_name: str) -> bool:
        """Verify image can start and respond to health check using Docker SDK"""
        if not self.client:
            return False
        
        logger.info(f"🔍 Verifying image: {image_name}")
        
        container_name = f"mcp-verify-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        try:
            # Start container
            container = self.client.containers.run(
                image_name,
                name=container_name,
                detach=True,
            )
            
            # Wait a moment
            await asyncio.sleep(2)
            
            # Check if running
            container.reload()
            is_running = container.status == 'running'
            
            return is_running
            
        except Exception as e:
            logger.error(f"Verification failed: {e}")
            return False
            
        finally:
            # Cleanup
            try:
                container = self.client.containers.get(container_name)
                container.remove(force=True)
            except Exception:
                pass
    
    def list_mcp_containers(self) -> List[Dict[str, Any]]:
        """List all MCP-managed containers"""
        if not self.client:
            return []
        
        try:
            containers = self.client.containers.list(
                all=True,
                filters={"label": "mcp.managed=true"}
            )
            
            result = []
            for c in containers:
                result.append({
                    "id": c.short_id,
                    "name": c.name,
                    "status": c.status,
                    "image": c.image.tags[0] if c.image.tags else "unknown",
                    "ports": c.ports,
                })
            
            return result
        except Exception:
            return []
    
    def build_sync(
        self,
        connector_dir: Path,
        name: str,
        version: str = "latest",
    ) -> DockerBuildResult:
        """Synchronous build wrapper"""
        return asyncio.run(self.build(connector_dir, name, version))
