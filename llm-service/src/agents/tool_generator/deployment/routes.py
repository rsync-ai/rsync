"""
Connector lifecycle routes — moat-free.

Serves POST /v1/deploy: (re)start or JIT-build an EXISTING connector container.
This is the ONLY connector-lifecycle endpoint the Go data plane calls
(backend-orchestrator server_manager.go -> TOOL_GENERATOR_URL/v1/deploy) for
self-heal / pinned-version JIT builds.

Relocated here (out of agents/integration.py) so the community / OSS image can
serve connector deploy WITHOUT importing the connector-generation package or its
curated config YAML. This module's import closure is stdlib + fastapi/pydantic +
the moat-free deployment service + on-disk connector-path helpers — it must NEVER
import .orchestrator, ..config, or any generation agent. The private (cloud)
image re-mounts this router from register_agentic_routes() so its /v1/deploy
behaviour is byte-for-byte unchanged.
"""

import os
import re
import json
import asyncio
import logging
from pathlib import Path
from typing import Dict, Any, Optional

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field

from src.utils.connector_paths import iter_connector_dirs, resolve_current_dir
from .service import DeploymentService

logger = logging.getLogger(__name__)

lifecycle_router = APIRouter(tags=["Connector Lifecycle"])


def require_internal_secret(
    x_internal_secret: Optional[str] = Header(default=None),
) -> None:
    """S2S gate: connector-lifecycle sits on rsync-ai-mcp with untrusted JIT
    connectors and holds docker.sock, so /v1/deploy can build+start containers.
    Mirror api-gateway InternalServiceMiddleware / orchestrator requirePrincipal.
    No-op when INTERNAL_SERVICE_SECRET is unset in dev/e2e/OSS; enforced when set;
    FAIL-CLOSED in production even if unset (an empty S2S secret must never leave
    the docker.sock-backed /v1/deploy endpoint open to an unauthenticated caller
    on rsync-ai-mcp). The prod compose also guards presence with ${...:?}; this is
    defense-in-depth for any other launch path.
    """
    secret = (os.getenv("INTERNAL_SERVICE_SECRET") or "").strip()
    if not secret:
        env = (os.getenv("ENVIRONMENT") or "").strip().lower()
        if env in ("production", "prod"):
            raise HTTPException(status_code=503, detail="internal_secret_not_configured")
        return
    if (x_internal_secret or "").strip() != secret:
        raise HTTPException(status_code=401, detail="invalid_internal_secret")


_RE_NON_ID = re.compile(r"[^a-z0-9-]+")

# Network aliases a JIT-deployed connector must claim so kafka-mcp-sink can resolve
# it by the SAME unversioned hostname it uses for compose-started connectors.
# MUST stay in lockstep with the canonical map in
# scripts/mcp_generate_compose.py (NETWORK_ALIASES_BY_CONNECTOR_ID).
_NETWORK_ALIASES_BY_CONNECTOR_ID: dict[str, list[str]] = {
    "mysql": ["rsync-ai-mysql-mcp"],
    "postgresql": ["rsync-ai-postgresql-mcp"],
    "aws-s3": ["rsync-ai-aws_s3-mcp", "rsync-ai-aws-s3-mcp"],
}


def _managed_connectors_enabled() -> bool:
    """
    Managed connectors mode:
    - true: tool-generator may build Docker images + start containers (requires docker.sock / Docker daemon)
    - false (default): tool-generator only writes artifacts; operator runs external MCP stack separately
    """
    v = (os.getenv("RSYNC_MANAGED_CONNECTORS") or "").strip().lower()
    return v in ("1", "true", "yes")


def _canonicalize_connector_id(raw: str) -> str:
    """
    Canonical connector id (kebab-case).

    This MUST match how the rest of the platform identifies connectors:
    - metadata.json `id`
    - metadata.json `connector_type`
    - MCP tool prefix (e.g., aws-s3_export)
    """
    s = (raw or "").strip().lower()
    s = s.replace(" ", "-").replace("_", "-")
    s = _RE_NON_ID.sub("", s)
    while "--" in s:
        s = s.replace("--", "-")
    s = s.strip("-")
    # Small compatibility aliases (keep minimal and predictable)
    if s == "s3":
        return "aws-s3"
    if s == "postgres":
        return "postgresql"
    return s


class DeployRequestV1(BaseModel):
    """
    (Re)deploy an existing connector container.

    Used by backend-orchestrator for self-healing: if a connector container is missing,
    start it without regenerating code.
    """
    connector_name: str = Field(..., description="Connector identifier (any alias accepted)")
    version: str = Field("latest", description="Connector version (e.g., 'latest' or 'v1.2.3')")
    build_if_missing: bool = Field(False, description="If true, build image if not present")
    force_rebuild: bool = Field(
        False,
        description=(
            "Regenerate path: always rebuild the image + recreate the container so the "
            "freshly written code ships. Leaves the self-heal start-only path unchanged when false."
        ),
    )


class DeployResponseV1(BaseModel):
    success: bool
    connector_id: str
    version: str
    container_name: str
    image: Optional[str] = None
    started: bool = False
    built: bool = False
    # True when an image build was kicked off in the background (a cold build can take
    # minutes). The caller should poll the container's /health rather than block on /deploy.
    building: bool = False
    error_message: Optional[str] = None


# In-flight JIT builds, keyed by target container_name. Prevents two concurrent /deploy
# calls (e.g. two pipelines pinned to the same old version) from launching duplicate
# builds. Accessed only from async handlers on a single event loop, so a plain set with
# synchronous check-then-add (no await between) is race-free.
_INFLIGHT_BUILDS: set = set()


async def _build_and_start_in_background(
    *,
    deploy_service,
    connector_id: str,
    concrete_version: str,
    version_dir: Path,
    container_name: str,
    net_aliases,
    recreate: bool = False,
) -> None:
    """Build a connector image from versions/<v>/ and start its container.

    Runs as a detached task so the /deploy response can return immediately; the caller
    polls the container's /health to learn when it becomes ready. Always clears the
    in-flight marker on exit so a failed build can be retried on the next run.
    """
    try:
        build_result = await deploy_service.docker_builder.build(
            connector_dir=version_dir,
            name=connector_id,
            version=concrete_version,
        )
        if not build_result.success:
            logger.error(
                f"❌ JIT build failed for {container_name}: "
                f"{build_result.error_message or 'unknown error'}"
            )
            return
        start_ok, start_info = await deploy_service.docker_builder.start_container(
            image_name=build_result.full_image_name,
            container_name=container_name,
            port=None,
            network=deploy_service.docker_network,
            env_vars={
                "MCP_CONNECTOR_NAME": connector_id,
                "MCP_CONNECTOR_VERSION": concrete_version,
                "MCP_PORT": "8000",
                "PORT": "8000",
                "MCP_HTTP_MODE": "true",
                "DOCKER_CONTAINER": "true",
            },
            aliases=net_aliases,
            recreate=recreate,
        )
        if start_ok:
            logger.info(f"✅ JIT build+start complete for {container_name}")
        else:
            logger.error(f"❌ JIT start failed for {container_name}: {start_info}")
    except Exception as e:
        logger.error(f"❌ JIT build task crashed for {container_name}: {e}")
    finally:
        _INFLIGHT_BUILDS.discard(container_name)


@lifecycle_router.post("/deploy", dependencies=[Depends(require_internal_secret)])
async def deploy_connector_v1(request: DeployRequestV1) -> Dict[str, Any]:
    """
    Ensure a connector container is running (HTTP MCP mode).

    Default behavior is "start-only" (fast). If image is missing and build_if_missing=true,
    we build it from the connector's latest versioned snapshot.
    """
    tools_dir = os.getenv("TOOLS_DIR", "/app/shared/mcp-connectors")
    connector_id = _canonicalize_connector_id(request.connector_name)

    if not _managed_connectors_enabled():
        raise HTTPException(
            status_code=501,
            detail=(
                "Managed connector deployment is disabled for this tool-generator instance. "
                "Start the external `rsync-ai-mcp` stack (or set RSYNC_MANAGED_CONNECTORS=true to enable managed mode)."
            ),
        )

    # Resolve on-disk directory across the new nested layout:
    # - <base>/public/<category>/<id>
    # - <base>/internal/<id>
    # - legacy flat: <base>/<id>
    base = Path(tools_dir)
    if base.name in ("public", "internal"):
        base = base.parent

    candidates = {
        connector_id,
        connector_id.replace("-", "_"),
        connector_id.replace("_", "-"),
    }

    def find_connector_dir() -> Optional[Path]:
        # 1) internal direct
        internal_root = base / "internal"
        if internal_root.is_dir():
            for cand in candidates:
                p = internal_root / cand
                if p.is_dir():
                    return p

        # A public connector root is identified by latest.json (the version
        # pointer). Requiring it here stops an empty lock-only dir (e.g. a stray
        # flat public/mysql holding only .promotion.lock) from shadowing the
        # canonical category-layout root public/database/mysql. See
        # KI-MYSQL-DUP-DIR. (Internal step 1 is left as-is: context7 has no
        # latest.json; step 5 is the metadata-driven safety net.)
        def _is_root(p: Path) -> bool:
            return p.is_dir() and (p / "latest.json").is_file()

        # 2) public direct (flat)
        public_root = base / "public"
        if public_root.is_dir():
            for cand in candidates:
                p = public_root / cand
                if _is_root(p):
                    return p

            # 3) public category layout
            for cat in public_root.iterdir():
                if not cat.is_dir():
                    continue
                for cand in candidates:
                    p = cat / cand
                    if _is_root(p):
                        return p

        # 4) legacy flat under base
        for cand in candidates:
            p = base / cand
            if _is_root(p):
                return p

        # 5) metadata-driven fallback (future-proof)
        roots = [base, base / "public", base / "internal"]
        for r in roots:
            if not r.is_dir():
                continue
            try:
                for cdir in iter_connector_dirs(r):
                    meta_path = resolve_current_dir(cdir) / "metadata.json"
                    if not meta_path.exists():
                        continue
                    try:
                        data = json.loads(meta_path.read_text())
                    except Exception:
                        continue
                    cid = _canonicalize_connector_id(str(data.get("id") or data.get("connector_type") or cdir.name))
                    if cid == connector_id:
                        return cdir
            except Exception:
                continue
        return None

    connector_dir = find_connector_dir()
    if connector_dir is None:
        raise HTTPException(404, f"Connector '{request.connector_name}' not found in TOOLS_DIR")

    # Resolve concrete version
    requested_version = (request.version or "latest").strip()
    if requested_version == "":
        requested_version = "latest"

    if requested_version.lower() == "latest":
        latest_path = connector_dir / "latest.json"
        if not latest_path.exists():
            raise HTTPException(400, f"Connector '{connector_id}' is not versioned (missing latest.json)")
        try:
            latest = json.loads(latest_path.read_text())
        except Exception as e:
            raise HTTPException(500, f"Failed to read latest.json for '{connector_id}': {e}")
        concrete_version = (latest.get("current_version") or "").strip()
        if not concrete_version:
            raise HTTPException(500, f"latest.json for '{connector_id}' has no current_version")
    else:
        concrete_version = requested_version
        if not concrete_version.startswith("v"):
            concrete_version = f"v{concrete_version}"

    version_dir = connector_dir / "versions" / concrete_version
    if not version_dir.is_dir():
        raise HTTPException(404, f"Version path not found: {version_dir}")

    # Container naming: ALWAYS versioned.
    #
    # We resolve "latest" to a concrete version above, so runtime should never depend on a
    # stable container name like `rsync-ai-<id>-mcp` (which would break pinned pipelines).
    version_part = concrete_version.lstrip("v").replace(".", "-")
    container_name = f"rsync-ai-{connector_id}-v{version_part}-mcp"

    deploy_service = DeploymentService()

    # Attempt fast start first (no build).
    image_ref = f"mcp-{connector_id}:{concrete_version}"
    started = False
    built = False
    # Every JIT-deployed connector claims the unversioned hostname first, matching
    # mcp_generate_compose.py. The map below only adds legacy extras (aws_s3).
    net_aliases = [f"rsync-ai-{connector_id}-mcp"] + [
        a
        for a in _NETWORK_ALIASES_BY_CONNECTOR_ID.get(connector_id, [])
        if a != f"rsync-ai-{connector_id}-mcp"
    ]

    # Regenerate path: the on-disk code for this version was just (re)written, but an
    # image tagged mcp-<id>:<version> may already exist from a prior build. The
    # start-only fast path below would reuse that stale image (and reuse an
    # already-running container), silently shipping the OLD code. When force_rebuild is
    # set, rebuild THIS version and recreate its container instead. Self-heal /deploy
    # callers leave force_rebuild False and keep the start-only path.
    if request.force_rebuild:
        if container_name in _INFLIGHT_BUILDS:
            logger.info(f"🔁 Build already in flight for {container_name} — returning building=true")
        else:
            _INFLIGHT_BUILDS.add(container_name)
            asyncio.create_task(
                _build_and_start_in_background(
                    deploy_service=deploy_service,
                    connector_id=connector_id,
                    concrete_version=concrete_version,
                    version_dir=version_dir,
                    container_name=container_name,
                    net_aliases=net_aliases,
                    recreate=True,
                )
            )
            logger.info(
                f"🏗️  Force-rebuild {image_ref} in background for {container_name} (regenerate)"
            )
        return DeployResponseV1(
            success=True,
            connector_id=connector_id,
            version=concrete_version,
            container_name=container_name,
            image=image_ref,
            started=False,
            built=False,
            building=True,
        ).model_dump(exclude_none=False, exclude_defaults=False)

    start_ok, start_info = await deploy_service.docker_builder.start_container(
        image_name=image_ref,
        container_name=container_name,
        port=None,
        network=deploy_service.docker_network,
        env_vars={
            "MCP_CONNECTOR_NAME": connector_id,
            "MCP_CONNECTOR_VERSION": concrete_version,
            "MCP_PORT": "8000",
            "PORT": "8000",
            "MCP_HTTP_MODE": "true",
            "DOCKER_CONTAINER": "true",
        },
        aliases=net_aliases,
    )
    if start_ok:
        started = True
    else:
        # Image missing and a build is allowed: build IN THE BACKGROUND and return
        # immediately. A cold build can take minutes — far longer than the orchestrator's
        # deploy POST timeout — so blocking the response would just make the caller give
        # up. Instead we kick off the build as a detached task and report building=true;
        # the caller polls the container's /health (with an extended deadline) to learn
        # when it's ready. This is the path that serves pipelines pinned to an old version
        # whose image was pruned after a newer version was promoted.
        if request.build_if_missing and "Image not found" in (start_info or ""):
            if container_name in _INFLIGHT_BUILDS:
                logger.info(f"🔁 Build already in flight for {container_name} — returning building=true")
            else:
                _INFLIGHT_BUILDS.add(container_name)
                asyncio.create_task(
                    _build_and_start_in_background(
                        deploy_service=deploy_service,
                        connector_id=connector_id,
                        concrete_version=concrete_version,
                        version_dir=version_dir,
                        container_name=container_name,
                        net_aliases=net_aliases,
                    )
                )
                logger.info(
                    f"🏗️  JIT building {image_ref} in background for {container_name} "
                    f"(pinned version not pre-built)"
                )
            return DeployResponseV1(
                success=True,
                connector_id=connector_id,
                version=concrete_version,
                container_name=container_name,
                image=image_ref,
                started=False,
                built=False,
                building=True,
            ).model_dump(exclude_none=False, exclude_defaults=False)
        else:
            return DeployResponseV1(
                success=False,
                connector_id=connector_id,
                version=concrete_version,
                container_name=container_name,
                image=image_ref,
                started=False,
                built=False,
                error_message=start_info or "Failed to start container",
            ).model_dump(exclude_none=False, exclude_defaults=False)

    # Verify health (best-effort with retries). Containers often take a few seconds to boot.
    try:
        import time

        deadline = time.time() + 30.0  # seconds
        last_err: Optional[str] = None
        async with httpx.AsyncClient(timeout=2.0) as client:
            while time.time() < deadline:
                try:
                    resp = await client.get(f"http://{container_name}:8000/health")
                    if resp.status_code == 200:
                        last_err = None
                        break
                    last_err = f"HTTP {resp.status_code}"
                except Exception as e:
                    last_err = str(e)

                await asyncio.sleep(1.0)

        if last_err:
            return DeployResponseV1(
                success=False,
                connector_id=connector_id,
                version=concrete_version,
                container_name=container_name,
                image=image_ref,
                started=True,
                built=built,
                error_message=f"Health check failed: {last_err}",
            ).model_dump(exclude_none=False, exclude_defaults=False)
    except Exception as e:
        return DeployResponseV1(
            success=False,
            connector_id=connector_id,
            version=concrete_version,
            container_name=container_name,
            image=image_ref,
            started=True,
            built=built,
            error_message=f"Health check failed: {e}",
        ).model_dump(exclude_none=False, exclude_defaults=False)

    return DeployResponseV1(
        success=True,
        connector_id=connector_id,
        version=concrete_version,
        container_name=container_name,
        image=image_ref,
        started=started,
        built=built,
    ).model_dump(exclude_none=False, exclude_defaults=False)
