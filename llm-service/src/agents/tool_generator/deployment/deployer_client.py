"""
Deployer RPC client (SEC-H-02).

`tool-generator` runs LLM/user-influenced connector code, so it drops root + the
Docker socket and delegates every container build/spawn/teardown to the small,
trusted `connector-deployer` service over an authenticated internal RPC. This
module is the thin client for that RPC; it maps 1:1 to
``connector-deployer/CONTRACT.md``:

    POST /v1/deploy    build-if-missing + run  (the JIT connector spawn)
    POST /v1/undeploy  stop + remove           (idempotent)
    GET  /v1/status    exists / status / id

The client is selected by ``DockerBuilder`` **only** when ``DEPLOYER_URL`` is set
(the one-release rollback feature flag). When it is unset, tool-generator keeps
its in-process Docker-SDK path and this module is never imported.

Design notes:
- Requests carry ``X-Internal-Secret: <INTERNAL_SERVICE_SECRET>`` on every call,
  mirroring ``routes.py::require_internal_secret`` on the server side.
- The request body is **DATA only** — no HostConfig-adjacent fields. The image
  ref is DERIVED server-side as ``mcp-<connector_id>:<version>`` and is never sent.
- ``context_subdir`` is the connector artifacts dir **relative to** ``TOOLS_DIR``
  (``/app/shared/mcp-connectors``). The deployer mounts the same tree read-only
  and builds server-side, so both sides address the artifacts by the same
  relative path. We resolve + relativize here (rejecting any ``..`` escape).
- Methods are synchronous (one HTTP round-trip each); ``DockerBuilder`` calls
  them from a thread executor so the event loop is never blocked, mirroring the
  legacy ``_run_docker_build`` pattern.
"""

from __future__ import annotations

import os
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

# Cold builds run server-side inside /v1/deploy and can take minutes (the legacy
# CLI build used a 900s cap). Keep a short connect timeout but a generous read
# timeout so a first-time image build isn't cut off mid-flight.
_DEFAULT_DEPLOY_TIMEOUT = float(os.getenv("DEPLOYER_DEPLOY_TIMEOUT", "900"))
_DEFAULT_RPC_TIMEOUT = float(os.getenv("DEPLOYER_RPC_TIMEOUT", "30"))
_DEFAULT_CONNECT_TIMEOUT = float(os.getenv("DEPLOYER_CONNECT_TIMEOUT", "10"))


class DeployerError(RuntimeError):
    """Raised for any deployer RPC failure (transport, timeout, non-2xx, bad body).

    ``status_code`` carries the HTTP status when the failure was an error
    response; it is ``None`` for transport/timeout failures.
    """

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class DeployerClient:
    """Authenticated client for the connector-deployer RPC.

    All configuration is read from process env by default (so a caller cannot
    influence the deployer target/secret); the constructor args exist for tests.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        secret: Optional[str] = None,
        tools_dir: Optional[str] = None,
        *,
        deploy_timeout: Optional[float] = None,
        rpc_timeout: Optional[float] = None,
        transport: Optional[httpx.BaseTransport] = None,
    ):
        self.base_url = (
            base_url if base_url is not None else os.getenv("DEPLOYER_URL", "")
        ).strip().rstrip("/")
        self.secret = (
            secret if secret is not None else os.getenv("INTERNAL_SERVICE_SECRET", "")
        ).strip()
        self.tools_dir = tools_dir or os.getenv("TOOLS_DIR", "/app/shared/mcp-connectors")
        self._deploy_timeout = deploy_timeout if deploy_timeout is not None else _DEFAULT_DEPLOY_TIMEOUT
        self._rpc_timeout = rpc_timeout if rpc_timeout is not None else _DEFAULT_RPC_TIMEOUT
        # Injected in tests (httpx.MockTransport). None ⇒ real network transport.
        self._transport = transport

    # ------------------------------------------------------------------ #
    # context_subdir resolution
    # ------------------------------------------------------------------ #
    def resolve_context_subdir(self, connector_id: str, version: str) -> str:
        """Resolve ``<connector artifacts>/versions/<version>`` and return it
        RELATIVE to ``TOOLS_DIR``.

        Mirrors the connector-dir discovery in ``routes.py`` (nested public
        category layout, internal, and legacy flat) so the relative path we hand
        the deployer matches the read-only tree it mounts. Rejects any path that
        escapes ``TOOLS_DIR`` (no ``..`` segment) per the contract.
        """
        tools_dir = Path(self.tools_dir)
        base = tools_dir
        if base.name in ("public", "internal"):
            base = base.parent

        candidates = {
            connector_id,
            connector_id.replace("-", "_"),
            connector_id.replace("_", "-"),
        }
        # Try the version both as-given and with the conventional leading "v".
        versions_to_try: List[str] = []
        for v in (version, f"v{version}" if version and not version.startswith("v") else version):
            if v and v not in versions_to_try:
                versions_to_try.append(v)

        roots: List[Path] = []
        internal_root = base / "internal"
        for cand in candidates:
            roots.append(internal_root / cand)
        public_root = base / "public"
        for cand in candidates:
            roots.append(public_root / cand)
        if public_root.is_dir():
            try:
                for cat in sorted(public_root.iterdir()):
                    if cat.is_dir():
                        for cand in candidates:
                            roots.append(cat / cand)
            except OSError:
                pass
        for cand in candidates:
            roots.append(base / cand)

        for root in roots:
            for v in versions_to_try:
                version_dir = root / "versions" / v
                if version_dir.is_dir():
                    return self._relativize(version_dir)

        raise DeployerError(
            f"cannot resolve connector context dir for '{connector_id}' "
            f"version '{version}' under TOOLS_DIR={self.tools_dir}"
        )

    def _relativize(self, version_dir: Path) -> str:
        """Return ``version_dir`` relative to ``TOOLS_DIR``; reject escapes."""
        tools_dir = Path(self.tools_dir)
        rel = os.path.relpath(str(version_dir), str(tools_dir))
        parts = Path(rel).parts
        if os.path.isabs(rel) or ".." in parts:
            raise DeployerError(
                f"resolved context dir {version_dir} escapes TOOLS_DIR={self.tools_dir} "
                f"(relative={rel!r})"
            )
        # Normalise to forward slashes — the deployer resolves a POSIX subpath.
        return rel.replace(os.sep, "/")

    # ------------------------------------------------------------------ #
    # RPC endpoints
    # ------------------------------------------------------------------ #
    def deploy(
        self,
        *,
        connector_id: str,
        version: str,
        container_name: str,
        aliases: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = None,
        port: int = 0,
        recreate: bool = False,
        build_args: Optional[Dict[str, str]] = None,
        context_subdir: Optional[str] = None,
    ) -> Dict[str, Any]:
        """POST /v1/deploy — build-if-missing + run.

        ``context_subdir`` is resolved from ``connector_id``/``version`` when not
        supplied. Returns the parsed JSON body on ``200``.
        """
        if context_subdir is None:
            context_subdir = self.resolve_context_subdir(connector_id, version)
        payload = {
            "connector_id": connector_id,
            "version": version,
            "context_subdir": context_subdir,
            "container_name": container_name,
            "aliases": list(aliases or []),
            "env": dict(env or {}),
            "port": int(port or 0),
            "recreate": bool(recreate),
            "build_args": dict(build_args or {}),
        }
        return self._request("POST", "/v1/deploy", json_body=payload, timeout=self._deploy_timeout)

    def undeploy(self, container_name: str) -> Dict[str, Any]:
        """POST /v1/undeploy — stop + remove (idempotent)."""
        return self._request(
            "POST", "/v1/undeploy", json_body={"container_name": container_name},
            timeout=self._rpc_timeout,
        )

    def status(self, name: str) -> Dict[str, Any]:
        """GET /v1/status?name=<container_name>."""
        return self._request(
            "GET", "/v1/status", params={"name": name}, timeout=self._rpc_timeout,
        )

    def healthz(self) -> Dict[str, Any]:
        """GET /healthz — daemon reachability (no auth required, but harmless)."""
        return self._request("GET", "/healthz", timeout=self._rpc_timeout)

    # ------------------------------------------------------------------ #
    # transport
    # ------------------------------------------------------------------ #
    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.secret:
            headers["X-Internal-Secret"] = self.secret
        return headers

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: float,
    ) -> Dict[str, Any]:
        if not self.base_url:
            raise DeployerError("DEPLOYER_URL is not configured")

        url = f"{self.base_url}{path}"
        timeout_cfg = httpx.Timeout(timeout, connect=_DEFAULT_CONNECT_TIMEOUT)
        try:
            with httpx.Client(transport=self._transport, timeout=timeout_cfg) as client:
                resp = client.request(
                    method, url, json=json_body, params=params, headers=self._headers(),
                )
        except httpx.TimeoutException as exc:
            raise DeployerError(f"deployer {method} {path} timed out: {exc}") from exc
        except httpx.RequestError as exc:
            raise DeployerError(f"deployer {method} {path} request failed: {exc}") from exc

        if resp.status_code >= 400:
            raise DeployerError(
                f"deployer {method} {path} returned {resp.status_code}: "
                f"{self._error_detail(resp)}",
                status_code=resp.status_code,
            )
        try:
            data = resp.json()
        except ValueError as exc:
            raise DeployerError(
                f"deployer {method} {path} returned non-JSON body: {resp.text[:200]!r}"
            ) from exc
        if not isinstance(data, dict):
            raise DeployerError(
                f"deployer {method} {path} returned a non-object JSON body: {data!r}"
            )
        return data

    @staticmethod
    def _error_detail(resp: httpx.Response) -> str:
        """Best-effort extraction of a human-readable error detail from a response."""
        try:
            body = resp.json()
        except ValueError:
            return (resp.text or "")[:300]
        if isinstance(body, dict):
            for key in ("detail", "error", "message"):
                if body.get(key):
                    return str(body[key])[:300]
        return str(body)[:300]
