"""
Deployment Service

Orchestrates the full deployment pipeline for generated connectors:
1. Save artifacts to disk
2. Build Docker image
3. Start container
4. Register with backend-orchestrator
5. Verify deployment

Uses environment variables for configuration - no hardcoded paths.

VERSION: 2.0.0
"""

import os
import json
import asyncio
import shutil
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from enum import Enum

logger = logging.getLogger(__name__)

# Lazy imports to avoid circular dependencies
def _get_docker_builder():
    from .docker_builder import DockerBuilder
    return DockerBuilder

def _get_version_manager():
    from .version_manager import ConnectorVersionManager, VersionBumpType
    return ConnectorVersionManager, VersionBumpType


class DeploymentStatus(str, Enum):
    """Deployment status"""
    PENDING = "pending"
    SAVING = "saving"
    BUILDING = "building"
    STARTING = "starting"
    REGISTERING = "registering"
    VERIFYING = "verifying"
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


@dataclass
class DeploymentResult:
    """Result of connector deployment"""
    success: bool
    status: DeploymentStatus = DeploymentStatus.PENDING
    
    # Connector info
    connector_name: str = ""
    connector_type: str = ""
    output_path: str = ""
    # Concrete version tag used for this deploy (e.g., "v1.0.4")
    version: str = ""
    
    # Docker build info
    docker_image: str = ""
    docker_tag: str = ""
    build_time_seconds: float = 0
    
    # Container info
    container_id: str = ""
    container_name: str = ""
    container_port: int = 0
    
    # Verification
    verified: bool = False
    health_check_passed: bool = False
    
    # Timing
    total_time_ms: float = 0
    
    # Error info
    error_message: str = ""
    warnings: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "status": self.status.value,
            "connector_name": self.connector_name,
            "connector_type": self.connector_type,
            "output_path": self.output_path,
            "version": self.version,
            "docker": {
                "image": self.docker_image,
                "tag": self.docker_tag,
                "build_time_seconds": self.build_time_seconds,
            } if self.docker_image else None,
            "container": {
                "id": self.container_id,
                "name": self.container_name,
                "port": self.container_port,
            } if self.container_id else None,
            "verified": self.verified,
            "health_check_passed": self.health_check_passed,
            "total_time_ms": self.total_time_ms,
            "error_message": self.error_message,
            "warnings": self.warnings,
        }


@dataclass
class ConnectorArtifacts:
    """Container for all connector artifacts"""
    name: str
    code: str
    metadata_json: str
    requirements_txt: str
    dockerfile: Optional[str] = None
    spec_json: Optional[str] = None
    mock_profile_json: Optional[str] = None
    version: str = "1.0.0"
    
    def generate_dockerfile(self) -> str:
        """Generate a standard Dockerfile for MCP connectors"""
        return f"""# Auto-generated Dockerfile for {self.name} MCP Connector
FROM python:3.13-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy connector code
COPY . .

# Shared warehouse adapter pulled at build time via the `shared` named context —
# never bundled per-connector (anti-drift). Required for database/data_warehouse
# connectors to import it at runtime; harmless otherwise.
COPY --from=shared warehouse_adapters.py /app/warehouse_adapters.py

# Expose MCP port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \\
    CMD curl -f http://localhost:8000/health || exit 1

# Run connector
CMD ["python", "connector.py"]
"""


class DeploymentService:
    """
    Orchestrates connector deployment using OS filesystem and Docker SDK.
    
    All paths come from environment variables for production compatibility.
    
    Environment Variables:
        TOOLS_DIR: Base directory for connectors (default: /app/shared/mcp-connectors)
        DOCKER_NETWORK: Docker network for connectors (default: rsync-ai-mcp)
        MCP_PORT_START: Starting port for auto-assignment (default: 8090)
    """
    
    def __init__(self, connectors_path: str = None):
        """
        Initialize deployment service.
        
        Args:
            connectors_path: Override for connector storage path
        """
        # Get paths from environment (production-ready)
        self.connectors_path = Path(
            connectors_path or 
            os.getenv("TOOLS_DIR", "/app/shared/mcp-connectors")
        ).resolve()
        
        # JIT-deployed connectors land on the dedicated rsync-ai-mcp network — the
        # same network the compose-generated USER connectors use
        # (scripts/mcp_generate_compose.py) — so untrusted connector code is isolated
        # from the internal services (kafka/temporal/redis/postgres) that live on
        # rsync-ai_default (SEC-M-06). orchestrator, api-gateway, tool-generator, and
        # kafka-mcp-sink all bridge into rsync-ai-mcp, so they still resolve JIT
        # connectors by container name / alias there. Compose sets DOCKER_NETWORK
        # explicitly (all stacks now point here); this default matters only if unset.
        self.docker_network = os.getenv("DOCKER_NETWORK", "rsync-ai-mcp")
        self.port_start = int(os.getenv("MCP_PORT_START", "8090"))
        
        # Lazy-load Docker builder and version manager
        self._docker_builder = None
        self._version_manager = None
        
        # Status callback
        self._status_callback = None
        
        logger.info(f"DeploymentService initialized with connectors_path: {self.connectors_path}")
    
    @property
    def docker_builder(self):
        """Lazy-load Docker builder"""
        if self._docker_builder is None:
            DockerBuilder = _get_docker_builder()
            self._docker_builder = DockerBuilder()
        return self._docker_builder
    
    @property
    def version_manager(self):
        """Lazy-load version manager"""
        if self._version_manager is None:
            ConnectorVersionManager, _ = _get_version_manager()
            self._version_manager = ConnectorVersionManager(str(self.connectors_path))
        return self._version_manager
    
    def set_status_callback(self, callback):
        """Set callback for status updates"""
        self._status_callback = callback
    
    def _update_status(self, status: DeploymentStatus, details: Dict = None):
        """Update deployment status"""
        logger.info(f"Deployment status: {status.value}")
        if self._status_callback:
            self._status_callback(status, details or {})
    
    async def deploy(
        self,
        artifacts: ConnectorArtifacts,
        build_docker: bool = True,
        start_container: bool = True,
        verify: bool = True,
        validation_passed: bool = True,
    ) -> DeploymentResult:
        """
        Deploy a generated connector.
        
        Pipeline:
        1. Create directory structure
        2. Save all artifact files
        3. Build Docker image (if requested)
        4. Start container (if requested)
        5. Verify deployment (if requested)
        
        Args:
            artifacts: Connector artifacts to deploy
            build_docker: Whether to build Docker image
            start_container: Whether to start the container
            verify: Whether to verify deployment
        
        Returns:
            DeploymentResult with deployment status
        """
        start_time = datetime.now()
        
        result = DeploymentResult(
            success=False,
            connector_name=artifacts.name,
            connector_type=artifacts.name,
        )
        
        logger.info(f"🚀 Deploying connector: {artifacts.name}")
        
        try:
            # Determine a concrete version tag ONCE and use it consistently for:
            # - versions/<version>/ save
            # - Docker image tag
            # This avoids mismatches where the saved version differs from the built image tag.
            desired_version = (artifacts.version or "").strip()
            if desired_version == "" or desired_version.lower() == "latest":
                _, VersionBumpType = _get_version_manager()
                desired_version = self.version_manager.calculate_next_version(
                    artifacts.name,
                    bump_type=VersionBumpType.PATCH,
                )
            if not desired_version.startswith("v"):
                desired_version = f"v{desired_version}"
            artifacts.version = desired_version
            result.version = desired_version

            # Step 1: Create directory and save files
            self._update_status(DeploymentStatus.SAVING)
            
            output_dir = self.connectors_path / artifacts.name
            save_success = await self._save_artifacts(
                artifacts,
                output_dir,
                validation_passed=validation_passed,
                version=artifacts.version,
            )
            
            if not save_success:
                result.error_message = "Failed to save artifacts"
                result.status = DeploymentStatus.FAILED
                return result
            
            result.output_path = str(output_dir)
            logger.info(f"✅ Saved artifacts to: {output_dir}")
            
            # Step 2: Build Docker image
            if build_docker:
                self._update_status(DeploymentStatus.BUILDING)
                
                build_result = await self.docker_builder.build(
                    connector_dir=output_dir,
                    name=artifacts.name,
                    version=artifacts.version,
                )
                
                if build_result.success:
                    result.docker_image = build_result.image_name
                    result.docker_tag = build_result.image_tag
                    result.build_time_seconds = build_result.build_time_seconds
                    logger.info(f"✅ Built Docker image: {build_result.full_image_name}")
                else:
                    result.warnings.append(f"Docker build failed: {build_result.error_message}")
                    logger.warning(f"⚠️ Docker build failed: {build_result.error_message}")
                
                # Step 3: Start container (only if build succeeded)
                if build_result.success and start_container:
                    self._update_status(DeploymentStatus.STARTING)
                    
                    # MCP connectors don't need external port exposure
                    # They're accessed internally via Docker network DNS
                    
                    # Container naming: ALWAYS versioned.
                    #
                    # Even when callers ask for "latest", we resolve a concrete version earlier
                    # (artifacts.version) and run `rsync-ai-<id>-vX-Y-Z-mcp` so pipelines remain immutable.
                    version_part = artifacts.version.lstrip("v").replace(".", "-")
                    container_name = f"rsync-ai-{artifacts.name}-v{version_part}-mcp"
                    
                    start_success, container_info = await self.docker_builder.start_container(
                        image_name=build_result.full_image_name,
                        container_name=container_name,
                        # deploy() just (re)built the image at step 2 — recreate the
                        # container so it runs the fresh image, never a stale running one.
                        recreate=True,
                        port=None,  # No external port - internal access only
                        network=self.docker_network,
                        env_vars={
                            "MCP_CONNECTOR_NAME": artifacts.name,
                            "MCP_CONNECTOR_VERSION": artifacts.version,
                            "MCP_PORT": "8000",
                            "PORT": "8000",
                            "MCP_HTTP_MODE": "true",
                            "DOCKER_CONTAINER": "true",
                            # Ensure connector containers can decrypt tokens stored by tool-generator TokenManager
                            **(
                                {"OAUTH_ENCRYPTION_KEY": os.getenv("OAUTH_ENCRYPTION_KEY")}
                                if os.getenv("OAUTH_ENCRYPTION_KEY")
                                else {}
                            ),
                        }
                    )
                    
                    if start_success:
                        result.container_id = container_info
                        result.container_name = container_name
                        result.container_port = 8000  # Internal port only
                        logger.info(f"✅ Container started: {container_name} (internal access via Docker DNS)")
                    else:
                        result.warnings.append(f"Container start failed: {container_info}")
                        logger.warning(f"⚠️ Container start failed: {container_info}")
            
            # Step 4: Verify deployment
            if verify:
                self._update_status(DeploymentStatus.VERIFYING)
                result.verified = await self._verify_deployment(output_dir)
                
                if not result.verified:
                    result.warnings.append("Deployment verification failed")
            
            # Determine final status
            if result.warnings:
                result.status = DeploymentStatus.PARTIAL
                result.success = True  # Partial success - files saved
            else:
                result.status = DeploymentStatus.SUCCESS
                result.success = True
            
            self._update_status(result.status)
            
        except Exception as e:
            result.error_message = str(e)
            result.status = DeploymentStatus.FAILED
            logger.error(f"❌ Deployment failed: {e}")
        
        result.total_time_ms = (datetime.now() - start_time).total_seconds() * 1000
        logger.info(f"{'✅' if result.success else '❌'} Deployment complete: {artifacts.name} ({result.status.value})")
        
        return result
    
    async def _save_artifacts(
        self,
        artifacts: ConnectorArtifacts,
        output_dir: Path,
        use_versioning: bool = True,
        version: Optional[str] = None,
        changes: Optional[List[str]] = None,
        validation_passed: bool = True,
    ) -> bool:
        """
        Save all artifacts to disk using versioned storage.
        
        If use_versioning is True:
        1. Determine next version
        2. Save to versions/<version>/
        3. Update latest.json atomically
        4. Sync to top-level for backward compat
        
        If use_versioning is False:
        Uses legacy direct save to output_dir (for backward compat).
        
        Args:
            artifacts: Connector artifacts to save
            output_dir: Output directory (used as connector base path for versioning)
            use_versioning: Whether to use versioned storage (default: True)
            version: Specific version to use (default: auto-calculate)
            changes: List of changes for changelog
            validation_passed: Whether validation passed
        
        Returns:
            True if save successful
        """
        try:
            if not use_versioning:
                # Legacy mode: direct save
                return await self._save_artifacts_legacy(artifacts, output_dir)
            
            # Versioned mode
            connector_name = artifacts.name
            
            # Determine version
            if version is None:
                _, VersionBumpType = _get_version_manager()
                # Default to patch bump unless validation failed
                bump_type = VersionBumpType.PATCH if validation_passed else VersionBumpType.PATCH
                version = self.version_manager.calculate_next_version(connector_name, bump_type)
            
            logger.info(f"Saving {connector_name} {version} with versioning")

            # Normalize the on-disk version directory name vs the semantic version inside JSON.
            # - Directory: versions/vX.Y.Z
            # - JSON fields: "version": "X.Y.Z" (no leading "v" to satisfy ConnectorSpec validator)
            semver = str(version).lstrip("v")

            def _patch_json_version_and_aliases(raw_json: Optional[str]) -> Optional[str]:
                if not raw_json:
                    return raw_json
                try:
                    data = json.loads(raw_json)
                    if isinstance(data, dict):
                        data["version"] = semver
                        # De-dupe aliases (preserve order)
                        aliases = data.get("aliases")
                        if isinstance(aliases, list):
                            seen = set()
                            deduped = []
                            for a in aliases:
                                if a in seen:
                                    continue
                                deduped.append(a)
                                seen.add(a)
                            data["aliases"] = deduped
                    return json.dumps(data, indent=2, ensure_ascii=False)
                except Exception:
                    return raw_json

            # Patch metadata/spec JSONs so the generated artifacts reflect the concrete version.
            artifacts.metadata_json = _patch_json_version_and_aliases(artifacts.metadata_json) or artifacts.metadata_json
            artifacts.spec_json = _patch_json_version_and_aliases(artifacts.spec_json) if artifacts.spec_json else artifacts.spec_json
            
            # Prepare artifacts as dict
            artifacts_dict = {
                "connector.py": artifacts.code,
                "metadata.json": artifacts.metadata_json,
                "requirements.txt": artifacts.requirements_txt,
            }
            
            # Add optional files
            dockerfile = artifacts.dockerfile or artifacts.generate_dockerfile()
            artifacts_dict["Dockerfile"] = dockerfile
            
            if artifacts.spec_json:
                artifacts_dict["spec.json"] = artifacts.spec_json
            
            if artifacts.mock_profile_json:
                artifacts_dict["mock_profile.json"] = artifacts.mock_profile_json
            
            artifacts_dict["__init__.py"] = f'"""MCP Connector: {connector_name}"""\n'
            
            # Copy shared runtime helpers into the build context.
            # NOTE: TOOLS_DIR may point at /public; shared helpers typically live at the repo root
            # /shared/mcp-connectors (one level up from /public).
            def _read_shared_helper(rel_name: str) -> Optional[str]:
                candidates = [
                    self.connectors_path / rel_name,
                    self.connectors_path.parent / rel_name,
                    self.connectors_path.parent.parent / rel_name,
                ]
                for c in candidates:
                    try:
                        if c.exists():
                            return c.read_text()
                    except Exception:
                        continue
                return None

            base_connector_text = _read_shared_helper("base_connector.py")
            if base_connector_text:
                artifacts_dict["base_connector.py"] = base_connector_text

            # warehouse_adapters.py is intentionally NOT bundled per-connector. It
            # lives ONCE at public/warehouse_adapters.py and is pulled into every
            # image at build time via the Dockerfile's `COPY --from=shared
            # warehouse_adapters.py` (Dockerfile.j2 + the fallback generators). A
            # physical per-connector copy silently goes stale — it produced three
            # dead frozen copies (stripe/github-rest/petstore) carrying an old
            # load-job bug; see the Connector Patch Sync Ledger in
            # CAPABILITIES-ARCHIVE.md. base_connector.py still bundles (above): it
            # uses a different runtime-dedupe pattern, out of scope here.

            # Copy oauth helpers into the build context (required for OAuth2 connectors)
            oauth_dir = self.connectors_path / "oauth"
            if not oauth_dir.exists():
                oauth_dir = self.connectors_path.parent / "oauth"
            if oauth_dir.exists() and oauth_dir.is_dir():
                for p in oauth_dir.glob("*.py"):
                    artifacts_dict[f"oauth/{p.name}"] = p.read_text()
            
            # Save to versioned path
            version_path = self.version_manager.save_to_versioned_path(
                connector_name,
                version,
                artifacts_dict
            )
            
            # Download logo to versioned path
            await self._download_logo_to_path(connector_name, version_path)
            
            # Promote to latest (atomic operation with lock)
            if changes is None:
                changes = ["Generated connector"]
            
            promote_success = self.version_manager.promote_to_latest(
                connector_name,
                version,
                changes,
                validation_passed=validation_passed,
                breaking_changes=False,
            )
            
            if not promote_success:
                logger.warning(f"Promotion to latest failed for {connector_name} {version}")
                return False
            
            logger.info(f"✅ Saved and promoted {connector_name} {version}")

            # Register OAuth provider in shared/mcp-connectors/oauth/providers.json.
            # The agentic /v1/generate path runs through DeploymentService and
            # previously skipped this — the upsert lived only in
            # ConnectorArtifacts.save_to_directory (orchestrator) and the session
            # fast-path, neither of which this path invokes. Result: OAuth-capable
            # connectors got metadata.json oauth fields but were never added to
            # providers.json, so api-gateway's "Connect with <provider>" stayed
            # unavailable. Read from metadata.json (already on disk) and upsert.
            try:
                from ..agents.session_fast_path import _upsert_oauth_provider_from_metadata
                _md = json.loads(artifacts.metadata_json) if artifacts.metadata_json else {}
                _upsert_oauth_provider_from_metadata(version_path, _md)
            except Exception as _oauth_err:
                logger.warning(
                    f"OAuth provider registration skipped for {connector_name}: {_oauth_err}"
                )

            return True

        except Exception as e:
            logger.error(f"❌ Failed to save artifacts: {e}", exc_info=True)
            return False
    
    async def _save_artifacts_legacy(
        self,
        artifacts: ConnectorArtifacts,
        output_dir: Path,
    ) -> bool:
        """
        Legacy save method: directly to output_dir without versioning.
        Kept for backward compatibility.
        """
        try:
            # Ensure parent directory exists
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            
            logger.info(f"Saving artifacts to: {output_dir}")
            
            # Save connector.py
            connector_file = output_dir / "connector.py"
            connector_file.write_text(artifacts.code)
            logger.debug(f"Saved: {connector_file}")
            
            # Save metadata.json
            metadata_file = output_dir / "metadata.json"
            metadata_file.write_text(artifacts.metadata_json)
            logger.debug(f"Saved: {metadata_file}")
            
            # Save requirements.txt
            requirements_file = output_dir / "requirements.txt"
            requirements_file.write_text(artifacts.requirements_txt)
            logger.debug(f"Saved: {requirements_file}")
            
            # Save Dockerfile (generate if not provided)
            dockerfile = artifacts.dockerfile or artifacts.generate_dockerfile()
            dockerfile_file = output_dir / "Dockerfile"
            dockerfile_file.write_text(dockerfile)
            logger.debug(f"Saved: {dockerfile_file}")
            
            # Save spec.json if available
            if artifacts.spec_json:
                spec_file = output_dir / "spec.json"
                spec_file.write_text(artifacts.spec_json)
                logger.debug(f"Saved: {spec_file}")
            
            # Save mock_profile.json if available
            if artifacts.mock_profile_json:
                mock_file = output_dir / "mock_profile.json"
                mock_file.write_text(artifacts.mock_profile_json)
                logger.debug(f"Saved: {mock_file}")
            
            # Create __init__.py for Python import
            init_file = output_dir / "__init__.py"
            init_file.write_text(f'"""MCP Connector: {artifacts.name}"""\n')
            
            # Copy base_connector.py for Docker build context
            base_connector_src = self.connectors_path / "base_connector.py"
            if base_connector_src.exists():
                base_connector_dest = output_dir / "base_connector.py"
                import shutil
                shutil.copy2(str(base_connector_src), str(base_connector_dest))
                logger.debug(f"Copied base_connector.py to build context")

            # Copy oauth helpers into the build context (required for OAuth2 connectors)
            oauth_dir = self.connectors_path / "oauth"
            if oauth_dir.exists() and oauth_dir.is_dir():
                try:
                    import shutil
                    shutil.copytree(str(oauth_dir), str(output_dir / "oauth"), dirs_exist_ok=True)
                    logger.debug("Copied oauth/ to build context")
                except Exception as e:
                    logger.warning(f"Failed to copy oauth/ to build context: {e}")
            
            # Download logo for the connector
            await self._download_logo_to_path(artifacts.name, output_dir)
            
            # Verify files were created
            required_files = ["connector.py", "metadata.json", "requirements.txt", "Dockerfile"]
            for filename in required_files:
                if not (output_dir / filename).exists():
                    logger.error(f"Failed to create: {filename}")
                    return False
            
            logger.info(f"✅ All artifacts saved to: {output_dir}")
            return True
            
        except PermissionError as e:
            logger.error(f"Permission denied writing to {output_dir}: {e}")
            return False
        except OSError as e:
            logger.error(f"OS error saving artifacts: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to save artifacts: {e}")
            return False
    
    async def _download_logo_to_path(self, connector_name: str, output_dir: Path) -> bool:
        """
        Download logo for a connector to a specific path.
        
        Args:
            connector_name: Name of the connector
            output_dir: Directory to save logo to
        
        Returns:
            True if logo downloaded successfully, False otherwise
        """
        try:
            import importlib.util
            
            # Try multiple import approaches
            download_logo = None
            
            # Approach 1: Direct import
            try:
                from utils.logo_downloader import download_logo
            except ImportError:
                pass
            
            # Approach 2: Import with explicit path (try multiple locations)
            if download_logo is None:
                possible_paths = [
                    Path('/app/src/utils/logo_downloader.py'),  # Docker container
                    Path(__file__).parent.parent.parent / 'utils' / 'logo_downloader.py',  # Relative (local dev)
                ]
                for logo_downloader_path in possible_paths:
                    if logo_downloader_path.exists():
                        spec = importlib.util.spec_from_file_location("logo_downloader", logo_downloader_path)
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                        download_logo = module.download_logo
                        break
            
            if download_logo is not None:
                if connector_name:
                    logger.info(f"🎨 Downloading logo for {connector_name}...")
                    success, logo_filename = download_logo(connector_name, str(output_dir))
                    if success:
                        logger.info(f"✅ Logo downloaded: {logo_filename}")
                        return True
                    else:
                        logger.warning(f"⚠️ Could not download logo for {connector_name}")
                        return False
            else:
                logger.warning(f"⚠️ Logo downloader not available")
                return False
        except Exception as e:
            logger.warning(f"⚠️ Logo download failed: {e}")
            return False
    
    async def _verify_deployment(self, output_dir: Path) -> bool:
        """Verify connector deployment by checking files and syntax"""
        try:
            output_dir = Path(output_dir)
            
            # Check required files exist
            required_files = ["connector.py", "metadata.json", "requirements.txt", "Dockerfile"]
            for filename in required_files:
                filepath = output_dir / filename
                if not filepath.exists():
                    logger.warning(f"Missing file: {filepath}")
                    return False
                if filepath.stat().st_size == 0:
                    logger.warning(f"Empty file: {filepath}")
                    return False
            
            # Validate Python syntax
            import ast
            connector_code = (output_dir / "connector.py").read_text()
            try:
                ast.parse(connector_code)
            except SyntaxError as e:
                logger.warning(f"Connector syntax error: {e}")
                return False
            
            # Validate JSON files
            for json_file in ["metadata.json"]:
                try:
                    content = (output_dir / json_file).read_text()
                    json.loads(content)
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid {json_file}: {e}")
                    return False
            
            logger.info("✅ Deployment verification passed")
            return True
            
        except Exception as e:
            logger.error(f"Verification failed: {e}")
            return False
    
    async def undeploy(
        self,
        connector_name: str,
        remove_files: bool = False,
        remove_docker: bool = True,
    ) -> bool:
        """
        Undeploy a connector.
        
        Args:
            connector_name: Name of connector to undeploy
            remove_files: Whether to remove files from disk
            remove_docker: Whether to remove Docker container and image
        
        Returns:
            Success boolean
        """
        logger.info(f"🗑️ Undeploying connector: {connector_name}")
        
        success = True
        
        # Stop and remove container
        if remove_docker:
            try:
                # Versioned-only runtime: remove all versioned containers for this connector.
                connector_id = (connector_name or "").strip().lower().replace("_", "-")
                prefixes = [
                    f"rsync-ai-{connector_id}-v",
                    f"rsync-ai-{connector_id.replace('-', '_')}-v",
                ]
                legacy = [
                    f"rsync-ai-{connector_id}-mcp",
                    f"rsync-ai-{connector_id.replace('-', '_')}-mcp",
                ]

                # Remove versioned containers (best-effort). Enumerate through the
                # builder helper (not the raw .client) so this works in both the
                # legacy Docker-SDK mode and deployer mode. In deployer mode the RPC
                # contract has no list endpoint, so enumeration yields nothing and we
                # fall through to the deterministic legacy-name teardown below (which
                # routes through /v1/undeploy per name).
                try:
                    for cname in self.docker_builder.list_managed_container_names():
                        if not cname.endswith("-mcp"):
                            continue
                        if any(cname.startswith(p) for p in prefixes):
                            try:
                                await self.docker_builder.stop_container(cname)
                            except Exception:
                                pass
                            try:
                                await self.docker_builder.remove_container(cname)
                                logger.info(f"Removed container: {cname}")
                            except Exception:
                                pass
                except Exception:
                    pass

                # Remove legacy stable container names (best-effort cleanup)
                for cname in legacy:
                    try:
                        await self.docker_builder.stop_container(cname)
                    except Exception:
                        pass
                    try:
                        await self.docker_builder.remove_container(cname)
                        logger.info(f"Removed legacy container: {cname}")
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"Failed to remove container: {e}")
        
        # Remove files
        if remove_files:
            try:
                connector_dir = self.connectors_path / connector_name
                if connector_dir.exists():
                    shutil.rmtree(str(connector_dir))
                    logger.info(f"Removed files: {connector_dir}")
            except Exception as e:
                logger.error(f"Failed to remove files: {e}")
                success = False
        
        return success
    
    async def list_deployed(self) -> List[Dict[str, Any]]:
        """List all deployed connectors.

        Connectors use the versioned layout (``latest.json`` at root +
        ``versions/<current_version>/...``); there are no root copies of
        connector.py/metadata.json. Resolve each connector's current versioned
        dir via the shared resolver rather than reading root-level files —
        otherwise JIT-generated connectors (versioned-only) are silently
        under-reported. iter_connector_dirs also finds nested connectors
        (e.g. database/mysql, storage/aws-s3) the old top-level iterdir missed.
        """
        connectors: List[Dict[str, Any]] = []

        try:
            if not self.connectors_path.exists():
                return connectors
            from src.utils.connector_paths import (
                iter_connector_dirs,
                resolve_current_dir,
                resolve_current_version,
            )
            for d in iter_connector_dirs(self.connectors_path):
                cur = resolve_current_dir(d)
                if not (cur / "connector.py").exists():
                    continue
                info: Dict[str, Any] = {
                    "name": d.name,
                    "path": str(d),
                    "has_dockerfile": (cur / "Dockerfile").exists(),
                    "has_metadata": (cur / "metadata.json").exists(),
                }

                # Load metadata if available
                meta_path = cur / "metadata.json"
                if meta_path.exists():
                    try:
                        metadata = json.loads(meta_path.read_text())
                        info["display_name"] = metadata.get("name", d.name)
                        info["version"] = metadata.get(
                            "version", resolve_current_version(d) or "unknown"
                        )
                        info["category"] = metadata.get("category", "unknown")
                    except Exception:
                        pass

                connectors.append(info)

        except Exception as e:
            logger.error(f"Failed to list connectors: {e}")

        return connectors
    
    def deploy_sync(
        self,
        artifacts: ConnectorArtifacts,
        build_docker: bool = True,
        start_container: bool = True,
    ) -> DeploymentResult:
        """Synchronous wrapper for deploy"""
        return asyncio.run(self.deploy(
            artifacts,
            build_docker=build_docker,
            start_container=start_container
        ))
