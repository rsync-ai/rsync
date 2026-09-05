"""
Version Manager for Connectors

Manages connector versioning, including:
- Version calculation (semver)
- latest.json management
- Atomic promotion from sandbox to canonical
- Version history and changelog

VERSION: 1.0.0
"""

import os
import json
import fcntl
import hashlib
import tempfile
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from enum import Enum

logger = logging.getLogger(__name__)


class VersionBumpType(str, Enum):
    """Type of version bump"""
    MAJOR = "major"  # Breaking changes
    MINOR = "minor"  # New features, non-breaking
    PATCH = "patch"  # Bug fixes


@dataclass
class VersionInfo:
    """Information about a connector version"""
    version: str
    released: str
    changes: List[str] = field(default_factory=list)
    breaking_changes: bool = False
    validation_passed: bool = False
    deprecated: bool = False
    deprecation_reason: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        result = {
            "released": self.released,
            "changes": self.changes,
            "breaking_changes": self.breaking_changes,
            "validation_passed": self.validation_passed,
        }
        if self.deprecated:
            result["deprecated"] = True
            if self.deprecation_reason:
                result["deprecation_reason"] = self.deprecation_reason
        return result


@dataclass
class LatestManifest:
    """Contents of latest.json"""
    current_version: str
    updated_at: str
    changelog: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    all_versions: List[str] = field(default_factory=list)
    deprecated_versions: List[str] = field(default_factory=list)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LatestManifest":
        """Create from dictionary"""
        return cls(
            current_version=data["current_version"],
            updated_at=data["updated_at"],
            changelog=data.get("changelog", {}),
            all_versions=data.get("all_versions", []),
            deprecated_versions=data.get("deprecated_versions", []),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "current_version": self.current_version,
            "updated_at": self.updated_at,
            "changelog": self.changelog,
            "all_versions": self.all_versions,
            "deprecated_versions": self.deprecated_versions,
        }


class ConnectorVersionManager:
    """
    Manages connector versions with atomic promotion and latest.json.
    
    Key features:
    - Generates connectors to versioned sandbox paths first
    - Updates latest.json atomically using file rename
    - Promotes to top-level only after validation passes
    - Maintains changelog and version history
    """
    
    def __init__(self, connectors_base_path: str = None):
        """
        Initialize version manager.
        
        Args:
            connectors_base_path: Base path for all connectors (e.g., /shared/mcp-connectors)
        """
        base = connectors_base_path or os.getenv("TOOLS_DIR", "/app/shared/mcp-connectors")
        base_path = Path(base)
        # Prefer writing generated (public) connectors into /public when available.
        if base_path.name not in ("public", "internal") and (base_path / "public").is_dir():
            base_path = base_path / "public"
        self.base_path = base_path
        logger.info(f"ConnectorVersionManager initialized with base_path: {self.base_path}")
    
    def get_connector_path(self, connector_name: str) -> Path:
        """Resolve a connector's base directory.

        A connector root is identified by ``latest.json`` (the version pointer).
        Prefer an existing root — flat ``<base>/<id>`` or category-layout
        ``<base>/<category>/<id>`` — so we never write the version tree or the
        promotion lock into a stray flat dir that shadows a canonical
        category-layout connector (e.g. ``public/mysql`` masking
        ``public/database/mysql`` — see KI-MYSQL-DUP-DIR). For a brand-new
        connector with no existing root, default to the flat layout.
        """
        flat = self.base_path / connector_name
        if (flat / "latest.json").is_file():
            return flat
        # Look one level down for a category-layout root: <base>/<category>/<id>
        if self.base_path.is_dir():
            for cat in self.base_path.iterdir():
                if not cat.is_dir():
                    continue
                candidate = cat / connector_name
                if (candidate / "latest.json").is_file():
                    return candidate
        return flat
    
    def get_versions_path(self, connector_name: str) -> Path:
        """Get the versions directory for a connector"""
        return self.get_connector_path(connector_name) / "versions"
    
    def get_version_path(self, connector_name: str, version: str) -> Path:
        """Get the path for a specific version"""
        return self.get_versions_path(connector_name) / version
    
    def get_latest_json_path(self, connector_name: str) -> Path:
        """Get the path to latest.json"""
        return self.get_connector_path(connector_name) / "latest.json"
    
    def read_latest_manifest(self, connector_name: str) -> Optional[LatestManifest]:
        """
        Read latest.json for a connector.
        
        Returns None if file doesn't exist (first generation).
        """
        latest_json_path = self.get_latest_json_path(connector_name)
        
        if not latest_json_path.exists():
            logger.debug(f"latest.json not found for {connector_name} (first generation)")
            return None
        
        try:
            with open(latest_json_path, 'r') as f:
                data = json.load(f)
            return LatestManifest.from_dict(data)
        except Exception as e:
            logger.error(f"Failed to read latest.json for {connector_name}: {e}")
            return None
    
    def calculate_next_version(
        self,
        connector_name: str,
        bump_type: VersionBumpType = VersionBumpType.PATCH,
        force_version: Optional[str] = None,
    ) -> str:
        """
        Calculate the next version number.
        
        Args:
            connector_name: Name of the connector
            bump_type: Type of version bump (major/minor/patch)
            force_version: Force a specific version (overrides bump_type)
        
        Returns:
            Next version string (e.g., "v1.2.3")
        """
        if force_version:
            # Ensure it starts with 'v'
            return force_version if force_version.startswith('v') else f'v{force_version}'
        
        manifest = self.read_latest_manifest(connector_name)
        
        if not manifest:
            # First version
            return "v1.0.0"
        
        # Parse current version
        current = manifest.current_version.lstrip('v')
        try:
            major, minor, patch = map(int, current.split('.'))
        except ValueError:
            logger.warning(f"Invalid version format: {manifest.current_version}, defaulting to v1.0.0")
            return "v1.0.0"
        
        # Bump version
        if bump_type == VersionBumpType.MAJOR:
            major += 1
            minor = 0
            patch = 0
        elif bump_type == VersionBumpType.MINOR:
            minor += 1
            patch = 0
        else:  # PATCH
            patch += 1
        
        return f"v{major}.{minor}.{patch}"
    
    def save_to_versioned_path(
        self,
        connector_name: str,
        version: str,
        artifacts: Dict[str, str],
    ) -> Path:
        """
        Save connector artifacts to a versioned sandbox path.
        
        Args:
            connector_name: Name of the connector
            version: Version string (e.g., "v1.0.0")
            artifacts: Dict mapping filename -> content
        
        Returns:
            Path to the saved version directory
        """
        version_path = self.get_version_path(connector_name, version)
        version_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Saving {connector_name} {version} to {version_path}")
        
        for filename, content in artifacts.items():
            file_path = version_path / filename
            # Support nested artifact paths (e.g., oauth/token_manager.py)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(content)
            logger.debug(f"Saved {filename} to {file_path}")
        
        return version_path
    
    def promote_to_latest(
        self,
        connector_name: str,
        version: str,
        changes: List[str],
        validation_passed: bool = True,
        breaking_changes: bool = False,
    ) -> bool:
        """
        Promote a version to latest with atomic operation and lock.
        
        This implements the two-phase commit:
        1. Update latest.json atomically (via temp file + rename)
        2. Sync top-level files from versioned path
        
        Args:
            connector_name: Name of the connector
            version: Version to promote
            changes: List of changes for changelog
            validation_passed: Whether validation passed
            breaking_changes: Whether this version has breaking changes
        
        Returns:
            True if promotion successful
        """
        # ENFORCEMENT RULE:
        # Never update latest.json when validation did not pass.
        # This ensures runtime version resolution remains safe (fail-closed) and prevents
        # broken connectors from being marked as "latest".
        if not validation_passed:
            logger.warning(
                f"Not promoting {connector_name} {version} to latest because validation_passed=false"
            )
            return False

        connector_path = self.get_connector_path(connector_name)
        version_path = self.get_version_path(connector_name, version)
        
        if not version_path.exists():
            logger.error(f"Version {version} not found at {version_path}")
            return False
        
        # Acquire lock for atomic promotion
        lock_file_path = connector_path / ".promotion.lock"
        connector_path.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(lock_file_path, 'w') as lock_file:
                # Acquire exclusive lock
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                
                try:
                    # Read or create manifest
                    manifest = self.read_latest_manifest(connector_name)
                    
                    if manifest is None:
                        # First version
                        manifest = LatestManifest(
                            current_version=version,
                            updated_at=datetime.now().isoformat(),
                            all_versions=[version],
                            deprecated_versions=[],
                            changelog={}
                        )
                    else:
                        # Update manifest
                        old_version = manifest.current_version
                        manifest.current_version = version
                        manifest.updated_at = datetime.now().isoformat()
                        
                        if version not in manifest.all_versions:
                            manifest.all_versions.append(version)
                    
                    # Add version info to changelog
                    version_info = VersionInfo(
                        version=version,
                        released=datetime.now().isoformat(),
                        changes=changes,
                        breaking_changes=breaking_changes,
                        validation_passed=validation_passed,
                    )
                    manifest.changelog[version] = version_info.to_dict()
                    
                    # Write latest.json atomically (temp file + rename)
                    latest_json_path = self.get_latest_json_path(connector_name)
                    
                    with tempfile.NamedTemporaryFile(
                        mode='w',
                        dir=connector_path,
                        delete=False,
                        suffix='.json'
                    ) as temp_file:
                        json.dump(manifest.to_dict(), temp_file, indent=2)
                        temp_path = temp_file.name
                    
                    # Atomic rename
                    os.rename(temp_path, latest_json_path)
                    logger.info(f"✅ Updated latest.json for {connector_name} to {version}")

                    # NOTE: connector artifacts are NOT copied back to the connector
                    # root. The canonical source is versions/<current_version>/ and all
                    # readers resolve it via src/utils/connector_paths. latest.json (the
                    # version pointer, written above) is the only file at the connector
                    # root. (Removed the old _sync_top_level_files root-copy duplication.)
                    return True
                    
                finally:
                    # Release lock
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        
        except Exception as e:
            logger.error(f"Failed to promote {connector_name} {version}: {e}", exc_info=True)
            return False
    
    def deprecate_version(
        self,
        connector_name: str,
        version: str,
        reason: str,
    ) -> bool:
        """
        Mark a version as deprecated.
        
        Args:
            connector_name: Name of the connector
            version: Version to deprecate
            reason: Reason for deprecation
        
        Returns:
            True if successful
        """
        lock_file_path = self.get_connector_path(connector_name) / ".promotion.lock"
        
        try:
            with open(lock_file_path, 'w') as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                
                try:
                    manifest = self.read_latest_manifest(connector_name)
                    if not manifest:
                        logger.error(f"No manifest found for {connector_name}")
                        return False
                    
                    # Update changelog entry
                    if version in manifest.changelog:
                        manifest.changelog[version]["deprecated"] = True
                        manifest.changelog[version]["deprecation_reason"] = reason
                    
                    # Add to deprecated list
                    if version not in manifest.deprecated_versions:
                        manifest.deprecated_versions.append(version)
                    
                    # Write atomically
                    latest_json_path = self.get_latest_json_path(connector_name)
                    with tempfile.NamedTemporaryFile(
                        mode='w',
                        dir=self.get_connector_path(connector_name),
                        delete=False,
                        suffix='.json'
                    ) as temp_file:
                        json.dump(manifest.to_dict(), temp_file, indent=2)
                        temp_path = temp_file.name
                    
                    os.rename(temp_path, latest_json_path)
                    logger.info(f"✅ Deprecated {connector_name} {version}: {reason}")
                    return True
                    
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        
        except Exception as e:
            logger.error(f"Failed to deprecate {connector_name} {version}: {e}", exc_info=True)
            return False
    
    def compute_directory_hash(self, path: Path) -> str:
        """
        Compute hash of all files in a directory for integrity checking.
        
        Args:
            path: Directory path
        
        Returns:
            SHA256 hex digest
        """
        hasher = hashlib.sha256()
        
        # Get all files sorted for consistent hashing
        files = sorted(path.rglob('*'))
        
        for file_path in files:
            if file_path.is_file():
                # Hash filename (relative)
                rel_path = file_path.relative_to(path)
                hasher.update(str(rel_path).encode())
                
                # Hash file contents
                with open(file_path, 'rb') as f:
                    for chunk in iter(lambda: f.read(4096), b''):
                        hasher.update(chunk)
        
        return hasher.hexdigest()
    
    def list_versions(self, connector_name: str) -> List[str]:
        """
        List all versions for a connector.
        
        Returns:
            List of version strings (e.g., ["v1.0.0", "v1.1.0"])
        """
        versions_path = self.get_versions_path(connector_name)
        
        if not versions_path.exists():
            return []
        
        versions = []
        for version_dir in versions_path.iterdir():
            if version_dir.is_dir():
                versions.append(version_dir.name)
        
        return sorted(versions, reverse=True)  # Latest first

