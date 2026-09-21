"""
Spec Versioning and Migration

Manages spec versions and provides migration logic for upgrading old specs
to the current schema version.

Usage:
    from versioning import SPEC_VERSION, migrate_spec, is_compatible_version
    
    if not is_compatible_version(old_spec.get('spec_version')):
        migrated_spec = migrate_spec(old_spec)

VERSION: 1.0.0
"""

from typing import Dict, Any, Optional, Tuple, List, Callable
from packaging import version
import logging
import copy

logger = logging.getLogger(__name__)

# =============================================================================
# CURRENT SPEC VERSION
# =============================================================================

SPEC_VERSION = "1.1"

# Minimum supported version for migration (older versions cannot be migrated)
MIN_SUPPORTED_VERSION = "0.9"


# =============================================================================
# VERSION COMPATIBILITY
# =============================================================================

def parse_version(ver: str) -> Tuple[int, int]:
    """Parse version string to (major, minor) tuple"""
    try:
        parts = ver.split('.')
        major = int(parts[0]) if parts else 1
        minor = int(parts[1]) if len(parts) > 1 else 0
        return (major, minor)
    except (ValueError, IndexError):
        return (1, 0)


def is_compatible_version(spec_version: Optional[str]) -> bool:
    """
    Check if a spec version is compatible with current version.
    
    Args:
        spec_version: Version string from spec (e.g., "1.0", "0.9")
    
    Returns:
        True if spec can be loaded (possibly with migration)
    """
    if not spec_version:
        # Assume legacy spec without version field
        return True
    
    try:
        spec_ver = version.parse(spec_version)
        current_ver = version.parse(SPEC_VERSION)
        min_ver = version.parse(MIN_SUPPORTED_VERSION)
        
        # Compatible if spec version is between min and current
        return min_ver <= spec_ver <= current_ver
    except version.InvalidVersion:
        logger.warning(f"Invalid version format: {spec_version}")
        return False


def requires_migration(spec_version: Optional[str]) -> bool:
    """Check if a spec needs migration to current version"""
    if not spec_version:
        return True
    
    try:
        spec_ver = version.parse(spec_version)
        current_ver = version.parse(SPEC_VERSION)
        return spec_ver < current_ver
    except version.InvalidVersion:
        return True


# =============================================================================
# MIGRATION FUNCTIONS
# =============================================================================

# Registry of migration functions: (from_version, to_version) -> migration_fn
_migrations: Dict[Tuple[str, str], Callable[[Dict], Dict]] = {}


def register_migration(from_ver: str, to_ver: str):
    """Decorator to register a migration function"""
    def decorator(fn: Callable[[Dict], Dict]):
        _migrations[(from_ver, to_ver)] = fn
        return fn
    return decorator


# =============================================================================
# VERSION 0.9 -> 1.0 MIGRATION
# =============================================================================

@register_migration("0.9", "1.0")
def migrate_0_9_to_1_0(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Migrate spec from v0.9 to v1.0
    
    Changes:
    - 'type' field renamed to 'category'
    - 'required_config' list moved to 'config_fields' objects
    - 'auth_type' moved to 'auth.type'
    - Add default operations if missing
    """
    result = copy.deepcopy(spec)
    
    # Rename 'type' to 'category'
    if 'type' in result and 'category' not in result:
        result['category'] = result.pop('type')
    
    # Convert 'required_config' list to 'config_fields' objects
    if 'required_config' in result and 'config_fields' not in result:
        required = result.pop('required_config')
        optional = result.pop('optional_config', [])
        
        config_fields = []
        for field in required:
            config_fields.append({
                'name': field,
                'type': 'string',
                'required': True,
                'secret': field in ['password', 'api_key', 'secret', 'token'],
            })
        for field in optional:
            config_fields.append({
                'name': field,
                'type': 'string',
                'required': False,
            })
        
        result['config_fields'] = config_fields
    
    # Move auth_type to auth object
    if 'auth_type' in result and 'auth' not in result:
        auth_type = result.pop('auth_type')
        result['auth'] = {
            'type': auth_type,
            'header_name': result.pop('auth_header', 'Authorization'),
        }
    
    # Ensure operations exist
    if 'operations' not in result or not result['operations']:
        category = result.get('category', 'api_saas')
        result['operations'] = _get_default_operations_for_category(category)
    
    # Update spec version
    result['spec_version'] = '1.0'

    logger.info(f"Migrated spec '{result.get('name', 'unknown')}' from 0.9 to 1.0")
    return result


# =============================================================================
# VERSION 1.0 -> 1.1 MIGRATION
# =============================================================================

@register_migration("1.0", "1.1")
def migrate_1_0_to_1_1(spec: Dict[str, Any]) -> Dict[str, Any]:
    """Migrate spec from v1.0 to v1.1.

    Changes:
      - Adds `protocol` field (defaults to 'rest' — preserves behavior).
      - Each OperationConfig may carry a new optional `graphql` block;
        v1.0 specs simply omit it (None).

    This migration is purely additive: a v1.1 generator regenerates a v1.0
    spec byte-identically because protocol=rest matches the implicit default
    that was hard-coded in v1.0 templates.
    """
    result = copy.deepcopy(spec)
    if "protocol" not in result:
        result["protocol"] = "rest"
    result["spec_version"] = "1.1"
    logger.info(
        f"Migrated spec '{result.get('name', 'unknown')}' from 1.0 to 1.1 "
        f"(protocol='{result['protocol']}')"
    )
    return result


# =============================================================================
# LEGACY SPEC MIGRATION (no version field)
# =============================================================================

def migrate_legacy_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Migrate a legacy spec (no version field) to current version.
    
    This handles the oldest format from the initial tool generator.
    """
    result = copy.deepcopy(spec)
    
    # Infer connector name from class name if missing
    if 'name' not in result and 'class_name' in result:
        class_name = result['class_name']
        # Convert PascalCase to snake_case
        import re
        name = re.sub(r'(?<!^)(?=[A-Z])', '_', class_name).lower()
        name = name.replace('_mcp_server', '').replace('_connector', '')
        result['name'] = name
    
    # Set display name from name
    if 'display_name' not in result:
        name = result.get('name', 'Unknown')
        result['display_name'] = name.replace('_', ' ').title()
    
    # Infer category from connector name or base_url
    if 'category' not in result and 'type' not in result:
        result['category'] = _infer_category(result)
    
    # Set default auth
    if 'auth' not in result and 'auth_type' not in result:
        result['auth'] = {
            'type': 'bearer',
            'header_name': 'Authorization',
            'env_var': 'MCP_API_KEY',
        }
    
    # Add version
    result['spec_version'] = '0.9'
    
    # Now migrate through the chain
    return migrate_spec(result)


def _infer_category(spec: Dict[str, Any]) -> str:
    """Infer connector category from spec fields"""
    name = spec.get('name', '').lower()
    base_url = spec.get('base_url', '').lower()
    
    # Database patterns
    db_keywords = ['mysql', 'postgres', 'postgresql', 'sqlite', 'mariadb', 'oracle', 'sqlserver']
    if any(kw in name for kw in db_keywords):
        return 'relational_db'
    
    # Document DB patterns
    doc_keywords = ['mongo', 'couch', 'elastic', 'dynamo']
    if any(kw in name for kw in doc_keywords):
        return 'document_db'
    
    # Storage patterns
    storage_keywords = ['s3', 'gcs', 'minio', 'azure', 'blob', 'storage']
    if any(kw in name for kw in storage_keywords):
        return 'cloud_storage'
    
    # Warehouse patterns
    warehouse_keywords = ['snowflake', 'bigquery', 'redshift', 'databricks']
    if any(kw in name for kw in warehouse_keywords):
        return 'data_warehouse'
    
    # Streaming patterns
    streaming_keywords = ['kafka', 'kinesis', 'pubsub', 'event']
    if any(kw in name for kw in streaming_keywords):
        return 'streaming'
    
    # Default to API/SaaS
    return 'api_saas'


def _get_default_operations_for_category(category: str) -> List[Dict[str, Any]]:
    """Get default operations for a connector category"""
    # Core operations for all categories
    core_ops = [
        {'name': 'test_connection', 'type': 'core', 'description': 'Test connectivity'},
        {'name': 'validate_config', 'type': 'core', 'description': 'Validate configuration'},
        {'name': 'discover_schema', 'type': 'core', 'description': 'Discover schema'},
        {'name': 'get_capabilities', 'type': 'core', 'description': 'Get connector capabilities'},
    ]
    
    # Category-specific operations
    category_ops = {
        'relational_db': [
            {'name': 'export', 'type': 'source', 'description': 'Export data from table'},
            {'name': 'import_data', 'type': 'destination', 'description': 'Import data to table'},
        ],
        'document_db': [
            {'name': 'export', 'type': 'source', 'description': 'Export documents from collection'},
            {'name': 'import_data', 'type': 'destination', 'description': 'Import documents to collection'},
        ],
        'cloud_storage': [
            {'name': 'read', 'type': 'source', 'description': 'Read file from storage'},
            {'name': 'import_data', 'type': 'destination', 'description': 'Write data to storage'},
        ],
        'api_saas': [
            {'name': 'export', 'type': 'source', 'description': 'Export data from API'},
        ],
        'streaming': [
            {'name': 'consume', 'type': 'source', 'description': 'Consume messages'},
            {'name': 'produce', 'type': 'destination', 'description': 'Produce messages'},
        ],
        'data_warehouse': [
            {'name': 'query', 'type': 'source', 'description': 'Execute query'},
            {'name': 'load', 'type': 'destination', 'description': 'Load data'},
        ],
    }
    
    return core_ops + category_ops.get(category, [])


# =============================================================================
# MAIN MIGRATION FUNCTION
# =============================================================================

def migrate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Migrate a spec to the current version.
    
    Applies migration functions in sequence until reaching SPEC_VERSION.
    
    Args:
        spec: The spec dict to migrate
    
    Returns:
        Migrated spec dict with updated spec_version
    
    Raises:
        ValueError: If spec cannot be migrated (too old or invalid)
    """
    current_spec = copy.deepcopy(spec)
    
    # Handle legacy specs without version
    if 'spec_version' not in current_spec:
        logger.info("Migrating legacy spec (no version)")
        return migrate_legacy_spec(spec)
    
    current_ver = current_spec.get('spec_version', '0.9')
    
    # Check if migration is needed
    if not requires_migration(current_ver):
        return current_spec
    
    # Check minimum version
    if not is_compatible_version(current_ver):
        raise ValueError(
            f"Spec version {current_ver} is too old. "
            f"Minimum supported version is {MIN_SUPPORTED_VERSION}"
        )
    
    # Get migration path
    migration_path = _get_migration_path(current_ver, SPEC_VERSION)
    
    # Apply migrations in sequence
    for from_ver, to_ver in migration_path:
        migration_key = (from_ver, to_ver)
        if migration_key in _migrations:
            logger.info(f"Applying migration: {from_ver} -> {to_ver}")
            current_spec = _migrations[migration_key](current_spec)
        else:
            logger.warning(f"No migration found for {from_ver} -> {to_ver}")
    
    # Ensure final version is set
    current_spec['spec_version'] = SPEC_VERSION
    
    return current_spec


def _get_migration_path(from_ver: str, to_ver: str) -> List[Tuple[str, str]]:
    """
    Get the sequence of migrations needed to go from one version to another.
    
    Returns list of (from_version, to_version) tuples.
    """
    # For now, simple linear path
    # In future, could support branching versions

    version_order = ['0.9', '1.0', '1.1']
    
    try:
        from_idx = version_order.index(from_ver)
        to_idx = version_order.index(to_ver)
    except ValueError:
        return []
    
    path = []
    for i in range(from_idx, to_idx):
        path.append((version_order[i], version_order[i + 1]))
    
    return path


# =============================================================================
# VALIDATION
# =============================================================================

def validate_spec_version(spec: Dict[str, Any]) -> List[str]:
    """
    Validate spec version and return any warnings/issues.
    
    Returns:
        List of warning messages (empty if valid)
    """
    warnings = []
    
    spec_version = spec.get('spec_version')
    
    if not spec_version:
        warnings.append("Missing spec_version field - assuming legacy spec")
    elif not is_compatible_version(spec_version):
        warnings.append(f"Spec version {spec_version} may not be fully compatible")
    elif requires_migration(spec_version):
        warnings.append(f"Spec version {spec_version} will be migrated to {SPEC_VERSION}")
    
    return warnings


# =============================================================================
# EXPORT SPEC TO YAML/JSON
# =============================================================================

def export_spec_to_dict(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Export spec to a clean dict suitable for YAML/JSON serialization.
    
    Ensures spec_version is included.
    """
    result = copy.deepcopy(spec)
    result['spec_version'] = SPEC_VERSION
    return result


def get_spec_version_info() -> Dict[str, Any]:
    """Get information about current and supported versions"""
    return {
        'current_version': SPEC_VERSION,
        'min_supported_version': MIN_SUPPORTED_VERSION,
        'available_migrations': list(_migrations.keys()),
    }

