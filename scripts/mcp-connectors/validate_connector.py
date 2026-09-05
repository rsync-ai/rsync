#!/usr/bin/env python3
"""
MCP Connector Validator (v2.0)
Ensures connectors follow the generic AI agent interface.

CHECKS:
1. Directory structure (connector.py, metadata.json, requirements.txt)
2. Metadata fields and capabilities
3. Class structure and base inheritance
4. Required methods implementation
5. Response format validation
6. Dry-run validation (can instantiate and call basic methods)

Usage:
    python validate_connector.py <connector_name>
    python validate_connector.py --all
    python validate_connector.py --all --strict  # Fail on warnings
"""

import sys
import os
import json
import importlib.util
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple


# Connectors live in shared/mcp-connectors/, NOT next to this script. The canonical
# source for every connector is its current versioned directory —
# <connector_root>/versions/<latest.json .current_version>/ — and the connector root
# holds only latest.json. Mirrors _resolve_current_dir() in scripts/mcp_generate_compose.py.
CONNECTORS_ROOT = (Path(__file__).resolve().parents[2] / "shared" / "mcp-connectors")


def _resolve_current_dir(connector_dir: Path) -> Path:
    """Connector root -> versions/<current_version>/. Falls back to the connector
    dir itself for any flat-layout connector."""
    latest_path = connector_dir / "latest.json"
    if latest_path.is_file():
        try:
            cv = (json.loads(latest_path.read_text()).get("current_version") or "").strip()
        except Exception:
            cv = ""
        if cv:
            vdir = connector_dir / "versions" / cv
            if vdir.is_dir():
                return vdir
    return connector_dir


def _iter_connector_roots(root: Path):
    """Yield each connector ROOT dir under `root` (any directory holding latest.json,
    at any nesting depth, excluding versions/ subtrees)."""
    for latest_path in sorted(root.rglob("latest.json")):
        if "versions" in latest_path.parts:
            continue
        yield latest_path.parent


def _find_connector(name: str) -> Optional[Path]:
    """Resolve a connector by directory name at any depth under CONNECTORS_ROOT."""
    for cdir in _iter_connector_roots(CONNECTORS_ROOT):
        if cdir.name == name:
            return cdir
    return None


# Connectors that are standalone MCP servers rather than BaseMCPConnector subclasses.
# AUTHORITY: shared/mcp-connectors/tests/test_tool_surface_conformance.py — this copy is
# pinned to it by tests/test_validate_connector_script_finds_connectors.py, which fails if
# the two sets drift.
NON_SUBCLASS_CONNECTORS = {"debezium", "minio", "sample-data"}


class ConnectorValidator:
    """Validates that connectors follow the generic interface"""
    
    REQUIRED_METHODS = [
        'test_connection',
        'discover_schema',
        'validate_config',
        'export',
        'get_capabilities',
        'list_tools',
        'handle_request'
    ]
    
    OPTIONAL_METHODS = [
        'get_schema',
        'import_data',
        'query',
        'run'
    ]
    
    REQUIRED_ATTRIBUTES = [
        'connector_type',
        'connector_category'
    ]
    
    VALID_CATEGORIES = [
        'relational_db',
        'document_db',
        'cloud_storage',
        'api_saas',
        'streaming',
        'data_warehouse'
    ]
    
    # The four operations every data connector is expected to expose.
    CORE_OPERATIONS = ['test_connection', 'discover_schema', 'validate_config', 'export']

    REQUIRED_METADATA_FIELDS = [
        'name',
        'display_name',
        'version',
        'description',
        'category',
        'capabilities'
    ]
    
    def __init__(self, connector_dir: Path, strict: bool = False, name: str = None):
        self.connector_dir = connector_dir
        # connector_dir is the versioned dir (…/versions/v1.0.0), so its own name is a
        # version string — the connector's identity comes from its root dir.
        self.connector_name = name or connector_dir.name
        self.strict = strict
        self.errors = []
        self.warnings = []
        self.passed = []
        self.connector_class = None
        self.connector_instance = None
    
    def validate(self) -> bool:
        """Run all validation checks"""
        print(f"\n🔍 Validating connector: {self.connector_name}")
        print("=" * 60)
        
        # Run checks in order
        self.check_directory_structure()
        self.check_metadata()
        self.check_connector_class()
        self.check_base_inheritance()
        if self.connector_name in NON_SUBCLASS_CONNECTORS:
            # A standalone MCP server answers a different contract (it dispatches tool
            # names itself), so the BaseMCPConnector attribute/method surface does not
            # apply. Say so out loud — a check that silently does not run reads exactly
            # like a check that passed.
            print("   ℹ  standalone MCP server: BaseMCPConnector attribute/method-surface "
                  "checks skipped (they do not apply), not passed")
        else:
            self.check_required_attributes()
            self.check_methods()
            self.check_response_formats()
            self.dry_run_validation()
        
        # Print results
        self._print_results()
        
        # Determine success
        if self.errors:
            return False
        if self.strict and self.warnings:
            return False
        
        print(f"\n🎉 Connector '{self.connector_name}' is valid!")
        return True
    
    def _print_results(self):
        """Print validation results"""
        print(f"\n✅ Passed: {len(self.passed)}")
        for msg in self.passed:
            print(f"   ✓ {msg}")
        
        if self.warnings:
            print(f"\n⚠️  Warnings: {len(self.warnings)}")
            for msg in self.warnings:
                print(f"   ⚠ {msg}")
        
        if self.errors:
            print(f"\n❌ Errors: {len(self.errors)}")
            for msg in self.errors:
                print(f"   ✗ {msg}")
    
    def check_directory_structure(self):
        """Check required files exist"""
        required_files = ['connector.py', 'metadata.json', 'requirements.txt']
        optional_files = ['README.md', 'logo.svg', 'logo.png']
        
        for filename in required_files:
            filepath = self.connector_dir / filename
            if filepath.exists():
                self.passed.append(f"File exists: {filename}")
            else:
                self.errors.append(f"Missing required file: {filename}")
        
        for filename in optional_files:
            filepath = self.connector_dir / filename
            if filepath.exists():
                self.passed.append(f"Optional file exists: {filename}")
    
    @staticmethod
    def _declared_operations(metadata: dict) -> Optional[List[str]]:
        """Tool names declared in metadata.json. 27 connectors use 'operations',
        debezium uses 'tools'. Both forms appear as a list of {name: ...} objects or
        of bare strings. Returns None when neither key is present."""
        for key in ('operations', 'tools'):
            ops = metadata.get(key)
            if ops is None:
                continue
            if isinstance(ops, dict):
                return list(ops)
            if isinstance(ops, list):
                return [o.get('name') if isinstance(o, dict) else o for o in ops]
        return None

    def check_metadata(self):
        """Validate metadata.json"""
        metadata_path = self.connector_dir / 'metadata.json'
        
        if not metadata_path.exists():
            return
        
        try:
            with open(metadata_path) as f:
                metadata = json.load(f)
            
            for field in self.REQUIRED_METADATA_FIELDS:
                if field in metadata:
                    self.passed.append(f"Metadata has '{field}'")
                else:
                    self.errors.append(f"Metadata missing field: {field}")
            
            # 'capabilities' is a dict of feature FLAGS (supports_cdc, max_batch_size,
            # …) in all 28 connectors — never a list of tool names. Testing tool names
            # for membership in it warned on every connector in the repo, always.
            # Tool names live under 'operations' (or 'tools' for debezium).
            capabilities = metadata.get('capabilities')
            if capabilities is not None and not isinstance(capabilities, dict):
                self.errors.append(
                    f"Metadata 'capabilities' must be an object of feature flags, "
                    f"got {type(capabilities).__name__}"
                )
            
            declared_ops = self._declared_operations(metadata)
            if declared_ops is None:
                self.errors.append("Metadata declares no 'operations' (or 'tools') list")
            else:
                for cap in self.CORE_OPERATIONS:
                    if cap in declared_ops:
                        self.passed.append(f"Operation declared: {cap}")
                    else:
                        self.warnings.append(f"Core operation not declared: {cap}")
            
            # Check category is valid
            category = metadata.get('category', '')
            if category in self.VALID_CATEGORIES or category in ['database', 'saas', 'storage', 'api']:
                self.passed.append(f"Valid category: {category}")
            else:
                self.warnings.append(f"Non-standard category: {category}")
            
        except json.JSONDecodeError as e:
            self.errors.append(f"Invalid JSON in metadata.json: {e}")
    
    def check_connector_class(self):
        """Check connector.py can be imported"""
        connector_path = self.connector_dir / 'connector.py'
        
        if not connector_path.exists():
            return
        
        try:
            spec = importlib.util.spec_from_file_location(
                f"{self.connector_name}_connector",
                connector_path
            )
            module = importlib.util.module_from_spec(spec)
            
            # base_connector.py is shipped INSIDE the versioned dir, alongside
            # connector.py — importing from the parent finds nothing. Each connector
            # ships its own copy, so evict the previous one or --all leaks the first
            # connector's base class into every later import and becomes order-dependent.
            import_dir = str(self.connector_dir)
            sys.path.insert(0, import_dir)
            stale_base = sys.modules.pop('base_connector', None)
            try:
                spec.loader.exec_module(module)
            finally:
                try:
                    sys.path.remove(import_dir)
                except ValueError:
                    pass
                sys.modules.pop('base_connector', None)
                if stale_base is not None:
                    sys.modules['base_connector'] = stale_base
            
            self.passed.append("Connector module imports successfully")
            
            # The repo uses TWO naming conventions, and a suffix-only rule silently
            # missed all 12 of the '<Name>Connector' ones: databases and object stores
            # are '<Name>MCPServer', REST/GraphQL/SaaS ones are '<Name>Connector'.
            # Identity is inheritance, not spelling — match the conformance test
            # (shared/mcp-connectors/tests/test_tool_surface_conformance.py) and look
            # for a BaseMCPConnector subclass DEFINED IN THIS MODULE first, so an
            # imported base class can never be mistaken for the connector.
            candidates = [
                (name, obj) for name, obj in vars(module).items()
                if isinstance(obj, type)
                and not name.startswith('_')
                and getattr(obj, '__module__', None) == module.__name__
            ]
            subclasses = [
                (name, obj) for name, obj in candidates
                if any(b.__name__ == 'BaseMCPConnector' for b in obj.__mro__[1:])
            ]
            by_name = [
                (name, obj) for name, obj in candidates
                if name.endswith('MCPServer') or name.endswith('Connector')
            ]
            found = subclasses or by_name
            
            if found:
                self.passed.append(f"Connector class found: {found[0][0]}")
                self.connector_class = found[0][1]
            else:
                self.errors.append(
                    "No connector class found (expected a BaseMCPConnector subclass, "
                    "or a class named '<Name>MCPServer' / '<Name>Connector')"
                )
                self.connector_class = None
                
        except ImportError as e:
            # Check if it's a missing optional dependency
            error_str = str(e)
            if 'No module named' in error_str:
                module_name = error_str.split("'")[1] if "'" in error_str else "unknown"
                self.warnings.append(f"Optional dependency not installed: {module_name}")
                self.warnings.append("Connector may work with dependencies installed")
            else:
                self.errors.append(f"Failed to import connector: {e}")
            self.connector_class = None
        except Exception as e:
            self.errors.append(f"Failed to import connector: {e}")
            self.connector_class = None
    
    def check_required_attributes(self):
        """Check connector has required attributes"""
        if not self.connector_class:
            return
        
        try:
            # Try to instantiate
            self.connector_instance = self.connector_class()
            
            for attr in self.REQUIRED_ATTRIBUTES:
                if hasattr(self.connector_instance, attr):
                    value = getattr(self.connector_instance, attr)
                    self.passed.append(f"Attribute '{attr}' = '{value}'")
                    
                    # Validate category
                    if attr == 'connector_category' and value not in self.VALID_CATEGORIES:
                        self.warnings.append(f"Non-standard connector_category: {value}")
                else:
                    self.errors.append(f"Missing required attribute: {attr}")
                    
        except Exception as e:
            self.warnings.append(f"Could not instantiate to check attributes: {e}")
    
    def check_base_inheritance(self):
        """Check if connector inherits from BaseMCPConnector"""
        if not self.connector_class:
            return
        
        try:
            # Check if BaseMCPConnector is in the MRO
            base_names = [base.__name__ for base in self.connector_class.__mro__]
            
            if 'BaseMCPConnector' in base_names:
                if self.connector_name in NON_SUBCLASS_CONNECTORS:
                    self.errors.append(
                        f"{self.connector_name} is now a BaseMCPConnector subclass but is "
                        "listed in NON_SUBCLASS_CONNECTORS — remove it from that set here "
                        "and in tests/test_tool_surface_conformance.py."
                    )
                else:
                    self.passed.append("✓ Inherits from BaseMCPConnector")
            elif self.connector_name in NON_SUBCLASS_CONNECTORS:
                self.passed.append(
                    "✓ Standalone MCP server (not a BaseMCPConnector subclass, by design)"
                )
            else:
                # This is now an error, not a warning
                self.errors.append(
                    "Must inherit from BaseMCPConnector. "
                    "Change 'class XMCPServer:' to 'class XMCPServer(BaseMCPConnector):'"
                )
            
        except Exception as e:
            self.warnings.append(f"Could not check inheritance: {e}")
    
    def check_methods(self):
        """Check all required methods are implemented"""
        if not self.connector_instance:
            return
        
        # Check required methods
        for method_name in self.REQUIRED_METHODS:
            if hasattr(self.connector_instance, method_name):
                method = getattr(self.connector_instance, method_name)
                if callable(method):
                    self.passed.append(f"Method implemented: {method_name}()")
                else:
                    self.errors.append(f"'{method_name}' is not callable")
            else:
                self.errors.append(f"Missing required method: {method_name}()")
        
        # Check optional methods
        for method_name in self.OPTIONAL_METHODS:
            if hasattr(self.connector_instance, method_name):
                method = getattr(self.connector_instance, method_name)
                if callable(method):
                    self.passed.append(f"Optional method: {method_name}()")
    
    def check_response_formats(self):
        """Check that methods return correct response formats"""
        if not self.connector_instance:
            return
        
        # Check validate_config returns correct format
        self._check_response_format(
            'validate_config',
            {},
            required_keys=['valid', 'errors'],
            optional_keys=['success', 'warnings']
        )
        
        # Check get_capabilities returns correct format
        self._check_response_format(
            'get_capabilities',
            {},
            required_keys=['connector_type', 'connector_category'],
            optional_keys=['success', 'operations', 'terminology']
        )
    
    def _check_response_format(
        self, 
        method_name: str, 
        params: Dict,
        required_keys: List[str],
        optional_keys: List[str] = None
    ):
        """Check a method's response format"""
        if not hasattr(self.connector_instance, method_name):
            return
        
        try:
            method = getattr(self.connector_instance, method_name)
            result = method(params)
            
            if not isinstance(result, dict):
                self.errors.append(f"{method_name}() must return a dict, got {type(result)}")
                return
            
            # Check required keys
            missing_keys = [k for k in required_keys if k not in result]
            if missing_keys:
                self.warnings.append(
                    f"{method_name}() response missing keys: {missing_keys}"
                )
            else:
                self.passed.append(f"{method_name}() returns valid format")
                
        except NotImplementedError:
            self.warnings.append(f"{method_name}() not fully implemented (TODO)")
        except Exception as e:
            self.warnings.append(f"{method_name}() execution error: {e}")
    
    def dry_run_validation(self):
        """Perform a dry-run to ensure connector can be instantiated and called"""
        if not self.connector_instance:
            return
        
        # Test list_tools
        try:
            tools_result = self.connector_instance.list_tools({})
            if isinstance(tools_result, dict) and 'tools' in tools_result:
                tool_count = len(tools_result.get('tools', []))
                self.passed.append(f"Dry-run: list_tools() returned {tool_count} tools")
            else:
                self.warnings.append("Dry-run: list_tools() returned unexpected format")
        except Exception as e:
            self.warnings.append(f"Dry-run: list_tools() failed: {e}")
        
        # Test handle_request with tools/list
        try:
            request = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
            response = self.connector_instance.handle_request(request)
            
            if isinstance(response, dict):
                if 'result' in response:
                    self.passed.append("Dry-run: handle_request() for tools/list works")
                elif 'error' in response:
                    self.warnings.append(
                        f"Dry-run: handle_request() returned error: {response.get('error')}"
                    )
            else:
                self.warnings.append("Dry-run: handle_request() returned non-dict")
        except Exception as e:
            self.warnings.append(f"Dry-run: handle_request() failed: {e}")


def validate_all_connectors(strict: bool = False):
    """Validate all connectors in the directory"""
    # Each connector root is resolved to its current versioned directory, which is
    # where connector.py/metadata.json actually live.
    # (connector name, current versioned dir) — the versioned dir is where
    # connector.py/metadata.json actually live; the name comes from the root.
    connectors = [
        (root.name, _resolve_current_dir(root))
        for root in _iter_connector_roots(CONNECTORS_ROOT)
    ]
    connectors = [(n, d) for n, d in connectors if (d / 'connector.py').exists()]
    
    if not connectors:
        print(f"❌ No connectors found under {CONNECTORS_ROOT}!")
        return False
    
    print(f"\n🔍 Found {len(connectors)} connector(s) to validate")
    
    results = {}
    for name, connector_dir in sorted(connectors):
        validator = ConnectorValidator(connector_dir, strict=strict, name=name)
        results[name] = validator.validate()
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 VALIDATION SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for v in results.values() if v)
    failed = len(results) - passed
    
    for connector_name, is_valid in sorted(results.items()):
        status = "✅ PASS" if is_valid else "❌ FAIL"
        print(f"  {status} - {connector_name}")
    
    print(f"\nTotal: {passed}/{len(results)} passed")
    standalone = sorted(n for n in results if n in NON_SUBCLASS_CONNECTORS)
    if standalone:
        print(f"  note: {len(standalone)} standalone MCP server(s) ({', '.join(standalone)}) "
              f"are not BaseMCPConnector subclasses — their class/method-surface checks were "
              f"skipped, not passed")
    
    if failed > 0:
        print(f"\n💡 Tip: Run 'python validate_connector.py <name>' for detailed errors")
    
    return failed == 0


def main():
    if len(sys.argv) < 2:
        print("MCP Connector Validator v2.0")
        print("\nUsage:")
        print("  python validate_connector.py <connector_name>")
        print("  python validate_connector.py --all")
        print("  python validate_connector.py --all --strict  # Fail on warnings")
        print("\nExamples:")
        print("  python validate_connector.py mysql")
        print("  python validate_connector.py hubspot")
        print("  python validate_connector.py --all")
        sys.exit(1)
    
    strict = '--strict' in sys.argv
    
    if '--all' in sys.argv:
        success = validate_all_connectors(strict=strict)
        sys.exit(0 if success else 1)
    
    connector_name = sys.argv[1]
    if connector_name.startswith('--'):
        print(f"❌ Unknown option: {connector_name}")
        sys.exit(1)
    
    connector_root = _find_connector(connector_name)
    
    if connector_root is None:
        print(f"❌ Connector not found: {connector_name}")
        print(f"   Looking under: {CONNECTORS_ROOT}")
        sys.exit(1)
    
    connector_dir = _resolve_current_dir(connector_root)
    
    validator = ConnectorValidator(connector_dir, strict=strict, name=connector_root.name)
    success = validator.validate()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
