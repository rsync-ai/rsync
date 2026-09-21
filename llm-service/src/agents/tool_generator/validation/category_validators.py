"""
Category-Specific Validators

Registry of validators for different connector categories.
Each category may have specialized validation rules.

Uses fast AST-based code checks with caching and actionable errors.

VERSION: 2.0.0 - Contract-driven validation
"""

import re
import logging
from typing import Optional, List, Dict, Any
from abc import ABC, abstractmethod

try:
    from ..schemas.spec import ConnectorSpec, ConnectorCategory
    from .models import ValidationResult
    try:
        # Prefer absolute import when running inside the service container
        from src.agents.tool_generator.contracts.code_checks import validate_connector_code, ASTCache, ValidationError
    except Exception:
        # Fallback to relative import when running as a package
        from ..contracts.code_checks import validate_connector_code, ASTCache, ValidationError
except Exception:
    # Fallback for different execution context
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from schemas.spec import ConnectorSpec, ConnectorCategory
    from validation.models import ValidationResult
    # Best-effort import of contract-driven code checks in non-package contexts (e.g. pytest with sys.path hacks)
    try:
        from contracts.code_checks import validate_connector_code, ASTCache, ValidationError
    except Exception:
        validate_connector_code = None
        ASTCache = None
        ValidationError = None

logger = logging.getLogger(__name__)


def run_code_validation(code: str, category: str) -> List[Dict[str, Any]]:
    """
    Run fast code validation using contract-driven checks.
    
    Returns:
        List of validation error dicts (empty if valid)
    """
    if validate_connector_code is None or ASTCache is None:
        logger.warning("Code validation not available (contracts.code_checks not imported)")
        return []
    
    ast_cache = ASTCache()
    errors = validate_connector_code(code, category, ast_cache)
    return [e.to_dict() for e in errors]


class CategoryValidator(ABC):
    """Base class for category-specific validators"""
    
    @abstractmethod
    def get_io_patterns(self) -> dict:
        """Return patterns that indicate I/O operations for this category"""
        pass
    
    @abstractmethod
    def validate_import_data(self, code: str, spec: ConnectorSpec) -> ValidationResult:
        """Validate import_data implementation"""
        pass
    
    @abstractmethod
    def validate_export_data(self, code: str, spec: ConnectorSpec) -> ValidationResult:
        """Validate export implementation"""
        pass


class APISaaSValidator(CategoryValidator):
    """Validator for API/SaaS connectors"""
    
    def get_io_patterns(self) -> dict:
        return {
            "write_operations": [
                "_make_request(",
                "requests.post",
                "requests.put",
                "requests.patch",
                "requests.delete",
                "httpx.post",
                "httpx.put",
                "httpx.patch",
                "httpx.delete",
                "self.client.post",
                "self.client.put",
                "self.client.patch",
                "self.client.delete",
            ],
            "read_operations": [
                "requests.get",
                "httpx.get",
                "self.client.get",
            ],
        }
    
    def validate_import_data(self, code: str, spec: ConnectorSpec) -> ValidationResult:
        """Validate API/SaaS import_data has HTTP write operations"""
        result = ValidationResult(passed=True)
        
        if "def import_data" not in code:
            return result  # Not implementing import_data is fine for read-only APIs
        
        # Extract import_data method
        import_data_match = re.search(
            r"def import_data\(.*?\):.*?(?=\n    def |\nclass |\Z)",
            code,
            re.DOTALL
        )
        
        if not import_data_match:
            result.add_error(
                "Could not parse import_data method",
                location="connector.py:import_data",
                code="PARSE_ERROR"
            )
            return result
        
        method_code = import_data_match.group(0).lower()
        
        # Check for HTTP write operations
        io_patterns = self.get_io_patterns()
        has_write_op = any(
            pattern.lower() in method_code
            for pattern in io_patterns["write_operations"]
        )
        
        if not has_write_op:
            result.add_error(
                "API/SaaS import_data must include HTTP write operations "
                "(e.g., _make_request, requests.post, httpx.post)",
                location="connector.py:import_data",
                code="NO_HTTP_WRITE",
                fix_suggestion="Add HTTP POST/PUT/PATCH calls to actually write data"
            )
        
        # Check for stub patterns
        stub_patterns = [
            "return {'success': true",  # Lowercase for case-insensitive match
            "return {'success': false, 'error': 'not implemented'",
            "raise notimplementederror",
            "pass  # todo",
        ]
        
        has_stub = any(pattern in method_code for pattern in stub_patterns)
        if has_stub:
            result.add_warning(
                "import_data appears to be a stub implementation",
                location="connector.py:import_data",
                code="STUB_DETECTED"
            )
        
        return result
    
    def validate_export_data(self, code: str, spec: ConnectorSpec) -> ValidationResult:
        """Validate export implementation for API/SaaS"""
        result = ValidationResult(passed=True)
        
        # For API/SaaS, export is optional (read-only APIs may not have it)
        # Just check if it exists and isn't obviously a stub
        
        if "def export" not in code:
            return result
        
        export_match = re.search(
            r"def export\(.*?\):.*?(?=\n    def |\nclass |\Z)",
            code,
            re.DOTALL
        )
        
        if export_match:
            method_code = export_match.group(0).lower()
            
            # Check for obvious stub
            if "raise notimplementederror" in method_code:
                result.add_warning(
                    "export appears to be a stub (raises NotImplementedError)",
                    location="connector.py:export",
                    code="STUB_DETECTED"
                )
        
        return result


class DatabaseValidator(CategoryValidator):
    """Validator for database connectors (relational + document)"""
    
    def get_io_patterns(self) -> dict:
        return {
            "write_operations": [
                "cursor.execute",
                "session.execute",
                "connection.execute",
                "collection.insert",
                "collection.update",
                "conn.commit()",
                "session.commit()",
            ],
            "read_operations": [
                "cursor.fetchall",
                "cursor.fetchone",
                "collection.find",
                "select(",
            ],
        }
    
    def validate_import_data(self, code: str, spec: ConnectorSpec) -> ValidationResult:
        """Validate database import_data has actual DB writes"""
        result = ValidationResult(passed=True)
        
        if "def import_data" not in code:
            result.add_error(
                "Database destination connector must have import_data method",
                location="connector.py",
                code="MISSING_IMPORT_DATA"
            )
            return result
        
        import_data_match = re.search(
            r"def import_data\(.*?\):.*?(?=\n    def |\nclass |\Z)",
            code,
            re.DOTALL
        )
        
        if not import_data_match:
            result.add_error(
                "Could not parse import_data method",
                location="connector.py:import_data",
                code="PARSE_ERROR"
            )
            return result
        
        method_code = import_data_match.group(0).lower()
        
        # Check for DB write operations
        io_patterns = self.get_io_patterns()
        has_write_op = any(
            pattern.lower() in method_code
            for pattern in io_patterns["write_operations"]
        )
        
        if not has_write_op:
            result.add_error(
                "Database import_data must include actual DB write operations "
                "(e.g., cursor.execute, conn.commit)",
                location="connector.py:import_data",
                code="NO_DB_WRITE",
                fix_suggestion="Add cursor.execute() or equivalent DB write calls"
            )
        
        # Check for commit
        if "commit()" not in method_code:
            result.add_warning(
                "Database import_data should call commit() to persist changes",
                location="connector.py:import_data",
                code="NO_COMMIT"
            )
        
        return result
    
    def validate_export_data(self, code: str, spec: ConnectorSpec) -> ValidationResult:
        """Validate database export has actual DB reads"""
        result = ValidationResult(passed=True)
        
        if "def export" not in code:
            result.add_error(
                "Database source connector must have export method",
                location="connector.py",
                code="MISSING_EXPORT"
            )
            return result
        
        export_match = re.search(
            r"def export\(.*?\):.*?(?=\n    def |\nclass |\Z)",
            code,
            re.DOTALL
        )
        
        if export_match:
            method_code = export_match.group(0).lower()
            
            # Check for DB read operations
            io_patterns = self.get_io_patterns()
            has_read_op = any(
                pattern.lower() in method_code
                for pattern in io_patterns["read_operations"]
            )
            
            if not has_read_op:
                result.add_error(
                    "Database export must include actual DB read operations "
                    "(e.g., cursor.execute, cursor.fetchall)",
                    location="connector.py:export",
                    code="NO_DB_READ"
                )
        
        return result


class CloudStorageValidator(CategoryValidator):
    """Validator for cloud storage connectors"""
    
    def get_io_patterns(self) -> dict:
        return {
            "write_operations": [
                "put_object",
                "upload_file",
                "upload_fileobj",
                "s3_client.put_object",
                "bucket.upload_file",
            ],
            "read_operations": [
                "get_object",
                "download_file",
                "download_fileobj",
                "s3_client.get_object",
                "bucket.download_file",
            ],
        }
    
    def validate_import_data(self, code: str, spec: ConnectorSpec) -> ValidationResult:
        """Validate cloud storage import_data uploads files"""
        result = ValidationResult(passed=True)
        
        if "def import_data" not in code:
            result.add_error(
                "Cloud storage destination connector must have import_data method",
                location="connector.py",
                code="MISSING_IMPORT_DATA"
            )
            return result
        
        import_data_match = re.search(
            r"def import_data\(.*?\):.*?(?=\n    def |\nclass |\Z)",
            code,
            re.DOTALL
        )
        
        if import_data_match:
            method_code = import_data_match.group(0).lower()
            
            # Check for upload operations
            io_patterns = self.get_io_patterns()
            has_upload = any(
                pattern.lower() in method_code
                for pattern in io_patterns["write_operations"]
            )
            
            if not has_upload:
                result.add_error(
                    "Cloud storage import_data must include file upload operations "
                    "(e.g., put_object, upload_file)",
                    location="connector.py:import_data",
                    code="NO_UPLOAD"
                )
        
        return result
    
    def validate_export_data(self, code: str, spec: ConnectorSpec) -> ValidationResult:
        """Validate cloud storage export downloads files"""
        result = ValidationResult(passed=True)
        
        if "def export" not in code:
            result.add_error(
                "Cloud storage source connector must have export method",
                location="connector.py",
                code="MISSING_EXPORT"
            )
            return result
        
        export_match = re.search(
            r"def export\(.*?\):.*?(?=\n    def |\nclass |\Z)",
            code,
            re.DOTALL
        )
        
        if export_match:
            method_code = export_match.group(0).lower()
            
            # Check for download operations
            io_patterns = self.get_io_patterns()
            has_download = any(
                pattern.lower() in method_code
                for pattern in io_patterns["read_operations"]
            )
            
            if not has_download:
                result.add_error(
                    "Cloud storage export must include file download operations "
                    "(e.g., get_object, download_file)",
                    location="connector.py:export",
                    code="NO_DOWNLOAD"
                )
        
        return result


# Registry of category validators
CATEGORY_VALIDATORS = {
    ConnectorCategory.API_SAAS: APISaaSValidator(),
    ConnectorCategory.RELATIONAL_DB: DatabaseValidator(),
    ConnectorCategory.DOCUMENT_DB: DatabaseValidator(),
    ConnectorCategory.DATA_WAREHOUSE: DatabaseValidator(),
    ConnectorCategory.WIDE_COLUMN_DB: DatabaseValidator(),
    ConnectorCategory.CLOUD_STORAGE: CloudStorageValidator(),
    # STREAMING: No specialized validator yet (uses generic)
}


def get_category_validator(category: ConnectorCategory) -> Optional[CategoryValidator]:
    """
    Get validator for a specific category.
    
    Args:
        category: Connector category
        
    Returns:
        CategoryValidator instance or None if no specialized validator exists
    """
    return CATEGORY_VALIDATORS.get(category)


def validate_category_fast(code: str, spec: ConnectorSpec) -> ValidationResult:
    """
    Fast category validation using contract-driven AST checks.
    
    This is the new recommended validation path that uses:
    - Substring prefilter for fast checks
    - Targeted AST parsing (only specific functions)
    - Per-request AST caching
    - Structured actionable error messages
    
    Args:
        code: Generated connector code
        spec: ConnectorSpec
    
    Returns:
        ValidationResult with pass/fail and actionable errors
    """
    result = ValidationResult(passed=True)
    
    # Get category string value
    try:
        category_str = spec.category.value if hasattr(spec.category, 'value') else str(spec.category)
    except Exception:
        category_str = "unknown"
    
    # Run fast code validation
    validation_errors = run_code_validation(code, category_str)
    
    # Convert to ValidationResult
    for err in validation_errors:
        why = err.get("why") or ""
        evidence = err.get("evidence") or ""
        how_to_fix = err.get("how_to_fix") or []
        if isinstance(how_to_fix, list):
            fix_suggestion = " | ".join([str(x) for x in how_to_fix if x])
        else:
            fix_suggestion = str(how_to_fix)

        extra = []
        if why:
            extra.append(f"why={why}")
        if evidence:
            extra.append(f"evidence={evidence}")
        extra_text = ("; " + "; ".join(extra)) if extra else ""

        result.add_error(
            message=(err.get("message", "") + extra_text).strip(),
            location=err.get("location", ""),
            code=err.get("code", "VALIDATION_ERROR"),
            fix_suggestion=fix_suggestion or None,
        )
    
    return result
