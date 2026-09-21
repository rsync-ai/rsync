"""
Code validation helpers for generated connectors.

Provides fast AST-based validation with:
- Substring prefilter for quick checks
- Targeted AST parsing (only specific functions)
- Per-request AST caching
- Structured actionable error messages
"""

import ast
import hashlib
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class ValidationError:
    """Structured validation error"""
    code: str
    location: str
    message: str
    why: str
    how_to_fix: List[str] = field(default_factory=list)
    evidence: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "location": self.location,
            "message": self.message,
            "why": self.why,
            "how_to_fix": self.how_to_fix,
            "evidence": self.evidence,
        }


class ASTCache:
    """In-memory cache for parsed ASTs (per-request lifetime)"""
    
    def __init__(self):
        self.cache: Dict[str, ast.Module] = {}
    
    def get_or_parse(self, code: str, template_version: str = "1.0") -> Optional[ast.Module]:
        """Get cached AST or parse and cache"""
        cache_key = hashlib.sha256(f"{code}:{template_version}".encode()).hexdigest()
        
        if cache_key in self.cache:
            logger.debug(f"AST cache hit: {cache_key[:8]}")
            return self.cache[cache_key]
        
        try:
            tree = ast.parse(code)
            self.cache[cache_key] = tree
            logger.debug(f"AST parsed and cached: {cache_key[:8]}")
            return tree
        except SyntaxError as e:
            logger.error(f"AST parse error: {e}")
            return None


def substring_prefilter(code: str, markers: List[str]) -> bool:
    """
    Fast O(n) substring check for validation markers.
    
    Returns True if any marker is found, False otherwise.
    """
    for marker in markers:
        if marker in code:
            return True
    return False


def find_function_def(tree: ast.Module, func_name: str) -> Optional[ast.FunctionDef]:
    """
    Find a specific function definition in AST.
    
    Returns the FunctionDef node or None if not found.
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            return node
    return None


def function_calls_method(func_def: ast.FunctionDef, method_pattern: str) -> bool:
    """
    Check if a function calls a specific method (e.g., 'self._storage_handler.export').
    
    Args:
        func_def: The function definition node
        method_pattern: Pattern like '_storage_handler.export' or 'StorageHandler'
    
    Returns:
        True if the pattern is found in any call within the function
    """
    for node in ast.walk(func_def):
        # Check function calls
        if isinstance(node, ast.Call):
            call_str = ast.unparse(node.func) if hasattr(ast, 'unparse') else ''
            if method_pattern in call_str:
                return True
        
        # Check attribute access
        if isinstance(node, ast.Attribute):
            attr_str = ast.unparse(node) if hasattr(ast, 'unparse') else ''
            if method_pattern in attr_str:
                return True
    
    return False


def validate_cloud_storage_export(code: str, ast_cache: ASTCache) -> List[ValidationError]:
    """
    Validate cloud_storage export uses StorageHandler.
    
    Fast path:
    1. Substring prefilter for 'StorageHandler' or '_storage_handler'
    2. If not found, fail immediately
    3. If found, targeted AST check for export() function
    """
    errors = []
    
    # Fast prefilter
    if not substring_prefilter(code, ['StorageHandler', '_storage_handler']):
        errors.append(ValidationError(
            code="NO_STORAGE_HANDLER",
            location="connector.py:export",
            message="Cloud storage export must use StorageHandler",
            why="StorageHandler provides standardized file listing and download operations required for validation.",
            how_to_fix=[
                "Initialize self._storage_handler = StorageHandler(connector=self) in __init__",
                "Call self._storage_handler.export(...) in export() method",
                "Implement list_objects() and download_file() methods on connector",
            ],
            evidence="No 'StorageHandler' or '_storage_handler' found in generated code"
        ))
        return errors
    
    # Targeted AST check
    tree = ast_cache.get_or_parse(code)
    if not tree:
        errors.append(ValidationError(
            code="AST_PARSE_ERROR",
            location="connector.py",
            message="Generated code has syntax errors",
            why="Code must be valid Python to pass validation.",
            how_to_fix=["Check template for syntax errors", "Review generated code manually"],
        ))
        return errors
    
    export_func = find_function_def(tree, "export")
    if not export_func:
        errors.append(ValidationError(
            code="NO_EXPORT_METHOD",
            location="connector.py",
            message="Connector missing export() method",
            why="export() is required for source connectors.",
            how_to_fix=["Add export() method to connector class"],
        ))
        return errors
    
    # Check if export calls _storage_handler.export
    if not function_calls_method(export_func, "_storage_handler.export"):
        errors.append(ValidationError(
            code="NO_STORAGE_HANDLER_CALL",
            location="connector.py:export",
            message="export() does not call StorageHandler.export()",
            why="StorageHandler.export() ensures files are actually downloaded for validation.",
            how_to_fix=[
                "Replace custom export logic with: result = self._storage_handler.export(...)",
                "Ensure list_objects() and download_file() are implemented",
            ],
            evidence="export() function found but no call to _storage_handler.export"
        ))

    # Contract: connector must implement list_objects() and download_file()
    if "def list_objects" not in code:
        errors.append(ValidationError(
            code="MISSING_LIST_OBJECTS",
            location="connector.py",
            message="Cloud storage connector missing list_objects()",
            why="StorageHandler requires list_objects() to enumerate keys under bucket/prefix.",
            how_to_fix=[
                "Implement def list_objects(self, config: Dict, bucket: str, prefix: str = '', max_keys: int = 1000) -> List[Dict]",
                "Use S3 list_objects_v2 with continuation tokens until max_keys reached",
            ],
            evidence="No 'def list_objects' found in generated code",
        ))
    if "def download_file" not in code:
        errors.append(ValidationError(
            code="MISSING_DOWNLOAD_FILE",
            location="connector.py",
            message="Cloud storage connector missing download_file()",
            why="StorageHandler requires download_file() to fetch object bytes (download path).",
            how_to_fix=[
                "Implement def download_file(self, config: Dict, bucket: str, key: str) -> bytes",
                "Use S3 get_object and read Body",
            ],
            evidence="No 'def download_file' found in generated code",
        ))
    
    return errors


def validate_api_export(code: str, ast_cache: ASTCache) -> List[ValidationError]:
    """
    Validate api_saas export uses ApiHandler.

    ApiHandler is a REST-only abstraction. GraphQL connectors paginate
    via cursor fields embedded in the query template (Relay
    Connection.pageInfo.endCursor) and stream results through the
    architect-generated query loop, so they don't (and shouldn't) use
    ApiHandler. The hand-authored production connectors confirm this:
    shopify-admin-graphql v1.0.0 ships zero ApiHandler references and
    handles cursor pagination directly in its export() method.

    Skip the ApiHandler requirement when the code is clearly a GraphQL
    connector (uses a `_post_graphql` / `graphql_query` helper or imports
    a graphql client). REST connectors still require ApiHandler.
    """
    errors = []

    # GraphQL bypass: detect the connector idiom rather than requiring
    # caller to pass protocol. False positives are safe — at worst we
    # skip a quality check on a REST connector that uses GraphQL-sounding
    # helper names.
    if substring_prefilter(code, [
        "_post_graphql", "graphql_query", "GRAPHQL_OPERATIONS",
        "import graphql", "from graphql ",
    ]):
        return errors

    # Fast prefilter
    if not substring_prefilter(code, ['ApiHandler', '_api_handler']):
        errors.append(ValidationError(
            code="NO_API_HANDLER",
            location="connector.py:export",
            message="API export must use ApiHandler",
            why="ApiHandler provides standardized pagination, rate limiting, and error handling.",
            how_to_fix=[
                "Initialize self._api_handler = ApiHandler(connector=self, ...) in __init__",
                "Call self._api_handler.export_resource(...) in export() method",
            ],
            evidence="No 'ApiHandler' or '_api_handler' found in generated code"
        ))
        return errors
    
    # Targeted AST check
    tree = ast_cache.get_or_parse(code)
    if not tree:
        return [ValidationError(
            code="AST_PARSE_ERROR",
            location="connector.py",
            message="Generated code has syntax errors",
            why="Code must be valid Python.",
            how_to_fix=["Check template for errors"],
        )]
    
    export_func = find_function_def(tree, "export")
    if not export_func:
        return [ValidationError(
            code="NO_EXPORT_METHOD",
            location="connector.py",
            message="Connector missing export() method",
            why="export() required for source connectors.",
            how_to_fix=["Add export() method"],
        )]

    # Exception: some "api_saas" connectors intentionally use an SDK client instead of HTTP+ApiHandler.
    # Example: AWS Lambda uses boto3 (SigV4 + AWS credential chain) and therefore does not paginate
    # via ApiHandler. Allow this *only* when we detect the explicit `_get_lambda_client()` helper.
    has_lambda_helper = bool(find_function_def(tree, "_get_lambda_client"))
    uses_lambda_helper = has_lambda_helper and function_calls_method(export_func, "_get_lambda_client")

    if (not function_calls_method(export_func, "_api_handler.export_resource")) and (not uses_lambda_helper):
        errors.append(ValidationError(
            code="NO_API_HANDLER_CALL",
            location="connector.py:export",
            message="export() does not call ApiHandler.export_resource()",
            why="ApiHandler ensures pagination and rate limiting are applied.",
            how_to_fix=[
                "Replace custom export logic with: result = self._api_handler.export_resource(...)",
            ],
            evidence="export() found but no call to _api_handler.export_resource"
        ))

    # Auth/runtime contracts for api_saas
    #
    # 1) _make_request_v2 must exist and delegate to super()._make_request_v2 (to get 429/401 logic)
    make_req = find_function_def(tree, "_make_request_v2")
    if not make_req:
        errors.append(ValidationError(
            code="MISSING_MAKE_REQUEST_V2",
            location="connector.py",
            message="API connector missing _make_request_v2()",
            why="ApiHandler requires _make_request_v2() to perform HTTP requests with standardized error surface.",
            how_to_fix=[
                "Implement _make_request_v2(self, method, endpoint, config, params=None, data=None)",
                "Delegate to: return super()._make_request_v2(method=..., url=..., headers=..., params=..., json_data=..., timeout=...)",
            ],
            evidence="No def _make_request_v2 found",
        ))
    else:
        if not function_calls_method(make_req, "super()._make_request_v2"):
            errors.append(ValidationError(
                code="MAKE_REQUEST_V2_NOT_DELEGATING",
                location="connector.py:_make_request_v2",
                message="_make_request_v2() must delegate to BaseMCPConnector._make_request_v2()",
                why="BaseMCPConnector provides retry/429 handling and OAuth 401 refresh behavior.",
                how_to_fix=[
                    "Call: return super()._make_request_v2(method=..., url=..., headers=..., params=..., json_data=..., timeout=...)",
                ],
                evidence="No call to super()._make_request_v2 detected",
            ))

    # 2) _get_headers must exist and must return a value at least once.
    # We previously required a literal dict return (return { ... }), but oauth2 connectors
    # legitimately delegate to helpers like return self._get_auth_headers().
    get_headers = find_function_def(tree, "_get_headers")
    if not get_headers:
        errors.append(ValidationError(
            code="MISSING_GET_HEADERS",
            location="connector.py",
            message="API connector missing _get_headers()",
            why="Generated API connectors must build auth headers deterministically (prevents broken auth / duplicate headers).",
            how_to_fix=[
                "Implement _get_headers(self, config: Dict) -> Dict[str, str]",
                "Return a dict containing auth header + Content-Type + Accept",
            ],
            evidence="No def _get_headers found",
        ))
    else:
        has_non_none_return = False
        for node in ast.walk(get_headers):
            if isinstance(node, ast.Return):
                val = getattr(node, "value", None)
                # Accept any non-None return value; this catches both:
                # - return { ... }
                # - return self._get_auth_headers()
                # - return headers
                if val is None:
                    continue
                if isinstance(val, ast.Constant) and val.value is None:
                    continue
                has_non_none_return = True
                break
        if not has_non_none_return:
            errors.append(ValidationError(
                code="GET_HEADERS_NO_RETURN",
                location="connector.py:_get_headers",
                message="_get_headers() must return headers (non-None)",
                why="Missing return causes runtime auth failures (requests sent without required auth header).",
                how_to_fix=[
                    "Ensure all branches return a Dict[str,str] (literal dict, variable, or helper call like self._get_auth_headers())",
                ],
                evidence="_get_headers found but no non-None return detected",
            ))

    # 3) __init__ must set auth_type to a safe value for api_saas.
    # Allowed: oauth (for oauth2) or none (for api_key/bearer/basic) to prevent Base auto-inject duplicates.
    init_func = find_function_def(tree, "__init__")
    if init_func:
        assigned = set()
        for node in ast.walk(init_func):
            if isinstance(node, ast.Assign):
                for tgt in node.targets:
                    try:
                        if ast.unparse(tgt) == "self.auth_type":
                            val = getattr(node, "value", None)
                            if isinstance(val, ast.Constant) and isinstance(val.value, str):
                                assigned.add(val.value)
                    except Exception:
                        pass
        if assigned and not assigned.issubset({"oauth", "none"}):
            errors.append(ValidationError(
                code="INVALID_AUTH_TYPE",
                location="connector.py:__init__",
                message=f"api_saas connector sets unsupported auth_type(s): {sorted(assigned)}",
                why="For api_saas connectors, auth_type must be 'oauth' (oauth2) or 'none' (manual header) to avoid duplicate auth headers.",
                how_to_fix=[
                    "Set self.auth_type = 'oauth' only for oauth2 connectors",
                    "Otherwise set self.auth_type = 'none' and emit exactly one auth header in _get_headers()",
                ],
                evidence="Detected assignment(s) to self.auth_type in __init__",
            ))
    
    return errors


def validate_database_export(code: str, ast_cache: ASTCache) -> List[ValidationError]:
    """
    Validate database export uses DatabaseHandler or explicit query execution.
    """
    errors = []
    
    # Database connectors can use DatabaseHandler OR directly execute queries
    # Check for either pattern
    has_db_handler = substring_prefilter(code, ['DatabaseHandler', '_db_handler'])
    has_execute_query = substring_prefilter(code, ['execute_query', 'cursor.execute'])
    
    if not has_db_handler and not has_execute_query:
        errors.append(ValidationError(
            code="NO_DB_QUERY_PATH",
            location="connector.py:export",
            message="Database export must use DatabaseHandler or execute_query",
            why="Database export requires actual query execution to retrieve data.",
            how_to_fix=[
                "Option 1: Initialize self._db_handler = DatabaseHandler(connector=self) and call export()",
                "Option 2: Implement execute_query() and call it from export()",
            ],
            evidence="No DatabaseHandler or execute_query found in code"
        ))
    
    return errors


def validate_connector_code(code: str, category: str, ast_cache: Optional[ASTCache] = None) -> List[ValidationError]:
    """
    Main entry point for code validation.
    
    Args:
        code: Generated connector code
        category: Connector category
        ast_cache: Optional AST cache (create new if None)
    
    Returns:
        List of validation errors (empty if valid)
    """
    if ast_cache is None:
        ast_cache = ASTCache()
    
    if category == "cloud_storage":
        return validate_cloud_storage_export(code, ast_cache)
    elif category == "api_saas":
        return validate_api_export(code, ast_cache)
    elif category in ["relational_db", "document_db", "data_warehouse", "wide_column_db"]:
        return validate_database_export(code, ast_cache)
    else:
        return []  # No validation for unknown categories

