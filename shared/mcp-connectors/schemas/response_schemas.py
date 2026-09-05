#!/usr/bin/env python3
"""
MCP Connector Response Schemas

Pydantic models that define the expected response format for all connector methods.
These ensure consistent responses across all connectors and enable runtime validation.

Usage:
    from schemas import validate_response, DiscoverSchemaResponse
    
    class MyConnector(BaseMCPConnector):
        @validate_response(DiscoverSchemaResponse)
        def discover_schema(self, params):
            return {"success": True, "tables": [...], "total_tables": 10}
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import List, Optional, Any, Dict, Union
from enum import Enum
from functools import wraps
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS
# =============================================================================

class ConnectorCategory(str, Enum):
    """Valid connector categories"""
    RELATIONAL_DB = "relational_db"
    DOCUMENT_DB = "document_db"
    CLOUD_STORAGE = "cloud_storage"
    API_SAAS = "api_saas"
    STREAMING = "streaming"
    DATA_WAREHOUSE = "data_warehouse"


# =============================================================================
# CANONICAL TYPE SYSTEM
# =============================================================================
#
# The canonical type vocabulary and dialect-aware ``canonicalize_type`` live in
# the shippable, pydantic-free ``canonical_types`` module
# (shared/mcp-connectors/public/canonical_types.py) so BOTH this response
# validator (CI/llm-service) and every connector image (which pulls it via
# ``COPY --from=shared canonical_types.py``) share ONE source of truth — no
# per-connector drift. See that module for the full contract + dialect maps.
import os as _os
import sys as _sys

_PUBLIC_DIR = _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", "public"))
if _PUBLIC_DIR not in _sys.path:
    _sys.path.insert(0, _PUBLIC_DIR)

from canonical_types import (  # noqa: E402
    CANONICAL_TYPES,
    canonicalize_type,
)


# =============================================================================
# SCHEMA MODELS (for nested structures)
# =============================================================================

class ColumnSchema(BaseModel):
    """Schema for a single column/field in a table.

    `type` is coerced to a canonical type (see CANONICAL_TYPES) on
    validation. Source connectors should emit canonical directly; legacy
    dialect strings are normalized via canonicalize_type().
    """
    name: str
    type: str
    nullable: bool = True
    key: Optional[str] = None  # PRI, UNI, MUL for databases
    default: Optional[Any] = None
    extra: Optional[str] = None  # auto_increment, etc.

    @field_validator("type", mode="before")
    @classmethod
    def _coerce_canonical_type(cls, v):
        return canonicalize_type(v)

    class Config:
        extra = "allow"  # Allow additional fields


class TableSchema(BaseModel):
    """Schema for a single table/collection/object.

    `primary_keys` is part of the discovery contract — sources that know
    their PKs (DB sources via information_schema, GraphQL sources via
    ID-typed fields) declare them. Empty list means "no PK known" and
    sinks fall back to a synthetic `_rsync_row_hash` upsert key.
    """
    name: str
    columns: List[ColumnSchema] = []
    row_count: Optional[int] = None
    schema_name: Optional[str] = None  # For namespaced tables (e.g., PostgreSQL schemas)
    primary_keys: List[str] = []
    primary_key_source: Optional[str] = None  # "declared" | "inferred" | "synthetic"

    class Config:
        extra = "allow"  # Allow additional fields like 'size_bytes', 'last_modified'


class OperationSchema(BaseModel):
    """Schema for a connector operation"""
    name: str
    method: Optional[str] = None
    description: Optional[str] = None
    type: str = "core"  # core, source, destination


# =============================================================================
# BASE RESPONSE
# =============================================================================

class BaseResponse(BaseModel):
    """
    Base response that all connector methods should return.
    
    All responses MUST have:
    - success: bool - Whether the operation succeeded
    - error: str (optional) - Error message if success is False
    """
    success: bool
    error: Optional[str] = None
    
    @model_validator(mode='after')
    def validate_error_on_failure(self):
        """Ensure error message is provided when success is False"""
        if not self.success and not self.error:
            # Don't raise, just set a default error
            object.__setattr__(self, 'error', 'Operation failed (no error message provided)')
        return self
    
    class Config:
        extra = "allow"  # Allow additional fields in responses


# =============================================================================
# STANDARD RESPONSE MODELS
# =============================================================================

class TestConnectionResponse(BaseResponse):
    """
    Response from test_connection()
    
    Required:
        success: bool
    Optional:
        message: str - Human-readable success message
        version: str - Data source version
        error: str - Error message if failed
    """
    message: Optional[str] = None
    version: Optional[str] = None


class ValidateConfigResponse(BaseResponse):
    """
    Response from validate_config()
    
    Required:
        success: bool
        valid: bool - Whether config is valid
    Optional:
        errors: List[str] - Validation errors
        warnings: List[str] - Non-fatal warnings
    """
    valid: bool = False
    errors: List[str] = []
    warnings: List[str] = []
    
    @model_validator(mode='after')
    def sync_success_and_valid(self):
        """Ensure success matches valid status"""
        # If valid is explicitly set, use it to determine success
        if hasattr(self, 'valid'):
            object.__setattr__(self, 'success', self.valid or len(self.errors) == 0)
        return self


class DiscoverSchemaResponse(BaseResponse):
    """
    Response from discover_schema()
    
    Required:
        success: bool
        tables: List[TableSchema] - List of discovered tables
        total_tables: int - Total count
    Optional:
        error: str - Error message if failed
    """
    tables: List[TableSchema] = []
    total_tables: int = 0
    
    @model_validator(mode='after')
    def set_total_tables(self):
        """Auto-set total_tables from tables list if not provided"""
        if self.total_tables == 0 and self.tables:
            object.__setattr__(self, 'total_tables', len(self.tables))
        return self


class ExportResponse(BaseResponse):
    """
    Response from export()
    
    Required:
        success: bool
        data: List[Dict] - Exported rows
    Optional:
        columns: List[str] - Column names
        row_count: int - Number of rows returned
        has_more: bool - Whether more data is available
        next_offset: int - Offset for next page
        table: str - Table name exported from
        format: str - Data format (json, csv, etc.)
    """
    data: List[Dict[str, Any]] = []
    columns: List[str] = []
    row_count: int = 0
    has_more: bool = False
    next_offset: Optional[int] = None
    table: Optional[str] = None
    format: Optional[str] = "json"
    
    @model_validator(mode='after')
    def set_row_count(self):
        """Auto-set row_count from data list if not provided"""
        if self.row_count == 0 and self.data:
            object.__setattr__(self, 'row_count', len(self.data))
        return self


class ImportResponse(BaseResponse):
    """
    Response from import_data()
    
    Required:
        success: bool
    Optional:
        rows_inserted: int - Number of rows inserted
        rows_updated: int - Number of rows updated
        rows_failed: int - Number of rows that failed
        table: str - Target table name
    """
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_failed: int = 0
    table: Optional[str] = None


class QueryResponse(BaseResponse):
    """
    Response from query() - similar to ExportResponse
    """
    data: List[Dict[str, Any]] = []
    columns: List[str] = []
    row_count: int = 0
    
    @model_validator(mode='after')
    def set_row_count(self):
        if self.row_count == 0 and self.data:
            object.__setattr__(self, 'row_count', len(self.data))
        return self


class GetSchemaResponse(BaseResponse):
    """
    Response from get_schema()
    
    Returns schema for a specific table or list of tables.
    """
    table: Optional[str] = None
    tables: Optional[List[str]] = None
    table_schema: Optional[List[ColumnSchema]] = None  # Renamed from 'schema' to avoid pydantic warning
    columns: Optional[List[ColumnSchema]] = None  # Alias for table_schema


class CapabilitiesResponse(BaseResponse):
    """
    Response from get_capabilities()
    
    Required:
        success: bool
        connector_type: str
        connector_category: str
    Optional:
        supports_source: bool
        supports_destination: bool
        operations: List[OperationSchema]
        terminology: Dict
        features: Dict
    """
    connector_type: str
    connector_category: str
    supports_source: bool = True
    supports_destination: bool = False
    default_source_operation: str = "export"
    default_destination_operation: str = "import"
    terminology: Dict[str, str] = {}
    operations: List[Dict[str, Any]] = []
    features: Optional[Dict[str, Any]] = None
    optimization_category: Optional[str] = None
    performance_target_ms: Optional[int] = None


class ListToolsResponse(BaseModel):
    """
    Response from list_tools() - MCP protocol standard
    """
    tools: List[Dict[str, Any]] = []
    capabilities: Optional[Dict[str, Any]] = None


# =============================================================================
# VALIDATION DECORATOR
# =============================================================================

def validate_response(response_model: type):
    """
    Decorator to validate method responses against a Pydantic model.
    
    If validation fails, returns an error response instead of crashing.
    This ensures connectors always return valid JSON-RPC responses.
    
    Usage:
        @validate_response(DiscoverSchemaResponse)
        def discover_schema(self, params):
            return {"success": True, "tables": [...], "total_tables": 10}
    
    Args:
        response_model: Pydantic model class to validate against
    
    Returns:
        Decorated method that validates responses
    """
    def decorator(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            try:
                # Call the original method
                result = method(self, *args, **kwargs)
                
                # If already a Pydantic model, convert to dict
                if isinstance(result, BaseModel):
                    result = result.model_dump()
                
                # Validate against the schema
                validated = response_model(**result)
                
                # Return as dict for JSON serialization
                return validated.model_dump(exclude_none=True)
                
            except Exception as e:
                # Log the validation error
                logger.error(f"Response validation failed for {method.__name__}: {e}")
                
                # Return a valid error response instead of crashing
                return {
                    "success": False,
                    "error": f"Internal error: {str(e)}"
                }
        
        # Preserve the original method for introspection
        wrapper._original_method = method
        wrapper._response_model = response_model
        
        return wrapper
    return decorator


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_response_model_for_method(method_name: str) -> Optional[type]:
    """
    Get the appropriate response model for a given method name.
    
    Args:
        method_name: Name of the connector method
    
    Returns:
        Pydantic model class or None
    """
    method_to_model = {
        'test_connection': TestConnectionResponse,
        'validate_config': ValidateConfigResponse,
        'discover_schema': DiscoverSchemaResponse,
        'export': ExportResponse,
        'import': ImportResponse,
        'import_data': ImportResponse,
        'query': QueryResponse,
        'get_schema': GetSchemaResponse,
        'get_capabilities': CapabilitiesResponse,
        'list_tools': ListToolsResponse,
    }
    return method_to_model.get(method_name)


def validate_response_dict(response: Dict, method_name: str) -> tuple[bool, Optional[str]]:
    """
    Validate a response dictionary against the expected schema.
    
    Args:
        response: Response dictionary to validate
        method_name: Name of the method that produced the response
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    model = get_response_model_for_method(method_name)
    if not model:
        return True, None  # No schema defined, allow any response
    
    try:
        model(**response)
        return True, None
    except Exception as e:
        return False, str(e)
