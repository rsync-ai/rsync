"""
MCP Connector Schemas

Contains:
1. Response Schemas - Pydantic models for validating connector responses
2. Operation Schemas - Standard operations for each connector category
"""

from .response_schemas import (
    # Base
    BaseResponse,
    
    # Standard responses
    TestConnectionResponse,
    ValidateConfigResponse,
    DiscoverSchemaResponse,
    ExportResponse,
    ImportResponse,
    CapabilitiesResponse,
    QueryResponse,
    GetSchemaResponse,
    ListToolsResponse,
    
    # Schema models
    ColumnSchema,
    TableSchema,
    
    # Decorator
    validate_response,
    get_response_model_for_method,
    validate_response_dict,
    
    # Enums
    ConnectorCategory,
)

from .operation_schemas import (
    # Schemas
    OPERATION_SCHEMAS,
    API_OBJECTS,
    
    # Functions
    get_category_schema,
    get_required_methods,
    get_optional_methods,
    get_terminology,
    get_api_config,
    get_api_objects,
    get_operations_for_connector,
    generate_method_signature,
    get_schema_prompt_for_category,
    get_api_prompt_for_connector,
)

__all__ = [
    # Response Schemas
    'BaseResponse',
    'TestConnectionResponse',
    'ValidateConfigResponse',
    'DiscoverSchemaResponse',
    'ExportResponse',
    'ImportResponse',
    'CapabilitiesResponse',
    'QueryResponse',
    'GetSchemaResponse',
    'ListToolsResponse',
    'ColumnSchema',
    'TableSchema',
    'validate_response',
    'get_response_model_for_method',
    'validate_response_dict',
    'ConnectorCategory',
    
    # Operation Schemas
    'OPERATION_SCHEMAS',
    'API_OBJECTS',
    'get_category_schema',
    'get_required_methods',
    'get_optional_methods',
    'get_terminology',
    'get_api_config',
    'get_api_objects',
    'get_operations_for_connector',
    'generate_method_signature',
    'get_schema_prompt_for_category',
    'get_api_prompt_for_connector',
]
