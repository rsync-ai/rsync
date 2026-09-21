"""
Category Contract Registry

Defines explicit contracts for each connector category:
- Required operations
- Required methods
- Context7 validation thresholds
- QA smoke test requirements

This registry drives both generation and validation to ensure consistency.
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field


@dataclass
class Context7Thresholds:
    """Thresholds for Context7 capability validation strictness"""
    
    # Endpoint validation strictness
    min_endpoints_for_endpoint_contradiction: int = 10
    min_endpoints_for_method_contradiction: int = 5
    min_endpoints_for_param_contradiction: int = 5
    
    # Documentation quality thresholds
    min_doc_length_for_authority: int = 1000
    min_endpoints_for_pagination_authority: int = 3
    
    # Auth validation
    auth_none_requires_explicit_keywords: List[str] = field(default_factory=lambda: [
        "no authentication",
        "no auth",
        "without authentication",
        "public endpoint",
        "unauthenticated",
    ])
    
    # OAuth vs Bearer compatibility
    oauth_bearer_compatible: bool = True  # If docs say oauth2 and spec uses bearer, treat as compatible


@dataclass
class CategoryContract:
    """Contract definition for a connector category"""
    
    category: str
    handler_class: str  # ApiHandler, StorageHandler, DatabaseHandler
    
    # Required operations
    required_operations: List[str] = field(default_factory=list)
    
    # Required methods on connector
    required_methods: List[str] = field(default_factory=list)
    
    # AST validation markers (fast prefilter check)
    ast_prefilter_markers: List[str] = field(default_factory=list)
    
    # Context7 thresholds
    context7_thresholds: Context7Thresholds = field(default_factory=Context7Thresholds)
    
    # QA smoke test requirements
    qa_smoke_operations: List[str] = field(default_factory=list)
    qa_smoke_min_records: int = 1
    
    # Category-specific validation rules
    validation_rules: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# CATEGORY REGISTRY
# =============================================================================

CATEGORY_CONTRACTS: Dict[str, CategoryContract] = {
    "api_saas": CategoryContract(
        category="api_saas",
        handler_class="ApiHandler",
        required_operations=["export", "test_connection"],
        required_methods=["_make_request_v2", "_get_headers"],
        ast_prefilter_markers=["ApiHandler", "_api_handler.export_resource"],
        context7_thresholds=Context7Thresholds(
            min_endpoints_for_endpoint_contradiction=10,
            min_endpoints_for_method_contradiction=5,
            min_endpoints_for_param_contradiction=5,
        ),
        qa_smoke_operations=["export"],
        qa_smoke_min_records=1,
        validation_rules={
            "requires_pagination": True,
            "requires_rate_limiting": True,
            "requires_auth": True,
        },
    ),
    
    "cloud_storage": CategoryContract(
        category="cloud_storage",
        handler_class="StorageHandler",
        required_operations=["export", "import_data", "test_connection"],
        required_methods=["list_objects", "download_file"],
        ast_prefilter_markers=["StorageHandler", "_storage_handler.export"],
        context7_thresholds=Context7Thresholds(
            min_endpoints_for_endpoint_contradiction=5,  # Storage APIs have fewer endpoints
            min_doc_length_for_authority=500,
        ),
        qa_smoke_operations=["export"],
        qa_smoke_min_records=1,
        validation_rules={
            "requires_download_operation": True,
            "export_must_download_files": True,
        },
    ),
    
    "relational_db": CategoryContract(
        category="relational_db",
        handler_class="DatabaseHandler",
        required_operations=["export", "import_data", "test_connection", "discover_schema"],
        required_methods=["execute_query", "insert_data"],
        ast_prefilter_markers=["DatabaseHandler", "_db_handler.export", "_db_handler.import_data"],
        context7_thresholds=Context7Thresholds(
            min_endpoints_for_endpoint_contradiction=3,  # DBs have few API endpoints
            min_doc_length_for_authority=800,
        ),
        qa_smoke_operations=["export", "import_data"],
        qa_smoke_min_records=1,
        validation_rules={
            "requires_connection_pooling": False,
            "supports_transactions": True,
        },
    ),
    
    "document_db": CategoryContract(
        category="document_db",
        handler_class="DatabaseHandler",
        required_operations=["export", "import_data", "test_connection", "discover_schema"],
        required_methods=["execute_query", "insert_data"],
        ast_prefilter_markers=["DatabaseHandler", "_db_handler.export", "_db_handler.import_data"],
        context7_thresholds=Context7Thresholds(
            min_endpoints_for_endpoint_contradiction=3,
            min_doc_length_for_authority=800,
        ),
        qa_smoke_operations=["export", "import_data"],
        qa_smoke_min_records=1,
        validation_rules={
            "supports_nested_documents": True,
        },
    ),
    
    "data_warehouse": CategoryContract(
        category="data_warehouse",
        handler_class="DatabaseHandler",
        required_operations=["export", "import_data", "test_connection", "discover_schema"],
        required_methods=["execute_query", "insert_data"],
        ast_prefilter_markers=["DatabaseHandler", "_db_handler.export", "_db_handler.import_data"],
        context7_thresholds=Context7Thresholds(
            min_endpoints_for_endpoint_contradiction=5,
            min_doc_length_for_authority=1000,
        ),
        qa_smoke_operations=["export", "import_data"],
        qa_smoke_min_records=1,
        validation_rules={
            "requires_bulk_load": True,
        },
    ),
    
    "wide_column_db": CategoryContract(
        category="wide_column_db",
        handler_class="DatabaseHandler",
        required_operations=["export", "import_data", "test_connection", "discover_schema"],
        required_methods=["execute_query", "insert_data"],
        ast_prefilter_markers=["DatabaseHandler", "_db_handler.export", "_db_handler.import_data"],
        context7_thresholds=Context7Thresholds(
            min_endpoints_for_endpoint_contradiction=3,
            min_doc_length_for_authority=800,
        ),
        qa_smoke_operations=["export", "import_data"],
        qa_smoke_min_records=1,
        validation_rules={},
    ),
}


def get_contract(category: str) -> Optional[CategoryContract]:
    """Get contract for a category, returns None if not found"""
    return CATEGORY_CONTRACTS.get(category)


def get_context7_thresholds(category: str) -> Context7Thresholds:
    """Get Context7 thresholds for a category, returns defaults if not found"""
    contract = get_contract(category)
    return contract.context7_thresholds if contract else Context7Thresholds()

