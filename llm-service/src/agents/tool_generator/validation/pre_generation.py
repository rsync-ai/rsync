"""
Pre-Generation Validator

Validates ConnectorSpec before code generation to catch issues early.

Checks:
- Required operations exist
- Config fields are valid
- Resources are properly configured
- Capability consistency (e.g., destination support requires import_data)
- Category-specific requirements

VERSION: 1.0.0
"""

import logging
from typing import List

try:
    from ..schemas.spec import ConnectorSpec, ConnectorCategory, OperationType
    from .models import ValidationResult
except ImportError:
    # Fallback for different execution context
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from schemas.spec import ConnectorSpec, ConnectorCategory, OperationType
    from validation.models import ValidationResult

logger = logging.getLogger(__name__)


class PreGenerationValidator:
    """
    Validates ConnectorSpec before code generation.
    
    This catches structural issues early, before expensive LLM calls
    or code generation attempts.
    """
    
    def validate(self, spec: ConnectorSpec) -> ValidationResult:
        """
        Run all pre-generation validations.
        
        Args:
            spec: ConnectorSpec to validate
            
        Returns:
            ValidationResult with errors/warnings
        """
        result = ValidationResult(passed=True)
        
        # Run all validation checks
        self._validate_basic_fields(spec, result)
        self._validate_operations(spec, result)
        self._validate_capability_consistency(spec, result)
        self._validate_category_requirements(spec, result)
        self._validate_resources(spec, result)
        self._validate_config_fields(spec, result)
        
        logger.info(
            f"Pre-generation validation for {spec.name}: "
            f"{'PASSED' if result.passed else 'FAILED'} "
            f"({len(result.errors)} errors, {len(result.warnings)} warnings)"
        )
        
        return result
    
    def _validate_basic_fields(self, spec: ConnectorSpec, result: ValidationResult) -> None:
        """Validate basic required fields"""
        if not spec.name:
            result.add_error("Connector name is required", location="spec.name")
        
        if not spec.display_name:
            result.add_error("Display name is required", location="spec.display_name")
        
        if not spec.description:
            result.add_warning(
                "Description is empty; consider adding a description",
                location="spec.description"
            )
        
        if not spec.category:
            result.add_error("Category is required", location="spec.category")
    
    def _validate_operations(self, spec: ConnectorSpec, result: ValidationResult) -> None:
        """Validate operations list"""
        if not spec.operations:
            result.add_error(
                "No operations defined; connector must have at least core operations",
                location="spec.operations",
                code="NO_OPERATIONS"
            )
            return
        
        operation_names = [op.name for op in spec.operations]
        
        # Check for required core operations
        required_core_ops = ["test_connection", "validate_config", "get_capabilities"]
        for op_name in required_core_ops:
            if op_name not in operation_names:
                result.add_error(
                    f"Missing required core operation: {op_name}",
                    location="spec.operations",
                    code="MISSING_CORE_OPERATION"
                )
        
        # Check for duplicate operation names
        if len(operation_names) != len(set(operation_names)):
            duplicates = [name for name in operation_names if operation_names.count(name) > 1]
            result.add_error(
                f"Duplicate operation names found: {set(duplicates)}",
                location="spec.operations",
                code="DUPLICATE_OPERATIONS"
            )
    
    def _validate_capability_consistency(
        self,
        spec: ConnectorSpec,
        result: ValidationResult
    ) -> None:
        """
        Validate that operations match advertised capabilities.
        
        Rules:
        - supports_destination=True requires import_data operation
        - supports_source=True requires export operation (usually)
        - API/SaaS connectors should not have supports_cdc=True
        """
        operation_names = [op.name for op in spec.operations]
        
        # Destination capability requires import_data
        if spec.supports_destination and "import_data" not in operation_names:
            result.add_error(
                "supports_destination=True but import_data operation is missing",
                location="spec.supports_destination",
                code="MISSING_IMPORT_OPERATION",
                fix_suggestion="Add import_data operation or set supports_destination=False"
            )
        
        # Non-destination should not have import_data
        if not spec.supports_destination and "import_data" in operation_names:
            result.add_warning(
                "supports_destination=False but import_data operation exists",
                location="spec.operations",
                code="UNEXPECTED_IMPORT_OPERATION",
                fix_suggestion="Remove import_data operation or set supports_destination=True"
            )
        
        # Source capability should have export (unless read-only API)
        if spec.supports_source and spec.category != ConnectorCategory.API_SAAS:
            if "export" not in operation_names:
                result.add_warning(
                    "supports_source=True but export operation is missing",
                    location="spec.supports_source",
                    code="MISSING_EXPORT_OPERATION"
                )
        
        # API/SaaS should not have CDC
        if spec.category == ConnectorCategory.API_SAAS and spec.supports_cdc:
            result.add_error(
                "API/SaaS connectors do not support CDC",
                location="spec.supports_cdc",
                code="INVALID_CDC_FOR_API",
                fix_suggestion="Set supports_cdc=False for API/SaaS connectors"
            )
    
    def _validate_category_requirements(
        self,
        spec: ConnectorSpec,
        result: ValidationResult
    ) -> None:
        """Validate category-specific requirements"""
        category = spec.category
        
        # Database connectors should have database-specific operations
        if category in [ConnectorCategory.RELATIONAL_DB, ConnectorCategory.DOCUMENT_DB]:
            operation_names = [op.name for op in spec.operations]
            if "discover_schema" not in operation_names:
                result.add_warning(
                    f"{category.value} connectors should have discover_schema operation",
                    location="spec.operations",
                    code="MISSING_DISCOVER_SCHEMA"
                )
        
        # Cloud storage connectors should have base_url or bucket config
        if category == ConnectorCategory.CLOUD_STORAGE:
            config_names = [f.name for f in spec.config_fields]
            if "bucket" not in config_names and not spec.base_url:
                result.add_warning(
                    "Cloud storage connector should have 'bucket' config or base_url",
                    location="spec.config_fields",
                    code="MISSING_BUCKET_CONFIG"
                )
    
    def _validate_resources(self, spec: ConnectorSpec, result: ValidationResult) -> None:
        """Validate API resources (for API/SaaS connectors)"""
        if spec.category != ConnectorCategory.API_SAAS:
            return
        
        if not spec.resources:
            result.add_warning(
                "API/SaaS connector has no resources defined",
                location="spec.resources",
                code="NO_RESOURCES"
            )
            return
        
        for i, resource in enumerate(spec.resources):
            if not resource.name:
                result.add_error(
                    f"Resource {i} has no name",
                    location=f"spec.resources[{i}].name"
                )
            
            if not resource.endpoint:
                result.add_warning(
                    f"Resource '{resource.name}' has no endpoint defined",
                    location=f"spec.resources[{i}].endpoint"
                )
            
            if not resource.id_field:
                result.add_warning(
                    f"Resource '{resource.name}' has no id_field (defaulting to 'id')",
                    location=f"spec.resources[{i}].id_field"
                )
    
    def _validate_config_fields(self, spec: ConnectorSpec, result: ValidationResult) -> None:
        """Validate configuration fields"""
        if not spec.config_fields:
            result.add_warning(
                "No config fields defined; connector may not be configurable",
                location="spec.config_fields"
            )
            return
        
        field_names = [f.name for f in spec.config_fields]
        
        # Check for duplicates
        if len(field_names) != len(set(field_names)):
            duplicates = [name for name in field_names if field_names.count(name) > 1]
            result.add_error(
                f"Duplicate config field names: {set(duplicates)}",
                location="spec.config_fields",
                code="DUPLICATE_CONFIG_FIELDS"
            )
        
        # Category-specific checks
        if spec.category in [ConnectorCategory.RELATIONAL_DB, ConnectorCategory.DOCUMENT_DB]:
            # Databases typically need connection params
            if "host" not in field_names and "connection_uri" not in field_names:
                result.add_warning(
                    "Database connector should have 'host' or 'connection_uri' config",
                    location="spec.config_fields"
                )

