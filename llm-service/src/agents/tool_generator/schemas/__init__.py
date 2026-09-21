"""
Tool Generator Schemas Package

Provides Pydantic models for the Pure Agentic MCP Connector Transformation pipeline.

Modules:
- spec: ConnectorSpec and related configuration models
- mock: MockProfile for Digital Twin simulation
- versioning: Spec versioning and migration utilities
"""

from .spec import (
    ConnectorSpec,
    AuthConfig,
    AuthType,
    ResourceConfig,
    OperationConfig,
    ConnectorCategory,
    ParameterDefinition,
)
from .mock import (
    MockProfile,
    MockEndpoint,
    MockResponse,
    ChaosConfig,
)
from .versioning import (
    SPEC_VERSION,
    migrate_spec,
    is_compatible_version,
)

__all__ = [
    # spec.py
    "ConnectorSpec",
    "AuthConfig",
    "AuthType",
    "ResourceConfig",
    "OperationConfig",
    "ConnectorCategory",
    "ParameterDefinition",
    # mock.py
    "MockProfile",
    "MockEndpoint",
    "MockResponse",
    "ChaosConfig",
    # versioning.py
    "SPEC_VERSION",
    "migrate_spec",
    "is_compatible_version",
]

