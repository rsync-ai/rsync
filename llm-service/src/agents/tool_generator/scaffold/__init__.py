"""
Deterministic connector scaffolder.

Turns an OpenAPI/Swagger document into a working MCP connector with no LLM
involved at any step. Self-hosted operators get connector generation offline,
without an API key and without an account.

The agentic pipeline (``..agents``) solves the harder problem: researching an
API that has no machine-readable spec. This package deliberately does not.
It requires the operator to supply the spec, and in exchange it is fully
deterministic, offline, and reproducible.

Import closure is stdlib + pydantic + jinja2 by construction: nothing here
imports ``..agents``, ``..config`` or ``..utils``. ``test_scaffold_is_llm_free``
fails the build if that ever stops being true.
"""

from .openapi_to_spec import (
    OpenAPIConversionError,
    ConversionReport,
    openapi_to_connector_spec,
)

__all__ = [
    "OpenAPIConversionError",
    "ConversionReport",
    "openapi_to_connector_spec",
]
