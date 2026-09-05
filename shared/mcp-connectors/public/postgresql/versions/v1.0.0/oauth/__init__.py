"""
OAuth Module for MCP Connectors

Provides OAuth 2.0 authentication support for SaaS API connectors.
"""

from .oauth_registry import (
    OAUTH_PROVIDERS,
    get_oauth_provider,
    get_oauth_url,
    get_token_url,
    is_oauth_supported,
    list_oauth_providers,
)

from .token_manager import (
    TokenManager,
    OAuthToken,
    encrypt_token,
    decrypt_token,
)

__all__ = [
    # Registry
    'OAUTH_PROVIDERS',
    'get_oauth_provider',
    'get_oauth_url',
    'get_token_url',
    'is_oauth_supported',
    'list_oauth_providers',
    # Token Manager
    'TokenManager',
    'OAuthToken',
    'encrypt_token',
    'decrypt_token',
]
