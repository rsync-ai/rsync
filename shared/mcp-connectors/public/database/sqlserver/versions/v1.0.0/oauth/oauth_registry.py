#!/usr/bin/env python3
"""
OAuth Provider Registry

Centralized configuration for OAuth 2.0 providers.
Similar to Fivetran's OAuth integration approach.

Each provider entry contains:
- auth_url: Authorization endpoint
- token_url: Token exchange endpoint  
- scopes: Required OAuth scopes
- client_id_env: Environment variable name for client ID
- client_secret_env: Environment variable name for client secret
- token_refresh_buffer: Seconds before expiry to refresh token
"""

import os
import re
from typing import Dict, List, Optional, Any

# A provider subdomain/shop label is substituted directly into the provider's
# auth_url/token_url ({subdomain}/{shop}/{domain}) and is therefore host-injection
# sensitive: an unsanitized value like "x.attacker.com/" or "169.254.169.254#"
# would point the flow — and the platform client_secret POST — at an attacker host
# (SSRF + secret exfil). Restrict to a SINGLE DNS label, mirroring the Go
# api-gateway sanitizeShopSubdomain (handlers/oauth.go). MUST stay in lockstep.
_SUBDOMAIN_LABEL_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$")
# Known provider host suffixes stripped to recover the bare account label, so both
# "acme" and "acme.myshopify.com" are accepted; everything else must be a bare label.
_SUBDOMAIN_KNOWN_SUFFIXES = (".myshopify.com", ".zendesk.com", ".freshdesk.com")


def sanitize_subdomain(raw: Optional[str]) -> str:
    """Validate a user-supplied OAuth subdomain/shop before it is substituted into a
    provider URL. Single DNS label only — any dot/slash/'@'/'#'/host-or-path
    metacharacter is rejected so the value cannot redirect the flow (and the platform
    client_secret) to an attacker host. Returns the label or raises ValueError."""
    s = (raw or "").strip()
    low = s.lower()
    for suf in _SUBDOMAIN_KNOWN_SUFFIXES:
        if low.endswith(suf):
            s = s[: -len(suf)]
            break
    s = s.strip()
    if not s:
        raise ValueError("subdomain is required")
    if not _SUBDOMAIN_LABEL_RE.match(s):
        raise ValueError(f"invalid subdomain/shop label: {raw!r} (expected a single DNS label)")
    return s


# ============================================================================
# OAuth 2.0 Provider Registry
# ============================================================================

OAUTH_PROVIDERS: Dict[str, Dict[str, Any]] = {
    
    # -------------------------------------------------------------------------
    # CRM Providers
    # -------------------------------------------------------------------------
    
    "hubspot": {
        "name": "HubSpot",
        "auth_url": "https://app.hubspot.com/oauth/authorize",
        "token_url": "https://api.hubapi.com/oauth/v1/token",
        "scopes": [
            "crm.objects.contacts.read",
            "crm.objects.contacts.write",
            "crm.objects.companies.read",
            "crm.objects.companies.write",
            "crm.objects.deals.read",
            "crm.objects.deals.write",
            "crm.schemas.contacts.read",
            "crm.schemas.companies.read",
            "crm.schemas.deals.read",
        ],
        "client_id_env": "HUBSPOT_CLIENT_ID",
        "client_secret_env": "HUBSPOT_CLIENT_SECRET",
        "token_refresh_buffer": 300,  # 5 minutes
        "grant_type": "authorization_code",
        "response_type": "code",
        "logo": "hubspot.svg",
        "category": "crm",
        "docs_url": "https://developers.hubspot.com/docs/api/oauth-quickstart-guide",
    },
    
    "salesforce": {
        "name": "Salesforce",
        "auth_url": "https://login.salesforce.com/services/oauth2/authorize",
        "token_url": "https://login.salesforce.com/services/oauth2/token",
        "scopes": [
            "api",
            "refresh_token",
            "offline_access",
        ],
        "client_id_env": "SALESFORCE_CLIENT_ID",
        "client_secret_env": "SALESFORCE_CLIENT_SECRET",
        "token_refresh_buffer": 300,
        "grant_type": "authorization_code",
        "response_type": "code",
        "logo": "salesforce.svg",
        "category": "crm",
        "docs_url": "https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_web_server_flow.htm",
        "extra_params": {
            "prompt": "consent",  # Always show consent screen
        },
    },
    
    "pipedrive": {
        "name": "Pipedrive",
        "auth_url": "https://oauth.pipedrive.com/oauth/authorize",
        "token_url": "https://oauth.pipedrive.com/oauth/token",
        "scopes": [],  # Pipedrive uses app-level permissions
        "client_id_env": "PIPEDRIVE_CLIENT_ID",
        "client_secret_env": "PIPEDRIVE_CLIENT_SECRET",
        "token_refresh_buffer": 300,
        "grant_type": "authorization_code",
        "response_type": "code",
        "logo": "pipedrive.svg",
        "category": "crm",
        "docs_url": "https://pipedrive.readme.io/docs/marketplace-oauth-authorization",
    },
    
    "zoho-crm": {
        "name": "Zoho CRM",
        "auth_url": "https://accounts.zoho.com/oauth/v2/auth",
        "token_url": "https://accounts.zoho.com/oauth/v2/token",
        "scopes": [
            "ZohoCRM.modules.ALL",
            "ZohoCRM.settings.ALL",
        ],
        "client_id_env": "ZOHO_CLIENT_ID",
        "client_secret_env": "ZOHO_CLIENT_SECRET",
        "token_refresh_buffer": 300,
        "grant_type": "authorization_code",
        "response_type": "code",
        "logo": "zoho.svg",
        "category": "crm",
        "docs_url": "https://www.zoho.com/crm/developer/docs/api/v2/oauth-overview.html",
        "extra_params": {
            "access_type": "offline",  # Required for refresh tokens
            "prompt": "consent",
        },
    },
    
    # -------------------------------------------------------------------------
    # Support/Ticketing Providers
    # -------------------------------------------------------------------------
    
    "zendesk": {
        "name": "Zendesk",
        "auth_url": "https://{subdomain}.zendesk.com/oauth/authorizations/new",
        "token_url": "https://{subdomain}.zendesk.com/oauth/tokens",
        "scopes": [
            "read",
            "write",
        ],
        "client_id_env": "ZENDESK_CLIENT_ID",
        "client_secret_env": "ZENDESK_CLIENT_SECRET",
        "token_refresh_buffer": 300,
        "grant_type": "authorization_code",
        "response_type": "code",
        "logo": "zendesk.svg",
        "category": "support",
        "docs_url": "https://developer.zendesk.com/api-reference/ticketing/oauth/oauth/",
        "requires_subdomain": True,
    },
    
    "freshdesk": {
        "name": "Freshdesk",
        "auth_url": "https://{domain}.freshdesk.com/oauth/authorize",
        "token_url": "https://{domain}.freshdesk.com/oauth/token",
        "scopes": [],
        "client_id_env": "FRESHDESK_CLIENT_ID",
        "client_secret_env": "FRESHDESK_CLIENT_SECRET",
        "token_refresh_buffer": 300,
        "grant_type": "authorization_code",
        "response_type": "code",
        "logo": "freshdesk.svg",
        "category": "support",
        "requires_subdomain": True,
    },
    
    "jira": {
        "name": "Jira Cloud",
        "auth_url": "https://auth.atlassian.com/authorize",
        "token_url": "https://auth.atlassian.com/oauth/token",
        "scopes": [
            "read:jira-user",
            "read:jira-work",
            "write:jira-work",
            "offline_access",
        ],
        "client_id_env": "JIRA_CLIENT_ID",
        "client_secret_env": "JIRA_CLIENT_SECRET",
        "token_refresh_buffer": 300,
        "grant_type": "authorization_code",
        "response_type": "code",
        "logo": "jira.svg",
        "category": "project_management",
        "docs_url": "https://developer.atlassian.com/cloud/jira/platform/oauth-2-3lo-apps/",
        "extra_params": {
            "audience": "api.atlassian.com",
            "prompt": "consent",
        },
    },
    
    # -------------------------------------------------------------------------
    # Communication Providers
    # -------------------------------------------------------------------------
    
    "slack": {
        "name": "Slack",
        "auth_url": "https://slack.com/oauth/v2/authorize",
        "token_url": "https://slack.com/api/oauth.v2.access",
        "scopes": [
            "channels:read",
            "channels:history",
            "users:read",
            "chat:write",
        ],
        "client_id_env": "SLACK_CLIENT_ID",
        "client_secret_env": "SLACK_CLIENT_SECRET",
        "token_refresh_buffer": 0,  # Slack tokens don't expire
        "grant_type": "authorization_code",
        "response_type": "code",
        "logo": "slack.svg",
        "category": "communication",
        "docs_url": "https://api.slack.com/authentication/oauth-v2",
        "token_type": "bot",  # Returns bot token by default
    },
    
    # -------------------------------------------------------------------------
    # Developer Tools
    # -------------------------------------------------------------------------
    
    "github": {
        "name": "GitHub",
        "auth_url": "https://github.com/login/oauth/authorize",
        "token_url": "https://github.com/login/oauth/access_token",
        "scopes": [
            "repo",
            "read:user",
            "read:org",
        ],
        "client_id_env": "GITHUB_CLIENT_ID",
        "client_secret_env": "GITHUB_CLIENT_SECRET",
        "token_refresh_buffer": 0,  # GitHub tokens don't expire by default
        "grant_type": "authorization_code",
        "response_type": "code",
        "logo": "github.svg",
        "category": "devtools",
        "docs_url": "https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps",
    },

    "linear": {
        "name": "Linear",
        "auth_url": "https://linear.app/oauth/authorize",
        "token_url": "https://api.linear.app/oauth/token",
        "scopes": [
            "read",
            "write",
        ],
        "client_id_env": "LINEAR_CLIENT_ID",
        "client_secret_env": "LINEAR_CLIENT_SECRET",
        "token_refresh_buffer": 0,
        "grant_type": "authorization_code",
        "response_type": "code",
        "logo": "linear.svg",
        "category": "devtools",
        "docs_url": "https://developers.linear.app/docs/oauth/authentication",
    },

    "notion": {
        "name": "Notion",
        "auth_url": "https://api.notion.com/v1/oauth/authorize",
        "token_url": "https://api.notion.com/v1/oauth/token",
        "scopes": [],  # Notion uses page-level permissions
        "client_id_env": "NOTION_CLIENT_ID",
        "client_secret_env": "NOTION_CLIENT_SECRET",
        "token_refresh_buffer": 0,  # Notion tokens don't expire
        "grant_type": "authorization_code",
        "response_type": "code",
        "logo": "notion.svg",
        "category": "productivity",
        "docs_url": "https://developers.notion.com/docs/authorization",
        "extra_params": {
            "owner": "user",
        },
    },
    
    # -------------------------------------------------------------------------
    # E-commerce Providers
    # -------------------------------------------------------------------------
    
    "shopify": {
        "name": "Shopify",
        "auth_url": "https://{shop}.myshopify.com/admin/oauth/authorize",
        "token_url": "https://{shop}.myshopify.com/admin/oauth/access_token",
        # Keep in lockstep with oauth/providers.json (the api-gateway/Go grant source).
        # write_products is required for the Shopify products *destination* connector.
        "scopes": [
            "read_products",
            "write_products",
            "read_orders",
            "read_customers",
            "read_inventory",
            "read_locations",
            "read_markets_home",
            "read_collections",
        ],
        "client_id_env": "SHOPIFY_CLIENT_ID",
        "client_secret_env": "SHOPIFY_CLIENT_SECRET",
        "token_refresh_buffer": 0,  # Shopify tokens don't expire
        "grant_type": "authorization_code",
        "response_type": "code",
        "logo": "shopify.svg",
        "category": "ecommerce",
        "docs_url": "https://shopify.dev/docs/apps/auth/oauth",
        "requires_subdomain": True,
    },
    
    # -------------------------------------------------------------------------
    # Cloud Storage Providers
    # -------------------------------------------------------------------------
    
    "google": {
        "name": "Google",
        "auth_url": "https://accounts.google.com/o/oauth2/v2/auth",
        "token_url": "https://oauth2.googleapis.com/token",
        "scopes": [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
        ],
        "client_id_env": "GOOGLE_CLIENT_ID",
        "client_secret_env": "GOOGLE_CLIENT_SECRET",
        "token_refresh_buffer": 300,
        "grant_type": "authorization_code",
        "response_type": "code",
        "logo": "google.svg",
        "category": "cloud",
        "docs_url": "https://developers.google.com/identity/protocols/oauth2",
        "extra_params": {
            "access_type": "offline",
            "prompt": "consent",
        },
    },
    
    "dropbox": {
        "name": "Dropbox",
        "auth_url": "https://www.dropbox.com/oauth2/authorize",
        "token_url": "https://api.dropboxapi.com/oauth2/token",
        "scopes": [],  # Dropbox uses app-level permissions
        "client_id_env": "DROPBOX_CLIENT_ID",
        "client_secret_env": "DROPBOX_CLIENT_SECRET",
        "token_refresh_buffer": 0,  # Dropbox uses long-lived tokens
        "grant_type": "authorization_code",
        "response_type": "code",
        "logo": "dropbox.svg",
        "category": "storage",
        "docs_url": "https://www.dropbox.com/developers/documentation/http/documentation#oauth2-authorize",
        "extra_params": {
            "token_access_type": "offline",
        },
    },
    
    # -------------------------------------------------------------------------
    # Payment Providers
    # -------------------------------------------------------------------------
    
    "stripe": {
        "name": "Stripe",
        "auth_url": "https://connect.stripe.com/oauth/authorize",
        "token_url": "https://connect.stripe.com/oauth/token",
        "scopes": [
            "read_write",
        ],
        "client_id_env": "STRIPE_CLIENT_ID",
        "client_secret_env": "STRIPE_SECRET_KEY",
        "token_refresh_buffer": 0,
        "grant_type": "authorization_code",
        "response_type": "code",
        "logo": "stripe.svg",
        "category": "payments",
        "docs_url": "https://stripe.com/docs/connect/oauth-reference",
        "extra_params": {
            "stripe_landing": "login",
        },
    },
    
    # -------------------------------------------------------------------------
    # Marketing/Analytics Providers
    # -------------------------------------------------------------------------
    
    "mailchimp": {
        "name": "Mailchimp",
        "auth_url": "https://login.mailchimp.com/oauth2/authorize",
        "token_url": "https://login.mailchimp.com/oauth2/token",
        "scopes": [],
        "client_id_env": "MAILCHIMP_CLIENT_ID",
        "client_secret_env": "MAILCHIMP_CLIENT_SECRET",
        "token_refresh_buffer": 0,
        "grant_type": "authorization_code",
        "response_type": "code",
        "logo": "mailchimp.svg",
        "category": "marketing",
        "docs_url": "https://mailchimp.com/developer/marketing/guides/access-user-data-oauth-2/",
    },
    
    "intercom": {
        "name": "Intercom",
        "auth_url": "https://app.intercom.com/oauth",
        "token_url": "https://api.intercom.io/auth/eagle/token",
        "scopes": [],
        "client_id_env": "INTERCOM_CLIENT_ID",
        "client_secret_env": "INTERCOM_CLIENT_SECRET",
        "token_refresh_buffer": 0,
        "grant_type": "authorization_code",
        "response_type": "code",
        "logo": "intercom.svg",
        "category": "support",
        "docs_url": "https://developers.intercom.com/docs/build-an-integration/learn-more/authentication/setting-up-oauth/",
    },
}


# ============================================================================
# Helper Functions
# ============================================================================

def get_oauth_provider(provider_name: str) -> Optional[Dict[str, Any]]:
    """
    Get OAuth provider configuration by name.
    
    Args:
        provider_name: Provider name (e.g., 'hubspot', 'salesforce')
        
    Returns:
        Provider configuration dict or None if not found
    """
    normalized = provider_name.lower().replace('_', '-').replace(' ', '-')
    return OAUTH_PROVIDERS.get(normalized)


def get_oauth_url(provider_name: str, 
                  redirect_uri: str,
                  state: str,
                  subdomain: Optional[str] = None) -> Optional[str]:
    """
    Build the OAuth authorization URL for a provider.
    
    Args:
        provider_name: Provider name
        redirect_uri: Callback URL after authorization
        state: CSRF state parameter
        subdomain: Required for some providers (Zendesk, Shopify)
        
    Returns:
        Complete authorization URL or None if provider not found
    """
    provider = get_oauth_provider(provider_name)
    if not provider:
        return None
    
    auth_url = provider['auth_url']
    
    # Replace subdomain if required
    if provider.get('requires_subdomain'):
        if not subdomain:
            raise ValueError(f"{provider_name} requires a subdomain")
        subdomain = sanitize_subdomain(subdomain)  # host-injection guard
        auth_url = auth_url.replace('{subdomain}', subdomain)
        auth_url = auth_url.replace('{shop}', subdomain)
        auth_url = auth_url.replace('{domain}', subdomain)
    
    # Get client ID from environment
    client_id = os.getenv(provider['client_id_env'], '')
    if not client_id:
        raise ValueError(f"Missing environment variable: {provider['client_id_env']}")
    
    # Build scopes string
    scopes = ' '.join(provider.get('scopes', []))
    
    # Build query parameters
    params = {
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'response_type': provider.get('response_type', 'code'),
        'state': state,
    }
    
    if scopes:
        params['scope'] = scopes
    
    # Add extra parameters
    if provider.get('extra_params'):
        params.update(provider['extra_params'])
    
    # Build URL
    from urllib.parse import urlencode
    return f"{auth_url}?{urlencode(params)}"


def get_token_url(provider_name: str, subdomain: Optional[str] = None) -> Optional[str]:
    """
    Get the token exchange URL for a provider.
    
    Args:
        provider_name: Provider name
        subdomain: Required for some providers
        
    Returns:
        Token URL or None if provider not found
    """
    provider = get_oauth_provider(provider_name)
    if not provider:
        return None
    
    token_url = provider['token_url']
    
    # Replace subdomain if required
    if provider.get('requires_subdomain') and subdomain:
        subdomain = sanitize_subdomain(subdomain)  # host-injection guard
        token_url = token_url.replace('{subdomain}', subdomain)
        token_url = token_url.replace('{shop}', subdomain)
        token_url = token_url.replace('{domain}', subdomain)
    
    return token_url


def is_oauth_supported(provider_name: str) -> bool:
    """Check if a provider supports OAuth."""
    return get_oauth_provider(provider_name) is not None


def list_oauth_providers() -> List[Dict[str, str]]:
    """
    List all available OAuth providers.
    
    Returns:
        List of provider summaries with name, category, and logo
    """
    return [
        {
            'id': name,
            'name': config['name'],
            'category': config.get('category', 'other'),
            'logo': config.get('logo', f'{name}.svg'),
            'requires_subdomain': config.get('requires_subdomain', False),
        }
        for name, config in OAUTH_PROVIDERS.items()
    ]


def get_provider_credentials(provider_name: str) -> Dict[str, str]:
    """
    Get client credentials for a provider from environment variables.
    
    Args:
        provider_name: Provider name
        
    Returns:
        Dict with client_id and client_secret
        
    Raises:
        ValueError: If credentials not found
    """
    provider = get_oauth_provider(provider_name)
    if not provider:
        raise ValueError(f"Unknown OAuth provider: {provider_name}")
    
    client_id = os.getenv(provider['client_id_env'], '')
    client_secret = os.getenv(provider['client_secret_env'], '')
    
    if not client_id:
        raise ValueError(f"Missing {provider['client_id_env']}")
    if not client_secret:
        raise ValueError(f"Missing {provider['client_secret_env']}")
    
    return {
        'client_id': client_id,
        'client_secret': client_secret,
    }


# ============================================================================
# PKCE Support (for public clients)
# ============================================================================

def generate_pkce_challenge() -> Dict[str, str]:
    """
    Generate PKCE code verifier and challenge.
    
    Returns:
        Dict with code_verifier and code_challenge
    """
    import secrets
    import hashlib
    import base64
    
    # Generate code verifier (43-128 characters)
    code_verifier = secrets.token_urlsafe(64)[:128]
    
    # Generate code challenge (SHA256 hash, base64url encoded)
    digest = hashlib.sha256(code_verifier.encode()).digest()
    code_challenge = base64.urlsafe_b64encode(digest).decode().rstrip('=')
    
    return {
        'code_verifier': code_verifier,
        'code_challenge': code_challenge,
        'code_challenge_method': 'S256',
    }


if __name__ == "__main__":
    # List all providers for testing
    print("=" * 60)
    print("OAuth Provider Registry")
    print("=" * 60)
    
    providers = list_oauth_providers()
    print(f"\nTotal providers: {len(providers)}\n")
    
    for provider in providers:
        subdomain_flag = " (requires subdomain)" if provider['requires_subdomain'] else ""
        print(f"  {provider['id']}: {provider['name']} [{provider['category']}]{subdomain_flag}")
