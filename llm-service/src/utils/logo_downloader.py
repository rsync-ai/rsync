"""
Logo Downloader Utility

Generic, extensible logo downloader for MCP connectors.
Supports databases, SaaS, streaming, cloud services, and any external tool.
Prefers SVG format, falls back to PNG, generates placeholder as last resort.

Features:
- Async/Parallel downloading for speed
- Category-aware logo sources
- Multiple fallback sources
- LLM-assisted domain inference
- Smart icon search via heuristics
- SVG validation
- Logo caching

VERSION: 2.0.0 (Async + Enhanced Inference)
"""

import os
import re
import json
import logging
import asyncio
import httpx
import time
import requests
from typing import Optional, Dict, Tuple, List, Union
from urllib.parse import urlparse, urljoin
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

logger = logging.getLogger("logo-downloader")

# Some SaaS connectors don't have reliable SVG brand assets in our public SVG sources.
# For these, we prefer PNG favicon/brand sources to avoid accidentally selecting a parent-company SVG
# (e.g., Freshdesk vs Freshworks).
FORCE_PNG_CONNECTORS = {
    "freshdesk",
}

# =============================================================================
# LOGO CACHING
# =============================================================================

# Cache directory for downloaded logos
LOGO_CACHE_DIR = os.getenv("LOGO_CACHE_DIR", "/tmp/logo_cache")

def _ensure_cache_dir():
    """Ensure the logo cache directory exists"""
    Path(LOGO_CACHE_DIR).mkdir(parents=True, exist_ok=True)

def _get_cache_path(connector_name: str, ext: str = "svg", variant: Optional[str] = None) -> str:
    """Get cache file path for a connector logo (variant-aware)"""
    safe_name = connector_name.lower().replace(" ", "_").replace("-", "_")
    if variant:
        safe_variant = str(variant).lower().replace(" ", "_").replace("-", "_")
        safe_name = f"{safe_name}__{safe_variant}"
    return os.path.join(LOGO_CACHE_DIR, f"{safe_name}.{ext}")

def get_cached_logo(
    connector_name: str,
    force_refresh: bool = False,
    variant: Optional[str] = None,
    prefer_svg: bool = True,
) -> Optional[Tuple[bytes, str]]:
    """
    Check if logo is cached and return it.
    
    Returns:
        Tuple of (content, extension) or None if not cached
    """
    if force_refresh:
        return None
        
    exts = ["svg", "png"] if prefer_svg else ["png", "svg"]
    for ext in exts:
        cache_path = _get_cache_path(connector_name, ext, variant=variant)
        if os.path.exists(cache_path):
            try:
                # Check age (optional invalidation)
                mtime = os.path.getmtime(cache_path)
                if time.time() - mtime > 86400 * 30:  # 30 days
                    continue
                    
                with open(cache_path, 'rb') as f:
                    content = f.read()
                if len(content) > 100:  # Valid content
                    logger.debug(f"Cache hit for {connector_name}")
                    return content, ext
            except Exception as e:
                logger.debug(f"Cache read failed: {e}")
    return None

def cache_logo(connector_name: str, content: bytes, ext: str, variant: Optional[str] = None) -> bool:
    """Cache a downloaded logo for future use"""
    try:
        _ensure_cache_dir()
        cache_path = _get_cache_path(connector_name, ext, variant=variant)
        with open(cache_path, 'wb') as f:
            f.write(content)
        logger.debug(f"Cached logo for {connector_name}")
        return True
    except Exception as e:
        logger.debug(f"Cache write failed: {e}")
        return False

def _remove_other_logo_files(connector_path: str, keep_ext: str) -> None:
    """Ensure only one logo file exists (prevents UI from always picking stale SVG)."""
    try:
        keep_ext = (keep_ext or "").lower().lstrip(".")
        for ext in ["svg", "png", "jpg", "jpeg", "ico"]:
            if ext == keep_ext:
                continue
            p = os.path.join(connector_path, f"logo.{ext}")
            if os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass
    except Exception:
        pass


# =============================================================================
# LLM-BASED DOMAIN INFERENCE
# =============================================================================

async def _infer_domain_with_llm(connector_name: str, description: str = "") -> Optional[str]:
    """
    Use LLM to infer the official domain for an unknown connector.
    Uses async call wrapper around sync OpenAI client.
    """
    try:
        import openai
        
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return None
        
        # Run sync OpenAI call in thread pool via shared factory (honours OPENAI_BASE_URL / Azure)
        def run_inference():
            from src.utils.openai_client import make_sync_client, resolve_provider, get_default_model
            provider = resolve_provider()
            client = make_sync_client(provider)
            model = get_default_model(provider)
            prompt = f"""For the software/service called "{connector_name}", what is its official company domain?
{f'Context: {description}' if description else ''}

Return ONLY the domain (e.g., "stripe.com", "mongodb.com").
If you don't recognize the service, respond with "unknown".
Do not include "www." or "https://".
Also, search your knowledge for "official brand assets" or "press kit" to confirm."""

            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=50
            )
            return response.choices[0].message.content.strip().lower()

        domain = await asyncio.to_thread(run_inference)
        
        if domain and domain != "unknown" and "." in domain:
            domain = domain.replace("https://", "").replace("http://", "").replace("www.", "")
            logger.info(f"  🤖 LLM inferred domain for {connector_name}: {domain}")
            return domain
        return None
        
    except ImportError:
        return None
    except Exception as e:
        logger.debug(f"LLM domain inference failed: {e}")
        return None


# =============================================================================
# REGISTRY & CONFIG
# =============================================================================

# File path for persisted runtime registry additions
RUNTIME_REGISTRY_FILE = os.getenv(
    "LOGO_REGISTRY_FILE", 
    os.path.join(os.path.dirname(__file__), ".logo_registry_additions.json")
)

def _load_runtime_registry() -> Dict:
    try:
        if os.path.exists(RUNTIME_REGISTRY_FILE):
            with open(RUNTIME_REGISTRY_FILE, 'r') as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def _save_runtime_registry(additions: Dict):
    try:
        with open(RUNTIME_REGISTRY_FILE, 'w') as f:
            json.dump(additions, f, indent=2)
    except Exception:
        pass

_RUNTIME_REGISTRY_ADDITIONS = _load_runtime_registry()

class ConnectorCategory(Enum):
    DATABASE = "database"
    STREAMING = "streaming"
    CLOUD = "cloud"
    SAAS = "saas"
    API = "api"
    DEVTOOL = "devtool"
    MONITORING = "monitoring"
    UNKNOWN = "unknown"

@dataclass
class LogoSource:
    name: str
    url_template: str
    min_size: int = 100
    headers: Dict = None
    categories: List[ConnectorCategory] = None
    priority: int = 50
    
    def __post_init__(self):
        if self.headers is None:
            self.headers = {}
        if self.categories is None:
            self.categories = list(ConnectorCategory)

CONNECTOR_REGISTRY = {
    # DATABASES
    "mysql": {"domain": "mysql.com", "icon": "mysql", "category": ConnectorCategory.DATABASE},
    "postgresql": {"domain": "postgresql.org", "icon": "postgresql", "category": ConnectorCategory.DATABASE},
    "postgres": {"domain": "postgresql.org", "icon": "postgresql", "category": ConnectorCategory.DATABASE},
    "mongodb": {"domain": "mongodb.com", "icon": "mongodb", "category": ConnectorCategory.DATABASE},
    "sqlite": {"domain": "sqlite.org", "icon": "sqlite", "category": ConnectorCategory.DATABASE},
    "redis": {"domain": "redis.io", "icon": "redis", "category": ConnectorCategory.DATABASE},
    "cassandra": {"domain": "cassandra.apache.org", "icon": "apachecassandra", "category": ConnectorCategory.DATABASE},
    "oracle": {"domain": "oracle.com", "icon": "oracle", "category": ConnectorCategory.DATABASE},
    "sqlserver": {"domain": "microsoft.com", "icon": "microsoftsqlserver", "category": ConnectorCategory.DATABASE},
    "mariadb": {"domain": "mariadb.org", "icon": "mariadb", "category": ConnectorCategory.DATABASE},
    "dynamodb": {"domain": "aws.amazon.com", "icon": "amazondynamodb", "category": ConnectorCategory.DATABASE},
    "firestore": {"domain": "firebase.google.com", "icon": "firebase", "category": ConnectorCategory.DATABASE},
    "supabase": {"domain": "supabase.com", "icon": "supabase", "category": ConnectorCategory.DATABASE},
    "clickhouse": {"domain": "clickhouse.com", "icon": "clickhouse", "category": ConnectorCategory.DATABASE},
    "snowflake": {"domain": "snowflake.com", "icon": "snowflake", "category": ConnectorCategory.DATABASE},
    "bigquery": {"domain": "cloud.google.com", "icon": "googlebigquery", "category": ConnectorCategory.DATABASE},
    # Amazon Redshift: Simple Icons removed the `amazonredshift` mark under
    # trademark policy (→404), so the real mark is picked up from the Gil Barbara
    # source via the `aws-redshift` alternate slug (see _BRAND_ALTERNATE_SLUGS).
    "redshift": {"domain": "aws.amazon.com", "icon": "amazonredshift", "category": ConnectorCategory.DATABASE},
    "amazon-redshift": {"domain": "aws.amazon.com", "icon": "amazonredshift", "category": ConnectorCategory.DATABASE},
    "elasticsearch": {"domain": "elastic.co", "icon": "elasticsearch", "category": ConnectorCategory.DATABASE},
    
    # STREAMING
    "kafka": {"domain": "kafka.apache.org", "icon": "apachekafka", "category": ConnectorCategory.STREAMING},
    "debezium": {"domain": "debezium.io", "icon": "debezium", "category": ConnectorCategory.STREAMING},
    "rabbitmq": {"domain": "rabbitmq.com", "icon": "rabbitmq", "category": ConnectorCategory.STREAMING},
    "airbyte": {"domain": "airbyte.com", "icon": "airbyte", "category": ConnectorCategory.STREAMING},
    
    # CLOUD & STORAGE
    "aws": {"domain": "aws.amazon.com", "icon": "amazonaws", "category": ConnectorCategory.CLOUD},
    "s3": {"domain": "aws.amazon.com", "icon": "amazons3", "category": ConnectorCategory.CLOUD},
    "aws-s3": {"domain": "aws.amazon.com", "icon": "amazons3", "category": ConnectorCategory.CLOUD},
    "aws_s3": {"domain": "aws.amazon.com", "icon": "amazons3", "category": ConnectorCategory.CLOUD},
    "minio": {"domain": "min.io", "icon": "minio", "category": ConnectorCategory.CLOUD},
    "gcs": {"domain": "cloud.google.com", "icon": "googlecloudstorage", "category": ConnectorCategory.CLOUD},
    "azure": {"domain": "azure.microsoft.com", "icon": "microsoftazure", "category": ConnectorCategory.CLOUD},
    # The Azure Blob Storage connector id is `azure-blob`, which doesn't match
    # the `azure` key — register it explicitly. `microsoftazure` is 404 on Simple
    # Icons, so the real mark comes from Gil Barbara via `microsoft-azure`.
    "azure-blob": {"domain": "azure.microsoft.com", "icon": "microsoftazure", "category": ConnectorCategory.CLOUD},
    "azure-blob-storage": {"domain": "azure.microsoft.com", "icon": "microsoftazure", "category": ConnectorCategory.CLOUD},
    "vercel": {"domain": "vercel.com", "icon": "vercel", "category": ConnectorCategory.CLOUD},
    "netlify": {"domain": "netlify.com", "icon": "netlify", "category": ConnectorCategory.CLOUD},
    
    # SAAS
    "stripe": {"domain": "stripe.com", "icon": "stripe", "category": ConnectorCategory.SAAS},
    "hubspot": {"domain": "hubspot.com", "icon": "hubspot", "category": ConnectorCategory.SAAS},
    "salesforce": {"domain": "salesforce.com", "icon": "salesforce", "category": ConnectorCategory.SAAS},
    "slack": {"domain": "slack.com", "icon": "slack", "category": ConnectorCategory.SAAS},
    "shopify": {"domain": "shopify.com", "icon": "shopify", "category": ConnectorCategory.SAAS},
    "zendesk": {"domain": "zendesk.com", "icon": "zendesk", "category": ConnectorCategory.SAAS},
    # Freshdesk is a Freshworks product, but we want the Freshdesk brand/logo specifically.
    "freshdesk": {"domain": "freshdesk.com", "icon": "freshdesk", "category": ConnectorCategory.SAAS},
    "jira": {"domain": "atlassian.com", "icon": "jira", "category": ConnectorCategory.SAAS},
    "notion": {"domain": "notion.so", "icon": "notion", "category": ConnectorCategory.SAAS},
    "twilio": {"domain": "twilio.com", "icon": "twilio", "category": ConnectorCategory.SAAS},
    "sendgrid": {"domain": "sendgrid.com", "icon": "sendgrid", "category": ConnectorCategory.SAAS},
    "mailchimp": {"domain": "mailchimp.com", "icon": "mailchimp", "category": ConnectorCategory.SAAS},
    "intercom": {"domain": "intercom.com", "icon": "intercom", "category": ConnectorCategory.SAAS},
    "auth0": {"domain": "auth0.com", "icon": "auth0", "category": ConnectorCategory.SAAS},
    
    # DEV TOOLS
    "github": {"domain": "github.com", "icon": "github", "category": ConnectorCategory.DEVTOOL},
    "gitlab": {"domain": "gitlab.com", "icon": "gitlab", "category": ConnectorCategory.DEVTOOL},
    "docker": {"domain": "docker.com", "icon": "docker", "category": ConnectorCategory.DEVTOOL},
    "kubernetes": {"domain": "kubernetes.io", "icon": "kubernetes", "category": ConnectorCategory.DEVTOOL},
    "linear": {"domain": "linear.app", "icon": "linear", "category": ConnectorCategory.SAAS},
    "pipedrive": {"domain": "pipedrive.com", "icon": "pipedrive", "category": ConnectorCategory.SAAS},

    # MISC
    "json": {"domain": "json.org", "icon": "json", "category": ConnectorCategory.API},
}

# Auto-seed CONNECTOR_REGISTRY from vendor_apis.yaml so every YAML-curated
# vendor gets a brand-aware logo lookup without manual editing here. Manual
# entries above win on conflict (e.g. hubspot.com curated wins over the YAML
# endpoint hostname `hubapi.com`). Runs once at import time.
def _seed_registry_from_vendor_yaml() -> None:
    """Best-effort: add every vendor_apis.yaml top-level key to CONNECTOR_REGISTRY.

    Domain derivation:
      1. Parse `endpoint_template` hostname; if it contains `{...}` templating
         (e.g. `{shop}.myshopify.com`) → fall through to step 3.
      2. Strip a leading `api.` prefix (so `api.stripe.com` → `stripe.com`).
      3. Otherwise fall back to `{vendor_key}.com` (matches Clearbit's
         expectation for most SaaS brands).
    Icon = vendor_key (matches Simple Icons slug convention).
    """
    try:
        import yaml
    except ImportError:
        return
    yaml_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),  # src/utils/.. → src
        "agents", "tool_generator", "config", "vendor_apis.yaml",
    )
    if not os.path.exists(yaml_path):
        return
    try:
        with open(yaml_path, "r") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        logger.debug("vendor_apis.yaml seed skipped: %s", e)
        return

    for vendor_key, vendor_data in data.items():
        if not isinstance(vendor_data, dict):
            continue
        # Inline normalize so we don't depend on _normalize_name (defined later).
        key = (vendor_key or "").lower().replace("_", "-").replace(" ", "-").strip()
        if key in CONNECTOR_REGISTRY:
            continue  # manual entry wins

        domain = None
        for api in vendor_data.get("apis", []) or []:
            tmpl = (api.get("endpoint_template") or "").strip()
            if "://" not in tmpl:
                continue
            try:
                host = urlparse(tmpl).netloc
            except Exception:
                continue
            if not host or "{" in host:
                continue
            if host.startswith("api."):
                host = host[len("api."):]
            domain = host
            break
        if not domain:
            domain = f"{key}.com"

        CONNECTOR_REGISTRY[key] = {
            "domain": domain,
            "icon": key,
            "category": ConnectorCategory.SAAS,
        }
        logger.debug("vendor_apis.yaml seed: %s → %s", key, domain)


_seed_registry_from_vendor_yaml()


# Generated connectors are named like "stripe-rest", "hubspot-rest-v3",
# "github-graphql-v4", "slack-web-api". The CONNECTOR_REGISTRY keys are the
# bare brand names ("stripe", "hubspot", "github", "slack"). Strip these
# protocol/version suffixes during lookup so the registry resolves correctly
# instead of falling through to the made-up domain "stripe-rest.com" — which
# would return junk Clearbit / Google-favicon fallbacks (the same gray
# placeholder image for unrelated brands).
_PROTOCOL_SUFFIXES = (
    "-admin-graphql",
    "-graphql-v4", "-graphql-v3", "-graphql-v2", "-graphql-v1", "-graphql",
    "-gql-v1", "-gql",
    "-rest-v4", "-rest-v3", "-rest-v2", "-rest-v1", "-rest-api", "-rest",
    "-web-api", "-api-v4", "-api-v3", "-api-v2", "-api-v1", "-api",
    "-soap", "-grpc",
)


def _strip_protocol_suffix(name: str) -> str:
    """Strip trailing protocol/version tokens so registry lookups hit the brand."""
    n = name.lower()
    # Longest first (already ordered above); strip once.
    for suffix in _PROTOCOL_SUFFIXES:
        if n.endswith(suffix) and len(n) > len(suffix):
            return n[: -len(suffix)]
    return n


# Hostnames whose registrable label is a generic hosting/PaaS provider, NOT the
# connector's brand. We must not derive a Simple-Icons slug from these or we'd
# fetch the host provider's logo (e.g. Heroku's) for an unrelated API.
_GENERIC_HOST_LABELS = {
    "amazonaws", "googleapis", "cloudfront", "herokuapp", "azurewebsites",
    "netlify", "vercel", "onrender", "render", "railway", "fly", "ngrok",
    "githubusercontent", "readthedocs", "rapidapi", "mashape", "apiary",
    "apigee", "cloudfunctions", "workers", "repl", "glitch",
}
# Multi-part public suffixes so foo.co.uk → "foo" not "co".
_MULTI_TLDS = {
    "co.uk", "com.au", "co.jp", "co.in", "com.br", "co.nz", "org.uk",
    "gov.uk", "ac.uk", "co.za", "com.mx", "com.sg",
}


def _brand_slug_from_host(host: Optional[str]) -> Optional[str]:
    """Best-effort brand label from a hostname, usable as a Simple-Icons slug.

    petstore3.swagger.io → "swagger"; api.stripe.com → "stripe";
    countries.trevorblades.com → "trevorblades". This is the fix for
    spec-imported connectors whose id is NOT a brand (e.g. "petstore"): the
    real brand lives in the spec's base_url host, which we previously threw
    away (keeping only the connector id as the icon slug → Simple-Icons miss →
    grey-letter placeholder). Returns None for generic hosting providers and
    bare/IP hosts so we never fetch the wrong brand's logo.
    """
    if not host:
        return None
    host = host.lower().strip().strip(".").split(":")[0]
    labels = [lbl for lbl in host.split(".") if lbl]
    if len(labels) < 2:
        return None
    if all(lbl.isdigit() for lbl in labels):  # IPv4 → no brand
        return None
    last2 = ".".join(labels[-2:])
    if last2 in _MULTI_TLDS and len(labels) >= 3:
        brand = labels[-3]
    else:
        brand = labels[-2]
    if not brand or brand in _GENERIC_HOST_LABELS:
        return None
    return brand


def _protocol_fallback_icons(connector_name: str, protocol_hint: Optional[str]) -> List[str]:
    """Simple-Icons slugs to try as a meaningful last resort before the grey
    placeholder, based on the connector's wire protocol. A GraphQL connector
    gets the GraphQL mark; an OpenAPI/REST one gets the Swagger/OpenAPI mark.
    This is what gives every spec-generated connector a real, on-brand-by-
    protocol logo instead of a grey letter box."""
    n = (connector_name or "").lower()
    h = (protocol_hint or "").lower()
    icons: List[str] = []
    if h == "graphql" or n.endswith(("-graphql", "-gql")) or "graphql" in n:
        icons.append("graphql")
    if h in ("openapi", "rest", "swagger") or n.endswith(("-rest", "-openapi", "-api")):
        icons.extend(["swagger", "openapiinitiative"])
    seen: set = set()
    return [i for i in icons if not (i in seen or seen.add(i))]


SVG_LOGO_SOURCES = [
    LogoSource("Simple Icons", "https://cdn.simpleicons.org/{icon}", 50, priority=90),
    LogoSource("Simple Icons (colored)", "https://cdn.simpleicons.org/{icon}/{color}", 50, priority=85),
    LogoSource("Devicons Original", "https://raw.githubusercontent.com/devicons/devicon/master/icons/{icon}/{icon}-original.svg", 50, priority=80),
    LogoSource("Devicons Plain", "https://raw.githubusercontent.com/devicons/devicon/master/icons/{icon}/{icon}-plain.svg", 50, priority=75),
    LogoSource("Gil Barbara", "https://raw.githubusercontent.com/gilbarbara/logos/main/logos/{icon}.svg", 50, priority=70),
    LogoSource("Gil Barbara Icon", "https://raw.githubusercontent.com/gilbarbara/logos/main/logos/{icon}-icon.svg", 50, priority=65),
    LogoSource("VSCode Icons", "https://raw.githubusercontent.com/vscode-icons/vscode-icons/master/icons/file_type_{icon}.svg", 50, priority=60),
    LogoSource("Worldvectorlogo", "https://cdn.worldvectorlogo.com/logos/{icon}.svg", 50, priority=55),
]

# Logo.dev API token — read from env; empty default so the source degrades
# gracefully to lower-priority PNG sources (Google/DuckDuckGo) when unset. Mirrors
# the OPENAI_API_KEY os.getenv usage in this file. NEVER hardcode a token here.
LOGO_DEV_TOKEN = os.getenv("LOGO_DEV_TOKEN", "")

PNG_LOGO_SOURCES = [
    LogoSource("Clearbit", "https://logo.clearbit.com/{domain}?size=256", 2000, priority=90),
    LogoSource("Brandfetch", "https://asset.brandfetch.io/id/{domain}/logo", 2000, priority=85),
    LogoSource("Logo.dev", "https://img.logo.dev/{domain}?token=" + LOGO_DEV_TOKEN, 2000, priority=80),
    LogoSource("Google", "https://www.google.com/s2/favicons?domain={domain}&sz=128", 1000, priority=70),
    LogoSource("DuckDuckGo", "https://icons.duckduckgo.com/ip3/{domain}.ico", 500, priority=50),
]

# Legacy compatibility mappings
KNOWN_DOMAINS = {k: v["domain"] for k, v in CONNECTOR_REGISTRY.items()}
SVG_NAME_MAPPINGS = {k: v["icon"] for k, v in CONNECTOR_REGISTRY.items()}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def _normalize_name(name: str) -> str:
    return name.lower().replace('_', '-').replace(' ', '-').strip()

# Defence-in-depth against XXE / billion-laughs entity expansion, mirroring
# tool_generator/utils/csdl_converter.py::_DANGEROUS_XML_RE. A legitimate logo SVG
# never declares a DOCTYPE or custom ENTITY, so their presence anywhere is a red
# flag — reject before handing the bytes to ElementTree. Scanned over the WHOLE
# document (not a leading window) so a declaration pushed past a large comment
# cannot slip past. Bytes pattern because logo content arrives as raw bytes off the
# wire (the csdl guard operates on already-decoded str).
_DANGEROUS_XML_RE = re.compile(rb"<!\s*(?:doctype|entity)", re.IGNORECASE)


def _is_valid_svg(content: bytes) -> bool:
    """Robust SVG validation"""
    if not content: return False

    # Basic check
    start = content[:1000].lower().strip()
    if not (b'<svg' in start or (b'<?xml' in start and b'svg' in start)):
        return False

    # XXE / entity-expansion guard: an SVG that declares a DOCTYPE or ENTITY is
    # hostile (x-logo bytes are attacker-controlled — see _download_vendor_logo).
    # Reject OUTRIGHT and DO NOT fall through to the lenient size fallback below,
    # which would otherwise accept the payload. Must run before ElementTree so a
    # billion-laughs document never reaches the parser.
    if _DANGEROUS_XML_RE.search(content):
        return False

    # XML validation (lightweight)
    try:
        from xml.etree import ElementTree
        ElementTree.fromstring(content)
        return True
    except Exception:
        # Fallback
        return len(content) > 50

def _is_valid_image(content: bytes) -> bool:
    if not content: return False
    if content.startswith(b'\x89PNG'): return True
    if content.startswith(b'\xff\xd8'): return True # JPEG
    if content.startswith(b'\x00\x00\x01\x00'): return True # ICO
    return _is_valid_svg(content)

def _detect_image_ext(content: bytes) -> str:
    """Best-effort detect file extension from magic bytes."""
    if not content:
        return "png"
    if content.startswith(b"\x89PNG"):
        return "png"
    if content.startswith(b"\xff\xd8"):
        return "jpg"
    if content.startswith(b"\x00\x00\x01\x00"):
        return "ico"
    # If it looks like SVG, store as svg even in PNG phase
    if _is_valid_svg(content):
        return "svg"
    return "png"

# =============================================================================
# ASYNC DOWNLOAD LOGIC
# =============================================================================

async def _fetch_url(client: httpx.AsyncClient, url: str, source: LogoSource) -> Optional[Tuple[bytes, str, LogoSource]]:
    """Fetch single URL with timeout"""
    try:
        response = await client.get(url, headers=source.headers, follow_redirects=True)
        if response.status_code == 200:
            content = response.content
            if len(content) >= source.min_size:
                return content, url, source
    except Exception:
        pass
    return None

async def _try_sources_async(
    sources: List[LogoSource],
    params: Dict[str, str],
    is_svg: bool
) -> Optional[Tuple[bytes, str]]:
    """
    Try multiple sources in parallel.
    Returns first successful result (prioritized).
    """
    timeout = httpx.Timeout(10.0, connect=5.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        tasks = []
        
        # Sort sources
        sorted_sources = sorted(sources, key=lambda s: s.priority, reverse=True)
        
        # Create all tasks
        for source in sorted_sources:
            try:
                # Format URL
                url = source.url_template.format(**params)
                tasks.append(_fetch_url(client, url, source))
            except KeyError:
                # Try simpler format
                try:
                    clean_params = {k: v for k, v in params.items() if k in ['icon', 'domain', 'connector']}
                    # Manual replace as fallback
                    url = source.url_template
                    for k, v in clean_params.items():
                        url = url.replace(f"{{{k}}}", v)
                    tasks.append(_fetch_url(client, url, source))
                except Exception:
                    continue

        # Execute in batches/parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results - find highest priority success
        valid_results = []
        for res in results:
            if isinstance(res, tuple):
                content, url, source = res
                valid_check = _is_valid_svg(content) if is_svg else _is_valid_image(content)
                if valid_check:
                    valid_results.append((content, source.priority))
        
        if valid_results:
            # Sort by priority
            valid_results.sort(key=lambda x: x[1], reverse=True)
            content = valid_results[0][0]
            if is_svg:
                return content, "svg"
            return content, _detect_image_ext(content)
            
    return None

# =============================================================================
# MAIN FUNCTIONS
# =============================================================================

def _extract_company_variations(connector_name: str, domain: str) -> List[str]:
    """Extract company/brand variations from domain intelligently."""
    variations = set()
    name_lower = _normalize_name(connector_name)
    
    if domain:
        domain_parts = domain.replace('www.', '').split('.')
        base_domain = domain_parts[0] if domain_parts else ""
        if base_domain:
            variations.add(base_domain)
            for part in domain_parts:
                if part not in ['api', 'www', 'docs', 'app', 'admin', 'com', 'io', 'net', 'org']:
                    variations.add(part)
    
    if '-' in name_lower:
        variations.update(name_lower.split('-'))
        
    # Cloud patterns
    for cloud in ['aws', 'gcp', 'azure', 'google', 'amazon']:
        if name_lower.startswith(cloud):
            remainder = name_lower[len(cloud):].lstrip('-_')
            if remainder: variations.add(remainder)
            
    return list(variations)

# Brands whose real mark lives on a wired source under a DIFFERENT slug than the
# connector id / Simple-Icons slug — chiefly AWS/Microsoft product icons that
# Simple Icons removed under trademark policy but Gil Barbara (already in
# SVG_LOGO_SOURCES) still hosts. Keyed by BOTH the connector id and the primary
# Simple-Icons slug so the alternate fires no matter which one resolved. Every
# slug here was verified to return a real SVG from a wired source.
_BRAND_ALTERNATE_SLUGS = {
    "redshift": ["aws-redshift"],
    "amazon-redshift": ["aws-redshift"],
    "amazonredshift": ["aws-redshift"],
    "azure-blob": ["microsoft-azure"],
    "azure-blob-storage": ["microsoft-azure"],
    "azure": ["microsoft-azure"],
    "microsoftazure": ["microsoft-azure"],
}


def _get_alternate_icon_names(connector_name: str, primary_icon: str) -> List[str]:
    """Generate comprehensive alternate icon names."""
    name_lower = _normalize_name(connector_name)
    alternates = {name_lower, primary_icon}

    # Curated brand alternates: the real mark lives on a wired source under a
    # different slug (keyed by connector id AND primary icon).
    for key in (name_lower, primary_icon):
        alternates.update(_BRAND_ALTERNATE_SLUGS.get(key, ()))

    # Format variations
    for suffix in ['-icon', '-logo', '-mark']:
        alternates.add(f"{name_lower}{suffix}")

    # Prefix removal
    for prefix in ['mcp-', 'connector-', 'api-']:
        if name_lower.startswith(prefix):
            alternates.add(name_lower[len(prefix):])
            
    # Sorted list
    return sorted(list(alternates), key=len)

def get_connector_info(connector_name: str, base_url: Optional[str] = None, description: str = "") -> Dict:
    """Get connector info from registry, inference, or defaults."""
    name_lower = _normalize_name(connector_name)
    # Brand match — try the connector id verbatim, then with protocol/version
    # suffixes stripped (e.g. "stripe-rest" → "stripe").
    candidates = [name_lower]
    stripped = _strip_protocol_suffix(name_lower)
    if stripped and stripped != name_lower:
        candidates.append(stripped)

    # 1. Registry (verbatim, then suffix-stripped brand name)
    for key in candidates:
        if key in CONNECTOR_REGISTRY:
            info = CONNECTOR_REGISTRY[key].copy()
            return {"domain": info["domain"], "icon": info["icon"], "category": info["category"]}

    # 2. Runtime (same candidate order)
    for key in candidates:
        if key in _RUNTIME_REGISTRY_ADDITIONS:
            info = _RUNTIME_REGISTRY_ADDITIONS[key]
            return {
                "domain": info.get("domain"),
                "icon": info.get("icon"),
                "category": ConnectorCategory(info.get("category", "unknown")) if isinstance(info.get("category"), str) else ConnectorCategory.UNKNOWN
            }

    # 3. Base URL
    if base_url:
        try:
            netloc = urlparse(base_url).netloc
            domain = netloc
            # Strip leading "api." subdomain so Clearbit hits the marketing
            # site (api.asana.com → asana.com gets the brand logo, while
            # api.asana.com sometimes returns the API portal's generic icon).
            if domain.startswith("api."):
                domain = domain[len("api."):]
            # Derive the Simple-Icons slug from the host's brand label, not the
            # connector id. A spec-imported connector whose id is not a brand
            # (e.g. "petstore" served from petstore3.swagger.io) then resolves
            # the real "swagger" SVG instead of missing every source and falling
            # to the grey-letter placeholder. Fall back to the suffix-stripped
            # id when the host has no usable brand label.
            host_brand = _brand_slug_from_host(netloc)
            icon = host_brand or stripped or name_lower
            return {"domain": domain, "icon": icon, "category": ConnectorCategory.UNKNOWN}
        except Exception:
            pass
        
    # 4. LLM (Async - handled in download_logo_async)
    # We return basic info here and let async downloader do the heavy lifting
    
    return {"domain": f"{name_lower}.com", "icon": name_lower, "category": ConnectorCategory.UNKNOWN}

def register_connector(name: str, domain: str, icon: str = None, category: ConnectorCategory = ConnectorCategory.UNKNOWN):
    """Runtime registration"""
    name_lower = _normalize_name(name)
    CONNECTOR_REGISTRY[name_lower] = {
        "domain": domain,
        "icon": icon or name_lower,
        "category": category,
    }

# Max bytes to accept for a vendor-declared (x-logo) logo fetch, and the redirect
# budget. A tiny OpenAPI spec can point info.x-logo.url at an arbitrarily large
# body or a redirect chain; cap both so a malicious/broken URL can't exhaust
# memory. Non-fatal: over-budget → skip the logo.
_VENDOR_LOGO_MAX_BYTES = 5 * 1024 * 1024  # 5 MiB
_VENDOR_LOGO_MAX_REDIRECTS = 4


async def _download_vendor_logo(
    logo_url: str, connector_name: str
) -> Optional[Tuple[bytes, str]]:
    """Fetch a vendor-declared logo (OpenAPI ``info.x-logo.url``) through the same
    SSRF protection as the rest of tool-generator.

    ``info.x-logo.url`` is attacker-controlled — it rides in an uploaded OpenAPI
    spec — so this must not be a naive ``client.get``: an internal / cloud-metadata
    target (169.254.169.254, localhost, RFC1918) or a public URL that 30x-redirects
    to one would otherwise be reachable server-side. We therefore (1) pin DNS to a
    validated global IP via ``pinned_async_client`` so check-and-connect is atomic
    (no DNS-rebinding), (2) disable httpx's own redirect following and re-validate
    every hop, (3) cap the body size, and (4) require an ``image/*`` content type
    plus a magic-byte image check before accepting the bytes.

    Returns ``(content, ext)`` or ``None`` — a blocked / failed / oversized fetch
    is NON-FATAL (the caller falls back to heuristic logo sources), never raised.
    """
    try:
        # Lazy import (mirrors the in-function ``src.agents.*`` imports already used
        # in download_logo_async) — reuses the single pinned-DNS SSRF helper rather
        # than duplicating the guard here.
        from src.agents.tool_generator.utils.pinned_dns import (
            pinned_async_client,
            resolve_global_ip,
        )
    except Exception as e:  # pragma: no cover - defensive
        logger.debug(f"x-logo SSRF helper unavailable, skipping x-logo: {e}")
        return None

    try:
        timeout = httpx.Timeout(12.0, connect=5.0)
        current = logo_url
        async with pinned_async_client(
            httpx, timeout=timeout, follow_redirects=False
        ) as client:
            for _ in range(_VENDOR_LOGO_MAX_REDIRECTS + 1):
                host = urlparse(current).hostname
                if resolve_global_ip(host) is None:
                    logger.debug(
                        f"x-logo SSRF guard blocked non-public host for {connector_name}: {current}"
                    )
                    return None
                # STREAM the body so the size cap is enforced as bytes arrive.
                # A plain client.get() buffers the whole body before any len()
                # check, and a chunked response carries no Content-Length — so the
                # cap was effectively skipped and a huge/endless body could OOM.
                async with client.stream(
                    "GET",
                    current,
                    headers={"User-Agent": "rsync-toolgen-logo/1.0"},
                    follow_redirects=False,
                ) as resp:
                    status = resp.status_code
                    if 300 <= status < 400:
                        location = (resp.headers or {}).get("location")
                        if not location:
                            return None
                        current = urljoin(current, location)
                        continue
                    if status != 200:
                        return None
                    # Content-Type must be an image (defence beyond the magic-byte
                    # check). Checked before streaming so a non-image is a cheap out.
                    content_type = (resp.headers or {}).get("content-type", "") or ""
                    content_type = content_type.split(";")[0].strip().lower()
                    if content_type and not content_type.startswith("image/"):
                        logger.debug(
                            f"x-logo non-image content-type '{content_type}' for {connector_name}"
                        )
                        return None
                    # Honest servers advertise an over-budget size up front — early out.
                    clen = (resp.headers or {}).get("content-length")
                    if clen is not None:
                        try:
                            if int(clen) > _VENDOR_LOGO_MAX_BYTES:
                                logger.debug(
                                    f"x-logo too large (content-length={clen}) for {connector_name}"
                                )
                                return None
                        except (TypeError, ValueError):
                            pass
                    # Never trust Content-Length: accumulate and abort the moment the
                    # running total crosses the cap (covers chunked / lying servers).
                    buffer = bytearray()
                    async for chunk in resp.aiter_bytes():
                        buffer += chunk
                        if len(buffer) > _VENDOR_LOGO_MAX_BYTES:
                            logger.debug(
                                f"x-logo body exceeds {_VENDOR_LOGO_MAX_BYTES} bytes for {connector_name}"
                            )
                            return None
                    content = bytes(buffer)
                    if not _is_valid_image(content):
                        return None
                    return content, _detect_image_ext(content)
        # Redirect budget exhausted.
        return None
    except Exception as e:
        logger.debug(f"x-logo download failed for {connector_name}: {e}")
        return None


async def download_logo_async(
    connector_name: str,
    connector_path: str,
    domain: Optional[str] = None,
    base_url: Optional[str] = None,
    prefer_svg: bool = True,
    color: str = None,
    force_refresh: bool = False,
    protocol_hint: Optional[str] = None,
    logo_url: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    Async logo downloader using AI agent for name resolution.

    protocol_hint ("graphql"|"openapi"|"rest") drives a protocol-aware fallback
    (GraphQL/Swagger mark) before the grey placeholder. logo_url, when given
    (e.g. an OpenAPI `info.x-logo.url`), is the vendor's own declared asset and
    is tried first.
    """
    normalized_name = _normalize_name(connector_name)

    # 0. Vendor-declared logo (OpenAPI info.x-logo.url) wins — it is the brand's
    # own asset, more authoritative than any heuristic source. logo_url is
    # attacker-controlled (it rides in an uploaded spec), so the fetch is routed
    # through the shared pinned-DNS SSRF guard (no rebinding, redirects
    # re-validated, size + image content-type enforced). A blocked/failed fetch is
    # non-fatal — fall through to the heuristic sources below.
    if logo_url and isinstance(logo_url, str) and logo_url.startswith(("http://", "https://")):
        vendor_logo = await _download_vendor_logo(logo_url, connector_name)
        if vendor_logo is not None:
            content, ext = vendor_logo
            _save_file(connector_path, f"logo.{ext}", content)
            cache_logo(connector_name, content, ext)
            _remove_other_logo_files(connector_path, ext)
            logger.info(f"  ✅ Used spec-declared logo (x-logo) for {connector_name}")
            return True, f"logo.{ext}"

    # Force PNG for known problematic connectors
    if normalized_name in FORCE_PNG_CONNECTORS:
        prefer_svg = False

    # 1. Get Info (will try registry as fallback)
    # We do this early so cache can be variant-aware (icon-specific).
    info = get_connector_info(connector_name, base_url)
    icon = info["icon"]
    resolved_domain = domain or info["domain"]

    # 2. Check cache (variant-aware)
    if not force_refresh:
        cached = get_cached_logo(connector_name, variant=icon, prefer_svg=prefer_svg)
        if cached:
            content, ext = cached
            _save_file(connector_path, f"logo.{ext}", content)
            _remove_other_logo_files(connector_path, ext)
            return True, f"logo.{ext}"

    # 3. Use AI Agent to get canonical name and aliases
    try:
        from src.agents.connector_resolver.agent import ConnectorResolverAgent
        
        resolver = ConnectorResolverAgent()
        resolution = resolver.resolve(connector_name)
        
        if resolution.canonical and resolution.confidence >= 0.7:
            canonical = resolution.canonical
            # Use canonical + suggestions as aliases to try
            aliases = [canonical] + [s.get("name", "") for s in resolution.suggestions[:5] if s.get("name")]
            logger.info(f"🎨 AI Agent resolved '{connector_name}' → '{canonical}' (trying {len(aliases)} variations)")
        else:
            # Fallback to original name
            aliases = [connector_name]
            logger.info(f"🎨 AI Agent couldn't resolve '{connector_name}', using original name")
    except Exception as e:
        logger.debug(f"AI Agent resolution failed: {e}, falling back to registry")
        aliases = [connector_name]
    
    # Async LLM inference if needed for domain
    if resolved_domain == f"{_normalize_name(connector_name)}.com" and not domain:
        llm_domain = await _infer_domain_with_llm(connector_name)
        if llm_domain:
            resolved_domain = llm_domain
            # Persist
            _RUNTIME_REGISTRY_ADDITIONS[_normalize_name(connector_name)] = {
                "domain": resolved_domain,
                "icon": icon,
                "category": "unknown"
            }
            _save_runtime_registry(_RUNTIME_REGISTRY_ADDITIONS)

    params = {
        "icon": icon,
        "connector": icon,
        "domain": resolved_domain,
        "color": color or "000000"
    }
    
    logger.info(f"Downloading logo for {connector_name} (async)")
    
    # 4. Parallel Fetch (SVG) - Try all AI-resolved aliases
    if prefer_svg:
        # Try all aliases from AI agent
        for i, alias in enumerate(aliases[:10]):  # Try up to 10 aliases
            alias_normalized = _normalize_name(alias)
            params["icon"] = alias_normalized
            params["connector"] = alias_normalized
            
            result = await _try_sources_async(SVG_LOGO_SOURCES, params, is_svg=True)
            if result:
                content, ext = result
                _save_file(connector_path, f"logo.{ext}", content)
                cache_logo(connector_name, content, ext, variant=icon)
                _remove_other_logo_files(connector_path, ext)
                logger.info(f"  ✅ Found SVG with alias '{alias}' (#{i+1})")
                return True, f"logo.{ext}"
        
        # Fallback: Try traditional alternate names
        alts = _get_alternate_icon_names(connector_name, icon)
        for alt in alts[:5]:
            params["icon"] = alt
            params["connector"] = alt
            result = await _try_sources_async(SVG_LOGO_SOURCES, params, is_svg=True)
            if result:
                content, ext = result
                _save_file(connector_path, f"logo.{ext}", content)
                cache_logo(connector_name, content, ext, variant=icon)
                _remove_other_logo_files(connector_path, ext)
                logger.info(f"  ✅ Found SVG with alternate '{alt}'")
                return True, f"logo.{ext}"
    
    # 4b. Strong protocol fallback (GraphQL): for a GraphQL API a generic domain
    # favicon is almost never better than the GraphQL mark, so prefer it over the
    # PNG/favicon sources below. (REST/OpenAPI keep favicon priority — their
    # Swagger fallback stays at step 6, just before the placeholder, so a real
    # brand's favicon/Clearbit logo still wins for REST connectors.)
    for fb in _protocol_fallback_icons(connector_name, protocol_hint):
        if fb != "graphql":
            continue
        params["icon"] = fb
        params["connector"] = fb
        result = await _try_sources_async(SVG_LOGO_SOURCES, params, is_svg=True)
        if result:
            content, ext = result
            _save_file(connector_path, f"logo.{ext}", content)
            cache_logo(connector_name, content, ext, variant=fb)
            _remove_other_logo_files(connector_path, ext)
            logger.info(f"  ✅ Used GraphQL protocol logo for {connector_name}")
            return True, f"logo.{ext}"

    # 5. Parallel Fetch (PNG) with domain
    logger.info(f"  🖼️  Trying PNG sources with domain {resolved_domain}")
    result = await _try_sources_async(PNG_LOGO_SOURCES, params, is_svg=False)
    if result:
        content, ext = result
        _save_file(connector_path, f"logo.{ext}", content)
        cache_logo(connector_name, content, ext, variant=icon)
        _remove_other_logo_files(connector_path, ext)
        logger.info(f"  ✅ Found PNG logo")
        return True, f"logo.{ext}"
    
    # 6. Protocol-aware fallback — a GraphQL/OpenAPI connector with no resolvable
    # brand logo gets its PROTOCOL mark (GraphQL hexagon / Swagger) instead of a
    # grey letter box. This is what makes every spec-generated connector whose id
    # isn't a known brand (petstore, countries-gql, …) still look intentional.
    for fb in _protocol_fallback_icons(connector_name, protocol_hint):
        params["icon"] = fb
        params["connector"] = fb
        result = await _try_sources_async(SVG_LOGO_SOURCES, params, is_svg=True)
        if result:
            content, ext = result
            _save_file(connector_path, f"logo.{ext}", content)
            cache_logo(connector_name, content, ext, variant=fb)
            _remove_other_logo_files(connector_path, ext)
            logger.info(f"  ✅ Used protocol-fallback logo '{fb}' for {connector_name}")
            return True, f"logo.{ext}"

    # 7. Last resort: Generate placeholder
    logger.warning(f"  ⚠️  No logo found for {connector_name}, generating placeholder")
    generate_placeholder_svg(connector_name, connector_path)
    return True, "logo.svg"  # Return success with placeholder

def download_logo(
    connector_name: str,
    connector_path: str,
    domain: Optional[str] = None,
    base_url: Optional[str] = None,
    prefer_svg: bool = True,
    color: str = None,
    force_refresh: bool = False,
    protocol_hint: Optional[str] = None,
    logo_url: Optional[str] = None,
) -> Tuple[bool, str]:
    """Sync wrapper - handles both sync and async contexts"""
    try:
        # Check if we're already in an event loop
        loop = asyncio.get_running_loop()
        # If we're in a running loop, we can't use asyncio.run()
        # Create a new thread to run the async function
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(
                asyncio.run,
                download_logo_async(
                    connector_name, connector_path, domain, base_url, prefer_svg,
                    color, force_refresh, protocol_hint, logo_url,
                )
            )
            return future.result()
    except RuntimeError:
        # No event loop running, safe to use asyncio.run()
        return asyncio.run(download_logo_async(
            connector_name, connector_path, domain, base_url, prefer_svg,
            color, force_refresh, protocol_hint, logo_url,
        ))

def _save_file(path: str, filename: str, content: bytes):
    full_path = os.path.join(path, filename)
    with open(full_path, 'wb') as f:
        f.write(content)

def generate_placeholder_svg(connector_name: str, connector_path: str, color: str = "#6B7280") -> bool:
    """Generate placeholder"""
    initial = connector_name[0].upper()
    svg = f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><rect width="100" height="100" fill="{color}"/><text x="50" y="50" fill="white" font-size="50" text-anchor="middle" dy=".3em">{initial}</text></svg>'
    _save_file(connector_path, "logo.svg", svg.encode('utf-8'))
    return True

def ensure_logo_for_connector(connector_name: str, connector_path: str) -> bool:
    """Entry point"""
    return download_logo(connector_name, connector_path)[0]
