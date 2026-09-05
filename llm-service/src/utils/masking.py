"""
Masking Utility for Sensitive Data

This module provides functions to mask sensitive data before logging.
Used across all Python services to prevent credential leaks in logs.
"""

import re
from typing import Any, Dict, Optional, Union
import copy

# List of keys that should be masked in logs
SENSITIVE_KEYS = {
    "password",
    "secret",
    "secret_key",
    "secret_access_key",
    "api_key",
    "apikey",
    "token",
    "access_key",
    "access_key_id",
    "refresh_token",
    "access_token",
    "bearer",
    "authorization",
    "credentials",
    "credentials_json",
    "private_key",
    "client_secret",
    "aws_secret_access_key",
    "aws_access_key_id",
    "connection_string",
    "conn_str",
    "db_password",
}

MASKED_VALUE = "********"


def is_sensitive_key(key: str) -> bool:
    """Check if a key should be masked."""
    if not key:
        return False
    key_lower = key.lower()
    return any(sensitive in key_lower for sensitive in SENSITIVE_KEYS)


def mask_dict(data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Create a copy of a dictionary with sensitive values masked.
    
    Works recursively on nested dictionaries.
    
    Args:
        data: Dictionary to mask
        
    Returns:
        A new dictionary with sensitive values replaced with "********"
    """
    if data is None:
        return None
    
    if not isinstance(data, dict):
        return data
    
    masked = {}
    for key, value in data.items():
        if is_sensitive_key(key):
            masked[key] = MASKED_VALUE
        elif isinstance(value, dict):
            masked[key] = mask_dict(value)
        elif isinstance(value, list):
            masked[key] = [
                mask_dict(item) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            masked[key] = value
    
    return masked


def mask_config(config: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Mask sensitive fields in connector configuration.
    
    This is the primary function to use before logging any config.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Masked configuration dictionary
    """
    return mask_dict(config)


def mask_connection_string(conn_str: str) -> str:
    """
    Mask passwords and secrets in connection strings.
    
    Args:
        conn_str: Connection string (e.g., "postgresql://user:password@host/db")
        
    Returns:
        Connection string with sensitive parts masked
    """
    if not conn_str:
        return conn_str
    
    # Mask URL-style passwords: ://user:password@
    result = re.sub(
        r'(://[^:]+:)([^@]+)(@)',
        rf'\1{MASKED_VALUE}\3',
        conn_str
    )
    
    # Mask key=value style passwords
    patterns = [
        r'(?i)(password|pwd|passwd)=([^;&\s]+)',
        r'(?i)(secret|api_key|token)=([^;&\s]+)',
    ]
    
    for pattern in patterns:
        result = re.sub(pattern, rf'\1={MASKED_VALUE}', result)
    
    return result


def mask_json_string(json_str: str) -> str:
    """
    Mask sensitive fields in a JSON-like string.
    
    Useful for logging raw payloads without parsing.
    
    Args:
        json_str: JSON string to mask
        
    Returns:
        JSON string with sensitive values masked
    """
    if not json_str:
        return json_str
    
    result = json_str
    for key in SENSITIVE_KEYS:
        # Match patterns like "password": "value" or "password":"value"
        pattern = rf'(?i)"{re.escape(key)}"\s*:\s*"[^"]*"'
        replacement = f'"{key}": "{MASKED_VALUE}"'
        result = re.sub(pattern, replacement, result)
    
    return result


def config_summary(config: Optional[Dict[str, Any]]) -> str:
    """
    Return a brief summary of config for logging.
    
    Shows key names with [REDACTED] for sensitive values.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        String like "host=localhost, port=3306, password=[REDACTED]"
    """
    if not config:
        return "(empty)"
    
    parts = []
    for key, value in config.items():
        if is_sensitive_key(key):
            parts.append(f"{key}=[REDACTED]")
        elif isinstance(value, str):
            display_value = value[:50] + "..." if len(value) > 50 else value
            parts.append(f"{key}={display_value}")
        elif isinstance(value, (dict, list)):
            parts.append(f"{key}=[complex]")
        else:
            parts.append(f"{key}={value}")
    
    return ", ".join(parts)


def sanitize_for_log(config: Optional[Dict[str, Any]]) -> Optional[Dict[str, str]]:
    """
    Create a log-safe summary of config (key names only, no values).
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Dictionary with keys and "[REDACTED]" or "[SET]" values
    """
    if config is None:
        return None
    
    summary = {}
    for key in config:
        if is_sensitive_key(key):
            summary[key] = "[REDACTED]"
        else:
            summary[key] = "[SET]"
    
    return summary


def mask_headers(headers: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
    """
    Mask sensitive HTTP headers.
    
    Args:
        headers: HTTP headers dictionary
        
    Returns:
        Headers with sensitive values masked
    """
    if headers is None:
        return None
    
    sensitive_headers = {"authorization", "x-api-key", "x-auth-token", "cookie"}
    
    masked = {}
    for key, value in headers.items():
        key_lower = key.lower()
        if key_lower in sensitive_headers or is_sensitive_key(key):
            masked[key] = MASKED_VALUE
        else:
            masked[key] = value
    
    return masked


# ------------------------------------------------------------------------------
# LLM privacy scrubber — Python mirror of backend-orchestrator/pkg/llmscrub.
# Contract (CLAUDE.md "LLM data privacy"): the LLM may see schema metadata and
# user-authored text — never row values, credentials, or PII. Any free-form
# error/log text destined for a prompt must pass through scrub_error_for_llm.
# Keep the regex set in lockstep with the Go implementation.
# ------------------------------------------------------------------------------

_SCRUB_REDACTED = "[redacted]"

_SCRUB_PATTERNS = [
    # Postgres not-null/check DETAIL dumps the ENTIRE row — greedy to end of
    # string so a truncated dump can't leak a partial row.
    (re.compile(r"\bFailing row contains\b.*", re.IGNORECASE | re.DOTALL), f"Failing row contains ({_SCRUB_REDACTED})"),
    # Everything after a SQL VALUES keyword is row data (multi-tuple included).
    (re.compile(r"\bVALUES\b\s*\(.*", re.IGNORECASE | re.DOTALL), f"VALUES ({_SCRUB_REDACTED})"),
    # Postgres duplicate-key detail: Key (email)=(user@x.com) — keep column names.
    (re.compile(r"(\bKey\s*\([^)]*\)=)\((?:[^()]|\([^)]*\))*\)", re.IGNORECASE), rf"\1({_SCRUB_REDACTED})"),
    # Credentials embedded in URLs: scheme://user:pass@host
    (re.compile(r"(\w+://)[^/\s:@]+:[^/\s@]*@"), rf"\1{_SCRUB_REDACTED}@"),
    # HTTP credential headers — must run before the KV rule, whose \S+ would
    # otherwise consume only the word "Bearer" and leave the token itself.
    (re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=\-]+", re.IGNORECASE), f"Bearer {_SCRUB_REDACTED}"),
    # key=value / key: value pairs with credential-bearing key names.
    (
        re.compile(
            r"\b(password|passwd|pwd|secret|token|api[_-]?key|apikey|authorization|bearer|access[_-]?key|private[_-]?key|client[_-]?secret)\b\s*[=:]\s*\S+",
            re.IGNORECASE,
        ),
        rf"\1={_SCRUB_REDACTED}",
    ),
    # The same key=value shape when the credential word is fused into a longer
    # key name. The rule above is anchored with \b, and an underscore is a word
    # character, so KAFKA_SASL_PASSWORD, SASL_PASSWORD, sasl_plain_password,
    # KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET and the OAuthClientSecret field of
    # kafkaclient.Config under %#v had no word boundary before the credential
    # word and matched nothing at all. (Config.String() redacts, so %v/%+v are
    # already safe; %#v and reflect-based dumps bypass it.)
    #
    # Kafka is where that bites hardest: every SASL credential this platform
    # reads is spelled that way, and a broker credential is shared across all
    # tenants rather than scoped to one, so a single connect error carrying an
    # env dump leaks a cluster-wide secret. AWS_SECRET_ACCESS_KEY /
    # AWS_SESSION_TOKEN ride the same shape on the MSK IAM path.
    #
    # The key name is kept: it is configuration metadata, and it names the
    # variable an operator has to rotate. Bare "key" is deliberately NOT a
    # suffix — masking it would eat the Postgres "Key (email)=" detail and
    # schema metadata like partition_key. Character classes are spelled out
    # rather than using \w because Python's \w is Unicode-aware and Go's is not.
    # MUST stay lockstep with Go llmscrub + the sink worker's scrubLog.
    (
        re.compile(
            r"\b([A-Za-z0-9_.\-]*(?:password|passwd|pwd|secret|token|api[_.\-]?key|access[_.\-]?key|private[_.\-]?key))\b\s*[=:]\s*\S+",
            re.IGNORECASE,
        ),
        rf"\1={_SCRUB_REDACTED}",
    ),
    # Bare JWTs (base64url evades the base64 rule's charset/length): eyJ = {"
    (re.compile(r"\beyJ[A-Za-z0-9_\-]{4,}(?:\.[A-Za-z0-9_\-]+){1,2}"), _SCRUB_REDACTED),
    # Double-quoted string directly after a colon — Postgres quotes offending
    # VALUES this way ('invalid input syntax for type integer: "jane"') and it
    # catches JSON string values; identifier quoting never follows a colon.
    (re.compile(r'(:\s*)"(?:[^"\\]|\\.)*"'), rf'\1"{_SCRUB_REDACTED}"'),
    # JSON numeric values ("age": 41) — quoted key + colon + number.
    (re.compile(r'("[\w\-]+"\s*:\s*)-?\d[\d.eE+\-]*'), r"\1[num-redacted]"),
    # Single-quoted literals — SQL string values (fail-closed on identifiers).
    # `(?:'|\Z)` also covers a literal left OPEN by upstream log truncation.
    #
    # The leading `(^|[^A-Za-z0-9_])` keeps prose intact: a SQL literal opens at
    # the start of the text or after a delimiter, while an apostrophe between
    # word characters is a contraction ("couldn't", "the user's"). Without it,
    # "Couldn't … pipeline 'orders-sync'" consumed the prose as the literal and
    # left the pipeline name in the clear — redacting the wrong half.
    #
    # Paired and unpaired are ONE rule: a second `'…$` pass re-read the
    # `'[redacted]'` this rule had just written and appended another
    # `'[redacted]`, corrupting already-redacted text. `\Z` (not `$`) so this
    # matches Go's `$`, which never matches before a trailing newline.
    # MUST stay lockstep with Go llmscrub + base_connector scrub_sensitive.
    (re.compile(r"(^|[^A-Za-z0-9_])'(?:[^'\\]|\\.)*(?:'|\Z)", re.DOTALL), rf"\1'{_SCRUB_REDACTED}'"),
    # Email addresses appearing outside quotes.
    (re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"), "[email-redacted]"),
    # IPv4 addresses — client/host IPs are personal data under GDPR and aren't
    # needed for diagnosis. Octets are validated 0-255, so 3-part version strings
    # (8.0.32) and ISO dates don't match; a 4-part dotted-quad (incl. a rare
    # 1.2.3.4 version) is an accepted loss. MUST stay lockstep with Go llmscrub.
    (
        re.compile(r"\b(?:(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\b"),
        "[ip-redacted]",
    ),
    # Long base64-ish blobs (tokens, keys). 40+ chars keeps 32-hex trace ids.
    (re.compile(r"\b[A-Za-z0-9+/]{40,}={0,2}\b"), _SCRUB_REDACTED),
    # Dashed SSN / US-phone shapes (ISO dates \d{4}-\d{2}-\d{2} don't match —
    # timestamps must survive for diagnosis).
    (re.compile(r"\b\d{3}-\d{2}-\d{4}\b"), "[num-redacted]"),
    (re.compile(r"\b\d{3}[-.]\d{3}[-.]\d{4}\b"), "[num-redacted]"),
    # Digit runs of 7+ — likely record ids / SSNs / phones; ports and counts survive.
    (re.compile(r"\d{7,}"), "[num-redacted]"),
]


def scrub_error_for_llm(text: Optional[str], max_len: int = 0) -> str:
    """Mask likely customer data (row values, credentials, PII) in error text.

    Lossy by design: losing a fragment of an error message costs diagnosis
    quality; leaking a row value breaks the privacy contract. Truncation (when
    ``max_len`` > 0) happens after scrubbing so a cut can never split a masked
    token back open.
    """
    if not text:
        return ""
    result = text
    for pattern, replacement in _SCRUB_PATTERNS:
        result = pattern.sub(replacement, result)
    if max_len > 0 and len(result) > max_len:
        result = result[:max_len] + "…"
    return result


# Convenience function for trace context with masked config
def log_context(trace_id: str, config: Optional[Dict[str, Any]] = None, **extra) -> Dict[str, Any]:
    """
    Create a log context dictionary with trace_id and masked config.
    
    Args:
        trace_id: Trace ID for correlation
        config: Optional config to mask and include
        **extra: Additional fields to include
        
    Returns:
        Dictionary suitable for structured logging
    """
    context = {
        "trace_id": trace_id,
        **extra
    }
    
    if config is not None:
        context["config_summary"] = config_summary(config)
    
    return context
