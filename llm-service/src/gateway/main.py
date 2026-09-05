from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import os
import json
from datetime import datetime
import re
import logging
import asyncio
from typing import Optional
import sqlglot

# Configure trace-aware logging BEFORE importing any first-party module. This
# ordering is load-bearing, not cosmetic: several `src.*` modules log at import
# time — the Explorer routers record which LLM provider and model each endpoint
# resolved to, which is the audit trail for what leaves this deployment. A
# record emitted before any handler is installed falls through to
# logging.lastResort, which passes WARNING and above only, so those INFO lines
# are dropped on the floor while their ERROR siblings survive. The imports below
# therefore sit under this call on purpose (hence the E402 markers), and
# tests/test_startup_logging_order.py fails if one drifts back above it.
from src.utils.telemetry import (
    init_tracer, instrument_fastapi, instrument_requests,
    start_span, get_current_trace_id, setup_logging
)

SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "llm-service")
setup_logging(SERVICE_NAME)

from src.utils.prompt_registry import PromptRegistry  # noqa: E402
from src.utils.chat_knowledge import ChatKnowledge  # noqa: E402
from src.text2sql.schema import TypedSchema, TableSchema, ColumnSchema, ForeignKey  # noqa: E402
from src.text2sql.query_spec import QuerySpec  # noqa: E402
from src.text2sql.normalizer import normalize_query_spec, coerce_query_spec_payload  # noqa: E402
from src.text2sql.compiler import compile_query_spec, resolve_dialect as _resolve_sqlglot_dialect  # noqa: E402
from src.text2sql.engine_profile import get_engine_profile  # noqa: E402
from src.text2sql.validator import validate_sql  # noqa: E402
from src.text2sql.mysql_sanitize import unqualify_builtin_functions as mysql_unqualify_builtin_functions  # noqa: E402
from src.text2sql.mysql_sanitize import fix_only_full_group_by_order as mysql_fix_only_full_group_by_order  # noqa: E402
from src.text2sql.mysql_sanitize import quote_reserved_identifiers as mysql_quote_reserved_identifiers  # noqa: E402
from src.agents.suggestions.api import router as suggestions_router  # noqa: E402
from src.agents.explorer.api import router as explorer_router  # noqa: E402
from src.agents.explorer.rank_tables import router as rank_tables_router  # noqa: E402
from src.utils.ratelimit import get_limiter  # noqa: E402
from src.utils.openai_client import (  # noqa: E402
    env_bool as _env_bool,
    resolve_explorer_provider as _resolve_explorer_provider,
    explorer_default_model as _explorer_default_model,
    explorer_default_sql_model as _explorer_default_sql_model,
    # Provider resolution and client construction, imported rather than
    # reimplemented. This module carried private copies of all three until they
    # were found to disagree with the shared ones — see the note above
    # DEFAULT_LLM_PROVIDER below.
    resolve_provider as _resolve_provider,
    make_async_client as _create_async_client,
    client_egress_host as _client_egress_host,
    _ollama_base_url,
)


def mock_intent_classification(user_msg: str) -> str:
    """
    Mock intent classification for when LLM is not available.
    Returns JSON string matching the expected format.
    """
    user_msg = user_msg.lower()

    def has_word(word: str) -> bool:
        # word boundary match (avoid "my" matching "mysql")
        return re.search(r"(?:^|\\b)" + re.escape(word) + r"(?:\\b|$)", user_msg) is not None
    
    # Connector detection
    sources = {
        "mysql": ["mysql", "mariadb"],
        "postgresql": ["postgres", "postgresql", "pg"],
        "mongodb": ["mongodb", "mongo"],
        "oracle": ["oracle"],
        "sqlserver": ["sqlserver", "mssql", "sql server"],
        "cassandra": ["cassandra"],
        "sqlite": ["sqlite"],
        "dynamodb": ["dynamodb", "dynamo"],
        "db2": ["db2"],
    }
    
    destinations = {
        "s3": ["s3", "aws-s3", "aws s3", "amazon s3"],
        "snowflake": ["snowflake"],
        "bigquery": ["bigquery", "bq"],
        "redshift": ["redshift"],
        "elasticsearch": ["elasticsearch", "elastic"],
        "kafka": ["kafka"],
        "minio": ["minio"],
        "gcs": ["gcs", "google cloud storage"],
        "azure-blob": ["azure blob", "azure-blob", "blob storage"],
        # Databases can also be destinations (DB → DB)
        "mysql": ["mysql", "mariadb"],
        "postgresql": ["postgres", "postgresql", "pg"],
        "mongodb": ["mongodb", "mongo"],
    }
    
    detected_source = ""
    detected_dest = ""
    
    # Prefer explicit "from X to Y" / "X to Y" parsing.
    # This handles DB→DB like "postgresql to mysql" reliably.
    m_pair = re.search(r"\\bfrom\\s+([a-z0-9_-]+)\\s+(?:to|->|→)\\s*([a-z0-9_-]+)\\b", user_msg)
    if not m_pair:
        m_pair = re.search(r"\\b([a-z0-9_-]+)\\s+(?:to|->|→)\\s*([a-z0-9_-]+)\\b", user_msg)

    def resolve_connector(token: str) -> str:
        t = (token or "").strip().lower()
        for name, aliases in {**sources, **destinations}.items():
            for alias in aliases:
                if t == alias.replace(" ", "") or t == alias.replace(" ", "-") or t == alias:
                    return name
        # Fallback: direct key match
        if t in sources:
            return t
        if t in destinations:
            return t
        return ""

    if m_pair:
        a = resolve_connector(m_pair.group(1))
        b = resolve_connector(m_pair.group(2))
        if a:
            detected_source = a
        if b:
            detected_dest = b

    # If not found via pair parsing, fall back to loose detection.
    if not detected_source:
        for name, aliases in sources.items():
            for alias in aliases:
                if alias in user_msg:
                    detected_source = name
                    break
            if detected_source:
                break

    if not detected_dest:
        # Prefer token after "to" if present
        m_to = re.search(r"\\bto\\s+([a-z0-9_-]+)\\b", user_msg)
        if m_to:
            detected_dest = resolve_connector(m_to.group(1))
        if not detected_dest:
            for name, aliases in destinations.items():
                for alias in aliases:
                    if alias in user_msg:
                        detected_dest = name
                        break
                if detected_dest:
                    break
    
    # Some connectors can be both source and destination
    if not detected_source:
        for name, aliases in destinations.items():
            for alias in aliases:
                if alias in user_msg:
                    # Check if it's used as source (before "to")
                    if f"{alias} to" in user_msg or f"from {alias}" in user_msg:
                        detected_source = name
                        break
            if detected_source:
                break
    
    # Sync mode detection
    cdc_keywords = ["stream", "streaming", "real-time", "realtime", "live", "cdc", "continuous", "replicate"]
    batch_keywords = ["sync", "move", "transfer", "copy", "export", "migrate", "backup", "import", "load", "pull", "push"]
    
    sync_mode = "auto"
    for kw in cdc_keywords:
        if kw in user_msg:
            sync_mode = "cdc"
            break
    if sync_mode == "auto":
        for kw in batch_keywords:
            if kw in user_msg:
                sync_mode = "batch"
                break
    
    # Extract table names
    tables = []
    table_patterns = [
        r'\b(\w+)\s+table\b',
        r'\btable\s+(\w+)\b',
        r'\b(users?|customers?|orders?|products?|transactions?|events?|accounts?|items?|logs?)\b'
    ]
    for pattern in table_patterns:
        matches = re.findall(pattern, user_msg)
        for match in matches:
            if match.lower() not in ["the", "a", "an", "this", "that", "mysql", "postgres", "s3", "snowflake"]:
                tables.append(match)
    
    # Help/how-to questions: if user is asking "how" / "help" / "what is" and hasn't provided
    # both source AND destination, treat as general knowledge (no execution).
    if (has_word("how") or has_word("help") or "what is" in user_msg or "how to" in user_msg) and not (detected_source and detected_dest):
        return json.dumps({"intent": "general_knowledge", "requires_execution": False, "parameters": {}})

    # Check for data sync intent
    # Prefer explicit detection of both ends; otherwise require a real sync verb (not just "to").
    sync_verbs = ["sync", "move", "transfer", "copy", "export", "migrate", "stream", "replicate", "backup", "import", "load", "pull", "push", "send", "fetch"]
    has_sync_verb = any(has_word(kw) for kw in sync_verbs)

    if detected_source and detected_dest:
        # Even without an explicit verb, "mysql to s3" should be treated as data_sync in mock mode.
        # Return format expected by prompts/chat/intent_classification.yaml
        return json.dumps({
            "intent": "data_sync",
            "requires_execution": True,
            "parameters": {
                "source": detected_source,
                "destination": detected_dest,
                "tables": tables if tables else [],
                "sync_mode": sync_mode,
            }
        })
    if (detected_source or detected_dest) and has_sync_verb:
        return json.dumps({
            "intent": "data_sync",
            "requires_execution": True,
            "parameters": {
                "source": detected_source,
                "destination": detected_dest,
                "tables": tables if tables else [],
                "sync_mode": sync_mode,
            }
        })
    
    # Check for list connections
    if has_word("connection") and any(has_word(kw) for kw in ["list", "show", "all", "my", "display"]):
        return json.dumps({"intent": "list_connections", "requires_execution": True, "parameters": {}})
    
    # Check for list pipelines
    if has_word("pipeline") and any(has_word(kw) for kw in ["list", "show", "all", "my", "display"]):
        return json.dumps({"intent": "list_pipelines", "requires_execution": True, "parameters": {}})
    
    # Check for create connection
    if has_word("create") and has_word("connection"):
        return json.dumps({"intent": "create_connection", "requires_execution": True, "parameters": {}})
    
    # Check for test connection
    if (has_word("test") or has_word("check")) and has_word("connection"):
        return json.dumps({"intent": "check_connection", "requires_execution": True, "parameters": {}})
    
    # Default to general knowledge
    return json.dumps({"intent": "general_knowledge", "requires_execution": False, "parameters": {}})


def mock_schema_healer(prompt: str) -> str:
    """
    Mock schema healer for analyzing schema changes.
    Determines if changes are safe to auto-apply.
    """
    prompt_lower = prompt.lower()
    
    # Detect change type
    if "add column" in prompt_lower or "add_column" in prompt_lower:
        result = {
            "safe_to_auto_migrate": True,
            "reasoning": "ADD COLUMN is safe - new columns with NULL default don't affect existing data",
            "suggested_ddl": None,
            "risks": [],
            "requires_approval": False,
            "notify_user": False,
            "user_message": None
        }
    elif "drop column" in prompt_lower or "drop_column" in prompt_lower:
        result = {
            "safe_to_auto_migrate": False,
            "reasoning": "DROP COLUMN may cause data loss - requires user approval",
            "suggested_ddl": None,
            "risks": ["Data loss", "Breaking downstream queries"],
            "requires_approval": True,
            "notify_user": True,
            "user_message": "A column drop was detected. Please review before applying."
        }
    elif "drop table" in prompt_lower or "drop_table" in prompt_lower:
        result = {
            "safe_to_auto_migrate": False,
            "reasoning": "DROP TABLE will cause complete data loss - requires explicit approval",
            "suggested_ddl": None,
            "risks": ["Complete data loss", "Breaking downstream dependencies"],
            "requires_approval": True,
            "notify_user": True,
            "user_message": "A table drop was detected. This is a destructive operation."
        }
    elif "modify column" in prompt_lower or "modify_column" in prompt_lower or "alter column" in prompt_lower:
        result = {
            "safe_to_auto_migrate": False,
            "reasoning": "Column type change may cause data truncation",
            "suggested_ddl": None,
            "risks": ["Data truncation", "Type conversion errors"],
            "requires_approval": True,
            "notify_user": True,
            "user_message": "A column type change was detected. Please verify data compatibility."
        }
    elif "create table" in prompt_lower or "create_table" in prompt_lower:
        result = {
            "safe_to_auto_migrate": True,
            "reasoning": "CREATE TABLE is safe - no existing data affected",
            "suggested_ddl": None,
            "risks": [],
            "requires_approval": False,
            "notify_user": False,
            "user_message": None
        }
    else:
        # Unknown change type - be cautious
        result = {
            "safe_to_auto_migrate": False,
            "reasoning": "Unknown schema change type - requiring manual review for safety",
            "suggested_ddl": None,
            "risks": ["Unknown impact"],
            "requires_approval": True,
            "notify_user": True,
            "user_message": "A schema change was detected that requires manual review."
        }
    
    return json.dumps(result)


def mock_decision_agent(prompt: str) -> str:
    """
    Mock decision agent for determining sync mode.
    Parses the prompt to extract context and make a decision.
    """
    prompt_lower = prompt.lower()
    
    # CDC-capable sources
    cdc_sources = ["mysql", "postgresql", "postgres", "mongodb", "sqlserver", "oracle", "db2", "cassandra"]
    
    # API sources (batch only)
    api_sources = ["freshdesk", "hubspot", "salesforce", "zendesk", "jira", "stripe", "shopify"]
    
    # Detect source type from prompt
    source_type = None
    is_api_source = False
    for src in api_sources:
        if src in prompt_lower:
            source_type = src
            is_api_source = True
            break
    
    if not source_type:
        for src in cdc_sources:
            if src in prompt_lower:
                source_type = src
                break
    
    # Detect user intent
    wants_cdc = any(kw in prompt_lower for kw in ["stream", "real-time", "realtime", "continuous", "cdc", "replicate"])
    wants_batch = any(kw in prompt_lower for kw in ["sync", "backup", "copy", "migrate", "export", "one-time"])
    
    # Check for active pipelines mentioned
    has_active_pipelines = "active pipeline" in prompt_lower or "running pipeline" in prompt_lower
    
    # Make decision
    if is_api_source:
        # API sources always use batch with pagination
        result = {
            "recommended_mode": "batch",
            "reason": f"{source_type.title()} is an API connector. Using batch mode with pagination.",
            "source_connection_id": None,
            "dest_connection_id": None,
            "requires_user_choice": False,
            "choice_type": None,
            "choice_message": None,
            "choice_options": None,
            "auto_proceed": True,
            "additional_context": {
                "cdc_method": "polling",
                "debezium_connector": None
            }
        }
    elif source_type and source_type in cdc_sources:
        if wants_cdc:
            result = {
                "recommended_mode": "cdc",
                "reason": f"Using real-time CDC streaming for {source_type.title()}.",
                "source_connection_id": None,
                "dest_connection_id": None,
                "requires_user_choice": False,
                "choice_type": None,
                "choice_message": None,
                "choice_options": None,
                "auto_proceed": True,
                "additional_context": {
                    "cdc_method": "debezium",
                    "debezium_connector": f"io.debezium.connector.{source_type.lower()}.{source_type.title()}Connector"
                }
            }
        elif wants_batch and not has_active_pipelines:
            result = {
                "recommended_mode": "batch",
                "reason": f"One-time batch sync from {source_type.title()}.",
                "source_connection_id": None,
                "dest_connection_id": None,
                "requires_user_choice": False,
                "choice_type": None,
                "choice_message": None,
                "choice_options": None,
                "auto_proceed": True,
                "additional_context": {
                    "cdc_method": None,
                    "debezium_connector": None
                }
            }
        else:
            # Ask user to choose
            result = {
                "recommended_mode": "batch",
                "reason": f"{source_type.title()} supports both batch and real-time sync.",
                "source_connection_id": None,
                "dest_connection_id": None,
                "requires_user_choice": True,
                "choice_type": "sync_mode_select",
                "choice_message": f"How would you like to sync data from {source_type.title()}?",
                "choice_options": [
                    {"id": "batch", "label": "📦 One-time sync (Batch)"},
                    {"id": "cdc", "label": "🔄 Real-time sync (CDC)"},
                    {"id": "initial_plus_cdc", "label": "📦➕🔄 Full sync + Real-time"}
                ],
                "auto_proceed": False,
                "additional_context": {
                    "cdc_method": "debezium",
                    "debezium_connector": None
                }
            }
    else:
        # Unknown source - default to batch
        result = {
            "recommended_mode": "batch",
            "reason": "Using batch mode for data sync.",
            "source_connection_id": None,
            "dest_connection_id": None,
            "requires_user_choice": False,
            "choice_type": None,
            "choice_message": None,
            "choice_options": None,
            "auto_proceed": True,
            "additional_context": {
                "cdc_method": None,
                "debezium_connector": None
            }
        }
    
    return json.dumps(result)

def mock_slot_filling(
    user_msg: str,
    state: str,
    pending_source: str = "",
    pending_destination: str = "",
    last_response: str = "",
) -> str:
    """
    Mock slot filling for multi-turn chat when LLM is not available.
    Returns JSON string matching the slot_filling prompt contract.
    """
    msg = (user_msg or "").lower()
    state = (state or "").lower()

    stopwords = {
        "want", "create", "setup", "make", "build", "new", "pipeline",
        "sync", "transfer", "move", "copy", "data", "help", "please",
        "need", "like", "would", "can", "could", "should",
        "the", "a", "an", "my", "our", "some", "any",
    }

    aliases = {
        # Sources
        "mysql": ["mysql", "mariadb"],
        "postgresql": ["postgresql", "postgres", "pg"],
        "mongodb": ["mongodb", "mongo"],
        "oracle": ["oracle"],
        "sqlserver": ["sqlserver", "mssql", "sql server"],
        # Destinations / mixed
        "aws-s3": ["aws-s3", "aws s3", "amazon s3", "s3"],
        "snowflake": ["snowflake"],
        "bigquery": ["bigquery", "bq"],
        "redshift": ["redshift"],
        "elasticsearch": ["elasticsearch", "elastic", "es"],
        "kafka": ["kafka"],
        "minio": ["minio"],
        "google-cloud-storage": ["google cloud storage", "gcs"],
        "azure-blob-storage": ["azure blob storage", "azure blob", "azure-blob", "blob storage"],
        "redis": ["redis"],
    }

    def find_connector(text: str) -> str:
        for canonical, al in aliases.items():
            for a in al:
                # word-ish boundary match; also allows multi-word aliases
                if re.search(r"(?:^|\\b)" + re.escape(a) + r"(?:\\b|$)", text):
                    if a.strip() in stopwords:
                        continue
                    return canonical
        return ""

    extracted_value = find_connector(msg)

    # Determine which slot we're trying to fill
    desired_slot = None
    if "awaiting_source" in state:
        desired_slot = "source"
    elif "awaiting_destination" in state:
        desired_slot = "destination"
    elif pending_source.strip() == "":
        desired_slot = "source"
    elif pending_destination.strip() == "":
        desired_slot = "destination"

    # Apply extraction
    new_source = pending_source or ""
    new_dest = pending_destination or ""
    extracted_slot = None
    confidence = 0.0

    if extracted_value:
        extracted_slot = desired_slot
        confidence = 0.95
        if desired_slot == "source":
            new_source = extracted_value
        elif desired_slot == "destination":
            new_dest = extracted_value

    still_missing = []
    if (new_source or "").strip() == "":
        still_missing.append("source")
    if (new_dest or "").strip() == "":
        still_missing.append("destination")

    next_question = None
    if still_missing:
        if still_missing[0] == "source":
            next_question = "Great — what’s your data source? (e.g., MySQL, Postgres, MongoDB)"
        else:
            next_question = "Great — where should the data go? (e.g., S3, Snowflake, BigQuery)"

    result = {
        "answering_previous": state.startswith("awaiting_") or bool(last_response),
        "extracted_slot": extracted_slot,
        "extracted_value": extracted_value or None,
        "still_missing": still_missing,
        "next_question": next_question,
        "confidence": confidence,
    }

    return json.dumps(result)

def mock_help_response(user_msg: str) -> str:
    """
    Mock help/how-to response for pipeline creation guidance.
    Returns JSON string matching prompts/chat/help_response.yaml contract.
    """
    msg = (user_msg or "").strip()
    low = msg.lower()

    # Very light heuristic personalization
    # Prefer explicit "<x> to <y>" parsing; otherwise look for a single connector mention.
    src = ""
    dst = ""

    m_pair = re.search(r"\\bfrom\\s+([a-z0-9_-]+)\\s+(?:to|->|→)\\s*([a-z0-9_-]+)\\b", low)
    if not m_pair:
        m_pair = re.search(r"\\b([a-z0-9_-]+)\\s+(?:to|->|→)\\s*([a-z0-9_-]+)\\b", low)
    if m_pair:
        src = m_pair.group(1)
        dst = m_pair.group(2)
    else:
        for s in ["mysql", "postgres", "postgresql", "mongodb", "mongo", "oracle", "sqlserver", "mssql"]:
            if re.search(r"(?:^|\\b)" + re.escape(s) + r"(?:\\b|$)", low):
                src = "postgres" if s == "postgresql" else s
                break

        for d in ["s3", "aws-s3", "snowflake", "bigquery", "redshift", "minio", "kafka", "elasticsearch"]:
            if re.search(r"(?:^|\\b)" + re.escape(d) + r"(?:\\b|$)", low):
                dst = "aws-s3" if d == "s3" else d
                break

    if src and dst:
        message = (
            f"To create a pipeline, you can say something like: `{src} to {dst}`.\n"
            f"Optionally add tables (e.g. `users, orders`) and mode (`batch` or `cdc`)."
        )
        suggestions = [
            f"{src} to {dst}",
            f"{src} users table to {dst}",
            f"stream {src} to {dst}",
            f"copy {src} to {dst}",
        ]
    elif src and not dst:
        message = (
            f"Great — you mentioned **{src}**. Next, tell me the destination.\n"
            "Example: `mysql to s3`."
        )
        suggestions = [
            f"{src} to aws-s3",
            f"{src} to snowflake",
            f"{src} to bigquery",
            f"stream {src} to redshift",
        ]
    elif dst and not src:
        message = (
            f"Great — you mentioned **{dst}**. Next, tell me the source.\n"
            "Example: `postgres to s3`."
        )
        suggestions = [
            f"mysql to {dst}",
            f"postgres to {dst}",
            f"mongodb to {dst}",
            f"stream postgres to {dst}",
        ]
    else:
        message = (
            "To create a pipeline, tell me **your source** and **destination**.\n"
            "Example: `mysql to aws-s3`.\n"
            "Optionally include tables (e.g. `users, orders`) and mode (`batch` or `cdc`)."
        )
        suggestions = [
            "mysql to aws-s3",
            "postgres to bigquery",
            "mysql users table to snowflake",
            "stream postgres to redshift",
        ]

    return json.dumps({"message": message, "suggestions": suggestions})

logger = logging.getLogger("llm-gateway")

# Initialize OpenTelemetry tracer
tracer = init_tracer("llm-service", "1.0.0")
instrument_requests()

app = FastAPI(title="LLM Gateway")

# Instrument FastAPI with OpenTelemetry
instrument_fastapi(app)

# Prometheus metrics endpoint (/prometheus) — scraped by OTEL Collector → SigNoz. F-Obs-2.
from src.utils.metrics import mount_metrics
mount_metrics(app)

# ============================================================================
# Phase 5: Safety controls (rate limiting + cost caps)
# ============================================================================

def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name, "") or "").strip()
    if raw == "":
        return default
    try:
        return int(raw)
    except Exception:
        return default

LLM_MAX_REQUEST_BYTES = _env_int("RSYNC_LLM_MAX_REQUEST_BYTES", 200_000)  # 200KB
LLM_MAX_CONCURRENCY = _env_int("RSYNC_LLM_MAX_CONCURRENCY", 8)

# Fixed-window rate limits (per minute).
RL_IP_PER_MIN = _env_int("RSYNC_LLM_RL_IP_PER_MIN", 300)
RL_USER_PER_MIN = _env_int("RSYNC_LLM_RL_USER_PER_MIN", 600)
# Stricter for expensive endpoints — raised to avoid false 429s during pipeline creation
# (schema fetch + AI suggestions both hit /v1/completion in quick succession).
RL_COMPLETION_PER_MIN = _env_int("RSYNC_LLM_RL_COMPLETION_PER_MIN", 120)
RL_SQL_PER_MIN = _env_int("RSYNC_LLM_RL_SQL_PER_MIN", 60)

_limiter = get_limiter()
_semaphore = asyncio.Semaphore(max(1, LLM_MAX_CONCURRENCY))

def _client_ip(req: Request) -> str:
    xff = (req.headers.get("x-forwarded-for") or "").split(",")[0].strip()
    if xff:
        return xff
    return (req.client.host if req.client else "unknown") or "unknown"

def _user_key(req: Request) -> Optional[str]:
    # Best-effort identity header forwarded by gateway/services.
    for h in ("x-rsync-user-id", "x-user-id", "x-userid", "x-forwarded-user"):
        v = (req.headers.get(h) or "").strip()
        if v:
            return v
    return None

@app.middleware("http")
async def safety_middleware(request: Request, call_next):
    # Never rate limit health checks.
    if request.url.path == "/health":
        return await call_next(request)

    async with _semaphore:
        # Enforce request size caps for body-carrying methods.
        if request.method in ("POST", "PUT", "PATCH"):
            body = await request.body()
            if body is not None and len(body) > LLM_MAX_REQUEST_BYTES:
                return JSONResponse(
                    status_code=413,
                    content={
                        "error": "request_too_large",
                        "message": f"Request body exceeds limit ({LLM_MAX_REQUEST_BYTES} bytes)",
                    },
                )

        ip = _client_ip(request)
        user = _user_key(request)
        path = request.url.path

        # Global limits
        try:
            ok, retry = await _limiter.hit(f"rl:ip:{ip}", RL_IP_PER_MIN, 60)
            if not ok:
                return JSONResponse(
                    status_code=429,
                    content={"error": "rate_limited", "scope": "ip"},
                    headers={"Retry-After": str(retry)},
                )
            if user:
                ok, retry = await _limiter.hit(f"rl:user:{user}", RL_USER_PER_MIN, 60)
                if not ok:
                    return JSONResponse(
                        status_code=429,
                        content={"error": "rate_limited", "scope": "user"},
                        headers={"Retry-After": str(retry)},
                    )
        except Exception as e:
            # Fail open if Redis is temporarily unavailable.
            logger.warning(f"⚠️  Rate limiter error (fail-open): {e}")

        # Endpoint-specific limits
        try:
            if request.method == "POST" and path == "/v1/completion":
                k = f"rl:completion:{user or ip}"
                ok, retry = await _limiter.hit(k, RL_COMPLETION_PER_MIN, 60)
                if not ok:
                    return JSONResponse(
                        status_code=429,
                        content={"error": "rate_limited", "scope": "completion"},
                        headers={"Retry-After": str(retry)},
                    )
            if request.method == "POST" and path == "/api/v1/sql/generate":
                k = f"rl:sql:{user or ip}"
                ok, retry = await _limiter.hit(k, RL_SQL_PER_MIN, 60)
                if not ok:
                    return JSONResponse(
                        status_code=429,
                        content={"error": "rate_limited", "scope": "sql_generate"},
                        headers={"Retry-After": str(retry)},
                    )
        except Exception as e:
            logger.warning(f"⚠️  Rate limiter error (fail-open): {e}")

        return await call_next(request)

# Include routers
app.include_router(suggestions_router)
app.include_router(explorer_router)
app.include_router(rank_tables_router)

# Mount PII scanner HTTP endpoints
try:
    from src.agents.pii_scanner.service import create_router as _create_pii_router
    _pii_router = _create_pii_router()
    if _pii_router is not None:
        app.include_router(_pii_router)
        logger.info("PII scanner router mounted at /pii/*")
except Exception as _exc:
    logger.warning("PII scanner router unavailable: %s", _exc)

# Start async PII Kafka consumer as background task on startup
@app.on_event("startup")
async def _start_pii_kafka_consumer() -> None:
    try:
        from src.agents.pii_scanner.kafka_consumer import run_pii_kafka_consumer
        asyncio.create_task(run_pii_kafka_consumer())
        logger.info("PII Kafka consumer background task started")
    except Exception as exc:
        logger.warning("PII Kafka consumer failed to start: %s", exc)

# Initialize Registry, Knowledge Base, and OpenAI Client
registry = PromptRegistry(prompts_dir="prompts")
chat_knowledge = ChatKnowledge()

# Global mock mode (useful for tests). Default should be FALSE.
USE_MOCK = _env_bool("USE_MOCK_LLM", False)

# Operational guardrail: never allow mock LLM in non-dev environments.
# This prevents a misconfigured flag from silently routing production traffic through stub logic.
if USE_MOCK:
    env = (
        (os.getenv("ENVIRONMENT") or os.getenv("APP_ENV") or os.getenv("ENV") or os.getenv("NODE_ENV") or "")
        .strip()
        .lower()
    )
    # "docker" is treated as a local/dev environment in our compose setup.
    allowed = {"development", "dev", "test", "testing", "local", "docker"}
    # Treat an unset environment as unsafe: require an explicit dev/test environment when mocking.
    if env not in allowed:
        env_display = env or "<unset>"
        raise RuntimeError(
            f"USE_MOCK_LLM=true is not allowed when ENVIRONMENT={env_display!r}. "
            "Set USE_MOCK_LLM=false (recommended) or run with ENVIRONMENT=development/test."
        )

# Default LLM client for /v1/completion and other non-Explorer prompts.
DEFAULT_LLM_PROVIDER = _resolve_provider(os.getenv("LLM_PROVIDER", ""))
default_client = None if USE_MOCK else _create_async_client(DEFAULT_LLM_PROVIDER)
# LLM_MODEL overrides the model baked into prompt YAML files (e.g. gpt-4o → qwen2.5:7b).
# When LLM_PROVIDER=ollama this should point to a locally-pulled model name.
# When LLM_PROVIDER=groq set this to e.g. "llama-3.3-70b-versatile".
DEFAULT_LLM_MODEL = os.getenv("LLM_MODEL", "").strip()

# Explorer offline mode.
# Default is now False — Explorer uses the same LLM_PROVIDER as the rest of the stack
# (Azure OpenAI in production). Set EXPLORER_OFFLINE_ONLY=true to force Ollama for air-gapped
# or on-prem deployments where schema metadata must not leave the system.
EXPLORER_OFFLINE_ONLY = _env_bool("EXPLORER_OFFLINE_ONLY", False)
# Provider + model defaults come from src.utils.openai_client so the gateway and the
# explorer router (src/agents/explorer/api.py) cannot drift apart. They previously
# each carried their own copy and disagreed about whether LLM_MODEL applied.
EXPLORER_LLM_PROVIDER = _resolve_explorer_provider()
explorer_client = None if USE_MOCK else _create_async_client(EXPLORER_LLM_PROVIDER)
_default_explorer_model = _explorer_default_model(EXPLORER_LLM_PROVIDER)
_default_sql_model = _explorer_default_sql_model(EXPLORER_LLM_PROVIDER)
EXPLORER_TABLE_LINK_MODEL = (os.getenv("EXPLORER_TABLE_LINK_MODEL") or _default_explorer_model).strip()
EXPLORER_COLUMN_LINK_MODEL = (os.getenv("EXPLORER_COLUMN_LINK_MODEL") or _default_explorer_model).strip()
EXPLORER_NEXT_STEPS_MODEL = (os.getenv("EXPLORER_NEXT_STEPS_MODEL") or _default_explorer_model).strip()
EXPLORER_QUERY_SPEC_MODEL = (os.getenv("EXPLORER_QUERY_SPEC_MODEL") or _default_explorer_model).strip()
EXPLORER_SQL_MODEL = (os.getenv("EXPLORER_SQL_MODEL") or _default_sql_model).strip()
EXPLORER_SQL_MODEL_MYSQL = (os.getenv("EXPLORER_SQL_MODEL_MYSQL") or _default_sql_model).strip()

# SQL generation provider — defaults to same as Explorer provider (online when offline_only=false).
EXPLORER_SQL_ALLOW_ONLINE = _env_bool("EXPLORER_SQL_ALLOW_ONLINE", not EXPLORER_OFFLINE_ONLY)
EXPLORER_SQL_PROVIDER = os.getenv("EXPLORER_SQL_PROVIDER", "").strip()
if EXPLORER_OFFLINE_ONLY and not EXPLORER_SQL_ALLOW_ONLINE:
    EXPLORER_SQL_PROVIDER = "ollama"
sql_provider_resolved = _resolve_provider(EXPLORER_SQL_PROVIDER) if EXPLORER_SQL_PROVIDER else EXPLORER_LLM_PROVIDER
if EXPLORER_OFFLINE_ONLY and not EXPLORER_SQL_ALLOW_ONLINE and sql_provider_resolved != "ollama":
    sql_provider_resolved = "ollama"
sql_client = None if USE_MOCK else _create_async_client(sql_provider_resolved)

# Announce where Explorer prompts actually go. Without this, "I set
# EXPLORER_OFFLINE_ONLY=true" and "the flag reached the container" look identical
# from outside the process — an operator who believes schema metadata is staying
# on-prem has no way to check. Logged once at import; no keys, no endpoints.
#
# `egress=` is read off the CONSTRUCTED clients, not off the provider string.
# That distinction is the whole point: when this module built its own clients,
# LLM_PROVIDER=groq produced an OpenAI client and this line still said
# provider=groq, so the log confirmed a destination the process was not using.
logger.info(
    "explorer llm: provider=%s offline_only=%s sql_provider=%s "
    "table_link_model=%s column_link_model=%s query_spec_model=%s sql_model=%s "
    "ollama_base=%s egress=%s sql_egress=%s default_egress=%s",
    EXPLORER_LLM_PROVIDER,
    EXPLORER_OFFLINE_ONLY,
    sql_provider_resolved,
    EXPLORER_TABLE_LINK_MODEL,
    EXPLORER_COLUMN_LINK_MODEL,
    EXPLORER_QUERY_SPEC_MODEL,
    EXPLORER_SQL_MODEL,
    _ollama_base_url() if "ollama" in (EXPLORER_LLM_PROVIDER, sql_provider_resolved) else "-",
    _client_egress_host(explorer_client),
    _client_egress_host(sql_client),
    _client_egress_host(default_client),
)
if EXPLORER_OFFLINE_ONLY and "ollama" not in (EXPLORER_LLM_PROVIDER, sql_provider_resolved):
    logger.error(
        "EXPLORER_OFFLINE_ONLY=true but Explorer resolved to provider=%s sql_provider=%s — "
        "schema metadata WILL leave this deployment",
        EXPLORER_LLM_PROVIDER,
        sql_provider_resolved,
    )

# Instrument the three gateway LLM clients once at module load so every
# scattered `*.completions.create` call site is recorded with no per-call
# edits. F-Obs-2 (Obs-LLMService).
from src.utils.metrics import instrument_openai_client as _instrument_openai_client
_instrument_openai_client(default_client, service="gateway", provider=DEFAULT_LLM_PROVIDER)
_instrument_openai_client(explorer_client, service="gateway", provider=EXPLORER_LLM_PROVIDER)
_instrument_openai_client(sql_client, service="gateway", provider=sql_provider_resolved)

def _explorer_model_for_sql(dialect: str) -> str:
    d = (dialect or "").strip().lower()
    if "mysql" in d or "mariadb" in d:
        return EXPLORER_SQL_MODEL_MYSQL
    return EXPLORER_SQL_MODEL

def _infer_time_grain(question: str) -> str:
    """Infer requested time bucket grain from a natural-language question."""
    q = (question or "").strip().lower()
    if not q:
        return ""
    # Prefer coarser explicit grains first.
    if re.search(r"\b(by|per)\s+month\b|\bmonth[-\s]?wise\b|\bmonthly\b", q):
        return "month"
    if re.search(r"\b(by|per)\s+week\b|\bweek[-\s]?wise\b|\bweekly\b", q):
        return "week"
    if re.search(r"\b(by|per)\s+day\b|\bday[-\s]?wise\b|\bdaily\b", q):
        return "day"
    if re.search(r"\b(by|per)\s+quarter\b|\bquarter[-\s]?wise\b|\bquarterly\b", q):
        return "quarter"
    if re.search(r"\b(by|per)\s+year\b|\byear[-\s]?wise\b|\byearly\b|\bannual(?:ly)?\b", q):
        return "year"
    return ""


def _infer_time_range_relative(question: str) -> str:
    """
    Infer a relative time range token from a natural-language question.

    Returns a QuerySpec.time_range.relative value (e.g., 'last_30_days', 'last_6_months').
    """
    q = (question or "").strip().lower()
    if not q:
        return ""

    # Explicit calendar language
    if re.search(r"\blast calendar month\b|\bprevious calendar month\b", q):
        return "last_calendar_month"
    if re.search(r"\bthis year\b|\bytd\b|\byear to date\b", q):
        return "this_year"

    # Common relative patterns
    m = re.search(r"\blast\s+(\d+)\s+days?\b", q)
    if m:
        return f"last_{int(m.group(1))}_days"

    m = re.search(r"\blast\s+(\d+)\s+months?\b", q)
    if m:
        return f"last_{int(m.group(1))}_months"

    # Defaults when user says "last month/week" without a number
    if re.search(r"\blast\s+month\b", q):
        return "last_1_months"
    if re.search(r"\blast\s+week\b", q):
        return "last_7_days"

    return ""

def _pick_time_column(tables: list[dict]) -> str:
    """
    Pick a likely time column from the provided schema slice.
    
    Priority:
    1. Columns with temporal SQL types (timestamp, datetime, date)
    2. Columns with common timestamp names (created_at, updated_at, etc.)
    3. Columns with name patterns (*_at, *_date, *_time)
    """
    temporal_types = ("timestamp", "datetime", "date", "time", "timestamptz")
    
    # Priority 1: Columns with explicit temporal types
    for t in tables or []:
        for col in t.get("columns") or []:
            if not isinstance(col, dict):
                continue
            col_type = str(col.get("type", "")).lower()
            col_name = str(col.get("name", "")).strip()
            if col_type and any(tt in col_type for tt in temporal_types):
                return col_name
    
    # Priority 2: Common timestamp field names
    candidates = [
        "created_at",
        "updated_at",
        "timestamp",
        "event_time",
        "event_at",
        "date",
        "dt",
    ]
    for t in tables or []:
        cols = [str(c.get("name", "")).strip().lower() for c in (t.get("columns") or []) if isinstance(c, dict)]
        for c in candidates:
            if c in cols:
                return c
    
    # Priority 3: Any column with a time-like name pattern
    for t in tables or []:
        cols = [str(c.get("name", "")).strip() for c in (t.get("columns") or []) if isinstance(c, dict)]
        for c in cols:
            lc = c.lower()
            if lc.endswith("_at") or "date" in lc or "time" in lc:
                return c
    return ""

def _sql_satisfies_time_grain(sql: str, dialect: str, grain: str, time_col: str) -> bool:
    """Best-effort semantic check for requested time grain."""
    if not sql or not grain or not time_col:
        return True
    s = sql.lower()
    col = re.escape(time_col.lower())

    # If the SQL never references the time column, we can't enforce.
    if re.search(rf"\b{col}\b", s) is None:
        return True

    d = (dialect or "").lower()
    is_mysql = "mysql" in d or "mariadb" in d
    is_pg = "postgres" in d

    if grain == "day":
        return bool(re.search(rf"\bdate\s*\(\s*{col}\s*\)", s)) or (is_pg and bool(re.search(rf"date_trunc\s*\(\s*'day'\s*,\s*{col}\s*\)", s)))

    if grain == "month":
        # Reject day bucketing when month requested.
        if re.search(rf"\bdate\s*\(\s*{col}\s*\)", s):
            return False
        if is_pg and re.search(rf"date_trunc\s*\(\s*'month'\s*,\s*{col}\s*\)", s):
            return True
        if is_mysql and re.search(rf"date_format\s*\(\s*{col}\s*,\s*'%[yY]-%m", s):
            return True
        # Accept year+month extraction (must include year to avoid cross-year collisions).
        has_year = bool(re.search(rf"extract\s*\(\s*year\s+from\s+{col}\s*\)|year\s*\(\s*{col}\s*\)", s))
        has_month = bool(re.search(rf"extract\s*\(\s*month\s+from\s+{col}\s*\)|month\s*\(\s*{col}\s*\)", s))
        return has_year and has_month

    if grain == "year":
        if is_pg and re.search(rf"date_trunc\s*\(\s*'year'\s*,\s*{col}\s*\)", s):
            return True
        return bool(re.search(rf"extract\s*\(\s*year\s+from\s+{col}\s*\)|year\s*\(\s*{col}\s*\)", s))

    if grain == "week":
        # MySQL: YEARWEEK(col, mode) or DATE_SUB(DATE(col), INTERVAL WEEKDAY(col) DAY)
        if is_mysql and re.search(rf"yearweek\s*\(\s*{col}\s*,", s):
            return True
        if re.search(rf"week\s*\(\s*{col}\s*\)", s):
            # Must include year too to avoid collisions
            has_year = bool(re.search(rf"year\s*\(\s*{col}\s*\)|extract\s*\(\s*year\s+from\s+{col}\s*\)", s))
            return has_year
        if is_pg and re.search(rf"date_trunc\s*\(\s*'week'\s*,\s*{col}\s*\)", s):
            return True
        return True  # don't over-reject

    if grain == "quarter":
        has_quarter = bool(re.search(rf"quarter\s*\(\s*{col}\s*\)|extract\s*\(\s*quarter\s+from\s+{col}\s*\)", s))
        if not has_quarter:
            return False
        # Ensure year is also present for safety
        has_year = bool(re.search(rf"year\s*\(\s*{col}\s*\)|extract\s*\(\s*year\s+from\s+{col}\s*\)", s))
        return has_year

    return True

def _rewrite_time_bucket_simple(sql: str, dialect: str, grain: str, time_col: str) -> str:
    """
    Deterministic, best-effort rewrite for common time-bucketing mistakes.

    This intentionally targets the most frequent failure modes (e.g., DATE(col) used for month-wise).
    If it cannot safely rewrite, returns the original SQL.
    """
    if not sql or not grain or not time_col:
        return sql
    d = (dialect or "").lower()
    is_mysql = "mysql" in d or "mariadb" in d
    is_pg = "postgres" in d
    col_re = re.escape(time_col)

    out = sql
    if grain == "month":
        if is_mysql:
            # sqlglot transpilation sometimes yields a month-bucket pattern like:
            # DATE_ADD('0000-01-01 00:00:00', INTERVAL (TIMESTAMPDIFF(MONTH, '0000-01-01 00:00:00', col)) MONTH)
            # Normalize it to a standard, simpler MySQL month bucket.
            out = re.sub(
                rf"date_add\(\s*'0000-01-01(?: 00:00:00)?'\s*,\s*interval\s*\(\s*timestampdiff\(\s*month\s*,\s*'0000-01-01(?: 00:00:00)?'\s*,\s*{col_re}\s*\)\s*\)\s*month\s*\)",
                f"DATE_FORMAT({time_col}, '%Y-%m-01')",
                out,
                flags=re.IGNORECASE,
            )
            out = re.sub(
                rf"\bDATE\s*\(\s*{col_re}\s*\)",
                f"DATE_FORMAT({time_col}, '%Y-%m-01')",
                out,
                flags=re.IGNORECASE,
            )
            # If model grouped by month number, rewrite to a month bucket.
            out = re.sub(
                rf"\bEXTRACT\s*\(\s*MONTH\s+FROM\s+{col_re}\s*\)",
                f"DATE_FORMAT({time_col}, '%Y-%m-01')",
                out,
                flags=re.IGNORECASE,
            )
            out = re.sub(
                rf"\bMONTH\s*\(\s*{col_re}\s*\)",
                f"DATE_FORMAT({time_col}, '%Y-%m-01')",
                out,
                flags=re.IGNORECASE,
            )
        elif is_pg:
            out = re.sub(
                rf"\bDATE\s*\(\s*{col_re}\s*\)",
                f"DATE_TRUNC('month', {time_col})",
                out,
                flags=re.IGNORECASE,
            )
            out = re.sub(
                rf"\bEXTRACT\s*\(\s*MONTH\s+FROM\s+{col_re}\s*\)",
                f"DATE_TRUNC('month', {time_col})",
                out,
                flags=re.IGNORECASE,
            )
        return out

    if grain == "year":
        if is_mysql:
            out = re.sub(
                rf"\bDATE\s*\(\s*{col_re}\s*\)",
                f"YEAR({time_col})",
                out,
                flags=re.IGNORECASE,
            )
        elif is_pg:
            out = re.sub(
                rf"\bDATE\s*\(\s*{col_re}\s*\)",
                f"DATE_TRUNC('year', {time_col})",
                out,
                flags=re.IGNORECASE,
            )
        return out

    # For other grains we avoid risky rewrites for now.
    return out


def _detect_source_dialect(sql: str) -> str:
    """Heuristic detection of likely source dialect from SQL text."""
    s = (sql or "").lower()
    if "date_trunc(" in s or "ilike" in s or "::" in s:
        return "postgres"
    if "date_format(" in s or "timestampdiff(" in s or "date_sub(" in s:
        return "mysql"
    if re.search(r"\btop\s+\d+\b", s):
        return "tsql"
    # Unknown
    return ""


def _maybe_transpile_sql(sql: str, target_dialect: str) -> str:
    """
    Best-effort SQL transpilation into the target dialect using sqlglot.

    We only transpile when:
    - the SQL fails to parse for the target dialect, OR
    - we detect strong source-dialect markers that mismatch the target.
    """
    if not sql:
        return sql

    target_glot = _resolve_sqlglot_dialect(target_dialect)
    src_hint = _detect_source_dialect(sql)

    def _parse_ok(d: str) -> bool:
        try:
            _ = sqlglot.parse_one(sql, dialect=d)
            return True
        except Exception:
            return False

    should_transpile = False
    if not _parse_ok(target_glot):
        should_transpile = True
    elif src_hint and src_hint != target_glot:
        should_transpile = True

    if not should_transpile:
        return sql

    # Try parsing with the hinted dialect first, then a small fallback set.
    candidates = []
    if src_hint:
        candidates.append(src_hint)
    candidates.extend(["postgres", "mysql", "sqlite", "snowflake", "bigquery", "tsql", "oracle"])

    for read in candidates:
        try:
            ast = sqlglot.parse_one(sql, dialect=read)
            out = ast.sql(dialect=target_glot)
            if out and out.strip():
                return out.strip()
        except Exception:
            continue

    return sql


def _qualify_from_join_tables(sql: str, tables: list[dict] | None) -> str:
    """
    Qualify unqualified table identifiers in FROM/JOIN using the provided typed schema slice.

    This is a conservative rewrite:
    - Only rewrites FROM <ident> / JOIN <ident>
    - Only when <ident> matches a unique table name in tables and is not already qualified.
    """
    if not sql or not tables:
        return sql

    # Build name -> schema map (case-insensitive). Only keep if unique.
    name_to_schema: dict[str, str] = {}
    dup: set[str] = set()
    for t in tables or []:
        try:
            name = str(t.get("name") or "").strip()
            schema = str(t.get("schema") or "").strip()
        except Exception:
            continue
        if not name or not schema:
            continue
        key = name.lower()
        if key in name_to_schema and name_to_schema[key].lower() != schema.lower():
            dup.add(key)
        else:
            name_to_schema[key] = schema
    for k in dup:
        name_to_schema.pop(k, None)

    if not name_to_schema:
        return sql

    def repl(m: re.Match) -> str:
        kw = m.group(1)
        ident = m.group(2)
        if "." in ident:
            return m.group(0)
        schema = name_to_schema.get(ident.lower())
        if not schema:
            return m.group(0)
        return f"{kw} {schema}.{ident}"

    return re.sub(r"\b(from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)\b", repl, sql, flags=re.IGNORECASE)


def _rewrite_unknown_tables_to_schema(sql: str, tables: list[dict] | None) -> str:
    """
    If the model uses generic table names (e.g. 'transactions') that are not present
    in the provided schema slice, rewrite FROM/JOIN identifiers to the best matching
    table in the schema slice (token-based).

    This is generic and works across databases because it only rewrites identifiers,
    not dialect-specific functions.
    """
    if not sql or not tables:
        return sql

    # Collect allowed table names (unqualified) from schema slice.
    allowed = []
    for t in tables or []:
        try:
            name = str(t.get("name") or "").strip()
        except Exception:
            continue
        if name:
            allowed.append(name)
    if not allowed:
        return sql

    allowed_lower = [a.lower() for a in allowed]

    def toks(s: str) -> set[str]:
        s = (s or "").lower().strip()
        parts = re.split(r"[^a-z0-9_]+", s)
        out = set()
        for p in parts:
            if not p:
                continue
            for seg in p.split("_"):
                seg = seg.strip()
                if not seg:
                    continue
                if seg.endswith("ies") and len(seg) > 3:
                    out.add(seg[:-3] + "y")
                if seg.endswith("s") and len(seg) > 3:
                    out.add(seg[:-1])
                out.add(seg)
        return out

    def best_match(name: str) -> str | None:
        n = (name or "").strip()
        if not n:
            return None
        nl = n.lower()
        if nl in allowed_lower:
            return None
        nt = toks(nl)
        best_i = -1
        best_score = 0
        tie = False
        for i, a in enumerate(allowed_lower):
            at = toks(a)
            score = 0
            if a.endswith("_" + nl) or a.endswith(nl):
                score += 4
            if nl in a:
                score += 2
            score += len(nt & at)
            if score > best_score:
                best_score = score
                best_i = i
                tie = False
            elif score == best_score and score > 0:
                tie = True
        if best_i >= 0 and best_score >= 3 and not tie:
            return allowed[best_i]
        return None

    def repl(m: re.Match) -> str:
        kw = m.group(1)
        ident = m.group(2)
        if "." in ident:
            return m.group(0)
        bm = best_match(ident)
        if not bm:
            return m.group(0)
        return f"{kw} {bm}"

    return re.sub(r"\b(from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)\b", repl, sql, flags=re.IGNORECASE)


def _auto_qualify_unqualified_columns(sql: str, dialect: str, tables: list[dict] | None) -> str:
    """
    Use sqlglot AST to qualify unqualified columns when possible.

    Strategy:
    - Build alias -> table columns map from the SQL AST and provided schema slice.
    - For each unqualified column:
      - If only one table/alias contains it => qualify it.
      - If multiple contain it => choose the most common alias used by other qualified select columns
        (fallback: FROM-table alias; fallback: first candidate).
    """
    if not sql or not tables:
        return sql

    target_glot = _resolve_sqlglot_dialect(dialect)
    try:
        ast = sqlglot.parse_one(sql, dialect=target_glot)
    except Exception:
        return sql

    # Build table metadata map (name -> set(columns))
    meta_cols: dict[str, set[str]] = {}
    for t in tables or []:
        try:
            name = str(t.get("name") or "").strip()
            cols = [str(c.get("name") or "").strip() for c in (t.get("columns") or []) if isinstance(c, dict)]
        except Exception:
            continue
        if not name:
            continue
        meta_cols[name.lower()] = set([c.lower() for c in cols if c])

    # Build alias -> table name from AST
    alias_to_table: dict[str, str] = {}
    from_alias: str | None = None
    for tnode in ast.find_all(sqlglot.exp.Table):
        tname = (tnode.name or "").strip()
        alias = tnode.args.get("alias")
        alias_name = None
        try:
            alias_name = alias.name if alias is not None else None
        except Exception:
            alias_name = None
        key = alias_name or tname
        if key:
            alias_to_table[key] = tname
        if from_alias is None:
            from_alias = key

    # Alias prominence: count how often an alias appears in already-qualified SELECT columns.
    alias_votes: dict[str, int] = {}
    for c in ast.find_all(sqlglot.exp.Column):
        if c.table:
            alias_votes[c.table] = alias_votes.get(c.table, 0) + 1

    def candidates_for(col_name: str) -> list[str]:
        cand = []
        for alias, tname in alias_to_table.items():
            cols = meta_cols.get(tname.lower())
            if cols and col_name.lower() in cols:
                cand.append(alias)
        return cand

    def pick_best(cands: list[str]) -> str | None:
        if not cands:
            return None
        if len(cands) == 1:
            return cands[0]
        # Prefer most-voted alias
        cands_sorted = sorted(cands, key=lambda a: alias_votes.get(a, 0), reverse=True)
        if alias_votes.get(cands_sorted[0], 0) > 0:
            return cands_sorted[0]
        # Else prefer FROM alias if it matches
        if from_alias and from_alias in cands:
            return from_alias
        return cands_sorted[0]

    # Mutate AST: qualify unqualified columns.
    changed = False
    for c in ast.find_all(sqlglot.exp.Column):
        if c.table:
            continue
        col = c.name
        if not col or col == "*":
            continue
        cands = candidates_for(col)
        alias = pick_best(cands)
        if alias:
            c.set("table", sqlglot.exp.to_identifier(alias))
            changed = True

    if not changed:
        return sql
    try:
        return ast.sql(dialect=target_glot)
    except Exception:
        return sql


def _mysql_fix_common_time_diff(sql: str) -> str:
    """
    Deterministic fixes for common MySQL time-diff mistakes.
    - DATEDIFF(end, start) / HOUR  -> TIMESTAMPDIFF(HOUR, start, end)
    """
    if not sql:
        return sql
    out = re.sub(
        r"datediff\s*\(\s*([a-zA-Z0-9_\.]+)\s*,\s*([a-zA-Z0-9_\.]+)\s*\)\s*/\s*hour\b",
        r"TIMESTAMPDIFF(HOUR, \2, \1)",
        sql,
        flags=re.IGNORECASE,
    )
    return out


def _mysql_rewrite_month_bucket_origin(sql: str) -> str:
    """
    Normalize an ugly-but-valid month bucketing pattern into a simpler MySQL form.

    Some generators (and sometimes sqlglot transpilation) produce:
      DATE_ADD('<origin>', INTERVAL (TIMESTAMPDIFF(MONTH, '<origin>', <col>)) MONTH)

    This is mathematically equivalent to truncating <col> to the first day of its month,
    but it's hard to read and can break under strict MySQL modes when the origin uses year 0000.

    We rewrite it to:
      DATE_FORMAT(<col>, '%Y-%m-01')
    """
    if not sql:
        return sql

    # Match common origins seen in the wild (0000/1970/1900) with optional time part.
    origin = r"(?:0000|1970|1900)-01-01(?:\s+00:00:00)?"
    origin_lit = rf"(?:'{origin}'|\"{origin}\")"

    # Capture a "simple" column-ish expression for <col> (qualified identifiers, with optional quoting).
    col = r"(?P<col>[a-zA-Z0-9_`\".\$]+(?:\s*\.\s*[a-zA-Z0-9_`\".\$]+)*)"

    pat = re.compile(
        rf"""
        date_add\(
            \s*{origin_lit}\s*,
            \s*interval\s*
            \(?\s*
                timestampdiff\(
                    \s*month\s*,
                    \s*{origin_lit}\s*,
                    \s*{col}\s*
                \)
            \s*\)?\s*
            month
        \s*\)
        """,
        flags=re.IGNORECASE | re.VERBOSE,
    )

    def repl(m: re.Match) -> str:
        c = (m.group("col") or "").strip()
        if not c:
            return m.group(0)
        return f"DATE_FORMAT({c}, '%Y-%m-01')"

    return pat.sub(repl, sql)


def _apply_mysql_cleanup(sql: str, time_grain: str | None, time_column: str | None, known_qualifiers: set[str]) -> str:
    out = sql
    out = _mysql_rewrite_month_bucket_origin(out)
    out = _mysql_rewrite_postgresisms(out, time_grain, time_column)
    out = _mysql_fix_common_time_diff(out)
    out = re.sub(r"\bpublic\.", "", out, flags=re.IGNORECASE)
    out = _mysql_unqualify_builtin_functions(out, known_qualifiers)
    out = mysql_fix_only_full_group_by_order(out)
    # Backtick-quote reserved-word identifiers (e.g. `... AS year_month`) so they don't
    # raise MySQL Error 1064 at execution. Must run last, on the final SQL form.
    out = mysql_quote_reserved_identifiers(out)
    return out


def _deterministic_schema_repair(sql: str, dialect: str, tables: list[dict] | None, schema: TypedSchema | None) -> str:
    """
    Deterministic, schema-driven repair pass (no LLM) to improve executability.

    Currently targets:
    - Unqualified columns that clearly belong to exactly one table in the provided schema slice
    - Missing JOINs for tables required by those columns, using provided FK hints
    - Ambiguous columns: pick an anchor table if a unique column (e.g. mcc) implies it
    """
    if not sql or not tables or schema is None:
        return sql

    target_glot = _resolve_sqlglot_dialect(dialect)
    try:
        ast = sqlglot.parse_one(sql, dialect=target_glot)
    except Exception:
        return sql

    exp = sqlglot.exp

    # Table metadata from the schema slice
    table_cols: dict[str, set[str]] = {}
    table_schema: dict[str, str] = {}
    for t in tables or []:
        try:
            name = str(t.get("name") or "").strip()
            sch = str(t.get("schema") or "").strip()
            cols = [str(c.get("name") or "").strip().lower() for c in (t.get("columns") or []) if isinstance(c, dict)]
        except Exception:
            continue
        if not name:
            continue
        table_cols[name.lower()] = set([c for c in cols if c])
        if sch:
            table_schema[name.lower()] = sch

    # Existing tables in query (by table name)
    existing_tables: list[str] = []
    for tnode in ast.find_all(exp.Table):
        if tnode.name:
            existing_tables.append(tnode.name)
    if not existing_tables:
        return sql

    base_table = existing_tables[0]

    # Collect unqualified columns
    unqualified: list[exp.Column] = [c for c in ast.find_all(exp.Column) if not c.table and c.name and c.name != "*"]

    # Candidate tables per column
    cand_by_col: dict[str, list[str]] = {}
    for c in unqualified:
        col = c.name.lower()
        cands = [t for t, cols in table_cols.items() if col in cols]
        cand_by_col[col] = cands

    # Anchor table: the table implied by any unique column(s) among the unqualified set
    anchor_votes: dict[str, int] = {}
    for col, cands in cand_by_col.items():
        if len(cands) == 1:
            anchor_votes[cands[0]] = anchor_votes.get(cands[0], 0) + 1
    anchor = None
    if anchor_votes:
        anchor = sorted(anchor_votes.items(), key=lambda kv: kv[1], reverse=True)[0][0]

    # Decide chosen table for each column
    chosen_for_col: dict[str, str] = {}
    for col, cands in cand_by_col.items():
        if not cands:
            continue
        if len(cands) == 1:
            chosen_for_col[col] = cands[0]
            continue
        if anchor and anchor in cands:
            chosen_for_col[col] = anchor
            continue
        # Prefer base table if it has the column
        if base_table.lower() in cands:
            chosen_for_col[col] = base_table.lower()
            continue
        # Else take first
        chosen_for_col[col] = cands[0]

    # Ensure required tables are present via joins (1-hop using FK hints)
    required_tables = set(chosen_for_col.values())
    present = set([t.lower() for t in existing_tables])
    needed = [t for t in required_tables if t and t not in present]

    if needed:
        select_node = ast if isinstance(ast, exp.Select) else ast.find(exp.Select)
        root_with = ast if isinstance(ast, exp.With) else None
        if select_node is not None:
            for need in needed:
                # Find a FK edge from any present table to needed.
                edge = None
                for fk in (schema.foreign_keys or []):
                    ft = (fk.from_table or "").lower()
                    tt = (fk.to_table or "").lower()
                    if ft in present and tt == need:
                        edge = ("forward", fk)
                        break
                    if tt in present and ft == need:
                        edge = ("reverse", fk)
                        break
                if not edge:
                    continue
                dirn, fk = edge
                left_table = fk.from_table if dirn == "forward" else fk.to_table
                left_col = fk.from_column if dirn == "forward" else fk.to_column
                right_table = fk.to_table if dirn == "forward" else fk.from_table
                right_col = fk.to_column if dirn == "forward" else fk.from_column

                # Build JOIN table expression (include schema/db when available)
                rt = right_table.lower()
                db = table_schema.get(rt)
                join_tbl = exp.Table(this=exp.to_identifier(right_table))
                if db:
                    join_tbl.set("db", exp.to_identifier(db))

                on_expr = exp.EQ(
                    this=exp.column(left_col, left_table),
                    expression=exp.column(right_col, right_table),
                )
                # sqlglot returns a new Select when adding joins.
                select_node = select_node.join(join_tbl, join_type="INNER", on=on_expr)
                if isinstance(ast, exp.Select):
                    ast = select_node
                elif root_with is not None:
                    root_with.set("this", select_node)
                present.add(rt)

    # Qualify the unqualified columns now that tables should be present.
    for c in unqualified:
        col = c.name.lower()
        t = chosen_for_col.get(col)
        if not t:
            continue
        c.set("table", exp.to_identifier(t))

    try:
        return ast.sql(dialect=target_glot)
    except Exception:
        return sql


def _mysql_rewrite_postgresisms(sql: str, grain: str | None, time_col: str | None) -> str:
    """
    Best-effort rewrite of common Postgres-isms into MySQL-compatible syntax.

    This is intentionally conservative and only targets the patterns we've seen in live runs:
    - DATE_TRUNC('month'|'day'|'year', col) -> DATE_FORMAT/DATE/YEAR
    - CURRENT_DATE - INTERVAL '30' DAY -> DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
    - INTERVAL '30' DAY -> INTERVAL 30 DAY
    """
    if not sql:
        return sql

    out = sql

    # Normalize INTERVAL quoting (MySQL does not accept quoted numbers).
    out = re.sub(
        r"\bINTERVAL\s*'(\d+)'\s*(DAY|HOUR|MINUTE|SECOND|WEEK|MONTH|YEAR)\b",
        r"INTERVAL \1 \2",
        out,
        flags=re.IGNORECASE,
    )

    # Normalize Postgres-style interval literals like INTERVAL '30 days' → INTERVAL 30 DAY
    out = re.sub(
        r"\bINTERVAL\s*'(\d+)\s*(day|days|hour|hours|minute|minutes|second|seconds|week|weeks|month|months|year|years)'\b",
        lambda m: f"INTERVAL {m.group(1)} {m.group(2).upper().rstrip('S')}",
        out,
        flags=re.IGNORECASE,
    )

    # Rewrite CURRENT_DATE - INTERVAL ... to DATE_SUB(CURRENT_DATE, INTERVAL ...)
    out = re.sub(
        r"\bCURRENT_DATE\s*-\s*INTERVAL\s*(\d+)\s*(DAY|HOUR|MINUTE|SECOND|WEEK|MONTH|YEAR)\b",
        r"DATE_SUB(CURRENT_DATE, INTERVAL \1 \2)",
        out,
        flags=re.IGNORECASE,
    )
    out = re.sub(
        r"\bCURRENT_DATE\s*-\s*INTERVAL\s*'(\d+)\s*(day|days|hour|hours|minute|minutes|second|seconds|week|weeks|month|months|year|years)'\b",
        lambda m: f"DATE_SUB(CURRENT_DATE, INTERVAL {m.group(1)} {m.group(2).upper().rstrip('S')})",
        out,
        flags=re.IGNORECASE,
    )

    # Rewrite DATE_TRUNC() when it appears (fallback models).
    # Prefer the inferred grain/time_col when available, otherwise rewrite the specific calls.
    g = (grain or "").strip().lower()
    col = (time_col or "").strip()

    if g == "month" and col:
        col_re = re.escape(col)
        out = re.sub(
            rf"date_trunc\s*\(\s*'month'\s*,\s*{col_re}\s*\)",
            f"DATE_FORMAT({col}, '%Y-%m-01')",
            out,
            flags=re.IGNORECASE,
        )
    if g == "day" and col:
        col_re = re.escape(col)
        out = re.sub(
            rf"date_trunc\s*\(\s*'day'\s*,\s*{col_re}\s*\)",
            f"DATE({col})",
            out,
            flags=re.IGNORECASE,
        )
    if g == "year" and col:
        col_re = re.escape(col)
        out = re.sub(
            rf"date_trunc\s*\(\s*'year'\s*,\s*{col_re}\s*\)",
            f"YEAR({col})",
            out,
            flags=re.IGNORECASE,
        )

    # Generic fallback: rewrite DATE_TRUNC('month', <expr>) even if time_col isn't matched.
    out = re.sub(
        r"date_trunc\s*\(\s*'month'\s*,\s*([^)]+)\)",
        r"DATE_FORMAT(\1, '%Y-%m-01')",
        out,
        flags=re.IGNORECASE,
    )
    out = re.sub(
        r"date_trunc\s*\(\s*'day'\s*,\s*([^)]+)\)",
        r"DATE(\1)",
        out,
        flags=re.IGNORECASE,
    )
    out = re.sub(
        r"date_trunc\s*\(\s*'year'\s*,\s*([^)]+)\)",
        r"YEAR(\1)",
        out,
        flags=re.IGNORECASE,
    )

    # Remove ::date casts
    out = re.sub(r"::\s*date\b", "", out, flags=re.IGNORECASE)

    return out

def _extract_json_text(raw: str) -> str:
    """Best-effort extraction of a JSON object from model output."""
    if raw is None:
        return "{}"
    text = str(raw).strip()
    # Prefer fenced JSON blocks
    m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text, flags=re.IGNORECASE)
    if m:
        text = m.group(1).strip()
    # Fall back to first {...} span
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        return text[start : end + 1].strip()
    return text

def _extract_sql_text(raw: str) -> str:
    """Best-effort extraction of a SQL query from model output."""
    if raw is None:
        return ""
    text = str(raw).strip()
    # Prefer fenced SQL blocks
    m = re.search(r"```(?:sql)?\s*([\s\S]*?)\s*```", text, flags=re.IGNORECASE)
    if m:
        text = m.group(1).strip()
    # If the model returned prose + SQL, slice from the first SELECT/WITH.
    m2 = re.search(r"\b(SELECT|WITH)\b[\s\S]*", text, flags=re.IGNORECASE)
    if m2:
        text = m2.group(0).strip()
    return text

def _mysql_unqualify_builtin_functions(sql: str, prefixes: set[str]) -> str:
    # Backwards-compatible wrapper for older call sites.
    return mysql_unqualify_builtin_functions(sql, prefixes)

class PromptRequest(BaseModel):
    prompt_name: str
    variables: dict
    model_override: str | None = None
    # Optional attribution for cost tracking + quota enforcement. Callers
    # (api-gateway, orchestrator, temporal-adapter) should forward these
    # when available so cost is attributed to the right tenant/pipeline.
    user_id: str | None = None
    pipeline_id: str | None = None

@app.get("/health")
async def health():
    return {"status": "ok", "service": "llm-gateway"}


# /version — canonical "what code is actually running" answer for the
# deployment-drift check. GIT_COMMIT and BUILD_TIME are injected by the
# Dockerfile build args; locally they fall back to "dev"/"unknown".
_LLM_STARTED_AT = datetime.utcnow().isoformat() + "Z"

@app.get("/version")
async def version():
    return {
        "service": "llm-service",
        "commit": os.getenv("GIT_COMMIT", "dev"),
        "built_at": os.getenv("BUILD_TIME", "unknown"),
        "started_at": _LLM_STARTED_AT,
    }


# ---------------------------------------------------------------------------
# Diagnose: LLM-powered triage for misbehaving pipelines.
#
# This is intentionally read-only and on-demand. The api-gateway gathers
# structured evidence (runtime view, dep health, recent events) and POSTs it
# here; we ask the model for a single root-cause hypothesis and a suggested
# next action. The model never decides on remediation — it explains.
# ---------------------------------------------------------------------------
class DiagnoseRequest(BaseModel):
    pipeline_id: str
    evidence: dict


class DiagnoseResponse(BaseModel):
    summary: str
    root_cause: str
    suggested_action: str
    confidence: str
    evidence_pointers: list[str] = []
    raw: str | None = None  # populated when JSON parse fails so caller can show the model's text


@app.post("/v1/diagnose/pipeline", response_model=DiagnoseResponse)
async def diagnose_pipeline(request: DiagnoseRequest):
    if not request.pipeline_id:
        raise HTTPException(status_code=400, detail="pipeline_id required")
    if not isinstance(request.evidence, dict) or not request.evidence:
        raise HTTPException(status_code=400, detail="evidence must be a non-empty object")

    # Mock mode: return a deterministic stub so the UI is exercisable in dev
    # without burning API tokens.
    if USE_MOCK or default_client is None:
        return DiagnoseResponse(
            summary="[mock] pipeline appears stalled",
            root_cause="[mock] LLM is in mock mode — set USE_MOCK=false and OPENAI_API_KEY to get real diagnoses.",
            suggested_action="Enable a real LLM provider in llm-service to use diagnostic mode.",
            confidence="low",
            evidence_pointers=list(request.evidence.keys())[:3],
        )

    # Defense-in-depth (H6): scrub only free-text error/log fields in the evidence
    # before rendering it into the LLM prompt. Metadata fields (schema, table/column
    # names, counts) are left intact so diagnosis quality is preserved; only the
    # error strings — the sole realistic row-value leak vector — are scrubbed. The
    # Go producer already scrubs; this is a backstop for a new/forgotten field.
    from src.utils.masking import scrub_error_for_llm as _scrub_ll

    _FREETEXT_KEYS = {
        "error", "last_error", "error_message", "stderr", "stdout", "logs", "log",
        "message", "detail", "exception", "traceback", "output", "reason",
    }

    def _scrub_evidence(v):
        if isinstance(v, dict):
            return {
                k: (_scrub_ll(x) if (k in _FREETEXT_KEYS and isinstance(x, str)) else _scrub_evidence(x))
                for k, x in v.items()
            }
        if isinstance(v, list):
            return [_scrub_evidence(x) for x in v]
        return v

    try:
        evidence_json = json.dumps(_scrub_evidence(request.evidence), indent=2, default=str)
    except (TypeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"evidence not JSON-serializable: {e}")

    try:
        messages = registry.render_messages(
            "diagnose/pipeline_failure",
            {"pipeline_id": request.pipeline_id, "evidence_json": evidence_json},
        )
        config = registry.get_config("diagnose/pipeline_failure")
        response = await default_client.chat.completions.create(
            model=config["model"],
            messages=messages,
            temperature=config["parameters"].get("temperature", 0.2),
            max_tokens=config["parameters"].get("max_tokens", 600),
        )
        text = (response.choices[0].message.content or "").strip()
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=f"diagnose prompt missing: {e}")
    except Exception as e:
        logger.exception("diagnose: LLM call failed")
        raise HTTPException(status_code=502, detail=f"LLM call failed: {e}")

    # Strip markdown code fences if the model wrapped its JSON.
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].strip()

    try:
        parsed = json.loads(text)
        return DiagnoseResponse(
            summary=str(parsed.get("summary", "")).strip(),
            root_cause=str(parsed.get("root_cause", "")).strip(),
            suggested_action=str(parsed.get("suggested_action", "")).strip(),
            confidence=str(parsed.get("confidence", "low")).strip().lower(),
            evidence_pointers=[str(p) for p in (parsed.get("evidence_pointers") or [])][:5],
        )
    except json.JSONDecodeError:
        # Model didn't comply with JSON-only — return raw text so UI can still
        # display something useful instead of a 500.
        return DiagnoseResponse(
            summary="(model returned unstructured output)",
            root_cause=text[:1500],
            suggested_action="Review the raw output below; consider re-running.",
            confidence="low",
            raw=text,
        )

@app.post("/v1/completion")
async def completion(request: PromptRequest):
    try:
        # Two-tier approach for chat: check local knowledge first, then OpenAI
        if request.prompt_name == "chat/assistant" and not USE_MOCK:
            user_msg = request.variables.get("user_message", "")
            
            # Tier 1: Try local knowledge base
            local_response = chat_knowledge.get_response(user_msg)
            if local_response:
                logger.info(f"Chat: Using local knowledge for: {user_msg[:50]}...")
                return {
                    "content": local_response,
                    "model": "local-knowledge",
                    "usage": {
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0
                    }
                }
            else:
                # Tier 2: Escalate to OpenAI for complex questions
                logger.info(f"Chat: Escalating to OpenAI for: {user_msg[:50]}...")
        
        # 2. Call LLM or use mock
        if USE_MOCK or default_client is None:
            # Mock response for testing - bypass prompt registry
            if request.prompt_name == "intent_classification" or request.prompt_name == "chat/intent_classification":
                # Parse the user message to determine intent
                user_msg = request.variables.get("user_message", "").lower()
                
                # Simple keyword-based intent classification for mock mode
                # This mirrors what the real LLM should return
                intent_result = mock_intent_classification(user_msg)
                result = intent_result
            elif request.prompt_name == "slot_filling" or request.prompt_name == "chat/slot_filling":
                # Mock slot-filling for multi-turn chat
                user_msg = request.variables.get("user_message", "")
                state = request.variables.get("state", "")
                pending_source = request.variables.get("pending_source", "")
                pending_destination = request.variables.get("pending_destination", "")
                last_response = request.variables.get("last_response", "")
                result = mock_slot_filling(
                    user_msg=user_msg,
                    state=state,
                    pending_source=pending_source,
                    pending_destination=pending_destination,
                    last_response=last_response,
                )
            elif request.prompt_name == "chat/help_response":
                user_msg = request.variables.get("user_message", "")
                result = mock_help_response(user_msg)
            elif request.prompt_name == "planner/dynamic_planning":
                # Extract table name from user request if possible
                user_request = request.variables.get("user_request", "")
                table_name = "products"  # Default
                for word in ["products", "customers", "orders", "users"]:
                    if word in user_request.lower():
                        table_name = word
                        break
                
                # Build mock plan based on available connections
                available_conns = request.variables.get("available_connections", "[]")
                import json as _json
                try:
                    conns = _json.loads(available_conns)
                except:
                    conns = []
                
                # Find mysql and aws-s3 connections (canonical tool id: aws-s3; connection type: aws-s3)
                mysql_conn = next((c for c in conns if c.get("type") == "mysql"), None)
                s3_conn = next(
                    (
                        c
                        for c in conns
                        if (c.get("type") or "").lower() in ("aws-s3", "aws_s3", "s3")
                    ),
                    None,
                )
                
                result = _json.dumps({
                    "pipeline_id": "mock-plan-001",
                    "steps": [
                        {
                            "id": "step_1",
                            "tool": "mysql",
                            "method": "export",
                            "connection_id": mysql_conn["id"] if mysql_conn else "mock-mysql-conn",
                            "description": f"Export {table_name} from MySQL",
                            "params": {"table": table_name}
                        },
                        {
                            "id": "step_2", 
                            "tool": "aws-s3",
                            "method": "import_data",
                            "connection_id": s3_conn["id"] if s3_conn else "mock-s3-conn",
                            "description": "Upload exported data to S3",
                            "params": {"source": "$step_1.output", "bucket": "data-exports"}
                        }
                    ]
                })
            elif request.prompt_name == "chat/decision_agent":
                # Mock decision agent - parse prompt for context
                prompt = request.variables.get("prompt", "")
                decision_result = mock_decision_agent(prompt)
                result = decision_result
            elif request.prompt_name == "chat/schema_healer":
                # Mock schema healer - analyze schema changes
                prompt = request.variables.get("prompt", "")
                healer_result = mock_schema_healer(prompt)
                result = healer_result
            elif request.prompt_name == "chat/assistant":
                # Mock chat assistant response
                user_msg = request.variables.get("user_message", "")
                result = f"Hello! I'm your RSYNC AI assistant. I can help you with:\n\n" \
                        f"• Understanding pipeline status and execution\n" \
                        f"• Debugging pipeline failures\n" \
                        f"• Analyzing connections and data flows\n" \
                        f"• Optimizing performance\n\n" \
                        f"You asked: \"{user_msg}\"\n\n" \
                        f"How can I assist you today?"
            else:
                result = '{"status": "success", "message": "Mock LLM response for ' + request.prompt_name + '"}'
            
            return {
                "content": result,
                "model": "gpt-4-mock",
                "usage": {
                    "prompt_tokens": 100,
                    "completion_tokens": 50,
                    "total_tokens": 150
                }
            }
        else:
            # 1. Render Messages (only for real OpenAI calls)
            messages = registry.render_messages(request.prompt_name, request.variables)
            config = registry.get_config(request.prompt_name)
            
            # Resolution order: per-request override → LLM_MODEL env var → prompt YAML default
            model = request.model_override or DEFAULT_LLM_MODEL or config["model"]
            
            response = await default_client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=config["parameters"].get("temperature", 0.7),
                max_tokens=config["parameters"].get("max_tokens", 1024),
            )
            
            # 3. Return Result
            result = response.choices[0].message.content
            usage = response.usage

            # Cost tracking + atomic per-user quota charge (best-effort, non-blocking)
            try:
                from src.utils.llm_cost import record_usage
                cost_cents = record_usage(
                    user_id=request.user_id,
                    pipeline_id=request.pipeline_id,
                    prompt_name=request.prompt_name,
                    provider=DEFAULT_LLM_PROVIDER,
                    model=model,
                    prompt_tokens=usage.prompt_tokens,
                    completion_tokens=usage.completion_tokens,
                )
            except Exception as _cost_exc:
                logger.warning("LLM cost tracking failed (non-fatal): %s", _cost_exc)
                cost_cents = 0

            return {
                "content": result,
                "model": model,
                "usage": {
                    "prompt_tokens": usage.prompt_tokens,
                    "completion_tokens": usage.completion_tokens,
                    "total_tokens": usage.total_tokens,
                    "cost_usd_cents": cost_cents,
                }
            }

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Prompt '{request.prompt_name}' not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==============================================================================
# SQL GENERATION (NL → SQL) ENDPOINT
# ==============================================================================

from pydantic import AliasChoices, ConfigDict, Field


class ColumnSchemaInput(BaseModel):
    """Column schema for typed schema input."""
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    name: str
    type: str = "unknown"
    is_primary_key: bool = False
    is_nullable: bool = True


class TableSchemaInput(BaseModel):
    """Table schema for typed schema input."""
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    name: str
    # Accept both `schema` (from API gateway) and `schema_name` (plan terminology).
    schema: str = Field(default="public", validation_alias=AliasChoices("schema", "schema_name"))
    columns: list[ColumnSchemaInput] = Field(default_factory=list)
    primary_key: list[str] = Field(default_factory=list)
    row_count: int | None = None


class ForeignKeyInput(BaseModel):
    """Foreign key relationship for typed schema input."""
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    confidence: float = 1.0


class SQLGenerateRequest(BaseModel):
    """Request to generate SQL from natural language"""
    question: str
    dialect: str = "postgresql"
    # Legacy string-based schema (backward compatible)
    schema: str | None = None  # Schema context as string: "orders(id, created_at, ...)"
    tables: str | None = None  # Comma-separated table names to focus on
    # New typed schema (preferred when available)
    tables_typed: list[TableSchemaInput] | None = None  # Structured tables with column types
    foreign_keys: list[ForeignKeyInput] | None = None  # FK relationships for join guidance
    # Generation parameters
    max_tokens: int = 512
    temperature: float = 0.1


class SQLGenerateResponse(BaseModel):
    """Response with generated SQL"""
    sql: str
    explanation: str | None = None
    confidence: float = 0.0
    tables: list[str] | None = None
    warnings: list[str] | None = None


def _typed_schema_from_request(req: SQLGenerateRequest) -> TypedSchema | None:
    """Convert request typed schema inputs into our internal TypedSchema model."""
    if not req.tables_typed:
        return None

    def _norm_table_ident(raw: str) -> str:
        # Accept qualified names like "db.schema.table" or "schema.table" or "`schema`.`table`"
        s = (raw or "").strip()
        if not s:
            return s
        s = s.strip().strip("`").strip('"').strip("'")
        parts = [p.strip().strip("`").strip('"').strip("'") for p in s.split(".") if p.strip()]
        return parts[-1] if parts else s

    tables: list[TableSchema] = []
    for t in req.tables_typed:
        cols = [
            ColumnSchema(
                name=c.name,
                type=c.type or "unknown",
                is_primary_key=bool(c.is_primary_key),
                is_nullable=bool(c.is_nullable),
            )
            for c in (t.columns or [])
        ]
        tables.append(
            TableSchema(
                name=t.name,
                schema_name=t.schema or "public",
                columns=cols,
                primary_key=list(t.primary_key or []),
                row_count=t.row_count,
            )
        )

    fks: list[ForeignKey] = []
    for fk in req.foreign_keys or []:
        fks.append(
            ForeignKey(
                from_table=_norm_table_ident(fk.from_table),
                from_column=fk.from_column,
                to_table=_norm_table_ident(fk.to_table),
                to_column=fk.to_column,
                confidence=float(getattr(fk, "confidence", 1.0) or 1.0),
            )
        )

    return TypedSchema(tables=tables, foreign_keys=fks)


async def _generate_query_spec(question: str, dialect: str, tables: list[dict], relationships: list[str] | None, schema: TypedSchema | None) -> QuerySpec:
    """
    Generate a QuerySpec IR using the explorer offline LLM.

    This is used by the new IR-based path (QuerySpec → compiler → validator).
    """
    if USE_MOCK:
        # Minimal spec for tests; compilation/validation steps will refine later.
        spec = QuerySpec(from_table=tables[0]["name"] if tables else "unknown", intent_description=question)
        return normalize_query_spec(spec, schema)

    if explorer_client is None:
        raise HTTPException(status_code=503, detail="Explorer LLM client unavailable (offline-only)")

    prompt_name = "explorer/query_spec"
    variables = {
        "question": question,
        "dialect": dialect,
        "tables": tables,
        "relationships": relationships,
    }
    messages = registry.render_messages(prompt_name, variables)
    cfg = registry.get_config(prompt_name)

    max_parse_attempts = int(os.getenv("TEXT2SQL_QUERY_SPEC_PARSE_ATTEMPTS", "2") or "2")
    last_err: Exception | None = None
    for attempt in range(max_parse_attempts):
        temperature = cfg["parameters"].get("temperature", 0.1) if attempt == 0 else 0.0
        response = await explorer_client.chat.completions.create(
            model=EXPLORER_QUERY_SPEC_MODEL,
            messages=messages,
            temperature=temperature,
            max_tokens=cfg["parameters"].get("max_tokens", 1024),
        )
        content = (response.choices[0].message.content or "").strip()
        if not content:
            # Fallback to completions for some Ollama models
            prompt_text = "\n\n".join([m.get("content", "") for m in (messages or []) if m.get("content")])
            resp2 = await explorer_client.completions.create(
                model=EXPLORER_QUERY_SPEC_MODEL,
                prompt=prompt_text,
                temperature=temperature,
                max_tokens=cfg["parameters"].get("max_tokens", 1024),
            )
            content = (getattr(resp2.choices[0], "text", "") or "").strip()

        try:
            raw = json.loads(_extract_json_text(content))
            raw = coerce_query_spec_payload(raw)
            spec = QuerySpec.model_validate(raw)
            return normalize_query_spec(spec, schema)
        except Exception as e:
            last_err = e
            logger.warning(f"[Text2SQL] Failed to parse QuerySpec JSON (attempt {attempt+1}/{max_parse_attempts}): {e}")
            continue

    raise HTTPException(status_code=500, detail=f"Failed to parse QuerySpec JSON: {last_err}")

    # unreachable


async def _repair_query_spec(
    *,
    question: str,
    dialect: str,
    tables: list[dict],
    relationships: list[str] | None,
    previous_spec: QuerySpec,
    errors: list[str],
    fix_hints: list[str],
    schema: TypedSchema | None,
) -> QuerySpec:
    """Ask the model to repair a previous QuerySpec based on validation feedback."""
    if USE_MOCK:
        # In mock mode, just return the previous spec.
        return normalize_query_spec(previous_spec, schema)

    if explorer_client is None:
        raise HTTPException(status_code=503, detail="Explorer LLM client unavailable (offline-only)")

    prompt_name = "explorer/query_spec_repair"
    variables = {
        "question": question,
        "dialect": dialect,
        "tables": tables,
        "relationships": relationships,
        "previous_spec": json.dumps(previous_spec.model_dump(), ensure_ascii=False),
        "errors": errors,
        "fix_hints": fix_hints,
    }
    messages = registry.render_messages(prompt_name, variables)
    cfg = registry.get_config(prompt_name)

    response = await explorer_client.chat.completions.create(
        model=EXPLORER_QUERY_SPEC_MODEL,
        messages=messages,
        temperature=cfg["parameters"].get("temperature", 0.0),
        max_tokens=cfg["parameters"].get("max_tokens", 1024),
    )
    content = (response.choices[0].message.content or "").strip()
    if not content:
        prompt_text = "\n\n".join([m.get("content", "") for m in (messages or []) if m.get("content")])
        resp2 = await explorer_client.completions.create(
            model=EXPLORER_QUERY_SPEC_MODEL,
            prompt=prompt_text,
            temperature=cfg["parameters"].get("temperature", 0.0),
            max_tokens=cfg["parameters"].get("max_tokens", 1024),
        )
        content = (getattr(resp2.choices[0], "text", "") or "").strip()

    raw = coerce_query_spec_payload(json.loads(_extract_json_text(content)))
    spec = QuerySpec.model_validate(raw)
    return normalize_query_spec(spec, schema)


@app.post("/api/v1/sql/generate", response_model=SQLGenerateResponse)
async def generate_sql(request: SQLGenerateRequest):
    """
    Generate SQL from natural language using LLM.
    
    This endpoint converts natural language questions into SQL queries.
    It uses schema context when provided to improve accuracy.
    
    Example:
        POST /api/v1/sql/generate
        {
            "question": "Show me total revenue by month for 2024",
            "dialect": "postgresql",
            "schema": "orders(id, customer_id, total, created_at), customers(id, name, email)"
        }
        
        Response:
        {
            "sql": "SELECT DATE_TRUNC('month', created_at) AS month, SUM(total) AS revenue FROM orders WHERE EXTRACT(YEAR FROM created_at) = 2024 GROUP BY 1 ORDER BY 1",
            "explanation": "Aggregates orders by month for 2024",
            "confidence": 0.85
        }
    """
    try:
        logger.info(
            f"📊 SQL generation request: {request.question[:100]}... "
            f"(dialect={request.dialect}, tables_typed={len(request.tables_typed or [])}, foreign_keys={len(request.foreign_keys or [])})"
        )
        
        # In mock mode we don't call any LLM provider.
        if USE_MOCK:
            sql = _mock_sql_generation(request.question, request.dialect, request.schema, request.tables)
            return SQLGenerateResponse(
                sql=sql,
                explanation="Mock mode (offline)",
                confidence=0.1,
                tables=[],
                warnings=["Mock mode enabled"],
            )

        if sql_client is None:
            raise HTTPException(status_code=503, detail="SQL LLM client unavailable")

        profile = get_engine_profile(request.dialect)

        # Use a provider-appropriate prompt template.
        # Offline models are much more reliable when asked for plain SQL (not JSON).
        prompt_name = "explorer/sql_generate" if sql_provider_resolved == "openai" else "explorer/sql_generate_offline"

        # Build parsed_tables from typed schema (preferred) or legacy string schema.
        # The typed schema includes column types, enabling proper validation and time-column detection.
        parsed_tables: list[dict] = []
        
        if request.tables_typed:
            # Use typed schema (new format with column types)
            for t in request.tables_typed:
                parsed_tables.append({
                    "schema": t.schema or "public",
                    "name": t.name,
                    "alias": t.name,
                    "columns": [
                        {
                            "name": c.name,
                            "type": c.type or "unknown",
                            "is_primary_key": c.is_primary_key,
                            "is_nullable": c.is_nullable,
                        }
                        for c in t.columns
                    ],
                    "primary_key": t.primary_key or [],
                    "row_count": t.row_count,
                })
        elif request.schema:
            # Fallback to legacy string schema: "t(col1, col2); t2(col3)"
            for m in re.finditer(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\(([^)]*)\)", request.schema):
                tname = m.group(1).strip()
                cols_raw = m.group(2)
                cols = [c.strip() for c in cols_raw.split(",") if c.strip()]
                parsed_tables.append(
                    {
                        "schema": "public",
                        "name": tname,
                        "alias": tname,
                        "columns": [{"name": c, "type": "unknown"} for c in cols],
                    }
                )

        def _parse_focus_table(raw: str) -> tuple[str, str]:
            """
            Parse focus table identifiers that may be:
              - table
              - schema.table
              - db.schema.table
            We treat the last segment as table name and the remaining prefix as schema/db prefix.
            """
            s = (raw or "").strip()
            if not s:
                return ("", "")
            # strip common quoting and whitespace
            s = s.strip().strip("`").strip('"').strip("'")
            parts = [p.strip().strip("`").strip('"').strip("'") for p in s.split(".") if p.strip()]
            if not parts:
                return ("", "")
            if len(parts) == 1:
                return ("", parts[0])
            return (".".join(parts[:-1]), parts[-1])

        focus_tables = [t.strip() for t in (request.tables or "").split(",") if t.strip()]
        if focus_tables:
            _before_focus = list(parsed_tables)
            focus_parsed = [_parse_focus_table(t) for t in focus_tables]
            focus_names = {name.lower() for (_schema, name) in focus_parsed if name}
            focus_qualified = {
                f"{schema.lower()}.{name.lower()}"
                for (schema, name) in focus_parsed
                if schema and name
            }

            def _matches_focus(tbl: dict) -> bool:
                name = str(tbl.get("name", "") or "").strip()
                schema = str(tbl.get("schema", "") or "").strip()
                if not name:
                    return False
                n = name.lower()
                if n in focus_names:
                    return True
                if schema:
                    k = f"{schema.lower()}.{n}"
                    if k in focus_qualified:
                        return True
                return False

            parsed_tables = [t for t in parsed_tables if isinstance(t, dict) and _matches_focus(t)]
            # Safety: never let focus filtering erase the schema context entirely; it disables the
            # QuerySpec path and makes models much less reliable.
            if not parsed_tables and _before_focus:
                parsed_tables = _before_focus

        time_grain = _infer_time_grain(request.question)
        time_column = _pick_time_column(parsed_tables)

        # Build join candidates from provided FK relationships (when available).
        join_plan = None
        relationships = None
        if request.foreign_keys:
            table_set = {str(t.get("name", "")).strip() for t in (parsed_tables or []) if isinstance(t, dict)}
            rels: list[str] = []
            jp: list[dict] = []
            for fk in request.foreign_keys:
                try:
                    ft = (fk.from_table or "").strip()
                    tt = (fk.to_table or "").strip()
                    fc = (fk.from_column or "").strip()
                    tc = (fk.to_column or "").strip()
                    if not ft or not tt or not fc or not tc:
                        continue
                    if ft in table_set and tt in table_set:
                        cond = f"{ft}.{fc} = {tt}.{tc}"
                        rels.append(cond)
                        jp.append({"type": "INNER", "right": tt, "condition": cond})
                except Exception:
                    continue
            if rels:
                relationships = rels[:10]
                join_plan = jp[:10]

        variables = {
            "question": request.question,
            "dialect": request.dialect,
            "tables": parsed_tables,
            "join_plan": join_plan,
            "column_mapping": None,
            "previous_sql": None,
            "time_grain": time_grain,
            "time_column": time_column,
            "relationships": relationships,
        }
        # Known identifiers that might incorrectly qualify built-in functions in MySQL output.
        known_qualifiers: set[str] = set()
        for t in parsed_tables or []:
            if isinstance(t, dict):
                if t.get("name"):
                    known_qualifiers.add(str(t.get("name")))
                if t.get("schema"):
                    known_qualifiers.add(str(t.get("schema")))

        # IR-based Text2SQL path (QuerySpec → sqlglot compiler → AST validator).
        # This provides correctness guarantees across dialects (SELECT-only, schema-aware).
        # Default to the SQL-model path (sqlcoder) for correctness on complex queries.
        # The QuerySpec path remains available behind TEXT2SQL_USE_QUERY_SPEC=true.
        use_query_spec = _env_bool("TEXT2SQL_USE_QUERY_SPEC", False)
        allow_legacy_fallback = _env_bool("TEXT2SQL_ALLOW_LEGACY_FALLBACK", True)
        typed_schema = _typed_schema_from_request(request)
        if use_query_spec and parsed_tables:
            try:
                max_attempts = int(os.getenv("TEXT2SQL_REPAIR_MAX_ATTEMPTS", "3") or "3")
                best = None

                spec = await _generate_query_spec(
                    question=request.question,
                    dialect=request.dialect,
                    tables=parsed_tables,
                    relationships=relationships,
                    schema=typed_schema,
                )

                for attempt in range(max_attempts):
                    # If the model omitted time grain/column, fall back to the gateway's inference.
                    if not getattr(spec, "time_grain", None) and time_grain:
                        try:
                            from src.text2sql.query_spec import TimeGrain as _TG
                            spec.time_grain = _TG(str(time_grain).lower())
                        except (ImportError, ValueError, TypeError) as e:
                            logger.debug("Failed to coerce time_grain into QuerySpec TimeGrain", exc_info=True)
                    if not getattr(spec, "time_column", None) and time_column:
                        spec.time_column = time_column
                    # If the model omitted time range, infer a relative time range token.
                    if not getattr(spec, "time_range", None):
                        rel = _infer_time_range_relative(request.question)
                        if rel:
                            try:
                                from src.text2sql.query_spec import TimeRange as _TR
                                spec.time_range = _TR(relative=rel)
                            except (ImportError, TypeError) as e:
                                logger.debug("Failed to coerce relative time_range into QuerySpec TimeRange", exc_info=True)

                    compiled_sql = compile_query_spec(spec, request.dialect, typed_schema).strip()
                    if not compiled_sql:
                        raise HTTPException(status_code=500, detail="IR compilation produced empty SQL")

                    validation = validate_sql(
                        compiled_sql,
                        request.dialect,
                        schema=typed_schema,
                        spec=spec,
                        require_limit=True,
                        max_limit=10000,
                    )

                    # Track best attempt by fewest errors.
                    score = len(validation.errors)
                    if best is None or score < best["score"]:
                        best = {"score": score, "sql": compiled_sql, "spec": spec, "validation": validation}

                    # Success: no schema/AST errors and no HITL requirement.
                    if not validation.errors and not getattr(spec, "requires_hitl", False):
                        best = {"score": 0, "sql": compiled_sql, "spec": spec, "validation": validation}
                        break

                    # If we have errors, try repair.
                    if validation.errors and attempt < max_attempts-1:
                        err_msgs = [e.message for e in validation.errors]
                        fix_hints = [e.fix_hint for e in validation.errors if e.fix_hint] + [w.fix_hint for w in validation.warnings if w.fix_hint]
                        spec = await _repair_query_spec(
                            question=request.question,
                            dialect=request.dialect,
                            tables=parsed_tables,
                            relationships=relationships,
                            previous_spec=spec,
                            errors=err_msgs,
                            fix_hints=[h for h in fix_hints if h],
                            schema=typed_schema,
                        )

                assert best is not None
                validation = best["validation"]
                spec = best["spec"]
                compiled_sql = best["sql"]

                warn_msgs: list[str] = []
                if validation.warnings:
                    warn_msgs.extend([w.message for w in validation.warnings])
                if validation.errors:
                    warn_msgs.extend([e.message for e in validation.errors])

                conf = 0.85
                if getattr(spec, "requires_hitl", False):
                    conf = min(conf, 0.6)
                    warn_msgs.append(spec.hitl_reason or "Requires clarification")
                if validation.errors:
                    conf = min(conf, 0.55)

                tables = _extract_tables_from_sql(compiled_sql)
                # MySQL: strip accidental `table.DATE_FORMAT(...)` qualification.
                if profile.is_mysql:
                    compiled_sql = _mysql_unqualify_builtin_functions(compiled_sql, known_qualifiers)
                    compiled_sql = mysql_fix_only_full_group_by_order(compiled_sql)
                return SQLGenerateResponse(
                    sql=compiled_sql,
                    explanation=f"SQL query for: {request.question[:100]}",
                    confidence=float(conf),
                    tables=tables,
                    warnings=warn_msgs or None,
                )
            except Exception as e:
                if not allow_legacy_fallback:
                    logger.exception(f"[Text2SQL] QuerySpec path failed (legacy fallback disabled): {e}")
                    raise HTTPException(status_code=500, detail=f"Text2SQL (QuerySpec) failed: {e}")
                logger.warning(f"[Text2SQL] QuerySpec path failed; falling back to legacy SQL generation: {e}")
        messages = registry.render_messages(prompt_name, variables)
        cfg = registry.get_config(prompt_name)

        model = _explorer_model_for_sql(request.dialect)
        if sql_provider_resolved == "openai":
            model = (os.getenv("EXPLORER_SQL_OPENAI_MODEL") or cfg.get("model") or model).strip()

        temperature = cfg["parameters"].get("temperature", request.temperature)
        max_tokens = cfg["parameters"].get("max_tokens", request.max_tokens)

        # Some Ollama models (notably sqlcoder) work reliably via /v1/completions rather than chat-completions.
        response = await sql_client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        content = (response.choices[0].message.content or "").strip()
        if not content and sql_provider_resolved == "ollama":
            prompt_text = "\n\n".join([m.get("content", "") for m in (messages or []) if m.get("content")])
            resp2 = await sql_client.completions.create(
                model=model,
                prompt=prompt_text,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            # Ollama OpenAI-compat returns text completions in choices[].text
            content = (getattr(resp2.choices[0], "text", "") or "").strip()
        logger.info(
            f"🧠 SQL model output (provider={sql_provider_resolved}, model={model}) len={len(content)} preview={content[:200]!r}"
        )
        parsed: dict = {}
        sql_candidate = ""
        explanation = None
        confidence = None
        warnings = None
        # Some offline models (e.g., sqlcoder) may ignore the JSON contract and return plain SQL.
        try:
            parsed = json.loads(_extract_json_text(content))
            sql_candidate = str(parsed.get("sql", "") or "")
            explanation = parsed.get("explanation")
            confidence = parsed.get("confidence")
            warnings = parsed.get("warnings")
        except Exception:
            sql_candidate = content

        sql = _extract_sql_text(sql_candidate)
        if not sql:
            logger.warning(
                f"❌ SQL extraction failed (provider={sql_provider_resolved}, model={model}). "
                f"raw_len={len(sql_candidate)} raw_preview={str(sql_candidate)[:200]!r}"
            )
            raise HTTPException(status_code=500, detail=f"No SQL returned for prompt {prompt_name}")

        # Enforce SELECT-only at the gateway boundary to prevent unsafe statements from reaching Explorer.
        if re.search(r"^\s*(SELECT|WITH)\b", sql, flags=re.IGNORECASE) is None:
            # If the model returned something else, try one more extraction pass first.
            sql2 = _extract_sql_text(str(sql_candidate))
            if re.search(r"^\s*(SELECT|WITH)\b", sql2 or "", flags=re.IGNORECASE) is not None:
                sql = sql2
            else:
                # Offline models occasionally emit a single token like "Query". Retry once with a stricter prompt.
                retry_sql = ""
                if sql_provider_resolved == "ollama":
                    try:
                        prompt_text = "\n\n".join([m.get("content", "") for m in (messages or []) if m.get("content")])
                        prompt_text = (
                            prompt_text
                            + "\n\nReturn ONLY executable SQL starting with SELECT or WITH. No other text."
                        )
                        resp_retry = await sql_client.completions.create(
                            model=model,
                            prompt=prompt_text,
                            temperature=0.0,
                            max_tokens=max_tokens,
                        )
                        retry_content = (getattr(resp_retry.choices[0], "text", "") or "").strip()
                        retry_sql = _extract_sql_text(retry_content) or ""
                    except Exception:
                        retry_sql = ""

                if re.search(r"^\s*(SELECT|WITH)\b", retry_sql or "", flags=re.IGNORECASE) is not None:
                    sql = retry_sql
                else:
                    # Final attempt: ask the repair prompt to rewrite into valid SELECT-only SQL.
                    try:
                        repair_prompt = "explorer/sql_repair" if sql_provider_resolved == "openai" else "explorer/sql_repair_offline"
                        issues = [
                            {
                                "type": "not_select",
                                "detail": f"Model did not return SELECT-only SQL. Output preview: {str(sql_candidate)[:120]!r}",
                            }
                        ]
                        repair_vars = dict(variables)
                        repair_vars.update({"previous_sql": str(sql_candidate), "issues": issues})
                        repair_messages = registry.render_messages(repair_prompt, repair_vars)
                        repair_cfg = registry.get_config(repair_prompt)
                        repair_temperature = repair_cfg["parameters"].get("temperature", 0.0)
                        repair_max_tokens = repair_cfg["parameters"].get("max_tokens", request.max_tokens)
                        repair_resp = await sql_client.chat.completions.create(
                            model=model,
                            messages=repair_messages,
                            temperature=repair_temperature,
                            max_tokens=repair_max_tokens,
                        )
                        repair_content = (repair_resp.choices[0].message.content or "").strip()
                        if not repair_content and sql_provider_resolved == "ollama":
                            repair_prompt_text = "\n\n".join(
                                [m.get("content", "") for m in (repair_messages or []) if m.get("content")]
                            )
                            repair_resp2 = await sql_client.completions.create(
                                model=model,
                                prompt=repair_prompt_text,
                                temperature=repair_temperature,
                                max_tokens=repair_max_tokens,
                            )
                            repair_content = (getattr(repair_resp2.choices[0], "text", "") or "").strip()

                        repaired_sql = _extract_sql_text(repair_content) or ""
                        if re.search(r"^\s*(SELECT|WITH)\b", repaired_sql, flags=re.IGNORECASE) is not None:
                            sql = repaired_sql
                        else:
                            sql = ""
                    except HTTPException:
                        raise
                    except Exception:
                        sql = ""

                # If still not SELECT-only, fall back to a more instruction-following model.
                if re.search(r"^\s*(SELECT|WITH)\b", sql or "", flags=re.IGNORECASE) is None:
                    fallbacks = (os.getenv("EXPLORER_SQL_FALLBACK_MODELS") or "qwen2.5:7b,llama3:latest,codellama:7b-instruct").strip()
                    fallback_models = [m.strip() for m in fallbacks.split(",") if m.strip() and m.strip() != model]
                    prompt_text = "\n\n".join([m.get("content", "") for m in (messages or []) if m.get("content")])
                    prompt_text = (
                        prompt_text
                        + "\n\nReturn ONLY executable SQL starting with SELECT or WITH. No other text."
                    )
                    for fm in fallback_models:
                        try:
                            resp_fb = await sql_client.completions.create(
                                model=fm,
                                prompt=prompt_text,
                                temperature=0.0,
                                max_tokens=max_tokens,
                            )
                            fb_content = (getattr(resp_fb.choices[0], "text", "") or "").strip()
                            fb_sql = _extract_sql_text(fb_content) or ""
                            if re.search(r"^\s*(SELECT|WITH)\b", fb_sql, flags=re.IGNORECASE) is not None:
                                logger.warning(
                                    f"[Text2SQL] Fallback model succeeded for non-SQL output: primary={model} fallback={fm}"
                                )
                                sql = fb_sql
                                break
                        except Exception:
                            continue

                if re.search(r"^\s*(SELECT|WITH)\b", sql or "", flags=re.IGNORECASE) is None:
                    raise HTTPException(status_code=500, detail="Model did not return a SELECT-only query")

        # Dialect normalization (generic): if model output drifts to another dialect,
        # transpile it back to the requested dialect.
        sql = _maybe_transpile_sql(sql, request.dialect)
        # Schema grounding: rewrite generic/hallucinated table names to the provided schema slice.
        sql = _rewrite_unknown_tables_to_schema(sql, parsed_tables)
        # Ensure FROM/JOIN tables are qualified when schema is known (generic across dialects).
        sql = _qualify_from_join_tables(sql, parsed_tables)
        # Column grounding: qualify unqualified columns when possible.
        sql = _auto_qualify_unqualified_columns(sql, request.dialect, parsed_tables)

        # MySQL doesn't typically use a 'public.' schema prefix (Postgres-style).
        # Our prompt schema uses 'public' as a placeholder; strip it for mysql/mariadb dialects.
        d = (request.dialect or "").lower()
        if profile.is_mysql:
            # Extra hardening for MySQL: strip common Postgres artifacts + normalize intervals.
            sql = _apply_mysql_cleanup(sql, time_grain, time_column, known_qualifiers)

        # Semantic guardrail: enforce requested time bucketing (e.g., month-wise must be month buckets).
        guardrail_warnings: list[str] = []
        if time_grain and time_column and not _sql_satisfies_time_grain(sql, request.dialect, time_grain, time_column):
            guardrail_warnings.append(
                f"Repaired SQL to satisfy requested time grain '{time_grain}' (time column: '{time_column}')."
            )
            repair_prompt = "explorer/sql_repair" if sql_provider_resolved == "openai" else "explorer/sql_repair_offline"
            repair_vars = dict(variables)
            repair_vars.update(
                {
                    "previous_sql": sql,
                    "issues": [
                        {
                            "type": "time_grain_mismatch",
                            "detail": f"Question requests {time_grain}-wise results, but SQL does not bucket {time_column} correctly.",
                        }
                    ],
                }
            )
            repair_messages = registry.render_messages(repair_prompt, repair_vars)
            repair_cfg = registry.get_config(repair_prompt)
            repair_temperature = repair_cfg["parameters"].get("temperature", 0.0)
            repair_max_tokens = repair_cfg["parameters"].get("max_tokens", request.max_tokens)
            repair_resp = await sql_client.chat.completions.create(
                model=model,
                messages=repair_messages,
                temperature=repair_temperature,
                max_tokens=repair_max_tokens,
            )

            repair_content = (repair_resp.choices[0].message.content or "").strip()
            if not repair_content and sql_provider_resolved == "ollama":
                repair_prompt_text = "\n\n".join(
                    [m.get("content", "") for m in (repair_messages or []) if m.get("content")]
                )
                repair_resp2 = await sql_client.completions.create(
                    model=model,
                    prompt=repair_prompt_text,
                    temperature=repair_temperature,
                    max_tokens=repair_max_tokens,
                )
                repair_content = (getattr(repair_resp2.choices[0], "text", "") or "").strip()
            try:
                repair_parsed = json.loads(_extract_json_text(repair_content))
                repaired_sql_candidate = str(repair_parsed.get("sql", "") or "")
            except Exception:
                repaired_sql_candidate = repair_content

            repaired_sql = _extract_sql_text(repaired_sql_candidate)
            if repaired_sql:
                if "mysql" in d or "mariadb" in d:
                    repaired_sql = re.sub(r"\bpublic\.", "", repaired_sql, flags=re.IGNORECASE)
                # Only accept repair if it actually satisfies the grain.
                if _sql_satisfies_time_grain(repaired_sql, request.dialect, time_grain, time_column):
                    sql = repaired_sql
            # Deterministic fallback for common mistakes (ensures consistent UX even if model is flaky).
            if not _sql_satisfies_time_grain(sql, request.dialect, time_grain, time_column):
                rewritten = _rewrite_time_bucket_simple(sql, request.dialect, time_grain, time_column)
                if rewritten and rewritten != sql and _sql_satisfies_time_grain(rewritten, request.dialect, time_grain, time_column):
                    sql = rewritten
                    guardrail_warnings.append("Applied deterministic time-bucketing rewrite.")

        # Validate and repair SQL using the typed schema when available.
        # This is the primary correctness loop for the SQLCoder path.
        sql_repair_max = int(os.getenv("TEXT2SQL_SQL_REPAIR_MAX_ATTEMPTS", "3") or "3")
        best_sql = sql
        best_validation = validate_sql(
            best_sql,
            request.dialect,
            schema=typed_schema,
            spec=None,
            require_limit=True,
            max_limit=10000,
        )

        # Deterministic schema-based repair before asking the model.
        if best_validation.errors:
            maybe = _deterministic_schema_repair(best_sql, request.dialect, parsed_tables, typed_schema)
            if maybe and maybe != best_sql:
                if profile.is_mysql:
                    maybe = _apply_mysql_cleanup(maybe, time_grain, time_column, known_qualifiers)
                v2 = validate_sql(
                    maybe,
                    request.dialect,
                    schema=typed_schema,
                    spec=None,
                    require_limit=True,
                    max_limit=10000,
                )
                if len(v2.errors) < len(best_validation.errors):
                    best_sql = maybe
                    best_validation = v2

        if best_validation.errors and sql_repair_max > 0:
            repair_prompt = "explorer/sql_repair" if sql_provider_resolved == "openai" else "explorer/sql_repair_offline"
            for attempt in range(sql_repair_max):
                issues = [{"type": e.code, "detail": e.message} for e in (best_validation.errors or [])]
                repair_vars = dict(variables)
                repair_vars.update({"previous_sql": best_sql, "issues": issues})
                repair_messages = registry.render_messages(repair_prompt, repair_vars)
                repair_cfg = registry.get_config(repair_prompt)
                repair_temperature = repair_cfg["parameters"].get("temperature", 0.0)
                repair_max_tokens = repair_cfg["parameters"].get("max_tokens", request.max_tokens)

                repair_resp = await sql_client.chat.completions.create(
                    model=model,
                    messages=repair_messages,
                    temperature=repair_temperature,
                    max_tokens=repair_max_tokens,
                )
                repair_content = (repair_resp.choices[0].message.content or "").strip()
                if not repair_content and sql_provider_resolved == "ollama":
                    repair_prompt_text = "\n\n".join(
                        [m.get("content", "") for m in (repair_messages or []) if m.get("content")]
                    )
                    repair_resp2 = await sql_client.completions.create(
                        model=model,
                        prompt=repair_prompt_text,
                        temperature=repair_temperature,
                        max_tokens=repair_max_tokens,
                    )
                    repair_content = (getattr(repair_resp2.choices[0], "text", "") or "").strip()

                try:
                    repair_parsed = json.loads(_extract_json_text(repair_content))
                    repaired_sql_candidate = str(repair_parsed.get("sql", "") or "")
                except Exception:
                    repaired_sql_candidate = repair_content

                repaired_sql = _extract_sql_text(repaired_sql_candidate) or ""
                if not repaired_sql:
                    break

                # Enforce SELECT-only after repair.
                if re.search(r"^\s*(SELECT|WITH)\b", repaired_sql, flags=re.IGNORECASE) is None:
                    break

                # MySQL cleanup after repair.
                if profile.is_mysql:
                    repaired_sql = _apply_mysql_cleanup(repaired_sql, time_grain, time_column, known_qualifiers)

                v = validate_sql(
                    repaired_sql,
                    request.dialect,
                    schema=typed_schema,
                    spec=None,
                    require_limit=True,
                    max_limit=10000,
                )

                # Keep best attempt; stop early on success.
                if len(v.errors) < len(best_validation.errors):
                    best_sql = repaired_sql
                    best_validation = v
                if not best_validation.errors:
                    break

            sql = best_sql
            if best_validation.errors:
                # Production safety: do NOT return SQL that fails schema/AST validation.
                # Surface a structured error so UI can ask for clarification (HITL) or narrow tables.
                err_msgs = [e.message for e in best_validation.errors]
                raise HTTPException(
                    status_code=422,
                    detail={
                        "message": "Generated SQL failed validation",
                        "errors": err_msgs[:10],
                    },
                )
        
        # Extract tables mentioned
        tables = _extract_tables_from_sql(sql)
        
        if not isinstance(warnings, list):
            warnings = None
        if guardrail_warnings:
            warnings = (warnings or []) + guardrail_warnings

        # Final safety gate: never return SQL that fails schema/AST validation when typed schema is present.
        if typed_schema is not None:
            final_v = validate_sql(
                sql,
                request.dialect,
                schema=typed_schema,
                spec=None,
                require_limit=True,
                max_limit=10000,
            )
            if final_v.errors:
                # Last-resort deterministic repair: add missing JOINs / qualify columns using FK hints.
                repaired = _deterministic_schema_repair(sql, request.dialect, parsed_tables, typed_schema)
                if repaired and repaired != sql:
                    if profile.is_mysql:
                        repaired = _apply_mysql_cleanup(repaired, time_grain, time_column, known_qualifiers)
                    v2 = validate_sql(
                        repaired,
                        request.dialect,
                        schema=typed_schema,
                        spec=None,
                        require_limit=True,
                        max_limit=10000,
                    )
                    if not v2.errors:
                        sql = repaired
                    else:
                        err_msgs = [e.message for e in v2.errors]
                        raise HTTPException(
                            status_code=422,
                            detail={
                                "message": "Generated SQL failed validation",
                                "errors": err_msgs[:10],
                            },
                        )
                else:
                    err_msgs = [e.message for e in final_v.errors]
                    raise HTTPException(
                        status_code=422,
                        detail={
                            "message": "Generated SQL failed validation",
                            "errors": err_msgs[:10],
                        },
                    )

        return SQLGenerateResponse(
            sql=sql,
            explanation=explanation or f"SQL query for: {request.question[:100]}",
            confidence=float(confidence or 0.75),
            tables=tables,
            warnings=warnings,
        )
        
    except HTTPException:
        # Preserve intended HTTP status codes (e.g. 422 validation failures).
        raise
    except Exception as e:
        logger.exception(f"SQL generation failed: {e}")
        raise HTTPException(status_code=500, detail=f"SQL generation failed: {str(e)}")


def _mock_sql_generation(question: str, dialect: str, schema: str | None, tables: str | None) -> str:
    """Mock SQL generation for testing (intentionally minimal).

    We do not hardcode domain keywords here. In mock mode, Explorer should rely on
    HITL to confirm tables and then operators can validate the generated SQL path.
    """
    import re

    # Pick a real table name if provided
    focus_tables: list[str] = []
    if tables:
        focus_tables = [t.strip() for t in tables.split(",") if t.strip()]

    # Try extracting table names from schema string: "table(col1,...); other(col...)"
    schema_tables: list[str] = []
    if schema:
        schema_tables = re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\(", schema)

    # Prefer focus tables, then schema tables, else fallback
    default_table = (focus_tables[0] if focus_tables else (schema_tables[0] if schema_tables else "table_name"))

    # Deterministic safe fallback
    return f"SELECT * FROM {default_table} LIMIT 100"


def _extract_tables_from_sql(sql: str) -> list[str]:
    """Extract table names from SQL query"""
    import re
    tables = set()
    
    # Match FROM table and JOIN table patterns
    patterns = [
        r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, sql, re.IGNORECASE)
        tables.update(matches)
    
    return list(tables)


# ==============================================================================
# HITL NODE INPUT INTERPRETATION ENDPOINT
# ==============================================================================

class HITLInterpretRequest(BaseModel):
    """Request to interpret a user's NL response for a DAG node"""
    node_id: str
    node_kind: str
    hitl_prompt: str
    user_response: str
    node_context: dict | None = None


class HITLInterpretResponse(BaseModel):
    """Response with interpreted configuration"""
    success: bool
    config_patch: dict | None = None
    error: str | None = None
    needs_clarification: bool = False
    clarification_prompt: str | None = None
    confidence: float = 1.0


@app.post("/v1/hitl/interpret", response_model=HITLInterpretResponse)
async def interpret_hitl_input(request: HITLInterpretRequest):
    """
    Interpret a user's natural language response into structured config updates.
    
    This endpoint is called by the Temporal workflow when a DAG node is waiting
    for user input and the user responds in natural language.
    
    Example:
        POST /v1/hitl/interpret
        {
            "node_id": "source_1",
            "node_kind": "source",
            "hitl_prompt": "Which tables do you want to sync?",
            "user_response": "sync the users and orders tables"
        }
        
        Response:
        {
            "success": true,
            "config_patch": {"tables": ["users", "orders"]},
            "confidence": 0.9
        }
    """
    try:
        logger.info(f"🎯 HITL interpretation request for node {request.node_id}")
        
        # Import the interpreter
        from src.agents.dag_planner.hitl_interpreter import interpret_node_input
        from src.utils.llm_client import LLMClient
        
        llm_client = LLMClient()
        
        result = interpret_node_input(
            llm_client=llm_client,
            node_id=request.node_id,
            node_kind=request.node_kind,
            hitl_prompt=request.hitl_prompt,
            user_response=request.user_response,
            node_context=request.node_context,
        )
        
        return HITLInterpretResponse(
            success=result.get("success", False),
            config_patch=result.get("config_patch"),
            error=result.get("error"),
            needs_clarification=result.get("needs_clarification", False),
            clarification_prompt=result.get("clarification_prompt"),
            confidence=result.get("confidence", 1.0),
        )
        
    except Exception as e:
        logger.exception(f"HITL interpretation failed: {e}")
        return HITLInterpretResponse(
            success=False,
            error=str(e),
            needs_clarification=True,
            clarification_prompt="Sorry, I had trouble understanding. Could you please try again?",
        )


## Explorer endpoints are implemented in `src/agents/explorer/api.py` and mounted via
## `app.include_router(explorer_router)` above.


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
