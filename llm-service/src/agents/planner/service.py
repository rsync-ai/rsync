#!/usr/bin/env python3
"""
Planner HTTP Service
Converts natural language to pipeline plans using LLM

Features:
- Dynamic plan generation from natural language
- Just-in-Time (JIT) connector generation for unknown tools
- Connection matching and validation
- Pluggable planning strategies (LLM + Heuristic fallback)
"""

import os
import sys
import logging
import time
import json
import requests
from typing import Dict, Any, Optional, List, Tuple

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn
import asyncio

# Add parent directory to path to import utils
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))
from src.utils.llm_client import LLMClient
from src.utils.telemetry import (
    init_tracer, instrument_fastapi, instrument_requests,
    start_span, get_current_trace_id, add_span_attributes, inject_trace_context,
    setup_logging, bind_log_context, reset_log_context, ids_from_carrier
)
from src.utils.ratelimit import get_limiter
from src.utils.masking import mask_config, config_summary, mask_dict
from src.utils.connector_paths import resolve_current_dir, iter_connector_dirs
from opentelemetry.trace import SpanKind
from opentelemetry import trace

# DAG planner strategy (graph planning only; execution remains in Temporal)
from src.agents.dag_planner.strategy import DAGPlanningStrategy

# Setup trace-aware logging FIRST
SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "planner")
setup_logging(SERVICE_NAME)
logger = logging.getLogger("planner-service")

# Import planning strategies
from .strategies import (
    PlanningContext,
    PlanResult,
    LLMPlanningStrategy,
    HeuristicPlanningStrategy,
    CompositePlanningStrategy,
    AsyncConnectorChecker,
    ToolRegistry,
    get_tool_registry,
    detect_tools_from_text,
    # Phase 1: Metadata-driven CDC Provider Selection
    CDCProviderRegistry,
    get_cdc_provider_registry,
    # Phase 2: Plan-time Topic Provisioning
    TopicProvisioner,
    TopicProvisioningResult,
    get_topic_provisioner,
    # Smart Pipeline Decider
    TableStats,
    DataStatistics,
    PipelinePhase,
    SmartStrategyDecision,
    SmartPipelineDecider,
    get_smart_pipeline_decider,
)

# Import CDC config generator
from .cdc_config_generator import (
    CDCConfigGenerator,
    CDCConfigResult,
    get_cdc_config_generator
)

# Initialize OpenTelemetry tracer
tracer = init_tracer("planner", "2.0.0")
instrument_requests()

app = FastAPI(
    title="Planner Service",
    description="Natural language to pipeline planning with JIT connector generation",
    version="2.0.0"
)

# Instrument FastAPI with OpenTelemetry
instrument_fastapi(app)

# Prometheus metrics endpoint (/prometheus) — scraped by OTEL Collector → SigNoz. F-Obs-2.
from src.utils.metrics import mount_metrics  # noqa: E402
mount_metrics(app)

# ============================================================================
# Phase 5: Safety controls (rate limiting + request size + concurrency)
# ============================================================================

def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name, "") or "").strip()
    if raw == "":
        return default
    try:
        return int(raw)
    except Exception:
        return default

PLANNER_MAX_REQUEST_BYTES = _env_int("RSYNC_LLM_MAX_REQUEST_BYTES", 200_000)
PLANNER_MAX_CONCURRENCY = _env_int("RSYNC_LLM_MAX_CONCURRENCY", 8)
RL_IP_PER_MIN = _env_int("RSYNC_LLM_RL_IP_PER_MIN", 120)
RL_USER_PER_MIN = _env_int("RSYNC_LLM_RL_USER_PER_MIN", 240)

_limiter = get_limiter()
_semaphore = asyncio.Semaphore(max(1, PLANNER_MAX_CONCURRENCY))

def _client_ip(req: Request) -> str:
    xff = (req.headers.get("x-forwarded-for") or "").split(",")[0].strip()
    if xff:
        return xff
    return (req.client.host if req.client else "unknown") or "unknown"

def _user_key(req: Request) -> Optional[str]:
    for h in ("x-rsync-user-id", "x-user-id", "x-userid", "x-forwarded-user"):
        v = (req.headers.get(h) or "").strip()
        if v:
            return v
    return None

@app.middleware("http")
async def safety_middleware(request: Request, call_next):
    if request.url.path == "/health":
        return await call_next(request)

    async with _semaphore:
        body = None
        if request.method in ("POST", "PUT", "PATCH"):
            body = await request.body()
            if body is not None and len(body) > PLANNER_MAX_REQUEST_BYTES:
                return JSONResponse(
                    status_code=413,
                    content={"error": "request_too_large", "message": "Request body exceeds limit"},
                )

        # Bind pipeline/execution ids so all logs for this request correlate by pipeline in SigNoz.
        pid, eid = ids_from_carrier(request.headers, body)
        ctx_tokens = bind_log_context(pid, eid) if (pid or eid) else None

        ip = _client_ip(request)
        user = _user_key(request)

        try:
            try:
                ok, retry = await _limiter.hit(f"rl:planner:ip:{ip}", RL_IP_PER_MIN, 60)
                if not ok:
                    return JSONResponse(status_code=429, content={"error": "rate_limited", "scope": "ip"}, headers={"Retry-After": str(retry)})
                if user:
                    ok, retry = await _limiter.hit(f"rl:planner:user:{user}", RL_USER_PER_MIN, 60)
                    if not ok:
                        return JSONResponse(status_code=429, content={"error": "rate_limited", "scope": "user"}, headers={"Retry-After": str(retry)})
            except Exception as e:
                logger.warning(f"⚠️  Rate limiter error (fail-open): {e}")

            return await call_next(request)
        finally:
            if ctx_tokens is not None:
                reset_log_context(ctx_tokens)

# Configuration
TOOL_GENERATOR_URL = os.getenv("TOOL_GENERATOR_URL", "http://tool-generator:5010")
API_GATEWAY_URL = os.getenv("API_GATEWAY_URL", "http://api-gateway:8080")
CONNECTORS_DIR = os.getenv("CONNECTORS_DIR", "/app/shared/mcp-connectors")
llm_client = LLMClient()

# Initialize dynamic tool registry (replaces hardcoded KNOWN_* lists)
tool_registry = get_tool_registry(TOOL_GENERATOR_URL)

# Initialize planning strategies
llm_strategy = LLMPlanningStrategy(llm_client, tool_registry)
heuristic_strategy = HeuristicPlanningStrategy(tool_registry)
base_planning_strategy = CompositePlanningStrategy(primary=llm_strategy, fallback=heuristic_strategy)

# DAG-first routing: only used when DAG strategy can_handle() returns True
dag_strategy = DAGPlanningStrategy(llm_client, tool_registry)
planning_strategy = CompositePlanningStrategy(primary=dag_strategy, fallback=base_planning_strategy)

# Async connector checker for non-blocking JIT generation
connector_checker = AsyncConnectorChecker(CONNECTORS_DIR, TOOL_GENERATOR_URL)

# Request/Response Models
class PlanRequest(BaseModel):
    """Request to create a pipeline plan"""
    natural_language: str
    context: Optional[Dict[str, Any]] = None
    pipeline_id: Optional[str] = None

class PlanResponse(BaseModel):
    """Response with pipeline plan"""
    success: bool
    pipeline_id: str
    plan: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    generated_connectors: Optional[List[str]] = None
    strategy_used: Optional[str] = None  # Track which planning strategy was used


# =============================================================================
# CDC CONFIG GENERATION MODELS
# =============================================================================

class CDCConfigRequest(BaseModel):
    """Request to generate CDC (Debezium) configuration"""
    source_type: str  # mysql, postgresql, mongodb, oracle, sqlserver, etc.
    connection_config: Dict[str, Any]  # host, port, user, password, database
    tables: List[str]  # Tables to capture: ["users", "orders"] or ["db.users"]
    cdc_mode: str = "initial"  # "initial" (snapshot+stream) or "streaming_only"
    connector_name: Optional[str] = None  # Auto-generated if not provided
    overrides: Optional[Dict[str, Any]] = None  # Additional Debezium config


class CDCConfigResponse(BaseModel):
    """Response with generated Debezium configuration"""
    success: bool
    config: Optional[Dict[str, Any]] = None
    connector_name: Optional[str] = None
    kafka_topic: Optional[str] = None
    error: Optional[str] = None
    method_used: Optional[str] = None  # "metadata", "template", or "llm"
    warnings: Optional[List[str]] = None


class SupportedCDCDatabasesResponse(BaseModel):
    """Response listing supported CDC databases"""
    databases: Dict[str, Any]
    count: int


# Initialize CDC config generator (lazy, created on first use)
_cdc_generator: Optional[CDCConfigGenerator] = None


def get_cdc_generator() -> CDCConfigGenerator:
    """Get or create the CDC config generator"""
    global _cdc_generator
    if _cdc_generator is None:
        _cdc_generator = get_cdc_config_generator(llm_client, CONNECTORS_DIR)
    return _cdc_generator


# =============================================================================
# SMART PIPELINE STRATEGY MODELS (Phase 2)
# =============================================================================

class TableStatsRequest(BaseModel):
    """Statistics for a single table"""
    name: str
    row_count: int = 0
    estimated_size_mb: float = 0.0
    column_count: int = 0
    has_primary_key: bool = True


class DataStatsRequest(BaseModel):
    """Aggregate data statistics for strategy decision"""
    tables: List[TableStatsRequest]
    total_rows: Optional[int] = None  # Auto-calculated if not provided
    estimated_size_mb: Optional[float] = None


class SmartStrategyRequest(BaseModel):
    """Request for smart pipeline strategy recommendation"""
    sync_mode: str  # "batch" or "cdc"
    source_type: str  # "mysql", "postgresql", etc.
    data_stats: DataStatsRequest
    cdc_mode: Optional[str] = None  # "initial" or "streaming_only"
    user_preference: Optional[str] = None  # Explicit strategy preference


class PhaseResponse(BaseModel):
    """A single phase in a multi-phase pipeline"""
    phase: int
    type: str
    description: str
    config: Dict[str, Any] = {}
    estimated_duration: Optional[str] = None


class SmartStrategyResponse(BaseModel):
    """Response with strategy recommendation"""
    success: bool
    strategy: str  # "cdc_snapshot", "batch", "hybrid", "cdc_streaming_only"
    phases: List[PhaseResponse]
    recommendation: str  # Human-readable recommendation
    requires_user_confirmation: bool
    estimated_total_time: Optional[str] = None
    benefits: List[str] = []
    warnings: List[str] = []
    data_summary: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


# Initialize smart pipeline decider
_smart_decider: Optional[SmartPipelineDecider] = None


def get_strategy_decider() -> SmartPipelineDecider:
    """Get or create the smart pipeline decider"""
    global _smart_decider
    if _smart_decider is None:
        _smart_decider = get_smart_pipeline_decider()
    return _smart_decider


# =============================================================================
# JUST-IN-TIME (JIT) CONNECTOR GENERATION
# =============================================================================

def connector_exists(connector_name: str) -> bool:
    """Check if a connector exists in the shared connectors directory."""
    raw = (connector_name or "").strip().lower()
    if not raw:
        return False

    # Accept canonical kebab-case ids while supporting underscore on-disk folders.
    candidates = [raw]
    if "-" in raw:
        candidates.append(raw.replace("-", "_"))
    if "_" in raw:
        candidates.append(raw.replace("_", "-"))

    def _exists_in_tree(base_root: str, cand: str) -> bool:
        if not base_root or not os.path.isdir(base_root):
            return False
        # Direct child: <root>/<id> is a connector root. resolve_current_dir maps it
        # to versions/<current_version>/ (canonical source); non-existent paths fall
        # back harmlessly to a missing connector.py.
        if (resolve_current_dir(os.path.join(base_root, cand)) / "connector.py").exists():
            return True
        # Nested path (public/<category>/<id>, internal/<id>): find a connector root named cand.
        for connector_dir in iter_connector_dirs(base_root):
            if connector_dir.name == cand and (resolve_current_dir(connector_dir) / "connector.py").exists():
                return True
        return False

    roots = [
        CONNECTORS_DIR,  # legacy flat + shared libs
        os.path.join(CONNECTORS_DIR, "public"),
        os.path.join(CONNECTORS_DIR, "internal"),
    ]

    for cand in dict.fromkeys(candidates):
        for root in roots:
            if _exists_in_tree(root, cand):
                logger.debug(f"✅ Connector '{connector_name}' resolved to '{cand}' under {root}")
                return True

    logger.debug(f"❌ Connector '{connector_name}' not found (candidates: {candidates})")
    return False


def generate_connector_sync(connector_name: str, description: str = None) -> Tuple[bool, str]:
    """
    Synchronously generate a new MCP connector using the Tool Generator.
    This is the core of Just-in-Time integration.
    """
    logger.info(f"🔧 JIT: Generating connector '{connector_name}'...")
    
    try:
        gen_request = {
            "connector_name": connector_name,
            "description": description or f"MCP connector for {connector_name} database/service",
            "force_regenerate": False
        }
        
        headers = {"Content-Type": "application/json"}
        inject_trace_context(headers)
        
        # Tool-generator v1 API: POST /v1/generate (legacy /generate was removed)
        logger.info(f"📡 Calling Tool Generator at {TOOL_GENERATOR_URL}/v1/generate [trace_id={get_current_trace_id()}]")
        resp = requests.post(
            f"{TOOL_GENERATOR_URL}/v1/generate",
            json=gen_request,
            headers=headers,
            timeout=120
        )
        
        if resp.status_code == 200:
            result = resp.json()
            if result.get("success"):
                logger.info(f"✅ JIT: Connector '{connector_name}' generated successfully!")
                return True, f"Connector '{connector_name}' generated"
            else:
                error = result.get("error", "Unknown error")
                logger.error(f"❌ JIT: Generation failed: {error}")
                return False, error
        else:
            logger.error(f"❌ JIT: Tool Generator returned {resp.status_code}")
            return False, f"Tool Generator error: {resp.status_code}"
            
    except requests.exceptions.Timeout:
        logger.error(f"❌ JIT: Generation timed out for '{connector_name}'")
        return False, "Generation timed out (>120s)"
    except Exception as e:
        logger.exception(f"❌ JIT: Error generating connector: {e}")
        return False, str(e)


def ensure_connectors_exist(tools: List[str], context: str = None) -> Tuple[List[str], List[str]]:
    """
    Ensure all required connectors exist, generating missing ones.
    Returns tuple of (available_tools, generated_tools)
    """
    available = []
    generated = []
    
    for tool in tools:
        if connector_exists(tool):
            logger.info(f"✅ Connector '{tool}' exists")
            available.append(tool)
        else:
            logger.info(f"⚠️  Connector '{tool}' not found, triggering JIT generation...")
            success, message = generate_connector_sync(tool, context)
            if success:
                available.append(tool)
                generated.append(tool)
            else:
                logger.warning(f"❌ Could not generate '{tool}': {message}")
    
    return available, generated


def fetch_connections(user_id: str = None):
    """Fetch available connections from API Gateway"""
    try:
        headers = {}
        inject_trace_context(headers)
        if user_id:
            headers["X-User-ID"] = user_id
        
        resp = requests.get(
            f"{API_GATEWAY_URL}/api/v1/connections",
            headers=headers,
            timeout=3
        )
        if resp.status_code == 200:
            data = resp.json()
            connections = data.get("connections", [])
            logger.info(f"✅ Fetched {len(connections)} connections from API Gateway [trace_id={get_current_trace_id()}]")
            return connections
        else:
            logger.warning(f"⚠️  Failed to fetch connections: {resp.status_code}")
            return []
    except Exception as e:
        logger.warning(f"⚠️  Could not fetch connections from API Gateway: {e}")
        return []


def fetch_registry_tools() -> List[str]:
    """Fetch available tools from the Tool Generator registry.

    The tool-generator serves the catalog at ``/v1/connectors`` — the
    unversioned ``/connectors`` path was the legacy alias and is now a
    404. The previous code hit the 404 every call and silently fell back
    to the static registry, which (a) spammed the tool-generator logs
    and (b) hid newly-generated connectors from the planner.

    The ``/v1/connectors`` response is a list of objects with metadata
    (``{name, path, category, …}``) — older callers expected a plain
    list of name strings, and the planner downstream uses ``set(...)``
    on the result, which raised ``TypeError: unhashable type: 'dict'``
    when the new shape leaked through. We project to ``[name, ...]``
    here so the contract returned to callers is unchanged.
    """
    try:
        resp = requests.get(f"{TOOL_GENERATOR_URL}/v1/connectors", timeout=2)
        if resp.status_code == 200:
            raw = resp.json().get("connectors", [])
            names: List[str] = []
            for c in raw:
                if isinstance(c, str):
                    s = c.strip()
                    if s:
                        names.append(s)
                elif isinstance(c, dict):
                    n = c.get("name")
                    if isinstance(n, str) and n.strip():
                        names.append(n.strip())
            return names
        logger.debug(
            "tool-generator /v1/connectors returned %s; using static registry fallback",
            resp.status_code,
        )
    except Exception as e:
        logger.warning(f"Could not fetch tools from {TOOL_GENERATOR_URL}: {e}")

    # Fallback to dynamic registry defaults
    return tool_registry.get_all_tools()

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "planner",
        "trace_id": get_current_trace_id(),
        "version": "2.0.0"
    }


# =============================================================================
# CDC CONFIGURATION ENDPOINTS
# =============================================================================

@app.post("/generate-cdc-config", response_model=CDCConfigResponse)
async def generate_cdc_config(request: CDCConfigRequest):
    """
    Generate Debezium CDC configuration for any database type.
    
    This endpoint replaces hardcoded CDC config generation in Go handlers.
    Configuration is generated using:
    1. Connector metadata.json templates (preferred)
    2. Built-in database templates (common databases)
    3. LLM generation (unknown database types)
    
    Args:
        request: CDCConfigRequest with source_type, connection_config, tables, cdc_mode
        
    Returns:
        CDCConfigResponse with generated Debezium configuration
        
    Example:
        POST /generate-cdc-config
        {
            "source_type": "mysql",
            "connection_config": {
                "host": "mysql.example.com",
                "port": 3306,
                "user": "cdc_user",
                "password": "secret",
                "database": "production"
            },
            "tables": ["users", "orders"],
            "cdc_mode": "initial"
        }
    """
    try:
        logger.info(f"🔧 Generating CDC config for {request.source_type}")
        logger.info(f"   Tables: {request.tables}")
        logger.info(f"   CDC Mode: {request.cdc_mode}")
        
        generator = get_cdc_generator()
        
        result: CDCConfigResult = generator.generate_config(
            source_type=request.source_type,
            connection_config=request.connection_config,
            tables=request.tables,
            cdc_mode=request.cdc_mode,
            connector_name=request.connector_name,
            **(request.overrides or {})
        )
        
        if result.success:
            # Mask sensitive data in logs
            masked_config = mask_dict(result.config) if result.config else {}
            logger.info(f"✅ CDC config generated using {result.method_used}")
            logger.info(f"   Connector: {result.connector_name}")
            logger.info(f"   Kafka topic: {result.kafka_topic}")
            logger.debug(f"   Config (masked): {masked_config}")
        else:
            logger.error(f"❌ CDC config generation failed: {result.error}")
        
        return CDCConfigResponse(
            success=result.success,
            config=result.config,
            connector_name=result.connector_name,
            kafka_topic=result.kafka_topic,
            error=result.error,
            method_used=result.method_used,
            warnings=result.warnings if result.warnings else None
        )
        
    except Exception as e:
        logger.exception(f"Error generating CDC config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/cdc/supported-databases", response_model=SupportedCDCDatabasesResponse)
async def get_supported_cdc_databases():
    """
    Get list of databases supported for CDC with their capabilities.
    
    Returns:
        SupportedCDCDatabasesResponse with database info
    """
    try:
        generator = get_cdc_generator()
        databases = generator.get_supported_databases()
        
        return SupportedCDCDatabasesResponse(
            databases=databases,
            count=len(databases)
        )
    except Exception as e:
        logger.exception(f"Error getting supported databases: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/cdc/validate-config")
async def validate_cdc_config(config: Dict[str, Any]):
    """
    Validate a Debezium CDC configuration.
    
    Args:
        config: Debezium configuration to validate
        
    Returns:
        Validation result with errors and warnings
    """
    try:
        generator = get_cdc_generator()
        result = generator.validate_config(config)
        
        return {
            "valid": result["valid"],
            "errors": result["errors"],
            "warnings": result["warnings"],
        }
    except Exception as e:
        logger.exception(f"Error validating CDC config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# SMART PIPELINE STRATEGY ENDPOINTS (Phase 2)
# =============================================================================

@app.post("/strategy/recommend", response_model=SmartStrategyResponse)
async def recommend_pipeline_strategy(request: SmartStrategyRequest):
    """
    Get intelligent pipeline strategy recommendation based on data volume.
    
    This endpoint analyzes data statistics and recommends the optimal sync strategy:
    - Small data (< 100K rows): Standard CDC snapshot
    - Medium data (100K - 10M rows): CDC snapshot with optimizations
    - Large data (> 10M rows or > 100GB): Hybrid batch + CDC approach
    
    Args:
        request: SmartStrategyRequest with sync_mode, source_type, data_stats
        
    Returns:
        SmartStrategyResponse with strategy, phases, and recommendation
        
    Example:
        POST /strategy/recommend
        {
            "sync_mode": "cdc",
            "source_type": "mysql",
            "data_stats": {
                "tables": [
                    {"name": "users", "row_count": 50000000, "estimated_size_mb": 25000},
                    {"name": "orders", "row_count": 100000000, "estimated_size_mb": 75000}
                ]
            }
        }
    """
    try:
        logger.info(f"📊 Recommending strategy for {request.source_type} ({request.sync_mode})")
        
        # Build DataStatistics from request
        stats = DataStatistics()
        for table in request.data_stats.tables:
            table_stats = TableStats(
                name=table.name,
                row_count=table.row_count,
                estimated_size_mb=table.estimated_size_mb,
                column_count=table.column_count,
                has_primary_key=table.has_primary_key,
            )
            stats.add_table(table_stats)
        
        # Override totals if provided
        if request.data_stats.total_rows is not None:
            stats.total_rows = request.data_stats.total_rows
        if request.data_stats.estimated_size_mb is not None:
            stats.estimated_size_mb = request.data_stats.estimated_size_mb
            stats.estimated_size_gb = request.data_stats.estimated_size_mb / 1024
        
        logger.info(f"   Total: {stats.total_rows:,} rows, {stats.estimated_size_gb:.2f} GB")
        
        # Get strategy recommendation
        decider = get_strategy_decider()
        decision = decider.decide_strategy(
            sync_mode=request.sync_mode,
            data_stats=stats,
            source_type=request.source_type,
            cdc_mode=request.cdc_mode,
            user_preference=request.user_preference,
        )
        
        logger.info(f"✅ Recommended strategy: {decision.strategy}")
        if decision.requires_user_confirmation:
            logger.info(f"   ⚠️ Requires user confirmation (large dataset)")
        
        # Convert phases to response format
        phases = [
            PhaseResponse(
                phase=p.phase_number,
                type=p.phase_type,
                description=p.description,
                config=p.config,
                estimated_duration=p.estimated_duration,
            )
            for p in decision.phases
        ]
        
        return SmartStrategyResponse(
            success=True,
            strategy=decision.strategy,
            phases=phases,
            recommendation=decision.recommendation,
            requires_user_confirmation=decision.requires_user_confirmation,
            estimated_total_time=decision.estimated_total_time,
            benefits=decision.benefits,
            warnings=decision.warnings,
            data_summary={
                "total_rows": stats.total_rows,
                "estimated_size_gb": round(stats.estimated_size_gb, 2),
                "table_count": stats.table_count,
                "largest_table": stats.largest_table,
            } if stats else None,
        )
        
    except Exception as e:
        logger.exception(f"Error recommending strategy: {e}")
        return SmartStrategyResponse(
            success=False,
            strategy="unknown",
            phases=[],
            recommendation="",
            requires_user_confirmation=False,
            error=str(e),
        )


@app.get("/strategy/thresholds")
async def get_strategy_thresholds():
    """
    Get the current thresholds used for strategy decisions.
    
    Returns:
        Dict with threshold values for each strategy decision point
    """
    decider = get_strategy_decider()
    return {
        "small_table_rows": decider.SMALL_TABLE_ROWS,
        "medium_table_rows": decider.MEDIUM_TABLE_ROWS,
        "large_table_gb": decider.LARGE_TABLE_GB,
        "hybrid_recommended_rows": decider.HYBRID_RECOMMENDED_ROWS,
        "descriptions": {
            "cdc_snapshot": f"Tables with < {decider.SMALL_TABLE_ROWS:,} rows",
            "cdc_snapshot_optimized": f"Tables with {decider.SMALL_TABLE_ROWS:,} - {decider.MEDIUM_TABLE_ROWS:,} rows",
            "hybrid": f"Tables with > {decider.HYBRID_RECOMMENDED_ROWS:,} rows OR > {decider.LARGE_TABLE_GB} GB",
        }
    }


# =============================================================================
# SMART PIPELINE INTELLIGENCE ENDPOINTS
# =============================================================================

@app.get("/intelligence/cdc-providers")
async def get_cdc_providers():
    """
    Get all CDC-capable source types and their providers.
    
    This endpoint exposes the metadata-driven CDC provider registry,
    showing which source types can use CDC and which provider handles them.
    
    Returns:
        Dict with CDC providers and their supported sources
    """
    try:
        registry = get_cdc_provider_registry()
        sources = registry.get_all_cdc_sources()
        
        provider_map = {}
        for source in sources:
            provider = registry.get_cdc_provider(source)
            config = registry.get_provider_config(source)
            
            if provider not in provider_map:
                provider_map[provider] = {"sources": [], "config": {}}
            
            provider_map[provider]["sources"].append(source)
            if config:
                provider_map[provider]["config"][source] = config
        
        return {
            "success": True,
            "cdc_capable_sources": sources,
            "providers": provider_map,
            "total_sources": len(sources),
            "message": f"System supports CDC for {len(sources)} database types"
        }
    except Exception as e:
        logger.exception(f"Error getting CDC providers: {e}")
        return {"success": False, "error": str(e)}


@app.get("/intelligence/topic-calculator")
async def calculate_topic_partitions(
    sync_mode: str = "cdc",
    estimated_size_gb: float = 0,
    table_count: int = 1
):
    """
    Calculate optimal Kafka topic partitions for a given workload.
    
    Args:
        sync_mode: "cdc" or "batch"
        estimated_size_gb: Estimated data size in GB
        table_count: Number of tables
        
    Returns:
        Recommended partition configuration
    """
    try:
        provisioner = get_topic_provisioner()
        partitions = provisioner.calculate_optimal_partitions(
            sync_mode=sync_mode,
            estimated_size_gb=estimated_size_gb,
            table_count=table_count
        )
        
        return {
            "success": True,
            "sync_mode": sync_mode,
            "input": {
                "estimated_size_gb": estimated_size_gb,
                "table_count": table_count
            },
            "recommendation": {
                "partitions": partitions,
                "replication_factor": provisioner.DEFAULT_REPLICATION,
                "estimated_throughput_mb_s": partitions * 10,  # ~10MB/s per partition
            },
            "explanation": (
                f"CDC mode: {table_count} tables → {partitions} partitions (1 per table for ordering)"
                if sync_mode == "cdc"
                else f"Batch mode: {estimated_size_gb:.1f}GB → {partitions} partitions ({provisioner.GB_PER_PARTITION}GB per partition)"
            )
        }
    except Exception as e:
        logger.exception(f"Error calculating partitions: {e}")
        return {"success": False, "error": str(e)}


@app.get("/intelligence/status")
async def get_intelligence_status():
    """
    Get the status of all smart pipeline intelligence components.
    
    Returns:
        Status of CDC registry, topic provisioner, and other components
    """
    try:
        cdc_registry = get_cdc_provider_registry()
        topic_provisioner = get_topic_provisioner()
        
        return {
            "success": True,
            "components": {
                "cdc_provider_registry": {
                    "status": "active",
                    "cdc_capable_sources": len(cdc_registry.get_all_cdc_sources()),
                    "description": "Metadata-driven CDC provider selection"
                },
                "topic_provisioner": {
                    "status": "active",
                    "min_partitions": topic_provisioner.MIN_PARTITIONS,
                    "max_partitions": topic_provisioner.MAX_PARTITIONS,
                    "description": "Plan-time topic provisioning with optimal partitions"
                },
                "cdc_validator": {
                    "status": "active",
                    "supported_databases": ["mysql", "postgresql", "sqlserver", "oracle", "mongodb"],
                    "description": "Pre-flight CDC prerequisite validation"
                }
            },
            "message": "🧠 Smart Pipeline Intelligence is active and ready"
        }
    except Exception as e:
        logger.exception(f"Error getting intelligence status: {e}")
        return {"success": False, "error": str(e)}


@app.post("/connectors/ensure")
async def ensure_connectors(tools: List[str], background: bool = False):
    """
    Ensure connectors exist, optionally in background.
    
    Args:
        tools: List of connector names to ensure exist
        background: If true, runs in background and returns immediately
        
    Returns:
        Status of connector availability
    """
    from fastapi import BackgroundTasks
    import asyncio
    
    if background:
        # Queue for background processing
        asyncio.create_task(
            connector_checker.ensure_connectors_exist_async(tools, "Background generation request")
        )
        return {
            "status": "queued",
            "tools": tools,
            "message": "Connector generation queued in background"
        }
    else:
        # Sync processing
        available, generated = await connector_checker.ensure_connectors_exist_async(tools)
        return {
            "status": "complete",
            "available": available,
            "generated": generated
    }

@app.post("/plan", response_model=PlanResponse)
async def create_plan(request: PlanRequest):
    """
    Create a pipeline plan from natural language.
    
    Features:
    - Just-in-Time (JIT) connector generation for unknown tools (async)
    - Pluggable planning strategies (LLM primary, heuristic fallback)
    - Dynamic tool registry integration
        
    Returns:
        PlanResponse with pipeline plan and metadata
    """
    try:
        logger.info("=" * 60)
        logger.info(f"🎯 Creating plan from NL: {request.natural_language[:100]}...")
        
        # =================================================================
        # STEP 1: Detect tools from natural language
        # =================================================================
        detected_tools = detect_tools_from_text(request.natural_language, tool_registry)
        
        # Augment with tools from explicit context (e.g. from Orchestrator)
        if request.context:
            if source_type := request.context.get("source_type"):
                if isinstance(source_type, str):
                    detected_tools.append(source_type.strip().lower().replace("_", "-"))
            if dest_type := request.context.get("destination_type"):
                if isinstance(dest_type, str):
                    detected_tools.append(dest_type.strip().lower().replace("_", "-"))
            detected_tools = list(set(detected_tools))
            
        logger.info(f"🔍 Detected tools: {detected_tools}")
        
        # =================================================================
        # STEP 2: Ensure all connectors exist (ASYNC JIT GENERATION)
        # =================================================================
        generated_connectors = []
        if detected_tools:
            # Use async connector checker for non-blocking JIT generation
            try:
                available_tools, generated = await connector_checker.ensure_connectors_exist_async(
                    detected_tools, 
                    request.natural_language
                )
                generated_connectors = generated
            except Exception as e:
                # Fallback to sync if async fails
                logger.warning(f"⚠️  Async connector check failed, using sync: {e}")
                available_tools, generated = ensure_connectors_exist(
                    detected_tools,
                    request.natural_language
                )
                generated_connectors = generated
            
            if generated_connectors:
                logger.info(f"🔧 JIT Generated {len(generated)} connector(s): {generated}")
        
        # =================================================================
        # STEP 3: Fetch available tools and connections
        # =================================================================
        registry_tools = fetch_registry_tools()
        all_available_tools = list(set(registry_tools + detected_tools))
        logger.info(f"📦 All available tools: {all_available_tools}")
        
        available_connections = fetch_connections(
            request.context.get("user_id") if request.context else None
        )

        # If the orchestrator passed connection IDs in context, attach the full connection
        # objects to the PlanningContext. This lets heuristic planning disambiguate
        # connectors that can be BOTH sources and destinations (e.g., postgresql/mysql).
        source_connection = None
        destination_connection = None
        if request.context and available_connections:
            src_id = request.context.get("source_connection_id")
            dst_id = request.context.get("destination_connection_id")
            if isinstance(src_id, str) and src_id:
                source_connection = next((c for c in available_connections if c.get("id") == src_id), None)
            if isinstance(dst_id, str) and dst_id:
                destination_connection = next((c for c in available_connections if c.get("id") == dst_id), None)
        
        # =================================================================
        # STEP 4: Build planning context and execute strategy
        # =================================================================
        # Extract sync mode and CDC mode from context
        sync_mode = None
        cdc_mode = None
        estimated_data_size_mb = None
        error_context = None

        if request.context:
            # Auto-replan (beta): a prior plan failed during execution and the workflow
            # re-invoked the planner with the sanitized failure. Thread it into planning so
            # the strategy can correct the plan. Absent on a normal first pass.
            raw_error_context = request.context.get("error_context")
            if isinstance(raw_error_context, dict) and raw_error_context:
                error_context = raw_error_context

            # Check for sync_mode in context (priority)
            raw_sync_mode = request.context.get("sync_mode")
            if isinstance(raw_sync_mode, str):
                sync_mode = raw_sync_mode.strip().lower()

            # Fallback: infer from common streaming flags
            is_streaming_flag = bool(request.context.get("is_streaming") or request.context.get("is_cdc"))
            if not sync_mode and is_streaming_flag:
                sync_mode = "cdc"

            # Normalize common sync_mode variants
            if sync_mode in ["streaming", "realtime", "real-time", "real time"]:
                sync_mode = "cdc"
            if sync_mode in ["one-time", "one_time", "once"]:
                sync_mode = "batch"

            # Get CDC mode (initial vs streaming_only)
            raw_cdc_mode = request.context.get("cdc_mode")
            if isinstance(raw_cdc_mode, str):
                cdc_mode = raw_cdc_mode.strip().lower()
            elif raw_cdc_mode is True:
                cdc_mode = "initial"

            # Normalize cdc_mode variants
            if cdc_mode in ["streaming", "stream_only", "stream-only", "streaming-only", "no_snapshot", "nosnapshot"]:
                cdc_mode = "streaming_only"
            if cdc_mode in ["never"]:
                cdc_mode = "never"
            
            # Get estimated data size
            estimated_data_size_mb = request.context.get("estimated_data_size_mb")
        
        planning_context = PlanningContext(
            natural_language=request.natural_language,
            pipeline_id=request.pipeline_id,
            detected_tools=detected_tools,
            available_connections=available_connections,
            available_tools=all_available_tools,
            generated_connectors=generated_connectors,
            trace_id=get_current_trace_id(),
            sync_mode=sync_mode,
            cdc_mode=cdc_mode,
            estimated_data_size_mb=estimated_data_size_mb,
            source_connection=source_connection,
            destination_connection=destination_connection,
            error_context=error_context,
        )
        
        # Execute planning strategy (LLM with heuristic fallback)
        logger.info("🤖 Executing planning strategy...")
        result = planning_strategy.create_plan(planning_context)

        # Robust logging even if result.plan is missing (e.g. guardrail error)
        step_count = 0
        if result.plan and isinstance(result.plan, dict):
            steps = result.plan.get("steps", [])
            if isinstance(steps, list):
                step_count = len(steps)
        logger.info(f"✅ Plan created using '{result.strategy_used}' strategy: {step_count} steps")
        logger.info("=" * 60)
            
        return PlanResponse(
            success=result.success,
            pipeline_id=result.pipeline_id,
            plan=result.plan,
            error=result.error,
            generated_connectors=result.generated_connectors,
            strategy_used=result.strategy_used
        )
            
    except Exception as e:
        logger.exception(f"Error creating plan: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def main():
    """Run the service"""
    port = int(os.getenv("PORT", "5011"))
    host = os.getenv("HOST", "0.0.0.0")
    enable_kafka = os.getenv("ENABLE_KAFKA_CONSUMER", "true").lower() == "true"
    
    logger.info("=" * 60)
    logger.info("🚀 Planner Service Starting")
    logger.info("=" * 60)
    logger.info(f"Host: {host}")
    logger.info(f"Port: {port}")
    logger.info(f"Kafka Consumer: {'Enabled' if enable_kafka else 'Disabled'}")
    logger.info(f"Endpoints:")
    logger.info(f"  - GET  http://localhost:{port}/health")
    logger.info(f"  - POST http://localhost:{port}/plan")
    if enable_kafka:
        logger.info(f"  - Kafka Topic: agent.planner.requests (consuming)")
        logger.info(f"  - Kafka Topic: agent.planner.responses (producing)")
    logger.info("=" * 60)
    
    # Start Kafka consumer if enabled
    if enable_kafka:
        try:
            import sys
            from pathlib import Path
            # Add current directory to path for kafka_consumer import
            current_dir = Path(__file__).parent
            if str(current_dir) not in sys.path:
                sys.path.insert(0, str(current_dir))
            
            from kafka_consumer import PlannerKafkaConsumer
            consumer = PlannerKafkaConsumer()
            if consumer.start():
                logger.info("✅ Kafka consumer started successfully")
            else:
                logger.warning("⚠️  Kafka consumer failed to start, continuing with HTTP only")
        except ImportError as e:
            logger.warning(f"⚠️  kafka_consumer module not found: {e}, skipping Kafka consumer")
        except Exception as e:
            logger.warning(f"⚠️  Failed to start Kafka consumer: {e}")
            logger.warning("⚠️  Continuing with HTTP only")
    
    # Start HTTP server
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info"
    )

if __name__ == "__main__":
    main()

