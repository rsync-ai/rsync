"""
OpenTelemetry utilities for Python services
Provides distributed tracing across all Python agents
"""

import os
import sys
import logging
import json
import contextvars
from typing import Optional, Dict, Any, Tuple
from contextlib import contextmanager
from datetime import datetime, timezone

# =============================================================================
# PIPELINE / EXECUTION LOG CONTEXT
# -----------------------------------------------------------------------------
# trace_id correlates a log to a single request span; pipeline_id/execution_id
# correlate it to the *pipeline run* it belongs to. The Go data-plane services
# already emit pipeline_id on their logs — these ContextVars let the Python
# services (planner / tool-generator) do the same so SigNoz can filter every
# service's logs by pipeline. Bound per-request by each service's HTTP middleware.
# =============================================================================
_pipeline_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("pipeline_id", default="")
_execution_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("execution_id", default="")


def bind_log_context(pipeline_id: Optional[str] = None, execution_id: Optional[str] = None) -> Tuple[Any, Any]:
    """Bind pipeline/execution ids to the current (async) context so every log
    record emitted while handling the request carries them. Returns reset tokens;
    pass them to ``reset_log_context`` in a finally block to avoid leakage."""
    pt = _pipeline_id_var.set((pipeline_id or "").strip())
    et = _execution_id_var.set((execution_id or "").strip())
    return pt, et


def reset_log_context(tokens: Tuple[Any, Any]) -> None:
    """Reset the pipeline/execution context vars using tokens from bind_log_context."""
    pt, et = tokens
    try:
        _pipeline_id_var.reset(pt)
        _execution_id_var.reset(et)
    except Exception:
        pass


def ids_from_carrier(headers: Any = None, body: Optional[bytes] = None) -> Tuple[str, str]:
    """Best-effort extraction of pipeline_id/execution_id from request headers
    (X-Pipeline-Id / X-Execution-Id, case-insensitive) falling back to the JSON
    body. Never raises — returns ("", "") if nothing is found."""
    pid = eid = ""
    try:
        if headers is not None:
            pid = headers.get("x-pipeline-id") or ""
            eid = headers.get("x-execution-id") or ""
        if (not pid or not eid) and body:
            data = json.loads(body)
            if isinstance(data, dict):
                pid = pid or str(data.get("pipeline_id") or "")
                eid = eid or str(data.get("execution_id") or "")
    except Exception:
        pass
    return pid.strip(), eid.strip()

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.propagate import set_global_textmap, get_global_textmap, inject, extract
from opentelemetry.trace import Status, StatusCode, SpanKind
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor


# =============================================================================
# TRACE-AWARE JSON LOGGING
# =============================================================================

class TraceContextFilter(logging.Filter):
    """
    Logging filter that injects OpenTelemetry trace_id and span_id into log records.
    This enables log-trace correlation in SigNoz.
    """
    
    def filter(self, record):
        span = trace.get_current_span()
        if span and span.get_span_context().is_valid:
            record.trace_id = format(span.get_span_context().trace_id, '032x')
            record.span_id = format(span.get_span_context().span_id, '016x')
        else:
            # If tracing is not active for this request, do NOT emit the all-zero IDs.
            # All-zero IDs are valid "default" placeholders in some SDKs, but they cause
            # confusion during debugging and can look like a real trace.
            record.trace_id = ""
            record.span_id = ""
        # Pipeline-run correlation (empty unless bound by request middleware).
        pid = _pipeline_id_var.get()
        eid = _execution_id_var.get()
        if pid:
            record.pipeline_id = pid
        if eid:
            record.execution_id = eid
        return True


class JSONFormatter(logging.Formatter):
    """
    JSON log formatter with trace context for SigNoz log ingestion.
    Outputs logs in a structured JSON format that fluent-bit can parse.
    """
    
    def __init__(self, service_name: str = "unknown"):
        super().__init__()
        self.service_name = service_name
    
    def format(self, record):
        # Get trace context
        span = trace.get_current_span()
        trace_id = ""
        span_id = ""
        
        if span and span.get_span_context().is_valid:
            trace_id = format(span.get_span_context().trace_id, '032x')
            span_id = format(span.get_span_context().span_id, '016x')
        
        log_record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "message": record.getMessage(),
            "service": self.service_name,
            "trace_id": trace_id,
            "span_id": span_id,
            "logger": record.name,
        }

        # Pipeline-run correlation (only when bound, to avoid empty-field noise).
        pid = _pipeline_id_var.get()
        eid = _execution_id_var.get()
        if pid:
            log_record["pipeline_id"] = pid
        if eid:
            log_record["execution_id"] = eid

        # Add exception info if present
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in ('name', 'msg', 'args', 'created', 'filename', 'funcName',
                          'levelname', 'levelno', 'lineno', 'module', 'msecs',
                          'pathname', 'process', 'processName', 'relativeCreated',
                          'stack_info', 'exc_info', 'exc_text', 'thread', 'threadName',
                          'trace_id', 'span_id', 'message'):
                if isinstance(value, (str, int, float, bool, type(None))):
                    log_record[key] = value
        
        return json.dumps(log_record)


def setup_logging(service_name: str, log_level: str = None):
    """
    Setup logging with trace context injection for SigNoz correlation.
    
    Args:
        service_name: Name of the service (e.g., "tool-generator", "planner")
        log_level: Log level (DEBUG, INFO, WARNING, ERROR). Defaults to LOG_LEVEL env var or INFO.
    """
    # Determine log level
    level_str = log_level or os.getenv("LOG_LEVEL", "INFO")
    level = getattr(logging, level_str.upper(), logging.INFO)
    
    # Determine format (JSON for production/docker, text for development)
    use_json = os.getenv("LOG_FORMAT", "").lower() == "json" or os.getenv("ENVIRONMENT", "") in ("docker", "production")
    
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    
    # Add trace context filter
    handler.addFilter(TraceContextFilter())
    
    if use_json:
        handler.setFormatter(JSONFormatter(service_name))
    else:
        # Text format with trace IDs for development
        formatter = logging.Formatter(
            '%(asctime)s [%(name)s] %(levelname)s [trace_id=%(trace_id)s span_id=%(span_id)s]: %(message)s'
        )
        handler.setFormatter(formatter)
    
    root_logger.addHandler(handler)
    
    # Also configure uvicorn loggers to use same format
    for logger_name in ("uvicorn", "uvicorn.access", "uvicorn.error"):
        uvicorn_logger = logging.getLogger(logger_name)
        uvicorn_logger.handlers = []
        uvicorn_logger.addHandler(handler)
    
    logging.info(f"📝 Logging configured for {service_name} (format={'json' if use_json else 'text'}, level={level_str})")


logger = logging.getLogger(__name__)

# Global tracer
_tracer: Optional[trace.Tracer] = None
_propagator = TraceContextTextMapPropagator()


def init_tracer(service_name: str, service_version: str = "1.0.0") -> trace.Tracer:
    """
    Initialize OpenTelemetry tracer with OTLP exporter
    
    Args:
        service_name: Name of the service (e.g., "planner", "tool-generator")
        service_version: Version of the service
        
    Returns:
        Configured tracer instance
    """
    global _tracer
    
    # Get OTLP endpoint from environment
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
    
    # Create resource with service info
    resource = Resource.create({
        SERVICE_NAME: service_name,
        SERVICE_VERSION: service_version,
        "deployment.environment": os.getenv("ENVIRONMENT", "development"),
    })
    
    # Create trace provider
    provider = TracerProvider(resource=resource)
    
    # Create OTLP exporter
    try:
        exporter = OTLPSpanExporter(
            endpoint=endpoint,
            insecure=True,  # Use insecure for development
        )
        provider.add_span_processor(BatchSpanProcessor(exporter))
        logger.info(f"✅ OpenTelemetry initialized for service: {service_name} (endpoint: {endpoint})")
    except Exception as e:
        logger.warning(f"⚠️ Failed to connect to OTLP endpoint {endpoint}: {e}")
        # Continue without exporter for local development
    
    # Set global trace provider
    trace.set_tracer_provider(provider)
    
    # Set global propagator
    set_global_textmap(_propagator)
    
    # Get tracer
    _tracer = trace.get_tracer(service_name, service_version)
    
    return _tracer


def get_tracer() -> trace.Tracer:
    """Get the global tracer instance"""
    global _tracer
    if _tracer is None:
        return trace.get_tracer("default")
    return _tracer


def _install_otel_route_guard():
    """Make OpenTelemetry's route-name extraction tolerant of routes lacking ``.path``.

    ``opentelemetry-instrumentation-fastapi==0.46b0`` walks ``app.routes`` and reads
    ``route.path`` with no guard. FastAPI (>=0.119, here 0.137.x) places
    ``_IncludedRouter`` objects — which have no ``.path`` — into ``app.routes`` for
    every router added via ``include_router``. The unguarded access raises
    ``AttributeError: '_IncludedRouter' object has no attribute 'path'`` inside the
    ASGI middleware *before the handler runs*, 500-ing every ``include_router``
    endpoint (all of ``/api/v1/explorer/*``). Upstream OTel later added this same
    getattr guard; we backport it so we can stay on the pinned 0.46b0 line.
    """
    try:
        from opentelemetry.instrumentation import fastapi as _otel_fastapi
        from starlette.routing import Match

        def _safe_get_route_details(scope):
            route = None
            for starlette_route in scope["app"].routes:
                try:
                    match, _ = starlette_route.matches(scope)
                except Exception:
                    continue
                if match == Match.FULL:
                    route = getattr(starlette_route, "path", None)
                    break
                if match == Match.PARTIAL:
                    route = getattr(starlette_route, "path", None)
            return route

        # _get_default_span_details() resolves _get_route_details from the module
        # global at call time, so replacing the module attribute is sufficient.
        _otel_fastapi._get_route_details = _safe_get_route_details
    except Exception as e:  # never let telemetry hardening break startup
        logger.warning(f"⚠️ Could not install OTel route-detail guard: {e}")


def instrument_fastapi(app):
    """Instrument a FastAPI application with OpenTelemetry"""
    try:
        _install_otel_route_guard()
        FastAPIInstrumentor.instrument_app(app)
        logger.info("✅ FastAPI instrumented with OpenTelemetry")
    except Exception as e:
        logger.warning(f"⚠️ Failed to instrument FastAPI: {e}")


def instrument_requests():
    """Instrument the requests library with OpenTelemetry"""
    try:
        RequestsInstrumentor().instrument()
        logger.info("✅ Requests library instrumented with OpenTelemetry")
    except Exception as e:
        logger.warning(f"⚠️ Failed to instrument requests: {e}")


@contextmanager
def start_span(name: str, kind: SpanKind = SpanKind.INTERNAL, attributes: Dict[str, Any] = None):
    """
    Context manager for creating spans
    
    Usage:
        with start_span("process_request", attributes={"request_id": "123"}) as span:
            # do work
            span.set_attribute("result", "success")
    """
    tracer = get_tracer()
    with tracer.start_as_current_span(name, kind=kind) as span:
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, str(value) if value is not None else "")
        try:
            yield span
        except Exception as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            raise


def start_span_from_context(name: str, carrier: Dict[str, str], kind: SpanKind = SpanKind.SERVER):
    """
    Start a span from trace context in headers/carrier
    
    Args:
        name: Span name
        carrier: Dict containing trace context headers (traceparent, tracestate)
        kind: Span kind
        
    Returns:
        Tuple of (context, span)
    """
    tracer = get_tracer()
    propagator = get_global_textmap()
    
    # Extract context from carrier
    ctx = propagator.extract(carrier=carrier)
    
    # Start span as child of extracted context
    return tracer.start_span(name, context=ctx, kind=kind)


def inject_trace_context(carrier: Dict[str, str]):
    """
    Inject current trace context into a carrier dict
    
    Args:
        carrier: Dict to inject trace headers into
    """
    inject(carrier)


def extract_trace_context(carrier: Dict[str, str]):
    """
    Extract trace context from a carrier dict
    
    Args:
        carrier: Dict containing trace headers
        
    Returns:
        Context object
    """
    return extract(carrier=carrier)


def get_current_trace_id() -> Optional[str]:
    """Get the current trace ID as a 32-char hex string (OTel standard)"""
    span = trace.get_current_span()
    if span and span.get_span_context().is_valid:
        return format(span.get_span_context().trace_id, '032x')
    return None


def get_current_trace_id_uuid() -> Optional[str]:
    """
    Get the current trace ID formatted as a UUID string for PostgreSQL.
    Converts 32-char hex to UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    """
    trace_id = get_current_trace_id()
    if trace_id:
        return trace_id_to_uuid(trace_id)
    return None


def get_current_span_id() -> Optional[str]:
    """Get the current span ID as a string"""
    span = trace.get_current_span()
    if span and span.get_span_context().is_valid:
        return format(span.get_span_context().span_id, '016x')
    return None


# =============================================================================
# TRACE ID FORMAT UTILITIES (Ensures consistency across Go/Python/PostgreSQL)
# =============================================================================

def trace_id_to_uuid(trace_id: str) -> str:
    """
    Convert a 32-char hex trace_id to UUID format for PostgreSQL storage.
    
    OpenTelemetry generates 32-char hex: 0af7651916cd43dd8448eb211c80319c
    PostgreSQL UUID format: 0af76519-16cd-43dd-8448-eb211c80319c
    
    Args:
        trace_id: 32-character hex string
        
    Returns:
        UUID-formatted string
    """
    if not trace_id or len(trace_id) < 32:
        return trace_id
    
    # Remove any existing dashes
    clean_id = trace_id.replace("-", "")
    
    # Format as UUID: 8-4-4-4-12
    return f"{clean_id[:8]}-{clean_id[8:12]}-{clean_id[12:16]}-{clean_id[16:20]}-{clean_id[20:32]}"


def uuid_to_trace_id(uuid_str: str) -> str:
    """
    Convert a UUID string back to 32-char hex trace_id format.
    
    PostgreSQL UUID: 0af76519-16cd-43dd-8448-eb211c80319c
    OpenTelemetry format: 0af7651916cd43dd8448eb211c80319c
    
    Args:
        uuid_str: UUID-formatted string
        
    Returns:
        32-character hex string
    """
    if not uuid_str:
        return uuid_str
    return uuid_str.replace("-", "").lower()


def normalize_trace_id(trace_id: str, format: str = "hex") -> str:
    """
    Normalize a trace_id to the specified format.
    
    Args:
        trace_id: Trace ID in any format (hex or uuid)
        format: Target format - "hex" (32-char) or "uuid"
        
    Returns:
        Normalized trace ID string
    """
    if not trace_id:
        return trace_id
    
    # First, convert to canonical 32-char hex
    clean_id = trace_id.replace("-", "").lower()
    
    if format == "uuid":
        return trace_id_to_uuid(clean_id)
    else:
        return clean_id


def is_valid_trace_id(trace_id: str) -> bool:
    """
    Validate that a trace_id is properly formatted.
    
    Args:
        trace_id: Trace ID string to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not trace_id:
        return False
    
    clean_id = trace_id.replace("-", "").lower()
    # Reject the all-zero trace id (represents "invalid" in many OTel SDKs).
    if clean_id == "0" * 32:
        return False
    
    # Must be exactly 32 hex characters
    if len(clean_id) != 32:
        return False
    
    try:
        int(clean_id, 16)
        return True
    except ValueError:
        return False


def add_span_attributes(**attributes):
    """Add attributes to the current span"""
    span = trace.get_current_span()
    if span and span.is_recording():
        for key, value in attributes.items():
            span.set_attribute(key, str(value) if value is not None else "")


def record_exception(exception: Exception, attributes: Dict[str, Any] = None):
    """Record an exception on the current span"""
    span = trace.get_current_span()
    if span and span.is_recording():
        span.record_exception(exception, attributes=attributes)
        span.set_status(Status(StatusCode.ERROR, str(exception)))


def set_span_status(success: bool, description: str = ""):
    """Set the status of the current span"""
    span = trace.get_current_span()
    if span and span.is_recording():
        if success:
            span.set_status(Status(StatusCode.OK, description))
        else:
            span.set_status(Status(StatusCode.ERROR, description))


class TracedOperation:
    """
    Decorator for tracing function calls
    
    Usage:
        @TracedOperation("process_data")
        def process_data(data):
            return transformed_data
    """
    
    def __init__(self, name: str, kind: SpanKind = SpanKind.INTERNAL):
        self.name = name
        self.kind = kind
    
    def __call__(self, func):
        def wrapper(*args, **kwargs):
            with start_span(self.name, kind=self.kind) as span:
                span.set_attribute("function", func.__name__)
                try:
                    result = func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise
        return wrapper

