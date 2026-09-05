#!/usr/bin/env python3
"""
Telemetry Agent Service
Aggregates pipeline metrics and provides telemetry endpoints
"""

import os
import sys
import json
import logging
import time
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from collections import defaultdict
from threading import Lock

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

# Add parent directory to path to import utils
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))
from src.utils.telemetry import (
    init_tracer, instrument_fastapi, start_span,
    get_current_trace_id, add_span_attributes, setup_logging
)
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.trace import SpanKind

# Setup trace-aware logging FIRST
SERVICE_NAME_VALUE = os.getenv("OTEL_SERVICE_NAME", "telemetry-agent")
setup_logging(SERVICE_NAME_VALUE)
logger = logging.getLogger("telemetry-agent")

# Initialize OpenTelemetry tracer
tracer = init_tracer("telemetry-agent", "1.0.0")

# Initialize OpenTelemetry metrics
def init_metrics():
    """Initialize OpenTelemetry metrics with OTLP exporter"""
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
    
    resource = Resource.create({SERVICE_NAME: "telemetry-agent"})
    
    try:
        exporter = OTLPMetricExporter(endpoint=endpoint, insecure=True)
        reader = PeriodicExportingMetricReader(exporter, export_interval_millis=10000)
        provider = MeterProvider(resource=resource, metric_readers=[reader])
        metrics.set_meter_provider(provider)
        logger.info(f"✅ Metrics initialized (endpoint: {endpoint})")
    except Exception as e:
        logger.warning(f"⚠️ Failed to initialize metrics: {e}")

init_metrics()
meter = metrics.get_meter("telemetry-agent")

# Create metrics
pipeline_duration = meter.create_histogram(
    name="pipeline_duration_seconds",
    description="Pipeline execution duration in seconds",
    unit="s"
)

pipeline_status_counter = meter.create_counter(
    name="pipeline_status_total",
    description="Total pipeline executions by status"
)

agent_duration = meter.create_histogram(
    name="agent_duration_seconds",
    description="Per-agent processing duration in seconds",
    unit="s"
)

mcp_call_duration = meter.create_histogram(
    name="mcp_call_duration_seconds",
    description="MCP connector call duration in seconds",
    unit="s"
)

llm_tokens = meter.create_counter(
    name="llm_tokens_total",
    description="Total LLM tokens used"
)

app = FastAPI(
    title="Telemetry Agent Service",
    description="Pipeline metrics aggregation and telemetry",
    version="1.0.0"
)

# Instrument FastAPI
instrument_fastapi(app)

# In-memory metrics storage (for querying)
class MetricsStore:
    """In-memory store for pipeline metrics"""
    
    def __init__(self, retention_minutes: int = 60):
        self.retention = timedelta(minutes=retention_minutes)
        self.pipelines: Dict[str, Dict[str, Any]] = {}
        self.agent_metrics: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.lock = Lock()
    
    def record_pipeline_start(self, pipeline_id: str, trace_id: str):
        """Record pipeline start"""
        with self.lock:
            self.pipelines[pipeline_id] = {
                "pipeline_id": pipeline_id,
                "trace_id": trace_id,
                "status": "running",
                "started_at": datetime.utcnow().isoformat(),
                "steps": [],
                "errors": []
            }
    
    def record_pipeline_step(self, pipeline_id: str, step: Dict[str, Any]):
        """Record a pipeline step execution"""
        with self.lock:
            if pipeline_id in self.pipelines:
                self.pipelines[pipeline_id]["steps"].append(step)
    
    def record_pipeline_complete(self, pipeline_id: str, success: bool, error: str = None):
        """Record pipeline completion"""
        with self.lock:
            if pipeline_id in self.pipelines:
                p = self.pipelines[pipeline_id]
                p["status"] = "completed" if success else "failed"
                p["completed_at"] = datetime.utcnow().isoformat()
                if error:
                    p["errors"].append(error)
                
                # Calculate duration
                started = datetime.fromisoformat(p["started_at"])
                duration = (datetime.utcnow() - started).total_seconds()
                p["duration_seconds"] = duration
                
                # Record metrics
                pipeline_duration.record(duration, {"pipeline_id": pipeline_id})
                pipeline_status_counter.add(1, {"status": p["status"]})
    
    def record_agent_execution(self, agent: str, pipeline_id: str, duration: float, success: bool):
        """Record agent execution metrics"""
        with self.lock:
            self.agent_metrics[agent].append({
                "pipeline_id": pipeline_id,
                "duration": duration,
                "success": success,
                "timestamp": datetime.utcnow().isoformat()
            })
            agent_duration.record(duration, {"agent": agent, "success": str(success)})
    
    def record_mcp_call(self, connector: str, operation: str, duration: float, success: bool):
        """Record MCP connector call metrics"""
        mcp_call_duration.record(duration, {
            "connector": connector,
            "operation": operation,
            "success": str(success)
        })
    
    def record_llm_tokens(self, tokens: int, prompt_name: str):
        """Record LLM token usage"""
        llm_tokens.add(tokens, {"prompt": prompt_name})
    
    def get_pipeline(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """Get pipeline metrics by ID"""
        with self.lock:
            return self.pipelines.get(pipeline_id)
    
    def get_all_pipelines(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent pipelines"""
        with self.lock:
            pipelines = list(self.pipelines.values())
            return sorted(pipelines, key=lambda p: p.get("started_at", ""), reverse=True)[:limit]
    
    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary"""
        with self.lock:
            total = len(self.pipelines)
            completed = sum(1 for p in self.pipelines.values() if p["status"] == "completed")
            failed = sum(1 for p in self.pipelines.values() if p["status"] == "failed")
            running = sum(1 for p in self.pipelines.values() if p["status"] == "running")
            
            durations = [p.get("duration_seconds", 0) for p in self.pipelines.values() if "duration_seconds" in p]
            avg_duration = sum(durations) / len(durations) if durations else 0
            
            return {
                "total_pipelines": total,
                "completed": completed,
                "failed": failed,
                "running": running,
                "success_rate": (completed / total * 100) if total > 0 else 0,
                "avg_duration_seconds": avg_duration
            }
    
    def cleanup_old(self):
        """Remove old metrics beyond retention period"""
        with self.lock:
            cutoff = datetime.utcnow() - self.retention
            to_remove = []
            for pid, p in self.pipelines.items():
                try:
                    completed = datetime.fromisoformat(p.get("completed_at", p["started_at"]))
                    if completed < cutoff:
                        to_remove.append(pid)
                except (KeyError, ValueError, TypeError):
                    # Ignore malformed timestamps (best-effort cleanup path).
                    continue
            for pid in to_remove:
                del self.pipelines[pid]

# Global metrics store
metrics_store = MetricsStore()

# Request/Response Models
class PipelineStartRequest(BaseModel):
    pipeline_id: str
    trace_id: str

class PipelineStepRequest(BaseModel):
    pipeline_id: str
    step: Dict[str, Any]

class PipelineCompleteRequest(BaseModel):
    pipeline_id: str
    success: bool
    error: Optional[str] = None

class AgentMetricRequest(BaseModel):
    agent: str
    pipeline_id: str
    duration: float
    success: bool

class MCPMetricRequest(BaseModel):
    connector: str
    operation: str
    duration: float
    success: bool

class LLMTokenRequest(BaseModel):
    tokens: int
    prompt_name: str

# Endpoints

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "telemetry-agent",
        "trace_id": get_current_trace_id()
    }

@app.get("/metrics/summary")
async def get_metrics_summary():
    """Get overall metrics summary"""
    with start_span("get_metrics_summary"):
        return metrics_store.get_summary()

@app.get("/metrics/pipelines")
async def get_pipelines(limit: int = 100):
    """Get recent pipeline metrics"""
    with start_span("get_pipelines", attributes={"limit": limit}):
        pipelines = metrics_store.get_all_pipelines(limit)
        return {"pipelines": pipelines, "count": len(pipelines)}

@app.get("/metrics/pipelines/{pipeline_id}")
async def get_pipeline(pipeline_id: str):
    """Get metrics for a specific pipeline"""
    with start_span("get_pipeline", attributes={"pipeline_id": pipeline_id}):
        pipeline = metrics_store.get_pipeline(pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        return pipeline

@app.post("/metrics/pipeline/start")
async def record_pipeline_start(request: PipelineStartRequest):
    """Record pipeline start event"""
    with start_span("record_pipeline_start", attributes={"pipeline_id": request.pipeline_id}):
        metrics_store.record_pipeline_start(request.pipeline_id, request.trace_id)
        return {"status": "recorded"}

@app.post("/metrics/pipeline/step")
async def record_pipeline_step(request: PipelineStepRequest):
    """Record pipeline step execution"""
    with start_span("record_pipeline_step", attributes={"pipeline_id": request.pipeline_id}):
        metrics_store.record_pipeline_step(request.pipeline_id, request.step)
        return {"status": "recorded"}

@app.post("/metrics/pipeline/complete")
async def record_pipeline_complete(request: PipelineCompleteRequest):
    """Record pipeline completion"""
    with start_span("record_pipeline_complete", attributes={"pipeline_id": request.pipeline_id, "success": request.success}):
        metrics_store.record_pipeline_complete(request.pipeline_id, request.success, request.error)
        return {"status": "recorded"}

@app.post("/metrics/agent")
async def record_agent_metric(request: AgentMetricRequest):
    """Record agent execution metric"""
    with start_span("record_agent_metric", attributes={"agent": request.agent}):
        metrics_store.record_agent_execution(
            request.agent,
            request.pipeline_id,
            request.duration,
            request.success
        )
        return {"status": "recorded"}

@app.post("/metrics/mcp")
async def record_mcp_metric(request: MCPMetricRequest):
    """Record MCP connector call metric"""
    with start_span("record_mcp_metric", attributes={"connector": request.connector}):
        metrics_store.record_mcp_call(
            request.connector,
            request.operation,
            request.duration,
            request.success
        )
        return {"status": "recorded"}

@app.post("/metrics/llm")
async def record_llm_tokens(request: LLMTokenRequest):
    """Record LLM token usage"""
    with start_span("record_llm_tokens", attributes={"tokens": request.tokens}):
        metrics_store.record_llm_tokens(request.tokens, request.prompt_name)
        return {"status": "recorded"}

@app.get("/metrics/trace/{trace_id}")
async def get_metrics_by_trace(trace_id: str):
    """Get all metrics associated with a trace ID"""
    with start_span("get_metrics_by_trace", attributes={"trace_id": trace_id}):
        # Find pipelines with matching trace_id
        pipelines = [
            p for p in metrics_store.get_all_pipelines(1000)
            if p.get("trace_id") == trace_id
        ]
        return {"trace_id": trace_id, "pipelines": pipelines}

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5012"))
    logger.info(f"🚀 Starting Telemetry Agent on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)

