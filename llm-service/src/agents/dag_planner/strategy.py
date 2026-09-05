"""
DAG Planning Strategy
=====================

Strategy class that integrates the LangGraph-based DAG planner
with the existing planning infrastructure in strategies.py.

This ensures backward compatibility while enabling DAG workflows.
"""

import logging
import time
from typing import Dict, Any, Optional

from .agent import DAGPlannerAgent
from src.agents.planner.strategies import PlanningStrategy, PlanningContext, PlanResult

logger = logging.getLogger("dag_planner.strategy")


class DAGPlanningStrategy(PlanningStrategy):
    """
    LangGraph-based DAG planning strategy.
    
    Produces a workflow_graph in the plan output for DAG execution.
    Maintains backward compatibility by also including steps: [].
    
    This strategy is routed when:
    1. User explicitly requests a "workflow" or "DAG"
    2. NL implies branching/parallel/multi-actions
    3. Planner detects complex non-linear requirements
    
    Example output:
        {
            "pipeline_id": "pipe-123",
            "steps": [],  # Empty for backward compatibility
            "workflow_graph": {
                "graph_id": "...",
                "nodes": [...],
                "edges": [...],
            },
            "is_dag": True,
        }
    """
    
    def __init__(self, llm_client, tool_registry=None):
        """
        Initialize the DAG planning strategy.
        
        Args:
            llm_client: LLMClient instance for LLM calls
            tool_registry: Optional ToolRegistry for connector lookups
        """
        self.llm_client = llm_client
        self.tool_registry = tool_registry
        self.agent = DAGPlannerAgent(llm_client, tool_registry)
        
    @property
    def name(self) -> str:
        return "dag_planner"
    
    def can_handle(self, context: PlanningContext) -> bool:
        """
        Check if this strategy should handle the request.

        Returns True if the request suggests a DAG/workflow:
        - Explicit workflow/DAG keywords
        - Branching keywords (if/then/else, branch, parallel)
        - Multiple distinct actions (and then, after that, finally)
        - Notification/alert patterns after data actions
        """
        nl = context.natural_language.lower()

        # Guard: any pipeline whose destination is a relational/warehouse store
        # is a plain extract→load batch — no DAG semantics needed. Route to the
        # heuristic strategy which is the known-good path for Shopify→Postgres,
        # Shopify→MySQL, Postgres→MySQL, etc. Without this guard, NL containing
        # "(GraphQL)" matches the "graph" substring in dag_keywords below and
        # falsely routes here, then the DAG→steps converter emits 0 steps and
        # Temporal rejects with [DETERMINISTIC:INTENT_FAILED].
        _SIMPLE_LOAD_DESTS = {
            "postgresql", "postgres", "mysql", "mariadb", "sqlite",
            "clickhouse", "snowflake", "bigquery", "redshift",
        }
        try:
            dst = context.destination_connection or {}
            dst_tool = (dst.get("connector_type") if isinstance(dst, dict) else "") or ""
            dst_tool = dst_tool.strip().lower().replace("_", "-")
        except Exception:
            dst_tool = ""
        if dst_tool in _SIMPLE_LOAD_DESTS:
            return False
        # Fallback signal: planner's NL preprocessor populates detected_tools
        # regardless of whether the chat path passed connection IDs. If any
        # detected tool is a simple-load destination, route to heuristic too.
        try:
            detected = [str(t).strip().lower().replace("_", "-")
                        for t in (getattr(context, "detected_tools", None) or [])]
        except Exception:
            detected = []
        if any(t in _SIMPLE_LOAD_DESTS for t in detected):
            return False

        # Explicit DAG/workflow keywords
        dag_keywords = [
            "workflow", "dag", "graph", "pipeline flow",
            "flow", "orchestrat", "automat",
        ]
        if any(kw in nl for kw in dag_keywords):
            return True
        
        # Branching patterns
        branch_patterns = [
            "if ", "then ", "else ", "branch", "parallel",
            "condition", "when ", "based on",
        ]
        if any(pat in nl for pat in branch_patterns):
            return True
        
        # Multi-step patterns (more than 2 distinct actions)
        multi_step_patterns = [
            "and then", "after that", "finally", "next",
            "first ", "second ", "third ",
            ", then ", "; then ",
        ]
        step_count = sum(1 for pat in multi_step_patterns if pat in nl)
        if step_count >= 2:
            return True
        
        # Notification after action pattern
        if any(n in nl for n in ["slack", "email", "notify", "alert"]):
            if any(a in nl for a in ["sync", "transfer", "load", "export", "import"]):
                return True
        
        return False
    
    def create_plan(self, context: PlanningContext) -> PlanResult:
        """
        Create a DAG-based pipeline plan.
        
        Args:
            context: PlanningContext with user request and resources
            
        Returns:
            PlanResult-compatible dict with workflow_graph
        """
        logger.info(f"🔀 DAG Planning: {context.natural_language[:50]}...")
        
        # Run the DAG planner agent
        src_tool = ""
        dst_tool = ""
        try:
            if context.source_connection and isinstance(context.source_connection, dict):
                src_tool = (context.source_connection.get("connector_type") or "").strip().lower().replace("_", "-")
            if context.destination_connection and isinstance(context.destination_connection, dict):
                dst_tool = (context.destination_connection.get("connector_type") or "").strip().lower().replace("_", "-")
        except Exception:
            src_tool = ""
            dst_tool = ""

        result = self.agent.plan(
            user_request=context.natural_language,
            pipeline_id=context.pipeline_id,
            context={
                "sync_mode": context.sync_mode,
                "cdc_mode": context.cdc_mode,
                # Prefer explicit connection-derived connector types to avoid ambiguous
                # heuristic fallbacks like "storage".
                "source_connector_type": src_tool,
                "destination_connector_type": dst_tool,
                "detected_tools": list(getattr(context, "detected_tools", []) or []),
            } if hasattr(context, "sync_mode") else {},
            available_connections=context.available_connections if hasattr(context, "available_connections") else [],
            available_tools=context.available_tools if hasattr(context, "available_tools") else [],
        )
        
        if not result.get("success"):
            logger.error(f"❌ DAG planning failed: {result.get('error')}")
            return self._create_plan_result(
                success=False,
                pipeline_id=context.pipeline_id or "",
                error=result.get("error", "DAG planning failed"),
            )
        
        workflow_graph = result.get("workflow_graph")
        
        # Build the plan with workflow_graph
        plan = {
            "pipeline_id": context.pipeline_id or f"dag-{int(time.time())}",
            "steps": [],  # Empty for backward compatibility
            "workflow_graph": workflow_graph,
            "is_dag": True,
            "is_streaming": False,  # DAG workflows are typically not streaming
            "sync_mode": "dag",
            "planning_metadata": {
                "strategy": self.name,
                "planning_steps": result.get("planning_steps", []),
                "warnings": result.get("validation_warnings", []),
            },
        }
        
        logger.info(f"✅ DAG plan created: {len(workflow_graph.get('nodes', []))} nodes")
        
        return self._create_plan_result(
            success=True,
            pipeline_id=plan["pipeline_id"],
            plan=plan,
        )
    
    def _create_plan_result(
        self,
        success: bool,
        pipeline_id: str,
        plan: Optional[Dict] = None,
        error: Optional[str] = None,
    ) -> PlanResult:
        """Create a PlanResult instance"""
        return PlanResult(
            success=success,
            pipeline_id=pipeline_id,
            plan=plan,
            error=error,
            generated_connectors=None,
            strategy_used=self.name,
        )


def should_use_dag_strategy(context: PlanningContext) -> bool:
    """
    Heuristic to determine if DAG planning should be used.
    
    This can be called by CompositePlanningStrategy to route
    complex requests to the DAG planner.
    
    Args:
        context: PlanningContext
        
    Returns:
        True if DAG planning is recommended
    """
    nl = (context.natural_language or "").lower()
    
    # Quick heuristics for DAG-worthy requests
    
    # 1. Explicit workflow keywords
    if any(kw in nl for kw in ["workflow", "dag", "orchestrat", "automat"]):
        return True
    
    # 2. Branching/parallel patterns
    if any(pat in nl for pat in ["if ", "branch", "parallel", "condition"]):
        return True
    
    # 3. Multiple distinct data destinations
    dest_keywords = ["s3", "snowflake", "bigquery", "kafka", "slack", "email"]
    dest_count = sum(1 for d in dest_keywords if d in nl)
    if dest_count >= 2:
        return True
    
    # 4. Complex multi-step patterns
    if nl.count(" then ") >= 2 or nl.count(" and ") >= 3:
        return True
    
    return False
