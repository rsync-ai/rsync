"""
DAG Planner Agent
=================

Agent for generating workflow DAGs from natural language.

IMPORTANT: This agent uses rsync-ai's existing LLM gateway (LLMClient) for all
LLM calls. We do NOT use langchain_anthropic/ChatAnthropic directly.

The agent implements a planning loop:
1. Think: Analyze the user request and identify required nodes/edges
2. Tool Calls: Query available connections, check connectors, get schemas
3. Validate: Ensure the generated DAG is valid (no cycles, valid edges)
4. Refine: If validation fails, refine the plan based on errors

Note: This implementation uses a simple state machine instead of LangGraph
to avoid dependency version conflicts. The logic is equivalent.
"""

import logging
import json
import uuid
import re
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

from .types import (
    DAGPlannerState,
    PlanningStep,
    WorkflowGraphOutput,
    NodeSpec,
    EdgeSpec,
    NodeKind,
    EdgeType,
    DAG_PLANNER_TOOLS,
)

logger = logging.getLogger("dag_planner.agent")


@dataclass
class PlannerState:
    """State for the DAG planning process"""
    user_request: str
    pipeline_id: str
    context: Dict[str, Any] = field(default_factory=dict)
    available_connections: List[Dict[str, Any]] = field(default_factory=list)
    available_tools: List[str] = field(default_factory=list)
    planning_steps: List[Dict[str, Any]] = field(default_factory=list)
    current_nodes: List[Dict[str, Any]] = field(default_factory=list)
    current_edges: List[Dict[str, Any]] = field(default_factory=list)
    validation_errors: List[str] = field(default_factory=list)
    validation_warnings: List[str] = field(default_factory=list)
    iteration: int = 0
    max_iterations: int = 3
    final_graph: Optional[Dict[str, Any]] = None
    success: bool = False
    error: Optional[str] = None


class DAGPlannerAgent:
    """
    DAG planning agent using a simple state machine.
    
    Uses the existing LLMClient for all LLM calls to ensure:
    - Centralized model/provider control
    - Telemetry and cost tracking
    - Prompt registry usage
    
    Example:
        from src.utils.llm_client import LLMClient
        
        llm_client = LLMClient()
        agent = DAGPlannerAgent(llm_client)
        result = agent.plan(
            "sync mysql to s3, then send slack notification",
            pipeline_id="pipe-123"
        )
    """
    
    def __init__(self, llm_client, tool_registry=None):
        """
        Initialize the DAG planner agent.
        
        Args:
            llm_client: LLMClient instance for making LLM calls
            tool_registry: Optional ToolRegistry for connector lookups
        """
        self.llm_client = llm_client
        self.tool_registry = tool_registry
    
    def plan(
        self,
        user_request: str,
        pipeline_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        available_connections: Optional[List[Dict[str, Any]]] = None,
        available_tools: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Generate a workflow DAG from a natural language request.
        
        Args:
            user_request: Natural language description of the workflow
            pipeline_id: Optional pipeline ID (generated if not provided)
            context: Additional context (sync_mode, connection IDs, etc.)
            available_connections: List of available connections
            available_tools: List of available tool/connector types
            
        Returns:
            Dict with:
            - success: bool
            - workflow_graph: Dict (if success)
            - error: str (if failure)
            - planning_steps: List of planning steps taken
        """
        pipeline_id = pipeline_id or f"dag-{uuid.uuid4().hex[:8]}"
        
        # Initialize state
        state = PlannerState(
            user_request=user_request,
            pipeline_id=pipeline_id,
            context=context or {},
            available_connections=available_connections or [],
            available_tools=available_tools or [],
        )
        
        try:
            # Run the planning state machine
            state = self._run_planning_loop(state)
            
            return {
                "success": state.success,
                "workflow_graph": state.final_graph,
                "error": state.error,
                "planning_steps": state.planning_steps,
                "validation_warnings": state.validation_warnings,
            }
        except Exception as e:
            logger.exception(f"DAG planning failed: {e}")
            return {
                "success": False,
                "workflow_graph": None,
                "error": str(e),
                "planning_steps": [],
            }
    
    def _run_planning_loop(self, state: PlannerState) -> PlannerState:
        """Run the planning state machine"""
        # Step 1: Analyze
        state = self._analyze_request(state)
        
        # Step 2-4: Generate -> Validate -> Refine loop
        while state.iteration < state.max_iterations:
            # Generate
            state = self._generate_dag(state)
            
            # Validate
            state = self._validate_dag(state)
            
            # Check if valid
            if not state.validation_errors:
                break
            
            # Refine
            state = self._refine_dag(state)
        
        # Step 5: Finalize
        state = self._finalize(state)
        
        return state
    
    # ==========================================================================
    # Planning Steps
    # ==========================================================================
    
    def _analyze_request(self, state: PlannerState) -> PlannerState:
        """Analyze the user request to understand intent and requirements"""
        logger.info(f"📊 Analyzing request: {state.user_request[:50]}...")
        
        # Use LLM to analyze the request
        try:
            response = self.llm_client.complete(
                "planner/dag_planning",  # Uses prompt registry
                {
                    "user_request": state.user_request,
                    "available_connections": json.dumps(state.available_connections),
                    "available_tools": json.dumps(state.available_tools),
                    "phase": "analyze",
                }
            )
            
            analysis = response.get("content", "")
            
            state.planning_steps.append({
                "step_type": "think",
                "content": f"Analysis: {analysis[:500]}...",
            })
            
            logger.info(f"✅ Analysis complete")
            
        except Exception as e:
            logger.warning(f"Analysis failed, continuing with defaults: {e}")
            state.planning_steps.append({
                "step_type": "think",
                "content": f"Analysis skipped due to error: {e}",
                "error": str(e),
            })
        
        return state
    
    def _generate_dag(self, state: PlannerState) -> PlannerState:
        """Generate the DAG structure from the analyzed request"""
        logger.info(f"🔧 Generating DAG (iteration {state.iteration + 1})...")
        
        state.iteration += 1
        
        # Build context for LLM
        llm_context = {
            "user_request": state.user_request,
            "available_connections": json.dumps(state.available_connections),
            "available_tools": json.dumps(state.available_tools),
            "phase": "generate",
            "current_nodes": json.dumps(state.current_nodes),
            "current_edges": json.dumps(state.current_edges),
            "validation_errors": json.dumps(state.validation_errors),
            "iteration": state.iteration,
        }
        
        try:
            response = self.llm_client.complete(
                "planner/dag_planning",
                llm_context
            )
            
            content = response.get("content", "")
            
            # Parse the generated DAG from the response
            dag_json = self._extract_json_from_response(content)
            
            if dag_json:
                nodes = dag_json.get("nodes", [])
                edges = dag_json.get("edges", [])
                
                # Guard: if model returned an empty graph, treat as invalid and fall back
                if not nodes:
                    logger.warning("LLM returned empty DAG (0 nodes); using simple fallback DAG")
                    state = self._generate_simple_dag(state)
                    return state

                state.current_nodes = nodes
                state.current_edges = edges
                
                state.planning_steps.append({
                    "step_type": "generate",
                    "content": f"Generated {len(nodes)} nodes, {len(edges)} edges",
                })
                
                logger.info(f"✅ Generated DAG: {len(nodes)} nodes, {len(edges)} edges")
            else:
                # Fallback: generate a simple linear DAG
                state = self._generate_simple_dag(state)
                
        except Exception as e:
            logger.error(f"DAG generation failed: {e}")
            state.planning_steps.append({
                "step_type": "generate",
                "content": f"Generation failed: {e}",
                "error": str(e),
            })
            # Try simple fallback
            state = self._generate_simple_dag(state)
        
        return state
    
    def _generate_simple_dag(self, state: PlannerState) -> PlannerState:
        """Generate a simple linear DAG as fallback"""
        logger.info("🔄 Generating simple linear DAG as fallback")
        
        user_request = state.user_request.lower()
        nodes = []
        edges = []
        
        # Prefer explicit connector types from context when available (connection-derived),
        # so we don't produce ambiguous placeholders like "storage".
        source_type = ""
        dest_type = ""
        if state.context:
            source_type = str(state.context.get("source_connector_type") or "").strip().lower().replace("_", "-")
            dest_type = str(state.context.get("destination_connector_type") or "").strip().lower().replace("_", "-")

        # If we still don't have connector types, consult detected tools (if the outer planner
        # extracted them from NL or context). This is more reliable than substring matching.
        if state.context and (not source_type or not dest_type):
            detected = state.context.get("detected_tools")
            if isinstance(detected, list):
                tools = [str(t).strip().lower().replace("_", "-") for t in detected if str(t).strip()]
                if not source_type:
                    for cand in ["mysql", "postgresql", "postgres", "mongodb", "sqlite", "oracle", "sqlserver", "mariadb"]:
                        if cand in tools:
                            source_type = "postgresql" if cand == "postgres" else cand
                            break
                if not dest_type:
                    for cand in ["aws-s3", "postgresql", "postgres", "snowflake", "bigquery", "kafka", "gcs", "redshift", "minio"]:
                        if cand in tools:
                            dest_type = "aws-s3" if cand == "s3" else ("postgresql" if cand == "postgres" else cand)
                            break

        # Fallback: detect from request text
        if not source_type:
            source_type = self._detect_source_type(user_request)
        if not dest_type:
            dest_type = self._detect_destination_type(user_request)
        
        # Create source node
        source_node = {
            "id": "source_1",
            "kind": "source",
            "display_name": f"Extract from {source_type}",
            "config": {"connector_type": source_type},
        }
        nodes.append(source_node)
        
        # Check for transforms
        if any(kw in user_request for kw in ["transform", "filter", "clean", "map"]):
            # Emit structured transform configs that the executor already understands.
            # This keeps "transform" as a lightweight config-producing node (no custom code).
            transforms_list: List[Dict[str, Any]] = []

            # Heuristic: "status = X" / "status is X" / "status equals X"
            m = re.search(r"\bstatus\s*(=|is|equals)\s*['\"]?([a-z0-9_\-]+)['\"]?\b", user_request)
            if m:
                status_val = m.group(2)
                transforms_list.append({
                    "type": "filter",
                    "config": {
                        "condition": f"status = '{status_val}'",
                    },
                    "summary": f"Filter status='{status_val}'",
                })
            else:
                # Generic heuristic: capture a simple WHERE clause.
                # Examples:
                # - "where d > 5 then ..."
                # - "filter d > 5 and ..."
                m_where = re.search(r"\bwhere\s+(.+?)(?:\bthen\b|\binto\b|\bto\b|\band\b|;|,|$)", user_request)
                m_filter = re.search(r"\bfilter\s+(.+?)(?:\bthen\b|\binto\b|\bto\b|\band\b|;|,|$)", user_request)
                clause = (m_where.group(1) if m_where else (m_filter.group(1) if m_filter else "")).strip()
                if clause:
                    transforms_list.append({
                        "type": "filter",
                        "config": {
                            "condition": clause,
                        },
                        "summary": f"Filter where {clause}",
                    })

            transform_node = {
                "id": "transform_1",
                "kind": "transform",
                "display_name": "Transform Data",
                "config": {"transforms": transforms_list} if transforms_list else {},
            }
            nodes.append(transform_node)
            edges.append({"from": "source_1", "to": "transform_1", "type": "data"})
            last_node = "transform_1"
        else:
            last_node = "source_1"
        
        # Create destination node
        dest_node = {
            "id": "dest_1",
            "kind": "destination",
            "display_name": f"Load to {dest_type}",
            "config": {"connector_type": dest_type},
        }
        nodes.append(dest_node)
        edges.append({"from": last_node, "to": "dest_1", "type": "data"})
        
        # Check for notifications
        if any(kw in user_request for kw in ["slack", "email", "notify", "alert"]):
            notify_type = "slack" if "slack" in user_request else "email"
            notify_node = {
                "id": "notify_1",
                "kind": "notification",
                "display_name": f"Send {notify_type.capitalize()} Notification",
                "config": {"channel": notify_type},
            }
            nodes.append(notify_node)
            edges.append({"from": "dest_1", "to": "notify_1", "type": "data"})
        
        state.current_nodes = nodes
        state.current_edges = edges
        
        state.planning_steps.append({
            "step_type": "generate",
            "content": f"Generated simple DAG: {len(nodes)} nodes",
        })
        
        return state
    
    def _detect_source_type(self, text: str) -> str:
        """Detect source type from text"""
        sources = ["mysql", "postgresql", "postgres", "mongodb", "sqlite", "oracle", "sqlserver"]
        for src in sources:
            if src in text:
                return src
        return "database"
    
    def _detect_destination_type(self, text: str) -> str:
        """Detect destination type from text"""
        # Prefer canonical connector ids used across rsync-ai (kebab-case).
        # IMPORTANT: We intentionally map "s3" → "aws-s3" (we do not ship a "s3" connector).
        dests = [
            "aws-s3",
            "s3",
            "postgresql",
            "postgres",
            "snowflake",
            "bigquery",
            "kafka",
            "gcs",
            "redshift",
            "minio",
        ]
        for dest in dests:
            if dest in text:
                if dest == "s3" or dest == "aws-s3":
                    return "aws-s3"
                if dest == "postgres":
                    return "postgresql"
                return dest
        return "storage"
    
    def _execute_tools(self, state: PlannerState) -> PlannerState:
        """Execute tool calls to gather more information"""
        logger.info("🔧 Executing tool calls...")
        
        # For now, we implement basic tool functionality inline
        # In production, these would call actual APIs
        
        tool_results = []
        
        # List connections tool
        if state.available_connections:
            tool_results.append({
                "tool": "list_connections",
                "result": state.available_connections,
            })
        
        state.planning_steps.append({
            "step_type": "tool_call",
            "content": f"Executed {len(tool_results)} tool calls",
        })
        
        return state
    
    def _validate_dag(self, state: PlannerState) -> PlannerState:
        """Validate the generated DAG"""
        logger.info("✅ Validating DAG...")
        
        errors = []
        warnings = []
        
        nodes = state.current_nodes
        edges = state.current_edges
        
        # Check for empty DAG
        if not nodes:
            errors.append("DAG has no nodes")
            state.validation_errors = errors
            return state
        
        # Check unique node IDs
        node_ids = set()
        for node in nodes:
            node_id = node.get("id")
            if not node_id:
                errors.append("Node missing 'id' field")
            elif node_id in node_ids:
                errors.append(f"Duplicate node ID: {node_id}")
            else:
                node_ids.add(node_id)
        
        # Check edges reference valid nodes
        for edge in edges:
            from_node = edge.get("from")
            to_node = edge.get("to")
            if from_node not in node_ids:
                errors.append(f"Edge references unknown source: {from_node}")
            if to_node not in node_ids:
                errors.append(f"Edge references unknown target: {to_node}")
            if from_node == to_node:
                errors.append(f"Self-loop detected: {from_node}")
        
        # Check for cycles (simple DFS)
        if not errors:
            has_cycle = self._detect_cycle(nodes, edges)
            if has_cycle:
                errors.append("Graph contains a cycle (not a valid DAG)")
        
        # Check condition nodes have proper edges
        for node in nodes:
            if node.get("kind") == "condition":
                then_count = sum(1 for e in edges if e.get("from") == node.get("id") and e.get("type") == "control_then")
                if then_count != 1:
                    errors.append(f"Condition node {node.get('id')} must have exactly one 'control_then' edge")
        
        # Warnings
        root_nodes = self._get_root_nodes(nodes, edges)
        if len(root_nodes) > 1:
            warnings.append(f"Multiple root nodes detected: {root_nodes}")
        
        state.validation_errors = errors
        state.validation_warnings = warnings
        
        state.planning_steps.append({
            "step_type": "validate",
            "content": f"Validation: {len(errors)} errors, {len(warnings)} warnings",
        })
        
        if errors:
            logger.warning(f"❌ Validation failed: {errors}")
        else:
            logger.info("✅ Validation passed")
        
        return state
    
    def _detect_cycle(self, nodes: List[Dict], edges: List[Dict]) -> bool:
        """Detect cycles using DFS"""
        adj = {}
        for node in nodes:
            adj[node.get("id")] = []
        for edge in edges:
            adj[edge.get("from")].append(edge.get("to"))
        
        visited = {}  # 0: unvisited, 1: in-progress, 2: done
        
        def dfs(node_id):
            if visited.get(node_id) == 1:
                return True
            if visited.get(node_id) == 2:
                return False
            visited[node_id] = 1
            for neighbor in adj.get(node_id, []):
                if dfs(neighbor):
                    return True
            visited[node_id] = 2
            return False
        
        for node in nodes:
            if visited.get(node.get("id"), 0) == 0:
                if dfs(node.get("id")):
                    return True
        return False
    
    def _get_root_nodes(self, nodes: List[Dict], edges: List[Dict]) -> List[str]:
        """Get nodes with no incoming edges"""
        has_incoming = set()
        for edge in edges:
            has_incoming.add(edge.get("to"))
        
        return [n.get("id") for n in nodes if n.get("id") not in has_incoming]
    
    def _refine_dag(self, state: PlannerState) -> PlannerState:
        """Refine the DAG based on validation errors"""
        logger.info("🔄 Refining DAG based on validation errors...")
        
        # Keep the errors around; if we exhaust iterations we should fail (or fallback),
        # not silently "succeed" with an invalid graph.
        errors = state.validation_errors.copy()
        
        state.planning_steps.append({
            "step_type": "refine",
            "content": f"Refining based on errors: {errors}",
        })
        
        return state
    
    def _finalize(self, state: PlannerState) -> PlannerState:
        """Finalize the DAG and prepare output"""
        logger.info("🎯 Finalizing DAG...")
        
        # Guard: never finalize an empty graph
        if not state.current_nodes:
            state.success = False
            state.error = "DAG planning produced an empty graph (0 nodes)"
            state.validation_errors = ["DAG has no nodes"]
            return state

        if state.validation_errors:
            state.success = False
            state.error = f"Validation failed: {state.validation_errors}"
            return state
        
        # Build the final graph
        graph_id = f"graph-{state.pipeline_id}"
        
        final_graph = {
            "graph_id": graph_id,
            "schema_version": "1.0.0",
            "nodes": state.current_nodes,
            "edges": state.current_edges,
            "metadata": {
                "user_request": state.user_request,
                "planning_iterations": state.iteration,
            }
        }
        
        state.final_graph = final_graph
        state.success = True
        
        state.planning_steps.append({
            "step_type": "finalize",
            "content": f"DAG finalized: {len(state.current_nodes)} nodes",
        })
        
        logger.info(f"✅ DAG planning complete: {len(state.current_nodes)} nodes, {len(state.current_edges)} edges")
        
        return state
    
    # ==========================================================================
    # Utilities
    # ==========================================================================
    
    def _extract_json_from_response(self, content: str) -> Optional[Dict]:
        """Extract JSON from LLM response"""
        try:
            # Try direct parse
            return json.loads(content)
        except json.JSONDecodeError:
            pass
        
        # Try extracting from markdown code block
        if "```json" in content:
            try:
                json_str = content.split("```json")[1].split("```")[0].strip()
                return json.loads(json_str)
            except (IndexError, json.JSONDecodeError):
                pass
        
        if "```" in content:
            try:
                json_str = content.split("```")[1].split("```")[0].strip()
                return json.loads(json_str)
            except (IndexError, json.JSONDecodeError):
                pass
        
        return None
