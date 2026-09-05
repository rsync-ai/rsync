"""
DAG Planner Types
=================

Type definitions for the DAG planning agent.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Literal
from enum import Enum


class NodeKind(str, Enum):
    """Node types in a workflow graph"""
    SOURCE = "source"
    DESTINATION = "destination"
    TRANSFORM = "transform"
    API_CALL = "api_call"
    CONDITION = "condition"
    LLM = "llm"
    NOTIFICATION = "notification"
    WAIT = "wait"
    MERGE = "merge"
    SPLIT = "split"


class EdgeType(str, Enum):
    """Edge types in a workflow graph"""
    DATA = "data"
    CONTROL_THEN = "control_then"
    CONTROL_ELSE = "control_else"


@dataclass
class NodeSpec:
    """Specification for a single node in the workflow graph"""
    id: str
    kind: str  # NodeKind value
    display_name: str
    config: Dict[str, Any] = field(default_factory=dict)
    inputs: List[Dict[str, Any]] = field(default_factory=list)
    outputs_schema: Optional[Dict[str, Any]] = None
    requires_hitl: bool = False
    hitl_config: Optional[Dict[str, Any]] = None
    icon: Optional[str] = None
    color: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "id": self.id,
            "kind": self.kind,
            "display_name": self.display_name,
            "config": self.config,
        }
        if self.inputs:
            result["inputs"] = self.inputs
        if self.outputs_schema:
            result["outputs_schema"] = self.outputs_schema
        if self.requires_hitl:
            result["requires_hitl"] = True
            if self.hitl_config:
                result["hitl_config"] = self.hitl_config
        if self.icon:
            result["icon"] = self.icon
        if self.color:
            result["color"] = self.color
        return result


@dataclass
class EdgeSpec:
    """Specification for an edge in the workflow graph"""
    from_node: str
    to_node: str
    edge_type: str = "data"  # EdgeType value
    from_port: Optional[str] = None
    to_port: Optional[str] = None
    label: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "from": self.from_node,
            "to": self.to_node,
            "type": self.edge_type,
        }
        if self.from_port:
            result["from_port"] = self.from_port
        if self.to_port:
            result["to_port"] = self.to_port
        if self.label:
            result["label"] = self.label
        return result


@dataclass
class WorkflowGraphOutput:
    """Output format for a workflow graph"""
    graph_id: str
    schema_version: str = "1.0.0"
    nodes: List[NodeSpec] = field(default_factory=list)
    edges: List[EdgeSpec] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "graph_id": self.graph_id,
            "schema_version": self.schema_version,
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
            "metadata": self.metadata,
        }


@dataclass
class PlanningStep:
    """A step in the planning process"""
    step_type: str  # "think", "tool_call", "validate", "refine"
    content: str
    tool_name: Optional[str] = None
    tool_input: Optional[Dict[str, Any]] = None
    tool_output: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


@dataclass
class DAGPlannerState:
    """
    State for the LangGraph DAG planning agent.
    
    This state is passed between nodes in the planning graph.
    """
    # Input
    user_request: str
    pipeline_id: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)
    
    # Available resources
    available_connections: List[Dict[str, Any]] = field(default_factory=list)
    available_tools: List[str] = field(default_factory=list)
    
    # Planning state
    planning_steps: List[PlanningStep] = field(default_factory=list)
    current_plan: Optional[WorkflowGraphOutput] = None
    
    # Validation
    validation_errors: List[str] = field(default_factory=list)
    validation_warnings: List[str] = field(default_factory=list)
    
    # Iteration control
    iteration: int = 0
    max_iterations: int = 3
    
    # Output
    final_graph: Optional[Dict[str, Any]] = None
    success: bool = False
    error: Optional[str] = None


# Tool definitions for LangGraph

@dataclass
class ToolDefinition:
    """Definition of a tool available to the planner"""
    name: str
    description: str
    parameters: Dict[str, Any]
    
    def to_langchain_tool(self) -> Dict[str, Any]:
        """Convert to LangChain tool format"""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            }
        }


# Predefined tools for the DAG planner
DAG_PLANNER_TOOLS = [
    ToolDefinition(
        name="list_connections",
        description="List available data connections for the user (databases, APIs, storage)",
        parameters={
            "type": "object",
            "properties": {
                "connection_type": {
                    "type": "string",
                    "enum": ["source", "destination", "any"],
                    "description": "Filter by connection type"
                }
            },
            "required": []
        }
    ),
    ToolDefinition(
        name="check_connector",
        description="Check if a connector exists for a given tool type",
        parameters={
            "type": "object",
            "properties": {
                "connector_type": {
                    "type": "string",
                    "description": "Type of connector (e.g., 'mysql', 'aws-s3', 'slack')"
                }
            },
            "required": ["connector_type"]
        }
    ),
    ToolDefinition(
        name="get_schema",
        description="Get schema information for a connection (tables, fields)",
        parameters={
            "type": "object",
            "properties": {
                "connection_id": {
                    "type": "string",
                    "description": "ID of the connection to query"
                }
            },
            "required": ["connection_id"]
        }
    ),
    ToolDefinition(
        name="validate_dag",
        description="Validate a workflow DAG for correctness (no cycles, valid edges)",
        parameters={
            "type": "object",
            "properties": {
                "nodes": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": "List of node definitions"
                },
                "edges": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": "List of edge definitions"
                }
            },
            "required": ["nodes", "edges"]
        }
    ),
    ToolDefinition(
        name="get_node_template",
        description="Get a template for a specific node type",
        parameters={
            "type": "object",
            "properties": {
                "node_kind": {
                    "type": "string",
                    "enum": ["source", "destination", "transform", "api_call", "condition", "llm", "notification", "wait", "merge", "split"],
                    "description": "Type of node to get template for"
                },
                "connector_type": {
                    "type": "string",
                    "description": "Optional: specific connector type for source/destination nodes"
                }
            },
            "required": ["node_kind"]
        }
    ),
]
