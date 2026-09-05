"""
DAG Planner Module
==================

LangGraph-based DAG planner for generating n8n-style workflow graphs from natural language.

This module uses LangGraph for the planning loop (think → tool calls → validate → refine)
but delegates all LLM calls to rsync-ai's existing LLM gateway via LLMClient.

Key Components:
- DAGPlannerAgent: Main LangGraph agent for DAG generation
- DAGPlanningStrategy: Strategy class compatible with existing planning infrastructure
- HITLInterpreter: Interprets NL user responses into structured node configs

Usage:
    from src.agents.dag_planner import DAGPlanningStrategy, DAGPlannerAgent
    
    # As a strategy (integrates with existing planner service)
    dag_strategy = DAGPlanningStrategy(llm_client, tool_registry)
    result = dag_strategy.create_plan(context)
    
    # Direct agent usage
    agent = DAGPlannerAgent(llm_client)
    graph = agent.plan("sync mysql to s3, then notify slack on completion")
"""

from .agent import DAGPlannerAgent
from .strategy import DAGPlanningStrategy
from .hitl_interpreter import HITLInterpreter
from .types import (
    DAGPlannerState,
    PlanningStep,
    WorkflowGraphOutput,
    NodeSpec,
    EdgeSpec,
)

__all__ = [
    "DAGPlannerAgent",
    "DAGPlanningStrategy", 
    "HITLInterpreter",
    "DAGPlannerState",
    "PlanningStep",
    "WorkflowGraphOutput",
    "NodeSpec",
    "EdgeSpec",
]
