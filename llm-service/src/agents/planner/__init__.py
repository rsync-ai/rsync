"""
Planner Agent Package

This package contains the Planner Agent components:
- strategies.py: Planning strategies (LLM, Heuristic, Composite)
- cdc_config_generator.py: LLM-driven CDC configuration generation
- service.py: HTTP service endpoints
- kafka_consumer.py: Kafka consumer for async planning
"""

from .strategies import (
    PlanningContext,
    PlanResult,
    PlanningStrategy,
    LLMPlanningStrategy,
    HeuristicPlanningStrategy,
    CompositePlanningStrategy,
    ToolRegistry,
    get_tool_registry,
    detect_tools_from_text,
    DataPathDecision,
    DataPathDecider,
    get_data_path_decider,
    # Phase 2: Smart Pipeline Decider
    TableStats,
    DataStatistics,
    PipelinePhase,
    SmartStrategyDecision,
    SmartPipelineDecider,
    get_smart_pipeline_decider,
)

from .cdc_config_generator import (
    CDCConfigGenerator,
    CDCConfigResult,
    get_cdc_config_generator,
)

__all__ = [
    # Planning strategies
    "PlanningContext",
    "PlanResult",
    "PlanningStrategy",
    "LLMPlanningStrategy",
    "HeuristicPlanningStrategy",
    "CompositePlanningStrategy",
    "ToolRegistry",
    "get_tool_registry",
    "detect_tools_from_text",
    "DataPathDecision",
    "DataPathDecider",
    "get_data_path_decider",
    # Smart Pipeline Strategy (Phase 2)
    "TableStats",
    "DataStatistics",
    "PipelinePhase",
    "SmartStrategyDecision",
    "SmartPipelineDecider",
    "get_smart_pipeline_decider",
    # CDC config generation
    "CDCConfigGenerator",
    "CDCConfigResult",
    "get_cdc_config_generator",
]
