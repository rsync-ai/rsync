"""
HITL Interpreter
================

Interprets natural language user responses during HITL (Human-in-the-Loop)
interactions and converts them into structured node configuration updates.

This is used when a DAG node requires user input (e.g., missing config,
decision required) and the user responds in natural language.

Example:
    - Node asks: "Which tables do you want to sync?"
    - User says: "sync users and orders tables"
    - Interpreter returns: {"tables": ["users", "orders"]}
"""

import logging
import json
import re
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

logger = logging.getLogger("dag_planner.hitl_interpreter")


@dataclass
class InterpretResult:
    """Result of interpreting a user response"""
    success: bool
    config_patch: Optional[Dict[str, Any]] = None  # JSON Patch or full config
    error: Optional[str] = None
    confidence: float = 1.0
    needs_clarification: bool = False
    clarification_prompt: Optional[str] = None


class HITLInterpreter:
    """
    Interprets natural language HITL responses into structured configs.
    
    Uses the LLM gateway for complex interpretations but has built-in
    heuristics for common patterns to reduce latency.
    
    Example:
        interpreter = HITLInterpreter(llm_client)
        
        result = interpreter.interpret(
            node_id="source_1",
            node_kind="source",
            hitl_prompt="Which tables do you want to sync?",
            user_response="sync users and orders tables",
            node_context={"connector_type": "mysql"}
        )
        
        # result.config_patch = {"tables": ["users", "orders"]}
    """
    
    def __init__(self, llm_client):
        """
        Initialize the HITL interpreter.
        
        Args:
            llm_client: LLMClient instance for LLM calls
        """
        self.llm_client = llm_client
    
    def interpret(
        self,
        node_id: str,
        node_kind: str,
        hitl_prompt: str,
        user_response: str,
        node_context: Optional[Dict[str, Any]] = None,
        required_fields: Optional[List[Dict[str, Any]]] = None,
    ) -> InterpretResult:
        """
        Interpret a user's natural language response into a config patch.
        
        Args:
            node_id: ID of the node requiring input
            node_kind: Kind of node (source, destination, etc.)
            hitl_prompt: The question that was asked to the user
            user_response: User's natural language response
            node_context: Current node configuration context
            required_fields: Schema of required fields (optional)
            
        Returns:
            InterpretResult with config_patch or error
        """
        logger.info(f"🎯 Interpreting HITL response for node {node_id}: {user_response[:50]}...")
        
        # First, try fast heuristic interpretation
        heuristic_result = self._try_heuristic_interpretation(
            node_kind=node_kind,
            hitl_prompt=hitl_prompt,
            user_response=user_response,
            node_context=node_context,
        )
        
        if heuristic_result.success and heuristic_result.confidence >= 0.9:
            logger.info(f"✅ Heuristic interpretation succeeded (confidence: {heuristic_result.confidence})")
            return heuristic_result
        
        # Fall back to LLM interpretation for complex cases
        return self._llm_interpretation(
            node_id=node_id,
            node_kind=node_kind,
            hitl_prompt=hitl_prompt,
            user_response=user_response,
            node_context=node_context,
            required_fields=required_fields,
        )
    
    def _try_heuristic_interpretation(
        self,
        node_kind: str,
        hitl_prompt: str,
        user_response: str,
        node_context: Optional[Dict[str, Any]] = None,
    ) -> InterpretResult:
        """
        Try to interpret using fast heuristics.
        
        Returns high-confidence results for common patterns.
        """
        response_lower = user_response.lower().strip()
        prompt_lower = hitl_prompt.lower()
        
        # Pattern 1: Table selection
        if any(kw in prompt_lower for kw in ["table", "which table", "select table"]):
            tables = self._extract_table_names(user_response)
            if tables:
                return InterpretResult(
                    success=True,
                    config_patch={"tables": tables},
                    confidence=0.95,
                )
        
        # Pattern 2: Yes/No confirmation
        if any(kw in prompt_lower for kw in ["confirm", "proceed", "continue", "approve"]):
            if any(yes in response_lower for yes in ["yes", "yeah", "yep", "confirm", "proceed", "approve", "ok", "sure"]):
                return InterpretResult(
                    success=True,
                    config_patch={"confirmed": True},
                    confidence=0.95,
                )
            if any(no in response_lower for no in ["no", "nope", "cancel", "stop", "reject"]):
                return InterpretResult(
                    success=True,
                    config_patch={"confirmed": False},
                    confidence=0.95,
                )
        
        # Pattern 3: Choice selection (numbered or named)
        if "choose" in prompt_lower or "select" in prompt_lower or "option" in prompt_lower:
            # Extract number if present
            numbers = re.findall(r'\b(\d+)\b', response_lower)
            if numbers:
                return InterpretResult(
                    success=True,
                    config_patch={"selected_option": int(numbers[0])},
                    confidence=0.9,
                )
        
        # Pattern 4: Filter/condition
        if "filter" in prompt_lower or "condition" in prompt_lower:
            # Look for simple conditions
            if "where" in response_lower or "=" in response_lower:
                return InterpretResult(
                    success=True,
                    config_patch={"filter": user_response},
                    confidence=0.8,
                )
        
        # Pattern 5: Column/field selection
        if "column" in prompt_lower or "field" in prompt_lower:
            columns = self._extract_column_names(user_response)
            if columns:
                return InterpretResult(
                    success=True,
                    config_patch={"columns": columns},
                    confidence=0.9,
                )
        
        # Pattern 6: Batch size
        if "batch" in prompt_lower or "size" in prompt_lower:
            numbers = re.findall(r'\b(\d+)\b', response_lower)
            if numbers:
                return InterpretResult(
                    success=True,
                    config_patch={"batch_size": int(numbers[0])},
                    confidence=0.95,
                )
        
        # Pattern 7: Destination path
        if "path" in prompt_lower or "location" in prompt_lower or "bucket" in prompt_lower:
            # Extract path-like strings
            path_match = re.search(r'["\']?(/[\w/-]+|s3://[\w/-]+|[\w-]+/[\w/-]+)["\']?', user_response)
            if path_match:
                return InterpretResult(
                    success=True,
                    config_patch={"path": path_match.group(1)},
                    confidence=0.9,
                )
        
        # No heuristic match
        return InterpretResult(
            success=False,
            confidence=0.0,
        )
    
    def _extract_table_names(self, text: str) -> List[str]:
        """Extract table names from text"""
        # Common patterns
        # "users and orders tables"
        # "the users, orders, and products tables"
        # "sync users orders"
        
        # Remove common noise words
        text = text.lower()
        noise_words = ["the", "table", "tables", "sync", "select", "from", "and", "or", ","]
        
        # Split and filter
        words = re.split(r'[\s,]+', text)
        tables = []
        
        for word in words:
            word = word.strip()
            if word and word not in noise_words and len(word) > 1:
                # Check if it looks like a table name (alphanumeric with underscores)
                if re.match(r'^[a-z][a-z0-9_]*$', word):
                    tables.append(word)
        
        return tables
    
    def _extract_column_names(self, text: str) -> List[str]:
        """Extract column names from text"""
        # Similar to table names but also consider camelCase
        text = text.lower()
        noise_words = ["the", "column", "columns", "field", "fields", "select", "include", "and", "or", ","]
        
        words = re.split(r'[\s,]+', text)
        columns = []
        
        for word in words:
            word = word.strip()
            if word and word not in noise_words and len(word) > 1:
                if re.match(r'^[a-z][a-z0-9_]*$', word):
                    columns.append(word)
        
        return columns
    
    def _llm_interpretation(
        self,
        node_id: str,
        node_kind: str,
        hitl_prompt: str,
        user_response: str,
        node_context: Optional[Dict[str, Any]] = None,
        required_fields: Optional[List[Dict[str, Any]]] = None,
    ) -> InterpretResult:
        """
        Use LLM for complex interpretation.
        
        This is the fallback when heuristics don't work.
        """
        logger.info("🤖 Using LLM for HITL interpretation...")
        
        try:
            response = self.llm_client.complete(
                "hitl/node_input",  # Use prompt registry
                {
                    "node_id": node_id,
                    "node_kind": node_kind,
                    "hitl_prompt": hitl_prompt,
                    "user_response": user_response,
                    "node_context": json.dumps(node_context or {}),
                    "required_fields": json.dumps(required_fields or []),
                }
            )
            
            content = response.get("content", "")
            
            # Parse the LLM response
            config_patch = self._parse_llm_response(content)
            
            if config_patch:
                return InterpretResult(
                    success=True,
                    config_patch=config_patch,
                    confidence=0.85,
                )
            else:
                return InterpretResult(
                    success=False,
                    error="Could not parse LLM response",
                    needs_clarification=True,
                    clarification_prompt="I didn't understand your response. Could you please rephrase?",
                )
                
        except Exception as e:
            logger.error(f"LLM interpretation failed: {e}")
            return InterpretResult(
                success=False,
                error=str(e),
                needs_clarification=True,
                clarification_prompt="Sorry, I had trouble understanding. Could you please try again?",
            )
    
    def _parse_llm_response(self, content: str) -> Optional[Dict[str, Any]]:
        """Parse LLM response to extract config patch"""
        # Try to extract JSON from response
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass
        
        # Try markdown code block
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


def interpret_node_input(
    llm_client,
    node_id: str,
    node_kind: str,
    hitl_prompt: str,
    user_response: str,
    node_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Convenience function for interpreting HITL node input.
    
    This is the main entry point for the Temporal activity
    InterpretNodeInputActivity.
    
    Args:
        llm_client: LLMClient instance
        node_id: ID of the node requiring input
        node_kind: Kind of node
        hitl_prompt: Question asked to user
        user_response: User's response
        node_context: Current node context
        
    Returns:
        Dict with:
        - success: bool
        - config_patch: Dict (if success)
        - error: str (if failure)
        - needs_clarification: bool
        - clarification_prompt: str (if needs clarification)
    """
    interpreter = HITLInterpreter(llm_client)
    result = interpreter.interpret(
        node_id=node_id,
        node_kind=node_kind,
        hitl_prompt=hitl_prompt,
        user_response=user_response,
        node_context=node_context,
    )
    
    return {
        "success": result.success,
        "config_patch": result.config_patch,
        "error": result.error,
        "needs_clarification": result.needs_clarification,
        "clarification_prompt": result.clarification_prompt,
        "confidence": result.confidence,
    }
