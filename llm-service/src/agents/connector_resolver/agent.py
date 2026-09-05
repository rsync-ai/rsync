"""
Connector Name Resolver Agent

Uses ReAct pattern to intelligently resolve connector names:
- Tool 1: discover_connectors() - List all available connectors
- Tool 2: get_connector_metadata(name) - Read connector details
- Tool 3: search_by_category(category) - Filter by category
- Tool 4: search_by_description(query) - Semantic search in descriptions

The agent reasons through the user's input and uses tools to find the best match.
"""

import os
import json
import logging
from typing import List, Dict, Optional, Tuple, Any
from pathlib import Path
from dataclasses import dataclass, asdict
from openai import OpenAI  # kept for type hints in the class
from src.utils.connector_paths import iter_connector_dirs, resolve_current_dir
try:
    # Normal import when the src/ package tree is the root (production / Docker).
    from ...utils.openai_client import make_sync_client, resolve_provider, get_default_model
except ImportError:
    # Fallback: load by file path so sys.path pollution (a third-party 'utils'
    # package shadowing ours) cannot interfere.
    import importlib.util as _ilu, os as _os
    _spec = _ilu.spec_from_file_location(
        "_rsync_openai_client",
        _os.path.realpath(_os.path.join(_os.path.dirname(__file__), "..", "..", "utils", "openai_client.py"))
    )
    _m = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_m)
    make_sync_client = _m.make_sync_client
    resolve_provider  = _m.resolve_provider
    get_default_model = _m.get_default_model
    del _ilu, _os, _spec, _m

logger = logging.getLogger("connector-resolver-agent")

@dataclass
class ResolverResult:
    """Result from connector name resolution"""
    canonical: Optional[str]
    confidence: float
    suggestions: List[Dict[str, Any]]  # List of alternatives
    reasoning: str  # Agent's reasoning process
    correction_message: Optional[str]
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return asdict(self)

class ConnectorResolverAgent:
    """
    AI Agent that resolves connector names using dynamic discovery and LLM reasoning.
    No hardcoded mappings - works with any connector.
    """
    
    def __init__(self, connectors_dir: str = None):
        self.connectors_dir = connectors_dir or os.getenv(
            "CONNECTORS_DIR", 
            "/app/shared/mcp-connectors"
        )
        
        # Check if we're in local development
        if not os.path.exists(self.connectors_dir):
            # Try relative path for local dev
            local_path = os.path.join(
                os.path.dirname(__file__), 
                "../../../../shared/mcp-connectors"
            )
            if os.path.exists(local_path):
                self.connectors_dir = os.path.abspath(local_path)
                logger.info(f"Using local connectors path: {self.connectors_dir}")
        
        _provider = resolve_provider()
        self._model = get_default_model(_provider)
        try:
            # Clean up proxy environment variables that might interfere with the SDK
            proxy_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']
            saved_proxies = {}
            for var in proxy_vars:
                if var in os.environ:
                    saved_proxies[var] = os.environ[var]
                    del os.environ[var]

            try:
                self.client = make_sync_client()
            finally:
                for var, value in saved_proxies.items():
                    os.environ[var] = value

            logger.info("Connector resolver using provider=%s model=%s", _provider, self._model)
        except Exception as e:
            logger.error(f"Failed to initialize LLM client: {e}")
            self.client = None
        
    # =========================================================================
    # TOOLS (Functions the agent can call)
    # =========================================================================
    
    def discover_connectors(self) -> List[Dict[str, str]]:
        """
        Tool: Discover all available connectors by scanning directory.
        Returns list of {name, path, has_metadata}
        """
        connectors = []
        try:
            connectors_path = Path(self.connectors_dir)
            if not connectors_path.exists():
                logger.warning(f"Connectors directory not found: {self.connectors_dir}")
                return connectors
                
            # Connector roots are identified by latest.json (any nesting depth);
            # metadata lives in the canonical version dir (versions/<current_version>/).
            for connector_dir in iter_connector_dirs(connectors_path):
                metadata_file = resolve_current_dir(connector_dir) / "metadata.json"
                connectors.append({
                    "name": connector_dir.name,
                    "path": str(connector_dir),
                    "has_metadata": metadata_file.exists()
                })
        except Exception as e:
            logger.error(f"Error discovering connectors: {e}")
        
        logger.info(f"🔍 Discovered {len(connectors)} connectors")
        return connectors
    
    def get_connector_metadata(self, connector_name: str) -> Optional[Dict]:
        """
        Tool: Read metadata for a specific connector.
        Returns metadata dict or None
        """
        try:
            # Direct flat layout: <connectors_dir>/<name>/versions/<cv>/metadata.json
            connector_path = resolve_current_dir(Path(self.connectors_dir) / connector_name) / "metadata.json"
            if connector_path.exists():
                with open(connector_path) as f:
                    return json.load(f)
            # Nested layout (e.g. database/<name>, internal/<name>): match by dir name.
            for connector_dir in iter_connector_dirs(self.connectors_dir):
                if connector_dir.name == connector_name:
                    meta = resolve_current_dir(connector_dir) / "metadata.json"
                    if meta.exists():
                        with open(meta) as f:
                            return json.load(f)
        except Exception as e:
            logger.debug(f"Could not read metadata for {connector_name}: {e}")
        return None
    
    def search_by_category(self, category: str) -> List[str]:
        """
        Tool: Find connectors by category (database, cloud_storage, api_saas, etc.)
        """
        connectors = self.discover_connectors()
        matches = []
        
        for conn in connectors:
            metadata = self.get_connector_metadata(conn["name"])
            if metadata:
                conn_category = metadata.get("category", "").lower()
                if conn_category == category.lower():
                    matches.append(conn["name"])
        
        logger.info(f"🔍 Found {len(matches)} connectors in category '{category}'")
        return matches
    
    def search_by_description(self, query: str) -> List[Tuple[str, str, float]]:
        """
        Tool: Search connectors by keyword in name/description.
        Returns list of (name, description, relevance_score)
        """
        query_lower = query.lower()
        matches = []
        
        connectors = self.discover_connectors()
        for conn in connectors:
            score = 0.0
            metadata = self.get_connector_metadata(conn["name"])
            
            name = conn["name"].lower()
            description = ""
            display_name = ""
            
            if metadata:
                description = metadata.get("description", "").lower()
                display_name = metadata.get("name", "").lower()
            
            # Simple relevance scoring
            if query_lower == name:
                score = 1.0
            elif query_lower in name:
                score = 0.9
            elif query_lower in display_name:
                score = 0.85
            elif query_lower in description:
                score = 0.7
            # Check for partial matches in name
            elif any(part in name for part in query_lower.split()):
                score = 0.6
            
            if score > 0:
                matches.append((
                    conn["name"],
                    description or display_name or name,
                    score
                ))
        
        # Sort by score
        matches.sort(key=lambda x: x[2], reverse=True)
        logger.info(f"🔍 Found {len(matches)} connectors matching '{query}'")
        return matches[:10]  # Top 10
    
    # =========================================================================
    # AGENT REASONING (ReAct Pattern)
    # =========================================================================
    
    def resolve(self, user_input: str) -> ResolverResult:
        """
        Main entry point: Resolve a connector name using AI reasoning.
        
        Uses ReAct pattern:
        1. Agent reasons about the input
        2. Agent calls tools to gather information
        3. Agent decides on best match or suggestions
        """
        logger.info(f"🤖 Resolving connector name: '{user_input}'")
        
        if not self.client:
            logger.error("OpenAI client not initialized - cannot resolve")
            return ResolverResult(
                canonical=None,
                confidence=0.0,
                suggestions=[],
                reasoning="OpenAI API key not configured",
                correction_message=None
            )
        
        # Define tools for the agent
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "discover_connectors",
                    "description": "List all available MCP connectors by scanning the connectors directory. Returns connector names and paths.",
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_connector_metadata",
                    "description": "Get detailed metadata for a specific connector including name, description, category, and capabilities.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "connector_name": {
                                "type": "string",
                                "description": "The connector directory name to look up"
                            }
                        },
                        "required": ["connector_name"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "search_by_category",
                    "description": "Find connectors by category (database, cloud_storage, api_saas, streaming, etc.)",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "category": {
                                "type": "string",
                                "description": "Category to filter by"
                            }
                        },
                        "required": ["category"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "search_by_description",
                    "description": "Search connectors by keyword in their names and descriptions. Returns ranked results.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "Search query or keyword"
                            }
                        },
                        "required": ["query"]
                    }
                }
            }
        ]
        
        # Initial prompt for the agent
        system_prompt = """You are a Connector Name Resolver Agent. Your job is to match user input to the correct canonical connector name.

When given a connector name (which might have typos, brand variations, or be incomplete), you should:
1. Use your tools to discover available connectors
2. Analyze the user input for clues (brand names, technology type, common abbreviations)
3. Search for matching connectors
4. Return the best canonical match with confidence score

Common patterns to recognize:
- "s3", "amazons3", "amazon s3", "AWS S3" → likely "aws-s3" or "aws_s3"
- "postgre", "postgres" → likely "postgresql"
- "google bigquery", "bq" → likely "bigquery"
- Brand prefixes (Amazon, Google, Microsoft) often map to tech names

If confident (>0.9), return single match.
If uncertain (0.7-0.9), return top 3 suggestions.
If no match (<0.7), return top 3 similar connectors.

Be intelligent about:
- Typos and misspellings
- Brand name variations
- Abbreviations
- Compound names (e.g., "google cloud storage" → "gcs")
"""
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Find the best matching connector for user input: '{user_input}'"}
        ]
        
        # Run agent loop (max 3 tool calls)
        try:
            for iteration in range(3):
                response = self.client.chat.completions.create(
                    model=self._model,
                    messages=messages,
                    tools=tools,
                    tool_choice="auto"
                )
                
                message = response.choices[0].message
                messages.append(message)
                
                # Check if agent wants to call tools
                if message.tool_calls:
                    for tool_call in message.tool_calls:
                        function_name = tool_call.function.name
                        arguments = json.loads(tool_call.function.arguments)
                        
                        logger.info(f"  🔧 Agent calling tool: {function_name}({arguments})")
                        
                        # Execute tool
                        if function_name == "discover_connectors":
                            result = self.discover_connectors()
                        elif function_name == "get_connector_metadata":
                            result = self.get_connector_metadata(arguments["connector_name"])
                        elif function_name == "search_by_category":
                            result = self.search_by_category(arguments["category"])
                        elif function_name == "search_by_description":
                            result = self.search_by_description(arguments["query"])
                        else:
                            result = {"error": f"Unknown tool: {function_name}"}
                        
                        # Add tool result to messages
                        messages.append({
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": json.dumps(result)
                        })
                else:
                    # Agent has made final decision
                    break
            
            # Get final answer from agent
            final_response = self.client.chat.completions.create(
                model=self._model,
                messages=messages + [{
                    "role": "user",
                    "content": """Based on your analysis, provide your final decision in JSON format:
{
  "canonical": "exact_connector_name or null",
  "confidence": 0.0-1.0,
  "suggestions": [
    {"name": "connector1", "reason": "why this might match"},
    {"name": "connector2", "reason": "alternative option"}
  ],
  "reasoning": "brief explanation of your decision",
  "correction_message": "user-friendly message or null"
}

If confidence > 0.9: Return single canonical match
If confidence 0.7-0.9: Return top 3 suggestions
If confidence < 0.7: Return top 3 similar + indicate no exact match"""
                }],
                response_format={"type": "json_object"}
            )
            
            # Parse agent's decision (tolerate rare markdown-wrapped output)
            from src.utils.json_extract import loads as _safe_json_loads
            decision = _safe_json_loads(final_response.choices[0].message.content)
            return ResolverResult(
                canonical=decision.get("canonical"),
                confidence=decision.get("confidence", 0.0),
                suggestions=decision.get("suggestions", []),
                reasoning=decision.get("reasoning", ""),
                correction_message=decision.get("correction_message")
            )
        except Exception as e:
            logger.error(f"Failed to resolve connector name: {e}", exc_info=True)
            return ResolverResult(
                canonical=None,
                confidence=0.0,
                suggestions=[],
                reasoning=f"Agent error: {e}",
                correction_message=None
            )


# =========================================================================
# HTTP SERVICE ENDPOINT
# =========================================================================

def create_resolver_service():
    """Create FastAPI app for connector resolver service"""
    from fastapi import FastAPI
    
    app = FastAPI(title="Connector Resolver Agent", version="1.0.0")
    agent = ConnectorResolverAgent()
    
    @app.post("/resolve")
    async def resolve_connector(request: dict):
        """
        Resolve connector name using AI agent.
        
        Request: {"connector_name": "amazons3"}
        Response: {
            "canonical": "aws-s3",
            "confidence": 0.95,
            "suggestions": [...],
            "reasoning": "...",
            "correction_message": "Using aws-s3 instead of amazons3"
        }
        """
        user_input = request.get("connector_name", "").strip()
        if not user_input:
            return {"error": "connector_name is required"}
        
        result = agent.resolve(user_input)
        
        return result.to_dict()
    
    return app

if __name__ == "__main__":
    import uvicorn
    app = create_resolver_service()
    uvicorn.run(app, host="0.0.0.0", port=5003)

