# Connector Resolver Agent

AI-powered connector name resolution using the ReAct pattern. This agent dynamically discovers available connectors and intelligently matches user input to canonical names without requiring hardcoded mappings.

## Overview

The Connector Resolver Agent replaces hardcoded typo mappings (like `KNOWN_TYPOS`) with an intelligent AI system that:

- **Dynamically discovers** available connectors by scanning the filesystem
- **Uses LLM reasoning** to match user input to canonical names
- **Handles typos, brand variations, and abbreviations** automatically
- **Works with any connector** including JIT-generated ones
- **Requires zero maintenance** as new connectors are added

## Architecture

The agent uses the **ReAct pattern** (Reasoning + Acting):

1. **Reasoning**: Analyzes user input for clues (brand names, technology type, abbreviations)
2. **Acting**: Calls tools to gather information about available connectors
3. **Decision**: Returns the best match with confidence score or suggestions

## Tools Available to the Agent

The agent has 4 tools it can use:

1. `discover_connectors()` - Lists all available connectors
2. `get_connector_metadata(name)` - Reads detailed metadata for a connector
3. `search_by_category(category)` - Filters connectors by category
4. `search_by_description(query)` - Searches by keyword in names/descriptions

## Usage

### Python API

```python
from agents.connector_resolver.agent import ConnectorResolverAgent

agent = ConnectorResolverAgent()
result = agent.resolve("amazons3")

if result.confidence >= 0.9:
    print(f"Resolved to: {result.canonical}")
    # Use result.canonical for connector generation
elif result.confidence >= 0.7:
    print("Multiple matches found:")
    for suggestion in result.suggestions:
        print(f"  - {suggestion['name']}: {suggestion['reason']}")
else:
    print("No exact match, showing alternatives + generate new option")
```

### HTTP API

The agent is also available as a FastAPI service:

```bash
# Start the service
cd llm-service/src/agents/connector_resolver
python agent.py

# Make a request
curl -X POST http://localhost:5003/resolve \
  -H "Content-Type: application/json" \
  -d '{"connector_name": "amazons3"}'
```

Response:
```json
{
  "canonical": "aws_s3",
  "confidence": 0.95,
  "suggestions": [
    {"name": "aws_s3", "reason": "AWS S3 cloud storage connector"}
  ],
  "reasoning": "User input 'amazons3' matches 'aws_s3' directory with high confidence",
  "correction_message": "Using aws_s3 instead of amazons3"
}
```

## Confidence Levels

The agent returns a confidence score with its decision:

- **>= 0.9**: High confidence - Single canonical match
- **0.7 - 0.9**: Medium confidence - Show top 3 suggestions to user
- **< 0.7**: Low confidence - Show alternatives + "Generate New" option

## Integration

The agent is integrated into:

1. **Tool Generator** (`llm-service/src/agents/tool_generator/service.py`)
   - Resolves connector names before generation
   - Shows suggestions dialog when uncertain
   - Notifies user of name corrections

2. **Logo Downloader** (`llm-service/src/utils/logo_downloader.py`)
   - Uses resolved aliases to try multiple logo sources
   - Falls back to traditional methods if agent unavailable

## Examples

### Typo Correction
```
Input: "postgre"
Agent: Searches for "postgre" → Finds "postgresql" directory
Result: canonical="postgresql", confidence=0.95
```

### Brand Variation
```
Input: "amazon s3"
Agent: Recognizes brand prefix "amazon" → Searches for "s3" → Finds "aws_s3"
Result: canonical="aws_s3", confidence=0.92
```

### Abbreviation
```
Input: "bq"
Agent: Searches by description → Finds "bigquery" with "bq" mentioned
Result: canonical="bigquery", confidence=0.85
```

### Uncertain Match
```
Input: "cloud storage"
Agent: Finds multiple matches: aws_s3, gcs, minio
Result: confidence=0.75, suggestions=[aws_s3, gcs, minio]
Action: Show user selection dialog
```

## Testing

Run the test suite to verify agent behavior:

```bash
cd llm-service
export OPENAI_API_KEY='your-key-here'
python test_connector_resolver.py
```

The test suite covers:
- Exact names
- Typos
- Brand variations
- Abbreviations
- Natural language queries
- Unknown connectors

## Configuration

Environment variables:

- `CONNECTORS_DIR`: Path to mcp-connectors directory (default: `/app/shared/mcp-connectors`)
- `OPENAI_API_KEY`: Required for LLM reasoning (required)

## Benefits

### Zero Maintenance
No need to update hardcoded mappings when new connectors are added. The agent discovers them automatically.

### Intelligent Reasoning
Understands context, brand names, abbreviations, and natural language. Learns from connector metadata.

### Transparent
Agent's reasoning is visible in logs and returned to user, making decisions explainable.

### Extensible
Easy to add new tools (e.g., semantic embeddings, user feedback learning) without changing core logic.

## Migration from KNOWN_TYPOS

The old `FuzzyConnectorMatcher` class with `KNOWN_TYPOS` (formerly `agents.intent.service`) has been **removed** (PR #167) — use `ConnectorResolverAgent`:

```python
# Old way — REMOVED (agents.intent.service.FuzzyConnectorMatcher no longer exists)
#   matcher = FuzzyConnectorMatcher(known_connectors)
#   result = matcher.find_best_match("postgre")

# New way (current)
from agents.connector_resolver.agent import ConnectorResolverAgent
agent = ConnectorResolverAgent()
result = agent.resolve("postgre")
```

The tool generator and logo downloader now use the new agent by default.

## Future Enhancements

Potential improvements:

1. **Semantic Embeddings**: Use vector similarity for better matching
2. **User Feedback**: Learn from user selections to improve future matches
3. **Caching**: Cache common resolutions for faster response
4. **Multi-language**: Support connector names in multiple languages
5. **Version Awareness**: Consider connector versions in matching logic

