"""
Context7 MCP Server
Provides documentation lookup via Context7 API for connector generation
"""

import os
import logging
import re
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s", "service": "context7-mcp"}'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Context7 MCP Server", version="1.0.0")

# Context7 API Configuration
CONTEXT7_API_URL = os.getenv("CONTEXT7_API_URL", "https://context7.com/api")
CONTEXT7_API_KEY = os.getenv("CONTEXT7_API_KEY")
CONTEXT7_TIMEOUT = int(os.getenv("CONTEXT7_TIMEOUT", "30"))
CONTEXT7_DEFAULT_TOKENS = int(os.getenv("CONTEXT7_DEFAULT_TOKENS", "10000"))


class ResolveRequest(BaseModel):
    library_name: str
    # Optional: require a minimum match score (0..1) between the query and the selected result.
    # If provided and the best match is below this threshold, we return success=false.
    min_score: Optional[float] = None


class ResolveResponse(BaseModel):
    success: bool
    library_id: Optional[str] = None
    match_score: float = 0.0
    # Best-effort passthrough of the selected search result for debugging/observability.
    # (Not required by callers; kept optional for backward compatibility.)
    top_result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class DocsRequest(BaseModel):
    library_id: str
    topic: Optional[str] = None
    tokens: Optional[int] = None


class DocsResponse(BaseModel):
    success: bool
    documentation: Optional[str] = None
    error: Optional[str] = None


def _context7_headers() -> Dict[str, str]:
    """
    Context7 v2 REST API supports anonymous usage with lower rate limits.
    If an API key is available, pass it for higher limits.
    """
    headers: Dict[str, str] = {"Accept": "*/*"}
    if CONTEXT7_API_KEY:
        headers["Authorization"] = f"Bearer {CONTEXT7_API_KEY}"
    return headers


async def _context7_get_text(endpoint: str, params: Optional[Dict[str, Any]] = None) -> str:
    """GET a text/plain response from Context7 API."""
    try:
        async with httpx.AsyncClient(timeout=CONTEXT7_TIMEOUT) as client:
            resp = await client.get(
                f"{CONTEXT7_API_URL}{endpoint}",
                params=params or {},
                headers=_context7_headers(),
            )
            resp.raise_for_status()
            return resp.text
    except httpx.HTTPStatusError as e:
        logger.error(f"Context7 API error: {e.response.status_code} - {e.response.text[:300]}")
        raise HTTPException(e.response.status_code, f"Context7 API error: {e.response.text}")
    except Exception as e:
        logger.error(f"Context7 request failed: {e}")
        raise HTTPException(500, f"Context7 request failed: {str(e)}")


async def _context7_get_json(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """GET a JSON response from Context7 API."""
    try:
        async with httpx.AsyncClient(timeout=CONTEXT7_TIMEOUT) as client:
            resp = await client.get(
                f"{CONTEXT7_API_URL}{endpoint}",
                params=params or {},
                headers={**_context7_headers(), "Accept": "application/json"},
            )
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Context7 API error: {e.response.status_code} - {e.response.text[:300]}")
        raise HTTPException(e.response.status_code, f"Context7 API error: {e.response.text}")
    except Exception as e:
        logger.error(f"Context7 request failed: {e}")
        raise HTTPException(500, f"Context7 request failed: {str(e)}")


def _parse_library_id(library_id: str) -> tuple[str, str, Optional[str]]:
    """
    Parse a Context7 library ID like:
      /owner/repo
      /owner/repo/version
    """
    parts = [p for p in (library_id or "").strip().split("/") if p]
    if len(parts) < 2:
        raise HTTPException(400, "Invalid library_id. Expected /owner/repo[/version]")
    owner, repo = parts[0], parts[1]
    version = parts[2] if len(parts) >= 3 else None
    return owner, repo, version


_RE_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def _norm(s: str) -> str:
    return _RE_NON_ALNUM.sub("", (s or "").strip().lower())


def _levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Levenshtein distance between two strings (iterative DP)."""
    if len(s1) < len(s2):
        return _levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    return previous_row[-1]


def _similarity_score(query: str, candidate: str) -> float:
    """
    Return a best-effort similarity score in [0,1].

    We prefer deterministic string similarity rather than Context7 "relevance" which
    may not be stable across API versions.
    """
    q = _norm(query)
    c = _norm(candidate)
    if not q or not c:
        return 0.0
    # Avoid over-confident substring matches for very short queries.
    if len(q) >= 4 and q in c:
        return 0.95
    d = _levenshtein_distance(q, c)
    denom = max(len(q), len(c))
    if denom == 0:
        return 0.0
    return max(0.0, min(1.0, 1.0 - (d / float(denom))))


def _best_match_score(query: str, result: Dict[str, Any]) -> float:
    """
    Compute a match score between the user query and a Context7 search result.
    Uses only fields we can safely rely on: id + optional name fields.
    """
    candidates: List[str] = []

    # Prefer id because it is always present when a result is valid.
    rid = str(result.get("id") or "")
    if rid:
        candidates.append(rid)
        try:
            owner, repo, _version = _parse_library_id(rid)
            candidates.extend([owner, repo, f"{owner}/{repo}", f"{owner}-{repo}"])
        except Exception:
            # If the id isn't parseable, still keep it as a raw candidate.
            pass

    # Best-effort: some APIs may include a display name.
    for k in ("name", "title", "package", "repo", "library"):
        v = result.get(k)
        if isinstance(v, str) and v.strip():
            candidates.append(v)

    return max((_similarity_score(query, c) for c in candidates), default=0.0)


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "context7-mcp",
        "api_configured": bool(CONTEXT7_API_KEY)
    }


@app.post("/resolve", response_model=ResolveResponse)
async def resolve_library(request: ResolveRequest):
    """
    Resolve library name to Context7 library ID
    
    Example:
        POST /resolve
        {"library_name": "stripe"}
        
        Response:
        {"success": true, "library_id": "/stripe/stripe-node"}
    """
    logger.info(f"Resolving library: {request.library_name}")
    
    try:
        # Context7 v2 REST: GET /v2/search?query=...
        data = await _context7_get_json("/v2/search", params={"query": request.library_name})
        results = data.get("results") or []
        if not results:
            logger.warning(f"❌ Library not found: {request.library_name}")
            return ResolveResponse(success=False, error="Library not found")

        # Pick the top result. (API already ranks by relevance.)
        top = results[0] if isinstance(results[0], dict) else {}
        library_id = (top or {}).get("id")
        if library_id:
            score = _best_match_score(request.library_name, top)
            min_score = request.min_score
            if min_score is not None:
                try:
                    min_score = float(min_score)
                except Exception:
                    min_score = None
            if min_score is not None and (score < 0.0 or score > 1.0):
                min_score = None

            if min_score is not None and score < min_score:
                logger.info(
                    f"❌ Resolve rejected (low match): query='{request.library_name}' "
                    f"id='{library_id}' score={score:.2f} min_score={min_score:.2f}"
                )
                return ResolveResponse(
                    success=False,
                    library_id=None,
                    match_score=score,
                    top_result=top,
                    error="Low match score for query",
                )

            logger.info(f"✅ Resolved {request.library_name} -> {library_id} (match_score={score:.2f})")
            return ResolveResponse(success=True, library_id=library_id, match_score=score, top_result=top)

        logger.warning(f"❌ Library not found (no id in results): {request.library_name}")
        return ResolveResponse(success=False, error="Library not found")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Resolve failed: {e}")
        return ResolveResponse(success=False, error=str(e))


@app.post("/docs", response_model=DocsResponse)
async def fetch_documentation(request: DocsRequest):
    """
    Fetch documentation for a library
    
    Example:
        POST /docs
        {
            "library_id": "/stripe/stripe-node",
            "topic": "authentication",
            "tokens": 5000
        }
        
        Response:
        {"success": true, "documentation": "..."}
    """
    logger.info(f"Fetching docs: library_id={request.library_id}, topic={request.topic}")
    
    try:
        owner, repo, version = _parse_library_id(request.library_id)

        # Context7 v2 REST:
        # - GET /v2/docs/info/{owner}/{repo}
        # - GET /v2/docs/info/{owner}/{repo}/{version}
        if version:
            endpoint = f"/v2/docs/info/{owner}/{repo}/{version}"
        else:
            endpoint = f"/v2/docs/info/{owner}/{repo}"

        # Context7 supports paging; keep it simple for our MCP use.
        params: Dict[str, Any] = {"page": 1}
        if request.topic:
            params["topic"] = request.topic

        documentation = await _context7_get_text(endpoint, params=params)
        if documentation and documentation.strip():
            logger.info(f"✅ Fetched {len(documentation)} chars for {request.library_id}")
            return DocsResponse(success=True, documentation=documentation)

        logger.warning(f"❌ No documentation found for {request.library_id}")
        return DocsResponse(success=False, error="No documentation found")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Docs fetch failed: {e}")
        return DocsResponse(success=False, error=str(e))


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8080"))
    logger.info(f"🚀 Context7 MCP Server starting on port {port}")
    logger.info(f"Context7 API URL: {CONTEXT7_API_URL}")
    logger.info(f"API Key configured: {bool(CONTEXT7_API_KEY)}")
    uvicorn.run(app, host="0.0.0.0", port=port)
