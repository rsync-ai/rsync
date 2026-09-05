"""
rsync connector-lifecycle service — OSS / community edition entrypoint.

This is the moat-free counterpart to the private `tool-generator` service. It serves
ONLY the connector-lifecycle surface the self-hosted data plane needs:

    GET  /health        liveness
    POST /v1/deploy     (re)start or JIT-build an EXISTING connector container
                        (backend-orchestrator calls this via TOOL_GENERATOR_URL for
                        self-heal / pinned-version JIT builds)

It deliberately does NOT import the connector-generation package or its curated config
(vendor_apis.yaml / auth_rules.yaml / capability_rules.yaml / learned_apis.jsonl). The
import closure of this module is stdlib + fastapi/pydantic + the moat-free deployment
service — verify with:

    python -c "import src.lifecycle.main"

which must succeed even when src/agents/tool_generator/{agents,config} and the LLM gateway
are physically absent from the image (see Dockerfile.oss allowlist COPY).

Connector GENERATION, NL→pipeline, NL→SQL, planning and the OAuth registry are cloud-only
and are NOT served here; the Go data plane degrades gracefully when they are absent.
"""

import os
import logging

from fastapi import FastAPI

# Only the moat-free lifecycle router. This import must not transitively reach
# tool_generator.agents.orchestrator or tool_generator.config.
from src.agents.tool_generator.deployment.routes import lifecycle_router

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format=os.getenv("LOG_FORMAT_STR", "%(asctime)s %(levelname)s %(name)s %(message)s"),
)
logger = logging.getLogger("connector-lifecycle")

app = FastAPI(title="rsync connector-lifecycle", description="Community-edition connector lifecycle service")

# POST /v1/deploy (the only endpoint the data plane calls).
app.include_router(lifecycle_router, prefix="/v1", tags=["Connector Lifecycle"])


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "connector-lifecycle",
        "edition": os.getenv("RSYNC_EDITION", "community"),
    }


@app.get("/version")
async def version():
    return {
        "service": "connector-lifecycle",
        "edition": os.getenv("RSYNC_EDITION", "community"),
        "version": os.getenv("RSYNC_VERSION", "dev"),
    }


def main():
    import uvicorn

    port = int(os.getenv("PORT", "5010"))
    logger.info("🚀 connector-lifecycle (community) starting on :%s", port)
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
