"""
rsync connector-lifecycle service — OSS / community edition entrypoint.

This is the moat-free counterpart to the private `tool-generator` service. It serves
the connector-lifecycle surface the self-hosted data plane needs, plus deterministic
connector generation:

    GET  /health        liveness
    POST /v1/deploy     (re)start or JIT-build an EXISTING connector container
                        (backend-orchestrator calls this via TOOL_GENERATOR_URL for
                        self-heal / pinned-version JIT builds)
    POST /v1/generate   OpenAPI/Swagger document in, working MCP connector out —
                        no model call, no API key, no outbound request
                        (src/lifecycle/scaffold_routes.py)

Both editions answer `/v1/generate` on the same wire contract, imported from
`src.agents.tool_generator.contracts.generate_v1`; the difference is which handler is
mounted by which entrypoint, never a runtime edition branch. The cloud service adds the
agentic path — research, discovery sessions, docs-to-connector, GraphQL introspection —
which needs an LLM; this handler refuses those inputs with an actionable 400 rather than
degrading into a wrong answer.

It therefore imports the RENDERING half of the connector-generation package (scaffold,
schemas, generator, templates, validation) but still NOT the agentic half
(`tool_generator/agents`) and not its curated config (vendor_apis.yaml / auth_rules.yaml
/ capability_rules.yaml / learned_apis.jsonl). The import closure of this module is
stdlib + fastapi/pydantic/jinja2 + the moat-free deployment and scaffolding modules —
verify with:

    python -c "import src.lifecycle.main"

which must succeed even when src/agents/tool_generator/{agents,config} and the LLM gateway
are physically absent from the image (see Dockerfile.oss allowlist COPY).

NL→pipeline, NL→SQL, planning and the OAuth registry remain cloud-only and are NOT served
here; the Go data plane degrades gracefully when they are absent.
"""

import os
import logging

from fastapi import FastAPI

# Moat-free routers only. Neither import may transitively reach
# tool_generator.agents.orchestrator or tool_generator.config.
from src.agents.tool_generator.deployment.routes import lifecycle_router
from src.lifecycle.scaffold_routes import scaffold_router

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format=os.getenv("LOG_FORMAT_STR", "%(asctime)s %(levelname)s %(name)s %(message)s"),
)
logger = logging.getLogger("connector-lifecycle")

app = FastAPI(title="rsync connector-lifecycle", description="Community-edition connector lifecycle service")

# POST /v1/deploy — what the data plane calls for self-heal / JIT builds.
app.include_router(lifecycle_router, prefix="/v1", tags=["Connector Lifecycle"])

# POST /v1/generate — the deterministic scaffolder. Mounted at the same path the
# cloud service serves, so the api-gateway's TOOL_GENERATOR_URL needs no edition
# branch and the frontend talks to one endpoint shape.
app.include_router(scaffold_router, prefix="/v1")


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
