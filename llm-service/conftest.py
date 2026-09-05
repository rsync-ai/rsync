import os
import sys


def _ensure_repo_paths_on_syspath() -> None:
    """
    Make llm-service tests runnable in different environments (docker, host, CI).

    - `agents.*` modules live under `llm-service/src/agents`
    - `base_connector` lives under `shared/mcp-connectors`
    - `src.*` modules live under `llm-service/src`
    """
    here = os.path.dirname(__file__)
    src_dir = os.path.join(here, "src")
    shared_connectors_dir = os.path.abspath(os.path.join(here, "..", "shared", "mcp-connectors"))

    # Add BOTH:
    # - repo root (`here`) so `import src.*` resolves to ./src/...
    # - src_dir so `import agents.*` resolves to ./src/agents/...
    for p in (here, src_dir, shared_connectors_dir):
        if p and p not in sys.path:
            sys.path.insert(0, p)

    # Some installed dependencies/plugins may import a different `agents` module
    # before our tests run. Ensure we always resolve `agents.*` from our repo.
    for k in list(sys.modules.keys()):
        if k == "agents" or k.startswith("agents."):
            del sys.modules[k]

    # Ensure we don't accidentally resolve `src.*` from an installed dependency.
    for k in list(sys.modules.keys()):
        if k == "src" or k.startswith("src."):
            del sys.modules[k]

    if os.getenv("RSYNC_PYTEST_DEBUG_PATH") == "1":
        try:
            import importlib.util

            print("[llm-service][pytest] sys.path[:8] =", sys.path[:8])
            print("[llm-service][pytest] agents spec =", importlib.util.find_spec("agents"))
            print("[llm-service][pytest] agents.topology spec =", importlib.util.find_spec("agents.topology"))
            print(
                "[llm-service][pytest] agents.connector_resolver spec =",
                importlib.util.find_spec("agents.connector_resolver"),
            )
            print("[llm-service][pytest] src spec =", importlib.util.find_spec("src"))
            print("[llm-service][pytest] src.utils.telemetry spec =", importlib.util.find_spec("src.utils.telemetry"))
        except Exception as e:
            print("[llm-service][pytest] debug failed:", e)

_ensure_repo_paths_on_syspath()

