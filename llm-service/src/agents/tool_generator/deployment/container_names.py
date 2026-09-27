"""MCP connector container names, STACK_PREFIX-aware.

A connector container is named ``<STACK_PREFIX>-<id>-vX-Y-Z-mcp``. STACK_PREFIX is
``rsync-ai`` on a default stack and something else (``rsync-ci``) on an isolated stack
that shares the Docker host with it. The orchestrator builds the same names
(backend-orchestrator internal/mcp/server_manager.go StackPrefix and
containerNameCandidates) and connector-deployer parses them (internal/dockerx/compose.go
parseContainerName), so all three have to agree. A hard-coded ``rsync-ai-`` made
tool-generator start and tear down containers belonging to the OTHER stack
(KI-MCP-REDEPLOY-IGNORES-STACK-PREFIX-AND-FAILED-DEPLOY-READS-AS-SUCCESS).

The unversioned network aliases (``rsync-ai-<id>-mcp``) are NOT built here: they stay
literal on every stack because kafka-mcp-sink resolves destinations from a hard-coded
candidate list that does not read STACK_PREFIX (scripts/mcp_generate_compose.py).
"""

import os
import re
from typing import List, Optional, Tuple

DEFAULT_STACK_PREFIX = "rsync-ai"


def stack_prefix() -> str:
    """The running stack's container-name prefix (default ``rsync-ai``)."""
    return (os.getenv("STACK_PREFIX") or "").strip() or DEFAULT_STACK_PREFIX


def versioned_container_name(connector_id: str, version: str) -> str:
    """``<prefix>-<id>-vX-Y-Z-mcp`` for a concrete version (``v1.0.0`` or ``1.0.0``)."""
    version_part = version.lstrip("v").replace(".", "-")
    return f"{stack_prefix()}-{connector_id}-v{version_part}-mcp"


def teardown_name_patterns(*connector_ids: str) -> Tuple[List[str], List[str]]:
    """This stack's names to remove for a connector: (versioned-name prefixes, legacy
    stable names). Each id is tried as given and with ``-`` → ``_`` (the legacy
    underscore variant). Scoped to STACK_PREFIX so a teardown on one stack never
    matches another stack's containers on the same Docker host."""
    prefix = stack_prefix()
    ids: List[str] = []
    for cid in connector_ids:
        for variant in (cid, cid.replace("-", "_")):
            if variant and variant not in ids:
                ids.append(variant)
    versioned = [f"{prefix}-{cid}-v" for cid in ids]
    legacy = [f"{prefix}-{cid}-mcp" for cid in ids]
    return versioned, legacy


def strip_stack_affixes(container_name: str) -> Optional[str]:
    """``<prefix>-<core>-mcp`` → ``<core>``, or None when the name is not this stack's."""
    prefix = stack_prefix() + "-"
    name = (container_name or "").strip()
    if not (name.startswith(prefix) and name.endswith("-mcp")):
        return None
    return name[len(prefix):-len("-mcp")]


def parse_versioned_container_name(container_name: str) -> Optional[Tuple[str, str]]:
    """``<prefix>-<id>-vX-Y-Z-mcp`` → ``(id, "X-Y-Z")``; None if not this stack's."""
    core = strip_stack_affixes(container_name)
    if core is None:
        return None
    m = re.match(r"^(.*)-v(\d+-\d+-\d+)$", core)
    if not m:
        return None
    return m.group(1), m.group(2)
