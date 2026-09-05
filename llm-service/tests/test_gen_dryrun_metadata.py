"""RED regression for KI-GEN-DRYRUN-META — a successful deterministic
generation via the session fast-path returns an EMPTY capability ``metadata``
on the dry-run (``save_artifacts=False``) branch.

Root cause: ``agents/session_fast_path.py::try_session_fast_path`` builds its
response dict (the block that begins ``response: Dict[str, Any] = {...}``)
WITHOUT a ``metadata`` key. The capability flags the API Gateway / Frontend
read — ``supports_source``, ``supports_destination``, ``destination_modes`` —
live only in ``spec.to_metadata_dict()`` and are never attached. On the persist
path (``save_artifacts=True``) ``integration.py::_try_session_fast_path`` layers
managed-mode flags on top, but on a dry-run it returns the response untouched,
so ``metadata`` collapses to ``None``/``{}``.

The agentic pipeline already attaches ``spec.to_metadata_dict()`` unconditionally
(``integration.py`` ~line 680), so this is a fast-path/spec-first-only gap.

This drives the REAL ``try_session_fast_path`` in-process with a monkeypatched
session store and ``save_artifacts=False`` — so NOTHING is written to disk (the
persist branch is guarded on ``save_artifacts``), leaving zero repo residue.
The contract is the curated tier-1 ``pipedrive`` vendor (deterministic, no
network): resources + auth are injected from the real vendor registry inside
the fast-path exactly as production does.

RED today: ``result["metadata"]`` is missing/empty → capability flags absent.
GREEN after fix: ``result["metadata"]`` carries ``supports_source`` /
``supports_destination`` / ``destination_modes`` (i.e. ``spec.to_metadata_dict()``).
"""

from __future__ import annotations

import asyncio
import os
import sys

# tool_generator package root: .../llm-service/src/agents/tool_generator
_TOOLGEN = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),  # .../llm-service/tests
        "..",                       # .../llm-service
        "src",
        "agents",
        "tool_generator",
    )
)
if _TOOLGEN not in sys.path:
    sys.path.insert(0, _TOOLGEN)

from schemas.contract import (  # noqa: E402
    Dimension,
    Fact,
    FactSource,
    empty_contract,
)
import utils.session_store as session_store_mod  # noqa: E402
from agents import session_fast_path  # noqa: E402


_CANONICAL = "pipedrive"
_SESSION_ID = "sess-dryrun-meta"


def _pipedrive_contract():
    """A can_generate=True tier-1 pipedrive contract. The fast-path injects the
    curated resources/auth itself from vendor_apis.yaml, so we only need the four
    CRITICAL_DIMENSIONS present + evaluate()."""
    contract = empty_contract(_SESSION_ID, _CANONICAL)
    for dim, value in (
        (Dimension.PROTOCOL, "rest"),
        (Dimension.BASE_URL, "https://api.pipedrive.com/v1"),
        (Dimension.AUTH_TYPE, "pipedrive"),
        (Dimension.OPERATIONS, ["list_deals"]),
    ):
        contract.set_fact(
            Fact(dimension=dim, value=value, confidence=1.0, source=FactSource.VENDOR_YAML)
        )
    contract.evaluate()
    assert contract.can_generate, f"fixture contract not generatable: {contract.refusal_reason}"
    return contract


class _FakeStore:
    """Minimal async session store — try_session_fast_path only calls .get()."""

    def __init__(self, contract):
        self._contract = contract

    async def get(self, session_id):  # noqa: D401
        return self._contract if session_id == _SESSION_ID else None


def _run_dry_run(monkeypatch):
    """Invoke the real fast-path with save_artifacts=False (no disk writes)."""
    contract = _pipedrive_contract()

    async def _fake_get_session_store():
        return _FakeStore(contract)

    # try_session_fast_path late-imports get_session_store from utils.session_store,
    # so patching the source module's attribute is picked up at call time.
    monkeypatch.setattr(session_store_mod, "get_session_store", _fake_get_session_store)

    return asyncio.run(
        session_fast_path.try_session_fast_path(
            api_name=_CANONICAL,
            session_id=_SESSION_ID,
            canonical_id=_CANONICAL,
            save_artifacts=False,
        )
    )


def test_dryrun_fast_path_attaches_capability_metadata(monkeypatch):
    result = _run_dry_run(monkeypatch)
    assert result is not None, "fast-path fell through (returned None) for a curated tier-1 vendor"
    assert result.get("success") is True, f"generation did not succeed: {result.get('error_message')!r}"

    metadata = result.get("metadata")
    assert metadata, (
        "dry-run generation returned empty capability metadata (KI-GEN-DRYRUN-META); "
        f"got {metadata!r}"
    )
    for key in ("supports_source", "supports_destination", "destination_modes"):
        assert key in metadata, f"capability flag {key!r} absent from dry-run metadata: {sorted(metadata)}"
    assert metadata.get("id") == _CANONICAL, (
        f"metadata identity mismatch; got id={metadata.get('id')!r}"
    )


def test_dryrun_metadata_capability_flags_are_typed(monkeypatch):
    result = _run_dry_run(monkeypatch)
    metadata = (result or {}).get("metadata") or {}
    assert isinstance(metadata.get("destination_modes"), list), (
        f"destination_modes must be a list; got {type(metadata.get('destination_modes')).__name__}"
    )
    assert isinstance(metadata.get("supports_source"), bool), "supports_source must be a bool"
    assert isinstance(metadata.get("supports_destination"), bool), "supports_destination must be a bool"
    # operations come straight from spec.to_metadata_dict() — proves it's the real
    # spec metadata, not a hand-stubbed placeholder.
    assert metadata.get("operations"), "dry-run metadata carries no operations list"
