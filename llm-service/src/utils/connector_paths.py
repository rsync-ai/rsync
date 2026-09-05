"""Canonical connector path resolution.

The single source of truth for every MCP connector is its **current versioned
directory** — ``<connector_dir>/versions/<current_version>/`` where
``<current_version>`` comes from ``<connector_dir>/latest.json``. Nothing is
duplicated at the connector root anymore (the old ``_sync_top_level_files``
root-copy mechanism is gone); ``latest.json`` is the only file that lives at the
connector root, and it is purely a pointer.

Every reader that needs a connector artifact (``connector.py``, ``metadata.json``,
``spec.json``, ``Dockerfile``, …) must resolve it through here so there is exactly
one place that knows the layout:

    from src.utils.connector_paths import resolve_current_dir, iter_connector_dirs

    meta = resolve_current_dir(connector_dir) / "metadata.json"
    for cdir in iter_connector_dirs(connectors_root):
        ...

A connector directory is identified by the presence of ``latest.json`` (never
inside ``versions/``), at any nesting depth — connectors live under
``public/<id>``, ``public/<category>/<id>`` (e.g. ``public/database/mysql``),
and ``internal/<id>``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator, Optional, Union

PathLike = Union[str, Path]


def resolve_current_version(connector_dir: PathLike) -> Optional[str]:
    """Return the connector's current version string from ``latest.json``.

    Tolerates the historical key variants (``current_version`` / ``version`` /
    ``path: versions/<v>``). Returns ``None`` when there is no readable
    ``latest.json`` or no version can be derived.
    """
    latest_path = Path(connector_dir) / "latest.json"
    if not latest_path.exists():
        return None
    try:
        data = json.loads(latest_path.read_text())
    except Exception:
        return None
    cv = (data.get("current_version") or data.get("version") or "").strip()
    if not cv:
        # Some manifests only carry `path: "versions/<v>"`.
        cv = (data.get("path") or "").strip().removeprefix("versions/").strip("/").strip()
    return cv or None


def resolve_current_dir(connector_dir: PathLike) -> Path:
    """Resolve a connector root dir to its current versioned directory.

    Returns ``<connector_dir>/versions/<current_version>`` when that directory
    exists. Falls back to ``connector_dir`` itself for flat-layout connectors
    (no ``versions/`` subtree) or when the version can't be resolved — so callers
    never get a path that doesn't exist on disk for a real connector.
    """
    connector_dir = Path(connector_dir)
    cv = resolve_current_version(connector_dir)
    if cv:
        versioned = connector_dir / "versions" / cv
        if versioned.is_dir():
            return versioned
    return connector_dir


def iter_connector_dirs(root: PathLike) -> Iterator[Path]:
    """Yield each connector ROOT directory under ``root``.

    A connector root is any directory containing ``latest.json`` (the version
    pointer), found at any nesting depth and excluding ``versions/`` subtrees.
    """
    for latest_path in Path(root).rglob("latest.json"):
        if "versions" in latest_path.parts:
            continue
        yield latest_path.parent


def iter_current_artifact(root: PathLike, filename: str) -> Iterator[Path]:
    """Yield ``versions/<current_version>/<filename>`` for every connector under
    ``root`` whose current version actually contains ``filename``.

    Drop-in replacement for the old ``root.rglob("<filename>")`` +
    ``"versions" not in parts`` discovery idiom, but reads the canonical
    versioned copy instead of the deleted root copy.
    """
    for connector_dir in iter_connector_dirs(root):
        artifact = resolve_current_dir(connector_dir) / filename
        if artifact.exists():
            yield artifact
