#!/usr/bin/env python3
"""Namespace filter: which databases or schemas a connection may see (issue #31).

Dependency-free and kept under ``public/`` (the ``shared`` Docker build
context), like ``canonical_types.py``, so a connector image can ship it with
``COPY --from=shared namespace_filter.py /app/namespace_filter.py``.

The Go twin is ``backend-orchestrator/pkg/namespacefilter``; the connection
modal previews with a TypeScript copy,
``frontend/src/lib/pipeline/namespaceFilter.ts``. All three are pinned to the
same cases in ``namespace_filter_vectors.json`` (next to this file); change the
algorithm in all of them, or in none.

Config keys (connection config values are strings):
  * ``namespace_filter_mode``: ``all`` (default when missing or empty),
    ``include`` (only these) or ``exclude`` (all except). Trimmed and
    case-insensitive. Any other value is an error: never silently ``all``.
  * ``namespace_filter_patterns``: comma-separated. Entries are trimmed and
    empty entries dropped. At most 100 patterns, each at most 128 UTF-8 bytes.
    ``*`` matches any run of characters, including none. Every other character
    is literal. A pattern must match the whole name, case-insensitively.

``include`` with no patterns is an error. ``exclude`` with no patterns keeps
everything. System namespaces are always rejected first, whatever the mode.
Nothing matching is a warning for the caller, not an error: a database may
not exist yet.

Error and warning messages are fixed text with no names or patterns in them.
"""
from dataclasses import dataclass
from typing import Any, Iterable, List, Mapping, Tuple

MODE_KEY = "namespace_filter_mode"
PATTERNS_KEY = "namespace_filter_patterns"

MODE_ALL = "all"
MODE_INCLUDE = "include"
MODE_EXCLUDE = "exclude"
MODES = (MODE_ALL, MODE_INCLUDE, MODE_EXCLUDE)

MAX_PATTERNS = 100
MAX_PATTERN_BYTES = 128

NO_MATCH_WARNING = "The namespace filter matched no databases or schemas."


class NamespaceFilterError(ValueError):
    """The filter config is invalid. The caller must fail closed."""


@dataclass(frozen=True)
class NamespaceFilter:
    mode: str
    patterns: Tuple[str, ...]
    _lowered: Tuple[str, ...] = ()

    @property
    def active(self) -> bool:
        """True when the filter can drop a non-system name."""
        return self.mode == MODE_INCLUDE or (self.mode == MODE_EXCLUDE and bool(self.patterns))


@dataclass(frozen=True)
class ApplyResult:
    kept: List[str]
    matched: int
    excluded: int
    warning: str  # empty when there is nothing to warn about


def _glob_match(pattern: str, name: str) -> bool:
    """Whole-name glob match where ``*`` is the only wildcard.

    Iterative two-pointer matcher, O(len(pattern) * len(name)) worst case.
    A backtracking regex (``.*`` per star) is exponential in the star count
    for a non-matching name, so a user-authored pattern could hang discovery.
    Both arguments must already be lowercased.
    """
    p = n = 0
    star = -1
    mark = 0
    while n < len(name):
        if p < len(pattern) and pattern[p] != "*" and pattern[p] == name[n]:
            p += 1
            n += 1
        elif p < len(pattern) and pattern[p] == "*":
            star = p
            mark = n
            p += 1
        elif star != -1:
            p = star + 1
            mark += 1
            n = mark
        else:
            return False
    while p < len(pattern) and pattern[p] == "*":
        p += 1
    return p == len(pattern)


def parse(config: Mapping[str, Any]) -> NamespaceFilter:
    """Read the filter from a connection config. Raises NamespaceFilterError."""
    raw_mode = config.get(MODE_KEY) if config else None
    raw_patterns = config.get(PATTERNS_KEY) if config else None
    if raw_mode is None:
        raw_mode = ""
    if raw_patterns is None:
        raw_patterns = ""
    if not isinstance(raw_mode, str):
        raise NamespaceFilterError("namespace_filter_mode must be a string")
    if not isinstance(raw_patterns, str):
        raise NamespaceFilterError("namespace_filter_patterns must be a comma-separated string")

    mode = raw_mode.strip().lower() or MODE_ALL
    if mode not in MODES:
        raise NamespaceFilterError("namespace_filter_mode must be one of: all, include, exclude")

    patterns = tuple(p.strip() for p in raw_patterns.split(",") if p.strip())
    if len(patterns) > MAX_PATTERNS:
        raise NamespaceFilterError(f"namespace_filter_patterns allows at most {MAX_PATTERNS} patterns")
    for p in patterns:
        if len(p.encode("utf-8", "surrogatepass")) > MAX_PATTERN_BYTES:
            raise NamespaceFilterError(
                f"each namespace_filter_patterns entry must be at most {MAX_PATTERN_BYTES} bytes"
            )
    if mode == MODE_INCLUDE and not patterns:
        raise NamespaceFilterError("namespace_filter_mode 'include' needs at least one pattern")

    return NamespaceFilter(mode=mode, patterns=patterns, _lowered=tuple(p.lower() for p in patterns))


def _matches_any(name: str, f: NamespaceFilter) -> bool:
    lowered = name.lower()
    return any(_glob_match(p, lowered) for p in f._lowered)


def allowed(name: str, f: NamespaceFilter, system_names: Iterable[str] = ()) -> bool:
    """True when ``name`` passes: not a system namespace, then the mode applies."""
    lowered = name.lower()
    if any(lowered == s.lower() for s in system_names):
        return False
    if f.mode == MODE_INCLUDE:
        return _matches_any(name, f)
    if f.mode == MODE_EXCLUDE:
        return not _matches_any(name, f)
    return True


def apply(names: Iterable[str], f: NamespaceFilter, system_names: Iterable[str] = ()) -> ApplyResult:
    """Keep the allowed names in their input order.

    ``warning`` is set when an active filter leaves nothing, so the caller can
    tell the user instead of failing.
    """
    systems = list(system_names)
    candidates = list(names)
    kept = [n for n in candidates if allowed(n, f, systems)]
    warning = NO_MATCH_WARNING if f.active and not kept else ""
    return ApplyResult(kept=kept, matched=len(kept), excluded=len(candidates) - len(kept), warning=warning)
