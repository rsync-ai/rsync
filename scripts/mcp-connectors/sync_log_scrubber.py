#!/usr/bin/env python3
"""Propagate the sensitive-data log scrubber from the canonical root
base_connector.py into every versioned base_connector.py copy.

Why: connectors run from their versioned snapshot dir
(server_manager.go resolves versions/<v>/), and those snapshots are frozen at
generation time — most DB/storage connectors carry a base that predates the
scrubber. This script injects the scrubber block (delimited by the
RSYNC_LOG_SCRUBBER_BEGIN/END markers in the root) and attaches
SensitiveDataScrubbingFilter to each copy's module logger + setup_traced_logging.

Safety:
  * additive only — it adds a logging.Filter that redacts log output; it cannot
    change pipeline behaviour (worst case: a log line is over-redacted).
  * idempotent — re-running replaces the marked block in place (keeps lockstep).
  * self-verifying — every patched file must ast.parse before it is written.

Usage:
    python3 scripts/mcp-connectors/sync_log_scrubber.py [--check]
    --check : report drift and exit non-zero if any copy is missing/stale
              (no writes) — suitable for CI.
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
ROOT = REPO / "shared" / "mcp-connectors" / "base_connector.py"
BEGIN = "# >>> RSYNC_LOG_SCRUBBER_BEGIN"
END = "# >>> RSYNC_LOG_SCRUBBER_END"

MOD_LOGGER_RE = re.compile(r"^logger = logging\.getLogger\([^\n]*\)[^\n]*$", re.M)
SETUP_TRACE_RE = re.compile(
    r"^(?P<i>[ \t]*)logger_instance\.addFilter\(TraceContextFilter\(\)\)[ \t]*$", re.M
)
BLOCK_RE = re.compile(re.escape(BEGIN) + r".*?" + re.escape(END), re.DOTALL)
MOD_ADDFILTER = "logger.addFilter(SensitiveDataScrubbingFilter())"
SETUP_ADDFILTER = "logger_instance.addFilter(SensitiveDataScrubbingFilter())"


def canonical_block() -> str:
    m = BLOCK_RE.search(ROOT.read_text())
    if not m:
        sys.exit(f"FATAL: scrubber markers not found in {ROOT}")
    return m.group(0)


def patch(text: str, block: str) -> str | None:
    """Return patched text, or None if the module-logger anchor is missing."""
    if BEGIN in text:
        text = BLOCK_RE.sub(lambda _: block, text)  # re-sync in place
    else:
        m = MOD_LOGGER_RE.search(text)
        if not m:
            return None
        text = text[: m.start()] + block + "\n\n\n" + text[m.start():]

    if MOD_ADDFILTER not in text:
        m = MOD_LOGGER_RE.search(text)
        text = text[: m.end()] + "\n" + MOD_ADDFILTER + text[m.end():]

    if SETUP_ADDFILTER not in text:
        text = SETUP_TRACE_RE.sub(
            lambda m: m.group(0) + "\n" + m.group("i") + SETUP_ADDFILTER, text, count=1
        )
    return text


def versioned_copies() -> list[Path]:
    root_real = ROOT.resolve()
    return sorted(
        p for p in REPO.glob("shared/mcp-connectors/**/base_connector.py")
        if p.resolve() != root_real
    )


def main() -> int:
    check = "--check" in sys.argv
    block = canonical_block()
    copies = versioned_copies()
    changed, ok, failed, drift = 0, 0, 0, []

    for p in copies:
        original = p.read_text()
        patched = patch(original, block)
        if patched is None:
            print(f"SKIP (no logger anchor): {p.relative_to(REPO)}")
            failed += 1
            continue
        try:
            ast.parse(patched)
        except SyntaxError as e:
            print(f"PARSE-FAIL (not written): {p.relative_to(REPO)}: {e}")
            failed += 1
            continue
        if patched != original:
            drift.append(str(p.relative_to(REPO)))
            if not check:
                p.write_text(patched)
                changed += 1
        else:
            ok += 1

    print(f"\ncopies={len(copies)} up-to-date={ok} "
          f"{'drifted' if check else 'patched'}={len(drift)} failed={failed}")
    if check and drift:
        print("DRIFT:", *(f"\n  - {d}" for d in drift), sep="")
        return 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
