#!/usr/bin/env python3
"""Regenerate the capability tables in docs/connectors/reference.md from the tree.

The tables this replaces were hand-maintained and had drifted badly: they listed
four connectors that do not exist (SQLite, HubSpot, Slack, Twitter/X), omitted
thirteen that do, and described AWS S3, GCS, Shopify and Stripe as one-directional
when every one of them declares both source and destination. Nothing failed,
because a prose table has no reader that can disagree with it.

Two rules govern how connectors are discovered here, and both exist because the
obvious spelling is wrong:

1. **Discovery is `git ls-files`, never a filesystem walk.** `iter_connector_dirs`
   rglobs for `latest.json`, which on a developer machine also finds locally
   generated connectors that are deliberately untracked -- today that is four of
   them (jsonph, nasa-apod, pokeapi, rickmorty), so a walk yields 25 where the
   repo contains 21. A published reference must describe the repository, not
   whichever machine last ran the generator.

2. **The CDC column is computed from policy, not copied from metadata.** A
   connector's `supports_cdc` field is NOT what the product serves: the tools
   API overwrites it with `isCDCExposed(category, id)` (api-gateway
   `internal/handlers/tools.go:610`), which requires both a database category
   and membership of the Debezium allowlist. Databricks declares
   `supports_cdc: true` while being a `data_warehouse` with no registered CDC
   provider, so copying the field would print a capability the product refuses
   to offer. The allowlist is read from `capability_rules.yaml`, the file the
   Go map names as its own source of truth, rather than hardcoded a third time.
   That file is stripped from the public tree by `llm-service/oss-strip-list.txt`
   while THIS script ships (docs/connectors/reference.md:9 tells readers to run
   `make connector-reference`), so it falls back to parsing the Go map -- the
   other copy of the same policy, and one that does ship. Still not a third copy:
   both branches read a file that already governs the product's behaviour.

3. **Version resolution goes through the shared resolver.** The canonical source
   is `versions/<latest.json.current_version>/`, which is NOT necessarily the
   highest-numbered directory, and there are no root copies to fall back on.
   CLAUDE.md requires every reader to use `resolve_current_dir` so exactly one
   place knows the layout.

Usage:
    python3 scripts/generate_connector_reference.py            # rewrite in place
    python3 scripts/generate_connector_reference.py --check    # exit 1 if stale
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "llm-service"))

from src.utils.connector_paths import resolve_current_dir  # noqa: E402

CAPABILITY_RULES = (REPO / "llm-service/src/agents/tool_generator/config"
                    / "capability_rules.yaml")

# The same policy, in the language that serves it. Public, and the file whose
# comment names capability_rules.yaml as its counterpart -- they are kept in
# lockstep by llm-service/tests/test_connector_metadata_matches_cdc_policy.py.
GO_POLICY = REPO / "api-gateway/internal/handlers/tools.go"

# isCDCExposed() rejects every category outside this set before it even consults
# the allowlist, so a data_warehouse named "mysql" would still not expose CDC.
CDC_CATEGORIES = {"relational_db", "document_db", "wide_column_db"}

DOC = REPO / "docs" / "connectors" / "reference.md"
BEGIN = "<!-- BEGIN GENERATED: connectors -->"
END = "<!-- END GENERATED: connectors -->"

# metadata.json `category` is a machine token; these are the reader-facing names.
CATEGORY_LABELS = {
    "relational_db": "Relational database",
    "document_db": "Document database",
    "data_warehouse": "Data warehouse",
    "cloud_storage": "Object storage",
    "api_saas": "SaaS API",
    "streaming": "Streaming / CDC",
    "sample": "Demo data",
}

YES, NO = "✅", "—"


def _normalise(name: str) -> str:
    return str(name).strip().lower().replace(" ", "_").replace("-", "_")


def cdc_policy_source() -> Path:
    """Which of the two copies of the CDC allowlist this tree actually has.

    Callers that compare the two against each other must consult this first: on
    the public tree there is only one, and comparing it with itself passes while
    checking nothing.
    """
    return CAPABILITY_RULES if CAPABILITY_RULES.is_file() else GO_POLICY


def _allowlist_from_yaml() -> set[str]:
    import yaml
    rules = yaml.safe_load(CAPABILITY_RULES.read_text())
    names = rules.get("cdc_policy", {}).get("debezium_supported_databases") or []
    if not names:
        raise SystemExit(f"{CAPABILITY_RULES}: cdc_policy.debezium_supported_databases is empty")
    return {_normalise(n) for n in names}


def _allowlist_from_go() -> set[str]:
    """Parse `var debeziumSupportedDatabases = map[string]bool{...}` out of tools.go."""
    try:
        src = GO_POLICY.read_text(encoding="utf-8")
    except OSError as exc:
        raise SystemExit(
            f"neither {CAPABILITY_RULES} nor {GO_POLICY} is readable, so the CDC "
            f"allowlist cannot be determined: {exc}"
        ) from exc
    block = re.search(
        r"var debeziumSupportedDatabases = map\[string\]bool\{(.*?)\n\}", src, re.S
    )
    if not block:
        raise SystemExit(
            f"{GO_POLICY}: debeziumSupportedDatabases literal not found. It is the "
            f"only copy of the CDC allowlist in this tree "
            f"({CAPABILITY_RULES.name} is stripped from the public build), so this "
            f"is fatal rather than a fallback to a hardcoded list."
        )
    names = {_normalise(n) for n in re.findall(r'"([^"]+)":\s*true', block.group(1))}
    if len(names) < 5:
        raise SystemExit(
            f"{GO_POLICY}: parsed only {sorted(names)} from debeziumSupportedDatabases "
            f"-- the parser is stale, not the map. Refusing to publish a CDC column "
            f"computed from a truncated allowlist."
        )
    return names


def load_cdc_allowlist() -> set[str]:
    """The Debezium-supported database names, from whichever policy file exists."""
    if CAPABILITY_RULES.is_file():
        return _allowlist_from_yaml()
    return _allowlist_from_go()


def cdc_exposed(md: dict, allowlist: set[str]) -> bool:
    """Mirror of api-gateway `isCDCExposed` -- category gate, then allowlist."""
    if str(md.get("category", "")).strip().lower() not in CDC_CATEGORIES:
        return False
    ident = str(md.get("id") or md.get("name") or "")
    return ident.strip().lower().replace(" ", "_").replace("-", "_") in allowlist


def tracked_connector_dirs(subtree: str) -> list[Path]:
    """Connector root dirs under `shared/mcp-connectors/<subtree>`, tracked only.

    The pathspec `*/latest.json` is a git pathspec, whose `*` DOES cross `/`, so
    this finds nested connectors (`database/mysql`, `storage/aws-s3`) as well as
    flat ones. The identically-spelled Python glob would not, and would silently
    return ten of twenty-one.
    """
    out = subprocess.run(
        ["git", "ls-files", f"shared/mcp-connectors/{subtree}/*/latest.json"],
        cwd=REPO, capture_output=True, text=True, check=True,
    ).stdout.split()
    return sorted((REPO / p).parent for p in out)


def read_metadata(connector_dir: Path) -> dict | None:
    meta = resolve_current_dir(connector_dir) / "metadata.json"
    if not meta.is_file():
        return None
    try:
        return json.loads(meta.read_text())
    except Exception:
        return None


def row(connector_dir: Path, subtree: str, md: dict, cdc: bool) -> tuple[str, str, str]:
    """One markdown row, prefixed by its sort key (category, then name)."""
    # The ID a user types is the path below the subtree root, so nested
    # connectors read as `database/mysql` rather than a bare `mysql`.
    cid = connector_dir.relative_to(REPO / "shared/mcp-connectors" / subtree)
    name = md.get("display_name") or md.get("name") or md.get("id") or connector_dir.name
    category = CATEGORY_LABELS.get(md.get("category", ""), md.get("category") or "—")
    cells = (
        f"| {name} | `{cid}` | "
        f"{YES if md.get('supports_source') else NO} | "
        f"{YES if md.get('supports_destination') else NO} | "
        f"{YES if cdc else NO} | {category} |"
    )
    return (category, name.lower(), cells)


def build_block() -> str:
    lines: list[str] = [
        BEGIN,
        "<!-- Generated by scripts/generate_connector_reference.py. Do not edit by hand:",
        "     `make connector-reference` rewrites it, and a CI guard fails if it is stale. -->",
        "",
    ]

    allowlist = load_cdc_allowlist()

    public = []
    for d in tracked_connector_dirs("public"):
        md = read_metadata(d)
        if md is None:
            continue
        exposed = cdc_exposed(md, allowlist)
        if bool(md.get("supports_cdc")) != exposed:
            # Not fatal: the doc reports what the product serves either way. But
            # a connector whose metadata disagrees with the policy is a real
            # defect, so say so rather than silently normalising it away.
            print(f"warning: {d.name} metadata says supports_cdc="
                  f"{bool(md.get('supports_cdc'))} but policy exposes {exposed}",
                  file=sys.stderr)
        public.append(row(d, "public", md, exposed))

    lines += [
        f"### Connectors ({len(public)})",
        "",
        "| Connector | ID | Source | Destination | CDC | Category |",
        "|---|---|:--:|:--:|:--:|---|",
    ]
    lines += [cells for _, _, cells in sorted(public)]

    internal = []
    for d in tracked_connector_dirs("internal"):
        md = read_metadata(d)
        if md is None:
            continue
        # Internal components are engine parts, not selectable endpoints, so the
        # endpoint-exposure policy does not apply to them -- report what they declare.
        internal.append(row(d, "internal", md, bool(md.get("supports_cdc"))))

    lines += [
        "",
        f"### Internal components ({len(internal)})",
        "",
        "These ship with the stack and are not selectable as pipeline endpoints.",
        "",
        "| Component | ID | Source | Destination | CDC | Category |",
        "|---|---|:--:|:--:|:--:|---|",
    ]
    lines += [cells for _, _, cells in sorted(internal)]
    lines += ["", END]
    return "\n".join(lines)


def splice(doc_text: str, block: str) -> str:
    start, end = doc_text.find(BEGIN), doc_text.find(END)
    if start == -1 or end == -1:
        raise SystemExit(f"{DOC}: missing {BEGIN} / {END} markers")
    return doc_text[:start] + block + doc_text[end + len(END):]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true",
                    help="exit 1 if the doc is stale, without writing")
    args = ap.parse_args()

    current = DOC.read_text()
    updated = splice(current, build_block())

    if args.check:
        if current != updated:
            print(f"{DOC.relative_to(REPO)} is stale — run: make connector-reference",
                  file=sys.stderr)
            return 1
        print(f"{DOC.relative_to(REPO)} is up to date")
        return 0

    if current != updated:
        DOC.write_text(updated)
        print(f"rewrote {DOC.relative_to(REPO)}")
    else:
        print(f"{DOC.relative_to(REPO)} already up to date")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
