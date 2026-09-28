"""Every copy of the local-vs-remote DB host check answers shared/local_db_host_golden.json.

A LOCAL host gets TLS off by default; a REMOTE one gets TLS on. The check was written
for docker: a dotless name (``postgres``) is a compose service, a dotted one is the
internet. On Kubernetes an in-cluster service is dotted
(``mongo.sources.svc.cluster.local``), so it read as remote and got TLS forced on
against a database with no TLS listener. On GKE (v0.1.7 RC) a MongoDB connection
failed with "SSL handshake failed" and saved only from a hand-written connection
string; PostgreSQL made the same choice.

The check is copied, not shared -- each connector directory is its own Docker build
context -- so this file FINDS the copies instead of listing them, and runs each one's
real source:

  * every ``def _is_local_db_host`` in a connector's current version, and in the
    generator template connector_database.py.j2 (future generated connectors);
  * every inline ``is_local_host = (... "." not in host ...)`` expression.

The Go copies are pinned to the same file by their own tests.

Bug class: a host classifier that knows docker's service names but not Kubernetes'.
"""
from __future__ import annotations

import ast
import ipaddress
import json
import os
import re
from typing import Any, Dict, List, Optional

import pytest

_CONNECTORS = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_REPO = os.path.abspath(os.path.join(_CONNECTORS, "..", ".."))
_TEMPLATE = os.path.join(
    _REPO, "llm-service", "src", "agents", "tool_generator", "templates", "connector_database.py.j2"
)
_GOLDEN = json.load(open(os.path.join(_REPO, "shared", "local_db_host_golden.json")))

# Copies known on 2026-09-28. Discovery may find more (they are tested too); finding
# fewer means one was renamed or moved and this file would stop covering it.
_KNOWN_FUNCTIONS = {"clickhouse", "mongodb", "mysql", "oracle", "sqlserver", "redshift", "template"}
_KNOWN_INLINE = {"postgresql", "oracle", "sqlserver"}


def _current_connector_files():
    """(connector name, path) for each .py in every connector's CURRENT version."""
    for dirpath, _dirs, files in os.walk(os.path.join(_CONNECTORS, "public")):
        if "latest.json" not in files:
            continue
        with open(os.path.join(dirpath, "latest.json")) as f:
            version = json.load(f).get("current_version")
        vdir = os.path.join(dirpath, "versions", str(version))
        if not version or not os.path.isdir(vdir):
            continue
        for name in sorted(os.listdir(vdir)):
            if name.endswith(".py") and not name.startswith("test_"):
                yield os.path.basename(dirpath), os.path.join(vdir, name)


_FILES = list(_current_connector_files())


def _function_copies():
    sources = _FILES + [("template", _TEMPLATE)]
    found = {}
    for name, path in sources:
        text = open(path).read()
        lines = text.splitlines()
        start = next((i for i, l in enumerate(lines) if l.startswith("def _is_local_db_host(")), None)
        if start is None:
            continue
        end = next(
            (j for j in range(start + 1, len(lines)) if lines[j] and not lines[j][0].isspace()),
            len(lines),
        )
        body = "\n".join(lines[start:end])
        ns: Dict[str, Any] = {"ipaddress": ipaddress, "Any": Any, "Optional": Optional}
        # Module constants the function reads (redshift's _CGNAT_NET).
        for m in re.finditer(r"^(_[A-Z][A-Z0-9_]*)\s*=\s*(.+)$", text, re.M):
            if m.group(1) in body:
                exec(m.group(0), ns)
        exec(body, ns)
        found[name] = ns["_is_local_db_host"]
    return found


def _inline_copies():
    """Inline ``x = (host in (...) or "." not in host ...)`` checks, as callables."""
    found = {}
    for name, path in _FILES:
        text = open(path).read()
        if "host.docker.internal" not in text:
            continue
        for node in ast.walk(ast.parse(text)):
            if not isinstance(node, ast.Assign):
                continue
            parts = list(ast.walk(node.value))
            dotless = any(
                isinstance(c, ast.Compare)
                and isinstance(c.left, ast.Constant)
                and c.left.value == "."
                and any(isinstance(op, ast.NotIn) for op in c.ops)
                for c in parts
            )
            docker = any(isinstance(c, ast.Constant) and c.value == "host.docker.internal" for c in parts)
            if not (dotless and docker):
                continue
            names = {n.id for n in ast.walk(node.value) if isinstance(n, ast.Name)}
            code = compile(ast.Expression(node.value), path, "eval")
            key = f"{name}:{node.lineno}"
            # Callers normalise the same way: str(...).strip().lower().
            found[key] = lambda h, code=code, names=names: eval(
                code, {}, {n: str(h).strip().lower() for n in names}
            )
    return found


_FUNCS = _function_copies()
_INLINE = _inline_copies()


def _is_ip(h):
    try:
        ipaddress.ip_address(h.strip().strip("[]"))
        return True
    except ValueError:
        return False


def test_the_golden_lists_are_disjoint_and_cover_kubernetes():
    local, remote = set(_GOLDEN["local"]), set(_GOLDEN["remote"])
    assert not local & remote, f"a host is both local and remote: {local & remote}"
    assert any(h.strip().lower().endswith(".svc.cluster.local") for h in local)
    assert any(h.strip().lower().endswith(".svc") for h in local)


def test_every_known_copy_was_found():
    assert _KNOWN_FUNCTIONS <= set(_FUNCS), f"missing function copies: {_KNOWN_FUNCTIONS - set(_FUNCS)}"
    inline_names = {k.split(":")[0] for k in _INLINE}
    assert _KNOWN_INLINE <= inline_names, f"missing inline copies: {_KNOWN_INLINE - inline_names}"


@pytest.mark.parametrize("copy", sorted(_FUNCS))
def test_function_copy_matches_the_golden(copy):
    fn = _FUNCS[copy]
    wrong = [h for h in _GOLDEN["local"] if fn(h) is not True]
    wrong += [h for h in _GOLDEN["remote"] if fn(h) is not False]
    assert not wrong, f"{copy} _is_local_db_host disagrees with the golden on: {wrong}"


@pytest.mark.parametrize("copy", sorted(_INLINE))
def test_inline_copy_matches_the_golden_for_hostnames(copy):
    """Inline checks never classified IP literals by address (a private 10.x reads
    remote there) -- that predates this file, so only NAMES are compared."""
    fn = _INLINE[copy]
    local = [h for h in _GOLDEN["local"] if not _is_ip(h)]
    remote = [h for h in _GOLDEN["remote"] if not _is_ip(h)]
    assert local and remote
    wrong = [h for h in local if not fn(h)] + [h for h in remote if fn(h)]
    assert not wrong, f"{copy} inline host check disagrees with the golden on: {wrong}"


def _mongo_build_uri():
    path = next(p for n, p in _FILES if n == "mongodb" and os.path.basename(p) == "connector.py")
    text = open(path).read()
    fn = next(
        n for n in ast.walk(ast.parse(text)) if isinstance(n, ast.FunctionDef) and n.name == "_build_uri"
    )
    ns = {"Dict": Dict, "Any": Any, "List": List, "_is_local_db_host": _FUNCS["mongodb"]}
    exec(ast.get_source_segment(text, fn).replace("\n    ", "\n"), ns)
    return lambda cfg: ns["_build_uri"](None, cfg)


@pytest.mark.parametrize(
    "host,tls",
    [
        ("mongo.sources.svc.cluster.local", False),  # the GKE failure
        ("mongo.sources.svc", False),
        ("mongo", False),
        ("mongo.acme.com", True),  # a self-hosted remote keeps TLS by default
        ("cluster0.abcd1.mongodb.net", True),
    ],
)
def test_mongo_uri_for_an_in_cluster_host_has_no_forced_tls(host, tls):
    uri = _mongo_build_uri()({"host": host, "port": 27017, "user": "u", "password": "p"})
    assert ("tls=true" in uri) is tls, uri


def test_mongo_explicit_tls_still_wins_in_cluster():
    build = _mongo_build_uri()
    host = "mongo.sources.svc.cluster.local"
    assert "tls=true" in build({"host": host, "tls": "true"})
    assert "tls=true" not in build({"host": "mongo.acme.com", "tls": "disable"})
