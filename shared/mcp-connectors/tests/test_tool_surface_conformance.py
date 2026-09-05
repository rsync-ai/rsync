"""Every tool a connector advertises must be a tool it can actually run.

``tools/list`` is the whole surface an LLM planner sees: it plans against these names
and nothing else. Three ways that surface used to lie, all found by running the real
connectors rather than by reading them:

* six connectors raised ``KeyError: 'description'`` out of ``list_tools`` because their
  hand-written ``get_capabilities`` omitted a key the publisher indexed unguarded -- one
  malformed entry failed the ENTIRE endpoint, so bigquery, clickhouse, databricks,
  snowflake, redshift and shopify-admin-graphql advertised nothing at all;
* sqlserver advertised twelve ``mysql_*`` names copied from the mysql connector, so
  every one of its tools resolved to "Unknown tool" at call time;
* three connectors -- bigquery, redshift and shopify-admin-graphql -- defined
  ``get_capabilities()`` with no ``params``, so the name resolved, was published, and
  then raised ``TypeError: takes 1 positional argument but 2 were given`` on every
  single call. Resolution is not callability: a zero-arg method is still ``callable()``.

None of the three was visible statically: ``get_capabilities`` is overridden by every
connector, so reading the base class tells you nothing about what ships. This suite
therefore instantiates each connector for real and asserts the surface it publishes --
the rule being that an advertised name must BOTH resolve to a handler AND be able to
bind the dispatcher's ``handler(normalized_args)`` call.

Each connector is probed in its own subprocess. That is load-bearing, not tidiness:
every connector ships a module literally named ``connector.py``, so a single process
would resolve ``sys.modules['connector']`` to whichever imported first and silently
test that one N times.

Offline: connectors are constructed with no config and only metadata methods are called;
no driver connects and no network is touched.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from glob import glob

import pytest

# This file lives in shared/mcp-connectors/tests/, so the connector tree is its parent.
CONNECTOR_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# The shared BuildKit context each Dockerfile mounts as `shared`; carries
# canonical_types.py and rsync_protocol/, which several connectors import.
SHARED_PUBLIC = os.path.join(CONNECTOR_ROOT, "public")

# Connectors that are standalone MCP servers rather than BaseMCPConnector subclasses.
# They do not inherit list_tools, so this contract does not apply to them. Pinned as an
# exact set: a connector dropping out of the subclass tree must break this suite loudly
# rather than quietly stop being checked.
NON_SUBCLASS_CONNECTORS = {"debezium", "minio", "sample-data"}

# Floors. A conformance suite that silently probes nothing passes just as green as one
# that probes everything, so both the denominator and the probed count are asserted.
MIN_CONNECTORS_DISCOVERED = 20
MIN_CONNECTORS_PROBED = 18

# Modules this repo vendors into each connector's versioned directory. One of these going
# missing is vendoring drift -- a real defect -- so it must never be excused as a missing
# driver, or the suite quietly stops covering that connector.
REPO_OWNED_MODULES = {"base_connector", "canonical_types", "connector", "rsync_protocol"}

# Runs inside the connector's own directory, mirroring how the container starts it
# (WORKDIR /app, connector.py as __main__).
_PROBE = r"""
import importlib.util, inspect, json, logging, os, sys, traceback
logging.disable(logging.CRITICAL)
d, shared = sys.argv[1], sys.argv[2]

# Reimplements handle_invoke's dispatch rule -- deliberately, rather than calling the
# connector's own helper. An oracle that borrows the code under test can only ever agree
# with it; written out here, it also catches publisher and dispatcher drifting apart.
def resolves(inst, name):
    handler = name or ""
    prefix = inst.connector_type + "_"
    if handler.startswith(prefix):
        handler = handler[len(prefix):]
    if not handler or handler.startswith("_"):
        return False
    return callable(getattr(inst, handler, None))

# Resolution is not callability. _handle_tool_call resolves the handler and then calls
# handler(normalized_args) with exactly ONE positional dict, so a handler that cannot
# bind that call is advertised and raises TypeError on every invocation -- and resolves()
# above cannot see it, because a zero-arg method is still callable().
#
# The fix deliberately lives HERE and not in base_connector._resolves_to_handler (and not
# in its 25 vendored copies). Two reasons: (a) tightening the publisher would convert a
# loud "advertised tool raises" bug into a silent "tool quietly withheld" one, weakening
# the very signal this guard is being added to catch; (b) the canonical file's contract is
# that publishing and dispatch share ONE rule, and test_credential_validation_contract.py
# byte-pins _handle_tool_call across every vendored copy -- changing the predicate on the
# canonical file alone would create exactly the versioned-dir drift the repo rule forbids,
# and changing all 26 copies is a runtime surface change that fixes nothing today.
def accepts_dispatch_arity(inst, name):
    handler = name or ""
    prefix = inst.connector_type + "_"
    if handler.startswith(prefix):
        handler = handler[len(prefix):]
    fn = getattr(inst, handler, None)
    try:
        inspect.signature(fn).bind({})
        return True
    except TypeError:
        return False

os.chdir(d)
sys.path.insert(0, shared)
sys.path.insert(0, d)
try:
    if not os.path.isfile(os.path.join(d, "base_connector.py")):
        print("RESULT " + json.dumps({"subclass": False}))
        raise SystemExit(0)
    spec = importlib.util.spec_from_file_location("connector", os.path.join(d, "connector.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules["connector"] = mod
    spec.loader.exec_module(mod)
    import base_connector as bc
    subs = [o for _, o in inspect.getmembers(mod, inspect.isclass)
            if issubclass(o, bc.BaseMCPConnector) and o is not bc.BaseMCPConnector]
    concrete = [c for c in subs if not inspect.isabstract(c)]
    if not concrete:
        print("RESULT " + json.dumps({"subclass": False}))
        raise SystemExit(0)
    concrete.sort(key=lambda c: (c.__module__ != "connector", c.__name__))
    inst = concrete[0]()
    tools = inst.list_tools().get("tools", [])
    unresolved = [t["name"] for t in tools if not resolves(inst, t["name"])]
    # Declared UNION published, not published alone. Published alone is sufficient today,
    # but if _advertisable_operations ever gains an arity gate it would WITHHOLD the broken
    # tool and make this check blind again; the declared set keeps it alive through that.
    declared = [op.get("method") for op in (inst.get_capabilities().get("operations") or [])
                if isinstance(op, dict) and op.get("method")]
    # ...plus "get_capabilities" unconditionally. _handle_tool_call has NO allowlist -- it
    # resolves whatever name tools/call supplies via getattr -- so get_capabilities is
    # dispatchable on every connector even where it is neither declared nor published, and
    # the step-5 zero-arg fallback exists precisely because it is expected to be invoked.
    # Without this, the guard would have caught only shopify-admin-graphql of the three
    # connectors that shipped this exact bug: bigquery and redshift advertise 6 tools each
    # and get_capabilities is not among them.
    candidates = sorted(set(declared) | {t["name"] for t in tools} | {"get_capabilities"})
    arity_checked = [n for n in candidates if resolves(inst, n)]
    arity_mismatched = [n for n in arity_checked if not accepts_dispatch_arity(inst, n)]
    print("RESULT " + json.dumps({
        "subclass": True,
        "connector_type": inst.connector_type,
        "tools": sorted(t["name"] for t in tools),
        "missing_description": sorted(t["name"] for t in tools if not t.get("description")),
        "unresolved": sorted(unresolved),
        "arity_checked": len(arity_checked),
        "arity_mismatched": sorted(arity_mismatched),
    }))
except SystemExit:
    raise
except Exception:
    print("TRACE " + json.dumps({"tb": traceback.format_exc()}))
"""


def _discover():
    """Every connector, found by RECURSIVE glob.

    A flat listdir of public/ finds ten of twenty-four -- database/ and storage/ are
    nested a level deeper -- and reports no error while doing it.
    """
    found = []
    for manifest in sorted(glob(os.path.join(CONNECTOR_ROOT, "**", "latest.json"), recursive=True)):
        root = os.path.dirname(manifest)
        if os.sep + "versions" + os.sep in manifest:
            continue
        with open(manifest) as handle:
            current = json.load(handle)["current_version"]
        version_dir = os.path.join(root, "versions", str(current))
        if os.path.isfile(os.path.join(version_dir, "connector.py")):
            found.append((os.path.basename(root), version_dir))
    return found


CONNECTORS = _discover()


def _probe(version_dir):
    proc = subprocess.run(
        [sys.executable, "-c", _PROBE, version_dir, SHARED_PUBLIC],
        capture_output=True, text=True, timeout=300,
    )
    for line in proc.stdout.splitlines():
        if line.startswith("RESULT "):
            return json.loads(line[len("RESULT "):]), None
        if line.startswith("TRACE "):
            return None, json.loads(line[len("TRACE "):])["tb"]
    return None, (proc.stderr or proc.stdout or "probe produced no output")


def test_connector_discovery_is_not_vacuous():
    """The suite must be looking at the whole tree before any assertion below means anything."""
    assert len(CONNECTORS) >= MIN_CONNECTORS_DISCOVERED, (
        f"discovered only {len(CONNECTORS)} connectors "
        f"({[n for n, _ in CONNECTORS]}); expected at least {MIN_CONNECTORS_DISCOVERED}. "
        "A recursive-glob regression would look exactly like this."
    )


@pytest.mark.parametrize("name,version_dir", CONNECTORS, ids=[n for n, _ in CONNECTORS])
def test_advertised_tools_are_callable(name, version_dir):
    """tools/list must return, and every name in it must reach a handler."""
    result, failure = _probe(version_dir)

    if failure is not None:
        missing = re.findall(r"No module named '([^'.]+)", failure)
        if missing and not (set(missing) & REPO_OWNED_MODULES):
            pytest.skip(f"{name}: driver '{missing[-1]}' not installed here\n{failure}")
        pytest.fail(f"{name}: probing tools/list raised\n{failure}")

    if not result["subclass"]:
        assert name in NON_SUBCLASS_CONNECTORS, (
            f"{name} is no longer a BaseMCPConnector subclass, so it no longer inherits "
            "list_tools and this contract stopped covering it. If that is intended, add "
            f"it to NON_SUBCLASS_CONNECTORS; {sorted(NON_SUBCLASS_CONNECTORS)}."
        )
        pytest.skip(f"{name} is a standalone MCP server, not a BaseMCPConnector subclass")

    assert name not in NON_SUBCLASS_CONNECTORS, (
        f"{name} IS a BaseMCPConnector subclass but is listed in NON_SUBCLASS_CONNECTORS; "
        "remove it so it is actually checked."
    )
    assert result["tools"], f"{name}: tools/list published an empty tool surface"
    assert not result["unresolved"], (
        f"{name}: advertises {len(result['unresolved'])} tool(s) that handle_invoke "
        f"answers with 'Unknown tool': {result['unresolved']}"
    )
    assert result["arity_checked"] > 0, (
        f"{name}: the arity check examined 0 handlers, so its verdict is vacuous -- "
        "get_capabilities()['operations'] and tools/list were both empty of resolvable names."
    )
    assert not result["arity_mismatched"], (
        f"{name}: {len(result['arity_mismatched'])} tool(s) resolve to a handler that "
        "cannot bind the dispatcher's single positional dict, so every call raises "
        f"TypeError: {result['arity_mismatched']}. _handle_tool_call does "
        "handler(normalized_args) -- give the handler a `params` argument. bigquery, "
        "redshift and shopify-admin-graphql all shipped exactly this shape (fixed in #883)."
    )
    assert not result["missing_description"], (
        f"{name}: tool(s) published with no description for a planner to read: "
        f"{result['missing_description']}"
    )


def test_most_connectors_were_actually_probed():
    """Skips must not be able to hollow the suite out into a green no-op."""
    probed = 0
    unprobed = []
    for name, version_dir in CONNECTORS:
        result, failure = _probe(version_dir)
        if failure is None and result.get("subclass"):
            probed += 1
        else:
            unprobed.append(name)
    assert probed >= MIN_CONNECTORS_PROBED, (
        f"only {probed} of {len(CONNECTORS)} connectors were probed "
        f"(unprobed: {unprobed}); expected at least {MIN_CONNECTORS_PROBED}."
    )


# --- /invoke route: the second dispatcher, and the one with no allowlist -------

INVOKE_ROUTE = '@app.post("/invoke/{tool_name}")'
GETATTR_GUARD = 'method_name.startswith("_")'


def _invoke_connectors():
    """Connectors whose FastAPI app exposes the direct /invoke route."""
    out = []
    for name, version_dir in CONNECTORS:
        with open(os.path.join(version_dir, "connector.py"), encoding="utf-8") as handle:
            if INVOKE_ROUTE in handle.read():
                out.append((name, version_dir))
    return out


INVOKE_CONNECTORS = _invoke_connectors()

# debezium reaches its tools through _dispatch_tool, which resolves names against an
# explicit allowlist instead of getattr. There is no getattr to guard, so the textual
# guard below would be meaningless there. Any OTHER connector claiming this exemption
# has to earn it the same way -- by not calling getattr on a caller-supplied name.
ALLOWLIST_DISPATCH = {"debezium"}


def test_invoke_route_discovery_is_not_vacuous():
    """An empty set would let every assertion below pass while checking nothing."""
    assert len(INVOKE_CONNECTORS) >= 10, (
        f"found only {len(INVOKE_CONNECTORS)} connectors exposing {INVOKE_ROUTE} "
        f"({[n for n, _ in INVOKE_CONNECTORS]}); expected at least 10."
    )


@pytest.mark.parametrize("name,version_dir", INVOKE_CONNECTORS,
                         ids=[n for n, _ in INVOKE_CONNECTORS])
def test_invoke_route_refuses_private_methods(name, version_dir):
    """/invoke bypasses _handle_tool_call, so it needs its own copy of that guard.

    Without it, POST /invoke/_cleanup_worker resolves through getattr and hands any
    caller that can reach the container a private method. This is the check that was
    missing when stripe shipped the route unguarded: the fix had been applied to
    sixteen connectors and to the generator template, and nothing failed for the
    seventeenth, because no test looked at the route at all.
    """
    with open(os.path.join(version_dir, "connector.py"), encoding="utf-8") as handle:
        source = handle.read()

    if name in ALLOWLIST_DISPATCH:
        assert "_dispatch_tool(" in source, (
            f"{name} is exempted from the getattr guard on the grounds that it "
            "dispatches through an allowlist, but _dispatch_tool is gone. Either "
            "restore it or drop the exemption and add the guard."
        )
        assert "getattr(server, method_name)" not in source, (
            f"{name} claims allowlist dispatch but still calls getattr on a "
            "caller-supplied name."
        )
        return

    assert GETATTR_GUARD in source, (
        f"{name} exposes {INVOKE_ROUTE} and resolves the tool name with getattr, "
        f"but never rejects names starting with '_'. POST /invoke/_get_config "
        "would reach a private method."
    )
