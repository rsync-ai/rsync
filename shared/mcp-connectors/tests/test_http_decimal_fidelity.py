"""Numeric values must cross the connector HTTP boundary without losing precision.

The defect this guards against was silent data corruption, not a crash.

A source ``numeric`` column arrives from psycopg2 as a ``Decimal`` -- exact. But
connectors run under FastAPI in HTTP mode (``create_http_app``), and FastAPI's
``jsonable_encoder`` maps ``Decimal -> float`` via ``ENCODERS_BY_TYPE``. That cast
happens before anything else sees the row, so the value is already wrong when it
reaches Kafka:

    source  123456789012345678.5
    wire    123456789012345680      <- float64 round-trip, no error anywhere
    dest    123456789012345680.000000000

The stdio path was never affected: it serialises with ``json.dumps(..., default=str)``,
which renders Decimal losslessly as a string. So the *only* lossy leg was HTTP -- and
HTTP is the leg every containerised deployment uses. That asymmetry is why the bug
survived: any test driving a connector over stdio exercises the safe serialiser and
passes, while production silently corrupts every decimal column it moves.

Hence two independent guards below:

  * a static one, that the neutralising assignment is present and *reachable* in every
    connector that builds a FastAPI app, and in the templates that generate them;
  * a behavioural one, that the assignment still actually works against the installed
    FastAPI -- because the fix reaches into a library global, and a future FastAPI
    could stop consulting ``ENCODERS_BY_TYPE`` and turn the fix into a silent no-op
    that the static guard would happily keep passing.

The behavioural pair includes a *control* that asserts the defect still exists without
the fix. If FastAPI ever encodes Decimal losslessly on its own, that control fails and
tells us this guard is redundant -- rather than the whole file passing for a reason
that has nothing to do with the code it is supposed to protect.
"""

import json
import pathlib
import re
import subprocess

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
PUBLIC = ROOT / "public"
TEMPLATES = (
    ROOT.parents[1] / "llm-service" / "src" / "agents" / "tool_generator" / "templates"
)

# The templates that emit a FastAPI app. Pinned, not discovered: a discovered set
# cannot tell "no template generates HTTP connectors" apart from "the template was
# renamed and now escapes this guard".
HTTP_TEMPLATES = (
    "connector_database.py.j2",
    "connector.py.j2",
    "connector_graphql.py.j2",
)

# The assignment that neutralises FastAPI's lossy Decimal encoder, anchored to the
# start of a line so a commented-out copy cannot satisfy it.
GUARD = re.compile(r"^\s*_ENCODERS_BY_TYPE\[_Decimal\] = str\s*$", re.M)
IMPORT = re.compile(
    r"^\s*from fastapi\.encoders import ENCODERS_BY_TYPE as _ENCODERS_BY_TYPE\s*$", re.M
)
# Detect FastAPI *use*, not the name of the factory that happens to wrap it. Keying
# off "create_http_app" let a connector escape the guard just by renaming that
# function -- and worse, connector_graphql.py.j2 never defines one at all: it builds
# `app = FastAPI()` inline, so every GraphQL-generated connector would have slipped
# past this file unchecked. The invariant is "imports FastAPI => pins Decimal".
BUILDS_HTTP_APP = re.compile(r"^\s*from fastapi import\b", re.M)

# Every connector tracked in git that serves over HTTP. Pinned, not merely counted:
# a >= N floor cannot tell "this connector no longer needs the guard" apart from
# "this connector fell out of the scan and is now silently unguarded". Mutation
# testing proved that gap was real -- renaming the factory dropped one connector
# from the census while the floor still passed. Add a row here when you add an
# HTTP connector; that is the point.
#
# Untracked, locally generated connectors are deliberately absent: they exist on
# dev machines but not in CI, so pinning them would make this test machine-specific.
# They are still checked -- the per-connector test runs over everything discovered,
# and this roster only asserts the floor of what must always be there.
#
# The second block below was invisible to this file until the discovery glob was
# fixed: every one of them lives under public/database/ or public/storage/, which
# `glob("*/latest.json")` cannot reach. They are pinned here so that a future
# discovery regression fails by name instead of by a quietly smaller census.
PINNED_HTTP_CONNECTORS = {
    "github-rest",
    "google-sheets",
    "notion-rest",
    "petstore",
    "postgresql",
    "redshift",
    "shopify-admin-graphql",
    "stripe",
    "widgets-graphql",
    # nested tree
    "aws-s3",
    "azure-blob",
    "bigquery",
    "clickhouse",
    "databricks",
    "gcs",
    "mongodb",
    "mysql",
    "oracle",
    "snowflake",
    "sqlserver",
}


def _tracked_connector_ids():
    """The same census, taken a completely different way: ask git.

    This exists to be a second opinion on the filesystem walk above. A walk and a
    pathspec fail differently, so a shortfall in either shows up as a difference
    between them -- which is the only reason the flat-glob shortfall below is
    detectable at all rather than being a smaller number nobody can see is wrong.
    """
    out = subprocess.run(
        ["git", "-C", str(ROOT.parents[1]), "ls-files", "shared/mcp-connectors/public"],
        capture_output=True, text=True, check=True,
    ).stdout
    ids = set()
    for line in out.splitlines():
        line = line.strip()
        if not line.endswith("/latest.json"):
            continue
        latest = ROOT.parents[1] / line
        try:
            cv = json.loads(latest.read_text())["current_version"]
        except (OSError, ValueError, KeyError):
            continue
        if (latest.parent / "versions" / cv / "connector.py").is_file():
            ids.add(latest.parent.name)
    return ids


def _current_connector_files():
    """Every connector.py that is actually shipped, per its own version pointer.

    Resolved through latest.json rather than by taking the highest versions/ dir --
    the current_version pointer is what the Docker build context uses, so it is the
    only copy that runs.
    """
    out = []
    # rglob, not glob("*/latest.json"). pathlib's `*` does not cross a directory
    # separator, so the flat form found the 10 connectors sitting directly under
    # public/ and silently skipped all 11 under public/database/ and
    # public/storage/ -- including mysql, oracle, snowflake, bigquery and
    # sqlserver, i.e. exactly the connectors whose decimal columns this file
    # exists to protect. Nothing failed; the census just got smaller.
    for latest in sorted(PUBLIC.rglob("latest.json")):
        try:
            cv = json.loads(latest.read_text())["current_version"]
        except (ValueError, KeyError):
            continue
        f = latest.parent / "versions" / cv / "connector.py"
        if f.is_file():
            out.append(f)
    return out


def _http_connector_files():
    return [f for f in _current_connector_files() if BUILDS_HTTP_APP.search(f.read_text())]


def test_the_scan_actually_finds_connectors():
    """Vacuity floor: a parser that reads nothing passes every assertion below it.

    A count of zero is not an error, so it has to be asserted against explicitly --
    and a count alone is not enough either, hence the roster comparison.
    """
    allc = _current_connector_files()
    tracked = _tracked_connector_ids()
    assert tracked, "git found no connectors at all -- the ls-files pathspec stopped matching"

    # A set difference, not a `>= N` count. The count floor here used to read
    # `>= 10`, and the scan it guarded globbed `public/*/latest.json`, which
    # finds exactly the 10 flat connectors -- so the floor was satisfied by
    # precisely the broken value it existed to reject, and 11 connectors sat
    # outside the census serving rows over HTTP unguarded. A number someone typed
    # can agree with a bug; the tree cannot.
    scanned = {p.parts[-4] for p in allc}
    invisible = sorted(tracked - scanned)
    assert not invisible, (
        f"{invisible} are tracked in git and ship a connector.py at their pinned "
        "current_version, but the filesystem scan above did not find them. Every "
        "per-connector assertion in this file skips them silently -- unguarded "
        "and uncounted at the same time, which is how this passed for months."
    )

    found = {p.parts[-4] for p in _http_connector_files()}
    missing = PINNED_HTTP_CONNECTORS - found
    assert not missing, (
        f"{sorted(missing)} no longer register as HTTP connectors. Either they stopped "
        "importing FastAPI (then delete them from PINNED_HTTP_CONNECTORS deliberately), "
        "or the detector stopped matching them -- in which case they are serving rows "
        "over HTTP completely unguarded."
    )


@pytest.mark.parametrize("path", _http_connector_files(), ids=lambda p: p.parts[-4])
def test_every_http_connector_neutralises_the_decimal_encoder(path):
    body = path.read_text()
    assert IMPORT.search(body), (
        f"{path} builds a FastAPI app but never imports ENCODERS_BY_TYPE -- every "
        "Decimal it returns will be floated"
    )
    assert GUARD.search(body), (
        f"{path} builds a FastAPI app without pinning Decimal to str, so numeric "
        "columns lose precision silently in HTTP mode"
    )


def _require_a_pre_cut_tree():
    """Delegate to ``llm-service/tests/_flip_cut.py`` -- one definition of "the cut ran".

    Loaded by path rather than imported, so nothing is added to ``sys.path``: that
    directory holds a ``conftest.py`` of its own, and putting it on the path from
    here would let it shadow this suite's.
    """
    import importlib.util

    src = ROOT.parents[1] / "llm-service" / "tests" / "_flip_cut.py"
    spec = importlib.util.spec_from_file_location("_flip_cut_for_decimal_fidelity", src)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.require_a_pre_cut_tree()


@pytest.mark.parametrize("name", HTTP_TEMPLATES)
def test_the_generator_templates_carry_the_fix_forward(name):
    """A fix only in the generated copies is one regeneration away from being gone."""
    if not TEMPLATES.is_dir():
        # `src/agents/tool_generator/templates` is oss-strip-list.txt:39, so the public
        # cut removes it along with the rest of the generator package: there is no
        # template in that tree for a regeneration to lose, and nothing here to check.
        #
        # Skip on the CUT, not on the directory being absent. "Missing => skip" is a
        # guard that disarms itself -- rename the directory in this repo and the check
        # stops running with a green tick. _flip_cut reads one witness from each of the
        # two lists the cut consumes; both gone is the public tree, both present is
        # this one (so the assertion below runs and a rename fails, which is the point),
        # and one of each is a state no procedure produces, which it fails on.
        _require_a_pre_cut_tree()
    path = TEMPLATES / name
    assert path.is_file(), f"{path} is missing -- template renamed?"
    body = path.read_text()
    assert GUARD.search(body), (
        f"{name} generates FastAPI connectors but does not pin Decimal to str; "
        "every connector regenerated from it will silently corrupt decimals again"
    )


# --- behavioural: does the mechanism still do anything? ----------------------------

fastapi_encoders = pytest.importorskip(
    "fastapi.encoders", reason="fastapi not installed in this lane"
)

# Every value here must be one float64 genuinely cannot represent -- i.e. needing more
# than ~17 significant digits. That is a narrower set than it looks: 0.123456789012 and
# -0.000000000123456 both survive a float round-trip untouched, so using them here would
# make the control assert something false. (They are still corrupted end-to-end, but by
# the destination's NUMERIC(38,9) scale, which is a separate defect -- see
# KI-CONNECTOR-UNCONSTRAINED-NUMERIC-ROUNDS-AT-9DP.) Three shapes that do overflow a
# float: sub-1 precision, large-magnitude precision, and a value whose float collapses
# to a completely different rendering.
LOSSY_VALUES = [
    "0.123456789012345678",
    "123456789012345678.5",
    "-0.100000000000000000001",
]


@pytest.fixture
def restore_encoder():
    from decimal import Decimal

    before = fastapi_encoders.ENCODERS_BY_TYPE.get(Decimal)
    yield
    if before is None:
        fastapi_encoders.ENCODERS_BY_TYPE.pop(Decimal, None)
    else:
        fastapi_encoders.ENCODERS_BY_TYPE[Decimal] = before


@pytest.mark.parametrize("raw", LOSSY_VALUES)
def test_control_fastapi_still_loses_precision_without_the_fix(raw, restore_encoder):
    """The defect must still be real, or this whole file is guarding nothing.

    If a future FastAPI encodes Decimal losslessly by itself, this fails -- which is
    the signal to retire the guard deliberately, rather than let the file keep passing
    for a reason unrelated to the code it protects.
    """
    from decimal import Decimal

    fastapi_encoders.ENCODERS_BY_TYPE[Decimal] = (
        fastapi_encoders.decimal_encoder
        if hasattr(fastapi_encoders, "decimal_encoder")
        else float
    )
    encoded = fastapi_encoders.jsonable_encoder({"v": Decimal(raw)})["v"]
    assert Decimal(str(encoded)) != Decimal(raw), (
        f"stock FastAPI now round-trips {raw} exactly ({encoded!r}); the Decimal->str "
        "pin may no longer be needed -- retire it deliberately"
    )


@pytest.mark.parametrize("raw", LOSSY_VALUES)
def test_the_fix_makes_the_round_trip_exact(raw, restore_encoder):
    """The pin must survive a real JSON round-trip, nested the way a row batch is.

    Encoding a bare Decimal is not the shape the connectors emit; the export result is
    a dict of lists of dicts, and jsonable_encoder recurses. This asserts on the
    recursive path, and asserts non-Decimal scalars are left alone -- a fix that
    stringified every number would break primary keys and row counts downstream.
    """
    from decimal import Decimal

    fastapi_encoders.ENCODERS_BY_TYPE[Decimal] = str
    payload = {
        "success": True,
        "row_count": 3,
        "data": [{"id": 9003, "amount": Decimal(raw)}],
    }
    wire = json.loads(json.dumps(fastapi_encoders.jsonable_encoder(payload)))
    got = wire["data"][0]["amount"]
    assert isinstance(got, str), f"{raw} crossed the boundary as {type(got).__name__}"
    assert Decimal(got) == Decimal(raw), f"{raw} came back as {got!r}"
    assert isinstance(wire["row_count"], int), "row_count must stay an int"
    assert isinstance(wire["data"][0]["id"], int), "integer PKs must stay ints"
    assert wire["success"] is True, "booleans must stay booleans"
