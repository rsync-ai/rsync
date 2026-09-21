"""The advisory CDC-config generator must not hand out an Atlas-broken MongoDB URI.

``CDCConfigGenerator._generate_from_template`` built ``mongodb.connection.string``
from ``host``/``port`` unconditionally. MongoDB Atlas — the deployment a company
adopting this repo almost certainly has — is reachable only as ``mongodb+srv://…``:
SRV resolves the replica-set seed list, so there is no single host:port, and the
generator's own ``host`` fallback is the literal ``"localhost"``. The result was
``mongodb://localhost:27017/?authSource=admin``: wrong scheme, no TLS, wrong
topology, presented as the configuration to use.

This module is ADVISORY — no Go service and no frontend route calls
``/generate-cdc-config``; the config that actually starts a connector is built by
``_build_connector_config`` in the debezium MCP connector. So the blast radius is
wrong advice, not a stalled pipeline. The lockstep test at the bottom is what
keeps that distinction honest: the docstring in the generator claims it mirrors
the connector, and a claim nothing checks drifts.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.agents.planner.cdc_config_generator import CDCConfigGenerator  # noqa: E402
from src.utils.connector_paths import resolve_current_dir  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
# Resolved through latest.json by the repo's shared resolver rather than naming
# versions/v1.0.0: per CLAUDE.md the canonical source is
# versions/<current_version>, so a pinned version dir stops pointing at the code
# that runs the moment anyone bumps it.
DEBEZIUM_CONNECTOR = (
    resolve_current_dir(REPO_ROOT / "shared/mcp-connectors/internal/debezium")
    / "connector.py"
)

ATLAS_URI = "mongodb+srv://svc:pw@cluster0.abcd.mongodb.net/?retryWrites=true&w=majority"

# The four keys the debezium MCP connector reads as an explicit URI, and which
# mongodb's metadata.json declares as config_aliases for `host`.
URI_KEYS = ("connection_string", "mongodb_connection_string", "mongodb_uri", "uri")


@pytest.fixture
def generator():
    return CDCConfigGenerator()


def _mongo_config(generator, connection_config):
    result = generator._generate_from_template(
        source_type="mongodb",
        config=connection_config,
        tables=["orders", "customers"],
        snapshot_mode="initial",
        connector_name="cdc-shop",
        overrides={},
    )
    assert result.success, f"template generation failed: {result.error}"
    return result.config


@pytest.mark.parametrize("uri_key", URI_KEYS)
def test_an_explicit_atlas_uri_is_used_verbatim(generator, uri_key):
    cfg = _mongo_config(generator, {uri_key: ATLAS_URI, "database": "shop"})
    assert cfg["mongodb.connection.string"] == ATLAS_URI, (
        f"{uri_key} was supplied but the generator synthesised a URI instead; "
        "an SRV seed list cannot be reconstructed from host:port"
    )


def test_an_atlas_connection_never_degrades_to_plaintext_localhost(generator):
    # The exact shape the bug produced: no host key at all, because Atlas users
    # have nothing to put there.
    cfg = _mongo_config(generator, {"connection_string": ATLAS_URI, "database": "shop"})
    uri = cfg["mongodb.connection.string"]
    assert uri.startswith("mongodb+srv://"), f"scheme was downgraded: {uri}"
    assert "localhost" not in uri, f"the host fallback leaked into the URI: {uri}"


def test_a_self_hosted_host_port_connection_still_works(generator):
    """Regression floor — the pre-existing discrete-field path must be untouched."""
    cfg = _mongo_config(
        generator,
        {"host": "mongo.internal", "port": 27017, "username": "svc",
         "password": "pw", "database": "shop"},
    )
    uri = cfg["mongodb.connection.string"]
    assert uri.startswith("mongodb://svc:pw@mongo.internal:27017/"), uri
    assert "authSource=admin" in uri
    assert "tls=true" not in uri, "a plain self-hosted deployment must not be forced onto TLS"


def test_an_atlas_shaped_host_turns_tls_on(generator):
    """A host on mongodb.net mandates TLS; the old branch never set it."""
    cfg = _mongo_config(
        generator,
        {"host": "cluster0-shard-00-00.abcd.mongodb.net", "port": 27017,
         "username": "svc", "password": "pw", "database": "shop"},
    )
    assert "tls=true" in cfg["mongodb.connection.string"]


def test_an_explicit_sslmode_turns_tls_on(generator):
    cfg = _mongo_config(
        generator,
        {"host": "mongo.internal", "port": 27017, "sslmode": "require",
         "username": "svc", "password": "pw", "database": "shop"},
    )
    assert "tls=true" in cfg["mongodb.connection.string"]


def test_credentials_are_percent_encoded(generator):
    """A password with a reserved character must not corrupt the URI's structure."""
    cfg = _mongo_config(
        generator,
        {"host": "mongo.internal", "port": 27017, "username": "svc",
         "password": "p@ss/w:rd", "database": "shop"},
    )
    uri = cfg["mongodb.connection.string"]
    assert "p%40ss%2Fw%3Ard" in uri, uri
    # Exactly one '@' — the userinfo separator. A raw '@' in the password would
    # add a second and re-point the URI at a different host entirely.
    assert uri.count("@") == 1, uri


def test_the_relational_only_keys_are_absent(generator):
    """MongoDB has no schema history or DDL stream; those keys fail validation."""
    cfg = _mongo_config(generator, {"connection_string": ATLAS_URI, "database": "shop"})
    assert cfg["capture.mode"] == "change_streams_update_full"
    for key in ("database.hostname", "database.port", "mongodb.hosts", "mongodb.name"):
        assert key not in cfg, f"{key} was removed in Debezium 2.x and fails validation"


def test_it_stays_in_lockstep_with_the_connector_that_actually_runs():
    """The generator's docstring claims it mirrors the debezium MCP connector.

    A claim nothing checks drifts. This asserts the narrow thing that matters:
    both sides accept the SAME set of explicit-URI keys. If the connector learns a
    fifth key, the advice here silently stops matching what a user's connection
    will actually do.
    """
    source = DEBEZIUM_CONNECTOR.read_text()
    # Vacuity floor: a moved file would make every `in` check below pass trivially
    # on an empty string, or fail for the wrong reason.
    assert len(source) > 10_000, (
        f"{DEBEZIUM_CONNECTOR} read back as {len(source)} bytes — that is not the "
        "connector, and the assertions below would be meaningless"
    )
    assert "mongodb.connection.string" in source, "the connector no longer sets the key under test"

    for key in URI_KEYS:
        assert f'"{key}"' in source, (
            f"the generator accepts {key!r} as an explicit MongoDB URI but the "
            "debezium connector does not read it — advice that cannot be acted on"
        )


def test_a_single_database_scopes_the_change_stream(generator):
    """#19: Debezium's default cluster-wide change stream is refused to a user with
    read on only the source database, and the task stays RUNNING while writing
    nothing. The advice must scope the stream the way the running connector does."""
    cfg = _mongo_config(generator, {"connection_string": ATLAS_URI, "database": "shop"})
    assert cfg["collection.include.list"] == "shop.orders,shop.customers"
    assert cfg["capture.scope"] == "database"
    assert cfg["capture.target"] == "shop"


@pytest.mark.parametrize(
    "include_list,want",
    [
        ("shop.orders", {"capture.scope": "database", "capture.target": "shop"}),
        ("shop.orders.archive", {"capture.scope": "database", "capture.target": "shop"}),
        ("shop.orders,crm.contacts", {}),
        ("shop.orders,orders", {}),
        (r"shop\.orders", {}),
        ("", {}),
    ],
)
def test_capture_scope_mirrors_the_connector_rule(include_list, want):
    assert CDCConfigGenerator._mongo_capture_scope(include_list) == want


def test_the_connector_scopes_mongodb_change_streams_too():
    """Lockstep for the #19 rule: the advice must not scope a stream the running
    connector leaves cluster-wide, or the reverse."""
    source = DEBEZIUM_CONNECTOR.read_text()
    assert len(source) > 10_000
    assert 'cfg["capture.scope"] = "database"' in source
    assert "def mongo_capture_databases(" in source
