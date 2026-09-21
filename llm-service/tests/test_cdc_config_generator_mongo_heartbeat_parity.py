"""The advisory MongoDB CDC config must not contradict the connector that ships.

There are two places that name a MongoDB heartbeat interval, and there has to be:

  * ``_DEFAULT_HEARTBEAT_INTERVAL_MS`` in the debezium MCP connector -- the LIVE
    path. It runs in its own container and cannot import llm-service.
  * ``MONGO_HEARTBEAT_INTERVAL_MS`` in ``cdc_config_generator`` -- advisory only;
    no Go service and no frontend route starts a connector from it.

Because the generator is advisory, a divergence is wrong advice rather than an
outage -- which is exactly why nothing else would catch it. An operator who
copies the advised config and gets a different heartbeat cadence than the
platform actually uses has been handed a number that no longer describes rsync.

Heartbeats are not cosmetic here. Debezium only commits a FRESH resume token when
it emits an event from a captured collection, so an idle MongoDB source keeps
re-committing a token that ages while the oplog rolls forward; the next reconnect
is refused with ChangeStreamHistoryLost and the stream position is gone
(KI-CDC-MONGO-RESUME-TOKEN-SILENT-STALL).

Run: pytest llm-service/tests/test_cdc_config_generator_mongo_heartbeat_parity.py
"""

import importlib.util
from pathlib import Path

from src.agents.planner.cdc_config_generator import MONGO_HEARTBEAT_INTERVAL_MS

_CONNECTOR = (
    Path(__file__).resolve().parents[2]
    / "shared" / "mcp-connectors" / "internal" / "debezium"
    / "versions" / "v1.0.0" / "connector.py"
)


def _load_connector():
    """Import the connector module that actually ships.

    Pinned to versions/v1.0.0 via the same path the Docker build context uses. If
    the connector is version-bumped this fails loudly on a missing file, which is
    the correct outcome -- the new version needs to be re-pointed here, not
    silently skipped.
    """
    assert _CONNECTOR.is_file(), (
        f"{_CONNECTOR} is missing. If the debezium connector was version-bumped, "
        "point this test at the new versions/<current_version>/connector.py and "
        "re-run -- do not delete the test."
    )
    spec = importlib.util.spec_from_file_location("debezium_connector_hb_parity", _CONNECTOR)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_advised_heartbeat_interval_matches_the_connector_that_ships():
    live = _load_connector()._DEFAULT_HEARTBEAT_INTERVAL_MS
    assert MONGO_HEARTBEAT_INTERVAL_MS == live, (
        f"cdc_config_generator advises heartbeat.interval.ms="
        f"{MONGO_HEARTBEAT_INTERVAL_MS!r} but the connector that actually starts "
        f"the pipeline uses {live!r}"
    )


def test_both_sides_are_a_positive_integer_of_milliseconds():
    # Guards against the parity test passing vacuously if both sides were set to
    # "" or None together -- equal is not the same as correct.
    live = _load_connector()._DEFAULT_HEARTBEAT_INTERVAL_MS
    for name, value in (
        ("cdc_config_generator.MONGO_HEARTBEAT_INTERVAL_MS", MONGO_HEARTBEAT_INTERVAL_MS),
        ("connector._DEFAULT_HEARTBEAT_INTERVAL_MS", live),
    ):
        assert isinstance(value, str) and value.strip() == value, f"{name} = {value!r}"
        assert int(value) > 0, f"{name} = {value!r}"


def test_the_advisory_mongodb_config_actually_carries_the_heartbeat():
    """The constant existing is not the same as the generated config using it."""
    from src.agents.planner.cdc_config_generator import CDCConfigGenerator

    result = CDCConfigGenerator()._generate_from_template(
        source_type="mongodb",
        config={
            "connection_string": "mongodb+srv://u:p@cluster0.example.mongodb.net/",
            "database": "shop",
        },
        tables=["orders"],
        snapshot_mode="initial",
        connector_name="cdc-shop",
        overrides={},
    )
    assert result.success, f"template generation failed: {result.error}"
    assert result.config["heartbeat.interval.ms"] == MONGO_HEARTBEAT_INTERVAL_MS
