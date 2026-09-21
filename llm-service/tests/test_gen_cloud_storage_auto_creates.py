"""Regression guard: a generated CLOUD_STORAGE destination must declare
``auto_create_destination_tables``.

The flag answers "will the destination have somewhere to write without the
user pre-creating anything?", which is a WIDER question than "can it run
CREATE TABLE". An object store answers yes because it has no tables at all —
the sink writes one object per batch under a prefix.

Defaulting it from ``category == RELATIONAL_DB and supports_ddl`` made every
generated object-storage connector declare False, and the api-gateway's
pre-migration assessment then raised a BLOCKING ``SINK_NO_DDL`` error on every
table of every MongoDB -> GCS / S3 / Azure-Blob pipeline — a stop the user
could not clear, because there was no table for them to go and pre-create.

``supports_ddl`` must stay False for object storage: the kafka sink worker
gates ``ensure_table`` on ``supports_ddl AND auto_create``, so flipping it
would make the worker issue CREATE TABLE calls against a bucket.
"""

from __future__ import annotations

import sys
from pathlib import Path

_TOOLGEN = str(
    Path(__file__).resolve().parents[1] / "src" / "agents" / "tool_generator"
)
if _TOOLGEN not in sys.path:
    sys.path.insert(0, _TOOLGEN)

from schemas.spec import (  # noqa: E402
    AuthConfig,
    AuthType,
    ConnectorCategory,
    ConnectorSpec,
)


def _spec(category: ConnectorCategory, **kw) -> ConnectorSpec:
    return ConnectorSpec(
        name=kw.pop("name", "probe_connector"),
        display_name="Probe Connector",
        category=category,
        supports_destination=kw.pop("supports_destination", True),
        auth=AuthConfig(type=AuthType.NONE),
        **kw,
    )


def test_cloud_storage_destination_auto_creates_without_claiming_ddl():
    spec = _spec(ConnectorCategory.CLOUD_STORAGE)
    assert spec.auto_create_destination_tables is True
    assert spec.supports_ddl is False, "an object store must not claim DDL"

    # The flag has to survive serialisation, and in BOTH places the generator
    # writes it — the api-gateway reader checks the top level first and the
    # capabilities block second.
    meta = spec.to_metadata_dict()
    assert meta["auto_create_destination_tables"] is True
    assert meta["capabilities"]["auto_create_destination_tables"] is True
    assert meta["supports_ddl"] is False
    assert meta["capabilities"]["supports_ddl"] is False


def test_cloud_storage_source_only_does_not_auto_create():
    """Negative control on the other axis: the flag describes a DESTINATION."""
    spec = _spec(ConnectorCategory.CLOUD_STORAGE, supports_destination=False)
    assert spec.auto_create_destination_tables is False


def test_relational_destination_still_needs_ddl_to_auto_create():
    """Negative control: the relational rule is unchanged."""
    assert _spec(ConnectorCategory.RELATIONAL_DB, name="postgres_probe").supports_ddl is True
    assert _spec(ConnectorCategory.RELATIONAL_DB, name="postgres_probe").auto_create_destination_tables is True
    # An unknown dialect stays fail-closed on both flags.
    unknown = _spec(ConnectorCategory.RELATIONAL_DB, name="some_new_dialect")
    assert unknown.supports_ddl is False
    assert unknown.auto_create_destination_tables is False


def test_api_saas_destination_stays_fail_closed():
    """Negative control: nothing else was widened."""
    assert _spec(ConnectorCategory.API_SAAS).auto_create_destination_tables is False


def test_explicit_spec_value_still_wins():
    spec = _spec(ConnectorCategory.CLOUD_STORAGE, auto_create_destination_tables=False)
    assert spec.auto_create_destination_tables is False
