"""The CDC config generator must actually find the connectors it walks.

``CDCConfigGenerator._load_connector_metadata`` used to do
``connectors_path.iterdir()`` looking for ``<connector>/metadata.json``. Both
halves are wrong for this layout — connectors are nested under
``public/``/``internal/``/``oauth/`` (sometimes one level deeper still, e.g.
``public/database/mysql``), and the canonical ``metadata.json`` lives at
``versions/<current_version>/`` since the root-copy mechanism was deleted. It
found zero connectors out of twenty-eight, on every deployment, since the
layout changed.

Nothing raised, and that is the whole point: an empty metadata map is
indistinguishable from "no connector declares CDC support". The
metadata-driven branch of ``generate_config`` simply never fired and every
source fell through to the built-in template, which is also what a correct
run looks like today. A count of zero has to be asserted against, because it
will never announce itself.

The second test guards the other direction. Fixing the loader alone would have
made ``GET /cdc/supported-databases`` start advertising ``databricks``,
``debezium`` and ``kafka-mcp-sink`` — none of them databases — because that
method gated on ``supports_cdc`` alone while ``generate_config`` required
``supports_cdc`` *and* ``cdc_config_template``. A connector reaching the API
with ``"connector_class": null`` is worse than the silence it replaced.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.agents.planner.cdc_config_generator import CDCConfigGenerator
from src.utils.connector_paths import iter_connector_dirs

_REPO = Path(__file__).resolve().parents[2]
_CONNECTORS_ROOT = _REPO / "shared" / "mcp-connectors"

# Connectors that carry supports_cdc but are not databases and must never be
# offered as a CDC source. databricks is a warehouse destination; debezium and
# kafka-mcp-sink are pipeline infrastructure.
_NOT_DATABASES = ("databricks", "debezium", "kafka-mcp-sink")


def _generator() -> CDCConfigGenerator:
    return CDCConfigGenerator(connectors_dir=str(_CONNECTORS_ROOT))


def test_the_tree_and_the_resolver_both_work():
    """Non-vacuity floor: a green suite below must mean something.

    If the checkout has no connectors, or the resolver stops finding them, every
    other assertion here passes for the wrong reason.
    """
    roots = list(iter_connector_dirs(_CONNECTORS_ROOT))
    assert len(roots) >= 20, (
        f"only {len(roots)} connector roots under {_CONNECTORS_ROOT} — the tree or "
        "the resolver is broken, so the rest of this file proves nothing"
    )
    names = {d.name for d in roots}
    for expected in ("postgresql", "mysql", "kafka-mcp-sink"):
        assert expected in names, f"{expected!r} missing from the connector tree"


def test_the_loader_finds_connectors():
    """The defect itself: the walk returned zero and nothing complained."""
    generator = _generator()
    found = len(generator.connector_metadata)

    assert found > 0, (
        "CDCConfigGenerator loaded metadata for 0 connectors. The tree is present "
        "(the floor test above proves it), so the walk is not reaching the "
        "canonical versions/<current_version>/metadata.json — resolve it through "
        "src.utils.connector_paths."
    )
    # Named connectors, not just a count: a walk that found only the four
    # untracked scratch dirs a developer happens to have would still be broken.
    for expected in ("postgresql", "mysql", "kafka-mcp-sink"):
        assert expected in generator.connector_metadata, (
            f"{expected!r} has metadata on disk but the loader did not find it"
        )


def test_the_loader_can_answer_both_ways(tmp_path):
    """Two-sided probe, so a pass above is 'found them', not 'found anything'.

    Same code, a directory with no connectors in it: the count must go to zero.
    A loader that reported a non-zero count here would make the test above
    unfalsifiable.
    """
    empty = tmp_path / "no-connectors"
    (empty / "public" / "decoy").mkdir(parents=True)
    # A decoy metadata.json in the shape the OLD code looked for. It must NOT be
    # picked up: without latest.json this is not a connector.
    (empty / "public" / "decoy" / "metadata.json").write_text('{"supports_cdc": true}')

    assert len(CDCConfigGenerator(connectors_dir=str(empty)).connector_metadata) == 0
    assert len(_generator().connector_metadata) > 0


@pytest.mark.parametrize("name", _NOT_DATABASES)
def test_a_non_database_is_never_offered_as_a_cdc_source(name):
    """The regression the loader fix would otherwise have introduced."""
    generator = _generator()
    if name not in generator.connector_metadata:
        pytest.skip(f"{name} is not in this checkout")
    if not generator.connector_metadata[name].get("supports_cdc"):
        pytest.skip(f"{name} no longer declares supports_cdc — this guard is moot")

    supported = generator.get_supported_databases()
    assert name not in supported, (
        f"{name!r} is offered by GET /cdc/supported-databases but it is not a "
        "database. get_supported_databases must require cdc_config_template, not "
        "just supports_cdc, before naming a connector it does not have a "
        "built-in template for."
    )


def test_every_offered_database_has_a_connector_class():
    """The user-facing invariant, independent of which connectors exist.

    planner/service.py returns this map verbatim over HTTP. An entry whose
    connector_class is null is not a usable CDC source; it is a promise the
    executor cannot keep.
    """
    supported = _generator().get_supported_databases()
    assert supported, "get_supported_databases returned nothing — the floor is gone"

    null_class = sorted(n for n, v in supported.items() if not v.get("connector_class"))
    assert not null_class, (
        f"{len(null_class)} database(s) offered with no connector_class: {null_class}. "
        "Each would fail at connector-creation time with a null class."
    )
