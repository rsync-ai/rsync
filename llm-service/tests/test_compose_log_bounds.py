"""Every service a customer runs must bound its log file.

A service with no `logging:` block does not get "no logging" -- it inherits the
daemon default, which is json-file with no cap. That is the failure mode this
file exists to prevent, and it is measured rather than theoretical:
rsync-ai-otel-collector was found at 106 MB with driver=json-file and
options={}, having never been given a block. On an unattended self-host box the
only bound on that is the size of the disk.

Scope is the two compose files that run STANDALONE and reach a customer:
docker-compose.yml and docker-compose.quickstart.yml. Overlays
(prod/vps-2c8g/oss/ci-isolate) legitimately patch a subset of services and
inherit the rest, so a missing block there is not a defect -- but an overlay
that declares json-file and drops the caps IS, so they are checked for that.

Deliberately NOT in scope: the e2e/*.dbs/ci-isolate/ollama stacks. Those are
test scaffolding, brought up and torn down by e2e/run_gate.sh, and never run on
a customer box. Excluded on purpose and named here so the gap is visible rather
than looking like an oversight.
"""

import os

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Standalone, customer-facing: every service must carry a bounded block.
BASE_COMPOSE = ["docker-compose.yml", "docker-compose.quickstart.yml"]

# Overlays: may omit `logging:` (they inherit), may not declare an unbounded one.
OVERLAY_COMPOSE = [
    "docker-compose.prod.yml",
    "docker-compose.vps-2c8g.yml",
    "docker-compose.oss.yml",
    "docker-compose.ci-isolate.yml",
]


class _ComposeLoader(yaml.SafeLoader):
    """docker-compose.oss.yml uses the `!override` merge tag, which SafeLoader
    rejects outright. The tag only changes how compose MERGES a key; the value
    under it is ordinary YAML, so unwrapping it is faithful for this check."""


_ComposeLoader.add_constructor(
    "!override", lambda loader, node: loader.construct_mapping(node, deep=True)
    if isinstance(node, yaml.MappingNode)
    else (loader.construct_sequence(node, deep=True)
          if isinstance(node, yaml.SequenceNode) else loader.construct_scalar(node))
)


def _services(name):
    path = os.path.join(REPO_ROOT, name)
    with open(path) as fh:
        doc = yaml.load(fh, Loader=_ComposeLoader) or {}
    return {n: s for n, s in (doc.get("services") or {}).items() if isinstance(s, dict)}


def _unbounded(spec):
    """True if this service declares json-file without both caps."""
    logging_cfg = spec.get("logging") or {}
    if logging_cfg.get("driver") != "json-file":
        return False
    opts = logging_cfg.get("options") or {}
    return not ("max-size" in opts and "max-file" in opts)


@pytest.mark.parametrize("compose", BASE_COMPOSE)
def test_every_service_declares_a_bounded_log_block(compose):
    services = _services(compose)
    assert services, f"{compose} parsed to zero services -- the loader broke, not the file"

    missing = sorted(n for n, s in services.items() if "logging" not in s)
    assert not missing, (
        f"{compose}: these services have no `logging:` block, so they inherit the "
        f"daemon default (json-file, NO max-size) and grow until the disk is full: "
        f"{missing}"
    )

    unbounded = sorted(n for n, s in services.items() if _unbounded(s))
    assert not unbounded, (
        f"{compose}: these services declare json-file without both max-size and "
        f"max-file: {unbounded}"
    )


@pytest.mark.parametrize("compose", OVERLAY_COMPOSE)
def test_no_overlay_reintroduces_an_unbounded_log_block(compose):
    path = os.path.join(REPO_ROOT, compose)
    if not os.path.exists(path):
        pytest.skip(f"{compose} not present in this checkout")
    unbounded = sorted(n for n, s in _services(compose).items() if _unbounded(s))
    assert not unbounded, (
        f"{compose} overrides logging for {unbounded} with json-file but no caps, "
        f"which REPLACES the bounded block from the base file rather than adding to it"
    )


@pytest.mark.parametrize("compose", BASE_COMPOSE + OVERLAY_COMPOSE)
def test_no_service_pushes_to_the_fluentd_driver(compose):
    """`fluentd-address: localhost:24224` + `fluentd-async: "true"` on a box that
    runs no collector is a silent no-op, not an error. The quickstart stack ships
    no fluent-bit, so on a self-host install that was every service."""
    path = os.path.join(REPO_ROOT, compose)
    if not os.path.exists(path):
        pytest.skip(f"{compose} not present in this checkout")
    offenders = sorted(
        n for n, s in _services(compose).items()
        if (s.get("logging") or {}).get("driver") == "fluentd"
    )
    assert not offenders, (
        f"{compose}: {offenders} are back on the fluentd driver, pushing to a "
        f"collector a self-host box does not run -- asynchronously, so it fails silently"
    )
