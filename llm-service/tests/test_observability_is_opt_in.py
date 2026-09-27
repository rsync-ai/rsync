"""The default stack runs no log shipper and no collector, and exports nothing.

WHY. fluent-bit and otel-collector used to start with every `docker compose up`. Their only
job is to forward to an observability backend, and prod runs none (measured: zero containers), so on prod
they tailed every container's logs and buffered them for a backend that never answered.
The collector was OOM-killed on a loop until it got a 512M cap. The six services that
export OTLP did the same, retrying against a collector with nowhere to send.

Now both containers live in the opt-in overlay `docker-compose.observability.yml`, which
also flips `OTEL_ENABLED` to "true". The base and prod files default it to "false", and
`docker logs` is the log path. This guard keeps a later edit from putting a container back
in the default stack, or turning export back on by default.

The public repo ships `docker logs` only, so the overlay is on scripts/flip/excludes.txt.
The overlay tests skip when that file is absent, keyed on the file itself. The base-stack
tests run in both trees.
"""

import os

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))

OVERLAY = "docker-compose.observability.yml"
SHIPPERS = {"fluent-bit", "otel-collector"}

# The services whose code reads OTEL_ENABLED and exports OTLP when it is true.
EXPORTERS = [
    "temporal-adapter",
    "orchestrator",
    "api-gateway",
    "llm-service",
    "tool-generator",
    "planner",
]

DEFAULT_STACK = [
    "docker-compose.yml",
    "docker-compose.prod.yml",
    "docker-compose.quickstart.yml",
    "docker-compose.staging.yml",
    "docker-compose.ci-isolate.yml",
]


class _Loader(yaml.SafeLoader):
    """docker-compose.prod.yml uses `!override`; the value under it is plain YAML."""


_Loader.add_constructor(
    "!override",
    lambda loader, node: loader.construct_mapping(node, deep=True)
    if isinstance(node, yaml.MappingNode)
    else (loader.construct_sequence(node, deep=True)
          if isinstance(node, yaml.SequenceNode) else loader.construct_scalar(node)),
)


def _load(name):
    with open(os.path.join(REPO_ROOT, name)) as fh:
        return yaml.load(fh, Loader=_Loader) or {}


def _services(name):
    return {n: s for n, s in (_load(name).get("services") or {}).items() if isinstance(s, dict)}


def _env(spec):
    """Compose accepts environment as a mapping or as a KEY=VALUE list."""
    env = spec.get("environment") or {}
    if isinstance(env, dict):
        return {k: ("" if v is None else str(v)) for k, v in env.items()}
    out = {}
    for item in env:
        key, _, value = str(item).partition("=")
        out[key] = value
    return out


def _depends(spec):
    dep = spec.get("depends_on") or []
    return set(dep) if isinstance(dep, list) else set(dep.keys())


@pytest.mark.parametrize("compose", DEFAULT_STACK)
def test_default_stack_runs_no_log_shipper_or_collector(compose):
    services = _services(compose)
    assert services, f"{compose} parsed to zero services -- the loader broke, not the file"

    declared = sorted(SHIPPERS & services.keys())
    assert not declared, (
        f"{compose} declares {declared}. They belong in {OVERLAY} only: nothing in the "
        f"default stack reads what they ship, and prod runs no backend for them."
    )

    dependents = sorted(n for n, s in services.items() if _depends(s) & SHIPPERS)
    assert not dependents, (
        f"{compose}: {dependents} depend on a shipper that the default stack no longer "
        f"defines, so `docker compose up` would refuse to start"
    )


@pytest.mark.parametrize("compose", ["docker-compose.yml", "docker-compose.prod.yml"])
def test_export_defaults_off(compose):
    services = _services(compose)
    checked = 0
    for name in EXPORTERS:
        env = _env(services.get(name) or {})
        if "OTEL_ENABLED" not in env:
            continue
        checked += 1
        assert env["OTEL_ENABLED"] == "${OTEL_ENABLED:-false}", (
            f"{compose}: {name} sets OTEL_ENABLED={env['OTEL_ENABLED']!r}. It must default "
            f"to false so the service does not export to a collector the default stack "
            f"does not run; {OVERLAY} is what turns it on."
        )
    # Denominator: every exporter carries the setting in both files today. A lower count
    # means a rename or a moved block, and a silent pass would hide it.
    assert checked == len(EXPORTERS), (
        f"{compose}: found OTEL_ENABLED on {checked} of {len(EXPORTERS)} exporting services"
    )


def _overlay_or_skip():
    if not os.path.exists(os.path.join(REPO_ROOT, OVERLAY)):
        pytest.skip(f"{OVERLAY} is not in this tree (the public cut excludes it)")
    return _services(OVERLAY)


def test_overlay_brings_both_shippers():
    services = _overlay_or_skip()
    missing = sorted(SHIPPERS - services.keys())
    assert not missing, f"{OVERLAY} no longer defines {missing}"
    for name in SHIPPERS:
        assert services[name].get("image"), f"{OVERLAY}: {name} has no image"


def test_overlay_turns_export_on_for_every_exporter():
    services = _overlay_or_skip()
    off = [n for n in EXPORTERS if _env(services.get(n) or {}).get("OTEL_ENABLED") != "true"]
    assert not off, (
        f"{OVERLAY} does not set OTEL_ENABLED=true on {off}, so applying the overlay "
        f"starts a collector those services never send to"
    )
