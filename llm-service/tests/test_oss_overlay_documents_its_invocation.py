"""`docker-compose.oss.yml` is an OVERLAY. Rendering it alone fails:

    $ docker compose -f docker-compose.oss.yml config --services
    service "connector-seed" refers to undefined volume mcp_connectors: invalid compose project

That is correct behaviour -- the overlay deliberately inherits `mcp_connectors`
and `rsync-ai-mcp` from the base it is layered onto, because declaring its own
copies would give it an EMPTY volume that `connector-seed` would seed and the
orchestrator would never read. What is not correct is a reader having to
discover that from a compose error, so the file's header carries the required
invocation.

A header is documentation, and documentation rots silently. This guard pins the
three ways it can go stale, each of which has already happened once:

1.  The header named `--scale context7-mcp=0`, but `context7-mcp` carries
    `profiles: ["generate"]` in the base and is absent from the default
    project -- the flag scaled a service the rendered project does not contain.
2.  The header did not say WHICH base. `docker-compose.yml` (cloud) has no
    `mcp_connectors` volume, so layering onto it fails; only the quickstart
    stack declares it.
3.  A future edit could add a top-level `volumes:`/`networks:` block to the
    overlay to "fix" the standalone render, silently splitting the volume.

Text-only: pure YAML/regex, no `docker` shell-out, so it self-gates under
ci.yml's `pytest tests/` glob and its `docker-compose*.yml` paths filter.
"""

import os
import re

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
OVERLAY = "docker-compose.oss.yml"


class _ComposeLoader(yaml.SafeLoader):
    """Compose's `!override` merge tag is not standard YAML.

    `docker-compose.oss.yml` uses it on `tool-generator.depends_on`.
    `yaml.safe_load` raises ConstructorError on it, so a guard that wrapped the
    parse in try/except would skip its own subject and still report green.
    `test_the_overlay_actually_parsed` below pins that it does not.
    """


def _passthrough(loader, node):
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)
    return loader.construct_scalar(node)


for _tag in ("!override", "!reset"):
    _ComposeLoader.add_constructor(_tag, _passthrough)


def _read(name):
    with open(os.path.join(REPO_ROOT, name), encoding="utf-8") as fh:
        return fh.read()


def _load(name):
    return yaml.load(_read(name), Loader=_ComposeLoader) or {}


def _header(name):
    """The leading comment block, up to the first non-comment/non-blank line."""
    lines = []
    for line in _read(name).splitlines():
        if line.startswith("#") or not line.strip():
            lines.append(line)
            continue
        break
    return "\n".join(lines)


# `docker compose -f A -f B ...` invocations documented in the header. Compose
# args may be wrapped over a `\` continuation, so join the header first.
_INVOCATION = re.compile(r"docker compose\s+((?:-f\s+\S+\s*)+)")


def _documented_invocations():
    text = _header(OVERLAY).replace("#", " ")
    text = re.sub(r"\\\s*\n", " ", text)
    text = re.sub(r"\n", " ", text)
    out = []
    for m in _INVOCATION.finditer(text):
        files = re.findall(r"-f\s+(\S+)", m.group(1))
        if OVERLAY in files:
            out.append(files)
    return out


def _base_files():
    """Every file the header layers UNDER the overlay, deduped, order-preserved."""
    seen, bases = set(), []
    for files in _documented_invocations():
        for f in files:
            if f != OVERLAY and f not in seen:
                seen.add(f)
                bases.append(f)
    return bases


# ---------------------------------------------------------------------------
# Denominators. Every assertion below is parametrised over a discovered set; if
# discovery silently returned nothing, the whole file would pass vacuously.
# ---------------------------------------------------------------------------


def test_the_overlay_actually_parsed():
    services = _load(OVERLAY).get("services")
    assert services, (
        f"{OVERLAY} parsed to no services; the !override constructor regressed "
        "and this guard is checking an empty document"
    )
    assert len(services) >= 3, f"expected >=3 overlay services, got {sorted(services)}"


def test_the_header_documents_at_least_one_invocation():
    invocations = _documented_invocations()
    assert invocations, (
        f"{OVERLAY} is not standalone-renderable, so its header MUST show a "
        "`docker compose -f <base> -f docker-compose.oss.yml` invocation. "
        "None found."
    )
    assert _base_files(), "documented invocations name no base file to layer under"


# ---------------------------------------------------------------------------
# 1. Every base the header names must satisfy what the overlay inherits.
# ---------------------------------------------------------------------------


def _inherited_volumes():
    """Named-volume refs in the overlay (`name:/path`, not `/abs/path:/path`)."""
    names = set()
    for svc in _load(OVERLAY).get("services", {}).values():
        for mount in svc.get("volumes", []) or []:
            if isinstance(mount, str) and not mount.startswith(("/", ".", "~")):
                names.add(mount.split(":", 1)[0])
            elif isinstance(mount, dict) and mount.get("type") == "volume":
                names.add(mount["source"])
    return names


def _inherited_networks():
    names = set()
    for svc in _load(OVERLAY).get("services", {}).values():
        nets = svc.get("networks") or []
        names.update(nets if isinstance(nets, list) else nets.keys())
    return {n for n in names if n != "default"}


def test_the_overlay_references_volumes_and_networks_it_does_not_declare():
    vols, nets = _inherited_volumes(), _inherited_networks()
    assert vols, "found no named volume refs in the overlay -- extraction broke"
    assert nets, "found no non-default network refs in the overlay -- extraction broke"
    declared_v = set(_load(OVERLAY).get("volumes") or {})
    declared_n = set(_load(OVERLAY).get("networks") or {})
    assert not (vols & declared_v), (
        f"{OVERLAY} declares its own {sorted(vols & declared_v)}. An overlay that "
        "declares the volume it is meant to inherit gets a SECOND, empty volume: "
        "connector-seed seeds it and the orchestrator reads the other one. "
        "Inherit from the base instead."
    )
    assert not (nets & declared_n), (
        f"{OVERLAY} declares its own {sorted(nets & declared_n)}; inherit it from the base."
    )


@pytest.mark.parametrize("base", _base_files())
def test_each_documented_base_declares_what_the_overlay_inherits(base):
    doc = _load(base)
    top_v = set(doc.get("volumes") or {})
    top_n = set(doc.get("networks") or {})
    missing_v = _inherited_volumes() - top_v
    missing_n = _inherited_networks() - top_n
    assert not missing_v, (
        f"{OVERLAY}'s header documents layering onto {base}, but {base} declares no "
        f"top-level volume(s) {sorted(missing_v)}. That invocation fails with "
        f"'refers to undefined volume {sorted(missing_v)[0]}: invalid compose project'."
    )
    assert not missing_n, (
        f"{OVERLAY}'s header documents layering onto {base}, but {base} declares no "
        f"top-level network(s) {sorted(missing_n)}."
    )


@pytest.mark.parametrize("base", _base_files())
def test_each_documented_base_defines_every_service_the_overlay_overrides(base):
    """The overlay only ever *overrides*; a name the base lacks is a new service
    carrying only the overlay's partial spec (no image for connector-deployer,
    no ports, no env) -- valid YAML, broken stack."""
    base_services = set(_load(base).get("services") or {})
    overlay_services = set(_load(OVERLAY).get("services") or {})
    assert base_services, f"{base} parsed to no services"
    missing = overlay_services - base_services
    assert not missing, (
        f"{base} has no service(s) {sorted(missing)} for {OVERLAY} to override; "
        "the overlay would create them from its partial spec alone."
    )


# ---------------------------------------------------------------------------
# 2. --scale in the header must name services that are in the DEFAULT project.
# ---------------------------------------------------------------------------

_SCALE = re.compile(r"--scale\s+([A-Za-z0-9._-]+)=")


def _scaled_services():
    text = re.sub(r"\\\s*\n", " ", _header(OVERLAY).replace("#", " "))
    return sorted(set(_SCALE.findall(text)))


def test_every_scaled_service_is_in_the_default_project_of_every_base():
    scaled = _scaled_services()
    if not scaled:
        pytest.skip("header documents no --scale flags")
    for base in _base_files():
        services = _load(base).get("services") or {}
        default = {n for n, s in services.items() if not (s or {}).get("profiles")}
        profiled = {
            n: (s or {})["profiles"] for n, s in services.items() if (s or {}).get("profiles")
        }
        for name in scaled:
            assert name not in profiled, (
                f"{OVERLAY}'s header scales '{name}', but {base} puts it behind "
                f"profiles={profiled[name]} -- it is ABSENT from the default project, "
                "so the flag names a service the rendered project does not contain. "
                "(This is exactly the context7-mcp bug.) Drop the flag, or document "
                "the --profile that activates it."
            )
            assert name in default, (
                f"{OVERLAY}'s header scales '{name}', which {base} does not define at all."
            )
