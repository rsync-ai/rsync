"""Every in-chart hostname the Helm chart hands a service must be a Service the chart declares.

This exists because `helm template` and `helm lint` cannot see the difference.
`rsync-ai.postgres.host` emitted `<fullname>-postgresql` while
templates/infra/postgresql.yaml declares the Service as `<fullname>-postgres`.
Both render as perfectly valid YAML, both pass lint, and CI was green -- the
mismatch is only decidable against a live cluster's DNS. On kind it surfaced as
the orchestrator and temporal-adapter dying on `lookup rsync-postgresql ... no
such host`, Temporal wedged on `nc: bad address`, and -- worst of the three --
the api-gateway staying 1/1 Ready and serving MOCK DATA, because it logs one
warning on a failed DB connect and never retries.

So the check is static and cheap: pull the suffix out of every hostname helper
that points at something inside the chart, pull the suffix out of every Service
the chart declares, and require the first set to be contained in the second.

Only IN-CHART hostnames are checkable. The `else` branch of each helper is a BYO
endpoint supplied by the operator (postgresql.external.host, kafka.external.
bootstrapServers, ...) and resolves in the customer's DNS, not ours -- there is
nothing in this repo to check it against.
"""

import pathlib
import re

import pytest
import yaml

CHART = pathlib.Path(__file__).resolve().parents[2] / "deploy" / "helm" / "rsync-ai"
TEMPLATES = CHART / "templates"
HELPERS = TEMPLATES / "_helpers.tpl"

# `printf "%s-postgres" (include "rsync-ai.fullname" .)`
# `printf "%s-kafka:9092" (include "rsync-ai.fullname" .)`
# `printf "http://%s-minio:9000" (include "rsync-ai.fullname" .)`
_HOSTNAME = re.compile(
    r'printf\s+"[^"]*%s-(?P<suffix>[a-z0-9]+(?:-[a-z0-9]+)*)[^"]*"\s+\(include\s+"rsync-ai\.fullname"'
)

# Service names are written either through the helper directly or through a
# `$fullname` the template assigned from it. Both reduce to the same suffix.
_SERVICE_NAME = re.compile(
    r'^\s*name:\s*\{\{-?\s*(?:include\s+"rsync-ai\.fullname"\s+\.|\$fullname)\s*-?\}\}-(?P<suffix>[a-z0-9-]+)\s*$',
    re.M,
)


# Helpers that build `<fullname>-<suffix>` for something that is NOT a network
# target. They are listed rather than pattern-excluded so that adding one is a
# reviewed decision: the default for a new `<fullname>-x` helper is "this names a
# Service and must resolve", which is the safe direction to be wrong in.
NON_NETWORK_HELPERS = {
    "rsync-ai.secretName": "names the Secret, not a Service",
}


def _helper_hostname_suffixes():
    """Suffixes of every in-chart hostname built in _helpers.tpl, by defining helper."""
    text = HELPERS.read_text()
    out = {}
    for define in re.split(r"\{\{-?\s*define\s+", text)[1:]:
        name = re.match(r'"([^"]+)"', define)
        if not name or name.group(1) in NON_NETWORK_HELPERS:
            continue
        for m in _HOSTNAME.finditer(define):
            out.setdefault(m.group("suffix"), set()).add(name.group(1))
    return out


def test_the_non_network_allowlist_has_no_stale_entries():
    """An entry that no longer exists is documentation claiming an exemption for
    nothing, and it hides the next real one."""
    text = HELPERS.read_text()
    for helper in NON_NETWORK_HELPERS:
        assert f'define "{helper}"' in text, (
            f"{helper} is exempted from the hostname check but is no longer defined "
            f"in _helpers.tpl -- drop the entry"
        )


def _declared_service_suffixes():
    """Suffixes of every Service the chart declares with a statically known name.

    Services whose name is computed per item (the connector fleet, generation
    services) are skipped: their names come from values, so there is no fixed
    string to compare a helper against.
    """
    out = {}
    for path in sorted(TEMPLATES.rglob("*.yaml")):
        text = path.read_text()
        for doc in text.split("\n---"):
            if not re.search(r"^kind:\s*Service\s*$", doc, re.M):
                continue
            for m in _SERVICE_NAME.finditer(doc):
                out.setdefault(m.group("suffix"), set()).add(
                    str(path.relative_to(TEMPLATES))
                )
    return out


def test_the_chart_was_parsed():
    """Vacuity floor. A rename under deploy/helm/ must fail here loudly rather than
    quietly reduce this whole file to zero assertions."""
    helpers = _helper_hostname_suffixes()
    services = _declared_service_suffixes()
    assert len(helpers) >= 5, f"hostname helpers parsed as near-empty: {helpers}"
    assert len(services) >= 5, f"chart Services parsed as near-empty: {services}"


@pytest.mark.parametrize("suffix", sorted(_helper_hostname_suffixes()))
def test_every_in_chart_hostname_matches_a_declared_service(suffix):
    services = _declared_service_suffixes()
    helpers = _helper_hostname_suffixes()
    assert suffix in services, (
        f"{sorted(helpers[suffix])} builds the hostname `<fullname>-{suffix}`, but no "
        f"Service by that name is declared in the chart -- the declared ones are "
        f"{sorted(services)}. Every consumer of that helper resolves nothing on a real "
        f"cluster. `helm template` and `helm lint` both pass on this, because the wrong "
        f"hostname is still valid YAML."
    )


def test_the_in_chart_branch_of_each_infra_helper_is_gated_on_its_enabled_flag():
    """The suffix check above is only meaningful for hostnames that point INSIDE the
    chart. This pins that reading: each infra helper must have both branches -- the
    in-chart name and a `required`-guarded BYO value -- so a BYO deployment fails with
    the operator's missing value named, not with a dangling in-cluster DNS name."""
    text = HELPERS.read_text()
    for helper in (
        "rsync-ai.postgres.host",
        "rsync-ai.redis.host",
        "rsync-ai.kafka.bootstrap",
        "rsync-ai.temporal.address",
        "rsync-ai.minio.endpoint",
    ):
        body = re.split(rf'\{{\{{-?\s*define\s+"{re.escape(helper)}"', text)
        assert len(body) == 2, f"{helper} is not defined exactly once in _helpers.tpl"
        block = re.split(r"\{\{-?\s*end\s*-?\}\}\s*\n\s*\n", body[1])[0]
        assert "required" in block, (
            f"{helper} has no `required` BYO branch -- a deployment that turns this "
            f"component off would silently keep pointing at an in-cluster Service that "
            f"was never created."
        )


def test_the_chart_is_loadable_yaml():
    """Chart.yaml and values.yaml must parse; the tests above read templates as text,
    so a broken chart manifest would otherwise go unnoticed here."""
    assert yaml.safe_load((CHART / "Chart.yaml").read_text())["name"] == "rsync-ai"
    assert isinstance(yaml.safe_load((CHART / "values.yaml").read_text()), dict)
