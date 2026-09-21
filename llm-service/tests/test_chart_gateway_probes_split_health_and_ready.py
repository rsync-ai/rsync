"""The api-gateway's readiness probe must be /ready and its liveness probe /health.

This pairing is the single fact that six operator-facing documents now assert.
They say that a wrong or empty `secrets.postgresPassword` leaves the api-gateway
pod at `0/1` with `/ready` answering 503 `db_ping_failed` -- a failure the
operator can see -- rather than the silent one those same documents promised
before readiness moved to /ready: "stays 1/1 Ready and serves mock data".

Both halves of that older sentence are false today, and for different reasons:

  * Ready. `/health` is a static literal (api-gateway/cmd/server/main.go:621) and
    cannot fail for the reason that matters. Readiness moved to `/ready`
    (deploy/helm/rsync-ai/templates/apps/api-gateway.yaml), which returns 503
    `db_not_connected` / `db_ping_failed` / `schema_not_migrated`
    (api-gateway/cmd/server/ready.go readinessVerdict), so a DB-dead replica is
    excluded from its Service's endpoints instead of advertised as healthy.
  * Mock data. The emitter gates on `db.GetDB() == nil`
    (api-gateway/internal/handlers/connections.go), and `db.Init` assigns `DB`
    from `sql.Open` *before* it pings, so a ping failure leaves `DB` non-nil and
    that branch unreachable. Requests 503 out of the workspace middleware
    instead.

Point readiness back at `/health` and every one of those six documents silently
becomes a lie again, with nothing failing: the pod would report `1/1` while its
database was gone. `helm lint` and `helm template` are both blind to it -- a
probe path is valid YAML whatever it addresses. So the check is here, and it is
structural rather than a scan for wording: it reads the probe blocks the chart
actually ships and the routes the server actually registers.

The liveness half is asserted too, in the opposite direction. Liveness on
`/ready` would crash-loop every replica during a database outage, which cannot
reach an unreachable Postgres and only slows recovery -- the comment in the
template says so, and this test is what keeps the comment true.

No `helm` binary is needed: the probe blocks carry no template expressions, and
the other chart guards in this directory parse the chart the same way.
"""

import pathlib
import re

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
DEPLOYMENT = REPO / "deploy" / "helm" / "rsync-ai" / "templates" / "apps" / "api-gateway.yaml"
MAIN_GO = REPO / "api-gateway" / "cmd" / "server" / "main.go"

# `          readinessProbe:` ... `            httpGet: { path: /ready, port: http }`
# Anchored on the probe key so a path belonging to some other block cannot be
# mistaken for one of these, and non-greedy so the nearest httpGet wins.
_PROBE = re.compile(
    r"^(?P<indent>\s*)(?P<probe>startupProbe|readinessProbe|livenessProbe):\s*$"
    r"(?P<body>(?:\n(?:\1\s+.*|\s*))*?)"
    r"\n\s*httpGet:\s*\{\s*path:\s*(?P<path>\S+?)\s*,",
    re.MULTILINE,
)

EXPECTED = {
    "startupProbe": "/health",   # "has it bound yet", nothing more
    "readinessProbe": "/ready",  # pings the pool AND asserts migrations applied
    "livenessProbe": "/health",  # a DB outage must drain traffic, not restart pods
}


def _probes():
    text = DEPLOYMENT.read_text()
    return {m.group("probe"): m.group("path") for m in _PROBE.finditer(text)}


def test_the_scan_found_probes_to_check():
    """A guard that matches nothing reports success identically to a clean one."""
    found = _probes()
    assert found, (
        f"parsed 0 probes out of {DEPLOYMENT.relative_to(REPO)}. Either the probe "
        "blocks moved or their YAML shape changed; this guard is asserting "
        "nothing until that is fixed."
    )
    assert set(found) == set(EXPECTED), (
        f"expected exactly {sorted(EXPECTED)}, parsed {sorted(found)}"
    )


@pytest.mark.parametrize("probe,path", sorted(EXPECTED.items()))
def test_probe_targets_the_documented_endpoint(probe, path):
    found = _probes()
    assert found.get(probe) == path, (
        f"api-gateway {probe} points at {found.get(probe)!r}, expected {path!r}.\n"
        "readinessProbe must be /ready: /health is a static literal, so pointing "
        "readiness at it marks a database-dead replica Ready and re-breaks the "
        "operator docs in deploy/helm/rsync-ai/README.md, values.yaml, "
        "templates/validate.yaml and docs/deployment/kubernetes.md, which all "
        "tell the reader to expect a 0/1 pod.\n"
        "livenessProbe must stay /health: liveness on /ready crash-loops every "
        "replica during a database outage."
    )


@pytest.mark.parametrize("route", sorted(set(EXPECTED.values())))
def test_the_route_a_probe_addresses_is_registered(route):
    """A probe pointing at an unregistered route 404s, and 404 is never Ready."""
    text = MAIN_GO.read_text()
    assert f'r.GET("{route}"' in text, (
        f'{route} is not registered as a top-level GET in '
        f'{MAIN_GO.relative_to(REPO)}, but the chart probes it. A probe against '
        f'a route the server does not serve fails forever.'
    )
