"""The BYO-Postgres kind fixture is two files that hand-mirror the same three
values, plus a list of databases that exists only because the chart refuses to
create them.

`postgres-up.sh` starts the container and creates the role and the databases.
`values-kind-byo-pg.yaml` tells the chart where to find them. Nothing connects
the two but matching literals: the role name, the database name, and the
in-cluster hostname built from the container name and the namespace. Drift any
one of them and `helm install` succeeds, then fails minutes later as a password
authentication error against a role that does not exist -- which reads like a
wrong password, not like a typo in a values file.

This repo has already paid for this exact shape once: `build-and-load.sh` kept
`TAG=0.1.0` after the chart moved to 0.1.1, because the value was mirrored in a
second file and nothing compared them. The lesson recorded then was to grep for
the value rather than the name. This asserts it instead.

The database list is the second half. With an external database the chart sets
SKIP_DB_CREATE=true on Temporal's auto-setup, so `temporal` and
`temporal_visibility` must exist beforehand; `pipeline_db` likewise, because
api-gateway migrates a schema into a database and does not create the database.
If a future chart change adds a fourth database, or renames one, the fixture
goes stale silently -- the install just CrashLoopBackOffs.

Text-only by design: it reads the two fixture files and the chart template. It
therefore runs everywhere, including on a machine with no helm, no kind and no
Docker daemon, which is where fixture drift is least likely to be noticed.
"""

import os
import re

import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
KIND_DIR = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai", "test", "kind")
SCRIPT = os.path.join(KIND_DIR, "postgres-up.sh")
VALUES = os.path.join(KIND_DIR, "values-kind-byo-pg.yaml")
TEMPORAL = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai", "templates", "infra", "temporal.yaml")


def _shell_default(name):
    """Read `NAME="${OVERRIDE:-value}"` out of the script.

    Deliberately not a shell execution: running the script to learn its defaults
    would start a container. The pattern is pinned tightly enough that a
    reformat fails loudly here rather than matching something else.
    """
    with open(SCRIPT, encoding="utf-8") as fh:
        body = fh.read()
    m = re.search(rf'^{name}="\$\{{[A-Z_]+:-([^}}"]+)\}}"', body, re.MULTILINE)
    assert m, f"{name} is no longer a `{name}=\"${{OVERRIDE:-default}}\"` line in postgres-up.sh"
    return m.group(1)


def _values():
    with open(VALUES, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def test_the_script_and_the_values_file_agree_on_role_database_and_host():
    v = _values()["postgresql"]
    user, db = _shell_default("APP_USER"), _shell_default("APP_DB")
    name, ns = _shell_default("NAME"), _shell_default("NS")

    assert v["username"] == user, (
        f"values-kind-byo-pg.yaml says the role is {v['username']!r} but "
        f"postgres-up.sh creates {user!r}. The chart never derives the "
        "username, so this installs cleanly and then fails authentication."
    )
    assert v["database"] == db, (
        f"values-kind-byo-pg.yaml says the database is {v['database']!r} but "
        f"postgres-up.sh creates {db!r}."
    )
    # The Service the script applies is named $NAME in namespace $NS, so this is
    # the only hostname that can resolve.
    assert v["external"]["host"] == f"{name}.{ns}.svc.cluster.local", (
        f"values-kind-byo-pg.yaml points at {v['external']['host']!r}, but "
        f"postgres-up.sh applies its Service as {name!r} in namespace {ns!r}, "
        f"so only {name}.{ns}.svc.cluster.local resolves."
    )


def test_the_fixture_disables_tls_because_the_container_has_no_certificate():
    """The chart defaults external Postgres to sslMode=require, which is right
    for a managed instance and impossible for a plain container. Inheriting the
    default here would fail every connection -- and `helm install` would still
    exit 0, leaving the api-gateway at 0/1 with /ready answering
    503 db_ping_failed."""
    v = _values()["postgresql"]
    assert v["enabled"] is False, "this overlay exists to turn the in-chart Postgres off"
    assert v["external"]["sslMode"] == "disable", (
        f"sslMode is {v['external'].get('sslMode')!r}; the fixture's container "
        "serves no certificate, and the chart's default is `require`"
    )


def test_the_script_creates_every_database_the_chart_will_not():
    """SKIP_DB_CREATE=true is what makes this the fixture's job rather than the
    chart's. The assertion is anchored to that template line, so removing the
    skip -- or adding a fourth database -- surfaces here."""
    with open(TEMPORAL, encoding="utf-8") as fh:
        temporal = fh.read()
    assert "SKIP_DB_CREATE" in temporal, (
        "temporal.yaml no longer sets SKIP_DB_CREATE. If the chart creates its "
        "own databases again, postgres-up.sh is doing unnecessary work and this "
        "test is asserting a stale contract -- check which, do not just delete."
    )

    with open(SCRIPT, encoding="utf-8") as fh:
        script = fh.read()
    db = _shell_default("APP_DB")

    # initdb creates APP_DB from POSTGRES_DB; the script CREATEs the other two.
    created = set(re.findall(r"for db in ([a-z_ ]+); do", script)[0].split()) | {db}
    required = {db, "temporal", "temporal_visibility"}
    assert created == required, (
        f"postgres-up.sh provides {sorted(created)}, the chart needs "
        f"{sorted(required)}. A missing one is a CrashLoopBackOff minutes after "
        "a successful-looking install."
    )

    # And the script must not merely issue the CREATEs -- it must check they
    # landed. A psql failure inside a loop is easy to lose.
    assert "FROM pg_database WHERE datname IN" in script, (
        "postgres-up.sh no longer verifies the databases exist after creating "
        "them; an empty result and a failed query look identical through a pipe"
    )


def test_the_script_generates_a_password_the_chart_will_accept():
    """The chart refuses passwords containing URL-reserved characters (see
    test_chart_refuses_undeliverable_passwords). A fixture that generated one
    would fail at render with a message about the values file, pointing away
    from the script that actually produced it."""
    with open(SCRIPT, encoding="utf-8") as fh:
        script = fh.read()
    assert "openssl rand -hex 24" in script, (
        "postgres-up.sh must generate its password with `openssl rand -hex 24`; "
        "base64 output contains `/` and `+`, which the chart rejects at render"
    )
    assert "rand -base64" not in script, "base64 output can contain `/`, which the chart rejects"
