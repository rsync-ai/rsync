"""Temporal's auto-setup must not attempt CREATE DATABASE against a BYO Postgres.

The `temporalio/auto-setup` image creates its two databases (`temporal`,
`temporal_visibility`) on first boot. The create is guarded by ONE condition,
and it is not the one it looks like -- from `/etc/temporal/auto-setup.sh`:

    if [[ ${DBNAME} != "${POSTGRES_USER}" && ${SKIP_DB_CREATE} != true ]]; then
        temporal-sql-tool ... --db "${DBNAME}" create
    fi

Existence is never checked. The guard is a NAME INEQUALITY, so on any external
instance -- where POSTGRES_USER is `rsync`, not `temporal` -- the create runs
unconditionally and returns 1 with `pq: permission denied to create database`
for every role that lacks CREATEDB. A managed instance's application role
normally lacks it. Measured against a NOSUPERUSER/NOCREATEDB role on PG16:

    | privilege     | DB exists | DB absent |
    | superuser     | rc=0      | rc=0      |
    | non-superuser | rc=1      | rc=1      |

Both non-superuser cells fail, which is why "create the two databases by hand"
was NOT a fix -- the create is still attempted, and still denied. Privilege is
the discriminating variable, not existence.

That rc=1 is fatal rather than degrading. `entrypoint.sh` runs
`auto-setup.sh && start-temporal.sh` under `set -eu -o pipefail`, so a failed
create means the server is never exec'd: the container crash-loops and every
pipeline hangs with no workflow engine. Observed on the shipped
quickstart+byo-postgres stack: RestartCount=10, 0 tables in both databases.

So SKIP_DB_CREATE=true is required on every external-Postgres path. Like the
`profiles:` / `required: false` pairing this repo already documents, it is one
half of two and each half alone fails: skip the create WITHOUT pre-creating the
databases and `setup-schema` fails instead with `pq: database "temporal" does
not exist`. The operator docs carry the other half; this file guards the switch.

The control matters as much as the check. Setting SKIP_DB_CREATE on the BUNDLED
path would be wrong -- there POSTGRES_USER is the postgres image's superuser,
the create legitimately succeeds, and nothing pre-creates the databases -- so
every assertion below is paired with a bundled-path control that must NOT have
it. A fix that simply set it everywhere would pass a one-sided check.
"""

import glob
import os
import shutil
import subprocess

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Same stack list as test_byo_parked_deps_in_every_compose_file.py, and for the
# same reason: it cannot be derived without inventing invalid combinations.
STACKS = {
    "quickstart": ["docker-compose.quickstart.yml"],
    "base": ["docker-compose.yml"],
    "prod": ["docker-compose.yml", "docker-compose.prod.yml"],
}

# Values the chart marks `required:` -- unrelated to this test, but the render
# fails without them, and a render that errors would skip the check silently.
HELM_REQUIRED = [
    "secrets.jwtSecret=FAKEPLACEHOLDER",
    "secrets.encryptionKey=FAKEPLACEHOLDERFAKEPLACEHOLDER32",
    "secrets.postgresPassword=FAKEPLACEHOLDER",
    "secrets.minioAccessKey=FAKEPLACEHOLDER",
    "secrets.minioSecretKey=FAKEPLACEHOLDER",
    "secrets.redisPassword=FAKEPLACEHOLDER",
    "frontend.apiUrl=https://rsync.example.com",
    "frontend.publicUrl=https://rsync.example.com",
]


class _ComposeLoader(yaml.SafeLoader):
    """SafeLoader that reads through Compose's `!override` / `!reset` tags."""


def _resolve_tag(loader, node):
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node, deep=True)
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node, deep=True)
    return loader.construct_scalar(node)


for _tag in ("!override", "!reset"):
    _ComposeLoader.add_constructor(_tag, _resolve_tag)


def _env_of(service):
    """Compose accepts both list and map form; normalise to a dict."""
    env = (service or {}).get("environment") or {}
    if isinstance(env, list):
        out = {}
        for item in env:
            k, _, v = str(item).partition("=")
            out[k] = v
        return out
    return {str(k): ("" if v is None else str(v)) for k, v in env.items()}


def _services(path):
    with open(path) as fh:
        doc = yaml.load(fh, Loader=_ComposeLoader) or {}
    return {n: b for n, b in (doc.get("services") or {}).items() if isinstance(b, dict)}


def postgres_parking_overlays():
    """The byo-* overlays that park the bundled `postgres` service."""
    found = []
    for path in sorted(glob.glob(os.path.join(REPO_ROOT, "docker-compose.byo-*.yml"))):
        svcs = _services(path)
        if (svcs.get("postgres") or {}).get("profiles"):
            found.append(path)
    assert found, "no byo overlay parks `postgres` -- the derivation is broken, not the repo"
    return found


def test_every_postgres_parking_overlay_sets_skip_db_create():
    """The switch must live in the overlay, so it cannot reach the bundled path."""
    missing = []
    for path in postgres_parking_overlays():
        env = _env_of(_services(path).get("temporal"))
        if env.get("SKIP_DB_CREATE") != "true":
            missing.append(f"{os.path.basename(path)} (got {env.get('SKIP_DB_CREATE')!r})")
    assert not missing, (
        "these overlays park the bundled `postgres` but leave Temporal's auto-setup "
        "attempting CREATE DATABASE against the operator's external instance. It "
        "exits 1 on any role without CREATEDB and the container crash-loops:\n  "
        + "\n  ".join(missing)
    )


def test_prod_overlay_sets_skip_db_create():
    """prod.yml points Temporal at an external host in its own env, not via an overlay."""
    env = _env_of(_services(os.path.join(REPO_ROOT, "docker-compose.prod.yml")).get("temporal"))
    assert "${POSTGRES_HOST}" in env.get("POSTGRES_SEEDS", ""), (
        "prod.yml's temporal no longer takes POSTGRES_SEEDS from ${POSTGRES_HOST} -- "
        "re-check this test's premise that prod is an external-database path"
    )
    assert env.get("SKIP_DB_CREATE") == "true", (
        f"docker-compose.prod.yml points Temporal at an external database but "
        f"SKIP_DB_CREATE is {env.get('SKIP_DB_CREATE')!r}"
    )


def test_no_base_file_sets_skip_db_create():
    """CONTROL. On the bundled path the create is correct and must still happen.

    The bundled Postgres connects as the image superuser, so the create succeeds,
    and nothing else creates the two databases. Setting the switch there would
    turn a working default install into `database "temporal" does not exist`.
    """
    offenders = []
    for fname in ("docker-compose.yml", "docker-compose.quickstart.yml"):
        env = _env_of(_services(os.path.join(REPO_ROOT, fname)).get("temporal"))
        if "SKIP_DB_CREATE" in env:
            offenders.append(f"{fname} sets SKIP_DB_CREATE={env['SKIP_DB_CREATE']!r}")
    assert not offenders, (
        "a BASE compose file sets SKIP_DB_CREATE, which breaks the bundled install: "
        "nothing would create `temporal`/`temporal_visibility` and setup-schema fails "
        "with `database \"temporal\" does not exist`. It belongs in the byo overlay:\n  "
        + "\n  ".join(offenders)
    )


def _render(files, env_file):
    cmd = ["docker", "compose", "--env-file", env_file]
    for f in files:
        cmd += ["-f", f]
    cmd += ["config", "--format", "json"]
    out = subprocess.run(cmd, capture_output=True, text=True, cwd=REPO_ROOT)
    return out.returncode, out.stdout, out.stderr


def _required_env(files):
    import re

    env = {}
    for fname in files:
        with open(os.path.join(REPO_ROOT, fname)) as fh:
            for var in re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*):?\?", fh.read()):
                env[var] = "FAKEPLACEHOLDER"
    return env


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not installed")
@pytest.mark.parametrize("stack_name", sorted(STACKS))
def test_rendered_external_postgres_stack_skips_db_create(tmp_path, stack_name):
    """Render layer: whenever `postgres` is gone but `temporal` remains, the switch is on.

    This is the derived form of the rule -- it does not care which file supplies
    the setting, only that the shipped project a BYO operator actually runs has
    it. Paired with the bundled control below, which must NOT have it.
    """
    import json

    overlay = "docker-compose.byo-postgres.yml"
    files = STACKS[stack_name]
    env_file = str(tmp_path / "env")
    with open(env_file, "w") as fh:
        for k, v in _required_env(files + [overlay]).items():
            fh.write(f"{k}={v}\n")

    rc, out, err = _render(files, env_file)
    assert rc == 0, f"CONTROL: {stack_name} does not render on its own:\n{err}"
    control = json.loads(out)["services"]
    assert len(control) > 10, (
        f"CONTROL: {stack_name} rendered only {sorted(control)} -- refusing to report "
        "a pass on a denominator this small"
    )
    # Whether a stack is bundled is DERIVED per render, never assumed from its name.
    # prod.yml parks `postgres` behind the never-activated `local-db` profile and
    # points temporal at ${POSTGRES_HOST}, so it is already external with no overlay
    # -- a hard-coded "every control has a bundled postgres" would fail there for
    # exactly the right behaviour. test_at_least_two_stacks_bundle_postgres keeps
    # this branch from quietly becoming unreachable for all three.
    control_env = _env_of(control.get("temporal"))
    if control_env.get("POSTGRES_SEEDS") == "postgres":
        assert "SKIP_DB_CREATE" not in control_env, (
            f"CONTROL: {stack_name} points temporal at the in-project `postgres` "
            "service yet skips the create. Nothing else creates `temporal`/"
            "`temporal_visibility` on that path, so setup-schema fails."
        )

    rc, out, err = _render(files + [overlay], env_file)
    assert rc == 0, f"{stack_name} + {overlay} fails to render:\n{err}"
    svcs = json.loads(out)["services"]
    assert "postgres" not in svcs, f"{overlay} was layered but postgres is still present"
    if "temporal" not in svcs:
        pytest.skip(f"{stack_name} renders no temporal service")

    env = _env_of(svcs["temporal"])
    assert env.get("SKIP_DB_CREATE") == "true", (
        f"{stack_name} + {overlay} renders a Temporal pointed at an EXTERNAL database "
        f"with SKIP_DB_CREATE={env.get('SKIP_DB_CREATE')!r}. auto-setup will attempt "
        "CREATE DATABASE as the operator's role and crash-loop the container."
    )
    assert len(env) > 5, (
        f"{stack_name} + {overlay} left temporal with only {sorted(env)} -- the overlay's "
        "environment block REPLACED the base's instead of merging into it"
    )


def _helm(*extra):
    cmd = ["helm", "template", "r", "deploy/helm/rsync-ai"]
    for v in HELM_REQUIRED:
        cmd += ["--set", v]
    for v in extra:
        cmd += ["--set", v]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=REPO_ROOT)


def _temporal_env_names(rendered):
    for doc in yaml.safe_load_all(rendered):
        if not doc or doc.get("kind") != "Deployment":
            continue
        name = doc["metadata"]["name"]
        if "temporal" in name and "adapter" not in name:
            return [e["name"] for e in doc["spec"]["template"]["spec"]["containers"][0]["env"]]
    return None


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_helm_sets_skip_db_create_only_for_external_postgres():
    """Both arms in one test, because either alone is satisfiable by a wrong fix.

    Setting it unconditionally passes the external arm while breaking the in-chart
    Postgres install; omitting it passes the bundled arm while leaving the BYO pod
    in CrashLoopBackOff.
    """
    ext = _helm("postgresql.enabled=false", "postgresql.external.host=db.example.com")
    assert ext.returncode == 0, f"external-postgres render failed:\n{ext.stderr}"
    ext_env = _temporal_env_names(ext.stdout)
    assert ext_env, "external render produced no temporal Deployment"

    bundled = _helm()
    assert bundled.returncode == 0, f"bundled render failed:\n{bundled.stderr}"
    bundled_env = _temporal_env_names(bundled.stdout)
    assert bundled_env, "bundled render produced no temporal Deployment"

    assert "SKIP_DB_CREATE" in ext_env, (
        "postgresql.enabled=false renders a Temporal pod that will attempt CREATE "
        "DATABASE against the operator's external instance and CrashLoopBackOff. "
        f"Its env is {ext_env}"
    )
    assert "SKIP_DB_CREATE" not in bundled_env, (
        "the in-chart Postgres path must keep creating its own databases -- setting "
        "SKIP_DB_CREATE there fails setup-schema with `database \"temporal\" does not "
        f"exist`. Its env is {bundled_env}"
    )
    assert set(bundled_env) < set(ext_env), (
        "the external arm should differ from the bundled arm by exactly the added "
        f"switch; got bundled={bundled_env} external={ext_env}"
    )


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not installed")
def test_at_least_two_stacks_bundle_postgres(tmp_path):
    """The bundled control above is an `if`; this proves the `if` is still taken.

    If every stack drifted to an external default, the control branch would stop
    running and the parametrised test would go on passing while checking only half
    of what its name claims.
    """
    import json

    bundled = []
    for name, files in sorted(STACKS.items()):
        env_file = str(tmp_path / f"env-{name}")
        with open(env_file, "w") as fh:
            for k, v in _required_env(files).items():
                fh.write(f"{k}={v}\n")
        rc, out, err = _render(files, env_file)
        assert rc == 0, f"{name} does not render:\n{err}"
        svcs = json.loads(out)["services"]
        if "postgres" in svcs and _env_of(svcs.get("temporal")).get("POSTGRES_SEEDS") == "postgres":
            bundled.append(name)
    assert len(bundled) >= 2, (
        f"only {bundled} render temporal against a bundled postgres, so the "
        "bundled-path control is barely exercised -- re-check the stack list"
    )
