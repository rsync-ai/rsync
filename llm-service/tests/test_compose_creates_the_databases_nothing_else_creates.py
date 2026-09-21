"""On the BYO-Postgres compose path, nothing but `db-init` creates a database.

The bundled stack creates three databases without anyone noticing, through two
upstream images: the postgres image's initdb makes POSTGRES_DB, and Temporal's
`auto-setup` makes `temporal` and `temporal_visibility`. The overlay removes the
first (the bundled container is parked in a profile that is never activated) and
`SKIP_DB_CREATE` removes the second -- so on this path the databases had no
creator at all, and the gap was fifty lines of instructions in a file header.

Only one of the three failures is loud. Temporal's entrypoint is
`auto-setup.sh && start-temporal.sh` under `set -e`, so a missing Temporal
database crash-loops the container. A missing application database does not:
`db.Init` failing is a warning and not an exit (api-gateway/cmd/server/main.go:306-308)
and the migration runner sits inside that call's success branch, so the schema
is never created and the process serves on. Compose declares no healthcheck on
either api-gateway or orchestrator, so `docker compose ps` reports both as plain
`running`. `/ready` is the endpoint that knows (api-gateway/cmd/server/ready.go:15-25),
and install.sh polls it -- a hand-run `docker compose up` polls nothing.

These tests read the overlay as data. They deliberately assert on the mkdb/mkext
CALLS rather than on the names appearing anywhere in the script: a bare
`"pg_trgm" in script` stays true after the call is deleted, because the name
survives in the comment that explains it.
"""

import os
import re
import shutil
import subprocess

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
BASE = os.path.join(REPO_ROOT, "docker-compose.quickstart.yml")
OVERLAY = os.path.join(REPO_ROOT, "docker-compose.byo-postgres.yml")

# Every var the base file guards with `:?` must have a value or `docker compose
# config` refuses to render anything at all. All fake.
FAKE_ENV = {
    "ENCRYPTION_KEY": "FAKEPLACEHOLDER",
    "JWT_SECRET": "FAKEPLACEHOLDER",
    "MINIO_ACCESS_KEY": "FAKEPLACEHOLDER",
    "MINIO_SECRET_KEY": "FAKEPLACEHOLDER",
    "POSTGRES_PASSWORD": "FAKEPLACEHOLDER",
    "REDIS_PASSWORD": "FAKEPLACEHOLDER",
}

GATED_ON_DB_INIT = ("temporal", "orchestrator", "api-gateway")


def _load(path):
    with open(path) as fh:
        return yaml.safe_load(fh)


def overlay_services():
    return _load(OVERLAY)["services"]


def db_init():
    services = overlay_services()
    assert "db-init" in services, (
        "docker-compose.byo-postgres.yml no longer defines db-init; on this path "
        "nothing else issues a CREATE DATABASE"
    )
    return services["db-init"]


def script():
    """The shell body, unescaped.

    Compose's `$$` is a literal `$` at run time. Leaving it in place makes every
    `sh -n` below parse a different program than the container runs -- and one
    that happens to be invalid, at `n=$((n + 1))`, so the check would look
    strict while proving nothing about the real script.
    """
    command = db_init()["command"]
    assert isinstance(command, list) and len(command) == 1, (
        "db-init's command must stay a single-element list. Compose splits a "
        f"string command into argv, so `sh -c` would run only the first word; got {command!r}"
    )
    return command[0].replace("$$", "$")


def code_lines():
    """Script lines with comments and blanks removed, so a name in prose cannot pass."""
    out = []
    for line in script().split("\n"):
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            out.append(stripped)
    assert len(out) > 20, f"suspiciously short script body ({len(out)} lines) -- refusing to assert against it"
    return out


# --------------------------------------------------------------------------
# What the service is
# --------------------------------------------------------------------------

def test_the_overlay_parks_the_bundled_container_and_adds_a_creator():
    """Both halves, in one place. Either alone leaves the path without databases."""
    services = overlay_services()
    assert services["postgres"].get("profiles"), (
        "the bundled postgres must stay parked in an inactive profile; without "
        "that the overlay does not actually hand over to an external instance"
    )
    assert "db-init" in services


def test_db_init_runs_once_and_is_not_restarted():
    body = db_init()
    assert body["restart"] == "no", (
        "a completed bootstrap job that Compose restarts would re-run the "
        f"existence checks forever; got restart={body.get('restart')!r}"
    )
    assert body["entrypoint"] == ["/bin/sh", "-c"], (
        "the image is Alpine and has no bash; got "
        f"entrypoint={body.get('entrypoint')!r}"
    )
    assert "alpine" in body["image"], f"expected the small postgres client image, got {body['image']!r}"


def test_the_maintenance_database_is_never_one_of_the_created_ones():
    """CREATE DATABASE has to be issued from a database that already exists."""
    env = db_init()["environment"]
    assert "POSTGRES_MAINTENANCE_DB" in env["PGDATABASE"], (
        f"PGDATABASE must be operator-overridable; got {env['PGDATABASE']!r}"
    )
    assert ":-postgres}" in env["PGDATABASE"], (
        "the default maintenance database must be `postgres`, which exists on "
        f"RDS, Cloud SQL and Azure Database alike; got {env['PGDATABASE']!r}"
    )


def test_the_ca_path_is_not_handed_to_libpq_as_an_environment_variable():
    """libpq reads an EMPTY PGSSLROOTCERT as a filename and fails to open it.

    So an unset CA would break the connection under verify-ca rather than fall
    back to the system store. The script exports it only when non-empty.
    """
    env = db_init()["environment"]
    assert "PGSSLROOTCERT" not in env, (
        "PGSSLROOTCERT must not appear in the environment map -- an empty value "
        "is a filename to libpq, not an absence"
    )
    body = script()
    assert re.search(
        r'if \[ -n "\$\{POSTGRES_TLS_CA_FILE:-\}" \];\s*then\s*export PGSSLROOTCERT=', body
    ), "the conditional PGSSLROOTCERT export is gone; an empty CA path would reach libpq"


def test_the_password_is_required_rather_than_defaulted():
    env = db_init()["environment"]
    assert env["PGPASSWORD"].startswith("${POSTGRES_PASSWORD:?"), (
        "db-init must fail interpolation on a missing password rather than "
        f"authenticate as nobody; got {env['PGPASSWORD']!r}"
    )


# --------------------------------------------------------------------------
# What it creates
# --------------------------------------------------------------------------

def test_it_creates_exactly_the_three_databases_the_stack_needs():
    calls = [l for l in code_lines() if l.startswith("mkdb ")]
    assert calls == [
        'mkdb "$APP_DATABASE";',
        'mkdb "$TEMPORAL_DATABASE";',
        'mkdb "$TEMPORAL_VISIBILITY_DATABASE";',
    ], f"the mkdb calls changed: {calls}"


def test_it_creates_both_extensions_the_first_migrations_need():
    calls = [l for l in code_lines() if l.startswith("mkext ")]
    assert len(calls) == 2, f"expected two mkext calls, got {calls}"
    assert calls[0].startswith('mkext "uuid-ossp"'), (
        "001_init_schema.sql calls uuid_generate_v4() and is the FIRST migration; "
        f"got {calls[0]}"
    )
    assert calls[1].startswith('mkext "pg_trgm"'), (
        f"020b_workspace_connection_refs.sql builds gin_trgm_ops indexes; got {calls[1]}"
    )


def test_extensions_go_into_the_application_database_only():
    """Temporal's two need no extensions, and their schema is another product's."""
    body = script()
    mkext = body[body.index("mkext() {"):body.index('mkext "uuid-ossp"')]
    assert '-d "$APP_DATABASE"' in mkext, (
        "mkext must target the application database explicitly; without -d it "
        f"would run against PGDATABASE, the maintenance database:\n{mkext}"
    )
    for other in ("TEMPORAL_DATABASE", "TEMPORAL_VISIBILITY_DATABASE"):
        assert other not in mkext, f"mkext must not touch {other}"


def test_idempotency_is_an_existence_check_and_not_a_swallowed_error():
    """`permission denied to create database` must not read as `already exists`.

    CREATE DATABASE has no IF NOT EXISTS and cannot run inside a transaction, so
    the only two options are an existence check or absorbing the error -- and
    absorbing it makes the one failure that has to stay fatal indistinguishable
    from the ordinary re-run.
    """
    body = script()
    mkdb = body[body.index("mkdb() {"):body.index('mkdb "$APP_DATABASE"')]
    assert "SELECT 1 FROM pg_database WHERE datname" in mkdb, (
        f"the pg_database existence check is gone from mkdb:\n{mkdb}"
    )
    assert "ON_ERROR_STOP=1" in mkdb, "psql must stop on error rather than report success"
    assert "|| true" not in mkdb, f"an error is being swallowed in mkdb:\n{mkdb}"
    assert "2>/dev/null" not in mkdb, (
        f"discarding stderr makes a permission failure unreadable:\n{mkdb}"
    )


def test_a_role_without_createdb_gets_the_grant_it_needs_and_a_nonzero_exit():
    body = script()
    assert 'ALTER ROLE \\"$PGUSER\\" CREATEDB;' in body, (
        "the remediation must name the exact statement to run"
    )
    for managed in ("rds_superuser", "azure_pg_admin", "cloudsqlsuperuser"):
        assert managed in body, f"the managed-instance admin role {managed} is no longer named"
    assert len([l for l in code_lines() if l == "exit 1;"]) >= 3, (
        "every failure path -- unreachable, cannot create a database, cannot "
        "create an extension -- must exit non-zero, or the gate below opens"
    )


def test_no_owner_clause_is_named_on_create_database():
    """The creating role owns what it creates.

    Naming an owner is how this breaks on a managed instance whose admin role is
    not POSTGRES_USER.
    """
    creates = [l for l in code_lines() if "CREATE DATABASE" in l]
    assert creates, "no CREATE DATABASE left in the script"
    for line in creates:
        assert "OWNER" not in line.upper(), f"an OWNER clause reappeared: {line}"


def test_it_does_not_try_to_create_the_role_it_authenticates_as():
    """The one statement that stays the operator's, because it cannot be automated."""
    assert "CREATE ROLE" not in script(), (
        "db-init logs in AS POSTGRES_USER, so it cannot create that role; the "
        "docs keep that statement"
    )


@pytest.mark.skipif(shutil.which("sh") is None, reason="no POSIX shell")
def test_the_script_is_valid_posix_shell():
    """busybox ash, not bash. A syntax error here fails at run time, not at render."""
    body = script()
    out = subprocess.run(["sh", "-n"], input=body, capture_output=True, text=True)
    assert out.returncode == 0, f"`sh -n` rejected the db-init script:\n{out.stderr}"
    # Negative control: the same check must reject something broken, or a green
    # above would prove only that `sh -n` was handed nothing it could object to.
    broken = subprocess.run(["sh", "-n"], input="if [ 1 -eq 1 ]; then\n", capture_output=True, text=True)
    assert broken.returncode != 0, "`sh -n` accepted an unterminated `if` -- the check is inert"


# --------------------------------------------------------------------------
# What waits for it
# --------------------------------------------------------------------------

@pytest.mark.parametrize("service", GATED_ON_DB_INIT)
def test_every_service_that_reads_a_database_waits_for_it(service):
    dep = overlay_services()[service]["depends_on"]["db-init"]
    assert dep["condition"] == "service_completed_successfully", (
        f"{service} must wait for db-init to COMPLETE, not merely to start; got {dep}"
    )
    assert dep.get("required") is not False, (
        f"{service} -> db-init is marked optional. Measured on Compose 2.x, a "
        "FAILED completion gate marked `required: false` degrades to a warning "
        "and the dependent starts anyway with `up` still exiting 0 -- which is "
        "precisely the start-on-an-empty-schema this service exists to prevent"
    )


def test_temporal_is_told_not_to_create_its_own_databases():
    """auto-setup's create is unconditional; the name test is the only guard."""
    env = overlay_services()["temporal"]["environment"]
    assert env["SKIP_DB_CREATE"] == "true", (
        "without this, auto-setup runs CREATE DATABASE as POSTGRES_USER, which "
        f"on a managed instance normally lacks CREATEDB; got {env.get('SKIP_DB_CREATE')!r}"
    )


def test_the_creator_and_the_consumer_read_one_pair_of_names():
    """A name each side hardcodes separately is a name that can drift."""
    creator = db_init()["environment"]
    consumer = overlay_services()["temporal"]["environment"]
    assert creator["TEMPORAL_DATABASE"] == consumer["DBNAME"], (
        f"{creator['TEMPORAL_DATABASE']!r} != {consumer['DBNAME']!r}"
    )
    assert creator["TEMPORAL_VISIBILITY_DATABASE"] == consumer["VISIBILITY_DBNAME"], (
        f"{creator['TEMPORAL_VISIBILITY_DATABASE']!r} != {consumer['VISIBILITY_DBNAME']!r}"
    )


def test_the_bundled_path_is_untouched():
    """This must not change how the default install behaves.

    There POSTGRES_USER is the superuser and both upstream images already create
    everything, so a bootstrap job would be redundant -- and SKIP_DB_CREATE on
    the bundled path would break Temporal outright.
    """
    base = _load(BASE)["services"]
    assert "db-init" not in base, "db-init must live in the overlay only"
    assert "SKIP_DB_CREATE" not in (base["temporal"].get("environment") or {}), (
        "SKIP_DB_CREATE must not reach the bundled path"
    )


# --------------------------------------------------------------------------
# Render layer
# --------------------------------------------------------------------------

@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not installed")
def test_the_layered_project_renders_with_db_init_and_without_postgres(tmp_path):
    env_file = tmp_path / "env"
    env_file.write_text("".join(f"{k}={v}\n" for k, v in FAKE_ENV.items()))
    out = subprocess.run(
        ["docker", "compose", "--env-file", str(env_file), "-f", BASE, "-f", OVERLAY, "config"],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )
    assert out.returncode == 0, f"`docker compose config` failed:\n{out.stderr}"
    rendered = yaml.safe_load(out.stdout)["services"]
    assert len(rendered) > 10, f"suspiciously small render ({len(rendered)} services)"
    assert "db-init" in rendered
    assert "postgres" not in rendered, "the bundled container is still in the project"
    assert len(rendered["db-init"]["command"]) == 1, (
        f"the rendered command must stay one argv element; got {rendered['db-init']['command']!r}"
    )
    for service in GATED_ON_DB_INIT:
        dep = rendered[service]["depends_on"]["db-init"]
        assert dep["condition"] == "service_completed_successfully"
        assert dep["required"] is True, f"{service} -> db-init rendered as optional: {dep}"
