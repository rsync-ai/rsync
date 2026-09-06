"""Every component that talks to the metadata database must honour sslMode.

The chart already had one knob -- postgresql.external.sslMode, defaulting to
`require` because that is what every managed Postgres wants. Three of the five
consumers ignored it:

  * orchestrator     reads DB_SSLMODE       (internal/config/config.go:221, default "disable")
  * temporal-adapter reads POSTGRES_SSLMODE (internal/db/db.go:23,          default "disable")
  * temporal         reads none of the above -- the auto-setup image has its own
                     SQL_TLS_ENABLED / SQL_HOST_VERIFICATION / SQL_HOST_NAME / SQL_CA

Only api-gateway and the CDC sink carried it, and they carried it because their
DSN is a URL with `?sslmode=` spelled out in the template.

This is the worst shape a security defect can take, because it is quiet in both
directions. A pgx DSN naming no sslmode does not error -- it negotiates
PLAINTEXT -- so the orchestrator shipped the database password in the clear
while the api-gateway in the same pod network connected over TLS and looked like
proof the setting had taken. And on a database that REFUSES plaintext the
failure lands on Temporal, which then cannot create its schemas, so the symptom
is not "TLS error" but "every pipeline hangs".

Two layers, because one of them must hold without anything installed:

  1. static, runs everywhere: any template that wires up a Postgres connection
     must also reference one of the sslmode/TLS helpers.
  2. render, needs helm: asserts the derivation actually produces the right
     values across the sslMode range. A skip is not a pass, which is why layer 1
     does not depend on it.
"""

import glob
import os
import re
import shutil
import subprocess

import pytest

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
CHART = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai")
TEMPLATES = os.path.join(CHART, "templates")

# Anything that establishes a Postgres connection references one of these.
_WIRES_POSTGRES = re.compile(r'rsync-ai\.postgres\.host|rsync-ai\.postgresEnv')
# ...and must then reference one of these to say how it is secured.
_CARRIES_SSLMODE = re.compile(
    r'rsync-ai\.postgres\.sslmode|rsync-ai\.postgres\.tlsEnabled|rsync-ai\.postgresEnv'
)

BASE_SET = [
    "--set", "secrets.encryptionKey=FAKEPLACEHOLDER",
    "--set", "secrets.jwtSecret=FAKEPLACEHOLDER",
    "--set", "secrets.minioAccessKey=FAKEPLACEHOLDER",
    "--set", "secrets.minioSecretKey=FAKEPLACEHOLDER",
    "--set", "secrets.postgresPassword=FAKEPLACEHOLDER",
    "--set", "frontend.apiUrl=http://api.example.com",
    "--set", "frontend.publicUrl=http://app.example.com",
]


def _template_files():
    out = []
    for root, _, files in os.walk(TEMPLATES):
        for f in files:
            if f.endswith((".yaml", ".tpl")):
                out.append(os.path.join(root, f))
    assert out, "no chart templates found -- the walk is wrong"
    return out


def test_postgresenv_itself_carries_db_sslmode():
    """The shared helper is where four of the five consumers get their env."""
    body = open(os.path.join(TEMPLATES, "_helpers.tpl")).read()
    block = body.split('define "rsync-ai.postgresEnv"')[1].split("{{- end -}}")[0]
    assert "DB_SSLMODE" in block, (
        "rsync-ai.postgresEnv wires DB_HOST/DB_USER/DB_PASSWORD but not DB_SSLMODE. "
        "Every consumer of it then defaults to sslmode=disable and connects in "
        "plaintext, silently, no matter what postgresql.external.sslMode says."
    )


def test_every_template_that_wires_postgres_also_says_how_it_is_secured():
    checked, offenders = 0, []
    for path in _template_files():
        body = open(path).read()
        if not _WIRES_POSTGRES.search(body):
            continue
        checked += 1
        if not _CARRIES_SSLMODE.search(body):
            offenders.append(os.path.relpath(path, REPO_ROOT))
    assert checked >= 4, f"only {checked} templates matched -- the pattern has gone stale"
    assert not offenders, (
        "these templates open a Postgres connection without referencing sslMode or "
        "the derived TLS helpers, so they connect in plaintext regardless of "
        "postgresql.external.sslMode:\n  " + "\n  ".join(offenders)
    )


def test_the_server_and_schema_tool_tls_families_are_set_together():
    """auto-setup runs TWO binaries that configure TLS under different names.

    The server reads its config template: SQL_TLS_ENABLED, SQL_CA, SQL_HOST_NAME,
    SQL_HOST_VERIFICATION. The schema step is temporal-sql-tool, whose own flags
    (verified with `temporal-sql-tool --help` on temporalio/auto-setup:1.22.4)
    are SQL_TLS, SQL_TLS_CA_FILE, SQL_TLS_SERVER_NAME and -- inverted --
    SQL_TLS_DISABLE_HOST_VERIFICATION.

    The schema tool runs FIRST. Configure only the server family and a
    TLS-mandatory database rejects the schema step, auto-setup exits before the
    server binds, and the operator sees hung pipelines with no TLS error in any
    log. The two families are one fact and must be written together.
    """
    sources = sorted(
        glob.glob(os.path.join(REPO_ROOT, "docker-compose*.yml")) + _template_files()
    )

    def sets(body, var):
        """True only for a real assignment -- not a mention in a comment.

        Three spellings reach a container: compose map (`VAR: x`), compose list
        (`- VAR=x`), and a k8s env entry (`- name: VAR`). A `#` line is prose,
        and prose about SQL_TLS_ENABLED is exactly what _helpers.tpl carries.
        """
        for raw in body.splitlines():
            line = raw.strip()
            if line.startswith("#"):
                continue
            line = line.lstrip("- ").strip()
            if line.startswith(f"{var}:") or line.startswith(f"{var}="):
                return True
            if line == f"name: {var}":
                return True
        return False

    checked, offenders = 0, []
    for path in sources:
        body = open(path).read()
        if not sets(body, "SQL_TLS_ENABLED"):
            continue
        checked += 1
        if not sets(body, "SQL_TLS"):
            offenders.append(os.path.relpath(path, REPO_ROOT))
    assert checked >= 2, f"only {checked} files set SQL_TLS_ENABLED -- has it been renamed?"
    assert not offenders, (
        "these configure the Temporal server's TLS but not temporal-sql-tool's, "
        "so auto-setup's schema step still connects in plaintext:\n  "
        + "\n  ".join(offenders)
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed on this machine")
@pytest.mark.parametrize(
    "sslmode,tls_enabled,host_verification",
    [
        ("require", "true", "false"),
        ("verify-ca", "true", "false"),
        ("verify-full", "true", "true"),
        ("disable", "false", "false"),
        # libpq's "try TLS, fall back to plaintext". Temporal is on-or-off, so it
        # maps to off -- the weaker of the two, never silently promoted.
        ("prefer", "false", "false"),
        ("allow", "false", "false"),
    ],
)
def test_temporal_tls_is_derived_from_sslmode(sslmode, tls_enabled, host_verification):
    cmd = ["helm", "template", "r", CHART] + BASE_SET + [
        "--set", "postgresql.enabled=false",
        "--set", "postgresql.external.host=db.example.com",
        "--set", f"postgresql.external.sslMode={sslmode}",
    ]
    out = subprocess.run(cmd, capture_output=True, text=True)
    assert out.returncode == 0, f"helm template failed:\n{out.stderr}"
    pairs = dict(re.findall(r"- name: ([A-Z_]+)\n\s+value: \"?([a-zA-Z0-9.\-/]*)\"?", out.stdout))
    assert pairs.get("SQL_TLS_ENABLED") == tls_enabled, (
        f"sslMode={sslmode}: SQL_TLS_ENABLED={pairs.get('SQL_TLS_ENABLED')}, want {tls_enabled}"
    )
    assert pairs.get("SQL_HOST_VERIFICATION") == host_verification, (
        f"sslMode={sslmode}: SQL_HOST_VERIFICATION={pairs.get('SQL_HOST_VERIFICATION')}, want {host_verification}"
    )
    # The schema tool's family -- same facts, and the last one inverted.
    assert pairs.get("SQL_TLS") == tls_enabled, (
        f"sslMode={sslmode}: SQL_TLS={pairs.get('SQL_TLS')}, want {tls_enabled}"
    )
    inverse = "false" if host_verification == "true" else "true"
    assert pairs.get("SQL_TLS_DISABLE_HOST_VERIFICATION") == inverse, (
        f"sslMode={sslmode}: SQL_TLS_DISABLE_HOST_VERIFICATION="
        f"{pairs.get('SQL_TLS_DISABLE_HOST_VERIFICATION')}, want {inverse} "
        "(it is the INVERSE of SQL_HOST_VERIFICATION, not a copy)"
    )
    for var in ("DB_SSLMODE", "POSTGRES_SSLMODE"):
        assert pairs.get(var) == sslmode, f"sslMode={sslmode}: {var}={pairs.get(var)}"
    assert f"sslmode={sslmode}" in out.stdout, "the api-gateway DATABASE_URL lost its sslmode"
