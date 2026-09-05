"""docker-compose.prod.yml must not hold TLS half-on.

docker-compose.prod.yml is the EXTERNAL-database path: POSTGRES_HOST is
required, every service carries RSYNC_REQUIRE_REMOTE_DB=true, and the target is
a managed instance (RDS / Aurora / Cloud SQL / Azure Flexible Server) reached
over a network. TLS there is not optional.

It was reached through TWO variables that did not agree:

  * DB_SSLMODE            -- the documented knob (.env.prod.example), read by
                             orchestrator (internal/config/config.go:168) and,
                             under its native name POSTGRES_SSLMODE, by
                             temporal-adapter (internal/db/db.go:23). Default
                             here: `require`.
  * POSTGRES_TLS_ENABLED  -- Temporal's switch, because Temporal reads neither
                             of the above. Default here: `false`.

So an operator who set the documented knob got three services on TLS and
Temporal on plaintext, with no error anywhere. That shape is quiet in both
directions:

  * A pgx/libpq DSN naming no sslmode does not fail -- it negotiates PLAINTEXT.
    The database password crosses the network in the clear while api-gateway in
    the same project connects over TLS and looks like proof the setting took.
  * On a database that REFUSES plaintext the failure lands on Temporal's schema
    step, which runs BEFORE the server binds, so the symptom is not a TLS error
    but "every pipeline hangs".

Two more services rendered plaintext at the prod defaults for the same reason:
kafka-mcp-sink-mcp (base default ${DB_SSLMODE:-disable}; a failed ledger ping
stops the worker, 0 rows reach the destination, pipeline still reports
"completed") and llm-service (base default hardcoded `?sslmode=disable`).

Two layers, because the render layer needs a docker CLI and a skip is not a
pass:

  1. static, runs everywhere: the defaults written in the file must agree.
  2. render: `docker compose config` across the knob's range, asserting the
     merged project is coherent at each point AND that it still moves -- a file
     that pinned TLS on would pass a one-sided check.
"""

import os
import re
import shutil
import subprocess

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
PROD = os.path.join(REPO_ROOT, "docker-compose.prod.yml")
BASE = os.path.join(REPO_ROOT, "docker-compose.yml")

# Temporal is two binaries reading different env names for one fact. The schema
# tool (temporal-sql-tool) runs FIRST and reads the left column; the server
# reads the right. Writing one without the other is the silent-failure shape.
TEMPORAL_TLS_SWITCHES = ("SQL_TLS", "SQL_TLS_ENABLED")

# libpq sslmode values that actually establish TLS. `prefer` and `allow` are
# "try TLS, fall back to plaintext" -- they are NOT protection, so they are not
# here, and Temporal's on/off switch maps them to off (the weaker of the two,
# never silently promoted).
TLS_SSLMODES = ("require", "verify-ca", "verify-full")

# Vars the merged prod render hard-requires via ${VAR:?...}. One unset `:?`
# aborts interpolation for the WHOLE merged config, so the render fails with a
# message about that var and nothing about TLS.
REQUIRED = {
    "INTERNAL_SERVICE_SECRET": "FAKEPLACEHOLDER",
    "MINIO_ACCESS_KEY_ID": "FAKEPLACEHOLDER",
    "MINIO_SECRET_ACCESS_KEY": "FAKEPLACEHOLDER",
    "POSTGRES_USER": "FAKEPLACEHOLDER",
    "POSTGRES_PASSWORD": "FAKEPLACEHOLDER",
    "POSTGRES_HOST": "db.example.com",
    "ENCRYPTION_KEY": "FAKEPLACEHOLDER",
    "REDIS_PASSWORD": "FAKEPLACEHOLDER",
}


def _uncommented(body):
    """The file with `#` lines dropped.

    Not cosmetic. This file explains its own defaults in prose, so the comment
    beside the kafka-mcp-sink override quotes `${DB_SSLMODE:-disable}` verbatim
    to say what the base file does. Scanning raw text sees that quotation as a
    second, disagreeing default and fails on the documentation of the fix --
    the same trap the sibling chart test hit with SQL_TLS_ENABLED prose.
    """
    return "\n".join(l for l in body.splitlines() if not l.strip().startswith("#"))


def _defaults_for(var, body):
    """Every default written for ${var:-X} in real settings, as a set of X."""
    return set(re.findall(r"\$\{" + re.escape(var) + r":-([^}]*)\}", _uncommented(body)))


# ---------------------------------------------------------------------------
# Layer 1 -- static. No docker needed, so it runs in CI unconditionally.
# ---------------------------------------------------------------------------


def test_the_two_tls_defaults_in_prod_agree():
    """DB_SSLMODE's default and POSTGRES_TLS_ENABLED's default are one fact.

    This is the assertion the original defect violated: `require` on one side,
    `false` on the other, in the same file.
    """
    body = open(PROD).read()

    sslmodes = _defaults_for("DB_SSLMODE", body)
    switches = _defaults_for("POSTGRES_TLS_ENABLED", body)

    assert sslmodes, "no ${DB_SSLMODE:-...} in docker-compose.prod.yml -- renamed?"
    assert switches, "no ${POSTGRES_TLS_ENABLED:-...} in docker-compose.prod.yml -- renamed?"
    assert len(sslmodes) == 1, f"DB_SSLMODE defaults disagree with each other: {sslmodes}"
    assert len(switches) == 1, f"POSTGRES_TLS_ENABLED defaults disagree with each other: {switches}"

    sslmode = sslmodes.pop()
    switch = switches.pop()
    want = "true" if sslmode in TLS_SSLMODES else "false"

    assert switch == want, (
        f"docker-compose.prod.yml defaults DB_SSLMODE to `{sslmode}` but "
        f"POSTGRES_TLS_ENABLED to `{switch}` (expected `{want}`).\n"
        "Temporal reads neither DB_SSLMODE nor POSTGRES_SSLMODE, so these two "
        "defaults are the same fact written twice. Disagreeing, they put the "
        "Go services and Temporal on opposite sides of the TLS boundary with "
        "no error: either the metadata-DB password crosses the network in "
        "plaintext, or Temporal's schema step is rejected before the server "
        "binds and every pipeline hangs with no TLS error in any log."
    )


def test_both_temporal_tls_binaries_are_configured_from_the_same_source():
    """SQL_TLS (schema tool) and SQL_TLS_ENABLED (server) must move together."""
    body = open(PROD).read()
    sources = {}
    for line in body.splitlines():
        line = line.strip().lstrip("- ").strip()
        for var in TEMPORAL_TLS_SWITCHES:
            if line.startswith(f"{var}="):
                sources[var] = line.split("=", 1)[1]

    missing = [v for v in TEMPORAL_TLS_SWITCHES if v not in sources]
    assert not missing, (
        f"docker-compose.prod.yml sets {sorted(sources)} but not {missing}. "
        "auto-setup runs temporal-sql-tool (SQL_TLS) and then the server "
        "(SQL_TLS_ENABLED); configuring only one leaves the other in plaintext."
    )
    assert len(set(sources.values())) == 1, (
        "the Temporal schema tool and the Temporal server read their TLS "
        f"switch from different expressions: {sources}. They are one fact."
    )


# ---------------------------------------------------------------------------
# Layer 2 -- render. Asserts on the merged project, not on the source text.
# ---------------------------------------------------------------------------


def _render(**overrides):
    env = dict(os.environ)
    env.update(REQUIRED)
    env.update(overrides)
    out = subprocess.run(
        ["docker", "compose", "-f", BASE, "-f", PROD, "config"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        env=env,
    )
    assert out.returncode == 0, f"docker compose config failed:\n{out.stderr[-3000:]}"
    return yaml.safe_load(out.stdout)


def _tls_facts(doc):
    """(service, key, value) for everything in the render that decides TLS."""
    facts = []
    for name, svc in sorted((doc.get("services") or {}).items()):
        env = svc.get("environment") or {}
        if isinstance(env, list):
            env = dict((e.partition("=")[0], e.partition("=")[2]) for e in env)
        for k, v in sorted(env.items()):
            v = "" if v is None else str(v)
            if k in TEMPORAL_TLS_SWITCHES:
                facts.append((name, k, v.lower()))
            elif "sslmode=" in v.lower():
                facts.append((name, k, v.lower().split("sslmode=")[1].split("&")[0]))
            elif k in ("DB_SSLMODE", "POSTGRES_SSLMODE"):
                facts.append((name, k, v.lower()))
    return facts


docker_required = pytest.mark.skipif(
    shutil.which("docker") is None, reason="docker CLI not installed"
)


@docker_required
@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({}, id="prod-defaults"),
        pytest.param({"DB_SSLMODE": "require", "POSTGRES_TLS_ENABLED": "true"}, id="require"),
        pytest.param(
            {"DB_SSLMODE": "verify-full", "POSTGRES_TLS_ENABLED": "true"}, id="verify-full"
        ),
    ],
)
def test_no_service_falls_back_to_plaintext_when_tls_is_on(overrides):
    """With TLS on, nothing in the merged project may render plaintext.

    Deliberately whole-project: the defect was never in the service you were
    looking at, it was in the one you were not.
    """
    facts = _tls_facts(_render(**overrides))
    assert facts, "the render exposed no TLS settings at all -- the probe is broken"

    offenders = [
        f"{svc}: {key}={val}"
        for svc, key, val in facts
        if val in ("disable", "false", "prefer", "allow")
    ]
    assert not offenders, (
        "TLS is on, but these render plaintext to the metadata database:\n  "
        + "\n  ".join(offenders)
        + "\n\nEvery one of these is silent: libpq/pgx negotiates plaintext "
        "rather than erroring, so the deploy looks healthy while the database "
        "password crosses the network in the clear."
    )


@docker_required
def test_tls_can_still_be_turned_off():
    """The escape hatch, and the control that stops the test above being vacuous.

    A file that hardcoded TLS on everywhere would satisfy the assertion above
    without honouring any knob. It must not: pointing this file at a plaintext
    database is supported, it just takes both halves.
    """
    facts = _tls_facts(_render(DB_SSLMODE="disable", POSTGRES_TLS_ENABLED="false"))
    stuck_on = [
        f"{svc}: {key}={val}"
        for svc, key, val in facts
        if val in TLS_SSLMODES or val == "true"
    ]
    assert not stuck_on, (
        "TLS was switched off but these are pinned on, so the knob is a "
        "decoration and the coherence test above proves nothing:\n  "
        + "\n  ".join(stuck_on)
    )


# ---------------------------------------------------------------------------
# Layer 3 -- the shipped template and the gate that blesses it.
#
# Layers 1 and 2 both render with NO --env-file, so they assert on defaults the
# shipped template can override -- and it did. The documented prod launch is
# `--env-file .env.prod` (docker-compose.prod.yml:5, docs/runbook.md), copied
# from .env.prod.example, so the template is the file that actually decides the
# posture of a real deploy. A template shipping `DB_SSLMODE=disable` +
# `POSTGRES_TLS_ENABLED=false` cancels a correct compose default silently, and
# no assertion above can see it.
#
# The gate has to agree with the template too. preflight-prod-config.sh greps
# the env file for a literal `DB_SSLMODE=require`, so a template that omits the
# line, or sets it to anything else, is refused -- and before this layer
# existed the ONE pairing the gate accepted was the half-on one.
# ---------------------------------------------------------------------------

ENV_PROD_TEMPLATE = os.path.join(REPO_ROOT, ".env.prod.example")
PREFLIGHT = os.path.join(REPO_ROOT, "scripts", "preflight-prod-config.sh")


def _template_setting(var, path=None):
    """Last real (uncommented) assignment of `var`, or None if never set."""
    body = open(path or ENV_PROD_TEMPLATE).read()
    hits = re.findall(r"^" + re.escape(var) + r"=(.*)$", body, re.M)
    return hits[-1].strip() if hits else None


def test_the_prod_template_ships_tls_on():
    """.env.prod.example is the EXTERNAL-database template; TLS is not optional.

    Absent is acceptable (the compose defaults are TLS-on and Layer 1 pins
    them); an explicit off is not, because the documented launch passes this
    file with --env-file and an explicit value beats the default.
    """
    sslmode = _template_setting("DB_SSLMODE")
    switch = _template_setting("POSTGRES_TLS_ENABLED")
    assert sslmode is None or sslmode in TLS_SSLMODES, (
        f".env.prod.example ships DB_SSLMODE={sslmode!r}. The documented prod "
        "launch is `--env-file .env.prod` copied from this template, so this "
        "value overrides the compose default and is what a real deploy gets."
    )
    assert switch is None or switch == "true", (
        f".env.prod.example ships POSTGRES_TLS_ENABLED={switch!r}, which "
        "overrides the TLS-on compose default and puts Temporal's metadata "
        "connection back on plaintext -- the exact defect Layer 1 fixed, "
        "re-created one file over."
    )


def test_the_prod_template_is_not_half_on():
    """The two keys are one fact in the template too, not just in compose."""
    sslmode = _template_setting("DB_SSLMODE")
    switch = _template_setting("POSTGRES_TLS_ENABLED")
    if sslmode is None and switch is None:
        return  # both inherit the compose defaults, which Layer 1 pins together
    go_side_on = sslmode is None or sslmode in TLS_SSLMODES
    temporal_side_on = switch is None or switch == "true"
    assert go_side_on == temporal_side_on, (
        f"half-on template: DB_SSLMODE={sslmode!r} POSTGRES_TLS_ENABLED="
        f"{switch!r}. Temporal reads neither DB_SSLMODE nor any DSN, so these "
        "disagreeing means one side of the deploy is plaintext with no error."
    )


def _preflight(env_file):
    out = subprocess.run(
        ["bash", PREFLIGHT],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        env={**os.environ, **REQUIRED, "ENV_FILE": env_file},
    )
    return out.returncode, out.stdout + out.stderr


bash_required = pytest.mark.skipif(
    shutil.which("bash") is None, reason="bash not installed"
)


@bash_required
def test_preflight_refuses_the_half_on_pair(tmp_path):
    """The control that makes the test below mean something.

    This is the pairing the gate used to be the ONLY acceptor of: DB_SSLMODE
    satisfies its literal grep while POSTGRES_TLS_ENABLED=false leaves Temporal
    on plaintext, and it printed "Safe to deploy." Both halves are checked now.
    It exits at the grep stage, before any render, so no docker is needed.
    """
    body = open(ENV_PROD_TEMPLATE).read()
    body = re.sub(r"^POSTGRES_TLS_ENABLED=.*$", "POSTGRES_TLS_ENABLED=false", body, flags=re.M)
    half_on = tmp_path / "half-on.env"
    half_on.write_text(body)
    rc, log = _preflight(str(half_on))
    assert rc != 0, (
        "preflight blessed a half-on env file. DB_SSLMODE=require satisfies its "
        "literal grep, but Temporal reads POSTGRES_TLS_ENABLED and would carry "
        f"the database password in the clear.\n{log[-2000:]}"
    )
    assert "POSTGRES_TLS_ENABLED" in log, (
        "preflight refused the file, but not for the TLS reason -- so this test "
        f"would pass for an unrelated failure and prove nothing.\n{log[-2000:]}"
    )


@docker_required
@bash_required
def test_preflight_accepts_the_shipped_template():
    """The template as shipped must be the configuration the gate blesses.

    Otherwise the first thing an operator does -- copy the template, run the
    documented preflight -- fails, and the repo has no path that is both
    blessed and correct. Before this, the template was rejected verbatim AND
    its own remedy ("delete these two lines") was rejected too.
    """
    rc, log = _preflight(ENV_PROD_TEMPLATE)
    assert rc == 0, (
        "preflight rejects .env.prod.example as shipped, so no operator "
        f"following the documented path gets a clean gate.\n{log[-3000:]}"
    )


@bash_required
def test_preflight_refuses_a_vacuous_render():
    """Three of the four TLS checks are ABSENCE assertions, so an empty render
    satisfies all of them and the gate would bless nothing at all.

    This is the mutation that proves the checks have a subject. Feed the script
    a render forced to the empty string -- the exact shape `2>/dev/null` on the
    `docker compose config` capture can produce -- and it must refuse.

    The pre-fix script failed this: it exited 1 blaming the connector-fs-init
    service for being "absent from the prod render" and told the operator to
    restore it in docker-compose.yml, which is a correct file. Only the single
    PRESENCE assertion (check 4) could fire at all; checks 1-3 printed nothing,
    having passed over the empty set. A gate whose failure message names the
    wrong file is worse than no gate, so assert on the reason, not just on rc.
    """
    src = open(PREFLIGHT).read()
    mutant, n = re.subn(
        r'RENDERED="\$\(docker compose.*?\n\}\n', 'RENDERED=""\n', src, flags=re.S
    )
    assert n == 1, "could not find the render capture to mutate -- test is stale"

    # $ROOT is derived from BASH_SOURCE, so the mutant has to live in scripts/.
    mut_path = os.path.join(REPO_ROOT, "scripts", ".preflight-vacuous-render-mutant.sh")
    try:
        with open(mut_path, "w") as fh:
            fh.write(mutant)
        out = subprocess.run(
            ["bash", mut_path], capture_output=True, text=True, cwd=REPO_ROOT,
            env={**os.environ, **REQUIRED, "ENV_FILE": ENV_PROD_TEMPLATE},
        )
    finally:
        if os.path.exists(mut_path):
            os.remove(mut_path)

    log = out.stdout + out.stderr
    assert out.returncode != 0, (
        f"preflight blessed an EMPTY render -- every check below it is vacuous.\n{log}"
    )
    assert "not a usable subject" in log, (
        "preflight refused the empty render, but not because it noticed the render "
        "was empty -- so it is still one compose edit away from passing vacuously, "
        f"and its message points the operator at the wrong file.\n{log}"
    )


@bash_required
def test_preflight_reads_the_render_without_a_pipe():
    """`set -o pipefail` + `grep -q` + a large piped string is a coin-flip verdict.

    `grep -q` exits at its first match, leaving `echo` writing into a closed
    pipe. `echo` takes SIGPIPE, the pipeline status becomes 141, pipefail
    propagates it, and `if ! ...` inverts it into a confident MISSING for
    content that is provably in the render. It fired on roughly a third of runs
    of this file -- the guard reported connector-fs-init absent from a render
    that contained it at line 229 of 1529.

    A here-string is a temp file, not a pipe, so the reader cannot signal the
    writer. This asserts the shape rather than re-running the race, because the
    race is load-dependent and a green run does not prove its absence.
    """
    src = open(PREFLIGHT).read()
    piped = [
        line.strip()
        for line in src.splitlines()
        if re.search(r'echo\s+"\$RENDERED"\s*\|', line)
    ]
    assert not piped, (
        "these lines pipe the whole render into grep under `set -o pipefail`, "
        "which makes their verdict depend on whether echo finishes writing "
        "before grep exits. Use `grep ... <<< \"$RENDERED\"`:\n  "
        + "\n  ".join(piped)
    )
