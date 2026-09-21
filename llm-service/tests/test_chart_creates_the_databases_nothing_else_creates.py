"""On an external PostgreSQL, the chart must create the databases itself.

Nothing in rsync.ai's own code creates a database. On the bundled paths two
upstream images do it as a side effect -- the postgres image's initdb makes
`pipeline_db` from POSTGRES_DB, and Temporal's auto-setup makes `temporal` and
`temporal_visibility` -- and both of those are gone the moment
`postgresql.enabled=false`. That left four CREATE DATABASE statements and two
extensions as the operator's manual job, documented in four places.

Every one of the resulting failures is silent, which is why this is a guard and
not a doc:

  * `pipeline_db` missing -- api-gateway logs `db.Init()` failure at WARN and
    keeps going (cmd/server/main.go:306: the migrate call lives inside the
    success branch, and neither branch exits), so the process stays up serving
    a schema that was never created. In Kubernetes it then wedges rather than
    crashes: readiness is /ready, which pings the pool and asserts SchemaReady
    (main.go:646), so the replica never goes Ready -- while liveness is
    deliberately the static /health (api-gateway.yaml:212-218), so nothing
    restarts it either. The pod sits at 0/1 indefinitely, the Deployment never
    reports Available, and `helm install --wait` times out with no error naming
    the database. Quiet, not loud: the operator gets a timeout, not a cause.
  * `temporal` / `temporal_visibility` missing -- auto-setup runs its schema
    step under `set -e` and the container CrashLoopBackOffs.
  * `uuid-ossp` missing -- 001_init_schema.sql is the FIRST migration and calls
    uuid_generate_v4(). internal/db/migrate.go:127 stops at the first failing
    file, so NO table is created.
  * `pg_trgm` missing -- 020b_workspace_connection_refs.sql builds gin_trgm_ops
    indexes, and the same early stop applies from that file onward.

templates/jobs/db-init.yaml closes all four in one pre-install hook. These
tests pin the three things about it that a plausible re-implementation gets
wrong, each paired with the control that a one-sided fix would still pass.

The ROLE is deliberately NOT created and never will be: db-init authenticates
AS postgresql.username, so it cannot be the thing that creates it. That one
statement stays with the operator, and test_the_role_is_never_claimed_as_created
keeps a later "let's automate the rest of it too" from quietly claiming
otherwise.
"""

import os
import shutil
import subprocess

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
CHART = os.path.join("deploy", "helm", "rsync-ai")

# Same list, same reason, as test_temporal_skips_db_create_on_external_postgres:
# the chart marks these required and a render that ERRORS would skip every
# assertion below while looking exactly like a pass.
HELM_REQUIRED = [
    "secrets.jwtSecret=FAKEPLACEHOLDER",
    "secrets.encryptionKey=FAKEPLACEHOLDERFAKEPLACEHOLDER32",
    "secrets.minioAccessKey=FAKEPLACEHOLDER",
    "secrets.minioSecretKey=FAKEPLACEHOLDER",
    "secrets.redisPassword=FAKEPLACEHOLDER",
    "frontend.apiUrl=https://rsync.example.com",
    "frontend.publicUrl=https://rsync.example.com",
]

PASSWORD = ["secrets.postgresPassword=FAKEPLACEHOLDER"]
EXTERNAL = ["postgresql.enabled=false", "postgresql.external.host=db.example.com"]

pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")


def _helm(*extra):
    cmd = ["helm", "template", "r", CHART]
    for v in list(HELM_REQUIRED) + list(extra):
        cmd += ["--set", v]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=REPO_ROOT)


def _render(*extra):
    out = _helm(*extra)
    assert out.returncode == 0, f"render failed for {extra}:\n{out.stderr}"
    docs = [d for d in yaml.safe_load_all(out.stdout) if d]
    # Positive denominator. A render that produced three objects would satisfy
    # every "is absent" assertion in this file for the wrong reason.
    assert len(docs) > 20, f"only {len(docs)} objects rendered for {extra} -- refusing to judge"
    return docs


def _db_init(docs, kind):
    for d in docs:
        if d.get("kind") == kind and d["metadata"]["name"].endswith("-db-init"):
            return d
    return None


def _job_script(job):
    """The whole shell body. It lives in command[2], NOT args."""
    return job["spec"]["template"]["spec"]["containers"][0]["command"][2]


def _code_lines(script):
    """The shell's executable lines only.

    Every assertion below that asks "does db-init do X" has to ask it of code.
    Asked of the raw text, `"pg_trgm" in script` and `"pg_database" in script`
    both stay true after the call is commented out or replaced -- the name
    survives in the comment explaining it. Two mutations proved exactly that
    before this function existed.
    """
    return [ln for ln in script.splitlines() if not ln.lstrip().startswith("#")]


def _job_env(job):
    env = {}
    for e in job["spec"]["template"]["spec"]["containers"][0]["env"]:
        if "value" in e:
            env[e["name"]] = e["value"]
        else:
            ref = e.get("valueFrom", {}).get("secretKeyRef", {})
            env[e["name"]] = f"<secret {ref.get('name')}/{ref.get('key')}>"
    return env


def test_external_postgres_renders_a_db_init_job():
    docs = _render(*PASSWORD, *EXTERNAL)
    job = _db_init(docs, "Job")
    assert job is not None, (
        "postgresql.enabled=false renders no db-init Job, so nothing creates "
        "pipeline_db. api-gateway's db.Init cannot open a database that does "
        "not exist, so it exits after DB_CONNECT_TIMEOUT and the pod goes into "
        "CrashLoopBackOff -- and no number of restarts helps, because nothing "
        "will ever create the database the gateway is waiting for."
    )


def test_bundled_postgres_renders_no_db_init_job():
    """CONTROL. The in-chart path already creates everything; a hook there is wrong.

    On the bundled path `pipeline_db` comes from the postgres image's
    POSTGRES_DB and Temporal creates its own two as the image superuser.
    Rendering db-init there would connect as that superuser to a database the
    initdb has not necessarily finished with, for no gain. A fix that simply
    rendered the Job unconditionally would pass the test above and fail here.
    """
    docs = _render(*PASSWORD)
    assert _db_init(docs, "Job") is None, "db-init rendered on the bundled-Postgres path"
    assert _db_init(docs, "Secret") is None, "the db-init hook Secret rendered on the bundled path"


def test_the_job_creates_every_database_and_extension_that_is_missing():
    docs = _render(*PASSWORD, *EXTERNAL)
    job = _db_init(docs, "Job")
    env, script = _job_env(job), _job_script(job)

    # The three database NAMES arrive as env, never baked into the shell -- so
    # they can be the same values Temporal and api-gateway read.
    assert env.get("APP_DATABASE") == "pipeline_db"
    assert env.get("TEMPORAL_DATABASE") == "temporal"
    assert env.get("TEMPORAL_VISIBILITY_DATABASE") == "temporal_visibility"

    calls = [ln.strip() for ln in _code_lines(script) if ln.strip().startswith("mkext ")]
    for ext, why in (
        ("uuid-ossp", "001_init_schema.sql is the FIRST migration and calls uuid_generate_v4()"),
        ("pg_trgm", "020b_workspace_connection_refs.sql builds gin_trgm_ops indexes"),
    ):
        assert any(f'"{ext}"' in c for c in calls), (
            f"db-init never CALLS mkext for {ext}. {why}, and "
            f"internal/db/migrate.go:127 stops at the first failing file. "
            f"Calls found: {calls}"
        )


def test_the_create_is_idempotent_by_existence_check_not_by_ignoring_errors():
    """`CREATE DATABASE ... || true` would make a denied create look like a success.

    CREATE DATABASE has no IF NOT EXISTS and cannot run in a transaction, so the
    only two ways to be idempotent are an existence check or swallowing the
    error. Swallowing makes `permission denied to create database` -- the single
    most likely failure on a managed instance -- indistinguishable from `already
    exists`, and the install then proceeds to the exact silent-empty-schema
    outcome this whole hook exists to prevent.
    """
    code = _code_lines(_job_script(_db_init(_render(*PASSWORD, *EXTERNAL), "Job")))

    assert any("pg_database" in ln for ln in code), (
        "no executable line queries pg_database, so db-init is either not "
        "idempotent or idempotent by swallowing the error"
    )
    assert any("ON_ERROR_STOP=1" in ln for ln in code), (
        "psql runs without ON_ERROR_STOP, so a failed statement does not fail the step"
    )

    swallowed = [
        ln.strip()
        for ln in code
        if "CREATE DATABASE" in ln and ("|| true" in ln or "|| :" in ln or "2>/dev/null" in ln)
    ]
    assert not swallowed, (
        "CREATE DATABASE's failure is discarded, which makes `permission denied to "
        "create database` -- the likeliest failure on a managed instance -- "
        f"indistinguishable from `already exists`:\n  " + "\n  ".join(swallowed)
    )
    assert any(ln.strip() == "exit 1" for ln in code), (
        "nothing in db-init exits non-zero, so every failure it detects is reported "
        "to a Job that Helm will nonetheless record as succeeded"
    )


def test_the_hook_runs_before_the_application_not_after():
    """post-install would race a 60s timer that does not retry after it expires.

    internal/db/db.go bounds its connect retry at DB_CONNECT_TIMEOUT, default
    60s, and its own comment says losing it is not self-correcting: main() only
    migrates inside the success branch, so the process runs on with an empty
    schema while /health answers 200. Liveness is /health, so the pod never
    restarts to pick the database up later. The databases must exist BEFORE the
    application Deployment is created, which is what pre-install means and what
    post-install -- kafka-init's phase -- does not.
    """
    docs = _render(*PASSWORD, *EXTERNAL)
    job = _db_init(docs, "Job")
    hook = job["metadata"]["annotations"]["helm.sh/hook"]
    assert hook == "pre-install,pre-upgrade", f"db-init runs at {hook!r}, not pre-install"

    secret = _db_init(docs, "Secret")
    assert secret is not None, (
        "pre-install hooks run BEFORE the release's own resources are created, so "
        "the release Secret does not exist yet and the Job has no password to "
        "mount. A transient hook Secret is what closes that."
    )
    weights = (
        int(secret["metadata"]["annotations"]["helm.sh/hook-weight"]),
        int(job["metadata"]["annotations"]["helm.sh/hook-weight"]),
    )
    assert weights[0] < weights[1], (
        f"the hook Secret's weight {weights[0]} does not precede the Job's {weights[1]}, "
        "so the Job can be created before the Secret it mounts exists"
    )


def test_an_operator_managed_secret_is_read_directly_and_no_hook_secret_is_made():
    """CONTROL for the block above: the transient Secret exists only to fill a gap.

    With secrets.existingSecret the Secret is managed outside the chart and so
    exists independently of Helm's phases -- there is no gap to fill, and
    minting a second copy of the password would put it somewhere the operator
    did not put it.
    """
    docs = _render(*PASSWORD, *EXTERNAL, "secrets.existingSecret=my-operator-secret")
    job = _db_init(docs, "Job")
    assert job is not None, "existingSecret suppressed the whole Job, not just the hook Secret"
    assert _db_init(docs, "Secret") is None, "a hook Secret was minted alongside an existingSecret"
    assert _job_env(job)["PGPASSWORD"] == "<secret my-operator-secret/POSTGRES_PASSWORD>"


def test_external_temporal_gets_no_temporal_databases():
    """CONTROL. temporal.enabled=false means BYO Temporal, not "no Temporal".

    Its databases then belong to the cluster the operator runs, or to Temporal
    Cloud, where this role has no business creating anything. Only the
    application database is ours on that path.
    """
    docs = _render(
        *PASSWORD,
        *EXTERNAL,
        "temporal.enabled=false",
        "temporal.external.address=tmprl.example.com:7233",
    )
    env = _job_env(_db_init(docs, "Job"))
    assert env.get("APP_DATABASE") == "pipeline_db"
    assert "TEMPORAL_DATABASE" not in env, (
        "the chart offers to create Temporal's databases on an instance whose "
        f"Temporal it does not run. Env: {sorted(env)}"
    )


@pytest.mark.parametrize(
    "opt_out,why",
    [
        (["postgresql.dbInit.enabled=false"], "the operator's explicit opt-out"),
        (
            ["postgresql.external.iamAuth=true"],
            "IAM auth leaves db-init no password, and a failing PRE-install hook "
            "aborts the install before anything else is created",
        ),
    ],
)
def test_db_init_is_absent_on_every_path_where_it_could_not_work(opt_out, why):
    extra = list(EXTERNAL) + opt_out
    if "iamAuth=true" not in " ".join(opt_out):
        extra += PASSWORD
    docs = _render(*extra)
    assert _db_init(docs, "Job") is None, f"db-init rendered although {why}"


def test_temporal_is_told_the_truth_about_who_created_its_databases():
    """The manifest an operator reads must not claim a hook that is not rendered.

    templates/infra/temporal.yaml carries the note explaining where `temporal`
    and `temporal_visibility` come from. On the opt-out paths that note has to
    flip, or it sends someone looking for a Job that was never created while
    their pod CrashLoopBackOffs on `database "temporal" does not exist`.
    """

    def _temporal_deployment(docs):
        for d in docs:
            if d.get("kind") != "Deployment":
                continue
            name = d["metadata"]["name"]
            if "temporal" in name and "adapter" not in name:
                return yaml.dump(d)
        return None

    on = _helm(*PASSWORD, *EXTERNAL)
    off = _helm(*PASSWORD, *EXTERNAL, "postgresql.dbInit.enabled=false")
    assert on.returncode == 0 and off.returncode == 0

    # The comment is stripped by the YAML parse, so this one reads the raw text.
    on_text = [b for b in on.stdout.split("---") if "kind: Deployment" in b and "-temporal\n" in b]
    off_text = [b for b in off.stdout.split("---") if "kind: Deployment" in b and "-temporal\n" in b]
    assert on_text and off_text, "no temporal Deployment block found in either render"

    assert "db-init.yaml creates them" in on_text[0], (
        "with db-init on, temporal's manifest no longer says what creates its databases"
    )
    assert "db-init.yaml creates them" not in off_text[0], (
        "with db-init OFF, temporal's manifest still tells the operator a hook creates "
        "its databases. Nothing does, and the pod fails with `database \"temporal\" "
        "does not exist`."
    )


def test_the_role_is_never_claimed_as_created():
    """db-init logs in AS postgresql.username, so it cannot create that role.

    That is the one statement that stays manual, and the failure message has to
    say so -- an operator debugging `password authentication failed` should not
    be hunting for the job that was supposed to have created their role.
    """
    script = _job_script(_db_init(_render(*PASSWORD, *EXTERNAL), "Job"))
    assert not any("CREATE ROLE" in ln for ln in _code_lines(script)), (
        "db-init contains CREATE ROLE. It authenticates as that very role, so the "
        "statement can only run when it is not needed."
    )
    assert "never creates the" in script and "role" in script, (
        "the connection-failure message does not tell the operator that the role is "
        "theirs to create, which is the most likely cause of the failure it reports"
    )
