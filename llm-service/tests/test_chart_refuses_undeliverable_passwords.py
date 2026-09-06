"""Three chart secrets are read two incompatible ways; the chart must refuse the
characters that make the two readings disagree, and no shipped doc may hand the
operator a generator that produces them.

Each of `secrets.postgresPassword`, `secrets.redisPassword` and
`secrets.demoWarehousePassword` is spliced into a URL by one consumer and read
verbatim by another:

    postgresPassword       postgres:// URL (api-gateway DATABASE_URL,
                           connectors/cdc.yaml POSTGRES_URL)
                           vs a libpq `password=` keyword string built by
                           concatenation in backend-orchestrator
                           internal/config/config.go and backend-temporal-adapter
                           internal/db/db.go
                           vs Temporal's verbatim POSTGRES_PWD
    redisPassword          redis:// URL (connectors/cdc.yaml REDIS_URL)
                           vs Options.Password at a dozen Go/Python call sites
                           vs `requirepass <value>` written into redis.conf by
                           the in-chart server's own entrypoint
    demoWarehousePassword  postgres:// URL (api-gateway
                           RSYNC_DEMO_DESTINATION_DSN)
                           vs POSTGRES_PASSWORD on the demo warehouse container,
                           which is what initdb SETS the role's password to

The two readings want opposite escapings, so past a reserved character no value
satisfies both: percent-encode and the verbatim reader authenticates as the
literal `%40`; leave it raw and the URL parser reads the password as a hostname.
The chart cannot re-encode a value it hands to consumers with different parsers,
so it restricts the alphabet instead.

Why this is fatal at render rather than a warning: the runtime failure is
silent. The api-gateway logs one warning on a failed connect, stays 1/1 Ready
and serves mock data, so `helm install` and `kubectl get pods` both look
successful and the operator finds out from the data.

The second test here is the one that would have caught the regression this pair
was written with. Adding the guard turned three shipped
`openssl rand -base64 24` one-liners -- including two in the repo's top-level
README -- into instructions that fail roughly three installs in four, because
base64's alphabet contains `/` and `+`. A rule and the instructions for
satisfying it are two files apart; only a check that reads both keeps them in
step.
"""

import json
import os
import re
import shutil
import subprocess

import pytest

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
CHART = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai")

# The three keys and what each additionally needs before the chart will render
# far enough to reach its own guard. demoWarehousePassword is only read when the
# demo is on, and the demo has two prerequisites of its own.
DEMO_ENABLING = {
    "demo": {"enabled": True},
    "connectors": {
        "sampleData": {"enabled": True},
        "fleet": [
            {
                "id": "postgresql",
                "version": "v1.0.0",
                "image": {"repository": "mcp-postgresql", "tag": "test"},
            }
        ],
    },
}
RESTRICTED = {
    "postgresPassword": {},
    "redisPassword": {},
    "demoWarehousePassword": DEMO_ENABLING,
}

# One representative per disallowed class. Passed through a VALUES FILE, never
# `--set`: `helm --set` consumes backslashes itself, so a `--set` harness would
# hand the chart `backslash` and report the guard as passing on an input it
# never saw.
UNDELIVERABLE = [
    "p@ssword",       # URL userinfo terminator
    "a/b",            # path separator
    "a:b",            # userinfo separator
    "has space",      # breaks a libpq keyword string and redis.conf
    "quo'te",         # needs libpq quoting
    'dq"ote',
    "back\\slash",    # needs libpq escaping
    "pct%20",         # already-encoded input would double-encode
    "hash#es",        # URL fragment; also starts a redis.conf comment
    "q?mark",         # URL query
    "br[ackets]",     # URL host literal
    "tab\there",
]

# Deliberately awkward but deliverable -- every one of these is legal in URL
# userinfo, in a libpq keyword string and verbatim. They exist so the guard
# cannot pass by rejecting everything.
DELIVERABLE = [
    "Abc123-._~",
    "A!$&*+,;=",
    "plainpassword",
    "x^y|z<>()",
    "0123456789abcdef0123456789abcdef",  # what `openssl rand -hex 16` produces
]

BASE_VALUES = {
    "secrets": {
        "jwtSecret": "FAKEPLACEHOLDER",
        "encryptionKey": "FAKEPLACEHOLDERFAKEPLACEHOLDER32",
        "postgresPassword": "FAKEPLACEHOLDER",
        "minioAccessKey": "FAKEPLACEHOLDER",
        "minioSecretKey": "FAKEPLACEHOLDER",
    },
    "frontend": {
        "apiUrl": "https://rsync.example.com",
        "publicUrl": "https://rsync.example.com",
    },
}


def _merge(base, extra):
    out = json.loads(json.dumps(base))
    for k, v in extra.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = v
    return out


def _render(tmp_path, values):
    path = os.path.join(str(tmp_path), "values.json")
    with open(path, "w") as fh:
        json.dump(values, fh)
    proc = subprocess.run(
        ["helm", "template", "release", CHART, "-f", path],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    return proc.returncode, proc.stdout, proc.stderr


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
@pytest.mark.parametrize("key", sorted(RESTRICTED))
def test_the_chart_refuses_a_password_it_cannot_deliver(key, tmp_path):
    extra = RESTRICTED[key]

    # Vacuity floor. Every rejection below is only evidence if the SAME values
    # render cleanly with a safe password -- otherwise the guard could be
    # "passing" on an unrelated render error.
    rc, _, err = _render(tmp_path, _merge(BASE_VALUES, _merge(extra, {"secrets": {key: "safe0123"}})))
    assert rc == 0, f"control render for {key} failed, so nothing below is evidence:\n{err}"

    for password in UNDELIVERABLE:
        vals = _merge(BASE_VALUES, _merge(extra, {"secrets": {key: password}}))
        rc, _, err = _render(tmp_path, vals)
        assert rc != 0, f"secrets.{key}={password!r} rendered; the chart cannot deliver it"
        assert f"secrets.{key}" in err, f"the failure for {password!r} does not name secrets.{key}:\n{err}"
        assert password not in err, f"the failure message echoes the password itself:\n{err}"

    for password in DELIVERABLE:
        vals = _merge(BASE_VALUES, _merge(extra, {"secrets": {key: password}}))
        rc, _, err = _render(tmp_path, vals)
        assert rc == 0, f"secrets.{key}={password!r} is deliverable but was refused:\n{err}"


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_an_empty_redis_password_is_still_allowed(tmp_path):
    """Redis auth is optional and empty means "no password"; the alphabet rule
    only constrains a value that is actually set. A guard written as "must match
    the safe alphabet" rather than "must not contain a bad character" would
    reject the empty string and break every install that does not use Redis
    auth."""
    rc, _, err = _render(tmp_path, _merge(BASE_VALUES, {"secrets": {"redisPassword": ""}}))
    assert rc == 0, err


# `openssl rand -base64 N` emits `+`, `/` and `=`; `/` is disallowed, so such a
# one-liner produces an unusable password most of the time. Anything that
# generates only [A-Za-z0-9] or the URL-safe punctuation is fine.
_GENERATOR = re.compile(r"(openssl\s+rand\s+-\S+(?:\s+\d+)?)")
_DOC_GLOBS = [
    "README.md",
    os.path.join("deploy", "helm", "rsync-ai", "README.md"),
    os.path.join("deploy", "helm", "rsync-ai", "values.yaml"),
    os.path.join("deploy", "helm", "rsync-ai", "templates", "validate.yaml"),
    os.path.join("docs", "deployment", "kubernetes.md"),
    os.path.join("docs", "deployment", "self-hosting.md"),
]


def test_no_shipped_file_tells_you_to_generate_a_password_the_chart_will_refuse():
    offenders = []
    checked = 0
    for rel in _DOC_GLOBS:
        path = os.path.join(REPO_ROOT, rel)
        if not os.path.exists(path):
            continue
        with open(path) as fh:
            lines = fh.read().splitlines()
        for number, line in enumerate(lines, start=1):
            for key in RESTRICTED:
                if key not in line:
                    continue
                checked += 1
                for gen in _GENERATOR.findall(line):
                    if "-base64" in gen:
                        offenders.append(f"{rel}:{number}: {key} <- `{gen}`")

    # Vacuity floor: this test is worthless if it inspected nothing. Every one
    # of the three keys is named in values.yaml at minimum.
    assert checked >= 3, f"only {checked} key mentions scanned; the doc list has gone stale"
    assert not offenders, (
        "these tell the operator to generate a password from an alphabet the "
        "chart refuses (base64 emits `/` and `+`); use `openssl rand -hex 24`:\n  "
        + "\n  ".join(offenders)
    )
