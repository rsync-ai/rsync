"""One loopback rule, implemented eight times, in five languages.

A plain-http OAuth token endpoint is refused everywhere because the
client-credentials grant POSTs the client secret to that URL on *every* token
fetch. Over http to a remote host that hands a permanent credential to anyone
on the path -- strictly worse than the short-lived bearer token it buys, and
not repairable by any broker-side setting. The broker never sees this hop, so
there is no central place to enforce it: each client that fetches a token must
refuse for itself.

Loopback is exempt and the exemption is load-bearing, not a convenience:
Google's Workload Identity token server is ``http://localhost:14293``, so a
rule that said "https only" would make GCP's own recommended setup
unconfigurable. That is why the rule is "http off loopback", which needs a host
parse -- and a host parse is where eight independent implementations can
plausibly disagree.

The eight sites, and why they cannot be one function:

  shared/go/kafkaclient/config.go                Go tier (own test: tokenmech_test.go)
  llm-service/src/utils/kafka_security.py        Python tier
  shared/mcp-connectors/.../debezium/connector.py  Debezium connector build
  shared/internal/infra/kafka-connect/connect-entrypoint.sh  Connect image (bash)
  scripts/kafka-init-new-topics.sh               add-a-topic script (POSIX sh)
  docker-compose.quickstart.yml                  inline kafka-init (POSIX sh)
  deploy/helm/rsync-ai/templates/jobs/kafka-init.yaml  inline kafka-init (POSIX sh)
  deploy/helm/rsync-ai/templates/_helpers.tpl    render-time gate (sprig)

install.sh downloads exactly one file, the quickstart compose, so the compose
side can ship no sidecar to source; the chart renders rather than executes; the
images do not contain each other's scripts. Duplication is the design. This
file is the check that the duplicates agree.

The interesting cases are not the obvious ones. ``127.0.0.1.evil.example.com``
and ``localhost.evil.example.com`` are ordinary remote hosts that a prefix or
substring match reads as loopback -- an attacker registers either and the
refusal is bypassed for free. They are in the table below for that reason, and
test_control_the_table_discriminates proves the table can tell the two answers
apart rather than agreeing on everything.
"""

from __future__ import annotations

import json
import pathlib
import re
import shutil
import subprocess
import sys
import textwrap

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from src.utils.kafka_security import (  # noqa: E402
    ENV_OAUTH_ALLOW_INSECURE_TOKEN_ENDPOINT,
    _token_endpoint_is_insecure,
)

REPO = pathlib.Path(__file__).resolve().parents[2]

# One whole repo-relative string per subject, not `REPO / "a" / "b"` segments.
# test_ci_filter_covers_every_guard_subject.py derives a guard's subjects from
# its source by reading string literals that look like paths; a path assembled
# from segments is invisible to it, so the census would report this guard as
# declaring no subject and stay green while the ci.yml `llm` filter missed one.
# That is how shared/go/kafkaclient/** came to be absent from the filter in the
# first place -- the census could not see the only subject that was uncovered.
GO_CONFIG = REPO / "shared/go/kafkaclient/config.go"
PY_SECURITY = REPO / "llm-service/src/utils/kafka_security.py"
DEBEZIUM = REPO / "shared/mcp-connectors/internal/debezium/versions/v1.0.0/connector.py"
CONNECT_ENTRYPOINT = REPO / "shared/internal/infra/kafka-connect/connect-entrypoint.sh"
OPS_SCRIPT = REPO / "scripts/kafka-init-new-topics.sh"
QUICKSTART = REPO / "docker-compose.quickstart.yml"
CHART_JOB = REPO / "deploy/helm/rsync-ai/templates/jobs/kafka-init.yaml"
HELPERS = REPO / "deploy/helm/rsync-ai/templates/_helpers.tpl"

# The compose files that describe a REAL deployment. The opt-out is for a
# disposable test rig and must never be given a value in either of these; see
# test_no_real_deployment_turns_the_opt_out_on.
REAL_COMPOSE = (REPO / "docker-compose.yml", REPO / "docker-compose.prod.yml")

OPT_OUT = ENV_OAUTH_ALLOW_INSECURE_TOKEN_ENDPOINT


# ---------------------------------------------------------------------------
# the table every implementation must reproduce
#
# True  = insecure, must be refused
# False = allowed (https anywhere, or http to loopback)
# ---------------------------------------------------------------------------
TABLE = [
    # https is always fine, whatever the host
    ("https://idp.example/oauth2/token", False),
    ("https://idp.example:8443/oauth2/token", False),
    ("HTTPS://IDP.EXAMPLE/oauth2/token", False),
    # http to loopback: the Google Workload Identity sidecar shape
    ("http://localhost:14293/token", False),
    ("http://localhost/token", False),
    ("http://LOCALHOST:14293/token", False),
    ("http://localhost.:14293/token", False),     # trailing dot is still loopback
    ("http://idp.localhost/token", False),        # RFC 6761 reserves *.localhost
    ("http://127.0.0.1:8080/token", False),
    ("http://127.9.9.9:8080/token", False),       # all of 127.0.0.0/8 is loopback
    ("http://[::1]:14293/token", False),
    # http to anywhere else: refused
    ("http://idp.example/oauth2/token", True),
    ("http://idp.example:8080/oauth2/token", True),
    ("http://10.0.0.5:8080/token", True),
    ("http://[2001:db8::1]:8080/token", True),
    # loopback-LOOKING, actually remote -- a substring or prefix match fails here
    ("http://127.0.0.1.evil.example.com/token", True),
    ("http://localhost.evil.example.com/token", True),
    ("http://evil.example.com/?x=localhost", True),
    ("http://user@evil.example.com/token", True),      # userinfo is not the host
    ("http://user:pw@localhost.evil.example.com/t", True),
]

URLS = [url for url, _ in TABLE]
EXPECTED = [verdict for _, verdict in TABLE]


def _text(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Python (the tier, and the Debezium connector's own copy)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("url,expected", TABLE)
def test_the_python_tier_reproduces_the_table(url, expected):
    assert _token_endpoint_is_insecure(url) is expected


@pytest.mark.parametrize("url,expected", TABLE)
def test_the_debezium_connector_reproduces_the_table(url, expected):
    """The connector ships in its own image and cannot import the tier."""
    namespace: dict = {}
    src = _text(DEBEZIUM)
    match = re.search(
        r"^def _token_endpoint_is_insecure.*?(?=^def |\Z)", src, re.S | re.M
    )
    assert match, "_token_endpoint_is_insecure not found in the debezium connector"
    helper = re.search(r"^def _is_loopback_host.*?(?=^def |\Z)", src, re.S | re.M)
    assert helper, "_is_loopback_host not found in the debezium connector"
    exec("import ipaddress, urllib.parse\n" + helper.group(0) + match.group(0), namespace)
    assert namespace["_token_endpoint_is_insecure"](url) is expected


# ---------------------------------------------------------------------------
# the four shell copies
#
# Each is extracted from its own file and run against the whole table in one
# subprocess. The function bodies are not reimplemented here -- if a site is
# edited, the edited code is what runs.
# ---------------------------------------------------------------------------

def _shell_function(path: pathlib.Path, name: str, *, unescape_compose=False) -> str:
    src = _text(path)
    if unescape_compose:
        # Compose doubles '$' to escape its own interpolation; the shell inside
        # the container sees single '$'.
        src = src.replace("$$", "$")
    match = re.search(
        rf"^( *){re.escape(name)}\(\) \{{.*?^\1\}}", src, re.S | re.M
    )
    assert match, f"{name}() not found in {path.name}"
    return textwrap.dedent(match.group(0))


SHELL_SITES = {
    "connect-entrypoint.sh": (CONNECT_ENTRYPOINT, "token_endpoint_is_insecure", "bash", False),
    "kafka-init-new-topics.sh": (OPS_SCRIPT, "_insecure_token_endpoint", "sh", False),
    "docker-compose.quickstart.yml": (QUICKSTART, "_ins_tok", "sh", True),
    "chart kafka-init.yaml": (CHART_JOB, "insecure_token_endpoint", "sh", False),
}


@pytest.mark.parametrize("site", sorted(SHELL_SITES))
def test_every_shell_copy_reproduces_the_table(site):
    path, name, shell, unescape = SHELL_SITES[site]
    body = _shell_function(path, name, unescape_compose=unescape)
    script = body + "\nfor u in " + " ".join(f"'{u}'" for u in URLS) + "; do\n"
    script += f"  if {name} \"$u\"; then echo true; else echo false; fi\ndone\n"
    proc = subprocess.run(
        [shell, "-c", script], capture_output=True, text=True, timeout=60
    )
    assert proc.returncode == 0, proc.stderr
    got = [line == "true" for line in proc.stdout.split()]
    assert len(got) == len(EXPECTED), proc.stdout
    mismatched = [
        (url, want, have)
        for url, want, have in zip(URLS, EXPECTED, got)
        if want != have
    ]
    assert not mismatched, f"{site} disagrees with the table: {mismatched}"


# ---------------------------------------------------------------------------
# the chart's render-time gate
# ---------------------------------------------------------------------------

BASE_VALUES = {
    "secrets": {
        "jwtSecret": "x" * 32,
        "encryptionKey": "y" * 32,
        "postgresPassword": "pw",
        "minioAccessKey": "ak",
        "minioSecretKey": "sk",
    },
    "frontend": {"apiUrl": "http://api.example", "publicUrl": "http://app.example"},
}


def _render(tmp_path, endpoint, allow_insecure=False):
    values = json.loads(json.dumps(BASE_VALUES))
    values["kafka"] = {
        "enabled": False,
        "replicationFactor": "1",
        "external": {
            "bootstrapServers": "broker:9093",
            "securityProtocol": "SASL_SSL",
            "saslMechanism": "OAUTHBEARER",
            "oauth": {
                "tokenEndpoint": endpoint,
                "clientId": "cid",
                "clientSecret": "s3cr3t",
                "allowInsecureTokenEndpoint": allow_insecure,
            },
        },
    }
    path = tmp_path / "values.yaml"
    path.write_text(json.dumps(values), encoding="utf-8")
    return subprocess.run(
        ["helm", "template", "release", str(REPO / "deploy" / "helm" / "rsync-ai"),
         "-f", str(path)],
        capture_output=True, text=True, cwd=REPO, timeout=180,
    )


# Spelled out on each case rather than aliased to a name. The alias read
# better, but test_ci_guarantees_the_helm_the_chart_guards_need.py finds
# helm-gated cases by reading each decorator's SOURCE, so `@needs_helm` is
# invisible to it -- and a case it cannot see is a case its CI check does not
# cover. The repetition is what keeps this file inside that guarantee.
@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
@pytest.mark.parametrize("url,expected", TABLE)
def test_the_chart_refuses_exactly_the_same_urls(tmp_path, url, expected):
    """The chart is the only site that can refuse before anything runs.

    It has to agree with the runtime sites anyway: an endpoint injected through
    existingSecret or extraEnv never passes through a template, so the
    containers keep their own check and the two must not disagree about which
    URLs are acceptable.
    """
    proc = _render(tmp_path, url)
    refused = proc.returncode != 0 and "not loopback" in (proc.stderr + proc.stdout)
    assert refused is expected, proc.stderr or proc.stdout


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_the_chart_opt_out_permits_the_refused_url(tmp_path):
    proc = _render(tmp_path, "http://idp.example/oauth2/token", allow_insecure=True)
    assert proc.returncode == 0, proc.stderr


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_the_opt_out_env_ships_only_when_opted_in(tmp_path):
    """Default-off is what keeps the containers' own refusal armed.

    If the chart shipped the opt-out unconditionally, every runtime check in
    every pod would be disabled by the chart that is supposed to enforce them.
    """
    entry = re.compile(rf"^\s*- name: {re.escape(OPT_OUT)}$", re.M)
    off = _render(tmp_path, "https://idp.example/oauth2/token")
    assert off.returncode == 0, off.stderr
    assert not entry.search(off.stdout)
    on = _render(tmp_path, "http://idp.example/oauth2/token", allow_insecure=True)
    assert on.returncode == 0, on.stderr
    assert entry.search(on.stdout), "opt-out was requested but no container got it"


# ---------------------------------------------------------------------------
# the escape hatch itself
# ---------------------------------------------------------------------------

ALL_SITES = {
    "go": GO_CONFIG,
    "python": PY_SECURITY,
    "debezium": DEBEZIUM,
    "connect-entrypoint": CONNECT_ENTRYPOINT,
    "ops-script": OPS_SCRIPT,
    "quickstart": QUICKSTART,
    "chart-job": CHART_JOB,
    "chart-helpers": HELPERS,
}


@pytest.mark.parametrize("site", sorted(ALL_SITES))
def test_every_refusal_names_its_own_escape_hatch(site):
    """A refusal that only says "use https" strands the operator whose IdP
    genuinely is on a plain-http host they control -- they have no way to learn
    the opt-out exists except by reading our source."""
    text = _text(ALL_SITES[site])
    if site == "chart-helpers":
        # The helper computes the verdict; the chart's message lives in
        # validate.yaml and is asserted by the render tests above.
        assert "allowInsecureTokenEndpoint" in text
        return
    assert OPT_OUT in text or "allowInsecureTokenEndpoint" in text


# A line that ASSIGNS the value, as opposed to the many lines that merely name
# it -- every refusal message does the latter, on purpose, and several of those
# messages wrap so that the name starts a line inside a quoted string. What
# separates the two is what FOLLOWS the value: an assignment ends there (end of
# line, or a closing brace/comma), while a message carries on in prose
# ("...=true for a disposable test rig").
SETS_IT = re.compile(
    rf"""(?:^[\s\-]*(?:export\s+|name:\s*)?   # a line of its own, or a k8s entry
         |[{{,]\s*)                           # or a dict/JSON member
        ["']?{re.escape(OPT_OUT)}["']?
        \s*[:=]\s*                            # the assignment itself
        (?:\$\{{[^}}]*:-)?                    # compose passthrough: ${{VAR:-true}}
        ["']?true["']?
        \s*[,;}}\]]*\s*(?:\#.*)?$             # ...and nothing else on the line
     """,
    re.I | re.M | re.X,
)

# The Helm env entry spells the value on the NEXT line, so match the pair.
SETS_IT_K8S = re.compile(
    rf"^\s*-\s*name:\s*{re.escape(OPT_OUT)}\s*\n\s*value:\s*[\"']?true", re.I | re.M
)

# The chart emits the k8s env entry only when an operator asks for it. These two
# files are allowed to contain SETS_IT_K8S for that reason, and
# test_the_chart_env_entries_are_guarded proves the guard is really there.
@pytest.mark.parametrize("path", REAL_COMPOSE, ids=lambda p: p.name)
def test_no_real_deployment_turns_the_opt_out_on(path):
    """The opt-out exists for a throwaway rig with a fixture secret on a
    private network. Giving it a value in a file that describes a real
    deployment disables the check for the exact case it was written for,
    silently.

    The rule is about the VALUE, not the name. docker-compose.yml passes the
    variable through with an empty default, because kafka-connect and the
    kafka-init job read their whole security profile from the x-kafka-security
    anchor: without the passthrough the JVM containers are the only ones an
    operator cannot configure, which would leave the same image behaving
    differently under compose than under the chart. An empty default is
    "unset", so the refusal stays armed -- forwarding a variable is not
    setting it. What must never appear is an actual value, which is what
    SETS_IT / SETS_IT_K8S below match and what
    test_the_test_rig_is_the_only_place_that_sets_it polices repo-wide.
    """
    if not path.exists():
        pytest.skip(f"{path.name} not present")
    text = _text(path)
    assert not SETS_IT.search(text), f"{path.name} turns the opt-out on"
    assert not SETS_IT_K8S.search(text), f"{path.name} turns the opt-out on"


CHART_CONDITIONAL = (
    "deploy/helm/rsync-ai/templates/_helpers.tpl",
    "deploy/helm/rsync-ai/templates/connectors/cdc.yaml",
)


@pytest.mark.parametrize("name", CHART_CONDITIONAL)
def test_the_chart_env_entries_are_guarded(name):
    """An unguarded entry would disable every pod's own check by default."""
    text = _text(REPO / name)
    match = SETS_IT_K8S.search(text)
    assert match, f"{name} no longer ships the opt-out entry at all"
    preceding = text[:match.start()].rsplit("{{", 1)[-1]
    assert "allowInsecureTokenEndpoint" in preceding, (
        f"{name} emits the opt-out without checking the values key"
    )


def test_the_test_rig_is_the_only_place_that_sets_it():
    """Inverse of the rule above: exactly one tracked file may TURN IT ON.

    The chart's conditional env entry does not count -- it is emitted only when
    an operator asks for it, and test_the_opt_out_env_ships_only_when_opted_in
    proves the condition holds.
    """
    tracked = subprocess.run(
        ["git", "ls-files"], cwd=REPO, capture_output=True, text=True, timeout=120
    )
    assert tracked.returncode == 0, tracked.stderr
    setters = []
    for name in tracked.stdout.splitlines():
        path = REPO / name
        if path.suffix not in (".yml", ".yaml", ".py", ".sh", ".env", ".tpl", ".tmpl"):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError, FileNotFoundError):
            continue
        if SETS_IT.search(text) or (
            SETS_IT_K8S.search(text) and name not in CHART_CONDITIONAL
        ):
            setters.append(name)
    assert setters == ["deploy/helm/rsync-ai/test/kind/kafka-matrix/run.py"], setters


def test_control_the_setter_regex_tells_setting_from_naming():
    """Without this control the test above passes by matching nothing."""
    assert SETS_IT.search('KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT=true')
    assert SETS_IT.search('  "KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT": "true"')
    assert SETS_IT.search('  KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT: "true"')
    assert SETS_IT_K8S.search(
        '- name: KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT\n  value: "true"'
    )
    assert SETS_IT.search(
        'ALLOW = {"KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT": "true"}'
    )
    # A compose passthrough whose DEFAULT is true turns it on for everyone who
    # does not override it -- the same outcome as a literal, so it must match.
    assert SETS_IT.search(
        '  KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT: '
        '${KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT:-true}'
    )
    # ...but a passthrough that defaults to EMPTY is a forward, not a setting:
    # empty means unset, so the refusal stays armed. This is the shape
    # docker-compose.yml and docker-compose.quickstart.yml actually ship.
    assert not SETS_IT.search(
        '  KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT: '
        '${KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT:-}'
    )
    # the shapes that must NOT count: a refusal message (including one that
    # wraps so the name starts the line), and a read
    assert not SETS_IT.search(
        'echo "  set KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT=true for a rig"'
    )
    assert not SETS_IT.search(
        '    "KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT=true for a "'
    )
    assert not SETS_IT.search(
        '[ "${KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT:-}" != "true" ]'
    )


# ---------------------------------------------------------------------------
# controls -- a table that cannot fail proves nothing
# ---------------------------------------------------------------------------

def test_control_the_table_discriminates():
    """Both verdicts are represented, and the adversarial rows are the ones
    that separate a host parse from a substring match."""
    assert True in EXPECTED and False in EXPECTED
    assert _token_endpoint_is_insecure("http://localhost/token") is False
    assert _token_endpoint_is_insecure("http://localhost.evil.example.com/t") is True


@pytest.mark.parametrize("site", sorted(SHELL_SITES))
def test_control_each_extraction_reads_its_own_file(site):
    """Guards against an extraction that silently matched the wrong file and
    so tested the same text four times."""
    path, name, _shell, unescape = SHELL_SITES[site]
    body = _shell_function(path, name, unescape_compose=unescape)
    assert f"{name}() {{" in body
    assert body.count("localhost") >= 1
