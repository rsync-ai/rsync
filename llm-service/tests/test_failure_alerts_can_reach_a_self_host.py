"""A pipeline that fails silently on a self-host is the failure mode this guard exists for.

api-gateway always runs the notifier consumer (``cmd/server/main.go`` starts it
ungated) and always writes a row to ``pipeline_notifications``. Whether that row also
reaches a human is decided purely by whether a handful of environment variables have
values:

    slackEnabled = NOTIFIER_SLACK_WEBHOOK_URL != ""
    emailEnabled = SMTP_HOST != "" && SMTP_FROM != ""      (notifier.go, both halves)

Neither branch logs an error when it is off, because "off" is a legitimate
configuration. So a stack where the variables are simply not passed through to the
container behaves *exactly* like a stack where the operator chose not to be paged:
rows accumulate in the database, nothing arrives in Slack or email, and no layer
reports a problem. That is what ``docker-compose.quickstart.yml`` -- the only compose
file ``install.sh`` downloads, and the one a self-host actually runs -- did until the
notifier block was added to its api-gateway service. The code was correct the whole
time; the delivery surface was the bug.

``test_quickstart_delivers_what_the_code_reads.py`` could not have caught it. Its name
is broader than its subject: it knows about the Redis, LLM and OTEL variable families
by name and has no notion of notifier delivery. This file covers that surface, and
covers it by DERIVING the variable set from the notifier package's own
``os.Getenv`` calls rather than restating a list here -- add a variable to the
notifier and this test starts demanding it, with no edit in this file.

Two further things are checked, because passing the variable through is necessary and
not sufficient:

* **The link has to resolve.** Every alert's "Open pipeline" button is built on
  ``APP_BASE_URL`` (notifier.go), whose code fallback is ``http://localhost:3000``.
  On a server install that is a dead link in every alert that will ever be sent, and
  it is dead in a way that looks fine from the sender's side. So the quickstart value
  must chain to a variable ``install.sh`` actually persists.
* **The operator has to be able to find the knob.** A variable compose forwards but
  the generated ``.env`` never mentions is not discoverable; the operator would have
  to read the compose file to learn that alerting exists at all. So each variable is
  required to appear in the file ``install.sh`` writes -- or, when its compose value
  chains off another variable, that other variable is required instead.

Static and cheap: text and YAML parsing only, no docker, no Go toolchain.
"""

import functools
import pathlib
import re

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
QUICKSTART = REPO / "docker-compose.quickstart.yml"
INSTALL_SH = REPO / "install.sh"
NOTIFIER_PKG = REPO / "api-gateway" / "internal" / "notifier"
# Named as one repo-relative literal, not assembled from segments, so the CI-reach
# census (test_ci_filter_covers_every_guard_subject.py) can see it: that census derives
# a guard's subjects from string literals that resolve to real files, and a path built
# segment-by-segment resolves to a directory it discards. This file is where the
# slack/email gating lives, so it is a subject of this guard in its own right.
NOTIFIER_GO = REPO / "api-gateway/internal/notifier/notifier.go"

# The service that runs the notifier consumer. Named here rather than derived because
# the coupling is to a Go entrypoint (cmd/server/main.go), not to anything the compose
# file states -- test_the_derivation_found_the_real_notifier asserts the service exists
# so a rename fails loudly instead of emptying the subject.
API_GATEWAY_SERVICE = "api-gateway"

# Variables the notifier reads that the self-host compose deliberately does NOT
# forward. Each entry is a decision, not an oversight, and each is checked two ways
# below: the source must still read it (or the exemption is stale and should be
# deleted) and the compose must still omit it (or the exemption is a lie).
DELIBERATELY_NOT_FORWARDED = {
    "SLACK_SIGNING_SECRET": (
        "Gates the interactive Approve/Reject buttons on Slack messages "
        "(notifier.go), which post back to a PUBLIC inbound endpoint "
        "(/api/v1/slack/interactions). A self-host behind NAT would render buttons "
        "that silently do nothing, which is worse than the plain link button it "
        "falls back to. Enabling inbound Slack interactivity is a separate setup "
        "with its own requirements, not part of being told a pipeline broke."
    ),
}

# The three the delivery decision actually turns on. Used as a positive control: if
# the derivation ever returns a set missing one of these, it has stopped reading the
# notifier and every assertion below would pass on nothing.
DELIVERY_GATES = ("NOTIFIER_SLACK_WEBHOOK_URL", "SMTP_HOST", "SMTP_FROM")

_GETENV = re.compile(r'os\.Getenv\(\s*"([A-Z0-9_]+)"\s*\)')
_VAR_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)")


@functools.lru_cache(maxsize=None)
def _notifier_env_reads() -> frozenset[str]:
    """Every variable the notifier package reads, from its own source."""
    names: set[str] = set()
    for path in sorted(NOTIFIER_PKG.glob("*.go")):
        if path.name.endswith("_test.go"):
            continue
        names.update(_GETENV.findall(path.read_text()))
    return frozenset(names)


@functools.lru_cache(maxsize=None)
def _notifier_go() -> str:
    assert NOTIFIER_GO.is_file(), f"{NOTIFIER_GO} is gone; the gating contract moved"
    return NOTIFIER_GO.read_text()


def _ignore_unknown_tag(loader, suffix, node):
    """Compose merge directives (``!override``) are not YAML tags SafeLoader knows."""
    if isinstance(node, yaml.ScalarNode):
        return loader.construct_scalar(node)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    return loader.construct_mapping(node)


class _ComposeLoader(yaml.SafeLoader):
    pass


_ComposeLoader.add_multi_constructor("!", _ignore_unknown_tag)


@functools.lru_cache(maxsize=None)
def _quickstart_api_gateway_env() -> dict:
    doc = yaml.load(QUICKSTART.read_text(), Loader=_ComposeLoader) or {}
    services = doc.get("services", {}) or {}
    service = services.get(API_GATEWAY_SERVICE)
    if service is None:
        pytest.fail(
            f"{QUICKSTART.name} has no service named {API_GATEWAY_SERVICE!r} "
            f"(it has {sorted(services)}). Either the service was renamed -- in which "
            "case fix API_GATEWAY_SERVICE here -- or this guard is checking an empty "
            "set and reporting a pass."
        )
    raw = service.get("environment") or {}
    if isinstance(raw, list):
        out = {}
        for item in raw:
            name, _, value = str(item).partition("=")
            out[name.strip()] = value
        return out
    return {str(k): ("" if v is None else str(v)) for k, v in raw.items()}


@functools.lru_cache(maxsize=None)
def _install_sh() -> str:
    return INSTALL_SH.read_text()


def _forwarded() -> list[str]:
    """Notifier variables the self-host compose is expected to pass through."""
    return sorted(_notifier_env_reads() - set(DELIBERATELY_NOT_FORWARDED))


def test_the_derivation_found_the_real_notifier():
    """Vacuity floor. Everything below is quantified over a derived set; an empty or
    truncated derivation would make every one of those assertions pass on nothing."""
    assert NOTIFIER_PKG.is_dir(), f"{NOTIFIER_PKG} is gone; this guard has no subject"

    reads = _notifier_env_reads()
    assert len(reads) >= 6, (
        f"derived only {sorted(reads)} from {NOTIFIER_PKG}. The notifier reads at "
        "least the slack webhook, five SMTP variables and the link base -- a set this "
        "small means the regex stopped matching, not that the code got simpler."
    )

    missing_gates = [v for v in DELIVERY_GATES if v not in reads]
    assert not missing_gates, (
        f"{missing_gates} are the variables the fan-out decision turns on and the "
        "derivation did not find them. Either the gating moved out of os.Getenv or "
        "the scan is reading the wrong files."
    )

    # The two-variable email contract, asserted against the source rather than
    # restated. SMTP_HOST alone leaves email off, and that omission is indis-
    # tinguishable from deliberately leaving email off -- which is exactly why both
    # halves have to survive into the operator-facing template below.
    src = _notifier_go()
    assert 'smtpHost != "" && n.smtpFrom != ""' in src.replace("n.smtpHost", "smtpHost"), (
        "notifier.go no longer gates email on BOTH SMTP_HOST and SMTP_FROM. That "
        "conjunction is the premise of test_install_sh_lets_the_operator_find_the_knob "
        "-- re-read the new gating before editing this assertion away."
    )

    # Positive control on the compose side too: the loader has to reach a real service
    # with a real environment block, or the delivery assertions check an empty dict.
    env = _quickstart_api_gateway_env()
    assert len(env) >= 20, (
        f"{API_GATEWAY_SERVICE} in {QUICKSTART.name} declares only {len(env)} "
        "environment entries; that is not the real service block."
    )


def test_quickstart_delivers_every_var_the_notifier_reads():
    """The bug this file exists for: the code reads it, the container never gets it,
    and nothing anywhere says so."""
    env = _quickstart_api_gateway_env()
    missing = [name for name in _forwarded() if name not in env]
    assert not missing, (
        f"{missing} are read by {NOTIFIER_PKG.name} but are not passed to the "
        f"{API_GATEWAY_SERVICE} service in {QUICKSTART.name}. A self-host gets "
        "persist-only alerting: rows in pipeline_notifications, nothing in Slack or "
        "email, and no error at any layer to say so. Add "
        "`NAME: ${NAME:-}` to the service, or -- if the omission is deliberate -- "
        "record it in DELIBERATELY_NOT_FORWARDED with the reason."
    )


def test_every_forwarded_var_is_actually_settable():
    """A hardcoded value forwards the variable and still denies the operator the
    knob, while reading as coverage to the next person who greps for the name."""
    env = _quickstart_api_gateway_env()
    not_settable = []
    for name in _forwarded():
        if name not in env:
            continue  # absence is the previous test's finding, not this one's
        value = str(env[name])
        if f"${{{name}" not in value and not _VAR_REF.findall(value):
            not_settable.append((name, value))
    assert not not_settable, (
        f"{not_settable} are pinned to literals in {QUICKSTART.name} rather than "
        "interpolated from the environment, so nothing an operator puts in .env "
        "reaches the container."
    )


@pytest.mark.parametrize("name", sorted(DELIBERATELY_NOT_FORWARDED))
def test_each_exemption_is_still_true(name):
    """Negative control on the exemption list. An exemption that outlives its reason
    is a hole with a comment over it."""
    assert name in _notifier_env_reads(), (
        f"{name} is exempted here but the notifier no longer reads it. Delete the "
        "entry -- a stale exemption quietly shrinks the set this guard checks."
    )
    assert name not in _quickstart_api_gateway_env(), (
        f"{name} is exempted here as deliberately not forwarded, but "
        f"{QUICKSTART.name} forwards it. One of the two is wrong; if forwarding it "
        "was intended, remove the exemption so the delivery assertion covers it."
    )


def test_the_alert_link_resolves_on_a_server_install():
    """APP_BASE_URL decides where every "Open pipeline" button points. Its code
    fallback is http://localhost:3000, which is correct on a laptop and a dead link in
    every alert on a server -- and dead in a way the sender cannot detect."""
    env = _quickstart_api_gateway_env()
    value = str(env.get("APP_BASE_URL", ""))
    assert value, (
        f"{QUICKSTART.name} does not pass APP_BASE_URL, so the notifier falls back to "
        "http://localhost:3000 and every alert link is dead on a server install."
    )

    referenced = set(_VAR_REF.findall(value))
    referenced.discard("APP_BASE_URL")
    assert referenced, (
        f"APP_BASE_URL is set to {value!r}, which chains off nothing. On a server the "
        "operator would have to know to set a variable no prompt asks about; it needs "
        "to default to the public URL install.sh already collects."
    )

    install = _install_sh()
    persisted = [v for v in referenced if re.search(rf"^{re.escape(v)}=", install, re.M)]
    assert persisted, (
        f"APP_BASE_URL chains off {sorted(referenced)}, and install.sh writes none of "
        "them into the generated .env. The chain is only worth having if the variable "
        "at the end of it is one the installer actually sets."
    )


def test_install_sh_lets_the_operator_find_the_knob():
    """Alerting nobody knows how to turn on is alerting that stays off. Every
    forwarded variable has to surface in the .env install.sh writes -- directly, or
    through the variable its compose value chains off."""
    install = _install_sh()
    assert "write_env()" in install and len(install) > 20_000, (
        "install.sh does not look like the installer (no write_env, or far too "
        "small). Reading the wrong file would make this assertion vacuous."
    )

    env = _quickstart_api_gateway_env()
    undiscoverable = []
    for name in _forwarded():
        value = str(env.get(name, ""))
        # `${FOO:-${BAR:-default}}` is satisfied by BAR: that is the variable the
        # operator is actually meant to set.
        candidates = set(_VAR_REF.findall(value)) | {name}
        if not any(re.search(rf"^{re.escape(c)}=", install, re.M) for c in sorted(candidates)):
            undiscoverable.append(name)
    assert not undiscoverable, (
        f"{undiscoverable} are forwarded by {QUICKSTART.name} but appear in no "
        "assignment in the .env install.sh generates, so an operator has no way to "
        "learn the knob exists short of reading the compose file."
    )
