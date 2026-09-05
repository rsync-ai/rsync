"""An orchestrator feature flag the compose file does not pass through is dead.

`os.Getenv("SOMETHING_ENABLED")` in the orchestrator reads the *container's*
environment. Docker Compose does NOT forward the host env or the `.env` file
into a container automatically -- only the keys named in that service's
`environment:` block arrive. So a flag the code reads but no compose file
passes through can never be set: the operator writes it in `.env`, nothing
happens, and there is no error to tell them why. That silent disagreement is
the defect, and it has now happened three times in this repo
(RSYNC_SCHEMA_DRIFT_AUTOAPPLY, RSYNC_LLM_DIAGNOSER_ENABLED, and the
RETENTION_* family below).

This guard makes the class self-detecting: every `*_ENABLED` flag the
orchestrator's Go source reads must appear as a key under the `orchestrator`
service's `environment:` in docker-compose.yml, or be exempted BY NAME here.

Scope, and the deliberate exclusions:
  * Only docker-compose.yml is checked. The prod/staging/vps-2c8g/oss/
    ci-isolate files are OVERLAYS merged onto this base file, so they inherit
    its `environment:` block -- a key missing there is not a defect.
  * docker-compose.quickstart.yml is standalone but deliberately ships no
    optional feature flags at all (it carries neither CDC_RECOVERY_ENABLED nor
    either schema-drift flag), so holding it to this rule would be wrong.
  * RETENTION_ARCHIVE_ENABLED is exempt -- see EXEMPT below. It is the SAME
    defect, still open, named here so the gap is visible rather than looking
    like an oversight.
"""

import os
import re

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))

ORCHESTRATOR_SRC = os.path.join(REPO_ROOT, "backend-orchestrator")
BASE_COMPOSE = os.path.join(REPO_ROOT, "docker-compose.yml")

# `os.Getenv("NAME_ENABLED")` — the exact shape every orchestrator feature gate
# uses today, whether or not it is wrapped in strings.EqualFold/TrimSpace.
GETENV_ENABLED = re.compile(r'os\.Getenv\(\s*"([A-Z0-9_]+_ENABLED)"\s*\)')

# The regex found 5 names at the time this guard was written. If it ever finds
# far fewer, the Go source has moved to a shape it no longer matches and this
# file has quietly become decoration -- an empty discovered set would otherwise
# make every assertion below pass vacuously.
MIN_EXPECTED_FLAGS = 4

# Exempted BY NAME, not by silence:
#   RETENTION_ARCHIVE_ENABLED (backend-orchestrator/internal/agents/retention/
#   config.go:170) is unwired in exactly the way this guard exists to prevent,
#   as is the whole RETENTION_* family around it (RETENTION_ENABLED_TABLES,
#   RETENTION_ARCHIVE_BUCKET, ...). Wiring one key of that family without the
#   rest would be worse than leaving it, so it is tracked as its own issue.
#   Delete this entry when that family is wired; do not add to this set to
#   silence a NEW failure.
EXEMPT = {"RETENTION_ARCHIVE_ENABLED"}


def _discover_enabled_flags():
    """Every `*_ENABLED` env var the orchestrator's non-test Go source reads."""
    found = {}
    for dirpath, dirnames, filenames in os.walk(ORCHESTRATOR_SRC):
        dirnames[:] = [d for d in dirnames if d not in (".git", "vendor", "bin")]
        for name in filenames:
            if not name.endswith(".go") or name.endswith("_test.go"):
                continue
            path = os.path.join(dirpath, name)
            with open(path, encoding="utf-8", errors="replace") as fh:
                text = fh.read()
            for flag in GETENV_ENABLED.findall(text):
                found.setdefault(flag, os.path.relpath(path, REPO_ROOT))
    return found


def _orchestrator_environment():
    """Keys under services.orchestrator.environment in the base compose file.

    yaml.safe_load resolves the `<<: *kafka-security` merge key natively, so
    the inherited BYO-broker keys are present here too.
    """
    with open(BASE_COMPOSE, encoding="utf-8") as fh:
        doc = yaml.safe_load(fh) or {}
    services = doc.get("services") or {}
    orch = services.get("orchestrator")
    assert isinstance(orch, dict), (
        "docker-compose.yml has no `orchestrator` service -- the loader broke, "
        "or the service was renamed and this guard now checks nothing"
    )
    env = orch.get("environment")
    assert isinstance(env, dict), (
        "services.orchestrator.environment is not a mapping "
        f"(got {type(env).__name__}); this guard only understands the map form"
    )
    return env


def test_flag_discovery_still_finds_the_known_flags():
    """Self-test, deliberately first: an empty denominator reads as a pass.

    If this trips, the assertions below are not proving anything -- fix the
    regex before trusting a green run of the rest of this file.
    """
    found = _discover_enabled_flags()
    assert len(found) >= MIN_EXPECTED_FLAGS, (
        f"only {len(found)} *_ENABLED flag(s) discovered in {ORCHESTRATOR_SRC} "
        f"({sorted(found)}); expected at least {MIN_EXPECTED_FLAGS}. The Go "
        "source has changed shape and GETENV_ENABLED no longer matches it, so "
        "this guard has become decoration."
    )


def test_every_orchestrator_enabled_flag_is_passed_through_by_compose():
    found = _discover_enabled_flags()
    assert len(found) >= MIN_EXPECTED_FLAGS, "see test_flag_discovery_still_finds_the_known_flags"

    env_keys = set(_orchestrator_environment())
    missing = sorted(
        f"{flag} (read at {src})"
        for flag, src in found.items()
        if flag not in EXEMPT and flag not in env_keys
    )
    assert not missing, (
        "the orchestrator reads these *_ENABLED flags, but docker-compose.yml "
        "does not pass them into the container, so setting them in .env does "
        "nothing and the operator gets no error: "
        + "; ".join(missing)
        + ". Add `      NAME: \"${NAME:-false}\"` to the orchestrator "
        "`environment:` block (use `:-`, never `:?` -- one `:?` aborts "
        "interpolation for the whole merged config), or exempt it by name in "
        "EXEMPT with a comment saying why."
    )


def test_wired_flags_default_to_off():
    """A pass-through must not flip behaviour on by default.

    `${NAME:-true}` would turn every existing stack's flag on at the next
    `docker compose up` -- a live behaviour change disguised as a config
    tidy-up. Every discovered flag that IS wired must default to a non-"true"
    literal.
    """
    found = _discover_enabled_flags()
    env = _orchestrator_environment()
    offenders = []
    for flag in sorted(found):
        if flag not in env:
            continue
        rendered = str(env[flag]).strip().strip('"').strip("'")
        # `${FLAG:-false}` -> the default is what follows `:-`.
        m = re.fullmatch(r"\$\{" + re.escape(flag) + r"(?::-|:\?|-)?([^}]*)\}", rendered)
        default = m.group(1) if m else rendered
        if default.strip().lower() == "true":
            offenders.append(f"{flag}={env[flag]!r}")
    assert not offenders, (
        "these orchestrator feature flags default to ON, which changes the "
        f"behaviour of every stack that does not set them: {offenders}"
    )


@pytest.mark.parametrize("flag", sorted(EXEMPT))
def test_exempted_flags_are_still_actually_unwired(flag):
    """Keeps EXEMPT honest: once a flag IS wired, drop it from the set.

    An exemption that outlives the defect it names hides the next one.
    """
    if flag not in _discover_enabled_flags():
        pytest.skip(f"{flag} is no longer read by the orchestrator; drop it from EXEMPT")
    assert flag not in _orchestrator_environment(), (
        f"{flag} is now passed through by docker-compose.yml -- remove it from "
        "EXEMPT so the guard covers it"
    )
