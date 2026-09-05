"""A bring-your-own overlay layers onto EVERY base, so every base needs the pairing half.

`test_byo_overlays_are_complete.py` already asserts the two-halves rule -- a
service parked in a never-activated profile is still named by `depends_on`, and
Compose then rejects the WHOLE project with "depends on undefined service", so
nothing starts. That test is correct. Its blind spot is its denominator: it
hard-codes `BASE = docker-compose.quickstart.yml` and checks that one file.

The repo ships two bases. `docker-compose.quickstart.yml` is the one-liner
install; `docker-compose.yml` (+ `docker-compose.prod.yml`) is the
Docker-on-a-VM path. Only the first was ever checked, so the second carried
seven uncovered `depends_on: kafka` and three uncovered `depends_on: postgres`
entries -- and `-f docker-compose.yml -f docker-compose.byo-kafka.yml` rendered
zero services with rc=1 while the quickstart-scoped guard stayed green. A guard
whose scope is one file cannot see the file it is not looking at.

So this file asks the same question with the denominator opened all the way up:
EVERY `docker-compose*.yml`, not one of them. The parked-service set is still
DERIVED from `docker-compose.byo-*.yml`, so adding `docker-compose.byo-redis.yml`
tomorrow tightens this automatically instead of leaving it quietly stale.

WHY EVERY FILE, INCLUDING THE OVERLAYS. `docker-compose.prod.yml` re-declares
`depends_on` under Compose's `!override` tag, which REPLACES the base mapping
rather than merging into it. A `required: false` written in `docker-compose.yml`
is therefore discarded for exactly those services, and a check scoped to base
files alone would report a fix that the shipped stack does not have. The
override is invisible to `yaml.safe_load`, which raises on the unknown tag --
hence the loader below, which resolves `!override`/`!reset` to their underlying
value so the entries inside them are actually read rather than skipped.

The failure mode is nasty because it is invisible until somebody layers the
overlay: the default `up` is completely healthy, so CI, staging and every
developer laptop stay green while the BYO path is dead. It has already cost a
~4-minute production outage once, with each half proven separately fatal.
"""

import glob
import os
import re
import shutil
import subprocess

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Stacks are spelled out because they cannot be derived: `docker-compose.staging.yml`
# amends `kafka` but is NOT valid on top of docker-compose.yml alone (traefik lives in
# prod.yml), so "every file that amends a parked service" would invent broken stacks.
# The completeness of this list is guarded by test_every_base_file_appears_in_a_stack,
# which derives the base set independently.
STACKS = {
    "quickstart": ["docker-compose.quickstart.yml"],
    "base": ["docker-compose.yml"],
    "prod": ["docker-compose.yml", "docker-compose.prod.yml"],
}


class _ComposeLoader(yaml.SafeLoader):
    """SafeLoader that reads through Compose's `!override` / `!reset` tags.

    yaml.safe_load raises ConstructorError on them, and a test that cannot parse
    prod.yml would silently check one fewer file than it claims to.
    """


def _resolve_tag(loader, node):
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node, deep=True)
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node, deep=True)
    return loader.construct_scalar(node)


for _tag in ("!override", "!reset"):
    _ComposeLoader.add_constructor(_tag, _resolve_tag)


def _load(path):
    with open(path) as fh:
        return yaml.load(fh, Loader=_ComposeLoader) or {}


def _services(path):
    return {
        name: body
        for name, body in (_load(path).get("services") or {}).items()
        if isinstance(body, dict)
    }


def compose_files():
    found = sorted(
        os.path.basename(p) for p in glob.glob(os.path.join(REPO_ROOT, "docker-compose*.yml"))
    )
    assert len(found) > 5, f"docker-compose*.yml glob found only {found} -- the glob is wrong"
    return found


def overlay_paths():
    found = sorted(glob.glob(os.path.join(REPO_ROOT, "docker-compose.byo-*.yml")))
    assert found, "no docker-compose.byo-*.yml found -- glob is wrong or files moved"
    return found


def parked_services():
    """{service: overlay basename} for every service a byo-* overlay hides via profiles."""
    parked = {}
    for path in overlay_paths():
        for name, body in _services(path).items():
            if body.get("profiles"):
                parked[name] = os.path.basename(path)
    return parked


def _depends_on_entries():
    """Yield (file, service, dep, options) for every depends_on entry in every file.

    `options` is None for the short list form, which cannot express `required`.
    """
    for fname in compose_files():
        for svc, body in _services(os.path.join(REPO_ROOT, fname)).items():
            deps = body.get("depends_on")
            if isinstance(deps, list):
                for dep in deps:
                    yield fname, svc, dep, None
            elif isinstance(deps, dict):
                for dep, opts in deps.items():
                    yield fname, svc, dep, (opts if isinstance(opts, dict) else {})


def test_overlays_park_at_least_kafka_and_postgres():
    """A count of zero is not an error, so assert the denominator explicitly."""
    parked = parked_services()
    assert {"kafka", "postgres"} <= set(parked), (
        f"expected the kafka and postgres overlays to park their bundled service; got {parked}"
    )


def test_prod_overlay_is_actually_parsed():
    """The loader is load-bearing: safe_load raises on prod.yml's `!override`.

    Without this, a regression that reverted the custom loader would make the
    main check quietly skip the one file that needed it most.
    """
    prod = os.path.join(REPO_ROOT, "docker-compose.prod.yml")
    with open(prod) as fh:
        raw = fh.read()
    assert "!override" in raw, "prod.yml no longer uses !override -- re-check this test's premise"
    with pytest.raises(yaml.YAMLError):
        yaml.safe_load(raw)  # control: proves the plain loader really does fail here
    assert _services(prod), "the custom loader parsed prod.yml to zero services"


def test_every_base_file_appears_in_a_stack():
    """STACKS is hand-written; this derives the base set so it cannot go stale.

    A "base" is a file that DEFINES a parked service with an image/build, i.e.
    concretely provides the thing a byo overlay parks. Files that merely amend it
    (prod, staging, ci-isolate, and the overlays themselves) are not bases.
    """
    parked = set(parked_services())
    bases = set()
    for fname in compose_files():
        for name, body in _services(os.path.join(REPO_ROOT, fname)).items():
            if name in parked and (body.get("image") or body.get("build")):
                bases.add(fname)
    assert bases, "derived zero base files -- the derivation is broken, not the repo"
    covered = {f for files in STACKS.values() for f in files}
    assert bases <= covered, (
        f"these files define a service a byo overlay parks, but no STACKS entry "
        f"covers them, so nothing checks their BYO path: {sorted(bases - covered)}"
    )


def test_no_short_form_depends_on_names_a_parked_service():
    """The list form cannot carry `required: false` at all -- it must be converted.

    Called out separately because the message a bare `required is not False`
    check produces ("required=None") reads as a missing key rather than as a
    depends_on shape that has nowhere to put the key.
    """
    parked = parked_services()
    offenders = [
        f"{fname}::{svc} -> {dep} (parked by {parked[dep]})"
        for fname, svc, dep, opts in _depends_on_entries()
        if opts is None and dep in parked
    ]
    assert not offenders, (
        "these depends_on entries use the short list form, which cannot express "
        "`required: false`, while naming a service a byo overlay parks. Convert "
        "the whole block to the long mapping form:\n  " + "\n  ".join(offenders)
    )


def test_every_dependant_of_a_parked_service_marks_it_not_required():
    """The pairing half, asked of EVERY compose file rather than one base."""
    parked = parked_services()
    checked = 0
    missing = []
    for fname, svc, dep, opts in _depends_on_entries():
        if dep not in parked:
            continue
        checked += 1
        if opts is None or opts.get("required") is not False:
            missing.append(f"{fname}::{svc} -> {dep} (parked by {parked[dep]})")
    assert checked > 10, (
        f"only {checked} depends_on entries named a parked service across "
        f"{len(compose_files())} compose files -- suspiciously few, refusing to "
        "report a pass on a denominator that small"
    )
    assert not missing, (
        "these depends_on entries name a service a byo-* overlay parks in an "
        "inactive profile, but do not carry `required: false`. Layering that "
        "overlay makes Compose reject the whole project and NOTHING starts:\n  "
        + "\n  ".join(missing)
    )


def _required_env(files):
    """Placeholder values for every var a stack guards with `${VAR:?}`.

    One such var left unset aborts interpolation for the WHOLE merged config, so
    this is derived from the files rather than hand-listed.
    """
    env = {}
    for fname in files:
        with open(os.path.join(REPO_ROOT, fname)) as fh:
            for var in re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*):?\?", fh.read()):
                env[var] = "FAKEPLACEHOLDER"
    return env


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not installed")
@pytest.mark.parametrize("stack_name", sorted(STACKS))
@pytest.mark.parametrize("overlay", [os.path.basename(p) for p in overlay_paths()])
def test_stack_still_renders_with_each_overlay(tmp_path, stack_name, overlay):
    """Render layer. The static checks never depend on this: a skip is not a pass.

    Asserts against a positive control (the same stack with no overlay), because
    an empty render and a refused project both produce zero services and only the
    control tells them apart.
    """
    files = STACKS[stack_name]
    env_file = tmp_path / "env"
    env_file.write_text("".join(f"{k}={v}\n" for k, v in _required_env(files).items()))

    def render(*extra):
        cmd = ["docker", "compose", "--env-file", str(env_file)]
        for f in list(files) + list(extra):
            cmd += ["-f", f]
        cmd += ["config", "--services"]
        out = subprocess.run(cmd, capture_output=True, text=True, cwd=REPO_ROOT)
        return out.returncode, set(out.stdout.split()), out.stderr

    rc, control, err = render()
    assert rc == 0, f"CONTROL: {stack_name} does not render on its own:\n{err}"
    assert len(control) > 10, (
        f"CONTROL: {stack_name} rendered only {sorted(control)} -- refusing to trust "
        "a zero-services 'pass' against a denominator this small"
    )

    rc, got, err = render(overlay)
    assert rc == 0, (
        f"{stack_name} + {overlay} fails to render, so NOTHING starts on that "
        f"BYO path (the control above rendered {len(control)} services):\n{err}"
    )
    assert len(got) > 10, f"{stack_name} + {overlay} rendered only {sorted(got)}"

    for svc, ov in parked_services().items():
        if ov == overlay:
            assert svc not in got, f"{overlay} was layered but {svc} is still in the project"
    assert got - control == set(), f"{overlay} unexpectedly ADDED {got - control}"
