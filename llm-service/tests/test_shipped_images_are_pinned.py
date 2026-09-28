"""No image a self-hoster pulls may carry a floating tag.

WHY THIS EXISTS. A floating tag is the one kind of change that arrives with no
change on our side: nothing in the repo differs, no test runs, no reviewer sees
a diff, and CI cannot go red -- the new bits simply appear on the next
`docker compose pull` or `--force-recreate`. It had already happened here.
`tecnativa/docker-socket-proxy:latest` moved v0.4.2 -> v0.5.0 on 2026-07-27, and
that container IS the docker-socket security boundary: it is what stands between
Traefik/api-gateway and the raw socket. `scripts/deploy-service.sh` runs
`--force-recreate`, so the swap would have landed on the next deploy of that
service, on prod, unannounced.

BACKLOG.md tracked a narrower version of this as F-CHART-MINIO-FLOATING-TAGS. It
named only `values.yaml` and only MinIO -- it never named the one-command
install (docker-compose.quickstart.yml), the base stack, the prod overlay, or
the socket proxy, and its cited line numbers had drifted. This test is the
generalisation: it checks every shipped compose file and the chart, so the next
floating tag cannot be introduced anywhere in the delivery surface.

WHAT COUNTS AS FLOATING. `:latest` explicitly, any of the other conventional
moving aliases, and -- the sneakiest form -- NO TAG AT ALL, which Docker
resolves to `:latest` while reading in a diff as if it were a considered choice.
A registry that publishes nothing but moving tags can still be pinned, by
digest. Chainguard's registry serves only `latest` anonymously, so the MinIO
image (server and `mc` jobs alike) is written `@sha256:...` on the delivery
surface, while the e2e fixtures in ALLOWLIST float on its `latest`.

A PIN FIXES WHICH BUILD ARRIVES, NOT WHAT IS IN IT. The MinIO image that replaced
the walled one ships `mc`, a shell and coreutils, and no curl, wget, nc,
busybox, grep or find. A healthcheck copied from another service or an older
branch -- `curl -f .../minio/health/live` was the e2e fixture's until the swap
-- then never passes, and everything that waits on `service_healthy` never
starts: minio-init in the e2e stack, minio-lifecycle-init and minio-mcp in the
base one. Nothing about that is visible to a test that only reads tags, so
IMAGE_TOOLS records what each such image ships and every healthcheck and init
script that runs in it is checked against that, in compose and in the chart.

FIRST-PARTY IMAGES ARE OUT OF SCOPE, DELIBERATELY. `ghcr.io/rsync-ai/*` images
are written `:${RSYNC_VERSION:-latest}` by convention, and install.sh derives
RSYNC_VERSION from the ref it fetched the compose file from, so both halves of
an install name the same code (#892). That convention is already enforced --
test_shipped_images_are_publishable.py fails any first-party image NOT written
that way -- and pinning them here would fight it. What IS asserted below is the
complement: no first-party image may be HARD-pinned to a bare `:latest` with the
interpolation dropped. That is the exact shape of the bug #892 fixed -- half the
first-party images honoured the variable and half were hard-pinned `:latest`, so
`RSYNC_VERSION=v0.1.0 docker compose up` silently ran a mix of two refs.

A GUARD IS ONLY WORTH ITS REACH. ci.yml runs this file from the
`llm-service-unit` job, which is gated on the `llm` paths filter. When this
file was first written that filter named exactly two root compose files, so
the guard was dead on the other 18 -- including docker-compose.yml, the base
stack the prod overlay merges onto and where the socket-proxy pin lives. A PR
floating a tag there would have skipped the job, and a skipped check looks
exactly like a passing one. ci.yml's own comments record five earlier
instances of that shape; nothing enforced the rule. The last test below does.
"""

import fnmatch
import glob
import os
import re
import shutil
import subprocess

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))

FIRST_PARTY = "ghcr.io/rsync-ai/"

# The quickstart names its registry as `${RSYNC_IMAGE_REGISTRY:-ghcr.io/rsync-ai}`
# so an install can pull from a mirror. Resolved to the default compose uses when
# nothing is set: a literal `ghcr.io/rsync-ai/` match reads that form as a
# third-party image, and every first-party check here would lose the quickstart.
_REGISTRY_KNOB = re.compile(r"\$\{RSYNC_IMAGE_REGISTRY:-([^}]*)\}")


def _default_registry(image):
    return _REGISTRY_KNOB.sub(r"\1", image)

# A tag that names a moving target rather than a build. `latest` is the one that
# bit us; the rest are the conventional aliases that mean the same thing and
# would sail past a check that only looked for `latest`.
MOVING_TAGS = {"latest", "stable", "edge", "nightly", "main", "master", "dev"}


class _ComposeLoader(yaml.SafeLoader):
    """Compose's merge tags are not standard YAML.

    `docker-compose.oss.yml` uses `!override`. `yaml.safe_load` raises
    ConstructorError on it, so a census that did not handle it would either
    crash or -- far worse, if someone wrapped the parse in a try/except -- skip
    that file silently and report green while checking one fewer file than it
    claimed. `test_every_compose_file_parsed` below pins that it does not.
    """


def _passthrough(loader, node):
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)
    return loader.construct_scalar(node)


for _tag in ("!override", "!reset"):
    _ComposeLoader.add_constructor(_tag, _passthrough)


# Test fixtures, not delivery. These compose files start throwaway databases for
# the e2e suite; they are never fetched by install.sh, never referenced by the
# chart, and their containers are torn down at the end of a run. A moving tag
# there costs a flaky test, not a silent change on someone's server. Keyed by
# (file, service) so the exemption cannot widen to a file's other services.
ALLOWLIST = {
    ("docker-compose.e2e.dbs.yml", "minio"):
        "e2e fixture: throwaway object store, torn down with the test run",
    ("docker-compose.e2e.dbs.yml", "minio-init"):
        "e2e fixture: one-shot bucket creation against the fixture above",
    ("docker-compose.e2e.dbs.extended.yml", "sqlserver-e2e-init"):
        "e2e fixture: mssql-tools publishes no stable version tag we pin elsewhere",
}


# A compose file at any depth, root included. Anchored on a path separator so
# `my-docker-compose.yml` does not match, and `[^/]*` keeps the wildcard inside
# one path segment -- the looseness that let an earlier pattern list look wider
# than it was.
_COMPOSE_RE = re.compile(r"(?:^|/)docker-compose[^/]*\.ya?ml$")


def _compose_files():
    """-> every tracked compose file in the tree, repo-relative, sorted.

    Discovery, not a hand-kept pattern list. The list this replaced named three
    patterns and still missed
    `shared/internal/infra/kafka-connect/docker-compose.kafka-connect.yml` --
    the same cover-half-the-tree defect the widening before it was written to
    fix, one directory further out. That is the shape of the bug: a pattern list
    has to be edited by whoever puts a compose file somewhere new, nothing fails
    when they don't, and the census just gets quietly smaller while still
    reporting PASS.

    `git ls-files` is the oracle because the predicate is "what this repo
    ships". An untracked compose file in someone's working tree is not shipped;
    a tracked one is, wherever it sits.

    Discovery also removes the last repo-specific line from this file. A pattern
    list had to name a path that exists in only one of the two repositories, so
    the other repo's copy carried either a dead pattern or a divergence. This
    version is byte-identical in both and correct in both.
    """
    listing = subprocess.run(
        ["git", "-C", REPO_ROOT, "ls-files", "-z"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    names = sorted(p for p in listing.split("\0") if _COMPOSE_RE.search(p))
    # Loud here rather than vacuous later: this feeds a parametrisation, and an
    # empty one collects zero cases and reports green.
    assert names, "git ls-files returned no compose files; the enumeration failed"
    return names


def _load_compose(name):
    with open(os.path.join(REPO_ROOT, name)) as fh:
        return yaml.load(fh, Loader=_ComposeLoader) or {}


def _compose_image_refs():
    """-> [(file, service, image)] for every service that names an image."""
    out = []
    for name in _compose_files():
        doc = _load_compose(name)
        for svc, spec in (doc.get("services") or {}).items():
            if isinstance(spec, dict) and isinstance(spec.get("image"), str):
                out.append((name, svc, spec["image"].strip()))
    return out


def _chart_image_refs():
    """-> [(file, dotted.key, image)] for the chart's flat image strings.

    Only the flat `image:`/`mcImage:` strings -- the third-party ones. First-party
    images use a `repository:` + `tag:` block whose tag defaults to
    `.Chart.AppVersion`, and test_shipped_images_are_publishable.py owns that.
    """
    out = []
    for path in sorted(glob.glob(os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai", "values*.yaml"))):
        with open(path) as fh:
            doc = yaml.safe_load(fh) or {}
        rel = os.path.relpath(path, REPO_ROOT)

        def walk(node, prefix):
            if isinstance(node, dict):
                for key, val in node.items():
                    dotted = f"{prefix}.{key}".lstrip(".")
                    if isinstance(val, str) and "image" in key.lower() and val.strip():
                        out.append((rel, dotted, val.strip()))
                    walk(val, dotted)
            elif isinstance(node, list):
                for item in node:
                    walk(item, prefix)

        walk(doc, "")
    return out


def _tag_of(image):
    """-> the tag, or None when the ref carries none (Docker reads that as `latest`).

    Splits on the last `/` first so a registry host's port (`host:5000/img`) is
    never mistaken for a tag. A digest pin (`img@sha256:...`) is not a tag and is
    treated as pinned, which it is -- more tightly than any tag.
    """
    if "@" in image:
        return None if ":" in image.split("@")[0].rsplit("/", 1)[-1] else "@digest"
    last = image.rsplit("/", 1)[-1]
    return last.split(":", 1)[1] if ":" in last else None


def _is_floating(image):
    if "@sha256:" in image:
        return False
    tag = _tag_of(image)
    if tag is None:
        return True  # untagged == :latest, just harder to see
    if "${" in tag:
        return False  # resolved from the install ref by design; see the docstring
    return tag.lower() in MOVING_TAGS


def _shipped_third_party():
    """Third-party image refs on the delivery surface, minus the e2e fixtures."""
    return [
        (f, svc, img)
        for f, svc, img in _compose_image_refs()
        if FIRST_PARTY not in _default_registry(img) and (f, svc) not in ALLOWLIST
    ]


# --------------------------------------------------------------------------
# Vacuity guards. Every assertion below is parametrised over a discovered set,
# so a rename, a moved directory or a parser that quietly returns nothing would
# reduce this file to zero assertions and still report green. Pin each
# denominator so that failure is loud instead.
# --------------------------------------------------------------------------

def test_every_compose_file_parsed():
    files = _compose_files()
    assert len(files) >= 15, f"expected >=15 tracked compose files, found {files}"
    for name in files:
        doc = _load_compose(name)
        assert isinstance(doc, dict), f"{name} did not parse to a mapping"
    # The `!override` file specifically -- the trap this loader exists for.
    assert "docker-compose.oss.yml" in files
    assert (_load_compose("docker-compose.oss.yml").get("services")), \
        "docker-compose.oss.yml parsed to no services; the !override constructor regressed"


def test_the_image_census_is_not_empty():
    refs = _compose_image_refs()
    assert len(refs) >= 60, f"only {len(refs)} compose image refs found; the census under-read its subject"
    shipped = _shipped_third_party()
    assert len(shipped) >= 25, f"only {len(shipped)} shipped third-party refs; assertions below are near-vacuous"
    # The quickstart alone names 15. A prefix match that stopped recognising them
    # would not shrink the census -- it would move them into the third-party set
    # above, where `${RSYNC_VERSION:-latest}` passes as "resolved by design".
    first = [i for _, _, i in refs if FIRST_PARTY in _default_registry(i)]
    assert len(first) >= 15, f"only {len(first)} first-party refs; the quickstart's were misread"
    chart = _chart_image_refs()
    assert len(chart) >= 6, f"only {len(chart)} chart image strings found: {chart}"


# --------------------------------------------------------------------------
# The guard itself.
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "compose_file,service,image",
    [pytest.param(f, s, i, id=f"{f}::{s}") for f, s, i in _shipped_third_party()],
)
def test_no_shipped_third_party_image_floats(compose_file, service, image):
    assert not _is_floating(image), (
        f"{compose_file} service '{service}' pulls `{image}`, whose tag names a moving "
        f"target. Upstream can change what this starts with no diff here, so no test "
        f"or review can catch it. Pin it to a released version, or -- if it is a test "
        f"fixture rather than something a self-hoster pulls -- add it to ALLOWLIST "
        f"with the reason."
    )


@pytest.mark.parametrize(
    "values_file,key,image",
    [pytest.param(f, k, i, id=f"{os.path.basename(f)}::{k}") for f, k, i in _chart_image_refs()],
)
def test_no_chart_image_floats(values_file, key, image):
    assert not _is_floating(image), (
        f"{values_file} key `{key}` is `{image}`. This file's own rule at the `tag:` "
        f"key says it: an unpinned tag silently changes what a re-pulled pod runs."
    )


@pytest.mark.parametrize(
    "compose_file,service,image",
    [
        pytest.param(f, s, i, id=f"{f}::{s}")
        for f, s, i in _compose_image_refs()
        if FIRST_PARTY in _default_registry(i)
    ],
)
def test_no_first_party_image_is_pinned_to_latest(compose_file, service, image):
    """First-party tags come from `${RSYNC_VERSION:-latest}`, never a bare `latest`.

    A hard-pinned `:latest` ignores RSYNC_VERSION, so `RSYNC_VERSION=v0.1.0
    docker compose up` runs that service at the newest release while its
    neighbours run 0.1.0 -- one stack built from two refs, invisible in a diff.
    """
    assert _tag_of(image) != "latest", (
        f"{compose_file} service '{service}' hard-pins `{image}`. Write it as "
        f"`:${{RSYNC_VERSION:-latest}}` so one variable moves every first-party image "
        f"together; a bare `latest` ignores RSYNC_VERSION and splits the stack across refs."
    )


def test_the_allowlist_has_no_stale_entries():
    """A stale exemption is an exemption for nothing, and it hides the next one.

    If an allowlisted service is renamed or deleted, the entry silently stops
    matching -- and someone reading the list still believes that file is covered.
    """
    live = {(f, s) for f, s, _ in _compose_image_refs()}
    stale = sorted(k for k in ALLOWLIST if k not in live)
    assert not stale, f"ALLOWLIST names services that no longer exist: {stale}"


def test_every_allowlisted_entry_actually_needed_the_exemption():
    """An entry that is already pinned should be deleted, not left standing.

    Otherwise the allowlist grows into a list of things nobody has re-checked,
    and its length stops meaning anything.
    """
    by_key = {(f, s): i for f, s, i in _compose_image_refs()}
    unnecessary = sorted(k for k in ALLOWLIST if not _is_floating(by_key.get(k, "")))
    assert not unnecessary, (
        f"these are pinned already and no longer need an exemption: {unnecessary}"
    )


# --------------------------------------------------------------------------
# What the image can run. See "A PIN FIXES WHICH BUILD ARRIVES" above.
# --------------------------------------------------------------------------

# Image repository (tag and digest stripped) -> what it ships. Read off the image,
# not assumed -- 2026-09-26, the digest the delivery surface pins:
#   docker run --rm --entrypoint /bin/sh cgr.dev/chainguard/minio@sha256:bd0143... \
#     -c 'for t in mc curl wget nc busybox grep find; do command -v $t || echo MISSING $t; done'
# `probes` is what a healthcheck may run. `lacks` is what nothing may run: the
# tools a healthcheck or a wait loop reaches for by habit that this image does not
# have. Extend either set only from a listing like the one above.
IMAGE_TOOLS = {
    "cgr.dev/chainguard/minio": {
        "probes": frozenset({"mc"}),
        "lacks": frozenset({"curl", "wget", "nc", "busybox", "grep", "find"}),
    },
}

CHART_DIR = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai")

# Allowed in a CMD-SHELL healthcheck on top of `probes`: the shell supplies them.
_SHELL_BUILTINS = frozenset({"exit", "test", "[", "true", "false", "echo", ":"})

# Words that open a clause rather than name a command, and commands that run the
# next word as the real one -- `timeout 3 curl ...` runs curl. Every wrapper here
# is in the image above (coreutils, or a bash builtin/keyword).
_RESERVED = frozenset({"if", "then", "else", "elif", "fi", "do", "done", "while", "until", "!", "{", "}"})
_WRAPPERS = frozenset({"exec", "command", "time", "timeout", "nohup", "nice", "env", "stdbuf"})
_ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")
_WRAPPER_ARG = re.compile(r"^(?:-.*|\d+(?:\.\d+)?[smhd]?)$")

# `["CMD", "/bin/sh", "-c", "<script>"]` is a shell healthcheck spelled as exec,
# and so is a k8s exec probe written that way; judge the script, not the shell.
_SHELLS = frozenset({"sh", "bash"})
_SHELL_C = re.compile(r"^-[a-z]*c[a-z]*$")

_COMMENT = re.compile(r"(?m)(?:^|(?<=\s))#.*$")
_QUOTED = re.compile(r"'[^']*'|\"(?:\\.|[^\"\\])*\"")
_ARITHMETIC = re.compile(r"\$\(\([^)]*\)\)")
_REDIRECT = re.compile(r"\d*(?:&?>{1,2}|<{1,3})&?[\d-]*")
_SEPARATOR = re.compile(r"&&|\|\||\$\(|[;&|()`\n]")


def _repo_of(image):
    """`host/repo:tag` or `host/repo@sha256:...` -> `host/repo`."""
    head, _, last = image.split("@", 1)[0].rpartition("/")
    return f"{head}/{last.split(':', 1)[0]}" if head else last.split(":", 1)[0]


def _unwrap(words):
    """-> the command a list of words runs, past keywords, `VAR=x` and wrappers."""
    words = list(words)
    while words:
        if words[0] in _RESERVED or _ASSIGNMENT.match(words[0]):
            words.pop(0)
        elif os.path.basename(words[0]) in _WRAPPERS:
            words.pop(0)
            while words and (_WRAPPER_ARG.match(words[0]) or _ASSIGNMENT.match(words[0])):
                words.pop(0)
        else:
            return os.path.basename(words[0])
    return None


def _commands(script):
    """-> every command a shell script runs, in order.

    A lexer, not a shell: it finds the word in command position of each clause,
    which is all the checks below ask. Comments go first (an apostrophe in one
    would otherwise open a quote), then quoted text -- `echo "no curl here"` runs
    echo -- so `mc find` is mc and `# curl ...` is nothing. Compose's `$$` is
    the shell's `$`, which makes `$$(curl ...)` a substitution that runs curl.
    """
    s = script.replace("$$", "$").replace("\\\n", " ")
    s = _COMMENT.sub("", s)
    s = _QUOTED.sub("''", s)
    s = _ARITHMETIC.sub("0", s)
    s = _REDIRECT.sub(" ", s)
    return [c for c in (_unwrap(part.split()) for part in _SEPARATOR.split(s)) if c]


def _healthcheck_misses(test, tools):
    """-> what this healthcheck runs that the image cannot; [] when it can run.

    Judged against `probes`, an allowlist: a healthcheck is one command, and one
    that is not the image's own probe is a copy from somewhere else.
    """
    if isinstance(test, str):
        test = ["CMD-SHELL", test]
    if not test or test[0] == "NONE":
        return []
    argv = [str(a) for a in test[1:]]
    if test[0] == "CMD" and len(argv) > 2 and os.path.basename(argv[0]) in _SHELLS and _SHELL_C.match(argv[1]):
        test = ["CMD-SHELL", argv[2]]
    if test[0] == "CMD":
        ran, allowed = [_unwrap(test[1:])], tools["probes"]
    else:
        ran, allowed = _commands(" ".join(test[1:])), tools["probes"] | _SHELL_BUILTINS
    ran = [c for c in ran if c]
    if not ran:
        return ["<no command found>"]
    return [c for c in ran if c not in allowed]


def _lacking(values, tools):
    """-> the tools the image lacks that these entrypoint/command values run.

    Each value may be a string or a list of them, as compose writes both forms.
    """
    strings = [x for v in values for x in (v if isinstance(v, list) else [v]) if isinstance(x, str)]
    return sorted({c for s in strings for c in _commands(s) if c in tools["lacks"]})


def _services_running_a_listed_image():
    """-> [(file, service, repo, spec)] for every compose service an IMAGE_TOOLS image runs.

    A service that names no `image:` and no `build:` is an overlay fragment: it
    runs whatever image the file it merges onto gives that name, so it is judged
    against every image any compose file gives it. docker-compose.prod.yml and
    docker-compose.ci-isolate.yml both carry a `minio` like that; a healthcheck
    added there runs in the MinIO image all the same, and a per-file check that
    asked only for `image:` would never look at it.
    """
    docs = {name: _load_compose(name) for name in _compose_files()}
    named = {}
    for doc in docs.values():
        for svc, spec in (doc.get("services") or {}).items():
            if isinstance(spec, dict) and isinstance(spec.get("image"), str):
                named.setdefault(svc, set()).add(_repo_of(spec["image"].strip()))
    out = []
    for name, doc in docs.items():
        for svc, spec in (doc.get("services") or {}).items():
            if not isinstance(spec, dict) or ("build" in spec and "image" not in spec):
                continue
            if isinstance(spec.get("image"), str):
                repos = {_repo_of(spec["image"].strip())}
            else:
                repos = named.get(svc, set())
            out.extend((name, svc, repo, spec) for repo in sorted(repos & IMAGE_TOOLS.keys()))
    return out


def _chart_templates_naming_a_listed_image():
    """-> the chart templates that render an IMAGE_TOOLS image, repo-relative."""
    keys = {k for _, k, img in _chart_image_refs() if _repo_of(img) in IMAGE_TOOLS}
    out = set()
    for path in glob.glob(os.path.join(CHART_DIR, "templates", "**", "*"), recursive=True):
        if os.path.isfile(path):
            with open(path) as fh:
                if any(f".Values.{k}" in fh.read() for k in keys):
                    out.add(os.path.relpath(path, REPO_ROOT))
    return out


# The chart marks these required. A render that errors would leave the chart case
# below with nothing to judge, so it asserts the render instead of skipping.
_CHART_REQUIRED = [
    "secrets.jwtSecret=FAKEPLACEHOLDER",
    "secrets.encryptionKey=FAKEPLACEHOLDERFAKEPLACEHOLDER32",
    "secrets.postgresPassword=FAKEPLACEHOLDER",
    "secrets.minioAccessKey=FAKEPLACEHOLDER",
    "secrets.minioSecretKey=FAKEPLACEHOLDER",
    "secrets.redisPassword=FAKEPLACEHOLDER",
    "frontend.apiUrl=https://rsync.example.com",
    "frontend.publicUrl=https://rsync.example.com",
]


def _pod_spec(doc):
    if not isinstance(doc, dict):
        return {}
    spec = doc.get("spec") or {}
    if doc.get("kind") == "CronJob":
        spec = (spec.get("jobTemplate") or {}).get("spec") or {}
    if doc.get("kind") == "Pod":
        return spec
    return (spec.get("template") or {}).get("spec") or {}


@pytest.mark.parametrize(
    "script,command,runs",
    [
        # Controls that must be SEEN. Without them a lexer returning [] passes
        # every case below while checking nothing.
        ("until curl -sf http://minio:9000/minio/health/live; do sleep 2; done", "curl", True),
        ("timeout 3 wget -qO- http://minio:9000 >/dev/null 2>&1 || exit 1", "wget", True),
        ("if ! nc -z minio 9000; then exit 1; fi", "nc", True),
        ("ok=$$(busybox wget -q -O- http://minio:9000)", "busybox", True),
        ("/usr/bin/curl -f http://localhost:9000/minio/health/live", "curl", True),
        ("set -e\nmc alias set local http://minio:9000 a b \\\n  || curl -f http://x", "curl", True),
        # ... and ones that must NOT be.
        ('echo "curl is not in this image"', "curl", False),
        ("mc find local/b --name '*.parquet' | head -1", "find", False),
        ("# curl http://minio:9000 -- don't\nmc ready local", "curl", False),
    ],
)
def test_the_command_lexer_sees_what_a_script_runs(script, command, runs):
    assert (command in _commands(script)) is runs, (
        f"_commands({script!r}) = {_commands(script)}; expected `{command}` "
        f"{'among' if runs else 'absent from'} them"
    )


@pytest.mark.parametrize(
    "test,misses",
    [
        (["CMD", "mc", "ready", "local"], []),
        (["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"], ["curl"]),
        (["CMD", "/bin/sh", "-c", "mc ready local || exit 1"], []),
        (["CMD", "/bin/sh", "-lc", "wget -qO- http://localhost:9000/minio/health/live"], ["wget"]),
        (["CMD-SHELL", "curl -f http://localhost:9000/minio/health/live || exit 1"], ["curl"]),
        ("mc ready local", []),
        (["NONE"], []),
    ],
)
def test_a_healthcheck_is_judged_by_what_it_runs(test, misses):
    tools = IMAGE_TOOLS["cgr.dev/chainguard/minio"]
    assert _healthcheck_misses(test, tools) == misses


def test_the_tool_check_has_subjects():
    """The denominators for the two cases below."""
    svcs = _services_running_a_listed_image()
    assert len(svcs) >= 5, f"only {len(svcs)} compose services run an IMAGE_TOOLS image: {svcs}"
    probed = [s for s in svcs if "test" in (s[3].get("healthcheck") or {})]
    assert len(probed) >= 3, f"only {len(probed)} of them carry a healthcheck to judge: {probed}"
    assert _chart_templates_naming_a_listed_image(), "no chart template renders an IMAGE_TOOLS image"


@pytest.mark.parametrize(
    "compose_file,service,repo,spec",
    [pytest.param(f, s, r, sp, id=f"{f}::{s}") for f, s, r, sp in _services_running_a_listed_image()],
)
def test_no_compose_service_runs_a_tool_its_image_lacks(compose_file, service, repo, spec):
    tools = IMAGE_TOOLS[repo]
    problems = []
    hc = spec.get("healthcheck")
    if isinstance(hc, dict) and not hc.get("disable") and "test" in hc:
        bad = _healthcheck_misses(hc["test"], tools)
        if bad:
            problems.append(
                f"healthcheck {hc['test']!r} runs {bad}; in this image a healthcheck "
                f"may run only {sorted(tools['probes'])} (e.g. [\"CMD\", \"mc\", \"ready\", \"local\"])"
            )
    lacking = _lacking([spec.get("entrypoint"), spec.get("command")], tools)
    if lacking:
        problems.append(f"entrypoint/command runs {lacking}, which the image does not ship")
    assert not problems, (
        f"{compose_file} service '{service}' runs in `{repo}`, which has "
        f"no {', '.join(sorted(tools['lacks']))}:\n  " + "\n  ".join(problems) + "\n"
        f"A healthcheck that cannot run never passes, so every service waiting on "
        f"`service_healthy` never starts; a wait loop on a missing tool spins forever."
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_no_chart_container_runs_a_tool_its_image_lacks():
    cmd = ["helm", "template", "r", CHART_DIR]
    for v in _CHART_REQUIRED:
        cmd += ["--set", v]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert proc.returncode == 0, f"helm template failed:\n{proc.stderr[-3000:]}"
    checked, problems = 0, []
    for doc in yaml.safe_load_all(proc.stdout):
        pod = _pod_spec(doc)
        for c in (pod.get("initContainers") or []) + (pod.get("containers") or []):
            repo = _repo_of(c.get("image") or "")
            if repo not in IMAGE_TOOLS:
                continue
            checked += 1
            tools = IMAGE_TOOLS[repo]
            where = f"{doc['kind']}/{doc['metadata']['name']} container '{c['name']}'"
            for probe in ("livenessProbe", "readinessProbe", "startupProbe"):
                argv = ((c.get(probe) or {}).get("exec") or {}).get("command")
                if argv and _healthcheck_misses(["CMD", *argv], tools):
                    problems.append(f"{where} {probe} runs {argv!r}; allowed: {sorted(tools['probes'])}")
            lacking = _lacking([c.get("command"), c.get("args")], tools)
            if lacking:
                problems.append(f"{where} command/args run {lacking}, which `{repo}` does not ship")
    # Two today: the server StatefulSet and the bucket hook Job.
    assert checked >= 2, f"only {checked} rendered containers run an IMAGE_TOOLS image"
    assert not problems, "\n".join(problems)


# --------------------------------------------------------------------------
# Reach. Everything above is worthless on a PR that does not run it.
# --------------------------------------------------------------------------

CI_WORKFLOW = os.path.join(REPO_ROOT, ".github", "workflows", "ci.yml")


# The filters left ci.yml with the `changes` job -- see "Why there is no
# `changes` job" in ci.yml. This file is now the one definition.
CI_FILTERS = os.path.join(REPO_ROOT, ".github", "paths-filters.yml")


def _llm_filter_patterns():
    """-> the `llm` paths-filter patterns that decide whether this file runs.

    `llm-service-unit` runs dorny/paths-filter against CI_FILTERS itself and
    gates every step on `steps.filter.outputs.llm`; the `llm` key of that file
    is therefore the list that decides whether this file executes at all.
    """
    return yaml.safe_load(open(CI_FILTERS))["llm"]


def test_the_llm_job_is_still_the_one_gated_on_that_filter():
    """The premise of the next test: change the gate and it stops meaning anything."""
    doc = yaml.safe_load(open(CI_WORKFLOW))
    job = doc["jobs"]["llm-service-unit"]
    # Two halves, and both are needed. The job must still consult the `llm`
    # filter, AND it must consult it from CI_FILTERS -- a job pointing
    # dorny/paths-filter at some other `filters:` would make the patterns read
    # above describe a list CI does not use.
    assert any(
        (st.get("with") or {}).get("filters") == ".github/paths-filters.yml"
        for st in job["steps"]
    ), (
        "llm-service-unit no longer runs dorny/paths-filter against "
        ".github/paths-filters.yml, so the patterns read from that file decide "
        "nothing here."
    )
    assert "steps.filter.outputs.llm == 'true'" in yaml.safe_dump(job), (
        "llm-service-unit is no longer gated on the `llm` paths filter, so the "
        "reach test below is asserting against a filter that decides nothing. "
        "Point it at whatever gates the job now."
    )
    pats = _llm_filter_patterns()
    assert len(pats) >= 8, f"only {len(pats)} llm filter patterns read from ci.yml: {pats}"


@pytest.mark.parametrize(
    "subject",
    [pytest.param(f, id=f) for f in sorted(
        set(_compose_files())
        | {f for f, _, _ in _chart_image_refs()}
        | _chart_templates_naming_a_listed_image()
    )],
)
def test_the_ci_filter_covers_every_file_this_guard_reads(subject):
    """Every file the census reads must be able to trigger the job that reads it.

    Matching is fnmatch, not the picomatch dorny/paths-filter actually uses.
    fnmatch is the looser of the two -- its `*` crosses `/` where picomatch's
    does not -- so this can only ever be more permissive than CI, never
    stricter. It cannot fail a pattern CI would honour.
    """
    pats = _llm_filter_patterns()
    assert any(fnmatch.fnmatch(subject, p) for p in pats), (
        f"`{subject}` is read by this guard, but no `llm` paths-filter pattern in "
        f".github/paths-filters.yml matches it. A PR touching only that file would skip "
        f"llm-service-unit, so the guard would not run on exactly the change it "
        f"exists to catch -- and a skipped check reads as a passing one.\n"
        f"Add a pattern covering it to the `llm:` filter in {os.path.relpath(CI_FILTERS, REPO_ROOT)}.\n"
        f"Patterns today: {pats}"
    )
