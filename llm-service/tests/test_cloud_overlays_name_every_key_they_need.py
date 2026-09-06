"""A provider overlay has to name every key it leaves for the reader to supply.

The defect: `values-gke.yaml`, `values-eks.yaml` and `values-aks.yaml` each open
by telling the reader to run

    helm install rsync ./rsync-ai -f rsync-ai/values-gke.yaml -f my-values.yaml

and then said nothing at all about what goes in that second file. Six keys live
there and in no file the chart ships, and they fail in two different ways:

  * `secrets.jwtSecret`, `secrets.encryptionKey`, `frontend.apiUrl` and
    `frontend.publicUrl` abort the render -- ONE AT A TIME, because each guard
    is its own `required`/`fail`, so an operator missing four of them pays four
    consecutive failed installs to discover them;
  * `secrets.postgresPassword` and `secrets.redisPassword` do NOT abort. With
    `postgresql.enabled=false` (which is what every cloud overlay sets) the
    Secret renders `POSTGRES_PASSWORD: ""`, the install reports success, and the
    failure arrives later as an auth error from Cloud SQL or Memorystore that
    names neither key.

The second class is why this file exists and why it is not folded into
test_documented_install_commands_render.py. That test's render layer synthesizes
a stand-in values file which itself hand-writes `frontend.apiUrl`,
`frontend.publicUrl` and every external endpoint -- exactly the keys the overlays
were failing to document. It therefore rendered green over this defect for the
whole life of the overlays, and could not have done otherwise: it supplies the
missing keys before running the command. A guard cannot catch a gap it fills.

The contract asserted here is derived from the overlay file itself, never
hand-maintained -- the same principle as the required-key derivation in
test_documented_install_commands_render.py, for the same reason: a second
hand-written copy of the key list would rot exactly the way the overlay headers
did.

    Every key a reader must supply is EITHER present-and-blank in the overlay's
    own body, OR named in the `my-values.yaml` skeleton in its header comment.

Fill precisely those two sets, render, and two things must hold: the render
succeeds (catches the abort class), and no credential in the rendered Secret
came out empty (catches the silent class). Adding a new required key to the
chart without adding it to an overlay fails one or the other.
"""

import base64
import glob
import os
import re
import shutil
import subprocess

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
CHART_DIR = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai")

# `secrets.existingSecret` is blank in values-byo-everything.yaml, but it is an
# ALTERNATIVE to the four secret keys rather than a fifth one: setting it makes
# templates/secret.yaml render nothing at all (the whole file sits under
# `{{- if not .Values.secrets.existingSecret }}`), which would delete the very
# Secret the emptiness assertion below reads. Filling it would make this test
# weaker, not stronger, so it is the one documented carve-out.
NOT_A_READER_KEY = {"secrets.existingSecret"}

# The two that do not abort the render. With postgresql.enabled=false -- which
# every cloud overlay sets -- templates/secret.yaml takes its unguarded branch
# and writes POSTGRES_PASSWORD: "" instead of failing, and REDIS_PASSWORD is
# `| default ""` on every path. Neither can be made a hard `fail`: Cloud SQL and
# RDS IAM database authentication take no password, and Memorystore/ElastiCache
# can run with AUTH disabled. Documentation is the only correct fix, which is
# why it is asserted rather than validated.
SILENT_WHEN_UNSET = ["secrets.postgresPassword", "secrets.redisPassword"]

# Values that satisfy the chart's own format guards -- a bootstrap server has to
# carry a port, an ingress host has to look like a hostname. All fake.
_SHAPES = [
    (re.compile(r"bootstrapServers$"), "b-1.example.com:9092"),
    (re.compile(r"(^|\.)apiUrl$"), "https://api.example.com"),
    (re.compile(r"(^|\.)publicUrl$"), "https://app.example.com"),
    (re.compile(r"[Uu]rl$"), "https://s3.example.com"),
    (re.compile(r"hosts\.api$"), "api.example.com"),
    (re.compile(r"hosts\.app$"), "app.example.com"),
    (re.compile(r"[Hh]ost$"), "db.example.com"),
    (re.compile(r"address$"), "temporal.example.com:7233"),
    (re.compile(r"region$"), "us-east-1"),
    (re.compile(r"className$"), "nginx"),
    (re.compile(r"bucket$"), "rsync-example"),
    (re.compile(r"accessKeyId$"), "AKIAEXAMPLE"),
]


def _placeholder(path):
    for pattern, value in _SHAPES:
        if pattern.search(path):
            return value
    return "FAKEPLACEHOLDER"


def _overlays():
    """Every provider overlay the chart ships, discovered, not listed."""
    return sorted(
        p for p in glob.glob(os.path.join(CHART_DIR, "values-*.yaml"))
        # values.yaml itself is the base; -oss is a runtime split, not a
        # provider, and has its own guard in
        # test_oss_overlay_documents_its_invocation.py.
        if not os.path.basename(p).startswith("values-oss")
    )


def _blank_leaves(path):
    """Keys the overlay body leaves visibly empty -- `host: ""` and friends.

    A blank string in a shipped values file is the chart telling the reader
    "this one is yours". That is documentation the reader cannot miss, because
    it is on the line they are already looking at.
    """
    out = []

    def walk(node, trail):
        if isinstance(node, dict):
            for key, value in node.items():
                walk(value, trail + [key])
        elif node == "":
            out.append(".".join(trail))

    with open(path) as fh:
        walk(yaml.safe_load(fh) or {}, [])
    return [k for k in out if k not in NOT_A_READER_KEY]


# `#   secrets:` opens a section, `#     jwtSecret: ""` is a leaf inside it.
_SECTION = re.compile(r'^#   ([A-Za-z][A-Za-z0-9]*):\s*$')
_LEAF = re.compile(r'^#     ([A-Za-z][A-Za-z0-9]*):\s*""')


def _header_skeleton_keys(path):
    """Keys the header's `my-values.yaml` skeleton tells the reader to write."""
    keys, section = [], None
    with open(path) as fh:
        for line in fh:
            if not line.startswith("#"):
                break  # header comment is over; the values start here
            match = _SECTION.match(line.rstrip("\n"))
            if match:
                section = match.group(1)
                continue
            match = _LEAF.match(line.rstrip("\n"))
            if match and section:
                keys.append(f"{section}.{match.group(1)}")
    return keys


def _nest(pairs):
    root = {}
    for path, value in pairs:
        node = root
        parts = path.split(".")
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = value
    return root


def test_every_overlay_is_discovered_and_documents_something():
    """Assert the denominator before asserting anything about it.

    Every check below iterates over the glob and over two derived key lists. If
    the glob stopped matching, or the header format changed so the skeleton
    parser found nothing, the render check would run on an empty work list and
    pass while checking nothing -- the failure mode this repo keeps hitting.
    """
    overlays = _overlays()
    assert len(overlays) >= 4, (
        f"expected the chart to ship several provider overlays, found {overlays}"
    )

    for path in overlays:
        name = os.path.basename(path)
        documented = set(_blank_leaves(path)) | set(_header_skeleton_keys(path))
        assert len(documented) >= 8, (
            f"{name}: only {len(documented)} reader-supplied key(s) derived "
            f"({sorted(documented)}). Either the overlay stopped marking them "
            "blank and stopped listing them in its header, or the parsers here "
            "broke -- both make the render check below vacuous."
        )

    # The three cloud overlays are the ones that had the defect: they set
    # postgresql.enabled=false, which is the branch where an unset
    # secrets.postgresPassword renders empty instead of aborting.
    cloud = [p for p in overlays if re.search(r"values-(gke|eks|aks)\.yaml$", p)]
    assert len(cloud) == 3, f"expected gke/eks/aks overlays, found {cloud}"
    # The only two key names written by hand in this file, because the property
    # that makes them special -- rendering empty instead of aborting -- lives in
    # a Go template branch (`{{- if .Values.postgresql.enabled }}` in
    # templates/secret.yaml) that is not worth parsing. So assert they still
    # exist there: a rename then fails HERE rather than rotting into a pair of
    # names that match nothing and quietly assert nothing.
    with open(os.path.join(CHART_DIR, "templates", "secret.yaml")) as fh:
        secret_tpl = fh.read()
    for value_path in SILENT_WHEN_UNSET:
        attr = ".Values." + value_path
        assert attr in secret_tpl, (
            f"{attr} is no longer read by templates/secret.yaml, so the "
            "assertion below is checking a key that does not exist. Re-derive "
            "which keys render as an empty credential and update this list."
        )

    for path in cloud:
        keys = _header_skeleton_keys(path)
        gap = [k for k in SILENT_WHEN_UNSET if k not in keys]
        assert gap == [], (
            f"{os.path.basename(path)}: header does not name {gap}. These do "
            "not abort the render -- they come out as an EMPTY credential and "
            "the install reports success -- so the header is the only thing "
            f"that can tell the reader about them. Found: {keys}"
        )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
@pytest.mark.parametrize(
    "overlay", _overlays(), ids=lambda p: os.path.basename(p).replace(".yaml", "")
)
def test_overlay_renders_from_the_keys_it_names_and_leaves_no_empty_credential(
    overlay, tmp_path
):
    """Fill exactly what the overlay says is yours. It must render, and render full.

    Nothing is added beyond the two derived sets, which is the whole point: any
    key the chart needs that the overlay neither blanks nor names is missing
    here, and shows up as an aborted render or an empty Secret value.
    """
    documented = sorted(set(_blank_leaves(overlay)) | set(_header_skeleton_keys(overlay)))
    reader_values = tmp_path / "my-values.yaml"
    reader_values.write_text(
        yaml.safe_dump(_nest([(k, _placeholder(k)) for k in documented]))
    )

    proc = subprocess.run(
        [
            "helm", "template", "rsync", CHART_DIR,
            "-f", overlay,
            "-f", str(reader_values),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, (
        f"{os.path.basename(overlay)} does not render from the keys it names.\n"
        f"Supplied: {documented}\n"
        f"helm said: {proc.stderr.strip()}\n\n"
        "The overlay needs a key it neither leaves blank in its body nor lists "
        "in its header skeleton, so a reader following it hits this too."
    )

    empty = []
    for doc in yaml.safe_load_all(proc.stdout):
        if not doc or doc.get("kind") != "Secret":
            continue
        for key, value in (doc.get("stringData") or {}).items():
            if value == "":
                empty.append(f"{doc['metadata']['name']}.{key}")
        for key, value in (doc.get("data") or {}).items():
            if base64.b64decode(value) == b"":
                empty.append(f"{doc['metadata']['name']}.{key}")

    assert empty == [], (
        f"{os.path.basename(overlay)} renders successfully but leaves "
        f"{empty} empty even though the reader filled in every key the overlay "
        "told them to. That is the silent half of this defect: the install "
        "reports success and fails later as an auth error naming neither the "
        "key nor this chart. Add the key to the header skeleton."
    )
