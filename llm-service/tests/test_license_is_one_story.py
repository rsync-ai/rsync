"""The licence has to be said the same way everywhere a stranger can read it.

On 2026-10-02 the licence moved from the Elastic License 2.0 to the rsync.ai
Source-Available License v1.0, because the product owner wants nobody who
downloads or forks the code to be able to sell it, host it for others, or build a
competing product from it -- while a company running it for its own business
stays free. ELv2 only forbids a hosted service; it permits every other commercial
use, so it could not say that.

A licence file that forbids commercialising, sitting beside metadata that says
otherwise, is a licence with a hole in it. That was the real defect found while
making this change: the tool generator's spec schema defaulted every connector's
``license`` field to ``"MIT"``, so 32 connector files described first-party code as
MIT. A fork reading ``metadata.json`` has an argument that the connector is MIT.
Nothing in the repo would have noticed, because nothing compared those labels to
the licence.

So this file compares them. It checks four things, each of which was wrong or
unguarded before the change:

  1. ``LICENSE`` is the current text, ``LICENSES/`` carries a byte-identical copy
     under its SPDX name, and the text ELv2 releases shipped with is preserved
     rather than overwritten. Releases up to v0.1.7 are irrevocably licensed under
     that text; losing it would leave the earlier grants without their terms.
  2. ``REUSE.toml`` names the same identifier, so a scanner reading the tree gets
     the same answer a person reading ``LICENSE`` does.
  3. Every connector's ``license`` field -- and the generator default that writes
     it -- is that identifier.
  4. The prose a newcomer reads first does not present ELv2 as the current licence.
     A line may still *mention* ELv2 when it is plainly about the earlier releases.

The identifier is a ``LicenseRef-``: this is a custom licence, not an SPDX-listed
one, and calling it anything else would be a false label.

Everything here is present in both the private tree and the public cut, except the
generator schema, which the OSS strip list removes -- that one check skips itself
when its subject is absent, rather than failing the public suite.
"""

import json
import pathlib
import re

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]

LICENSE_NAME = "rsync.ai Source-Available License"
LICENSE_ID = "LicenseRef-rsync.ai-SAL-1.0"
LEGACY_ID = "LicenseRef-rsync.ai-ELv2-legacy"

# Prose that tells a newcomer which licence applies. Not the internal design docs:
# those record decisions and legitimately discuss ELv2 as history.
PROSE = [
    "README.md",
    "TRADEMARK.md",
    "SUPPORT.md",
    "CONTRIBUTING.md",
    "install.sh",
    "install-k8s.sh",
    "THIRD_PARTY_NOTICES.md",
    "scripts/gen-third-party-notices.sh",
    ".github/workflows/licenses.yml",
]

# A line that names ELv2 is fine when it is visibly about the earlier releases.
HISTORICAL = re.compile(
    r"earlier|legacy|previous|prior|before|replaced|v0\.1\.7|until|formerly",
    re.IGNORECASE,
)
ELV2 = re.compile(r"ELv2|Elastic License", re.IGNORECASE)

SKIP_DIRS = {"node_modules", ".git", "vendor"}


def _read(rel):
    return (REPO / rel).read_text(encoding="utf-8")


def _connector_metadata():
    """Every connector spec.json / metadata.json, minus dependency trees."""
    root = REPO / "shared" / "mcp-connectors"
    found = []
    for name in ("spec.json", "metadata.json"):
        for path in root.rglob(name):
            if SKIP_DIRS.intersection(path.parts):
                continue
            found.append(path)
    return sorted(found)


def test_license_file_is_the_current_text():
    text = _read("LICENSE")
    assert text.splitlines()[0].strip() == LICENSE_NAME, (
        "LICENSE must open with the licence's name; it still reads as the old text"
    )
    # The clauses that make "can not commercialize" true. Each one is a promise the
    # README makes, so a LICENSE that loses it leaves the README lying.
    for clause in (
        "internal business purposes",
        "No commercialization",
        "hosted, managed, or cloud service",
        "compete",
        "Earlier versions",
        "Commercial licenses",
    ):
        assert clause in text, f"LICENSE no longer contains {clause!r}"


def test_the_spdx_named_copy_is_byte_identical():
    copy = REPO / "LICENSES" / f"{LICENSE_ID}.txt"
    assert copy.exists(), f"LICENSES/{LICENSE_ID}.txt is missing"
    assert copy.read_bytes() == (REPO / "LICENSE").read_bytes(), (
        "LICENSES/ has drifted from LICENSE; edit LICENSE and copy it, never the reverse"
    )


def test_the_text_elv2_releases_shipped_with_is_preserved():
    legacy = REPO / "LICENSES" / f"{LEGACY_ID}.txt"
    assert legacy.exists(), (
        f"LICENSES/{LEGACY_ID}.txt is missing: v0.1.7 and earlier were granted under "
        "it, irrevocably, and the grant is meaningless without its terms"
    )
    first = legacy.read_text(encoding="utf-8").splitlines()[0]
    assert "Elastic License 2.0" in first, f"legacy text was altered: {first!r}"


def test_reuse_declares_the_same_identifier():
    reuse = REPO / "REUSE.toml"
    assert reuse.exists(), "REUSE.toml is missing"
    assert LICENSE_ID in reuse.read_text(encoding="utf-8")


def test_every_connector_says_the_same_licence():
    files = _connector_metadata()
    # Control: a glob that matches nothing would make the loop below pass vacuously.
    assert len(files) >= 20, f"expected the connector metadata, found {len(files)} files"

    wrong = {}
    labelled = 0
    for path in files:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or "license" not in data:
            continue
        labelled += 1
        if data["license"] != LICENSE_ID:
            wrong[str(path.relative_to(REPO))] = data["license"]
    assert labelled >= 20, f"expected most connectors to carry a licence, found {labelled}"
    assert not wrong, (
        f"connector metadata labelling first-party code with another licence: {wrong}"
    )


def test_the_generator_default_matches():
    schema = REPO / "llm-service/src/agents/tool_generator/schemas/spec.py"
    if not schema.exists():
        pytest.skip("the tool generator is not in the public cut")
    match = re.search(r"^\s*license:\s*str\s*=\s*Field\(default=\"([^\"]*)\"", schema.read_text(), re.M)
    assert match, "could not find the generator's licence default"
    assert match.group(1) == LICENSE_ID, (
        f"the generator writes {match.group(1)!r} into every new connector"
    )


@pytest.mark.parametrize("rel", PROSE)
def test_prose_does_not_present_elv2_as_current(rel):
    if not (REPO / rel).exists():
        pytest.skip(f"{rel} is not in this tree")
    lines = _read(rel).splitlines()
    stale = []
    for i, line in enumerate(lines):
        if not ELV2.search(line):
            continue
        # Prose wraps: "...up to v0.1.7 were published under the" / "Elastic License 2.0".
        # Judge a line together with its neighbours, or a wrapped sentence about the
        # earlier releases reads as a claim about the current one.
        window = " ".join(lines[max(0, i - 1) : i + 2])
        if not HISTORICAL.search(window):
            stale.append(f"{rel}:{i + 1}: {line.strip()[:120]}")
    assert not stale, "still presents ELv2 as the licence:\n" + "\n".join(stale)


def test_contributions_come_in_under_a_cla():
    cla = REPO / "CLA.md"
    assert cla.exists(), "CLA.md is missing"
    text = cla.read_text(encoding="utf-8")
    # The reason to have a CLA at all: the licensor must be able to relicense. A
    # CLA without this grant is a DCO with extra steps.
    assert "any license terms" in text, "the CLA no longer grants the right to relicense"
    assert "CLA.md" in _read("CONTRIBUTING.md"), "CONTRIBUTING.md does not send people to the CLA"
