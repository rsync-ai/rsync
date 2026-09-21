"""The four public-cut gates must follow ``oss-strip-list.txt``, not a copy of it.

WHAT THIS PREVENTS, MEASURED. ``llm-service/oss-strip-list.txt`` decides which half
of ``src/agents/tool_generator/`` reaches the public repo. Four gates under
``llm-service/tests/`` acted on that decision while each holding its own hand-typed
copy of the answer:

    _cut_collection.py   _SHIMMED_ROOTS = ("schemas", "generator", "validation", ...)
    conftest.py          _DEPS = ("schemas", "generator")
    unit/conftest.py     _DEPS = ("agents", "utils", "validation")
    _flip_cut.py         .../tool_generator/generator          (the moat witness)

Promoting the deterministic renderer -- contracts/, generator/, scaffold/, schemas/,
templates/, validation/ -- out of the strip list falsified all four in one commit.
Every copy still named a directory that exists, so no private check could go red;
the damage was reserved for the public repo, where it was measured as:

  * ``_flip_cut`` seeing its witness present while ``scripts/flip`` was gone, which
    is its "half-cut tree" case -> ``pytest.fail`` in all EIGHT guards importing it;
  * both conftests seeing a promoted directory and concluding the tree was intact,
    so their gates never fired, sixteen orphaned suites were collected, and each
    directory's whole pytest run aborted with exit 2.

None of it was reachable from the private repo. That is the shape of the defect:
an assertion outliving its subject, with an executable consequence somewhere the
author cannot see.

THE FIX THIS FILE GUARDS. All four now derive their names from the strip list,
which survives the cut (``scripts/flip/excludes.txt`` may name nothing under
``llm-service/`` -- enforced by
``test_flip_excludes_do_not_delete_the_community_image.py``). So the next promotion
moves every gate by editing one line, and these tests fail if a copy comes back.

WHY THE CROSS-CHECK USES THE DOCKERFILE. Asking the strip list whether the strip
list is right proves nothing. ``Dockerfile.community``'s COPY allowlist is the other
half of the same split and an independent statement of it, so it can say whether the
detector ignores a suite whose imports the community image actually ships -- which
is how the first draft of the fix was caught quietly hiding
``test_scaffold_renders_offline.py``, the guard for the very feature the promotion
exists to deliver.
"""

from __future__ import annotations

import ast
import os
import re

import pytest

import _cut_collection
import _flip_cut
from _cut_collection import (
    STRIPPED_MODULES,
    STRIPPED_PATHS,
    ignored_modules,
    tree_is_intact,
)


@pytest.fixture(autouse=True)
def _only_before_the_cut():
    """Every check here reads a directory the cut deletes; see _flip_cut for why.

    Note what this is NOT: a `skipif` on one hard-coded path. Naming a subtree here
    would plant a fifth copy of the fact these tests exist to keep single, and it
    would go stale on exactly the promotion they are meant to survive.
    """
    _flip_cut.require_a_pre_cut_tree()

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
UNIT_DIR = os.path.join(TESTS_DIR, "unit")
LLM_SERVICE = os.path.dirname(TESTS_DIR)
PACKAGE = os.path.join(LLM_SERVICE, "src", "agents", "tool_generator")
STRIP_LIST = os.path.join(LLM_SERVICE, "oss-strip-list.txt")
DOCKERFILE_COMMUNITY = os.path.join(LLM_SERVICE, "Dockerfile.community")

# The three gates that CONSUME the derivation. _cut_collection.py is excluded on
# purpose: it is the one file allowed to spell the package path out.
CONSUMERS = (
    os.path.join(TESTS_DIR, "conftest.py"),
    os.path.join(UNIT_DIR, "conftest.py"),
    os.path.join(TESTS_DIR, "_flip_cut.py"),
)

_REL_PACKAGE = "src/agents/tool_generator"
_ABSOLUTE_PREFIX = "agents.tool_generator"

# A local copy, deliberately not imported from the boundary guard: two independent
# readings of one Dockerfile is the point, and a shared parser that stops matching
# would take both readings with it.
_COPY = re.compile(r"^\s*COPY\s+(?!--)(\S+)\s+(\S+)\s*$")


def _strip_list_children() -> set[str]:
    """Direct children of the package named by the strip list. Parsed here, again."""
    out = set()
    with open(STRIP_LIST, encoding="utf-8") as handle:
        for raw in handle:
            entry = raw.strip()
            if not entry or entry.startswith("#"):
                continue
            if entry.startswith(_REL_PACKAGE + "/"):
                tail = entry[len(_REL_PACKAGE) + 1:]
                if "/" not in tail:
                    out.add(tail)
    return out


def _shipped_children() -> set[str]:
    """Package children the community image COPYs. The independent half."""
    out = set()
    with open(DOCKERFILE_COMMUNITY, encoding="utf-8") as handle:
        for line in handle:
            match = _COPY.match(line)
            if not match:
                continue
            source = match.group(1).rstrip("/")
            if source.startswith(_REL_PACKAGE + "/"):
                tail = source[len(_REL_PACKAGE) + 1:]
                if "/" not in tail:
                    out.add(tail[:-3] if tail.endswith(".py") else tail)
    return out


def _test_modules(directory: str) -> list[str]:
    return [
        n for n in sorted(os.listdir(directory))
        if n.startswith("test_") and n.endswith(".py")
    ]


def _package_children_imported(path: str) -> set[str]:
    """Package children this file reaches for, by either import shape."""
    modules = _cut_collection._imported_modules(path) or []
    with open(path, encoding="utf-8") as handle:
        names_the_package = "tool_generator" in handle.read()
    src_agents = os.path.join(LLM_SERVICE, "src", "agents")
    wanted = set()
    for module in modules:
        parts = module.split(".")
        if module.startswith(_ABSOLUTE_PREFIX + ".") and len(parts) > 2:
            wanted.add(parts[2])
        elif names_the_package and os.path.isdir(os.path.join(PACKAGE, parts[0])):
            if parts[0] == "agents":
                # src/agents claims the same name and survives; only a child it does
                # NOT have is a shim reach-in into the package's own agents/.
                sibling = os.path.join(src_agents, parts[1]) if len(parts) > 1 else ""
                if sibling and not (os.path.isdir(sibling) or os.path.exists(sibling + ".py")):
                    wanted.add("agents")
            else:
                wanted.add(parts[0])
    return wanted


# ── the derivation itself ────────────────────────────────────────────────────

def test_the_gates_read_the_strip_list_and_parse_all_of_it():
    """Re-parsed here. A parser that silently stops matching ignores nothing."""
    assert STRIPPED_PATHS == _strip_list_children(), (
        "_cut_collection.STRIPPED_PATHS disagrees with a second reading of "
        f"{STRIP_LIST}: derived={sorted(STRIPPED_PATHS)}, "
        f"re-parsed={sorted(_strip_list_children())}"
    )
    # 11 entries today. The floor tells "the parser stopped matching" from "the list
    # legitimately shrank", so it sits below the real count with room for the next
    # promotion rather than flush against it.
    assert len(STRIPPED_PATHS) >= 6, f"parsed only {sorted(STRIPPED_PATHS)}"


def test_every_derived_name_is_a_path_that_exists():
    """A rename would otherwise turn a gate's subject into a silent no-op."""
    missing = [n for n in sorted(STRIPPED_PATHS) if not os.path.exists(os.path.join(PACKAGE, n))]
    assert not missing, (
        f"oss-strip-list.txt names {missing} under {_REL_PACKAGE}/, which are not on "
        "disk. Every gate keyed on the strip list now has a subject that cannot be "
        "found, and each one fails open in a different direction."
    )


def test_no_consumer_hard_codes_a_package_child_name():
    """The regression this whole file exists for: a fifth copy of the answer.

    Scans assignments only, so the explanatory prose in these files' docstrings --
    which does name the directories, and should -- is not the subject.
    """
    every_child = set(os.listdir(PACKAGE))
    offenders = []
    for path in CONSUMERS:
        with open(path, encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            if node.value is None:
                continue
            for child in ast.walk(node.value):
                if isinstance(child, ast.Constant) and isinstance(child.value, str):
                    if child.value in every_child:
                        offenders.append(
                            f"{os.path.relpath(path, LLM_SERVICE)}:{child.lineno} "
                            f"assigns the literal {child.value!r}"
                        )
    assert not offenders, (
        "A public-cut gate names a tool_generator child in code instead of deriving "
        "it from oss-strip-list.txt:\n  " + "\n  ".join(offenders) + "\n"
        "Four such literals went stale in one commit and broke the public repo in "
        "three places while every private check stayed green. Import STRIPPED_PATHS "
        "/ tree_is_intact() from _cut_collection instead."
    )


# ── the cross-check, against the other half of the split ─────────────────────

def test_the_split_is_a_partition():
    """Stripped and shipped must not overlap, and must cover the package."""
    shipped, stripped = _shipped_children(), STRIPPED_MODULES
    both = sorted(shipped & stripped)
    assert not both, f"named by both Dockerfile.community and oss-strip-list.txt: {both}"

    on_disk = {
        n[:-3] if n.endswith(".py") else n
        for n in os.listdir(PACKAGE)
        if not n.startswith("__") and not n.endswith(".md")
    }
    neither = sorted(on_disk - shipped - stripped)
    assert not neither, (
        f"{_REL_PACKAGE}/{{{','.join(neither)}}} is neither COPYd by "
        "Dockerfile.community nor stripped by oss-strip-list.txt. The community "
        "image would silently omit it and no gate would know which side it is on."
    )


def test_the_detector_ignores_exactly_the_suites_the_image_cannot_satisfy():
    """Classification cross-checked against the Dockerfile, not the strip list.

    A suite is orphaned publicly if and only if it reaches for a package child the
    community image does not COPY. Comparing that to the detector's answer catches
    both directions: a missed suite reds the public build, and an over-matched one
    silently drops coverage the public repo was supposed to keep.
    """
    shipped = _shipped_children()
    assert shipped, "parsed no COPY lines from Dockerfile.community"

    wrong = []
    for directory, label in ((TESTS_DIR, "tests"), (UNIT_DIR, "tests/unit")):
        ignored = set(ignored_modules(directory))
        for name in _test_modules(directory):
            wanted = _package_children_imported(os.path.join(directory, name))
            unsatisfied = sorted(wanted - shipped)
            expected = bool(unsatisfied)
            actual = name in ignored
            if expected != actual:
                wrong.append(
                    f"{label}/{name}: image ships {sorted(wanted & shipped)}, cannot "
                    f"satisfy {unsatisfied}; detector says "
                    f"{'ignore' if actual else 'collect'}"
                )
    assert not wrong, (
        "_cut_collection's ignore set disagrees with what Dockerfile.community "
        "ships:\n  " + "\n  ".join(wrong)
    )


# ── liveness: the gates must actually re-read, not merely happen to agree ────

def test_promoting_a_subpackage_changes_what_is_ignored(monkeypatch):
    """A frozen constant that happens to match today is indistinguishable by value.

    Only a mutation tells them apart: drop ``agents`` from the derived set as a
    promotion would, and the twelve shim-importing suites must stop being ignored.
    """
    before = set(ignored_modules(TESTS_DIR))
    assert before, "nothing ignored, so this mutation could not show anything"

    monkeypatch.setattr(
        _cut_collection, "STRIPPED_MODULES", STRIPPED_MODULES - {"agents"}
    )
    after = set(ignored_modules(TESTS_DIR))

    freed = before - after
    assert freed, (
        "removing 'agents' from the derived set changed nothing, so the detector is "
        "not reading it -- the exact failure mode this file guards"
    )
    assert all(n.startswith("test_gen_") for n in freed), sorted(freed)


def test_the_intactness_gate_follows_the_same_source(monkeypatch):
    """``tree_is_intact`` is what both conftests and _flip_cut now key on."""
    assert tree_is_intact(), "the private checkout must read as intact"

    # A tree where nothing the strip list names survives is a cut tree, whatever
    # else is on disk -- this is the public repo's state.
    monkeypatch.setattr(_cut_collection, "STRIPPED_PATHS", frozenset({"does-not-exist"}))
    assert not tree_is_intact()


@pytest.mark.parametrize(
    "module,reaches,why",
    [
        # Absolute imports: the THIRD component decides. The bare prefix means
        # nothing now that half the package ships -- reading the first component
        # instead is the original defect, and it saw 'agents' in every one of these.
        ("agents.tool_generator.generator.builder", False, "generator/ ships"),
        ("agents.tool_generator.scaffold.cli", False, "scaffold/ ships"),
        ("agents.tool_generator.validation.protocol_invariants", False, "validation/ ships"),
        ("agents.tool_generator", False, "the package root itself ships"),
        ("agents.tool_generator.utils.x", True, "utils/ is stripped"),
        ("agents.tool_generator.agents.rest", True, "agents/ is stripped"),
        # Shim imports, resolved by sys.path against the package directory.
        ("agents.architect_rest", True, "src/agents has no architect_rest"),
        ("agents.planner.x", False, "src/agents/planner ships publicly"),
        ("generator.builder", False, "generator/ ships"),
        ("utils.x", True, "utils/ is stripped"),
        ("pytest", False, "not the package at all"),
    ],
)
def test_the_two_import_shapes_are_judged_by_different_rules(module, reaches, why):
    """Guards the discriminator no file under tests/ currently exercises.

    Every bare ``agents.*`` import in this repo today names a child ``src/agents``
    does not have, so removing the existence check changes nothing measurable and
    the Dockerfile cross-check above stays green through it -- measured, not
    assumed. The check is here for the case that arrives later: a suite importing
    ``agents.planner``, which ships, must not be dropped from the public run. So it
    gets a direct test rather than a bystander one.
    """
    assert _cut_collection._reaches_into_the_moat(module) is reaches, (
        f"{module!r} should {'' if reaches else 'not '}count as a reach-in: {why}"
    )


def test_a_missing_strip_list_fails_loudly(tmp_path, monkeypatch):
    """Failing open here would ignore nothing and abort the public run with exit 2."""
    empty = tmp_path / "oss-strip-list.txt"
    empty.write_text("# only comments\n", encoding="utf-8")
    monkeypatch.setattr(_cut_collection, "STRIP_LIST", str(empty))
    with pytest.raises(RuntimeError, match="named no direct child"):
        _cut_collection._read_strip_list()


def test_first_party_reads_the_same_after_the_cut_deletes_the_moat(tmp_path, monkeypatch):
    """``is_first_party`` must not change its answer when the stripped half is gone.

    It is what tells a boundary defect from an absent dependency in
    ``test_community_image_can_scaffold.py``: a shipped module that imports
    ``agents.tool_generator.agents`` fails the build, one that imports ``fastapi``
    does not. The first draft asked only the filesystem, which answers correctly in
    the private repo and stops being able to answer in the public one -- there the
    reach-in resolves to no path at all, reads as third-party, and the live defect
    ships green. That is the same shape as the four hand-copied name lists this file
    exists to keep single: a fact read from a place the cut deletes.

    The private repo cannot show that by running, because both clauses agree here.
    So the filesystem half is pointed at an empty directory -- the post-cut tree as
    far as this rule can see -- and the verdicts must not move. Without that, the
    regression is invisible to every check CI runs and only surfaces on the public
    repo, which is exactly where it does damage.
    """
    # Unpatched first: the shipped half is present in BOTH trees, so disk answers it
    # and no strip-list clause is involved.
    for name in ("agents.tool_generator.scaffold", "agents.tool_generator.generator"):
        assert _cut_collection.is_first_party(name) is True, (
            f"{name!r} ships and is on disk in both trees"
        )

    empty = str(tmp_path / "post-cut")
    os.makedirs(empty)
    monkeypatch.setattr(_cut_collection, "_LLM_SERVICE", empty)
    monkeypatch.setattr(_cut_collection, "_SRC_AGENTS", os.path.join(empty, "src", "agents"))

    for name in (
        "agents.tool_generator.agents",
        "agents.tool_generator.agents.base",
        "agents.tool_generator.utils.vendor_registry",
        "agents.tool_generator.config",
    ):
        assert _cut_collection.is_first_party(name) is True, (
            f"{name!r} is ours and the cut removes it, so the filesystem cannot say "
            "so -- the strip list has to"
        )
    for name in ("fastapi", "openai", "jinja2"):
        assert _cut_collection.is_first_party(name) is False, (
            f"{name!r} is a dependency; calling it first-party would fail the build "
            "on any machine that has not installed it"
        )
