"""A shipped compose file may not command a Python module the public cut deletes.

WHAT BROKE. `docker-compose.yml:1608` ran

    command: ["python", "-m", "src.agents.tool_generator.service"]

and `llm-service/oss-strip-list.txt:38` deletes `src/agents/tool_generator/service.py`.
`docker-compose.yml` is not on `scripts/flip/excludes.txt`, so the public repo ships
that line verbatim. The container starts, imports nothing, and dies:
`No module named src.agents.tool_generator.service`. The base block declares no
`restart:`, so on a bare `docker compose up` it just sits Exited -- but
`docs/deployment/self-hosting.md:215` hands a VM self-hoster
`-f docker-compose.yml -f docker-compose.prod.yml`, and the prod overlay adds
`restart: unless-stopped`. One dead container becomes a crash loop on a supported
public path.

THE PROJECT ALREADY KNEW THE RULE. `deploy/helm/rsync-ai/templates/apps/generation.yaml`
commands `src.lifecycle.main` for the same service and says why in its header
comment: "the latter is the connector-GENERATION service, which is not in any
published image ... the pod would CrashLoopBackOff on import." The chart obeyed the
rule; the compose base is the one delivery surface that did not.

THE FIX. `scripts/flip/delink-docs.sh` gained a PATCHED list -- non-markdown files
whose REWRITES entries it applies -- and an entry that swaps the command for
`src.lifecycle.main`, which survives the strip and serves the `/v1/deploy` surface
the Go data plane calls through `TOOL_GENERATOR_URL`. Repairing it at cut time
rather than in this repo is what CLAUDE.md's OSS/cloud rule requires: the private
default stays the cloud behaviour, and only the public tree differs.

WHY A STATIC GUARD. Executing the cut means `git rm -r` + `rm -rf` over 261 paths;
a unit test in a working checkout may not do that. What it can hold is the
invariant the scratch-clone replay verified: every `python -m` a shipped compose
file commands must name a module that survives the cut, or be a module the
de-linker is on record replacing.

WHY IT CANNOT PASS VACUOUSLY. Three floors. The strip list must parse to paths that
exist; the compose sweep must find a plausible number of entrypoints (seven today);
and the two control assertions pin the ends of the mapping -- `src/lifecycle/main.py`
must survive and `src/agents/tool_generator/service.py` must not. If the strip list
is renamed, or the command syntax changes, these fail instead of ranging over
nothing.
"""

import os
import re

import pytest

import _flip_cut

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MOAT_LIST = os.path.join("llm-service", "oss-strip-list.txt")
MOAT_PREFIX = "llm-service/"
EXCLUDES_LIST = os.path.join("scripts", "flip", "excludes.txt")
DELINKER = os.path.join("scripts", "flip", "delink-docs.sh")
CHART = os.path.join("deploy", "helm", "rsync-ai", "templates", "apps", "generation.yaml")
RUNBOOK = os.path.join("docs", "internal", "public-flip-runbook.md")

# The moat-free entrypoint the chart already uses and the cut rewrites the compose to.
LIFECYCLE = "src.lifecycle.main"
# The module the cut deletes. Named here so the control below can prove the mapping
# has two distinguishable ends rather than trivially agreeing with itself.
STRIPPED = "src.agents.tool_generator.service"

# `command: ["python", "-m", "<module>"]`, the only entrypoint form the root compose
# files use. Seven sites today across three files; the floor is set under that so an
# ordinary edit does not move it, while a broken regex still fails loudly.
COMMAND = re.compile(r'command:\s*\[\s*"python"\s*,\s*"-m"\s*,\s*"([A-Za-z0-9_.]+)"\s*\]')
MIN_COMMANDS = 5


@pytest.fixture(autouse=True)
def _only_before_the_cut():
    """These guards read files the cut deletes; see _flip_cut for why not a skipif."""
    _flip_cut.require_a_pre_cut_tree()



def _read(rel):
    path = os.path.join(REPO_ROOT, rel)
    if not os.path.isfile(path):
        pytest.fail("%s is missing -- this guard's subject moved or was renamed" % rel)
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def _entries(rel, prefix=""):
    out = []
    for line in _read(rel).splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            out.append((prefix + line).rstrip("/"))
    if not out:
        pytest.fail("%s parsed to zero paths" % rel)
    return out


def _removed():
    """Every repo-relative path the public cut takes away, from BOTH lists."""
    return _entries(EXCLUDES_LIST) + _entries(MOAT_LIST, MOAT_PREFIX)


def _is_removed(path, removed):
    return any(path == r or path.startswith(r + "/") for r in removed)


def _module_path(module):
    """Where a `python -m` module under llm-service/ lives, or None if nowhere."""
    base = os.path.join("llm-service", *module.split("."))
    for candidate in (base + ".py", os.path.join(base, "__main__.py"),
                      os.path.join(base, "__init__.py")):
        if os.path.isfile(os.path.join(REPO_ROOT, candidate)):
            return candidate
    return None


def _stored_command_literal(module):
    """How a compose `command:` line looks INSIDE delink-docs.sh's REWRITES source.

    The entries are Python string literals in a shell heredoc, so the compose file's
    own double quotes arrive backslash-escaped. Building the needle from the module
    name rather than hard-coding it means this keeps working if the command moves.
    """
    return 'command: [\\"python\\", \\"-m\\", \\"%s\\"]' % module


def _compose_files():
    root = [n for n in sorted(os.listdir(REPO_ROOT))
            if n.startswith("docker-compose") and n.endswith(".yml")]
    if not root:
        pytest.fail("no docker-compose*.yml at the repo root -- the sweep has no subject")
    return root


def _entrypoints():
    """(file, module) for every `python -m` command in a compose file the cut keeps."""
    removed = _removed()
    found = []
    for name in _compose_files():
        if _is_removed(name, removed):
            continue                      # this compose file is not shipped
        for module in COMMAND.findall(_read(name)):
            found.append((name, module))
    return found


# --------------------------------------------------------------------------- floors


def test_the_strip_list_names_paths_that_exist():
    """A renamed strip entry would make every assertion below range over nothing."""
    entries = _entries(MOAT_LIST, MOAT_PREFIX)
    present = [e for e in entries if os.path.exists(os.path.join(REPO_ROOT, e))]
    assert len(present) >= len(entries) // 2, (
        "only %d of %d strip-list entries exist on disk -- the list has drifted from "
        "the tree, so 'this module survives the cut' means nothing"
        % (len(present), len(entries))
    )


def test_the_command_sweep_actually_finds_entrypoints():
    """A count of zero is not a pass; it is the regex being broken."""
    found = _entrypoints()
    assert len(found) >= MIN_COMMANDS, (
        "found only %d `python -m` command(s) across the shipped compose files "
        "(expected at least %d) -- the COMMAND regex no longer matches the file's "
        "syntax, so the assertion below is vacuous. Found: %r"
        % (len(found), MIN_COMMANDS, found)
    )


def test_the_two_ends_of_the_mapping_are_distinguishable():
    """Control: the survivor must survive and the stripped module must be stripped.

    Without this, a strip list that stopped removing anything would let every
    entrypoint pass as a survivor and the guard would confirm nothing.
    """
    removed = _removed()

    survivor = _module_path(LIFECYCLE)
    assert survivor is not None, "%s has no file -- the moat-free entrypoint moved" % LIFECYCLE
    assert not _is_removed(survivor, removed), (
        "%s (%s) is now removed by the cut. The compose rewrite points at a module "
        "the public tree will not have." % (LIFECYCLE, survivor)
    )

    stripped = _module_path(STRIPPED)
    assert stripped is not None, "%s has no file -- update this control" % STRIPPED
    assert _is_removed(stripped, removed), (
        "%s (%s) is no longer stripped. If the moat boundary really moved, this "
        "guard's premise changed and the compose rewrite may be unnecessary."
        % (STRIPPED, stripped)
    )


# --------------------------------------------------------------------------- the rule


def test_every_shipped_compose_entrypoint_survives_or_is_rewritten():
    """The invariant: nothing a shipped compose commands may vanish with the cut.

    Two ways to satisfy it. Either the module's file survives, or
    `delink-docs.sh` is on record repairing that exact file -- it must list the
    compose file in PATCHED and carry a REWRITES entry naming the module.
    """
    removed = _removed()
    delinker = _read(DELINKER)
    offenders = []

    for name, module in _entrypoints():
        path = _module_path(module)
        if path is None:
            offenders.append(
                "%s commands `python -m %s`, which resolves to no file under "
                "llm-service/" % (name, module))
            continue
        if not _is_removed(path, removed):
            continue                      # survives -- nothing to repair

        patched = re.search(r"^PATCHED\s*=\s*\[(.*?)\]", delinker, re.S | re.M)
        if not patched or '"%s"' % name not in patched.group(1):
            offenders.append(
                "%s commands `python -m %s` (%s), which the cut deletes, and "
                "scripts/flip/delink-docs.sh does not list %s in PATCHED -- so the "
                "public tree ships the broken command."
                % (name, module, path, name))
            continue
        # Both halves of the swap, matched as the literal command line rather than
        # the bare module name: `src.agents.tool_generator.service` also appears in
        # that file's explanatory comments, so a name check would stay green if the
        # REWRITES entry itself were deleted.
        if _stored_command_literal(module) not in delinker:
            offenders.append(
                "%s is in the de-linker's PATCHED list, but no REWRITES entry there "
                "searches for `command: [\"python\", \"-m\", \"%s\"]`. The file is "
                "visited and the broken command is left alone."
                % (name, module))
            continue
        if _stored_command_literal(LIFECYCLE) not in delinker:
            offenders.append(
                "the de-linker searches for `%s` but its replacement does not command "
                "`%s` -- the swap does not land on the moat-free entrypoint."
                % (module, LIFECYCLE))

    assert not offenders, (
        "A shipped compose file commands a module the public cut removes:\n  "
        + "\n  ".join(offenders)
    )


def test_the_chart_and_the_compose_agree_on_the_public_entrypoint():
    """The chart states the rule in prose; both surfaces must land on one module.

    The chart is where this was already right. If someone 'fixes' the chart back to
    the generation service, the EKS self-host path breaks in exactly the way its own
    comment predicts, and nothing else in the suite would notice.
    """
    chart = _read(CHART)
    # The chart builds its command through a Helm `list`, not a JSON array, so the
    # compose regex does not apply. Match the Helm shape instead.
    commanded = re.findall(r'"command"\s*\(list\s*"python"\s*"-m"\s*"([A-Za-z0-9_.]+)"\)', chart)
    assert commanded, (
        "%s no longer builds a `\"command\" (list \"python\" \"-m\" ...)` -- this "
        "assertion has nothing to check and would pass on any file." % CHART
    )
    assert LIFECYCLE in commanded, (
        "%s commands %r, not %s. The k8s self-host path now starts a module the "
        "published image does not contain -- exactly what that file's own header "
        "comment says must not happen." % (CHART, commanded, LIFECYCLE)
    )
    assert STRIPPED not in commanded, (
        "%s commands %s, which llm-service/oss-strip-list.txt deletes." % (CHART, STRIPPED)
    )


def test_the_runbook_stages_what_the_markdown_pathspec_cannot():
    """The repair has to reach the commit, not just the disk.

    Section 2c stages the de-linker's output with `git add -u -- '*.md'`, and
    docker-compose.yml was already staged -- unrepaired -- by step 2's `git add -A`.
    Without a second staging step the rewrite happens, nothing goes red, and the
    published commit still carries the broken command: the fix undone by the line
    that saves it. Measured on the cut replay, `-- '*.md'` left exactly
    docker-compose.yml behind.
    """
    runbook = _read(RUNBOOK)
    assert "git add -u -- '*.md'" in runbook, (
        "%s no longer stages the de-linker's markdown output the way this guard "
        "expects -- re-read 2c before trusting the assertion below." % RUNBOOK
    )
    assert "git diff --name-only -z | xargs -0 -r git add --" in runbook, (
        "%s stages only '*.md' after running the de-linker. Its PATCHED files "
        "(docker-compose.yml today) would be repaired on disk and committed broken."
        % RUNBOOK
    )
