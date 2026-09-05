"""The self-host boundary claims must stay true on disk.

Three doc claims landed together, and each is the kind that goes false silently:

  1. "Connector *generation* is not available self-hosted." True today because
     the route's only definition lives under a path in `oss-strip-list.txt` and
     the OSS container runs a different program. Re-add generation to either the
     image or the lifecycle router and the docs become a lie with nothing failing.

  2. "`cors-locked` is defined but attached to nothing." A one-line
     `middlewares=` edit makes that false. CAPABILITIES.md used to make the
     opposite claim -- an active CORS allow-list -- for three months.

  3. `scripts/flip/delink-docs.sh` rewrites prose it verified present at
     authoring time. Its own miss check is FATAL, but it only ever runs on flip
     day; until then an entry can rot for weeks and the first symptom is the cut
     aborting. Assert the find-strings match TODAY instead.

Every census here asserts a non-zero floor first. A count of zero is not an
error, so a matcher that silently stops matching passes a subject-less test.
Stdlib only -- this runs in doc-links.yml's light venv.
"""

from __future__ import annotations

import ast
import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

# Docs an operator reads on the way to building a connector. Not every file that
# mentions the endpoint: architecture/LLD docs describe the hosted pipeline and
# are correct to. These two are the ones the connector index points a
# self-hoster at, so these two must carry the caveat.
OPERATOR_CONNECTOR_DOCS = (
    "docs/connectors/developer-guide.md",
    "docs/connectors/reference.md",
)

BOUNDARY_DOC = "docs/deployment/self-host-feature-boundary.md"

# Any one of these, in the same file, counts as stating the caveat. Matched
# against whitespace-collapsed text: these docs hard-wrap, and reference.md
# states it as "in the\nhosted service only" -- a substring check against the
# raw file would miss the caveat that is plainly there.
CAVEAT_PATTERNS = (
    r"not available (?:in a )?self-hosted",
    r"hosted[- ](?:service )?only",
    r"only in the hosted",
    r"stripped from self-hosted",
    r"not in a self-hosted",
)


def _states_caveat(text: str) -> bool:
    flat = " ".join(text.split()).lower()
    return any(re.search(p, flat) for p in CAVEAT_PATTERNS)

# Phrases that record cors-locked as inert. Any one is enough.
UNATTACHED_PHRASES = (
    "attached to nothing",
    "affects no request",
    "unattached",
    "not attached",
)


def _tracked() -> set[str]:
    out = subprocess.run(
        ["git", "ls-files", "-z"], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout
    return {p for p in out.split("\0") if p}


def _read(rel: str) -> str:
    return (REPO / rel).read_text(encoding="utf-8")


def _strip_list() -> list[str]:
    entries = []
    for line in _read("llm-service/oss-strip-list.txt").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            entries.append(line)
    return entries


def _generate_route_definitions() -> list[tuple[str, int]]:
    """Server-side definitions of the connector-generation route.

    A definition, not a mention: a FastAPI decorator whose path is the generate
    route. Callers (planner/) and prose keep mentioning it and must not count.
    """
    out = subprocess.run(
        [
            "git", "grep", "-n", "-E",
            r'@[a-z_]*router\.post\("/generate"',
            "--", "llm-service/src",
        ],
        cwd=REPO, capture_output=True, text=True,
    ).stdout
    hits = []
    for line in out.splitlines():
        path, lineno, _rest = line.split(":", 2)
        hits.append((path, int(lineno)))
    return hits


# ---------------------------------------------------------------------------
# 1. Generation really is absent from the self-hosted stack
# ---------------------------------------------------------------------------

# The generate route's home. `llm-service/oss-strip-list.txt` removes it from both
# published images, which is the whole mechanism this section asserts -- so on a tree
# where the strip has been applied the route is GONE, and the census floor below has
# to expect a different number rather than fail. Derived from the directory, not from
# an env var or the repo name: put the route back and the private-tree floor returns.
TOOL_GENERATOR_AGENTS = "llm-service/src/agents/tool_generator/agents"
_TOOL_GENERATOR_SHIPS = (REPO / TOOL_GENERATOR_AGENTS).is_dir()


def test_the_route_census_finds_the_definitions_at_all():
    """Floor. If the decorator shape changes, every assertion below goes vacuous."""
    hits = _generate_route_definitions()
    floor = 2 if _TOOL_GENERATOR_SHIPS else 1
    assert len(hits) >= floor, (
        f"Expected at least {floor} generate route(s) "
        + ("(tool-generator and suggestions)" if _TOOL_GENERATOR_SHIPS
           else "(suggestions; the tool-generator's is stripped from this tree)")
        + f"; found {hits}. The decorator pattern probably changed -- fix the matcher, "
        "do not delete the test."
    )


def test_the_route_matcher_discriminates():
    """Control: the matcher must not fire on a caller or on prose."""
    pattern = re.compile(r'@[a-z_]*router\.post\("/generate"')
    assert pattern.search('@router.post("/generate", dependencies=[])')
    assert pattern.search('@lifecycle_router.post("/generate")')
    # A caller, a doc line, and a neighbouring route must all fail to match.
    assert not pattern.search('requests.post(f"{URL}/v1/generate", json=payload)')
    assert not pattern.search("`POST http://tool-generator:5010/v1/generate`")
    assert not pattern.search('@lifecycle_router.post("/deploy")')


def test_the_generation_route_lives_only_under_a_stripped_path():
    """The mechanism behind "generation is hosted-only".

    The tool-generator's generate route must sit inside a path that
    oss-strip-list.txt removes from BOTH published images. Move it out -- or add
    a second definition somewhere that ships -- and this fires.
    """
    stripped = _strip_list()
    tool_gen = [
        (p, n) for p, n in _generate_route_definitions()
        if "tool_generator" in p
    ]
    if not _TOOL_GENERATOR_SHIPS:
        # The strip has already been applied to this tree, so the claim is directly
        # checkable rather than inferred from a path: the route must be ABSENT, not
        # merely covered by a list entry. This is the stronger of the two assertions
        # and it is the one that runs where it matters -- the published tree.
        assert not tool_gen, (
            f"{TOOL_GENERATOR_AGENTS} was stripped from this tree, yet a "
            f"connector-generation route still ships at {tool_gen}. "
            f"{OPERATOR_CONNECTOR_DOCS[0]} tells self-hosters generation is not "
            f"available; this route makes that false."
        )
        return
    assert tool_gen, "no generate route under tool_generator/ -- see the census test"
    for path, lineno in tool_gen:
        rel = path[len("llm-service/"):]
        covered = [e for e in stripped if rel == e or rel.startswith(e.rstrip("/") + "/")]
        assert covered, (
            f"{path}:{lineno} defines the connector-generation route, but no "
            f"oss-strip-list.txt entry covers {rel!r}. It would ship in the "
            f"community image, and {OPERATOR_CONNECTOR_DOCS[0]} says it does not."
        )


def test_the_oss_lifecycle_router_serves_deploy_and_nothing_else():
    """The second half of the mechanism: the OSS container runs this router.

    docker-compose.quickstart.yml runs `python -m src.lifecycle.main`, which
    mounts lifecycle_router under /v1. If a generate route appears here it is
    served self-hosted regardless of the strip list.
    """
    src = _read("llm-service/src/agents/tool_generator/deployment/routes.py")
    paths = sorted(set(re.findall(r'@lifecycle_router\.\w+\(\s*"([^"]+)"', src)))
    assert paths == ["/deploy"], (
        f"lifecycle_router now declares {paths}. The self-host boundary doc and "
        f"{OPERATOR_CONNECTOR_DOCS[0]} both state the self-hosted service answers "
        "/health, /version and POST /v1/deploy only."
    )

    compose = _read("docker-compose.quickstart.yml")
    assert "src.lifecycle.main" in compose, (
        "docker-compose.quickstart.yml no longer runs src.lifecycle.main, so the "
        "router asserted above is no longer what a self-hoster gets."
    )


# ---------------------------------------------------------------------------
# 2. The docs an operator reads still carry the caveat
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("rel", OPERATOR_CONNECTOR_DOCS)
def test_operator_connector_docs_are_tracked(rel):
    assert rel in _tracked(), f"{rel} is untracked -- the guard's subject moved"


@pytest.mark.parametrize("rel", OPERATOR_CONNECTOR_DOCS)
def test_a_doc_that_names_the_generate_endpoint_says_it_is_hosted_only(rel):
    body = _read(rel)
    if "/v1/generate" not in body:
        pytest.skip(f"{rel} no longer names the endpoint")
    assert _states_caveat(body), (
        f"{rel} points a reader at POST /v1/generate without saying it is not "
        f"available self-hosted. Expected one of: {CAVEAT_PATTERNS}"
    )


def test_the_caveat_matcher_discriminates():
    """Control: a doc that names the endpoint with no caveat must be flagged."""
    assert not _states_caveat("Generate the connector source: POST /v1/generate")
    assert not _states_caveat("Self-hosted stacks run the same images.")
    assert _states_caveat("Step 1 is **not available in a self-hosted stack**.")
    assert _states_caveat("Connector generation is hosted-only.")
    # The wrapped form reference.md actually uses.
    assert _states_caveat("generated from an API docs -- **in the\nhosted service only.**")


def test_the_boundary_doc_states_the_caveat_where_the_guides_link_to_it():
    body = _read(BOUNDARY_DOC)
    assert _states_caveat(body), (
        f"{BOUNDARY_DOC} is the target both connector guides defer to; it must "
        "state the caveat itself, not only link onward."
    )


def _slug(heading: str) -> str:
    """GitHub's heading -> anchor rule, enough of it for these docs."""
    s = heading.strip().lstrip("#").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    return re.sub(r"\s+", "-", s)


def test_every_anchor_link_into_the_boundary_doc_resolves():
    """A doc link rots three ways; a renamed heading is the quiet one.

    Nothing fails when `#connector-generation-what-exactly-is-missing` stops
    existing -- the link just lands at the top of the page.
    """
    slugs = {
        _slug(l) for l in _read(BOUNDARY_DOC).splitlines() if l.startswith("#")
    }
    assert len(slugs) >= 5, f"only {len(slugs)} headings parsed out of {BOUNDARY_DOC}"

    linkers = [
        p for p in _tracked()
        if p.endswith(".md") and "self-host-feature-boundary.md#" in _read(p)
    ]
    assert linkers, (
        "no tracked doc anchor-links into the boundary doc; this test's subject "
        "is gone (the two connector guides linked into it when it was written)"
    )
    for rel in sorted(linkers):
        for anchor in re.findall(r"self-host-feature-boundary\.md#([\w-]+)", _read(rel)):
            assert anchor in slugs, (
                f"{rel} links to {BOUNDARY_DOC}#{anchor}, which is not a heading "
                f"in that file. Present: {sorted(slugs)}"
            )


# ---------------------------------------------------------------------------
# 3. cors-locked is still inert, and still documented as inert
# ---------------------------------------------------------------------------

def _traefik_attachments() -> list[str]:
    # YAML only. A traefik label has effect in a compose file or a chart
    # template and nowhere else -- and scanning wider swept up this test's own
    # control fixture below, which made the guard report its own fixture as the
    # defect. A census whose subject set is "every tracked file" is the wrong
    # subject set.
    out = subprocess.run(
        ["git", "grep", "-h", "-E",
         r"traefik\.http\.routers\.[a-z-]+\.middlewares=", "--", "*.yml", "*.yaml"],
        cwd=REPO, capture_output=True, text=True,
    ).stdout
    # Drop commented-out examples: deploy/traefik/dynamic.yml documents how an
    # operator would attach cors-locked, and a comment attaches nothing.
    return [l for l in out.splitlines() if not l.lstrip().startswith("#")]


def test_the_attachment_census_finds_routers_at_all():
    lines = _traefik_attachments()
    assert len(lines) >= 4, (
        f"found only {len(lines)} router middleware attachments; the label shape "
        "changed and the next test would pass vacuously"
    )


def test_the_attachment_matcher_discriminates():
    pattern = re.compile(r"traefik\.http\.routers\.[a-z-]+\.middlewares=")
    assert pattern.search(
        '- "traefik.http.routers.api.middlewares=security-headers@file"'
    )
    assert not pattern.search('- "traefik.http.routers.api.rule=Host(`x`)"')
    assert not pattern.search("  cors-locked:")
    # A commented example is not an attachment -- _traefik_attachments filters
    # these out, and this is the line in dynamic.yml that proved it must.
    commented = '    #   - "traefik.http.routers.api.middlewares=cors-locked@file"'
    assert pattern.search(commented), "matcher should still see the text"
    assert commented.lstrip().startswith("#"), "and the filter should drop it"


def test_cors_locked_is_attached_to_no_router():
    attached = [l for l in _traefik_attachments() if "cors-locked" in l]
    assert not attached, (
        "cors-locked is now attached to a router:\n  "
        + "\n  ".join(a.strip() for a in attached)
        + "\nThat is a real change, and it makes the CAPABILITIES.md row and "
          "deploy/traefik/dynamic.yml's comment wrong: both say it affects no "
          "request. Update them in the same commit -- and remember the origin "
          "list is empty, so attaching it blocks every cross-origin request."
    )


def test_capabilities_does_not_advertise_cors_locked_as_an_active_control():
    if not (REPO / "CAPABILITIES.md").exists():
        pytest.skip(
            "CAPABILITIES.md is absent -- removed by scripts/flip/excludes.txt. "
            "The two tests above still hold the actual control: nothing attaches "
            "cors-locked. This one only guards how that is written down."
        )
    rows = [
        l for l in _read("CAPABILITIES.md").splitlines()
        if l.startswith("|") and "cors-locked" in l
    ]
    assert rows, (
        "no CAPABILITIES.md row mentions cors-locked. It was corrected, not "
        "deleted -- a self-hoster reading the status board needs to know the "
        "middleware is an opt-in starting point, not a shipped control."
    )
    for row in rows:
        assert any(p in row.lower() for p in UNATTACHED_PHRASES), (
            "This CAPABILITIES.md row presents cors-locked without recording "
            f"that nothing attaches it:\n  {row.strip()[:160]}\n"
            f"Expected one of: {UNATTACHED_PHRASES}"
        )


# ---------------------------------------------------------------------------
# 4. delink-docs.sh's prose rewrites still match their targets
# ---------------------------------------------------------------------------

DELINK = "scripts/flip/delink-docs.sh"


def _rewrites() -> list[tuple[str, str, str]]:
    if not (REPO / DELINK).exists():
        # Removed by scripts/flip/excludes.txt:132 -- the flip tooling is private
        # and does not ship. This is read at COLLECTION time by the parametrize
        # below, so raising here does not fail one test: it aborts the whole
        # pytest session with exit 2 and takes every other suite in the run with
        # it. Measured on the materialised public tree: that plus one sibling was
        # enough to make 0 of doc-links.yml's 12 guards execute.
        #
        # Returning [] keeps this module's other sections alive. It cannot read as
        # a silent pass: both consumers below say out loud that the table is gone.
        return []
    lines = _read(DELINK).splitlines()
    starts = [i for i, l in enumerate(lines) if l.startswith("REWRITES = [")]
    assert len(starts) == 1, f"expected one REWRITES assignment, found {len(starts)}"
    # Bracket counting is wrong here: the find-strings are full of markdown
    # links and `- [ ]` checkboxes, so `[` inside a string literal derails it.
    # The list's terminator is a bare `]` in column 0, and there is exactly one.
    ends = [i for i in range(starts[0] + 1, len(lines)) if lines[i] == "]"]
    assert ends, f"no column-0 `]` closing REWRITES in {DELINK}"
    block = "\n".join(lines[starts[0] : ends[0] + 1]).split("=", 1)[1]
    return list(ast.literal_eval(block))


def test_the_rewrite_table_was_parsed():
    """Floor. A parse that yields nothing would make the sweep below vacuous."""
    if not (REPO / DELINK).exists():
        pytest.skip(f"{DELINK} is absent -- removed by scripts/flip/excludes.txt")
    entries = _rewrites()
    assert len(entries) >= 15, f"parsed only {len(entries)} REWRITES entries"
    assert all(len(e) == 3 for e in entries)


# The `or [...]` sentinel is the in-repo idiom for a parametrize whose source can
# legitimately be empty: zero cases render as "collected 0 items" for this test,
# which is indistinguishable from a pass. One named case is not.
@pytest.mark.parametrize(
    "idx",
    range(len(_rewrites())) or [pytest.param(-1, id="delink-script-absent")],
)
def test_each_rewrite_find_string_still_matches_its_target(idx):
    """delink-docs.sh's own miss check is FATAL -- but only on flip day.

    Between now and then a source line can be reworded by any unrelated PR, and
    the first symptom would be the public cut aborting mid-run.
    """
    if idx < 0:
        pytest.skip(f"{DELINK} is absent -- removed by scripts/flip/excludes.txt")
    rel, find, _replace = _rewrites()[idx]
    assert rel in _tracked(), f"REWRITES entry {idx} names untracked file {rel}"
    body = _read(rel)
    count = body.count(find)
    assert count == 1, (
        f"REWRITES entry {idx} for {rel} matches {count} times, expected exactly 1.\n"
        f"find: {find[:120]!r}\n"
        "Flip day would abort here. Re-read the file and update the entry -- and "
        "if the sentence no longer needs surgery, delete the entry."
    )
