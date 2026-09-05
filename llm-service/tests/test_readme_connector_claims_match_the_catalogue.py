"""The README's connector claims must agree with the generated catalogue.

README.md makes five separately-falsifiable claims about the connector set: a
count badge (``connectors-21``), the prose sentence "**21 connectors ship in the
box** -- every one is a source, 17 are also destinations, and five support change
data capture", and a six-row category table naming every connector plus the CDC
subset. All five are hand-maintained.

``docs/connectors/reference.md`` is not: its two tables are emitted by
``scripts/generate_connector_reference.py`` and already guarded against the
connector tree by ``test_connector_reference_matches_disk.py``. So the reference
is ground truth here, and this suite is the second hop -- reference to README --
that closes the gap between "the catalogue is right" and "the page every new
reader lands on is right".

Why this file exists at all: the README's numbers were correct when written and
nothing would have noticed when they stopped being. Adding a connector rewrites
the reference automatically and leaves the README saying 21. That is the same
self-destructing-status-claim shape as the CHANGELOG announcing a release that
was never cut -- a true sentence with no mechanism holding it true.

The name check is a bijection, not a substring sweep. A README cell resolves to
a catalogue row when it matches the row's name outright or is a whole-word
prefix of it -- "Oracle" for "Oracle Database", "GitHub" for "GitHub REST",
"Sample Data" for "Sample Data (Demo)". Requiring the resolution to be
one-to-one in both directions is what makes the loose match safe: a bare
substring rule would let "SQL" stand for "SQL Server", but it cannot also leave
every other row matched exactly once.

Internal components (MinIO, Debezium, Kafka MCP Sink) are deliberately outside
the count: they are plumbing, not something a user connects to, and the README
says "ship in the box" about the user-facing set. The reference splits them into
their own ``### Internal components`` table, and the extractor below stops at
that heading -- so this suite fails, rather than quietly absorbing them, if a
future generator merges the two tables.
"""

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
README = REPO / "README.md"
REFERENCE = REPO / "docs" / "connectors" / "reference.md"

# Floors. Both are far below the real answer (21) and exist only to turn a
# broken extractor from a silent pass into a named failure -- an empty set
# compares equal to another empty set, and every assertion below would hold.
_MIN_CONNECTORS = 15
_MIN_README_NAMES = 15

_ROW = re.compile(r"^\|(?P<cells>.+)\|\s*$")


def _cells(line: str) -> list[str]:
    m = _ROW.match(line)
    if not m:
        return []
    return [c.strip() for c in m.group("cells").split("|")]


def _is_separator(cells: list[str]) -> bool:
    return bool(cells) and all(re.fullmatch(r":?-{2,}:?", c) for c in cells)


def catalogue(text: str) -> dict[str, dict[str, bool]]:
    """The user-facing rows of the generated catalogue, keyed by display name.

    Reads only the ``### Connectors (N)`` table and stops at the next ``###``,
    which is how the internal components stay out of the count.
    """
    out: dict[str, dict[str, bool]] = {}
    inside = False
    for line in text.splitlines():
        if line.startswith("### "):
            inside = line.startswith("### Connectors")
            continue
        if not inside:
            continue
        cells = _cells(line)
        if len(cells) != 6 or _is_separator(cells) or cells[0] == "Connector":
            continue
        out[cells[0]] = {
            "source": cells[2] == "✅",
            "destination": cells[3] == "✅",
            "cdc": cells[4] == "✅",
        }
    return out


def readme_table(text: str) -> list[tuple[str, list[str], list[str]]]:
    """The README's category table as (category, connector names, cdc names).

    Anchored on the ``## Connectors`` heading so the other README tables --
    "What you get", "Documentation" -- cannot be mistaken for it.
    """
    rows: list[tuple[str, list[str], list[str]]] = []
    inside = False
    for line in text.splitlines():
        if line.startswith("## "):
            inside = line.strip() == "## Connectors"
            continue
        if not inside:
            continue
        cells = _cells(line)
        if len(cells) != 3 or _is_separator(cells) or cells[0] == "Category":
            continue
        category = cells[0].strip("*")
        names = [_strip_gloss(n) for n in cells[1].split(",")]
        cdc = [] if cells[2] == "—" else [_strip_gloss(n) for n in cells[2].split(",")]
        rows.append((category, names, cdc))
    return rows


def _strip_gloss(name: str) -> str:
    """"Sample Data (credential-free demo source)" -> "Sample Data"."""
    return re.sub(r"\s*\(.*?\)\s*$", "", name.strip()).strip()


def _resolve(name: str, catalogue_names: list[str]) -> list[str]:
    """Catalogue rows a README cell could name. Exact, or whole-word prefix."""
    low = name.casefold()
    return [
        c
        for c in catalogue_names
        if c.casefold() == low or c.casefold().startswith(low + " ")
    ]


# --------------------------------------------------------------------------
# The extractors, armed against known answers before anything trusts them.
# --------------------------------------------------------------------------

_FIXTURE_REFERENCE = """\
## Catalogue

### Connectors (2)

| Connector | ID | Source | Destination | CDC | Category |
|---|---|:--:|:--:|:--:|---|
| Widget DB | `database/widget` | ✅ | ✅ | ✅ | Relational database |
| Gadget API | `gadget` | ✅ | — | — | SaaS API |

### Internal components (1)

| Component | ID | Source | Destination | CDC | Category |
|---|---|:--:|:--:|:--:|---|
| Plumbing | `plumbing` | — | ✅ | — | Object storage |
"""

_FIXTURE_README = """\
## What you get

| | |
|---|---|
| **Decoy** | a two-column table that must not be read as the connector table |

## Connectors

**2 connectors ship in the box**

| Category | Connectors | CDC |
|---|---|---|
| **Relational** | Widget DB | Widget DB |
| **APIs** | Gadget API (an example) | — |

## The Data Explorer
"""


def test_the_catalogue_extractor_is_not_vacuous():
    got = catalogue(_FIXTURE_REFERENCE)
    assert got == {
        "Widget DB": {"source": True, "destination": True, "cdc": True},
        "Gadget API": {"source": True, "destination": False, "cdc": False},
    }, "the internal-components table must stay out, and flags must be read per column"


def test_the_readme_extractor_is_not_vacuous():
    got = readme_table(_FIXTURE_README)
    assert got == [
        ("Relational", ["Widget DB"], ["Widget DB"]),
        ("APIs", ["Gadget API"], []),
    ], "only the ## Connectors table, with parenthetical glosses stripped"


def test_the_resolver_refuses_an_ambiguous_prefix():
    # The safety property the loose match leans on: "SQL" is a whole-word prefix
    # of two rows, so it resolves to two candidates and the bijection below
    # rejects it. If this ever returns one, the name check has gone soft.
    assert len(_resolve("SQL", ["SQL Server", "SQL Warehouse", "MySQL"])) == 2
    assert _resolve("Oracle", ["Oracle Database", "PostgreSQL"]) == ["Oracle Database"]


# --------------------------------------------------------------------------
# The real subjects.
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def shipped() -> dict[str, dict[str, bool]]:
    got = catalogue(REFERENCE.read_text(encoding="utf-8"))
    assert len(got) >= _MIN_CONNECTORS, (
        f"only {len(got)} connector(s) parsed out of {REFERENCE.relative_to(REPO)} -- "
        "the generated table's shape changed and every check below is vacuous"
    )
    return got


@pytest.fixture(scope="module")
def advertised() -> list[tuple[str, list[str], list[str]]]:
    rows = readme_table(README.read_text(encoding="utf-8"))
    names = [n for _, ns, _ in rows for n in ns]
    assert len(names) >= _MIN_README_NAMES, (
        f"only {len(names)} connector name(s) parsed out of README.md's "
        "## Connectors table -- the check below would pass on nothing"
    )
    return rows


def test_the_readme_names_exactly_the_shipped_connectors(shipped, advertised):
    catalogue_names = list(shipped)
    advertised_names = [n for _, ns, _ in advertised for n in ns]

    assert len(advertised_names) == len(set(advertised_names)), (
        "README lists a connector twice: "
        f"{sorted({n for n in advertised_names if advertised_names.count(n) > 1})}"
    )

    resolved: dict[str, str] = {}
    unmatched, ambiguous = [], []
    for name in advertised_names:
        hits = _resolve(name, catalogue_names)
        if not hits:
            unmatched.append(name)
        elif len(hits) > 1:
            ambiguous.append((name, hits))
        else:
            resolved[name] = hits[0]

    assert not unmatched, (
        f"README's ## Connectors table names {unmatched}, which the generated "
        f"catalogue in {REFERENCE.relative_to(REPO)} does not ship"
    )
    assert not ambiguous, f"README name resolves to several catalogue rows: {ambiguous}"

    missing = set(catalogue_names) - set(resolved.values())
    assert not missing, (
        f"{sorted(missing)} ship(s) but the README's category table never names "
        "it -- a reader would not know it exists"
    )


def test_the_readme_counts_match_the_catalogue(shipped, advertised):
    text = README.read_text(encoding="utf-8")

    total = len(shipped)
    destinations = sum(1 for f in shipped.values() if f["destination"])
    cdc = sum(1 for f in shipped.values() if f["cdc"])
    sources = sum(1 for f in shipped.values() if f["source"])

    badge = re.search(r"!\[Connectors\]\(https://img\.shields\.io/badge/connectors-(\d+)-", text)
    assert badge, "README lost its connectors badge, or the badge URL shape changed"
    assert int(badge.group(1)) == total, (
        f"badge says {badge.group(1)} connectors, catalogue ships {total}"
    )

    assert f"**{total} connectors ship in the box**" in text, (
        f"README's prose count disagrees with the catalogue's {total}"
    )
    assert sources == total, (
        f"README claims every connector is a source; {total - sources} are not"
    )
    assert f"{destinations} are also destinations" in text, (
        f"README's destination count disagrees with the catalogue's {destinations}"
    )

    # Spelled out in the prose, so compare against the word rather than the digit.
    words = {5: "five", 6: "six", 7: "seven", 8: "eight", 9: "nine", 10: "ten"}
    assert cdc in words, f"{cdc} CDC connectors -- add the number word and update the prose"
    assert f"{words[cdc]} support change data capture" in text, (
        f"README's CDC count disagrees with the catalogue's {cdc}"
    )


def test_the_readme_cdc_column_names_exactly_the_cdc_connectors(shipped, advertised):
    catalogue_cdc = {n for n, f in shipped.items() if f["cdc"]}
    claimed = {
        _resolve(n, list(shipped))[0]
        for _, _, cdc_names in advertised
        for n in cdc_names
        if _resolve(n, list(shipped))
    }
    assert claimed == catalogue_cdc, (
        "README's CDC column and the catalogue's CDC flag disagree -- "
        f"README-only: {sorted(claimed - catalogue_cdc)}, "
        f"catalogue-only: {sorted(catalogue_cdc - claimed)}"
    )
