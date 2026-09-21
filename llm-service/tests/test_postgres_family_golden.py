"""Pin CDCConfigGenerator.POSTGRES_FAMILY to shared/postgres_family_golden.json.

The orchestrator's ``postgresFamilyTypes`` (executor/hybrid_cdc.go) is pinned to the
same file by ``postgres_family_golden_test.go``. The executor creates the publication
before the slot for a family member; this set is what forces Debezium's
``publication.autocreate.mode`` to ``"disabled"``. A derivative in one list and not
the other keeps one guard and loses the second, and CDC drops rows silently.
"""

import json
import os

from src.agents.planner.cdc_config_generator import CDCConfigGenerator

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_GOLDEN_PATH = os.path.join(REPO_ROOT, "shared", "postgres_family_golden.json")


def _golden() -> dict:
    with open(_GOLDEN_PATH) as f:
        return json.load(f)


def test_postgres_family_is_the_shared_members_plus_aliases():
    golden = _golden()
    assert golden["members"], "golden members is empty"
    # This side lower-cases and swaps '-' for '_' but maps no aliases, so every
    # alias the orchestrator's normaliser folds onto a member must be listed raw.
    want = set(golden["members"]) | set(golden["aliases"])
    assert CDCConfigGenerator.POSTGRES_FAMILY == want, (
        f"POSTGRES_FAMILY drifted from shared/postgres_family_golden.json: "
        f"missing {sorted(want - CDCConfigGenerator.POSTGRES_FAMILY)}, "
        f"extra {sorted(CDCConfigGenerator.POSTGRES_FAMILY - want)}"
    )


def test_normalised_raw_names_classify_like_the_orchestrator():
    golden = _golden()
    assert golden["raw_members"] and golden["non_members"], "golden fixture has an empty group"
    family = CDCConfigGenerator.POSTGRES_FAMILY
    # Same normalisation generate_config applies before the family check.
    norm = lambda s: s.lower().replace("-", "_")  # noqa: E731
    assert [s for s in golden["raw_members"] if norm(s) not in family] == []
    assert [s for s in golden["non_members"] if norm(s) in family] == []
