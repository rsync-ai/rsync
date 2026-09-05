#!/usr/bin/env python3
"""
Test suite for auto-replan (beta) error_context threading in the planner.

Covers the llm-service half of the auto-replan feature: a V2 Temporal workflow
re-invokes the planner after a deterministic executor failure, passing a sanitized
failure snapshot as ``error_context``. The planner folds a corrective instruction
into the LLM request so it produces a fixed plan instead of regenerating the
failing one.

Key invariant: when ``error_context`` is absent (the normal first pass), the
request text is byte-identical to ``natural_language`` — the existing path is
unchanged.

Run with: python -m pytest test_auto_replan.py -v
Or directly: python test_auto_replan.py
"""

import unittest
import os
import sys

# Add parent directory to path (matches the other planner tests)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from src.agents.planner.strategies import (
    PlanningContext,
    build_replan_user_request,
)


class TestBuildReplanUserRequest(unittest.TestCase):
    """Unit tests for the pure prompt-augmentation helper."""

    BASE = "Sync orders from Postgres to Snowflake"

    def test_none_error_context_returns_request_unchanged(self):
        """No error_context (normal first pass) -> byte-identical request."""
        self.assertEqual(build_replan_user_request(self.BASE, None), self.BASE)

    def test_empty_dict_error_context_returns_request_unchanged(self):
        """Empty error_context is falsy -> request unchanged (no spurious replan text)."""
        self.assertEqual(build_replan_user_request(self.BASE, {}), self.BASE)

    def test_reason_is_threaded_into_request(self):
        """A failure reason is surfaced in the corrective instruction."""
        out = build_replan_user_request(
            self.BASE, {"reason": "table public.orders does not exist", "stage": "extract"}
        )
        self.assertIn(self.BASE, out)
        self.assertIn("table public.orders does not exist", out)
        self.assertIn("extract", out)
        self.assertIn("CORRECTED plan", out)
        # The original request is preserved as a prefix.
        self.assertTrue(out.startswith(self.BASE))

    def test_policy_code_used_when_reason_absent(self):
        """Falls back to policy_code when no human-readable reason is present."""
        out = build_replan_user_request(self.BASE, {"policy_code": "DEST_WRITE_DENIED"})
        self.assertIn("DEST_WRITE_DENIED", out)

    def test_default_reason_and_stage_when_both_absent(self):
        """Degrades gracefully when neither reason nor policy_code is present."""
        out = build_replan_user_request(self.BASE, {"foo": "bar"})
        self.assertIn("unknown execution error", out)
        self.assertIn("execution", out)  # default stage

    def test_reason_preferred_over_policy_code(self):
        """When both present, the human-readable reason wins."""
        out = build_replan_user_request(
            self.BASE, {"reason": "human readable", "policy_code": "CODE_X"}
        )
        self.assertIn("human readable", out)
        self.assertNotIn("CODE_X", out)


class TestPlanningContextErrorContext(unittest.TestCase):
    """The PlanningContext dataclass carries error_context, defaulting to None."""

    def test_default_is_none(self):
        ctx = PlanningContext(natural_language="x")
        self.assertIsNone(ctx.error_context)

    def test_accepts_error_context(self):
        ec = {"reason": "boom", "stage": "load"}
        ctx = PlanningContext(natural_language="x", error_context=ec)
        self.assertEqual(ctx.error_context, ec)


if __name__ == "__main__":
    unittest.main()
