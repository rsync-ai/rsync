"""Namespace filter contract (issue #31).

``public/namespace_filter.py`` and ``backend-orchestrator/pkg/namespacefilter``
must behave identically. Both suites load ``public/namespace_filter_vectors.json``
and assert it is non-empty, so neither can pass on zero cases.

Offline, stdlib only.
"""
from __future__ import annotations

import json
import os
import sys

import pytest

_PUBLIC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "public"))
sys.path.insert(0, _PUBLIC)
import namespace_filter as nf  # noqa: E402

_VECTORS_PATH = os.path.join(_PUBLIC, "namespace_filter_vectors.json")


def _load_cases():
    with open(_VECTORS_PATH, encoding="utf-8") as fh:
        return json.load(fh)["cases"]


_CASES = _load_cases()


def test_vectors_are_not_empty():
    ok = [c for c in _CASES if not c.get("expect_error")]
    bad = [c for c in _CASES if c.get("expect_error")]
    print(f"namespace filter vectors: {len(_CASES)} ({len(ok)} apply, {len(bad)} error)")
    assert len(_CASES) > 0
    assert len(ok) > 0
    assert len(bad) > 0


@pytest.mark.parametrize("case", _CASES, ids=[c["name"] for c in _CASES])
def test_vectors(case):
    if case.get("expect_error"):
        with pytest.raises(nf.NamespaceFilterError):
            nf.parse(case["config"])
        return
    f = nf.parse(case["config"])
    assert f.mode == case["expect_mode"]
    assert list(f.patterns) == case["expect_patterns"]
    result = nf.apply(case["candidates"], f, case["system_names"])
    assert result.kept == case["expect_kept"]
    assert result.matched == len(case["expect_kept"])
    assert result.excluded == len(case["candidates"]) - len(case["expect_kept"])
    assert bool(result.warning) == case["expect_warning"]


def test_unknown_mode_fails_closed():
    with pytest.raises(nf.NamespaceFilterError):
        nf.parse({"namespace_filter_mode": "only", "namespace_filter_patterns": "sales"})


def test_include_with_no_patterns_fails_closed():
    with pytest.raises(nf.NamespaceFilterError):
        nf.parse({"namespace_filter_mode": "include", "namespace_filter_patterns": ""})


def test_regex_metacharacters_are_literal():
    f = nf.parse({"namespace_filter_mode": "include", "namespace_filter_patterns": "a.b,x*"})
    assert nf.allowed("a.b", f)
    assert not nf.allowed("aXb", f)
    assert nf.allowed("x.y", f)


def test_non_string_values_fail_closed():
    with pytest.raises(nf.NamespaceFilterError):
        nf.parse({"namespace_filter_mode": "include", "namespace_filter_patterns": ["sales"]})
    with pytest.raises(nf.NamespaceFilterError):
        nf.parse({"namespace_filter_mode": 1})


def test_messages_carry_no_config_values():
    for config in (
        {"namespace_filter_mode": "secret_mode_value"},
        {"namespace_filter_mode": "include", "namespace_filter_patterns": "secret_pattern_value" * 10},
    ):
        with pytest.raises(nf.NamespaceFilterError) as exc:
            nf.parse(config)
        assert "secret" not in str(exc.value)


def test_many_star_pattern_matches_in_linear_time():
    # A backtracking regex takes seconds at 5 stars and effectively forever at
    # 60 on this input; the glob matcher must stay well under a second.
    import time

    pattern = "*" + "a*" * 60 + "b"
    assert len(pattern.encode("utf-8")) <= nf.MAX_PATTERN_BYTES
    f = nf.parse({"namespace_filter_mode": "include", "namespace_filter_patterns": pattern})
    start = time.monotonic()
    for _ in range(10):
        assert not nf.allowed("a" * 63, f)
    assert time.monotonic() - start < 1.0


def test_lone_surrogate_pattern_does_not_raise_unicode_error():
    f = nf.parse({"namespace_filter_mode": "include", "namespace_filter_patterns": "db\ud800"})
    assert not nf.allowed("db", f)
