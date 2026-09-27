"""The planner service is HTTP only.

It used to also start a Kafka request loop on agent.planner.requests /
agent.planner.responses (planner/kafka_consumer.py) whenever
ENABLE_KAFKA_CONSUMER was unset or "true". Every deployment set the flag to
"false", so nothing produced to the request topic; the loop, its module and its
topics were removed before v0.1.6. These tests pin both halves of that: the
HTTP planner path is still served, and starting the service no longer reaches
for a Kafka consumer, whatever the environment says.
"""

import builtins
import sys
from pathlib import Path

import pytest

LLM_SERVICE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LLM_SERVICE))

# A plain import, not importorskip: if the service stops importing, that is the
# failure this file exists to report, not a reason to skip it.
import src.agents.planner.service as service  # noqa: E402


def test_the_http_planner_routes_are_still_served():
    paths = {getattr(r, "path", None) for r in service.app.routes}
    for route in ("/health", "/plan", "/generate-cdc-config", "/strategy/recommend", "/connectors/ensure"):
        assert route in paths, f"planner HTTP route {route} is gone"


def test_health_answers_over_http():
    from fastapi.testclient import TestClient

    resp = TestClient(service.app).get("/health")
    assert resp.status_code == 200, resp.text
    assert resp.json()["service"] == "planner"


def test_the_kafka_consumer_module_is_gone():
    assert not (LLM_SERVICE / "src" / "agents" / "planner" / "kafka_consumer.py").exists()


@pytest.mark.parametrize("flag", [None, "true"])
def test_main_starts_http_and_never_a_kafka_consumer(monkeypatch, flag):
    """The old main() imported ``kafka_consumer`` whenever this flag was unset or
    "true" (its default), so this fails against it. Both settings are pinned: a
    leftover ENABLE_KAFKA_CONSUMER=true in someone's environment must not bring
    the loop back."""
    if flag is None:
        monkeypatch.delenv("ENABLE_KAFKA_CONSUMER", raising=False)
    else:
        monkeypatch.setenv("ENABLE_KAFKA_CONSUMER", flag)

    runs = []
    monkeypatch.setattr(service.uvicorn, "run", lambda app, **kw: runs.append((app, kw)))

    imported = []
    real_import = builtins.__import__

    def recording_import(name, *args, **kwargs):
        imported.append(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", recording_import)
    service.main()
    monkeypatch.setattr(builtins, "__import__", real_import)

    assert len(runs) == 1 and runs[0][0] is service.app, runs
    kafka_imports = [n for n in imported if "kafka" in n.lower()]
    assert not kafka_imports, f"main() reached for Kafka: {kafka_imports}"
