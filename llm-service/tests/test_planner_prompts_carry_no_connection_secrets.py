"""The planner prompts may name a connection; they may not describe it.

CLAUDE.md's LLM privacy rule: prompts may carry schema/connector metadata and
user text, NEVER credentials, row values or PII. The DAG planner dumped the
whole connection record -- ``json.dumps(state.available_connections)`` -- into
both of its prompt phases, while its sibling ``LLMPlanningStrategy`` had always
whitelisted three fields. Both now go through ``connections_for_llm``.

The assertions are on the prompt variables the planners actually build, not on
the helper in isolation: a test of the helper alone stays green if a call site
goes back to dumping the record. Verified by mutation -- restoring
``json.dumps(state.available_connections)`` at either call site turns the
corresponding test RED on the ``password`` assertion.

Every fixture value here is synthetic.
"""

import json

from src.utils.masking import connections_for_llm


# A record shaped like what /api/v1/connections returns: the gateway masks known
# secret KEY NAMES, so `password` arrives masked -- and `db_pass` (a name nobody
# added to that denylist) arrives in cleartext. That asymmetry is the reason the
# prompt side is an allowlist.
CONNECTION_FIXTURE = {
    "id": "conn-1111",
    "name": "Prod Postgres",
    "connector_type": "postgresql",
    "type": "source",
    "config": {
        "host": "db.internal.customer.example",
        "port": 5432,
        "database": "customers",
        "user": "rsync_reader",
        "password": "••••••••",
        "db_pass": "hunter2-not-in-the-denylist",
    },
    "last_test_error": "FATAL: password authentication failed for user "
                       "\"rsync_reader\" at 10.4.2.19",
    "status": "active",
}

LEAKED_STRINGS = (
    "db.internal.customer.example",
    "rsync_reader",
    "hunter2-not-in-the-denylist",
    "10.4.2.19",
    "password authentication failed",
)


class _CapturingLLMClient:
    """Records the prompt variables and answers with something parseable."""

    def __init__(self, response):
        self.response = response
        self.calls = []

    def complete(self, prompt_name, variables, **kwargs):
        self.calls.append((prompt_name, variables))
        return {"content": self.response}


def _assert_no_leak(rendered: str):
    for needle in LEAKED_STRINGS:
        assert needle not in rendered, f"connection detail {needle!r} reached the prompt"


def test_connections_for_llm_keeps_only_the_three_naming_fields():
    out = connections_for_llm([CONNECTION_FIXTURE])
    assert out == [{"id": "conn-1111", "name": "Prod Postgres", "connector_type": "postgresql"}]
    _assert_no_leak(json.dumps(out))


def test_connections_for_llm_drops_non_dict_entries_and_empty_input():
    assert connections_for_llm(None) == []
    assert connections_for_llm([]) == []
    assert connections_for_llm(["conn-1111", None, 42]) == []


def test_dag_planner_analyze_phase_sends_only_metadata():
    from src.agents.dag_planner.agent import DAGPlannerAgent, PlannerState

    client = _CapturingLLMClient("analysis text")
    agent = DAGPlannerAgent(client)
    state = PlannerState(
        user_request="sync postgres to s3",
        pipeline_id="pipe-test",
        available_connections=[CONNECTION_FIXTURE],
        available_tools=["postgresql", "aws-s3"],
    )

    agent._analyze_request(state)

    assert len(client.calls) == 1, "analyze phase did not call the LLM"
    _, variables = client.calls[0]
    _assert_no_leak(variables["available_connections"])
    # Still useful: the planner has to be able to name the connection.
    assert "conn-1111" in variables["available_connections"]
    assert "postgresql" in variables["available_connections"]


def test_dag_planner_generate_phase_sends_only_metadata():
    from src.agents.dag_planner.agent import DAGPlannerAgent, PlannerState

    client = _CapturingLLMClient(json.dumps({"nodes": [], "edges": []}))
    agent = DAGPlannerAgent(client)
    state = PlannerState(
        user_request="sync postgres to s3",
        pipeline_id="pipe-test",
        available_connections=[CONNECTION_FIXTURE],
        available_tools=["postgresql", "aws-s3"],
    )

    agent._generate_dag(state)

    assert len(client.calls) == 1, "generate phase did not call the LLM"
    _, variables = client.calls[0]
    assert variables["phase"] == "generate"
    _assert_no_leak(variables["available_connections"])
    assert "conn-1111" in variables["available_connections"]
