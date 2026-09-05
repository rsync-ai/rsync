"""The planner must not tell anyone a Kafka topic will be auto-created.

`auto.create.topics.enable` is a BROKER setting. On a customer-managed cluster --
the deployment shape this platform now targets -- it is not ours to set, and on
several managed offerings it is off. A pipeline that depends on it does not fail
loudly: the producer's first send is rejected, or the sink subscribes to a topic
that does not exist, gets zero partitions assigned, and consumes nothing forever
while the pipeline reports running.

The planner's disabled branch used to state that dependency as the design --
"Topic will be auto-created by Kafka" -- and that sentence is why the gap read as
intentional for as long as it did. Every topic this platform produces to is now
created explicitly through the orchestrator's TopologyManager before the produce.
This test fails if the promise ever comes back, in any branch, because a comment
saying "don't rely on auto-create" is not enforceable and this is.
"""

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.agents.planner.strategies import TopicProvisioner  # noqa: E402

# Matched case-insensitively against every reason a provisioning call can return.
FORBIDDEN = ("auto-created", "auto created", "autocreate", "auto-create")


def _assert_no_promise(reason: str, context: str) -> None:
    lowered = (reason or "").lower()
    for phrase in FORBIDDEN:
        assert phrase not in lowered, (
            f"{context}: the planner reports {reason!r}, which tells the reader Kafka "
            f"will create this topic. It is a broker setting this platform does not "
            f"own on a customer-managed cluster, and when it is off the pipeline "
            f"reports running and moves zero rows. The executor pre-creates the topic; "
            f"say that instead."
        )


@pytest.fixture
def _flag_off(monkeypatch):
    monkeypatch.delenv("ENABLE_TOPIC_PROVISIONING", raising=False)


def test_disabled_branch_does_not_promise_auto_creation(_flag_off):
    """The default path on every deployment: the flag is set in no compose file."""
    provisioner = TopicProvisioner()
    assert provisioner.enabled is False, "the flag must stay off by default"

    result = provisioner.provision_topic(
        pipeline_id="abc12345-0000-0000-0000-000000000000",
        sync_mode="batch",
        tables=["public.orders"],
        estimated_size_gb=1.0,
    )
    _assert_no_promise(result.reason, "ENABLE_TOPIC_PROVISIONING unset")


def test_failure_branch_does_not_promise_auto_creation(monkeypatch):
    """The flag ON and the topology API unreachable -- the other place it was said."""
    monkeypatch.setenv("ENABLE_TOPIC_PROVISIONING", "true")
    provisioner = TopicProvisioner()
    assert provisioner.enabled is True

    def _boom(*_args, **_kwargs):
        raise ConnectionError("orchestrator unreachable")

    monkeypatch.setattr(provisioner, "_create_topic_via_topology_agent", _boom)

    result = provisioner.provision_topic(
        pipeline_id="abc12345-0000-0000-0000-000000000000",
        sync_mode="batch",
        tables=["public.orders"],
        estimated_size_gb=1.0,
    )
    assert result.success is False
    _assert_no_promise(result.reason, "topology API unreachable")


def test_the_flag_still_defaults_off():
    """Plan-time pre-creation stays opt-in.

    Not cosmetic. Turning it on fires a synchronous HTTP call on every plan, and for
    CDC it mints "cdc.<connection-name>" -- a topic nothing produces to, because the
    real CDC streams are Debezium's "<topic.prefix>.<db>.<table>". The flag survives
    the removal of the auto-create dependency because it never controlled it.
    """
    os.environ.pop("ENABLE_TOPIC_PROVISIONING", None)
    assert TopicProvisioner().enabled is False
