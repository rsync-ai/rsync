"""Topic naming for every Kafka name this service produces, consumes or writes
into a connector config.

Consumer group ids go through the same contract (see :func:`group`): topics and
groups are granted together in one ACL set, so they share one prefix.

This is the Python half of a two-language contract. The Go half lives in
``shared/go/kafkaclient/topics.go`` (groups: ``groups.go``) and the two MUST
agree byte-for-byte: the planner names a topic here, the orchestrator creates it
there, and the sink consumes it there. A divergence does not raise -- the
consumer simply subscribes to a topic nobody writes and blocks forever. Change
one, change the other.

Why a prefix at all: the platform grew up owning its broker and named topics
after their contents (``agent.planner.requests``, ``pipeline.<id8>.data``,
``cdc.<id8>``). On a customer's shared cluster -- the BYO-Kafka shape the
managed-Kubernetes deployment targets -- those are anonymous, and generic enough
to collide with another team's topics outright. One owned prefix makes every
topic this product creates greppable by the operator who has to live with them.
"""

import os
import re

ENV_TOPIC_PREFIX = "KAFKA_TOPIC_PREFIX"
DEFAULT_TOPIC_PREFIX = "rsync."

# Kafka's own topic charset. An operator-supplied prefix carrying anything else
# would make every derived topic illegal at once, and the broker's error names
# the derived topic rather than the prefix behind it.
_ILLEGAL = re.compile(r"[^a-zA-Z0-9._-]")


def topic_prefix() -> str:
    """Resolve the configured namespace, normalized so it concatenates safely.

    Setting the variable to the empty string disables qualification. That is the
    migration lever, not an end state: an existing deployment has live topics and
    committed consumer-group offsets under the unprefixed names, and renaming
    those is a coordinated, all-services-at-once deploy.
    """
    raw = os.getenv(ENV_TOPIC_PREFIX)
    if raw is None:
        return DEFAULT_TOPIC_PREFIX

    prefix = _ILLEGAL.sub("", raw.strip())
    if not prefix:
        return ""
    # Without a trailing separator the prefix runs into the name it qualifies
    # ("rsync" + "agent.x" = "rsyncagent.x"), which is a legal topic name and so
    # fails silently rather than loudly.
    if prefix[-1] not in "._-":
        prefix += "."
    return prefix


def topic(name: str) -> str:
    """Qualify a logical topic name with the deployment's namespace.

    Idempotent. Topic names are persisted (``pipelines.kafka_topic``, connector
    configs) and read back on the next run, so an already-qualified name arriving
    here a second time must not become ``rsync.rsync.cdc.abc12345``.
    """
    name = (name or "").strip()
    prefix = topic_prefix()
    if not prefix or not name or name.startswith(prefix):
        return name
    return prefix + name


def topics(*names: str) -> list:
    """Qualify a list of topic names, for consumers that subscribe to a fixed set."""
    return [topic(n) for n in names]


def group(name: str) -> str:
    """Qualify a consumer group id with the deployment's namespace.

    The Python half of ``shared/go/kafkaclient/groups.go``. Like the Go
    original this delegates to :func:`topic` rather than copying its
    computation: a group prefix that drifted from the topic prefix would mean an
    operator granting ``rsync.*`` on topics and something else on groups, and
    the join then fails with an authorization error naming neither variable.

    Why groups need this at all: on a customer-managed cluster the operator
    writes ACLs. If topics and groups sit under one prefix that is a single
    PREFIXED grant. If half the consumers build bare group ids, the operator
    grants ``rsync.`` and those consumers stall -- Kafka answers the join with
    an authorization failure that surfaces as a consumer which simply never
    receives anything, not as a crash.

    DECISION -- caller/operator-supplied group ids are qualified too.
    ``AvroKafkaConsumer(group_id=...)`` here, and ``KAFKA_GROUP_ID`` on the Go
    side, let a deployment name its own group; those names are routed through
    this function as well, so ``my-group`` becomes ``rsync.my-group``. The
    argument against is that an operator who explicitly set the value may not
    expect it to be modified. It loses to three things:

      * an unqualified escape hatch defeats the point -- the operator can no
        longer grant one PREFIXED rule and be done, and the group they forgot
        to grant separately fails in the silent way described above;
      * they keep the lever, and it is the coherent one: ``KAFKA_TOPIC_PREFIX``
        empty disables qualification for topics AND groups together. Disabling
        it for groups alone is exactly the topic/group drift this function
        exists to prevent;
      * qualification is idempotent, so an operator who does want to spell the
        namespace themselves can set ``rsync.my-group`` and get it back
        verbatim.

    Idempotent for the same reason topics are: group ids are persisted and read
    back on the next run, and a consumer rejoining as ``rsync.rsync.cdc-sink``
    is a *different* group, so it abandons its committed offsets and re-reads
    from ``auto_offset_reset``.
    """
    # Deliberately the same computation, not a parallel copy of it.
    return topic(name)


def in_namespace(name: str, namespace: str) -> bool:
    """Report whether ``name`` belongs to ``namespace``, prefixed or not.

    A deployment that adopts the prefix still has live topics minted under the
    bare names, so a guard that recognized only the qualified form would quietly
    misclassify every one of them.
    """
    name = (name or "").strip()
    if not name or not namespace:
        return False
    return name.startswith(namespace) or name.startswith(topic(namespace))
