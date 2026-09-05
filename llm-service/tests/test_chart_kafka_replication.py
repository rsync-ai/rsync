"""One cluster fact, derived three times, three different answers.

For a BYO cluster with `kafka.replicationFactor` unset, the chart used to answer
the question "how many replicas?" in three independent places and disagree with
itself in every one of them:

  orchestrator    `kafka/replication.go forCluster` -- 1 if <=1 broker, else
                  min(3, brokers), then clamped by `topology.go clampToCluster`.
                  Correct, because it is the only component holding a live,
                  authenticated broker view.
  kafka-connect   `connectors/cdc.yaml` -- `ternary "1" "3"`, so **3**.
  kafka-init      `jobs/kafka-init.yaml` -- shell `${VAR:-1}`, so **1**.

Same chart, same cluster, same install. The 3 is the fatal one: Kafka Connect asks
for three replicas for its own internal topics on a one-broker cluster and
crash-loops. What it does NOT say is why --

    org.apache.kafka.common.errors.TimeoutException:
        Timeout expired while trying to create topic(s)

not InvalidReplicationFactorException, and across 800 lines of worker log the
strings "available brokers", "broker count" and "INVALID_REPLICATION" appear zero
times. The only cause-bearing line is a startup config echo four minutes earlier.

Two things make this survive ordinary testing:

  * Connect creates those topics only if absent, so anyone who ever installed the
    chart at RF=1 never sees it again. Only a FRESH cluster fails -- the same
    shape as the `.env*.example` defect that kept prod, staging and CI green.
  * kafka-init cannot rescue it. That Job is a `post-install` hook and `--wait`
    holds hooks until pods are Ready, so while Connect crash-loops the Job does
    not exist (`kubectl get jobs` -> "No resources found").

So the value has to be right when it is RENDERED, and there must be exactly one
thing that decides it. That is `rsync-ai.kafka.replicationFactor`, and for a BYO
cluster with nothing set it fails closed rather than guessing -- a render-time
error naming the broker count is strictly better than a runtime crash-loop that
names nothing.

`min.insync.replicas` is the same class of trap in the other direction: unset does
not mean "no floor", it means the invisible broker default, which is 2 on MSK. An
RF=1 topic inheriting misr=2 is created successfully and is then permanently
unwritable. kafka-init pins min(2, RF) rather than inheriting.

Static and cheap on purpose -- this parses `deploy/helm` as text and needs no
cluster and no `helm` binary, matching test_chart_kafka_security_env.py.
"""

import pathlib
import re

import pytest

CHART = pathlib.Path(__file__).resolve().parents[2] / "deploy" / "helm" / "rsync-ai"
TEMPLATES = CHART / "templates"
HELPERS = TEMPLATES / "_helpers.tpl"
VALUES = CHART / "values.yaml"

HELPER_NAME = "rsync-ai.kafka.replicationFactor"

# The two templates allowed to touch `.Values.kafka.replicationFactor` directly.
# Everything else must go through the helper, or it is a second derivation again.
#   _helpers.tpl   defines the helper, and passes the RAW value through kafkaEnv --
#                  deliberately empty when unset, so the Go services defer to
#                  `forCluster`, which unlike the chart can see the cluster.
#   validate.yaml  guards the user's own input (misr <= rf, and rf > 1 against the
#                  single-node in-chart broker). It reads the value; it never
#                  derives one.
RAW_VALUE_ALLOWED = {"_helpers.tpl", "validate.yaml"}

# The two templates that must consume the helper. Pinned by name, not discovered:
# a discovered census cannot tell "no template derives this" from "the template
# was deleted", and deletion is exactly how this defect would come back.
HELPER_CONSUMERS = {
    "connectors/cdc.yaml": "Kafka Connect's three internal topics",
    "jobs/kafka-init.yaml": "the platform's own topics",
}

_HELM_COMMENT = re.compile(r"\{\{-?/\*.*?\*/-?\}\}", re.DOTALL)


def strip_helm_comments(text: str) -> str:
    """Drop `{{/* ... */}}` blocks.

    Needed because the fix deliberately quotes the code it replaced, inside a
    comment, so the next reader sees what the wrong version looked like. A grep
    that cannot tell code from commentary would flag that quote forever, and the
    obvious "fix" is to delete the explanation.
    """
    return _HELM_COMMENT.sub("", text)


def templates():
    """Every template file, as (relative-posix-path, comment-stripped text)."""
    out = []
    for p in sorted(TEMPLATES.rglob("*")):
        if p.is_file() and p.suffix in (".yaml", ".yml", ".tpl"):
            out.append((p.relative_to(TEMPLATES).as_posix(), strip_helm_comments(p.read_text())))
    return out


def helper_body() -> str:
    text = HELPERS.read_text()
    start = text.index(f'{{{{- define "{HELPER_NAME}" -}}}}')
    nxt = text.find("{{- define ", start + 1)
    return text[start : nxt if nxt != -1 else len(text)]


def kafka_init_script() -> str:
    return (TEMPLATES / "jobs" / "kafka-init.yaml").read_text()


# --------------------------------------------------------------------------
# vacuity floor -- run these first; every assertion below is worthless if the
# parser is reading nothing, and "reading nothing" passes silently.
# --------------------------------------------------------------------------


def test_the_parser_actually_reads_the_chart():
    names = [n for n, _ in templates()]
    assert len(names) > 15, f"only {len(names)} templates found -- rglob is not seeing the chart"
    for required in ("_helpers.tpl", "validate.yaml", *HELPER_CONSUMERS):
        assert required in names, f"{required} missing from the census"


def test_comment_stripper_removes_commentary_and_keeps_code():
    sample = 'a\n{{/*\n{{ .Values.kafka.replicationFactor }}\n*/}}\nb {{ .Values.kafka.enabled }}\n'
    stripped = strip_helm_comments(sample)
    assert ".Values.kafka.replicationFactor" not in stripped, "stripper left commentary behind"
    assert ".Values.kafka.enabled" in stripped, "stripper ate live code"
    assert "a" in stripped and "b" in stripped

    # ...and it must not over-strip the real files, which is the failure mode that
    # would make every test below pass on an empty string.
    for name, text in templates():
        if name in HELPER_CONSUMERS:
            assert f'include "{HELPER_NAME}"' in text, f"{name}: stripper ate the live include"
    assert f'define "{HELPER_NAME}"' in strip_helm_comments(HELPERS.read_text())


# --------------------------------------------------------------------------
# one derivation
# --------------------------------------------------------------------------


def test_replication_factor_is_derived_in_exactly_one_place():
    offenders = [
        name
        for name, text in templates()
        if ".Values.kafka.replicationFactor" in text and name not in RAW_VALUE_ALLOWED
    ]
    assert not offenders, (
        f"{offenders} read kafka.replicationFactor directly instead of "
        f'`include "{HELPER_NAME}"`. Two components deriving one cluster fact is '
        "the defect this file exists for: cdc.yaml said 3 and kafka-init said 1 "
        "for the same one-broker cluster, and the 3 crash-looped Connect."
    )


def test_no_template_guesses_a_replication_factor_with_ternary():
    """`ternary "1" "3" .Values.kafka.enabled` is the exact shape that shipped.

    Named separately from the test above so that re-introducing it in an ALLOWED
    file -- _helpers.tpl or validate.yaml -- is still caught.
    """
    for name, text in templates():
        for line in text.splitlines():
            if "replicationFactor" in line and "ternary" in line:
                pytest.fail(
                    f"{name}: {line.strip()!r} guesses a replication factor. "
                    "The chart cannot see the cluster; it must not guess."
                )


def test_both_consumers_go_through_the_shared_helper():
    for name, why in HELPER_CONSUMERS.items():
        text = strip_helm_comments((TEMPLATES / name).read_text())
        assert f'include "{HELPER_NAME}"' in text, (
            f"{name} sets the replication factor for {why} and must take it from "
            f"the shared helper -- these two disagreed for one cluster in one install."
        )


# --------------------------------------------------------------------------
# fail closed
# --------------------------------------------------------------------------


def test_helper_fails_closed_for_a_byo_cluster_with_no_explicit_factor():
    body = helper_body()
    assert "{{- fail " in body, (
        "the helper must `fail` when kafka.enabled=false and no replicationFactor "
        "is set. Guessing produces a runtime crash-loop whose error names neither "
        "the factor nor the broker count; failing produces a render-time error "
        "that names both."
    )
    # Presence is not reachability. The BYO branch must open DIRECTLY with the
    # fail: anything emitted ahead of it is a value, and a value is a guess --
    # which is the entire defect, restored, with the `fail` left in place looking
    # like protection. (A mutation that emitted "3" before the fail passed the
    # presence check alone.)
    byo_branch = body[body.index("{{- else -}}") :]
    assert re.match(r"\{\{-\s*else\s*-\}\}\s*\{\{-\s*fail\s", byo_branch), (
        "the BYO branch emits something before it fails -- so it does not fail, "
        f"it guesses: {byo_branch[:120]!r}"
    )
    # the fail must be the BYO branch, not the in-chart one: a single KRaft node
    # is knowable, so that branch has an answer (1) and must not fail.
    assert ".Values.kafka.enabled" in body, "helper does not distinguish in-chart from BYO"
    enabled_branch = body[body.index("else if .Values.kafka.enabled") : body.index("{{- fail ")]
    assert "1" in enabled_branch, "the in-chart single-node broker must render 1, not fail"


def test_the_failure_message_tells_the_operator_what_to_set():
    body = helper_body()
    fail_msg = body[body.index("{{- fail ") :]
    for token in ("kafka.replicationFactor", "TimeoutException", "broker count"):
        assert token in fail_msg, (
            f"the fail message must mention {token!r} -- the whole point is that "
            "the runtime error it replaces mentions none of them."
        )


# --------------------------------------------------------------------------
# kafka-init: reconcile intent with reality
# --------------------------------------------------------------------------


def test_kafka_init_clamps_the_factor_to_the_live_broker_count():
    """Mirrors `topology.go clampToCluster`, the Go side of the same reconciliation.

    kafka-init is the only component in the chart holding a live broker view, so
    it is the only place an overstated factor can be corrected. Without this, RF=3
    against one broker fails all 14 topic creates instead of degrading.
    """
    script = kafka_init_script()
    # Anchored to line starts throughout. An unanchored `in script` is satisfied
    # by the clamp COMMENTED OUT, which is how a mutation that disabled it
    # survived the first version of this test.
    assert re.search(r'^\s*BROKERS="\$\(kafka-broker-api-versions\.sh', script, re.M), (
        "no live broker probe -- kafka-init is the only component in the chart "
        "that can see the cluster, so it is the only place an overstated factor "
        "can be corrected"
    )
    assert re.search(r'^\s*if .*"\$RF"\s+-gt\s+"\$BROKERS"', script, re.M), (
        "the probe result is never compared to the requested factor"
    )
    assert re.search(r'^\s*RF="\$BROKERS"\s*$', script, re.M), "the comparison never clamps"
    # A failed probe must DISABLE the clamp, not be read as "zero brokers" --
    # otherwise an unreachable admin endpoint clamps every topic to RF 0.
    assert re.search(r'"\$BROKERS"\s+-gt\s+0', script), (
        "a failed probe (0) must disable the clamp rather than clamp to zero"
    )


def test_kafka_init_pins_min_insync_replicas_instead_of_inheriting_it():
    """Unset misr is not "no floor" -- it is the broker default, and it is 2 on MSK.

    An RF=1 topic that inherits misr=2 is created successfully and is then
    permanently unwritable, which is the worst available failure mode: no error at
    create time, no error at start-up, and producers rejected forever after.
    Mirrors `replication.go pinMinInsyncReplicas` -> min(2, RF).
    """
    script = kafka_init_script()
    assert re.search(r'^\s*MISR=2\s*$', script, re.M), (
        "misr is not pinned when unset -- it inherits the broker default, which is "
        "invisible from the chart and is 2 on MSK"
    )
    assert re.search(r'^\s*MISR_ARG="--config min\.insync\.replicas=\$MISR"\s*$', script, re.M), (
        "the pinned value is never passed to topic create"
    )
    assert re.search(r'^\s*--replication-factor "\$RF" \$MISR_ARG', script, re.M), (
        "MISR_ARG is computed but never reaches kafka-topics.sh"
    )


def test_kafka_init_never_emits_min_insync_replicas_above_the_factor():
    """misr > RF creates the topic and makes it unwritable. Both paths must clamp.

    Both, not one: the pinned path (misr unset, RF=1 -> 2 would exceed it) and the
    explicit path (operator sets misr=3 on a 1-broker cluster).
    """
    script = kafka_init_script()
    # Two guards, because there are two ways in: the pinned path (misr unset) and
    # the explicit path (operator-supplied). Matched by comparison-then-assignment
    # rather than by adjacency -- the two are separated by a warning echo, and a
    # test that demanded adjacency would break the next time someone adds one.
    guards = [m.start() for m in re.finditer(r'"\$MISR"\s+-gt\s+"\$RF"', script)]
    assert len(guards) >= 2, (
        f"found {len(guards)} misr>rf guards, expected 2 (pinned path + explicit "
        "path). An unguarded path emits misr>RF, and Kafka accepts that: the topic "
        "is created and is then permanently unwritable."
    )
    for at in guards:
        window = script[at : at + 600]
        assert 'MISR="$RF"' in window, (
            f"the misr>rf guard at offset {at} compares but never clamps"
        )


# --------------------------------------------------------------------------
# values.yaml
# --------------------------------------------------------------------------


def test_values_leaves_both_knobs_empty_and_says_why():
    text = VALUES.read_text()
    assert re.search(r'^\s*replicationFactor:\s*""\s*$', text, re.M), (
        "kafka.replicationFactor must ship empty -- a shipped default is a guess, "
        "and the chart cannot see the cluster"
    )
    assert re.search(r'^\s*minInsyncReplicas:\s*""\s*$', text, re.M)
    block = text[text.index("replicationFactor:") - 1200 : text.index("minInsyncReplicas:") + 900]
    for token in ("kafka.enabled=false", "min.insync.replicas", "broker"):
        assert token in block, f"values.yaml does not explain {token!r} to the operator"
