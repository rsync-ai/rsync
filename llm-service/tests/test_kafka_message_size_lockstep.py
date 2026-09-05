"""A single record's size limit is set in six places, and they must all agree.

Kafka's default cap on one record is 1 MiB. A CDC change event for a row with a
large JSON/text/blob column exceeds it, and the failure is not a rejected row:
the producer raises RecordTooLargeException, the Debezium task goes FAILED, and
what the operator sees is a connector restart loop naming none of the settings
involved. So the platform raises the cap -- and raising it correctly means
raising it on the broker, on Kafka Connect's producer, and keeping the sink
worker's consumer fetch cap above both.

KI-CHART-BROKER-MISSING-MESSAGE-MAX-BYTES was that lockstep broken in the chart:
templates/connectors/cdc.yaml carried CONNECT_PRODUCER_MAX_REQUEST_SIZE at
15 MiB while templates/infra/kafka.yaml left the broker at its 1 MiB default.
Not a tuning miss -- there was no lever at all. templates/infra/kafka.yaml
includes `rsync-ai.commonEnv` zero times, so even the global `extraEnv` escape
hatch could not reach the broker. docker-compose.quickstart.yml had exactly the
same half-pair; only docker-compose.yml was complete.

Two mechanisms hold the pair together now, and this file guards both:

  1. In the chart, the two halves READ ONE VALUE (kafka.maxMessageBytes), so
     they cannot drift by construction. What can still break is the plumbing --
     a template that hardcodes a number again, or one that drops the `| int64`.
     That pipe is load-bearing and looks like decoration: Helm decodes every
     number in a values file as a float64, so a bare `| quote` renders 15728640
     as "1.572864e+07". Valid YAML, pod starts, Kafka rejects the config value.
     That regression was caught by rendering, not by reading, which is why it is
     asserted here as a literal source requirement.

  2. The compose files and the Go sink cannot share a value with the chart, so
     those four sites are compared numerically against the chart's default.

Everything is derived from ONE source -- the chart's kafka.maxMessageBytes -- so
raising the cap means editing one number and watching this test tell you which
of the other five sites you forgot.

Text-only by design: CI has no helm binary (`helm` appears in .github/workflows
only inside the tag-gated chart-publish job), so a render-based check here would
skip in the one place it needs to run.
"""

import os
import re

import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
CHART_DIR = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai")
CHART_VALUES = os.path.join(CHART_DIR, "values.yaml")
BROKER_TEMPLATE = os.path.join(CHART_DIR, "templates", "infra", "kafka.yaml")
PRODUCER_TEMPLATE = os.path.join(CHART_DIR, "templates", "connectors", "cdc.yaml")
COMPOSE_FILES = ["docker-compose.yml", "docker-compose.quickstart.yml"]
SINK_MAIN = os.path.join(
    REPO_ROOT, "shared", "mcp-connectors", "internal", "kafka-mcp-sink",
    "worker-src", "cmd", "kafka-sink-worker", "main.go",
)

BROKER_KEY = "KAFKA_MESSAGE_MAX_BYTES"
PRODUCER_KEY = "CONNECT_PRODUCER_MAX_REQUEST_SIZE"
SHARED_VALUE_PATH = ".Values.kafka.maxMessageBytes"

# `- name: FOO` followed by its `value:` line, as the chart writes env entries.
def _template_env_value(path, key):
    """The raw template expression a chart template assigns to one env var."""
    text = open(path, encoding="utf-8").read()
    match = re.search(
        r"-\s*name:\s*%s\s*\n\s*value:\s*(.+)" % re.escape(key), text
    )
    return match.group(1).strip() if match else None


def _compose_env_number(path, key):
    """The integer a compose file assigns to one environment key, or None."""
    text = open(os.path.join(REPO_ROOT, path), encoding="utf-8").read()
    match = re.search(r"^\s*%s:\s*[\"']?(\d+)[\"']?\s*$" % re.escape(key), text, re.M)
    return int(match.group(1)) if match else None


def _chart_default():
    values = yaml.safe_load(open(CHART_VALUES, encoding="utf-8"))
    assert isinstance(values, dict) and "kafka" in values, (
        "%s did not parse into a mapping with a `kafka:` block -- this test's "
        "one source of truth is gone, so every assertion below would be "
        "vacuous." % CHART_VALUES
    )
    cap = values["kafka"].get("maxMessageBytes")
    assert isinstance(cap, int), (
        "kafka.maxMessageBytes is missing from %s (or is not an integer: %r).\n\n"
        "It is the single value both halves of the record-size pair read. "
        "Without it the broker silently falls back to Kafka's 1 MiB default and "
        "wide CDC rows fail the task with RecordTooLargeException."
        % (CHART_VALUES, cap)
    )
    return cap


def test_chart_broker_and_producer_read_one_value_through_int64():
    """Neither chart half may hardcode a number, and neither may drop `| int64`."""
    cap = _chart_default()
    assert cap > 1048576, (
        "kafka.maxMessageBytes=%d is at or below Kafka's 1 MiB default, which "
        "is the value this whole mechanism exists to raise." % cap
    )

    for path, key in ((BROKER_TEMPLATE, BROKER_KEY), (PRODUCER_TEMPLATE, PRODUCER_KEY)):
        expr = _template_env_value(path, key)
        rel = os.path.relpath(path, REPO_ROOT)
        assert expr is not None, (
            "%s emits no %s env var.\n\n"
            "The broker half was missing from the chart for the whole of its "
            "life (KI-CHART-BROKER-MISSING-MESSAGE-MAX-BYTES) and there was no "
            "values-level workaround: %s includes `rsync-ai.commonEnv` zero "
            "times, so `extraEnv` cannot reach the broker."
            % (rel, key, os.path.relpath(BROKER_TEMPLATE, REPO_ROOT))
        )
        assert SHARED_VALUE_PATH in expr, (
            "%s sets %s to %s instead of reading %s.\n\n"
            "The two halves share one value precisely so they cannot drift; a "
            "literal here reintroduces the drift the shared value removed."
            % (rel, key, expr, SHARED_VALUE_PATH)
        )
        assert "int64" in expr, (
            "%s sets %s to %s -- no `| int64`.\n\n"
            "Helm decodes every number in a values file as a float64, so `| quote` "
            "alone renders %d as \"%s\". That is valid YAML: the pod starts and "
            "Kafka rejects the config value, in an error naming neither this "
            "template nor the setting."
            % (rel, key, expr, cap, format(float(cap), "g"))
        )


def test_every_compose_file_sets_both_halves_to_the_chart_value():
    """Compose cannot share the chart's value, so it is compared to it."""
    cap = _chart_default()
    checked = 0
    for compose in COMPOSE_FILES:
        for key in (BROKER_KEY, PRODUCER_KEY):
            found = _compose_env_number(compose, key)
            assert found is not None, (
                "%s sets no %s.\n\n"
                "A stack that sets only one half of the pair produces a "
                "RecordTooLargeException on wide CDC rows: either the broker "
                "rejects what the producer was told it could send, or the "
                "broker accepts a record no consumer is configured to fetch. "
                "docker-compose.quickstart.yml shipped exactly this half-pair."
                % (compose, key)
            )
            assert found == cap, (
                "%s sets %s=%d but the chart's kafka.maxMessageBytes is %d.\n\n"
                "These are the same limit expressed in two deployment paths; a "
                "reader who raises one and not the other gets a Docker install "
                "and a Kubernetes install that fail on different rows."
                % (compose, key, found, cap)
            )
            checked += 1
    assert checked == len(COMPOSE_FILES) * 2, (
        "expected %d compose settings, checked %d"
        % (len(COMPOSE_FILES) * 2, checked)
    )


def test_the_sink_consumer_fetch_cap_exceeds_the_broker_cap():
    """The third leg: a record the broker accepts must still be fetchable."""
    cap = _chart_default()
    source = open(SINK_MAIN, encoding="utf-8").read()
    match = re.search(r"MaxBytes:\s*([0-9*\s]+?),", source)
    assert match, (
        "no ReaderConfig.MaxBytes found in %s -- the consumer leg of the "
        "lockstep cannot be checked, so this test would pass vacuously."
        % os.path.relpath(SINK_MAIN, REPO_ROOT)
    )
    expr = match.group(1).strip()
    assert re.fullmatch(r"[0-9]+(\s*\*\s*[0-9]+)*", expr), (
        "ReaderConfig.MaxBytes is %r, which this test will not evaluate. It "
        "reads a literal product of integers on purpose; if the cap became a "
        "variable, compare it to the chart value some other way rather than "
        "letting this assertion lapse." % expr
    )
    max_bytes = 1
    for part in expr.split("*"):
        max_bytes *= int(part.strip())
    assert max_bytes > cap, (
        "the sink worker fetches at most %d bytes but the broker accepts "
        "records up to %d.\n\n"
        "A max-size change event is then produced successfully and can never "
        "be read: the consumer stalls on it forever with no error on the "
        "producing side. The fetch cap must EXCEED the broker cap, not match "
        "it." % (max_bytes, cap)
    )
