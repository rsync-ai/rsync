"""The diagnose backstop has to name the keys the producer actually writes.

``POST /v1/diagnose/pipeline`` renders an evidence object into an LLM prompt.
The producer (api-gateway ``diagnose.go``) scrubs each
free-text field at source, and ``scrub_evidence_for_llm`` is the backstop for the
field it forgets. That backstop used to name ``stderr``, ``stdout``, ``traceback``
and ``exception`` -- none of which any producer writes -- while missing ``body``,
which is where every attached log line lands, and ``blocking_description``. A
backstop that covers no real field protects nothing.

It is not a hypothetical path either: the llm-service gateway declares no
``require_internal_secret`` anywhere, so this endpoint takes an arbitrary evidence
object from anything that can reach it, and then there is no producer upstream to
have scrubbed it at all.

The fixture below mirrors the real producer's shape key-for-key (see
``buildDiagnoseEvidence`` in ``api-gateway/internal/handlers/diagnose.go``), with a distinct row value planted in every free-text
position.
"""

from src.utils.masking import EVIDENCE_FREETEXT_KEYS, scrub_evidence_for_llm

# A connection id whose first group is all digits. The scrubber replaces any run of
# 7+ digits, so this is metadata that over-scrubbing visibly destroys -- which is what
# makes test_metadata_is_not_scrubbed able to fail. Ids are load-bearing here: the
# model is asked to point at the connection it blames.
SOURCE_CONN_ID = "41347622-9c1d-4e7a-b3f0-5d2a8e6c1904"
EXECUTION_ID = "exec-20260924103000"

# One distinct marker per free-text position, so a failure names the position.
LEAK_MARKERS = {
    "progress.message": "leak-in-progress-message",
    "progress.blocking_description": "leak-in-blocking-description",
    "latest_execution.error_message": "leak-in-exec-error",
    "dependencies[].last_error": "leak-in-dep-last-error",
    "dependencies[].details.*": "leak-in-dep-details",
    "sink_error_detail[].body": "leak-in-sink-body",
    "flow.stalled_stage_logs[].body": "leak-in-stalled-body",
}


def _evidence():
    """Producer-shaped evidence with a quoted row value in every free-text field.

    Quoted because that is the form ``scrub_error_for_llm`` removes: a SQL literal
    is how a failed row's values reach an error string in practice.
    """

    def err(marker):
        return f"ERROR: duplicate key value violates unique constraint: Key (email)=('{marker}')"

    return {
        "pipeline": {
            "name": "orders-sync",
            "status": "failed",
            "sync_mode": "cdc",
            "source_type": "postgresql",
            "destination_type": "bigquery",
            "source_connection_id": SOURCE_CONN_ID,
        },
        "progress": {
            "current_stage": "sink",
            "message": err(LEAK_MARKERS["progress.message"]),
            "percent": 62,
            "blocking_reason": "dest_write_failed",
            "blocking_description": err(LEAK_MARKERS["progress.blocking_description"]),
        },
        "latest_execution": {
            "id": EXECUTION_ID,
            "status": "failed",
            "error_message": err(LEAK_MARKERS["latest_execution.error_message"]),
        },
        "dependencies": [
            {
                "name": "kafka_sink_worker",
                "status": "unhealthy",
                "last_error": err(LEAK_MARKERS["dependencies[].last_error"]),
                # Arbitrary per-probe keys: no leaf-key list can enumerate these.
                "details": {"docker_output": err(LEAK_MARKERS["dependencies[].details.*"])},
            }
        ],
        "sink_error_detail": [
            {
                "timestamp": "2026-09-24 10:00:00.123",
                "severity": "error",
                "service": "kafka-sink-worker",
                "body": err(LEAK_MARKERS["sink_error_detail[].body"]),
            }
        ],
        "flow": {
            "available": True,
            "stalled_stage": "sink_write",
            "stalled_stage_logs": [
                {
                    "service": "kafka-sink-worker",
                    "body": err(LEAK_MARKERS["flow.stalled_stage_logs[].body"]),
                }
            ],
        },
        "table_stats_summary": {"total_events": 40045, "inserted_rows": 0},
    }


def test_no_row_value_survives_in_any_free_text_position():
    rendered = repr(scrub_evidence_for_llm(_evidence()))

    survived = sorted(
        position for position, marker in LEAK_MARKERS.items() if marker in rendered
    )
    assert not survived, (
        "row values reached the prompt at: "
        + ", ".join(survived)
        + f"\nrendered evidence:\n{rendered}"
    )


def test_the_fixture_actually_carries_every_marker():
    """Vacuity guard.

    If the fixture ever stops planting a marker, the test above passes for the
    wrong reason -- it would be asserting the absence of something that was never
    there.
    """
    raw = repr(_evidence())
    missing = sorted(p for p, m in LEAK_MARKERS.items() if m not in raw)
    assert not missing, f"fixture no longer plants a marker at: {missing}"


def test_metadata_is_not_scrubbed():
    """The control, and the reason this is an allowlist rather than scrub-everything.

    A model cannot diagnose a pipeline whose table names, stage ids and counts have
    been replaced by markers. Without this, a backstop that redacted the entire
    object would pass every assertion above.
    """
    out = scrub_evidence_for_llm(_evidence())

    assert out["pipeline"]["name"] == "orders-sync"
    assert out["pipeline"]["source_type"] == "postgresql"
    # The ids the model is asked to point at. These are the assertions that give
    # this control teeth: the values above happen to contain nothing the scrubber
    # rewrites, so on their own they would survive a scrub-everything mutation and
    # the control would be a fiction.
    assert out["pipeline"]["source_connection_id"] == SOURCE_CONN_ID
    assert out["latest_execution"]["id"] == EXECUTION_ID
    assert out["progress"]["current_stage"] == "sink"
    assert out["progress"]["percent"] == 62
    assert out["flow"]["stalled_stage"] == "sink_write"
    assert out["table_stats_summary"]["total_events"] == 40045
    # Inside a free-text SUBTREE, the metadata siblings are still scrubbed as a
    # class -- that is the deliberate cost of inheriting the verdict downward --
    # but the service name survives because there is nothing in it to redact.
    assert out["sink_error_detail"][0]["service"] == "kafka-sink-worker"


def test_the_keys_the_go_producer_writes_are_all_covered():
    """Drift guard against the next producer field.

    These are the free-text keys api-gateway writes into the evidence object today.
    Adding one there without adding it here reopens exactly the gap this file was
    written for, and no behavioural test would notice, because the fixture would
    not know to plant a marker in it.
    """
    produced = {
        "message",             # diagnose.go: progress
        "blocking_description",  # diagnose.go: progress
        "error_message",       # diagnose.go: latest_execution
        "last_error",          # diagnose.go: dependencies[]
        "details",             # diagnose.go: dependencies[] (arbitrary sub-keys)
        "body",                # attached log lines
    }
    assert produced <= EVIDENCE_FREETEXT_KEYS, (
        "the producer writes free-text keys the backstop does not cover: "
        f"{sorted(produced - EVIDENCE_FREETEXT_KEYS)}"
    )
