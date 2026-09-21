"""One exception string for five different causes -- and the fix.

kafka-python converts almost every connect-time failure into the same thing:

    KafkaTimeoutError: Failed to update metadata after 60.0 secs

A wrong SASL password, a CA the client does not trust, a missing client
certificate, an expired OAuth token and a broker that is simply not running all
arrive as that one sentence. Four of those are configuration mistakes an
operator can fix in a minute once they are named, and the fifth is not their
fault at all -- but the log says nothing that tells them apart, so every one of
them reads as "Kafka is down" and gets escalated as an outage.

The verdict is not lost, only discarded: kafka-python logs the real reason on
the ``kafka`` logger at WARNING/ERROR *before* it folds the failure into retry
state. ``arm_failure_explainer`` keeps the last few of those lines and
``explain_failure`` appends the most specific one to the exception. Nothing is
intercepted or re-raised; the exception the caller sees is unchanged.

Two properties matter as much as the explanation itself:

  * it must not print the credential. The message it quotes is kafka-python's,
    not ours, and a broker's rejection can echo back what was sent -- so the
    values of the four secret env vars are replaced before anything is
    returned. test_the_explanation_never_prints_the_credential pins that.
  * arming must not change what the service logs. Raising a level the service
    deliberately set, or flipping ``propagate``, would make this diagnostic
    aid quietly reformat an unrelated part of the output.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils import kafka_security  # noqa: E402
from src.utils.kafka_security import (  # noqa: E402
    arm_failure_explainer,
    explain_failure,
    failure_cause,
    reset_failure_cause,
)

KAFKA_LOGGER = "kafka"

# The real strings, as kafka-python / its SSL layer emit them.
WRONG_PASSWORD = (
    "<BrokerConnection node_id=1 host=broker:9093> failed authentication: "
    "SaslAuthenticationFailed: Authentication failed"
)
UNTRUSTED_CA = (
    "<BrokerConnection node_id=1 host=broker:9093> Error connecting: "
    "SSLError: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: "
    "unable to get local issuer certificate (_ssl.c:1006)"
)
BROKER_DOWN = "<BrokerConnection node_id=1 host=broker:9093> Connect attempt returned error 61"


class Timeout(Exception):
    """Stands in for KafkaTimeoutError, which needs the kafka package."""


TIMEOUT = Timeout("Failed to update metadata after 60.0 secs")


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    """Each test starts with an empty buffer and a disarmed explainer."""
    reset_failure_cause()
    logger = logging.getLogger(KAFKA_LOGGER)
    before = list(logger.handlers)
    level, propagate = logger.level, logger.propagate
    monkeypatch.setattr(kafka_security, "_cause_armed", False, raising=False)
    yield
    logger.handlers[:] = before
    logger.setLevel(level)
    logger.propagate = propagate
    monkeypatch.setattr(kafka_security, "_cause_armed", False, raising=False)
    reset_failure_cause()


def _emit(*messages, level=logging.WARNING):
    logger = logging.getLogger(KAFKA_LOGGER)
    for message in messages:
        logger.log(level, "%s", message)


# ---------------------------------------------------------------------------
# the explanation
# ---------------------------------------------------------------------------

def test_without_the_explainer_the_timeout_says_nothing():
    """The control: this is the status quo the change exists to replace."""
    assert failure_cause() is None
    assert explain_failure(TIMEOUT) == "Timeout: Failed to update metadata after 60.0 secs"


def test_a_rejected_password_is_named_instead_of_timed_out():
    arm_failure_explainer()
    _emit(WRONG_PASSWORD)
    explained = explain_failure(TIMEOUT)
    assert "Failed to update metadata" in explained, "the original must survive"
    assert "SaslAuthenticationFailed" in explained


def test_an_untrusted_ca_is_named_instead_of_timed_out():
    arm_failure_explainer()
    _emit(UNTRUSTED_CA)
    assert "CERTIFICATE_VERIFY_FAILED" in explain_failure(TIMEOUT)


def test_a_broker_that_is_merely_down_adds_nothing():
    """The one cause that is NOT a configuration mistake must not be dressed
    up as one -- a refused connection is already self-explanatory, and
    inventing an auth verdict for it would send the operator to the wrong
    place."""
    arm_failure_explainer()
    _emit(BROKER_DOWN)
    assert explain_failure(TIMEOUT) == "Timeout: Failed to update metadata after 60.0 secs"


def test_the_specific_verdict_wins_over_the_generic_one():
    """Both lines are logged for one failure: a generic SSL/transport note and
    the authentication verdict behind it. Reporting the last line would report
    the generic one, which is the same non-answer as the timeout."""
    arm_failure_explainer()
    _emit("Broker is not ready, closing ssl connection", WRONG_PASSWORD)
    assert "SaslAuthenticationFailed" in explain_failure(TIMEOUT)


def test_the_most_recent_failure_wins_over_an_older_one_of_equal_rank():
    """A reconnect loop records many attempts; the current one is the one the
    operator is looking at."""
    arm_failure_explainer()
    _emit(UNTRUSTED_CA)
    _emit(WRONG_PASSWORD)
    explained = explain_failure(TIMEOUT)
    assert "SaslAuthenticationFailed" in explained


# ---------------------------------------------------------------------------
# it must not leak the credential it is explaining
# ---------------------------------------------------------------------------

def test_the_explanation_never_prints_the_credential(monkeypatch):
    """The quoted text is kafka-python's, and a broker's rejection can echo
    back what was sent."""
    secret = "sup3r-s3cret-client-value"
    monkeypatch.setenv("KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET", secret)
    arm_failure_explainer()
    _emit(f"failed authentication: SaslAuthenticationFailed for token {secret}")
    explained = explain_failure(TIMEOUT)
    assert secret not in explained
    assert "***" in explained
    assert "SaslAuthenticationFailed" in explained


def test_the_exception_text_is_redacted_too(monkeypatch):
    """Some drivers put the credential in the exception rather than the log."""
    secret = "sup3r-s3cret-client-value"
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", secret)
    assert secret not in explain_failure(Timeout(f"auth failed for {secret}"))


def test_a_short_secret_is_not_used_as_a_redaction_pattern(monkeypatch):
    """Below a few characters a "secret" matches ordinary words, and a log line
    turned into '*** connecting to ***' explains nothing. A rig with a
    throwaway 3-character password is not a reason to destroy the message."""
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "ssl")
    arm_failure_explainer()
    _emit(UNTRUSTED_CA)
    assert "CERTIFICATE_VERIFY_FAILED" in explain_failure(TIMEOUT)


# ---------------------------------------------------------------------------
# arming must be invisible to everything else
# ---------------------------------------------------------------------------

def test_arming_is_idempotent():
    """Every client construction arms it; N clients must not mean N handlers
    and N copies of every line."""
    logger = logging.getLogger(KAFKA_LOGGER)
    before = len(logger.handlers)
    for _ in range(5):
        arm_failure_explainer()
    assert len(logger.handlers) == before + 1


def test_arming_does_not_lower_a_level_the_service_chose():
    """A service that set the kafka logger to ERROR did so deliberately."""
    logger = logging.getLogger(KAFKA_LOGGER)
    logger.setLevel(logging.ERROR)
    arm_failure_explainer()
    assert logger.level == logging.ERROR


def test_arming_raises_only_a_level_nobody_chose(monkeypatch):
    """The inverse: a kafka logger still at NOTSET under a quiet root never
    creates the records this depends on, and no one chose that for kafka
    specifically -- so it is the one case arming may correct."""
    root = logging.getLogger()
    monkeypatch.setattr(root, "level", logging.ERROR)
    logger = logging.getLogger(KAFKA_LOGGER)
    logger.setLevel(logging.NOTSET)
    arm_failure_explainer()
    assert logger.level == logging.WARNING


def test_arming_leaves_an_inherited_level_that_already_works(monkeypatch):
    """Under the default root the records already flow; setting a level on the
    kafka logger would pin it against a later reconfiguration for nothing."""
    root = logging.getLogger()
    monkeypatch.setattr(root, "level", logging.WARNING)
    logger = logging.getLogger(KAFKA_LOGGER)
    logger.setLevel(logging.NOTSET)
    arm_failure_explainer()
    assert logger.level == logging.NOTSET


def test_arming_does_not_touch_propagate():
    """propagate is the service's choice about its own output; flipping it
    would silently add or remove kafka lines from the service log."""
    logger = logging.getLogger(KAFKA_LOGGER)
    logger.propagate = False
    arm_failure_explainer()
    assert logger.propagate is False


def test_the_buffer_is_bounded():
    """A long-running consumer logs these continuously; an unbounded buffer
    would be a slow leak in a process that is meant to run for weeks."""
    arm_failure_explainer()
    _emit(*[f"failed authentication: attempt {i}" for i in range(500)])
    assert len(kafka_security._cause_records) <= kafka_security._CAUSE_BUFFER_SIZE


def test_a_broken_format_string_is_not_our_failure():
    """The handler must never raise inside someone else's logging call.

    Driven straight at the handler rather than through the logger, because
    every OTHER handler attached to "kafka" (pytest's included) raises on this
    record -- which is the point: ours must not add a second failure to it.
    """
    arm_failure_explainer()
    logger = logging.getLogger(KAFKA_LOGGER)
    recorder = next(
        h for h in logger.handlers if isinstance(h, kafka_security._CauseRecorder)
    )
    broken = logging.LogRecord(
        KAFKA_LOGGER, logging.WARNING, __file__, 0, "missing arg %s %s", ("one",), None
    )
    recorder.emit(broken)  # must not raise
    assert explain_failure(TIMEOUT) == "Timeout: Failed to update metadata after 60.0 secs"


def test_records_from_other_loggers_are_ignored():
    """Only kafka-python's own verdicts are quoted; an unrelated library's
    "certificate" line would be a confident wrong answer."""
    arm_failure_explainer()
    logging.getLogger("some.other.lib").warning(
        "certificate verify failed for an unrelated https call"
    )
    assert explain_failure(TIMEOUT) == "Timeout: Failed to update metadata after 60.0 secs"


# ---------------------------------------------------------------------------
# the wiring: every client in this service is built from kafka_security_kwargs
# ---------------------------------------------------------------------------

def test_building_client_kwargs_arms_the_explainer(monkeypatch):
    """This is why no call site has to remember to arm it."""
    monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL", raising=False)
    monkeypatch.delenv("KAFKA_SASL_MECHANISM", raising=False)
    logger = logging.getLogger(KAFKA_LOGGER)
    before = len(logger.handlers)
    kafka_security.kafka_security_kwargs()
    assert len(logger.handlers) == before + 1
