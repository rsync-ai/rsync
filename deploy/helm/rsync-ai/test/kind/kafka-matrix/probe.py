"""Python runtime of the Kafka security matrix.

Security kwargs come ONLY from rsync's brokers_from_env()/kafka_security_kwargs()
(llm-service/src/utils/kafka_security.py, mounted read-only from the checkout at
/rsync/kafka_security.py). Nothing security-related is set here. It writes
PROBE_ID to PROBE_TOPIC (default "kmatrix") and reads it back.

Output: one line starting with RESULT; exit 0 on PASS, 1 on FAIL.
"""
import importlib.util
import logging
import os
import sys
import time

spec = importlib.util.spec_from_file_location("kafka_security", "/rsync/kafka_security.py")
ks = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ks)

import kafka  # noqa: E402
from kafka import KafkaConsumer, KafkaProducer, TopicPartition  # noqa: E402

TOPIC = os.getenv("PROBE_TOPIC") or "kmatrix"
PID = os.environ["PROBE_ID"].encode()

# kafka-python reports most auth and TLS failures as a bare timeout and logs the
# real cause at WARNING/ERROR. Capture those so a FAIL line says WHY -- the
# matrix asserts the reason, not just the failure.
KEYS = ("ssl", "certificate", "handshake", "sasl", "authentication", "unknown ca",
        "bad certificate", "certificate required", "401", "token")
# Appended below every pre-existing entry, so no existing precedence changes:
# a hostname mismatch is a real, final verdict, and without it 10b-tls-wrong-host
# would look "no verdict yet" and retry to no purpose.
RANK = ("saslauthenticationfailed", "alert", "certificate_verify_failed",
        "could not obtain", "invalid_token",
        "hostname mismatch", "no subject alternative", "doesn't match")
UNRANKED = len(RANK)

# A single connection does not always observe the broker's verdict: kafka-python
# parses a response only if the same recv batch did not also see the peer's close,
# so an auth error can be replaced by a bare "socket disconnected". That made
# 09a/09b flaky under load. Extra attempts sharpen the DIAGNOSIS only -- attempt 1
# alone decides PASS/FAIL, so a retry can never turn a real regression green.
MAX_ATTEMPTS = 3
DEADLINE = time.monotonic() + 110  # stays clear of the rig's 150s CELL_TIMEOUT
seen = []


class Grab(logging.Handler):
    def emit(self, record):
        seen.append(record.getMessage())


log = logging.getLogger("kafka")
log.addHandler(Grab(level=logging.WARNING))
log.setLevel(logging.WARNING)
log.propagate = bool(os.getenv("PROBE_DEBUG"))


def rank(msg):
    return next((i for i, r in enumerate(RANK) if r in msg.lower()), UNRANKED)


def best_cause():
    # The most specific logged cause wins: an explicit auth/TLS verdict beats
    # a bare "socket disconnected", which the broker's close usually races to.
    ranked = sorted((m for m in seen if any(k in m.lower() for k in KEYS)), key=rank)
    return ranked[0] if ranked else None


def out(ok, msg):
    if not ok:
        cause = best_cause()
        if cause:
            msg += " | cause: " + cause
    print(f"RESULT {'PASS' if ok else 'FAIL'} python {' '.join(msg.split())[:600]}")
    sys.exit(0 if ok else 1)


def attempt(decides):
    """One round trip. Only the deciding attempt may report PASS; later attempts
    run purely to let the broker's real verdict land in `seen`."""
    p = c = None
    try:
        p = KafkaProducer(bootstrap_servers=brokers, request_timeout_ms=10000,
                          max_block_ms=15000, **sec)
        md = p.send(TOPIC, PID).get(timeout=15)
        p.close(timeout=5)
        p = None
        c = KafkaConsumer(bootstrap_servers=brokers, enable_auto_commit=False,
                          consumer_timeout_ms=15000, request_timeout_ms=10000, **sec)
        tp = TopicPartition(TOPIC, md.partition)
        c.assign([tp])
        c.seek(tp, md.offset)
        for m in c:
            if m.value == PID:
                if decides:
                    out(True, f"round-trip ok (kafka-python {kafka.__version__})")
                return None
        return "consume: timed out waiting for own message"
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001
        return f"{type(e).__name__}: {e}"
    finally:
        # Retries must not pile up live clients: each one keeps a sender/fetcher
        # thread that would go on logging into `seen`. The two close() signatures
        # differ (producer: timeout, consumer: timeout_ms).
        if p is not None:
            try:
                p.close(timeout=5)
            except Exception:  # noqa: BLE001
                pass
        if c is not None:
            try:
                c.close(timeout_ms=5000)
            except Exception:  # noqa: BLE001
                pass


try:
    brokers, sec = ks.brokers_from_env(), ks.kafka_security_kwargs()
except Exception as e:  # noqa: BLE001
    out(False, f"CONFIG_REJECTED: {type(e).__name__}: {e}")

err = attempt(decides=True)
for _ in range(MAX_ATTEMPTS - 1):
    cause = best_cause()
    if (cause and rank(cause) < UNRANKED) or time.monotonic() > DEADLINE:
        break
    attempt(decides=False)
out(False, err)
