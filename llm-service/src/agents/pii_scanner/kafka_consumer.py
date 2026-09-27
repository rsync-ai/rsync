"""Kafka consumer for async PII scan requests.

Reads from the 'pii.scan.request' topic, runs PIIScannerService.scan_schema_async,
and publishes the result to 'pii.scan.response' so the API gateway can update the
scan job status.
"""

import asyncio
import json
import logging
from typing import Any, Callable, Dict, List, Optional
from src.utils.kafka_topics import group, topic

logger = logging.getLogger(__name__)

# The in-cluster broker, matching every other Kafka client in this service. The
# old default was "localhost:9092", which is never right inside a container: the
# consumer died at startup, the failure was logged at warning level, and async
# PII scan requests were then accepted and silently never processed.
DEFAULT_KAFKA_BROKERS = "kafka:29092"
REQUEST_TOPIC = topic("pii.scan.request")
RESPONSE_TOPIC = topic("pii.scan.response")
# Qualified under the same KAFKA_TOPIC_PREFIX contract as the topics above, so
# one PREFIXED ACL on a customer-managed cluster covers this consumer's topics
# AND its group. A bare group id left outside that grant fails at join with an
# authorization error, which for this consumer looks like the pre-existing
# silent failure it was built to end: scan requests accepted, never processed.
#
# Import-time, like REQUEST_TOPIC/RESPONSE_TOPIC. The group and the topic it
# reads have to be resolved under the same environment, and nothing here mutates
# KAFKA_TOPIC_PREFIX after start-up. (Note _kafka_client_kwargs() is read at
# call time for a different reason -- broker/SASL settings, not naming.)
CONSUMER_GROUP = group("llm-service-pii-scanner")

# Kafka is a peer container: on a cold `docker compose up` the broker is
# routinely not listening when this service starts. Without a retry the consumer
# is dead for the life of the process. 10 x 5s.
CONNECT_ATTEMPTS = 10
CONNECT_RETRY_SECONDS = 5.0

# Waiting for REQUEST_TOPIC to exist. This consumer never creates the topic (see
# _wait_for_request_topic), so on a cold start it waits for the kafka-init step
# that does. The delay doubles from the first value up to the cap and then stays
# there; the number of checks is unbounded, because giving up would leave the
# consumer dead for the life of the process while the API keeps accepting scans.
TOPIC_WAIT_INITIAL_SECONDS = 5.0
TOPIC_WAIT_MAX_SECONDS = 60.0


def _kafka_client_kwargs() -> Dict[str, Any]:
    """Broker list + SASL/TLS kwargs shared by the consumer and the producer.

    Resolved through the same helpers as every other Kafka client in the product
    so one KAFKA_* configuration means one thing: ``brokers_from_env`` accepts
    KAFKA_BROKERS *or* KAFKA_BOOTSTRAP_SERVERS and splits a multi-broker CSV
    (reading only KAFKA_BROKERS and passing it unsplit made a 3-broker cluster
    one unresolvable hostname), and ``kafka_security_kwargs`` supplies the
    SASL/TLS profile without which a secured cluster refuses the connection.

    Read at call time, not import time, so the environment the container was
    started with is the one that counts.
    """
    try:
        from src.utils.kafka_security import brokers_from_env, kafka_security_kwargs
    except ImportError:  # pragma: no cover - import path differs under some runners
        from ...utils.kafka_security import brokers_from_env, kafka_security_kwargs

    return {
        "bootstrap_servers": brokers_from_env(DEFAULT_KAFKA_BROKERS),
        **kafka_security_kwargs(),
    }


async def _wait_for_request_topic(consumer: Any, explain_failure: Callable[[BaseException], str]) -> None:
    """Return once REQUEST_TOPIC exists on the cluster. Never raises.

    The consumer is built with allow_auto_create_topics=False, so it cannot
    create the topic itself. kafka-python defaults that flag to True, and the
    consumer's metadata request then created the topic with the broker's
    defaults: 1 partition and the broker's retention, not the 3-partition topic
    that kafka-init creates. Whichever of the two ran first decided the topic's
    shape, and kafka-init only ever reports a topic that already exists.

    consumer.topics() lists every topic through a metadata request for ALL
    topics, which never creates one. A failed listing (a broker restarting, say)
    is logged and retried like a missing topic: this runs as a fire-and-forget
    background task, so an exception here would end the consumer silently.
    """
    loop = asyncio.get_running_loop()
    delay = TOPIC_WAIT_INITIAL_SECONDS
    check = 0
    while True:
        check += 1
        try:
            # A blocking network call; keep it off the gateway's event loop.
            known = await loop.run_in_executor(None, consumer.topics)
            if REQUEST_TOPIC in known:
                if check > 1:
                    logger.info(
                        "PII Kafka consumer: topic '%s' exists after %d checks", REQUEST_TOPIC, check
                    )
                return
            reason = "it does not exist yet"
        except Exception as exc:  # noqa: BLE001 - see docstring: never raise
            reason = f"listing topics failed: {explain_failure(exc)}"
        logger.warning(
            "PII Kafka consumer waiting for topic '%s' (check %d: %s). The kafka-init "
            "step creates it; this consumer never does. Checking again in %ss",
            REQUEST_TOPIC,
            check,
            reason,
            delay,
        )
        await asyncio.sleep(delay)
        delay = min(delay * 2, TOPIC_WAIT_MAX_SECONDS)


def table_requests_from_payload(tables_raw: Any) -> List[Any]:
    """The scan request's tables, as the gateway sends them:
    ``[{"table_name": "public.users", "columns": [{"column_name": "email",
    "data_type": "text"}]}]``. Names and declared types only; no row value
    is ever in this message.

    A bare table name (the payload an older gateway sent) becomes a table with
    no columns, which scan_schema reports as not scanned rather than clean.
    """
    from .service import ColumnScanRequest, TableScanRequest

    requests = []
    for entry in tables_raw or []:
        if isinstance(entry, str):
            requests.append(TableScanRequest(table_name=entry, columns=[]))
            continue
        if not isinstance(entry, dict) or not entry.get("table_name"):
            continue
        columns = [
            ColumnScanRequest(
                column_name=str(c["column_name"]),
                samples=[],
                data_type=c.get("data_type") or None,
            )
            for c in (entry.get("columns") or [])
            if isinstance(c, dict) and c.get("column_name")
        ]
        requests.append(TableScanRequest(table_name=str(entry["table_name"]), columns=columns))
    return requests


def build_scan_response(scan_id: str, connection_id: str, trace_id: str, result: Any) -> Dict[str, Any]:
    """The pii.scan.response message for a scan that ran. The gateway projects
    ``result.tables`` into pii_findings and prunes, per table listed, the
    findings the scan no longer reports; so only a table that was really
    scanned may be listed."""
    tables = result.tables or []
    return {
        "trace_id": trace_id,
        "pipeline_id": "",
        "correlation_id": scan_id,
        "status": "completed",
        "agent": "pii_scanner",
        "result": {
            "scan_id": scan_id,
            "connection_id": connection_id,
            "tables_scanned": len(tables),
            "total_pii_columns_found": result.total_pii_columns_found,
            "scan_method": result.scan_method,
            "errors": result.errors,
            "tables": [
                {
                    "table_name": t.table_name,
                    "columns": [
                        {
                            "column_name": c.column_name,
                            "is_pii": c.is_pii,
                            "pii_type": c.pii_type,
                            "confidence": c.confidence,
                            "detection_method": c.detection_method,
                            "suggested_masking": c.suggested_masking,
                        }
                        for c in t.columns
                    ],
                }
                for t in tables
            ],
        },
        "error": "",
        "timestamp": "",
    }


def handle_scan_request(
    data: Dict[str, Any],
    scan: Callable[[Any], Any],
) -> Dict[str, Any]:
    """One pii.scan.request message in, its pii.scan.response message out.

    ``scan`` runs a SchemaScanRequest and returns its SchemaScanResult; the
    consumer passes one that runs on the event loop.
    """
    from .service import SchemaScanRequest

    scan_id = data.get("scan_id", "")
    connection_id = data.get("connection_id", "")
    trace_id = data.get("trace_id", scan_id)
    try:
        scan_request = SchemaScanRequest(
            connection_id=connection_id,
            tables=table_requests_from_payload(data.get("tables")),
            include_ml=bool(data.get("include_ml", True)),
        )
        return build_scan_response(scan_id, connection_id, trace_id, scan(scan_request))
    except Exception as exc:
        logger.exception("PII scan failed for scan_id=%s: %s", scan_id, exc)
        return {
            "trace_id": trace_id,
            "pipeline_id": "",
            "correlation_id": scan_id,
            "status": "failed",
            "agent": "pii_scanner",
            "result": {"scan_id": scan_id},
            "error": str(exc),
            "timestamp": "",
        }


async def run_pii_kafka_consumer() -> None:
    """Background task: consume pii.scan.request messages and publish results."""
    try:
        from kafka import KafkaConsumer, KafkaProducer
    except ImportError:
        logger.warning("kafka-python not installed; PII Kafka consumer disabled")
        return

    try:
        from .service import PIIScannerService
    except ImportError as exc:
        logger.warning("PIIScannerService unavailable; PII Kafka consumer disabled: %s", exc)
        return

    try:
        from src.utils.kafka_security import KafkaSecurityError, explain_failure
    except ImportError:  # pragma: no cover - import path differs under some runners
        from ...utils.kafka_security import KafkaSecurityError, explain_failure

    service = PIIScannerService()

    try:
        client_kwargs = _kafka_client_kwargs()
    except KafkaSecurityError as exc:
        # A rejected security profile never becomes valid by waiting, so this is
        # fatal rather than retried — and it is logged at error, naming the
        # consequence, because the caller (gateway/main.py) starts this as a
        # background task and cannot see the failure.
        logger.error(
            "PII Kafka consumer DISABLED: invalid Kafka security configuration: %s. "
            "Async PII scan requests will be accepted and never processed.",
            exc,
        )
        return

    brokers = client_kwargs["bootstrap_servers"]
    consumer: Optional[object] = None
    producer: Optional[object] = None
    for attempt in range(1, CONNECT_ATTEMPTS + 1):
        try:
            # No topic here: the subscription waits until REQUEST_TOPIC exists
            # (_wait_for_request_topic), and allow_auto_create_topics=False keeps
            # this consumer from creating it with the broker's defaults.
            consumer = KafkaConsumer(
                **client_kwargs,
                group_id=CONSUMER_GROUP,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                allow_auto_create_topics=False,
            )
            producer = KafkaProducer(
                **client_kwargs,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            break
        except Exception as exc:
            # explain_failure, not exc: a wrong password, an untrusted CA and a
            # broker that is simply down all raise the same KafkaTimeoutError.
            if attempt == CONNECT_ATTEMPTS:
                logger.error(
                    "PII Kafka consumer DISABLED: could not connect to %s after %d attempts: %s. "
                    "Async PII scan requests will be accepted and never processed.",
                    brokers,
                    CONNECT_ATTEMPTS,
                    explain_failure(exc),
                )
                return
            logger.warning(
                "PII Kafka consumer connect to %s failed (attempt %d/%d): %s; retrying in %ss",
                brokers,
                attempt,
                CONNECT_ATTEMPTS,
                explain_failure(exc),
                CONNECT_RETRY_SECONDS,
            )
            await asyncio.sleep(CONNECT_RETRY_SECONDS)

    await _wait_for_request_topic(consumer, explain_failure)
    consumer.subscribe([REQUEST_TOPIC])

    logger.info(
        "PII Kafka consumer started on topic '%s' (brokers=%s)", REQUEST_TOPIC, brokers
    )

    loop = asyncio.get_event_loop()

    def _scan(request: Any) -> Any:
        return asyncio.run_coroutine_threadsafe(
            service.scan_schema_async(request), loop
        ).result(timeout=300)

    def _consume() -> None:
        for msg in consumer:
            data = msg.value
            scan_id = data.get("scan_id", "")
            logger.info(
                "PII scan request received: scan_id=%s connection_id=%s",
                scan_id,
                data.get("connection_id", ""),
            )
            response = handle_scan_request(data, _scan)

            try:
                producer.send(RESPONSE_TOPIC, value=response)
                producer.flush()
                logger.info("PII scan response published: scan_id=%s status=%s", scan_id, response["status"])
            except Exception as exc:
                logger.error("Failed to publish PII scan response: %s", exc)

    # Run the blocking Kafka poll loop in a thread so the event loop stays free
    await loop.run_in_executor(None, _consume)
