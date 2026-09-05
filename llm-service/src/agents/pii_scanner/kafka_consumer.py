"""Kafka consumer for async PII scan requests.

Reads from the 'pii.scan.request' topic, runs PIIScannerService.scan_schema_async,
and publishes the result to 'pii.scan.response' so the API gateway can update the
scan job status.
"""

import asyncio
import json
import logging
from typing import Any, Dict, Optional
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
# is dead for the life of the process. Mirrors the planner consumer's 10 x 5s.
CONNECT_ATTEMPTS = 10
CONNECT_RETRY_SECONDS = 5.0


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


async def run_pii_kafka_consumer() -> None:
    """Background task: consume pii.scan.request messages and publish results."""
    try:
        from kafka import KafkaConsumer, KafkaProducer
    except ImportError:
        logger.warning("kafka-python not installed; PII Kafka consumer disabled")
        return

    try:
        from .service import PIIScannerService, SchemaScanRequest, TableScanRequest, ColumnScanRequest
    except ImportError as exc:
        logger.warning("PIIScannerService unavailable; PII Kafka consumer disabled: %s", exc)
        return

    try:
        from src.utils.kafka_security import KafkaSecurityError
    except ImportError:  # pragma: no cover - import path differs under some runners
        from ...utils.kafka_security import KafkaSecurityError

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
            consumer = KafkaConsumer(
                REQUEST_TOPIC,
                **client_kwargs,
                group_id=CONSUMER_GROUP,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
            )
            producer = KafkaProducer(
                **client_kwargs,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            break
        except Exception as exc:
            if attempt == CONNECT_ATTEMPTS:
                logger.error(
                    "PII Kafka consumer DISABLED: could not connect to %s after %d attempts: %s. "
                    "Async PII scan requests will be accepted and never processed.",
                    brokers,
                    CONNECT_ATTEMPTS,
                    exc,
                )
                return
            logger.warning(
                "PII Kafka consumer connect to %s failed (attempt %d/%d): %s; retrying in %ss",
                brokers,
                attempt,
                CONNECT_ATTEMPTS,
                exc,
                CONNECT_RETRY_SECONDS,
            )
            await asyncio.sleep(CONNECT_RETRY_SECONDS)

    logger.info(
        "PII Kafka consumer started on topic '%s' (brokers=%s)", REQUEST_TOPIC, brokers
    )

    loop = asyncio.get_event_loop()

    def _consume() -> None:
        for msg in consumer:
            data = msg.value
            scan_id = data.get("scan_id", "")
            connection_id = data.get("connection_id", "")
            tables_raw = data.get("tables") or []
            include_ml = bool(data.get("include_ml", True))
            trace_id = data.get("trace_id", scan_id)

            logger.info("PII scan request received: scan_id=%s connection_id=%s", scan_id, connection_id)

            try:
                # Build the scan request from the Kafka payload.
                # 'tables_raw' is a list of table names (strings); we have no sample
                # data at this stage — the scan will use the Presidio pattern detector
                # against column names only (zero-data-access mode).
                table_requests = [
                    TableScanRequest(table_name=t, columns=[])
                    for t in tables_raw
                ] if tables_raw else []

                scan_request = SchemaScanRequest(
                    connection_id=connection_id,
                    tables=table_requests,
                    include_ml=include_ml,
                )

                import asyncio as _asyncio
                result = _asyncio.run_coroutine_threadsafe(
                    service.scan_schema_async(scan_request), loop
                ).result(timeout=300)

                response = {
                    "trace_id": trace_id,
                    "pipeline_id": "",
                    "correlation_id": scan_id,
                    "status": "completed",
                    "agent": "pii_scanner",
                    "result": {
                        "scan_id": scan_id,
                        "connection_id": connection_id,
                        "tables_scanned": result.tables_scanned,
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
                            for t in (result.tables or [])
                        ],
                    },
                    "error": "",
                    "timestamp": "",
                }
            except Exception as exc:
                logger.exception("PII scan failed for scan_id=%s: %s", scan_id, exc)
                response = {
                    "trace_id": trace_id,
                    "pipeline_id": "",
                    "correlation_id": scan_id,
                    "status": "failed",
                    "agent": "pii_scanner",
                    "result": {"scan_id": scan_id},
                    "error": str(exc),
                    "timestamp": "",
                }

            try:
                producer.send(RESPONSE_TOPIC, value=response)
                producer.flush()
                logger.info("PII scan response published: scan_id=%s status=%s", scan_id, response["status"])
            except Exception as exc:
                logger.error("Failed to publish PII scan response: %s", exc)

    # Run the blocking Kafka poll loop in a thread so the event loop stays free
    await loop.run_in_executor(None, _consume)
