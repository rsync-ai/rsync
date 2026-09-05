from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True)
class EventOrder:
    """
    Canonical ordering metadata for CDC events.

    This is used as a deterministic tie-breaker for "latest wins" semantics.
    All fields are optional; consumers should compare using `event_order_key`.
    """

    # Postgres
    lsn: Optional[str] = None

    # MySQL (GTID or binlog)
    gtid: Optional[str] = None
    file: Optional[str] = None
    pos: Optional[int] = None
    row: Optional[int] = None

    # SQL Server
    commit_lsn: Optional[str] = None
    change_lsn: Optional[str] = None
    event_serial_no: Optional[int] = None

    # Generic / fallback (Kafka)
    kafka_partition: Optional[int] = None
    kafka_offset: Optional[int] = None

    # Commit timestamp (useful but not authoritative for ordering)
    source_ts_ms: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "lsn": self.lsn,
            "gtid": self.gtid,
            "file": self.file,
            "pos": self.pos,
            "row": self.row,
            "commit_lsn": self.commit_lsn,
            "change_lsn": self.change_lsn,
            "event_serial_no": self.event_serial_no,
            "kafka_partition": self.kafka_partition,
            "kafka_offset": self.kafka_offset,
            "source_ts_ms": self.source_ts_ms,
        }


def extract_debezium_event_order(event: Dict[str, Any]) -> EventOrder:
    """
    Extract ordering metadata from a Debezium event envelope.

    Supported best-effort shapes:
    - Debezium (Kafka Connect): {"payload":{"source":{...}}, "source":{...}}
    - Already-flattened events: {"source":{...}}
    - kafka metadata optionally present as {"kafka": {"partition":..., "offset":...}} or top-level keys.
    """
    if not isinstance(event, dict):
        return EventOrder()

    payload = event.get("payload") if isinstance(event.get("payload"), dict) else None
    source = None
    if payload and isinstance(payload.get("source"), dict):
        source = payload.get("source")
    elif isinstance(event.get("source"), dict):
        source = event.get("source")
    else:
        source = {}

    # Kafka metadata (optional)
    kafka = event.get("kafka") if isinstance(event.get("kafka"), dict) else {}
    kafka_partition = kafka.get("partition") or event.get("kafka_partition") or event.get("partition")
    kafka_offset = kafka.get("offset") or event.get("kafka_offset") or event.get("offset")

    def to_int(v: Any) -> Optional[int]:
        try:
            if v is None or v == "":
                return None
            return int(v)
        except Exception:
            return None

    # Common timestamp fields
    source_ts_ms = to_int(source.get("ts_ms") or source.get("ts_us") or source.get("ts_ns"))

    # PostgreSQL
    lsn = source.get("lsn") or source.get("commit_lsn")

    # MySQL
    gtid = source.get("gtid")
    file = source.get("file") or source.get("binlog_filename")
    pos = to_int(source.get("pos") or source.get("binlog_position"))
    row = to_int(source.get("row") or source.get("binlog_row"))

    # SQL Server
    commit_lsn = source.get("commit_lsn")
    change_lsn = source.get("change_lsn")
    event_serial_no = to_int(source.get("event_serial_no"))

    return EventOrder(
        lsn=str(lsn) if lsn is not None else None,
        gtid=str(gtid) if gtid is not None else None,
        file=str(file) if file is not None else None,
        pos=pos,
        row=row,
        commit_lsn=str(commit_lsn) if commit_lsn is not None else None,
        change_lsn=str(change_lsn) if change_lsn is not None else None,
        event_serial_no=event_serial_no,
        kafka_partition=to_int(kafka_partition),
        kafka_offset=to_int(kafka_offset),
        source_ts_ms=source_ts_ms,
    )


def event_order_key(order: EventOrder) -> Tuple:
    """
    Convert EventOrder into a tuple suitable for lexicographic comparison.

    Priority (descending):
    1) commit position (lsn/commit_lsn/file+pos)
    2) intra-commit tie-breaker (event_serial_no/row)
    3) kafka offset (fallback)
    4) source_ts_ms (not authoritative, last resort)

    NOTE: LSN/GTID are strings with connector-specific ordering semantics.
    For LSNs (Postgres, SQL Server), lexical compare works for consistent formats,
    but consumers should still treat this as best-effort unless normalizing.
    """
    # Prefer LSN-like fields when present
    if order.commit_lsn:
        return ("sqlserver", order.commit_lsn, order.change_lsn or "", order.event_serial_no or -1, order.kafka_offset or -1)
    if order.lsn:
        return ("postgres", order.lsn, order.event_serial_no or -1, order.kafka_offset or -1)

    # MySQL binlog position
    if order.file or order.pos is not None:
        return ("mysql", order.file or "", order.pos or -1, order.row or -1, order.kafka_offset or -1)

    # GTID (string compare is not always safe; treat as fallback signal)
    if order.gtid:
        return ("gtid", order.gtid, order.kafka_offset or -1)

    # Kafka fallback
    if order.kafka_offset is not None:
        return ("kafka", order.kafka_partition or -1, order.kafka_offset)

    # Last resort
    return ("ts", order.source_ts_ms or -1)

