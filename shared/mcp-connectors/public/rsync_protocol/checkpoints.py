from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional


@dataclass
class Checkpoint:
    """
    Minimal checkpoint structure.
    - For batch incremental: watermark or cursor token
    - For CDC: last processed event order / offsets (optional)
    """

    stream_id: str
    updated_at: str
    watermark: Optional[Any] = None
    cursor_token: Optional[str] = None
    last_pk: Optional[Any] = None
    last_event_order: Optional[Dict[str, Any]] = None
    last_batch_id: Optional[str] = None


def load_checkpoint_json(raw: str) -> Optional[Checkpoint]:
    if not raw:
        return None
    try:
        d = json.loads(raw)
        if not isinstance(d, dict):
            return None
        return Checkpoint(**d)
    except Exception:
        return None


def dump_checkpoint_json(
    *,
    stream_id: str,
    watermark: Optional[Any] = None,
    cursor_token: Optional[str] = None,
    last_pk: Optional[Any] = None,
    last_event_order: Optional[Dict[str, Any]] = None,
    last_batch_id: Optional[str] = None,
) -> str:
    cp = Checkpoint(
        stream_id=stream_id,
        updated_at=datetime.now(timezone.utc).isoformat(),
        watermark=watermark,
        cursor_token=cursor_token,
        last_pk=last_pk,
        last_event_order=last_event_order,
        last_batch_id=last_batch_id,
    )
    return json.dumps(asdict(cp), default=str, sort_keys=True)

