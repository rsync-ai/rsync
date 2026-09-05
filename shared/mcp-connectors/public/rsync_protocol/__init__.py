"""
Rsync protocol primitives shared by connectors.

Goal: centralize correctness-critical logic (ordering, idempotency manifests, checkpoints)
so generated connectors don't need to "guess" behavior.
"""

from .event_order import extract_debezium_event_order, event_order_key  # noqa: F401
from .manifest import (  # noqa: F401
    build_batch_manifest,
    manifest_success_key,
    manifest_key,
    pending_prefix,
)
from .checkpoints import (  # noqa: F401
    load_checkpoint_json,
    dump_checkpoint_json,
)

