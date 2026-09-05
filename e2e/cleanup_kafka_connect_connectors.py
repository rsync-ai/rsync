#!/usr/bin/env python3
"""
Best-effort cleanup for Kafka Connect connectors created by E2E runs.

Without cleanup, Postgres Debezium connectors can exhaust `max_wal_senders` by keeping
replication connections open, causing unrelated tests to fail.
"""

import time

import requests


KAFKA_CONNECT = "http://localhost:8083"


def main() -> None:
    try:
        r = requests.get(f"{KAFKA_CONNECT}/connectors", timeout=10)
        r.raise_for_status()
        connectors = r.json() or []
    except Exception as e:
        raise SystemExit(f"failed to list connectors: {e}")

    # Only delete known test prefixes (avoid nuking user-managed connectors).
    prefixes = ("debug-", "cdc-")
    to_delete = [c for c in connectors if isinstance(c, str) and c.startswith(prefixes)]

    for name in to_delete:
        try:
            requests.delete(f"{KAFKA_CONNECT}/connectors/{name}", timeout=10)
        except Exception:
            pass

    # Wait a bit for tasks to stop and connections to close.
    time.sleep(2)

    print(f"OK deleted={len(to_delete)}")


if __name__ == "__main__":
    main()

