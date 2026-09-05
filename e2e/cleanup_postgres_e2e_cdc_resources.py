#!/usr/bin/env python3
"""
Best-effort cleanup for Postgres logical replication resources created by Debezium E2E tests.

Debezium Postgres connector creates:
- replication slots (slot.name)
- publications (publication.name)

Deleting the Kafka Connect connector does NOT drop these by default, and they consume
`max_replication_slots`, eventually breaking CDC tests.
"""

import subprocess


PG_CONTAINER = "rsync-ai-postgres-e2e"


def sh(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, text=True)


def main() -> None:
    sql = r"""
DO $$
DECLARE r record;
BEGIN
  -- Drop Debezium-style publications created by tests
  FOR r IN
    SELECT pubname FROM pg_publication WHERE pubname LIKE 'dbz_pub_%'
  LOOP
    EXECUTE format('DROP PUBLICATION IF EXISTS %I', r.pubname);
  END LOOP;

  -- Drop replication slots created by tests
  FOR r IN
    SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'dbz_slot_%'
  LOOP
    BEGIN
      EXECUTE format('SELECT pg_drop_replication_slot(%L)', r.slot_name);
    EXCEPTION WHEN OTHERS THEN
      -- Ignore failures (active slot, permission, etc.)
      NULL;
    END;
  END LOOP;
END $$;
"""
    sh(
        [
            "docker",
            "exec",
            "-i",
            PG_CONTAINER,
            "psql",
            "-U",
            "e2e_user",
            "-d",
            "e2e_db",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            sql,
        ]
    )
    print("OK")


if __name__ == "__main__":
    main()

