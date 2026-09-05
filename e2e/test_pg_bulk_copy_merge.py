"""P2 proof: CDC upsert via COPY-into-staging + ON CONFLICT merge.

Drives the REAL inherited staged_upsert -> _stage_bulk_load (COPY when flag on,
else execute_values) -> _stage_merge. Asserts:
  - merge converges to ONE row per PK with latest values (update-in-place)
  - COPY path and executemany path produce byte-identical final tables
"""
import os, sys
import psycopg2
import connector as C

PG = dict(host=os.environ.get("PGHOST", "copytest-pg"), port=5432,
          dbname="testdb", user="postgres", password="test")
COLS = ["id", "name", "amount"]
KEYS = ["id"]
CT = {}
DDL = "CREATE TABLE {n} (id bigint PRIMARY KEY, name text, amount numeric(10,2))"

# batch 1 inserts pk 1,2,3 ; batch 2 updates pk 1,2 and inserts pk 4
B1 = [{"id":1,"name":"a","amount":10}, {"id":2,"name":"b","amount":20}, {"id":3,"name":"c","amount":30}]
B2 = [{"id":1,"name":"a2","amount":11}, {"id":2,"name":"b2","amount":22}, {"id":4,"name":"d","amount":40}]

def run(cur, table, use_copy):
    os.environ["RSYNC_PG_BULK_COPY"] = "true" if use_copy else ""
    srv = C.PostgresqlMCPServer()
    for batch in (B1, B2):
        srv.staged_upsert(cur, table, COLS, batch, KEYS, CT)

def fetch(cur, n):
    cur.execute(f"SELECT id, name, amount FROM {n} ORDER BY id")
    return cur.fetchall()

def main():
    conn = psycopg2.connect(**PG); conn.autocommit = False
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS m_exec, m_copy")
    cur.execute(DDL.format(n="m_exec")); cur.execute(DDL.format(n="m_copy"))
    conn.commit()

    run(cur, "m_exec", use_copy=False); conn.commit()
    run(cur, "m_copy", use_copy=True);  conn.commit()

    a, b = fetch(cur, "m_exec"), fetch(cur, "m_copy")
    expected = [(1, "a2", 11), (2, "b2", 22), (3, "c", 30), (4, "d", 40)]
    norm = lambda rows: [(r[0], r[1], float(r[2])) for r in rows]
    one_per_pk = (len(a) == 4 and len(b) == 4)
    merged_ok = (norm(a) == [(i, n, float(v)) for i, n, v in expected])
    parity = (a == b)
    print(f"[merge] m_exec={norm(a)}")
    print(f"[merge] m_copy={norm(b)}")
    print(f"[merge] one_row_per_pk={one_per_pk} update_in_place={merged_ok} copy==exec={parity}")
    ok = one_per_pk and merged_ok and parity
    print(f"[merge] RESULT: {'PASS' if ok else 'FAIL'}")
    cur.close(); conn.close()
    sys.exit(0 if ok else 1)

if __name__ == "__main__":
    main()
