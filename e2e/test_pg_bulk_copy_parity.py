"""Parity + throughput proof for the PG bulk COPY import path.

Exercises the REAL serializer from the (mounted) edited connector.py:
  - executemany baseline (exactly what import_data does today) -> table A
  - COPY via _build_pg_copy_csv + copy_expert (the new path)     -> table B
Asserts A and B store byte-identical rows across a hostile type matrix,
then times 100k simple rows both ways to evidence the throughput win.
"""
import os, sys, time, math, json
import datetime as dt
from decimal import Decimal
import psycopg2

import connector as C  # /app/connector.py (edited worktree version, mounted)

PG = dict(host=os.environ.get("PGHOST", "copytest-pg"), port=5432,
          dbname="testdb", user="postgres", password="test")

DDL = """
CREATE TABLE {n} (
  id         bigint,
  name       text,
  price      numeric(10,2),
  active     boolean,
  ratio      double precision,
  payload    jsonb,
  meta       json,
  created_at timestamptz,
  day        date,
  raw        bytea,
  note       text
)"""

COLS = ["id","name","price","active","ratio","payload","meta","created_at","day","raw","note"]
COL_TYPES = {"payload": "json", "meta": "json"}  # mark json cols for _pg_bind_value

ROWS = [
    {"id":1, "name":'a,b"c\nd\there', "price":Decimal("123.45"), "active":True,
     "ratio":0.1+0.2, "payload":{"k":["v",1,None]}, "meta":{"x":1},
     "created_at":dt.datetime(2024,1,15,12,0,0,123456,tzinfo=dt.timezone.utc),
     "day":dt.date(2024,1,15), "raw":bytes([0,255,16,10,34]), "note":""},
    {"id":2, "name":None, "price":None, "active":False, "ratio":float("nan"),
     "payload":None, "meta":None, "created_at":None, "day":None, "raw":None, "note":"plain"},
    {"id":3, "name":"münchen ☃", "price":Decimal("0.00"), "active":None, "ratio":-1.5e10,
     "payload":[1,2,3], "meta":"bareword", "created_at":dt.datetime(2024,6,1,0,0,0),
     "day":dt.date(1999,12,31), "raw":bytes(b"hi"), "note":"trailing space "},
]

def norm(v):
    # make NaN comparable and bytes/memoryview uniform
    if isinstance(v, float) and math.isnan(v): return "NaN"
    if isinstance(v, memoryview): return v.tobytes()
    return v

def fetch(cur, n):
    cur.execute(f"SELECT {', '.join(COLS)} FROM {n} ORDER BY id")
    return [tuple(norm(x) for x in r) for r in cur.fetchall()]

def main():
    conn = psycopg2.connect(**PG); conn.autocommit = False
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS t_exec, t_copy")
    cur.execute(DDL.format(n="t_exec")); cur.execute(DDL.format(n="t_copy"))
    conn.commit()

    col_str = ", ".join(f'"{c}"' for c in COLS)

    # --- baseline: exactly import_data's executemany loop ---
    ph = ", ".join(["%s"] * len(COLS))
    iq = f'INSERT INTO t_exec ({col_str}) VALUES ({ph})'
    vals = [tuple(C._pg_bind_value(r.get(c), COL_TYPES.get(c)) for c in COLS) for r in ROWS]
    cur.executemany(iq, vals); conn.commit()

    # --- new path: the REAL _build_pg_copy_csv + COPY ---
    import io
    buf = io.StringIO(C._build_pg_copy_csv(ROWS, COLS, COL_TYPES))
    cur.copy_expert(f'COPY t_copy ({col_str}) FROM STDIN WITH (FORMAT csv)', buf)
    copy_rowcount = cur.rowcount
    conn.commit()

    a, b = fetch(cur, "t_exec"), fetch(cur, "t_copy")
    ok = (a == b)
    print(f"[parity] executemany rows={len(a)} copy rows={len(b)} copy_rowcount={copy_rowcount}")
    if not ok:
        for i,(ra,rb) in enumerate(zip(a,b)):
            for c,va,vb in zip(COLS, ra, rb):
                if va != vb:
                    print(f"  MISMATCH row{i} col={c}: exec={va!r} copy={vb!r}")
    print(f"[parity] RESULT: {'PASS' if ok else 'FAIL'}")

    # --- throughput: 100k simple rows ---
    cur.execute("DROP TABLE IF EXISTS p_exec, p_copy")
    cur.execute("CREATE TABLE p_exec (id bigint, name text, ts timestamptz)")
    cur.execute("CREATE TABLE p_copy (id bigint, name text, ts timestamptz)")
    conn.commit()
    N = 100_000
    now = dt.datetime(2024,1,1, tzinfo=dt.timezone.utc)
    big = [{"id":i, "name":f"row-{i}-name", "ts":now} for i in range(N)]
    pcols = ["id","name","ts"]; pct = {}

    t0 = time.perf_counter()
    iq2 = 'INSERT INTO p_exec (id, name, ts) VALUES (%s, %s, %s)'
    for j in range(0, N, 1000):
        chunk = big[j:j+1000]
        cur.executemany(iq2, [(r["id"], r["name"], r["ts"]) for r in chunk])
    conn.commit()
    t_exec = time.perf_counter() - t0

    t0 = time.perf_counter()
    buf = io.StringIO(C._build_pg_copy_csv(big, pcols, pct))
    cur.copy_expert('COPY p_copy (id, name, ts) FROM STDIN WITH (FORMAT csv)', buf)
    conn.commit()
    t_copy = time.perf_counter() - t0

    cur.execute("SELECT count(*) FROM p_exec"); ce = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM p_copy"); cc = cur.fetchone()[0]
    print(f"[perf] N={N} executemany={t_exec:.3f}s copy={t_copy:.3f}s "
          f"speedup={t_exec/t_copy:.1f}x  counts exec={ce} copy={cc}")

    cur.close(); conn.close()
    sys.exit(0 if ok and ce == cc == N else 1)

if __name__ == "__main__":
    main()
