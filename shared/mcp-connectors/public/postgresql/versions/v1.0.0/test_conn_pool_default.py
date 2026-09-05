"""Regression tests: the process-global PostgreSQL connection pool is ON by default.

#595 added the pool (`_acquire_conn`/`_release_conn`/`_conn_is_live`) but left it
opt-in (RSYNC_PG_CONN_POOL unset => off), so a live 1M-row CDC drain to Azure PG paid a
fresh TCP+TLS connect on every flush and stalled at ~900 rows/s even though both the sink
and the pg connector were near-idle on CPU. This flips the default to ON — reuse a
liveness-checked idle connection across calls — with RSYNC_PG_CONN_POOL=false as the
escape hatch back to the legacy open-per-call path.

These tests are OFFLINE: the pool round-trip drives fake connections and monkeypatches
`_get_connection`, so no live database is touched.

Run inside the postgresql connector image:
    docker cp <thisfile> rsync-ai-postgresql-v1-0-0-mcp:/app/ && \
    docker exec rsync-ai-postgresql-v1-0-0-mcp python /app/test_conn_pool_default.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import connector as C  # noqa: E402
from connector import PostgresqlMCPServer  # noqa: E402


def _srv():
    return PostgresqlMCPServer.__new__(PostgresqlMCPServer)  # bypass __init__


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql):
        if self._conn.raise_on_query:
            raise RuntimeError("connection is dead")

    def fetchone(self):
        return (1,)

    def close(self):
        pass


class _FakeConn:
    """Minimal stand-in exercising the paths _conn_is_live / _release_conn use."""

    def __init__(self, raise_on_query=False):
        self.closed = 0
        self.raise_on_query = raise_on_query
        self.rollbacks = 0

    def cursor(self):
        return _FakeCursor(self)

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = 1


_CFG = {"host": "h", "port": 5432, "database": "d", "user": "u"}


def _reset_pool():
    with C._PG_POOL_LOCK:
        C._PG_IDLE_CONNS.clear()


def _with_env(value):
    """Set/unset RSYNC_PG_CONN_POOL and return the prior raw value for restore."""
    prior = os.environ.get("RSYNC_PG_CONN_POOL")
    if value is None:
        os.environ.pop("RSYNC_PG_CONN_POOL", None)
    else:
        os.environ["RSYNC_PG_CONN_POOL"] = value
    return prior


def _restore_env(prior):
    if prior is None:
        os.environ.pop("RSYNC_PG_CONN_POOL", None)
    else:
        os.environ["RSYNC_PG_CONN_POOL"] = prior


# --------------------------------------------------------------------------- #
# The default gate                                                            #
# --------------------------------------------------------------------------- #
def test_pool_enabled_by_default_when_unset():
    prior = _with_env(None)
    try:
        assert C._pg_pool_enabled() is True
    finally:
        _restore_env(prior)


def test_pool_disabled_by_explicit_false():
    prior = _with_env("false")
    try:
        for v in ("false", "0", "no", "off", "FALSE", "Off"):
            os.environ["RSYNC_PG_CONN_POOL"] = v
            assert C._pg_pool_enabled() is False, v
    finally:
        _restore_env(prior)


def test_pool_enabled_by_explicit_true():
    prior = _with_env("true")
    try:
        for v in ("true", "1", "yes", "on", "TRUE", "On"):
            os.environ["RSYNC_PG_CONN_POOL"] = v
            assert C._pg_pool_enabled() is True, v
    finally:
        _restore_env(prior)


# --------------------------------------------------------------------------- #
# Pool round-trip (default ON): reuse live, close broken/dead                  #
# --------------------------------------------------------------------------- #
def test_release_then_acquire_reuses_live_connection():
    prior = _with_env(None)  # default ON
    _reset_pool()
    try:
        s = _srv()
        live = _FakeConn()
        s._release_conn(live, pooled=True, config=_CFG)
        conn, pooled = s._acquire_conn(_CFG)
        assert conn is live and pooled is True
        assert live.rollbacks >= 1  # liveness check cleared txn state
        assert live.closed == 0
    finally:
        _reset_pool()
        _restore_env(prior)


def test_broken_connection_closed_not_pooled():
    prior = _with_env(None)
    _reset_pool()
    try:
        s = _srv()
        broken = _FakeConn()
        s._release_conn(broken, pooled=True, config=_CFG, broken=True)
        assert broken.closed == 1
        assert not (C._PG_IDLE_CONNS.get(C._pg_pool_key(_CFG)) or [])
    finally:
        _reset_pool()
        _restore_env(prior)


def test_dead_idle_connection_is_discarded_and_fresh_opened():
    prior = _with_env(None)
    _reset_pool()
    try:
        s = _srv()
        dead = _FakeConn(raise_on_query=True)   # SELECT 1 raises => not live
        s._release_conn(dead, pooled=True, config=_CFG)
        fresh = _FakeConn()
        s._get_connection = lambda cfg: fresh   # avoid a real connect
        conn, pooled = s._acquire_conn(_CFG)
        assert conn is fresh and pooled is True
        assert dead.closed == 1                 # dead one was closed
    finally:
        _reset_pool()
        _restore_env(prior)


def test_disabled_env_opens_fresh_per_call():
    prior = _with_env("false")
    _reset_pool()
    try:
        s = _srv()
        fresh = _FakeConn()
        s._get_connection = lambda cfg: fresh
        conn, pooled = s._acquire_conn(_CFG)
        assert conn is fresh and pooled is False   # non-pooled => caller closes
    finally:
        _reset_pool()
        _restore_env(prior)


# --------------------------------------------------------------------------- #
# Bucket key: credentials must partition the pool                              #
# --------------------------------------------------------------------------- #
# KI-PGMCP-POOL-KEY-OMITS-PASSWORD. The key used to be a "|"-join over
# ("host", "port", "database", "user") only, so two configs differing solely in
# password shared a bucket and _acquire_conn handed one config's AUTHENTICATED
# connection to the other. _conn_is_live could never catch it: `SELECT 1` proves
# the socket works, not that the caller's credentials do. Worst case is a
# rotated password or a REVOKEd role that keeps writing, upsert_data being the
# only pooled path and the WRITE.
#
# These assert the property (same config reuses, any difference does not),
# never the digest's format — hashing is an implementation detail and pinning it
# would just make the test brittle.
def test_differing_password_does_not_share_a_bucket():
    a = dict(_CFG, password="old-secret")
    b = dict(_CFG, password="rotated-secret")
    assert C._pg_pool_key(a) != C._pg_pool_key(b)


def test_identical_config_still_reuses_its_connection():
    # The other half of the property: over-partitioning would silently cost the
    # pool its entire reason to exist (the sink sends the same stable
    # DestinationConfig map on every flush).
    assert C._pg_pool_key(dict(_CFG)) == C._pg_pool_key(dict(_CFG))
    prior = _with_env(None)
    _reset_pool()
    try:
        s = _srv()
        live = _FakeConn()
        cfg = dict(_CFG, password="same-secret")
        s._release_conn(live, pooled=True, config=dict(cfg))
        conn, pooled = s._acquire_conn(dict(cfg))   # a fresh dict, equal contents
        assert conn is live and pooled is True
    finally:
        _reset_pool()
        _restore_env(prior)


def test_rotated_password_never_reuses_the_old_credentials_connection():
    """End-to-end shape of the bug, through _release_conn/_acquire_conn."""
    prior = _with_env(None)
    _reset_pool()
    try:
        s = _srv()
        old = _FakeConn()
        s._release_conn(old, pooled=True, config=dict(_CFG, password="old-secret"))
        fresh = _FakeConn()
        s._get_connection = lambda cfg: fresh
        conn, pooled = s._acquire_conn(dict(_CFG, password="rotated-secret"))
        # Pre-fix this returned `old` — a connection authenticated with the
        # password that was just rotated away.
        assert conn is fresh, "rotated credentials reused the old connection"
        assert old.closed == 0, "the old bucket should be untouched, not closed"
    finally:
        _reset_pool()
        _restore_env(prior)


# --------------------------------------------------------------------------- #
# Bucket cap: distinct-key growth is bounded, LRU-evicted                      #
# --------------------------------------------------------------------------- #
# Loop bounds are read from the constant but hard-capped: a mutant or a future
# retune that raises _PG_POOL_MAX_KEYS must not turn these into a billion-
# iteration hang. _CAP_PROBE is only ever "the cap, or a sane ceiling".
_CAP_PROBE = min(C._PG_POOL_MAX_KEYS, 64)


def test_distinct_buckets_are_capped_and_evicted_lru():
    """Keying on the whole config means every credential rotation and every
    pipeline mints a new bucket. Without _PG_POOL_MAX_KEYS a long-lived
    container would hold one permanently-open connection set per bucket against
    the database's backend limit."""
    prior = _with_env(None)
    _reset_pool()
    try:
        s = _srv()
        conns = []
        for i in range(_CAP_PROBE + 3):
            c = _FakeConn()
            conns.append(c)
            s._release_conn(c, pooled=True, config=dict(_CFG, database=f"db{i}"))
        assert len(C._PG_IDLE_CONNS) == _CAP_PROBE
        # The three oldest buckets were evicted AND their connections closed —
        # evicting without closing would leak the socket instead of the dict entry.
        assert [c.closed for c in conns[:3]] == [1, 1, 1]
        assert all(c.closed == 0 for c in conns[3:])
    finally:
        _reset_pool()
        _restore_env(prior)


def test_acquire_touches_lru_so_a_reused_bucket_outlives_an_idle_one():
    """_acquire_conn's move_to_end is what makes the cap an LRU rather than a
    FIFO. Without it the bucket a caller is actively reusing gets evicted on its
    insertion age alone, and the pool churns exactly the connection it should be
    keeping."""
    prior = _with_env(None)
    _reset_pool()
    try:
        s = _srv()
        cfgs = [dict(_CFG, database=f"db{i}") for i in range(_CAP_PROBE)]
        conns = [_FakeConn() for _ in cfgs]
        for cfg, c in zip(cfgs, conns):
            s._release_conn(c, pooled=True, config=dict(cfg))
        # Reuse the OLDEST bucket. Insertion order now says db0 is next to go;
        # the LRU touch inside _acquire_conn should move it to the back.
        got, _ = s._acquire_conn(dict(cfgs[0]))
        assert got is conns[0]
        # One more distinct bucket pushes the count past the cap.
        s._release_conn(_FakeConn(), pooled=True, config=dict(_CFG, database="newest"))
        assert C._pg_pool_key(cfgs[0]) in C._PG_IDLE_CONNS, "the reused bucket was evicted"
        assert C._pg_pool_key(cfgs[1]) not in C._PG_IDLE_CONNS, "the idle bucket survived"
        assert conns[1].closed == 1
    finally:
        _reset_pool()
        _restore_env(prior)


if __name__ == "__main__":
    import traceback

    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"ok   {fn.__name__}")
        except Exception:
            failed += 1
            print(f"FAIL {fn.__name__}")
            traceback.print_exc()
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    sys.exit(1 if failed else 0)
