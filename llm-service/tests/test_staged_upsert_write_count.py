"""KI-UPSERT-COUNT-AUDIT — staged_upsert must report the rows the MERGE wrote.

Guards the KI-WRITE-COUNT-1 standard on the shared stage-and-merge fast path:
the returned count comes from ``cursor.rowcount`` on the merge statement, NOT
from ``len(rows)``. An input-length count is the phantom-write-count signature
(success reported, 0 rows landed). ``base_connector`` resolves to
shared/mcp-connectors/base_connector.py via llm-service/conftest.py:15.
"""
from base_connector import DestinationLoadMixin, DestinationLoadSpec

COLS = ["id", "v"]
KEYS = ["id"]
ROWS = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 3, "v": "c"}]


class _Cursor:
    """Minimal DBAPI-ish cursor: only ``rowcount``, stamped by each stage hook."""

    def __init__(self):
        self.rowcount = -1


class _Dest(DestinationLoadMixin):
    """Concrete mixin host; each hook stamps the rowcount its statement would."""

    load_spec = DestinationLoadSpec(load_method="bulk", merge_method="upsert",
                                    supports_staging=True)

    def __init__(self, merge_rowcount, drop_rowcount=7):
        self._merge_rowcount = merge_rowcount
        self._drop_rowcount = drop_rowcount

    def _stage_create(self, cursor, target_table, columns):
        cursor.rowcount = -1
        return "_stg"

    def _stage_bulk_load(self, cursor, staging_table, columns, rows, col_types):
        cursor.rowcount = len(rows)

    def _stage_merge(self, cursor, target_table, staging_table, columns,
                     key_fields, merge_method):
        cursor.rowcount = self._merge_rowcount

    def _stage_drop(self, cursor, staging_table):
        cursor.rowcount = self._drop_rowcount


def test_staged_upsert_reports_zero_when_merge_wrote_nothing():
    # The KI-WRITE-COUNT-1 shape: the merge persisted nothing. Reporting
    # len(ROWS) here is the silent-data-loss fabrication.
    dest = _Dest(merge_rowcount=0)
    assert dest.staged_upsert(_Cursor(), "t", COLS, ROWS, KEYS) == 0


def test_staged_upsert_reads_merge_rowcount_not_drop_rowcount():
    # _stage_drop runs in a finally block and overwrites cursor.rowcount, so the
    # merge count must be captured before it.
    dest = _Dest(merge_rowcount=2, drop_rowcount=7)
    assert dest.staged_upsert(_Cursor(), "t", COLS, ROWS, KEYS) == 2


def test_staged_upsert_falls_back_to_input_count_when_driver_reports_none():
    # Documented fallback: rowcount < 0 means "driver reported no count".
    dest = _Dest(merge_rowcount=-1)
    assert dest.staged_upsert(_Cursor(), "t", COLS, ROWS, KEYS) == 3


def test_staged_upsert_empty_input_is_zero():
    dest = _Dest(merge_rowcount=5)
    assert dest.staged_upsert(_Cursor(), "t", COLS, [], KEYS) == 0


def test_staged_upsert_clamps_mysql_style_double_counted_rowcount():
    """MySQL's INSERT ... ON DUPLICATE KEY UPDATE reports rowcount 2 per UPDATED
    row (1 per inserted, 0 per unchanged), so a 3-row all-update merge reports 6.
    The merge cannot write more TARGET rows than were staged, so the count is
    clamped to the input length — otherwise fixing the under-report would have
    introduced an over-report on every MySQL destination. This clamp is a no-op
    on PostgreSQL-protocol and ClickHouse drivers, where rowcount <= n_input.
    """
    dest = _Dest(merge_rowcount=6)
    assert dest.staged_upsert(_Cursor(), "t", COLS, ROWS, KEYS) == 3


def test_staged_upsert_clamp_still_surfaces_a_partial_shortfall():
    """The clamp must not flatten real shortfalls into a full-success number —
    detecting "wrote fewer than we staged" is the entire point of the fix.
    """
    dest = _Dest(merge_rowcount=1)
    assert dest.staged_upsert(_Cursor(), "t", COLS, ROWS, KEYS) == 1
