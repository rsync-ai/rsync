"""
Value-level source<->destination reconciliation for E2E data-pipeline tests.

This is the primitive the fix->test->fix loop has been missing: a check that
proves a destination table is a *faithful copy* of its source at the VALUE
level, not merely that "a row exists". ``assert_reconciled()`` runs three
independent checks, each catching a distinct, historically-real bug class:

  1. Row-count equality        -> sink dropped or duplicated rows.
  2. Primary-key-set equality  -> right COUNT but wrong rows (off-by-one
                                  keyset pagination, re-keyed upserts, a
                                  partial/skipped delete leaving an orphan).
  3. Per-cell value equality   -> right rows but corrupted values (type
                                  coercion, NULL handling, decimal scale loss,
                                  unicode mangling, silent truncation).

Why three checks and not one checksum? A single aggregate hash tells you
"something differs" but not *what* — and the team needs the *what* to break
the loop. Each check here fails with a precise, single-line reason naming the
PK and column involved.

Cross-engine (MySQL <-> Postgres) reconciliation is the hard part the existing
``explorer_parity`` helper deliberately punted on (see its module docstring):
the two CLIs render NULL, decimals and booleans differently. We solve it in two
layers:

  (a) IN SQL: project every column to text with an explicit NULL sentinel, so
      both engines emit directly comparable, NULL-unambiguous cells. (psql -A -t
      renders NULL as empty string; mysql --batch renders it as the literal
      "NULL" — without the sentinel these are indistinguishable from real data.)
  (b) IN PYTHON: a canonicaliser removes the residual *rendering* differences
      (decimal trailing zeros: 1.50 == 1.5) WITHOUT masking real differences.

Known cross-type hazards (documented, not silently swallowed):
  - Booleans: Postgres bool renders 't'/'f'; MySQL tinyint(1) renders '1'/'0'.
    These are not reconciled by default (we cannot tell '1'-the-bool from
    '1'-the-int without type info). Pass ``column_normalizers`` to map them.
  - Timestamps with differing precision/zone rendering: pass a normalizer.
  - Embedded TAB or NEWLINE inside a string cell: out of scope (the CSV
    transport is tab/line delimited). The "nasty data" fixture exercises
    unicode, NULLs, decimals, negatives and empty strings — not control chars.

Direct DB reads use ``docker exec`` against the canonical ``psql``/``mysql``
CLIs, matching existing e2e conventions (no Python DB driver dependency).
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Callable, Optional

# A sentinel that real data will not contain. Emitted in-SQL for NULL so the two
# engines' divergent NULL rendering collapses to one unambiguous token.
NULL_SENTINEL = "⟪RSYNC_NULL⟫"  # ⟪RSYNC_NULL⟫

# Field separator for the CSV transport. TAB is the only separator mysql --batch
# emits, so we standardise on it for both engines.
SEP = "\t"

# Reuse the DBHandle shape from the sibling helper so callers construct one kind
# of object for both parity and reconciliation.
try:  # pragma: no cover - import shim for both "run as module" and "run as file"
    from .explorer_parity import DBHandle
except ImportError:  # pragma: no cover
    from explorer_parity import DBHandle  # type: ignore


# --------------------------------------------------------------------------- #
# Canonicalisation
# --------------------------------------------------------------------------- #

_NUMERIC_RE = re.compile(r"^-?\d+(\.\d+)?$")


def canon_cell(raw: str, normalizer: Optional[Callable[[str], str]] = None) -> Optional[str]:
    """Canonicalise one rendered cell into a comparable token.

    Returns ``None`` for SQL NULL (the sentinel). Otherwise returns a string
    that is stable across the two engines' rendering quirks:

      - decimals: trailing zeros after the point are stripped so MySQL's
        ``1.50`` reconciles with Postgres' ``1.5`` (both -> ``"1.5"``), and
        ``2.00`` -> ``"2"``. Integer-valued text is left untouched.
      - everything else: returned verbatim (unicode preserved).

    A caller-supplied ``normalizer`` runs FIRST (before NULL/decimal handling)
    for column-specific needs (e.g. boolean 't'/'1' unification, timestamp
    truncation). It must be a pure ``str -> str``.
    """
    if raw == NULL_SENTINEL:
        return None
    if normalizer is not None:
        raw = normalizer(raw)
        if raw == NULL_SENTINEL:
            return None
    if _NUMERIC_RE.match(raw) and "." in raw:
        try:
            d = Decimal(raw)
            # Normalize trailing zeros without scientific notation.
            d = d.normalize()
            text = format(d, "f")
            return text
        except InvalidOperation:  # pragma: no cover - regex already guards
            return raw
    return raw


# --------------------------------------------------------------------------- #
# SQL projection (dialect-aware)
# --------------------------------------------------------------------------- #


def _text_cast(db: DBHandle, col: str) -> str:
    """A ``col -> text, NULL -> sentinel`` projection for the dialect."""
    if db.dialect == "postgres":
        inner = f'CAST("{col}" AS TEXT)'
    else:  # mysql — CAST(... AS CHAR) yields a variable-length string, not char(1)
        inner = f"CAST(`{col}` AS CHAR)"
    return f"COALESCE({inner}, '{NULL_SENTINEL}')"


def _qualified(db: DBHandle, schema: str, table: str) -> str:
    return f'"{schema}"."{table}"' if db.dialect == "postgres" else f"`{table}`"


def _order_by(db: DBHandle, pk_cols: list[str]) -> str:
    cols = ", ".join(
        (f'"{c}"' if db.dialect == "postgres" else f"`{c}`") for c in pk_cols
    )
    return f"ORDER BY {cols}"


def _run(db: DBHandle, sql: str) -> str:
    if db.dialect == "postgres":
        cmd = [
            "docker", "exec", "-i", db.container,
            "psql", "-U", db.user, "-d", db.database,
            "-t", "-A", "-F", SEP, "-c", sql,
        ]
    else:
        cmd = [
            "docker", "exec", "-i", db.container,
            "mysql", "-u", db.user, db.database,
            "--batch", "--skip-column-names", "--raw", "-e", sql,
        ]
    return subprocess.check_output(cmd, text=True)


def _fetch_rows(
    db: DBHandle,
    schema: str,
    table: str,
    pk_cols: list[str],
    columns: list[str],
) -> list[list[str]]:
    """Fetch ``columns`` for every row, ordered by PK, as a list of cell lists.

    Each cell is the raw rendered string (NULL already collapsed to the
    sentinel in SQL). Canonicalisation happens in the caller so the raw
    transport stays simple and debuggable.
    """
    proj = ", ".join(_text_cast(db, c) for c in columns)
    sql = f"SELECT {proj} FROM {_qualified(db, schema, table)} {_order_by(db, pk_cols)};"
    out = _run(db, sql)
    rows: list[list[str]] = []
    for line in out.split("\n"):
        if line == "":
            continue
        rows.append(line.split(SEP))
    return rows


# --------------------------------------------------------------------------- #
# Reconciliation
# --------------------------------------------------------------------------- #


@dataclass
class ValueDiff:
    pk: tuple
    column: str
    source: Optional[str]
    dest: Optional[str]


@dataclass
class ReconcileResult:
    ok: bool
    source_count: int
    dest_count: int
    missing_pks: list[tuple] = field(default_factory=list)  # in source, absent in dest
    extra_pks: list[tuple] = field(default_factory=list)    # in dest, absent in source
    value_diffs: list[ValueDiff] = field(default_factory=list)

    def reason(self) -> str:
        """A single-line, precise failure reason (empty string if ``ok``)."""
        if self.ok:
            return ""
        if self.source_count != self.dest_count:
            parts = [f"row count: source={self.source_count} dest={self.dest_count}"]
        else:
            parts = []
        if self.missing_pks:
            parts.append(
                f"{len(self.missing_pks)} pk(s) missing in dest "
                f"(e.g. {self.missing_pks[:3]})"
            )
        if self.extra_pks:
            parts.append(
                f"{len(self.extra_pks)} extra pk(s) in dest "
                f"(e.g. {self.extra_pks[:3]})"
            )
        if self.value_diffs:
            d = self.value_diffs[0]
            parts.append(
                f"{len(self.value_diffs)} value diff(s) "
                f"(e.g. pk={d.pk} col={d.column}: source={d.source!r} dest={d.dest!r})"
            )
        return "; ".join(parts) if parts else "tables differ"


def reconcile_rows(
    source_rows: list[list[str]],
    dest_rows: list[list[str]],
    columns: list[str],
    pk_indexes: list[int],
    column_normalizers: Optional[dict[str, Callable[[str], str]]] = None,
) -> ReconcileResult:
    """Pure reconciliation over already-fetched, raw cell rows.

    Split out from the DB-fetching wrapper so the comparison logic is unit
    testable WITHOUT a live stack. ``columns`` is the column order of each row;
    ``pk_indexes`` are the indexes within that order that form the primary key.
    """
    norms = column_normalizers or {}

    def keyed(rows: list[list[str]]) -> dict[tuple, list[Optional[str]]]:
        out: dict[tuple, list[Optional[str]]] = {}
        for cells in rows:
            canon = [canon_cell(cells[i], norms.get(columns[i])) for i in range(len(columns))]
            pk = tuple(canon[i] for i in pk_indexes)
            out[pk] = canon
        return out

    src = keyed(source_rows)
    dst = keyed(dest_rows)

    src_keys = set(src)
    dst_keys = set(dst)
    missing = sorted(src_keys - dst_keys, key=lambda t: tuple("" if x is None else x for x in t))
    extra = sorted(dst_keys - src_keys, key=lambda t: tuple("" if x is None else x for x in t))

    value_diffs: list[ValueDiff] = []
    for pk in sorted(src_keys & dst_keys, key=lambda t: tuple("" if x is None else x for x in t)):
        s_row = src[pk]
        d_row = dst[pk]
        for i, col in enumerate(columns):
            if s_row[i] != d_row[i]:
                value_diffs.append(ValueDiff(pk=pk, column=col, source=s_row[i], dest=d_row[i]))

    ok = (
        len(source_rows) == len(dest_rows)
        and not missing
        and not extra
        and not value_diffs
    )
    return ReconcileResult(
        ok=ok,
        source_count=len(source_rows),
        dest_count=len(dest_rows),
        missing_pks=missing,
        extra_pks=extra,
        value_diffs=value_diffs,
    )


def reconcile(
    *,
    source: DBHandle,
    dest: DBHandle,
    source_table: str,
    dest_table: str,
    pk_cols: list[str],
    columns: list[str],
    source_schema: str = "public",
    dest_schema: str = "public",
    column_normalizers: Optional[dict[str, Callable[[str], str]]] = None,
) -> ReconcileResult:
    """Reconcile ``source_table`` against ``dest_table`` at the value level.

    ``columns`` must list every column to compare, in a stable order, and must
    include ``pk_cols``. Both sides are fetched ordered by PK and compared cell
    by cell after canonicalisation.
    """
    missing_pk = [c for c in pk_cols if c not in columns]
    if missing_pk:
        raise ValueError(f"pk_cols {missing_pk} not present in columns {columns}")

    source_rows = _fetch_rows(source, source_schema, source_table, pk_cols, columns)
    dest_rows = _fetch_rows(dest, dest_schema, dest_table, pk_cols, columns)
    pk_indexes = [columns.index(c) for c in pk_cols]
    return reconcile_rows(source_rows, dest_rows, columns, pk_indexes, column_normalizers)


def assert_reconciled(
    *,
    source: DBHandle,
    dest: DBHandle,
    source_table: str,
    dest_table: str,
    pk_cols: list[str],
    columns: list[str],
    source_schema: str = "public",
    dest_schema: str = "public",
    column_normalizers: Optional[dict[str, Callable[[str], str]]] = None,
) -> ReconcileResult:
    """Assert the destination is a faithful value-level copy of the source.

    Raises ``AssertionError`` with a precise single-line reason on any mismatch.
    Returns the ``ReconcileResult`` on success (callers may log the counts).
    """
    result = reconcile(
        source=source,
        dest=dest,
        source_table=source_table,
        dest_table=dest_table,
        pk_cols=pk_cols,
        columns=columns,
        source_schema=source_schema,
        dest_schema=dest_schema,
        column_normalizers=column_normalizers,
    )
    assert result.ok, (
        f"reconcile {source_schema}.{source_table} -> {dest_schema}.{dest_table}: "
        + result.reason()
    )
    return result
