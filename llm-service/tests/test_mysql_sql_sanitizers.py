import pytest

from src.text2sql.mysql_sanitize import unqualify_builtin_functions, fix_only_full_group_by_order, quote_reserved_identifiers


@pytest.mark.parametrize(
    "sql,expected_contains",
    [
        (
            "SELECT fintech_transactions.DATE_FORMAT(fintech_transactions.created_at, '%Y-%m-01') AS m FROM fintech_transactions",
            "SELECT DATE_FORMAT(",
        ),
        (
            "SELECT `fintech_transactions`.`DATE_FORMAT`(`fintech_transactions`.`created_at`, '%Y-%m-01') AS m FROM `fintech_transactions`",
            "SELECT DATE_FORMAT(",
        ),
    ],
)
def test_unqualify_builtin_functions(sql, expected_contains):
    out = unqualify_builtin_functions(sql, qualifiers=["fintech_transactions", "public"])
    assert expected_contains in out
    assert "fintech_transactions.DATE_FORMAT" not in out
    assert "`fintech_transactions`.`DATE_FORMAT`" not in out


def test_fix_only_full_group_by_strips_invalid_order_by_terms():
    bad = (
        "SELECT DATE(fintech_transactions.created_at) AS day_bucket, "
        "SUM(fintech_transactions.amount_cents) "
        "FROM fintech_transactions "
        "GROUP BY 1 "
        "ORDER BY 1 ASC, "
        "CASE WHEN DATE_FORMAT(created_at, '%Y-%m-%d') IS NULL THEN 1 ELSE 0 END, "
        "DATE_FORMAT(created_at, '%Y-%m-%d') ASC "
        "LIMIT 100"
    )

    out = fix_only_full_group_by_order(bad)
    # Should keep ORDER BY 1 and drop the extra expressions.
    assert "ORDER BY 1" in out.upper()
    assert "CASE WHEN" not in out.upper()
    assert "DATE_FORMAT(CREATED_AT" not in out.upper()


def test_fix_only_full_group_by_noop_when_no_group_by():
    sql = "SELECT * FROM fintech_transactions ORDER BY created_at DESC LIMIT 10"
    assert fix_only_full_group_by_order(sql) == sql


def test_fix_only_full_group_by_keeps_aggregate_order_by_desc():
    # "top N by <measure>" must keep ORDER BY on the aggregate (alias OR expression),
    # preserving DESC — MySQL permits ordering by aggregates under only_full_group_by.
    # Regression guard for the silent "top N -> name-sorted" bug (KI-EXPLORER-TOPN).
    by_alias = (
        "SELECT p.name AS product_name, SUM(oi.qty * oi.price) AS total_revenue "
        "FROM order_items oi JOIN products p ON oi.product_id = p.id "
        "GROUP BY p.name ORDER BY total_revenue DESC LIMIT 10"
    )
    out = fix_only_full_group_by_order(by_alias).upper()
    assert "ORDER BY TOTAL_REVENUE DESC" in out
    assert "ORDER BY 1 ASC" not in out

    by_expr = (
        "SELECT p.name, SUM(oi.qty) AS total "
        "FROM order_items oi JOIN products p ON oi.product_id = p.id "
        "GROUP BY p.name ORDER BY SUM(oi.qty) DESC LIMIT 10"
    )
    out2 = fix_only_full_group_by_order(by_expr).upper()
    assert "DESC" in out2
    assert "SUM(OI.QTY)" in out2


def test_fix_only_full_group_by_still_clamps_nongrouped_nonaggregate_column():
    # Safety purpose preserved: a genuinely-unsafe ORDER BY on a non-grouped, non-aggregate
    # column must still clamp to ORDER BY 1.
    bad = (
        "SELECT p.name, p.category, SUM(x) AS t "
        "FROM products p GROUP BY p.name ORDER BY p.category LIMIT 10"
    )
    out = fix_only_full_group_by_order(bad).upper()
    assert "ORDER BY 1" in out
    assert "ORDER BY P.CATEGORY" not in out



def test_quote_reserved_identifiers_backticks_reserved_aliases():
    # Bug B: reserved-word aliases (e.g. `AS year_month`) must be backtick-quoted or MySQL errors 1064.
    sql = "SELECT DATE_FORMAT(created_at, '%Y-%m') AS year_month, COUNT(*) AS order_count FROM orders GROUP BY year_month"
    out = quote_reserved_identifiers(sql)
    assert "`year_month`" in out


def test_quote_reserved_identifiers_noop_on_empty_and_plain():
    # Empty/whitespace short-circuits unchanged; a non-reserved alias is left intact (not over-quoted).
    assert quote_reserved_identifiers("") == ""
    assert quote_reserved_identifiers("   ") == "   "
    out = quote_reserved_identifiers("SELECT COUNT(*) AS total FROM orders")
    assert "total" in out
    assert "`total`" not in out
