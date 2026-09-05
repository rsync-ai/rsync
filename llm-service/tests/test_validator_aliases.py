from src.text2sql.schema import TypedSchema, TableSchema, ColumnSchema
from src.text2sql.validator import validate_sql


def test_validator_allows_join_alias_column_refs():
    schema = TypedSchema(
        tables=[
            TableSchema(
                name="orders",
                schema_name="public",
                columns=[
                    ColumnSchema(name="customer_id", type="int"),
                    ColumnSchema(name="amount_cents", type="int"),
                ],
            ),
            TableSchema(
                name="customers",
                schema_name="public",
                columns=[
                    ColumnSchema(name="id", type="int"),
                    ColumnSchema(name="name", type="varchar"),
                ],
            ),
        ]
    )

    sql = (
        "SELECT customer.name, SUM(orders.amount_cents) "
        "FROM orders INNER JOIN customers AS customer ON orders.customer_id = customer.id "
        "GROUP BY customer.name LIMIT 10"
    )
    res = validate_sql(sql, "mysql", schema=schema, spec=None, require_limit=True)
    assert not res.errors



def _schema(*tables):
    return TypedSchema(tables=list(tables))


def test_validator_allows_cte_name_and_projected_columns():
    # Bug A: CTE name is a valid relation; its projected (derived) columns are valid unqualified.
    schema = _schema(
        TableSchema(name="orders", schema_name="public", columns=[
            ColumnSchema(name="created_at", type="timestamp"),
            ColumnSchema(name="total_amount", type="decimal"),
        ]),
    )
    sql = (
        "WITH monthly_revenue AS ("
        "  SELECT DATE_FORMAT(created_at, '%Y-%m') AS year_month, SUM(total_amount) AS total_revenue "
        "  FROM orders GROUP BY year_month) "
        "SELECT year_month, total_revenue FROM monthly_revenue ORDER BY year_month LIMIT 100"
    )
    res = validate_sql(sql, "mysql", schema=schema, spec=None, require_limit=True)
    assert not res.errors, [e.code for e in res.errors]


def test_validator_allows_cte_referenced_by_alias_in_outer_query():
    # Bug C: outer query aliases CTEs (FROM total_revenue tr JOIN distinct_products dp) and
    # references tr.col / dp.col. These resolve to CTEs (derived), not physical tables.
    schema = _schema(
        TableSchema(name="order_items", schema_name="public", columns=[
            ColumnSchema(name="product_id", type="int"),
            ColumnSchema(name="unit_price", type="decimal"),
            ColumnSchema(name="quantity", type="int"),
        ]),
        TableSchema(name="products", schema_name="public", columns=[
            ColumnSchema(name="id", type="int"),
            ColumnSchema(name="category_id", type="int"),
        ]),
        TableSchema(name="categories", schema_name="public", columns=[
            ColumnSchema(name="id", type="int"),
            ColumnSchema(name="name", type="varchar"),
        ]),
    )
    sql = (
        "WITH total_revenue AS ("
        "  SELECT c.name AS category_name, SUM(oi.unit_price * oi.quantity) AS total_revenue "
        "  FROM order_items oi JOIN products p ON oi.product_id = p.id "
        "  JOIN categories c ON p.category_id = c.id GROUP BY c.name), "
        "distinct_products AS ("
        "  SELECT c.name AS category_name, COUNT(DISTINCT p.id) AS product_count "
        "  FROM products p JOIN categories c ON p.category_id = c.id GROUP BY c.name) "
        "SELECT tr.category_name, tr.total_revenue, dp.product_count "
        "FROM total_revenue tr JOIN distinct_products dp ON tr.category_name = dp.category_name LIMIT 100"
    )
    res = validate_sql(sql, "postgres", schema=schema, spec=None, require_limit=True)
    assert not res.errors, [e.code for e in res.errors]


def test_validator_flags_out_of_scope_cte_internal_alias():
    # W1: alias `p` is defined only inside the CTE; referencing p.price in the OUTER SELECT is
    # out of scope and errors at execution (MySQL 1054). Must be flagged so the repair loop fixes it.
    schema = _schema(
        TableSchema(name="products", schema_name="public", columns=[
            ColumnSchema(name="id", type="int"),
            ColumnSchema(name="name", type="varchar"),
            ColumnSchema(name="price", type="decimal"),
            ColumnSchema(name="category_id", type="int"),
        ]),
        TableSchema(name="categories", schema_name="public", columns=[
            ColumnSchema(name="id", type="int"),
            ColumnSchema(name="name", type="varchar"),
        ]),
    )
    sql = (
        "WITH ranked AS ("
        "  SELECT c.name AS category_name, p.name AS product_name, p.price, "
        "  RANK() OVER (PARTITION BY c.id ORDER BY p.price DESC) AS rnk "
        "  FROM products p JOIN categories c ON p.category_id = c.id) "
        "SELECT category_name, product_name, p.price, rnk FROM ranked WHERE rnk <= 3 LIMIT 100"
    )
    res = validate_sql(sql, "mysql", schema=schema, spec=None, require_limit=True)
    assert any(e.code == "OUT_OF_SCOPE_REFERENCE" for e in res.errors), [e.code for e in res.errors]


def test_validator_allows_correlated_subquery():
    # Regression for the W1 scope check: a correlated subquery references the OUTER table — valid,
    # must NOT be flagged out-of-scope.
    schema = _schema(
        TableSchema(name="users", schema_name="public", columns=[ColumnSchema(name="id", type="int")]),
        TableSchema(name="orders", schema_name="public", columns=[ColumnSchema(name="user_id", type="int")]),
    )
    sql = "SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id) LIMIT 100"
    res = validate_sql(sql, "postgres", schema=schema, spec=None, require_limit=True)
    assert not res.errors, [e.code for e in res.errors]
