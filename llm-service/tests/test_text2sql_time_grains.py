import pytest

import sqlglot

from src.text2sql.schema import TypedSchema, TableSchema, ColumnSchema
from src.text2sql.query_spec import QuerySpec, SelectColumn, TimeGrain, AggregationType, JoinSpec
from src.text2sql.compiler import compile_query_spec, resolve_dialect


def _schema_orders() -> TypedSchema:
    return TypedSchema(
        tables=[
            TableSchema(
                name="orders",
                schema_name="public",
                columns=[
                    ColumnSchema(name="id", type="int", is_primary_key=True, is_nullable=False),
                    ColumnSchema(name="created_at", type="datetime", is_nullable=False),
                    ColumnSchema(name="amount", type="decimal", is_nullable=False),
                ],
                primary_key=["id"],
            )
        ]
    )


@pytest.mark.parametrize(
    "dialect,grain,expect_substrings",
    [
        ("mysql", TimeGrain.MONTH, ["DATE_FORMAT", "%Y-%m-01", "GROUP BY 1"]),
        ("mysql", TimeGrain.DAY, ["DATE(", "GROUP BY 1"]),
        ("postgresql", TimeGrain.MONTH, ["DATE_TRUNC", "'month'", "GROUP BY 1"]),
        ("snowflake", TimeGrain.MONTH, ["DATE_TRUNC", "'month'", "GROUP BY 1"]),
        ("bigquery", TimeGrain.MONTH, ["TIMESTAMP_TRUNC", "MONTH", "GROUP BY 1"]),
        ("clickhouse", TimeGrain.MONTH, ["toStartOfMonth", "GROUP BY 1"]),
    ],
)
def test_time_grain_compilation(dialect, grain, expect_substrings):
    schema = _schema_orders()
    spec = QuerySpec(
        intent_description="test",
        from_table="orders",
        select_columns=[SelectColumn(column="amount", table="orders", aggregation=AggregationType.SUM, alias="total")],
        time_column="created_at",
        time_grain=grain,
        limit=100,
    )

    sql = compile_query_spec(spec, dialect, schema)
    assert sql.strip().upper().startswith("SELECT")

    # Must be parseable by sqlglot for the dialect
    sqlglot.parse_one(sql, dialect=resolve_dialect(dialect))

    upper = sql.upper()
    for s in expect_substrings:
        assert s.upper() in upper


def test_join_month_group_by_customer_name_mysql():
    schema = TypedSchema(
        tables=[
            TableSchema(
                name="orders",
                schema_name="public",
                columns=[
                    ColumnSchema(name="id", type="int", is_primary_key=True, is_nullable=False),
                    ColumnSchema(name="customer_id", type="int", is_nullable=False),
                    ColumnSchema(name="created_at", type="datetime", is_nullable=False),
                    ColumnSchema(name="amount_cents", type="int", is_nullable=False),
                ],
            ),
            TableSchema(
                name="customers",
                schema_name="public",
                columns=[
                    ColumnSchema(name="id", type="int", is_primary_key=True, is_nullable=False),
                    ColumnSchema(name="name", type="varchar", is_nullable=False),
                ],
            ),
        ]
    )
    spec = QuerySpec(
        intent_description="monthly spend by customer",
        from_table="orders",
        select_columns=[
            SelectColumn(column="name", table="customers", alias="customer_name"),
            SelectColumn(column="amount_cents", table="orders", aggregation=AggregationType.SUM, alias="total_spend"),
        ],
        joins=[
            JoinSpec(table="customers", alias="customer", on_conditions=["orders.customer_id = customers.id"]),
        ],
        time_column="created_at",
        time_grain=TimeGrain.MONTH,
        limit=100,
    )

    sql = compile_query_spec(spec, "mysql", schema)
    # Ensure we don't double-qualify (customers.customer.name) and we respect join alias.
    assert "CUSTOMERS.CUSTOMER.NAME" not in sql.upper()
    assert "CUSTOMER.ID" in sql.upper()
    upper = sql.upper()
    # Ensure group by exists and includes customer name (MySQL only_full_group_by safety)
    assert "GROUP BY" in upper
    assert "CUSTOMER.NAME" in upper

