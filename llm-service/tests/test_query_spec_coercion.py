from src.text2sql.normalizer import coerce_query_spec_payload
from src.text2sql.query_spec import QuerySpec, JoinSpec
from src.text2sql.normalizer import normalize_query_spec
from src.text2sql.schema import TypedSchema, TableSchema, ColumnSchema


def test_coerce_select_columns_nested_object():
    raw = {
        "intent_description": "monthly spend",
        "from_table": "orders",
        "select_columns": [
            {"column": {"column": "amount_cents", "table": "orders", "aggregation": "sum"}, "alias": "total"},
        ],
        "group_by": [{"column": "MONTH(created_at)"}],
        "order_by": [{"column": "created_at", "direction": None}],
        "preferred_join_path": ["public.orders", "customer"],
    }

    coerced = coerce_query_spec_payload(raw)
    spec = QuerySpec.model_validate(coerced)
    assert spec.select_columns[0].column == "amount_cents"
    assert spec.select_columns[0].aggregation.value == "sum"
    assert spec.order_by[0].direction.value == "asc"
    assert isinstance(spec.preferred_join_path, str)


def test_coerce_empty_time_grain_is_removed():
    raw = {
        "intent_description": "count customers",
        "from_table": "fintech_customers",
        "select_columns": [{"column": "*", "aggregation": "count"}],
        "time_grain": "",
    }
    coerced = coerce_query_spec_payload(raw)
    assert "time_grain" not in coerced
    spec = QuerySpec.model_validate(coerced)
    assert spec.time_grain is None


def test_normalize_infers_missing_from_table_alias_from_join_conditions():
    schema = TypedSchema(
        tables=[
            TableSchema(name="orders", schema_name="public", columns=[ColumnSchema(name="customer_id", type="int")]),
            TableSchema(name="customers", schema_name="public", columns=[ColumnSchema(name="id", type="int")]),
        ]
    )
    spec = QuerySpec(
        intent_description="join",
        from_table="orders",
        joins=[JoinSpec(table="customers", alias="c", on_conditions=["o.customer_id = c.id"])],
    )
    out = normalize_query_spec(spec, schema)
    assert out.from_table_alias == "o"


from src.text2sql.compiler import compile_query_spec
from src.text2sql.query_spec import Filter, FilterOperator, SelectColumn


def test_numeric_filter_rejects_predicate_injection():
    spec = QuerySpec(from_table='orders', select_columns=[SelectColumn(column='*')],
                     filters=[Filter(column='amount', operator=FilterOperator.GT, value='0 OR 1=1')], limit=100)
    sql = compile_query_spec(spec, 'postgres')
    assert 'OR 1=1' not in sql.replace("'0 OR 1=1'", '')  # value is quoted, not raw
    assert "amount > '0 OR 1=1'" in sql


def test_numeric_filter_preserves_numeric_string():
    spec = QuerySpec(from_table='orders', select_columns=[SelectColumn(column='*')],
                     filters=[Filter(column='amount', operator=FilterOperator.GT, value='5')], limit=100)
    assert 'amount > 5' in compile_query_spec(spec, 'postgres')


def test_bool_eq_filter_renders_sql_boolean_not_int():
    # Regression: bool is a subclass of int, so True must render as the SQL
    # boolean TRUE, not the integer 1 (which errors on Postgres boolean columns
    # with "operator does not exist: boolean = integer").
    spec = QuerySpec(from_table='users', select_columns=[SelectColumn(column='*')],
                     filters=[Filter(column='is_active', operator=FilterOperator.EQ, value=True)], limit=100)
    sql = compile_query_spec(spec, 'postgres')
    assert 'is_active = TRUE' in sql
    assert 'is_active = 1' not in sql

