"""LLM privacy scrubber tests — Python mirror of pkg/llmscrub (Go).

Contract: row values, credentials, and PII in error text must never reach an
LLM prompt; schema metadata and error shape must survive scrubbing.
All fixtures are synthetic.
"""

from src.utils.masking import scrub_error_for_llm


def test_scrub_postgres_duplicate_key_detail():
    out = scrub_error_for_llm(
        'ERROR: duplicate key value violates unique constraint "users_pkey" '
        "DETAIL: Key (email)=(jane.doe@acme.com) already exists."
    )
    assert "jane.doe@acme.com" not in out
    assert '"users_pkey"' in out
    assert "Key (email)=" in out


def test_scrub_insert_values_list():
    out = scrub_error_for_llm(
        "dest write failed: INSERT INTO public.customers (id, ssn, name) "
        "VALUES (9384756, '078-05-1120', 'Jane Doe'), (9384757, '078-05-1121', 'John Doe')"
    )
    assert "078-05-1120" not in out
    assert "Jane Doe" not in out
    assert "9384756" not in out
    assert "INSERT INTO public.customers (id, ssn, name)" in out


def test_scrub_url_credentials():
    out = scrub_error_for_llm("dial failed: postgres://appuser:S3cr3tPass@db.acme.internal:5432/prod")
    assert "S3cr3tPass" not in out
    assert "db.acme.internal:5432/prod" in out


def test_scrub_kv_credentials_and_email():
    out = scrub_error_for_llm("auth failed password=hunter22 for bob@customer.io id 88812345678")
    assert "hunter22" not in out
    assert "bob@customer.io" not in out
    assert "88812345678" not in out
    assert "auth failed" in out


def test_scrub_failing_row_detail():
    out = scrub_error_for_llm(
        'ERROR: null value in column "email" violates not-null constraint '
        "DETAIL: Failing row contains (42, Jane Doe, 555-0182, 123 Main St, jane)."
    )
    assert "Jane Doe" not in out
    assert "555-0182" not in out
    assert '"email"' in out
    assert "Failing row contains (" in out


def test_scrub_colon_double_quoted_value():
    out = scrub_error_for_llm('invalid input syntax for type integer: "jane-ssn-078-05-1120"')
    assert "jane-ssn" not in out
    assert "invalid input syntax for type integer" in out


def test_scrub_bearer_and_bare_jwt():
    out = scrub_error_for_llm(
        "401 Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiI0MiJ9.c2lnbmF0dXJl"
    )
    assert "eyJhbGciOiJIUzI1NiJ9" not in out
    assert "c2lnbmF0dXJl" not in out
    out2 = scrub_error_for_llm("token rejected eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiI0MiJ9.c2ln upstream")
    assert "eyJzdWIiOiI0MiJ9" not in out2


def test_scrub_json_record_and_dashed_pii():
    out = scrub_error_for_llm('sink reject: {"name": "Jane Doe", "ssn": "078-05-1120", "age": 41}')
    assert "Jane Doe" not in out
    assert "078-05-1120" not in out
    assert ": 41" not in out
    out2 = scrub_error_for_llm("rejected ssn 078-05-1120 phone 415-555-0182")
    assert "078-05-1120" not in out2
    assert "415-555-0182" not in out2


def test_scrub_truncation_reopened_quote():
    # Upstream truncation cut the closing quote off — the tail must still mask.
    out = scrub_error_for_llm("Duplicate entry 'jane.doe@acme.com for key uq_em")
    assert "jane.doe@acme.com" not in out
    assert "Duplicate entry" in out


def test_scrub_preserves_harmless_diagnostics():
    for msg in [
        "context deadline exceeded",
        "connection refused",
        'relation "public.orders" does not exist',
        "read 15000 rows, landed 0 rows (silent_drop)",
        "port 5432 unreachable",
        "last event at 2026-07-02 14:10:33 (3600 seconds ago)",
    ]:
        assert scrub_error_for_llm(msg) == msg


def test_scrub_truncates_after_scrubbing():
    out = scrub_error_for_llm("x" * 600 + " bob@x.io", max_len=100)
    assert len(out) <= 101
    assert "bob@x.io" not in out


def test_scrub_empty_and_none():
    assert scrub_error_for_llm("") == ""
    assert scrub_error_for_llm(None) == ""


def test_result_profile_rejects_row_data():
    """The explorer next-steps request must refuse row-bearing payloads."""
    import pytest
    from src.agents.explorer.api import ResultProfile

    ResultProfile(row_count=10, columns=["a", "b"])  # metadata-only: OK
    with pytest.raises(Exception):
        ResultProfile(row_count=10, columns=["a"], sample_rows=[{"a": 1}])
