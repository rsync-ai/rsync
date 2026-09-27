"""The async PII scan classifies columns by name, and answers the gateway in the
shape shared/pii_scan_contract_golden.json pins.

KI-PII-ASYNC-SCAN-ALWAYS-FAILS had two halves. The consumer read
``result.tables_scanned``, an attribute SchemaScanResult never had, so every
scan went out as failed. And the gateway sent bare table names, so the scanner
had no column to look at and would have reported every table as scanned and
clean, which the gateway's prune reads as "delete this table's findings".
Fixing the first half alone turns the second into data loss, so both are
pinned here together, against the same file the gateway's
pii_scan_contract_test.go reads.
"""

import json
import os

import pytest

from src.agents.pii_scanner.column_names import (
    column_name_tokens,
    pii_type_for_column_name,
)
from src.agents.pii_scanner.kafka_consumer import (
    handle_scan_request,
    table_requests_from_payload,
)
from src.agents.pii_scanner.service import PIIScannerService

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_GOLDEN_PATH = os.path.join(REPO_ROOT, "shared/pii_scan_contract_golden.json")


def _golden() -> dict:
    with open(_GOLDEN_PATH) as f:
        return json.load(f)


class _NoDetector:
    """Stands in for the ML detector: a names-only scan must never touch it
    (loading Presidio cold takes seconds and it has nothing to read)."""

    def __getattr__(self, name):
        raise AssertionError(f"detector.{name} was used by a scan with no samples")


def _names_only_service() -> PIIScannerService:
    service = PIIScannerService()
    service.detector = _NoDetector()
    return service


# The cases api-gateway/internal/handlers/pii_column_name_test.go pins for
# isPIIColumnName, which preview redaction uses. The two must agree, or the
# preview hides a column the scan calls clean.
@pytest.mark.parametrize(
    "name,is_pii",
    [
        ("distinct_ids", False),
        ("distinct_count", False),
        ("routing_key", False),
        ("setting_name", False),
        ("running_total", False),
        ("warning_count", False),
        ("using_index", False),
        ("business_unit", False),
        ("company_name", False),
        ("expansion", False),
        ("excellent_score", False),
        ("max_id", False),
        ("id", False),
        ("user_id", False),
        ("created_at", False),
        ("count(DISTINCT product_id)", False),
        ("COUNT(DISTINCT product_id)", False),
        ("count(*)", False),
        ("min(product_id)", False),
        ("max(product_id)", False),
        ("tin", True),
        ("tin_number", True),
        ("taxpayer_tin", True),
        ("customerTin", True),
        ("tin2", True),
        ("TIN", True),
        ("pan", True),
        ("pan_number", True),
        ("customerPAN", True),
        ("nin", True),
        ("sin", True),
        ("cell", True),
        ("cell_phone", True),
        ("email", True),
        ("user_email_address", True),
        ("e-mail", True),
        ("phone_number", True),
        ("mobile", True),
        ("msisdn", True),
        ("ssn", True),
        ("social_security_number", True),
        ("national_id", True),
        ("id_number", True),
        ("passport_number", True),
        ("drivers_license", True),
        ("tax_id", True),
        ("aadhaar", True),
        ("credit_card", True),
        ("cc_number", True),
        ("card_number", True),
    ],
)
def test_column_name_classification_matches_the_gateway(name, is_pii):
    assert (pii_type_for_column_name(name) is not None) is is_pii


@pytest.mark.parametrize(
    "name,tokens",
    [
        ("distinct_ids", ["distinct", "ids"]),
        ("customerPAN", ["customer", "pan"]),
        ("customerTin", ["customer", "tin"]),
        ("tin2", ["tin"]),
        ("  tin  ", ["tin"]),
        ("tax-id", ["tax", "id"]),
        ("a.b c", ["a", "b", "c"]),
        ("", []),
        ("123", []),
    ],
)
def test_tokens_match_the_gateway(name, tokens):
    assert column_name_tokens(name) == tokens


@pytest.mark.parametrize("data_type", ["boolean", "bool", "BIT", "tinyint(1)"])
def test_a_boolean_named_like_pii_is_a_flag_not_the_datum(data_type):
    assert pii_type_for_column_name("email_verified", data_type) is None
    assert pii_type_for_column_name("email_verified", "text") == "email"


def test_the_response_to_the_contract_request_is_the_contract_response():
    golden = _golden()
    assert golden["request"]["tables"], "golden request has no tables"

    response = handle_scan_request(golden["request"], _names_only_service().scan_schema)

    assert response == golden["response"]


def test_a_table_with_no_columns_is_an_error_not_a_clean_table():
    request = {
        "scan_id": "s-1",
        "connection_id": "c-1",
        "tables": [{"table_name": "public.archived", "columns": []}],
    }

    response = handle_scan_request(request, _names_only_service().scan_schema)

    assert response["status"] == "completed"
    assert response["result"]["tables"] == []
    assert response["result"]["tables_scanned"] == 0
    assert response["result"]["errors"] == [
        "Table public.archived: no columns were supplied, so it was not scanned"
    ]


def test_bare_table_names_from_an_older_gateway_are_not_scanned():
    # The message the gateway sent before this change.
    request = {"scan_id": "s-1", "connection_id": "c-1", "tables": ["public.users", "orders"]}

    response = handle_scan_request(request, _names_only_service().scan_schema)

    assert response["result"]["tables"] == []
    assert len(response["result"]["errors"]) == 2


def test_malformed_table_entries_are_skipped():
    tables = table_requests_from_payload(
        [
            None,
            42,
            {"columns": [{"column_name": "email"}]},
            {"table_name": "public.t", "columns": [None, {"data_type": "text"}, {"column_name": "email", "data_type": ""}]},
        ]
    )

    assert [t.table_name for t in tables] == ["public.t"]
    assert [(c.column_name, c.samples, c.data_type) for c in tables[0].columns] == [("email", [], None)]


def test_a_scan_that_raises_goes_out_as_failed():
    def broken(_request):
        raise RuntimeError("scan exploded")

    response = handle_scan_request({"scan_id": "s-9", "connection_id": "c-1", "tables": []}, broken)

    assert response["status"] == "failed"
    assert response["agent"] == "pii_scanner"
    assert response["result"] == {"scan_id": "s-9"}
    assert response["error"] == "scan exploded"
