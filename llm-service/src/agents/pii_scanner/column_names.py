"""Name-only PII classification for the async schema scan.

The async scan (``pii.scan.request``) is handed column names and declared types
and nothing else: no row value leaves the source for it. So the question it can
answer is what a column is *meant* to hold, read from its name.

The vocabulary is the API gateway's ``isPIIColumnName``
(api-gateway/internal/handlers/connections.go), which preview redaction already
uses, so a column the preview hides is the column the scan reports. Its tests
(pii_column_name_test.go) pin the false positives that shaped it; the same
cases are pinned here in test_pii_scan_names_only.py. Adding a keyword to one
without the other makes the two screens disagree about the same column.
"""

import re
from typing import List, Optional, Tuple

# A name is evidence of intent, not of content: `email` can be empty, and a
# free-text `notes` column can hold an email the name never mentions. Reported
# as one fixed value so nobody reads a measured score into it.
NAME_MATCH_CONFIDENCE = 0.6

DETECTION_METHOD = "column_name"

# (keyword, pii_type), checked in order; the first match wins. Types are the
# scanner's own PIIType values, except national_id, which PIIType has no member
# for and which the orchestrator's piiTypeForColumn already reports under that
# name.
_KEYWORDS: List[Tuple[str, str]] = [
    # Contact
    ("email", "email"),
    ("e-mail", "email"),
    ("phone", "phone"),
    ("mobile", "phone"),
    ("cell", "phone"),
    ("msisdn", "phone"),
    # Government / identity numbers
    ("ssn", "ssn"),
    ("social_security", "ssn"),
    ("social-security", "ssn"),
    ("national_id", "national_id"),
    ("national-id", "national_id"),
    ("id_number", "national_id"),
    ("id-number", "national_id"),
    ("passport", "passport"),
    ("driver_license", "driver_license"),
    ("driver_licence", "driver_license"),
    ("drivers_license", "driver_license"),
    ("drivers_licence", "driver_license"),
    ("tax_id", "national_id"),
    ("tax-id", "national_id"),
    ("tin", "national_id"),
    ("aadhar", "national_id"),
    ("aadhaar", "national_id"),
    ("nin", "national_id"),
    ("sin", "national_id"),
    # Payments
    ("credit_card", "credit_card"),
    ("cc_number", "credit_card"),
    ("card_number", "credit_card"),
    ("pan", "credit_card"),
]

# Short keywords that are also parts of ordinary words (dis-TIN-ct, com-PAN-y,
# run-NIN-g, u-SIN-g, ex-CELL-ent) only count as a whole token.
_AMBIGUOUS = {"tin", "nin", "sin", "pan", "cell"}

_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z])(?=[A-Z])")
_NON_LETTER = re.compile(r"[^A-Za-z]+")


def column_name_tokens(name: str) -> List[str]:
    """Lower-case word tokens, split on separators, digits and camelCase:
    ``customerPAN2`` -> ``[customer, pan]``. Mirrors the gateway's
    columnNameTokens."""
    spaced = _CAMEL_BOUNDARY.sub(" ", name.strip())
    return [t.lower() for t in _NON_LETTER.split(spaced) if t]


def is_boolean_type(data_type: Optional[str]) -> bool:
    """A boolean is a flag about PII (``email_verified``), never the datum, and
    masking it breaks the destination write. Same rule as the suggestions
    service's ``_is_boolean_column_type``."""
    t = str(data_type or "").strip().lower()
    return t == "bit" or t == "tinyint(1)" or t.startswith("bool")


def pii_type_for_column_name(name: str, data_type: Optional[str] = None) -> Optional[str]:
    """The PII type a column's name says it holds, or None."""
    if not name or is_boolean_type(data_type):
        return None
    lower = name.strip().lower()
    tokens: Optional[List[str]] = None
    for keyword, pii_type in _KEYWORDS:
        if keyword in _AMBIGUOUS:
            if tokens is None:
                tokens = column_name_tokens(name)
            if keyword in tokens:
                return pii_type
            continue
        if keyword in lower:
            return pii_type
    return None
