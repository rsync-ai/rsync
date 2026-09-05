"""Regression tests for the sensitive-data log scrubber in base_connector.

The Query/Import leak sites log raw DB-driver exceptions (which embed customer
row values, e.g. Postgres "Key (email)=(a@b.com)" / "Failing row contains (…)")
to the log backend. scrub_sensitive + SensitiveDataScrubbingFilter must redact
those from the message, %-args, and exception traceback while leaving
diagnostic context (table names, timestamps, trace ids) intact.
"""

from __future__ import annotations

import importlib.util
import logging
import os
import sys

_BC_PATH = os.path.join(os.path.dirname(__file__), "..", "base_connector.py")
_spec = importlib.util.spec_from_file_location("base_connector_scrub", _BC_PATH)
_bc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_bc)


# --- scrub_sensitive ---------------------------------------------------------

def test_scrub_redacts_row_values_and_secrets():
    cases = {
        "duplicate key value violates unique constraint; Key (email)=(jane@acme.com)": "jane@acme.com",
        "null value ... Failing row contains (1, jane@acme.com, 555-12-3456)": "jane@acme.com",
        "INSERT INTO t VALUES ('Jane', 'jane@acme.com', 41)": "jane@acme.com",
        "connect failed: postgres://user:hunter2@db:5432/app": "hunter2",
        "auth error: Bearer sk-abcdefghijklmnopqrstuvwxyz012345": "sk-abcdefghijklmnopqrstuvwxyz012345",
        'config: {"password": "hunter2", "api_key": "AKIA1234567890"}': "hunter2",
    }
    for text, leaked in cases.items():
        out = _bc.scrub_sensitive(text)
        assert leaked not in out, f"leaked {leaked!r} in scrubbed output: {out!r}"
        assert "redacted" in out.lower(), f"nothing redacted in: {out!r}"


def test_scrub_preserves_diagnostic_context():
    # Table names, ISO timestamps, and 32-hex trace ids must survive scrubbing.
    text = 'relation "users" does not exist at 2026-07-17T10:00:00 trace=abc123def4567890abc123def4567890'
    out = _bc.scrub_sensitive(text)
    assert '"users"' in out
    assert "2026-07-17T10:00:00" in out


def test_scrub_handles_empty_and_none():
    assert _bc.scrub_sensitive("") == ""
    assert _bc.scrub_sensitive(None) == ""


# --- SensitiveDataScrubbingFilter --------------------------------------------

def _emit(msg, args=None, exc_info=None):
    """Run a record through the filter and return what a handler would emit."""
    rec = logging.LogRecord("mcp.test", logging.ERROR, __file__, 1, msg, args, exc_info)
    assert _bc.SensitiveDataScrubbingFilter().filter(rec) is True
    return logging.Formatter("%(message)s").format(rec)


def test_filter_scrubs_message():
    # Mirrors base_connector.py:4338 `logger.error(f"Query execution failed: {e}")`
    out = _emit("Query execution failed: Key (email)=(jane@acme.com)")
    assert "jane@acme.com" not in out
    assert "[redacted]" in out


def test_filter_scrubs_percent_args():
    out = _emit("Import failed: %s", ("Failing row contains (1, secret_val, 42)",))
    assert "secret_val" not in out


def test_filter_scrubs_exception_traceback():
    try:
        raise ValueError("Key (email)=(jane@acme.com)")
    except ValueError:
        ei = sys.exc_info()
    out = _emit("insert failed", None, ei)
    assert "jane@acme.com" not in out


def test_filter_never_raises_on_bad_record():
    # A non-string msg must not break logging.
    rec = logging.LogRecord("mcp.test", logging.ERROR, __file__, 1, {"obj": 1}, None, None)
    assert _bc.SensitiveDataScrubbingFilter().filter(rec) is True
