#!/usr/bin/env python3
"""Guard: _enforce_config_precedence must not log api_key/token VALUES.

Connector stdout ships to SigNoz, so a cleartext credential in the override
warning would be persisted. This mirrors the Go twin's security.IsSensitiveKey
guard in backend-orchestrator/internal/mcp/client.go. Non-secret keys (host,
user, port, database) still log their values for debuggability.

Run: python3 test_config_precedence_masking.py
"""
import base_connector


class _Probe(base_connector.BaseMCPConnector):
    """Minimal concrete connector that records log() calls instead of emitting them."""

    def __init__(self):
        super().__init__()
        self.logged = []

    def log(self, message, level="info"):  # override — capture, don't emit
        self.logged.append(str(message))

    # abstract-method stubs (unused by this test)
    def test_connection(self, params=None):
        return {}

    def discover_schema(self, params=None):
        return {}

    def validate_config(self, params=None):
        return {}

    def export(self, params):
        return {}


def _run(params):
    p = _Probe()
    p._enforce_config_precedence(params)
    return p.logged


def test_api_key_value_not_logged():
    SECRET = "sk_live_SUPERSECRET_9f8e7d6c"
    logs = _run({"config": {"api_key": SECRET}, "api_key": "old_plan_key"})
    joined = "\n".join(logs)
    assert SECRET not in joined, f"api_key value leaked into log: {joined}"
    assert "old_plan_key" not in joined, f"prior api_key value leaked: {joined}"
    # the override still gets logged, just masked
    assert any("api_key" in m and "masked" in m for m in logs), logs


def test_token_value_not_logged():
    TOK = "ghp_TOKEN_abcdef123456"
    logs = _run({"config": {"token": TOK}, "token": "old_token"})
    joined = "\n".join(logs)
    assert TOK not in joined and "old_token" not in joined, f"token leaked: {joined}"


def test_non_sensitive_keys_still_log_values():
    # host is protected but NOT a secret — its value should remain visible.
    logs = _run({"config": {"host": "db.prod.internal"}, "host": "db.old.internal"})
    joined = "\n".join(logs)
    assert "db.prod.internal" in joined and "db.old.internal" in joined, \
        f"non-secret host value should still be logged: {joined}"


def test_no_conflict_no_log():
    # identical values → no override warning at all
    logs = _run({"config": {"api_key": "same"}, "api_key": "same"})
    assert logs == [], f"expected no logs when values match, got: {logs}"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"PASS {fn.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    raise SystemExit(1 if failed else 0)
