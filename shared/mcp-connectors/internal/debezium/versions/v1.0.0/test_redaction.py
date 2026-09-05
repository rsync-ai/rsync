#!/usr/bin/env python3
"""Unit tests for credential redaction in the Debezium MCP connector.

These guard the fix that stops the source DB password from riding up into the
orchestrator's tool-result (and from there into logs / SigNoz). The raw config
still POSTs to Kafka Connect unchanged — only what the connector RETURNS to the
caller is redacted.

Run: python3 -m pytest test_redaction.py   (or) python3 test_redaction.py
"""
import connector


def test_redact_config_masks_password():
    cfg = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "db.example.com",
        "database.user": "svc_reader",
        "database.password": "sup3r-s3cret-value",
        "topic.prefix": "cdc-abc123",
    }
    out = connector._redact_config(cfg)
    # secret masked
    assert out["database.password"] == connector._REDACTED
    assert "sup3r-s3cret-value" not in str(out)
    # non-secret keys preserved verbatim (debuggability retained)
    assert out["database.hostname"] == "db.example.com"
    assert out["database.user"] == "svc_reader"
    assert out["connector.class"].endswith("PostgresConnector")
    # original config is NOT mutated (POST payload must keep the real password)
    assert cfg["database.password"] == "sup3r-s3cret-value"


def test_redact_config_covers_all_secret_markers():
    cfg = {
        "database.password": "p1",
        "schema.history.internal.consumer.sasl.jaas.config": "org.x required password=\"p2\";",
        "some.token": "t1",
        "vault.secret": "s1",
        "ssl.keystore.credential": "c1",
        "ssl.private.key": "k1",
        "plain.setting": "keep-me",
    }
    out = connector._redact_config(cfg)
    for k in ("database.password", "some.token", "vault.secret",
              "ssl.keystore.credential", "ssl.private.key",
              "schema.history.internal.consumer.sasl.jaas.config"):
        assert out[k] == connector._REDACTED, f"{k} should be redacted"
    assert out["plain.setting"] == "keep-me"


def test_redact_config_masks_mongodb_uri_password():
    # MongoDB puts the source password inside a URI VALUE under a key whose name
    # ("mongodb.connection.string") matches none of the sensitive-key markers.
    cfg = {
        "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
        "mongodb.connection.string": "mongodb://svc_user:s3cr3t%40pass@cluster0.mongodb.net:27017/?tls=true&authSource=admin",
        "collection.include.list": "sales.orders",
    }
    out = connector._redact_config(cfg)
    blob = str(out)
    assert "s3cr3t%40pass" not in blob, f"mongodb password leaked: {blob}"
    assert "svc_user" not in blob, f"mongodb userinfo leaked: {blob}"
    assert connector._REDACTED in out["mongodb.connection.string"]
    # host/db kept for debuggability
    assert "cluster0.mongodb.net" in out["mongodb.connection.string"]
    assert out["collection.include.list"] == "sales.orders"


def test_mask_uri_credentials_variants():
    m = connector._mask_uri_credentials
    assert "pw" not in m("mongodb+srv://u:pw@host/db")
    # no credentials → unchanged (host:port must not be mistaken for user:pass)
    assert m("mongodb://host:27017/db") == "mongodb://host:27017/db"
    assert m("postgresql://a:b@h:5432/d").count("@") == 1 and "b" not in m("postgresql://a:b@h:5432/d")
    # non-URI / non-string passthrough
    assert m("just text") == "just text"
    assert m(1234) == 1234


def test_redact_config_leaves_empty_values_alone():
    # an empty/absent password should not become "***REDACTED***" (misleading)
    cfg = {"database.password": "", "database.user": "u"}
    out = connector._redact_config(cfg)
    assert out["database.password"] == ""


def test_redact_config_non_dict():
    assert connector._redact_config(None) == {}
    assert connector._redact_config("nope") == {}


def test_scrub_secrets_removes_literal():
    body = 'Invalid value "hunter2pass" for database.password on host db1'
    out = connector._scrub_secrets(body, "hunter2pass", "svc_user")
    assert "hunter2pass" not in out
    assert connector._REDACTED in out


def test_scrub_secrets_ignores_short_or_empty():
    # short/empty secrets must not nuke unrelated text
    body = "connection to host ab failed"
    assert connector._scrub_secrets(body, "", "ab") == body
    assert connector._scrub_secrets("", "whatever") == ""


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
