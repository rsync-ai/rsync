#!/usr/bin/env python3
"""Tests for FileConfigProvider secret externalization.

When DEBEZIUM_SECRETS_DIR is set, secret values move out of the connector config
into a per-connector .properties file (read by the Kafka Connect worker), leaving
only ${file:...} references in the config that gets POSTed to Connect — so the
plaintext never enters the config topic / REST config dump. When the env is unset,
the config stays inline (backward compatible).

Run: python3 test_externalize_secrets.py
"""
import os
import stat
import tempfile
import time

import connector


def _pg_cfg():
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "db.example.com",
        "database.user": "svc",
        "database.password": "P@ssw0rd-Very-Secret",
        "topic.prefix": "cdc-abc",
    }


def test_inline_when_env_unset():
    os.environ.pop("DEBEZIUM_SECRETS_DIR", None)
    cfg = _pg_cfg()
    out = connector.externalize_secrets("cdc-abc", cfg)
    assert out == cfg  # unchanged → inline behavior preserved
    assert out["database.password"] == "P@ssw0rd-Very-Secret"


def test_externalizes_password_to_file():
    with tempfile.TemporaryDirectory() as d:
        os.environ["DEBEZIUM_SECRETS_DIR"] = d
        try:
            cfg = _pg_cfg()
            out = connector.externalize_secrets("cdc-abc", cfg)
            # config now references the file, not the secret
            assert out["database.password"].startswith("${file:")
            assert out["database.password"].endswith(":database.password}")
            assert "P@ssw0rd-Very-Secret" not in str(out)
            # non-secret keys untouched
            assert out["database.hostname"] == "db.example.com"
            # the file holds the real secret and is world-readable (uid-1001 reader)
            path = connector._secret_file_path(d, "cdc-abc")
            assert os.path.exists(path)
            body = open(path).read()
            assert "database.password=P@ssw0rd-Very-Secret" in body
            mode = stat.S_IMODE(os.stat(path).st_mode)
            assert mode & stat.S_IROTH, f"file must be world-readable, got {oct(mode)}"
            # reference path matches the file we wrote
            assert path in out["database.password"]
        finally:
            os.environ.pop("DEBEZIUM_SECRETS_DIR", None)


def test_externalizes_mongodb_uri():
    with tempfile.TemporaryDirectory() as d:
        os.environ["DEBEZIUM_SECRETS_DIR"] = d
        try:
            cfg = {
                "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
                "mongodb.connection.string": "mongodb://u:s3cr3t@host:27017/?tls=true",
                "topic.prefix": "cdc-m",
            }
            out = connector.externalize_secrets("cdc-m", cfg)
            assert out["mongodb.connection.string"].startswith("${file:")
            assert "s3cr3t" not in str(out)
            body = open(connector._secret_file_path(d, "cdc-m")).read()
            assert "mongodb://u:s3cr3t@host:27017/?tls=true" in body
        finally:
            os.environ.pop("DEBEZIUM_SECRETS_DIR", None)


def test_no_secrets_returns_unchanged():
    with tempfile.TemporaryDirectory() as d:
        os.environ["DEBEZIUM_SECRETS_DIR"] = d
        try:
            cfg = {"connector.class": "X", "topic.prefix": "p", "database.hostname": "h"}
            out = connector.externalize_secrets("cdc-x", cfg)
            assert out == cfg
            assert not os.path.exists(connector._secret_file_path(d, "cdc-x"))
        finally:
            os.environ.pop("DEBEZIUM_SECRETS_DIR", None)


def test_cleanup_removes_file():
    with tempfile.TemporaryDirectory() as d:
        os.environ["DEBEZIUM_SECRETS_DIR"] = d
        try:
            connector.externalize_secrets("cdc-abc", _pg_cfg())
            path = connector._secret_file_path(d, "cdc-abc")
            assert os.path.exists(path)
            connector.cleanup_secret_file("cdc-abc")
            assert not os.path.exists(path)
            # idempotent — no error if already gone
            connector.cleanup_secret_file("cdc-abc")
        finally:
            os.environ.pop("DEBEZIUM_SECRETS_DIR", None)


UNWRITABLE = "/proc/nonexistent/cannot-create"


def test_unwritable_dir_fails_closed_by_default():
    """The whole point of setting DEBEZIUM_SECRETS_DIR is that the password must not
    reach the Kafka config topic. If we cannot honour that, the provisioning call has
    to fail -- the old behaviour returned the secret inline and logged a warning, which
    means a broken volume mount silently downgraded the guarantee it was there to make."""
    os.environ["DEBEZIUM_SECRETS_DIR"] = UNWRITABLE
    os.environ.pop("DEBEZIUM_SECRETS_ENFORCED", None)
    try:
        try:
            connector.externalize_secrets("cdc-abc", _pg_cfg())
        except connector.SecretExternalizationError as e:
            assert "P@ssw0rd-Very-Secret" not in str(e), "error text leaked the secret"
        else:
            raise AssertionError("externalize_secrets returned inline instead of failing closed")
    finally:
        os.environ.pop("DEBEZIUM_SECRETS_DIR", None)


def test_unwritable_dir_falls_back_inline_when_enforcement_disabled():
    """The availability escape hatch still exists -- it just has to be chosen."""
    os.environ["DEBEZIUM_SECRETS_DIR"] = UNWRITABLE
    os.environ["DEBEZIUM_SECRETS_ENFORCED"] = "false"
    try:
        out = connector.externalize_secrets("cdc-abc", _pg_cfg())
        assert out["database.password"] == "P@ssw0rd-Very-Secret"  # inline fallback
    finally:
        os.environ.pop("DEBEZIUM_SECRETS_DIR", None)
        os.environ.pop("DEBEZIUM_SECRETS_ENFORCED", None)


def test_enforcement_does_not_fire_when_feature_is_off():
    """Unset DEBEZIUM_SECRETS_DIR is 'inline mode', not 'externalization failed'.
    Conflating the two would make every self-host deployment fail to provision."""
    os.environ.pop("DEBEZIUM_SECRETS_DIR", None)
    os.environ.pop("DEBEZIUM_SECRETS_ENFORCED", None)
    out = connector.externalize_secrets("cdc-abc", _pg_cfg())
    assert out["database.password"] == "P@ssw0rd-Very-Secret"


def test_start_sync_returns_structured_error_not_a_raise():
    """The orchestrator classifies {"success": False, "error": ...}; an escaping
    exception becomes an HTTP 500 it cannot act on. Also pins that the error text
    stays free of the secret and of words the healer reads as transient."""
    os.environ["DEBEZIUM_SECRETS_DIR"] = UNWRITABLE
    os.environ.pop("DEBEZIUM_SECRETS_ENFORCED", None)
    try:
        srv = connector.DebeziumConnector()
        # If the guard is removed, start_sync proceeds to the real HTTP POST. Catch
        # that here instead of letting a network error decide the verdict: reaching
        # the POST at all already means the secret was not kept out of the config.
        try:
            out = srv.debezium_start_sync({
                "connector_name": "cdc-abc12345",
                "database_type": "postgresql",
                "host": "db.example.com", "port": 5432, "database": "app",
                "username": "svc", "password": "P@ssw0rd-Very-Secret",
                "tables": ["public.t"], "kafka_topic_prefix": "cdc-abc12345",
            })
        except Exception as e:  # noqa: BLE001
            raise AssertionError(
                f"start_sync proceeded past the failed externalization and attempted "
                f"the POST ({type(e).__name__}) instead of returning a structured error"
            ) from e
        assert out.get("success") is False, f"expected failure, got {out}"
        blob = str(out)
        assert "P@ssw0rd-Very-Secret" not in blob, "start_sync leaked the secret"
        for transient in ("timeout", "connection refused", "deadline exceeded"):
            assert transient not in blob.lower(), f"error reads as transient ({transient}); healer would retry"
    finally:
        os.environ.pop("DEBEZIUM_SECRETS_DIR", None)


# ── Orphan secret-file reaper ────────────────────────────────────────────────
# cleanup_secret_file only fires on debezium_stop_sync, which the real delete
# paths never call — the orchestrator deletes connectors straight over Kafka
# Connect's REST API. Every deleted pipeline therefore left a plaintext password
# on the shared volume. reap_orphan_secret_files removes files by absence.


def _touch_secret(d, connector_name, age_seconds=0.0):
    """Write a secret file for connector_name and backdate it by age_seconds."""
    path = connector._secret_file_path(d, connector_name)
    connector._write_secret_properties(path, {"database.password": "hunter2"})
    if age_seconds:
        past = time.time() - age_seconds
        os.utime(path, (past, past))
    return path


def test_reaper_removes_file_for_deleted_connector():
    with tempfile.TemporaryDirectory() as d:
        os.environ["DEBEZIUM_SECRETS_DIR"] = d
        try:
            gone = _touch_secret(d, "cdc-deleted", age_seconds=3600)
            live = _touch_secret(d, "cdc-live", age_seconds=3600)
            removed = connector.reap_orphan_secret_files(["cdc-live"])
            assert removed == 1, removed
            assert not os.path.exists(gone), "deleted connector's credentials survived"
            assert os.path.exists(live), "reaped a LIVE connector's credentials"
        finally:
            os.environ.pop("DEBEZIUM_SECRETS_DIR", None)


def test_reaper_removes_everything_when_last_pipeline_is_gone():
    # A successful empty list is the state right after the last pipeline is
    # deleted — the exact case the leak was worst in. It must not be mistaken
    # for "Connect is down"; that case is filtered out by the caller, which only
    # reaps when GET /connectors actually succeeded.
    with tempfile.TemporaryDirectory() as d:
        os.environ["DEBEZIUM_SECRETS_DIR"] = d
        try:
            _touch_secret(d, "cdc-a", age_seconds=3600)
            _touch_secret(d, "cdc-b", age_seconds=3600)
            assert connector.reap_orphan_secret_files([]) == 2
            assert os.listdir(d) == []
        finally:
            os.environ.pop("DEBEZIUM_SECRETS_DIR", None)


def test_reaper_spares_a_file_inside_the_grace_period():
    # externalize_secrets writes the file BEFORE the connector is POSTed, so a
    # fresh file with no connector is a provisioning in flight, not an orphan.
    with tempfile.TemporaryDirectory() as d:
        os.environ["DEBEZIUM_SECRETS_DIR"] = d
        try:
            fresh = _touch_secret(d, "cdc-being-created", age_seconds=0)
            assert connector.reap_orphan_secret_files([]) == 0
            assert os.path.exists(fresh), "reaped a connector that was still being created"
        finally:
            os.environ.pop("DEBEZIUM_SECRETS_DIR", None)


def test_reaper_removes_stale_tmp_debris_but_leaves_other_files():
    with tempfile.TemporaryDirectory() as d:
        os.environ["DEBEZIUM_SECRETS_DIR"] = d
        try:
            tmp = os.path.join(d, "cdc-crashed.properties.tmp")
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write("database.password=hunter2\n")
            unrelated = os.path.join(d, "README.txt")
            with open(unrelated, "w", encoding="utf-8") as fh:
                fh.write("not ours\n")
            past = time.time() - 3600
            os.utime(tmp, (past, past))
            os.utime(unrelated, (past, past))

            assert connector.reap_orphan_secret_files([]) == 1
            assert not os.path.exists(tmp)
            assert os.path.exists(unrelated), "reaper deleted a file it does not own"
        finally:
            os.environ.pop("DEBEZIUM_SECRETS_DIR", None)


def test_reaper_is_a_no_op_when_externalization_is_off():
    os.environ.pop("DEBEZIUM_SECRETS_DIR", None)
    assert connector.reap_orphan_secret_files([]) == 0


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
        except Exception as e:  # noqa: BLE001 — a crash is a failure, not a suite-ender
            # Previously only AssertionError was caught, so one unexpected exception
            # aborted the run and silently took the remaining tests' verdicts with it.
            failed += 1
            print(f"FAIL {fn.__name__}: unexpected {type(e).__name__}: {e}")
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    raise SystemExit(1 if failed else 0)
