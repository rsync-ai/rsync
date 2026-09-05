"""Contract proof for optional gzip compression of the MinIO claim-check payload.

The `minio` internal connector (shared/mcp-connectors/internal/minio/versions/
v1.0.0/connector.py) is the ONLY component that serializes the structured
claim-check payload to bytes and reads it back — the Go executor/sink only pass
rows over MCP JSON-RPC and never see the staged object. So compression lives
entirely here, gated by RSYNC_CLAIM_CHECK_GZIP (default off).

Design under test:
  WRITE (upload_for_staging) — gzip the JSON body IFF the flag is on; otherwise
    ship plain JSON exactly as before (no regression).
  READ  (read_from_staging) — ALWAYS magic-byte autodetect (gzip = 0x1f 0x8b) and
    transparently decompress. This makes the change backward/forward compatible:
    a reader can read both legacy plain-JSON objects and new gzip objects, so the
    writer flag can flip with nothing in flight stranded.

Runs standalone (no MinIO/boto3/network): an in-memory fake S3 client is injected
in place of the connector's boto3 client, so the REAL upload/read code paths run.
"""
import gzip
import importlib.util
import io
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(_HERE)
CONN_PATH = os.environ.get(
    "MINIO_CONNECTOR_PATH",
    os.path.join(_REPO, "shared", "mcp-connectors", "internal", "minio",
                 "versions", "v1.0.0", "connector.py"),
)

GZIP_MAGIC = b"\x1f\x8b"


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _FakeS3:
    """Minimal in-memory S3 stand-in covering the calls the connector makes."""

    def __init__(self):
        self.store = {}

    def put_object(self, Bucket=None, Key=None, Body=None, **kwargs):
        # capture metadata too, so we can assert ContentEncoding hygiene if set
        self.store[(Bucket, Key)] = (Body, kwargs)
        return {}

    def get_object(self, Bucket=None, Key=None, **kwargs):
        body, _ = self.store[(Bucket, Key)]
        return {"Body": io.BytesIO(body)}

    def delete_object(self, Bucket=None, Key=None, **kwargs):
        self.store.pop((Bucket, Key), None)
        return {}

    def head_bucket(self, Bucket=None, **kwargs):
        return {}


# hostile-ish payload mirroring a real claim-check body (wrapper + rows array)
PAYLOAD = {
    "table": "orders",
    "pipeline_id": "pl-1",
    "execution_id": "ex-1",
    "primary_keys": ["id"],
    "row_count": 3,
    "data": [
        {"id": 1, "name": "münchen ☃", "note": "a,b\"c\nd\there", "amount": "123.45",
         "active": True, "ratio": 0.1 + 0.2, "meta": {"k": ["v", 1, None]}, "raw": None},
        {"id": 2, "name": None, "note": "", "amount": None,
         "active": False, "ratio": -1.5e10, "meta": None, "raw": "trailing "},
        {"id": 3, "name": "x" * 5000, "note": "big", "amount": "0.00",
         "active": None, "ratio": 3.141592653589793, "meta": [1, 2, 3], "raw": "z"},
    ],
}

CFG = {"config": {"bucket": "pipeline-data", "endpoint_url": "http://fake:9000",
                  "access_key_id": "k", "secret_access_key": "s"}}


def _new_server(C):
    fake = _FakeS3()
    srv = C.MinioMCPServer()
    srv._client = lambda cfg: fake  # inject fake S3 into the real code paths
    return srv, fake


def main():
    C = _load("minio_conn", CONN_PATH)
    failures = []

    def check(label, cond, got=None):
        ok = bool(cond)
        print(f"[claim-gzip] {'PASS' if ok else 'FAIL'}: {label}"
              + ("" if ok else f"  (got={got!r})"))
        if not ok:
            failures.append(label)

    # ---- flag OFF (default): plain JSON, no regression --------------------
    os.environ.pop("RSYNC_CLAIM_CHECK_GZIP", None)
    srv, fake = _new_server(C)
    up = srv.upload_for_staging({**CFG, "data": PAYLOAD, "key": "staging/plain.json"})
    check("flag-off upload succeeds", up.get("success") is True, up)
    body_off = fake.store[("pipeline-data", "staging/plain.json")][0]
    check("flag-off body is PLAIN json (no gzip magic)",
          body_off[:2] != GZIP_MAGIC and body_off[:1] in (b"{", b"["), body_off[:8])
    rd = srv.read_from_staging({**CFG, "claim_check_url": up["claim_check_url"]})
    check("flag-off read round-trips identical",
          rd.get("success") is True and rd.get("data") == PAYLOAD, rd.get("data"))

    # ---- flag ON: gzip on the wire, transparent read ---------------------
    os.environ["RSYNC_CLAIM_CHECK_GZIP"] = "true"
    srv, fake = _new_server(C)
    up = srv.upload_for_staging({**CFG, "data": PAYLOAD, "key": "staging/gz.json"})
    check("flag-on upload succeeds", up.get("success") is True, up)
    body_on = fake.store[("pipeline-data", "staging/gz.json")][0]
    check("flag-on body IS gzip (magic 0x1f 0x8b)", body_on[:2] == GZIP_MAGIC, body_on[:8])
    check("flag-on gzip body is smaller than plain json",
          len(body_on) < len(body_off), (len(body_on), len(body_off)))
    check("flag-on gzip body decodes to original payload",
          json.loads(gzip.decompress(body_on).decode("utf-8")) == PAYLOAD)
    rd = srv.read_from_staging({**CFG, "claim_check_url": up["claim_check_url"]})
    check("flag-on read transparently decompresses to identical payload",
          rd.get("success") is True and rd.get("data") == PAYLOAD, rd.get("data"))

    # ---- backward-compat: read a LEGACY plain object while flag is ON ----
    # (autodetect must NOT assume gzip just because the flag is set)
    srv2, fake2 = _new_server(C)
    legacy = json.dumps(PAYLOAD, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    fake2.store[("pipeline-data", "staging/legacy.json")] = (legacy, {})
    rd = srv2.read_from_staging({**CFG, "claim_check_url": "s3://pipeline-data/staging/legacy.json"})
    check("flag-on reader still reads LEGACY plain-json object",
          rd.get("success") is True and rd.get("data") == PAYLOAD, rd.get("data"))

    os.environ.pop("RSYNC_CLAIM_CHECK_GZIP", None)
    if failures:
        print(f"[claim-gzip] RESULT: FAIL ({len(failures)}) -> {failures}")
        sys.exit(1)
    print("[claim-gzip] RESULT: PASS")
    sys.exit(0)


if __name__ == "__main__":
    main()
