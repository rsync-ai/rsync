"""Connector-level test for the blob (passthrough) source lane — universal-blob-
passthrough plan §2, Phase 2 DoD.

Proves, with an in-memory fake object store + fake claim-check staging client (no
boto3, no network), that ObjectStorageSourceMixin:
  - in BLOB mode emits one byte-identical blob envelope per object (pdf + csv),
    with the right content-type / size / sha256 and a staged data_ref, and NO rows;
  - in STRUCTURED mode behaves exactly as before (parses the csv to rows, the
    unparseable pdf is skipped-with-warning), emitting NO blobs;
  - round-trips: the bytes staged for each blob are identical to the source bytes;
  - paginates blobs by file via the {fkey, flm} cursor;
  - fails LOUD (never silent-drop) when staging is missing or an object is over cap.

Run:  cd shared/mcp-connectors/public && python3 -m unittest \
          rsync_protocol.test_object_storage_blob_export -v
  or:  PYTHONPATH=shared/mcp-connectors/public python3 \
          shared/mcp-connectors/public/rsync_protocol/test_object_storage_blob_export.py
"""
import hashlib
import unittest

from rsync_protocol.object_storage_source import (
    ObjectStorageSourceMixin,
    DEFAULT_MAX_BLOB_BYTES,
)
from rsync_protocol.file_formats import content_type_for_object


# Source objects: a binary PDF (non-tabular, the case structured mode silently
# drops today) and a CSV (parseable rows). Bytes include a NUL + 0xFF to prove
# byte-fidelity through staging.
PDF_BYTES = b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nendobj\n\x00\xff\xfeTAIL"
CSV_BYTES = b"id,name\n1,alice\n2,bob\n"
SRC_OBJECTS = {
    "docs/report.pdf": PDF_BYTES,
    "docs/data.csv": CSV_BYTES,
}
BUCKET = "source-bucket"
STAGING = {
    "bucket": "claimcheck",
    "prefix": "blobs",
    "endpoint": "http://minio:9000",
    "access_key": "k",
    "secret_key": "s",
}


class _FakeS3:
    """Captures put_object bytes + content-type so the test can assert byte parity
    and round-trip reads, mimicking just enough of boto3's S3 client surface."""

    def __init__(self):
        self.objects = {}   # (bucket, key) -> {"Body": bytes, "ContentType": str}
        self.buckets = set()

    def create_bucket(self, Bucket):
        self.buckets.add(Bucket)

    def put_object(self, Bucket, Key, Body, ContentType=None):
        assert isinstance(Body, (bytes, bytearray)), "blob body must be raw bytes"
        self.objects[(Bucket, Key)] = {"Body": bytes(Body), "ContentType": ContentType}

    def get_object(self, Bucket, Key):
        rec = self.objects[(Bucket, Key)]
        return {"Body": rec["Body"], "ContentType": rec["ContentType"]}


class FakeStorageConnector(ObjectStorageSourceMixin):
    """Minimal host connector: supplies the two provider primitives + staging
    client the mixin relies on. _assert_safe_endpoint is a no-op (empty endpoint =
    managed cloud; INV-1 is exercised elsewhere)."""

    connector_type = "fake-store"
    max_batch_size = 100

    def __init__(self):
        self.fake_s3 = _FakeS3()

    def _get_config(self, params):
        cfg = {"bucket": BUCKET, "file_format": "infer"}
        cfg.update(params or {})
        return cfg

    def _assert_safe_endpoint(self, endpoint_url):
        return None

    def list_objects(self, *, config, bucket, prefix="", max_keys=1000):
        out = []
        for key, data in SRC_OBJECTS.items():
            if prefix and not key.startswith(prefix):
                continue
            out.append({"key": key, "size": len(data),
                        "last_modified": "2024-01-01T00:00:00+00:00"})
        return out

    def download_file(self, *, config, bucket, key):
        return SRC_OBJECTS[key]

    def _get_staging_client(self, staging_config):
        return self.fake_s3


def _ref_parts(data_ref):
    assert data_ref.startswith("s3://"), data_ref
    bucket, key = data_ref[5:].split("/", 1)
    return bucket, key


class BlobExportTests(unittest.TestCase):

    def test_default_cap_is_airbyte_1_5_gib(self):
        self.assertEqual(DEFAULT_MAX_BLOB_BYTES, 1_610_612_736)

    def test_content_type_helper(self):
        self.assertEqual(content_type_for_object("a/report.pdf"), "application/pdf")
        self.assertEqual(content_type_for_object("a/data.csv"), "text/csv")
        # unknown extension -> opaque bytes, never an error
        self.assertEqual(content_type_for_object("a/blob.bin"), "application/octet-stream")
        # a store-declared type wins
        self.assertEqual(content_type_for_object("x.pdf", declared="image/png"), "image/png")

    def test_blob_mode_emits_byte_identical_envelopes(self):
        c = FakeStorageConnector()
        res = c.export({"copy_raw_files": True, "staging_config": STAGING})

        self.assertTrue(res["success"], res)
        self.assertEqual(res["records"], [], "blob mode must emit no rows")
        self.assertEqual(len(res["blobs"]), 2, res["blobs"])
        self.assertEqual(res["stats"]["modality"], "blob")

        by_key = {b["object_key"]: b for b in res["blobs"]}
        pdf, csv = by_key["docs/report.pdf"], by_key["docs/data.csv"]

        # content-type from extension
        self.assertEqual(pdf["content_type"], "application/pdf")
        self.assertEqual(csv["content_type"], "text/csv")
        # size + sha256 computed over the true bytes
        self.assertEqual(pdf["size"], len(PDF_BYTES))
        self.assertEqual(pdf["sha256"], hashlib.sha256(PDF_BYTES).hexdigest())
        self.assertEqual(csv["sha256"], hashlib.sha256(CSV_BYTES).hexdigest())

        # round-trip: the staged object is byte-identical to the source object
        for env, original in ((pdf, PDF_BYTES), (csv, CSV_BYTES)):
            b, k = _ref_parts(env["data_ref"])
            staged = c.fake_s3.get_object(Bucket=b, Key=k)
            self.assertEqual(staged["Body"], original, "staged bytes must be identical")
            self.assertEqual(staged["ContentType"], env["content_type"])

    def test_structured_mode_unchanged_and_drops_pdf(self):
        c = FakeStorageConnector()
        res = c.export({})  # default: structured (no opt-in)

        self.assertTrue(res["success"], res)
        self.assertEqual(res["blobs"], [], "structured mode must emit no blobs")
        # the csv parsed to 2 rows; the pdf is unparseable-as-json/csv -> skipped
        self.assertEqual(len(res["records"]), 2, res["records"])
        names = sorted(r.get("name") for r in res["records"])
        self.assertEqual(names, ["alice", "bob"])
        # provenance columns still present
        self.assertIn("_rsync_source_file", res["records"][0])

    def test_blob_pagination_by_file(self):
        c = FakeStorageConnector()
        first = c.export({"copy_raw_files": True, "staging_config": STAGING, "limit": 1})
        self.assertEqual(len(first["blobs"]), 1)
        self.assertIsNotNone(first["next_cursor"])

        second = c.export({"copy_raw_files": True, "staging_config": STAGING,
                           "limit": 1, "cursor": first["next_cursor"]})
        self.assertEqual(len(second["blobs"]), 1)
        # the two batches cover distinct objects (no re-emit)
        self.assertNotEqual(first["blobs"][0]["object_key"],
                            second["blobs"][0]["object_key"])

    def test_blob_mode_requires_staging_fail_loud(self):
        c = FakeStorageConnector()
        res = c.export({"copy_raw_files": True})  # no staging_config
        self.assertFalse(res["success"])
        self.assertEqual(res["errors"][0]["code"], "staging_required")

    def test_over_cap_object_fails_loud_no_partial(self):
        c = FakeStorageConnector()
        res = c.export({"copy_raw_files": True, "staging_config": STAGING,
                        "max_blob_bytes": 5})  # every object exceeds 5 bytes
        self.assertFalse(res["success"])
        self.assertEqual(res["errors"][0]["code"], "blob_over_size_cap")
        # fail-loud BEFORE staging anything (no partial copy)
        self.assertEqual(c.fake_s3.objects, {})


class _CustomObjectsConnector(FakeStorageConnector):
    """A connector whose object set the test controls per-instance."""

    def __init__(self, objects):
        super().__init__()
        self._objs = objects

    def list_objects(self, *, config, bucket, prefix="", max_keys=1000):
        return [{"key": k, "size": len(v),
                 "last_modified": "2024-01-01T00:00:00+00:00"}
                for k, v in self._objs.items()
                if not (prefix and not k.startswith(prefix))]

    def download_file(self, *, config, bucket, key):
        return self._objs[key]


class BlobEdgeCaseTests(unittest.TestCase):
    """Regression cover for the adversarial-review fixes (Phase 2)."""

    def test_zero_byte_object_kept_in_blob_mode_dropped_in_structured(self):
        # A legitimately-empty object must NOT be silently dropped in blob mode
        # (the feature's core guarantee); structured mode still drops it.
        c = _CustomObjectsConnector({"docs/empty.log": b"", "docs/data.csv": CSV_BYTES})
        blob = c.export({"copy_raw_files": True, "staging_config": STAGING})
        self.assertTrue(blob["success"], blob)
        keys = sorted(b["object_key"] for b in blob["blobs"])
        self.assertEqual(keys, ["docs/data.csv", "docs/empty.log"])
        empty = next(b for b in blob["blobs"] if b["object_key"] == "docs/empty.log")
        self.assertEqual(empty["size"], 0)
        self.assertEqual(empty["sha256"], hashlib.sha256(b"").hexdigest())
        self.assertEqual(empty["content_type"], "text/plain")  # .log via canonical map

        # structured lane: the 0-byte object is dropped at listing (no blobs, csv rows only)
        struct = c.export({})
        self.assertEqual(struct["blobs"], [])
        self.assertEqual(len(struct["records"]), 2)

    def test_distinct_objects_same_leaf_get_distinct_staging_keys(self):
        # Full-sha256 key: two different objects sharing a basename must never
        # collide / overwrite in staging.
        c = _CustomObjectsConnector({"a/dup.bin": b"AAAA", "b/dup.bin": b"BBBB"})
        res = c.export({"copy_raw_files": True, "staging_config": STAGING})
        self.assertTrue(res["success"], res)
        refs = {b["object_key"]: b["data_ref"] for b in res["blobs"]}
        self.assertEqual(len(set(refs.values())), 2, "data_refs must be distinct")
        # both staged byte-identical, no overwrite
        self.assertEqual(len(c.fake_s3.objects), 2)


class BlobDestFetchTests(unittest.TestCase):
    """Destination side of a blob copy (Phase 3): the staged bytes are fetched
    back RAW and verified, then written byte-identical. Covers the shared mixin
    methods _read_blob_from_staging + _prepare_blob_write (the per-connector
    provider put is exercised by the e2e harness, not here)."""

    def test_read_blob_from_staging_roundtrip(self):
        c = FakeStorageConnector()
        ref = c._stage_blob(PDF_BYTES, staging_config=STAGING, content_type="application/pdf",
                            object_key="docs/report.pdf",
                            sha256=hashlib.sha256(PDF_BYTES).hexdigest())
        self.assertEqual(c._read_blob_from_staging(ref, STAGING), PDF_BYTES)

    def test_prepare_blob_write_verifies_sha256_and_resolves(self):
        c = FakeStorageConnector()
        sha = hashlib.sha256(PDF_BYTES).hexdigest()
        ref = c._stage_blob(PDF_BYTES, staging_config=STAGING, content_type="application/pdf",
                            object_key="docs/report.pdf", sha256=sha)
        body, bucket, key, ct = c._prepare_blob_write({
            "data_ref": ref, "sha256": sha, "staging_config": STAGING,
            "bucket": "dest-bucket", "key": "out/report.pdf", "content_type": "application/pdf"})
        self.assertEqual(body, PDF_BYTES)
        self.assertEqual((bucket, key, ct), ("dest-bucket", "out/report.pdf", "application/pdf"))

    def test_prepare_blob_write_fails_loud_on_sha_mismatch(self):
        # A corrupted/altered object must NEVER be written silently.
        c = FakeStorageConnector()
        ref = c._stage_blob(PDF_BYTES, staging_config=STAGING, content_type="application/pdf",
                            object_key="docs/report.pdf",
                            sha256=hashlib.sha256(PDF_BYTES).hexdigest())
        with self.assertRaises(ValueError):
            c._prepare_blob_write({"data_ref": ref, "sha256": "deadbeef", "staging_config": STAGING,
                                   "bucket": "b", "key": "k", "content_type": "application/pdf"})

    def test_prepare_blob_write_container_alias(self):
        # Azure passes `container`; it must resolve as the bucket slot.
        c = FakeStorageConnector()
        ref = c._stage_blob(CSV_BYTES, staging_config=STAGING, content_type="text/csv",
                            object_key="d/data.csv", sha256=hashlib.sha256(CSV_BYTES).hexdigest())
        _, bucket, _, _ = c._prepare_blob_write({
            "data_ref": ref, "staging_config": STAGING,
            "container": "az-container", "key": "k.csv", "content_type": "text/csv"})
        self.assertEqual(bucket, "az-container")

    def test_read_blob_bad_ref_raises(self):
        c = FakeStorageConnector()
        with self.assertRaises(ValueError):
            c._read_blob_from_staging("not-an-s3-url", STAGING)

    def test_export_then_dest_fetch_byte_identical(self):
        # Full mixin round trip: source export stages blobs; the dest side fetches +
        # verifies each object byte-identical with its content-type carried through.
        c = FakeStorageConnector()
        res = c.export({"copy_raw_files": True, "staging_config": STAGING})
        self.assertTrue(res["success"], res)
        by_key = {b["object_key"]: b for b in res["blobs"]}
        for key, original in (("docs/report.pdf", PDF_BYTES), ("docs/data.csv", CSV_BYTES)):
            env = by_key[key]
            body, _, _, ct = c._prepare_blob_write({
                "data_ref": env["data_ref"], "sha256": env["sha256"],
                "staging_config": STAGING, "bucket": "dest", "key": "out/" + key,
                "content_type": env["content_type"]})
            self.assertEqual(body, original, "dest-fetched bytes must equal source bytes")
            self.assertEqual(ct, env["content_type"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
