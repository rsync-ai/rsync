"""Native-record fidelity for structured → object-storage moves —
universal-blob-passthrough plan §4 (modality B).

The object-storage destination's json/jsonl write path serializes rows with
json.dumps(..., default=str) (base_connector.convert_data_to_format), which
recurses into nested objects/arrays — so a structured move into an object store
must preserve the SOURCE's native JSON shape verbatim (no relational flattening).
This locks that contract in: a deeply-nested record round-trips through the
json AND jsonl serialize→parse cycle byte-for-structure identical.

Run:  cd shared/mcp-connectors/public && python3 -m unittest \
          rsync_protocol.test_native_record_fidelity -v
"""
import json
import unittest

from rsync_protocol.file_formats import parse_bytes_to_rows


# A record with nested objects, arrays, arrays-of-objects, and scalar edge values
# — exactly the shape a SaaS/API source emits and that relational flattening would
# destroy.
NESTED_RECORD = {
    "id": 1,
    "name": "widget",
    "meta": {"vendor": {"name": "acme", "tier": 3}, "active": True},
    "tags": ["a", "b", "c"],
    "variants": [{"sku": "x1", "price": 9.99}, {"sku": "x2", "price": None}],
    "note": "line1\nline2,with,commas",
}


def _serialize_json(rows):
    # Mirrors base_connector.convert_data_to_format('json').
    return json.dumps(rows, indent=2, default=str).encode("utf-8")


def _serialize_jsonl(rows):
    # Mirrors base_connector.convert_data_to_format('jsonl').
    return "\n".join(json.dumps(r, default=str) for r in rows).encode("utf-8")


class NativeRecordFidelityTests(unittest.TestCase):

    def test_nested_record_roundtrips_via_json(self):
        rows = [NESTED_RECORD]
        parsed = parse_bytes_to_rows(_serialize_json(rows), "json")
        self.assertEqual(parsed, rows, "nested object must survive json round-trip unflattened")
        # Spot-check the nesting is real structure, not a stringified blob.
        self.assertEqual(parsed[0]["meta"]["vendor"]["name"], "acme")
        self.assertEqual(parsed[0]["variants"][1]["price"], None)
        self.assertEqual(parsed[0]["tags"], ["a", "b", "c"])

    def test_nested_record_roundtrips_via_jsonl(self):
        rows = [NESTED_RECORD, {"id": 2, "meta": {"k": [1, 2, {"deep": "v"}]}}]
        parsed = parse_bytes_to_rows(_serialize_jsonl(rows), "jsonl")
        self.assertEqual(parsed, rows, "nested object must survive jsonl round-trip unflattened")
        self.assertEqual(parsed[1]["meta"]["k"][2]["deep"], "v")

    def test_keys_are_not_dotted_or_flattened(self):
        # A relational flattener would turn meta.vendor.name into a top-level
        # "meta.vendor.name" / "meta_vendor_name" column. Assert that did NOT happen.
        parsed = parse_bytes_to_rows(_serialize_json([NESTED_RECORD]), "json")
        top_keys = set(parsed[0].keys())
        self.assertEqual(top_keys, {"id", "name", "meta", "tags", "variants", "note"})
        for k in top_keys:
            self.assertNotIn(".", k)
            self.assertNotIn("__", k)


if __name__ == "__main__":
    unittest.main(verbosity=2)
