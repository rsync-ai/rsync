"""
Provider-agnostic object-storage SOURCE pipeline (shared across aws-s3, gcs,
azure-blob and any future bucket connector).

Why this lives in rsync_protocol
--------------------------------
`rsync_protocol` is the single `COPY --from=shared` package baked into every
connector image, so it is the only place shared connector logic can live without
drift. The S3 source pipeline was provider-agnostic except for two primitives
(list objects, download one object); factoring it out here lets gcs/azure-blob
implement just those two methods and inherit identical file-selection, table
mapping, sampling, schema inference and cursor-paged export behaviour. This
eliminates the per-connector copy-drift class that previously bit pipedrive +
aws-s3 (see CLAUDE.md "patch in place / no root copies").

Contract the host connector MUST provide
-----------------------------------------
* ``connector_type: str``                         — used in the INV-1 guard + logs
* ``max_batch_size: int``                         — default export page size
* ``_get_config(params) -> dict``                 — auth/config extraction
* ``list_objects(*, config, bucket, prefix, max_keys) -> list[{key,size,last_modified}]``
* ``download_file(*, config, bucket, key) -> bytes``

Everything else (settings resolution, glob, per_table/dynamic mapping, parsing,
discover_schema, export, provenance columns, the INV-1 endpoint guard) is here.

Return shapes mirror base_connector.ExportResult.to_dict() exactly
(``{success, records, errors, next_cursor, stats}``) so the batch executor reads
them identically to the S3 path — this module deliberately does NOT import
base_connector (it is a per-version sibling file, not part of the shared pkg).
"""
import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# Per-file size cap for the blob (passthrough) lane (plan §7). Matches Airbyte's
# 1.5 GiB per-object cap; overridable per-pipeline via params/config
# max_blob_bytes / max_file_bytes. An object above this fails loud (never a
# partial copy) — see ObjectStorageSourceMixin._export_blobs.
DEFAULT_MAX_BLOB_BYTES = 1_610_612_736  # 1.5 GiB


def _export_dict(
    success: bool,
    *,
    records: Optional[List[Dict[str, Any]]] = None,
    errors: Optional[List[Any]] = None,
    next_cursor: Optional[Any] = None,
    stats: Optional[Dict[str, Any]] = None,
    blobs: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Build the exact dict base_connector.ExportResult.to_dict() would produce.

    `blobs` is the parallel raw-bytes lane (plan §2): empty for the structured
    (row) path, populated only by the blob passthrough export. It is always
    present so the executor reads one stable shape regardless of modality.
    """
    return {
        "success": success,
        "records": records or [],
        "errors": errors or [],
        "next_cursor": next_cursor,
        "stats": stats or {},
        "blobs": blobs or [],
    }


class ObjectStorageSourceMixin:
    """Mix in before BaseMCPConnector to make a bucket connector a first-class
    SOURCE. The connector supplies ``list_objects`` + ``download_file`` (provider
    SDK calls) and ``_get_config``; this mixin supplies the rest."""

    # Provenance columns added to every source row so the destination can trace
    # each record back to its object + position. Mirrored into discover_schema's
    # column list so the dest DDL includes them.
    SOURCE_META_COLUMNS = [
        {"name": "_rsync_source_file", "type": "string", "nullable": False},
        {"name": "_rsync_source_file_modified", "type": "string", "nullable": True},
        {"name": "_rsync_source_row", "type": "integer", "nullable": False},
    ]

    # =========================================================================
    # INV-1 endpoint guard (shared)
    # =========================================================================
    def _assert_safe_endpoint(self, endpoint_url) -> None:
        """INV-1: a user object-storage connection must never resolve to
        rsync-ai's internal MinIO (the env-driven claim-check store). Empty
        endpoint => the provider's managed cloud (AWS/GCS/Azure)."""
        from rsync_protocol.storage_safety import assert_external_endpoint
        assert_external_endpoint(
            endpoint_url, connector_type=getattr(self, "connector_type", "storage")
        )

    # =========================================================================
    # SOURCE HELPERS (file selection · table mapping · sampling · parsing)
    # =========================================================================
    def _source_settings(self, config: Dict) -> Dict[str, Any]:
        """Resolve the source-side config (Groups E/F/G/H/I) with safe defaults."""
        prefix = (config.get("path_prefix") or config.get("prefix") or "").lstrip("/")
        globs = config.get("globs") or config.get("file_pattern") or "*"
        if isinstance(globs, str):
            globs = [g.strip() for g in globs.split(",") if g.strip()]
        if not globs:
            globs = ["*"]
        return {
            "bucket": (config.get("bucket") or config.get("bucket_name")
                       or config.get("container") or "").strip(),
            "prefix": prefix,
            "globs": globs,
            "file_format": (config.get("file_format") or "json").strip().lower(),
            "compression": (config.get("compression") or "none").strip().lower(),
            "file_mapping": (config.get("file_mapping") or "single").strip().lower(),
            "table_patterns": config.get("table_patterns"),
            "dynamic_table_regex": (config.get("dynamic_table_regex")
                                    or config.get("dynamic_table_pattern") or ""),
            "source_table_name": (config.get("source_table_name") or "").strip(),
            "sample_files": int(config.get("sample_files") or 5),
            "max_sample_rows": int(config.get("max_sample_rows") or 100),
            "start_date": (config.get("start_date") or "").strip(),
            "max_list_keys": int(config.get("max_list_keys") or 100000),
        }

    def _default_table_name(self, settings: Dict) -> str:
        return settings.get("source_table_name") or settings.get("bucket") or "data"

    @staticmethod
    def _glob_match(key: str, globs: List[str]) -> bool:
        import fnmatch
        leaf = key.split("/")[-1]
        return any(fnmatch.fnmatch(key, g) or fnmatch.fnmatch(leaf, g) for g in globs)

    def _list_source_objects(self, config: Dict, settings: Dict,
                             keep_empty: bool = False) -> List[Dict[str, Any]]:
        """List objects under the prefix, dropping dir markers / zero-byte
        objects and applying the glob + start_date filters (Group E).

        keep_empty=True is set by the blob (passthrough) lane: a legitimately
        zero-byte object IS data there, and silently dropping it would violate the
        feature's core "never silently drop" guarantee. Dir markers are still
        dropped regardless. The structured path keeps the default (drop empties:
        a 0-byte CSV/JSON has no rows, and 0-byte _SUCCESS markers are noise)."""
        objs = self.list_objects(
            config=config,
            bucket=settings["bucket"],
            prefix=settings["prefix"],
            max_keys=settings["max_list_keys"],
        )
        start_date = settings.get("start_date")
        out: List[Dict[str, Any]] = []
        for o in objs:
            k = o.get("key") or ""
            if not k or k.endswith("/"):
                continue
            if not keep_empty and o.get("size", 1) == 0:
                continue  # _SUCCESS markers / empty objects (structured lane only)
            if not self._glob_match(k, settings["globs"]):
                continue
            if start_date and str(o.get("last_modified") or "") < start_date:
                continue
            out.append(o)
        return out

    def _resolve_table_files(self, objects: List[Dict], settings: Dict) -> Dict[str, List[Dict]]:
        """Group objects into logical tables per file_mapping mode (Group F):
        single (all → one table), per_table (table_patterns map), or dynamic
        (a regex with a named (?<table>...) / (?P<table>...) group)."""
        import fnmatch
        mode = settings["file_mapping"]
        mapping: Dict[str, List[Dict]] = {}

        if mode == "per_table":
            patterns = settings.get("table_patterns")
            if isinstance(patterns, str):
                try:
                    patterns = json.loads(patterns)
                except Exception:
                    patterns = {}
            patterns = patterns or {}
            for tname, pat in patterns.items():
                pats = pat if isinstance(pat, list) else [pat]
                files = [
                    o for o in objects
                    if any(fnmatch.fnmatch(o.get("key", ""), p)
                           or fnmatch.fnmatch(o.get("key", "").split("/")[-1], p)
                           for p in pats)
                ]
                mapping[str(tname)] = files
            return mapping

        if mode == "dynamic":
            import re
            raw = (settings.get("dynamic_table_regex") or "").replace("(?<table>", "(?P<table>")
            try:
                rx = re.compile(raw) if raw else None
            except Exception as e:
                logger.warning("dynamic_table_regex invalid (%s): %s", raw, e)
                rx = None
            for o in objects:
                k = o.get("key", "")
                tname = None
                if rx is not None:
                    m = rx.search(k)
                    if m:
                        try:
                            tname = m.group("table")
                        except Exception:
                            tname = m.group(1) if m.groups() else None
                if tname:
                    mapping.setdefault(str(tname), []).append(o)
            return mapping

        # single (default)
        mapping[self._default_table_name(settings)] = list(objects)
        return mapping

    def _file_format_for(self, key: str, settings: Dict):
        """Resolve (format, compression) for one object, honoring file_format='infer'."""
        fmt = settings["file_format"]
        comp = settings["compression"]
        if fmt == "infer":
            from rsync_protocol.file_formats import sniff_format_from_key
            f, c = sniff_format_from_key(key)
            fmt = f or "json"
            if comp in ("", "none"):
                comp = c
        return fmt, comp

    def _parse_object(self, config: Dict, bucket: str, obj: Dict, settings: Dict) -> List[Dict]:
        """Download + parse one object into row dicts (Group G)."""
        from rsync_protocol.file_formats import parse_bytes_to_rows
        key = obj.get("key")
        fmt, comp = self._file_format_for(key, settings)
        content = self.download_file(config=config, bucket=bucket, key=key)
        return parse_bytes_to_rows(content, fmt, comp)

    # =========================================================================
    # CORE SOURCE OPERATIONS
    # =========================================================================
    def discover_schema(self, params: Dict = None) -> Dict[str, Any]:
        """Discover logical tables by sampling objects under the prefix.

        Objects are listed (Group E), grouped into tables per file_mapping
        (Group F), and a bounded sample of rows is parsed from each table's
        files (Group G) to infer column types (Group H). Provenance columns
        (_rsync_source_*) are appended so the destination DDL includes them.
        """
        config = self._get_config(params or {})
        try:
            from rsync_protocol.file_formats import infer_schema_from_rows

            settings = self._source_settings(config)
            if not settings["bucket"]:
                return {"success": False, "error": "Missing required field: bucket",
                        "tables": [], "total_tables": 0}
            self._assert_safe_endpoint(config.get("endpoint_url") or config.get("endpoint"))

            objects = self._list_source_objects(config, settings)
            table_map = self._resolve_table_files(objects, settings)

            tables: List[Dict[str, Any]] = []
            for tname, files in table_map.items():
                files_sorted = sorted(
                    files, key=lambda o: (str(o.get("last_modified") or ""), o.get("key") or "")
                )
                sample_rows: List[Dict] = []
                fmt_seen = settings["file_format"]
                for o in files_sorted[: settings["sample_files"]]:
                    try:
                        rows = self._parse_object(config, settings["bucket"], o, settings)
                    except Exception as e:
                        logger.warning("discover_schema: skip unparseable %s: %s", o.get("key"), e)
                        continue
                    if settings["file_format"] == "infer":
                        fmt_seen, _ = self._file_format_for(o.get("key"), settings)
                    sample_rows.extend(rows)
                    if len(sample_rows) >= settings["max_sample_rows"]:
                        sample_rows = sample_rows[: settings["max_sample_rows"]]
                        break
                cols = infer_schema_from_rows(sample_rows, fmt_seen)
                cols = cols + [dict(c) for c in self.SOURCE_META_COLUMNS]
                tables.append({
                    "name": tname,
                    "schema": "",
                    "columns": cols,
                    "row_count": -1,
                    "file_count": len(files),
                    "type": "table",
                    "discovery_status": "complete" if sample_rows else "empty",
                })

            tables.sort(key=lambda t: t["name"])
            return {"success": True, "tables": tables, "total_tables": len(tables)}
        except Exception as e:
            logger.error("discover_schema failed: %s", e, exc_info=True)
            return {"success": False, "error": str(e), "tables": [], "total_tables": 0}

    def export(self, params: Dict) -> Dict[str, Any]:
        """Export parsed rows for one logical table (REQUIRED by BaseMCPConnector).

        Driven by the batch executor, which calls this per table in a paginated
        loop reading `records` and advancing via `next_cursor`. The cursor is a
        keyed position {fkey, flm, ri} over the table's sorted file list — robust
        to files appended between batches. Each row gains _rsync_source_* columns.
        Incremental: when `since`/`modified_since` is supplied, only objects with
        a newer last_modified are read, and stats.watermark reports the high-water
        mark for the next scheduled run.
        """
        params = params or {}
        config = self._get_config(params)
        try:
            settings = self._source_settings(config)
            if not settings["bucket"]:
                return {"success": False, "error": "Missing required field: bucket"}
            self._assert_safe_endpoint(config.get("endpoint_url") or config.get("endpoint"))

            limit = int(params.get("limit") or params.get("max_records") or self.max_batch_size)
            if limit <= 0:
                limit = self.max_batch_size
            cursor = params.get("cursor") if isinstance(params.get("cursor"), dict) else None
            offset = int(params.get("offset") or 0)
            since_raw = (params.get("since") or params.get("modified_since")
                         or params.get("updated_since") or params.get("modified_after"))
            since = since_raw.strip() if isinstance(since_raw, str) else ""

            requested_table = (params.get("table") or params.get("selected_table")
                               or params.get("stream"))

            # Blob (passthrough) move? Resolved once: it also decides whether the
            # listing keeps legitimately-empty objects (never silently drop in blob).
            passthrough = self._is_passthrough(params, config)

            objects = self._list_source_objects(config, settings, keep_empty=passthrough)
            table_map = self._resolve_table_files(objects, settings)

            # Resolve the requested table → its files.
            if requested_table and requested_table in table_map:
                files = table_map[requested_table]
                table_name = requested_table
            elif requested_table and settings["file_mapping"] == "single":
                files = table_map.get(self._default_table_name(settings), [])
                table_name = requested_table
            elif not requested_table:
                files = list(objects)  # ad-hoc: flatten everything into one stream
                table_name = self._default_table_name(settings)
            else:
                # Asked for a table that doesn't exist under this config → empty (terminal).
                return _export_dict(True, records=[],
                                    stats={"total_files": 0, "table": requested_table})

            # Incremental: keep only objects newer than the watermark.
            if since:
                files = [o for o in files if str(o.get("last_modified") or "") > since]

            files_sorted = sorted(
                files, key=lambda o: (str(o.get("last_modified") or ""), o.get("key") or "")
            )

            # Blob passthrough lane (plan §2): when this is a blob move, emit each
            # object as opaque bytes (staged byte-identical to the claim-check store)
            # instead of parsing it into rows. Opt-in only — structured pipelines never
            # reach this branch, so existing behaviour is unchanged.
            if passthrough:
                return self._export_blobs(
                    params, config, settings, files_sorted, limit, cursor, table_name
                )

            # Resolve the start position (cursor preferred; offset is a rare fallback).
            start_fi, start_ri = 0, 0
            if cursor and cursor.get("fkey"):
                fkey = cursor.get("fkey")
                idx = next((i for i, o in enumerate(files_sorted) if o.get("key") == fkey), None)
                if idx is not None:
                    start_fi, start_ri = idx, int(cursor.get("ri") or 0)
                else:
                    # The cursored file is gone; resume just past its (lm, key) slot.
                    flm = cursor.get("flm") or ""
                    start_fi = next(
                        (i for i, o in enumerate(files_sorted)
                         if (str(o.get("last_modified") or ""), o.get("key") or "") > (flm, fkey)),
                        len(files_sorted),
                    )
                    start_ri = 0
            elif offset > 0:
                remaining = offset
                while start_fi < len(files_sorted) and remaining > 0:
                    try:
                        n = len(self._parse_object(config, settings["bucket"], files_sorted[start_fi], settings))
                    except Exception:
                        n = 0
                    if remaining < n:
                        start_ri = remaining
                        remaining = 0
                        break
                    remaining -= n
                    start_fi += 1

            # Walk files, collecting up to `limit` rows; cur_pos tracks the next
            # unread row so the cursor always advances (no re-emit, no infinite loop).
            collected: List[Dict[str, Any]] = []
            cur_pos: Optional[Dict[str, Any]] = None
            fi = start_fi
            ri_start = start_ri
            while fi < len(files_sorted) and len(collected) < limit:
                obj = files_sorted[fi]
                key = obj.get("key")
                lm = str(obj.get("last_modified") or "")
                try:
                    rows = self._parse_object(config, settings["bucket"], obj, settings)
                except Exception as e:
                    logger.warning("export: skip unparseable %s: %s", key, e)
                    fi += 1
                    ri_start = 0
                    continue
                ri = ri_start if fi == start_fi else 0
                while ri < len(rows) and len(collected) < limit:
                    row = dict(rows[ri])
                    row["_rsync_source_file"] = key
                    row["_rsync_source_file_modified"] = lm
                    row["_rsync_source_row"] = ri
                    collected.append(row)
                    cur_pos = {"fkey": key, "flm": lm, "ri": ri + 1}
                    ri += 1
                fi += 1
                ri_start = 0

            # Watermark = max last_modified across this batch's candidate files
            # (within one execution all files are consumed, so the last batch
            # carries the table-wide high-water mark). See INCREMENTAL.md §2.
            lms = [str(o.get("last_modified") or "") for o in files_sorted if o.get("last_modified")]
            watermark = max(lms) if lms else ""
            if since and (not watermark or since > watermark):
                watermark = since

            stats: Dict[str, Any] = {
                "total_files": len(files_sorted),
                "table": table_name,
                "rows": len(collected),
            }
            if watermark:
                stats["watermark"] = {"last_modified": watermark}

            # next_cursor only when more rows remain; None + empty records = terminal.
            next_cursor = cur_pos if collected else None
            return _export_dict(True, records=collected, next_cursor=next_cursor, stats=stats)
        except Exception as e:
            logger.error("Cloud storage export failed: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    # =========================================================================
    # BLOB PASSTHROUGH LANE (plan §2 — raw object copy, byte-identical)
    # =========================================================================
    @staticmethod
    def _param_truthy(v: Any) -> bool:
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.strip().lower() in ("true", "1", "yes", "on")
        return False

    def _is_passthrough(self, params: Optional[Dict], config: Optional[Dict]) -> bool:
        """Whether this run is a blob (raw passthrough) move. OPT-IN — mirrors the
        orchestrator capability gate's deriveMoveModality signals exactly, so the
        Python source and the Go gate agree on what 'blob' means:
          - copy_raw_files / passthrough / raw_files truthy, or
          - move_modality == 'blob', or
          - delivery_method in {copy_raw_files, raw, raw_files, blob}.
        Checked in both plan params and connection config."""
        for src in (params, config):
            if not isinstance(src, dict):
                continue
            if (self._param_truthy(src.get("copy_raw_files"))
                    or self._param_truthy(src.get("passthrough"))
                    or self._param_truthy(src.get("raw_files"))):
                return True
            if str(src.get("move_modality") or "").strip().lower() == "blob":
                return True
            if str(src.get("delivery_method") or "").strip().lower() in (
                "copy_raw_files", "raw", "raw_files", "blob"
            ):
                return True
        return False

    def _max_blob_bytes(self, params: Optional[Dict], config: Optional[Dict]) -> int:
        """Per-file blob size cap in bytes (plan §7). Override via params/config
        max_blob_bytes / max_file_bytes; else DEFAULT_MAX_BLOB_BYTES (1.5 GiB)."""
        for src in (params, config):
            if isinstance(src, dict):
                v = src.get("max_blob_bytes") or src.get("max_file_bytes")
                if v:
                    try:
                        return int(v)
                    except (TypeError, ValueError):
                        pass
        return DEFAULT_MAX_BLOB_BYTES

    def _stage_blob(self, content: bytes, *, staging_config: Dict,
                    content_type: str, object_key: str, sha256: str) -> str:
        """Copy one object's raw bytes byte-identical into the claim-check store
        (rsync's internal MinIO) and return its s3://bucket/key data_ref.

        Reuses the connector's staging S3 client (self._get_staging_client, supplied
        by BaseMCPConnector). The staged key is content-addressed by the FULL sha256
        plus the readable source basename, so a retry of the same object overwrites
        the same key (idempotent, no duplicate — plan §7) while two distinct objects
        can never collide (a collision would require a real sha256 collision)."""
        client = self._get_staging_client(staging_config)
        if client is None:
            raise RuntimeError(
                "blob passthrough requires a claim-check staging client "
                "(missing MinIO endpoint/credentials in staging_config)"
            )
        bucket = (staging_config.get("bucket") or "staging").strip()
        prefix = (staging_config.get("prefix") or "blobs").strip("/")
        leaf = (object_key or "object").split("/")[-1] or "object"
        ctype = getattr(self, "connector_type", "storage")
        key = f"{prefix}/{ctype}/{sha256}/{leaf}"
        try:
            client.create_bucket(Bucket=bucket)
        except Exception:
            pass  # bucket exists / AWS-managed — non-fatal; the put below is authoritative
        client.put_object(Bucket=bucket, Key=key, Body=content, ContentType=content_type)
        return f"s3://{bucket}/{key}"

    def _read_blob_from_staging(self, data_ref: str, staging_config: Dict) -> bytes:
        """Fetch one staged object's RAW bytes back from the claim-check store —
        the symmetric inverse of _stage_blob, used on the DESTINATION side of a
        blob (passthrough) copy. Unlike base_connector.read_from_staging (which
        json.loads the body, structured-only), this returns the bytes verbatim so
        an opaque object (PDF, parquet, image) round-trips byte-identical.

        data_ref is an s3://bucket/key pointer produced by _stage_blob. Raises
        (never silently returns empty) on a bad ref, a missing staging client, or
        an unreadable object, so the caller fails loud rather than writing a
        truncated/empty destination object."""
        if not isinstance(data_ref, str) or not data_ref.startswith("s3://"):
            raise ValueError(f"invalid blob data_ref (must be s3://bucket/key): {data_ref!r}")
        rest = data_ref[len("s3://"):]
        if "/" not in rest:
            raise ValueError(f"invalid blob data_ref (no key): {data_ref!r}")
        bucket, key = rest.split("/", 1)
        client = self._get_staging_client(staging_config)
        if client is None:
            raise RuntimeError(
                "blob passthrough requires a claim-check staging client "
                "(missing MinIO endpoint/credentials in staging_config)"
            )
        resp = client.get_object(Bucket=bucket, Key=key)
        body_obj = resp["Body"]
        # Real boto3 returns a StreamingBody (.read()); the unit-test fake returns
        # raw bytes directly. Support both.
        body = body_obj.read() if hasattr(body_obj, "read") else body_obj
        return body if isinstance(body, (bytes, bytearray)) else bytes(body)

    def _prepare_blob_write(self, params: Dict):
        """Destination-side prep for a blob (passthrough) write, shared by every
        object-storage connector. Fetches the staged bytes by data_ref, verifies
        them against the expected sha256 (fail loud on mismatch — never write a
        corrupt object), and resolves (bucket, key, content_type) from the EXPLICIT
        params the sink supplies. content_type is taken verbatim (NOT re-derived
        from a serialization format the way prepare_destination_params does) so the
        source MIME survives the copy. Returns (body, bucket, key, content_type)."""
        import hashlib
        staging_config = params.get("staging_config") or params.get("config") or {}
        body = self._read_blob_from_staging(params.get("data_ref"), staging_config)
        expected = str(params.get("sha256") or "").strip()
        if expected:
            actual = hashlib.sha256(body).hexdigest()
            if actual != expected:
                raise ValueError(
                    f"blob integrity check failed: sha256 {actual} != expected {expected}"
                )
        cfg = params.get("config") or {}
        bucket = (params.get("bucket") or params.get("container")
                  or cfg.get("bucket") or cfg.get("bucket_name") or cfg.get("container"))
        key = params.get("key")
        content_type = str(params.get("content_type") or "application/octet-stream")
        if not bucket or not key:
            raise ValueError("blob write requires bucket/container and key")
        return body, bucket, key, content_type

    def _export_blobs(self, params: Dict, config: Dict, settings: Dict,
                      files_sorted: List[Dict], limit: int,
                      cursor: Optional[Dict], table_name: str) -> Dict[str, Any]:
        """Emit one blob envelope per object (raw passthrough). Paginates by FILE:
        the cursor is {fkey, flm} over the same (last_modified, key) ordering the
        structured path uses — robust to files appended between batches. Each object's
        bytes are staged byte-identical to the claim-check store; the envelope carries
        {object_key, content_type, size, data_ref, sha256, source_file_modified}.

        Fail-loud guardrails (the user's core requirement — never silently drop):
          - no staging_config        -> the run fails (blob transport is mandatory),
          - object over the size cap  -> the run fails before any partial copy."""
        import hashlib
        from rsync_protocol.file_formats import content_type_for_object

        staging_config = params.get("staging_config") or config.get("staging_config")
        if not isinstance(staging_config, dict) or not staging_config:
            return _export_dict(
                False,
                errors=[{
                    "code": "staging_required",
                    "message": ("blob passthrough requires staging_config "
                                "(claim-check MinIO) to carry raw bytes"),
                }],
                stats={"table": table_name, "modality": "blob"},
            )
        cap = self._max_blob_bytes(params, config)

        def _over_cap(key, nbytes):
            return _export_dict(
                False,
                errors=[{
                    "code": "blob_over_size_cap",
                    "message": (f"object {key!r} is {nbytes} bytes, over the per-file "
                                f"cap of {cap} bytes — failing loud (no partial copy)"),
                    "resource": key,
                }],
                stats={"table": table_name, "modality": "blob", "cap_bytes": cap},
            )

        # Resume just past the last object emitted in the previous batch.
        start_fi = 0
        if cursor and cursor.get("fkey"):
            flm = cursor.get("flm") or ""
            fkey = cursor.get("fkey")
            start_fi = next(
                (i for i, o in enumerate(files_sorted)
                 if (str(o.get("last_modified") or ""), o.get("key") or "") > (flm, fkey)),
                len(files_sorted),
            )

        blobs: List[Dict[str, Any]] = []
        cur_pos: Optional[Dict[str, Any]] = None
        fi = start_fi
        while fi < len(files_sorted) and len(blobs) < limit:
            obj = files_sorted[fi]
            key = obj.get("key")
            lm = str(obj.get("last_modified") or "")
            size_hint = obj.get("size")
            # Cap check on the listing's size first (cheap, pre-download).
            if isinstance(size_hint, int) and cap and size_hint > cap:
                return _over_cap(key, size_hint)
            content = self.download_file(config=config, bucket=settings["bucket"], key=key)
            # Re-check against the real byte length (listing size may be absent/stale).
            if cap and len(content) > cap:
                return _over_cap(key, len(content))
            sha = hashlib.sha256(content).hexdigest()
            ctype = content_type_for_object(key)
            data_ref = self._stage_blob(
                content, staging_config=staging_config,
                content_type=ctype, object_key=key, sha256=sha,
            )
            blobs.append({
                "object_key": key,
                "content_type": ctype,
                "size": len(content),
                "data_ref": data_ref,
                "sha256": sha,
                "source_file_modified": lm,
            })
            cur_pos = {"fkey": key, "flm": lm}
            fi += 1

        stats: Dict[str, Any] = {
            "total_files": len(files_sorted),
            "table": table_name,
            "blobs": len(blobs),
            "modality": "blob",
        }
        # Mirror the structured path: hand back a cursor whenever we emitted anything;
        # the next call resumes past it and returns empty -> terminal.
        next_cursor = cur_pos if blobs else None
        return _export_dict(True, blobs=blobs, next_cursor=next_cursor, stats=stats)
